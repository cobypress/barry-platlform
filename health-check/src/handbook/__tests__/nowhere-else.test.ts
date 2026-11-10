import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { cpSync, readdirSync, readFileSync, statSync } from "node:fs";
import { join, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { contributeBenchmark, exportBenchmark } from "../../benchmark/export.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadRecording, ReplayTransport, type Recording } from "../../testing/replay.ts";
import { HANDBOOK_MARKER, HANDBOOK_PLANTED } from "../../testing/synthetic-org.ts";
import type { RunOutputRecord } from "../../worker/types.ts";
import { createJobHandler } from "../../worker/worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "../../worker/__tests__/helpers.ts";
import { HANDBOOK_FILE } from "../build.ts";

// handbook.json is the customer's own configuration, and goes nowhere but the
// documents made for them (DECISIONS.md 452). A marker planted in a validation
// rule's formula is followed through every route out of a run: the run
// folder's other files, the worker's store and the console, Claude, and the
// benchmark. It must be in handbook.json and nowhere else. The made-up key
// beside it must be nowhere at all.
//
// So with Apex (decisions 456 and 457): an object named only in a class's
// query reaches handbook.json and nowhere else, and a string and a comment
// from the same class reach nothing at all, the handbook included.

const APEX_OBJECT = "Zebra_Ledger__c";
// Each written as a query, so a reading that let either through would keep it.
const APEX_LITERAL = "ZebraLiteral0457";
const APEX_COMMENT = "ZebraComment0457";
/** In handbook.json and nowhere else. */
const ROUTED = [HANDBOOK_MARKER, APEX_OBJECT];
/** Nowhere at all. */
const NOWHERE = [HANDBOOK_PLANTED, APEX_LITERAL, APEX_COMMENT];

/** The recorded synthetic org, with one more class whose body holds all three. */
function plantedRecording(): Recording {
  const recording = loadRecording();
  const classes = recording.exchanges.find((e) => decodeURIComponent(e.key).includes("FROM ApexClass WHERE"))!;
  const page = JSON.parse(classes.text) as { totalSize: number; records: unknown[] };
  page.records.push({
    Id: "01p000000000099AAA",
    Name: "ZebraLedgerSync",
    ApiVersion: 62,
    Status: "Active",
    LengthWithoutComments: 200,
    Body: `public class ZebraLedgerSync { // Was [SELECT Id FROM ${APEX_COMMENT}]\n public void sync() { String tag = '[SELECT Id FROM ${APEX_LITERAL}]'; List<SObject> rows = [SELECT Id FROM ${APEX_OBJECT} WHERE Name = :tag]; update rows; } }`,
  });
  page.totalSize = page.records.length;
  classes.text = JSON.stringify(page);
  return recording;
}

const holding = (text: string, markers: string[]): string[] => markers.filter((m) => text.includes(m));

/** Every file in a run folder, by its path inside it. */
function filesIn(dir: string): Map<string, string> {
  const files = new Map<string, string>();
  for (const name of readdirSync(dir, { recursive: true }) as string[]) {
    const path = join(dir, name);
    if (statSync(path).isFile()) files.set(name.split(sep).join("/"), readFileSync(path, "latin1"));
  }
  return files;
}

/** Outputs: everything but the handbook itself and the raw extracts it's made from, which `hc purge` deletes. */
const isOutput = (path: string): boolean => path !== HANDBOOK_FILE && !path.startsWith("checkpoints/");

describe("the handbook's detail goes nowhere but handbook.json", () => {
  const asked: NarrativeRequest[] = [];
  const saved: RunOutputRecord[] = [];
  let deps: ReturnType<typeof baseDeps>;

  before(async () => {
    // Claude drafts every briefed section, so every prompt is seen.
    const narrative: NarrativeClient = {
      complete: async (request) => {
        asked.push(request);
        return { stopReason: "end_turn", model: request.model, text: JSON.stringify({ paragraphs: [{ text: "We looked closely at this area.", sources: [] }] }) };
      },
    };
    deps = baseDeps({ dataTransport: new ReplayTransport(plantedRecording()), narrative });
    // Exactly what the Postgres store would be handed.
    const save = deps.store.saveRunOutput.bind(deps.store);
    deps.store.saveRunOutput = async (runId, output) => {
      saved.push(structuredClone(output));
      return save(runId, output);
    };
    deps.store.addRun(runInput({ answers: loadAnswers() }));
    await deps.tokens.save(syntheticConnection());
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true })).outcome, "done");
  });

  test("the marker and the object named only in an Apex query reach handbook.json", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.deepEqual(holding(files.get(HANDBOOK_FILE)!, ROUTED), ROUTED);
    // As what the planted class queries, not by chance.
    const classes = (JSON.parse(files.get(HANDBOOK_FILE)!) as { apexClasses: { items: Array<{ name: string; touches: { queries: string[] } | null }> } }).apexClasses.items;
    assert.deepEqual(classes.find((c) => c.name === "ZebraLedgerSync")?.touches?.queries, [APEX_OBJECT]);
  });

  test("and no other file in the run folder", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    const outputs = [...files.keys()].filter(isOutput);
    // The files a worker's run leaves, so a new one can't slip past unchecked.
    for (const file of ["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json", "raw-notes.md", "report-sections.json", "facts.json", "run.json", "pseudonym-map.json"]) assert.ok(outputs.includes(file), file);
    assert.deepEqual(outputs.filter((f) => holding(files.get(f)!, ROUTED).length > 0), []);
  });

  test("and the made-up key in the same formula, and the Apex body's string and comment, reach no file at all, checkpoints included", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.ok(files.has(HANDBOOK_FILE) && files.has("checkpoints/apex.json"));
    assert.deepEqual([...files.keys()].filter((f) => holding(files.get(f)!, NOWHERE).length > 0), []);
  });

  test("nothing of it is handed to the worker's store or the console", () => {
    assert.equal(saved.length, 1);
    assert.ok(saved[0]!.findings.length > 0 && saved[0]!.sections.length > 0, "the results were saved");
    assert.deepEqual(holding(JSON.stringify(saved), [...ROUTED, ...NOWHERE]), []);
    const store = { runs: [...deps.store.runs.values()], writes: deps.store.writes, documents: [...deps.store.documents.values()], benchmark: deps.store.benchmarkRows, scans: [...deps.store.scanResults.values()], pulses: [...deps.store.pulseResults.values()], microsites: [...deps.store.microsites.values()] };
    assert.deepEqual(holding(JSON.stringify(store), [...ROUTED, ...NOWHERE]), []);
    assert.doesNotMatch(JSON.stringify(store), /"steps":\[\{"kind"/, "no handbook step either");
  });

  test("nothing of it is sent to Claude", () => {
    assert.ok(asked.length > 10, `${asked.length} prompts`);
    assert.deepEqual(holding(JSON.stringify(asked), [...ROUTED, ...NOWHERE]), []);
  });

  test("a free scan, which reads the same metadata, hands none of it over either", async () => {
    const scan = baseDeps({ dataTransport: new ReplayTransport(plantedRecording()) });
    scan.store.addRun(runInput({ run: consoleRun({ scope: "essentials", product: "scan" }) }));
    await scan.tokens.save(syntheticConnection());
    await createJobHandler(scan)({ kind: "run", runId: RUN_ID }, { final: true });
    assert.ok(scan.store.scanResults.get(RUN_ID), "the teaser is saved");
    assert.ok(scan.store.runs.get(RUN_ID)!.run.steps.some((s) => s.id === "metadata"), "the scan read the metadata");
    assert.deepEqual(holding(JSON.stringify({ runs: [...scan.store.runs.values()], scans: [...scan.store.scanResults.values()], writes: scan.store.writes }), [...ROUTED, ...NOWHERE]), []);
  });
});

describe("the benchmark", () => {
  let source: string;
  before(async () => {
    source = (await runFromRecording({ answers: loadAnswers(), transport: new ReplayTransport(plantedRecording()) })).dir;
  });
  const copy = (): string => {
    const dir = join(tempDir(), "run");
    cpSync(source, dir, { recursive: true });
    return dir;
  };
  const options = (runDir: string) => ({ runDir, industry: "technology", headcount: "51-200", consent: "Synthetic Person agreed by email", now: new Date("2026-11-03T10:00:00.000Z") });

  test("the run it's made from has the markers in its handbook", () => assert.deepEqual(holding(readFileSync(join(source, HANDBOOK_FILE), "utf8"), ROUTED), ROUTED));

  test("hc benchmark's store and consent record hold none of it", () => {
    const run = copy();
    const store = tempDir();
    const result = exportBenchmark({ ...options(run), storeDir: store });
    assert.ok(result.rows.length > 0);
    for (const [path, text] of [...filesIn(store), ...[...filesIn(run)].filter(([f]) => isOutput(f))]) assert.deepEqual(holding(text, [...ROUTED, ...NOWHERE]), [], path);
    assert.deepEqual(holding(JSON.stringify(result), [...ROUTED, ...NOWHERE]), []);
  });

  test("the worker's contribution to the console's benchmark table holds none of it", async () => {
    const written: unknown[] = [];
    const result = await contributeBenchmark(options(copy()), async (rows) => void written.push(...rows));
    assert.ok(written.length > 0);
    assert.deepEqual(holding(JSON.stringify([written, result]), [...ROUTED, ...NOWHERE]), []);
  });
});

describe("what reads it", () => {
  // The routes above are followed for the synthetic org; this holds the code to
  // them. Only these files may name the handbook. Each says why.
  const ALLOWED = new Map([
    ["src/handbook/schema.ts", "its shape"],
    ["src/handbook/detail.ts", "reads flows and validation rules"],
    ["src/handbook/build.ts", "builds the file"],
    ["src/extract/metadata-parse.ts", "keeps the detail on the retrieve route"],
    ["src/extract/metadata-tooling.ts", "keeps the detail on the Tooling route"],
    ["src/engine/run.ts", "writes handbook.json"],
    ["src/testing/fingerprint.ts", "hashes it, in tests only"],
    ["src/testing/synthetic-org.ts", "plants its traps"],
    ["src/facts/questions.ts", "names its section of QUESTIONS.md, and reads no run"],
  ]);
  const root = fileURLToPath(new URL("../../../", import.meta.url));

  test("no other source file names the handbook or its file", () => {
    const naming: string[] = [];
    for (const top of ["src", "scripts"]) {
      for (const name of readdirSync(join(root, top), { recursive: true }) as string[]) {
        const path = `${top}/${name.split(sep).join("/")}`;
        if (!path.endsWith(".ts") || path.includes("/__tests__/")) continue;
        if (/handbook/i.test(readFileSync(join(root, path), "utf8"))) naming.push(path);
      }
    }
    assert.deepEqual(naming.filter((p) => !ALLOWED.has(p)), []);
    // The scan finds what it should, so an empty answer above means something.
    assert.ok(naming.includes("src/engine/run.ts") && naming.includes("src/handbook/build.ts"));
  });
});
