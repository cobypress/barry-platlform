import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { contributeBenchmark, exportBenchmark } from "../../benchmark/export.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { HANDBOOK_MARKER, HANDBOOK_PLANTED } from "../../testing/synthetic-org.ts";
import type { RunOutputRecord } from "../../worker/types.ts";
import { createJobHandler } from "../../worker/worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "../../worker/__tests__/helpers.ts";
import { HANDBOOK_FILE } from "../build.ts";
import { HANDBOOK_DOCX } from "../docx.ts";
import { Handbook } from "../schema.ts";
import { docxText } from "../../testing/docx-text.ts";
import { processDocumentsJob } from "../../worker/documents-job.ts";
import type { DocumentDrive, DriveItem } from "../../worker/drive.ts";
import type { DocumentKey } from "../../render/render-released.ts";

// handbook.json is the customer's own configuration, and goes nowhere but the
// documents made for them (DECISIONS.md 452). A marker planted in a validation
// rule's formula is followed through every route out of a run: the run
// folder's other files, the worker's store and the console, Claude, and the
// benchmark. It must be in handbook.json and nowhere else. The made-up key
// beside it must be nowhere at all.

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
    deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), narrative });
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

  test("the marker reaches handbook.json", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.ok(files.get(HANDBOOK_FILE)!.includes(HANDBOOK_MARKER));
  });

  test("and no other file in the run folder", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    const outputs = [...files.keys()].filter(isOutput);
    // The files a worker's run leaves, so a new one can't slip past unchecked.
    for (const file of ["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json", "raw-notes.md", "report-sections.json", "facts.json", "run.json", "pseudonym-map.json"]) assert.ok(outputs.includes(file), file);
    assert.deepEqual(outputs.filter((f) => files.get(f)!.includes(HANDBOOK_MARKER)), []);
  });

  test("and the made-up key in the same formula reaches no file at all, checkpoints included", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(HANDBOOK_PLANTED)), []);
  });

  test("nothing of it is handed to the worker's store or the console", () => {
    assert.equal(saved.length, 1);
    assert.ok(saved[0]!.findings.length > 0 && saved[0]!.sections.length > 0, "the results were saved");
    assert.doesNotMatch(JSON.stringify(saved), new RegExp(HANDBOOK_MARKER));
    const store = { runs: [...deps.store.runs.values()], writes: deps.store.writes, documents: [...deps.store.documents.values()], benchmark: deps.store.benchmarkRows, scans: [...deps.store.scanResults.values()], pulses: [...deps.store.pulseResults.values()], microsites: [...deps.store.microsites.values()] };
    assert.doesNotMatch(JSON.stringify(store), new RegExp(HANDBOOK_MARKER));
    assert.doesNotMatch(JSON.stringify(store), /"steps":\[\{"kind"/, "no handbook step either");
  });

  test("nothing of it is sent to Claude", () => {
    assert.ok(asked.length > 10, `${asked.length} prompts`);
    assert.doesNotMatch(JSON.stringify(asked), new RegExp(HANDBOOK_MARKER));
  });

  test("a free scan, which reads the same metadata, hands none of it over either", async () => {
    const scan = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    scan.store.addRun(runInput({ run: consoleRun({ scope: "essentials", product: "scan" }) }));
    await scan.tokens.save(syntheticConnection());
    await createJobHandler(scan)({ kind: "run", runId: RUN_ID }, { final: true });
    assert.ok(scan.store.scanResults.get(RUN_ID), "the teaser is saved");
    assert.ok(scan.store.runs.get(RUN_ID)!.run.steps.some((s) => s.id === "metadata"), "the scan read the metadata");
    assert.doesNotMatch(JSON.stringify({ runs: [...scan.store.runs.values()], scans: [...scan.store.scanResults.values()], writes: scan.store.writes }), new RegExp(HANDBOOK_MARKER));
  });
});

describe("the documents job: handbook.json reaches the Word document and nothing else", () => {
  // Decision 452 as decision 443 amends it: the file's contents go to the
  // customer's handbook document and nowhere else. A literal planted in a
  // formula, a condition's value, a description and a message is followed
  // from the run folder through a documents make.
  const LITERAL = "HB-DOC-LITERAL-0443";
  const uploads: Array<{ parent: string; name: string; body: Buffer }> = [];
  let deps: ReturnType<typeof baseDeps>;
  let outcome: Awaited<ReturnType<typeof processDocumentsJob>>;
  let dir: string;

  /** Drive as the job sees it, keeping every upload. */
  class KeptDrive implements DocumentDrive {
    readonly rootFolderId = "root-folder-0001";
    readonly #items: Array<DriveItem & { parent: string; folder: boolean }> = [];
    #add(parent: string, name: string, folder: boolean): DriveItem {
      const item = { id: `drive-item-${String(this.#items.length + 1).padStart(4, "0")}`, name, url: "https://drive.example.com/x", parent, folder };
      this.#items.push(item);
      return item;
    }
    async folder(id: string) {
      return this.#items.find((i) => i.id === id && i.folder) ?? null;
    }
    async folders(parent: string) {
      return this.#items.filter((i) => i.parent === parent && i.folder);
    }
    async files(parent: string) {
      return this.#items.filter((i) => i.parent === parent && !i.folder);
    }
    async createFolder(parent: string, name: string) {
      return this.#add(parent, name, true);
    }
    async putFile(parent: string, name: string, _mime: string, body: Buffer, replaceId?: string) {
      uploads.push({ parent, name, body });
      return replaceId ? this.#items.find((i) => i.id === replaceId)! : this.#add(parent, name, false);
    }
    async moveFile() {}
  }

  before(async () => {
    const { dir: source } = await runFromRecording({ answers: loadAnswers() });
    deps = baseDeps({
      drive: new KeptDrive(),
      // The reports are placeholders: the handbook is made by the job itself, from the run folder, whatever renders them.
      renderReleased: async (_dir, _approved, out) => {
        mkdirSync(out, { recursive: true });
        const files = {} as Record<DocumentKey, string>;
        for (const key of ["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"] as DocumentKey[]) writeFileSync((files[key] = join(out, `${key}.bin`)), `synthetic ${key}`);
        return { files, warnings: [], held: [] };
      },
    });
    dir = join(deps.runsDir, RUN_ID);
    cpSync(source, dir, { recursive: true });
    const handbook = Handbook.parse(JSON.parse(readFileSync(join(dir, HANDBOOK_FILE), "utf8")));
    assert.ok(handbook.validationRules.status === "ok" && handbook.flows.status === "ok");
    handbook.validationRules.items.push({ object: "Invoice__c", name: "Planted_Rule", active: true, description: `Description ${LITERAL}`, formula: `CONTAINS(Notes__c, "${LITERAL}")`, formulaCut: false, formulaLength: 40, message: `Message ${LITERAL}`, messageField: null });
    const flow = handbook.flows.items.find((f) => f.name === "Opportunity_Stage_Review")!;
    flow.start.entry.conditions[0]!.value = LITERAL;
    writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(handbook));
    deps.store.addDocumentsJob({ run: consoleRun({ status: "released" }), intake: null, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
    outcome = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(outcome.outcome, "done", outcome.detail);
  });

  test("the Word document holds it, on the worker's disk and in the customer's folder", () => {
    for (const bytes of [readFileSync(join(dir, "documents", HANDBOOK_DOCX)), uploads.find((u) => u.name.endsWith(" - Org handbook.docx"))!.body]) {
      const text = docxText(bytes);
      for (const planted of [`CONTAINS(Notes__c, "${LITERAL}")`, `StageName equals ${LITERAL}`, `Description ${LITERAL}`, `Message ${LITERAL}`, HANDBOOK_MARKER]) assert.ok(text.includes(planted), planted);
    }
  });

  test("no other upload holds it", () => {
    const others = uploads.filter((u) => !u.name.endsWith(" - Org handbook.docx"));
    assert.equal(others.length, 7, "the six documents and the readout kit");
    for (const u of others) assert.ok(!u.body.includes(LITERAL) && !u.body.includes(HANDBOOK_MARKER), u.name);
  });

  test("nor the store's record, its warnings, the log or the queue's outcome", () => {
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.ok(made.files.length === 6 && !made.files.some((f) => /handbook/i.test(f.name)));
    const store = { documents: [...deps.store.documents.values()], writes: deps.store.writes, runs: [...deps.store.runs.values()], shares: deps.store.shares, microsites: [...deps.store.microsites.values()], benchmark: deps.store.benchmarkRows };
    for (const [where, text] of [["store", JSON.stringify(store)], ["warnings", JSON.stringify(made.warnings)], ["log", deps.lines.join("\n")], ["outcome", JSON.stringify(outcome)]] as const) {
      for (const planted of [LITERAL, HANDBOOK_MARKER, "Planted_Rule", "Opportunity_Stage_Review"]) assert.ok(!text.includes(planted), `${planted} in the ${where}`);
    }
  });

  test("nor any other file the make leaves in the run folder", () => {
    const files = filesIn(dir);
    const holding = [...files.keys()].filter((f) => isOutput(f) && f !== `documents/${HANDBOOK_DOCX}` && files.get(f)!.includes(LITERAL));
    assert.deepEqual(holding, []);
    assert.ok(files.has(`documents/${HANDBOOK_DOCX}`));
  });
});

describe("the benchmark", () => {
  let source: string;
  before(async () => {
    source = (await runFromRecording({ answers: loadAnswers() })).dir;
  });
  const copy = (): string => {
    const dir = join(tempDir(), "run");
    cpSync(source, dir, { recursive: true });
    return dir;
  };
  const options = (runDir: string) => ({ runDir, industry: "technology", headcount: "51-200", consent: "Synthetic Person agreed by email", now: new Date("2026-11-03T10:00:00.000Z") });

  test("the run it's made from has the marker in its handbook", () => assert.ok(readFileSync(join(source, HANDBOOK_FILE), "utf8").includes(HANDBOOK_MARKER)));

  test("hc benchmark's store and consent record hold none of it", () => {
    const run = copy();
    const store = tempDir();
    const result = exportBenchmark({ ...options(run), storeDir: store });
    assert.ok(result.rows.length > 0);
    for (const [path, text] of [...filesIn(store), ...[...filesIn(run)].filter(([f]) => isOutput(f))]) assert.ok(!text.includes(HANDBOOK_MARKER), path);
    assert.doesNotMatch(JSON.stringify(result), new RegExp(HANDBOOK_MARKER));
  });

  test("the worker's contribution to the console's benchmark table holds none of it", async () => {
    const written: unknown[] = [];
    const result = await contributeBenchmark(options(copy()), async (rows) => void written.push(...rows));
    assert.ok(written.length > 0);
    assert.doesNotMatch(JSON.stringify([written, result]), new RegExp(HANDBOOK_MARKER));
  });
});

describe("what reads it", () => {
  // The routes above are followed for the synthetic org; this holds the code to
  // them. Only these files may name the handbook. Each says why.
  const ALLOWED = new Map([
    ["src/handbook/schema.ts", "its shape"],
    ["src/handbook/detail.ts", "reads flows and validation rules"],
    ["src/handbook/build.ts", "builds the file"],
    ["src/handbook/read.ts", "the one reader of the file (DECISIONS.md 443)"],
    ["src/handbook/docx.ts", "makes the Word document from it"],
    ["src/handbook/sample.ts", "writes the synthetic org's document outside the repo, for a person to read"],
    ["src/worker/documents-job.ts", "puts the Word document in the customer's Drive folder, and lists it nowhere"],
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
