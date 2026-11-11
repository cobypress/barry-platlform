import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { contributeBenchmark, exportBenchmark } from "../../benchmark/export.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, loadRecording, ReplayTransport, syntheticProvider, type Recording } from "../../testing/replay.ts";
import { APEX_OUTSIDE_MODIFIER, AS_OF, CHANGES_DELEGATE, COMPONENT_OUTSIDE_MODIFIER, CHANGES_MARKER, CHANGES_PLANTED, HANDBOOK_CASE_MARKER, HANDBOOK_CASE_PLANTED, HANDBOOK_MARKER, HANDBOOK_PLANTED, USER_ADMIN_MARKER } from "../../testing/synthetic-org.ts";
import type { RunOutputRecord } from "../../worker/types.ts";
import { createJobHandler } from "../../worker/worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "../../worker/__tests__/helpers.ts";
import { HANDBOOK_FILE } from "../build.ts";
import { HANDBOOK_DOCX } from "../docx.ts";
import { Handbook } from "../schema.ts";
import { docxText } from "../../testing/docx-text.ts";
import { withEarlier } from "../../testing/earlier-handbook.ts";
import { processDocumentsJob } from "../../worker/documents-job.ts";
import type { DocumentDrive, DriveItem } from "../../worker/drive.ts";
import type { DocumentKey } from "../../render/render-released.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { Simulator } from "../../testing/simulator.ts";
import { MODELS } from "../../config.ts";
import { APEX_SOURCE_FILE } from "../../extract/apex-source.ts";
import { APEX_SUMMARY_PROMPT } from "../../narrative/apex-prompt.ts";
import { tokenSource } from "../../privacy/pseudonymiser.ts";
import { fingerprint } from "../../testing/fingerprint.ts";
import { usersTable } from "../../testing/synthetic-org.ts";
import { SUMMARIES_FILE, SUMMARIES_PARTIAL_FILE } from "../summaries.ts";

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
//
// And with the recent changes (decision 461): a marker in a setup change's
// sentence, and the username someone was logged in as, reach handbook.json and
// nowhere else; a key in the same sentence reaches no file but as [REDACTED];
// and a user administration change, with its person's email, reaches no file
// at all, the step's checkpoint included.
//
// And with who last changed each class (decision 473): a name Salesforce gives
// beside a class, for someone the run's user list doesn't hold, reaches the
// pseudonym map and the step's checkpoint, which the labelling reads, and no
// other file: handbook.json holds the token. Its only way out is the Word
// document, through the map.
//
// And with who last changed each flow and validation rule (decision 482): a
// name Salesforce gives beside a flow's active version, and another beside a
// rule, each for someone the run's user list doesn't hold, the same way, the
// flows' and rules' own step's checkpoint holding them.

const APEX_OBJECT = "Zebra_Ledger__c";
// Each written as a query, so a reading that let either through would keep it.
const APEX_LITERAL = "ZebraLiteral0457";
const APEX_COMMENT = "ZebraComment0457";
// The Case pair is in a rule on a standard object read for its rules only (decision 460).
/** The person a planted user administration change names. */
const USER_ADMIN_EMAIL = "pat.quinn@example.com";
/** In handbook.json and nowhere else. */
const ROUTED = [HANDBOOK_MARKER, HANDBOOK_CASE_MARKER, APEX_OBJECT, CHANGES_MARKER, CHANGES_DELEGATE];
/** Nowhere at all. */
const NOWHERE = [HANDBOOK_PLANTED, HANDBOOK_CASE_PLANTED, APEX_LITERAL, APEX_COMMENT, CHANGES_PLANTED, USER_ADMIN_MARKER, USER_ADMIN_EMAIL];
/** Who last changed the planted class, as Salesforce names them: in the pseudonym map and the step's checkpoint, and no other file. */
const APEX_MODIFIER = "Wren Quillfeather";
const APEX_MODIFIER_ID = "005000000000961AAA";
/** Who last changed a planted flow version and a rule, as Salesforce names them: in the pseudonym map and that step's checkpoint, and no other file. */
const FLOW_MODIFIER = "Fen Larkspur";
const FLOW_MODIFIER_ID = "005000000000966AAA";
const RULE_MODIFIER = "Oriel Thistlecombe";
const RULE_MODIFIER_ID = "005000000000967AAA";

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
    LastModifiedDate: "2026-10-02T09:00:00.000+0000",
    LastModifiedById: APEX_MODIFIER_ID,
    LastModifiedBy: { Name: APEX_MODIFIER },
    Body: `public class ZebraLedgerSync { // Was [SELECT Id FROM ${APEX_COMMENT}]\n public void sync() { String tag = '[SELECT Id FROM ${APEX_LITERAL}]'; List<SObject> rows = [SELECT Id FROM ${APEX_OBJECT} WHERE Name = :tag]; update rows; } }`,
  });
  page.totalSize = page.records.length;
  classes.text = JSON.stringify(page);
  // A later active version of a synthetic flow, and a change to a synthetic rule, each by someone the user list doesn't hold.
  const changed = (id: string, name: string) => ({ LastModifiedDate: "2026-10-03T09:00:00.000+0000", LastModifiedById: id, LastModifiedBy: { Name: name } });
  const plant = (from: string, edit: (records: Array<Record<string, any>>) => void) => {
    const exchange = recording.exchanges.find((e) => decodeURIComponent(e.key).includes(from))!;
    const list = JSON.parse(exchange.text) as { totalSize: number; records: Array<Record<string, any>> };
    edit(list.records);
    list.totalSize = list.records.length;
    exchange.text = JSON.stringify(list);
  };
  plant("LastModifiedBy.Name FROM Flow", (records) => {
    for (const r of records) if (r.Definition.DeveloperName === "Opportunity_Stage_Review") r.Status = "Obsolete";
    records.push({ Id: "301000000000999AAA", DefinitionId: "300000000000999AAA", Definition: { DeveloperName: "Opportunity_Stage_Review", NamespacePrefix: null }, VersionNumber: 2, Status: "Active", ...changed(FLOW_MODIFIER_ID, FLOW_MODIFIER) });
  });
  plant("LastModifiedBy.Name FROM ValidationRule", (records) => Object.assign(records.find((r) => r.ValidationName === "Reason_When_Escalated")!, changed(RULE_MODIFIER_ID, RULE_MODIFIER)));
  return recording;
}

const holding = (text: string, markers: string[]): string[] => markers.filter((m) => text.includes(m));
/** Who last changed a class, flow or rule, the planted ones' and the synthetic org's own: named in no output but the pseudonym map. */
const MODIFIERS = [APEX_MODIFIER, APEX_OUTSIDE_MODIFIER.name, FLOW_MODIFIER, RULE_MODIFIER, COMPONENT_OUTSIDE_MODIFIER.name];

/** Every file in a run folder, by its path inside it. */
function filesIn(dir: string): Map<string, string> {
  const files = new Map<string, string>();
  for (const name of readdirSync(dir, { recursive: true }) as string[]) {
    const path = join(dir, name);
    if (statSync(path).isFile()) files.set(name.split(sep).join("/"), readFileSync(path, "latin1"));
  }
  return files;
}

/**
 * Which reading made the handbook (decision 484): small numbers, held to
 * handbook.json and the two steps' own outputs all the same, with the build's
 * in handbook.json and the number a Tooling read was started under in its kept
 * file while it's part way (485). By their keys, since a number can't be followed.
 */
const READINGS = ["readBy", "handbookReading"];

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

  test("who last changed the planted class is a token in handbook.json, and named only in the pseudonym map and the step's checkpoint", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(APEX_MODIFIER)).sort(), ["checkpoints/apex.json", "pseudonym-map.json"]);
    const entry = (JSON.parse(files.get("pseudonym-map.json")!) as Array<{ token: string; id: string; name: string }>).find((e) => e.id === APEX_MODIFIER_ID)!;
    assert.equal(entry.name, APEX_MODIFIER);
    const classes = (JSON.parse(files.get(HANDBOOK_FILE)!) as { apexClasses: { items: Array<{ name: string; lastChanged: { at: string; by: string | null } | null }> } }).apexClasses.items;
    assert.deepEqual(classes.find((c) => c.name === "ZebraLedgerSync")?.lastChanged, { at: "2026-10-02T09:00:00.000+0000", by: entry.token });
    // And the synthetic org's own, the same way, who last changed a flow as well (decision 482).
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(APEX_OUTSIDE_MODIFIER.name)).sort(), ["checkpoints/apex.json", "checkpoints/component-changes.json", "pseudonym-map.json"]);
  });

  test("who last changed the planted flow and rule are tokens in handbook.json, and named only in the pseudonym map and their step's checkpoint", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    const map = JSON.parse(files.get("pseudonym-map.json")!) as Array<{ token: string; id: string; name: string }>;
    const handbook = JSON.parse(files.get(HANDBOOK_FILE)!) as { flows: { items: Array<{ name: string; lastChanged: unknown }> }; validationRules: { items: Array<{ object: string; name: string; lastChanged: unknown }> } };
    for (const [name, id, lastChanged] of [
      [FLOW_MODIFIER, FLOW_MODIFIER_ID, handbook.flows.items.find((f) => f.name === "Opportunity_Stage_Review")!.lastChanged],
      [RULE_MODIFIER, RULE_MODIFIER_ID, handbook.validationRules.items.find((r) => r.object === "Case" && r.name === "Reason_When_Escalated")!.lastChanged],
      [COMPONENT_OUTSIDE_MODIFIER.name, COMPONENT_OUTSIDE_MODIFIER.id, handbook.validationRules.items.find((r) => r.name === "Due_Date_Required")!.lastChanged],
    ] as const) {
      assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(name)).sort(), ["checkpoints/component-changes.json", "pseudonym-map.json"], name);
      const entry = map.find((e) => e.id === id)!;
      assert.equal(entry.name, name);
      assert.equal((lastChanged as { by: string }).by, entry.token, name);
    }
  });

  test("and the made-up key in the same formula, and the Apex body's string and comment, reach no file at all, checkpoints included", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.ok(files.has(HANDBOOK_FILE) && files.has("checkpoints/apex.json"));
    // The setup audit step's checkpoint holds the changes it kept, with the key redacted.
    assert.ok(files.get("checkpoints/setup-audit.json")!.includes(CHANGES_MARKER));
    assert.ok(files.get(HANDBOOK_FILE)!.includes(`Sync to ${CHANGES_MARKER}\\" api_key = \\"[REDACTED]\\"`));
    assert.deepEqual([...files.keys()].filter((f) => holding(files.get(f)!, NOWHERE).length > 0), []);
  });

  test("nothing of it is handed to the worker's store or the console", () => {
    assert.equal(saved.length, 1);
    assert.ok(saved[0]!.findings.length > 0 && saved[0]!.sections.length > 0, "the results were saved");
    assert.deepEqual(holding(JSON.stringify(saved), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
    const store = { runs: [...deps.store.runs.values()], writes: deps.store.writes, documents: [...deps.store.documents.values()], benchmark: deps.store.benchmarkRows, scans: [...deps.store.scanResults.values()], pulses: [...deps.store.pulseResults.values()], microsites: [...deps.store.microsites.values()] };
    assert.deepEqual(holding(JSON.stringify(store), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
    assert.doesNotMatch(JSON.stringify(store), /"steps":\[\{"kind"/, "no handbook step either");
  });

  test("which reading made it is in handbook.json and the two steps' checkpoints, and handed to nothing", () => {
    const files = filesIn(join(deps.runsDir, RUN_ID));
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes("readBy")), [HANDBOOK_FILE]);
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes("handbookReading")).sort(), ["checkpoints/apex.json", "checkpoints/metadata.json"]);
    const store = { saved, runs: [...deps.store.runs.values()], writes: deps.store.writes, documents: [...deps.store.documents.values()], benchmark: deps.store.benchmarkRows, scans: [...deps.store.scanResults.values()] };
    assert.deepEqual(holding(JSON.stringify([store, asked, deps.lines]), READINGS), []);
  });

  test("nothing of it is sent to Claude", () => {
    assert.ok(asked.length > 10, `${asked.length} prompts`);
    assert.deepEqual(holding(JSON.stringify(asked), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
  });

  test("a free scan, which reads the same metadata, hands none of it over either", async () => {
    const scan = baseDeps({ dataTransport: new ReplayTransport(plantedRecording()) });
    scan.store.addRun(runInput({ run: consoleRun({ scope: "essentials", product: "scan" }) }));
    await scan.tokens.save(syntheticConnection());
    await createJobHandler(scan)({ kind: "run", runId: RUN_ID }, { final: true });
    assert.ok(scan.store.scanResults.get(RUN_ID), "the teaser is saved");
    assert.ok(scan.store.runs.get(RUN_ID)!.run.steps.some((s) => s.id === "metadata"), "the scan read the metadata");
    assert.deepEqual(holding(JSON.stringify({ runs: [...scan.store.runs.values()], scans: [...scan.store.scanResults.values()], writes: scan.store.writes }), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
  });
});

describe("the number a Tooling read was started under (decision 485)", () => {
  test("is kept beside its items while the read is part way, holds nothing else, and is gone with them; no output holds a reading", async () => {
    const simulator = new Simulator({ readOnly: true });
    const dropping: Transport = { send: (request) => (decodeURIComponent(request.url).includes("FROM FieldDefinition") ? Promise.reject(new Error("socket hang up")) : simulator.send(request)) };
    const outDir = tempDir();
    const options = { provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), log: () => {} };
    await assert.rejects(runHealthCheck({ ...options, transport: dropping }), /socket hang up/);
    const [runId] = readdirSync(outDir);
    const stopped = filesIn(join(outDir, runId!));
    assert.equal(stopped.get("checkpoints/metadata-tooling-reading.partial.jsonl"), '{"handbookReading":1}\n');
    assert.deepEqual([...stopped.keys()].filter((f) => holding(stopped.get(f)!, READINGS).length > 0), ["checkpoints/metadata-tooling-reading.partial.jsonl"]);
    await runHealthCheck({ ...options, transport: new Simulator({ readOnly: true }), resume: runId });
    const files = filesIn(join(outDir, runId!));
    assert.ok(!files.has("checkpoints/metadata-tooling-reading.partial.jsonl"));
    assert.deepEqual([...files.keys()].filter((f) => holding(files.get(f)!, READINGS).length > 0).sort(), ["checkpoints/apex.json", "checkpoints/metadata.json", HANDBOOK_FILE]);
  });
});

describe("the documents job: handbook.json reaches the Word document and nothing else", () => {
  // Decision 452 as decision 443 amends it: the file's contents go to the
  // customer's handbook document and nowhere else. A literal planted in a
  // formula, a condition's value, a description, a message and a scheduled
  // path's label (decision 446) is followed from the run folder through a documents make, and so are objects named only
  // in an Apex class's and a trigger's queries (decisions 456 and 457).
  const LITERAL = "HB-DOC-LITERAL-0443";
  const CLASS_OBJECT = "Hbdoc_Class_Ledger_0443__c";
  const TRIGGER_OBJECT = "Hbdoc_Trigger_Ledger_0443__c";
  // And a re-check's earlier handbook (decision 483): a flow only the earlier
  // health check had, so it's in the part's "Removed" list, reaches the Word
  // document and nothing else; the earlier text of a rule's formula, which the
  // part says only differs, reaches nothing at all; and the earlier folder is
  // only read.
  const SINCE_MARKER = "HB-SINCE-MARKER-0483";
  const SINCE_FLOW = "Hb_Since_Removed_0483";
  const SINCE_NOWHERE = "HB-SINCE-NOWHERE-0483";
  /** In the Word document and nowhere else the make leaves. */
  const PLANTED = [LITERAL, HANDBOOK_MARKER, HANDBOOK_CASE_MARKER, CLASS_OBJECT, TRIGGER_OBJECT, CHANGES_MARKER, CHANGES_DELEGATE, SINCE_MARKER];
  const uploads: Array<{ parent: string; name: string; body: Buffer }> = [];
  let deps: ReturnType<typeof baseDeps>;
  let outcome: Awaited<ReturnType<typeof processDocumentsJob>>;
  let dir: string;
  let earlier: string;
  let earlierBefore: Map<string, string>;
  const asked: NarrativeRequest[] = [];

  /** Each file in a folder, by its path, with its size, time and hash: what a write would move. */
  const snapshot = (folder: string): Map<string, string> =>
    new Map(
      (readdirSync(folder, { recursive: true }) as string[]).sort().map((name) => {
        const s = statSync(join(folder, name));
        return [name, s.isFile() ? `${s.size} ${s.mtimeMs} ${s.mode} ${createHash("sha256").update(readFileSync(join(folder, name))).digest("hex")}` : `dir ${s.mtimeMs} ${s.mode}`];
      }),
    );

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
      // Nothing in a documents make goes to Claude: anything that did would be seen here.
      narrative: { complete: async (request) => (asked.push(request), { stopReason: "end_turn", model: request.model, text: "{}" }) },
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
    assert.ok(handbook.validationRules.status === "ok" && handbook.flows.status === "ok" && handbook.apexClasses.status === "ok" && handbook.apexTriggers.status === "ok");
    handbook.validationRules.items.push({ object: "Invoice__c", name: "Planted_Rule", active: true, description: `Description ${LITERAL}`, formula: `CONTAINS(Notes__c, "${LITERAL}")`, formulaCut: false, formulaLength: 40, message: `Message ${LITERAL}`, messageField: null, lastChanged: null });
    const flow = handbook.flows.items.find((f) => f.name === "Opportunity_Stage_Review")!;
    flow.start.entry.conditions[0]!.value = LITERAL;
    const scheduled = flow.start.paths.find((p) => p.kind === "scheduled")!;
    scheduled.label = `Remind ${LITERAL}`;
    const touches = (object: string) => ({ queries: [object], queriesTotal: 1, changes: ["update" as const], dynamicQueries: false, searches: false });
    handbook.apexClasses.items.push({ name: "PlantedLedgerSync", apiVersion: 62, status: "Active", isTest: false, touches: touches(CLASS_OBJECT), lastChanged: null });
    handbook.apexClasses.total += 1;
    handbook.apexTriggers.items.push({ name: "PlantedLedgerTrigger", apiVersion: 62, status: "Active", isTest: false, touches: touches(TRIGGER_OBJECT), lastChanged: null, object: "Account", events: ["after update"] });
    handbook.apexTriggers.total += 1;
    writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(handbook));
    earlier = withEarlier(dir, (h) => {
      assert.ok(h.flows.status === "ok" && h.validationRules.status === "ok");
      h.flows.items.push({ ...structuredClone(h.flows.items[0]!), name: SINCE_FLOW, label: SINCE_MARKER, description: `Description ${SINCE_NOWHERE}` });
      h.validationRules.items.find((r) => r.name === "Needs_Competitor")!.formula = `CONTAINS(Notes__c, "${SINCE_NOWHERE}")`;
    });
    earlierBefore = snapshot(earlier);
    deps.store.addDocumentsJob({ run: consoleRun({ status: "released" }), intake: null, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
    outcome = await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(outcome.outcome, "done", outcome.detail);
  });

  test("the Word document holds it, on the worker's disk and in the customer's folder", () => {
    for (const bytes of [readFileSync(join(dir, "documents", HANDBOOK_DOCX)), uploads.find((u) => u.name.endsWith(" - Org handbook.docx"))!.body]) {
      const text = docxText(bytes);
      for (const planted of [`CONTAINS(Notes__c, "${LITERAL}")`, `StageName equals ${LITERAL}`, `Description ${LITERAL}`, `Message ${LITERAL}`, HANDBOOK_MARKER, HANDBOOK_CASE_MARKER]) assert.ok(text.includes(planted), planted);
      // A recent change's sentence, its key redacted, and who was logged in as the user who made the newest.
      assert.ok(text.includes(`Sync to ${CHANGES_MARKER}" api_key = "[REDACTED]"`), CHANGES_MARKER);
      assert.ok(text.includes(`(${CHANGES_DELEGATE} logged in as them)`), CHANGES_DELEGATE);
      for (const nowhere of [CHANGES_PLANTED, USER_ADMIN_MARKER, USER_ADMIN_EMAIL]) assert.ok(!text.includes(nowhere), nowhere);
      // Who last changed a class, restored from the run's map: someone the user list doesn't hold, named as Salesforce named them.
      assert.ok(text.includes(`\nFinanceSync\nAPI version 62.0. Status: Active. Test: No.\nLast changed 13 Oct 2026 by ${APEX_OUTSIDE_MODIFIER.name}\n`), APEX_OUTSIDE_MODIFIER.name);
      // And a validation rule, the same way (decision 482).
      assert.ok(text.includes(`\n#### Due_Date_Required\n\nActive: Yes\n\nLast changed 6 Oct 2026 by ${COMPONENT_OUTSIDE_MODIFIER.name}\n`), COMPONENT_OUTSIDE_MODIFIER.name);
      // The path's label, under what starts the flow and on the step it starts at.
      assert.ok(text.includes(`\n'Remind ${LITERAL}': 30 days before CloseDate: step 9, Flag Stale\n`), "the path's label");
      assert.ok(text.includes(`Flag_Stale\nStarts the path 'Remind ${LITERAL}': 30 days before CloseDate\n`), "on its step");
      // Each Apex object as what its class or trigger queries.
      assert.ok(text.includes(`PlantedLedgerSync\nAPI version 62.0. Status: Active. Test: No.\nQueries: ${CLASS_OBJECT}\n`), CLASS_OBJECT);
      assert.ok(text.includes(`PlantedLedgerTrigger\nOn: Account\nRuns: after update\nAPI version 62.0. Status: Active.\nQueries: ${TRIGGER_OBJECT}\n`), TRIGGER_OBJECT);
    }
  });

  test("a flow only the earlier health check had is in the part's Removed list, and the earlier text of a changed formula is nowhere in it", () => {
    for (const bytes of [readFileSync(join(dir, "documents", HANDBOOK_DOCX)), uploads.find((u) => u.name.endsWith(" - Org handbook.docx"))!.body]) {
      const text = docxText(bytes);
      const part = text.slice(text.indexOf("\n## Changed since your last health check\n"), text.indexOf("\n## Recent changes\n"));
      assert.match(part, new RegExp(`\\n### Flows\\n\\nAdded: none\\n\\nRemoved: 1 flow\\n${SINCE_MARKER} \\(${SINCE_FLOW}\\)\\n`));
      assert.match(part, /\nNeeds_Competitor on Opportunity: formula\n/);
      assert.ok(!text.includes(SINCE_NOWHERE), "only that it differs");
      // Only in the part: the flow's own entry is this run's, which hasn't it.
      assert.equal(text.split(SINCE_MARKER).length, 2);
    }
  });

  test("the earlier health check's folder is only read: no file in it written, added or touched", () => {
    assert.deepEqual(snapshot(earlier), earlierBefore);
    assert.ok(earlierBefore.has(HANDBOOK_FILE) && earlierBefore.has("pseudonym-map.json"));
  });

  test("nothing of either handbook is sent to Claude by a documents make", () => {
    assert.deepEqual(holding(JSON.stringify(asked), [...PLANTED, SINCE_NOWHERE, SINCE_FLOW]), []);
  });

  test("no other upload holds it", () => {
    const others = uploads.filter((u) => !u.name.endsWith(" - Org handbook.docx"));
    assert.equal(others.length, 7, "the six documents and the readout kit");
    for (const u of others) assert.deepEqual([...PLANTED, SINCE_FLOW, SINCE_NOWHERE, CHANGES_PLANTED, USER_ADMIN_MARKER, USER_ADMIN_EMAIL, APEX_OUTSIDE_MODIFIER.name, COMPONENT_OUTSIDE_MODIFIER.name].filter((p) => u.body.includes(p)), [], u.name);
  });

  test("nor the store's record, its warnings, the log or the queue's outcome", () => {
    const made = deps.store.documents.get(RUN_ID)!.made!;
    assert.ok(made.files.length === 6 && !made.files.some((f) => /handbook/i.test(f.name)));
    const store = { documents: [...deps.store.documents.values()], writes: deps.store.writes, runs: [...deps.store.runs.values()], shares: deps.store.shares, microsites: [...deps.store.microsites.values()], benchmark: deps.store.benchmarkRows };
    for (const [where, text] of [["store", JSON.stringify(store)], ["warnings", JSON.stringify(made.warnings)], ["log", deps.lines.join("\n")], ["outcome", JSON.stringify(outcome)]] as const) {
      for (const planted of [...PLANTED, SINCE_FLOW, SINCE_NOWHERE, "Planted_Rule", "Opportunity_Stage_Review", "PlantedLedgerSync", "PlantedLedgerTrigger", APEX_OUTSIDE_MODIFIER.name, COMPONENT_OUTSIDE_MODIFIER.name]) assert.ok(!text.includes(planted), `${planted} in the ${where}`);
    }
  });

  test("nor which reading made either file: not in the store, its warnings, the log, the outcome, an upload or another file the make leaves", () => {
    const made = deps.store.documents.get(RUN_ID)!.made!;
    const store = { documents: [...deps.store.documents.values()], writes: deps.store.writes, runs: [...deps.store.runs.values()], shares: deps.store.shares, microsites: [...deps.store.microsites.values()], benchmark: deps.store.benchmarkRows };
    for (const [where, text] of [["store", JSON.stringify(store)], ["warnings", JSON.stringify(made.warnings)], ["log", deps.lines.join("\n")], ["outcome", JSON.stringify(outcome)], ["uploads", uploads.map((u) => u.body.toString("latin1")).join("\n")]] as const) assert.deepEqual(holding(text, READINGS), [], where);
    const files = filesIn(dir);
    assert.deepEqual([...files.keys()].filter((f) => isOutput(f) && holding(files.get(f)!, READINGS).length > 0), []);
  });

  test("nor any other file the make leaves in the run folder", () => {
    const files = filesIn(dir);
    const holding = [...files.keys()].filter((f) => isOutput(f) && f !== `documents/${HANDBOOK_DOCX}` && [...PLANTED, SINCE_FLOW, SINCE_NOWHERE].some((p) => files.get(f)!.includes(p)));
    assert.deepEqual(holding, []);
    // The names, as before the make: in the run's map alone.
    for (const name of [APEX_OUTSIDE_MODIFIER.name, COMPONENT_OUTSIDE_MODIFIER.name]) assert.deepEqual([...files.keys()].filter((f) => isOutput(f) && files.get(f)!.includes(name)), ["pseudonym-map.json"], name);
    assert.ok(files.has(`documents/${HANDBOOK_DOCX}`));
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
    for (const [path, text] of filesIn(store)) assert.deepEqual(holding(text, [...ROUTED, ...NOWHERE, ...MODIFIERS]), [], path);
    // The run's own pseudonym map holds who changed each class, flow and rule, as it holds every user's name.
    for (const [path, text] of [...filesIn(run)].filter(([f]) => isOutput(f))) assert.deepEqual(holding(text, [...ROUTED, ...NOWHERE, ...(path === "pseudonym-map.json" ? [] : MODIFIERS)]), [], path);
    assert.deepEqual(holding(JSON.stringify(result), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
  });

  test("the worker's contribution to the console's benchmark table holds none of it", async () => {
    const written: unknown[] = [];
    const result = await contributeBenchmark(options(copy()), async (rows) => void written.push(...rows));
    assert.ok(written.length > 0);
    assert.deepEqual(holding(JSON.stringify([written, result]), [...ROUTED, ...NOWHERE, ...MODIFIERS]), []);
  });

  test("nor which reading made it, in either", async () => {
    const store = tempDir();
    const result = exportBenchmark({ ...options(copy()), storeDir: store });
    for (const [path, text] of filesIn(store)) assert.deepEqual(holding(text, READINGS), [], path);
    const written: unknown[] = [];
    const contributed = await contributeBenchmark(options(copy()), async (rows) => void written.push(...rows));
    assert.deepEqual(holding(JSON.stringify([result, written, contributed]), READINGS), []);
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
    ["src/handbook/compare.ts", "sets a re-check's file beside its earlier health check's, for the Word document alone, keeping nothing (DECISIONS.md 483)"],
    ["src/testing/earlier-handbook.ts", "makes up a synthetic earlier health check beside a synthetic run, for the tests and the sample"],
    ["src/handbook/sample.ts", "writes the synthetic org's document outside the repo, for a person to read"],
    ["src/worker/documents-job.ts", "puts the Word document in the customer's Drive folder, and lists it nowhere"],
    ["src/extract/metadata-parse.ts", "keeps the detail on the retrieve route"],
    ["src/extract/metadata-tooling.ts", "keeps the detail on the Tooling route"],
    ["src/engine/run.ts", "writes handbook.json"],
    ["src/testing/fingerprint.ts", "hashes it, in tests only"],
    ["src/testing/synthetic-org.ts", "plants its traps"],
    ["src/facts/questions.ts", "names its section of QUESTIONS.md, and reads no run"],
    ["src/extract/apex-touches.ts", "holds the number of the Apex reading the apex step stamps on its output, for handbook.json (DECISIONS.md 484)"],
    ["src/extract/automation.ts", "stamps that number on the apex step's output, and keeps nothing else for the handbook"],
    ["src/handbook/summaries.ts", "writes Claude's summaries of the kept Apex code to a file of their own, never into handbook.json (DECISIONS.md 492)"],
    ["src/worker/run-job.ts", "runs those summaries after the analysis, and deletes the kept code when a run fails for good"],
    ["src/worker/retention.ts", "deletes the kept code and partial summaries after seven days, by their names"],
    ["src/worker/env.ts", "reads the switch, HC_HANDBOOK_SUMMARIES"],
    ["src/config.ts", "names the summaries' model"],
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

// Decision 490 (a): with HC_HANDBOOK_SUMMARIES on, Apex code reaches Claude by
// the summary route alone, and nothing else of the handbook does; with it off,
// as in every test above, none of it does, and no code is ever kept.

/** A credential a developer pasted into a class: redacted before the code is kept, so in no file and no request. */
const PASTED_CREDENTIAL = "zqSyntheticKey0491notreal";
/** A synthetic user named in a comment: masked as their token on the way to Claude. */
const COMMENTER = usersTable.find((u) => /^\w+ \w+$/.test(u.Name) && !/Shared|Integration/.test(u.Name))!;
/** The longest name Salesforce allows a class, forty characters (QUESTIONS.md). */
const LONGEST_NAME = `ZebraLongest${"x".repeat(28)}`;
/** A word only the stub's summaries hold. */
const SUMMARY_MARKER = "Quokkasummary";
const APEX_MARKERS = [APEX_OBJECT, APEX_LITERAL, APEX_COMMENT];

/** The planted recording, with a class holding a pasted credential and a comment naming a user, and one with the longest name. */
function summaryRecording(): Recording {
  const recording = plantedRecording();
  const classes = recording.exchanges.find((e) => decodeURIComponent(e.key).includes("FROM ApexClass WHERE"))!;
  const page = JSON.parse(classes.text) as { totalSize: number; records: Array<Record<string, unknown>> };
  const changed = { LastModifiedDate: "2026-10-02T09:00:00.000+0000", LastModifiedById: COMMENTER.Id, LastModifiedBy: { Name: COMMENTER.Name } };
  page.records.push(
    { Id: "01p000000000098AAA", Name: "ZebraKeyHolder", ApiVersion: 62, Status: "Active", LengthWithoutComments: 180, ...changed, Body: `public class ZebraKeyHolder { // ${COMMENTER.Name} asked for this\n String api_key = '${PASTED_CREDENTIAL}'; }` },
    { Id: "01p000000000097AAA", Name: LONGEST_NAME, ApiVersion: 62, Status: "Active", LengthWithoutComments: 90, ...changed, Body: `public class ${LONGEST_NAME} { }` },
  );
  page.totalSize = page.records.length;
  classes.text = JSON.stringify(page);
  return recording;
}

/** A stub Claude: a summary for the summary prompt, a paragraph for a section. Keeps every request, and calls `look` before each answer. */
function summaryClaude(look: (request: NarrativeRequest) => void = () => {}): { asked: NarrativeRequest[]; narrative: NarrativeClient } {
  const asked: NarrativeRequest[] = [];
  return {
    asked,
    narrative: {
      complete: async (request) => {
        look(request);
        asked.push(request);
        const text =
          request.system === APEX_SUMMARY_PROMPT
            ? JSON.stringify({ summary: `Keeps the ${SUMMARY_MARKER} ledger in step when ${request.prompt.match(/Its name: (\w+)/)![1]} runs.` })
            : JSON.stringify({ paragraphs: [{ text: "We looked closely at this area.", sources: [] }] });
        return { stopReason: "end_turn", model: request.model, text };
      },
    },
  };
}

/** A run job on the summaries' recording. `look` is called before each request to Claude, and with null before each to Salesforce. */
async function summaryJob(on: boolean, look?: (request: NarrativeRequest | null, dir: string) => void, input = runInput({ answers: loadAnswers() })) {
  let deps!: ReturnType<typeof baseDeps>;
  const dir = (): string => join(deps.runsDir, input.run.id);
  const claude = summaryClaude((request) => look?.(request, dir()));
  const replay = new ReplayTransport(summaryRecording());
  deps = baseDeps({ dataTransport: { send: (request) => (look?.(null, dir()), replay.send(request)) }, narrative: claude.narrative, apexSummaries: on });
  const saved: RunOutputRecord[] = [];
  const save = deps.store.saveRunOutput.bind(deps.store);
  deps.store.saveRunOutput = async (runId, output) => {
    saved.push(structuredClone(output));
    return save(runId, output);
  };
  deps.store.addRun(input);
  await deps.tokens.save(syntheticConnection());
  const outcome = await createJobHandler(deps)({ kind: "run", runId: input.run.id }, { final: true });
  return { deps, saved, outcome, asked: claude.asked, dir: join(deps.runsDir, input.run.id) };
}

describe("with the summaries' switch off, no Apex code is ever kept", () => {
  test("no apex-source.json at any request to Salesforce or to Claude, nor at the end, and no summaries file", async () => {
    const seen: string[] = [];
    let dir = "";
    const replay = new ReplayTransport(summaryRecording());
    const deps = baseDeps({ dataTransport: { send: (request) => (dir && existsSync(join(dir, APEX_SOURCE_FILE)) && seen.push("salesforce"), replay.send(request)) } });
    dir = join(deps.runsDir, RUN_ID);
    const claude = summaryClaude(() => existsSync(join(dir, APEX_SOURCE_FILE)) && seen.push("claude"));
    deps.narrative = claude.narrative;
    deps.store.addRun(runInput({ answers: loadAnswers() }));
    await deps.tokens.save(syntheticConnection());
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true })).outcome, "done");
    assert.ok(replay.requests.length > 10 && claude.asked.length > 10, "it was looked for at each request");
    assert.deepEqual(seen, []);
    assert.ok(!existsSync(join(dir, APEX_SOURCE_FILE)) && !existsSync(join(dir, SUMMARIES_FILE)) && !existsSync(join(dir, SUMMARIES_PARTIAL_FILE)));
    assert.ok(claude.asked.every((r) => r.system !== APEX_SUMMARY_PROMPT));
    assert.deepEqual(holding(JSON.stringify(claude.asked), [...ROUTED, ...NOWHERE, ...MODIFIERS, PASTED_CREDENTIAL, COMMENTER.Name]), []);
  });
});

describe("with the summaries' switch on, Apex code goes to Claude by the summary route alone", () => {
  let on: Awaited<ReturnType<typeof summaryJob>>;
  let off: Awaited<ReturnType<typeof summaryJob>>;
  /** apex-source.json as it stood when Claude was first asked for a summary. */
  let kept = "";

  before(async () => {
    on = await summaryJob(true, (request, dir) => {
      if (request?.system === APEX_SUMMARY_PROMPT && !kept) kept = readFileSync(join(dir, APEX_SOURCE_FILE), "latin1");
    });
    assert.equal(on.outcome.outcome, "done");
    off = await summaryJob(false);
    assert.equal(off.outcome.outcome, "done");
  });

  const summaryRequests = () => on.asked.filter((r) => r.system === APEX_SUMMARY_PROMPT);

  test("the Apex markers are only in requests with the summary prompt and the Haiku model, and every other request holds none", () => {
    const summaries = summaryRequests();
    assert.equal(summaries.length, 12, "the synthetic org's nine, the planted three");
    assert.ok(summaries.every((r) => r.model === MODELS.handbookSummaries && r.model === "claude-haiku-5-5"));
    assert.deepEqual(holding(JSON.stringify(summaries), APEX_MARKERS), APEX_MARKERS, "the route was taken");
    const others = on.asked.filter((r) => r.system !== APEX_SUMMARY_PROMPT);
    assert.ok(others.length > 10);
    assert.deepEqual(holding(JSON.stringify(others), APEX_MARKERS), []);
  });

  test("the flows', validation rules' and every other handbook marker, and who changed anything, are in no request at all", () => {
    const rest = [...ROUTED, ...NOWHERE, ...MODIFIERS].filter((m) => !APEX_MARKERS.includes(m));
    assert.deepEqual(holding(JSON.stringify(on.asked), rest), []);
  });

  test("the pasted credential is in no request, and wasn't in apex-source.json while it existed", () => {
    assert.ok(kept.includes("ZebraKeyHolder") && kept.includes("api_key = '[REDACTED]'"), "the file was seen");
    assert.ok(!kept.includes(PASTED_CREDENTIAL));
    assert.ok(!JSON.stringify(on.asked).includes(PASTED_CREDENTIAL));
  });

  test("a user named in a comment is masked as their token in the request", () => {
    const request = summaryRequests().find((r) => r.prompt.includes("Its name: ZebraKeyHolder"))!;
    for (const part of [COMMENTER.Name, ...COMMENTER.Name.split(" ")]) assert.ok(!request.prompt.includes(part), part);
    assert.match(request.prompt, new RegExp(`// ${tokenSource("user")} asked for this`));
  });

  test("at the end neither the code nor the partial is left, and only handbook-summaries.json holds the summaries", () => {
    const files = filesIn(on.dir);
    assert.ok(!files.has(APEX_SOURCE_FILE) && !files.has(SUMMARIES_PARTIAL_FILE));
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(SUMMARY_MARKER)), [SUMMARIES_FILE]);
    const summaries = JSON.parse(files.get(SUMMARIES_FILE)!) as { items: Array<{ name: string }>; leftOut: Record<string, number> };
    assert.equal(summaries.items.length, 12);
    assert.ok(summaries.items.some((i) => i.name === LONGEST_NAME && i.name.length === 40));
    assert.equal(summaries.items.length + Object.values(summaries.leftOut).reduce((a, b) => a + b, 0), 15, "every class and trigger the step read");
  });

  test("handbook-summaries.json holds no Apex literal, comment or object, since the stub didn't echo one, nor the credential", () => {
    assert.deepEqual(holding(readFileSync(join(on.dir, SUMMARIES_FILE), "utf8"), [...APEX_MARKERS, PASTED_CREDENTIAL, COMMENTER.Name]), []);
  });

  test("the store, the console's writes, the benchmark and the log hold nothing of the code or the summaries", async () => {
    const markers = [...APEX_MARKERS, PASTED_CREDENTIAL, SUMMARY_MARKER, LONGEST_NAME, "ZebraKeyHolder"];
    assert.equal(on.saved.length, 1);
    const store = { saved: on.saved, runs: [...on.deps.store.runs.values()], writes: on.deps.store.writes, documents: [...on.deps.store.documents.values()], benchmark: on.deps.store.benchmarkRows, scans: [...on.deps.store.scanResults.values()], pulses: [...on.deps.store.pulseResults.values()] };
    assert.deepEqual(holding(JSON.stringify(store), markers), []);
    assert.deepEqual(holding(on.deps.lines.join("\n"), markers), []);
    assert.ok(on.deps.lines.some((l) => /^Claude summarised 12 of the 12 Apex classes and triggers kept for run /.test(l)));
    const copy = join(tempDir(), "run");
    cpSync(on.dir, copy, { recursive: true });
    const written: unknown[] = [];
    const result = await contributeBenchmark({ runDir: copy, industry: "technology", headcount: "51-200", consent: "Synthetic Person agreed by email", now: new Date("2026-11-03T10:00:00.000Z") }, async (rows) => void written.push(...rows));
    assert.ok(written.length > 0);
    assert.deepEqual(holding(JSON.stringify([written, result]), markers), []);
  });

  test("handbook.json, findings.json, metrics.json and the fingerprint are byte for byte the same with the switch on and off", () => {
    for (const file of [HANDBOOK_FILE, "findings.json", "metrics.json"]) assert.equal(readFileSync(join(on.dir, file), "utf8"), readFileSync(join(off.dir, file), "utf8"), file);
    assert.deepEqual(fingerprint(on.dir), fingerprint(off.dir));
  });

  test("a scan and a pulse with the switch on keep nothing and ask nothing", async () => {
    for (const product of ["scan", "pulse"] as const) {
      const seen: string[] = [];
      let looked = 0;
      const run = await summaryJob(true, (request, dir) => {
        looked++;
        if (existsSync(join(dir, APEX_SOURCE_FILE))) seen.push("kept");
        if (request?.system === APEX_SUMMARY_PROMPT) seen.push("asked");
      }, runInput({ run: consoleRun({ scope: "essentials", product, ...(product === "pulse" ? { baselineRunId: "hc-2026-08-31-00000009" } : {}) }) }));
      assert.equal(run.outcome.outcome, "done", product);
      assert.ok(looked > 10, `${product}: looked for at each request`);
      assert.deepEqual(seen, [], product);
      assert.ok(!run.asked.some((r) => r.system === APEX_SUMMARY_PROMPT), product);
      if (existsSync(run.dir)) for (const file of [APEX_SOURCE_FILE, SUMMARIES_FILE, SUMMARIES_PARTIAL_FILE]) assert.ok(!existsSync(join(run.dir, file)), `${product} ${file}`);
      const store = { runs: [...run.deps.store.runs.values()], scans: [...run.deps.store.scanResults.values()], pulses: [...run.deps.store.pulseResults.values()], writes: run.deps.store.writes };
      assert.deepEqual(holding(JSON.stringify([store, run.deps.lines]), [...APEX_MARKERS, PASTED_CREDENTIAL, SUMMARY_MARKER]), [], product);
    }
  });
});
