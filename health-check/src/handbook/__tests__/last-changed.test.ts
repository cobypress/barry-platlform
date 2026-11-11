import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, sep } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { ApexClassRow, ApexData, ApexTriggerRow, LastChanged } from "../../extract/automation.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { loadPseudonymMap } from "../../render/render-run.ts";
import { docxText } from "../../testing/docx-text.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { APEX_OUTSIDE_MODIFIER, AS_OF, userId } from "../../testing/synthetic-org.ts";
import { buildHandbook, HANDBOOK_FILE } from "../build.ts";
import { renderHandbookDocx } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { CHANGES_NOT_KEPT, Handbook, HANDBOOK_VERSION, type HandbookApexClass } from "../schema.ts";

// When each Apex class and trigger was last changed, and by whom (DECISIONS.md
// 473). Who is a token in handbook.json, and a name only in the Word document.

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const OPTIONS = { clientName: "Synthetic Co", runDate: AS_OF };
const TOKEN = /\[\[[a-z]+:[A-Z]\d{4,}\]\]/;
const NEW_FIELDS = ", LastModifiedDate, LastModifiedById, LastModifiedBy.Name";
const REFUSED_SENTENCE = "We couldn't read when your Apex classes and triggers were last changed, or by whom, so this handbook doesn't say.";
/** Salesforce's own words for a refusal: in the run folder's checkpoints, and in nothing made from them. */
const REFUSAL = "APEX-REFUSED-0473: you can't see who changed this";
const refuseLastChange: SimulatorOptions["failQuery"] = (soql) => (/FROM Apex(Class|Trigger)\b/.test(soql) && soql.includes("LastModifiedBy.Name") ? { errorCode: "INSUFFICIENT_ACCESS", message: REFUSAL } : null);

async function simulated(options: SimulatorOptions = {}) {
  const transport = new Simulator(options);
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, log: () => {} });
  return { ...result, transport };
}

/** Each SOQL query the transport was asked, in order, whether in a URL or a composite body. */
function queries(sim: Simulator): string[] {
  return sim.requests.flatMap((r) => {
    const text = `${decodeURIComponent(r.url)}\n${r.body ?? ""}`;
    return [...text.matchAll(/q=(SELECT[^&"\n]+)/g)].map((m) => decodeURIComponent(m[1]!.replace(/\+/g, " ")));
  });
}
const apexQueries = (sim: Simulator, object: string): string[] => queries(sim).filter((q) => q.includes(`FROM ${object} WHERE`));

const handbookOf = (dir: string): Handbook => Handbook.parse(read(dir, HANDBOOK_FILE));
const classesOf = (h: Handbook) => (h.apexClasses.status === "ok" ? h.apexClasses : assert.fail("classes unavailable"));
const triggersOf = (h: Handbook) => (h.apexTriggers.status === "ok" ? h.apexTriggers : assert.fail("triggers unavailable"));
const mapOf = (dir: string): PseudonymEntry[] => read<PseudonymEntry[]>(dir, "pseudonym-map.json");
const textOf = async (dir: string): Promise<string> => docxText(await renderHandbookDocx(handbookOf(dir), { ...OPTIONS, pseudonyms: loadPseudonymMap(dir) }));
/** One class's or trigger's paragraph in the document's text. */
const entry = (text: string, name: string): string => text.slice(text.indexOf(`\n${name}\n`) + 1).split("\n\n")[0]!;

/** Run ids and times, which differ between two runs of the same org, as the fingerprint leaves them out. */
const VOLATILE = /hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}|"(startedAt|finishedAt|updatedAt|createdAt|madeAt|generatedAt)":\s*"[^"]*"/g;
/** A file as the step's read before version 10 would have left it: the Apex queries' text without the new fields. */
const asBefore = (text: string): string => text.replace(VOLATILE, "").split(NEW_FIELDS).join("");

/** Every file in a run folder, by its path inside it. */
function filesIn(dir: string): Map<string, string> {
  const files = new Map<string, string>();
  for (const name of readdirSync(dir, { recursive: true }) as string[]) {
    const path = join(dir, name);
    if (statSync(path).isFile()) files.set(name.split(sep).join("/"), readFileSync(path, "latin1"));
  }
  return files;
}

describe("the recorded synthetic org's Apex", () => {
  let dir: string;
  let text: string;
  before(async () => {
    dir = (await runFromRecording()).dir;
    text = await textOf(dir);
  });

  test("a class changed by one of the run's users names them, the date as the recent changes give theirs, without the time", () => {
    const h = handbookOf(dir);
    const handler = classesOf(h).items.find((c) => c.name === "OpportunityHandler")!;
    assert.equal(handler.lastChanged!.by, mapOf(dir).find((e) => e.id === userId(4))!.token);
    assert.equal(entry(text, "OpportunityHandler"), "OpportunityHandler\nAPI version 60.0. Status: Active. Test: No.\nLast changed 21 Sep 2026 by Jo Ellis\nQueries: Opportunity\nChanges it makes: update");
    assert.equal(entry(text, "AccountTrigger"), "AccountTrigger\nOn: Account\nRuns: before update\nAPI version 60.0. Status: Active.\nLast changed 26 Sep 2025 by Kim Fox\nQueries: none in its own code\nChanges it makes: none in its own code");
    // The token in the file, never the name.
    assert.ok(!readFileSync(join(dir, HANDBOOK_FILE), "utf8").includes("Jo Ellis"));
  });

  test("someone the run's user list doesn't hold is labelled from LastModifiedBy.Name, and named in the document", () => {
    const outside = mapOf(dir).find((e) => e.id === APEX_OUTSIDE_MODIFIER.id)!;
    assert.equal(outside.name, APEX_OUTSIDE_MODIFIER.name);
    const h = handbookOf(dir);
    assert.equal(classesOf(h).items.find((c) => c.name === "FinanceSync")!.lastChanged!.by, outside.token);
    assert.equal(triggersOf(h).items.find((t) => t.name === "InvoiceTrigger")!.lastChanged!.by, outside.token);
    assert.match(entry(text, "FinanceSync"), /\nLast changed 13 Oct 2026 by Ash Rowe\n/);
    assert.ok(!readFileSync(join(dir, HANDBOOK_FILE), "utf8").includes(APEX_OUTSIDE_MODIFIER.name));
  });

  test("a class Salesforce records no user for has none, and prints its date alone", () => {
    const factory = classesOf(handbookOf(dir)).items.find((c) => c.name === "TestDataFactory")!;
    assert.deepEqual(factory.lastChanged, { at: "2026-01-04T12:00:00.000+0000", by: null });
    assert.equal(entry(text, "TestDataFactory"), "TestDataFactory\nAPI version 62.0. Status: Active. Test: Yes.\nLast changed 4 Jan 2026\nQueries: none in its own code\nChanges it makes: none in its own code");
  });

  test("every class and trigger has its line, every token is in the run's map, and the document shows none", () => {
    const h = handbookOf(dir);
    const items: HandbookApexClass[] = [...classesOf(h).items, ...triggersOf(h).items];
    assert.equal(items.length, 12);
    assert.deepEqual([classesOf(h).lastChanges, triggersOf(h).lastChanges], ["read", "read"]);
    const tokens = new Set(mapOf(dir).map((e) => e.token));
    for (const item of items) {
      assert.ok(item.lastChanged, item.name);
      if (item.lastChanged.by !== null) assert.ok(tokens.has(item.lastChanged.by), item.name);
    }
    assert.equal(text.match(/\nLast changed /g)?.length, 12);
    assert.doesNotMatch(text, TOKEN);
  });

  test("\"What it covers\" says so once, after the Apex parts' own lines", () => {
    const covers = text.slice(text.indexOf("## What it covers"), text.indexOf("## Flows"));
    assert.match(covers, /\nApex triggers: 5 triggers, 5 of them active\.\n\nEach Apex class and trigger shows when it was last changed and by whom, or the date alone where Salesforce records no user\.\n\nRecent changes: /);
    assert.equal(text.split("shows when it was last changed").length - 1, 1);
    assert.ok(!text.includes(REFUSED_SENTENCE));
  });

  test("the rows the rules read carry no date, user or name: only the step's own record of them does", () => {
    const apex = read<ApexData>(join(dir, "checkpoints"), "apex.json");
    assert.ok(apex.classes.status === "ok" && apex.triggers.status === "ok");
    for (const row of [...apex.classes.data, ...apex.triggers.data] as Array<Record<string, unknown>>) {
      for (const field of ["LastModifiedDate", "LastModifiedById", "LastModifiedBy", "lastChanged"]) assert.ok(!(field in row), `${row.Name}.${field}`);
    }
    const classes = apex.lastChanged!.classes;
    assert.ok(classes.status === "read");
    assert.deepEqual(classes.byRow["01p000000000005AAA"], { at: "2026-10-13T12:00:00.000+0000", byId: APEX_OUTSIDE_MODIFIER.id, byName: APEX_OUTSIDE_MODIFIER.name });
  });
});

describe("the run's existing tokens", () => {
  test("don't move: the map is the one a run that read no last changes makes, and one more, last of the users", async () => {
    const before = mapOf((await simulated({ failQuery: refuseLastChange })).dir);
    const after = mapOf((await simulated()).dir);
    assert.ok(!before.some((e) => e.id === APEX_OUTSIDE_MODIFIER.id));
    // Every entry the same, token for token, and in the same order.
    const added = after.filter((e) => !before.some((b) => b.id === e.id));
    assert.deepEqual(after.filter((e) => !added.includes(e)), before);
    assert.deepEqual(added, [{ token: "[[user:U0016]]", id: APEX_OUTSIDE_MODIFIER.id, name: APEX_OUTSIDE_MODIFIER.name, aliases: [APEX_OUTSIDE_MODIFIER.name, APEX_OUTSIDE_MODIFIER.id] }]);
    // The run's users are U0001 to U0015, in id order, as before.
    assert.deepEqual(before.filter((e) => e.token.startsWith("[[user:")).map((e) => [e.token, e.id]), Array.from({ length: 15 }, (_, i) => [`[[user:U${String(i + 1).padStart(4, "0")}]]`, userId(i + 1)]));
  });
});

describe("a read Salesforce refuses with the last-changed fields", () => {
  let clean: Awaited<ReturnType<typeof simulated>>;
  let refused: Awaited<ReturnType<typeof simulated>>;
  before(async () => {
    clean = await simulated();
    refused = await simulated({ failQuery: refuseLastChange });
  });

  test("is made once more without them, for the classes and for the triggers", () => {
    for (const object of ["ApexClass", "ApexTrigger"]) {
      const asked = apexQueries(refused.transport, object);
      assert.equal(asked.length, 2, object);
      assert.ok(asked[0]!.includes(NEW_FIELDS), object);
      assert.equal(asked[1], asked[0]!.replace(NEW_FIELDS, ""), object);
      // A clean run reads each once.
      assert.deepEqual(apexQueries(clean.transport, object), [asked[0]]);
    }
  });

  test("and the findings, metrics and scores are a clean run's, but for the query text their evidence quotes", () => {
    for (const file of ["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json"]) {
      const was = asBefore(readFileSync(join(clean.dir, file), "utf8"));
      assert.equal(asBefore(readFileSync(join(refused.dir, file), "utf8")), was, file);
    }
    // The Apex rules did run, on the same rows.
    const apexFindings = JSON.parse(readFileSync(join(refused.dir, "findings.json"), "utf8")).filter((f: { evidence: Array<{ query?: string }> }) => f.evidence.some((e) => e.query?.includes("FROM ApexClass")));
    assert.ok(apexFindings.length > 0);
    assert.ok(apexFindings.every((f: { status: string }) => f.status !== "not_measurable"));
  });

  test("the handbook lists the same classes and triggers with no last change, and says once that it couldn't read them", async () => {
    const h = handbookOf(refused.dir);
    const strip = (items: HandbookApexClass[]) => items.map(({ lastChanged: _gone, ...item }) => item);
    assert.deepEqual([classesOf(h).lastChanges, triggersOf(h).lastChanges], ["refused", "refused"]);
    assert.deepEqual(strip(classesOf(h).items), strip(classesOf(handbookOf(clean.dir)).items));
    assert.deepEqual(strip(triggersOf(h).items), strip(triggersOf(handbookOf(clean.dir)).items));
    assert.ok([...classesOf(h).items, ...triggersOf(h).items].every((i) => i.lastChanged === null));
    const text = await textOf(refused.dir);
    assert.equal(text.split(REFUSED_SENTENCE).length - 1, 1);
    assert.match(text, new RegExp(`\\nApex triggers: 5 triggers, 5 of them active\\.\\n\\n${REFUSED_SENTENCE.replace(/[.?()]/g, "\\$&")}\\n\\nRecent changes: `));
    assert.doesNotMatch(text, /Last changed |shows when it was last changed/);
  });

  test("Salesforce's reason stays in the step's checkpoint, and reaches neither handbook.json nor the Word document nor any other file", async () => {
    const files = filesIn(refused.dir);
    assert.deepEqual([...files.keys()].filter((f) => files.get(f)!.includes(REFUSAL)), ["checkpoints/apex.json"]);
    const apex = read<ApexData>(join(refused.dir, "checkpoints"), "apex.json");
    const refusal: LastChanged = { status: "refused", reason: `INSUFFICIENT_ACCESS: ${REFUSAL}` };
    assert.deepEqual(apex.lastChanged, { classes: refusal, triggers: refusal });
    assert.ok(!(await textOf(refused.dir)).includes("APEX-REFUSED"));
    // Saved with the step, the record of the refusal is cleared.
    assert.ok(!existsSync(join(refused.dir, "checkpoints", "apex-last-changed-refused.partial.jsonl")));
  });

  test("an org without the fields, which refuses them by name, is read the same way", async () => {
    const missing = ["LastModifiedDate", "LastModifiedById", "LastModifiedBy"];
    const run = await simulated({ dropFields: { ApexClass: missing, ApexTrigger: missing } });
    assert.deepEqual(apexQueries(run.transport, "ApexClass").map((q) => q.includes(NEW_FIELDS)), [true, false]);
    assert.equal(asBefore(readFileSync(join(run.dir, "findings.json"), "utf8")), asBefore(readFileSync(join(clean.dir, "findings.json"), "utf8")));
    assert.deepEqual([classesOf(handbookOf(run.dir)).lastChanges, triggersOf(handbookOf(run.dir)).lastChanges], ["refused", "refused"]);
  });

  test("if the read without them fails too, the list is unavailable with that read's reason, as before", async () => {
    const run = await simulated({ failQuery: (soql) => (soql.includes("FROM ApexClass WHERE") ? { errorCode: "INSUFFICIENT_ACCESS", message: soql.includes("LastModifiedBy") ? REFUSAL : "No Apex for you" } : null) });
    const apex = read<ApexData>(join(run.dir, "checkpoints"), "apex.json");
    assert.ok(apex.classes.status === "unavailable");
    assert.equal(apex.classes.reason, "INSUFFICIENT_ACCESS: No Apex for you");
    assert.ok(apex.classes.source.kind === "soql" && !apex.classes.source.query.includes("LastModified"));
    assert.equal(handbookOf(run.dir).apexClasses.status, "unavailable");
    // The triggers were read whole.
    assert.equal(triggersOf(handbookOf(run.dir)).lastChanges, "read");
  });
});

describe("a run stopped between the refused read and the one without the fields", () => {
  test("resumes with the read without them, and never makes the refused read again", async () => {
    const outDir = tempDir();
    const options = { provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), log: () => {} };
    // The classes' read with the fields is refused, and the budget runs out on the read that follows.
    const first = new Simulator({
      failQuery: (soql) => (!soql.includes("FROM ApexClass WHERE") ? null : soql.includes("LastModifiedBy") ? { errorCode: "INSUFFICIENT_ACCESS", message: REFUSAL } : { errorCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." }),
    });
    const stopped = await runHealthCheck({ ...options, transport: first });
    assert.equal(stopped.run.status, "stopped_budget");
    assert.deepEqual(apexQueries(first, "ApexClass").map((q) => q.includes(NEW_FIELDS)), [true, false]);
    // What the stopped run keeps: the object and Salesforce's reason, nothing read from the org.
    const kept = readFileSync(join(stopped.dir, "checkpoints", "apex-last-changed-refused.partial.jsonl"), "utf8");
    assert.deepEqual(kept.trim().split("\n").map((line) => JSON.parse(line)), [{ object: "ApexClass", reason: `INSUFFICIENT_ACCESS: ${REFUSAL}` }]);

    // Resumed, the classes' read with the fields would be refused again if it were made.
    const second = new Simulator({ failQuery: refuseLastChange });
    const resumed = await runHealthCheck({ ...options, transport: second, resume: stopped.run.id });
    assert.ok(resumed.output, "the run finished");
    assert.deepEqual(apexQueries(second, "ApexClass").map((q) => q.includes(NEW_FIELDS)), [false]);
    const h = handbookOf(resumed.dir);
    assert.equal(classesOf(h).lastChanges, "refused");
    assert.equal(classesOf(h).total, 7);
    assert.ok(!existsSync(join(resumed.dir, "checkpoints", "apex-last-changed-refused.partial.jsonl")), "cleared once the step is saved");
  });
});

describe("a run resumed with the step's version 9 checkpoint", () => {
  test("reads Apex again: its findings and metrics don't move, and its handbook gains the lines", async () => {
    const first = await simulated();
    const was = Object.fromEntries(["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json"].map((f) => [f, asBefore(readFileSync(join(first.dir, f), "utf8"))]));

    // As a run in flight at deployment left it: the step at version 9, its
    // checkpoint with no last changes and the queries' old text, a version 3
    // handbook and a map without the user only those would have added.
    type Steps = { steps: Array<{ id: string; version: number }> };
    const run = read<Steps>(first.dir, "run.json");
    run.steps.find((s) => s.id === "apex")!.version = 9;
    writeFileSync(join(first.dir, "run.json"), JSON.stringify(run));
    const checkpoint = read<ApexData>(join(first.dir, "checkpoints"), "apex.json");
    delete checkpoint.lastChanged;
    writeFileSync(join(first.dir, "checkpoints", "apex.json"), asBefore(JSON.stringify(checkpoint)));
    writeFileSync(join(first.dir, HANDBOOK_FILE), JSON.stringify(olderApex({ ...read<Record<string, unknown>>(first.dir, HANDBOOK_FILE), version: 3 })));
    writeFileSync(join(first.dir, "pseudonym-map.json"), JSON.stringify(mapOf(first.dir).filter((e) => e.id !== APEX_OUTSIDE_MODIFIER.id)));
    const older = readHandbook(first.dir);
    assert.ok(older.ok);
    assert.doesNotMatch(docxText(await renderHandbookDocx(older.handbook, { ...OPTIONS, pseudonyms: loadPseudonymMap(first.dir) })), /Last changed /);

    const transport = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: join(first.dir, ".."), now: new Date(), transport, log: () => {}, resume: first.run.id });
    assert.equal(apexQueries(transport, "ApexClass").length, 1, "the step read Apex again");
    assert.equal(read<Steps>(first.dir, "run.json").steps.find((s) => s.id === "apex")!.version, 10);
    for (const [file, text] of Object.entries(was)) assert.equal(asBefore(readFileSync(join(first.dir, file), "utf8")), text, file);
    const h = handbookOf(first.dir);
    assert.equal(h.version, HANDBOOK_VERSION);
    assert.deepEqual([classesOf(h).lastChanges, triggersOf(h).lastChanges], ["read", "read"]);
    assert.equal((await textOf(first.dir)).match(/\nLast changed /g)?.length, 12);
    assert.equal(mapOf(first.dir).find((e) => e.id === APEX_OUTSIDE_MODIFIER.id)?.token, "[[user:U0016]]");
  });
});

/** A file's Apex parts as a version 3 or 2 file has them: no last change on any class or trigger, and no word of them on the part. */
function olderApex<T extends Record<string, unknown>>(file: T): T {
  const older = (part: { status: string; items?: Array<Record<string, unknown>>; lastChanges?: string }) => {
    if (part.status !== "ok") return part;
    const { lastChanges: _said, ...rest } = part;
    return { ...rest, items: part.items!.map(({ lastChanged: _gone, ...item }) => item) };
  };
  return { ...file, apexClasses: older(file.apexClasses as never), apexTriggers: older(file.apexTriggers as never) };
}

describe("a handbook.json from before last changes were kept", () => {
  let current: Record<string, unknown>;
  let dir: string;
  before(async () => {
    current = read<Record<string, unknown>>((await runFromRecording()).dir, HANDBOOK_FILE);
    dir = join(tempDir(), "run");
    mkdirSync(dir, { recursive: true });
  });
  const write = (file: unknown) => writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify(file));

  test("version 3 still makes its document, with no line and no word of them, and its recent changes whole", async () => {
    write(olderApex({ ...current, version: 3 }));
    const handbook = readHandbook(dir);
    assert.ok(handbook.ok);
    assert.equal(handbook.handbook.version, HANDBOOK_VERSION);
    assert.deepEqual([classesOf(handbook.handbook).lastChanges, triggersOf(handbook.handbook).lastChanges], ["not_kept", "not_kept"]);
    assert.ok([...classesOf(handbook.handbook).items, ...triggersOf(handbook.handbook).items].every((i) => i.lastChanged === null));
    assert.equal(handbook.handbook.recentChanges.status, "ok");
    const text = docxText(await renderHandbookDocx(handbook.handbook, OPTIONS));
    assert.doesNotMatch(text, /last changed/i);
    assert.match(text, /\nOpportunityHandler\nAPI version 60\.0\. Status: Active\. Test: No\.\nQueries: Opportunity\n/);
    assert.match(text, /\nRecent changes: 30 setup changes /);
  });

  test("version 2 does too, its recent changes saying they weren't kept", async () => {
    const { recentChanges: _gone, ...four } = olderApex<Record<string, unknown>>({ ...current, version: 2 });
    write(four);
    const handbook = readHandbook(dir);
    assert.ok(handbook.ok);
    assert.deepEqual(handbook.handbook.recentChanges, { status: "unavailable", reason: CHANGES_NOT_KEPT });
    assert.equal(classesOf(handbook.handbook).lastChanges, "not_kept");
    const text = docxText(await renderHandbookDocx(handbook.handbook, OPTIONS));
    assert.doesNotMatch(text, /last changed/i);
    assert.match(text, /\nRecent changes: this run was read before recent changes were kept\.\n/);
  });

  test("but a file that mixes the versions is read as none of them", () => {
    // A version 3 file with last changes, and a version 4 file without them.
    for (const wrong of [{ ...current, version: 3 }, olderApex({ ...current }), { ...current, version: 2 }]) {
      write(wrong);
      assert.deepEqual(readHandbook(dir), { ok: false, why: "format" });
    }
  });
});

describe("the line on its own", () => {
  const source = { kind: "soql" as const, api: "rest" as const, query: "SELECT Id FROM ApexClass" };
  const ok = <T>(data: T[]) => ({ status: "ok" as const, data, source, truncated: false });
  const row = (n: number, name: string): ApexClassRow => ({ Id: `01p${String(n).padStart(12, "0")}AAA`, Name: name, ApiVersion: 62, Status: "Active", LengthWithoutComments: 10, commented: false, isTest: false, scan: null, touches: null });
  const trigger = (n: number, name: string): ApexTriggerRow => ({ ...row(n, name), Id: `01q${String(n).padStart(12, "0")}AAA`, TableEnumOrId: "Account", UsageBeforeInsert: true });
  const read = (byRow: Record<string, { at: string | null; byId: string | null; byName: string | null }>): LastChanged => ({ status: "read", byRow });
  const apex = (classes: ApexClassRow[], triggers: ApexTriggerRow[], lastChanged?: ApexData["lastChanged"]): ApexData => ({ classes: ok(classes), triggers: ok(triggers), orgCoverage: ok([]), classCoverage: ok([]), references: { users: {}, fields: {} }, lastChanged });
  const JO = userId(4);
  const JO_TOKEN = "[[user:U0004]]";
  const pseudonyms = new Pseudonymiser([{ token: JO_TOKEN, id: JO, name: "Jo Ellis", aliases: [] }]);
  const render = async (h: Handbook) => docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms }));

  test("a token the map can't restore prints as a user whose name we couldn't look up, never as the token or its digits", async () => {
    const unknown = "[[user:U0999]]";
    const h = buildHandbook(undefined, apex([row(1, "Alpha")], [], { classes: read({ [row(1, "Alpha").Id]: { at: "2026-03-03T09:15:00.000+0000", byId: userId(999), byName: "Someone" } }), triggers: read({}) }), undefined, undefined, new Map([[userId(999), unknown]]));
    assert.deepEqual(classesOf(h).items[0]!.lastChanged, { at: "2026-03-03T09:15:00.000+0000", by: unknown });
    for (const names of [pseudonyms, null]) {
      const text = docxText(await renderHandbookDocx(h, { ...OPTIONS, pseudonyms: names }));
      assert.match(text, /\nAlpha\nAPI version 62\.0\. Status: Active\. Test: couldn't be told: the code couldn't be read\.\nLast changed 3 Mar 2026 by a user whose name we couldn't look up\n/);
      for (const shown of [unknown, "U0999", "0999", "[[", "Someone"]) assert.ok(!text.includes(shown), shown);
    }
  });

  test("names a user the map holds, and Jo's line reads as decision 473 has it", async () => {
    const h = buildHandbook(undefined, apex([row(1, "Alpha")], [trigger(1, "OnAccount")], { classes: read({ [row(1, "Alpha").Id]: { at: "2026-03-03T09:15:00.000+0000", byId: JO, byName: "Jo Ellis" } }), triggers: read({ [trigger(1, "OnAccount").Id]: { at: "2026-10-30T23:59:59.000+0000", byId: JO, byName: "Jo Ellis" } }) }), undefined, undefined, new Map([[JO, JO_TOKEN]]));
    const text = await render(h);
    assert.match(text, /\nLast changed 3 Mar 2026 by Jo Ellis\n/);
    assert.match(text, /\nOnAccount\nOn: Account\nRuns: before insert\nAPI version 62\.0\. Status: Active\.\nLast changed 30 Oct 2026 by Jo Ellis\n/);
    assert.doesNotMatch(text, TOKEN);
  });

  test("a change after the run's date is printed as Salesforce gave it", async () => {
    // The run is dated 31 Oct 2026, and the step read later, after someone changed the class.
    const h = buildHandbook(undefined, apex([row(1, "Alpha")], [], { classes: read({ [row(1, "Alpha").Id]: { at: "2026-12-02T08:00:00.000+0000", byId: JO, byName: "Jo Ellis" } }), triggers: read({}) }), undefined, undefined, new Map([[JO, JO_TOKEN]]));
    assert.equal(classesOf(h).items[0]!.lastChanged!.at, "2026-12-02T08:00:00.000+0000");
    assert.match(await render(h), /\nLast changed 2 Dec 2026 by Jo Ellis\n/);
  });

  test("a user without a token, or a change without a date, gives no line rather than a wrong one", async () => {
    const h = buildHandbook(undefined, apex([row(1, "Alpha"), row(2, "Beta")], [], { classes: read({ [row(1, "Alpha").Id]: { at: "2026-03-03T09:15:00.000+0000", byId: JO, byName: "Jo Ellis" }, [row(2, "Beta").Id]: { at: null, byId: null, byName: null } }), triggers: read({}) }));
    assert.deepEqual(classesOf(h).items.map((i) => i.lastChanged), [null, null]);
    assert.doesNotMatch(await render(h), /Last changed /);
  });

  test("\"What it covers\" names only the lists it read, says which it couldn't, and says nothing for a run that kept none", async () => {
    const one = { [row(1, "Alpha").Id]: { at: "2026-03-03T09:15:00.000+0000", byId: JO, byName: "Jo Ellis" } };
    const covers = async (lastChanged: ApexData["lastChanged"], triggers = [trigger(1, "OnAccount")]) => {
      const text = await render(buildHandbook(undefined, apex([row(1, "Alpha")], triggers, lastChanged), undefined, undefined, new Map([[JO, JO_TOKEN]])));
      return text.slice(text.indexOf("\nApex triggers: "), text.indexOf("\nWe couldn't read your recent setup changes"));
    };
    const refused: LastChanged = { status: "refused", reason: REFUSAL };
    assert.equal(await covers({ classes: read(one), triggers: refused }), "\nApex triggers: 1 trigger, 1 of them active.\n\nEach Apex class shows when it was last changed and by whom. We couldn't read when your Apex triggers were last changed, or by whom, so this handbook doesn't say.\n");
    assert.equal(await covers({ classes: refused, triggers: refused }), `\nApex triggers: 1 trigger, 1 of them active.\n\n${REFUSED_SENTENCE}\n`);
    // A date alone is said only when one is printed.
    assert.equal(await covers({ classes: read({ [row(1, "Alpha").Id]: { at: "2026-03-03T09:15:00.000+0000", byId: null, byName: null } }), triggers: read({}) }), "\nApex triggers: 1 trigger, 1 of them active.\n\nEach Apex class and trigger shows when it was last changed and by whom, or the date alone where Salesforce records no user.\n");
    // No triggers listed, so none is spoken of.
    assert.equal(await covers({ classes: read(one), triggers: refused }, []), "\nApex triggers: your org has none.\n\nEach Apex class shows when it was last changed and by whom.\n");
    assert.equal(await covers(undefined), "\nApex triggers: 1 trigger, 1 of them active.\n");
    assert.ok(!(await render(buildHandbook(undefined, apex([row(1, "Alpha")], [], { classes: refused, triggers: refused })))).includes(REFUSAL));
  });
});
