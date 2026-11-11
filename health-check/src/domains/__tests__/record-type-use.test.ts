import { before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { applyGuards } from "../../engine/evaluate.ts";
import type { IssueResult, RuleContext } from "../../engine/rule.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { RECORD_TYPE_OBJECTS, recordTypeUseExtractor, type RecordTypeUse } from "../../extract/record-types.ts";
import { daysBefore, soqlDateTime, upToItsSecond, type ExtractContext, type Extractor, type SourceInfo } from "../../extract/types.ts";
import { RECORD_TYPE_GUARD } from "../../guards/record-type-guards.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { ApiBudget, BudgetStopError } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import type { Transport, TransportRequest } from "../../salesforce/transport.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import { dataQualityModule } from "../data-quality/index.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { modulesFor } from "../scope.ts";

// Record-type use (DECISIONS.md 497): one step that reads the record types and
// two counts by type, all to the run's date, and one finding for the org that
// stays a review until what names a record type can be read.

const RULE = "dq.record-type-use";
const T = new Date(org.AS_OF);
const DAY = 86_400_000;
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
/** The instant `days` before `at`, as Salesforce writes it. */
const daysUntil = (at: Date, days: number): string => stamp(at.getTime() - days * DAY);

let n = 0;
const type = (SobjectType: string, DeveloperName: string, CreatedDate: string, more: Partial<org.RecordTypeRecord> = {}): org.RecordTypeRecord => ({
  Id: org.sfId("012", 700 + ++n),
  DeveloperName,
  Name: DeveloperName.replace(/_/g, " "),
  SobjectType,
  IsActive: true,
  NamespacePrefix: null,
  CreatedDate,
  ...more,
});

/** Every SOQL query sent, through the query route or a composite batch. */
function queriesOf(requests: TransportRequest[]): string[] {
  return requests.flatMap((r) => {
    const url = new URL(r.url);
    if (url.pathname.endsWith("/composite/batch")) return (JSON.parse(r.body!) as { batchRequests: Array<{ url: string }> }).batchRequests.map((s) => new URLSearchParams(s.url.split("?")[1]).get("q") ?? "");
    const q = url.searchParams.get("q");
    return q ? [q] : [];
  });
}
/** The calls the budget counts: everything but the limits and the versions list. */
const billable = (requests: TransportRequest[]): TransportRequest[] => requests.filter((r) => !r.url.endsWith("/limits") && !r.url.endsWith("/services/data/"));
const isList = (q: string): boolean => /FROM RecordType WHERE/.test(q);
const isCount = (q: string): boolean => /GROUP BY RecordTypeId$/.test(q);

/** The step on its own, through the real client and allowlist. */
async function read(now: Date, options: SimulatorOptions = {}, wrap: (sim: Simulator) => Transport = (sim) => sim): Promise<{ out: RecordTypeUse; sim: Simulator }> {
  const sim = new Simulator(options);
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: wrap(sim), sleep: async () => {} });
  const out = await recordTypeUseExtractor.run({ client, now } as unknown as ExtractContext);
  return { out, sim };
}

/** Composite answers changed by `edit`, for a subrequest whose query it picks out. */
const editComposite = (edit: (soql: string, item: { statusCode: number; result: Record<string, unknown> }) => void) => (sim: Simulator): Transport => ({
  async send(request) {
    const res = await sim.send(request);
    if (!request.url.endsWith("/composite/batch")) return res;
    const subs = (JSON.parse(request.body!) as { batchRequests: Array<{ url: string }> }).batchRequests;
    const body = JSON.parse(res.text) as { results: Array<{ statusCode: number; result: Record<string, unknown> }> };
    body.results.forEach((item, i) => edit(new URLSearchParams(subs[i]!.url.split("?")[1]).get("q") ?? "", item));
    return { ...res, text: JSON.stringify(body) };
  },
});

// The synthetic org through the simulator, not the recording: a change to a
// query then shows here as what it reads, not as a request the recording lacks.
let base: Map<string, unknown>;
let synthetic: { findings: Finding[]; dir: string; calls: number };
before(async () => {
  const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: new Simulator(), log: () => {} });
  base = loadExtracts(run.dir);
  synthetic = { findings: run.output!.findings, dir: run.dir, calls: run.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.calls };
});

/** The finding, from the synthetic run's other extracts with this step in place of its own. */
function judge(use: RecordTypeUse, now = T): Finding {
  const extracts = clone(base);
  extracts.set(recordTypeUseExtractor.id, use);
  const window = { from: soqlDateTime(daysBefore(now, 730)).slice(0, 10), to: now.toISOString().slice(0, 10) };
  return analyseExtracts(extracts, { now, window, answers: loadAnswers() }).findings.find((f) => f.ruleId === RULE)!;
}
const names = (f: Finding): string[] => f.subjects.map((s) => s.label);
const exclusion = (f: Finding, starts: string): string => f.evidence.flatMap((e) => e.exclusions).find((x) => x.startsWith(starts))!;

describe("the synthetic org", () => {
  test("names the type with no records and the one with none made in the last 12 months, apart", () => {
    const f = synthetic.findings.find((x) => x.ruleId === RULE)!;
    assert.equal(f.status, "issue");
    assert.equal(f.title, "Some record types have no records, or none made in the last 12 months");
    assert.deepEqual(names(f), ["Support on Case", "Credit Note on Invoice"]);
    assert.match(f.summary, /1 has no records at all: Support on Case\. 1 has records, but none made in the last 12 months: Credit Note on Invoice\./);
    assert.deepEqual(f.facts, { judged: 5, noRecords: 1, noneRecent: 1 });
    // Said as exactly that: a type in use with nothing new this year isn't "unused".
    assert.doesNotMatch(`${f.title} ${f.summary} ${f.recommendation}`, /\b(unused|dormant)\b/i);
    assert.match(f.evidence[1]!.note!, /With no records: Case\.Support$/);
    assert.match(f.evidence[2]!.note!, /none made in them: Invoice__c\.Credit_Note$/);
    assert.deepEqual([f.severity, f.confidence, f.owner, f.effort.size], ["low", "medium", "Salesforce admin", "S"]);
    assert.deepEqual(f.segmentsChecked, [{ dimension: "all records", segments: 5 }, { dimension: "records created in the last 12 months", segments: 5 }]);
  });

  test("leaves out the inactive type and the object with no records, and counts each", () => {
    const f = synthetic.findings.find((x) => x.ruleId === RULE)!;
    assert.ok(!names(f).some((s) => /Old Format|Default/.test(s)));
    assert.equal(exclusion(f, "Inactive record types"), "Inactive record types: 1");
    assert.equal(exclusion(f, "Objects with fewer than 20"), "Objects with fewer than 20 records made in the last 12 months: 1 (Empty_Object__c)");
  });

  test("the type made since the run's date isn't listed, and the records made since are in neither count", () => {
    const use = readCheckpoint(join(synthetic.dir, "checkpoints", `${recordTypeUseExtractor.id}.json`)) as RecordTypeUse;
    assert.ok(use.types.status === "ok");
    const later = org.RECORD_TYPES.find((t) => t.DeveloperName === "Pro_Forma")!;
    assert.ok(later.CreatedDate > stamp(T.getTime()));
    assert.ok(!use.types.data.some((t) => t.Id === later.Id));
    // The credit note and the case made since: neither is counted.
    const credit = org.RECORD_TYPES.find((t) => t.DeveloperName === "Credit_Note")!.Id;
    const support = org.RECORD_TYPES.find((t) => t.DeveloperName === "Support")!.Id;
    assert.deepEqual(use.counted.Invoice__c!.all.find((r) => r.RecordTypeId === credit), { RecordTypeId: credit, c: 140 });
    assert.equal(use.counted.Invoice__c!.recent.find((r) => r.RecordTypeId === credit), undefined);
    assert.equal(use.counted.Case!.all.find((r) => r.RecordTypeId === support), undefined);
    assert.ok(!use.counted.Invoice__c!.all.some((r) => r.RecordTypeId === later.Id));
  });

  test("the step makes at most three calls", () => {
    assert.ok(synthetic.calls >= 1 && synthetic.calls <= 3, `${synthetic.calls} calls`);
    assert.equal(synthetic.calls, 2);
  });
});

// Traps, each on one object or type (DECISIONS.md 497).
const TYPES = {
  standard: type("Invoice__c", "Standard", daysUntil(T, 800)),
  creditNote: type("Invoice__c", "Credit_Note", daysUntil(T, 700)),
  proForma: type("Invoice__c", "Pro_Forma", daysUntil(T, 400)),
  oldFormat: type("Invoice__c", "Old_Format", daysUntil(T, 1000), { IsActive: false }),
  // Made 30 days before the run, with no records: too new to judge.
  newType: type("Invoice__c", "New_Type", daysUntil(T, 30)),
  packaged: type("Opportunity", "Partner", daysUntil(T, 800), { NamespacePrefix: "acmecpq" }),
  // On an object we count, with no records: only being packaged keeps it out.
  packagedHere: type("Invoice__c", "Billing_Run", daysUntil(T, 800), { NamespacePrefix: "acmecpq" }),
  emptyDefault: type("Empty_Object__c", "Default", daysUntil(T, 300)),
  broken: type("Broken__c", "Thing", daysUntil(T, 300)),
  cut: type("Cut__c", "Thing", daysUntil(T, 300)),
  last: type("Zz_Last__c", "Thing", daysUntil(T, 300)),
};
// Enough objects of one type each that Zz_Last__c is the 26th.
const FILLERS = Array.from({ length: RECORD_TYPE_OBJECTS - 4 }, (_, i) => type(`Filler_${String(i + 1).padStart(2, "0")}__c`, "Thing", daysUntil(T, 300)));
const recent = (id: string | null, count: number) => ({ RecordTypeId: id, CreatedDate: daysUntil(T, 10), n: count });
const old = (id: string | null, count: number) => ({ RecordTypeId: id, CreatedDate: daysUntil(T, 500), n: count });
const GROUPS: Record<string, org.RecordTypeGroup[]> = {
  Invoice__c: [recent(TYPES.standard.Id, 30), old(TYPES.creditNote.Id, 5), old(TYPES.oldFormat.Id, 5)],
  Opportunity: [recent(null, 50)],
  Empty_Object__c: [],
  Broken__c: [recent(TYPES.broken.Id, 50)],
  Cut__c: [recent(null, 50)],
  Zz_Last__c: [recent(null, 50)],
};
const TRAPS: SimulatorOptions = {
  recordTypes: [...Object.values(TYPES), ...FILLERS],
  recordTypeGroups: GROUPS,
  failQuery: (soql) => (/FROM Broken__c /.test(soql) ? { errorCode: "INVALID_FIELD", message: "No such column 'RecordTypeId' on entity 'Broken__c'." } : null),
};
const cutShort = editComposite((soql, item) => {
  if (/FROM Cut__c /.test(soql)) item.result.nextRecordsUrl = `/services/data/v${org.API_VERSION}/query/01g000000000001-2000`;
});

describe("what isn't judged", () => {
  let traps: { out: RecordTypeUse; sim: Simulator };
  let f: Finding;
  before(async () => {
    traps = await read(T, TRAPS, cutShort);
    f = judge(traps.out);
  });

  test("flags only the type with no records and the one with none in the last 12 months", () => {
    assert.equal(f.status, "issue");
    assert.deepEqual(names(f), ["Pro Forma on Invoice", "Credit Note on Invoice"]);
    assert.equal(f.title, "Some record types have no records, or none made in the last 12 months");
    assert.deepEqual(f.facts, { judged: 3, noRecords: 1, noneRecent: 1 });
  });

  test("each quiet case is counted in its own exclusion", () => {
    assert.equal(exclusion(f, "Inactive record types"), "Inactive record types: 1");
    assert.equal(exclusion(f, "Record types from installed packages"), "Record types from installed packages: 2");
    assert.equal(exclusion(f, "Record types made in the 90 days"), "Record types made in the 90 days before the run: 1");
    // The empty object and the fillers, which have no records.
    assert.match(exclusion(f, "Objects with fewer than 20"), new RegExp(`: ${FILLERS.length + 1} \\(Empty_Object__c, Filler_01__c, `));
    assert.equal(exclusion(f, "Objects past the 25"), "Objects past the 25 with the most active record types: 1 (Zz_Last__c)");
    assert.equal(exclusion(f, "Objects whose count Salesforce refused"), "Objects whose count Salesforce refused: 1 (Broken__c)");
    assert.equal(exclusion(f, "Objects whose count came back cut short"), "Objects whose count came back cut short: 1 (Cut__c)");
    for (const quiet of ["Old Format", "New Type", "Partner", "Billing Run", "Default", "Thing"]) assert.ok(!names(f).some((s) => s.startsWith(quiet)), quiet);
  });

  test("the step keeps why each object wasn't counted, and reads 25 objects in three calls", () => {
    assert.deepEqual(traps.out.notRead, ["Zz_Last__c"]);
    assert.deepEqual(traps.out.refused, [{ object: "Broken__c", reason: "Broken__c.RecordTypeId isn't in this org." }]);
    assert.deepEqual(traps.out.truncated, ["Cut__c"]);
    // A refused or cut short object is never read as one with no records.
    assert.equal(traps.out.counted.Broken__c, undefined);
    assert.equal(traps.out.counted.Cut__c, undefined);
    assert.equal(Object.keys(traps.out.counted).length, RECORD_TYPE_OBJECTS - 2);
    assert.equal(billable(traps.sim.requests).length, 3);
    // The packaged type's object has no active type of our own, so it isn't counted.
    assert.ok(!queriesOf(traps.sim.requests).some((q) => /FROM Opportunity /.test(q)));
  });

  test("an object whose counts all failed isn't judged at all", async () => {
    const everyCount = { ...TRAPS, failQuery: (soql: string) => (isCount(soql) ? { errorCode: "MALFORMED_QUERY", message: "unexpected token" } : null) };
    const { out } = await read(T, everyCount);
    assert.deepEqual(out.counted, {});
    const g = judge(out);
    assert.equal(g.status, "not_measurable");
    assert.match(g.notMeasurableReason!, /^None of your \d+ record types could be judged: Salesforce refused or cut short the count on 25 objects; 1 object is past the 25 we count\.$/);
  });

  test("a whole composite call refused leaves its objects not judged, and the step still finishes", async () => {
    let calls = 0;
    const second = (sim: Simulator): Transport => ({
      async send(request) {
        // Every try at the second, retries too.
        if (request.url.endsWith("/composite/batch") && ++calls >= 2) return { status: 500, header: () => null, text: JSON.stringify([{ errorCode: "UNKNOWN_EXCEPTION", message: "An unexpected error occurred." }]) };
        return sim.send(request);
      },
    });
    const { out } = await read(T, TRAPS, second);
    // The second call holds the last 25 queries: the 13th object's second count and the 12 after it.
    assert.equal(out.refused.length, 13 + 1);
    assert.ok(out.refused.every((r) => /UNKNOWN_EXCEPTION/.test(r.reason) || r.object === "Broken__c"));
    assert.ok(Object.keys(out.counted).length > 0);
    // The first call's objects are still judged (Cut__c is whole here), and the finding counts the rest as refused.
    const f = judge(out);
    assert.deepEqual(names(f), ["Thing on Cut__c", "Pro Forma on Invoice", "Credit Note on Invoice"]);
    assert.match(exclusion(f, "Objects whose count Salesforce refused"), /^Objects whose count Salesforce refused: 14 \(/);
  });

  test("a budget stop isn't Salesforce's answer: it goes up, so the step is read again", async () => {
    const limit = (sim: Simulator): Transport => ({
      async send(request) {
        if (request.url.endsWith("/composite/batch")) return { status: 403, header: () => null, text: JSON.stringify([{ errorCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." }]) };
        return sim.send(request);
      },
    });
    await assert.rejects(read(T, TRAPS, limit), BudgetStopError);
  });
});

describe("mixed keys", () => {
  test("a count row for a type not in the list is ignored, and a row with no type counts towards the object's 20", async () => {
    const standard = type("Invoice__c", "Standard", daysUntil(T, 800));
    const spare = type("Invoice__c", "Spare", daysUntil(T, 800));
    // A type made since, or deleted, or Master's own id: none is in the list.
    const unknown = org.sfId("012", 0);
    const { out } = await read(T, { recordTypes: [standard, spare], recordTypeGroups: { Invoice__c: [recent(null, 19), recent(unknown, 1), old(standard.Id, 3)] } });
    const f = judge(out);
    // 19 with no type and 1 under an id we don't know make the 20; the unknown one is no type of ours.
    assert.equal(f.status, "issue");
    assert.deepEqual(names(f), ["Spare on Invoice", "Standard on Invoice"]);
    assert.deepEqual(f.facts, { judged: 2, noRecords: 1, noneRecent: 1 });
    assert.ok(!f.subjects.some((s) => s.label.includes(unknown)));
    // One short of the 20 and nothing is judged.
    const { out: short } = await read(T, { recordTypes: [standard, spare], recordTypeGroups: { Invoice__c: [recent(null, 18), recent(unknown, 1), old(standard.Id, 3)] } });
    assert.equal(judge(short).status, "not_measurable");
    assert.match(judge(short).notMeasurableReason!, /1 object has fewer than 20 records made in the last 12 months/);
  });
});

describe("clean data", () => {
  test("passes with both segments when every judged type has records made in the last 12 months", async () => {
    const { out } = await read(T, { recordTypes: [TYPES.standard, TYPES.creditNote, TYPES.oldFormat], recordTypeGroups: { Invoice__c: [recent(TYPES.standard.Id, 30), recent(TYPES.creditNote.Id, 1)] } });
    const f = judge(out);
    assert.equal(f.status, "passed");
    assert.equal(f.summary, "Each of the 2 record types we could judge has records made in the last 12 months.");
    assert.deepEqual(f.segmentsChecked, [{ dimension: "all records", segments: 2 }, { dimension: "records created in the last 12 months", segments: 2 }]);
  });

  test("an org with no record types of its own to judge passes, with the reason it has no segments", async () => {
    const { out } = await read(T, { recordTypes: [TYPES.oldFormat, TYPES.packaged, TYPES.newType], recordTypeGroups: {} });
    const f = judge(out);
    assert.equal(f.status, "passed");
    assert.equal(f.segmentExemptReason, "There are no record types to segment.");
  });

  test("a record type list that can't be read is not measurable, with the reason, and nothing is counted", async () => {
    const { out, sim } = await read(T, { failQuery: (soql) => (isList(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "insufficient access rights on object id" } : null) });
    assert.equal(out.types.status, "unavailable");
    assert.ok(!queriesOf(sim.requests).some(isCount));
    const f = judge(out);
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /^Record types couldn't be read: INSUFFICIENT_ACCESS/);
  });
});

describe("the run's date, fixed in the past", () => {
  const P = new Date(T.getTime() - 60 * DAY);
  const at = (days: number) => stamp(P.getTime() + days * DAY);
  const standard = type("Invoice__c", "Standard", at(-800));
  const onlyLater = type("Invoice__c", "Only_Later", at(-400));
  const oldPlusLater = type("Invoice__c", "Old_Plus_Later", at(-400));
  const madeSince = type("Invoice__c", "Made_Since", at(10));
  const options: SimulatorOptions = {
    recordTypes: [standard, onlyLater, oldPlusLater, madeSince],
    recordTypeGroups: {
      Invoice__c: [
        { RecordTypeId: standard.Id, CreatedDate: at(-10), n: 30 },
        { RecordTypeId: onlyLater.Id, CreatedDate: at(5), n: 4 },
        { RecordTypeId: oldPlusLater.Id, CreatedDate: at(-500), n: 1 },
        { RecordTypeId: oldPlusLater.Id, CreatedDate: at(5), n: 1 },
        { RecordTypeId: madeSince.Id, CreatedDate: at(20), n: 2 },
      ],
    },
  };

  test("every read ends at the run's date", async () => {
    const { sim } = await read(P, options);
    const end = `CreatedDate <= ${soqlDateTime(upToItsSecond(P))}`;
    const queries = queriesOf(sim.requests);
    const list = queries.filter(isList);
    const counts = queries.filter(isCount);
    assert.deepEqual(list, [`SELECT Id, DeveloperName, Name, SobjectType, IsActive, NamespacePrefix, CreatedDate FROM RecordType WHERE ${end}`]);
    assert.deepEqual(counts, [
      `SELECT RecordTypeId, COUNT(Id) c FROM Invoice__c WHERE ${end} GROUP BY RecordTypeId`,
      `SELECT RecordTypeId, COUNT(Id) c FROM Invoice__c WHERE CreatedDate >= ${soqlDateTime(new Date(new Date(P).setUTCMonth(P.getUTCMonth() - 12)))} AND ${end} GROUP BY RecordTypeId`,
    ]);
  });

  test("a type made since isn't listed, and records made since are in neither count", async () => {
    const { out } = await read(P, options);
    assert.ok(out.types.status === "ok");
    assert.ok(!out.types.data.some((t) => t.Id === madeSince.Id));
    const f = judge(out, P);
    assert.equal(f.status, "issue");
    // Its only records came after the run's date, so at it there are none.
    assert.match(f.summary, /1 has no records at all: Only Later on Invoice\./);
    // An old record and one since: none made in the 12 months to the run's date.
    assert.match(f.summary, /1 has records, but none made in the last 12 months: Old Plus Later on Invoice\./);
    assert.deepEqual(f.facts, { judged: 3, noRecords: 1, noneRecent: 1 });
    // Not listed, so not counted as made in the 90 days before the run either.
    assert.equal(exclusion(f, "Record types made in the 90 days"), "Record types made in the 90 days before the run: 0");
  });
});

describe("a run stopped between the reads", () => {
  const simulatorRun = (sim: Simulator, outDir: string, overrides: Partial<RunOptions> = {}) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir, now: T, transport: sim, log: () => {}, ...overrides });

  test("keeps no checkpoint for the step, and the resumed run reads all three again", async () => {
    // Stopped by the budget just before the counts, after the list was read, as run.test.ts stops one.
    const probe = new Simulator();
    const straight = await simulatorRun(probe, tempDir());
    const at = probe.requests.findIndex((r) => r.url.endsWith("/composite/batch") && queriesOf([r]).some(isCount));
    const spent = billable(probe.requests.slice(0, at)).length;
    const out = tempDir();
    const first = new Simulator({ used: 50_000 - spent });
    const stopped = await simulatorRun(first, out);
    assert.equal(stopped.run.status, "stopped_budget");
    assert.equal(queriesOf(first.requests).filter(isList).length, 1, "the list was read before the stop");
    assert.equal(queriesOf(first.requests).filter(isCount).length, 0);
    const checkpoints = readdirSync(join(stopped.dir, "checkpoints"));
    assert.ok(!checkpoints.some((f) => f.startsWith(recordTypeUseExtractor.id)), checkpoints.join(", "));
    assert.notEqual(stopped.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.status, "done");

    const second = new Simulator();
    const resumed = await simulatorRun(second, out, { resume: stopped.run.id });
    assert.equal(resumed.run.status, "in_review");
    const again = queriesOf(second.requests);
    assert.equal(again.filter(isList).length, 1, "the list is read again");
    assert.equal(again.filter(isCount).length, 2 * 4, "both counts on each object are read again");
    assert.ok(existsSync(join(resumed.dir, "checkpoints", `${recordTypeUseExtractor.id}.json`)));
    const finding = (fs: Finding[]) => fs.find((x) => x.ruleId === RULE);
    assert.deepEqual(finding(resumed.output!.findings), finding(straight.output!.findings));
  });
});

describe("the guard", () => {
  test("the finding the engine prints is a review, never an instruction to deactivate", () => {
    const f = synthetic.findings.find((x) => x.ruleId === RULE)!;
    assert.equal(f.recommendationType, "investigate");
    assert.equal(f.recommendation, "Review 2 record types with the people who own Case and Invoice; we aren't recommending deactivating them, because we can't see which profiles default to them or which flows, Apex and integrations name them.");
    assert.doesNotMatch(f.recommendation, /\bDeactivate\b/);
    // One guard per type, each unknown, so "not every guard passed" is true of it.
    assert.deepEqual(f.guards.map((g) => [g.guardId, g.subject, g.status]), [[RECORD_TYPE_GUARD, "Case.Support", "unknown"], [RECORD_TYPE_GUARD, "Invoice__c.Credit_Note", "unknown"]]);
    assert.ok(f.guards.every((g) => /We don't read which profiles default to this record type, or which flows, Apex and integrations name it/.test(g.detail)));
    // The engine's own words for a blocked finding: true of this one, which never had a figure.
    assert.deepEqual(f.value, { kind: "not_sized", reason: "Not counted as a saving, because not every guard passed." });
    assert.deepEqual(f.dependencies, ["Check which profiles default to each record type", "Check which flows, Apex and integrations name each record type"]);
  });

  test("the rule declares a deactivation held back by its guards, and the engine prints the blocked form", () => {
    const rule = dataQualityModule.rules.find((r) => r.id === RULE)!;
    const extracts = clone(base);
    const ctx = {
      calibration: loadCalibration(),
      now: T,
      window: { from: "2024-10-31", to: "2026-10-31" },
      metrics: [],
      pseudonyms: new Pseudonymiser(),
      data: <X>(e: Extractor<X>): X => extracts.get(e.id) as X,
      manual: () => assert.fail("no manual step"),
      ev: (source: SourceInfo) => ({ ...source, count: null, dateRange: null, exclusions: [], capturedAt: T.toISOString() }),
      userLabel: () => "a user",
    } as unknown as RuleContext;
    const raw = rule.evaluate(ctx) as IssueResult;
    assert.equal(raw.recommendationType, "deactivate");
    assert.match(raw.recommendation, /^Deactivate 2 record types once/);
    assert.ok(raw.guards!.length === 2 && raw.guards!.every((g) => g.guardId === RECORD_TYPE_GUARD && g.status === "unknown"));
    const printed = applyGuards(raw);
    assert.equal(printed.recommendationType, "investigate");
    assert.equal(printed.recommendation, raw.blockedRecommendation);
    // Were every guard to pass, the deactivation would print: it's the guard that holds it back.
    assert.equal(applyGuards({ ...raw, guards: raw.guards!.map((g) => ({ ...g, status: "passed" as const })) }).recommendationType, "deactivate");
  });
});

describe("a set-up review", () => {
  test("doesn't run the step or the rule", async () => {
    const essentials = modulesFor("essentials", SALES_CLOUD_MODULES);
    assert.ok(!essentials.some((m) => m.extractors.some((e) => e.id === recordTypeUseExtractor.id)));
    assert.ok(!essentials.some((m) => m.rules.some((r) => r.id === RULE)));
    assert.equal(recordTypeUseExtractor.records, true);
    const sim = new Simulator();
    const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, scope: "essentials", outDir: tempDir(), now: T, transport: sim, log: () => {} });
    assert.ok(!run.run.steps.some((s) => s.id === recordTypeUseExtractor.id));
    assert.ok(!queriesOf(sim.requests).some((q) => isList(q) || isCount(q)));
    assert.ok(!run.output!.findings.some((f) => f.ruleId === RULE));
  });
});
