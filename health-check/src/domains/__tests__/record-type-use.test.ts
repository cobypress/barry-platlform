import { before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { readCheckpoint, writeCheckpoint } from "../../engine/checkpoint.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { applyGuards } from "../../engine/evaluate.ts";
import type { IssueResult, RuleContext } from "../../engine/rule.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { isPartlyHidden, PARTLY_HIDDEN_OBJECTS, RECORD_TYPE_OBJECTS, recordTypeUseExtractor, type RecordTypeUse } from "../../extract/record-types.ts";
import { daysBefore, soqlDateTime, upToItsSecond, type ConnectedUser, type ExtractContext, type Extractor, type SourceInfo } from "../../extract/types.ts";
import { RECORD_TYPE_GUARD } from "../../guards/record-type-guards.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { escapeXml } from "../../render/format.ts";
import { renderContext, valueText } from "../../render/html.ts";
import { ApiBudget, BudgetStopError } from "../../salesforce/budget.ts";
import { SalesforceClient, SalesforceError } from "../../salesforce/client.ts";
import type { Transport, TransportRequest } from "../../salesforce/transport.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import { dataQualityModule } from "../data-quality/index.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { modulesFor } from "../scope.ts";

// Record-type use (DECISIONS.md 497 to 499): one step that reads the record
// types and two counts by type, all to the run's date, and one finding for the
// org that stays a review until what names a record type can be read. Objects a
// count can't see whole (tasks, events, files and article versions) are never
// counted, only objects the connected user sees whole are, and a temporary
// failure stops the step.

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
const isLookup = (q: string): boolean => /^SELECT Id FROM User WHERE Username = /.test(q);
const isViewAllData = (q: string): boolean => /^SELECT PermissionSet\.PermissionsViewAllData FROM PermissionSetAssignment /.test(q);
/** The two access reads, for a user id or the placeholder (DECISIONS.md 498 and 499). */
const VIEW_ALL_DATA = (user: string): string => `SELECT PermissionSet.PermissionsViewAllData FROM PermissionSetAssignment WHERE AssigneeId = '${user}' AND PermissionSet.HasActivationRequired = false`;
const VIEW_ALL_RECORDS = (user: string): string =>
  `SELECT SobjectType FROM ObjectPermissions WHERE PermissionsViewAllRecords = true AND Parent.HasActivationRequired = false AND ParentId IN (SELECT PermissionSetId FROM PermissionSetAssignment WHERE AssigneeId = '${user}')`;
const isViewAllRecords = (q: string): boolean => /^SELECT SobjectType FROM ObjectPermissions /.test(q);
const isAccess = (q: string): boolean => isLookup(q) || isViewAllData(q) || isViewAllRecords(q);
/** The connected user by id, as the worker and `hc run --connection` give it, or by username, as the `sf` route does. */
const BY_ID: ConnectedUser = { id: org.CONNECTED_USER.id };
const BY_NAME: ConnectedUser = { username: org.CONNECTED_USER.username };

/** The step on its own, through the real client and allowlist. */
async function read(now: Date, options: SimulatorOptions = {}, wrap: (sim: Simulator) => Transport = (sim) => sim, connectedUser: ConnectedUser | null = BY_ID): Promise<{ out: RecordTypeUse; sim: Simulator }> {
  const sim = new Simulator(options);
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: wrap(sim), sleep: async () => {} });
  const out = await recordTypeUseExtractor.run({ client, now, ...(connectedUser ? { connectedUser } : {}) } as unknown as ExtractContext);
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
  const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: new Simulator(), connectedUser: BY_ID, log: () => {} });
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
/** An evidence item's query, or its path for a read that isn't SOQL. */
const queryOf = (e: Finding["evidence"][number]): string => ("query" in e ? e.query : "path" in e ? e.path : "");
const exclusion = (f: Finding, starts: string): string => f.evidence.flatMap((e) => e.exclusions).find((x) => x.startsWith(starts))!;
/** The exclusion for the objects a count can't see whole, before its count (DECISIONS.md 499). */
const NEVER_COUNTED = "Objects we never count, because a count can't show every record on them (Salesforce archives older tasks and events, shows a user only the files they own or share, and returns article versions by publish status)";

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

  test("on the full route the user holds View All Data, so every object is counted, and the evidence says so", () => {
    const f = synthetic.findings.find((x) => x.ruleId === RULE)!;
    assert.equal(f.evidence.length, 4);
    const access = f.evidence[3]!;
    assert.equal(queryOf(access), VIEW_ALL_RECORDS("<the connected user>"));
    assert.equal(access.count, org.READ_ONLY_VIEW_ALL.length);
    // Not "every record in the org": a user with View All Data still sees only their own files (DECISIONS.md 499).
    assert.equal(access.note, `Objects the connected user holds View All Records on. They hold View All Data, so they see every record on the objects we count. View All Data read with: ${VIEW_ALL_DATA("<the connected user>")}`);
    assert.equal(exclusion(f, "Objects where the connected user"), "Objects where the connected user can't see every record: 0");
    // The value line is the rule's own, not the guards'.
    assert.deepEqual(f.value, { kind: "not_sized", reason: "We don't put a figure on this: the only one to hand would be a guess at upkeep hours." });
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

  test("the step makes four calls: the user's access, the list and one call of counts", () => {
    assert.ok(synthetic.calls >= 1 && synthetic.calls <= 6, `${synthetic.calls} calls`);
    assert.equal(synthetic.calls, 4);
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
  // Active and old, on objects with 50 recent records and none of them on it:
  // counted, each would be "no records at all". Activities never are.
  task: type("Task", "Call_Log", daysUntil(T, 800)),
  event: type("Event", "Site_Visit", daysUntil(T, 800)),
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
  Task: [recent(null, 50)],
  Event: [recent(null, 50)],
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
    assert.equal(exclusion(f, "Objects we never count"), `${NEVER_COUNTED}: 2 (Event and Task)`);
    for (const quiet of ["Old Format", "New Type", "Partner", "Billing Run", "Default", "Thing", "Call Log", "Site Visit"]) assert.ok(!names(f).some((s) => s.startsWith(quiet)), quiet);
  });

  test("tasks and events are never counted, and don't take the place of an object we can count", () => {
    assert.ok(!queriesOf(traps.sim.requests).some((q) => isCount(q) && /FROM (Task|Event) /.test(q)));
    assert.deepEqual(traps.out.partlyHidden, ["Event", "Task"]);
    assert.equal(traps.out.counted.Task, undefined);
    assert.equal(traps.out.counted.Event, undefined);
    // Left out before the 25, so the 26th object is still the only one past it.
    assert.deepEqual(traps.out.notRead, ["Zz_Last__c"]);
  });

  test("an activity's count, were one ever read, still judges no type of it", () => {
    const counted = { ...traps.out.counted, Task: { all: [{ RecordTypeId: null, c: 50 }], recent: [{ RecordTypeId: null, c: 50 }] } };
    const g = judge({ ...traps.out, counted });
    assert.ok(!names(g).some((s) => s.startsWith("Call Log")));
    assert.match(exclusion(g, "Objects we never count"), /: 2 \(Event and Task\)$/);
    assert.deepEqual([...PARTLY_HIDDEN_OBJECTS].sort(), ["ContentDocument", "ContentVersion", "Event", "Task"]);
  });

  test("an org whose only types are on tasks and events has nothing judged, and says why", async () => {
    const { out, sim } = await read(T, { recordTypes: [TYPES.task, TYPES.event], recordTypeGroups: GROUPS });
    assert.ok(!queriesOf(sim.requests).some(isCount));
    const g = judge(out);
    assert.equal(g.status, "not_measurable");
    assert.equal(g.notMeasurableReason, "None of your 2 record types could be judged: we don't count events and tasks, because a count can't show all of them.");
  });

  // A user with View All Data still sees only the files they own or share, and a
  // plain query returns article versions by publish status (DECISIONS.md 499).
  describe("files and article versions, with the user holding View All Data", () => {
    // Active and old, each on an object with 50 recent records and none on it:
    // counted, each would be "no records at all".
    const PLANTED = {
      contentVersion: type("ContentVersion", "Contract_File", daysUntil(T, 800)),
      contentDocument: type("ContentDocument", "Signed_Copy", daysUntil(T, 800)),
      kav: type("Thing__kav", "How_To", daysUntil(T, 800)),
      // Namespaced, and in capitals: still an article version object.
      namespacedKav: type("acme__Guide__KAV", "FAQ", daysUntil(T, 800)),
    };
    const planted: SimulatorOptions = {
      recordTypes: [TYPES.standard, TYPES.creditNote, TYPES.proForma, ...Object.values(PLANTED)],
      recordTypeGroups: { Invoice__c: GROUPS.Invoice__c!, ...Object.fromEntries(Object.values(PLANTED).map((t) => [t.SobjectType, [recent(null, 50)]])) },
    };
    const OBJECTS = ["ContentDocument", "ContentVersion", "Thing__kav", "acme__Guide__KAV"];

    test("no count is sent for any of them, none is flagged, and each is named in the exclusion", async () => {
      const { out, sim } = await read(T, planted);
      assert.equal(out.access.status === "ok" && out.access.viewAllData, true);
      const counts = queriesOf(sim.requests).filter(isCount);
      for (const object of OBJECTS) assert.ok(!counts.some((q) => q.includes(`FROM ${object} `)), `${object} counted`);
      assert.ok(counts.some((q) => /FROM Invoice__c /.test(q)));
      assert.deepEqual([...out.partlyHidden].sort(), OBJECTS);
      assert.deepEqual(out.notSeenWhole, []);
      const f = judge(out);
      assert.equal(f.status, "issue");
      assert.deepEqual(names(f), ["Pro Forma on Invoice", "Credit Note on Invoice"]);
      assert.equal(exclusion(f, "Objects we never count"), `${NEVER_COUNTED}: 4 (ContentDocument, ContentVersion, Thing__kav and acme__Guide__KAV)`);
      for (const object of OBJECTS) assert.ok(isPartlyHidden(object), object);
      // A name that only holds "kav" isn't one.
      assert.ok(!isPartlyHidden("Kavanagh__c") && !isPartlyHidden("Thing__kav__c"));
    });

    test("their counts, were one ever read, still judge no type of them", async () => {
      const { out } = await read(T, planted);
      const counted = { ...out.counted, ...Object.fromEntries(OBJECTS.map((o) => [o, { all: [{ RecordTypeId: null, c: 50 }], recent: [{ RecordTypeId: null, c: 50 }] }])) };
      const g = judge({ ...out, counted });
      assert.deepEqual(names(g), ["Pro Forma on Invoice", "Credit Note on Invoice"]);
    });

    test("an org whose only types are on files and article versions has nothing judged, and says why", async () => {
      const { out, sim } = await read(T, { ...planted, recordTypes: Object.values(PLANTED) });
      assert.ok(!queriesOf(sim.requests).some(isCount));
      const g = judge(out);
      assert.equal(g.status, "not_measurable");
      assert.equal(g.notMeasurableReason, "None of your 4 record types could be judged: we don't count article versions and files, because a count can't show all of them.");
    });

    test("with tasks and events too, the reason names each kind once", async () => {
      const { out } = await read(T, { recordTypes: [...Object.values(PLANTED), TYPES.task, TYPES.event], recordTypeGroups: {} });
      assert.equal(judge(out).notMeasurableReason, "None of your 6 record types could be judged: we don't count article versions, events, files and tasks, because a count can't show all of them.");
    });
  });

  test("the step keeps why each object wasn't counted, and reads 25 objects in five calls", () => {
    assert.deepEqual(traps.out.notRead, ["Zz_Last__c"]);
    assert.deepEqual(traps.out.refused, [{ object: "Broken__c", reason: "Broken__c.RecordTypeId isn't in this org." }]);
    assert.deepEqual(traps.out.truncated, ["Cut__c"]);
    // A refused or cut short object is never read as one with no records.
    assert.equal(traps.out.counted.Broken__c, undefined);
    assert.equal(traps.out.counted.Cut__c, undefined);
    assert.equal(Object.keys(traps.out.counted).length, RECORD_TYPE_OBJECTS - 2);
    // Two for the access, one for the list and two of counts.
    assert.equal(billable(traps.sim.requests).length, 5);
    // The packaged type's object has no active type of our own, so it isn't counted.
    assert.ok(!queriesOf(traps.sim.requests).some((q) => /FROM Opportunity /.test(q)));
  });

  test("an object whose counts all failed isn't judged at all", async () => {
    const everyCount = { ...TRAPS, failQuery: (soql: string) => (isCount(soql) ? { errorCode: "MALFORMED_QUERY", message: "unexpected token" } : null) };
    const { out } = await read(T, everyCount);
    assert.deepEqual(out.counted, {});
    const g = judge(out);
    assert.equal(g.status, "not_measurable");
    assert.match(g.notMeasurableReason!, /^None of your \d+ record types could be judged: we don't count events and tasks, because a count can't show all of them; Salesforce refused or cut short the count on 25 objects; 1 object is past the 25 we count\.$/);
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

  // Every try at a count call, retries and the try after a refresh too.
  const everyCountCall = (status: number, text: string) => (sim: Simulator): Transport => ({
    async send(request) {
      if (request.url.endsWith("/composite/batch") && queriesOf([request]).some(isCount)) return { status, header: () => null, text };
      return sim.send(request);
    },
  });
  const BAD_GATEWAY = everyCountCall(502, "<html><body>502 Bad Gateway</body></html>");
  const EXPIRED = everyCountCall(401, JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]));

  test("a 502 through every retry isn't Salesforce's answer: the step stops rather than saving the objects as refused", async () => {
    await assert.rejects(read(T, TRAPS, BAD_GATEWAY), (err: unknown) => err instanceof SalesforceError && err.code === "HTTP_502");
  });

  test("a session still expired after the refresh stops the step too", async () => {
    await assert.rejects(read(T, TRAPS, EXPIRED), (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_SESSION_ID");
  });

  // Inside a call that came back, one count failing is that object refused,
  // whatever the code, as the ownership counts have it (DECISIONS.md 498).
  for (const [name, item, reason] of [
    ["a 503 SERVER_UNAVAILABLE", { statusCode: 503, result: [{ errorCode: "SERVER_UNAVAILABLE", message: "Server temporarily unavailable" }] }, "SERVER_UNAVAILABLE: Server temporarily unavailable"],
    ["a bare 500 with no code", { statusCode: 500, result: [{}] }, "UNKNOWN: Subrequest failed"],
  ] as const) {
    test(`one count answered with ${name} is that object refused, and the step finishes`, async () => {
      const failing = editComposite((soql, answer) => {
        // Only Cut__c's count of every record: its other count comes back.
        if (/FROM Cut__c /.test(soql) && !/CreatedDate >=/.test(soql)) Object.assign(answer, structuredClone(item));
      });
      const { out } = await read(T, TRAPS, failing);
      assert.deepEqual(out.refused.map((r) => r.object).sort(), ["Broken__c", "Cut__c"]);
      assert.equal(out.refused.find((r) => r.object === "Cut__c")!.reason, reason);
      assert.equal(out.counted.Cut__c, undefined);
      // Every other object is still counted.
      assert.equal(Object.keys(out.counted).length, RECORD_TYPE_OBJECTS - 2);
      const f = judge(out);
      assert.equal(f.status, "issue");
      assert.equal(exclusion(f, "Objects whose count Salesforce refused"), "Objects whose count Salesforce refused: 2 (Broken__c and Cut__c)");
      // Cut__c has 50 records and none on Thing: counted, Thing would be "no records at all".
      assert.ok(!names(f).some((x) => x.endsWith("on Cut__c")));
      assert.deepEqual(names(f), ["Pro Forma on Invoice", "Credit Note on Invoice"]);
    });
  }
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
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir, now: T, transport: sim, connectedUser: BY_ID, log: () => {}, ...overrides });

  const finding = (fs: Finding[]) => fs.find((x) => x.ruleId === RULE)!;
  /** A run stopped by the budget just before the first request `at` picks out, as run.test.ts stops one, and the straight run beside it. */
  async function stoppedAt(at: (q: string) => boolean) {
    const probe = new Simulator();
    const straight = await simulatorRun(probe, tempDir());
    const index = probe.requests.findIndex((r) => queriesOf([r]).some(at));
    assert.ok(index > 0);
    const spent = billable(probe.requests.slice(0, index)).length;
    const out = tempDir();
    const first = new Simulator({ used: 50_000 - spent });
    const stopped = await simulatorRun(first, out);
    assert.equal(stopped.run.status, "stopped_budget");
    const checkpoints = readdirSync(join(stopped.dir, "checkpoints"));
    assert.ok(!checkpoints.some((f) => f.startsWith(recordTypeUseExtractor.id)), checkpoints.join(", "));
    assert.notEqual(stopped.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.status, "done");
    return { straight, stopped, out, before: queriesOf(first.requests) };
  }

  for (const [name, at, readBefore] of [
    ["between the access reads and the list", isList, { access: 2, list: 0 }],
    ["between the list and the counts", isCount, { access: 2, list: 1 }],
  ] as const) {
    test(`${name}: keeps no checkpoint for the step, and the resumed run makes every read again`, async () => {
      const { straight, stopped, out, before } = await stoppedAt(at);
      assert.equal(before.filter(isAccess).length, readBefore.access, "the access was read before the stop");
      assert.equal(before.filter(isList).length, readBefore.list);
      assert.equal(before.filter(isCount).length, 0);

      const second = new Simulator();
      const resumed = await simulatorRun(second, out, { resume: stopped.run.id });
      assert.equal(resumed.run.status, "in_review");
      const again = queriesOf(second.requests);
      assert.equal(again.filter(isViewAllData).length, 1, "View All Data is read again");
      assert.equal(again.filter(isViewAllRecords).length, 1, "View All Records is read again");
      assert.equal(again.filter(isList).length, 1, "the list is read again");
      assert.equal(again.filter(isCount).length, 2 * 4, "both counts on each object are read again");
      assert.ok(existsSync(join(resumed.dir, "checkpoints", `${recordTypeUseExtractor.id}.json`)));
      assert.deepEqual(finding(resumed.output!.findings), finding(straight.output!.findings));
    });
  }

  test("a run stopped before this step gets the connected user again on resume, from its caller, and gives the same finding", async () => {
    const { straight, stopped, out, before } = await stoppedAt(isViewAllData);
    assert.equal(before.filter(isAccess).length, 0);
    const resumed = await simulatorRun(new Simulator(), out, { resume: stopped.run.id });
    assert.equal(finding(resumed.output!.findings).status, "issue");
    assert.deepEqual(finding(resumed.output!.findings), finding(straight.output!.findings));
    // Nothing in the folder says who connected: resumed without being told, it can't judge.
    const again = await stoppedAt(isViewAllData);
    const untold = await simulatorRun(new Simulator(), again.out, { resume: again.stopped.run.id, connectedUser: undefined });
    assert.equal(finding(untold.output!.findings).status, "not_measurable");
    assert.match(finding(untold.output!.findings).notMeasurableReason!, /The run wasn't told which user connected\.$/);
  });

  // The step went to version 4 when activities became partlyHidden (DECISIONS.md 499).
  test("a run in flight with a version 3 checkpoint of the step reads it again on resume, to the run's own date", async () => {
    const straight = await simulatorRun(new Simulator(), tempDir());
    const first = await simulatorRun(new Simulator(), tempDir());
    // As a run in flight before this change left it: the step done at version
    // 3, its checkpoint in the old shape and holding nothing counted, then
    // stopped later on.
    const path = join(first.dir, "run.json");
    const state = JSON.parse(readFileSync(path, "utf8")) as { status: string; steps: Array<{ id: string; version: number }> };
    state.status = "stopped_budget";
    state.steps.find((s) => s.id === recordTypeUseExtractor.id)!.version = 3;
    writeFileSync(path, JSON.stringify(state));
    const checkpoint = join(first.dir, "checkpoints", `${recordTypeUseExtractor.id}.json`);
    const { partlyHidden: _p, ...shape } = readCheckpoint(checkpoint) as RecordTypeUse;
    writeCheckpoint(checkpoint, { ...shape, activities: [], counted: {}, queries: { ...shape.queries, viewAllData: VIEW_ALL_DATA("<the connected user>").replace(" AND PermissionSet.HasActivationRequired = false", "") } });

    const second = new Simulator();
    // A day after the run's date: the reads still end at the run's own.
    const resumed = await simulatorRun(second, dirname(first.dir), { resume: first.run.id, now: new Date(T.getTime() + DAY) });
    const again = queriesOf(second.requests);
    assert.deepEqual(again.filter(isViewAllData), [VIEW_ALL_DATA(org.CONNECTED_USER.id)], "the access is read again");
    assert.deepEqual(again.filter(isList), [`SELECT Id, DeveloperName, Name, SobjectType, IsActive, NamespacePrefix, CreatedDate FROM RecordType WHERE CreatedDate <= ${soqlDateTime(upToItsSecond(T))}`]);
    assert.equal(again.filter(isCount).length, 2 * 4, "both counts on each object are read again");
    assert.equal(resumed.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.version, 4);
    const use = readCheckpoint(checkpoint) as RecordTypeUse & { activities?: unknown };
    assert.equal(use.activities, undefined);
    assert.deepEqual(use.partlyHidden, []);
    assert.equal(use.queries.viewAllData, VIEW_ALL_DATA("<the connected user>"));
    assert.deepEqual(finding(resumed.output!.findings), finding(straight.output!.findings));
  });
});

describe("a run whose counts fail for a moment", () => {
  for (const [name, status, text, code] of [
    ["a 502 through every retry", 502, "<html><body>502 Bad Gateway</body></html>", "HTTP_502"],
    ["a session still expired after the refresh", 401, JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]), "INVALID_SESSION_ID"],
  ] as const) {
    test(`${name}: no checkpoint, the step isn't done, and a resume reads the list and both counts again`, async () => {
      const failing = new Simulator();
      const transport: Transport = {
        async send(request) {
          if (request.url.endsWith("/composite/batch") && queriesOf([request]).some(isCount)) return { status, header: () => null, text };
          return failing.send(request);
        },
      };
      const outDir = tempDir();
      const runId = `rt-${status}`;
      const options = { provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir, now: T, connectedUser: BY_ID, log: () => {} };
      await assert.rejects(runHealthCheck({ ...options, transport, runId }), (err: unknown) => err instanceof SalesforceError && err.code === code);
      const checkpoints = readdirSync(join(outDir, runId, "checkpoints"));
      assert.ok(!checkpoints.some((f) => f.startsWith(recordTypeUseExtractor.id)), checkpoints.join(", "));
      const run = JSON.parse(readFileSync(join(outDir, runId, "run.json"), "utf8")) as { status: string; steps: Array<{ id: string; status: string }> };
      assert.equal(run.status, "failed");
      assert.equal(run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.status, "failed");

      const second = new Simulator();
      const resumed = await runHealthCheck({ ...options, transport: second, resume: runId });
      assert.equal(resumed.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.status, "done");
      const again = queriesOf(second.requests);
      assert.equal(again.filter(isList).length, 1, "the list is read again");
      assert.equal(again.filter(isCount).length, 2 * 4, "both counts on each object are read again");
      const finding = (fs: Finding[]) => fs.find((x) => x.ruleId === RULE);
      assert.deepEqual(finding(resumed.output!.findings), synthetic.findings.find((x) => x.ruleId === RULE));
    });
  }
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
    // Its own reason for having no figure, kept when the guards block it (DECISIONS.md 498):
    // "not counted as a saving" would say a saving was waiting, and none ever is.
    const reason = "We don't put a figure on this: the only one to hand would be a guess at upkeep hours.";
    assert.deepEqual(f.value, { kind: "not_sized", reason });
    // As a report page prints it, beside "Not costed".
    assert.deepEqual(valueText(renderContext(new Pseudonymiser(), "GBP", false), f), { amount: "Not costed", note: escapeXml(reason) });
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
    assert.deepEqual(printed.value, raw.value);
    // Were every guard to pass, the deactivation would print: it's the guard that holds it back.
    assert.equal(applyGuards({ ...raw, guards: raw.guards!.map((g) => ({ ...g, status: "passed" as const })) }).recommendationType, "deactivate");
  });
});

// Only objects whose every record the connected user can see are counted, and
// the run is told who connected by its caller, never by its folder (DECISIONS.md 498).
describe("who connected", () => {
  const LOOKUP = `SELECT Id FROM User WHERE Username = '${org.CONNECTED_USER.username}'`;
  const UNKNOWN = "We couldn't tell which records the connected user can see, so finding none on a record type would prove nothing.";

  test("the read-only route: no count is sent for an object the user can't see whole, it's in its own exclusion, and Support on Case is still flagged", async () => {
    const sim = new Simulator({ readOnly: true });
    const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: sim, connectedUser: BY_ID, log: () => {} });
    const counts = queriesOf(sim.requests).filter(isCount);
    assert.ok(!counts.some((q) => /FROM (Invoice__c|Empty_Object__c) /.test(q)), counts.join("\n"));
    assert.ok(counts.some((q) => /FROM Case /.test(q)));
    const f = run.output!.findings.find((x) => x.ruleId === RULE)!;
    assert.equal(f.status, "issue");
    assert.deepEqual(names(f), ["Support on Case"]);
    assert.equal(exclusion(f, "Objects where the connected user"), "Objects where the connected user can't see every record: 2 (Empty_Object__c and Invoice__c)");
    // Not counted, so not refused either.
    assert.equal(exclusion(f, "Objects whose count Salesforce refused"), "Objects whose count Salesforce refused: 0");
    assert.equal(f.evidence[3]!.count, 6);
    assert.equal(
      f.evidence[3]!.note,
      `Objects the connected user holds View All Records on. They don't hold View All Data, so we count only objects among these, and they see every record on the objects we count. View All Data read with: ${VIEW_ALL_DATA("<the connected user>")}`,
    );
    assert.equal(run.run.steps.find((s) => s.id === recordTypeUseExtractor.id)!.calls, 4);
  });

  test("a user with neither View All Data nor View All Records anywhere: nothing is counted, and it's not measurable", async () => {
    const { out, sim } = await read(T, { access: { viewAllData: false, viewAllRecords: [] } });
    assert.ok(!queriesOf(sim.requests).some(isCount));
    // Most active types first, as the 25 are chosen.
    assert.deepEqual(out.notSeenWhole, ["Invoice__c", "Opportunity", "Case", "Empty_Object__c"]);
    const f = judge(out);
    assert.equal(f.status, "not_measurable");
    assert.equal(f.notMeasurableReason, "None of your 6 record types could be judged: the connected user can't see every record on 4 objects.");
    assert.equal(exclusion(f, "Objects where the connected user"), "Objects where the connected user can't see every record: 4 (Case, Empty_Object__c, Invoice__c and Opportunity)");
  });

  for (const [name, fails, user, reason] of [
    ["the username lookup", isLookup, BY_NAME, /^We couldn't look the connected user up by their username: /],
    ["the View All Data read", isViewAllData, BY_ID, /^The connected user's permission set assignments couldn't be read: /],
    ["the View All Records read", isViewAllRecords, BY_ID, /^The connected user's object permissions couldn't be read: /],
  ] as const) {
    test(`${name} refused: not measurable, and no count is sent`, async () => {
      const { out, sim } = await read(T, { failQuery: (soql) => (fails(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "insufficient access rights on object id" } : null) }, undefined, user);
      assert.ok(out.access.status === "unknown");
      assert.match(out.access.reason, reason);
      assert.ok(!queriesOf(sim.requests).some(isCount));
      const f = judge(out);
      assert.equal(f.status, "not_measurable");
      assert.equal(f.notMeasurableReason, `${UNKNOWN} ${out.access.reason}`);
      // Cites the access read, with the user as a placeholder.
      assert.deepEqual(f.evidence.map((e) => queryOf(e).includes("<the connected user>")), [false, true]);
      assert.equal(f.evidence[1]!.count, null);
    });
  }

  test("a planted object whose user reads 30 recent records of one type, without View All on it, isn't counted and nothing on it is flagged", async () => {
    const used = type("Planted__c", "Used", daysUntil(T, 800));
    const spare = type("Planted__c", "Spare", daysUntil(T, 800));
    const opportunitySpare = type("Opportunity", "Spare", daysUntil(T, 800));
    const { out, sim } = await read(T, {
      recordTypes: [used, spare, opportunitySpare],
      recordTypeGroups: { Planted__c: [recent(used.Id, 30)], Opportunity: [recent(null, 50)] },
      access: { viewAllData: false, viewAllRecords: org.READ_ONLY_VIEW_ALL },
    });
    assert.ok(!queriesOf(sim.requests).some((q) => /FROM Planted__c /.test(q)));
    assert.deepEqual(out.notSeenWhole, ["Planted__c"]);
    assert.equal(out.counted.Planted__c, undefined);
    const f = judge(out);
    assert.equal(f.status, "issue");
    // Counted, Spare on Planted__c would be "no records at all": its 30 records are only those the user sees.
    assert.deepEqual(names(f), ["Spare on Opportunity"]);
    assert.equal(exclusion(f, "Objects where the connected user"), "Objects where the connected user can't see every record: 1 (Planted__c)");
  });

  /** An access read answered with a fault that quotes the query and the user's id in both its forms, as Salesforce's do. The read is still sent. */
  const quotingId = (code: string, which: (q: string) => boolean) => (sim: Simulator): Transport => ({
    async send(request) {
      const res = await sim.send(request);
      const q = queriesOf([request])[0] ?? "";
      if (!which(q)) return res;
      const id = org.CONNECTED_USER.id;
      return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: code, message: `\n${q}\n^\nERROR at Row:1:Column:80\nbad value '${id}' or '${id.slice(0, 15)}'` }]) };
    },
  });

  // A set that has to be activated in a session shows as assigned while an API
  // session has none of its permissions (DECISIONS.md 499).
  describe("a permission set activated in a session", () => {
    const ONLY_THE_SIX = { viewAllData: false, viewAllRecords: org.READ_ONLY_VIEW_ALL };

    test("both access reads leave such sets out, in exactly these words", async () => {
      const { sim } = await read(T, { access: { ...ONLY_THE_SIX, session: { viewAllData: true, viewAllRecords: ["Invoice__c"] } } });
      const queries = queriesOf(sim.requests);
      assert.deepEqual(queries.filter(isViewAllData), [VIEW_ALL_DATA(org.CONNECTED_USER.id)]);
      assert.deepEqual(queries.filter(isViewAllRecords), [VIEW_ALL_RECORDS(org.CONNECTED_USER.id)]);
    });

    test("one that alone gives View All Data doesn't count: the objects it would open aren't counted", async () => {
      const { out, sim } = await read(T, { access: { ...ONLY_THE_SIX, session: { viewAllData: true, viewAllRecords: [] } } });
      // What's counted first: a read that let the set in would count both.
      const counts = queriesOf(sim.requests).filter(isCount);
      assert.ok(!counts.some((q) => /FROM (Invoice__c|Empty_Object__c) /.test(q)), `counted:\n${counts.join("\n")}`);
      assert.deepEqual(out.access, { status: "ok", viewAllData: false, viewAllRecords: [...org.READ_ONLY_VIEW_ALL].sort() });
      assert.deepEqual(out.notSeenWhole, ["Invoice__c", "Empty_Object__c"]);
      const f = judge(out);
      assert.deepEqual(names(f), ["Support on Case"]);
      assert.equal(exclusion(f, "Objects where the connected user"), "Objects where the connected user can't see every record: 2 (Empty_Object__c and Invoice__c)");
    });

    test("one that alone gives View All Records on an object doesn't count: that object isn't counted", async () => {
      const { out, sim } = await read(T, { access: { ...ONLY_THE_SIX, session: { viewAllData: false, viewAllRecords: ["Invoice__c"] } } });
      assert.ok(!queriesOf(sim.requests).some((q) => isCount(q) && /FROM Invoice__c /.test(q)), "Invoice__c counted");
      assert.ok(out.access.status === "ok" && !out.access.viewAllRecords.includes("Invoice__c"));
      assert.ok(out.notSeenWhole.includes("Invoice__c"));
      assert.ok(!names(judge(out)).some((s) => s.endsWith("on Invoice")));
    });

    // Every user holds their profile's own set, which never needs activating, so
    // no assignments at all still means we can't see them, not that there are none.
    test("a user with no assignments visible is access unknown, not no access: not measurable, and no count is sent", async () => {
      const { out, sim } = await read(T, {}, undefined, { id: org.sfId("005", 990001) });
      assert.deepEqual(out.access, { status: "unknown", reason: "No permission set assignments were visible for the connected user." });
      assert.ok(!queriesOf(sim.requests).some(isCount));
      assert.equal(judge(out).notMeasurableReason, `${UNKNOWN} No permission set assignments were visible for the connected user.`);
    });

    for (const [name, fails, reason] of [
      ["the View All Data read", (q: string) => isViewAllData(q) && /PermissionSet\.HasActivationRequired/.test(q), "The connected user's permission set assignments couldn't be read: PermissionSet.HasActivationRequired isn't in this org."],
      ["the View All Records read", (q: string) => isViewAllRecords(q) && /Parent\.HasActivationRequired/.test(q), "The connected user's object permissions couldn't be read: PermissionSet.HasActivationRequired isn't in this org."],
    ] as const) {
      test(`HasActivationRequired refused on ${name}: access unknown, not measurable, and no count is sent`, async () => {
        const { out, sim } = await read(T, { failQuery: (soql) => (fails(soql) ? { errorCode: "INVALID_FIELD", message: "No such column 'HasActivationRequired' on entity 'PermissionSet'." } : null) });
        assert.deepEqual(out.access, { status: "unknown", reason });
        assert.ok(!queriesOf(sim.requests).some(isCount));
        const f = judge(out);
        assert.equal(f.status, "not_measurable");
        assert.equal(f.notMeasurableReason, `${UNKNOWN} ${reason}`);
      });
    }
  });

  // The id the step looks up from a username is as much the user's as one it's
  // given, so every reason and fault from the reads that hold it hides it
  // (DECISIONS.md 499). The fault quotes the query, as Salesforce's do, and the
  // id in both its forms.
  describe("the id looked up from a username", () => {
    const ID = org.CONNECTED_USER.id;
    const run = (transport: Transport, outDir: string, runId?: string) =>
      runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir, now: T, transport, connectedUser: BY_NAME, log: () => {}, ...(runId ? { runId } : {}) });
    const holdsId = (text: string): boolean => text.includes(ID) || text.includes(ID.slice(0, 15));
    const filesIn = (dir: string): string[] => readdirSync(dir).flatMap((name) => (statSync(join(dir, name)).isDirectory() ? filesIn(join(dir, name)) : [join(dir, name)]));

    for (const [name, which, starts] of [
      ["the View All Data read", isViewAllData, "The connected user's permission set assignments couldn't be read: "],
      ["the View All Records read", isViewAllRecords, "The connected user's object permissions couldn't be read: "],
    ] as const) {
      test(`${name} refused with a reason quoting the id: the reason, the finding and the checkpoint hold the placeholder, never the id`, async () => {
        const sim = new Simulator();
        const done = await run(quotingId("INVALID_FIELD", which)(sim), tempDir());
        // The read did hold the id, so a leak would have something to leak.
        assert.ok(queriesOf(sim.requests).some((q) => which(q) && q.includes(ID)));
        const checkpoint = join(done.dir, "checkpoints", `${recordTypeUseExtractor.id}.json`);
        const use = readCheckpoint(checkpoint) as RecordTypeUse;
        assert.ok(use.access.status === "unknown");
        assert.equal(use.access.reason, `${starts}INVALID_FIELD: bad value '<the connected user>' or '<the connected user>'`);
        assert.ok(!holdsId(readFileSync(checkpoint, "utf8")));
        const f = done.output!.findings.find((x) => x.ruleId === RULE)!;
        assert.equal(f.status, "not_measurable");
        assert.equal(f.notMeasurableReason, `${UNKNOWN} ${use.access.reason}`);
        assert.ok(f.notMeasurableReason!.includes("<the connected user>"));
        assert.equal(f.evidence[1]!.note, `We couldn't tell which objects the connected user sees every record of. ${use.access.reason}`);
        for (const e of f.evidence) assert.ok(!holdsId(JSON.stringify(e)), JSON.stringify(e));
        for (const path of filesIn(done.dir)) assert.ok(!holdsId(readFileSync(path, "utf8")), path);
      });

      test(`${name} failing with a code that isn't Salesforce's answer, quoting the id: the step stops, and neither the fault nor run.json holds the id`, async () => {
        const outDir = tempDir();
        const runId = `rt-quote-${which === isViewAllData ? "vad" : "var"}`;
        await assert.rejects(run(quotingId("INVALID_QUERY_FILTER_OPERATOR", which)(new Simulator()), outDir, runId), (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_QUERY_FILTER_OPERATOR" && !holdsId(err.message) && err.message.includes("<the connected user>"));
        const saved = JSON.parse(readFileSync(join(outDir, runId, "run.json"), "utf8")) as { steps: Array<{ id: string; status: string; error: string | null }> };
        const step = saved.steps.find((s) => s.id === recordTypeUseExtractor.id)!;
        assert.equal(step.status, "failed");
        assert.ok(step.error!.includes("<the connected user>") && !holdsId(step.error!), step.error!);
        for (const path of filesIn(join(outDir, runId))) assert.ok(!holdsId(readFileSync(path, "utf8")), path);
      });
    }
  });

  test("a run told no one: no access read and no count, and not measurable", async () => {
    const { out, sim } = await read(T, {}, undefined, null);
    assert.deepEqual(out.access, { status: "unknown", reason: "The run wasn't told which user connected." });
    assert.ok(!queriesOf(sim.requests).some((q) => isAccess(q) || isCount(q)));
    const f = judge(out);
    assert.equal(f.status, "not_measurable");
    assert.equal(f.notMeasurableReason, `${UNKNOWN} The run wasn't told which user connected.`);
    assert.equal(f.evidence[1]!.note, "We couldn't tell which objects the connected user sees every record of. The run wasn't told which user connected.");
  });

  test("a username only is looked up, then read as the id is, to the same finding", async () => {
    const { out, sim } = await read(T, {}, undefined, BY_NAME);
    const queries = queriesOf(sim.requests);
    assert.deepEqual(queries.filter(isLookup), [LOOKUP]);
    assert.deepEqual(queries.filter(isViewAllData), [VIEW_ALL_DATA(org.CONNECTED_USER.id)]);
    assert.deepEqual(queries.filter(isViewAllRecords), [VIEW_ALL_RECORDS(org.CONNECTED_USER.id)]);
    assert.deepEqual(out.access, { status: "ok", viewAllData: true, viewAllRecords: [...org.READ_ONLY_VIEW_ALL].sort() });
    assert.deepEqual(judge(out), synthetic.findings.find((x) => x.ruleId === RULE));
    // A username, the list and the counts: five calls, and six at most.
    assert.equal(billable(sim.requests).length, 5);
  });

  test("with a username and 25 objects the step makes six calls", async () => {
    const { sim } = await read(T, TRAPS, undefined, BY_NAME);
    assert.equal(billable(sim.requests).length, 6);
    assert.equal(queriesOf(sim.requests).filter(isCount).length, 2 * RECORD_TYPE_OBJECTS);
  });

  test("a username matching no one is not measurable, and nothing else is read but the list", async () => {
    const { out, sim } = await read(T, {}, undefined, { username: "nobody@example.com" });
    assert.deepEqual(out.access, { status: "unknown", reason: "We couldn't find the connected user by their username." });
    const queries = queriesOf(sim.requests);
    assert.deepEqual(queries.filter((q) => isAccess(q) || isCount(q)), ["SELECT Id FROM User WHERE Username = 'nobody@example.com'"]);
    assert.equal(judge(out).status, "not_measurable");
  });

  test("a username holding a quote or a backslash is escaped, and can't end the string", async () => {
    const username = "o'brien\\' OR Username != '@example.com";
    const { out, sim } = await read(T, {}, undefined, { username });
    assert.deepEqual(queriesOf(sim.requests).filter(isLookup), ["SELECT Id FROM User WHERE Username = 'o\\'brien\\\\\\' OR Username != \\'@example.com'"]);
    // The simulator reads the literal back as one username, which is no one's.
    assert.deepEqual(out.access, { status: "unknown", reason: "We couldn't find the connected user by their username." });
    assert.ok(!JSON.stringify(out).includes("brien"));
  });

  for (const id of ["005' OR Id != '", "005000000990000AA", "", "005000000990000AAA "]) {
    test(`a malformed id (${JSON.stringify(id)}) is not measurable, and no query holds it`, async () => {
      const { out, sim } = await read(T, {}, undefined, { id });
      assert.ok(out.access.status === "unknown");
      assert.equal(out.access.reason, id ? "The connected user's id isn't the shape of a Salesforce id." : "The run wasn't told which user connected.");
      const queries = queriesOf(sim.requests);
      assert.ok(!queries.some((q) => isAccess(q) || isCount(q)), queries.join("\n"));
      assert.equal(judge(out).status, "not_measurable");
    });
  }

  test("a fault quoting the user in an access read stops the step, and its message doesn't hold them", async () => {
    const quoting = (sim: Simulator): Transport => ({
      async send(request) {
        const q = queriesOf([request])[0] ?? "";
        if (isViewAllData(q)) return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_QUERY_FILTER_OPERATOR", message: `\n${q}\n^\nERROR at Row:1:Column:80\nbad value '${org.CONNECTED_USER.id}'` }]) };
        return sim.send(request);
      },
    });
    await assert.rejects(read(T, {}, quoting, BY_ID), (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_QUERY_FILTER_OPERATOR" && !err.message.includes(org.CONNECTED_USER.id) && err.message.includes("<the connected user>"));
  });

  test("the id and the username are nowhere in the run folder, the log, the raw notes or any evidence", async () => {
    // On the username routes the id is the one the step looked up (DECISIONS.md 499).
    const secrets = [org.CONNECTED_USER.id, org.CONNECTED_USER.id.slice(0, 15), org.CONNECTED_USER.username];
    const files = (dir: string): string[] => readdirSync(dir).flatMap((name) => (statSync(join(dir, name)).isDirectory() ? files(join(dir, name)) : [join(dir, name)]));
    const asItIs = (sim: Simulator): Transport => sim;
    for (const [route, user, readOnly, wrap] of [
      ["by id", BY_ID, false, asItIs],
      ["by username", BY_NAME, false, asItIs],
      ["read-only, by username", BY_NAME, true, asItIs],
      ["by username, with View All Data refused quoting the looked-up id", BY_NAME, false, quotingId("INSUFFICIENT_ACCESS", isViewAllData)],
      ["by username, with View All Records refused quoting the looked-up id", BY_NAME, false, quotingId("INSUFFICIENT_ACCESS", isViewAllRecords)],
    ] as const) {
      const lines: string[] = [];
      const sim = new Simulator({ readOnly });
      const run = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: wrap(sim), connectedUser: user, log: (line) => lines.push(line) });
      if (wrap !== asItIs) assert.match(run.output!.findings.find((x) => x.ruleId === RULE)!.notMeasurableReason!, /couldn't be read: INSUFFICIENT_ACCESS: bad value '<the connected user>' or '<the connected user>'$/, route);
      // The reads did name the user, so a leak would have something to leak.
      assert.ok(queriesOf(sim.requests).some((q) => q.includes(org.CONNECTED_USER.id)), route);
      const written = files(run.dir);
      for (const name of ["run.json", "findings.json", "metrics.json", "facts.json", "raw-notes.md", join("checkpoints", `${recordTypeUseExtractor.id}.json`)]) assert.ok(written.includes(join(run.dir, name)), `${route}: ${name}`);
      const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(run.dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
      const notes = renderRawNotes({ run: run.run, calibration: loadCalibration(), findings: run.output!.findings, metrics: run.output!.metrics, manualSteps: run.output!.manualSteps, pseudonyms });
      assert.match(notes, /the connected user/);
      const texts = [...written.map((path) => [path, readFileSync(path, "utf8")] as const), ["log", lines.join("\n")] as const, ["raw notes", notes] as const];
      for (const [where, text] of texts) for (const secret of secrets) assert.ok(!text.includes(secret), `${route}: ${secret} in ${where}`);
      for (const e of run.output!.findings.flatMap((f) => f.evidence)) for (const secret of secrets) assert.ok(!queryOf(e).includes(secret) && !(e.note ?? "").includes(secret), `${route}: ${secret} in evidence`);
    }
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
