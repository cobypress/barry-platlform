import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { tempDir } from "../../testing/harness.ts";
import { summariseFlowMetadata, type MetadataBundle } from "../metadata-parse.ts";
import type { ReportScan } from "../report-definitions.ts";
import type { CustomObjectRow } from "../configuration.ts";
import type { RunAsData } from "../users.ts";
import type { ApexData } from "../automation.ts";
import type { Sourced } from "../types.ts";

// DECISIONS.md 73: a connection holding only Barometer Read Only can't use the
// Metadata API, so it reads the same things through Tooling and the Analytics
// API. The simulator's read-only mode answers as bc-hc-dev answered that user,
// and its Tooling view is built from the same files the retrieve returns, so the
// two routes can be compared like for like.

type Finding = { id: string; ruleId: string; status: string; facts: unknown; notMeasurableReason: string | null };
type Run = { dir: string; findings: Finding[]; requests: Array<{ method: string; url: string }> };

async function run(readOnly: boolean): Promise<Run> {
  const transport = new Simulator({ readOnly });
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, log: () => {} });
  return { dir: result.dir, findings: JSON.parse(readFileSync(join(result.dir, "findings.json"), "utf8")) as Finding[], requests: transport.requests };
}

const checkpoint = <T>(r: Run, id: string): T => JSON.parse(readFileSync(join(r.dir, "checkpoints", `${id}.json`), "utf8")) as T;

let admin: Run;
let readOnly: Run;
before(async () => {
  admin = await run(false);
  readOnly = await run(true);
});

describe("the read-only route reads what the retrieve reads", () => {
  test("the metadata bundle comes from Tooling, and matches the retrieve", () => {
    const a = checkpoint<Sourced<MetadataBundle>>(admin, "metadata");
    const b = checkpoint<Sourced<MetadataBundle>>(readOnly, "metadata");
    assert.equal(a.status, "ok");
    assert.equal(b.status, "ok");
    if (a.status !== "ok" || b.status !== "ok") return;
    assert.equal(a.data.route, "retrieve");
    assert.equal(b.data.route, "tooling");
    assert.deepEqual(b.data.flows, a.data.flows);
    assert.deepEqual(b.data.workflows, a.data.workflows);
    assert.deepEqual(b.data.validationRules, a.data.validationRules);
    assert.deepEqual(b.data.customFields, [...a.data.customFields].sort((x, y) => `${x.object}.${x.name}`.localeCompare(`${y.object}.${y.name}`)));
    assert.deepEqual(b.data.references, a.data.references);
    // Tooling gives a record type's label, not its API name. Only counts use them.
    const counts = (types: MetadataBundle["recordTypes"]) => types.map((t) => `${t.object}:${t.active}`).sort();
    assert.deepEqual(counts(b.data.recordTypes), counts(a.data.recordTypes));
  });

  test("what the Tooling route can't search is named, so reference guards stay unknown", () => {
    const b = checkpoint<Sourced<MetadataBundle>>(readOnly, "metadata");
    assert.ok(b.status === "ok");
    assert.deepEqual(b.data.unsearched, ["approval processes"]);
  });

  test("every rule that read the retrieve gives the same answer, except where the data really differs", () => {
    const byId = new Map(readOnly.findings.map((f) => [f.id, f]));
    const changed = admin.findings.filter((f) => {
      const other = byId.get(f.id);
      return !other || other.status !== f.status || JSON.stringify(other.facts) !== JSON.stringify(f.facts);
    });
    // Coverage can't be read by this user, and nor can custom objects' records.
    assert.deepEqual(changed.map((f) => f.ruleId).sort(), ["aut.apex-coverage", "bp.ownership-skew"]);
    assert.equal(readOnly.findings.length, admin.findings.length);
  });

  test("the run never asks the Metadata API more than once", () => {
    assert.equal(readOnly.requests.filter((r) => r.url.includes("/Soap/m/")).length, 2, "one retrieve for automation and one for reports, each refused");
    assert.ok(readOnly.requests.every((r) => r.method === "GET" || r.url.includes("/composite/batch") || r.url.includes("/Soap/m/")));
  });
});

describe("Apex on a read-only connection", () => {
  test("classes come from REST, and coverage is not measurable for the right reason", () => {
    const apex = checkpoint<ApexData>(readOnly, "apex");
    assert.equal(apex.classes.status, "ok");
    assert.equal(apex.orgCoverage.status, "unavailable");
    const coverage = readOnly.findings.find((f) => f.ruleId === "aut.apex-coverage")!;
    assert.equal(coverage.status, "not_measurable");
    assert.match(coverage.notMeasurableReason!, /^Apex coverage couldn't be read: ApexOrgWideCoverage isn't available/);
    assert.doesNotMatch(coverage.notMeasurableReason!, /tests haven't been run/);
  });
});

describe("custom objects from EntityDefinition", () => {
  test("lists custom objects by their 01I id, and leaves out other kinds", () => {
    const ids = checkpoint<Sourced<CustomObjectRow[]>>(readOnly, "custom-objects");
    assert.ok(ids.status === "ok");
    assert.deepEqual(ids.data.map((o) => o.DeveloperName).sort(), ["Empty_Object", "Invoice", "Project", "Project_Line"]);
    assert.ok(ids.data.every((o) => o.Id.startsWith("01I") && o.Id.length === 15));
  });
});

describe("report definitions through the Analytics API", () => {
  test("each report is described, and only the report's own fields count", () => {
    const scan = checkpoint<Sourced<ReportScan>>(readOnly, "report-definitions");
    assert.ok(scan.status === "ok");
    assert.equal(scan.data.route, "analytics");
    assert.equal(scan.data.retrieved, 80);
    assert.equal(scan.data.notRead, 0);
    assert.deepEqual(scan.data.fields["Old_Region__c"], ["reports/Marketing/Report_8.report"]);
    // In the report type, not in any report.
    assert.equal(scan.data.fields["Deal_Score__c"], undefined);
    // A filter value stands in for customer data, and mustn't be kept.
    assert.doesNotMatch(JSON.stringify(scan), /Confidential Buyer/);
  });

  test("the same field index as the retrieve", () => {
    const a = checkpoint<Sourced<ReportScan>>(admin, "report-definitions");
    const b = checkpoint<Sourced<ReportScan>>(readOnly, "report-definitions");
    assert.ok(a.status === "ok" && b.status === "ok");
    assert.deepEqual(b.data.fields, a.data.fields);
  });

  test("a COUNT() that ignores the private scope isn't taken as a count of private reports", () => {
    const scan = checkpoint<Sourced<ReportScan>>(readOnly, "report-definitions");
    assert.ok(scan.status === "ok");
    assert.equal(scan.data.privateReports, null);
    assert.match(scan.data.privateReason!, /manage all private reports/);
    const admins = checkpoint<Sourced<ReportScan>>(admin, "report-definitions");
    assert.ok(admins.status === "ok");
    assert.equal(admins.data.privateReports, 4);
  });
});

describe("things that run as a user", () => {
  test("an AuthProvider the user can't see leaves the scan incomplete, not clean", () => {
    assert.deepEqual(checkpoint<RunAsData>(readOnly, "run-as").incomplete.includes("auth providers"), true);
    assert.deepEqual(checkpoint<RunAsData>(admin, "run-as").incomplete.includes("auth providers"), false);
  });
});

describe("a flow from Tooling's Metadata field", () => {
  // The shape bc-hc-dev returned: typed values, and null for absent elements.
  const flow = summariseFlowMetadata("Account_Stamp", {
    apiVersion: 55,
    constants: [{ dataType: "String", name: "Owner", value: { stringValue: "005000000000001AAA" } }],
    description: null,
    label: "Account Stamp",
    processType: "AutoLaunchedFlow",
    recordUpdates: [{ faultConnector: null, filters: [], inputAssignments: [{ field: "Region__c", value: { stringValue: "owner@example.com" } }], inputReference: "$Record", name: "Stamp" }],
    actionCalls: [],
    loops: null,
    start: { object: "Account", triggerType: "RecordAfterSave", schedule: null },
    status: "Active",
  });

  test("reads typed values and treats null as absent", () => {
    assert.deepEqual(
      { status: flow.status, apiVersion: flow.apiVersion, object: flow.object, triggerType: flow.triggerType, scheduled: flow.scheduled, dml: flow.dmlOrActions, fault: flow.withFault, loops: flow.loops, described: flow.described },
      { status: "Active", apiVersion: 55, object: "Account", triggerType: "RecordAfterSave", scheduled: false, dml: 1, fault: 0, loops: 0, described: false },
    );
  });

  test("finds hardcoded ids and emails as whole values, and same-record updates", () => {
    assert.deepEqual([flow.hardcodedUserIds, flow.hardcodedEmails, flow.sameRecordUpdates], [1, 1, 1]);
    assert.equal(flow.path, "flows/Account_Stamp.flow");
  });
});

describe("reference guards on the read-only route", () => {
  test("a field we didn't find is unknown while approval processes can't be searched", () => {
    const guards = (readOnly.findings as Array<Finding & { guards?: Array<{ guardId: string; status: string; detail: string }> }>).flatMap((f) => f.guards ?? []);
    const automation = guards.filter((g) => g.guardId === "not-referenced-in-automation" && g.status !== "failed");
    assert.ok(automation.length > 0);
    for (const g of automation) {
      assert.equal(g.status, "unknown");
      assert.match(g.detail, /couldn't search these: approval processes/);
    }
  });
});

describe("when the Tooling route fails too", () => {
  test("automation is not measurable, and the reason says both routes were tried", async () => {
    const transport = new Simulator({ readOnly: true, failQuery: (soql) => (/FROM FlowDefinition/.test(soql) ? { errorCode: "INVALID_TYPE", message: "sObject type 'FlowDefinition' is not supported." } : null) });
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, log: () => {} });
    const metadata = JSON.parse(readFileSync(join(result.dir, "checkpoints", "metadata.json"), "utf8")) as Sourced<MetadataBundle>;
    assert.equal(metadata.status, "unavailable");
    if (metadata.status !== "unavailable") return;
    assert.match(metadata.reason, /^The Metadata API refused this connection \(.*ModifyMetadata.*\), and reading the same through the Tooling API didn't work either: FlowDefinition isn't available/);
  });
});
