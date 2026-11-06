import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { modulesFor } from "../scope.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { OrgDocumentation } from "../../extract/documentation.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";

const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const APPS = "sec.self-authorised-connected-apps";
const SITES = "sec.insecure-remote-sites";
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const withDoc = (edit: (d: OrgDocumentation) => void): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get("org-documentation") as OrgDocumentation);
  return analyseExtracts(changed).findings;
};
const ok = <T>(s: { status: string; data?: T }): T => s.data as T;

describe("connected apps anyone can authorise", () => {
  test("fires on the app users authorise themselves, high because it has API logins, with the reliance guard", () => {
    const f = one(output!.findings, APPS);
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "high");
    assert.equal(f.recommendationType, "change");
    assert.match(f.summary, /1 of 2 connected apps .*: Data Loader\./);
    assert.deepEqual(f.subjects.map((s) => s.label), ["Data Loader"]);
    assert.deepEqual(f.guards.map((g) => [g.subject, g.status]), [["Data Loader", "unknown"]]);
    assert.match(f.guards[0]!.detail, /relies on/);
  });

  test("medium when the open app has no API logins", () => {
    const f = one(withDoc((d) => ok<Array<{ Name: string }>>(d.connectedApps).forEach((a) => a.Name === "Data Loader" && (a.Name = "Quiet Tool"))), APPS);
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "medium");
  });

  test("passes when every app is admin-approved only", () => {
    const f = one(withDoc((d) => ok<Array<{ OptionsAllowAdminApprovedUsersOnly: boolean | null }>>(d.connectedApps).forEach((a) => (a.OptionsAllowAdminApprovedUsersOnly = true))), APPS);
    assert.equal(f.status, "passed");
  });

  test("a setting that couldn't be read is never taken as safe", () => {
    const f = one(withDoc((d) => ok<Array<{ OptionsAllowAdminApprovedUsersOnly: boolean | null }>>(d.connectedApps).forEach((a) => (a.OptionsAllowAdminApprovedUsersOnly = a.OptionsAllowAdminApprovedUsersOnly ? true : null))), APPS);
    assert.equal(f.status, "not_measurable");
  });

  test("not measurable when connected apps can't be read", () => {
    assert.equal(one(withDoc((d) => void (d.connectedApps = gone as never)), APPS).status, "not_measurable");
  });
});

describe("remote sites that allow plain HTTP", () => {
  test("fires on the active site that lets code mix HTTPS and HTTP, by host only", () => {
    const f = one(output!.findings, SITES);
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "medium");
    assert.equal(f.recommendationType, "change");
    assert.match(f.summary, /api\.finance\.example\.com/);
    assert.doesNotMatch(JSON.stringify(f), /not-a-real-key|https?:\/\/|\/v2/);
    // The plain HTTP site is inactive, so it isn't a risk.
    assert.doesNotMatch(f.summary, /legacy/);
  });

  test("an active plain HTTP site is caught too", () => {
    const f = one(withDoc((d) => ok<Array<{ host: string; IsActive: boolean }>>(d.remoteSites).forEach((r) => r.host === "legacy.example.com" && (r.IsActive = true))), SITES);
    assert.match(f.summary, /1 active remote site uses plain HTTP: legacy\.example\.com/);
    assert.equal(f.facts.plain, 1);
    assert.equal(f.facts.mixed, 1);
  });

  test("passes when every active site is HTTPS and keeps to it", () => {
    const f = one(withDoc((d) => ok<Array<{ ProtocolMismatch: boolean }>>(d.remoteSites).forEach((r) => (r.ProtocolMismatch = false))), SITES);
    assert.equal(f.status, "passed");
  });

  test("not measurable when remote sites can't be read", () => {
    assert.equal(one(withDoc((d) => void (d.remoteSites = gone as never)), SITES).status, "not_measurable");
  });
});

describe("in a set-up review", async () => {
  const { output: essentials } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });

  test("both rules run and fire, from set-up alone", () => {
    // In the integrations area since roadmap 3.2 (DECISIONS.md 170).
    const integrations = modulesFor("essentials", SALES_CLOUD_MODULES).find((m) => m.id === "integrations")!;
    for (const id of [APPS, SITES]) {
      assert.ok(integrations.rules.some((r) => r.id === id), id);
      assert.equal(one(essentials!.findings, id).status, "issue", id);
    }
  });
});
