import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { integrationsModule } from "../integrations/index.ts";
import { securityModule } from "../security/index.ts";
import { findingId } from "../../engine/evaluate.ts";
import type { ApexData } from "../../extract/automation.ts";
import type { ApiOnlyData, AsyncJobRow } from "../../extract/integrations.ts";
import type { LicenceData } from "../../extract/users.ts";
import type { Calibration, Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, userId } from "../../testing/synthetic-org.ts";

const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const edited = <T>(id: string, edit: (data: T) => void, options: { calibration?: Calibration } = {}): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return analyseExtracts(changed, options).findings;
};
const INTEGRATION = userId(11);

describe("the checks that moved from security", () => {
  test("sit in integrations, keep their ids, and leave security", () => {
    for (const id of ["sec.personal-integration-accounts", "sec.self-authorised-connected-apps", "sec.insecure-remote-sites"]) {
      assert.ok(integrationsModule.rules.some((r) => r.id === id), id);
      assert.ok(!securityModule.rules.some((r) => r.id === id), id);
      const f = one(output!.findings, id);
      assert.equal(f.domain, "integrations");
      assert.equal(f.id, findingId(id, ""));
    }
  });

  test("never say 'integration user', which a user in the org can be named", () => {
    // The synthetic org has a user called Integration User, as real orgs often do,
    // and the scrub would turn the words into that user's token.
    for (const f of output!.findings.filter((x) => x.domain === "integrations")) assert.doesNotMatch(`${f.summary} ${f.recommendation}`, /\[\[user:/, f.ruleId);
  });
});

describe("integration accounts on full licences", () => {
  test("fires on the integration account holding a Salesforce seat, worth one seat a year while integration licences sit spare", () => {
    const f = one(output!.findings, "int.integration-user-licences");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "medium");
    assert.deepEqual(f.facts, { accounts: 1, onFull: 1, spare: 5, movable: 1, annualSaving: 1200 });
    assert.deepEqual(f.value.kind === "licence_cost" ? f.value.amount : null, 1200);
    assert.equal(f.subjects.length, 1);
  });

  test("low and not sized when there's no spare integration licence", () => {
    const findings = edited<LicenceData>("licences", (l) => (l.userLicenses as { data: Array<{ Name: string; UsedLicenses: number; TotalLicenses: number }> }).data.forEach((x) => x.Name === "Salesforce Integration" && (x.UsedLicenses = x.TotalLicenses)));
    const f = one(findings, "int.integration-user-licences");
    assert.equal(f.severity, "low");
    assert.equal(f.value.kind, "not_sized");
  });

  test("passes when the account is on the integration licence", () => {
    const changed = clone(extracts);
    for (const u of (changed.get("users") as { data: Array<{ Id: string; Profile: { UserLicense: { Name: string } } }> }).data) if (u.Id === INTEGRATION) u.Profile.UserLicense.Name = "Salesforce Integration";
    assert.equal(one(analyseExtracts(changed).findings, "int.integration-user-licences").status, "passed");
  });

  test("an API-only account counts as an integration even when the calibration doesn't name it", () => {
    const calibration = { ...loadCalibration(), users: [] };
    const findings = edited<ApiOnlyData>("api-only", (d) => {
      (d.sets as { data: unknown[] }).data = [{ Id: "0PS000000000002AAA", IsOwnedByProfile: true, Profile: { Name: "API Only" } }];
    }, { calibration });
    assert.equal(one(findings, "int.integration-user-licences").status, "issue");
  });

  test("not measurable with no calibrated integration and no API-only read", () => {
    const findings = edited<ApiOnlyData>("api-only", (d) => void (d.sets = gone as never), { calibration: { ...loadCalibration(), users: [] } });
    assert.equal(one(findings, "int.integration-user-licences").status, "not_measurable");
  });
});

describe("integration accounts that can log in like a person", () => {
  test("fires on the account whose API Only profile lacks the permission", () => {
    const f = one(output!.findings, "int.integration-users-interactive");
    assert.equal(f.status, "issue");
    // Its logins are all through the API, so it's medium rather than high.
    assert.equal(f.severity, "medium");
    assert.deepEqual(f.facts, { accounts: 1, open: 1, loggedInByHand: 0 });
  });

  test("passes once the profile has API Only User", () => {
    const findings = edited<ApiOnlyData>("api-only", (d) => void ((d.sets as { data: unknown[] }).data = [{ Id: "0PS000000000002AAA", IsOwnedByProfile: true, Profile: { Name: "API Only" } }]));
    assert.equal(one(findings, "int.integration-users-interactive").status, "passed");
  });

  test("high when the account has logged in through the browser", () => {
    const changed = clone(extracts);
    const logins = changed.get("logins") as { data: { byUser: Record<string, { successes: number }> } };
    logins.data.byUser[INTEGRATION]!.successes += 2;
    assert.equal(one(analyseExtracts(changed).findings, "int.integration-users-interactive").severity, "high");
  });

  test("not measurable when no account is calibrated as an integration's", () => {
    const findings = analyseExtracts(clone(extracts), { calibration: { ...loadCalibration(), users: [] } }).findings;
    assert.equal(one(findings, "int.integration-users-interactive").status, "not_measurable");
  });
});

describe("daily allowances", () => {
  test("fires on async Apex nearly used up, and leaves API requests out of the issue", () => {
    const f = one(output!.findings, "int.api-usage");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "medium");
    assert.deepEqual(f.subjects.map((s) => s.label), ["asynchronous Apex runs"]);
    assert.match(f.summary, /88% of its daily asynchronous Apex runs \(220,000 of 250,000\)/);
  });

  test("high past 95%, and a pass when there's room", () => {
    const at = (remaining: number): Finding => one(edited<{ data: Record<string, { Remaining: number }> }>("api-limits", (l) => void (l.data.DailyAsyncApexExecutions!.Remaining = remaining)), "int.api-usage");
    assert.equal(at(5_000).severity, "high");
    assert.equal(at(200_000).status, "passed");
  });

  test("read on connecting, before the check spends its own calls", async () => {
    const { output: busy } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport: new Simulator({ asyncApex: { Max: 250_000, Remaining: 200_000 } }) });
    const f = one(busy!.findings, "int.api-usage");
    assert.equal(f.status, "passed");
    // The simulator starts at 1,000 API calls used; the run's own calls come after.
    assert.equal(f.facts.DailyApiRequestsUsed, 1000);
  });
});

describe("background jobs that failed", () => {
  test("fires on this week's failures and the batch with errors, leaving out a batch's chunks and older failures", () => {
    const f = one(output!.findings, "int.failed-async-jobs");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.facts, { jobs: 7, failed: 3, withErrors: 1, classes: 2, days: 7 });
    assert.deepEqual(f.subjects.map((s) => s.label), ["LegacySync", "FinanceSync"]);
    assert.equal(f.severity, "medium");
  });

  test("passes when nothing failed, and says so when nothing ran", () => {
    const clean = one(edited<{ data: AsyncJobRow[] }>("async-jobs", (j) => j.data.forEach((x) => ((x.Status = "Completed"), (x.NumberOfErrors = 0)))), "int.failed-async-jobs");
    assert.equal(clean.status, "passed");
    const none = one(edited<{ data: AsyncJobRow[] }>("async-jobs", (j) => void (j.data = [])), "int.failed-async-jobs");
    assert.match(none.summary, /No asynchronous Apex ran/);
  });

  test("from a log too long to read whole, failures are a floor, not the week's totals (rule 5, night 3 review)", () => {
    const f = one(edited<{ data: AsyncJobRow[]; truncated: boolean }>("async-jobs", (j) => void (j.truncated = true)), "int.failed-async-jobs");
    assert.equal(f.status, "issue");
    assert.match(f.summary, /^At least 3 asynchronous Apex jobs failed in the last 7 days/);
    assert.doesNotMatch(f.summary, /3 of 7 asynchronous Apex jobs/);
    assert.ok(f.evidence.some((e) => e.exclusions.some((x) => /too long to read in full/.test(x))));
  });

  test("a log too long to read whole is never a pass", () => {
    const f = one(edited<{ data: AsyncJobRow[]; truncated: boolean }>("async-jobs", (j) => ((j.truncated = true), j.data.forEach((x) => ((x.Status = "Completed"), (x.NumberOfErrors = 0))))), "int.failed-async-jobs");
    assert.equal(f.status, "not_measurable");
  });
});

describe("callouts to addresses written into code", () => {
  test("fires on the class with a literal address, not the one using a named credential or the address in a comment", () => {
    const f = one(output!.findings, "int.hardcoded-endpoints");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["LegacySync"]);
    assert.deepEqual(f.facts, { classes: 4, callouts: 2, literal: 1, addresses: 1 });
  });

  test("passes when every callout uses a named credential", () => {
    const findings = edited<ApexData>("apex", (a) => (a.classes as { data: Array<{ scan: { literalEndpoints: number } | null }> }).data.forEach((c) => c.scan && (c.scan.literalEndpoints = 0)));
    assert.equal(one(findings, "int.hardcoded-endpoints").status, "passed");
  });

  test("a literal address in a test class doesn't count (night 3 review: none had one before)", () => {
    const findings = edited<ApexData>("apex", (a) => {
      const classes = (a.classes as { data: Array<{ isTest: boolean; scan: { literalEndpoints: number } | null }> }).data;
      classes.forEach((c) => c.scan && (c.scan.literalEndpoints = 0));
      classes.find((c) => c.isTest && c.scan)!.scan!.literalEndpoints = 2;
    });
    assert.equal(one(findings, "int.hardcoded-endpoints").status, "passed");
  });
});
