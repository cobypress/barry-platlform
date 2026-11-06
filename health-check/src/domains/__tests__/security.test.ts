import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { HEALTH_RISKS, sfId, userId } from "../../testing/synthetic-org.ts";
import type { Finding } from "../../schemas/index.ts";
import { API_ONLY_ACCOUNT } from "../integrations/index.ts";

let extracts: Map<string, unknown>;
const tokenOf = (n: number): string => `[[user:U${String(n).padStart(4, "0")}]]`;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const status = (changed: Map<string, unknown>, ruleId: string): string => one(analyseExtracts(changed).findings, ruleId).status;
const change = <T>(id: string, edit: (data: T) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return changed;
};
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

describe("health check", () => {
  test("a low score is an issue, and says the target is ours", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.health-check-score");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.score, 62);
    assert.match(f.summary, /our judgement/);
  });

  test("a good score passes, showing the settings behind it", () => {
    const f = one(analyseExtracts(change<{ score: { data: Array<{ Score: string }> } }>("security-health-check", (d) => (d.score.data[0]!.Score = "91"))).findings, "sec.health-check-score");
    assert.equal(f.status, "passed");
    assert.ok(f.segmentsChecked.length > 0);
  });

  test("a good score can't pass if its settings can't be read", () => {
    const changed = change<{ score: { data: Array<{ Score: string }> }; risks: unknown }>("security-health-check", (d) => {
      d.score.data[0]!.Score = "91";
      d.risks = gone;
    });
    assert.equal(status(changed, "sec.health-check-score"), "not_measurable");
  });

  for (const blank of [null, "", "  "]) {
    test(`a blank score (${JSON.stringify(blank)}) is never read as zero`, () => {
      assert.equal(status(change<{ score: { data: Array<{ Score: string | null }> } }>("security-health-check", (d) => (d.score.data[0]!.Score = blank)), "sec.health-check-score"), "not_measurable");
    });
  }

  test("an unreadable health check is not measurable", () => {
    const changed = clone(extracts);
    changed.set("security-health-check", { score: gone, risks: gone });
    assert.equal(status(changed, "sec.health-check-score"), "not_measurable");
    assert.equal(status(changed, "sec.health-check-risks"), "not_measurable");
  });

  test("risky settings count high and medium, and show yours against the standard", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.health-check-risks");
    assert.equal(f.facts.highRisk, 2);
    assert.equal(f.facts.mediumRisk, 1);
    assert.equal(f.severity, "high");
    assert.ok(f.subjects.some((s) => s.label === "Password Policies: Minimum password length (yours: 8 characters, standard: 12 characters)"));
  });

  test("every setting at standard stays quiet", () => {
    const changed = change<{ risks: { data: Array<{ RiskType: string }> } }>("security-health-check", (d) => {
      for (const r of d.risks.data) r.RiskType = "MEETS_STANDARD";
    });
    assert.equal(status(changed, "sec.health-check-risks"), "passed");
  });

  test("an empty list of settings isn't a clean bill of health", () => {
    assert.equal(status(change<{ risks: { data: unknown[] } }>("security-health-check", (d) => (d.risks.data = [])), "sec.health-check-risks"), "not_measurable");
  });

  test("the extractor redacts a secret that turns up in a setting value", async () => {
    const { securityHealthExtractor } = await import("../../extract/security.ts");
    const ctx = {
      client: { query: async (soql: string) => ({ records: /Risks/.test(soql) ? [{ ...HEALTH_RISKS[0], OrgValue: 'password = "hunter2hunter2"' }] : [{ Score: "62" }], totalSize: 1, truncated: false }) },
    } as never;
    const out = await securityHealthExtractor.run(ctx);
    assert.ok(out.risks.status === "ok");
    assert.doesNotMatch(JSON.stringify(out.risks.data), /hunter2/);
  });
});

describe("privileged access", () => {
  test("a rep holding Modify All Data through a permission set is found", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.modify-all-data");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(2)]);
    assert.equal(f.facts.unexpected, 1);
    assert.equal(f.facts.holders, 3);
  });

  test("an inactive holder doesn't count", () => {
    assert.ok(!one(analyseExtracts(extracts).findings, "sec.modify-all-data").subjects.some((s) => s.label === tokenOf(9)));
  });

  test("view all data catches its own grant", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.view-all-data");
    assert.deepEqual(f.subjects.map((s) => s.label).sort(), [tokenOf(1), tokenOf(2)]);
  });

  test("admins and calibrated system accounts alone stay quiet", () => {
    const changed = change<{ assignments: { data: Array<{ PermissionSetId: string }> } }>("privileged-permissions", (d) => {
      d.assignments.data = d.assignments.data.filter((a) => a.PermissionSetId !== sfId("0PS", 3) && a.PermissionSetId !== sfId("0PS", 4));
    });
    assert.equal(status(changed, "sec.modify-all-data"), "passed");
    assert.equal(status(changed, "sec.view-all-data"), "passed");
  });

  test("a calibrated admin isn't flagged, whatever their profile", () => {
    const calibration = loadCalibration();
    calibration.users.push({ userId: userId(2), kind: "admin" });
    const f = analyseExtracts(extracts, { calibration }).findings.find((x) => x.ruleId === "sec.modify-all-data")!;
    assert.equal(f.facts.unexpected, 0);
  });

  test("a shared login with an admin profile is flagged, because the calibration says it isn't an admin", () => {
    const changed = clone(extracts);
    const users = changed.get("users") as { data: Array<{ Id: string; Profile: { Name: string } }> };
    users.data.find((u) => u.Id === userId(6))!.Profile.Name = "System Administrator";
    assert.ok(one(analyseExtracts(changed).findings, "sec.modify-all-data").subjects.some((s) => s.label === tokenOf(6)));
  });

  test("a large admin team is an issue even when everyone is expected", () => {
    const changed = clone(extracts);
    const users = changed.get("users") as { data: Array<{ Profile: { Name: string } }> };
    for (const u of users.data.slice(0, 5)) u.Profile.Name = "System Administrator";
    (changed.get("privileged-permissions") as { assignments: { data: unknown[] } }).assignments.data = [];
    const f = one(analyseExtracts(changed).findings, "sec.modify-all-data");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.unexpected, 0);
  });

  test("unreadable assignments are not measurable, not nobody", () => {
    assert.equal(status(change<{ assignments: unknown }>("privileged-permissions", (d) => (d.assignments = gone)), "sec.modify-all-data"), "not_measurable");
  });
});

describe("sharing defaults", () => {
  test("public read/write objects are named, at medium confidence", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.sharing-defaults");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Opportunity", "Lead"]);
    assert.equal(f.confidence, "medium");
    assert.equal(f.recommendationType, "investigate");
  });

  test("private and read-only defaults stay quiet", () => {
    const changed = change<{ data: Array<Record<string, string>> }>("org", (d) => {
      d.data[0]!.DefaultOpportunityAccess = "None";
      d.data[0]!.DefaultLeadAccess = "Read";
    });
    assert.equal(status(changed, "sec.sharing-defaults"), "passed");
  });

  test("no sharing fields is not measurable", () => {
    const changed = change<{ data: Array<Record<string, unknown>> }>("org", (d) => {
      for (const key of Object.keys(d.data[0]!)) if (key.startsWith("Default")) delete d.data[0]![key];
    });
    assert.equal(status(changed, "sec.sharing-defaults"), "not_measurable");
  });
});

describe("personal accounts used for integrations", () => {
  test("a rep's API logins are found, and a calibrated integration user isn't", () => {
    const f = one(analyseExtracts(extracts).findings, "sec.personal-integration-accounts");
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(14)]);
    assert.equal(f.facts.apiLogins, 6);
    assert.match(f.summary, /Partner Portal Sync/);
    assert.equal(f.confidence, "medium");
  });

  test("one data load by an admin stays quiet", () => {
    assert.ok(!one(analyseExtracts(extracts).findings, "sec.personal-integration-accounts").subjects.some((s) => s.label === tokenOf(4)));
  });

  test("a calibrated integration account stays quiet", () => {
    const calibration = loadCalibration();
    calibration.users.push({ userId: userId(14), kind: "integration" });
    const f = analyseExtracts(extracts, { calibration }).findings.find((x) => x.ruleId === "sec.personal-integration-accounts")!;
    assert.equal(f.status, "passed");
  });

  test("an account set to API only is an integration account here as on the licence check, described the same way (DECISIONS.md 323)", () => {
    const changed = change<{ sets: { data: unknown[] }; assignments: { data: unknown[] } }>("api-only", (d) => {
      d.sets.data.push({ Id: "0PS000000000099AAA", IsOwnedByProfile: false, Profile: null });
      d.assignments.data.push({ AssigneeId: userId(14), PermissionSetId: "0PS000000000099AAA" });
    });
    const findings = analyseExtracts(changed).findings;
    const personal = one(findings, "sec.personal-integration-accounts");
    assert.equal(personal.status, "passed", "an API-only account isn't a personal one");
    const licences = one(findings, "int.integration-user-licences");
    const before = one(analyseExtracts(extracts).findings, "int.integration-user-licences");
    assert.equal(licences.facts.accounts, (before.facts.accounts ?? 0) + 1, "the licence check counts it as an integration account");
    const notes = (f: Finding): string => f.evidence.map((e) => [e.note ?? "", ...(e.exclusions ?? [])].join(" ")).join(" ");
    for (const f of [personal, licences]) assert.match(notes(f), new RegExp(API_ONLY_ACCOUNT.replace(/[()]/g, "\\$&")), f.ruleId);
  });

  test("a truncated login log can't give a clean pass", () => {
    const changed = clone(extracts);
    const logins = changed.get("logins") as { truncated: boolean; data: { apiByUser: Record<string, unknown> } };
    logins.truncated = true;
    logins.data.apiByUser = {};
    assert.equal(status(changed, "sec.personal-integration-accounts"), "not_measurable");
  });

  test("failed API logins aren't logins", async () => {
    const { loginsExtractor } = await import("../../extract/users.ts");
    const row = { UserId: userId(1), LoginTime: "2026-10-30T00:00:00.000+0000", Status: "Invalid Password", ApiType: "SOAP Enterprise", Application: "X" };
    // No describe result means the query asks for every field, as before describes.
    const ctx = { now: new Date("2026-10-31T00:00:00Z"), get: () => ({}), client: { query: async () => ({ records: [row], totalSize: 1, truncated: false }) } } as never;
    const out = await loginsExtractor.run(ctx);
    assert.ok(out.status === "ok");
    assert.deepEqual(out.data.apiByUser, {});
  });
});
