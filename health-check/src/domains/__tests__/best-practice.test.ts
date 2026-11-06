import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { PRACTICES, bestPracticeModule } from "../best-practice/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { userId } from "../../testing/synthetic-org.ts";
import type { Finding } from "../../schemas/index.ts";

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
const change = <T>(id: string, edit: (data: T) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return changed;
};
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

describe("best practice", () => {
  test("every rule states Salesforce's position, its source and when it was checked", () => {
    for (const rule of bestPracticeModule.rules) {
      const practice = PRACTICES[rule.id];
      assert.ok(practice, rule.id);
      assert.ok(practice.position && practice.source);
      assert.match(practice.checkedOn, /^\d{4}-\d{2}-\d{2}$/);
      const f = one(analyseExtracts(extracts).findings, rule.id);
      assert.ok(f.summary.includes(practice.position), rule.id);
      assert.ok(f.evidence.some((e) => e.note?.includes(practice.source)), rule.id);
    }
  });

  test("a position from secondary sources can't be high confidence, and says so", () => {
    const secondary = Object.entries(PRACTICES).filter(([, p]) => p.basis === "secondary");
    assert.ok(secondary.length > 0);
    for (const [id] of secondary) {
      const f = one(analyseExtracts(extracts).findings, id);
      assert.equal(f.confidence, "medium");
      assert.match(f.summary, /secondary sources/);
    }
  });
});

describe("users without a role", () => {
  test("names people outside the hierarchy, and leaves out admins and system accounts", () => {
    const f = one(analyseExtracts(extracts).findings, "bp.users-without-role");
    assert.deepEqual(f.subjects.map((s) => s.label).sort(), [tokenOf(8), tokenOf(13)]);
    for (const n of [4, 6, 11]) assert.ok(!f.subjects.some((s) => s.label === tokenOf(n)));
  });

  test("everyone in a role stays quiet", () => {
    const changed = change<{ data: Array<{ UserRole: unknown }> }>("users", (d) => {
      for (const u of d.data) u.UserRole ??= { Name: "Sales UK" };
    });
    assert.equal(one(analyseExtracts(changed).findings, "bp.users-without-role").status, "passed");
  });

  test("a calibrated system account isn't asked to have a role", () => {
    const calibration = loadCalibration();
    calibration.users.push({ userId: userId(8), kind: "system" }, { userId: userId(13), kind: "system" });
    assert.equal(analyseExtracts(extracts, { calibration }).findings.find((f) => f.ruleId === "bp.users-without-role")!.status, "passed");
  });

  test("unreadable users are not measurable", () => {
    const changed = clone(extracts);
    changed.set("users", gone);
    assert.equal(one(analyseExtracts(changed).findings, "bp.users-without-role").status, "not_measurable");
  });
});

describe("ownership skew", () => {
  test("a custom object over the line is found", () => {
    const f = one(analyseExtracts(extracts).findings, "bp.ownership-skew");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.largest, 12_000);
    assert.deepEqual(f.subjects.map((s) => s.label), [`${tokenOf(11)}: Invoice`]);
  });

  test("exactly 10,000 isn't over the line", () => {
    const changed = change<{ byOwner: Record<string, Record<string, number>> }>("ownership", (d) => (d.byOwner[userId(11)]!.Invoice__c = 10_000));
    assert.equal(one(analyseExtracts(changed).findings, "bp.ownership-skew").status, "passed");
  });

  test("a queue is never given a person's token", () => {
    const changed = change<{ byOwner: Record<string, Record<string, number>> }>("ownership", (d) => (d.byOwner["00G000000000001AAA"] = { Lead: 20_000 }));
    assert.ok(one(analyseExtracts(changed).findings, "bp.ownership-skew").subjects.some((s) => s.label === "A queue: Lead"));
  });

  test("a gap in the scan can't give a clean pass", () => {
    const changed = change<{ byOwner: Record<string, Record<string, number>>; incomplete: string[] }>("ownership", (d) => {
      d.byOwner[userId(11)]!.Invoice__c = 1;
      d.incomplete = ["Case"];
    });
    assert.equal(one(analyseExtracts(changed).findings, "bp.ownership-skew").status, "not_measurable");
  });
});

describe("after-save same-record updates", () => {
  test("finds the flow, and ignores before-save and inactive ones", () => {
    const f = one(analyseExtracts(extracts).findings, "bp.after-save-same-record-updates");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Opportunity_Route_Deal"]);
  });

  test("clean flows stay quiet", () => {
    const changed = change<{ data: { flows: Array<{ sameRecordUpdates: number }> } }>("metadata", (d) => {
      for (const flow of d.data.flows) flow.sameRecordUpdates = 0;
    });
    assert.equal(one(analyseExtracts(changed).findings, "bp.after-save-same-record-updates").status, "passed");
  });

  test("unreadable metadata is not measurable", () => {
    const changed = clone(extracts);
    changed.set("metadata", gone);
    assert.equal(one(analyseExtracts(changed).findings, "bp.after-save-same-record-updates").status, "not_measurable");
  });
});

describe("scheduled jobs on inactive owners", () => {
  test("finds the job, and leaves active owners alone", () => {
    const f = one(analyseExtracts(extracts).findings, "bp.jobs-on-inactive-owners");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Quarterly Export"]);
    assert.equal(f.recommendationType, "investigate");
  });

  test("no inactive owners stays quiet", () => {
    const changed = change<{ data: Array<{ OwnerId: string }> }>("scheduled-jobs", (d) => {
      for (const j of d.data) j.OwnerId = userId(4);
    });
    assert.equal(one(analyseExtracts(changed).findings, "bp.jobs-on-inactive-owners").status, "passed");
  });
});
