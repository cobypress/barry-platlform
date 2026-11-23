import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { AS_OF, ORG_ID, STAGES } from "../../testing/synthetic-org.ts";
import { automaticCalibration, CALIBRATION_UNREADABLE, NO_WON_STAGE, ORG_SETTINGS_CALIBRATOR, STAGES_UNREAD, CURRENCY_UNREAD, CALIBRATION_REFUSED } from "../auto-calibration.ts";
import { orgStages, type OrgStage } from "../org-stages.ts";
import { calibrationConfirmation, type Calibration, type Finding, type Metric } from "../../schemas/index.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { buildReport, currentBody } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import type { OpportunityExtract } from "../../extract/opportunities.ts";

// An automatic run's calibration, from the org's stages alone (DECISIONS.md 692).

const synthetic = (): OrgStage[] => orgStages(clone(STAGES));
const build = (stages: OrgStage[] | null): Calibration => {
  const built = automaticCalibration(stages, { orgId: ORG_ID, at: AS_OF, currency: "GBP" });
  assert.ok(built.ok, "a calibration is built");
  return built.calibration;
};
const stage = (apiName: string, sortOrder: number | null, flags: Partial<OrgStage> = {}): OrgStage => ({ apiName, label: apiName, isActive: true, isClosed: false, isWon: false, sortOrder, ...flags });

describe("the calibration built from the synthetic org's stages", () => {
  test("names won stages won and active open stages open in Salesforce's sort order; lost, no decision and inactive stages are left to the run", () => {
    const c = build(synthetic());
    assert.deepEqual(c.stages, [
      { name: "Prospecting", category: "open", order: 1 },
      { name: "Discovery", category: "open", order: 2 },
      { name: "Proposal", category: "open", order: 3 },
      { name: "Negotiation", category: "open", order: 4 },
      { name: "Legal Review", category: "open", order: 5 },
      { name: "Closed Won", category: "won" },
    ]);
    // Closed and not won, Closed No Decision among them, is never named: the run reads it as lost from the flags (598, 600), never no decision.
    assert.ok(!c.stages.some((s) => s.category === "lost" || s.category === "no_decision" || s.category === "excluded"));
    // The inactive open stage has no place in the order Salesforce's list could give.
    assert.ok(!c.stages.some((s) => s.name === "Qualified"));
  });

  test("everything else is the design's conservative default, and nothing is guessed", () => {
    const c = build(synthetic());
    const { stages: _s, ...rest } = c;
    assert.deepEqual(rest, {
      version: 1,
      orgId: ORG_ID,
      confirmedBy: ORG_SETTINGS_CALIBRATOR,
      confirmedAt: AS_OF,
      source: "org_settings",
      scope: { recordTypes: "all", businessLines: [] },
      users: [],
      fields: {},
      targets: { source: "none" },
      analysisWindowMonths: 24,
      minSample: 20,
      dormantDays: 90,
      currency: "GBP",
      licenceCosts: {},
      effortMinutes: {},
    });
    assert.equal(c.fiscalYearStartMonth, undefined, "the org's own fiscal year");
    assert.equal(c.salesCycleAssumptionDays, undefined);
  });

  test("the order is Salesforce's sort order whatever order the stages arrive in, ties by API name, and a stage with no sort order is left out", () => {
    const c = build([stage("Won", 9, { isClosed: true, isWon: true }), stage("C", 3), stage("B", 1), stage("A", 3), stage("Unsorted", null)]);
    assert.deepEqual(c.stages.filter((s) => s.category === "open").map((s) => [s.name, s.order]), [["B", 1], ["A", 2], ["C", 3]]);
  });

  test("every won stage is named, inactive ones too; a won flag on an open stage counts for nothing", () => {
    const c = build([stage("Old Won", 7, { isActive: false, isClosed: true, isWon: true }), stage("Won", 6, { isClosed: true, isWon: true }), stage("Odd", 1, { isWon: true })]);
    assert.deepEqual(c.stages.map((s) => [s.name, s.category]), [["Old Won", "won"], ["Won", "won"]]);
  });

  test("nothing prints the system label as a person", () => {
    const c = build(synthetic());
    assert.ok(!calibrationConfirmation(c).includes(ORG_SETTINGS_CALIBRATOR));
    assert.match(calibrationConfirmation(c), /read from the org's own settings on 2026-10-31, with no person's confirmation/);
  });
});

describe("no calibration", () => {
  test("an org with no won stage gets none, with the word for the run's hold", () => {
    const stages = synthetic().map((s) => ({ ...s, isWon: false }));
    assert.deepEqual(automaticCalibration(stages, { orgId: ORG_ID, at: AS_OF, currency: "GBP" }), { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: NO_WON_STAGE });
    assert.deepEqual(automaticCalibration([], { orgId: ORG_ID, at: AS_OF, currency: "GBP" }), { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: NO_WON_STAGE });
  });

  test("a failed stage read gets none", () => {
    assert.deepEqual(automaticCalibration(null, { orgId: ORG_ID, at: AS_OF, currency: "GBP" }), { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: STAGES_UNREAD });
  });

  test("no currency read gets none, never the schema's pounds; the org's own currency is kept as read (DECISIONS.md 693)", () => {
    assert.deepEqual(automaticCalibration(synthetic(), { orgId: ORG_ID, at: AS_OF, currency: null }), { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: CURRENCY_UNREAD });
    const euros = automaticCalibration(synthetic(), { orgId: ORG_ID, at: AS_OF, currency: "EUR" });
    assert.ok(euros.ok);
    assert.equal(euros.ok && euros.calibration.currency, "EUR");
  });

  test("a calibration the schema refuses is a hold, never a throw (DECISIONS.md 693)", () => {
    // A currency code the schema can't hold stands in for anything it refuses.
    assert.deepEqual(automaticCalibration(synthetic(), { orgId: ORG_ID, at: AS_OF, currency: "EURO" }), { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: CALIBRATION_REFUSED });
  });

  test("neither reason names a stage", () => {
    for (const s of STAGES) for (const reason of [NO_WON_STAGE, STAGES_UNREAD, CURRENCY_UNREAD, CALIBRATION_REFUSED]) assert.ok(!reason.includes(s.ApiName));
  });
});

/**
 * Every check that isn't measurable under the automatic calibration and is
 * under the fixture's, with the same answers, over the recorded synthetic org.
 * This is the list the portal's "answer to unlock" is built from.
 */
const UNMEASURED_FINDINGS = [
  "act.deals-without-activity",
  "act.won-vs-lost-activity",
  "cur.coverage",
  "dq.loss-reason-missing",
  "fct.early-stage-commits",
  "int.integration-users-interactive",
  "pipe.dead-end-stage",
  "pipe.going-backwards",
  "pipe.skipped-stages",
  "rev.win-backs",
];
const UNMEASURED_METRICS = [
  "activity.per_lost_deal",
  "forecast.commit_against_quota",
  "historic.lost_by_stage",
  "historic.lost_value_by_stage",
  "historic.no_decision_rate",
  "historic.path_to_lost",
  "historic.path_to_won",
  "historic.quarterly_win_rate",
  "historic.sales_cycle_lost",
  "historic.stage_conversion",
  "historic.stage_transitions",
  "historic.went_backwards",
  "historic.win_drivers",
  "historic.win_rate",
  "historic.win_rate_by_value",
  "historic.won_skipped_stage",
  "investment.unused_seat_cost",
];
const ORDER_NOTE = "We took the order of your open stages from Salesforce's own stage settings, and haven't confirmed it with you.";
const BLEND_NOTE = /^Your deals use \d+ record types, which may be separate sales processes\. We've read them together, so these figures blend them\.$/;

describe("what the synthetic org can't measure under it", () => {
  let extracts: Map<string, unknown>;
  let fixture: ReturnType<typeof analyseExtracts>;
  let automatic: ReturnType<typeof analyseExtracts>;
  before(async () => {
    extracts = loadExtracts((await runFromRecording({ answers: loadAnswers() })).dir);
    fixture = analyseExtracts(extracts, { answers: loadAnswers() });
    automatic = analyseExtracts(extracts, { answers: loadAnswers(), calibration: build(synthetic()) });
  });
  const unmeasured = (o: { findings: Finding[] }) => new Set(o.findings.filter((f) => f.status === "not_measurable").map((f) => f.ruleId));
  const unmeasuredMetrics = (o: { metrics: Metric[] }) => new Set(o.metrics.filter((m) => m.status === "not_measurable").map((m) => m.id));

  test("is exactly the list, each with its plain reason, and nothing measured before is lost otherwise", () => {
    const before = unmeasured(fixture);
    const lost = automatic.findings.filter((f) => f.status === "not_measurable" && !before.has(f.ruleId));
    assert.deepEqual([...new Set(lost.map((f) => f.ruleId))].sort(), UNMEASURED_FINDINGS);
    for (const f of lost) assert.match(f.notMeasurableReason!, /^(Salesforce's setting can't tell lost from no decision for: .+\. The calibration can say which each is\.|The calibration doesn't say where these open stages come in the sales order: Qualified\.|No targets were given in the calibration\. Tell us your targets to see pipeline coverage\.|No account is calibrated as an integration account, so we can't tell which accounts integrations run on\.)$/, f.ruleId);
    const beforeMetrics = unmeasuredMetrics(fixture);
    assert.deepEqual([...unmeasuredMetrics(automatic)].filter((id) => !beforeMetrics.has(id)).sort(), UNMEASURED_METRICS);
    // Nothing the fixture's calibration couldn't measure is measured from a guess here.
    for (const id of before) assert.ok(unmeasured(automatic).has(id), id);
  });

  test("a measured figure that read the deals says the record types were read together", () => {
    const blended = automatic.metrics.filter((m) => m.status === "measured" && m.assumptions.some((a) => BLEND_NOTE.test(a)));
    assert.ok(blended.length > 10, `${blended.length}`);
    assert.ok(automatic.metrics.find((m) => m.id === "historic.stage_win_rate")?.status === "measured");
    // A consultant's calibration says nothing of the kind.
    assert.ok(!fixture.metrics.some((m) => m.status === "measured" && m.assumptions.some((a) => BLEND_NOTE.test(a) || a === ORDER_NOTE)));
    assert.ok(!fixture.findings.some((f) => f.summary.includes(ORDER_NOTE)));
  });

  test("with every open stage ordered, the checks that read the order are measured, say the order is unconfirmed, and are no surer than medium", () => {
    // Qualified made active and early, so no deal sits in an unordered stage.
    const stages = synthetic().map((s) => (s.apiName === "Qualified" ? { ...s, isActive: true, sortOrder: 1.5 } : s));
    const c = build(orgStages(stages.map((s) => ({ ApiName: s.apiName, MasterLabel: s.label, IsActive: s.isActive, IsClosed: s.isClosed, IsWon: s.isWon, SortOrder: s.sortOrder }))));
    assert.deepEqual(c.stages.slice(0, 3).map((s) => s.name), ["Prospecting", "Qualified", "Discovery"]);
    const ordered = analyseExtracts(extracts, { answers: loadAnswers(), calibration: c });
    const orderChecks = ["pipe.skipped-stages", "pipe.going-backwards", "fct.early-stage-commits"];
    for (const ruleId of orderChecks) {
      const findings = ordered.findings.filter((f) => f.ruleId === ruleId);
      assert.ok(findings.length > 0 && findings.every((f) => f.status !== "not_measurable"), `${ruleId} is measured`);
      for (const f of findings) {
        assert.ok(f.summary.includes(ORDER_NOTE), `${ruleId} says the order is unconfirmed`);
        assert.notEqual(f.confidence, "high", ruleId);
      }
    }
    for (const id of ["historic.stage_conversion", "historic.stage_transitions"]) {
      const m = ordered.metrics.find((x) => x.id === id)!;
      assert.equal(m.status, "measured", id);
      assert.ok(m.status === "measured" && m.assumptions.includes(ORDER_NOTE), id);
    }
    // The same calibration said to be a person's: no such words.
    const confirmed = analyseExtracts(extracts, { answers: loadAnswers(), calibration: { ...c, source: "confirmed" } });
    assert.ok(!confirmed.findings.some((f) => f.summary.includes(ORDER_NOTE)));
  });

  test("one record type among the deals says nothing of blending", () => {
    const one = clone(extracts);
    const opportunities = one.get("opportunities") as OpportunityExtract & { opportunities: { data: Array<{ RecordType: string | null }> } };
    for (const o of opportunities.opportunities.data) o.RecordType = "New_Business";
    const out = analyseExtracts(one, { answers: loadAnswers(), calibration: build(synthetic()) });
    assert.ok(!out.metrics.some((m) => m.status === "measured" && m.assumptions.some((a) => BLEND_NOTE.test(a))));
  });
});

describe("the documents of a run with it", () => {
  test("the raw notes and the assumptions section say it was read from the org, and never print the system label", async () => {
    const loaded = loadRun((await runFromRecording({ answers: loadAnswers() })).dir);
    const calibration = build(synthetic());
    const notes = renderRawNotes({ ...loaded.input, calibration, pseudonyms: loaded.pseudonyms });
    assert.ok(!notes.includes(ORG_SETTINGS_CALIBRATOR));
    const report = buildReport({ ...loaded.input, calibration });
    const body = currentBody(report.outputs.raw_notes, "assumptions");
    assert.ok(body.includes("with no person's confirmation") && !body.includes(ORG_SETTINGS_CALIBRATOR));
  });
});
