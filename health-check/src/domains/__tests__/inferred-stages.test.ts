import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { AS_OF, CONNECTED_USER, STAGES, UNCALIBRATED_STAGES } from "../../testing/synthetic-org.ts";
import { inferredMeanings, STAGE_MEANINGS_FILE, stageMap } from "../historic-pipeline/cohort.ts";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { Simulator } from "../../testing/simulator.ts";
import { loadRun } from "../../render/render-run.ts";
import { flowStages } from "../../render/full-report.ts";
import { processFlowChart } from "../../render/charts.ts";
import type { OpportunityExtract } from "../../extract/opportunities.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";

// Stages the calibration doesn't name take Salesforce's own flags, and a stage
// no deal in scope uses changes nothing (DECISIONS.md 598). The trap is in
// synthetic-org.ts: four stages left out, two open, one won and one used only
// by deals outside the calibrated scope.

const AREAS = ["current_pipeline", "forecast", "revenue_opportunities", "data_quality"];
const NOTE = "We haven't yet agreed with you what some stages mean, so we used Salesforce's own setting for each: Closed Won as won, Discovery as open and Negotiation as open.";
const STAGE_GAP = /The calibration doesn't say (what|whether) these stages/;
const ORDER_GAP = "The calibration doesn't say where these open stages come in the sales order: Discovery, Negotiation.";
const gone = { status: "unavailable", reason: "INVALID_TYPE: no", source: { kind: "soql", api: "rest", query: "x" } };

let extracts: Map<string, unknown>;
before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

/** The recorded run with the out-of-scope record type's open deals moved into the trap's unused stage. */
function withOutOfScopeStage(): { changed: Map<string, unknown>; moved: number } {
  const changed = clone(extracts);
  const opportunities = changed.get("opportunities") as OpportunityExtract & { opportunities: { data: any[] }; history: { data: any[] } };
  let moved = 0;
  for (const o of opportunities.opportunities.data) {
    if (o.RecordType !== UNCALIBRATED_STAGES.outOfScopeType || o.IsClosed) continue;
    o.StageName = UNCALIBRATED_STAGES.outOfScope;
    moved++;
    opportunities.history.data.push({ Id: `008${String(900_000 + moved).padStart(12, "0")}`, OpportunityId: o.Id, StageName: UNCALIBRATED_STAGES.outOfScope, Amount: o.Amount, CloseDate: o.CloseDate, CreatedDate: "2026-10-01T00:00:00.000+0000" });
  }
  return { changed, moved };
}

const scoped = (leaveOut: readonly string[]): Calibration => {
  const c = loadCalibration();
  c.scope.recordTypes = [...UNCALIBRATED_STAGES.scope];
  c.stages = c.stages.filter((s) => !leaveOut.includes(s.name));
  return c;
};
const TRAP = [...UNCALIBRATED_STAGES.open, UNCALIBRATED_STAGES.won, UNCALIBRATED_STAGES.outOfScope];
const inAreas = (findings: Finding[]): Finding[] => findings.filter((f) => AREAS.includes(f.domain));
const keyOf = (f: Finding): string => `${f.ruleId}|${f.id}`;
const metricOf = (metrics: Metric[], id: string): Metric => metrics.find((m) => m.id === id)!;

describe("a calibration that leaves out four stages (DECISIONS.md 598)", () => {
  let trap: ReturnType<typeof analyseExtracts>;
  let named: ReturnType<typeof analyseExtracts>;
  before(() => {
    const { changed, moved } = withOutOfScopeStage();
    assert.ok(moved > 0, "the trap needs open deals outside the scope");
    trap = analyseExtracts(changed, { calibration: scoped(TRAP) });
    named = analyseExtracts(changed, { calibration: scoped([]) });
  });

  test("current pipeline, forecast, revenue opportunities and data quality are measured as when every stage is named", () => {
    assert.deepEqual(inAreas(trap.findings).filter((f) => f.status === "not_measurable" && STAGE_GAP.test(f.notMeasurableReason!)).map((f) => [f.ruleId, f.notMeasurableReason]), []);
    // Only a check that needs the sales order is lost, and it says why.
    const before = new Set(inAreas(named.findings).filter((f) => f.status === "not_measurable").map(keyOf));
    const lost = inAreas(trap.findings).filter((f) => f.status === "not_measurable" && !before.has(keyOf(f)));
    assert.deepEqual(lost.map((f) => [f.ruleId, f.notMeasurableReason]), [["fct.early-stage-commits", ORDER_GAP]]);
    for (const area of AREAS) assert.ok(inAreas(trap.findings).some((f) => f.domain === area && f.status !== "not_measurable"), area);
  });

  test("each finding that read the deals says which stages took Salesforce's setting, at no more than medium confidence", () => {
    const base = new Map(named.findings.map((f) => [keyOf(f), f]));
    let noted = 0;
    for (const f of inAreas(trap.findings).filter((x) => x.status !== "not_measurable")) {
      const was = base.get(keyOf(f))!;
      assert.ok(was, keyOf(f));
      if (f.summary === was.summary) continue;
      // The figures are the same: the stages mean what the calibration would have said.
      assert.equal(f.summary, `${was.summary} ${NOTE}`, f.ruleId);
      assert.notEqual(f.confidence, "high", f.ruleId);
      noted++;
    }
    for (const ruleId of ["cur.stale-deals", "cur.coverage", "rev.stalled-deals", "fct.quota-gaps", "dq.missing-amounts"]) {
      const f = trap.findings.find((x) => x.ruleId === ruleId);
      if (!f || f.status === "not_measurable") continue;
      assert.ok(f.summary.endsWith(NOTE), ruleId);
      assert.notEqual(f.confidence, "high", ruleId);
    }
    assert.ok(noted >= 10, `${noted} findings carry the note`);
    // The stage only out-of-scope deals use is named nowhere.
    assert.ok(!trap.findings.some((f) => f.summary.includes(`${UNCALIBRATED_STAGES.outOfScope} as`)));
  });

  test("a measured metric carries the same note among its assumptions, with the same figure", () => {
    for (const id of ["historic.win_rate", "current.open_pipeline"]) {
      const m = metricOf(trap.metrics, id);
      const was = metricOf(named.metrics, id);
      assert.equal(m.status, "measured", id);
      assert.ok(m.status === "measured" && was.status === "measured");
      assert.equal(m.value, was.value, id);
      assert.ok(m.assumptions.includes(NOTE), id);
      assert.ok(!was.assumptions.includes(NOTE), id);
    }
    assert.equal(metricOf(trap.metrics, "historic.stage_conversion").status, "not_measurable");
  });

  test("pipe.unmapped-stages still reports the gap, and says what we did about it", () => {
    const f = trap.findings.find((x) => x.ruleId === "pipe.unmapped-stages")!;
    assert.equal(f.status, "issue");
    for (const stage of TRAP) assert.match(f.summary, new RegExp(stage));
    assert.match(f.summary, /Salesforce's own setting/);
  });

  test("a stage used only outside the scope makes nothing not measurable, even with no flags to read", () => {
    const { changed } = withOutOfScopeStage();
    changed.set("opportunity-stages", gone);
    const out = analyseExtracts(changed, { calibration: scoped([UNCALIBRATED_STAGES.outOfScope]) });
    assert.deepEqual(out.findings.filter((f) => f.status === "not_measurable" && /Legal Review/.test(f.notMeasurableReason ?? "")).map((f) => f.ruleId), []);
    assert.equal(metricOf(out.metrics, "historic.stage_conversion").status, "measured");
  });
});

describe("where Salesforce's stage flags can't be read (DECISIONS.md 598)", () => {
  test("a stage the calibration leaves out stays not measurable, with the reason, and nothing is inferred", () => {
    const changed = clone(extracts);
    changed.set("opportunity-stages", gone);
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed Won");
    const out = analyseExtracts(changed, { calibration: c });
    for (const ruleId of ["cur.stale-deals", "rev.stalled-deals"]) {
      const f = out.findings.find((x) => x.ruleId === ruleId)!;
      assert.equal(f.status, "not_measurable", ruleId);
      assert.match(f.notMeasurableReason!, /Closed Won/, ruleId);
    }
    assert.ok(!out.findings.some((f) => f.summary.includes("Salesforce's own setting for each")));
  });
});

describe("the calibration's word wins over Salesforce's flags (DECISIONS.md 598)", () => {
  test("a stage the calibration names keeps its category, whatever the flags say", () => {
    const c = loadCalibration();
    // Salesforce flags Closed No Decision as closed and not won, which would infer lost.
    const map = stageMap(c, STAGES);
    assert.equal(map.categoryOf("Closed No Decision"), "no_decision");
    assert.equal(map.isInferred("Closed No Decision"), false);
    c.stages = c.stages.filter((s) => s.name !== "Closed No Decision");
    const inferred = stageMap(c, STAGES);
    assert.equal(inferred.categoryOf("Closed No Decision"), "lost");
    assert.equal(inferred.isInferred("Closed No Decision"), true);
    // Flags never give a place in the sales order.
    assert.equal(stageMap(scoped(TRAP), STAGES).orderOf("Discovery"), null);
  });

  test("won and lost follow IsWon, and open follows IsClosed", () => {
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => !["Closed Won", "Closed Lost", "Discovery"].includes(s.name));
    const map = stageMap(c, STAGES);
    assert.deepEqual(["Closed Won", "Closed Lost", "Discovery"].map((s) => map.categoryOf(s)), ["won", "lost", "open"]);
    assert.equal(stageMap(c, null).categoryOf("Closed Won"), null);
  });
});

// Salesforce's flags can't tell lost from no decision, so nothing that splits
// the two is measured from an inferred lost stage (DECISIONS.md 600).
describe("a stage inferred as lost (DECISIONS.md 600)", () => {
  const UNSPLIT = "Salesforce's setting can't tell lost from no decision for: Closed No Decision. The calibration can say which each is.";
  // Every figure and check whose definition leaves no decision out or reports it on its own.
  const METRICS = ["historic.win_rate", "historic.win_rate_by_value", "historic.no_decision_rate", "historic.quarterly_win_rate", "historic.sales_cycle_lost", "historic.lost_by_stage", "historic.lost_value_by_stage", "historic.went_backwards", "historic.win_drivers", "activity.per_lost_deal"];
  const RULES = ["act.deals-without-activity", "act.won-vs-lost-activity", "dq.loss-reason-missing", "rev.win-backs", "cpq.discount-leakage", "cpq.quote-to-order"];
  let named: ReturnType<typeof analyseExtracts>;
  let flagged: ReturnType<typeof analyseExtracts>;
  before(async () => {
    const all = loadExtracts((await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir);
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed No Decision");
    named = analyseExtracts(all, { modules: ALL_MODULES, answers: loadAnswers() });
    flagged = analyseExtracts(all, { modules: ALL_MODULES, answers: loadAnswers(), calibration: c });
  });

  test("is never reported as a no decision rate, or a win rate that leaves no decision out", () => {
    for (const id of METRICS) {
      assert.equal(metricOf(named.metrics, id)?.status, "measured", `${id} is measured when every stage is named`);
      const m = metricOf(flagged.metrics, id);
      assert.equal(m.status, "not_measurable", id);
      assert.equal(m.status === "not_measurable" ? m.reason : "", UNSPLIT, id);
    }
  });

  test("the checks that leave no decision out, or report it, say why they can't run", () => {
    for (const ruleId of RULES) {
      const was = named.findings.filter((f) => f.ruleId === ruleId);
      assert.ok(was.length > 0 && was.every((f) => f.status !== "not_measurable"), `${ruleId} runs when every stage is named`);
      const now = flagged.findings.filter((f) => f.ruleId === ruleId);
      assert.ok(now.length > 0, ruleId);
      for (const f of now) {
        assert.equal(f.status, "not_measurable", ruleId);
        assert.equal(f.notMeasurableReason, UNSPLIT, ruleId);
      }
    }
  });

  test("the win rate including no decision stays measured, with the same figure, and says why", () => {
    const a = metricOf(named.metrics, "historic.win_rate_including_no_decision");
    const b = metricOf(flagged.metrics, "historic.win_rate_including_no_decision");
    assert.ok(a.status === "measured" && b.status === "measured");
    assert.equal(b.value, a.value);
    assert.equal(b.n, a.n);
    assert.ok(b.assumptions.some((x) => x.includes("Closed No Decision as lost")));
  });

  test("an inferred won or open stage leaves the win rate measured", () => {
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed Won" && s.name !== "Discovery");
    const out = analyseExtracts(extracts, { calibration: c });
    for (const id of ["historic.win_rate", "historic.no_decision_rate"]) {
      const m = metricOf(out.metrics, id);
      const was = metricOf(named.metrics, id);
      assert.equal(m.status, "measured", id);
      assert.ok(m.status === "measured" && was.status === "measured" && m.value === was.value, id);
    }
  });
});

// The deck's flow draws each stage as the engine used it (DECISIONS.md 600).
describe("the \"How deals move\" flow with a stage the calibration leaves out (DECISIONS.md 600)", () => {
  test("still draws the moves into Won when Closed Won isn't named", () => {
    const full = loadCalibration();
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed Won");
    const transitionsOf = (out: ReturnType<typeof analyseExtracts>) => out.metrics.find((m) => m.id === "historic.stage_transitions");
    const asNamed = processFlowChart(transitionsOf(analyseExtracts(extracts)), flowStages(full));
    const left = transitionsOf(analyseExtracts(extracts, { calibration: c }));
    const meanings = inferredMeanings(c, STAGES);
    assert.deepEqual(meanings, { "Closed Won": "won" });
    // The same chart as when the calibration names it.
    assert.deepEqual(JSON.stringify(processFlowChart(left, flowStages(c, meanings))), JSON.stringify(asNamed));
    // Without the engine's meanings, the arrows into Won are lost: the fault this mends.
    assert.notDeepEqual(JSON.stringify(processFlowChart(left, flowStages(c))), JSON.stringify(asNamed));
  });

  test("the calibration's word wins in the flow too", () => {
    const c = loadCalibration();
    assert.deepEqual(flowStages(c, { "Closed Won": "lost" }).filter((s) => s.name === "Closed Won"), [{ name: "Closed Won", category: "won" }]);
  });

  test("a run keeps the meanings beside its findings, and the documents read them", async () => {
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed Won");
    const { dir } = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: c, modules: SALES_CLOUD_MODULES, answers: loadAnswers(), outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator() });
    assert.deepEqual(JSON.parse(readFileSync(join(dir, STAGE_MEANINGS_FILE), "utf8")), { "Closed Won": "won" });
    assert.deepEqual(loadRun(dir).input.stageMeanings, { "Closed Won": "won" });
    // A fully calibrated run writes none, and reads none.
    const { dir: plain } = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, answers: loadAnswers(), outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator() });
    assert.ok(!existsSync(join(plain, STAGE_MEANINGS_FILE)));
    assert.equal(loadRun(plain).input.stageMeanings, null);
  });
});
