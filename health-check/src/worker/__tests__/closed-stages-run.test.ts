import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { parseStageAnswer, type StageAnswer } from "../../connect/stage-outcomes.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { compareWithBaseline } from "../../progress/compare.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Calibration } from "../../schemas/calibration.ts";
import { AutomaticReplay, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";

// The synthetic org's automatic run with the customer's answer about its
// closed stages, or with the name rule, against one whose calibration states
// the same outcomes plainly, as a person's would (DECISIONS.md 756).

const FOUR = ["act.deals-without-activity", "act.won-vs-lost-activity", "rev.win-backs", "pipe.segment-divergence"];
const ALL = { "Closed Lost": "lost", "Closed Lost - No Budget": "lost", "Closed No Decision": "no_decision" };
const MADE = "2026-11-25T09:00:00.000Z";
const UNSPLIT = /^Salesforce's setting can't tell lost from no decision for: /;
const ASSUMED = "We've assumed what some stages mean from their names, and you can tell us if any is wrong: Closed Lost as lost, Closed Lost - No Budget as lost and Closed No Decision as no decision.";

describe("an automatic run of the synthetic org with its closed stages set", async () => {
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const calibrate = (o: { stageAnswer?: StageAnswer; stageNames?: boolean } = {}): Calibration => {
    const b = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP", ...o });
    assert.ok(b.ok);
    return b.calibration;
  };
  const answer = (value: unknown) => parseStageAnswer(value) as StageAnswer;
  /** The same calibration with every stage stated plainly, as a person's would be: no mark of where it came from. */
  const stated = (c: Calibration): Calibration => ({ ...c, stages: c.stages.map(({ from: _f, ...s }) => s) });
  const run = async (calibration: Calibration, stageNames = false) => loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay(), stageNames })).dir);
  const none = await run(calibrate());
  const answered = await run(calibrate({ stageAnswer: answer(ALL) }));
  const plain = await run(stated(calibrate({ stageAnswer: answer(ALL) })));
  const partial = await run(calibrate({ stageAnswer: answer({ "Closed Lost": "lost" }) }));
  const named = await run(calibrate({ stageNames: true }), true);
  // A calibration that still holds readings from names, analysed with the switch off: set aside, as if never read.
  const namedOff = await run(calibrate({ stageNames: true }));
  const byId = (r: typeof none, id: string) => r.input.findings.filter((f) => f.ruleId === id);

  test("without an answer, the four checks are not measurable with the engine's sentence, as before", () => {
    for (const id of FOUR.slice(0, 3)) {
      const f = byId(none, id);
      assert.ok(f.length > 0 && f.every((x) => x.status === "not_measurable" && UNSPLIT.test(x.notMeasurableReason ?? "")), id);
      assert.equal(f[0]!.notMeasurableReason, "Salesforce's setting can't tell lost from no decision for: Closed Lost, Closed Lost - No Budget, Closed No Decision. If you tell us which each is, your next health check can measure this.");
    }
  });

  test("with the answer: findings, metrics and scores are exactly those of a calibration stating the same outcomes", () => {
    assert.deepEqual(answered.input.findings, plain.input.findings);
    assert.deepEqual(answered.input.metrics, plain.input.metrics);
    assert.deepEqual(answered.input.scores, plain.input.scores);
    assert.ok(!JSON.stringify(answered.input.findings).includes("can't tell lost from no decision"));
    assert.ok(!JSON.stringify(answered.input.metrics).includes("can't tell lost from no decision"));
    for (const id of FOUR) assert.ok(byId(answered, id).every((f) => f.status !== "not_measurable"), id);
    assert.ok(!JSON.stringify(answered.input.findings).includes("from its name") && !JSON.stringify(answered.input.findings).includes("from their names"), "an answer is the customer's word: no assumption");
  });

  test("a partial answer: the sentence lists only the stages still undecided, word for word otherwise", () => {
    for (const id of FOUR.slice(0, 3)) {
      assert.equal(byId(partial, id)[0]!.notMeasurableReason, "Salesforce's setting can't tell lost from no decision for: Closed Lost - No Budget, Closed No Decision. If you tell us which each is, your next health check can measure this.", id);
    }
  });

  test("the name rule: the same figures as the answer, every finding and metric that read them saying so as an assumption", () => {
    // The sentence and the marks of where its stage names sit are all that differ.
    const strip = (o: unknown) =>
      JSON.parse(JSON.stringify(o), (k, v) => (k === "orgWords" ? undefined : Array.isArray(v) ? v.filter((x) => x !== ASSUMED) : typeof v === "string" ? v.split(` ${ASSUMED}`).join("") : v));
    assert.deepEqual(strip(named.input.findings), strip(answered.input.findings));
    assert.deepEqual(strip(named.input.metrics), strip(answered.input.metrics));
    assert.deepEqual(named.input.scores, answered.input.scores);
    for (const id of FOUR) assert.ok(byId(named, id).every((f) => f.status !== "not_measurable" && JSON.stringify(f).includes(ASSUMED)), id);
  });

  test("the switch off at analysis: readings from names are set aside, whatever the calibration holds, and the run is the one with none", () => {
    assert.deepEqual(namedOff.input.findings, none.input.findings);
    assert.deepEqual(namedOff.input.metrics, none.input.metrics);
    assert.deepEqual(namedOff.input.scores, none.input.scores);
  });

  test("the assumption only on figures that split lost from no decision, or are sized by the win rate that does", () => {
    const has = (x: unknown) => JSON.stringify(x).includes(ASSUMED);
    // Not the rate including no decision: it counts both as not won, so it doesn't lean on which is which.
    assert.deepEqual(named.input.metrics.filter(has).map((m) => m.id).sort(), [
      "activity.per_lost_deal", "historic.lost_by_stage", "historic.lost_value_by_stage", "historic.no_decision_rate", "historic.quarterly_win_rate",
      "historic.sales_cycle_lost", "historic.win_rate", "historic.win_rate_by_value",
    ]);
    assert.deepEqual([...new Set(named.input.findings.filter(has).map((f) => f.ruleId))].sort(), [
      // Readers of the split.
      "act.deals-without-activity", "act.won-vs-lost-activity", "pipe.segment-divergence", "rev.win-backs",
      // Sized at the overall win rate every time (value.ts atOwnWinRate), so its figure moves with it.
      "rev.dormant-accounts",
    ].sort());
    assert.equal(named.input.findings.filter(has).length, 7);
  });

  test("a finding sized from comparable deals carries the assumption only where a thin group falls back to the overall win rate", async () => {
    const at = async (minSample: number) => run({ ...calibrate({ stageNames: true }), minSample }, true);
    const FALLBACK = "weighted at your overall win rate";
    const has = (x: unknown) => JSON.stringify(x).includes(ASSUMED);
    for (const r of [named, await at(40)]) {
      for (const f of r.input.findings.filter((x) => ["dq.open-opps-past-close", "dq.opps-inactive-owner", "dq.open-opps-no-activity", "cur.stale-deals", "rev.stalled-deals"].includes(x.ruleId))) {
        assert.equal(has(f), JSON.stringify(f.value).includes(FALLBACK), f.ruleId);
      }
    }
    const thin = await at(40);
    assert.ok(thin.input.findings.some((f) => f.ruleId === "dq.open-opps-no-activity" && JSON.stringify(f.value).includes(FALLBACK) && has(f)), "the fallback is used, and said");
  });

  test("released as an automatic run must be: nothing held, unmarked or unmatched, the stage names marked as the org's and nothing else in the sentence", () => {
    for (const r of [answered, named, partial]) {
      const d = checkedDigest({ input: r.input, pseudonyms: r.pseudonyms, inventory: null, madeAt: MADE });
      assert.equal(d.held, null);
      assert.equal(d.unmarked, 0);
      assert.equal(d.unmatched, 0);
    }
    const d = checkedDigest({ input: named.input, pseudonyms: named.pseudonyms, inventory: null, madeAt: MADE }).digest!;
    const f = d.findings.find((x) => x.summary?.includes(ASSUMED))!;
    assert.ok(f, "the assumption reaches the digest");
    const at = f.summary!.indexOf(ASSUMED);
    const spans = f.orgWords!.summary!.orgWords.filter((s) => s.start >= at && s.end <= at + ASSUMED.length).map((s) => f.summary!.slice(s.start, s.end));
    assert.deepEqual(spans, ["Closed Lost", "Closed Lost - No Budget", "Closed No Decision"]);
  });

  test("since last time: a check unmeasured then for want of the answer is found with nothing earlier to compare, never new", () => {
    const side = (r: typeof none, runId: string) => ({ runId, asOf: "2026-11-01T00:00:00.000Z", findings: r.input.findings, scores: [], overall: { status: "not_measurable", reason: "x" }, value: { currency: "GBP", total: { low: 0, expected: 0, high: 0 } }, limitsReadAt: null }) as never;
    const p = compareWithBaseline(side(none, "a"), { ...(side(answered, "b") as object), checkedByCode: true } as never);
    // A run whose digest no code checks (consultant-led) compares as before 756: the owner's open decision.
    const asBefore = compareWithBaseline(side(none, "a"), side(answered, "b"));
    assert.equal(asBefore.newlyMeasured, undefined);
    const raised = FOUR.filter((id) => byId(answered, id).some((f) => f.status === "issue") && byId(none, id).every((f) => f.status === "not_measurable"));
    assert.ok(raised.length >= 2, raised.join());
    for (const id of raised) {
      assert.ok(p.newlyMeasured?.some((c) => c.ruleId === id), id);
      assert.ok(!p.added.some((c) => c.ruleId === id), id);
    }
    // Without the answer on either side: as before.
    assert.equal(compareWithBaseline(side(none, "a"), side(none, "b")).newlyMeasured, undefined);
  });

  test("a consultant-led re-check compares as it did before 756: a check the calibration couldn't split before is new (the owner's open decision)", async () => {
    const { buildReport } = await import("../../narrative/sections.ts");
    const { buildPulseResult } = await import("../../pulse/result.ts");
    const { loadAnswers, loadCalibration } = await import("../../testing/replay.ts");
    const { cpSync, readFileSync, writeFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const { tempDir } = await import("../../testing/harness.ts");
    const cal = loadCalibration();
    const dropped = { ...cal, stages: cal.stages.filter((s) => s.name !== "Closed No Decision" && s.name !== "Closed Lost - No Budget") };
    const before = (await runFromRecording({ answers: loadAnswers(), calibration: dropped })).dir;
    const now = (await runFromRecording({ answers: loadAnswers() })).dir;
    const parent = tempDir();
    const earlier = join(parent, "hc-2026-10-09-earlier");
    const dir = join(parent, "hc-2026-11-09-now");
    cpSync(before, earlier, { recursive: true });
    cpSync(now, dir, { recursive: true });
    const json = (d: string) => JSON.parse(readFileSync(join(d, "run.json"), "utf8"));
    writeFileSync(join(earlier, "run.json"), JSON.stringify({ ...json(before), id: "hc-2026-10-09-earlier", asOf: "2026-10-09T12:00:00.000Z" }));
    writeFileSync(join(dir, "run.json"), JSON.stringify({ ...json(now), id: "hc-2026-11-09-now", baselineRunId: "hc-2026-10-09-earlier" }));
    const report = buildReport(loadRun(dir).input);
    const p = report.progress!;
    assert.deepEqual(p.added.map((c) => [c.ruleId, c.findingIds.length]), [["dq.loss-reason-missing", 1], ["pipe.segment-divergence", 3], ["act.deals-without-activity", 1], ["rev.win-backs", 1]]);
    assert.equal(p.newlyMeasured, undefined);
    assert.equal((buildPulseResult(report) as { counts: { new: number } }).counts.new, 4);
  });

  test("the first month as the reports and the scan's result show it: segments now measured are first checked, never new, answered or read by names", async () => {
    const { buildReport } = await import("../../narrative/sections.ts");
    const { buildPulseResult } = await import("../../pulse/result.ts");
    const { cpSync, readFileSync, writeFileSync } = await import("node:fs");
    const { join } = await import("node:path");
    const { tempDir } = await import("../../testing/harness.ts");
    const noneDir = (await runFromRecording({ calibration: calibrate(), reviewMode: "automatic", transport: new AutomaticReplay() })).dir;
    for (const [calibration, stageNames] of [[calibrate({ stageAnswer: answer(ALL) }), false], [calibrate({ stageNames: true }), true]] as const) {
      const nowDir = (await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay(), stageNames })).dir;
      const parent = tempDir();
      const earlier = join(parent, "hc-2026-10-09-earlier");
      const dir = join(parent, "hc-2026-11-09-now");
      cpSync(noneDir, earlier, { recursive: true });
      cpSync(nowDir, dir, { recursive: true });
      const json = (d: string) => JSON.parse(readFileSync(join(d, "run.json"), "utf8"));
      writeFileSync(join(earlier, "run.json"), JSON.stringify({ ...json(noneDir), id: "hc-2026-10-09-earlier", asOf: "2026-10-09T12:00:00.000Z" }));
      writeFileSync(join(dir, "run.json"), JSON.stringify({ ...json(nowDir), baselineRunId: "hc-2026-10-09-earlier" }));
      const report = buildReport(loadRun(dir).input);
      const p = report.progress!;
      assert.deepEqual(p.added, [], JSON.stringify(p.added));
      assert.ok(p.newlyMeasured!.filter((c) => c.ruleId === "pipe.segment-divergence").flatMap((c) => c.findingIds).length === 3, "all three win-rate segments, the team's and the reps'");
      const result = buildPulseResult(report) as { counts: { new: number }; newIssues?: unknown[] };
      assert.equal(result.counts.new, 0);
      assert.deepEqual(result.newIssues ?? [], []);
    }
  });
});

describe("a value's period marked only where code checks the digest (DECISIONS.md 756)", async () => {
  const { runHealthCheck } = await import("../../engine/run.ts");
  const { SALES_CLOUD_MODULES, CPQ_MODULES } = await import("../../domains/index.ts");
  const { Simulator } = await import("../../testing/simulator.ts");
  const { AS_OF, CONNECTED_USER } = await import("../../testing/synthetic-org.ts");
  const { syntheticProvider, loadAnswers } = await import("../../testing/replay.ts");
  const { analyseExtracts, loadExtracts, tempDir } = await import("../../testing/harness.ts");
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const decided = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency: "GBP", stageAnswer: parseStageAnswer(ALL) as StageAnswer });
  assert.ok(decided.ok);
  const pulse = async (gated: boolean, transport = new Simulator()) =>
    loadRun((await runHealthCheck({ provider: syntheticProvider, calibration: decided.calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, log: () => {}, product: "pulse", ...(gated ? { gated: true } : {}) })).dir);
  const PERIOD = /^in wins the gap may have cost, \d{4}-\d\d-\d\d to \d{4}-\d\d-\d\d$/;

  test("a gated monthly scan on a decided calibration is released: its periods marked as ours, nothing unmarked or held", async () => {
    for (const transport of [new Simulator(), new Simulator({ readOnly: true }), new Simulator({ organizationType: "Professional Edition" })]) {
      const r = await pulse(true, transport);
      const withPeriod = r.input.findings.filter((f) => f.value.kind !== "not_sized" && PERIOD.test(f.value.period));
      assert.ok(withPeriod.length > 0, "the win-rate segments are sized");
      for (const f of withPeriod) assert.deepEqual(f.orgWords?.valuePeriod?.spans, []);
      const d = checkedDigest({ input: r.input, pseudonyms: r.pseudonyms, inventory: null, madeAt: MADE });
      assert.equal(d.held, null);
      assert.equal(d.unmarked, 0);
    }
  });

  test("a run whose digest nobody checks by code keeps its findings exactly as before: no period mark", async () => {
    const r = await pulse(false);
    assert.ok(r.input.findings.some((f) => f.value.kind !== "not_sized" && PERIOD.test(f.value.period)));
    assert.ok(r.input.findings.every((f) => f.orgWords?.valuePeriod === undefined));
  });

  test("the CPQ discount's period too", async () => {
    const cpq = await runFromRecording({ answers: loadAnswers(), modules: [...SALES_CLOUD_MODULES, ...CPQ_MODULES] });
    for (const markPeriods of [true, false]) {
      const out = analyseExtracts(loadExtracts(cpq.dir), { modules: [...SALES_CLOUD_MODULES, ...CPQ_MODULES], markPeriods });
      const f = out.findings.find((x) => x.ruleId === "cpq.discount-leakage")!;
      assert.match(f.value.kind === "revenue" ? f.value.period : "", /^in discount beyond what peers gave, \d{4}-\d\d-\d\d to \d{4}-\d\d-\d\d, a potential figure$/);
      assert.deepEqual(f.orgWords?.valuePeriod?.spans, markPeriods ? [] : undefined);
    }
  });
});
