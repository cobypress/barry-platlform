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
  const run = async (calibration: Calibration) => loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  const none = await run(calibrate());
  const answered = await run(calibrate({ stageAnswer: answer(ALL) }));
  const plain = await run(stated(calibrate({ stageAnswer: answer(ALL) })));
  const partial = await run(calibrate({ stageAnswer: answer({ "Closed Lost": "lost" }) }));
  const named = await run(calibrate({ stageNames: true }));
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
    const p = compareWithBaseline(side(none, "a"), side(answered, "b"));
    const raised = FOUR.filter((id) => byId(answered, id).some((f) => f.status === "issue") && byId(none, id).every((f) => f.status === "not_measurable"));
    assert.ok(raised.length >= 2, raised.join());
    for (const id of raised) {
      assert.ok(p.newlyMeasured?.some((c) => c.ruleId === id), id);
      assert.ok(!p.added.some((c) => c.ruleId === id), id);
    }
    // Without the answer on either side: as before.
    assert.equal(compareWithBaseline(side(none, "a"), side(none, "b")).newlyMeasured, undefined);
  });
});
