import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { scoreDomain, scoreDomains, overallScore, unscoredVersion, UNSCORED_VERSIONS } from "../scoring.ts";
import { verdictOf } from "../../narrative/select.ts";
import { currentPipelineModule, currentPipelineUnscoredModule, withStuckUnscored } from "../current-pipeline/index.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { fixRuleOf, FIX_TABLE } from "../../fix/table.ts";
import { compareWithBaseline, type Progress, type ProgressSide } from "../../progress/compare.ts";
import { foundByStricter, issueUnder, LOOSENED, passedOnlyByChange, passedUnder, TIGHTENED } from "../../engine/loosened.ts";
import { ruleChangeOf } from "../../digest/build.ts";
import { stuckUnscoredStartLine } from "../../worker/env.ts";
import type { Finding } from "../../schemas/index.ts";

// cur.stuck-in-stage version 4, behind HC_STUCK_UNSCORED (DECISIONS.md 778): version 2's line, count and deal list,
// shown as an observation (an issue of severity info, as the code checker's groups are shown and not scored, 673)
// and never scored; no exposure figure; words that don't call it a problem. And two between-runs changes made with
// it: a scored reading followed by this one is the check changing, and the lead journey's versions are declared.

const RULE = "cur.stuck-in-stage";
let extracts: Map<string, unknown>;
const analysed = (o: { stuckUnscored?: boolean; stuckExpected?: boolean } = {}) => analyseExtracts(extracts, o);
const stuck = (o: { stuckUnscored?: boolean; stuckExpected?: boolean } = {}): Finding => analysed(o).findings.find((f) => f.ruleId === RULE)!;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

describe("version 4: shown, never scored (decision 778)", () => {
  test("the synthetic org: version 2's figure and deal list, as an observation, in words that don't call it a problem", () => {
    const out = analysed({ stuckUnscored: true });
    const f = out.findings.find((x) => x.ruleId === RULE)!;
    const v2 = stuck();
    assert.equal(f.ruleVersion, 4);
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "info", "never high");
    assert.deepEqual([f.facts.hits, f.facts.n, f.facts.rate], [v2.facts.hits, v2.facts.n, v2.facts.rate], "version 2's count, exactly");
    assert.equal(f.title, "Open deals that have waited longest in their stage");
    assert.equal(
      f.summary,
      "Open deals that have been in their stage longer than three quarters of your won deals took: 59 of 70 (84.3%). 6 more open deals couldn't be compared, as they're in stages with too few won deals to set a line. Some open deals are always past this point, because the deals still open at any moment are the slower ones, so this share on its own doesn't show a problem. The list is for your pipeline reviews.",
    );
    assert.equal(f.recommendation, "In pipeline reviews, go through the deals that have waited longest in each stage, and agree a next step or a decision for each.");
    assert.deepEqual(f.value, { kind: "not_sized", reason: "Some open deals are past this point in any pipeline, so the Amount on them doesn't show money at risk, and we don't put a figure on it." });
    assert.match(f.fixPrompt!, /At each review, every deal on the report has an agreed next step or a decision recorded\./);
    assert.doesNotMatch(`${f.title} ${f.summary} ${f.recommendation} ${f.fixPrompt}`, /sitting in stages for longer|shrinks at each review|move each deal on with a dated next step or close it/);
    const rows = out.records.find((r) => r.ruleId === RULE)!.rows;
    assert.ok(rows.length > 0 && rows.length <= f.facts.hits!);
    for (const r of rows) {
      const m = /^In \S+ for (\d+(?:\.\d)?) (day|days), where three quarters of won deals moved on within (\d+(?:\.\d)?) (day|days)$/.exec(r.problem);
      assert.ok(m, r.problem);
      assert.ok(Number(m[1]) > Number(m[3]), r.problem);
    }
  });

  test("it never moves an area or the overall score, isn't counted in an area's coverage, and stays out of the headline", () => {
    const on = analysed({ stuckUnscored: true }).findings;
    const without = on.filter((f) => f.ruleId !== RULE);
    const area = (fs: Finding[]) => scoreDomain("current_pipeline", fs);
    assert.deepEqual({ ...area(on), findingIds: undefined }, { ...area(without), findingIds: undefined });
    const ids = [...new Set(on.map((f) => f.domain))];
    assert.deepEqual(overallScore(scoreDomains(ids, on), on), overallScore(scoreDomains(ids, without), without));
    assert.deepEqual(verdictOf(on), verdictOf(without));
    assert.ok(unscoredVersion(on.find((f) => f.ruleId === RULE)!));
    assert.equal(unscoredVersion({ ruleId: RULE, ruleVersion: 2 }), false, "version 2 is scored as before");
    // Coverage: with three other checks unmeasured, version 2 not measurable tips the area; version 4 doesn't count.
    const thin = without.map((x) => (["cur.stale-deals", "cur.close-date-pushes", "cur.missing-next-steps"].includes(x.ruleId) ? { ...x, status: "not_measurable" as const, notMeasurableReason: "x" } : x));
    const v4 = { ...on.find((f) => f.ruleId === RULE)! };
    assert.deepEqual(area([...thin, v4]).status, area(thin).status);
    assert.equal(area(thin).status, "measured");
    assert.deepEqual(UNSCORED_VERSIONS, { [RULE]: [4] });
  });

  test("version 3 wins when both switches are on, and the start line says so", () => {
    assert.equal(stuck({ stuckUnscored: true, stuckExpected: true }).ruleVersion, 3);
    assert.match(stuckUnscoredStartLine(true, true), /version 3 wins, so it has no effect/);
    assert.match(stuckUnscoredStartLine(true, false), /moves no score/);
    assert.match(stuckUnscoredStartLine(false, false), /scored as before/);
  });

  test("with the switch off, version 2 exactly", () => {
    assert.deepEqual(analysed({ stuckUnscored: false }).findings, analysed().findings);
    assert.equal(stuck().ruleVersion, 2);
    assert.deepEqual(withStuckUnscored(SALES_CLOUD_MODULES).filter((m, i) => m !== SALES_CLOUD_MODULES[i]), [currentPipelineUnscoredModule]);
    assert.deepEqual(currentPipelineUnscoredModule.rules.map((r) => r.version), currentPipelineModule.rules.map((r) => (r.id === RULE ? 4 : r.version)));
    assert.equal(fixRuleOf(RULE, 2), FIX_TABLE[RULE]);
    assert.deepEqual(fixRuleOf(RULE, 4)!.hours, FIX_TABLE[RULE]!.hours);
  });
});

describe("between runs (decision 778)", () => {
  const f = (ruleId: string, status: "issue" | "passed", ruleVersion: number, facts: Record<string, number> = {}) => ({ id: `f-${ruleId}-${status}-${ruleVersion}`, ruleId, domain: "x", status, ruleVersion, title: "T", facts }) as unknown as Finding;
  const side = (runId: string, findings: Finding[], carried?: Record<string, number>): ProgressSide =>
    ({ runId, asOf: "2026-10-31T12:00:00.000Z", findings, scores: [], overall: { status: "not_measurable" }, value: { currency: "GBP", total: { expected: 0 } }, limitsReadAt: null, ...(carried ? { carried } : {}) }) as unknown as ProgressSide;
  const where = (p: Progress, ruleId = RULE): string[] => (["fixed", "remaining", "added", "unknown", "newlyMeasured", "ruleChanged", "stricter", "changedBack"] as const).filter((k) => (p[k] ?? []).some((c) => c.ruleId === ruleId));

  test("an issue under version 2, then the unscored reading: the check changed, never fixed and never still open", () => {
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "issue", 2)]), side("b", [f(RULE, "issue", 4, { wonLineRate: 84.3 })]))), ["changedBack"]);
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed", 2)]), side("b", [f(RULE, "issue", 4, { wonLineRate: 30 })]))), ["changedBack"], "a pass, then an observation: never new");
  });

  test("an issue under version 2, then no deal past the line under 4: fixed only where version 2 would pass it too", () => {
    const [l] = LOOSENED[RULE]!.filter((x) => x.version === 4);
    assert.deepEqual(l!.from, [1, 2]);
    assert.deepEqual(issueUnder(f(RULE, "passed", 4, { wonLineRate: 0 }), undefined), []);
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "issue", 2)]), side("b", [f(RULE, "passed", 4, { wonLineRate: 0 })]))), ["fixed"]);
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "issue", 2)]), side("b", [f(RULE, "passed", 4, { rate: 0 })]))), ["ruleChanged"], "no reading of version 2's: never a fix we can't show");
  });

  test("going back: the observation under 4, then version 2 again", () => {
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "issue", 4)]), side("b", [f(RULE, "passed", 2)]))), ["ruleChanged"]);
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed", 4)]), side("b", [f(RULE, "issue", 2)]))), ["changedBack"]);
  });

  test("the lead journey's versions read as the check changing: a pass across them is never fixed, an issue across them never new", () => {
    for (const [rule, from, to] of [["lead.response-time", 1, 2], ["lead.no-first-response", 2, 3], ["lead.no-first-response", 1, 3]] as const) {
      assert.equal(passedOnlyByChange(rule, from, to, issueUnder(f(rule, "passed", to), undefined)), true, `${rule} ${from} to ${to}: an issue then a pass`);
      assert.equal(foundByStricter(rule, from, to, passedUnder(f(rule, "issue", to), undefined)), true, `${rule} ${from} to ${to}: a pass then an issue`);
      assert.deepEqual(where(compareWithBaseline(side("a", [f(rule, "issue", from)]), side("b", [f(rule, "passed", to)])), rule), ["ruleChanged"]);
    }
    assert.ok(TIGHTENED["lead.response-time"] && TIGHTENED["lead.no-first-response"]);
    // With the journey off the versions are the earlier ones, and an issue then a pass under them is a fix, as before.
    assert.deepEqual(where(compareWithBaseline(side("a", [f("lead.response-time", "issue", 1)]), side("b", [f("lead.response-time", "passed", 1)])), "lead.response-time"), ["fixed"]);
  });

  test("the digest carries carriedFrom for any rule kept apart, and nothing new where nothing is kept apart", () => {
    const pass = f("tech.storage-use", "passed", 2);
    assert.deepEqual(ruleChangeOf(pass, { ownCalls: 0, carried: {} }), { ruleVersion: 2 });
    assert.deepEqual(ruleChangeOf(pass, { ownCalls: 0, carried: { "tech.storage-use": 3 } }), { ruleVersion: 2, carriedFrom: 3 });
    assert.deepEqual(ruleChangeOf(f("tech.storage-use", "issue", 2), { ownCalls: 0, carried: { "tech.storage-use": 3 } }), { ruleVersion: 2 }, "an issue never carries it");
  });
});
