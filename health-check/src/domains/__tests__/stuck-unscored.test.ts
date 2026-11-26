import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { steadyPipeline, withPipeline } from "../../testing/steady-pipeline.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { passedOnMerit, scoreDomain, scoreDomains, overallScore, unscoredVersion, UNSCORED_VERSIONS } from "../scoring.ts";
import { verdictOf } from "../../narrative/select.ts";
import { currentPipelineModule, currentPipelineUnscoredModule, withStuckUnscored } from "../current-pipeline/index.ts";
import { LATEST_RULE_VERSIONS, SALES_CLOUD_MODULES } from "../index.ts";
import { fixRuleOf, FIX_TABLE } from "../../fix/table.ts";
import { compareWithBaseline, type Progress, type ProgressSide } from "../../progress/compare.ts";
import { CHANGED, changedFrom, changedProblems, issueUnder, LOOSENED, TIGHTENED } from "../../engine/loosened.ts";
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
  test("the synthetic org: version 2's figure as a pass the score leaves out, in words that don't call it a problem or fine, with no deal list", () => {
    const out = analysed({ stuckUnscored: true });
    const f = out.findings.find((x) => x.ruleId === RULE)!;
    const v2 = stuck();
    assert.equal(f.ruleVersion, 4);
    assert.equal(f.status, "passed", "never an issue");
    assert.deepEqual([f.facts.hits, f.facts.n, f.facts.rate], [v2.facts.hits, v2.facts.n, v2.facts.rate], "version 2's count, exactly");
    assert.equal(f.title, "Open deals that have waited longest in their stage");
    assert.equal(
      f.summary,
      "Open deals that have been in their stage longer than about three quarters of won deals stayed there: 59 of 70 (84.3%). 6 more open deals couldn't be compared, as they're in stages with too few won deals to set a line. Some open deals are usually past this point, because the deals still open at any moment tend to be the slower ones. Whether this share is a problem depends on what is normal for these stages, which we can't tell from it.",
    );
    assert.equal(f.fixPrompt, undefined, "a pass has no fix prompt");
    assert.equal(out.records.find((r) => r.ruleId === RULE), undefined, "no deal list anywhere");
    assert.doesNotMatch(`${f.title} ${f.summary}`, /sitting in stages for longer|always past|doesn't show a problem|within 0 /);
  });

  test("it is never counted as a pass that reflects well: not in an area's passed tally, nor among what's working", () => {
    const on = analysed({ stuckUnscored: true }).findings;
    assert.equal(passedOnMerit(on.find((f) => f.ruleId === RULE)!), false);
    assert.equal(passedOnMerit(stuck()), false, "version 2's issue isn't a pass either");
    assert.equal(passedOnMerit({ ruleId: RULE, ruleVersion: 2, status: "passed" }), true, "version 2's pass counts as before");
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
    assert.equal(fixRuleOf(RULE, 4), FIX_TABLE[RULE], "version 4 is a pass, with no fix prompt of its own");
  });
});

describe("between runs (decision 778)", () => {
  const f = (ruleId: string, status: "issue" | "passed", ruleVersion: number, facts: Record<string, number> = {}) => ({ id: `f-${ruleId}-${status}-${ruleVersion}`, ruleId, domain: "x", status, ruleVersion, title: "T", facts }) as unknown as Finding;
  const side = (runId: string, findings: Finding[], carried?: Record<string, number>): ProgressSide =>
    ({ runId, asOf: "2026-10-31T12:00:00.000Z", findings, scores: [], overall: { status: "not_measurable" }, value: { currency: "GBP", total: { expected: 0 } }, limitsReadAt: null, ...(carried ? { carried } : {}) }) as unknown as ProgressSide;
  const where = (p: Progress, ruleId = RULE): string[] => (["fixed", "remaining", "added", "unknown", "newlyMeasured", "ruleChanged", "stricter", "changedBack"] as const).filter((k) => (p[k] ?? []).some((c) => c.ruleId === ruleId));

  test("an issue under version 2 (or 3), then version 4: the check changed, said once, never fixed; the next run under 4 says nothing", () => {
    for (const v of [2, 3]) {
      const p = compareWithBaseline(side("a", [f(RULE, "issue", v)]), side("b", [f(RULE, "passed", 4, { wonLineRate: 0 })]));
      assert.deepEqual(where(p), ["ruleChanged"], `from ${v}`);
      assert.equal(p.carried, undefined);
    }
    assert.deepEqual(where(compareWithBaseline(side("b", [f(RULE, "passed", 4)]), side("c", [f(RULE, "passed", 4)]))), [], "version 4 then version 4: nothing");
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed", 2)]), side("b", [f(RULE, "passed", 4)]))), [], "a pass under 2 then version 4: nothing");
    assert.equal(LOOSENED[RULE]!.some((l) => l.version === 4), false, "declared as changed, not loosened");
    assert.deepEqual(changedFrom(RULE), { "4": [1, 2, 3] });
  });

  test("going back: version 4, then version 2 again, by the went-back rule", () => {
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed", 4)]), side("b", [f(RULE, "issue", 2)]))), ["changedBack"], "open, never new, nothing said");
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed", 4)]), side("b", [f(RULE, "passed", 2)]))), []);
  });

  test("the lead journey's versions are declared as changed: an issue then a pass says so once, a pass or nothing then an issue is open and never new or stricter, and two runs alike compare as any", () => {
    for (const [rule, from, to] of [["lead.response-time", 1, 2], ["lead.no-first-response", 2, 3], ["lead.no-first-response", 1, 3]] as const) {
      const issueThenPass = compareWithBaseline(side("a", [f(rule, "issue", from)]), side("b", [f(rule, "passed", to)]));
      assert.deepEqual(where(issueThenPass, rule), ["ruleChanged"], `${rule} ${from} to ${to}`);
      assert.equal(issueThenPass.carried, undefined, "said once, carried nowhere");
      assert.deepEqual(where(compareWithBaseline(side("a", [f(rule, "passed", from)]), side("b", [f(rule, "issue", to)])), rule), ["changedBack"], "never new, never stricter");
      assert.deepEqual(where(compareWithBaseline(side("a", [{ ...f(rule, "passed", from), status: "not_measurable" } as Finding]), side("b", [f(rule, "issue", to)])), rule), ["changedBack"], "not measurable then: never new");
      // The next run at the same version: the ordinary comparison.
      assert.deepEqual(where(compareWithBaseline(side("b", [f(rule, "passed", to)]), side("c", [f(rule, "issue", to)])), rule), ["added"]);
      assert.deepEqual(where(compareWithBaseline(side("b", [f(rule, "issue", to)]), side("c", [f(rule, "passed", to)])), rule), ["fixed"]);
    }
    assert.equal(LOOSENED["lead.response-time"], undefined, "neither looser");
    assert.equal(TIGHTENED["lead.response-time"], undefined, "nor stricter");
    assert.deepEqual(Object.keys(TIGHTENED), [RULE], "the stricter note is reachable only through version 3, behind HC_STUCK_EXPECTED");
    assert.deepEqual(changedFrom("lead.no-first-response"), { "3": [1, 2] });
    assert.deepEqual(changedProblems(CHANGED, LATEST_RULE_VERSIONS), []);
    // With the journey off the versions are the earlier ones, and an issue then a pass under them is a fix, as before.
    assert.deepEqual(where(compareWithBaseline(side("a", [f("lead.response-time", "issue", 1)]), side("b", [f("lead.response-time", "passed", 1)])), "lead.response-time"), ["fixed"]);
  });

  test("the digest gives version 4 no passedUnder: it is not across version 3's tightening", () => {
    assert.deepEqual(ruleChangeOf(f(RULE, "issue", 4, { wonLineRate: 30 }), { ownCalls: 0, carried: {} }), { ruleVersion: 4 }, "were it ever an issue");
    assert.ok("passedUnder" in ruleChangeOf(f(RULE, "issue", 3, { wonLineRate: 30 }), { ownCalls: 0, carried: {} }), "version 3's issue still carries it");
  });

  test("the digest carries carriedFrom for any rule kept apart, and nothing new where nothing is kept apart", () => {
    const pass = f("tech.storage-use", "passed", 2);
    assert.deepEqual(ruleChangeOf(pass, { ownCalls: 0, carried: {} }), { ruleVersion: 2 });
    assert.deepEqual(ruleChangeOf(pass, { ownCalls: 0, carried: { "tech.storage-use": 3 } }), { ruleVersion: 2, carriedFrom: 3 });
    assert.deepEqual(ruleChangeOf(f("tech.storage-use", "issue", 2), { ownCalls: 0, carried: { "tech.storage-use": 3 } }), { ruleVersion: 2 }, "an issue never carries it");
  });
});
