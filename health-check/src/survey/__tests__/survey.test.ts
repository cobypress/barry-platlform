import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { Finding, Metric } from "../../schemas/index.ts";
import { SYNTHETIC_COMMENT, syntheticSurvey } from "../../testing/survey.ts";
import { buildPairings } from "../pairings.ts";
import { COMMENT_LEFT_OUT, QUESTIONS, readAnswers, SURVEY_VERSION, withoutComments } from "../questions.ts";
import { MIN_RESPONSES, SURVEY, SURVEY_METRIC_IDS, surveyMetrics } from "../summary.ts";

const AS_OF = "2026-11-05T09:00:00.000Z";
const measured = (metrics: Metric[], id: string): Extract<Metric, { status: "measured" }> => {
  const m = metrics.find((x) => x.id === id);
  assert.equal(m?.status, "measured", id);
  return m as Extract<Metric, { status: "measured" }>;
};

describe("the survey's questions", () => {
  test("are versioned, ask no name, and leave the comment optional", () => {
    assert.equal(SURVEY_VERSION, 1);
    assert.deepEqual(QUESTIONS.map((q) => q.id), ["role", "ease", "trust", "admin_hours", "rekeying_hours", "blockers", "recommend", "comment"]);
    assert.ok(QUESTIONS.every((q) => !/\bname\b/i.test(q.text) || q.id === "comment"), "only the comment mentions names, to ask for none");
    assert.deepEqual(QUESTIONS.filter((q) => !q.required).map((q) => q.id), ["comment"]);
  });

  test("drop answers that don't fit, and every answer to another version", () => {
    const survey = syntheticSurvey(6);
    survey.responses[0]!.answers = { ...(survey.responses[0]!.answers as object), ease: 9 };
    survey.responses[1]!.answers = { ...(survey.responses[1]!.answers as object), admin_hours: 1.2 };
    assert.deepEqual([readAnswers(survey).answers.length, readAnswers(survey).dropped], [4, 2]);
    assert.deepEqual(readAnswers({ ...survey, version: 2 }), { answers: [], dropped: 6 });
  });

  test("keep only whether someone commented, never what they wrote", () => {
    const { answers } = readAnswers(syntheticSurvey(6));
    assert.deepEqual(answers.map((a) => a.commented), [true, false, false, true, false, false]);
    assert.ok(!JSON.stringify(answers).includes(SYNTHETIC_COMMENT));
    const stripped = withoutComments(syntheticSurvey(6));
    assert.ok(!JSON.stringify(stripped).includes(SYNTHETIC_COMMENT));
    assert.equal((stripped.responses[0]!.answers as { comment: string }).comment, COMMENT_LEFT_OUT);
    assert.deepEqual(readAnswers(stripped).answers.map((a) => a.commented), [true, false, false, true, false, false]);
  });
});

describe("the survey's metrics", () => {
  test("are none at all without a survey", () => {
    assert.deepEqual(surveyMetrics(null, AS_OF), []);
  });

  test(`under ${MIN_RESPONSES} answers, are one line saying so, with no figure`, () => {
    for (const survey of [syntheticSurvey(0), syntheticSurvey(4), { ...syntheticSurvey(9), version: 2 }]) {
      const metrics = surveyMetrics(survey, AS_OF);
      assert.equal(metrics.length, 1);
      const [only] = metrics;
      assert.equal(only!.status, "not_measurable");
      assert.equal(only!.id, SURVEY.responses);
      assert.match(only!.status === "not_measurable" ? only!.reason : "", /^Fewer than 5 people answered the team survey/);
      assert.ok(!("value" in only!));
    }
  });

  test("answers that don't fit don't count towards the five", () => {
    const survey = syntheticSurvey(5);
    survey.responses[0]!.answers = { role: "sales_rep" };
    assert.equal(surveyMetrics(survey, AS_OF)[0]!.status, "not_measurable");
  });

  test("give medians, upper quartiles, shares and the recommend score", () => {
    const metrics = surveyMetrics(syntheticSurvey(10), AS_OF);
    assert.deepEqual(metrics.map((m) => m.id), SURVEY_METRIC_IDS);
    assert.ok(metrics.every((m) => m.domain === "survey" && m.evidence[0]!.kind === "engine"));
    assert.equal(measured(metrics, SURVEY.responses).value, 10);
    assert.equal(measured(metrics, SURVEY.responses).stats.comments, 4);
    // Ease runs 1 to 5 twice, so the median is 3; trust runs 1,3,5,2,4 twice, also 3.
    assert.equal(measured(metrics, SURVEY.ease).value, 3);
    assert.equal(measured(metrics, SURVEY.trust).value, 3);
    // Admin is 0, 1.5, 3, 4.5 in turn: sorted 0,0,0,1.5,1.5,1.5,3,3,4.5,4.5.
    assert.equal(measured(metrics, SURVEY.admin).value, 1.5);
    assert.equal(measured(metrics, SURVEY.admin).stats.p75, 3);
    // Each person's total, not the two medians added.
    const totals = Array.from({ length: 10 }, (_, i) => (i % 4) * 1.5 + (i % 3)).sort((a, b) => a - b);
    assert.equal((totals[4]! + totals[5]!) / 2, 2.75);
    assert.equal(measured(metrics, SURVEY.lost).value, 2.8, "rounded to one place");
    // Recommend is (i * 3) % 11: 0,3,6,9,1,4,7,10,2,5. Two answer 9 or 10, seven 6 or less.
    const recommend = measured(metrics, SURVEY.recommend);
    assert.equal(recommend.value, 20);
    assert.equal(recommend.stats.score, -50);
    assert.match(recommend.definition, /isn't one/);
    assert.ok(!/\bNPS\b/.test(JSON.stringify(metrics)));
    const blockers = measured(metrics, SURVEY.blockers);
    assert.equal(blockers.value, 80, "two in ten said nothing gets in their way");
    assert.equal(blockers.segments.find((s) => s.key.startsWith("Typing the same thing"))!.value, 40);
  });

  test(`show a role on its own only with ${MIN_RESPONSES} or more answers`, () => {
    // Synthetic roles run rep, rep, manager, admin, leadership: 10 people is 4 reps.
    const ten = surveyMetrics(syntheticSurvey(10), AS_OF);
    assert.ok(ten.every((m) => (m.status === "measured" ? m.segments : []).every((s) => s.dimension !== "role")));
    assert.match(measured(ten, SURVEY.ease).segmentExemptReason ?? "", /fewer than 5 answers/);
    // Fifteen is 6 reps and 3 of each other role.
    const fifteen = surveyMetrics(syntheticSurvey(15), AS_OF);
    const roles = measured(fifteen, SURVEY.ease).segments.filter((s) => s.dimension === "role");
    assert.deepEqual(roles.map((s) => [s.key, s.n]), [["Sales rep", 6]]);
    assert.ok(measured(fifteen, SURVEY.ease).assumptions.some((a) => /fewer than 5 answers aren't shown/.test(a)));
    assert.ok(fifteen.every((m) => (m.status === "measured" ? m.segments : []).filter((s) => s.dimension === "role").every((s) => s.n >= MIN_RESPONSES)));
  });
});

describe("what the team says, against what we found", () => {
  const issue = (ruleId: string, domain: string): Finding => ({ id: `f-${ruleId}`, ruleId, domain, status: "issue", severity: "medium", title: ruleId, value: { kind: "not_sized", reason: "Synthetic." } }) as unknown as Finding;
  const metric = (id: string, value: number): Metric => ({ id, status: "measured", value }) as unknown as Metric;

  test("reads each pairing by rule, the same way every time", () => {
    const low = [metric(SURVEY.trust, 2), metric(SURVEY.ease, 4.5), metric(SURVEY.rekeying, 0)];
    const pairs = buildPairings(low, [issue("dq.stale-leads", "data_quality"), issue("dq.duplicate-rules-missing", "data_quality")]);
    const by = Object.fromEntries(pairs.map((p) => [p.key, p]));
    assert.equal(by.trust!.reading, "What we found backs this up.");
    assert.equal(by.ease!.reading, "Your team and the data agree.");
    assert.equal(by.rekeying!.reading, "Your team rates this better than the data suggests, so these issues may not be felt yet.");
    assert.equal(by.rekeying!.issues.length, 1);
    const felt = buildPairings([metric(SURVEY.ease, 2)], []);
    assert.equal(felt[0]!.reading, "We didn't find a cause in what we read, so it's worth asking your team what's behind it.");
  });

  test("names the org measures it sets against ease and re-typing, when they're measured", () => {
    const pairs = buildPairings([metric(SURVEY.ease, 3), metric(SURVEY.rekeying, 1), metric("adoption.monthly_active_share", 60), metric("documented.integrations", 7)], []);
    assert.equal(pairs[0]!.found, "We found no issues with adoption, and 60% of users logged in last month.");
    assert.match(pairs[1]!.said, / 1 hour a week/);
    assert.equal(pairs[1]!.found, "We found no issues with duplicates and integrations, and the org has 7 integration points.");
  });

  test("is empty when the survey shows nothing", () => {
    assert.deepEqual(buildPairings(surveyMetrics(syntheticSurvey(3), AS_OF), [issue("dq.stale-leads", "data_quality")]), []);
  });
});
