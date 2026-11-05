import { pct } from "../engine/rule.ts";
import { median, quantile, round1 } from "../domains/historic-pipeline/metrics.ts";
import type { Evidence, Metric, MetricSegment } from "../schemas/index.ts";
import { BLOCKERS, readAnswers, ROLE_BANDS, type Survey, type SurveyAnswers } from "./questions.ts";

// What the team survey says, as metrics in their own "survey" domain: they
// describe the people, not the org, and no area is scored from them. Built when
// the report is, because answers keep arriving after the org is read. Fewer
// than five answers and nothing is shown, and a role is shown on its own only
// with five or more, so no answer can be traced to a person (DECISIONS.md 141).

export const MIN_RESPONSES = 5;
const DOMAIN = "survey";

export const SURVEY = {
  responses: "survey.responses",
  ease: "survey.ease",
  trust: "survey.trust",
  admin: "survey.admin_hours",
  rekeying: "survey.rekeying_hours",
  lost: "survey.hours_lost",
  blockers: "survey.blockers",
  recommend: "survey.recommend",
} as const;
export const SURVEY_METRIC_IDS: string[] = Object.values(SURVEY);

type Answer = Omit<SurveyAnswers, "comment"> & { commented: boolean };

const DEFS: Record<keyof typeof SURVEY, Pick<Metric, "id" | "label" | "unit" | "definition">> = {
  responses: { id: SURVEY.responses, label: "People who answered the team survey", unit: "count", definition: "Anonymous answers to the team survey. Each person answers once, and no name is asked." },
  ease: { id: SURVEY.ease, label: "How easy Salesforce is to use", unit: "count", definition: "The median answer, from 1 (very hard) to 5 (very easy), to how easy Salesforce is to use as it's set up." },
  trust: { id: SURVEY.trust, label: "Trust in the pipeline figures", unit: "count", definition: "The median answer, from 1 (not at all) to 5 (completely), to how much people trust the pipeline figures in Salesforce." },
  admin: { id: SURVEY.admin, label: "Hours a week on admin that doesn't help sell", unit: "count", definition: "The median hours a week people say they spend on Salesforce admin that doesn't help them sell, with the upper quartile." },
  rekeying: { id: SURVEY.rekeying, label: "Hours a week typing data in again", unit: "count", definition: "The median hours a week people say they spend typing data in again or keeping a spreadsheet alongside Salesforce, with the upper quartile." },
  lost: { id: SURVEY.lost, label: "Hours a week lost, all told", unit: "count", definition: "Each person's admin and re-typing hours added together, then the median and upper quartile of those totals." },
  blockers: { id: SURVEY.blockers, label: "People held back by something", unit: "percent", definition: "The share of people naming at least one thing that gets in their way, with the share naming each." },
  recommend: { id: SURVEY.recommend, label: "Would recommend Salesforce as it's set up", unit: "percent", definition: "The share answering 9 or 10, from 0 to 10, to how likely they are to recommend Salesforce as it's set up here to a colleague. The would recommend score is that share less the share answering 0 to 6. It's in the style of a net promoter score, and isn't one." },
};

const evidenceOf = (n: number, capturedAt: string): Evidence[] => [{ kind: "engine", path: "HealthCheckSurveyResponse", count: n, dateRange: null, exclusions: [], capturedAt, note: "Anonymous answers to the team survey, with comments left out" }];

/** Each role with five or more answers, on its own. Smaller ones are left out, not shown as too few. */
function byRole(answers: Answer[], value: (group: Answer[]) => number): MetricSegment[] {
  return ROLE_BANDS.flatMap((band) => {
    const group = answers.filter((a) => a.role === band.id);
    return group.length >= MIN_RESPONSES ? [{ dimension: "role", key: band.label, n: group.length, value: value(group), notMeasurableReason: null }] : [];
  });
}

const SMALL_ROLES = `Roles with fewer than ${MIN_RESPONSES} answers aren't shown on their own`;

/**
 * The survey's metrics, or one saying why there are none. Nothing at all when
 * the run has no survey. `capturedAt` is the report's date.
 */
export function surveyMetrics(survey: Survey | null | undefined, capturedAt: string): Metric[] {
  if (!survey) return [];
  const { answers, dropped } = readAnswers(survey);
  const evidence = evidenceOf(answers.length, capturedAt);
  const base = { domain: DOMAIN, evidence, exclusions: [...(dropped ? ["Answers that don't fit this version of the questions"] : []), "Comments, which never go in the documents"], assumptions: [] as string[] };
  if (answers.length < MIN_RESPONSES) {
    // No figure at all, not even how many answered: a line saying so is all.
    return [{ ...DEFS.responses, ...base, status: "not_measurable", reason: `Fewer than ${MIN_RESPONSES} people answered the team survey, so we don't show its results.` }];
  }
  const n = answers.length;
  const measured = (key: keyof typeof SURVEY, value: number, segments: MetricSegment[], stats: Record<string, number> = {}, exempt?: string): Metric => ({
    ...DEFS[key],
    ...base,
    status: "measured",
    value,
    n,
    dateRange: null,
    segments,
    segmentExemptReason: segments.length ? null : (exempt ?? SMALL_ROLES),
    series: [],
    stats,
    assumptions: segments.length ? [SMALL_ROLES] : [],
  });
  const med = (pick: (a: Answer) => number) => (group: Answer[]) => round1(median(group.map(pick)));
  const p75 = (pick: (a: Answer) => number) => round1(quantile(answers.map(pick), 0.75));
  const lost = (a: Answer): number => a.admin_hours + a.rekeying_hours;
  const promoters = (group: Answer[]) => pct(group.filter((a) => a.recommend >= 9).length, group.length);
  const detractors = (group: Answer[]) => pct(group.filter((a) => a.recommend <= 6).length, group.length);
  const held = (group: Answer[]) => pct(group.filter((a) => a.blockers.some((b) => b !== "nothing")).length, group.length);
  const commented = answers.filter((a) => a.commented).length;

  return [
    measured("responses", n, byRole(answers, (g) => g.length), { comments: commented }),
    measured("ease", med((a) => a.ease)(answers), byRole(answers, med((a) => a.ease))),
    measured("trust", med((a) => a.trust)(answers), byRole(answers, med((a) => a.trust))),
    measured("admin", med((a) => a.admin_hours)(answers), byRole(answers, med((a) => a.admin_hours)), { p75: p75((a) => a.admin_hours) }),
    measured("rekeying", med((a) => a.rekeying_hours)(answers), byRole(answers, med((a) => a.rekeying_hours)), { p75: p75((a) => a.rekeying_hours) }),
    measured("lost", med(lost)(answers), byRole(answers, med(lost)), { p75: p75(lost) }),
    measured(
      "blockers",
      held(answers),
      BLOCKERS.map((b) => ({ dimension: "blocker", key: b.label, n: answers.filter((a) => a.blockers.includes(b.id)).length, value: pct(answers.filter((a) => a.blockers.includes(b.id)).length, n), notMeasurableReason: null })).sort((a, b) => (b.value ?? 0) - (a.value ?? 0)),
    ),
    measured("recommend", promoters(answers), [
      { dimension: "answer", key: "9 or 10", n: answers.filter((a) => a.recommend >= 9).length, value: promoters(answers), notMeasurableReason: null },
      { dimension: "answer", key: "7 or 8", n: answers.filter((a) => a.recommend >= 7 && a.recommend <= 8).length, value: pct(answers.filter((a) => a.recommend >= 7 && a.recommend <= 8).length, n), notMeasurableReason: null },
      { dimension: "answer", key: "0 to 6", n: answers.filter((a) => a.recommend <= 6).length, value: detractors(answers), notMeasurableReason: null },
      ...byRole(answers, promoters),
    ], { score: round1(promoters(answers) - detractors(answers)) }),
  ];
}

/** The hours figures the documents write with "hours", for the number check. */
export function surveyHours(metrics: Metric[]): number[] {
  return metrics.filter((m) => m.status === "measured" && [SURVEY.admin, SURVEY.rekeying, SURVEY.lost].includes(m.id as never)).flatMap((m) => (m.status === "measured" ? [m.value, m.stats.p75 ?? m.value, ...m.segments.flatMap((s) => (s.value === null ? [] : [s.value]))] : []));
}
