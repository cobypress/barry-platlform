import { pct } from "../engine/rule.ts";
import { median, quantile, round1 } from "../domains/historic-pipeline/metrics.ts";
import type { Evidence, Metric, MetricSegment } from "../schemas/index.ts";
import { BLOCKERS, readAnswers, ROLE_BANDS, type Survey, type SurveyAnswers } from "./questions.ts";

// What the team survey says, as metrics in their own "survey" domain: they
// describe the people, not the org, and no area is scored from them. Built when
// the report is, because answers keep arriving after the org is read. Fewer
// than five answers and nothing is shown (DECISIONS.md 141), and no figure lets
// a reader work out fewer than five people's answers, on its own or by
// subtracting one shown group from another or from the total (648).

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

// Under five answers even the count is left out: it's a figure from fewer people
// than the survey shows, and the run's stored metrics hold it as the report does (DECISIONS.md 645).
const evidenceOf = (n: number, capturedAt: string): Evidence[] => [{ kind: "engine", path: "HealthCheckSurveyResponse", count: n < MIN_RESPONSES ? null : n, dateRange: null, exclusions: [], capturedAt, note: "Anonymous answers to the team survey, with comments left out" }];

/**
 * k people out of m, safe to show (DECISIONS.md 648): none or five or more, and
 * none or five or more left over, so neither the count nor what's left of the
 * group can be fewer than five people a reader can single out.
 */
export const safeCount = (k: number, m: number): boolean => (k === 0 || k >= MIN_RESPONSES) && (m - k === 0 || m - k >= MIN_RESPONSES);

type Role = { id: string; label: string; n: number };

/**
 * The roles shown on their own (648): each with five or more answers, largest
 * first, folding the smallest back while the answers outside every shown role
 * number one to four, so whoever is outside them is none or five or more. A
 * group of small roles shown as one could be under five itself, so they're
 * folded back, never merged. None when no split is safe.
 */
export function shownRoles(answers: Answer[]): Role[] {
  const roles = ROLE_BANDS.map((b) => ({ id: b.id as string, label: b.label as string, n: answers.filter((a) => a.role === b.id).length }))
    .filter((r) => r.n >= MIN_RESPONSES)
    .sort((x, y) => y.n - x.n);
  for (;;) {
    const outside = answers.length - roles.reduce((sum, r) => sum + r.n, 0);
    if (outside === 0 || outside >= MIN_RESPONSES || roles.length === 0) return roles;
    roles.pop();
  }
}

/** Each shown role, on its own. */
function byRole(answers: Answer[], roles: Role[], value: (group: Answer[]) => number): MetricSegment[] {
  return roles.map((role) => {
    const group = answers.filter((a) => a.role === role.id);
    return { dimension: "role", key: role.label, n: group.length, value: value(group), notMeasurableReason: null };
  });
}

const SMALL_ROLES = `A role is shown on its own only where ${MIN_RESPONSES} or more people gave it, and none or ${MIN_RESPONSES} or more gave every other role`;
/** Why a count isn't shown, in the words a page prints: fewer than five gave the answer, or fewer than five didn't. */
const fewer = (did: string): string => `fewer than ${MIN_RESPONSES} people ${did}, or fewer than ${MIN_RESPONSES} didn't`;

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
    return [{ ...DEFS.responses, ...base, status: "not_measurable", reason: `Fewer than ${MIN_RESPONSES} people answered the team survey, so we don't show its results.`, notMeasurableKind: "too_little_data" as const }];
  }
  const n = answers.length;
  const roles = shownRoles(answers);
  const measured = (key: keyof typeof SURVEY, value: number, segments: MetricSegment[], stats: Record<string, number> = {}, exempt?: string, notShown: string[] = []): Metric => ({
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
    assumptions: [...(segments.length ? [SMALL_ROLES] : []), ...notShown],
  });
  /** A figure that would let a reader count fewer than five people: its metric says why there's none. */
  const withheld = (key: keyof typeof SURVEY, what: string): Metric => ({ ...DEFS[key], ...base, status: "not_measurable", reason: `We don't show this, because ${what}.`, notMeasurableKind: "too_little_data" as const });
  const med = (pick: (a: Answer) => number) => (group: Answer[]) => round1(median(group.map(pick)));
  const p75 = (pick: (a: Answer) => number) => round1(quantile(answers.map(pick), 0.75));
  const lost = (a: Answer): number => a.admin_hours + a.rekeying_hours;
  const count = (group: Answer[], test: (a: Answer) => boolean) => group.filter(test).length;
  const promoter = (a: Answer) => a.recommend >= 9;
  const passive = (a: Answer) => a.recommend >= 7 && a.recommend <= 8;
  const detractor = (a: Answer) => a.recommend <= 6;
  const isHeld = (a: Answer) => a.blockers.some((b) => b !== "nothing");
  const commented = count(answers, (a) => a.commented);

  // Each blocker on its own: one that isn't safe is left out and named, never counted.
  const blockerCounts = BLOCKERS.map((b) => ({ label: b.label, k: count(answers, (a) => a.blockers.includes(b.id)) }));
  const blockerRows = blockerCounts.filter((b) => safeCount(b.k, n)).map((b) => ({ dimension: "blocker", key: b.label, n: b.k, value: pct(b.k, n), notMeasurableReason: null })).sort((a, b) => (b.value ?? 0) - (a.value ?? 0));
  const blockersHidden = blockerCounts.filter((b) => !safeCount(b.k, n)).map((b) => b.label);
  const held = count(answers, isHeld);
  const blockers = safeCount(held, n)
    ? measured("blockers", pct(held, n), blockerRows, {}, `None is shown, because for each one ${fewer("chose it")}`, blockersHidden.length ? [`Each of these isn't shown, because ${fewer("chose it")}: ${blockersHidden.join("; ")}`] : [])
    : withheld("blockers", fewer("named something that gets in their way"));

  // The three answers to recommend add up to everyone, so any two give the third: all three or none.
  const bands = [promoter, passive, detractor].map((test) => count(answers, test));
  const bandsSafe = bands.every((k) => safeCount(k, n));
  const promoters = bands[0]!;
  // A role's share of 9 or 10 is shown only if every count a reader could work out from it is safe, those outside the shown roles included.
  const inRoles = roles.reduce((sum, r) => sum + r.n, 0);
  const roleCells = roles.map((r) => ({ k: count(answers.filter((a) => a.role === r.id), promoter), m: r.n }));
  const outsideCell = { k: promoters - roleCells.reduce((sum, c) => sum + c.k, 0), m: n - inRoles };
  const crossSafe = roles.length > 0 && [...roleCells, ...(outsideCell.m > 0 ? [outsideCell] : [])].every((c) => safeCount(c.k, c.m));
  const recommend = safeCount(promoters, n)
    ? measured(
        "recommend",
        pct(promoters, n),
        [
          ...(bandsSafe
            ? [
                { dimension: "answer", key: "9 or 10", n: bands[0]!, value: pct(bands[0]!, n), notMeasurableReason: null },
                { dimension: "answer", key: "7 or 8", n: bands[1]!, value: pct(bands[1]!, n), notMeasurableReason: null },
                { dimension: "answer", key: "0 to 6", n: bands[2]!, value: pct(bands[2]!, n), notMeasurableReason: null },
              ]
            : []),
          ...(crossSafe ? byRole(answers, roles, (g) => pct(count(g, promoter), g.length)) : []),
        ],
        bandsSafe ? { score: round1(pct(bands[0]!, n) - pct(bands[2]!, n)) } : {},
        `Not split, because for at least one answer ${fewer("gave it")}`,
        [...(bandsSafe ? [] : [`How the answers split isn't shown, because for at least one answer ${fewer("gave it")}`]), ...(roles.length > 0 && !crossSafe ? [`The share by role isn't shown, because in a role, or outside the roles shown, ${fewer("answered 9 or 10")}`] : [])],
      )
    : withheld("recommend", fewer("answered 9 or 10"));

  return [
    measured("responses", n, byRole(answers, roles, (g) => g.length), safeCount(commented, n) ? { comments: commented } : {}),
    measured("ease", med((a) => a.ease)(answers), byRole(answers, roles, med((a) => a.ease))),
    measured("trust", med((a) => a.trust)(answers), byRole(answers, roles, med((a) => a.trust))),
    measured("admin", med((a) => a.admin_hours)(answers), byRole(answers, roles, med((a) => a.admin_hours)), { p75: p75((a) => a.admin_hours) }),
    measured("rekeying", med((a) => a.rekeying_hours)(answers), byRole(answers, roles, med((a) => a.rekeying_hours)), { p75: p75((a) => a.rekeying_hours) }),
    measured("lost", med(lost)(answers), byRole(answers, roles, med(lost)), { p75: p75(lost) }),
    blockers,
    recommend,
  ];
}

/** The hours figures the documents write with "hours", for the number check. */
export function surveyHours(metrics: Metric[]): number[] {
  return metrics.filter((m) => m.status === "measured" && [SURVEY.admin, SURVEY.rekeying, SURVEY.lost].includes(m.id as never)).flatMap((m) => (m.status === "measured" ? [m.value, m.stats.p75 ?? m.value, ...m.segments.flatMap((s) => (s.value === null ? [] : [s.value]))] : []));
}
