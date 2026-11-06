import { issuesOf } from "../narrative/select.ts";
import { percent } from "../render/format.ts";
import type { Finding, Metric } from "../schemas/index.ts";
import { CONCERN_AT, REKEYING_AT, hours, isRekeyingCheck } from "../survey/pairings.ts";
import { INTERVIEW_ROLES, questionFor, readInterviewAnswers, roleLabel, type Interview, type InterviewRole } from "./questions.ts";

// What people told us in interviews, set against what the data shows. Code
// reads each structured answer against a matching measure and says whether
// they agree, disagree or we can't tell (DECISIONS.md 181). Notes are never
// read here: they go to Claude, guarded, and a consultant reviews what it writes.
//
// A figure someone gave us is their statement. Every sentence that quotes one
// says it was said, and the number check holds it to that (DECISIONS.md 183).

export type Reading = "agrees" | "disagrees" | "cant_tell";

export const READING_LABELS: Record<Reading, string> = { agrees: "Agrees", disagrees: "Disagrees", cant_tell: "Can't tell" };

/** Within this many points, a share someone gave us agrees with the one we measured. */
export const SHARE_WITHIN_POINTS = 10;
/** Within this share of the measured figure, a count or a number of days agrees: "about 60 days" against 77 doesn't (DECISIONS.md 406). */
export const COUNT_WITHIN_SHARE = 0.15;
/** A count this close always agrees, however small: "6 systems" against 8 isn't a disagreement. */
export const COUNT_WITHIN_AT_LEAST = 2;
/** A theme reaches the executive summary when at least this many people's answers read the same way, and none the other. */
export const STRONG_AT = 2;
/** Strong themes the executive summary prints at most, most people first. */
export const STRONG_ON_EXECUTIVE = 3;

export type InterviewTheme = {
  /** The interview's source in the dataset, "interview:1". */
  source: string;
  role: InterviewRole;
  roleLabel: string;
  question: string;
  topic: string;
  /** What they told us, in one sentence, written as what was said. */
  said: string;
  /** What the data shows, in one sentence. */
  found: string;
  reading: Reading;
  /** Why it reads that way. */
  why: string;
  /** True when what they said is a problem: a low rating, or time lost. Null for a figure. */
  felt: boolean | null;
  issues: Finding[];
  metricId: string | null;
};

type IssueMeasure = { kind: "issues"; topic: string; issues: (f: Finding) => boolean; felt: (v: number) => boolean; said: (who: string, v: number) => string; context?: (metrics: Metric[]) => string[] };
type MetricMeasure = { kind: "metric"; topic: string; metric: string; unit: "percent" | "days" | "count"; said: (who: string, v: number) => string; found: (v: number) => string };
type NoMeasure = { kind: "none"; topic: string; said: (who: string, v: number) => string; why: string };
type Measure = IssueMeasure | MetricMeasure | NoMeasure;

const STAGE_RULES = new Set(["pipe.skipped-stages", "pipe.going-backwards", "pipe.dead-end-stage", "pipe.unmapped-stages", "cfg.stage-sprawl"]);

const valueOf = (metrics: Metric[], id: string): number | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m.value : null;
};

/** Each structured question's matching measure. A question with none is still shown, as one we can't tell. */
const MEASURES: Record<string, Measure> = {
  trust: {
    kind: "issues",
    topic: "Trust in the pipeline figures",
    issues: (f) => f.domain === "data_quality" || f.domain === "forecast",
    felt: (v) => v <= CONCERN_AT,
    said: (who, v) => `${who} rated their trust in the pipeline figures ${v} out of 5.`,
  },
  ease: {
    kind: "issues",
    topic: "How easy Salesforce is to use",
    issues: (f) => f.domain === "adoption",
    felt: (v) => v <= CONCERN_AT,
    said: (who, v) => `${who} rated how easy Salesforce is to use ${v} out of 5.`,
    context: (metrics) => {
      const active = valueOf(metrics, "adoption.monthly_active_share");
      return active === null ? [] : [`${percent(active)} of users logged in last month`];
    },
  },
  stages: {
    kind: "issues",
    topic: "Whether deals follow the stages",
    issues: (f) => STAGE_RULES.has(f.ruleId),
    felt: (v) => v <= CONCERN_AT,
    said: (who, v) => `${who} rated how closely deals follow the stages ${v} out of 5.`,
  },
  automation: {
    kind: "issues",
    topic: "Changing automation safely",
    issues: (f) => f.domain === "automation",
    felt: (v) => v <= CONCERN_AT,
    said: (who, v) => `${who} rated their confidence changing automation ${v} out of 5.`,
  },
  rekeying: {
    kind: "issues",
    topic: "Typing data in again",
    issues: isRekeyingCheck,
    felt: (v) => v >= REKEYING_AT,
    said: (who, v) => `${who} told us about ${hours(v)} a week go on typing data in again or keeping a spreadsheet alongside Salesforce.`,
  },
  win_rate: {
    kind: "metric",
    topic: "Win rate",
    metric: "historic.win_rate",
    unit: "percent",
    said: (who, v) => `${who} told us about ${v}% of deals that reach a decision are won.`,
    found: (v) => `We measured a win rate of ${percent(v)}.`,
  },
  forecast_hit: {
    kind: "metric",
    topic: "Deals closing when forecast",
    metric: "historic.close_date_reliability",
    unit: "percent",
    said: (who, v) => `${who} told us about ${v}% of won deals close in the quarter first forecast.`,
    found: (v) => `We measured ${percent(v)} of won deals closing in the quarter first forecast.`,
  },
  sales_cycle: {
    kind: "metric",
    topic: "How long deals take",
    metric: "historic.sales_cycle_won",
    unit: "days",
    said: (who, v) => `${who} told us a typical won deal takes about ${v} ${v === 1 ? "day" : "days"} from creation to close.`,
    found: (v) => `We measured a median of ${v} ${v === 1 ? "day" : "days"} from creation to close for won deals.`,
  },
  weekly_users: {
    kind: "metric",
    topic: "People using Salesforce each week",
    metric: "adoption.weekly_active_users",
    unit: "count",
    said: (who, v) => `${who} told us about ${v} ${v === 1 ? "person uses" : "people use"} Salesforce in a typical week.`,
    found: (v) => `We measured ${v} weekly active ${v === 1 ? "user" : "users"}.`,
  },
  integrations: {
    kind: "metric",
    topic: "Systems connected to Salesforce",
    metric: "documented.integrations",
    unit: "count",
    said: (who, v) => `${who} told us about ${v} other ${v === 1 ? "system sends" : "systems send"} data to Salesforce or read from it.`,
    found: (v) => `We found ${v} integration ${v === 1 ? "point" : "points"} in the org.`,
  },
  admin_hours: {
    kind: "none",
    topic: "Time on admin",
    said: (who, v) => `${who} told us about ${hours(v)} a week go on Salesforce admin that doesn't help anyone sell.`,
    why: "Salesforce doesn't record how long admin takes, so we can't set this against the data.",
  },
};

/** "Your sales director", "A sales rep": one person, by role. */
const ONE: Record<InterviewRole, string> = {
  sales_leader: "Your sales director",
  sales_manager: "A sales manager",
  sales_ops: "Someone in sales operations",
  admin: "Your Salesforce admin",
  sales_rep: "A sales rep",
  finance: "Someone in finance",
  marketing: "Someone in marketing",
  other: "Someone in another role",
};
/** "2 sales reps": more than one. */
const MANY: Record<InterviewRole, string> = {
  sales_leader: "sales directors",
  sales_manager: "sales managers",
  sales_ops: "people in sales operations",
  admin: "Salesforce admins",
  sales_rep: "sales reps",
  finance: "people in finance",
  marketing: "people in marketing",
  other: "people in other roles",
};

export const whoSaid = (role: InterviewRole): string => ONE[role];

/** "your sales director, 2 sales reps and your Salesforce admin", in the roles' order. */
export function rolesPhrase(roles: InterviewRole[]): string {
  const counts = new Map<InterviewRole, number>();
  for (const r of roles) counts.set(r, (counts.get(r) ?? 0) + 1);
  const parts = INTERVIEW_ROLES.flatMap(({ id }) => {
    const n = counts.get(id);
    if (!n) return [];
    return [n === 1 ? ONE[id].charAt(0).toLowerCase() + ONE[id].slice(1) : `${n} ${MANY[id]}`];
  });
  return parts.length <= 1 ? (parts[0] ?? "") : `${parts.slice(0, -1).join(", ")} and ${parts.at(-1)}`;
}

/** The dataset source for an interview, by its place in the list. */
export const interviewSource = (n: number): string => `interview:${n}`;
export const saidIn = (role: InterviewRole): string => `said in interview, ${roleLabel(role)}`;

function readIssues(m: IssueMeasure, value: number, findings: Finding[], metrics: Metric[]): Pick<InterviewTheme, "found" | "reading" | "why" | "felt" | "issues"> {
  const felt = m.felt(value);
  const own = findings.filter(m.issues);
  const issues = issuesOf(own);
  const topic = m.topic.charAt(0).toLowerCase() + m.topic.slice(1);
  // Nothing checked, or nothing we could measure: no basis to agree or disagree.
  if (own.every((f) => f.status === "not_measurable")) {
    return { found: `We couldn't check this in your org.`, reading: "cant_tell", why: "None of the checks that bear on it could be measured.", felt, issues: [] };
  }
  const context = m.context?.(metrics) ?? [];
  const found = `${issues.length === 0 ? "We found no issues" : `We found ${issues.length} ${issues.length === 1 ? "issue" : "issues"}`} that bear on ${topic}${context.length ? `, and ${context.join(" and ")}` : ""}.`;
  const backed = issues.length > 0;
  if (felt === backed) return { found, reading: "agrees", why: felt ? "What we found backs this up." : "The data doesn't show a problem either.", felt, issues };
  return { found, reading: "disagrees", why: felt ? "We didn't find a cause in what we read." : "What we found suggests it's worse than it feels.", felt, issues };
}

function readMetric(m: MetricMeasure, value: number, metrics: Metric[]): Pick<InterviewTheme, "found" | "reading" | "why" | "metricId"> {
  const measured = valueOf(metrics, m.metric);
  if (measured === null) return { found: "We couldn't measure this in your org.", reading: "cant_tell", why: "There's no figure from the data to set it against.", metricId: m.metric };
  const gap = value - measured;
  const close = m.unit === "percent" ? Math.abs(gap) <= SHARE_WITHIN_POINTS : Math.abs(gap) <= Math.max(COUNT_WITHIN_AT_LEAST, Math.abs(measured) * COUNT_WITHIN_SHARE);
  if (close) return { found: m.found(measured), reading: "agrees", why: "Close to what we measured.", metricId: m.metric };
  return { found: m.found(measured), reading: "disagrees", why: gap > 0 ? "Higher than the data shows." : "Lower than the data shows.", metricId: m.metric };
}

/** Interviews in the order they were held, so sources and the chapter read the same way every time. */
export function ordered(interviews: Interview[]): Interview[] {
  return interviews.slice().sort((a, b) => a.heldOn.localeCompare(b.heldOn) || a.id.localeCompare(b.id));
}

/** Every structured answer, read against its measure. Text answers aren't read: they go with the notes. */
export function buildThemes(interviews: Interview[], findings: Finding[], metrics: Metric[]): InterviewTheme[] {
  return ordered(interviews).flatMap((interview, i) => {
    const answers = readInterviewAnswers(interview);
    const who = whoSaid(interview.role);
    return Object.entries(answers).flatMap(([question, value]): InterviewTheme[] => {
      const measure = MEASURES[question];
      if (!measure || typeof value !== "number" || questionFor(question)?.kind === "text") return [];
      const base = { source: interviewSource(i + 1), role: interview.role, roleLabel: roleLabel(interview.role), question, topic: measure.topic, said: measure.said(who, value) };
      if (measure.kind === "none") return [{ ...base, found: "Not something Salesforce records.", reading: "cant_tell", why: measure.why, felt: null, issues: [], metricId: null }];
      if (measure.kind === "issues") return [{ ...base, ...readIssues(measure, value, findings, metrics), metricId: null }];
      return [{ ...base, ...readMetric(measure, value, metrics), felt: null, issues: [] }];
    });
  });
}

export type StrongTheme = { question: string; topic: string; reading: Exclude<Reading, "cant_tell">; themes: InterviewTheme[]; sentence: string };

/**
 * Themes where at least STRONG_AT people's answers read the same way and
 * none reads the other way. Only these reach the executive summary.
 */
export function strongThemes(themes: InterviewTheme[]): StrongTheme[] {
  const byQuestion = new Map<string, InterviewTheme[]>();
  for (const t of themes) if (t.reading !== "cant_tell") byQuestion.set(t.question, [...(byQuestion.get(t.question) ?? []), t]);
  return [...byQuestion.entries()].flatMap(([question, list]): StrongTheme[] => {
    const reading = list[0]!.reading as Exclude<Reading, "cant_tell">;
    if (list.length < STRONG_AT || list.some((t) => t.reading !== reading)) return [];
    // A problem felt by some and not others isn't one message, even if each agrees with the data.
    if (list.some((t) => t.felt !== list[0]!.felt)) return [];
    const topic = list[0]!.topic;
    const who = rolesPhrase(list.map((t) => t.role));
    const felt = list[0]!.felt;
    const what =
      felt === null
        ? reading === "agrees"
          ? "gave us figures close to what we measured"
          : `gave us figures ${list.every((t) => t.why.startsWith("Higher")) ? "higher than" : list.every((t) => t.why.startsWith("Lower")) ? "lower than" : "different from"} what we measured`
        : felt
          ? reading === "agrees"
            ? "told us this is a problem, and what we found backs them up"
            : "told us this is a problem, and we didn't find a cause in what we read"
          : reading === "agrees"
            ? "told us this works, and the data agrees"
            : "told us this works, but what we found suggests it's worse than it feels";
    return [{ question, topic, reading, themes: list, sentence: `${topic}: ${who} ${what}.` }];
  }).sort((a, b) => b.themes.length - a.themes.length);
}

/**
 * The strong themes worth an executive's time: a problem people feel that the
 * data backs up, or a gap between what people believe and what the data shows.
 * Agreement that something works, or a figure close to ours, stays in the chapter.
 */
export function executiveThemes(view: InterviewView | null): StrongTheme[] {
  return (view?.strong ?? []).filter((t) => t.reading === "disagrees" || t.themes[0]!.felt === true).slice(0, STRONG_ON_EXECUTIVE);
}

export type InterviewView = {
  /** How many interviews were recorded, and with whom. */
  count: number;
  roles: InterviewRole[];
  themes: InterviewTheme[];
  strong: StrongTheme[];
  /** The chapter's opening, built by code. */
  paragraph: string;
};

const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** Null when the run has no interviews. */
export function buildInterviewView(interviews: Interview[] | null | undefined, findings: Finding[], metrics: Metric[]): InterviewView | null {
  if (!interviews?.length) return null;
  const list = ordered(interviews);
  const themes = buildThemes(list, findings, metrics);
  const strong = strongThemes(themes);
  const roles = list.map((i) => i.role);
  const counts = { agrees: themes.filter((t) => t.reading === "agrees").length, disagrees: themes.filter((t) => t.reading === "disagrees").length, cant: themes.filter((t) => t.reading === "cant_tell").length };
  const tally = [counts.agrees ? `${counts.agrees} ${counts.agrees === 1 ? "agrees" : "agree"}` : "", counts.disagrees ? `${counts.disagrees} ${counts.disagrees === 1 ? "disagrees" : "disagree"}` : "", counts.cant ? `${counts.cant} we can't tell` : ""].filter(Boolean);
  const tallied = tally.length <= 1 ? tally.join("") : `${tally.slice(0, -1).join(", ")} and ${tally.at(-1)}`;
  const paragraph = [
    `We interviewed ${plural(list.length, "person", "people")}: ${rolesPhrase(roles)}.`,
    themes.length ? `We set ${themes.length} of their answers against what we found in Salesforce: ${tallied}.` : "",
    "What someone tells us in an interview is their view, not a measure. Where we quote a figure someone gave us, we say so.",
  ]
    .filter(Boolean)
    .join(" ");
  return { count: list.length, roles, themes, strong, paragraph };
}

/** The counts the chapter prints that no metric holds, for the number check. */
export function interviewViewFigures(view: InterviewView | null): number[] {
  if (!view) return [];
  const byRole = new Map<InterviewRole, number>();
  for (const r of view.roles) byRole.set(r, (byRole.get(r) ?? 0) + 1);
  return [
    view.count,
    view.themes.length,
    ...["agrees", "disagrees", "cant_tell"].map((r) => view.themes.filter((t) => t.reading === r).length),
    ...byRole.values(),
    ...view.themes.map((t) => t.issues.length),
    ...view.strong.map((s) => s.themes.length),
    CONCERN_AT,
    5,
    STRONG_AT,
    SHARE_WITHIN_POINTS,
  ];
}

/**
 * What Claude is given for the chapter: each interview's notes and answers
 * under its own source, and the readings code made. Who held the interview
 * and when stay out. The notes are guarded on the way like every source
 * (guardDeep, then sendPrompt), so a known name becomes a token, and the
 * chapter's brief refuses a draft that keeps one (DECISIONS.md 184).
 */
export function interviewSources(interviews: Interview[], view: InterviewView): Array<{ id: string; kind: string; [key: string]: unknown }> {
  const list = ordered(interviews);
  return [
    {
      id: INTERVIEWS_SOURCE,
      kind: "interview_readings",
      interviews: view.count,
      readings: view.themes.map((t) => ({ interview: t.source, role: t.roleLabel, topic: t.topic, theySaid: t.said, whatTheDataShows: t.found, reading: READING_LABELS[t.reading], why: t.why })),
    },
    ...list.map((interview, i) => ({
      id: interviewSource(i + 1),
      kind: "interview",
      role: roleLabel(interview.role),
      saidAnswers: interviewSaid(interview),
      notes: interview.notes,
    })),
  ];
}

export const INTERVIEWS_SOURCE = "interviews";

/** An interview's answers as sentences of what was said, text answers included. For the dataset and for Claude. */
export function interviewSaid(interview: Interview): string[] {
  const who = whoSaid(interview.role);
  return Object.entries(readInterviewAnswers(interview)).map(([question, value]) => {
    if (typeof value === "string") return `${who} told us the one thing they'd fix first: ${value}`;
    return MEASURES[question]?.said(who, value) ?? "";
  }).filter(Boolean);
}

/** The text the number check reads for each interview, marked as said, and the readings', which aren't. */
export function interviewDatasetText(interviews: Interview[], view: InterviewView): Array<{ source: string; text: string; said?: string }> {
  return [
    { source: INTERVIEWS_SOURCE, text: view.themes.map((t) => `${t.found} ${t.why}`).join("\n") },
    ...ordered(interviews).map((interview, i) => ({ source: interviewSource(i + 1), text: [...interviewSaid(interview), interview.notes].join("\n"), said: saidIn(interview.role) })),
  ];
}
