import { SALES_CLOUD_DOMAINS, type Finding, type Run, type Score } from "../schemas/index.ts";

// The choices a report makes about its findings: what's top, what goes when, and
// what an executive reader sees. All of it is code, so it's repeatable and Claude
// has no say in the ranking.

export const SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"] as const;
const CONFIDENCE_ORDER = ["high", "medium", "low"] as const;

export const DOMAIN_LABELS: Record<string, string> = {
  configuration: "Configuration",
  automation: "Automation",
  security: "Security and access",
  best_practice: "Best practice",
  data_quality: "Data quality",
  utilisation: "Utilisation",
  current_pipeline: "Current pipeline",
  historic_pipeline: "Historic pipeline",
  forecast: "Forecast",
  activity: "Activity",
  lead_funnel: "Lead funnel",
  revenue_opportunities: "Revenue opportunities",
};

export const domainLabel = (domain: string): string => DOMAIN_LABELS[domain] ?? domain.replace(/_/g, " ").replace(/^./, (c) => c.toUpperCase());

/** The version 1 domains in report order, then anything a plug-in added. */
export function domainsInOrder(findings: Finding[]): string[] {
  const extra = [...new Set(findings.map((f) => f.domain))].filter((d) => !(SALES_CLOUD_DOMAINS as readonly string[]).includes(d)).sort();
  return [...SALES_CLOUD_DOMAINS, ...extra];
}

const USER_TOKEN = /\[\[user:[A-Z]\d{4}\]\]/;

/**
 * Executive reports stay at team level (see QUESTIONS.md, question 7). A token
 * would become a name at render, so it's replaced with a description before then.
 */
export function teamLevelText(text: string): string {
  return text
    .replace(/\b[Rr]ep \[\[user:[A-Z]\d{4}\]\]/g, "a single rep")
    .replace(/(^|[.!?]\s+)\[\[user:[A-Z]\d{4}\]\]/g, "$1A single person")
    .replace(/\[\[user:[A-Z]\d{4}\]\]/g, "a single person")
    .replace(/(^|[.!?]\s+)a single (rep|person)/g, (_m, lead: string, noun: string) => `${lead}A single ${noun}`);
}

export function teamLevelFinding(f: Finding): Finding {
  return {
    ...f,
    title: teamLevelText(f.title),
    summary: teamLevelText(f.summary),
    recommendation: teamLevelText(f.recommendation),
    subjects: f.subjects.filter((s) => !USER_TOKEN.test(s.label)),
  };
}

const sizedAmount = (f: Finding): number => (f.value.kind === "not_sized" ? -1 : f.value.amount);

export function bySeverity(a: Finding, b: Finding): number {
  return (
    SEVERITY_ORDER.indexOf(a.severity) - SEVERITY_ORDER.indexOf(b.severity) ||
    sizedAmount(b) - sizedAmount(a) ||
    CONFIDENCE_ORDER.indexOf(a.confidence) - CONFIDENCE_ORDER.indexOf(b.confidence) ||
    a.id.localeCompare(b.id)
  );
}

export const issuesOf = (findings: Finding[]): Finding[] => findings.filter((f) => f.status === "issue").sort(bySeverity);

// Team-level findings have already lost their token, so both forms are checked.
const aboutOnePerson = (f: Finding): boolean => USER_TOKEN.test(f.title) || /\ba single (?:rep|person)\b/i.test(f.title);

/** A finding about one person's results doesn't belong in an executive top list. */
export function topIssues(findings: Finding[], count: number): Finding[] {
  return issuesOf(findings).filter((f) => !aboutOnePerson(f)).slice(0, count);
}

export type PlanPhase = { key: string; title: string; from: number; to: number; findings: Finding[] };
export type Plan = { phases: PlanPhase[]; later: Finding[] };

// What one team can take on in each 30 days, as our working figure. A plan that
// puts most of a report in its first month is a list, not a plan.
export const PLAN_CAPACITY = [8, 10, 10] as const;

const SMALL = new Set(["XS", "S", "M"]);

/** The index of the first phase an action may go in. Urgent work that can be done soon goes first, and large builds and low-severity tidying go last. */
function earliestPhase(f: Finding): number {
  const urgent = f.severity === "critical" || f.severity === "high";
  if (!SMALL.has(f.effort.size)) return f.severity === "critical" ? 1 : 2;
  if (urgent || (f.severity === "medium" && f.effort.size === "XS")) return 0;
  return f.severity === "medium" ? 1 : 2;
}

/**
 * Severity and effort decide the phase, and each phase has a capacity. Actions
 * go in ranked order, each into the earliest phase it's allowed in that has
 * room, so urgent work overflows into the next month rather than off the plan.
 * What doesn't fit comes after 90 days.
 */
export function buildPlan(findings: Finding[]): Plan {
  const seen = new Set<string>();
  const actions = issuesOf(findings).filter((f) => {
    if (!f.recommendation || f.recommendationType === "none" || f.severity === "info" || seen.has(f.recommendation)) return false;
    seen.add(f.recommendation);
    return true;
  });
  const phases: PlanPhase[] = [
    { key: "stabilise", title: "Stabilise", from: 1, to: 30, findings: [] },
    { key: "fix", title: "Fix", from: 31, to: 60, findings: [] },
    { key: "build", title: "Build", from: 61, to: 90, findings: [] },
  ];
  const later: Finding[] = [];
  for (const f of actions) {
    const index = [0, 1, 2].slice(earliestPhase(f)).find((i) => phases[i]!.findings.length < PLAN_CAPACITY[i]!);
    (index === undefined ? later : phases[index]!.findings).push(f);
  }
  return { phases, later };
}

export type Verdict = { headline: string; critical: number; high: number; issues: number };

export function verdictOf(findings: Finding[]): Verdict {
  const issues = issuesOf(findings);
  const critical = issues.filter((f) => f.severity === "critical").length;
  const high = issues.filter((f) => f.severity === "high").length;
  const headline = critical > 0 ? "Urgent action needed" : high > 0 ? "Significant issues to fix" : issues.length > 0 ? "Sound, with issues to fix" : "No material issues found";
  return { headline, critical, high, issues: issues.length };
}

export type ScorecardRow = { domain: string; label: string; score: number | null; note: string; assessed: boolean; issues: number; high: number };

/** A set-up review (roadmap 2.2): set-up only, and no CRM records. */
export const isSetupReview = (run: Pick<Run, "scope">): boolean => run.scope === "essentials";

/** The areas a run covers, in order. A set-up review covers only its own, so the record areas are never shown as gaps. */
export function areasOf(run: Pick<Run, "scope" | "modules">, findings: Finding[]): string[] {
  const all = domainsInOrder(findings);
  return isSetupReview(run) ? all.filter((d) => run.modules.includes(d)) : all;
}

/** The areas a set-up review leaves to the full check, because they're measured from records. None for a complete run. */
export function recordAreas(run: Pick<Run, "scope" | "modules">): string[] {
  return isSetupReview(run) ? SALES_CLOUD_DOMAINS.filter((d) => !run.modules.includes(d)) : [];
}

/** One line, as a fact, for where a set-up review would otherwise list the record areas as gaps. */
export function recordAreasLine(run: Pick<Run, "scope" | "modules">): string | null {
  const areas = recordAreas(run).map((d) => domainLabel(d).toLowerCase());
  if (!areas.length) return null;
  const list = areas.length === 1 ? areas[0]! : `${areas.slice(0, -1).join(", ")} and ${areas.at(-1)}`;
  return `${list.charAt(0).toUpperCase()}${list.slice(1)} are measured from your CRM records, which a set-up review doesn't read. They're part of the full Barometer check.`;
}

/** Never a zero for a gap: a domain with no score says why it hasn't got one. */
export function scorecard(findings: Finding[], scores: Score[], areas: string[] = domainsInOrder(findings)): ScorecardRow[] {
  return areas.map((domain) => {
    const own = findings.filter((f) => f.domain === domain);
    const issues = own.filter((f) => f.status === "issue");
    const score = scores.find((s) => s.domain === domain);
    const base = { domain, label: domainLabel(domain), assessed: own.length > 0, issues: issues.length, high: issues.filter((f) => f.severity === "critical" || f.severity === "high").length };
    if (score?.status === "measured") return { ...base, score: score.score, note: "" };
    if (score) return { ...base, score: null, note: score.reason };
    return { ...base, score: null, note: own.length ? "Not scored in this run" : "Not assessed in this run" };
  });
}
