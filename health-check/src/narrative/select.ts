import { SALES_CLOUD_DOMAINS, type Finding, type Score } from "../schemas/index.ts";

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

/** A finding about one person's results doesn't belong in an executive top list. */
export function topIssues(findings: Finding[], count: number): Finding[] {
  return issuesOf(findings).filter((f) => !USER_TOKEN.test(f.title)).slice(0, count);
}

export type PlanPhase = { key: string; title: string; from: number; to: number; findings: Finding[] };

/** Effort decides the phase: small fixes first, so the plan starts with what can be done now. */
export function buildPlan(findings: Finding[]): PlanPhase[] {
  const seen = new Set<string>();
  const actions = issuesOf(findings).filter((f) => {
    if (!f.recommendation || f.recommendationType === "none" || seen.has(f.recommendation)) return false;
    seen.add(f.recommendation);
    return true;
  });
  const phase = (sizes: string[]): Finding[] => actions.filter((f) => sizes.includes(f.effort.size));
  return [
    { key: "stabilise", title: "Stabilise", from: 1, to: 30, findings: phase(["XS", "S"]) },
    { key: "fix", title: "Fix", from: 31, to: 60, findings: phase(["M"]) },
    { key: "build", title: "Build", from: 61, to: 90, findings: phase(["L", "XL"]) },
  ];
}

export type Verdict = { headline: string; critical: number; high: number; issues: number };

export function verdictOf(findings: Finding[]): Verdict {
  const issues = issuesOf(findings);
  const critical = issues.filter((f) => f.severity === "critical").length;
  const high = issues.filter((f) => f.severity === "high").length;
  const headline = critical > 0 ? "Urgent action needed" : high > 0 ? "Significant issues to fix" : issues.length > 0 ? "Sound, with issues to fix" : "No material issues found";
  return { headline, critical, high, issues: issues.length };
}

export type ScorecardRow = { domain: string; label: string; score: number | null; note: string; issues: number; high: number };

/** Never a zero for a gap: a domain with no score says why it hasn't got one. */
export function scorecard(findings: Finding[], scores: Score[]): ScorecardRow[] {
  return domainsInOrder(findings).map((domain) => {
    const own = findings.filter((f) => f.domain === domain);
    const issues = own.filter((f) => f.status === "issue");
    const score = scores.find((s) => s.domain === domain);
    const base = { domain, label: domainLabel(domain), issues: issues.length, high: issues.filter((f) => f.severity === "critical" || f.severity === "high").length };
    if (score?.status === "measured") return { ...base, score: score.score, note: "" };
    if (score) return { ...base, score: null, note: score.reason };
    return { ...base, score: null, note: own.length ? "Not scored in this run" : "Not assessed in this run" };
  });
}
