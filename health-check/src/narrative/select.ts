import { SALES_CLOUD_DOMAINS, type Finding, type Run, type Score } from "../schemas/index.ts";
import { NOT_MONEY_RULES } from "../value/model.ts";

// The choices a report makes about its findings: what's top, what goes when, and
// what an executive reader sees. All of it is code, so it's repeatable and Claude
// has no say in the ranking.

export const SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"] as const;
const CONFIDENCE_ORDER = ["high", "medium", "low"] as const;

export const DOMAIN_LABELS: Record<string, string> = {
  configuration: "Configuration",
  automation: "Automation",
  technical_health: "Technical health",
  security: "Security and access",
  integrations: "Integrations",
  best_practice: "Best practice",
  data_quality: "Data quality",
  utilisation: "Utilisation",
  adoption: "Adoption and usage",
  current_pipeline: "Current pipeline",
  historic_pipeline: "Historic pipeline",
  forecast: "Forecast",
  activity: "Activity",
  lead_funnel: "Lead funnel",
  revenue_opportunities: "Revenue opportunities",
  service_operations: "Service operations",
  service_setup: "Service set-up",
  cpq_quoting: "Quoting and discounts",
  cpq_setup: "CPQ set-up",
  account_engagement: "Marketing handover",
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
export function teamLevelText(text: string, kind: "text" | "title" = "text"): string {
  // In a sentence, the first person named is "one rep", any after it "another", so two can be told apart
  // (night 3 reading pass). A title keeps "a single rep": it's how the ranking knows a finding is about one person.
  let named = 0;
  const who = (noun: string) => (kind === "title" ? `a single ${noun}` : `${named++ === 0 ? "one" : "another"} ${noun}`);
  return text
    .replace(/Worst by rep: \[\[user:[A-Z]\d{4}\]\], at/g, "The worst rep is at")
    .replace(/\b[Rr]ep \[\[user:[A-Z]\d{4}\]\]|\[\[user:[A-Z]\d{4}\]\]/g, (m) => who(/^[Rr]ep /.test(m) ? "rep" : "person"))
    .replace(/(^|[.!?]\s+)(one|another|a single) (rep|person)/g, (_m, lead: string, first: string, noun: string) => `${lead}${first.charAt(0).toUpperCase()}${first.slice(1)} ${noun}`);
}

export function teamLevelFinding(f: Finding): Finding {
  return {
    ...f,
    title: teamLevelText(f.title, "title"),
    summary: teamLevelText(f.summary),
    recommendation: teamLevelText(f.recommendation),
    subjects: f.subjects.filter((s) => !USER_TOKEN.test(s.label)),
  };
}

// A target such as pipeline needed for coverage isn't money, so it doesn't rank as money (DECISIONS.md 351):
// it comes after every issue with a figure in money, and before those with none.
const sizedAmount = (f: Finding): number => (f.value.kind === "not_sized" ? -1 : NOT_MONEY_RULES.has(f.ruleId) ? 0 : f.value.amount);

/**
 * What each issue adds to the total, after the overlap rules, by finding id
 * (DECISIONS.md 362): its expected figure when it's the one its group counts,
 * nothing when a larger overlapping issue holds the money, or when its figure is
 * exposure or a target. An issue with no figure isn't in it.
 */
export type Added = ReadonlyMap<string, number>;

const rankAmount = (f: Finding, added?: Added): number => (added ? (added.get(f.id) ?? (f.value.kind === "not_sized" ? -1 : 0)) : sizedAmount(f));

/** Severity first, then money: what each adds to the total when that's known, else the issue's own figure. */
export const rankBy =
  (added?: Added) =>
  (a: Finding, b: Finding): number =>
    SEVERITY_ORDER.indexOf(a.severity) - SEVERITY_ORDER.indexOf(b.severity) ||
    rankAmount(b, added) - rankAmount(a, added) ||
    // Issues that add the same, such as nothing, follow their own figures.
    (added ? sizedAmount(b) - sizedAmount(a) : 0) ||
    CONFIDENCE_ORDER.indexOf(a.confidence) - CONFIDENCE_ORDER.indexOf(b.confidence) ||
    a.id.localeCompare(b.id);

export const bySeverity = rankBy();

export const issuesOf = (findings: Finding[], added?: Added): Finding[] => findings.filter((f) => f.status === "issue").sort(rankBy(added));

// Team-level findings have already lost their token, so both forms are checked.
const aboutOnePerson = (f: Finding): boolean => USER_TOKEN.test(f.title) || /\ba single (?:rep|person)\b/i.test(f.title);

/** A finding about one person's results doesn't belong in an executive top list. */
export function topIssues(findings: Finding[], count: number, added?: Added): Finding[] {
  return issuesOf(findings, added).filter((f) => !aboutOnePerson(f)).slice(0, count);
}

export type PlanPhase = { key: string; title: string; from: number; to: number; findings: Finding[] };
/** `alsoFor` holds, by an action's finding id, the other issues from the same rule that the same action covers. */
export type Plan = { phases: PlanPhase[]; later: Finding[]; alsoFor: Record<string, Finding[]> };

// What one team can take on in each 30 days, as our working figure. A plan that
// puts most of a report in its first month is a list, not a plan.
export const PLAN_CAPACITY = [8, 10, 10] as const;

const SMALL = new Set(["XS", "S", "M"]);

/** The index of the first phase an action may go in. Urgent work that can be done soon goes first, and large builds and low-severity tidying go last. */
/**
 * A quick, cheap win: very small work that's at least medium, or that saves money,
 * such as a licence to release. It goes in the first 30 days, and doesn't count
 * against the phase's capacity, as it takes almost none of it (DECISIONS.md 363).
 */
export const isQuickWin = (f: Finding): boolean => f.effort.size === "XS" && (f.severity !== "low" && f.severity !== "info" ? true : f.value.kind === "licence_cost" || f.value.kind === "revenue");

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
export function buildPlan(findings: Finding[], added?: Added): Plan {
  // Every issue with something to do is planned (DECISIONS.md 363). One from the
  // same rule with the same action joins that action, rather than repeating it or
  // dropping off: before, R18.2 and R18.3 fell out of the plan this way.
  const lead = new Map<string, Finding>();
  const alsoFor: Record<string, Finding[]> = {};
  const actions = issuesOf(findings, added).filter((f) => {
    if (!f.recommendation || f.recommendationType === "none" || f.severity === "info") return false;
    const key = `${f.ruleId}\u0000${f.recommendation}`;
    const first = lead.get(key);
    if (first) {
      (alsoFor[first.id] ??= []).push(f);
      return false;
    }
    lead.set(key, f);
    return true;
  });
  const phases: PlanPhase[] = [
    { key: "stabilise", title: "Stabilise", from: 1, to: 30, findings: [] },
    { key: "fix", title: "Fix", from: 31, to: 60, findings: [] },
    { key: "build", title: "Build", from: 61, to: 90, findings: [] },
  ];
  const later: Finding[] = [];
  const load = (i: number): number => phases[i]!.findings.filter((f) => !isQuickWin(f)).length;
  for (const f of actions) {
    if (isQuickWin(f)) {
      phases[0]!.findings.push(f);
      continue;
    }
    const index = [0, 1, 2].slice(earliestPhase(f)).find((i) => load(i) < PLAN_CAPACITY[i]!);
    (index === undefined ? later : phases[index]!.findings).push(f);
  }
  return { phases, later, alsoFor };
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
  // Service Cloud's case area and CPQ's quoting area are record areas too, in a review that has them.
  const record: string[] = [...SALES_CLOUD_DOMAINS, ...(run.modules.includes("service_setup") ? ["service_operations"] : []), ...(run.modules.includes("cpq_setup") ? ["cpq_quoting"] : [])];
  return isSetupReview(run) ? record.filter((d) => !run.modules.includes(d)) : [];
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
