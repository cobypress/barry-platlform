import { plainWords, ukDates } from "../render/format.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { areasOf, bySeverity, domainLabel, issuesOf, scorecard } from "../narrative/select.ts";
import { ALL_MODULES } from "../domains/index.ts";
import { DOCUMENTED } from "../domains/configuration/documented.ts";
import type { Complexity } from "../schemas/complexity.ts";
import type { Finding } from "../schemas/index.ts";

// The free Barometer Scan's teaser (roadmap Stage 5): the score, each area's
// score and issue count, and a few real findings in full. Every other issue is
// counted, not shown. It's set-up only and team-level, with no names, so the
// console stores it as it is and shows it to the prospect without review.

export const SHOWN_FINDINGS = 4;

// What decision 623 added is optional throughout, so a console that doesn't
// know a field still reads the rest, and a console that does still reads a
// result written before it. No field from before changed meaning.

/** An area's issues by severity. `high` beside it still counts critical and high together. */
export type SeverityCounts = { critical: number; high: number; medium: number; low: number };

/** One count of what the org holds, read from set-up alone and only when read whole. */
export type OrgCount = { id: OrgCountId; label: string; count: number };
export type OrgCountId = "customObjects" | "customFields" | "flows" | "apexClasses" | "packages" | "activeUsers";

export type ScanResult = {
  score: number | null;
  grade: string | null;
  areas: Array<{ domain: string; label: string; score: number | null; issues: number; high: number; bySeverity?: SeverityCounts }>;
  shown: Array<{
    title: string;
    summary: string;
    severity: string;
    area: string;
    /** The finding's own recommendation, as the report prints it. Absent when it could name someone or something. */
    fix?: string;
    /** Where the rule's subject is in Setup, as the menu's labels. Absent for a rule that can't say for every org. */
    where?: string[];
  }>;
  lockedIssues: number;
  /** What the org holds, for "Found in your org". A count that wasn't read whole isn't here. */
  foundInOrg?: OrgCount[];
};

// A pseudonym token means a name was meant to go back in, which never happens on this page.
const TOKEN = /\[\[[a-z]+:/;

// A fix goes on a public page, so one that could hold a person or a record is left out
// rather than risk it: a token, an email, anything shaped like a Salesforce id, or the
// name of anything the finding is about.
const EMAIL = /@/;
const SALESFORCE_ID = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/;

function fixOf(f: Finding): string | null {
  const text = f.recommendation.trim();
  if (!text || TOKEN.test(text) || EMAIL.test(text) || SALESFORCE_ID.test(text)) return null;
  if (f.subjects.some((s) => text.includes(s.label))) return null;
  return plainWords(ukDates(text));
}

/** Each rule's Setup location, by rule id. */
const WHERE_BY_RULE: ReadonlyMap<string, readonly string[]> = new Map(ALL_MODULES.flatMap((m) => m.rules.flatMap((r) => (r.where ? [[r.id, r.where] as const] : []))));

/** The complexity score's counts we show, by component: it has already checked each component was read whole (DECISIONS.md 614). */
const FROM_COMPLEXITY: Array<{ id: OrgCountId; label: string; component: string; counts: string[] }> = [
  { id: "customObjects", label: "Custom objects of your own", component: "dataModel", counts: ["customObjects"] },
  { id: "customFields", label: "Custom fields of your own, on core and custom objects", component: "dataModel", counts: ["customFields"] },
  { id: "flows", label: "Active flows", component: "declarativeAutomation", counts: ["recordTriggeredFlows", "otherFlows"] },
  { id: "apexClasses", label: "Apex classes, not counting tests", component: "code", counts: ["apexClasses"] },
  { id: "packages", label: "Installed packages", component: "integrations", counts: ["packages"] },
];

function foundInOrg(report: BuiltReport, complexity: Complexity | null): OrgCount[] {
  const out: OrgCount[] = [];
  for (const c of FROM_COMPLEXITY) {
    const component = complexity?.components.find((x) => x.id === c.component);
    if (component?.status !== "measured") continue;
    const counts = c.counts.map((id) => component.counts.find((n) => n.id === id)?.count);
    if (counts.some((n) => n === undefined)) continue;
    out.push({ id: c.id, label: c.label, count: counts.reduce((a: number, n) => a + n!, 0) });
  }
  // Users are read in one query with no cap, so a measured count is every active user (src/extract/users.ts).
  const users = report.input.metrics.find((m) => m.id === DOCUMENTED.access);
  if (users?.status === "measured" && Number.isInteger(users.value) && users.value >= 0) out.push({ id: "activeUsers", label: "Active users", count: users.value });
  return out;
}

const severities = (issues: Finding[]): SeverityCounts => ({
  critical: issues.filter((f) => f.severity === "critical").length,
  high: issues.filter((f) => f.severity === "high").length,
  medium: issues.filter((f) => f.severity === "medium").length,
  low: issues.filter((f) => f.severity === "low").length,
});

/** `complexity` is the run's complexity.json, where it has one; without it, only the users are counted. */
export function buildScanResult(report: BuiltReport, complexity: Complexity | null = null): ScanResult {
  const team = report.teamFindings;
  const o = report.overall;
  const rows = scorecard(team, report.input.scores ?? [], areasOf(report.input.run, team)).filter((r) => r.assessed);
  const issues = issuesOf(team).slice().sort(bySeverity);
  // The most serious, one an area where we can, so the teaser shows breadth.
  const shown: typeof issues = [];
  for (const f of issues) {
    if (shown.length >= SHOWN_FINDINGS) break;
    if (TOKEN.test(f.title) || TOKEN.test(f.summary)) continue;
    if (shown.some((s) => s.domain === f.domain) && issues.some((g) => !shown.some((s) => s.domain === g.domain) && !TOKEN.test(g.title) && !TOKEN.test(g.summary))) continue;
    shown.push(f);
  }
  return {
    score: o.status === "measured" ? o.score : null,
    grade: o.status === "measured" ? o.grade : null,
    areas: rows.map((r) => ({ domain: r.domain, label: r.label, score: r.score, issues: r.issues, high: r.high, bySeverity: severities(issues.filter((f) => f.domain === r.domain)) })),
    // In the reports' words: UK dates and plain terms, as every other page has them (night 3 reading pass).
    shown: shown.map((f) => {
      const fix = fixOf(f);
      const where = WHERE_BY_RULE.get(f.ruleId);
      return { title: plainWords(ukDates(f.title)), summary: plainWords(ukDates(f.summary)), severity: f.severity, area: domainLabel(f.domain), ...(fix ? { fix } : {}), ...(where ? { where: [...where] } : {}) };
    }),
    lockedIssues: issues.length - shown.length,
    foundInOrg: foundInOrg(report, complexity),
  };
}
