import { plainWords, ukDates } from "../render/format.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { areasOf, bySeverity, domainLabel, issuesOf, scorecard } from "../narrative/select.ts";

// The free Barometer Scan's teaser (roadmap Stage 5): the score, each area's
// score and issue count, and a few real findings in full. Every other issue is
// counted, not shown. It's set-up only and team-level, with no names, so the
// console stores it as it is and shows it to the prospect without review.

export const SHOWN_FINDINGS = 4;

export type ScanResult = {
  score: number | null;
  grade: string | null;
  areas: Array<{ domain: string; label: string; score: number | null; issues: number; high: number }>;
  shown: Array<{ title: string; summary: string; severity: string; area: string }>;
  lockedIssues: number;
};

// A pseudonym token means a name was meant to go back in, which never happens on this page.
const TOKEN = /\[\[[a-z]+:/;

export function buildScanResult(report: BuiltReport): ScanResult {
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
    areas: rows.map((r) => ({ domain: r.domain, label: r.label, score: r.score, issues: r.issues, high: r.high })),
    // In the reports' words: UK dates and plain terms, as every other page has them (night 3 reading pass).
    shown: shown.map((f) => ({ title: plainWords(ukDates(f.title)), summary: plainWords(ukDates(f.summary)), severity: f.severity, area: domainLabel(f.domain) })),
    lockedIssues: issues.length - shown.length,
  };
}
