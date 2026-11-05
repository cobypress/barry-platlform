import type { BuiltReport } from "../narrative/sections.ts";
import { areasOf, bySeverity, domainLabel, issuesOf, scorecard } from "../narrative/select.ts";
import type { Finding } from "../schemas/index.ts";

// The monthly pulse (roadmap Stage 5, Barometer Ongoing): a lighter re-check
// that nobody reviews. Its result is what changed since the baseline the
// console chose, from the same progress model a full re-check uses. It's
// team-level and holds no names, so the console shows it as it is. Without a
// baseline we can read, every "before" is null rather than the pulse failing.

export const PULSE_LISTED = 5;

export type PulseResult = {
  version: 1;
  runId: string;
  /** The run compared against, or null when there was none we could read. */
  baselineRunId: string | null;
  asOf: string;
  baselineAsOf: string | null;
  score: { now: number | null; before: number | null };
  worth: { currency: string; now: number; before: number | null };
  /** Rules with an issue now, whatever the baseline. The rest are against the baseline, null without one. */
  counts: { openNow: number; fixed: number | null; stillOpen: number | null; new: number | null; cantTell: number | null };
  areas: Array<{ domain: string; label: string; now: number | null; before: number | null }>;
  newIssues: Array<{ title: string; severity: Finding["severity"]; area: string }>;
  fixed: Array<{ title: string; area: string }>;
};

// A pseudonym token means a name was meant to go back in, which never happens here.
const TOKEN = /\[\[[a-z]+:/;

export function buildPulseResult(report: BuiltReport): PulseResult {
  const { run } = report.input;
  const team = report.teamFindings;
  const p = report.progress;
  const before = new Map((p?.areas ?? []).map((a) => [a.domain, a.before]));
  const rows = scorecard(team, report.input.scores ?? [], areasOf(run, team)).filter((r) => r.assessed);
  const openNow = new Set(issuesOf(team).map((f) => f.ruleId)).size;

  // Each new rule at its most serious finding now, most serious first.
  const newIssues = (p?.added ?? [])
    .map((c) => issuesOf(team).filter((f) => f.ruleId === c.ruleId).sort(bySeverity)[0])
    .filter((f): f is Finding => f !== undefined && !TOKEN.test(f.title))
    .sort(bySeverity)
    .slice(0, PULSE_LISTED)
    .map((f) => ({ title: f.title, severity: f.severity, area: domainLabel(f.domain) }));
  // A fixed rule's area is where the baseline found it.
  const baseline = report.input.baseline?.findings ?? [];
  const fixed = (p?.fixed ?? [])
    .filter((c) => !TOKEN.test(c.title))
    .slice(0, PULSE_LISTED)
    .map((c) => ({ title: c.title, area: domainLabel(baseline.find((f) => f.ruleId === c.ruleId)?.domain ?? team.find((f) => f.ruleId === c.ruleId)?.domain ?? "") }));

  return {
    version: 1,
    runId: run.id,
    baselineRunId: p?.baselineRunId ?? null,
    asOf: run.asOf,
    baselineAsOf: p?.baselineAsOf ?? null,
    score: { now: report.overall.status === "measured" ? report.overall.score : null, before: p?.score.before ?? null },
    worth: { currency: report.value.currency, now: report.value.total.expected, before: p ? p.worth.before : null },
    counts: { openNow, fixed: p ? p.fixed.length : null, stillOpen: p ? p.remaining.length : null, new: p ? p.added.length : null, cantTell: p ? p.unknown.length : null },
    areas: rows.map((r) => ({ domain: r.domain, label: r.label, now: r.score, before: before.get(r.domain) ?? null })),
    newIssues,
    fixed,
  };
}
