import type { Finding, Score } from "../schemas/index.ts";
import type { OverallScore } from "../domains/scoring.ts";
import type { ValueModel } from "../value/model.ts";

// The re-check (roadmap Stage 5): what changed since the first health check.
// Checks are lined up by rule, not by finding, because a rule can raise one
// finding per segment and the segments move. A rule that found something then
// is fixed only if it ran now and passed; if it couldn't be measured now, we
// say we can't tell rather than claim it.

export type RuleChange = { ruleId: string; title: string; findingIds: string[] };

export type Progress = {
  baselineRunId: string;
  baselineAsOf: string;
  score: { before: number | null; after: number | null };
  /** What fixing the issues is worth, expected, then and now. */
  worth: { before: number; after: number };
  fixed: RuleChange[];
  remaining: RuleChange[];
  added: RuleChange[];
  unknown: RuleChange[];
  areas: Array<{ domain: string; before: number | null; after: number | null }>;
};

export type ProgressSide = { runId: string; asOf: string; findings: Finding[]; scores: Score[]; overall: OverallScore; value: ValueModel };

const byRule = (findings: Finding[]): Map<string, Finding[]> => {
  const map = new Map<string, Finding[]>();
  for (const f of findings) map.set(f.ruleId, [...(map.get(f.ruleId) ?? []), f]);
  return map;
};

const change = (ruleId: string, findings: Finding[]): RuleChange => ({ ruleId, title: findings[0]!.title, findingIds: findings.map((f) => f.id) });

export function compareWithBaseline(baseline: ProgressSide, current: ProgressSide): Progress {
  const before = byRule(baseline.findings);
  const now = byRule(current.findings);
  const fixed: RuleChange[] = [];
  const remaining: RuleChange[] = [];
  const added: RuleChange[] = [];
  const unknown: RuleChange[] = [];
  for (const [ruleId, then] of before) {
    const thenIssues = then.filter((f) => f.status === "issue");
    if (thenIssues.length === 0) continue;
    const nowFindings = now.get(ruleId) ?? [];
    const nowIssues = nowFindings.filter((f) => f.status === "issue");
    if (nowIssues.length) remaining.push(change(ruleId, nowIssues));
    else if (nowFindings.some((f) => f.status === "passed")) fixed.push(change(ruleId, thenIssues));
    else unknown.push(change(ruleId, thenIssues));
  }
  for (const [ruleId, nowFindings] of now) {
    const nowIssues = nowFindings.filter((f) => f.status === "issue");
    if (nowIssues.length && !(before.get(ruleId) ?? []).some((f) => f.status === "issue")) added.push(change(ruleId, nowIssues));
  }
  const domains = [...new Set([...baseline.scores, ...current.scores].map((s) => s.domain))];
  const scoreOf = (scores: Score[], domain: string): number | null => {
    const s = scores.find((x) => x.domain === domain);
    return s?.status === "measured" ? s.score : null;
  };
  return {
    baselineRunId: baseline.runId,
    baselineAsOf: baseline.asOf,
    score: { before: baseline.overall.status === "measured" ? baseline.overall.score : null, after: current.overall.status === "measured" ? current.overall.score : null },
    worth: { before: baseline.value.total.expected, after: current.value.total.expected },
    fixed,
    remaining,
    added,
    unknown,
    areas: domains.map((domain) => ({ domain, before: scoreOf(baseline.scores, domain), after: scoreOf(current.scores, domain) })).filter((a) => a.before !== null || a.after !== null),
  };
}

/** Every figure a page about progress quotes, for the number check. */
/** How many of each list the progress page names. The rest are counted, so the count is a figure too. */
export const PROGRESS_LISTED = 5;

export function progressFigures(p: Progress): number[] {
  const pairs = [p.score.before, p.score.after, ...p.areas.flatMap((a) => [a.before, a.after])].filter((n): n is number => n !== null);
  const changes = [
    p.score.before !== null && p.score.after !== null ? Math.abs(p.score.after - p.score.before) : null,
    ...p.areas.map((a) => (a.before !== null && a.after !== null ? Math.abs(a.after - a.before) : null)),
  ].filter((n): n is number => n !== null);
  const overflow = [p.fixed, p.remaining, p.added].filter((l) => l.length > PROGRESS_LISTED).map((l) => l.length - PROGRESS_LISTED);
  return [...pairs, ...changes, p.fixed.length, p.remaining.length, p.added.length, p.unknown.length, ...overflow];
}

export const worthFigures = (p: Progress): number[] => [p.worth.before, p.worth.after, Math.abs(p.worth.before - p.worth.after)];
