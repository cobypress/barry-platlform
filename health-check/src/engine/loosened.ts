// Rule versions that loosened a rule (DECISIONS.md 732). A version bump usually
// changes wording, evidence or a bug that made the rule miss something, and an
// issue from before it that passes now is a fix. A version that loosens the rule
// is different: something that was an issue under the earlier version can pass
// under it with nothing changed in the org, and calling that pass "fixed" would
// be untrue. Only the engine knows which bumps loosened a rule, so they are
// declared here, by rule, and published in fixtures/rule-where.json for the
// console.
//
// A pass after an issue is "passed only because the rule changed" when a version
// after the issue's, up to the pass's, loosened the rule from the issue's version,
// and the pass's own reading would still have been an issue under that earlier
// version. The reading is judged on the pass, never on the old issue: the pass is
// in the run being compared or digested, while the run that raised the issue may
// be another engine's, or have no link to this one (an automatic run has none).
// A loosening that can't judge a reading (no raisedBefore) says every such pass is
// passed only because the rule changed: never a fix we can't show.
//
// Whoever raises a rule's version decides whether it belongs here: the
// rule-where test fails until the file is made again, and its message says so.

export type Loosening = {
  /** The version that loosened the rule. */
  version: number;
  /**
   * The day the loosening version was first committed (YYYY-MM-DD). No run that
   * started before it can have been analysed under it, so the console can call a
   * pass from such a run an ordinary one without its version.
   */
  on: string;
  /** The earlier versions whose issues may no longer be issues under it, ascending. */
  from: readonly number[];
  /**
   * Whether a pass's reading, by its facts, would have been an issue under the
   * versions in `from`. `ownCalls` is the run's own API calls, where known: an
   * earlier version may have read later in its run than this one does.
   */
  raisedBefore?: (facts: Readonly<Record<string, number>>, ownCalls: number | undefined) => boolean;
};

/** int.api-usage's line before version 3: a daily allowance was an issue from 80% used (DECISIONS.md 726). */
export const API_USAGE_LINE_BEFORE_3 = 80;

export const LOOSENED: Readonly<Record<string, readonly Loosening[]>> = {
  // Version 3 raised the line from 80% to 95% (DECISIONS.md 726). Versions 1 and 2
  // read the same shares, used over max (726 only caps the printed share at 99.9 with
  // some left, which doesn't move it across 80). But version 3 reads the allowance
  // before the run's own calls, and 1 and 2 read it after some of them, so API
  // requests are judged with the run's own calls added (all of them, the most they
  // could have been; unknown, the reading is taken as still raised). The run starts
  // no background jobs, so their share is judged as read.
  "int.api-usage": [
    {
      version: 3,
      on: "2026-11-25",
      from: [1, 2],
      raisedBefore: (facts, ownCalls) =>
        Object.entries(facts).some(([key, share]) => {
          if (!key.endsWith("Share")) return false;
          if (share >= API_USAGE_LINE_BEFORE_3) return true;
          if (key !== "DailyApiRequestsShare") return false;
          const [used, max] = [facts.DailyApiRequestsUsed, facts.DailyApiRequestsMax];
          if (ownCalls === undefined || used === undefined || max === undefined || !(max > 0)) return true;
          return ((used + ownCalls) / max) * 100 >= API_USAGE_LINE_BEFORE_3;
        }),
    },
  ],
};

const loosenings = (ruleId: string): readonly Loosening[] => (Object.hasOwn(LOOSENED, ruleId) ? LOOSENED[ruleId]! : []);

/** By version, the day it was first committed, for rule-where.json; undefined for a rule that never loosened. */
export function loosenedOn(ruleId: string): Record<string, string> | undefined {
  const all = loosenings(ruleId);
  return all.length ? Object.fromEntries([...all].sort((a, b) => a.version - b.version).map((l) => [String(l.version), l.on])) : undefined;
}

/** By version, the earlier versions it loosened the rule from, for rule-where.json; undefined for a rule that never loosened. */
export function loosenedFrom(ruleId: string): Record<string, number[]> | undefined {
  const all = loosenings(ruleId);
  return all.length ? Object.fromEntries([...all].sort((a, b) => a.version - b.version).map((l) => [String(l.version), [...l.from]])) : undefined;
}

/** Whether the rule has ever loosened: only its findings carry a version and a verdict in the digest. */
export const hasLoosened = (ruleId: string): boolean => loosenings(ruleId).length > 0;

/**
 * For a pass under a rule that loosened at or before the pass's version: the
 * earlier versions under which its reading would have been an issue, ascending
 * (empty: it would have passed under every one). Undefined for anything else,
 * or when a loosening can't judge a reading.
 */
export function issueUnder(f: { ruleId: string; ruleVersion: number; status: string; facts: Readonly<Record<string, number>> }, ownCalls: number | undefined): number[] | undefined {
  if (f.status !== "passed") return undefined;
  const upTo = loosenings(f.ruleId).filter((l) => l.version <= f.ruleVersion);
  if (!upTo.length || upTo.some((l) => !l.raisedBefore)) return undefined;
  return [...new Set(upTo.filter((l) => l.raisedBefore!(f.facts, ownCalls)).flatMap((l) => l.from))].sort((a, b) => a - b);
}

/**
 * Whether a pass after an issue under the same rule passed only because the rule
 * changed: a version after the issue's, up to the pass's, loosened it from the
 * issue's version, and the pass's reading would still have been an issue there
 * (`under`, from issueUnder). An unknown version (a finding that didn't carry it)
 * matches any; an unknown verdict can't show a fix. The console reads the same rule
 * from rule-where.json and the digest (its portal/history.ts).
 */
export function passedOnlyByChange(ruleId: string, issueVersion: number | undefined, passVersion: number | undefined, under: readonly number[] | undefined): boolean {
  const between = loosenings(ruleId).filter((l) => (issueVersion === undefined || (l.version > issueVersion && l.from.includes(issueVersion))) && (passVersion === undefined || l.version <= passVersion));
  if (!between.length) return false;
  if (under === undefined) return true;
  return issueVersion === undefined ? under.length > 0 : under.includes(issueVersion);
}

/**
 * What is wrong with a table of loosenings against the rules' current versions,
 * in words; empty when nothing is. Each loosening names a version the rule has had
 * (2 up to its current one) once, and earlier versions that exist, ascending.
 */
export function loosenedProblems(table: Readonly<Record<string, readonly Loosening[]>>, versions: ReadonlyMap<string, number>): string[] {
  const out: string[] = [];
  for (const [ruleId, all] of Object.entries(table)) {
    const current = versions.get(ruleId);
    if (current === undefined) {
      out.push(`${ruleId}: no rule has this id`);
      continue;
    }
    if (!all.length) out.push(`${ruleId}: an empty list`);
    if (new Set(all.map((l) => l.version)).size !== all.length) out.push(`${ruleId}: a version loosened twice`);
    for (const l of all) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(l.on) || Number.isNaN(Date.parse(l.on))) out.push(`${ruleId} ${l.version}: "${l.on}" isn't a day`);
      if (!Number.isInteger(l.version) || l.version < 2 || l.version > current) out.push(`${ruleId}: version ${l.version} isn't one the rule has had after its first (it is at ${current})`);
      if (!l.from.length) out.push(`${ruleId} ${l.version}: loosened from nothing`);
      for (const [i, v] of l.from.entries()) {
        if (!Number.isInteger(v) || v < 1 || v >= l.version) out.push(`${ruleId} ${l.version}: from ${v}, which isn't an earlier version`);
        if (i > 0 && v <= l.from[i - 1]!) out.push(`${ruleId} ${l.version}: from isn't ascending and once each`);
      }
    }
  }
  return out;
}
