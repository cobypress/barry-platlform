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
// Whoever raises a rule's version decides whether it belongs here, as a
// loosening, a tightening (below, DECISIONS.md 754) or both: the rule-where test
// fails until the file is made again, and its message asks which.

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

/**
 * cur.stuck-in-stage's limits before version 3 (DECISIONS.md 777): an issue above 40% of compared open deals past
 * the won deals' line, or above 60% in a segment of at least 10. Versions 1 and 2 decided alike (2 only added the
 * left-out sentence, DECISIONS.md 388), and version 2 still reads them while HC_STUCK_EXPECTED is off.
 */
export const STUCK_LIMITS_BEFORE_3 = { total: 40, segment: 60 } as const;

/**
 * What versions 1 and 2 would have said of a version 3 reading, from the facts version 3 carries for it: the share
 * past the won deals' line (`wonLineRate`) and the worst segment's (`wonLineWorstSegmentRate`, absent when no segment
 * had 10 deals), each the rounded share versions 1 and 2 compared. With no `wonLineRate` (no stage had enough won
 * deals for a line, so the earlier versions would not have measured it) the verdict is unknown.
 */
export function stuckBefore3(facts: Readonly<Record<string, number>>): "raised" | "passed" | "unknown" {
  const total = facts.wonLineRate;
  if (total === undefined) return "unknown";
  const worst = facts.wonLineWorstSegmentRate;
  return total > STUCK_LIMITS_BEFORE_3.total || (worst !== undefined && worst > STUCK_LIMITS_BEFORE_3.segment) ? "raised" : "passed";
}

/** The day cur.stuck-in-stage version 3 was first committed, behind HC_STUCK_EXPECTED (DECISIONS.md 777). */
const STUCK_3_ON = "2026-11-27";

export const LOOSENED: Readonly<Record<string, readonly Loosening[]>> = {
  // Version 3 compares the share past the line with the share a steady pipeline would have (DECISIONS.md 777), where
  // 1 and 2 raised above a fixed share: a heavy-tailed org's steady pipeline passes now. Judged on the pass's own
  // reading against the won deals' line, which 3 carries for it; unknown is taken as still raised, never a fix.
  "cur.stuck-in-stage": [{ version: 3, on: STUCK_3_ON, from: [1, 2], raisedBefore: (facts) => stuckBefore3(facts) !== "passed" }],
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
  // A pass under an older version than the issue's (a switch turned back off, DECISIONS.md 777): the check changed
  // back, so nothing shows the org fixed it. Any rule, declared or not.
  if (wentBack(issueVersion, passVersion)) return true;
  const between = loosenings(ruleId).filter((l) => (issueVersion === undefined || (l.version > issueVersion && l.from.includes(issueVersion))) && (passVersion === undefined || l.version <= passVersion));
  if (!between.length) return false;
  if (under === undefined) return true;
  return issueVersion === undefined ? under.length > 0 : under.includes(issueVersion);
}

/**
 * Whether a later run read a rule at an older version than an earlier run did (DECISIONS.md 777): a version behind a
 * worker switch, with the switch turned off again. Only when both versions are known.
 */
export const wentBack = (earlier: number | undefined, later: number | undefined): boolean => earlier !== undefined && later !== undefined && later < earlier;

/**
 * What is wrong with a table of loosenings against the rules' current versions,
 * in words; empty when nothing is. Each loosening names a version the rule has had
 * (2 up to its current one) once, and earlier versions that exist, ascending.
 */
export function loosenedProblems(table: Readonly<Record<string, readonly Loosening[]>>, versions: ReadonlyMap<string, number>): string[] {
  return changeProblems(table, versions, "loosened");
}

/** The same check for a table of tightenings (DECISIONS.md 754). */
export function tightenedProblems(table: Readonly<Record<string, readonly Tightening[]>>, versions: ReadonlyMap<string, number>): string[] {
  return changeProblems(table, versions, "tightened");
}

function changeProblems(table: Readonly<Record<string, readonly { version: number; on: string; from: readonly number[] }[]>>, versions: ReadonlyMap<string, number>, done: "loosened" | "tightened"): string[] {
  const out: string[] = [];
  for (const [ruleId, all] of Object.entries(table)) {
    const current = versions.get(ruleId);
    if (current === undefined) {
      out.push(`${ruleId}: no rule has this id`);
      continue;
    }
    if (!all.length) out.push(`${ruleId}: an empty list`);
    if (new Set(all.map((l) => l.version)).size !== all.length) out.push(`${ruleId}: a version ${done} twice`);
    for (const l of all) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(l.on) || Number.isNaN(Date.parse(l.on))) out.push(`${ruleId} ${l.version}: "${l.on}" isn't a day`);
      if (!Number.isInteger(l.version) || l.version < 2 || l.version > current) out.push(`${ruleId}: version ${l.version} isn't one the rule has had after its first (it is at ${current})`);
      if (!l.from.length) out.push(`${ruleId} ${l.version}: ${done} from nothing`);
      for (const [i, v] of l.from.entries()) {
        if (!Number.isInteger(v) || v < 1 || v >= l.version) out.push(`${ruleId} ${l.version}: from ${v}, which isn't an earlier version`);
        if (i > 0 && v <= l.from[i - 1]!) out.push(`${ruleId} ${l.version}: from isn't ascending and once each`);
      }
    }
  }
  return out;
}

// Rule versions that tightened a rule (DECISIONS.md 754), the mirror of the
// above. A version that tightens a rule can raise an issue where the earlier
// version passed, with nothing changed in the org, and calling that issue "new"
// would say the org got worse when we can't show it. Such an issue is open (it
// is an issue under today's check, so it is in the open count, the score, the
// plan and the value) but never new: the comparison lists it apart, once.
//
// An issue after a pass is "found by a stricter check" when a version after the
// pass's, up to the issue's, tightened the rule from the pass's version, and the
// issue's own reading would have passed under that earlier version. Judged on the
// issue, for the same reason a loosening is judged on the pass, and per earlier
// version: only the tightenings that name the pass's own version speak for it, so
// an older tightening, or one from other versions, never settles it. A tightening
// that can't judge a reading (no passedBefore) leaves its versions unsettled, and
// an unsettled issue is found by a stricter check: never "new" when we can't show
// it. It is still open. A judge that throws fails the run, as a loosening's does:
// loud, never a silent verdict.
//
// A version may loosen some readings and tighten others, so the two tables are
// apart and a version can be in both: a pass after an issue reads only LOOSENED,
// an issue after a pass only TIGHTENED. The first rule tightened since the first
// real runs is cur.stuck-in-stage 3 (DECISIONS.md 777), behind HC_STUCK_EXPECTED:
// with the switch off the rule stays at 2, and a declaration for 3 reads no
// version 2 finding differently.

export type Tightening = {
  /** The version that tightened the rule. */
  version: number;
  /** The day the tightening version was first committed (YYYY-MM-DD), for the console, as Loosening's. */
  on: string;
  /** The earlier versions under which some readings it raises would have passed, ascending. */
  from: readonly number[];
  /**
   * Whether an issue's reading, by its facts, would have passed under `earlier`, one
   * of the versions in `from`: asked once for each, so a reading can pass under one
   * earlier version and be raised under another. A judge that ignores `earlier`
   * answers alike for all of them. `ownCalls` as in Loosening.
   */
  passedBefore?: (facts: Readonly<Record<string, number>>, ownCalls: number | undefined, earlier: number) => boolean;
};

export const TIGHTENED: Readonly<Record<string, readonly Tightening[]>> = {
  // Version 3 also raises where 1 and 2 passed (DECISIONS.md 777): a light-tailed org's steady share is under 25%,
  // so a share under 40% can be well above it. Judged on the issue's own reading as LOOSENED's is; unknown is
  // never "new".
  "cur.stuck-in-stage": [{ version: 3, on: STUCK_3_ON, from: [1, 2], passedBefore: (facts) => stuckBefore3(facts) !== "raised" }],
};

const tightenings = (ruleId: string): readonly Tightening[] => (Object.hasOwn(TIGHTENED, ruleId) ? TIGHTENED[ruleId]! : []);

/** By version, the day it was first committed, for rule-where.json; undefined for a rule that never tightened. */
export function tightenedOn(ruleId: string): Record<string, string> | undefined {
  const all = tightenings(ruleId);
  return all.length ? Object.fromEntries([...all].sort((a, b) => a.version - b.version).map((t) => [String(t.version), t.on])) : undefined;
}

/** By version, the earlier versions it tightened the rule from, for rule-where.json; undefined for a rule that never tightened. */
export function tightenedFrom(ruleId: string): Record<string, number[]> | undefined {
  const all = tightenings(ruleId);
  return all.length ? Object.fromEntries([...all].sort((a, b) => a.version - b.version).map((t) => [String(t.version), [...t.from]])) : undefined;
}

/** Whether the rule has ever tightened: only its issues carry a verdict in the digest. */
export const hasTightened = (ruleId: string): boolean => tightenings(ruleId).length > 0;

/**
 * For an issue under a rule that tightened at or before the issue's version: of the
 * earlier versions the tightenings up to it name, ascending, those under which its
 * reading is not shown to have been raised. An earlier version is left out only when
 * a tightening up to the issue's version that names it judges the reading raised
 * there; one that names it with no judge leaves it in (unsettled, never "new" we
 * can't show). The tightenings that name a version are exactly those crossed from a
 * pass under it, so this is the verdict for whichever version the pass was under.
 * Empty: raised under every one, so after any earlier pass it is new. Undefined for
 * a pass, or a rule not tightened at or before the issue's version.
 */
export function passedUnder(f: { ruleId: string; ruleVersion: number; status: string; facts: Readonly<Record<string, number>> }, ownCalls: number | undefined): number[] | undefined {
  if (f.status !== "issue") return undefined;
  const upTo = tightenings(f.ruleId).filter((t) => t.version <= f.ruleVersion);
  if (!upTo.length) return undefined;
  const earlier = [...new Set(upTo.flatMap((t) => t.from))].sort((a, b) => a - b);
  return earlier.filter((v) => upTo.every((t) => !t.from.includes(v) || !t.passedBefore || t.passedBefore(f.facts, ownCalls, v)));
}

/**
 * Whether an issue after a pass under the same rule was found by a stricter
 * check: a version after the pass's, up to the issue's, tightened it from the
 * pass's version, and the issue's reading isn't shown to have been raised under
 * the pass's version (`under`, from passedUnder). An unknown version matches any
 * tightening, and is found by a stricter check unless the reading is shown raised
 * under every earlier version. No verdict at all (a digest without passedUnder)
 * can't show the org got worse. The console reads the same rule from
 * rule-where.json and the digest.
 */
export function foundByStricter(ruleId: string, passVersion: number | undefined, issueVersion: number | undefined, under: readonly number[] | undefined): boolean {
  // An issue under an older version than the pass's (a switch turned back off, DECISIONS.md 777): the check changed
  // back, so nothing shows the org got worse. Open, never new. Any rule, declared or not.
  if (wentBack(passVersion, issueVersion)) return true;
  const between = tightenings(ruleId).filter((t) => (passVersion === undefined || (t.version > passVersion && t.from.includes(passVersion))) && (issueVersion === undefined || t.version <= issueVersion));
  if (!between.length) return false;
  if (under === undefined) return true;
  return passVersion === undefined ? under.length > 0 : under.includes(passVersion);
}
