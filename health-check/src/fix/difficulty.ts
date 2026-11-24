import type { Difficulty, EffortHours, FixComplexity } from "../schemas/index.ts";
import { countOf, MEDIUM_MAX_HOURS, SMALL_MAX_HOURS } from "./hours.ts";
import { FIX_TABLE, NEVER_RAISES } from "./table.ts";
import type { FixRule } from "./types.ts";

// How hard a fix is in Salesforce, said beside its severity (DECISIONS.md 714),
// so a reader deciding what to do first sees how much it matters and how hard
// it is.
//
// The one rule, from what the finding already holds and its rule declares:
//   no hours to fix (the rule's count is missing, or a consultant's correction
//   dropped them)                                   -> none: never a guess
//   the whole work's hours are over MEDIUM_MAX_HOURS (16): the high end, or for
//   hours capped to cover part of the work, the count times the minutes for
//   each unit before the cap                        -> "A project"
//   the hours are over SMALL_MAX_HOURS (4), or capped, or the rule's fix is
//   complex, removes set-up, changes an integration or automation (or what users
//   can save), turns on a setting that may not be undone, or its hours leave out
//   the people deciding or making the change        -> "Needs planning"
//   otherwise                                       -> "Quick fix"
//
// The cut-offs are the time band's (DECISIONS.md 636), so 40 hours is never a
// quick fix. Each property only ever raises the level, and one more unit moves a
// finding up at most one level. The hours are the ones the plan sums, after any
// correction (a correction that changes a figure drops the hours, and the
// difficulty with them). What each rule touches is declared in FIX_TABLE
// (`touches`, and the prompt's `removes`), beside its complexity.

/** The words a reader sees, beside the severity. None of them is a number or a price. */
export const DIFFICULTY_WORDS: Readonly<Record<Difficulty, string>> = {
  quick_fix: "Quick fix",
  needs_planning: "Needs planning",
  project: "A project",
};

/** Easiest first. */
export const DIFFICULTY_ORDER: readonly Difficulty[] = ["quick_fix", "needs_planning", "project"];

/** Why a rule's fix is never a quick fix, whatever its hours, in the order the rule above names them. Empty when it can be. */
export function neverQuick(rule: Pick<FixRule, "complexity" | "touches"> & { prompt?: { removes?: true } }): string[] {
  return [
    ...(rule.complexity === "complex" ? ["complex"] : []),
    ...(rule.prompt?.removes ? ["removes set-up"] : []),
    ...(rule.touches?.integration ? ["changes an integration"] : []),
    ...(rule.touches?.automation ? ["changes automation"] : []),
    ...(rule.touches?.mayNotUndo ? ["may not be undone"] : []),
    ...(rule.touches?.leavesOut ? ["hours leave out the decision or the change"] : []),
  ];
}

/**
 * A finding's difficulty from its own hours, or null when it has none.
 * `wholeHigh` is the high end of the whole work before any cap, given with
 * capped hours; `planned` is true when the rule is never a quick fix.
 */
export function difficultyOf(hours: Pick<EffortHours, "high" | "partial"> | undefined, planned: boolean, wholeHigh?: number): Difficulty | null {
  if (!hours) return null;
  const whole = hours.partial ? Math.max(hours.high, wholeHigh ?? Infinity) : hours.high;
  if (whole > MEDIUM_MAX_HOURS) return "project";
  if (hours.partial || hours.high > SMALL_MAX_HOURS || planned) return "needs_planning";
  return "quick_fix";
}

/** The high end of a per-unit rule's whole work, in whole hours, before its cap; null for a fixed range or no count. */
export function wholeHighOf(rule: FixRule, facts: Readonly<Record<string, number>>): number | null {
  if (rule.hours.kind !== "each") return null;
  const count = countOf(rule.hours, facts);
  return count === null ? null : Math.ceil((count * rule.hours.minutes[1]) / 60);
}

/**
 * Rules that can raise an issue and are given no difficulty, with the reason.
 * Empty today: every such rule declares hours and a fix complexity in FIX_TABLE.
 * A rule added without them must be listed here, or a test fails.
 */
export const NOT_ESTIMATED: Readonly<Record<string, string>> = {};

export type DifficultyBasis = {
  /** Where the hours come from: a fixed range, or minutes for each unit the finding counts. */
  hours: string;
  complexity: FixComplexity;
  /** Why it is never a quick fix, if it never is. */
  neverQuick: string[];
  /**
   * The difficulty for a fixed range, which is the same on every finding; for
   * a count, the most units that are still a quick fix (0 when none is) and
   * that still need planning, past which it is a project.
   */
  levels: Difficulty | { quickFixUpTo: number; needsPlanningUpTo: number };
};

/** The most units whose high end, rounded up to whole hours, is at most `maxHours`. */
const unitsWithin = (minutesHigh: number, maxHours: number): number => Math.floor((maxHours * 60) / minutesHigh);

/** Each rule's basis for its difficulty: rule id, hours, complexity, what keeps it from a quick fix, and where its levels fall (DECISIONS.md 714). */
export const DIFFICULTY_BASIS: Readonly<Record<string, DifficultyBasis>> = Object.fromEntries(
  Object.entries(FIX_TABLE)
    .filter(([id]) => !Object.hasOwn(NOT_ESTIMATED, id))
    .map(([id, rule]): [string, DifficultyBasis] => {
      const h = rule.hours;
      const never = neverQuick(rule);
      if (h.kind === "fixed") return [id, { hours: `${h.low} to ${h.high} hours, fixed`, complexity: rule.complexity, neverQuick: never, levels: difficultyOf({ high: h.high }, never.length > 0)! }];
      const count = Array.isArray(h.facts) ? h.facts.join(" + ") : "every count the finding holds";
      // A quick fix must also fit under the cap, or its hours are partial.
      const quick = never.length > 0 ? 0 : unitsWithin(h.minutes[1], Math.min(SMALL_MAX_HOURS, h.cap));
      return [id, { hours: `${h.minutes[0]} to ${h.minutes[1]} minutes for each of ${count}, at most ${h.cap} hours`, complexity: rule.complexity, neverQuick: never, levels: { quickFixUpTo: quick, needsPlanningUpTo: unitsWithin(h.minutes[1], MEDIUM_MAX_HOURS) } }];
    }),
);

/**
 * A rule's finding's difficulty, or null: only a rule with a basis in the table
 * gets one, so a rule listed as not estimated, or one the table doesn't hold,
 * never does, whatever hours it has. `facts` are the finding's, for the whole
 * work's hours when they are capped.
 */
export function difficultyFor(ruleId: string, hours: Pick<EffortHours, "high" | "partial"> | undefined, facts: Readonly<Record<string, number>> = {}): Difficulty | null {
  const basis = Object.hasOwn(DIFFICULTY_BASIS, ruleId) ? DIFFICULTY_BASIS[ruleId] : undefined;
  if (!basis) return null;
  return difficultyOf(hours, basis.neverQuick.length > 0, wholeHighOf(FIX_TABLE[ruleId]!, facts) ?? undefined);
}

/** Whether a rule has a basis for its difficulty, is listed as not estimated, or never raises an issue. */
export const difficultyCovered = (ruleId: string): boolean => Object.hasOwn(DIFFICULTY_BASIS, ruleId) || Object.hasOwn(NOT_ESTIMATED, ruleId) || NEVER_RAISES.has(ruleId);
