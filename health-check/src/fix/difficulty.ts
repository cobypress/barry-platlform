import type { Difficulty, EffortHours, FixComplexity } from "../schemas/index.ts";
import { MEDIUM_MAX_HOURS, SMALL_MAX_HOURS } from "./hours.ts";
import { FIX_TABLE, NEVER_RAISES } from "./table.ts";

// How hard a fix is, said beside its severity (DECISIONS.md 714), so a reader
// deciding what to do first sees how much it matters and how hard it is.
//
// The one rule, from what the finding already holds and nothing else:
//   no hours to fix (the rule's count is missing, or a consultant's correction
//   dropped them)                                  -> none: never a guess
//   the hours cover only part of the work, or their high end is over
//   MEDIUM_MAX_HOURS (16)                          -> "A project"
//   the high end is over SMALL_MAX_HOURS (4), or the rule's fix is complex
//   (it changes access, records in bulk, automation, Apex or sharing)
//                                                  -> "Needs planning"
//   otherwise (4 hours or less, simple or moderate) -> "Quick fix"
//
// The cut-offs are the time band's (DECISIONS.md 636), so a difficulty is never
// lower than the band of the same hours: 40 hours is never a quick fix. The
// hours are the ones the plan sums, after any correction (a correction that
// changes a figure drops the hours, and the difficulty with them).

/** The words a reader sees, beside the severity. None of them is a number or a price. */
export const DIFFICULTY_WORDS: Readonly<Record<Difficulty, string>> = {
  quick_fix: "Quick fix",
  needs_planning: "Needs planning",
  project: "A project",
};

/** Easiest first. */
export const DIFFICULTY_ORDER: readonly Difficulty[] = ["quick_fix", "needs_planning", "project"];

/** A finding's difficulty from its own hours and its rule's fix complexity, or null when it has no hours. */
export function difficultyOf(hours: Pick<EffortHours, "high" | "partial"> | undefined, complexity: FixComplexity | undefined): Difficulty | null {
  if (!hours) return null;
  if (hours.partial || hours.high > MEDIUM_MAX_HOURS) return "project";
  if (hours.high > SMALL_MAX_HOURS || complexity === "complex") return "needs_planning";
  return "quick_fix";
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
  /**
   * The difficulty for a fixed range, which is the same on every finding; for
   * a count, the most units that are still a quick fix (0 when none is) and
   * that still need planning, past which it is a project.
   */
  levels: Difficulty | { quickFixUpTo: number; needsPlanningUpTo: number };
};

/** The most units whose hours' high end is at most `maxHours`, under the rule's cap. */
function unitsWithin(minutesHigh: number, cap: number, maxHours: number): number {
  // Hours are the count times the minutes, rounded up to whole hours; past the cap the range is partial.
  const limit = Math.min(maxHours, cap);
  return Math.floor((limit * 60) / minutesHigh);
}

/** Each rule's basis for its difficulty: rule id, hours, complexity and where its levels fall (DECISIONS.md 714). */
export const DIFFICULTY_BASIS: Readonly<Record<string, DifficultyBasis>> = Object.fromEntries(
  Object.entries(FIX_TABLE)
    .filter(([id]) => !Object.hasOwn(NOT_ESTIMATED, id))
    .map(([id, rule]): [string, DifficultyBasis] => {
      const h = rule.hours;
      if (h.kind === "fixed") return [id, { hours: `${h.low} to ${h.high} hours, fixed`, complexity: rule.complexity, levels: difficultyOf({ high: h.high }, rule.complexity)! }];
      const count = Array.isArray(h.facts) ? h.facts.join(" + ") : "every count the finding holds";
      const quick = rule.complexity === "complex" ? 0 : unitsWithin(h.minutes[1], h.cap, SMALL_MAX_HOURS);
      return [id, { hours: `${h.minutes[0]} to ${h.minutes[1]} minutes for each of ${count}, at most ${h.cap} hours`, complexity: rule.complexity, levels: { quickFixUpTo: quick, needsPlanningUpTo: unitsWithin(h.minutes[1], h.cap, MEDIUM_MAX_HOURS) } }];
    }),
);

/**
 * A rule's finding's difficulty, or null: only a rule with a basis in the table
 * gets one, so a rule listed as not estimated, or one the table doesn't hold,
 * never does, whatever hours it has.
 */
export function difficultyFor(ruleId: string, hours: Pick<EffortHours, "high" | "partial"> | undefined): Difficulty | null {
  const basis = Object.hasOwn(DIFFICULTY_BASIS, ruleId) ? DIFFICULTY_BASIS[ruleId] : undefined;
  return basis ? difficultyOf(hours, basis.complexity) : null;
}

/** Whether a rule has a basis for its difficulty, is listed as not estimated, or never raises an issue. */
export const difficultyCovered = (ruleId: string): boolean => Object.hasOwn(DIFFICULTY_BASIS, ruleId) || Object.hasOwn(NOT_ESTIMATED, ruleId) || NEVER_RAISES.has(ruleId);
