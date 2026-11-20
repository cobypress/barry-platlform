// What each rule declares about fixing what it finds (DECISIONS.md 628): a range
// of hours, and the fixed text a fix prompt is built from. Data only: nothing here
// is printed in a document.

/**
 * Hours to fix, before the finding's own count is known. Either a fixed range,
 * or minutes for each unit times a count the finding holds in `facts`, rounded
 * up to whole hours and held under a cap.
 */
export type HoursRule =
  | { kind: "fixed"; low: number; high: number; basis: string }
  | {
      kind: "each";
      /**
       * The keys in the finding's `facts` whose sum is the count, or "all" for
       * every fact the finding holds (a rule whose facts are one count each, by
       * object). A key the finding doesn't hold adds nothing; none at all, no range.
       */
      facts: readonly string[] | "all";
      /** What one unit is, one and many, saying what the count counts: "flow with no fault path". */
      one: string;
      many: string;
      /** Minutes for each unit, low and high. */
      minutes: readonly [number, number];
      /** The most hours the range may claim, however large the count. */
      cap: number;
      /**
       * Only where the work truly stops growing with the count past the cap (one
       * bulk change): why. Without it the range covers the first units the cap
       * allows, and says the rest is scoped separately.
       */
      bulk?: string;
    };

/** The fixed text a rule's fix prompt is built from. */
export type PromptRule = {
  /** What the items are called, plural, when the prompt can't name them: "flows". */
  items: string;
  /**
   * Analysis only: the fix is a decision for people, or changes records, so the
   * prompt asks for the analysis, the list and the plan, and no change.
   */
  decision?: true;
  /**
   * The rule's fix can remove, deactivate or merge. When its finding isn't the
   * destructive recommendation (a guard held it back, or the rule itself says to
   * investigate), the prompt gives `review`, or the shared review, instead of `steps`.
   */
  removes?: true;
  /** The task, in order. */
  steps: readonly string[];
  /** How to tell it worked. */
  verify: readonly string[];
  /** The task when a `removes` rule's finding is held back. */
  review?: { steps: readonly string[]; verify: readonly string[] };
};

export type FixRule = { hours: HoursRule; prompt: PromptRule };
