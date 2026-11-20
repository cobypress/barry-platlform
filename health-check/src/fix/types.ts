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
      /** The key in the finding's `facts` that holds the count. */
      fact: string;
      /** The unit, one and many: "flow", "flows". */
      one: string;
      many: string;
      /** Minutes for each unit, low and high. */
      minutes: readonly [number, number];
      /** The most hours the range may claim, however large the count. */
      cap: number;
      /** Why the work stops growing with the count past the cap. */
      capReason: string;
    };

/** The fixed text a rule's fix prompt is built from. */
export type PromptRule = {
  /** What the items are called, plural, when the prompt can't name them: "flows". */
  items: string;
  /** The fix is a decision for people, not a change to the org: the prompt asks for the analysis and the options. */
  decision?: true;
  /** The task, in order. */
  steps: readonly string[];
  /** How to tell it worked. */
  verify: readonly string[];
};

export type FixRule = { hours: HoursRule; prompt: PromptRule };
