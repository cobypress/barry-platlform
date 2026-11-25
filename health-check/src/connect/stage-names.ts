import type { StageOutcome } from "./stage-outcomes.ts";

// Whether a closed, not-won stage is lost or no decision, read from its name by
// a fixed rule (DECISIONS.md 756): no model, no network, the same answer every
// time. Salesforce's flags have already said the stage is closed and not won;
// a name never makes a stage won or open, and is never read for any other
// stage. Used only on an automatic run with HC_STAGE_NAMES on, below a
// person's calibration and the customer's answer, and always said as an
// assumption the customer can correct.
//
// The name is normalised (camelCase split, case folded, everything but letters
// and digits a space, so "Closed_Lost", "closed-lost" and "ClosedLost" read
// alike) and matched on whole words. A name matching both lists, or a word that
// stops the rule, is undecided; so is a name matching neither, which is where
// every name not in English ends. Label and API name are read apart: one side
// from either and nothing from the other decides; any disagreement is undecided.
//
// Lost: the buyer chose against us, or we chose against the deal.
//   lost, lose, loss, lost to competitor, competitor, competition, disqualified,
//   unqualified, not qualified, rejected, declined, not interested, no budget,
//   churned, churn
//   "disqualified" / "unqualified": a decision was made (ours) that the deal
//   won't happen, which is a loss, not a deal that fizzled. "no budget": the
//   usual loss reason; the synthetic org's "Closed Lost - No Budget" is lost.
// No decision: the buyer never decided, and the deal went quiet or was parked.
//   no decision, no response, no reply, unresponsive, gone quiet, went quiet,
//   ghosted, on hold, hold, paused, deferred, postponed, abandoned, stalled,
//   nurture
// Left undecided on purpose (in neither list, so they decide nothing):
//   "closed": every stage here is closed; it says nothing either way.
//   "dead": orgs use it for every kind of loss and for fizzled deals alike.
//   "cancelled" / "canceled": the buyer cancelled a purchase it had decided on,
//   or a project that never got a decision; both are common.
//   "withdrawn": we withdrew (a loss) or the buyer withdrew (no decision).
//   "duplicate": not an outcome at all; such deals belong out of every rate,
//   and "excluded" is a person's choice, never ours.
//   "timing": most often a loss reason ("Lost - Timing"), sometimes a deferral.
// Stops the rule (undecided whatever else matches): "won", "win", "open": a
// closed, not-won stage named so is too confusing to read by name.

const LOST = ["lost", "lose", "loss", "lost to competitor", "competitor", "competition", "disqualified", "unqualified", "not qualified", "rejected", "declined", "not interested", "no budget", "churned", "churn"] as const;
const NO_DECISION = ["no decision", "no response", "no reply", "unresponsive", "gone quiet", "went quiet", "ghosted", "on hold", "hold", "paused", "deferred", "postponed", "abandoned", "stalled", "nurture"] as const;
const STOP = ["won", "win", "open"] as const;

/** The lists, for the tests and the decision row. */
export const STAGE_NAME_WORDS = { lost: LOST, no_decision: NO_DECISION, stop: STOP } as const;

/** Letters and digits only, words apart, lower case: "ClosedLost_NoBudget" reads "closed lost no budget". */
export function normaliseStageName(name: string): string {
  return name
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

const has = (normalised: string, phrase: string): boolean => ` ${normalised} `.includes(` ${phrase} `);

/** One name's reading: a side, "both" where the lists or a stop word say it can't be read, or null where it says nothing. */
function readName(name: string): StageOutcome | "both" | null {
  const n = normaliseStageName(name);
  if (!n) return null;
  if (STOP.some((w) => has(n, w))) return "both";
  const lost = LOST.some((w) => has(n, w));
  const noDecision = NO_DECISION.some((w) => has(n, w));
  if (lost && noDecision) return "both";
  return lost ? "lost" : noDecision ? "no_decision" : null;
}

/** A closed, not-won stage's outcome by its name, or null when the name can't say. The caller has checked the flags. */
export function outcomeFromName(label: string, apiName: string): StageOutcome | null {
  const readings = [readName(label), readName(apiName)];
  if (readings.includes("both")) return null;
  const sides = new Set(readings.filter((r): r is StageOutcome => r !== null));
  return sides.size === 1 ? [...sides][0]! : null;
}
