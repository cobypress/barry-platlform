import type { StageOutcome } from "./stage-outcomes.ts";

// Whether a closed, not-won stage is lost or no decision, read from its name by
// a fixed rule (DECISIONS.md 756): no model, no network, the same answer every
// time. Salesforce's flags have already said the stage is closed and not won;
// a name never makes a stage won or open, and is never read for any other
// stage. Used only on an automatic run with HC_STAGE_NAMES on, below a
// person's calibration and the customer's answer, and always said as an
// assumption the customer can correct. A wrong reading moves a win rate, so
// the rule reads a name only when it is plainly an outcome and nothing else:
// in doubt, undecided.
//
// The shape (the review's stricter one): the name is normalised (camel case
// split, case folded, anything but a letter or digit a space; a letter outside
// basic Latin makes the whole name undecided for now, so no other language is
// half read), then every outcome phrase is taken out, and what is left must be
// only the filler below. One side's phrases, and nothing else: that side. Both
// sides, a stop word anywhere, or any other word left (a negator such as "not",
// "never", "no longer"; a number; "contact"): undecided. So "Not Lost",
// "Lost 2", "Lost Contact" and "Lost - No Decision" decide nothing, while
// "Closed Lost", "Lost to Competitor", "Closed - No Decision", "On Hold" and
// "Disqualified" do. Label and API name are read apart: a name with no outcome
// phrase says nothing, so the other decides; any disagreement is undecided.
//
// Lost: the buyer chose against us, or we chose against the deal.
//   "disqualified", "unqualified", "not qualified": a decision was made (ours)
//   that the deal won't happen, a loss, not a deal that fizzled. "no budget":
//   the usual loss reason ("Closed Lost - No Budget" is lost). "not
//   interested" and "not qualified" are phrases in their own right, so their
//   "not" is part of them, not a negator.
// No decision: the buyer never decided, and the deal went quiet or was parked.
// Stop words (undecided whatever else is in the name):
//   "won", "win", "open", "reopen", "reopened": a closed, not-won stage named so
//   is too confusing to read.
//   "dead": used for every kind of loss and for fizzled deals alike.
//   "cancelled", "canceled": a decided purchase cancelled, or a project never
//   decided. "withdrawn": by us a loss, by the buyer no decision.
//   "duplicate", "error": not an outcome at all ("Opened in Error"); excluding
//   such deals is a person's choice. "timing": a loss reason as often as a deferral.
//   "converted", "recovered", "recovery", "found", "review", "risk",
//   "mitigated", "prevention", "billing", "revenue", "cart", "harmless",
//   "maker", "momentum", "upsell", "off": words of other processes that share
//   a word with an outcome ("Lost & Found", "Loss Prevention", "Hold Harmless",
//   "Deferred Revenue", "No Decision Maker", "Hold Off", "Lost Momentum").

const LOST = ["lost to competitor", "lost to competition", "not interested", "not qualified", "no budget", "disqualified", "unqualified", "competitor", "competition", "rejected", "declined", "churned", "churn", "lost", "lose", "loss"] as const;
const NO_DECISION = ["no decision", "no response", "no reply", "unresponsive", "gone quiet", "went quiet", "ghosted", "on hold", "hold", "paused", "deferred", "postponed", "abandoned", "stalled", "nurture"] as const;
const STOP = [
  "won", "win", "open", "reopen", "reopened", "dead", "cancelled", "canceled", "withdrawn", "duplicate", "error", "timing", "converted", "recovered",
  "recovery", "found", "review", "risk", "mitigated", "prevention", "billing", "revenue", "cart", "harmless", "maker", "momentum", "upsell", "off",
] as const;
/** Words that may stand beside an outcome without changing it. */
const FILLER = ["closed", "close", "to", "a", "an", "by", "the", "deal", "deals", "opportunity", "opp", "customer", "prospect", "client", "buyer", "stage"] as const;

/** The lists, for the tests and the decision row. */
export const STAGE_NAME_WORDS = { lost: LOST, no_decision: NO_DECISION, stop: STOP, filler: FILLER } as const;

/** Letters and digits only, words apart, lower case: "ClosedLost_NoBudget" reads "closed lost no budget". Letters outside basic Latin are kept, never turned to spaces. */
export function normaliseStageName(name: string): string {
  return name
    .normalize("NFC")
    .replace(/(\p{Ll}|\p{N})(\p{Lu})/gu, "$1 $2")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

/** Takes every whole-word occurrence of a phrase out of a padded name; true when there was one. */
function takeOut(padded: { text: string }, phrase: string): boolean {
  const needle = ` ${phrase} `;
  if (!padded.text.includes(needle)) return false;
  while (padded.text.includes(needle)) padded.text = padded.text.replace(needle, " ");
  return true;
}

/** One name's reading: a side, "unclear" where it holds an outcome but can't be read as one, or null where it holds none. */
function readName(name: string): StageOutcome | "unclear" | null {
  const n = normaliseStageName(name);
  if (!n) return null;
  // English only for now: a letter outside basic Latin, and the name isn't read at all.
  if (/[^a-z0-9 ]/.test(n)) return "unclear";
  const words = n.split(" ");
  const padded = { text: ` ${n} ` };
  // Longest phrases first, so "lost to competitor" goes whole before "lost" or "competitor".
  const lost = [...LOST].sort((a, b) => b.length - a.length).map((p) => takeOut(padded, p)).some(Boolean);
  const noDecision = [...NO_DECISION].sort((a, b) => b.length - a.length).map((p) => takeOut(padded, p)).some(Boolean);
  if (!lost && !noDecision) return STOP.some((w) => words.includes(w)) ? "unclear" : null;
  if (lost && noDecision) return "unclear";
  if (STOP.some((w) => words.includes(w))) return "unclear";
  const left = padded.text.split(" ").filter((w) => w !== "" && !(FILLER as readonly string[]).includes(w));
  if (left.length > 0) return "unclear";
  return lost ? "lost" : "no_decision";
}

/** A closed, not-won stage's outcome by its name, or null when the name can't say. The caller has checked the flags. */
export function outcomeFromName(label: string, apiName: string): StageOutcome | null {
  const readings = [readName(label), readName(apiName)];
  if (readings.includes("unclear")) return null;
  const sides = new Set(readings.filter((r): r is StageOutcome => r !== null));
  return sides.size === 1 ? [...sides][0]! : null;
}
