import { engineTextKey } from "../digest/engine-texts-key.ts";
import { ENGINE_TEXT_KEYS } from "../digest/engine-texts.ts";
import { REASON_PREFIX_KEYS, REASON_PREFIX_LONGEST } from "../digest/reason-prefixes.ts";
import { refusalSpans } from "../salesforce/refusal-words.ts";
import { orgWordsOf, saidMade, type OrgSpan } from "./org-words.ts";

// A refused read's reason passed on by a rule or an extractor, known as ours
// (DECISIONS.md 702). Rules write `Quotas couldn't be read: ${quotas.reason}`
// as a plain template, which said can't vouch for, so on an automatic run every
// check a refused read cost would hold the run for a person. The words before
// the reason are a template head the engine wrote whole (src/digest/reason-prefixes.ts,
// from the source), and the reason is one of the sentences in
// src/salesforce/refusal-words.ts, whose one org word, the object, is marked.
// Anything else stays unknown, as before.

/** How many heads may stand before the sentence: a rule's, and an extractor's inside it. */
const MAX_DEPTH = 3;

/**
 * The org's words in a passed-on refusal, as spans, or null when the text isn't
 * one. Never for a text equal to a value the org chose (DECISIONS.md 693): the
 * org could have written it whole.
 */
export function passedOnSpans(text: string, orgValues: ReadonlySet<string> = new Set(), depth = 0): OrgSpan[] | null {
  if (orgValues.has(text)) return null;
  const own = refusalSpans(text);
  if (own) return own;
  // A reason the engine's source writes whole, such as an extractor's or a loader's own sentence, passed on
  // whole or after a head (DECISIONS.md 743): ours, as engineOwn knows a metric's whole text. One said made
  // (passedOn notes the reasons it is given) keeps its own marks, unknown included.
  if (ENGINE_TEXT_KEYS.has(engineTextKey(text))) return saidMade(text) ? orgWordsOf(text) : [];
  if (depth >= MAX_DEPTH) return null;
  // No head is longer than the longest in the source, so the search stops there, whatever the text's length.
  const longest = Math.min(text.length - 1, REASON_PREFIX_LONGEST);
  for (let i = 1; i <= longest; i++) {
    if (!REASON_PREFIX_KEYS.has(engineTextKey(text.slice(0, i)))) continue;
    const rest = passedOnSpans(text.slice(i), orgValues, depth + 1);
    if (rest) return rest.map((s) => ({ start: s.start + i, end: s.end + i }));
  }
  return null;
}
