import { engineTextKey } from "../digest/engine-texts-key.ts";
import { REASON_PREFIX_KEYS } from "../digest/reason-prefixes.ts";
import { refusalSpans } from "../salesforce/refusal-words.ts";
import type { OrgSpan } from "./org-words.ts";

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

/** The org's words in a passed-on refusal, as spans, or null when the text isn't one. */
export function passedOnSpans(text: string, depth = 0): OrgSpan[] | null {
  const own = refusalSpans(text);
  if (own) return own;
  if (depth >= MAX_DEPTH) return null;
  for (let i = 1; i < text.length; i++) {
    if (!REASON_PREFIX_KEYS.has(engineTextKey(text.slice(0, i)))) continue;
    const rest = passedOnSpans(text.slice(i), depth + 1);
    if (rest) return rest.map((s) => ({ start: s.start + i, end: s.end + i }));
  }
  return null;
}
