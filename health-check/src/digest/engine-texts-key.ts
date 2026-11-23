import { createHash } from "node:crypto";

// The key a text is known by among the engine's own (DECISIONS.md 693): every
// run of digits, with the separators a figure, a date or a time holds inside
// it, read as one figure, so "3 deals in 2026-02-01" and the template
// "${n} deals in ${from}" read the same; then a short hash. A substitution is
// matched only where the run put a figure there, never a word, so a text the
// org could have put words into is never taken for ours.

const FIGURE = /\d(?:[\d,.:/-]*\d)?/g;

export function engineTextKey(text: string): string {
  return createHash("sha256").update(text.replace(FIGURE, "0"), "utf8").digest("hex").slice(0, 16);
}
