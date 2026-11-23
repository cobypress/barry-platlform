import type { OrgSpan } from "../engine/org-words.ts";

// Text with the characters the org chose marked, for the fix prompt (DECISIONS.md
// 672). The prompt is put together from these pieces, so its marks are made where
// it is assembled, never by searching the finished text. Spans are null when any
// piece can't be vouched for: the whole is then unknown, never wrongly marked.

/** Text and its org spans (UTF-16 code units, end exclusive, sorted and apart), or null spans when unknown. */
export type Marked = { text: string; spans: OrgSpan[] | null };

/** The engine's own words. */
export const ours = (text: string): Marked => ({ text, spans: [] });

/** A value the org chose, whole. */
export const theirs = (text: string): Marked => ({ text, spans: text === "" ? [] : [{ start: 0, end: text.length }] });

/** Text whose marks are not known. */
export const unknown = (text: string): Marked => ({ text, spans: null });

/** Adds a span, joining it to the one before when they touch, as `said` does. */
function push(spans: OrgSpan[], start: number, end: number): void {
  if (end <= start) return;
  const last = spans.at(-1);
  if (last && last.end === start) last.end = end;
  else spans.push({ start, end });
}

/** Pieces end to end. */
export function cat(...parts: readonly Marked[]): Marked {
  let text = "";
  const spans: OrgSpan[] = [];
  let known = true;
  for (const p of parts) {
    if (!p.spans) known = false;
    else for (const s of p.spans) push(spans, text.length + s.start, text.length + s.end);
    text += p.text;
  }
  return { text, spans: known ? spans : null };
}

/** Pieces joined, as `Array.join` would, by the engine's separator. */
export const joinMarked = (parts: readonly Marked[], separator: string): Marked => cat(...parts.flatMap((p, i) => (i === 0 ? [p] : [ours(separator), p])));

/** Characters `from` to `to`, with the spans inside them cut to fit: an org value's part is still the org's. */
export function sliceMarked(m: Marked, from: number, to: number): Marked {
  const text = m.text.slice(from, to);
  if (!m.spans) return unknown(text);
  const spans: OrgSpan[] = [];
  for (const s of m.spans) push(spans, Math.max(s.start, from) - from, Math.min(s.end, to) - from);
  return { text, spans };
}

/** Whether any org span has a character in `from` to `to`. */
const touches = (spans: readonly OrgSpan[], from: number, to: number): boolean => spans.some((s) => s.start < to && s.end > from);
/** Whether an org span crosses either end of `from` to `to`, so a rewrite of it would cut the org's value. */
const cuts = (spans: readonly OrgSpan[], from: number, to: number): boolean => spans.some((s) => (s.start < from && s.end > from) || (s.start < to && s.end > to));

/**
 * `text.replace(pattern, by)` for a global pattern, carrying the marks. With
 * `fixed`, what a match becomes is new text, the engine's words unless the
 * replacer gives it marked (a subject's type the engine can't vouch for): the
 * match may take in whole org spans, which go, but never cut one. Without it the rewrite
 * keeps part of the match, so only the characters that change are new, and they
 * may replace only the engine's characters. Anything else is unknown. The
 * replacer is called once per match, in order, as String.replace calls it.
 */
export function replaceMarked(m: Marked, pattern: RegExp, by: string | Marked | ((match: string, ...groups: string[]) => string), fixed: boolean): Marked {
  if (!pattern.global) throw new Error("replaceMarked takes a global pattern");
  // A replacement string could hold $ patterns, which would echo the match: worked out by String.replace, unknown.
  if (typeof by === "string" && by.includes("$")) return unknown(m.text.replace(pattern, by));
  const parts: Marked[] = [];
  let known = m.spans !== null;
  let at = 0;
  for (const hit of m.text.matchAll(pattern)) {
    const [from, match] = [hit.index, hit[0]];
    const to = from + match.length;
    const made = typeof by === "function" ? (by as (...args: unknown[]) => string)(match, ...hit.slice(1), from, m.text) : by;
    parts.push(sliceMarked(m, at, from));
    at = to;
    if (fixed) {
      if (m.spans && cuts(m.spans, from, to)) known = false;
      parts.push(typeof made === "string" ? ours(made) : made);
      continue;
    }
    // Only fixed rewrites take marked text.
    if (typeof made !== "string") throw new Error("A rewrite that keeps part of its match gives a string");
    const out = made;
    // What the match and its rewrite share at each end is kept as it was, marks and all.
    let head = 0;
    while (head < match.length && head < out.length && match[head] === out[head]) head++;
    let tail = 0;
    while (tail < match.length - head && tail < out.length - head && match[match.length - 1 - tail] === out[out.length - 1 - tail]) tail++;
    if (m.spans && touches(m.spans, from + head, to - tail)) known = false;
    parts.push(sliceMarked(m, from, from + head), ours(out.slice(head, out.length - tail)), sliceMarked(m, to - tail, to));
  }
  parts.push(sliceMarked(m, at, m.text.length));
  const whole = cat(...parts);
  return known ? whole : unknown(whole.text);
}
