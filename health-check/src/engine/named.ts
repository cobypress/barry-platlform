// The values a sentence of ours writes, named where it is made (DECISIONS.md 768,
// KAN-102). The strict check (src/digest/strict.ts, in shadow) traces each
// finding's figures to that finding's own computed values, never to its own
// prose; a value a rule computes and writes but never put in `facts` is named
// here, beside the sentence: `named(said\`...${rate}%...\`, { rate })`. The text
// is given back unchanged, so nothing a run serves or stores moves: the values
// go only to the run folder's named-values.json, never into a finding, the
// digest or a stored row (`facts` is stored and served; this isn't).

import { extractFigures } from "../numbercheck/extract.ts";

/** Where a run keeps them, beside findings.json. Run folder only: never in the digest, never stored, never served. */
export const NAMED_VALUES_FILE = "named-values.json";

/** Values named for the strict check, by finding or metric id, then by name ("name", or "name@unit" where the name doesn't say it). */
export type NamedValues = Readonly<Record<string, Readonly<Record<string, number>>>>;

/** A unit given with a value whose name doesn't carry it: `{ limit: [limit, "percent"] }`. */
export const NAMED_UNITS = ["money", "percent", "points", "ratio", "day", "hour", "week", "month", "minute", "year", "date"] as const;
export type NamedUnit = (typeof NAMED_UNITS)[number];
type Value = number | null | undefined | readonly [number | null | undefined, Exclude<NamedUnit, "date">] | readonly [string | null | undefined, "date"];

/** A date as kept: yyyymmdd, or yyyymm00 for a month, from an ISO date ("2025-12-31", "2026-02"). Null when it isn't one. */
export function dateValue(iso: string): number | null {
  const m = /^(\d{4})-(\d{2})(?:-(\d{2}))?/.exec(iso);
  return m ? Number(m[1]) * 10000 + Number(m[2]) * 100 + Number(m[3] ?? 0) : null;
}

/** Who was making sentences when one was named: the rule or metric builder running, or null between them. */
type Entry = { text: string; values: Record<string, number>; owner: string | null };

/** The sentences named in the analysis running now, or null outside one. Analyses run one at a time, synchronously. */
let ledger: Entry[] | null = null;
let owner: string | null = null;

/**
 * Sentences named outside any analysis: a rule's fixed title, made once when its
 * module loads, with the constant it writes. Each text once, and only so many,
 * since nothing is ever taken out. A finding takes only those of their values
 * the sentence writes (namedIn).
 */
const fixed = new Map<string, Record<string, number>>();
const FIXED_MAX = 5000;

/** The values kept, each a finite number, keyed by name or "name@unit". */
function kept(values: Readonly<Record<string, Value>>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const [name, v] of Object.entries(values)) {
    const [raw, unit] = Array.isArray(v) ? v : [v, undefined];
    // A date is kept as a number, yyyymmdd, under "name@date" (DECISIONS.md 772).
    const value = unit === "date" ? (typeof raw === "string" ? dateValue(raw) : null) : raw;
    if (typeof value === "number" && Number.isFinite(value)) out[unit ? `${name}@${unit}` : name] = value;
  }
  return out;
}

/** The text, unchanged, with the values it writes named for the strict check. An empty or blank text names nothing. */
export function named<T extends string, const V extends Readonly<Record<string, Value>>>(text: T, values: V): T {
  if (text.trim() === "") return text;
  const k = kept(values);
  if (Object.keys(k).length === 0) return text;
  if (ledger) ledger.push({ text, values: k, owner });
  else if (fixed.size < FIXED_MAX && !fixed.has(text)) fixed.set(text, k);
  return text;
}

/** Each item's values under one set of names, the second and later as "name#2" and on, for a sentence that lists them. */
export function each<T>(items: readonly T[], values: (item: T) => Readonly<Record<string, Value>>): Record<string, Value> {
  const out: Record<string, Value> = {};
  items.forEach((item, i) => {
    for (const [name, value] of Object.entries(values(item))) out[i === 0 ? name : `${name}#${i + 1}`] = value;
  });
  return out;
}

/** Where in a sentence of ours a number word claims nothing (word(), DECISIONS.md 773), by sentence. */
export type NotFigures = Readonly<Record<string, ReadonlyArray<{ sentence: string; spans: ReadonlyArray<{ start: number; end: number }> }>>>;
/** Where a run keeps them, by finding or metric id. Run folder only. */
export const NOT_FIGURES_FILE = "not-figures.json";
type WordEntry = { text: string; spans: Array<{ start: number; end: number }>; owner: string | null };
let words: WordEntry[] | null = null;
const fixedWords = new Map<string, Array<{ start: number; end: number }>>();

/** Records where a sentence's word() pieces sit, as said makes it. */
export function notFigures(text: string, spans: Array<{ start: number; end: number }>): void {
  if (words) words.push({ text, spans, owner });
  else if (fixedWords.size < FIXED_MAX && !fixedWords.has(text)) fixedWords.set(text, spans);
}

/**
 * word() for a whole literal of ours that can't be a said template without
 * losing what it is (a metric's definition, a rule's title, which the engine
 * texts' scan knows as written): the text, unchanged, with each whole `w` in it
 * recorded as no figure. The same lint holds `w` to one fixed word, never a digit.
 */
export function aside<T extends string>(text: T, w: string): T {
  const spans: Array<{ start: number; end: number }> = [];
  for (const m of text.matchAll(new RegExp(String.raw`(?<![\w-])${w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?![\w-])`, "g"))) spans.push({ start: m.index, end: m.index + w.length });
  if (spans.length > 0) notFigures(text, spans);
  return text;
}

/** The sentences these texts hold whole that carry word() pieces, made while `by` ran or at a module's load. */
export function notFiguresIn(texts: ReadonlyArray<string | null | undefined>, by: string | readonly string[]): Array<{ sentence: string; spans: Array<{ start: number; end: number }> }> | null {
  const owners = new Set(typeof by === "string" ? [by] : by);
  if (!words) return null;
  const own = texts.filter((t): t is string => typeof t === "string" && t.trim() !== "");
  const out = new Map<string, Array<{ start: number; end: number }>>();
  for (const [text, spans] of fixedWords) if (own.some((t) => t.includes(text))) out.set(text, spans);
  for (const e of words) if (e.owner !== null && owners.has(e.owner) && !out.has(e.text) && own.some((t) => t.includes(e.text))) out.set(e.text, e.spans);
  return out.size > 0 ? [...out].map(([sentence, spans]) => ({ sentence, spans })) : null;
}

/** Runs one analysis with its own ledger, as `said`'s marks are (DECISIONS.md 655). */
export function withNamedValues<T>(run: () => T): T {
  const [outerLedger, outerOwner, outerWords] = [ledger, owner, words];
  ledger = [];
  words = [];
  owner = null;
  try {
    return run();
  } finally {
    [ledger, owner, words] = [outerLedger, outerOwner, outerWords];
  }
}

/** Runs one rule or metric builder as the owner of the sentences named while it runs. */
export function namedFor<T>(key: string, run: () => T): T {
  const outer = owner;
  owner = key;
  try {
    return run();
  } finally {
    owner = outer;
  }
}

/** The figures a sentence writes, as values: a module-level sentence lends a finding only those of its values. */
function written(text: string): Set<number> {
  return new Set(extractFigures(text).figures.flatMap((f) => (f.value === undefined ? [] : [f.value])));
}

/**
 * The values named in sentences these texts hold whole, by name; a name already
 * taken by another value gets a number after it. Null when none. Only sentences
 * made while `by` (the finding's rule, or the metric's builder) ran, so a value
 * named for another finding never backs this one; and, of a module-level
 * sentence, only the values it writes.
 */
export function namedIn(texts: ReadonlyArray<string | null | undefined>, by: string | readonly string[]): Record<string, number> | null {
  const owners = new Set(typeof by === "string" ? [by] : by);
  if (!ledger) return null;
  const own = texts.filter((t): t is string => typeof t === "string" && t.trim() !== "");
  const out: Record<string, number> = {};
  const seen = new Set<string>();
  const add = (values: Record<string, number>): void => {
    for (const [name, value] of Object.entries(values)) {
      let key = name;
      for (let i = 2; Object.hasOwn(out, key) && out[key] !== value; i++) key = `${name}#${i}`;
      out[key] = value;
    }
  };
  for (const [text, values] of fixed) {
    if (!own.some((t) => t.includes(text))) continue;
    seen.add(text);
    const shown = written(text);
    add(Object.fromEntries(Object.entries(values).filter(([, v]) => shown.has(v))));
  }
  for (const entry of ledger) {
    if (entry.owner === null || !owners.has(entry.owner) || seen.has(entry.text) || !own.some((t) => t.includes(entry.text))) continue;
    seen.add(entry.text);
    add(entry.values);
  }
  return Object.keys(out).length > 0 ? out : null;
}
