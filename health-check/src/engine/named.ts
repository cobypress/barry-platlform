// The values a sentence of ours writes, named where it is made (DECISIONS.md 768,
// KAN-102). The strict check (src/digest/strict.ts, in shadow) traces each
// finding's figures to that finding's own computed values, never to its own
// prose; a value a rule computes and writes but never put in `facts` is named
// here, beside the sentence: `named(said\`...${rate}%...\`, { rate })`. The text
// is given back unchanged, so nothing a run serves or stores moves: the values
// go only to the run folder's named-values.json, never into a finding, the
// digest or a stored row (`facts` is stored and served; this isn't).

/** Where a run keeps them, beside findings.json. Run folder only: never in the digest, never stored, never served. */
export const NAMED_VALUES_FILE = "named-values.json";

/** Values named for the strict check, by finding or metric id, then by name. */
export type NamedValues = Readonly<Record<string, Readonly<Record<string, number>>>>;

type Entry = { text: string; values: Record<string, number> };

/** The sentences named in the analysis running now, or null outside one. Analyses run one at a time, synchronously. */
let ledger: Entry[] | null = null;

/**
 * Sentences named outside any analysis: a rule's fixed title, made once when its
 * module loads, with the constant it writes. Each text once, and only so many,
 * since nothing is ever taken out.
 */
const fixed = new Map<string, Record<string, number>>();
const FIXED_MAX = 5000;

/** The text, unchanged, with the values it writes named for the strict check. */
export function named<T extends string>(text: T, values: Readonly<Record<string, number | null | undefined>>): T {
  const kept: Record<string, number> = {};
  for (const [name, value] of Object.entries(values)) if (typeof value === "number" && Number.isFinite(value)) kept[name] = value;
  if (Object.keys(kept).length === 0) return text;
  if (ledger) ledger.push({ text, values: kept });
  else if (fixed.size < FIXED_MAX && !fixed.has(text)) fixed.set(text, kept);
  return text;
}

/** Each item's values under one set of names, the second and later as "name#2" and on, for a sentence that lists them. */
export function each<T>(items: readonly T[], values: (item: T) => Readonly<Record<string, number | null | undefined>>): Record<string, number | null | undefined> {
  const out: Record<string, number | null | undefined> = {};
  items.forEach((item, i) => {
    for (const [name, value] of Object.entries(values(item))) out[i === 0 ? name : `${name}#${i + 1}`] = value;
  });
  return out;
}

/** Runs one analysis with its own ledger, as `said`'s marks are (DECISIONS.md 655). */
export function withNamedValues<T>(run: () => T): T {
  const outer = ledger;
  ledger = [];
  try {
    return run();
  } finally {
    ledger = outer;
  }
}

/**
 * Every value named in a sentence one of these texts holds whole, by name; a
 * name already taken by another value gets a number after it. Null when none.
 * A finding's or metric's own texts only, so another's never backs it.
 */
export function namedIn(texts: ReadonlyArray<string | null | undefined>): Record<string, number> | null {
  if (!ledger) return null;
  const own = texts.filter((t): t is string => typeof t === "string" && t !== "");
  const out: Record<string, number> = {};
  const seen = new Set<string>();
  for (const entry of [...[...fixed].map(([text, values]) => ({ text, values })), ...ledger]) {
    if (seen.has(entry.text) || !own.some((t) => t.includes(entry.text))) continue;
    seen.add(entry.text);
    for (const [name, value] of Object.entries(entry.values)) {
      let key = name;
      for (let i = 2; Object.hasOwn(out, key) && out[key] !== value; i++) key = `${name}#${i}`;
      out[key] = value;
    }
  }
  return Object.keys(out).length > 0 ? out : null;
}
