import { createHash } from "node:crypto";

// Which characters of a finding's sentence were chosen inside the customer's org
// (DECISIONS.md 655): a flow's label, a field's name, a stage. The connector
// serves the digest to a customer's AI assistant, and an insider or a managed
// package can word a label as an instruction, so the engine says which words
// are the org's. The marks are made where the sentence is put together, never by
// searching a finished sentence for names and never by Claude.
//
// A rule writes `said\`… ${org(label)} …\``. `said` returns a plain string, so
// nothing a rule returns or a document prints changes, and records the
// sentence's spans in a ledger that lives for one analysis. The engine looks
// each finished sentence up there; one that isn't there is unknown.

/** Characters taken from the org: start inclusive, end exclusive, in UTF-16 code units. */
export type OrgSpan = { start: number; end: number };

declare const SAID: unique symbol;
/** A sentence made by `said` (or vouched for by `eng`): a plain string at run time. */
export type Said = string & { readonly [SAID]: true };

const ORG = Symbol("org words");
/** A value the org chose, wrapped for `said`. */
export type OrgWords = { readonly [ORG]: string };

/** Marks a value the org chose: a label, an API name, a stage, a picklist value. */
export const org = (text: string): OrgWords => ({ [ORG]: text });

/**
 * A string the types can't see as literal but which holds only the engine's own
 * words, such as `plural(3, "flow")` or a list of fixed tool names. Never for
 * anything read from the org: that is `org`.
 */
// It records nothing: interpolated, a string `said` didn't make is read as the
// engine's words, which is what eng vouches for, so a record would add nothing
// but a way for its text to be reused as a vouched sentence elsewhere. Every
// call is listed, and signed off, in src/engine/__tests__/org-words-eng.test.ts.
export const eng = (text: string): Said => text as Said;

/**
 * A string literal type only. `string`, a pattern such as `${string}!` (a
 * template or `as const` around a value read at run time) and an intrinsic such
 * as `Uppercase<string>` all have infinitely many members, so a record keyed by
 * them is an index signature `{}` fits; a literal's record has a required key.
 */
type Literal<T> = T extends string ? ({} extends Record<T, 1> ? never : T) : never;
/** What `said` takes: a number, org words, a sentence already made, or a literal. Never `any`. */
type Piece<T> = 0 extends 1 & T ? never : T extends number | OrgWords | Said ? T : Literal<T>;

/**
 * One analysis's sentences and their spans. A text made twice with different
 * spans is held as unknown, since a later lookup can't tell which was meant.
 */
class Ledger {
  readonly #spans = new Map<string, OrgSpan[] | null>();
  /** Spans, or null for a sentence that can't be vouched for (one with an unknown part). */
  note(text: string, spans: OrgSpan[] | null): void {
    if (!this.#spans.has(text)) {
      this.#spans.set(text, spans);
      return;
    }
    const held = this.#spans.get(text);
    if (held && (!spans || !sameSpans(held, spans))) this.#spans.set(text, null);
  }
  has(text: string): boolean {
    return this.#spans.has(text);
  }
  /** The spans of a sentence `said` made, or null: never made, or made twice differently, or with an unknown part. */
  get(text: string): OrgSpan[] | null {
    return this.#spans.get(text) ?? null;
  }
}

/**
 * Every sentence with org words, or unknown, that `said` has made in this
 * process, in any analysis or none. Only the open ledger can vouch for a
 * sentence, so one quoted from elsewhere (an analysis that has ended, the one
 * around a nested analysis, or none, as when a test calls a rule directly) must
 * not read as a literal: it's found here and reads as unknown. A sentence that
 * held only the engine's words needn't be kept, since read as a literal it is
 * what it was. Bounded: past TAINTED_LIMIT nothing more is added and every string
 * the open ledger doesn't hold reads as unknown for the rest of the process,
 * the safe side (marks degrade to unknown, as before decision 655). The engine
 * makes a few dozen such sentences an analysis, so the bound is thousands of runs.
 */
const TAINTED_LIMIT = 100_000;
const tainted = new Set<string>();
let taintedFull = false;
function taint(text: string, spans: OrgSpan[] | null): void {
  if (spans && spans.length === 0) return;
  if (tainted.has(text)) return;
  if (tainted.size >= TAINTED_LIMIT) taintedFull = true;
  else tainted.add(text);
}

const sameSpans = (a: OrgSpan[], b: OrgSpan[]): boolean => a.length === b.length && a.every((s, i) => s.start === b[i]!.start && s.end === b[i]!.end);

let ledger: Ledger | null = null;

/** Runs one analysis with its own ledger; the ledger goes when it ends, however it ends. */
export function withOrgWords<T>(fn: () => T): T {
  const outer = ledger;
  ledger = new Ledger();
  try {
    return fn();
  } finally {
    ledger = outer;
  }
}

/** A sentence's spans as `said` recorded them in this analysis, or null when it can't be vouched for. */
export function orgWordsOf(text: string): OrgSpan[] | null {
  return ledger?.get(text) ?? null;
}

/** Adds a span, joining it to the one before when they touch, so spans stay sorted and apart. */
function push(spans: OrgSpan[], start: number, end: number): void {
  if (end <= start) return;
  const last = spans.at(-1);
  if (last && last.end === start) last.end = end;
  else spans.push({ start, end });
}

/**
 * The text and its spans, or null spans when any part can't be vouched for: a
 * sentence the ledger holds as unknown, one made outside the analysis with org
 * words, or (where `sentences` says every string part is one `said` made) a part
 * the ledger doesn't hold. Unknown goes up through every level that quotes it.
 * A sentence with org words made anywhere but the open ledger is unknown here.
 */
function piecesOf(strings: readonly string[], pieces: readonly unknown[], sentences = false): { text: string; spans: OrgSpan[] | null } {
  let text = strings[0]!;
  const spans: OrgSpan[] = [];
  let known = true;
  pieces.forEach((piece, i) => {
    const at = text.length;
    if (piece !== null && typeof piece === "object" && ORG in piece) {
      const words = (piece as OrgWords)[ORG];
      text += words;
      push(spans, at, text.length);
    } else if (typeof piece === "string") {
      text += piece;
      if (ledger?.has(piece)) {
        // A sentence made by `said` brings its own spans, or its unknown.
        const held = ledger.get(piece);
        if (held) for (const s of held) push(spans, at + s.start, at + s.end);
        else known = false;
      } else if (sentences || tainted.has(piece) || taintedFull) {
        known = false;
      }
      // Otherwise a literal, all the types let through: the engine's.
    } else if (typeof piece === "number") {
      text += String(piece);
    } else {
      throw new Error("said takes numbers, org words, sentences it made and literals only");
    }
    text += strings[i + 1]!;
  });
  return { text, spans: known ? spans : null };
}

function record(text: string, spans: OrgSpan[] | null): void {
  ledger?.note(text, spans);
  taint(text, spans);
}

/** A sentence with the characters it took from the org marked. Returns the plain string. */
export function said<const P extends readonly unknown[]>(strings: TemplateStringsArray, ...pieces: P & { readonly [K in keyof P]: Piece<P[K]> }): Said {
  const { text, spans } = piecesOf(strings, pieces);
  record(text, spans);
  return text as Said;
}

/** Sentences joined, as `Array.join` would, keeping their spans. Every part must be one `said` made in this analysis, or the whole is unknown. */
export function joinSaid(parts: readonly Said[], separator: "" | " " | ", " | "; " | ". "): Said {
  // The separator is a fixed string; every part is interpolated, so the fixed strings around them are all empty.
  const strings = [""];
  for (let i = 0; i < parts.length; i++) strings.push(i < parts.length - 1 ? separator : "");
  const { text, spans } = piecesOf(strings, parts, true);
  record(text, spans);
  return text as Said;
}

/**
 * A finding's marked sentence as `Finding.orgWords` keeps it: the spans and the
 * SHA-256 of the text they were made for, so a later change to the text leaves
 * the marks unused rather than wrong.
 */
export type MarkedSentence = { spans: OrgSpan[]; textSha256: string };

export const textSha256 = (text: string): string => createHash("sha256").update(text, "utf8").digest("hex");

/** The marks for a sentence, or null when this analysis can't vouch for it. */
export function markSentence(text: string, known: OrgSpan[] | null = orgWordsOf(text)): MarkedSentence | null {
  return known ? { spans: known.map((s) => ({ ...s })), textSha256: textSha256(text) } : null;
}

/** The spans of a kept marking, only while the sentence still hashes to it and every span fits. */
export function spansIfCurrent(text: string, marked: MarkedSentence | undefined): OrgSpan[] | null {
  if (!marked || marked.textSha256 !== textSha256(text)) return null;
  return spansFit(text, marked.spans) ? marked.spans.map((s) => ({ ...s })) : null;
}

/** Each span non-empty, inside the text, sorted and apart. */
export function spansFit(text: string, spans: readonly OrgSpan[]): boolean {
  let from = 0;
  for (const s of spans) {
    if (!Number.isInteger(s.start) || !Number.isInteger(s.end) || s.start < from || s.end <= s.start || s.end > text.length) return false;
    from = s.end + 1;
  }
  return true;
}
