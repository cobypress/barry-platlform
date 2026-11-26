import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { passedOnSpans } from "./passed-on.ts";
import { notFigures } from "./named.ts";

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

const WORD = Symbol("not a figure");
/** A word of ours that reads as a number but claims none, wrapped for `said` (DECISIONS.md 773). */
export type NotAFigure = { readonly [WORD]: string };

/**
 * A number word of ours that is no claim: "we don't impute one", "count them
 * twice", "one trigger per object". The text is ours as written; said records
 * where it sits so the strict check (DECISIONS.md 773, in shadow) sets it aside.
 * Nothing else reads it: the reader and every served check see the word as before.
 * A lint test refuses it around a digit or anything but one literal.
 */
export const word = (text: string): NotAFigure => ({ [WORD]: text });

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
/** A string literal type and never `any`: what a helper such as `plural` may take as words of its own (DECISIONS.md 657). */
export type LiteralText<T> = 0 extends 1 & T ? never : Literal<T>;
const PASSED = Symbol("passed on");
/** A reason read elsewhere, handed on into a sentence by `passedOn`, with the passed-on route's spans or unknown. */
export type PassedOn = { readonly [PASSED]: { text: string; spans: OrgSpan[] | null; passed: OrgSpan[] } };

/** What `said` takes: a number, org words, a reason passed on, a sentence already made, or a literal. Never `any`. */
type Piece<T> = 0 extends 1 & T ? never : T extends number | OrgWords | PassedOn | Said | NotAFigure ? T : Literal<T>;

/**
 * One analysis's sentences and their spans. A text made twice with different
 * spans is held as unknown, since a later lookup can't tell which was meant.
 */
class Ledger {
  readonly #spans = new Map<string, OrgSpan[] | null>();
  readonly #passed = new Map<string, OrgSpan[]>();
  /** Where a sentence holds a reason passed on (DECISIONS.md 743), so a reason equal to a value the org chose is never ours. */
  notePassed(text: string, ranges: OrgSpan[]): void {
    if (ranges.length === 0) return;
    const held = this.#passed.get(text) ?? [];
    for (const r of ranges) if (!held.some((h) => h.start === r.start && h.end === r.end)) held.push({ ...r });
    this.#passed.set(text, held);
  }
  passed(text: string): OrgSpan[] {
    return this.#passed.get(text) ?? [];
  }
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
 * the safe side (marks degrade to unknown, as before decision 655). One run of
 * the small synthetic org adds about 160, so only a single run of a very big org
 * could fill one; each health check now has a store of its own (below), freed
 * when it ends, so a run that fills its own falls back to unknown marks and is
 * held for a person, and nothing carries to the next (DECISIONS.md 743, 749).
 */
const TAINTED_LIMIT = 100_000;
type Store = { texts: Set<string>; full: boolean };
const processStore: Store = { texts: new Set(), full: false };
/**
 * Each health check's own store (DECISIONS.md 743). A worker runs several orgs' checks in one
 * process, one after another and side by side (its concurrency), and a store shared by all of them
 * let one org's words reach another's run: an org whose stage is named "Modify All Data" left the
 * next org's literal of the same words unknown, and held its run. runHealthCheck runs inside
 * withRunOrgWords, so every sentence said makes during a check, in its analysis or its reads, is
 * kept in that check's store alone, through every await, and no check reads another's. Outside any
 * check (a test, a script) the process's store is used, as before.
 */
const runStores = new AsyncLocalStorage<Store>();
const store = (): Store => runStores.getStore() ?? processStore;

/** Runs a health check with a store of org-worded sentences of its own. */
export function withRunOrgWords<T>(fn: () => T): T {
  return runStores.run({ texts: new Set(), full: false }, fn);
}

/**
 * Set by the worker (DECISIONS.md 749): every sentence with org words it makes should be made inside
 * a health check's own store. One made outside means the check's context was lost on the way, so the
 * worker would be back to sharing one store between customers: said once, with no org text and no id.
 */
let outsideRun: { log: (line: string) => void; said: boolean } | null = null;
export function expectRunStores(log: (line: string) => void): void {
  outsideRun = { log, said: false };
}

function taint(text: string, spans: OrgSpan[] | null): void {
  if (spans && spans.length === 0) return;
  if (outsideRun && !outsideRun.said && !runStores.getStore()) {
    outsideRun.said = true;
    outsideRun.log("A sentence with the org's words was made outside any health check's own org-word store, so checks in this worker may share one. Shown once; see DECISIONS.md 749.");
  }
  const held = store();
  if (held.texts.has(text)) return;
  if (held.texts.size < TAINTED_LIMIT) held.texts.add(text);
  else if (!held.full) {
    held.full = true;
    // Once, and never with org text: from here every mark degrades to unknown until the worker restarts (DECISIONS.md 663).
    console.warn(`The org-word store is full (${TAINTED_LIMIT} sentences): sentence marks fall back to unknown until the worker restarts.`);
  }
}

/** How full the process's store of org-worded sentences is, for the worker's log line (DECISIONS.md 663). Counts only, never text. */
export const orgWordsStore = (): { size: number; limit: number; full: boolean } => ({ size: store().texts.size, limit: TAINTED_LIMIT, full: store().full });

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

/** Whether said made this text in this analysis at all, its marks known or not (DECISIONS.md 693): a text it recorded as unknown stays unknown. */
export function saidMade(text: string): boolean {
  return ledger?.has(text) ?? false;
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
function piecesOf(strings: readonly string[], pieces: readonly unknown[], sentences = false): { text: string; spans: OrgSpan[] | null; passed: OrgSpan[]; words: OrgSpan[] } {
  let text = strings[0]!;
  const spans: OrgSpan[] = [];
  const passed: OrgSpan[] = [];
  const words: OrgSpan[] = [];
  let known = true;
  pieces.forEach((piece, i) => {
    const at = text.length;
    if (piece !== null && typeof piece === "object" && PASSED in piece) {
      // A reason passed on: its own spans, or unknown; and where it sits, for the org-value guard.
      const { text: reason, spans: own, passed: inner } = (piece as PassedOn)[PASSED];
      text += reason;
      if (own) for (const s of own) push(spans, at + s.start, at + s.end);
      else known = false;
      passed.push({ start: at, end: text.length });
      // A reason passed on twice keeps where the first one sits, so it too is held to the org's values.
      for (const r of inner) passed.push({ start: at + r.start, end: at + r.end });
    } else if (piece !== null && typeof piece === "object" && WORD in piece) {
      // Our own word, and no figure: no span of the org's, only where it sits (DECISIONS.md 773).
      text += (piece as NotAFigure)[WORD];
      words.push({ start: at, end: text.length });
    } else if (piece !== null && typeof piece === "object" && ORG in piece) {
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
        for (const r of ledger.passed(piece)) passed.push({ start: at + r.start, end: at + r.end });
      } else if (sentences || store().texts.has(piece) || store().full) {
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
  return { text, spans: known ? spans : null, passed, words };
}

function record(text: string, spans: OrgSpan[] | null, passed: OrgSpan[] = []): void {
  ledger?.note(text, spans);
  ledger?.notePassed(text, passed);
  taint(text, spans);
}

/** A sentence with the characters it took from the org marked. Returns the plain string. */
export function said<const P extends readonly unknown[]>(strings: TemplateStringsArray, ...pieces: P & { readonly [K in keyof P]: Piece<P[K]> }): Said {
  const { text, spans, passed, words } = piecesOf(strings, pieces);
  record(text, spans, passed);
  if (words.length > 0) notFigures(text, words);
  return text as Said;
}

/**
 * A reason read elsewhere and passed on inside a sentence said makes, such as
 * `said\`Field usage on ${org(label)} couldn't be read: ${passedOn(scan.reason)}\``
 * (DECISIONS.md 743): known by the passed-on route's words (702), with the object
 * it names marked as the org's, or unknown, which makes the whole sentence
 * unknown. A sentence said already made keeps its own marks.
 */
export function passedOn(reason: string): PassedOn {
  // Recorded nowhere alone: the reason is vouched for only inside the sentence, where its place is kept, so a
  // reason equal to a value the org chose is found and left unknown when the run's values are known.
  const spans = ledger?.has(reason) ? ledger.get(reason) : passedOnSpans(reason);
  return { [PASSED]: { text: reason, spans: spans ? spans.map((s) => ({ ...s })) : null, passed: (ledger?.passed(reason) ?? []).map((r) => ({ ...r })) } };
}

/** Where a sentence said made holds a reason passed on, in this analysis (DECISIONS.md 743). */
export function passedRangesOf(text: string): OrgSpan[] {
  return ledger?.passed(text) ?? [];
}

/** Sentences joined, as `Array.join` would, keeping their spans. Every part must be one `said` made in this analysis, or the whole is unknown. */
export function joinSaid(parts: readonly Said[], separator: "" | " " | ", " | "; " | ". "): Said {
  // The separator is a fixed string; every part is interpolated, so the fixed strings around them are all empty.
  const strings = [""];
  for (let i = 0; i < parts.length; i++) strings.push(i < parts.length - 1 ? separator : "");
  const { text, spans, passed } = piecesOf(strings, parts, true);
  record(text, spans, passed);
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
