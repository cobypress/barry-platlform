import { extractFigures, normalise, type Figure as ReadFigure } from "../numbercheck/index.ts";
import { tokenSource } from "../privacy/pseudonymiser.ts";
import type { Finding, Metric, NumberOverride } from "../schemas/index.ts";

// An override is the only way a number changes without a re-run. The raw notes
// list it as it was, with the reason. Reports show the figure as it now stands,
// so an executive reader never sees the number a consultant has corrected.
//
// A finding's words are the rule's, and they don't say which printed figure is
// which fact. So a correction is carried into them only where that is certain,
// worked out from the finding as the run read it, never row by row: only the
// value as read and the last override's value count (DECISIONS.md 511). Where
// it isn't certain the finding stays as read, and the documents are held with a
// reason the consultant can act on (DECISIONS.md 512).
//
// A metric's chart, table and words print its parts and texts as stored, and
// none of them is rewritten. So a metric's figure is carried only where no part
// or text of the metric holds its value as read, judged the same way, and
// refused through the same hold everywhere else (DECISIONS.md 517).

/**
 * A figure an override couldn't carry into the documents, and why: a finding's,
 * with `findingId`, or a measure's, with `metricId`.
 */
export type OverrideRefusal = {
  /** The finding's title or the measure's label as read, tokens and all. Only `refusalReason` prints it, without them. */
  title: string;
  /** A finding's `facts.<key>` or `value.amount`; a measure's `value`, `n` or `stats.<key>`. */
  field: string;
  asRead: number;
  last: number;
  why:
    | { kind: "shared"; other: string }
    | { kind: "repeated"; places: number }
    | { kind: "rounded"; found: string }
    | { kind: "unsure"; found: string }
    | { kind: "stale"; from: number }
    // A measure's only: another of its figures holds the value as read, named as `refusalReason` prints it.
    | { kind: "part"; where: string }
    // A measure's only: one of its own texts prints the value as read.
    | { kind: "worded" };
} & ({ findingId: string; metricId?: never } | { metricId: string; findingId?: never });

const escapeRegex = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * A figure as the rules print it, "10,500", with every decimal kept. 100 is the
 * most Intl allows, which keeps every digit String writes. Minus zero is "0", as
 * String writes it.
 */
export const grouped = (value: number): string => value.toLocaleString("en-GB", { maximumFractionDigits: 100, signDisplay: "negative" });

/**
 * A figure as String writes it. A value String writes with an exponent (1e21 and
 * over, or under 0.000001) isn't how any rule prints one, so its digits are
 * written out in full instead, ungrouped.
 */
export const plain = (value: number): string => {
  const written = String(value);
  return /e/i.test(written) ? value.toLocaleString("en-GB", { useGrouping: false, maximumFractionDigits: 100, signDisplay: "negative" }) : written;
};

/** Never inside a longer number: no digit, point or comma before, and no digit, or point or comma then a digit, after (DECISIONS.md 506). */
const valuePattern = (value: number): RegExp => {
  const forms = [...new Set([plain(value), grouped(value)])].map(escapeRegex).join("|");
  return new RegExp(`(?<![\\d.,])(?:${forms})(?![\\d]|[.,]\\d)`, "g");
};

/**
 * A number in the wording, for the 5% test: digits with comma groups or without,
 * and a decimal part. Unsigned, so it's set against the value's size whatever
 * its sign. A currency sign before it, and %, k, m or bn after it, are kept only
 * to name it in a refusal.
 */
const NUMBER = /(?<![\d.])[£$€]?(\d{1,3}(?:,\d{3})+|\d+)(\.\d+)?(?:%|(?:k|m|bn)(?![A-Za-z]))?/g;

// A token's digits are never a figure. Each token is masked at its own length,
// so a match's place in the masked text is its place in the text, and the
// token itself is never rewritten.
const TOKEN = new RegExp(tokenSource(), "g");

// Nor are a date's or an identifier's (DECISIONS.md 514): the number check's
// own reader says which spans those are, so there is one definition of a date.
// It doesn't take a year and month, "2026-02", as one. A date written wholly in
// digits and separators (an ISO date with or without its time, 12/03/2026), an
// identifier, a year and month and a token hold digits that are never a count,
// so they're masked and never tested.
const YEAR_MONTH = /(?<![\w.,-])(?:19|20)\d{2}-(?:0[1-9]|1[0-2])(?!\d|[.,]\d|-\d)/g;
// But a date the reader reads from digits alone is a bare year, which it guesses
// from the word before: "12 of 2000 active users" is a year to it (DECISIONS.md
// 516). And one it reads by a word is a guess too: "3 may lose access" is 3 May
// to it, "In Q3 12 deals slipped" Q3 of 2012 (DECISIONS.md 518).
const bareYear = (f: ReadFigure): boolean => f.kind === "date" && /^\d+$/.test(f.raw);
const certainDate = (f: ReadFigure): boolean => f.kind === "date" && !bareYear(f) && (/^\d{4}-\d{2}-\d{2}T/.test(f.raw) || !/\p{L}/u.test(f.raw));
const notFigure = (f: ReadFigure): boolean => certainDate(f) || f.kind === "identifier";
const guessedDate = (f: ReadFigure): boolean => f.kind === "date" && !certainDate(f);
const LETTER = /\p{L}/u;

// Digits joined by a colon, a clock time, a ratio or "10:30:15", found by our
// own pattern whatever the reader calls them, since it calls "09:00" malformed
// (DECISIONS.md 515). A part may be grouped or have decimals, so no digit of
// "1.5:1" or "1,000:1" is left over to be read as a figure.
const COLON = /(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?::(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)+/g;

// A figure the reader finds with no digit in it as written, "five", "twice", "a
// dozen", "half", or the "one" and the "three" of "one in three", is a figure to
// it and to the printed-page check, which our own patterns never find
// (DECISIONS.md 520). Only digits 0 to 9 count: a full-width "５" is 5 to the
// reader and nothing to our patterns.
const DIGIT = /[0-9]/;

/**
 * A form that may or may not be the figure: a colon form, a guessed date or a
 * figure in words, as written, and its parts. It is never rewritten, and is
 * never passed over: a part equal to the value as read is a place that can't
 * be rewritten, and one near it is the figure perhaps rounded (DECISIONS.md
 * 516, 518, 520).
 */
type Unsure = { form: string; parts: number[] };

/** A text as `judge` reads it: masked, and the unsure forms taken out of it, in the order they stand. */
type Read = { text: string; unsure: Unsure[] };

/**
 * Where each span of the reader's text starts and ends in the text as given.
 * The reader works on `normalise(text)`, which can be longer or shorter ("½" is
 * " one half "), so the text is cut only where normalising each side alone gives
 * the reader's text: a piece between two cuts is masked whole if any of what it
 * became is masked. Characters that normalise together ("ㄱㅏ" is "가") are one piece.
 *
 * Finding the cuts normalises the text on each side of every place in it, so
 * they're found once for a text and every span of it is mapped through them: a
 * summary that joins every falling profile's name is thousands of characters.
 */
function spansInText(text: string, normalised: string): (spans: Array<[number, number]>) => Array<[number, number]> {
  if (normalised === text) return (spans) => spans;
  const cuts: Array<{ at: number; to: number }> = [{ at: 0, to: 0 }];
  for (let at = 1; at < text.length; at++) {
    const before = normalise(text.slice(0, at));
    if (before + normalise(text.slice(at)) === normalised) cuts.push({ at, to: before.length });
  }
  cuts.push({ at: text.length, to: normalised.length });
  return (spans) => {
    const pieces: Array<[number, number]> = [];
    for (let i = 0; i + 1 < cuts.length; i++) {
      const [from, to] = [cuts[i]!, cuts[i + 1]!];
      if (spans.some(([start, end]) => from.to < end && to.to > start)) pieces.push([from.at, to.at]);
    }
    return pieces;
  };
}

/**
 * The text with every token, date, identifier and unsure form masked at its own
 * length, so a place found in it is its place in the text. Colon forms are
 * looked for once the rest but guessed dates is masked: a time inside an ISO
 * date and time is the date's, and is neither found nor tested, while "2000:1"
 * is one colon form, not a year and a 1. Exported for its tests.
 */
export function masked(text: string): Read {
  const chars = text.replace(TOKEN, (token) => "#".repeat(token.length)).split("");
  const { normalised, figures } = extractFigures(text);
  const inText = spansInText(text, normalised);
  const shapes = inText(figures.filter((f) => notFigure(f) && !bareYear(f)).map((f): [number, number] => [f.start, f.end]));
  for (const m of text.matchAll(YEAR_MONTH)) shapes.push([m.index, m.index + m[0].length]);
  for (const [start, end] of shapes) chars.fill("#", start, end);
  const unsure: Array<Unsure & { at: number }> = [];
  for (const m of chars.join("").matchAll(COLON)) {
    unsure.push({ form: m[0], parts: m[0].split(":").map(partValue), at: m.index });
    chars.fill("#", m.index, m.index + m[0].length);
  }
  // A guessed date's digits that stand by themselves are its parts, and the form
  // is the date as written: "3 may", "March, 2000", "Q3 12". Digits joined to a
  // letter ("Q3", "FY26", "3rd") are never a count, and stay masked untested.
  for (const date of figures.filter(guessedDate)) {
    const pieces = inText([[date.start, date.end]]);
    const [start, end] = [Math.min(...pieces.map(([a]) => a)), Math.max(...pieces.map(([, b]) => b))];
    const parts: number[] = [];
    for (const run of text.slice(start, end).matchAll(/\d+/g)) {
      const [from, to] = [start + run.index, start + run.index + run[0].length];
      // Inside a colon form it is that form's part, and inside a year and month,
      // "in 2026-02", it is part of a date, never tested.
      if (chars.slice(from, to).includes("#")) continue;
      if (!LETTER.test(text[from - 1] ?? "") && !LETTER.test(text[to] ?? "")) parts.push(Number(run[0]));
      chars.fill("#", from, to);
    }
    if (parts.length > 0) unsure.push({ form: text.slice(start, end), parts, at: start });
  }
  // A figure in words: its one part is the value the reader gives it, whatever
  // its kind ("twice" a ratio of 2, "half" a fraction of 50, "five per cent" a
  // percent of 5, "a dozen" a number of 12). A figure the reader gives no value,
  // "hundreds" or a malformed run of words, can be no figure's value or near it.
  for (const word of figures) {
    if (typeof word.value !== "number") continue;
    const pieces = inText([[word.start, word.end]]);
    if (pieces.length === 0) continue;
    const [start, end] = [Math.min(...pieces.map(([a]) => a)), Math.max(...pieces.map(([, b]) => b))];
    const form = text.slice(start, end);
    if (DIGIT.test(form)) continue;
    unsure.push({ form, parts: [word.value], at: start });
    chars.fill("#", start, end);
  }
  return { text: chars.join(""), unsure: unsure.sort((a, b) => a.at - b.at).map(({ form, parts }) => ({ form, parts })) };
}

/** Every text of the finding a document prints, in a fixed order, for `withTexts` to put back. A text the finding leaves out is "". */
function textsOf(f: Finding): string[] {
  const value = f.value.kind === "not_sized" ? [f.value.reason] : [...f.value.assumptions, f.value.period];
  return [f.title, f.summary, f.recommendation, ...value, f.effort.note ?? "", f.owner, ...f.dependencies, f.notMeasurableReason ?? "", f.segmentExemptReason ?? "", ...f.guards.map((g) => g.detail)];
}

function withTexts(f: Finding, texts: string[]): Finding {
  let at = 0;
  const next = (): string => texts[at++]!;
  // A text the finding left out stays left out: "" holds no figure, so it was never rewritten.
  const kept = <T extends string | null | undefined>(was: T): T => {
    const text = next();
    return (was === null || was === undefined ? was : text) as T;
  };
  const [title, summary, recommendation] = [next(), next(), next()];
  const value: Finding["value"] = f.value.kind === "not_sized" ? { ...f.value, reason: next() } : { ...f.value, assumptions: f.value.assumptions.map(() => next()), period: next() };
  const note = kept(f.effort.note);
  const owner = next();
  const dependencies = f.dependencies.map(() => next());
  const notMeasurableReason = kept(f.notMeasurableReason);
  const segmentExemptReason = kept(f.segmentExemptReason);
  const guards = f.guards.map((g) => ({ ...g, detail: next() }));
  return { ...f, title, summary, recommendation, value, effort: note === undefined ? f.effort : { ...f.effort, note }, owner, dependencies, notMeasurableReason, segmentExemptReason, guards, facts: { ...f.facts } };
}

type Figure = { field: string; asRead: number };

/** The figures of a finding an override can name: each numeric fact, then the sized amount. */
function figuresOf(f: Finding): Figure[] {
  const facts = Object.entries(f.facts).map(([key, value]) => ({ field: `facts.${key}`, asRead: value }));
  return f.value.kind === "not_sized" ? facts : [...facts, { field: "value.amount", asRead: f.value.amount }];
}

/** A figure's name in a refusal: the fact's key, or "the sized amount". */
const nameOf = (field: string): string => (field === "value.amount" ? "the sized amount" : field.slice("facts.".length));

/** Within 5% of the value's size, or of it times or divided by 100, 1,000 or 1,000,000: printed rounded, as a percentage, or in thousands or millions. Nothing but 0 is near 0. */
function near(found: number, value: number): boolean {
  const size = Math.abs(value);
  return [1, 100, 1_000, 1_000_000].some((scale) => [size * scale, size / scale].some((target) => Math.abs(found - target) <= 0.05 * target));
}

/** Where a figure was found: which text, and the span. Under 1,000 both forms read the same, and the new value is written grouped, as the rules print it. */
type Place = { text: number; index: number; length: number; grouped: boolean };
type Verdict = { kind: "unchanged" } | { kind: "carried"; place: Place | null } | { kind: "refused"; why: OverrideRefusal["why"] };

/** A part of a colon form as a number: "09" is 9, "1,000" is 1000. */
const partValue = (part: string): number => Number(part.replace(/,/g, ""));

/** One figure, against the finding as read (DECISIONS.md 511, items 1 to 3, 515 and 516). `read` is each text masked. */
function judge(f: Finding, read: Read[], figure: Figure, rows: NumberOverride[]): Verdict {
  if (rows.at(-1)!.newValue === figure.asRead) return { kind: "unchanged" };
  if (rows[0]!.previousValue !== figure.asRead) return { kind: "refused", why: { kind: "stale", from: rows[0]!.previousValue } };
  const asPlain = plain(figure.asRead);
  const places: Place[] = read.flatMap(({ text }, at) =>
    [...text.matchAll(valuePattern(figure.asRead))].map((m) => ({ text: at, index: m.index, length: m[0].length, grouped: m[0] !== asPlain || asPlain === grouped(figure.asRead) })),
  );
  // Each part of an unsure form equal to the value as read is a place too, one that can't be rewritten (DECISIONS.md 516).
  const unsure = read.flatMap(({ unsure }) => unsure);
  const fixed = unsure.flatMap(({ form, parts }) => parts.filter((part) => part === figure.asRead).map(() => form));
  if (places.length + fixed.length > 0) {
    const shared = figuresOf(f).find((other) => other.field !== figure.field && other.asRead === figure.asRead);
    if (shared) return { kind: "refused", why: { kind: "shared", other: nameOf(shared.field) } };
    if (places.length + fixed.length > 1) return { kind: "refused", why: { kind: "repeated", places: places.length + fixed.length } };
    if (fixed.length > 0) return { kind: "refused", why: { kind: "unsure", found: fixed[0]! } };
    return { kind: "carried", place: places[0]! };
  }
  const close = roundedIn(read, figure.asRead);
  return close === null ? { kind: "carried", place: null } : { kind: "refused", why: { kind: "rounded", found: close } };
}

/**
 * Not printed itself: what in the texts may be the value printed rounded, by the
 * 5% test, as written. An unsure form with a part near it can't be rewritten
 * either (DECISIONS.md 515, 516).
 */
function roundedIn(read: Read[], value: number): string | null {
  const close = read.flatMap(({ unsure }) => unsure).find(({ parts }) => parts.some((part) => near(part, value)));
  if (close) return close.form;
  for (const { text } of read) {
    for (const m of text.matchAll(NUMBER)) {
      if (near(Number(`${m[1]!.replace(/,/g, "")}${m[2] ?? ""}`), value)) return m[0];
    }
  }
  return null;
}

/** The finding as reports show it, or the refusals that keep it as read (DECISIONS.md 511, item 4). */
function overrideFinding(f: Finding, overrides: NumberOverride[]): { finding: Finding; refused: OverrideRefusal[] } {
  const texts = textsOf(f);
  const read = texts.map(masked);
  const judged = figuresOf(f).flatMap((figure) => {
    const rows = overrides.filter((o) => o.target.field === figure.field);
    return rows.length ? [{ figure, last: rows.at(-1)!.newValue, verdict: judge(f, read, figure, rows) }] : [];
  });
  const refused = judged.flatMap(({ figure, last, verdict }) => (verdict.kind === "refused" ? [{ findingId: f.id, title: f.title, field: figure.field, asRead: figure.asRead, last, why: verdict.why }] : []));
  if (refused.length > 0) return { finding: f, refused };
  const carried = judged.flatMap(({ figure, last, verdict }) => (verdict.kind === "carried" ? [{ figure, last, place: verdict.place }] : []));
  // One pass over the texts as read, the last place first so no place moves: a new value is never matched again.
  const rewritten = texts.slice();
  const places = carried.flatMap(({ last, place }) => (place ? [{ last, place }] : [])).sort((a, b) => b.place.text - a.place.text || b.place.index - a.place.index);
  for (const { last, place } of places) {
    const text = rewritten[place.text]!;
    rewritten[place.text] = `${text.slice(0, place.index)}${place.grouped ? grouped(last) : plain(last)}${text.slice(place.index + place.length)}`;
  }
  const finding = withTexts(f, rewritten);
  for (const { figure, last } of carried) {
    if (figure.field === "value.amount") finding.value = finding.value.kind === "not_sized" ? finding.value : { ...finding.value, amount: last };
    else finding.facts[nameOf(figure.field)] = last;
  }
  // A finding's hours to fix and fix prompt were made from its figures and words as
  // read (DECISIONS.md 628). Nothing here can make them again, so a correction that
  // changes a figure drops both rather than leave them stale; one put back to the
  // value as read changes nothing and keeps them.
  if (carried.some(({ figure, last }) => last !== figure.asRead)) {
    delete finding.effortHours;
    delete finding.fixPrompt;
  }
  return { finding, refused: [] };
}

type Measured = Extract<Metric, { status: "measured" }>;

/** The figures of a measured metric an override can name, in the order a refusal names another: its value, n, then each stat. */
function metricFigures(m: Measured): Figure[] {
  return [{ field: "value", asRead: m.value }, { field: "n", asRead: m.n }, ...Object.entries(m.stats).map(([key, value]) => ({ field: `stats.${key}`, asRead: value }))];
}

/** A measure's figure's name in a refusal: "value", "n" or the stat's key. */
const metricNameOf = (field: string): string => (field.startsWith("stats.") ? field.slice("stats.".length) : field);

/**
 * Every text of the metric a document prints. None is rewritten: on the
 * synthetic run each is a constant, "Fewer than 5 people" beside a value of 5.
 */
function metricTexts(m: Measured): string[] {
  return [m.label, m.definition, ...m.assumptions, ...m.exclusions, m.segmentExemptReason ?? "", ...m.segments.map((s) => s.notMeasurableReason ?? ""), ...m.series.map((p) => p.notMeasurableReason ?? "")];
}

/**
 * Which other figure of the metric holds the value, the first in the order a
 * refusal names it. A segment's key is never named: it can be a person's token
 * or the customer's own stage name. The evidence counts are printed nowhere,
 * but are in the number check's data under the metric's name.
 */
function partHolding(m: Measured, figure: Figure): string | null {
  const value = figure.asRead;
  const other = metricFigures(m).find((x) => x.field !== figure.field && x.asRead === value);
  if (other) return other.field.startsWith("stats.") ? `the stat ${metricNameOf(other.field)}` : other.field;
  if (m.segments.some((s) => s.value === value || s.n === value)) return "a segment";
  if (m.series.some((p) => p.value === value || p.n === value)) return "a point in its series";
  if (m.evidence.some((e) => e.count === value)) return "an evidence count";
  return null;
}

/**
 * One figure of a measure, against the measure as read (DECISIONS.md 517). No
 * 5% test is put to its parts: a part near the value is another slice.
 */
function judgeMetric(m: Measured, read: Read[], figure: Figure, rows: NumberOverride[]): Verdict {
  if (rows.at(-1)!.newValue === figure.asRead) return { kind: "unchanged" };
  if (rows[0]!.previousValue !== figure.asRead) return { kind: "refused", why: { kind: "stale", from: rows[0]!.previousValue } };
  const where = partHolding(m, figure);
  if (where !== null) return { kind: "refused", why: { kind: "part", where } };
  const printed = read.some(({ text, unsure }) => valuePattern(figure.asRead).test(text) || unsure.some(({ parts }) => parts.includes(figure.asRead)));
  if (printed) return { kind: "refused", why: { kind: "worded" } };
  const close = roundedIn(read, figure.asRead);
  return close === null ? { kind: "carried", place: null } : { kind: "refused", why: { kind: "rounded", found: close } };
}

/** The metric as reports show it, or the refusals that keep it as read: one figure refused keeps the whole metric as read. */
function overrideMetric(m: Measured, overrides: NumberOverride[]): { metric: Metric; refused: OverrideRefusal[] } {
  const read = metricTexts(m).map(masked);
  const judged = metricFigures(m).flatMap((figure) => {
    const rows = overrides.filter((o) => o.target.field === figure.field);
    return rows.length ? [{ figure, last: rows.at(-1)!.newValue, verdict: judgeMetric(m, read, figure, rows) }] : [];
  });
  const refused = judged.flatMap(({ figure, last, verdict }) => (verdict.kind === "refused" ? [{ metricId: m.id, title: m.label, field: figure.field, asRead: figure.asRead, last, why: verdict.why }] : []));
  if (refused.length > 0) return { metric: m, refused };
  const metric: Measured = { ...m, stats: { ...m.stats } };
  for (const { figure, last, verdict } of judged) {
    if (verdict.kind !== "carried") continue;
    if (figure.field === "value") metric.value = last;
    else if (figure.field === "n") metric.n = last;
    else metric.stats[metricNameOf(figure.field)] = last;
  }
  return { metric, refused: [] };
}

/**
 * The findings and metrics as reports show them, and each figure that couldn't
 * be carried, findings' first, then metrics', each in the run's order. The
 * number check's data is built from these, with no rows: every row stays
 * history for the raw notes (DECISIONS.md 516). A row naming a finding, metric
 * or figure the run doesn't hold, or a metric that isn't measured, changes
 * nothing (DECISIONS.md 511, item 5, and 517).
 */
export function applyOverrides(findings: Finding[], metrics: Metric[], overrides: NumberOverride[]): { findings: Finding[]; metrics: Metric[]; refused: OverrideRefusal[] } {
  const refused: OverrideRefusal[] = [];
  const outF = findings.map((f) => {
    const own = overrides.filter((o) => o.target.type === "finding" && o.target.id === f.id);
    if (own.length === 0) return f;
    const result = overrideFinding(f, own);
    refused.push(...result.refused);
    return result.finding;
  });
  const outM = metrics.map((m) => {
    const own = overrides.filter((o) => o.target.type === "metric" && o.target.id === m.id);
    if (own.length === 0 || m.status !== "measured") return m;
    const result = overrideMetric(m, own);
    refused.push(...result.refused);
    return result.metric;
  });
  return { findings: outF, metrics: outM, refused };
}

// The reason a refusal holds the documents, in the shape the console lists
// (DECISIONS.md 512): the count, a line a refusal up to ten, then what to do.
// Whitespace is collapsed within each line, so nothing a title holds can start
// a line of its own. The console runs a copy of this file, and prints the same
// lines with its own close when it refuses a correction before it is saved or
// a release while one stands (DECISIONS.md 532, 533).

const REFUSALS_LISTED = 10;
const TITLE_MAX = 120;

const countOf = (n: number): string => `${n} corrected ${n === 1 ? "figure" : "figures"}`;

/** A title as a refusal prints it: a person's token as "a named person", whitespace collapsed, cut at 120 characters. */
function shownTitle(title: string): string {
  return Array.from(title.replace(TOKEN, "a named person").replace(/\s+/g, " ").trim()).slice(0, TITLE_MAX).join("");
}

function why(r: OverrideRefusal): string {
  const measure = r.metricId !== undefined;
  switch (r.why.kind) {
    case "part":
      // An evidence count is printed nowhere: its only reader is the number
      // check's data, which would pass the figure as read (DECISIONS.md 479).
      if (r.why.where === "an evidence count") return `the number of records this measure was read from is also ${grouped(r.asRead)}, and our check of printed figures would go on passing the figure as read`;
      return `another figure of this measure (${r.why.where}) is also ${grouped(r.asRead)}, and a chart, table or sentence prints it as read`;
    case "worded":
      return `the measure's own wording has ${grouped(r.asRead)}, which a correction doesn't rewrite`;
    case "shared":
      return `another figure of this finding (${r.why.other}) is also ${grouped(r.asRead)}, so we can't tell which one the wording prints`;
    case "repeated":
      return `the wording has ${grouped(r.asRead)} in ${r.why.places} places, and we can't tell which of them are this figure`;
    case "rounded":
      return `${measure ? "the measure's own wording" : "the wording"} doesn't print ${grouped(r.asRead)} itself and has ${r.why.found}, which may be this figure rounded`;
    case "unsure":
      return `the wording has ${grouped(r.asRead)} only as ${r.why.found}, which we can't be sure is this figure and can't rewrite`;
    case "stale":
      return `the correction was made from ${grouped(r.why.from)}, which this run no longer reads`;
  }
}

/**
 * The count, then a line a refusal up to ten, findings' first, then measures',
 * then how many more: everything but what to do, which depends on where it is
 * printed. For one refusal, the second line is that refusal's.
 */
export function refusalLines(refused: OverrideRefusal[]): string[] {
  const ordered = [...refused.filter((r) => r.metricId === undefined), ...refused.filter((r) => r.metricId !== undefined)];
  const head = `Held back: ${countOf(refused.length)} can't be carried into the documents.`;
  const listed = ordered.slice(0, REFUSALS_LISTED).map((r) => `"${shownTitle(r.title)}", ${r.metricId === undefined ? nameOf(r.field) : metricNameOf(r.field)}: ${grouped(r.asRead)} as read, corrected to ${grouped(r.last)}, but ${why(r)}.`);
  const more = refused.length > REFUSALS_LISTED ? [`And ${refused.length - REFUSALS_LISTED} more.`] : [];
  return [head, ...listed, ...more].map((line) => line.replace(/\s+/g, " ").trim());
}

// The worker makes documents only for a released run, where the console takes
// no correction, so the close can't tell the consultant to put one back: the
// console refuses it before it is saved, and a run released with one needs
// whoever looks after the health check (DECISIONS.md 532).
const CLOSE = "A finding's wording and a measure's chart or table can't be edited, so a figure we can't carry changes only with a new health check. The console refuses a correction like this before it is saved and holds a release while one stands. If this run is already released its corrections can no longer be changed there, so tell whoever looks after the health check.";

/** Why the documents were held back, for the console's Release tab and the command line. */
export function refusalReason(refused: OverrideRefusal[]): string {
  return [...refusalLines(refused), CLOSE].join("\n");
}

/** The same, for the worker's log and the job's outcome: the count only, since a title or a figure is the customer's. */
export function refusalSummary(refused: OverrideRefusal[]): string {
  return `${countOf(refused.length)} can't be carried. See the run's Release tab.`;
}
