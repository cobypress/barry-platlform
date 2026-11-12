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

/** A figure an override couldn't carry into its finding's wording, and why. */
export type OverrideRefusal = {
  findingId: string;
  /** The finding's title as read, tokens and all. Only `refusalReason` prints it, without them. */
  title: string;
  /** `facts.<key>` or `value.amount`. */
  field: string;
  asRead: number;
  last: number;
  why:
    | { kind: "shared"; other: string }
    | { kind: "repeated"; places: number }
    | { kind: "rounded"; found: string }
    | { kind: "stale"; from: number };
};

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
// It doesn't take a year and month, "2026-02", as one.
const YEAR_MONTH = /(?<![\w.,-])(?:19|20)\d{2}-(?:0[1-9]|1[0-2])(?!\d|[.,]\d|-\d)/g;
const notFigure = (f: ReadFigure): boolean => f.kind === "date" || f.kind === "identifier";

// Digits joined by a colon, a clock time, a ratio or "10:30:15", found by our
// own pattern whatever the reader calls them, since it calls "09:00" malformed
// (DECISIONS.md 515). Such a form is never a place a figure is found, and is
// never passed over for a figure not printed itself: a part equal or near it
// refuses. A part may be grouped or have decimals, so no digit of "1.5:1" or
// "1,000:1" is left over to be read as a figure.
const COLON = /(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?::(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)+/g;

/** A text as `judge` reads it: masked, and the colon forms taken out of it, as written. */
type Read = { text: string; colons: string[] };

/**
 * Where each span of the reader's text starts and ends in the text as given.
 * The reader works on `normalise(text)`, which can be longer or shorter ("½" is
 * " one half "), so the text is cut only where normalising each side alone gives
 * the reader's text: a piece between two cuts is masked whole if any of what it
 * became is masked. Characters that normalise together ("ㄱㅏ" is "가") are one piece.
 */
function spansInText(text: string, normalised: string, spans: Array<[number, number]>): Array<[number, number]> {
  if (normalised === text) return spans;
  const cuts: Array<{ at: number; to: number }> = [{ at: 0, to: 0 }];
  for (let at = 1; at < text.length; at++) {
    const before = normalise(text.slice(0, at));
    if (before + normalise(text.slice(at)) === normalised) cuts.push({ at, to: before.length });
  }
  cuts.push({ at: text.length, to: normalised.length });
  const pieces: Array<[number, number]> = [];
  for (let i = 0; i + 1 < cuts.length; i++) {
    const [from, to] = [cuts[i]!, cuts[i + 1]!];
    if (spans.some(([start, end]) => from.to < end && to.to > start)) pieces.push([from.at, to.at]);
  }
  return pieces;
}

/**
 * The text with every token, date, identifier and colon form masked at its own
 * length, so a place found in it is its place in the text. Colon forms are
 * looked for once the rest is masked: a time inside an ISO date and time is
 * the date's, and is neither found nor tested.
 */
function masked(text: string): Read {
  const chars = text.replace(TOKEN, (token) => "#".repeat(token.length)).split("");
  // No digit, nothing to find a figure in.
  if (!/\d/.test(text)) return { text: chars.join(""), colons: [] };
  const { normalised, figures } = extractFigures(text);
  const shapes = spansInText(text, normalised, figures.filter(notFigure).map((f): [number, number] => [f.start, f.end]));
  for (const m of text.matchAll(YEAR_MONTH)) shapes.push([m.index, m.index + m[0].length]);
  for (const [start, end] of shapes) chars.fill("#", start, end);
  const colons: string[] = [];
  for (const m of chars.join("").matchAll(COLON)) {
    colons.push(m[0]);
    chars.fill("#", m.index, m.index + m[0].length);
  }
  return { text: chars.join(""), colons };
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

/** One figure, against the finding as read (DECISIONS.md 511, items 1 to 3, and 515). `read` is each text masked. */
function judge(f: Finding, read: Read[], figure: Figure, rows: NumberOverride[]): Verdict {
  if (rows.at(-1)!.newValue === figure.asRead) return { kind: "unchanged" };
  if (rows[0]!.previousValue !== figure.asRead) return { kind: "refused", why: { kind: "stale", from: rows[0]!.previousValue } };
  const asPlain = plain(figure.asRead);
  const places: Place[] = read.flatMap(({ text }, at) =>
    [...text.matchAll(valuePattern(figure.asRead))].map((m) => ({ text: at, index: m.index, length: m[0].length, grouped: m[0] !== asPlain || asPlain === grouped(figure.asRead) })),
  );
  if (places.length > 0) {
    const shared = figuresOf(f).find((other) => other.field !== figure.field && other.asRead === figure.asRead);
    if (shared) return { kind: "refused", why: { kind: "shared", other: nameOf(shared.field) } };
    if (places.length > 1) return { kind: "refused", why: { kind: "repeated", places: places.length } };
    // A colon form beside the one place is passed over as any other number is: a
    // second figure worked out from the first stays as read (DECISIONS.md 511).
    return { kind: "carried", place: places[0]! };
  }
  // Not printed itself: a colon form with a part equal or near it may be this figure, and can't be rewritten (DECISIONS.md 515).
  const colon = read.flatMap(({ colons }) => colons).find((form) => form.split(":").some((part) => near(partValue(part), figure.asRead)));
  if (colon) return { kind: "refused", why: { kind: "rounded", found: colon } };
  for (const { text } of read) {
    for (const m of text.matchAll(NUMBER)) {
      if (near(Number(`${m[1]!.replace(/,/g, "")}${m[2] ?? ""}`), figure.asRead)) return { kind: "refused", why: { kind: "rounded", found: m[0] } };
    }
  }
  return { kind: "carried", place: null };
}

/** The finding as reports show it, or the refusals that keep it as read (DECISIONS.md 511, item 4), and a row a figure carried. */
function overrideFinding(f: Finding, overrides: NumberOverride[]): { finding: Finding; refused: OverrideRefusal[]; carried: NumberOverride[] } {
  const texts = textsOf(f);
  const read = texts.map(masked);
  const judged = figuresOf(f).flatMap((figure) => {
    const rows = overrides.filter((o) => o.target.field === figure.field);
    return rows.length ? [{ figure, row: rows.at(-1)!, last: rows.at(-1)!.newValue, verdict: judge(f, read, figure, rows) }] : [];
  });
  const refused = judged.flatMap(({ figure, last, verdict }) => (verdict.kind === "refused" ? [{ findingId: f.id, title: f.title, field: figure.field, asRead: figure.asRead, last, why: verdict.why }] : []));
  if (refused.length > 0) return { finding: f, refused, carried: [] };
  const carried = judged.flatMap(({ figure, row, last, verdict }) => (verdict.kind === "carried" ? [{ figure, row, last, place: verdict.place }] : []));
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
  // The number check is told what was carried, from the value as read, never the rows between (DECISIONS.md 514).
  return { finding, refused: [], carried: carried.map(({ figure, row, last }) => ({ ...row, previousValue: figure.asRead, newValue: last })) };
}

/** Whether a row names a figure the metric has: a measured metric's value, n, or one of its stats. */
const metricField = (m: Metric, field: string): boolean => m.status === "measured" && (field === "value" || field === "n" || /^stats\..+$/.test(field));

function overrideMetric(m: Metric, o: NumberOverride): Metric {
  if (!metricField(m, o.target.field) || m.status !== "measured") return m;
  if (o.target.field === "value") return { ...m, value: o.newValue };
  if (o.target.field === "n") return { ...m, n: o.newValue };
  return { ...m, stats: { ...m.stats, [o.target.field.slice("stats.".length)]: o.newValue } };
}

/**
 * A row a metric's field, for the number check: from the first row's old value
 * to the last row's new value, and none when they're equal (DECISIONS.md 514).
 * A metric the run doesn't hold, or doesn't measure, carries nothing.
 */
function metricRows(metrics: Metric[], overrides: NumberOverride[]): NumberOverride[] {
  const fields = new Map<string, NumberOverride[]>();
  for (const o of overrides) {
    const m = o.target.type === "metric" ? metrics.find((x) => x.id === o.target.id) : undefined;
    if (!m || !metricField(m, o.target.field)) continue;
    const key = JSON.stringify([o.target.id, o.target.field]);
    fields.set(key, [...(fields.get(key) ?? []), o]);
  }
  return [...fields.values()].flatMap((rows) => (rows[0]!.previousValue === rows.at(-1)!.newValue ? [] : [{ ...rows.at(-1)!, previousValue: rows[0]!.previousValue }]));
}

/**
 * The findings and metrics as reports show them, each figure that couldn't be
 * carried, and what was carried, a row a figure, for the number check's data:
 * the rows between are history for the raw notes (DECISIONS.md 514). A row
 * naming a finding, metric or figure the run doesn't hold changes nothing
 * (DECISIONS.md 511, item 5). A metric has no text of its own, so its overrides
 * apply in order, as before.
 */
export function applyOverrides(findings: Finding[], metrics: Metric[], overrides: NumberOverride[]): { findings: Finding[]; metrics: Metric[]; refused: OverrideRefusal[]; carried: NumberOverride[] } {
  const refused: OverrideRefusal[] = [];
  const carried: NumberOverride[] = [];
  const outF = findings.map((f) => {
    const own = overrides.filter((o) => o.target.type === "finding" && o.target.id === f.id);
    if (own.length === 0) return f;
    const result = overrideFinding(f, own);
    refused.push(...result.refused);
    carried.push(...result.carried);
    return result.finding;
  });
  let outM = metrics;
  for (const o of overrides) if (o.target.type === "metric") outM = outM.map((m) => (m.id === o.target.id ? overrideMetric(m, o) : m));
  return { findings: outF, metrics: outM, refused, carried: [...carried, ...metricRows(metrics, overrides)] };
}

// The reason a refusal holds the documents, in the shape the console lists
// (DECISIONS.md 512): the count, a line a refusal up to ten, then what to do.
// Whitespace is collapsed within each line, so nothing a title holds can start
// a line of its own.

const REFUSALS_LISTED = 10;
const TITLE_MAX = 120;

const countOf = (n: number): string => `${n} corrected ${n === 1 ? "figure" : "figures"}`;

/** A title as a refusal prints it: a person's token as "a named person", whitespace collapsed, cut at 120 characters. */
function shownTitle(title: string): string {
  return Array.from(title.replace(TOKEN, "a named person").replace(/\s+/g, " ").trim()).slice(0, TITLE_MAX).join("");
}

function why(r: OverrideRefusal): string {
  switch (r.why.kind) {
    case "shared":
      return `another figure of this finding (${r.why.other}) is also ${grouped(r.asRead)}, so we can't tell which one the wording prints`;
    case "repeated":
      return `the wording has ${grouped(r.asRead)} in ${r.why.places} places, and we can't tell which of them are this figure`;
    case "rounded":
      return `the wording doesn't print ${grouped(r.asRead)} itself and has ${r.why.found}, which may be this figure rounded`;
    case "stale":
      return `the correction was made from ${grouped(r.why.from)}, which this run no longer reads`;
  }
}

/** Why the documents were held back, for the console's Release tab and the command line. */
export function refusalReason(refused: OverrideRefusal[]): string {
  const head = refused.length === 1 ? "Held back: 1 corrected figure can't be carried into its finding's wording." : `Held back: ${countOf(refused.length)} can't be carried into their findings' wording.`;
  const listed = refused.slice(0, REFUSALS_LISTED).map((r) => `"${shownTitle(r.title)}", ${nameOf(r.field)}: ${grouped(r.asRead)} as read, corrected to ${grouped(r.last)}, but ${why(r)}.`);
  const more = refused.length > REFUSALS_LISTED ? [`And ${refused.length - REFUSALS_LISTED} more.`] : [];
  const close = "Override each one back to its figure as read, then make the documents again. A finding's own wording can't be edited, so a figure we can't carry changes only with a new health check.";
  return [head, ...listed, ...more, close].map((line) => line.replace(/\s+/g, " ").trim()).join("\n");
}

/** The same, for the worker's log and the job's outcome: the count only, since a title or a figure is the customer's. */
export function refusalSummary(refused: OverrideRefusal[]): string {
  return `${countOf(refused.length)} can't be carried. See the run's Release tab.`;
}
