import type { BuiltReport } from "../narrative/sections.ts";
import { INVESTMENT } from "../money/investment.ts";
import { HEADLINE_KINDS, KIND_LABELS, type ValueKind, type ValueModel } from "../value/model.ts";
import { compactMoney } from "./format.ts";
import { chartSlot, printed, textLines, wrappedLines, type Render } from "./html.ts";
import { HALF_WIDTH, waterfallChart } from "./charts.ts";
import { valueItemId } from "./references.ts";

// "What it's worth": the value model's total, by kind, and the groups behind it.
// Shared by both decks. Every figure is one the value model worked out, so the
// number check can trace each one.

export const MONEY_CSS = `
.money .headline-figure { font-size: 3.4em; font-weight: 700; line-height: 1; }
.money .range { color: var(--muted); margin: 6px 0 18px; }
.money .kinds { margin: 0 0 16px; }
.money .kind { display: grid; grid-template-columns: 38% 1fr 14%; gap: 12px; align-items: center; margin-bottom: 8px; }
.money .bar { height: 14px; background: var(--chartreuse); border-radius: 3px; min-width: 3px; }
.money .kind .amount { text-align: right; font-weight: 600; white-space: nowrap; }
.money .note { color: var(--muted); font-size: 0.85em; margin-top: 10px; }
.money .split { display: grid; grid-template-columns: 1fr 1.15fr; gap: 32px; align-items: start; }
.money .split .kind { grid-template-columns: 44% 1fr 20%; }
.money .waterfall { margin: 0 0 14px; }
.money .note.seats { color: var(--carbon); }
.money .basis { display: block; color: var(--muted); font-size: 0.78em; line-height: 1.3; margin-top: 1px; }
.money tr.total td { font-weight: 700; border-bottom: none; border-top: 1px solid var(--carbon); }
`;

/** Rows of groups the page has room for, the total row aside. */
const PAGE_GROUPS = 8;
/** The value model's basis for a figure measured over the analysis window. */
const OVER_WINDOW_BASIS = /divided by/;
/** What the figure before any share assumes, in words that fit the kind of money (DECISIONS.md 395). */
const IN_FULL: Record<Exclude<ValueKind, "exposure">, string> = {
  revenue_recoverable: "if all came back",
  conversion_uplift: "if the gap closed in full",
  licence_waste: "if every seat were released",
  time_cost: "if all those hours were saved",
};

// The page's height, estimated so the chart and the table fit the executive
// summary's page (DECISIONS.md 435). Measured in Chromium at A4 and 13.5px:
// 986.45px between the paddings, 170.41px from the eyebrow to the chart, the
// chart drawn 800 wide across 658px with 14px under it, the table's head 31.25px,
// and a note 10px above and 17.2px a line. Each is counted at or a little over
// what the browser sets, so the page fills its room before adding lines up
// (448). A note line is counted at 106 characters: the most that never counted
// a line too few on the prose was 110.
const MONEY_ROOM = 986.4;
const MONEY_HEAD = 170.5;
const CHART_SCALE = 658 / 800;
const TALL_CHART = 320;
const SHORT_CHART = 250;
const TABLE_HEAD = 31.3;
/** A kind's bar where the chart can't be drawn: its name on two lines and the gap under it. */
const BAR = 49;
const NOTE_GAP = 10;
const NOTE_LINE = 17.25;
const NOTE_CHARS = 106;

/**
 * How the money table is set, measured in Chromium. `text` is the table's width
 * less its three columns' padding: the label column has what the figures leave.
 * On the page the table is 657.6px wide, a row 13px and 18.75px a line at
 * 12.5px, and its basis 1px under the label and 12.672px a line at 9.75px,
 * counted as 12.68 (DECISIONS.md 448). On a slide a row is 11px and 23.19px a
 * line at 16px, counted as 23.2, with no basis: in the split
 * beside the figure the table is 611px wide, and on a continued slide the
 * slide's full 1174px. A label and its basis are set a word at a time by the
 * measured width of their letters (`textLines`, DECISIONS.md 438), so the
 * estimate counts the lines the browser sets, never fewer: characters a line had
 * to be set for the longest words, and counted the engine's labels and bases a
 * line long.
 */
type TableMeasure = { font: number; text: number; row: number; line: number; basis: { font: number; gap: number; line: number } | null };
const PAGE_TABLE: TableMeasure = { font: 12.5, text: 609.6, row: 13, line: 18.75, basis: { font: 9.75, gap: 1, line: 12.68 } };
const SLIDE_TABLE: TableMeasure = { font: 16, text: 563, row: 11, line: 23.2, basis: null };
const CONTINUED_TABLE: TableMeasure = { ...SLIDE_TABLE, text: 1126 };
// A slide's money starts 137.7px down, under the eyebrow and a one-line heading,
// and the slide's padding starts at 667.2px: 529.47px as Chromium sets them,
// counted as 529.4, as every row and the head are counted a little over
// (DECISIONS.md 448). The table's head is 33.69px, counted as 33.7. Beside the
// table the figure's column is 531.3px wide: the figure 52px, the range 4px
// under it at 29px a line in 20px type, and the chart 10px under that, drawn 560
// wide across the column with 14px under it.
// The notes are 14px type at 20.3px a line, each 10px under what's above it,
// across the slide's full width (DECISIONS.md 439).
const SLIDE_ROOM = 529.4;
const SLIDE_HEAD = 33.7;
const SLIDE_FIGURE = 66;
const SLIDE_RANGE_LINE = 29;
const SLIDE_RANGE_FONT = 20;
const SLIDE_FIGURE_WIDTH = 531.3;
const SLIDE_CHART_SCALE = SLIDE_FIGURE_WIDTH / HALF_WIDTH;
/** A kind's bar beside the table where the chart can't be drawn, its name on two lines. */
const SLIDE_BAR = 52;
const SLIDE_NOTE_GAP = 10;
const SLIDE_NOTE_LINE = 20.5;
const SLIDE_NOTE_FONT = 14;
const SLIDE_FULL_WIDTH = 1174;

// The figures' columns are as wide as their widest text, which never wraps, so
// wherever a label is longer than its line the label column is what they leave
// (DECISIONS.md 438). Each character as Chromium sets it in Poppins at 12.5px,
// rounded up, in regular for a line and in bold for the total; a capital or sign
// not named here as wide as W. Each heading is 600 at 12.5px.
const EXPECTED_HEAD = 58.2;
const RANGE_HEAD = 41;
const FIGURE_WIDTHS: Record<string, [regular: number, bold: number]> = {
  "0": [7.85, 8.15], "1": [4, 4.7], "2": [7.2, 7.15], "3": [7.4, 7.6], "4": [7.9, 8.5], "5": [7.85, 8.15], "6": [7.95, 8], "7": [6.85, 6.7], "8": [7.9, 8.1], "9": [7.9, 7.7],
  ".": [2.65, 3.55], ",": [2.5, 3.6], " ": [3.35, 2.65], "£": [7.75, 8.3], "€": [10.15, 9.95], $: [7.8, 8.25], "¥": [7.45, 8.4], k: [6.45, 7.75], m: [12.9, 13.25], t: [4.55, 5.1], o: [8, 8],
};
const FIGURE_AT = 12.5;
const figureWidth = (text: string, bold: boolean): number => [...text].reduce((sum, c) => sum + (FIGURE_WIDTHS[c] ?? [12.2, 13.2])[bold ? 1 : 0], 0);
/**
 * A reference tag beside a label, as wide as the widest of two figures and two
 * ("R66.66"), as it prints: 6px from the label in type 0.85 of the label's, with
 * its letter spacing. Chromium sets it 42.94px wide at 12.5px and 53.31px at 16px,
 * counted as 6px and 2.96 a pixel of the label's type (DECISIONS.md 448).
 */
const refTag = (font: number): number => 6 + 2.96 * font;

/** The lines past the first `from`, added up: what the money table's last row holds when it can't name them all. */
export function restOf(lines: ValueModel["groups"], from: number, k: "low" | "expected" | "high"): number {
  return Math.round(lines.slice(from).reduce((sum, g) => sum + g.counted[k], 0) * 100) / 100;
}

/**
 * How many lines a money table names, the rest added up in its last row
 * (DECISIONS.md 435 and 439): from the most it has room for, one fewer at a time
 * until the page or slide holds them, and never fewer than one. Where the cut
 * leaves one line, it didn't fit in the last row's place, so "And 1 smaller
 * line" is right (438): the row is shorter than the line.
 */
export function linesToName(count: number, most: number, holds: (named: number) => boolean): number {
  let named = count > most ? most - 1 : count;
  while (named > 1 && !holds(named)) named -= 1;
  return named;
}

type Group = ValueModel["groups"][number];
type Figures = { expected: number; low: number; high: number };

/** The last row's words, for the lines it adds up. */
const restLabel = (rest: number): string => `And ${rest} smaller ${rest === 1 ? "line" : "lines"}, in the raw notes.`;

/** What the money page and the slides share: every line, largest first, and how each is said. */
function moneyParts(r: Render, report: BuiltReport) {
  const { value } = report;
  const { total, byKind } = value;
  const m = (n: number) => compactMoney(n, value.currency);
  const kinds = HEADLINE_KINDS.filter((k) => byKind[k].expected > 0);
  const largest = Math.max(...kinds.map((k) => byKind[k].expected));
  const bars = kinds
    .map((k) => `<div class="kind"><span>${r.t(KIND_LABELS[k])}</span><div class="bar" style="width:${Math.max(1, Math.round((byKind[k].expected / largest) * 100))}%"></div><span class="amount">${m(byKind[k].expected)}</span></div>`)
    .join("");
  // Every group, so the rows add up to the headline (night 3 reading pass). Past what
  // a page or the slides hold, the rest are named as a count with where to find them.
  const all = value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
  const pct = (n: number) => `${Math.round(n * 100)}%`;
  // How each row was reached: its figure before any share, and the share counted. The page has room to say so; the slide points to it.
  const basis = (g: Group) => {
    const c = g.counted;
    const from = `${m(c.annual)} ${IN_FULL[g.kind as keyof typeof IN_FULL]}${g.lines.length > 1 ? ", the largest of overlapping issues" : ""}${OVER_WINDOW_BASIS.test(c.basis) ? ", a year's worth of what we measured" : ""}.`;
    return `${from} We count ${pct(g.realisation.expected)} (${pct(g.realisation.low)} to ${pct(g.realisation.high)}).`;
  };
  const range = (low: number, high: number) => `${m(low)} to ${m(high)}`;
  const exposure = value.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
  const exposureNote = exposure
    ? `<p class="note">${r.t(`Kept apart from this total: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's exposure, not money found, so it's never added.`)}${r.ref(valueItemId(exposure.id))}</p>`
    : "";
  const drawn = waterfallChart(value).kind === "chart";
  const ranged = r.t(`Over the next 12 months, expected. Between ${m(total.low)} and ${m(total.high)}.`);
  const figure = (chart: number | undefined, width?: number) => {
    const waterfall = waterfallChart(value, width, chart);
    // Each kind building to the total, exposure apart. The bars stay only where the waterfall can't be drawn.
    const build = waterfall.kind === "chart" ? `<div class="waterfall">${chartSlot(waterfall)}</div>` : `<div class="kinds">${bars}</div>`;
    return `<p class="headline-figure">${m(total.expected)}</p><p class="range">${ranged}</p>${build}`;
  };
  const rest = (from: number): Figures => ({ expected: restOf(all, from, "expected"), low: restOf(all, from, "low"), high: restOf(all, from, "high") });
  const row = (label: string, g: Figures, cls = "") => `<tr${cls ? ` class="${cls}"` : ""}><td>${label}</td><td class="num">${m(g.expected)}</td><td class="num">${range(g.low, g.high)}</td></tr>`;
  // With their amount, so the rows add up to the total beneath them (night 3 review), and the first's reference.
  const restRow = (from: number) => row(`${r.t(restLabel(all.length - from))}${r.ref(valueItemId(all[from]!.id))}`, rest(from));
  const totalRow = row(r.t("Total, each counted once"), total, "total");
  const table = (rows: string) => `<table class="grid"><thead><tr><th>Where it comes from</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}</tbody></table>`;
  return { value, total, m, kinds, all, basis, range, exposureNote, drawn, ranged, figure, rest, row, restRow, totalRow, table };
}
type Parts = ReturnType<typeof moneyParts>;

const EYEBROW = "What it's worth";
const TITLE = "What fixing this is worth";

/**
 * The label column's width where a table prints the lines from `from` to `to`
 * and, where `last`, the rest added up if any are left and the total: the
 * browser makes the figures' columns as wide as their widest text in the rows it
 * prints, and the label has the rest. Never wider than the browser's (DECISIONS.md 438).
 */
function labelColumn(parts: Parts, measure: TableMeasure, from: number, to: number, last: boolean): number {
  const { all, total } = parts;
  const rows = [...all.slice(from, to).map((g) => g.counted), ...(last && to < all.length ? [parts.rest(to)] : []), ...(last ? [total] : [])];
  const scale = measure.font / FIGURE_AT;
  const widest = (head: number, text: (x: Figures) => string) => scale * Math.max(head, ...rows.map((x) => figureWidth(text(x), x === total)));
  return measure.text - widest(EXPECTED_HEAD, (x) => parts.m(x.expected)) - widest(RANGE_HEAD, (x) => parts.range(x.low, x.high));
}

/**
 * A label's lines, with its reference, at the table's type: the tag follows the
 * last word with no space, so it goes to the next line with that word. Never
 * calls `r.ref`: on the executive summary that notes a reference as used.
 */
const labelLines = (label: string, column: number, measure: TableMeasure): number => textLines(printed(label), column, measure.font, refTag(measure.font));

/** A money line's label, with its reference, in lines `width` pixels wide on the page or a slide: what the estimate counts (DECISIONS.md 438). */
export const moneyLabelLines = (label: string, width: number, format: "page" | "slide"): number => labelLines(label, width, format === "page" ? PAGE_TABLE : SLIDE_TABLE);
/** A basis under a label on the page, in lines `width` pixels wide. */
export const moneyBasisLines = (basis: string, width: number): number => textLines(printed(basis), width, PAGE_TABLE.basis!.font);

/**
 * A table's rows: the lines from `from` to `to`, then, where `last`, the rest
 * added up if any are left and the total with the rule above it. The table's
 * borders collapse, so a row has half of the rule above it and half of the one
 * below, and the total, with none below, half a pixel less (DECISIONS.md 448).
 * Its label column is the one these rows print in.
 */
function rowsHeight(r: Render, parts: Parts, measure: TableMeasure, from: number, to: number, last: boolean): number {
  const { all } = parts;
  const left = last && to < all.length;
  const column = labelColumn(parts, measure, from, to, last);
  const line = (label: string) => measure.row + measure.line * labelLines(r.t(label), column, measure);
  const basis = (g: Group) => (measure.basis ? measure.basis.gap + measure.basis.line * textLines(printed(r.t(parts.basis(g))), column, measure.basis.font) : 0);
  return all.slice(from, to).reduce((sum, g) => sum + line(g.label) + basis(g), 0) + (left ? line(restLabel(all.length - to)) : 0) + (last ? line("Total, each counted once") - 0.5 : 0);
}

/** How the page sets its money: the chart's height and how many lines it names (DECISIONS.md 435 and 438). */
function pagePlan(r: Render, report: BuiltReport, parts: Parts, notes: string): { chart: number; named: number } {
  const { all } = parts;
  // Notes are counted by characters, wrapped a word at a time.
  const noted = [...notes.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].reduce((sum, [, text]) => sum + NOTE_GAP + NOTE_LINE * wrappedLines(printed(text!), NOTE_CHARS), 0);
  /** The page's height with the chart drawn `chart` high and the first `named` lines named, the rest added up in the last row. */
  const height = (chart: number, named: number) =>
    MONEY_HEAD + (parts.drawn ? chart * CHART_SCALE + 14 : parts.kinds.length * BAR + 16) + TABLE_HEAD + rowsHeight(r, parts, PAGE_TABLE, 0, named, true) + noted;
  // With the seats on this page, the chart is shorter to make room for them. Where the
  // page can't hold it all, the chart first takes its shorter height, then the table
  // names fewer lines and adds the rest up in its last row (DECISIONS.md 435).
  let chart = investmentHasPage(report) ? TALL_CHART : SHORT_CHART;
  const most = all.length > PAGE_GROUPS ? PAGE_GROUPS - 1 : all.length;
  if (height(chart, most) > MONEY_ROOM) chart = SHORT_CHART;
  return { chart, named: linesToName(all.length, PAGE_GROUPS, (named) => height(chart, named) <= MONEY_ROOM) };
}

const PAGE_HOW = "Not every case will come back, so we count a share of each figure: the expected share, with a cautious one and all of it either side. Where several issues describe the same money, only the largest counts.";
const pageNotes = (r: Render, parts: Parts) => `${parts.exposureNote}<p class="note">${r.t(PAGE_HOW)}</p>`;
/** A line's row on the page: its label, its reference and how it was reached beneath. */
const pageRow = (r: Render, parts: Parts, g: Group) => parts.row(`${r.t(g.label)}${r.ref(valueItemId(g.id))}<span class="basis">${r.t(parts.basis(g))}</span>`, g.counted);

/** The executive summary's page: the figure, the chart, and the table as the page holds it (DECISIONS.md 435 and 438). Empty when nothing could be given a figure in money. */
export function moneyPage(r: Render, report: BuiltReport): string {
  if (report.value.total.expected <= 0) return "";
  const parts = moneyParts(r, report);
  const { all, restRow, totalRow, table } = parts;
  const notes = pageNotes(r, parts);
  const seats = seatsOnMoneyPage(r, report);
  const { chart, named } = pagePlan(r, report, parts, `${notes}${seats}`);
  const rows = [...all.slice(0, named).map((g) => pageRow(r, parts, g)), ...(named < all.length ? [restRow(named)] : []), totalRow].join("");
  const head = `<div class="eyebrow">${r.t(EYEBROW)}</div><h2>${TITLE}</h2>`;
  return `<div class="money">${head}${parts.figure(chart)}${table(rows)}${notes}${seats}</div>`;
}

/** The line at `index`, largest first, in its row as the page prints it: for the test that shows by measure whether a line the last row stands for would fit (DECISIONS.md 448). */
export function moneyPageRow(r: Render, report: BuiltReport, index: number): string {
  const parts = moneyParts(r, report);
  return pageRow(r, parts, parts.all[index]!);
}

/** The label column the page's estimate sets its lines in, at the cut it prints: for the test that holds it to the browser's (DECISIONS.md 438). */
export function moneyPageColumn(r: Render, report: BuiltReport): number {
  const parts = moneyParts(r, report);
  const { named } = pagePlan(r, report, parts, `${pageNotes(r, parts)}${seatsOnMoneyPage(r, report)}`);
  return labelColumn(parts, PAGE_TABLE, 0, named, true);
}

/**
 * How the full report sets its money (DECISIONS.md 439): `chart` beside the
 * table, `first` lines on the first slide, the rest added up in its last row if
 * any are left; and where there's a continued slide, `continued` lines on it,
 * the rest added up in its last row instead.
 */
type SlidePlan = { chart: number; first: number; continued: number | null };

function slidePlan(r: Render, parts: Parts, notes: string, most: 1 | 2): SlidePlan {
  const { all } = parts;
  const n = all.length;
  /** The notes across the slide's full width. */
  const noted = [...notes.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].reduce((sum, [, text]) => sum + SLIDE_NOTE_GAP + SLIDE_NOTE_LINE * textLines(printed(text!), SLIDE_FULL_WIDTH, SLIDE_NOTE_FONT), 0);
  /** The figure's column: the figure, its range and the chart `chart` high, or the bars. */
  const figure = (chart: number) => SLIDE_FIGURE + SLIDE_RANGE_LINE * textLines(printed(parts.ranged), SLIDE_FIGURE_WIDTH, SLIDE_RANGE_FONT) + (parts.drawn ? chart * SLIDE_CHART_SCALE + 14 : parts.kinds.length * SLIDE_BAR + 16);
  // One slide: the figure beside the table, and the notes beneath both, under the taller.
  const one = (chart: number, named: number) => Math.max(figure(chart), SLIDE_HEAD + rowsHeight(r, parts, SLIDE_TABLE, 0, named, true)) + noted <= SLIDE_ROOM;
  for (const chart of [TALL_CHART, SHORT_CHART]) if (one(chart, n)) return { chart, first: n, continued: null };
  // Where the deck can't take a second slide, one adds the rest up, as the page does.
  if (most === 1) return { chart: SHORT_CHART, first: linesToName(n, n, (named) => one(SHORT_CHART, named)), continued: null };
  // Two lines can't be split without leaving one alone on the continued slide.
  if (n <= 2) return { chart: SHORT_CHART, first: n, continued: null };
  // Two: the first as many lines as fit beside the figure, the continued slide the rest
  // across the full width with the total and the notes, and never only one line.
  let fit = n - 1;
  while (fit > 1 && SLIDE_HEAD + rowsHeight(r, parts, SLIDE_TABLE, 0, fit, false) > SLIDE_ROOM) fit -= 1;
  const first = Math.max(1, Math.min(fit, n - 2));
  const continued = (named: number) => SLIDE_HEAD + rowsHeight(r, parts, CONTINUED_TABLE, first, first + named, true) + noted <= SLIDE_ROOM;
  // The deck has a ceiling, so where two can't hold every line the last row adds the rest up.
  return { chart: TALL_CHART, first, continued: linesToName(n - first, n - first, continued) };
}

/**
 * The full report's money (DECISIONS.md 439): one slide where it holds, the
 * figure and the chart beside the table and the notes beneath, the chart at its
 * shorter height if that's what it takes. Otherwise it carries on to one
 * following slide, its heading repeated and continued, with the table across the
 * full width and the total and the notes on it. The deck has a ceiling, so the
 * money never takes more than two slides, or more than `most`: where they can't
 * hold every line, the last row adds the rest up, as the executive summary's
 * does. Empty when nothing could be given a figure in money.
 */
export function moneySlides(r: Render, report: BuiltReport, most: 1 | 2 = 2): string[] {
  if (report.value.total.expected <= 0) return [];
  const parts = moneyParts(r, report);
  const { all, row, restRow, totalRow, table, exposureNote } = parts;
  const how = "Not every case will come back, so we count a share of each figure, with a cautious share and all of it either side. Where several issues describe the same money, only the largest counts. The executive summary shows how each line was reached.";
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  const { chart, first, continued } = slidePlan(r, parts, notes, most);
  const head = (more: boolean) => `<div class="eyebrow">${r.t(EYEBROW)}</div><h2>${TITLE}${more ? ", continued" : ""}</h2>`;
  const lines = (from: number, to: number) => all.slice(from, to).map((g) => row(`${r.t(g.label)}${r.ref(valueItemId(g.id))}`, g.counted)).join("");
  /** The lines from `from` to `to`, the rest added up if any are left, and the total. */
  const last = (from: number, to: number) => `${lines(from, to)}${to < all.length ? restRow(to) : ""}${totalRow}`;
  const split = (rows: string) => `<div class="split"><div>${parts.figure(chart, HALF_WIDTH)}</div><div>${table(rows)}</div></div>`;
  if (continued === null) return [`<div class="money">${head(false)}${split(last(0, first))}${notes}</div>`];
  return [`<div class="money">${head(false)}${split(lines(0, first))}</div>`, `<div class="money">${head(true)}${table(last(first, first + continued))}${notes}</div>`];
}

/** What fixing it is worth against what they pay a year, to one decimal place. Null without both. */
export function returnOnSpend(report: BuiltReport): number | null {
  const cost = report.investment.annualCost;
  if (cost.status !== "measured" || cost.value <= 0 || report.value.total.expected <= 0) return null;
  return Math.round((report.value.total.expected / cost.value) * 10) / 10;
}

/** The seats, as tiles and a line: what's paid for, held and unassigned, and what the unassigned cost. Null when no seat could be read. */
export function seatsSummary(r: Render, report: BuiltReport): { stats: string; line: string | null } | null {
  const { investment, metrics } = report;
  const measured = (id: string) => {
    const m = metrics.find((x) => x.id === id);
    return m?.status === "measured" ? m : null;
  };
  const paid = measured(INVESTMENT.seatsPaid);
  if (!paid) return null;
  const used = measured(INVESTMENT.seatsUsed);
  const unused = measured(INVESTMENT.seatsUnused);
  const active = measured(INVESTMENT.activeUserShare);
  const unusedCost = measured(INVESTMENT.unusedSeatCost);
  const m = (n: number) => compactMoney(n, investment.currency);
  // The value model's line for the same seats, so the two can be read together.
  const seatShare = report.value.groups.find((g) => g.id === "unassigned-seats")?.counted ?? null;
  const stat = (figure: string, label: string, id: string) => `<div class="stat"><b>${figure}</b><span>${r.t(label)}</span>${r.ref(id)}</div>`;
  const stats = [
    stat(String(paid.value), "Seats you pay for, all licence types", INVESTMENT.seatsPaid),
    used ? stat(String(used.value), "Seats in use", INVESTMENT.seatsUsed) : "",
    unused ? stat(String(unused.value), "Seats nobody is assigned to", INVESTMENT.seatsUnused) : "",
    active ? stat(`${active.value}%`, "Of users active", INVESTMENT.activeUserShare) : "",
  ].join("");
  const line = unusedCost ? `${unused ? `The ${unused.value} seats nobody is assigned to` : "Seats nobody is assigned to"} cost ${m(unusedCost.value)} a year.${seatShare ? ` We count a share of it, ${m(seatShare.expected)}, in what fixing this is worth.` : ""}` : null;
  return { stats, line };
}

/** Whether the investment has a page of its own: only with a figure for what they pay. Without one, its seats go on the money page (DECISIONS.md 401). */
export const investmentHasPage = (report: BuiltReport): boolean => report.investment.annualCost.status === "measured" || report.value.total.expected <= 0;

/** The seats on the money page, when there's no figure for what they pay: a paragraph, as the page has no room for tiles. */
export function seatsOnMoneyPage(r: Render, report: BuiltReport): string {
  if (investmentHasPage(report)) return "";
  const metric = (id: string) => {
    const m = report.metrics.find((x) => x.id === id);
    return m?.status === "measured" ? m : null;
  };
  const paid = metric(INVESTMENT.seatsPaid);
  if (!paid) return "";
  const used = metric(INVESTMENT.seatsUsed);
  const unused = metric(INVESTMENT.seatsUnused);
  const active = metric(INVESTMENT.activeUserShare);
  // The unassigned seats' cost is already a line of the table above, so it isn't said again.
  const parts = [
    `${r.t(`${paid.value} paid for, all licence types`)}${r.ref(INVESTMENT.seatsPaid)}`,
    used ? `${r.t(`${used.value} in use`)}${r.ref(INVESTMENT.seatsUsed)}` : "",
    unused ? `${r.t(`${unused.value} nobody is assigned to`)}${r.ref(INVESTMENT.seatsUnused)}` : "",
    active ? `${r.t(`${active.value}% of users active`)}${r.ref(INVESTMENT.activeUserShare)}` : "",
  ].filter(Boolean);
  return `<p class="note seats"><strong>${r.t("Your seats.")}</strong> ${parts.join("; ")}. ${r.t("We don't have a figure for your whole Salesforce bill.")}</p>`;
}

export function investmentPage(r: Render, report: BuiltReport): string {
  if (!investmentHasPage(report)) return "";
  const seats = seatsSummary(r, report);
  if (!seats) return "";
  const { investment } = report;
  const m = (n: number) => compactMoney(n, investment.currency);
  const cost = investment.annualCost;
  const headline =
    cost.status === "measured"
      ? `<p class="headline-figure">${m(cost.value)}</p><p class="range">${r.t(`A year. ${cost.label}.`)}</p>`
      : // Why it isn't priced is for the raw notes. Here it's enough to say what the page shows instead.
        `<p class="range">${r.t("We don't have a figure for your whole Salesforce bill, so this page shows your seats rather than what they all cost.")}</p>`;
  const ratio = returnOnSpend(report);
  const lines = [seats.line ?? "", ratio !== null ? `What fixing the issues is worth, ${m(report.value.total.expected)} over 12 months, is ${ratio} times what you pay for Salesforce a year.` : ""].filter(Boolean);
  return `<div class="money"><div class="eyebrow">Your investment</div><h2>What you pay, and what you use</h2>${headline}<div class="stat-row" style="margin:6px 0 16px">${seats.stats}</div>${lines.map((l) => `<p style="margin-bottom:8px">${r.t(l)}</p>`).join("")}</div>`;
}
