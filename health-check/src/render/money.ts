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
// 987px between the paddings, 171px from the eyebrow to the chart, the chart
// drawn 800 wide across 658px with 14px under it, the table's head 31.5px, and a
// note 10px above and 17.2px a line. A note line is counted at 106 characters:
// the most that never counted a line too few on the prose was 110.
const MONEY_ROOM = 986;
const MONEY_HEAD = 171;
const CHART_SCALE = 658 / 800;
const TALL_CHART = 320;
const SHORT_CHART = 250;
const TABLE_HEAD = 32;
/** A kind's bar where the chart can't be drawn: its name on two lines and the gap under it. */
const BAR = 49;
const NOTE_GAP = 10;
const NOTE_LINE = 17.25;
const NOTE_CHARS = 106;

/**
 * How the money table is set, measured in Chromium. `text` is the table's width
 * less its three columns' padding: the label column has what the figures leave.
 * On the page a row is 13px and 18.75px a line at 12.5px, and its basis 12.7px a
 * line at 9.75px after 1px, counted as 13.7. On a slide a row is 11px and 23.2px
 * a line at 16px, with no basis: in the split beside the figure the table is
 * 611px wide, and on a continued slide the slide's full 1174px. A label and its
 * basis are set a word at a time by the measured width of their letters
 * (`textLines`, DECISIONS.md 438), so the estimate counts the lines the browser
 * sets, never fewer: characters a line had to be set for the longest words, and
 * counted the engine's labels and bases a line long.
 */
type TableMeasure = { font: number; text: number; row: number; line: number; basis: { font: number; line: number } | null };
const PAGE_TABLE: TableMeasure = { font: 12.5, text: 610, row: 13, line: 18.75, basis: { font: 9.75, line: 13.7 } };
const SLIDE_TABLE: TableMeasure = { font: 16, text: 563, row: 11, line: 23.25, basis: null };
const CONTINUED_TABLE: TableMeasure = { ...SLIDE_TABLE, text: 1126 };
// A slide's table starts 138px down, under the eyebrow and a one-line heading, and
// the slide's padding starts at 667px: 529px, with 525 keeping clear. Its head is
// 33.7px, and a note 10px above and 20.3px a line at 14px, as wide as the table.
const SLIDE_ROOM = 525;
const SLIDE_HEAD = 34;
const SLIDE_NOTE_GAP = 10;
const SLIDE_NOTE_LINE = 20.5;
const SLIDE_NOTE_FONT = 14;
const SLIDE_SPLIT_WIDTH = 611;
const SLIDE_FULL_WIDTH = 1174;

/** The figures' headings in bold at 12.5px, at their widest: a column is never narrower. */
const EXPECTED_HEAD = 59;
const RANGE_HEAD = 41;
/** A figure's characters in bold at 12.5px, rounded up: a capital not named is 10px, any other 8.5px. */
const FIGURE_WIDTHS: Record<string, number> = { "1": 4.8, ".": 3.6, " ": 3.4, t: 5.1, m: 13.3, "€": 10.2, M: 11.5, W: 13.2 };
const FIGURE_AT = 12.5;
const figureWidth = (text: string): number => [...text].reduce((sum, c) => sum + (FIGURE_WIDTHS[c] ?? (/[A-Z]/.test(c) ? 10 : 8.5)), 0);
/** A reference tag beside a label, counted as a word, as wide as the widest. */
const REF_WORD = "R00.00";

/** The lines past the first `from`, added up: what the money table's last row holds when it can't name them all. */
export function restOf(lines: ValueModel["groups"], from: number, k: "low" | "expected" | "high"): number {
  return Math.round(lines.slice(from).reduce((sum, g) => sum + g.counted[k], 0) * 100) / 100;
}

/**
 * How many lines the page's table names, the rest added up in its last row
 * (DECISIONS.md 435): from the most it has room for, one fewer at a time until
 * the page holds them. The last row never stands for one line whose label takes
 * no more room than the row's own words (438): that row is as tall as the line,
 * less its basis, so the line is named where the page holds it, and where it
 * doesn't, the row stands for two. Where the line's label would take more room,
 * the row still stands for it alone.
 */
export function linesToName(count: number, most: number, holds: (named: number) => boolean, fitsRestRow: (line: number) => boolean): number {
  let named = count > most ? most - 1 : count;
  while (named > 1 && !holds(named)) named -= 1;
  if (count - named === 1 && fitsRestRow(named)) return holds(count) ? count : Math.max(1, named - 1);
  return named;
}

type Group = ValueModel["groups"][number];

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
  // the page holds, the rest are named as a count with where to find them; the slides name them all.
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
  const figure = (chart: number | undefined, width?: number) => {
    const waterfall = waterfallChart(value, width, chart);
    // Each kind building to the total, exposure apart. The bars stay only where the waterfall can't be drawn.
    const build = waterfall.kind === "chart" ? `<div class="waterfall">${chartSlot(waterfall)}</div>` : `<div class="kinds">${bars}</div>`;
    return `<p class="headline-figure">${m(total.expected)}</p><p class="range">${r.t(`Over the next 12 months, expected. Between ${m(total.low)} and ${m(total.high)}.`)}</p>${build}`;
  };
  const row = (label: string, g: { expected: number; low: number; high: number }, cls = "") => `<tr${cls ? ` class="${cls}"` : ""}><td>${label}</td><td class="num">${m(g.expected)}</td><td class="num">${range(g.low, g.high)}</td></tr>`;
  const totalRow = row(r.t("Total, each counted once"), total, "total");
  const table = (rows: string) => `<table class="grid"><thead><tr><th>Where it comes from</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}</tbody></table>`;
  return { value, total, m, kinds, all, basis, range, exposureNote, figure, row, totalRow, table };
}
type Parts = ReturnType<typeof moneyParts>;

const EYEBROW = "What it's worth";
const TITLE = "What fixing this is worth";

/**
 * The label column's width, from the widest figure in any row: the table makes
 * the figures' columns as wide as their widest text, and the label has the rest.
 */
function labelColumn(parts: Parts, measure: TableMeasure, extra: Array<{ expected: number; low: number; high: number }> = []): number {
  const lines = [...parts.all.map((g) => g.counted), parts.total, ...extra];
  const scale = measure.font / FIGURE_AT;
  const widest = (head: number, texts: string[]) => scale * Math.max(head, ...texts.map(figureWidth));
  return measure.text - widest(EXPECTED_HEAD, lines.map((x) => parts.m(x.expected))) - widest(RANGE_HEAD, lines.map((x) => parts.range(x.low, x.high)));
}

/** A label's lines, with its reference, at the table's type. Never calls `r.ref`: on the executive summary that notes a reference as used. */
const labelLines = (label: string, column: number, measure: TableMeasure): number => textLines(printed(`${label} ${REF_WORD}`), column, measure.font);

/** A money line's label, with its reference, in lines `width` pixels wide on the page or a slide: what the estimate counts (DECISIONS.md 438). */
export const moneyLabelLines = (label: string, width: number, format: "page" | "slide"): number => labelLines(label, width, format === "page" ? PAGE_TABLE : SLIDE_TABLE);
/** A basis under a label on the page, in lines `width` pixels wide. */
export const moneyBasisLines = (basis: string, width: number): number => textLines(printed(basis), width, PAGE_TABLE.basis!.font);

/** A line's row: its label and, on the page, its basis. */
function rowHeight(r: Render, parts: Parts, g: Group, column: number, measure: TableMeasure): number {
  const basis = measure.basis ? measure.basis.line * textLines(printed(r.t(parts.basis(g))), column, measure.basis.font) : 0;
  return measure.row + measure.line * labelLines(r.t(g.label), column, measure) + basis;
}

/** The executive summary's page: the figure, the chart, and the table as the page holds it (DECISIONS.md 435 and 438). Empty when nothing could be given a figure in money. */
export function moneyPage(r: Render, report: BuiltReport): string {
  if (report.value.total.expected <= 0) return "";
  const parts = moneyParts(r, report);
  const { all, total, m, basis, exposureNote, row, totalRow, table } = parts;
  const restLabel = (rest: number) => `And ${rest} smaller ${rest === 1 ? "line" : "lines"}, in the raw notes.`;
  const how = "Not every case will come back, so we count a share of each figure: the expected share, with a cautious one and all of it either side. Where several issues describe the same money, only the largest counts.";
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  const seats = seatsOnMoneyPage(r, report);
  const drawn = waterfallChart(report.value).kind === "chart";
  // Notes are counted by characters, wrapped a word at a time.
  const noted = [...`${notes}${seats}`.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].reduce((sum, [, text]) => sum + NOTE_GAP + NOTE_LINE * wrappedLines(printed(text!), NOTE_CHARS), 0);
  const rest = (named: number) => ({ expected: restOf(all, named, "expected"), low: restOf(all, named, "low"), high: restOf(all, named, "high") });
  // The figures in the last row can be wider than any line's, so the column allows for every cut's.
  const column = labelColumn(parts, PAGE_TABLE, all.slice(1).map((_, i) => rest(i + 1)));
  const restRow = (n: number) => PAGE_TABLE.row + PAGE_TABLE.line * labelLines(r.t(restLabel(n)), column, PAGE_TABLE);
  /** The page's height with the chart drawn `chart` high and the first `named` lines named, the rest added up in the last row. */
  const height = (chart: number, named: number) => {
    const rows =
      all.slice(0, named).reduce((sum, g) => sum + rowHeight(r, parts, g, column, PAGE_TABLE), 0) +
      (named < all.length ? restRow(all.length - named) : 0) +
      // The total, with the rule above it.
      1 +
      PAGE_TABLE.row +
      PAGE_TABLE.line * labelLines(r.t("Total, each counted once"), column, PAGE_TABLE);
    return MONEY_HEAD + (drawn ? chart * CHART_SCALE + 14 : parts.kinds.length * BAR + 16) + TABLE_HEAD + rows + noted;
  };
  // With the seats on this page, the chart is shorter to make room for them. Where the
  // page can't hold it all, the chart first takes its shorter height, then the table
  // names fewer lines and adds the rest up in its last row (DECISIONS.md 435).
  let chart = investmentHasPage(report) ? TALL_CHART : SHORT_CHART;
  const most = all.length > PAGE_GROUPS ? PAGE_GROUPS - 1 : all.length;
  if (height(chart, most) > MONEY_ROOM) chart = SHORT_CHART;
  const holds = (named: number) => height(chart, named) <= MONEY_ROOM;
  // A line takes the last row's place when its label is no taller than the row's own words (DECISIONS.md 438).
  const fitsRestRow = (line: number) => labelLines(r.t(all[line]!.label), column, PAGE_TABLE) <= labelLines(r.t(restLabel(1)), column, PAGE_TABLE);
  const named = linesToName(all.length, PAGE_GROUPS, holds, fitsRestRow);
  const left = all.length - named;
  const label = (g: Group) => `${r.t(g.label)}${r.ref(valueItemId(g.id))}<span class="basis">${r.t(basis(g))}</span>`;
  const rows = [
    ...all.slice(0, named).map((g) => row(label(g), g.counted)),
    // With their amount, so the rows add up to the total beneath them (night 3 review).
    ...(left ? [row(`${r.t(restLabel(left))}${r.ref(valueItemId(all[named]!.id))}`, rest(named))] : []),
    totalRow,
  ].join("");
  const head = `<div class="eyebrow">${r.t(EYEBROW)}</div><h2>${TITLE}</h2>`;
  return `<div class="money">${head}${parts.figure(chart)}${table(rows)}${notes}${seats}</div>`;
}

/**
 * The full report's money, set in two columns: the figure and the chart beside
 * the table. The full report has no ceiling, so where the table can't fit the
 * slide it carries on to the next, its heading repeated and continued, every
 * line named and the total on the last (DECISIONS.md 439). Nothing is cut or
 * added up. Empty when nothing could be given a figure in money.
 */
export function moneySlides(r: Render, report: BuiltReport): string[] {
  if (report.value.total.expected <= 0) return [];
  const parts = moneyParts(r, report);
  const { all, row, totalRow, table, exposureNote } = parts;
  const how = "Not every case will come back, so we count a share of each figure, with a cautious share and all of it either side. Where several issues describe the same money, only the largest counts. The executive summary shows how each line was reached.";
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  /** What follows the last line: the total, with the rule above it, and the notes, as wide as the table. */
  const end = (measure: TableMeasure, column: number, width: number) =>
    1 +
    measure.row +
    measure.line * labelLines(r.t("Total, each counted once"), column, measure) +
    [...notes.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].reduce((sum, [, text]) => sum + SLIDE_NOTE_GAP + SLIDE_NOTE_LINE * textLines(printed(text!), width, SLIDE_NOTE_FONT), 0);
  const slides: Group[][] = [];
  let current: Group[] = [];
  let used = SLIDE_HEAD;
  let measure = SLIDE_TABLE;
  let width = SLIDE_SPLIT_WIDTH;
  const heightOf = (g: Group, i: number) => {
    const column = labelColumn(parts, measure);
    return rowHeight(r, parts, g, column, measure) + (i === all.length - 1 ? end(measure, column, width) : 0);
  };
  all.forEach((g, i) => {
    // The last line goes with the total and the notes, so a slide never holds the total alone.
    if (current.length > 0 && used + heightOf(g, i) > SLIDE_ROOM) {
      slides.push(current);
      current = [];
      used = SLIDE_HEAD;
      measure = CONTINUED_TABLE;
      width = SLIDE_FULL_WIDTH;
    }
    current.push(g);
    used += heightOf(g, i);
  });
  slides.push(current);
  return slides.map((group, i) => {
    const last = i === slides.length - 1;
    const rows = `${group.map((g) => row(`${r.t(g.label)}${r.ref(valueItemId(g.id))}`, g.counted)).join("")}${last ? totalRow : ""}`;
    const tableAndNotes = `${table(rows)}${last ? notes : ""}`;
    const head = `<div class="eyebrow">${r.t(EYEBROW)}</div><h2>${TITLE}${i ? ", continued" : ""}</h2>`;
    return i === 0 ? `<div class="money">${head}<div class="split"><div>${parts.figure(TALL_CHART, HALF_WIDTH)}</div><div>${tableAndNotes}</div></div></div>` : `<div class="money">${head}${tableAndNotes}</div>`;
  });
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
