import type { BuiltReport } from "../narrative/sections.ts";
import { INVESTMENT } from "../money/investment.ts";
import { HEADLINE_KINDS, KIND_LABELS, type ValueKind, type ValueModel } from "../value/model.ts";
import { compactMoney } from "./format.ts";
import { chartSlot, printed, wrappedLines, type Render } from "./html.ts";
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

/** Rows of groups each format has room for, the total row aside. */
const PAGE_GROUPS = 8;
const SLIDE_GROUPS = 7;
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
// drawn 800 wide across 658px with 14px under it, the table's head 31.5px, a
// row 13px and 18.75px a line, its basis 12.7px a line after 1px, a note 10px
// above and 17.2px a line. The label column is what the figures leave: their
// characters as wide as the total row's bold sets them. Characters a line were
// set wrapping a word at a time, and the most that never counted a line too few
// were, for a label, 66 at 451px and 56 at 389px on long-worded prose; for a
// basis, which is always the same sentence, 89 and 81 on every form it takes;
// and 110 a note line on the prose. Each is set below that, so the estimate
// errs towards naming a line fewer.
const MONEY_ROOM = 986;
const MONEY_HEAD = 171;
const CHART_SCALE = 658 / 800;
const TALL_CHART = 320;
const SHORT_CHART = 250;
const TABLE_HEAD = 32;
/** A row's padding and rule. */
const ROW = 13;
const ROW_LINE = 18.75;
/** A basis line, with the 1px above the first counted on each. */
const BASIS_LINE = 13.7;
/** A kind's bar where the chart can't be drawn: its name on two lines and the gap under it. */
const BAR = 49;
const NOTE_GAP = 10;
const NOTE_LINE = 17.25;
const NOTE_CHARS = 106;
/** The page's 658px less the three columns' padding. */
const TABLE_TEXT = 610;
/** The figures' headings, at their widest: a column is never narrower. */
const EXPECTED_HEAD = 59;
const RANGE_HEAD = 41;
/** A figure's characters in bold at 12.5px, rounded up: a capital not named is 10px, any other 8.5px. */
const FIGURE_WIDTHS: Record<string, number> = { "1": 4.8, ".": 3.6, " ": 3.4, t: 5.1, m: 13.3, "€": 10.2, M: 11.5, W: 13.2 };
const figureWidth = (text: string): number => [...text].reduce((sum, c) => sum + (FIGURE_WIDTHS[c] ?? (/[A-Z]/.test(c) ? 10 : 8.5)), 0);
/** A label's and a basis's character, wrapped a word at a time. */
const LABEL_PX = 7;
const BASIS_PX = 5.1;
/** A reference tag beside a label, counted as a word, as wide as the widest. */
const REF_WORD = "R00.00";

/** The lines past the first `from`, added up: what the money table's last row holds when it can't name them all. */
export function restOf(lines: ValueModel["groups"], from: number, k: "low" | "expected" | "high"): number {
  return Math.round(lines.slice(from).reduce((sum, g) => sum + g.counted[k], 0) * 100) / 100;
}

/** Empty when nothing could be given a figure in money: the page is left out rather than showing a nought. A slide sets it in two columns. */
export function moneyPage(r: Render, report: BuiltReport, format: "page" | "slide" = "page"): string {
  const eyebrow = "What it's worth";
  const { value } = report;
  const { total, byKind } = value;
  if (total.expected <= 0) return "";
  const m = (n: number) => compactMoney(n, value.currency);
  const kinds = HEADLINE_KINDS.filter((k) => byKind[k].expected > 0);
  const largest = Math.max(...kinds.map((k) => byKind[k].expected));
  const bars = kinds
    .map((k) => `<div class="kind"><span>${r.t(KIND_LABELS[k])}</span><div class="bar" style="width:${Math.max(1, Math.round((byKind[k].expected / largest) * 100))}%"></div><span class="amount">${m(byKind[k].expected)}</span></div>`)
    .join("");
  // Every group, so the rows add up to the headline (night 3 reading pass). Past the
  // most a page holds, the rest are named as a count with where to find them.
  const all = value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
  const room = format === "slide" ? SLIDE_GROUPS : PAGE_GROUPS;
  const pct = (n: number) => `${Math.round(n * 100)}%`;
  // How each row was reached: its figure before any share, and the share counted. The page has room to say so; the slide points to it.
  const basis = (g: (typeof all)[number]) => {
    const c = g.counted;
    const from = `${m(c.annual)} ${IN_FULL[g.kind as keyof typeof IN_FULL]}${g.lines.length > 1 ? ", the largest of overlapping issues" : ""}${OVER_WINDOW_BASIS.test(c.basis) ? ", a year's worth of what we measured" : ""}.`;
    return `${from} We count ${pct(g.realisation.expected)} (${pct(g.realisation.low)} to ${pct(g.realisation.high)}).`;
  };
  const restLabel = (rest: number) => `And ${rest} smaller ${rest === 1 ? "line" : "lines"}, in the raw notes.`;
  const range = (low: number, high: number) => `${m(low)} to ${m(high)}`;
  const exposure = value.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
  const exposureNote = exposure
    ? `<p class="note">${r.t(`Kept apart from this total: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's exposure, not money found, so it's never added.`)}${r.ref(valueItemId(exposure.id))}</p>`
    : "";
  const how =
    format === "page"
      ? "Not every case will come back, so we count a share of each figure: the expected share, with a cautious one and all of it either side. Where several issues describe the same money, only the largest counts."
      : "Not every case will come back, so we count a share of each figure, with a cautious share and all of it either side. Where several issues describe the same money, only the largest counts. The executive summary shows how each line was reached.";
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  const seats = format === "page" ? seatsOnMoneyPage(r, report) : "";
  // With the seats on this page, the chart is shorter to make room for them.
  let chart = format === "page" && !investmentHasPage(report) ? SHORT_CHART : TALL_CHART;
  let named = all.length > room ? room - 1 : all.length;
  if (format === "page") {
    // Where the page can't hold it all, the chart first takes its shorter height, then
    // the table names fewer lines and adds the rest up in its last row (DECISIONS.md 435).
    const parts: PageParts = { all, total, drawn: waterfallChart(value).kind === "chart", kinds: kinds.length, notes: `${notes}${seats}`, basis, restLabel, range, m };
    const fits = (c: number, n: number) => moneyPageHeight(r, parts, c, n) <= MONEY_ROOM;
    if (!fits(chart, named)) chart = SHORT_CHART;
    while (named > 1 && !fits(chart, named)) named -= 1;
  }
  const groups = all.slice(0, named);
  const rest = all.length - named;
  const label = (g: (typeof all)[number]) => `${r.t(g.label)}${r.ref(valueItemId(g.id))}${format === "page" ? `<span class="basis">${r.t(basis(g))}</span>` : ""}`;
  const rows = [
    ...groups.map((g) => `<tr><td>${label(g)}</td><td class="num">${m(g.counted.expected)}</td><td class="num">${range(g.counted.low, g.counted.high)}</td></tr>`),
    // With their amount, so the rows add up to the total beneath them (night 3 review).
    ...(rest ? [`<tr><td>${r.t(restLabel(rest))}${r.ref(valueItemId(all[named]!.id))}</td><td class="num">${m(restOf(all, named, "expected"))}</td><td class="num">${range(restOf(all, named, "low"), restOf(all, named, "high"))}</td></tr>`] : []),
    `<tr class="total"><td>${r.t("Total, each counted once")}</td><td class="num">${m(total.expected)}</td><td class="num">${range(total.low, total.high)}</td></tr>`,
  ].join("");
  // Each kind building to the total, exposure apart. The bars stay only where the waterfall can't be drawn.
  const waterfall = waterfallChart(value, format === "slide" ? HALF_WIDTH : undefined, chart);
  const build = waterfall.kind === "chart" ? `<div class="waterfall">${chartSlot(waterfall)}</div>` : `<div class="kinds">${bars}</div>`;
  const figure = `<p class="headline-figure">${m(total.expected)}</p><p class="range">${r.t(`Over the next 12 months, expected. Between ${m(total.low)} and ${m(total.high)}.`)}</p>${build}`;
  const table = `<table class="grid"><thead><tr><th>Where it comes from</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}</tbody></table>`;
  const head = `<div class="eyebrow">${r.t(eyebrow)}</div><h2>What fixing this is worth</h2>`;
  if (format === "slide") return `<div class="money">${head}<div class="split"><div>${figure}</div><div>${table}${notes}</div></div></div>`;
  return `<div class="money">${head}${figure}${table}${notes}${seats}</div>`;
}

type Groups = ValueModel["groups"];
type PageParts = {
  all: Groups;
  total: ValueModel["total"];
  drawn: boolean;
  kinds: number;
  notes: string;
  basis: (g: Groups[number]) => string;
  restLabel: (rest: number) => string;
  range: (low: number, high: number) => string;
  m: (n: number) => string;
};

/**
 * The money page's height, estimated, with the chart drawn `chart` high and the
 * first `named` lines named in the table, the rest added up in its last row.
 * Never calls `r.ref`: on the executive summary that notes a reference as used.
 */
function moneyPageHeight(r: Render, parts: PageParts, chart: number, named: number): number {
  const { all, total, drawn, kinds, notes, basis, restLabel, range, m } = parts;
  const shown = all.slice(0, named);
  const rest = named < all.length ? { expected: restOf(all, named, "expected"), low: restOf(all, named, "low"), high: restOf(all, named, "high") } : null;
  const lines = [...shown.map((g) => g.counted), total, ...(rest ? [rest] : [])];
  // The figures' columns take what their widest text needs; the label column has the rest.
  const widest = (head: number, texts: string[]) => Math.max(head, ...texts.map(figureWidth));
  const column = TABLE_TEXT - widest(EXPECTED_HEAD, lines.map((x) => m(x.expected))) - widest(RANGE_HEAD, lines.map((x) => range(x.low, x.high)));
  const wrapped = (text: string, px: number) => wrappedLines(printed(text), Math.floor(column / px));
  const row = (label: string) => ROW + ROW_LINE * wrapped(label, LABEL_PX);
  const rows =
    shown.reduce((sum, g) => sum + row(`${r.t(g.label)} ${REF_WORD}`) + BASIS_LINE * wrapped(r.t(basis(g)), BASIS_PX), 0) +
    (rest ? row(`${r.t(restLabel(all.length - named))} ${REF_WORD}`) : 0) +
    // The total, with the rule above it.
    1 +
    row(r.t("Total, each counted once"));
  const noted = [...notes.matchAll(/<p[^>]*>([\s\S]*?)<\/p>/g)].reduce((sum, [, text]) => sum + NOTE_GAP + NOTE_LINE * wrappedLines(printed(text!), NOTE_CHARS), 0);
  return MONEY_HEAD + (drawn ? chart * CHART_SCALE + 14 : kinds * BAR + 16) + TABLE_HEAD + rows + noted;
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
