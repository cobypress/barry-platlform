import { colour, page, tokensCss } from "../design/tokens.ts";
import type { OverallScore } from "../domains/scoring.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { Finding, Run } from "../schemas/index.ts";
import type { Chart } from "./charts.ts";
import { fontFaces } from "./fonts.ts";
import { compactMoney, escapeXml, money, plainWords, ukDates } from "./format.ts";
import { EXPOSURE_RULES } from "../value/model.ts";

// Shared by both PDFs. Every page is a fixed box, so a page can be measured and
// checked rather than left to flow. Names go back in here, then get escaped, so a
// name holding markup can't break out of the page.

export type Render = {
  /** Restores names, puts it in plain words, then escapes. Use for every piece of report text. */
  t(text: string): string;
  currency: string;
  draft: boolean;
  /** Where in the raw notes a finding or metric's working is, as a small tag. Empty when it has none. */
  ref(id: string): string;
  /** A counted finding's own figure over 12 months, from the value model. */
  yearly(findingId: string): number | undefined;
};

/** A reference is a place in the raw notes, not a figure, so the number check passes over it. */
export const refTag = (ref: string): string => `<span class="ref" data-ref>${ref}</span>`;

export function renderContext(pseudonyms: Pseudonymiser, currency: string, draft: boolean, refs: Map<string, string> = new Map(), yearly: Map<string, number> = new Map()): Render {
  return {
    yearly: (findingId) => yearly.get(findingId),
    t: (text) => escapeXml(plainWords(ukDates(pseudonyms.restore(text)))),
    currency,
    draft,
    ref: (id) => {
      const ref = refs.get(id);
      return ref ? refTag(ref) : "";
    },
  };
}

/** The documents' title: a set-up review is a different product, and its cover says so. */
export const documentTitle = (run: Pick<Run, "scope">): string => (run.scope === "essentials" ? "Set-up review" : "Salesforce health check");

export const paragraphs = (r: Render, text: string): string =>
  text.split(/\n\n+/).filter((p) => p.trim()).map((p) => `<p>${r.t(p).replace(/\n/g, "<br>")}</p>`).join("");

export function chartSlot(chart: Chart): string {
  const inner = chart.kind === "chart" ? chart.svg : `<div class="chart-gap"><strong>Not measurable.</strong> ${escapeXml(plainWords(ukDates(chart.reason)))}</div>`;
  return `<figure class="chart-slot" data-expect-chart="${chart.id}">${inner}</figure>`;
}

export function severityChip(f: Finding): string {
  const alert = f.severity === "critical" || f.severity === "high";
  return `<span class="chip${alert ? " alert" : ""}">${f.severity.charAt(0).toUpperCase()}${f.severity.slice(1)}</span>`;
}

/** The figure alone, for a table where the period sentence would not fit. */
export function valueFigure(r: Render, f: Finding): string {
  if (f.value.kind === "not_sized") return "Not sized";
  const currency = /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : r.currency;
  return f.value.kind === "hours" ? `${f.value.amount} hours` : money(f.value.amount, currency);
}

export function valueText(r: Render, f: Finding): { amount: string; note: string } {
  // Not every issue has a cost we can put a figure on. Its severity is already beside it, so this says only that.
  if (f.value.kind === "not_sized") return { amount: "Not costed", note: r.t(sentence(f.value.reason)) };
  const target = targetText(r, f);
  if (target) return { amount: r.t(target), note: r.t(sentence(f.value.assumptions.join(". "))) };
  // Pipeline at risk, or spend not coming back, isn't money to win: it says so beside the figure (night 3 reading pass).
  // A segment's gap is over the whole window and may overlap another's: the card gives the value model's year (DECISIONS.md 405).
  const year = f.ruleId === "pipe.segment-divergence" ? r.yearly(f.id) : undefined;
  if (year !== undefined) return { amount: r.t(`About ${compactMoney(year, /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : r.currency)} a year in wins the gap may have cost`), note: r.t(sentence(f.value.assumptions.join(". "))) };
  if (EXPOSURE_RULES.has(f.ruleId) && f.ruleId !== "lead.campaign-return") return { amount: r.t(`At risk: ${valueFigure(r, f)} ${f.value.period}`), note: r.t(sentence(f.value.assumptions.join(". "))) };
  return { amount: r.t(`${valueFigure(r, f)} ${f.value.period}`), note: r.t(sentence(f.value.assumptions.join(". "))) };
}

export { compactMoney };

/**
 * Coverage's figure said as what it is: pipeline to build, not money (DECISIONS.md 351).
 * Null for any other finding, or when its facts don't hold what the sentence needs.
 */
function targetText(r: Render, f: Finding): string | null {
  if (f.ruleId !== "cur.coverage" || f.value.kind === "not_sized") return null;
  const { target, quota, won, remaining, shortfall } = f.facts;
  if (typeof target !== "number" || typeof quota !== "number" || typeof remaining !== "number" || typeof shortfall !== "number") return null;
  const currency = /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : r.currency;
  const m = (n: number) => compactMoney(n, currency);
  const of = won ? `what's left of your ${m(quota)} quota (${m(remaining)})` : `your ${m(quota)} quota`;
  // Enough overall, but not for every rep: there's no gap to state, only who's short (DECISIONS.md 369).
  if (shortfall <= 0) return `Open pipeline this quarter is ${f.facts.coverage}x ${won ? "what's left of quota" : "quota"} (${m(remaining)}), above the ${target}x target overall, but some reps are below it`;
  return `To reach ${target}x coverage of ${of} this quarter, you'd need ${m(shortfall)} more pipeline`;
}

/** Ends with a full stop, as a sentence the reader sees should. */
export const sentence = (text: string): string => (text === "" || /[.!?:)]$/.test(text.trim()) ? text : `${text.trim()}.`);

/** The Barometer score as the cover shows it. Nothing when the org couldn't be given one. */
export function coverScore(overall: OverallScore): string {
  if (overall.status !== "measured") return "";
  return `<div class="cover-score"><p class="for">Barometer score</p><p><b>${overall.score}</b><span>out of 100</span></p><p class="grade">Grade ${overall.grade}, ${overall.word.toLowerCase()}</p></div>`;
}

const EFFORT_WORDS: Record<string, string> = { XS: "very small", S: "small", M: "medium", L: "large", XL: "very large" };

/** T-shirt sizes are our shorthand. A reader gets words. */
export const effortText = (f: Finding): string => `Effort: ${EFFORT_WORDS[f.effort.size] ?? f.effort.size}`;

/** Lines of wrapped text, near enough to decide what fits on a page. */
export const estimateLines = (text: string, charsPerLine: number): number => Math.max(1, Math.ceil(text.length / charsPerLine));

const ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: '"' };
/** One character for an entity: the one it shows, or a letter for a space a line can't break at. */
const entity = (e: string): string => {
  const c = e.startsWith("#") ? String.fromCharCode(Number(e.slice(1))) : (ENTITIES[e] ?? "x");
  return /\s/.test(c) ? "x" : c;
};
/** Text as it prints: tags and the references in them as words, an entity as the one character it shows. */
export const printed = (html: string): string => html.replace(/<[^>]*>/g, " ").replace(/&(#\d+|[a-z]+);/g, (_, e: string) => entity(e));

/** Lines of text wrapped at word breaks, as a browser sets it, to a cautious width: a word that doesn't fit starts the next line. */
export function wrappedLines(text: string, chars: number): number {
  let lines = 1;
  let used = 0;
  for (const word of text.split(/\s+/).filter(Boolean)) {
    const need = used === 0 ? word.length : used + 1 + word.length;
    if (need <= chars) used = need;
    else {
      lines += used === 0 ? Math.ceil(word.length / chars) - 1 : 1;
      used = Math.min(word.length, chars);
    }
  }
  return lines;
}

// Each letter as wide as Chromium sets it in Poppins at 13.5px, rounded up
// (DECISIONS.md 436). Type at another size scales with it, so one table serves
// every size a page sets (438).
/** a to z in Poppins at 13.5px. */
const LOWER_WIDTHS = [9.2, 9.2, 8.2, 9.2, 8.4, 4.5, 9.2, 8.7, 3.4, 3.4, 7, 3.4, 14, 8.7, 8.7, 9.2, 9.2, 5.1, 7.1, 5, 8.7, 7.6, 11.1, 6.5, 7.6, 6.2];
const MEASURED_AT = 13.5;
/** The signs a money line says its basis with: a pound, a percentage and its brackets (DECISIONS.md 438). */
const SIGN_WIDTHS: Record<string, number> = { "£": 8.4, "%": 10.3, "(": 6.2, ")": 6.2 };
/** Any other character: capitals and figures at their widest, light punctuation, and the widest we measured for the rest. */
const glyphWidth = (c: string): number =>
  /[a-z]/.test(c) ? LOWER_WIDTHS[c.charCodeAt(0) - 97]! : /[MW]/.test(c) ? 13.2 : /[A-Z]/.test(c) ? 10.7 : /\d/.test(c) ? 8.6 : c === " " ? 3.6 : /[,.:;'!]/.test(c) ? 4.1 : c === "-" ? 7.5 : (SIGN_WIDTHS[c] ?? 14);

/**
 * Lines of text set `width` pixels wide in Poppins at `px`, a word at a time, as
 * the browser sets it: a word that doesn't fit starts the next line, and a word
 * is as wide as its letters. Give it the text as it prints (`printed`).
 */
export function textLines(text: string, width: number, px: number): number {
  const room = (width * MEASURED_AT) / px;
  const wide = (word: string) => [...word].reduce((sum, c) => sum + glyphWidth(c), 0);
  let lines = 1;
  let used = 0;
  for (const word of text.split(/\s+/).filter(Boolean)) {
    const need = used === 0 ? wide(word) : used + glyphWidth(" ") + wide(word);
    if (need <= room || used === 0) used = need;
    else {
      lines += 1;
      used = wide(word);
    }
  }
  return lines;
}

/** Fills pages in order, moving to a new page when the next item wouldn't fit. */
export function paginate<T>(items: T[], heightOf: (item: T) => number, budget: number): T[][] {
  const pages: T[][] = [];
  let current: T[] = [];
  let used = 0;
  for (const item of items) {
    const height = heightOf(item);
    if (current.length > 0 && used + height > budget) {
      pages.push(current);
      current = [];
      used = 0;
    }
    current.push(item);
    used += height;
  }
  if (current.length > 0) pages.push(current);
  return pages;
}

/**
 * Fills pages of side-by-side columns, as the browser fills a multi-column list
 * whose items don't break: an item goes on down the column it's in, or tops the
 * next, or starts a page. A height shared across the columns would let two tall
 * items into a column that holds only one (DECISIONS.md 442).
 */
export function paginateColumns<T>(items: T[], heightOf: (item: T) => number, columns: number, columnHeight: number): T[][] {
  const pages: T[][] = [];
  let current: T[] = [];
  let column = 0;
  let used = 0;
  for (const item of items) {
    const height = heightOf(item);
    if (current.length > 0 && used + height > columnHeight) {
      column += 1;
      used = 0;
      if (column === columns) {
        pages.push(current);
        current = [];
        column = 0;
      }
    }
    current.push(item);
    used += height;
  }
  if (current.length > 0) pages.push(current);
  return pages;
}

const BASE_CSS = `
*, *::before, *::after { box-sizing: border-box; }
html, body { margin: 0; padding: 0; background: var(--ivory); color: var(--carbon); font-family: var(--font); -webkit-print-color-adjust: exact; print-color-adjust: exact; }
.page { position: relative; overflow: hidden; break-after: page; background: var(--ivory); display: flex; flex-direction: column; }
.page > * { flex-shrink: 0; }
.page:last-child { break-after: auto; }
.page.dark { background: var(--carbon); color: var(--ivory); }
h1, h2, h3, p, figure { margin: 0; }
.eyebrow { font-weight: 600; letter-spacing: 0.02em; color: var(--muted); }
.dark .eyebrow { color: var(--chartreuse); }
.mark { display: inline-block; background: var(--chartreuse); color: var(--carbon); padding: 2px 10px; border-radius: var(--radius-s); font-weight: 600; }
.chip { display: inline-block; border: 1px solid var(--carbon); border-radius: var(--radius-s); padding: 1px 8px; font-size: 0.8em; font-weight: 500; white-space: nowrap; }
.chip.alert { background: var(--alert); border-color: var(--alert); color: var(--ivory); }
.chip.input { background: var(--chartreuse); border-color: var(--chartreuse); color: var(--carbon); }
.footer { position: absolute; display: flex; justify-content: space-between; color: var(--muted); font-size: 11px; }
.dark .footer { color: var(--panelMuted); }
.chart-slot svg { display: block; width: 100%; height: auto; }
.chart-gap { background: var(--snow); border-radius: var(--radius-m); padding: 20px; color: var(--muted); }
.stat-row { display: flex; gap: 12px; }
.stat { flex: 1 1 0; background: var(--snow); border-radius: var(--radius-m); padding: 14px 16px; min-width: 0; }
.stat b { display: block; font-weight: 600; }
.stat span { color: var(--muted); }
table.grid { width: 100%; border-collapse: collapse; }
table.grid th { text-align: left; font-weight: 600; border-bottom: 1px solid var(--carbon); padding: 6px 8px; }
table.grid td { border-bottom: 1px solid var(--rule); padding: 6px 8px; vertical-align: top; }
td.num, th.num { text-align: right; white-space: nowrap; }
.cover-score b { font-size: 3.2em; font-weight: 700; line-height: 1; color: var(--chartreuse); margin-right: 10px; }
.cover-score span { color: var(--panelMuted); }
.cover-score .grade { font-weight: 600; margin-top: 4px; }
.ref { color: var(--muted); font-size: 0.85em; font-weight: 400; letter-spacing: 0.02em; margin-left: 6px; white-space: nowrap; }
.dark .ref { color: var(--panelMuted); }
`;

export function document(title: string, size: "a4" | "slide", css: string, pages: string[]): string {
  const dims = page[size];
  const pageCss = `@page { size: ${dims.width} ${dims.height}; margin: 0; } .page { width: ${dims.width}; height: ${dims.height}; padding: ${dims.margin}; }`;
  return `<!doctype html><html lang="en-GB"><head><meta charset="utf-8"><title>${escapeXml(title)}</title><style>${fontFaces()}\n${tokensCss()}\n${BASE_CSS}\n${pageCss}\n${css}</style></head><body data-format="${size}">${pages.join("\n")}</body></html>`;
}

export const ALERT_ONLY_COLOUR = colour.alert;
