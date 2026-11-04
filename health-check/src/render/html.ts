import { colour, page, tokensCss } from "../design/tokens.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { Finding } from "../schemas/index.ts";
import type { Chart } from "./charts.ts";
import { fontFaces } from "./fonts.ts";
import { compactMoney, escapeXml, money, plainWords, ukDates } from "./format.ts";

// Shared by both PDFs. Every page is a fixed box, so a page can be measured and
// checked rather than left to flow. Names go back in here, then get escaped, so a
// name holding markup can't break out of the page.

export type Render = {
  /** Restores names, puts it in plain words, then escapes. Use for every piece of report text. */
  t(text: string): string;
  currency: string;
  draft: boolean;
};

export function renderContext(pseudonyms: Pseudonymiser, currency: string, draft: boolean): Render {
  return { t: (text) => escapeXml(plainWords(ukDates(pseudonyms.restore(text)))), currency, draft };
}

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
  if (f.value.kind === "not_sized") return { amount: "Not sized", note: r.t(f.value.reason) };
  return { amount: r.t(`${valueFigure(r, f)} ${f.value.period}`), note: r.t(f.value.assumptions.join(". ")) };
}

export { compactMoney };

const EFFORT_WORDS: Record<string, string> = { XS: "very small", S: "small", M: "medium", L: "large", XL: "very large" };

/** T-shirt sizes are our shorthand. A reader gets words. */
export const effortText = (f: Finding): string => `Effort: ${EFFORT_WORDS[f.effort.size] ?? f.effort.size}`;

/** Lines of wrapped text, near enough to decide what fits on a page. */
export const estimateLines = (text: string, charsPerLine: number): number => Math.max(1, Math.ceil(text.length / charsPerLine));

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
`;

export function document(title: string, size: "a4" | "slide", css: string, pages: string[]): string {
  const dims = page[size];
  const pageCss = `@page { size: ${dims.width} ${dims.height}; margin: 0; } .page { width: ${dims.width}; height: ${dims.height}; padding: ${dims.margin}; }`;
  return `<!doctype html><html lang="en-GB"><head><meta charset="utf-8"><title>${escapeXml(title)}</title><style>${fontFaces()}\n${tokensCss()}\n${BASE_CSS}\n${pageCss}\n${css}</style></head><body data-format="${size}">${pages.join("\n")}</body></html>`;
}

export const ALERT_ONLY_COLOUR = colour.alert;
