import type { BuiltReport } from "../narrative/sections.ts";
import { HEADLINE_KINDS, KIND_LABELS } from "../value/model.ts";
import { compactMoney } from "./format.ts";
import type { Render } from "./html.ts";
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
`;

const TOP_GROUPS = 5;

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
  const groups = value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected).slice(0, format === "slide" ? TOP_GROUPS - 1 : TOP_GROUPS);
  const rows = groups.map((g) => `<tr><td>${r.t(g.label)}${r.ref(valueItemId(g.id))}</td><td class="num">${m(g.counted.expected)}</td><td class="num">${m(g.counted.low)} to ${m(g.counted.high)}</td></tr>`).join("");
  const exposure = value.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
  const exposureNote = exposure
    ? `<p class="note">${r.t(`Kept apart from this total: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's exposure, not money found, so it's never added.`)}${r.ref(valueItemId(exposure.id))}</p>`
    : "";
  const how = "Where several issues describe the same money, only the largest counts. Each figure is what's there if every case were won back, so we count a share of it, and the expected case is the conservative one.";
  const figure = `<p class="headline-figure">${m(total.expected)}</p><p class="range">${r.t(`Over the next 12 months, expected. Between ${m(total.low)} and ${m(total.high)}.`)}</p><div class="kinds">${bars}</div>`;
  const table = `<table class="grid"><thead><tr><th>Where it comes from</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}</tbody></table>`;
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  const head = `<div class="eyebrow">${r.t(eyebrow)}</div><h2>What fixing this is worth</h2>`;
  if (format === "slide") return `<div class="money">${head}<div class="split"><div>${figure}</div><div>${table}${notes}</div></div></div>`;
  return `<div class="money">${head}${figure}${table}${notes}</div>`;
}
