import type { BuiltReport } from "../narrative/sections.ts";
import { INVESTMENT } from "../money/investment.ts";
import { HEADLINE_KINDS, KIND_LABELS } from "../value/model.ts";
import { compactMoney } from "./format.ts";
import { chartSlot, type Render } from "./html.ts";
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
.money .basis { display: block; color: var(--muted); font-size: 0.78em; line-height: 1.3; margin-top: 1px; }
.money tr.total td { font-weight: 700; border-bottom: none; border-top: 1px solid var(--carbon); }
`;

/** Rows of groups each format has room for, the total row aside. */
const PAGE_GROUPS = 8;
const SLIDE_GROUPS = 7;
/** The value model's basis for a figure measured over the analysis window. */
const OVER_WINDOW_BASIS = /divided by/;

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
  const groups = all.length > room ? all.slice(0, room - 1) : all;
  const rest = all.length - groups.length;
  const pct = (n: number) => `${Math.round(n * 100)}%`;
  // How each row was reached: its figure before any share, and the share counted. The page has room to say so; the slide points to it.
  const basis = (g: (typeof all)[number]) => {
    const c = g.counted;
    const from = `${m(c.annual)} if all came back${g.lines.length > 1 ? ", the largest of overlapping issues" : ""}${OVER_WINDOW_BASIS.test(c.basis) ? ", a year's worth of what we measured" : ""}.`;
    return `${from} We count ${pct(g.realisation.expected)} (${pct(g.realisation.low)} to ${pct(g.realisation.high)}).`;
  };
  const label = (g: (typeof all)[number]) => `${r.t(g.label)}${r.ref(valueItemId(g.id))}${format === "page" ? `<span class="basis">${r.t(basis(g))}</span>` : ""}`;
  const rows = [
    ...groups.map((g) => `<tr><td>${label(g)}</td><td class="num">${m(g.counted.expected)}</td><td class="num">${m(g.counted.low)} to ${m(g.counted.high)}</td></tr>`),
    ...(rest ? [`<tr><td colspan="3">${r.t(`And ${rest} smaller ${rest === 1 ? "line" : "lines"}, in the raw notes.`)}${r.ref(valueItemId(all[groups.length]!.id))}</td></tr>`] : []),
    `<tr class="total"><td>${r.t("Total, each counted once")}</td><td class="num">${m(total.expected)}</td><td class="num">${m(total.low)} to ${m(total.high)}</td></tr>`,
  ].join("");
  const exposure = value.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
  const exposureNote = exposure
    ? `<p class="note">${r.t(`Kept apart from this total: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's exposure, not money found, so it's never added.`)}${r.ref(valueItemId(exposure.id))}</p>`
    : "";
  const how =
    format === "page"
      ? "Not every case will come back, so we count a share of each figure: the expected share, with a cautious one and all of it either side. Where several issues describe the same money, only the largest counts."
      : "Not every case will come back, so we count a share of each figure, with a cautious share and all of it either side. Where several issues describe the same money, only the largest counts. The executive summary shows how each line was reached.";
  // Each kind building to the total, exposure apart. The bars stay only where the waterfall can't be drawn.
  const waterfall = waterfallChart(value, format === "slide" ? HALF_WIDTH : undefined);
  const build = waterfall.kind === "chart" ? `<div class="waterfall">${chartSlot(waterfall)}</div>` : `<div class="kinds">${bars}</div>`;
  const figure = `<p class="headline-figure">${m(total.expected)}</p><p class="range">${r.t(`Over the next 12 months, expected. Between ${m(total.low)} and ${m(total.high)}.`)}</p>${build}`;
  const table = `<table class="grid"><thead><tr><th>Where it comes from</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}</tbody></table>`;
  const notes = `${exposureNote}<p class="note">${r.t(how)}</p>`;
  const head = `<div class="eyebrow">${r.t(eyebrow)}</div><h2>What fixing this is worth</h2>`;
  if (format === "slide") return `<div class="money">${head}<div class="split"><div>${figure}</div><div>${table}${notes}</div></div></div>`;
  return `<div class="money">${head}${figure}${table}${notes}</div>`;
}

/** What fixing it is worth against what they pay a year, to one decimal place. Null without both. */
export function returnOnSpend(report: BuiltReport): number | null {
  const cost = report.investment.annualCost;
  if (cost.status !== "measured" || cost.value <= 0 || report.value.total.expected <= 0) return null;
  return Math.round((report.value.total.expected / cost.value) * 10) / 10;
}

/** "Your investment": what they pay, what they use, and what they pay for and don't use. Empty when no seat could be read. */
export function investmentPage(r: Render, report: BuiltReport): string {
  const { investment, metrics } = report;
  const measured = (id: string) => {
    const m = metrics.find((x) => x.id === id);
    return m?.status === "measured" ? m : null;
  };
  const paid = measured(INVESTMENT.seatsPaid);
  if (!paid) return "";
  const used = measured(INVESTMENT.seatsUsed);
  const unused = measured(INVESTMENT.seatsUnused);
  const active = measured(INVESTMENT.activeUserShare);
  const unusedCost = measured(INVESTMENT.unusedSeatCost);
  const m = (n: number) => compactMoney(n, investment.currency);
  const cost = investment.annualCost;
  // The value model's line for the same seats, so the two pages can be read together.
  const seatShare = report.value.groups.find((g) => g.id === "unassigned-seats")?.counted ?? null;
  const stat = (figure: string, label: string, id: string) => `<div class="stat"><b>${figure}</b><span>${r.t(label)}</span>${r.ref(id)}</div>`;
  const stats = [
    stat(String(paid.value), "Seats you pay for, all licence types", INVESTMENT.seatsPaid),
    used ? stat(String(used.value), "Seats in use", INVESTMENT.seatsUsed) : "",
    unused ? stat(String(unused.value), "Seats nobody is assigned to", INVESTMENT.seatsUnused) : "",
    active ? stat(`${active.value}%`, "Of users active", INVESTMENT.activeUserShare) : "",
  ].join("");
  const headline =
    cost.status === "measured"
      ? `<p class="headline-figure">${m(cost.value)}</p><p class="range">${r.t(`A year. ${cost.label}.`)}</p>`
      : // Why it isn't priced is for the raw notes. Here it's enough to say what the page shows instead.
        `<p class="range">${r.t("We don't have a figure for your whole Salesforce bill, so this page shows your seats rather than what they all cost.")}</p>`;
  const ratio = returnOnSpend(report);
  const lines = [
    unusedCost ? `${unused ? `The ${unused.value} seats nobody is assigned to` : "Seats nobody is assigned to"} cost ${m(unusedCost.value)} a year.${seatShare ? ` We count a share of it, ${m(seatShare.expected)}, in what fixing this is worth.` : ""}` : "",
    ratio !== null ? `What fixing the issues is worth, ${m(report.value.total.expected)} over 12 months, is ${ratio} times what you pay for Salesforce a year.` : "",
  ].filter(Boolean);
  return `<div class="money"><div class="eyebrow">Your investment</div><h2>What you pay, and what you use</h2>${headline}<div class="stat-row" style="margin:6px 0 16px">${stats}</div>${lines.map((l) => `<p style="margin-bottom:8px">${r.t(l)}</p>`).join("")}</div>`;
}
