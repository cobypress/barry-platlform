import { comparisonId, differenceWords, formatFigure, type Comparison, type GroupKind } from "../benchmark/compare.ts";
import type { Render } from "./html.ts";

// The "How you compare" slide. Published research and our own customers sit in
// separate columns and are never combined in one figure. A published figure is
// never shown without its source beside it.

export type CompareSlide = { html: string; classes?: string };

/** Styles the slide needs beyond the full report's own. */
export const COMPARE_CSS = `
table.compare th { white-space: normal; vertical-align: bottom; font-size: 13.5px; line-height: 1.25; }
table.compare td { font-size: 14.5px; line-height: 1.3; }
table.compare .src { display: block; color: var(--muted); font-size: 12px; }
`;

// Short, because the group's full name is in the raw notes beside its working.
const GROUP_WORDS: Record<GroupKind, string> = { "industry-and-size": "your industry and size", industry: "your industry", size: "your size", all: "all customers" };

const capital = (text: string): string => `${text.charAt(0).toUpperCase()}${text.slice(1)}`;

function ownCell(r: Render, c: Comparison): string {
  const own = c.own;
  if (own.status === "measured") {
    const fmt = (v: number): string => formatFigure(v, c.metric.unit);
    return `${r.t(`${fmt(own.p25)} to ${fmt(own.p75)}, median ${fmt(own.median)}`)}<span class="src">${r.t(`${own.group.orgs} orgs, ${GROUP_WORDS[own.group.kind]}`)}</span>`;
  }
  if (c.value === null || own.tried.length === 0) return `<span class="src">${r.t("Not available")}</span>`;
  const all = own.tried.at(-1)!.orgs ?? 0;
  // The floor and the reason are in the raw notes. The count is shown, as decided.
  return `<span class="src">${r.t(all === 0 ? "No figures yet" : `Not shown yet: ${all} ${all === 1 ? "customer" : "customers"} so far`)}</span>`;
}

function publishedCell(r: Render, c: Comparison): string {
  const p = c.published[0];
  if (!p) return `<span class="src">${r.t(c.value === null ? "Not available" : "None that measures this our way")}</span>`;
  const f = p.figure;
  // The source goes with the figure, always, in the same cell.
  // Higher isn't always better, so the sentence says which this is (night 3 reading pass).
  const against = `Yours is ${differenceWords(c, p)}${p.side === "worse" ? ", which is worse" : p.side === "better" ? ", which is better" : ""}`;
  return `${r.t(`${formatFigure(f.value, c.metric.unit)} ${f.statistic}. ${against}.`)}<span class="src">${r.t(`${f.source.publisher}, ${f.source.year}`)}</span>`;
}

/** One row per headline figure: yours, our customers' range and where you sit in it, and published research with its source. */
export function compareSlide(r: Render, comparisons: Comparison[], eyebrow = "Benchmarks"): CompareSlide {
  const rows = comparisons.map((c) => {
    const yours = c.value === null ? `<span class="src">${r.t("Not measured")}</span>` : r.t(formatFigure(c.value, c.metric.unit));
    const position = c.own.status === "measured" ? r.t(capital(c.own.position)) : `<span class="src">${r.t("No view yet")}</span>`;
    return `<tr><td>${r.t(c.metric.label)}${r.ref(comparisonId(c.metric.key))}</td><td class="num">${yours}</td><td>${ownCell(r, c)}</td><td>${position}</td><td>${publishedCell(r, c)}</td></tr>`;
  });
  const lede =
    "Your headline figures against other Black Cloud customers, and against published research. We never combine them: published research measures other companies in its own way, and the raw notes say how each figure differs from ours.";
  const note = "The top quarter is always the better end: higher for win rates and lead conversion, shorter or lower for the rest. The middle half is where the middle of the group sits.";
  return {
    classes: "tight",
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>How you compare</h2><p class="lede">${r.t(lede)}</p><table class="grid data compare"><thead><tr><th>Measure</th><th class="num">Yours</th><th>Black Cloud customers, middle half</th><th>Where you sit</th><th>Published research</th></tr></thead><tbody>${rows.join("")}</tbody></table><p class="small-note">${r.t(note)}</p>`,
  };
}
