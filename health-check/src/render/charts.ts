import { colour, font, radius } from "../design/tokens.ts";
import type { ScorecardRow } from "../narrative/select.ts";
import type { Metric, MetricSegment } from "../schemas/index.ts";
import { compactMoney, escapeXml, monthYear, percent, plainWords, ukDates } from "./format.ts";
import { FELL_MOST } from "../domains/adoption/index.ts";
import { HEADLINE_KINDS, KIND_LABELS, type ValueModel } from "../value/model.ts";
import { areaWeight } from "../domains/scoring.ts";
import type { Progress } from "../progress/compare.ts";

// Server-rendered SVG, with no browser and no chart library. Every chart sits on
// a Carbon panel, because Chartreuse only reaches 1.5:1 on Ivory. Values are
// labelled directly, so nothing relies on colour alone. A chart with no data
// returns its reason instead of an empty frame.

export type Chart = { kind: "chart"; id: string; title: string; alt: string; svg: string } | { kind: "gap"; id: string; title: string; reason: string };

const WIDTH = 800;
const PAD = 24;
const BAR = 18;
const CHAR_WIDTH = 0.58;

const text = (x: number, y: number, value: string, options: { size?: number; fill?: string; anchor?: "start" | "middle" | "end"; weight?: number; cls?: string } = {}): string => {
  const { size = 13, fill = colour.ivory, anchor = "start", weight = font.weights.regular, cls = "" } = options;
  return `<text x="${x}" y="${y}" font-size="${size}" font-weight="${weight}" fill="${fill}" text-anchor="${anchor}"${cls ? ` class="${cls}"` : ""}>${escapeXml(plainWords(ukDates(value)))}</text>`;
};

/** A bar with a rounded data end and a square baseline. */
function bar(x: number, y: number, width: number, height: number, fill: string): string {
  const w = Math.max(width, 1);
  const r = Math.min(radius.small, w, height / 2);
  return `<path d="M${x} ${y}h${w - r}a${r} ${r} 0 0 1 ${r} ${r}v${height - 2 * r}a${r} ${r} 0 0 1 ${-r} ${r}h${-(w - r)}z" fill="${fill}"/>`;
}

function frame(id: string, title: string, alt: string, height: number, body: string, width = WIDTH): Chart {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeXml(alt)}" font-family="${escapeXml(font.stack)}" data-chart="${id}"><rect width="${width}" height="${height}" rx="${radius.medium}" fill="${colour.carbon}"/>${body}</svg>`;
  return { kind: "chart", id, title, alt, svg };
}

function clip(value: string, width: number, size: number): string {
  const max = Math.floor(width / (size * CHAR_WIDTH));
  return value.length <= max ? value : `${value.slice(0, Math.max(max - 1, 1)).trimEnd()}…`;
}

const gap = (id: string, title: string, reason: string): Chart => ({ kind: "gap", id, title, reason });

const missing = (metric: Metric | undefined): string | null => (!metric ? "This metric wasn't part of the run." : metric.status === "not_measurable" ? metric.reason : null);

/** Team and record type only: executive reports stay at team level. Rep segments stay in the raw notes. */
export function winRateBySegment(metric: Metric | undefined, dimensions: string[] = ["team", "record type"], width = WIDTH): Chart {
  const id = "win-rate-by-segment";
  const title = "Win rate by segment";
  const reason = missing(metric);
  if (reason) return gap(id, title, reason);
  if (metric!.status !== "measured") return gap(id, title, "Not measured.");
  const m = metric!;
  const rows: Array<MetricSegment | { heading: string }> = dimensions.flatMap((d) => {
    const own = m.segments.filter((s) => s.dimension === d);
    return own.length ? [{ heading: d.charAt(0).toUpperCase() + d.slice(1) }, ...own] : [];
  });
  if (rows.length === 0) return gap(id, title, m.segmentExemptReason ?? "This metric has no segments to show.");

  const labelWidth = width < WIDTH ? 150 : 200;
  const left = PAD + labelWidth;
  const trackWidth = width - left - PAD - 90;
  const scale = (value: number): number => (value / 100) * trackWidth;
  const rowHeight = 30;
  const top = 44;
  const overall = left + scale(m.value);
  const body: string[] = [text(PAD, 28, `Overall ${percent(m.value)}, ${m.n} deals`, { size: 13, fill: colour.panelMuted })];
  let y = top;
  for (const row of rows) {
    if ("heading" in row) {
      body.push(text(PAD, y + 16, row.heading, { size: 12, weight: font.weights.semibold, fill: colour.panelMuted }));
      y += rowHeight - 6;
      continue;
    }
    body.push(text(PAD, y + 15, clip(row.key.replace(/_/g, " "), labelWidth - 12, 13), { size: 13 }));
    if (row.value === null) {
      body.push(text(left, y + 15, `Too few deals to measure (${row.n})`, { size: 12, fill: colour.panelMuted }));
    } else {
      body.push(bar(left, y + 2, scale(row.value), BAR, colour.chartreuse));
      // Moved past the overall line when it would otherwise be drawn through the value.
      const tip = left + scale(row.value) + 8;
      const crosses = tip < overall && tip + 52 > overall;
      body.push(text(crosses ? overall + 8 : tip, y + 15, percent(row.value), { size: 13, weight: font.weights.medium }));
    }
    y += rowHeight;
  }
  body.push(`<line x1="${overall}" x2="${overall}" y1="${top}" y2="${y - 6}" stroke="${colour.ivory}" stroke-width="1"/>`);
  const alt = `Win rate by segment against ${percent(m.value)} overall.`;
  return frame(id, title, alt, y + 14, body.join(""), width);
}

/** Bars show deals reaching each stage. The label says what share went on to the next. */
export function stageFunnel(metric: Metric | undefined, width = WIDTH): Chart {
  const id = "stage-funnel";
  const title = "Stage funnel";
  const reason = missing(metric);
  if (reason) return gap(id, title, reason);
  if (metric!.status !== "measured") return gap(id, title, "Not measured.");
  const steps = metric!.segments.filter((s) => s.dimension === "step");
  if (steps.length === 0) return gap(id, title, "No stage steps were measured.");

  const labelWidth = width < WIDTH ? 120 : 170;
  const left = PAD + labelWidth;
  const trackWidth = width - left - PAD - (width < WIDTH ? 175 : 220);
  const largest = Math.max(...steps.map((s) => s.n), 1);
  const rowHeight = 36;
  const body: string[] = [];
  steps.forEach((step, i) => {
    const y = 20 + i * rowHeight;
    const [from, to] = step.key.split(" to ");
    const length = (step.n / largest) * trackWidth;
    body.push(text(PAD, y + 15, clip(from ?? step.key, labelWidth - 12, 13), { size: 13 }));
    body.push(bar(left, y + 2, length, BAR, colour.chartreuse));
    body.push(text(left + length + 8, y + 15, `${step.n}`, { size: 13, weight: font.weights.medium }));
    if (step.value !== null) body.push(text(width - PAD, y + 15, `${percent(step.value)} to ${clip(to ?? "next", width < WIDTH ? 90 : 120, 12)}`, { size: 12, fill: colour.panelMuted, anchor: "end" }));
    else body.push(text(width - PAD, y + 15, "Too few to measure", { size: 12, fill: colour.panelMuted, anchor: "end" }));
  });
  return frame(id, title, "Deals reaching each stage, and the share that reached the next stage.", 20 + steps.length * rowHeight + 10, body.join(""), width);
}

/** A gap in the line is a month we couldn't measure, never a zero. */
export function pipelineOverTime(metric: Metric | undefined, currency: string): Chart {
  const id = "pipeline-over-time";
  const title = "Pipeline over time";
  const reason = missing(metric);
  if (reason) return gap(id, title, reason);
  if (metric!.status !== "measured") return gap(id, title, "Not measured.");
  const series = metric!.series;
  const points = series.filter((p) => p.value !== null);
  if (points.length < 2) return gap(id, title, "There aren't enough measured months to draw a line.");

  const height = 300;
  const [left, right, top, bottom] = [PAD + 54, WIDTH - PAD - 70, 28, height - 44];
  const ceiling = niceCeiling(Math.max(...points.map((p) => p.value!)));
  const x = (i: number): number => left + (i / Math.max(series.length - 1, 1)) * (right - left);
  const y = (value: number): number => bottom - (value / ceiling) * (bottom - top);
  const body: string[] = [];
  for (let t = 0; t <= 4; t++) {
    const value = (ceiling / 4) * t;
    body.push(`<line x1="${left}" x2="${right}" y1="${y(value)}" y2="${y(value)}" stroke="${colour.panelRule}" stroke-width="1"/>`);
    body.push(text(left - 8, y(value) + 4, compactMoney(value, currency), { size: 11, fill: colour.panelMuted, anchor: "end", cls: "tick" }));
  }
  let path = "";
  series.forEach((point, i) => {
    if (point.value === null) return;
    const joined = i > 0 && series[i - 1]!.value !== null;
    path += `${joined ? "L" : "M"}${x(i).toFixed(1)} ${y(point.value).toFixed(1)}`;
  });
  body.push(`<path d="${path}" fill="none" stroke="${colour.chartreuse}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`);
  const lastIndex = series.findLastIndex((p) => p.value !== null);
  const last = series[lastIndex]!;
  body.push(`<circle cx="${x(lastIndex)}" cy="${y(last.value!)}" r="5" fill="${colour.chartreuse}" stroke="${colour.carbon}" stroke-width="2"/>`);
  body.push(text(x(lastIndex) + 10, y(last.value!) + 4, compactMoney(last.value!, currency), { size: 13, weight: font.weights.medium }));
  const firstIndex = series.findIndex((p) => p.value !== null);
  body.push(text(x(firstIndex), bottom + 22, monthYear(series[firstIndex]!.date), { size: 11, fill: colour.panelMuted, cls: "tick" }));
  body.push(text(x(series.length - 1), bottom + 22, monthYear(series[series.length - 1]!.date), { size: 11, fill: colour.panelMuted, anchor: "end", cls: "tick" }));
  const skipped = series.filter((p) => p.value === null).length;
  if (skipped > 0) body.push(text(left, height - 8, "Gaps are months we couldn't measure.", { size: 11, fill: colour.panelMuted, cls: "tick" }));
  return frame(id, title, `Open pipeline at each month end, ending at ${compactMoney(last.value!, currency)}.`, height, body.join(""));
}

function niceCeiling(max: number): number {
  const magnitude = 10 ** Math.floor(Math.log10(max || 1));
  for (const step of [1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10]) if (step * magnitude >= max) return step * magnitude;
  return 10 * magnitude;
}

/** Weights always, and the last check's scores on a re-check. */
export const scorecardOptions = (report: { progress: Progress | null }): ScorecardOptions => ({
  weightOf: areaWeight,
  ...(report.progress ? { before: new Map(report.progress.areas.map((a) => [a.domain, a.before])) } : {}),
});

export type ScorecardOptions = {
  /** Each area's weight in the overall score. With it, rows are ranked by how much each holds the overall score back. */
  weightOf?: (domain: string) => number;
  /** On a re-check, each area's score last time, drawn as a mark on its bar. */
  before?: Map<string, number | null>;
};

/** The scorecard's order: the area that holds the overall score back most first, its weight times its shortfall from 100, and areas with no score last. */
export function rankScorecard<T extends { domain: string; score: number | null }>(rows: T[], weightOf: (domain: string) => number): T[] {
  return [...rows].sort((a, b) => (a.score === null ? 1 : 0) - (b.score === null ? 1 : 0) || (b.score === null || a.score === null ? 0 : weightOf(b.domain) * (100 - b.score) - weightOf(a.domain) * (100 - a.score)));
}

/**
 * One bar per area. With weights, the area that holds the overall score back
 * most comes first: its weight times its shortfall from 100. A domain with no
 * score shows why, last, and is never drawn as a zero.
 */
export function scorecardChart(rows: ScorecardRow[], options: ScorecardOptions = {}): Chart {
  const id = "scorecard";
  const title = "Scorecard";
  if (rows.length === 0) return gap(id, title, "No domains were assessed.");
  const { weightOf, before } = options;
  const ranked = weightOf ? rankScorecard(rows, weightOf) : rows;
  // The weight sits in its own column between the name and the bar.
  const weightColumn = weightOf ? 54 : 0;
  const labelWidth = 200;
  const left = PAD + labelWidth + weightColumn;
  const trackWidth = WIDTH - left - PAD - 170;
  // With the caption row, rows tighten so the chart is no taller than it was without it.
  // Past thirteen areas (integrations, technical health, Service Cloud), rows
  // tighten so the chart is never taller than thirteen rows were.
  const fullRow = weightOf ? 32 : 34;
  const rowHeight = Math.min(fullRow, Math.floor((fullRow * 13) / ranked.length));
  const top = weightOf ? 36 : 18;
  const body: string[] = weightOf
    ? [text(PAD, 22, before ? "Ordered by how much each area holds your score back. A white mark is the score last time." : "Ordered by how much each area holds your score back.", { size: 12, fill: colour.panelMuted }), text(PAD + labelWidth + weightColumn - 14, 22, "Weight", { size: 11, fill: colour.panelMuted, anchor: "end" })]
    : [];
  ranked.forEach((row, i) => {
    const y = top + i * rowHeight;
    body.push(text(PAD, y + 15, clip(row.label, labelWidth - 12, 13), { size: 13 }));
    if (weightOf) body.push(text(PAD + labelWidth + weightColumn - 14, y + 15, String(weightOf(row.domain)), { size: 12, fill: colour.panelMuted, anchor: "end" }));
    if (row.score === null) {
      body.push(text(left, y + 15, clip(row.note, trackWidth + 40, 12), { size: 12, fill: colour.panelMuted }));
    } else {
      const was = before?.get(row.domain) ?? null;
      body.push(bar(left, y + 2, (row.score / 100) * trackWidth, BAR, colour.chartreuse));
      if (was !== null) body.push(`<line x1="${left + (was / 100) * trackWidth}" x2="${left + (was / 100) * trackWidth}" y1="${y - 2}" y2="${y + BAR + 6}" stroke="${colour.ivory}" stroke-width="2"/>`);
      const end = left + (Math.max(row.score, was ?? 0) / 100) * trackWidth + 8;
      body.push(text(end, y + 15, was !== null && was !== row.score ? `${row.score}, was ${was}` : `${row.score}`, { size: 13, weight: font.weights.medium }));
    }
    // "0 issues" would read as a clean result for a domain nobody looked at.
    const count = row.assessed ? `${row.issues} ${row.issues === 1 ? "issue" : "issues"}` : "";
    if (count) body.push(text(WIDTH - PAD - (row.high > 0 ? 62 : 0), y + 15, count, { size: 12, fill: colour.panelMuted, anchor: "end" }));
    if (row.high > 0) {
      body.push(`<circle cx="${WIDTH - PAD - 52}" cy="${y + 11}" r="4" fill="${colour.alert}"/>`);
      body.push(text(WIDTH - PAD, y + 15, `${row.high} high`, { size: 12, anchor: "end" }));
    }
  });
  return frame(id, title, weightOf ? "Score and issue count for each area, the one holding the overall score back most first." : "Score and issue count for each domain.", top + ranked.length * rowHeight + 8, body.join(""));
}

// The pipeline deep dive. Each chart reads one or two metrics, and every figure
// it prints is a value, count or date those metrics hold.

/** Half a slide, so two charts can sit side by side at a readable size. */
export const HALF_WIDTH = 560;

type Measured = Extract<Metric, { status: "measured" }>;
type BarRow = { label: string; value: number | null; text: string };

const measuredOr = (id: string, title: string, metric: Metric | undefined): Measured | Chart => {
  const reason = missing(metric);
  return reason ? gap(id, title, reason) : (metric as Measured);
};
const isChart = (x: Measured | Chart): x is Chart => "kind" in x;
const segmentsOf = (metric: Measured, dimension: string): MetricSegment[] => metric.segments.filter((s) => s.dimension === dimension);

/** Horizontal bars with the figure written at the end of each. A row with no value says why instead. */
export function barChart(id: string, title: string, rows: BarRow[], options: { alt: string; width?: number; caption?: string; labelWidth?: number; textWidth?: number }): Chart {
  if (rows.length === 0) return gap(id, title, "There's nothing to show.");
  const width = options.width ?? WIDTH;
  const labelWidth = options.labelWidth ?? 170;
  const left = PAD + labelWidth;
  const trackWidth = width - left - PAD - (options.textWidth ?? 150);
  const largest = Math.max(...rows.map((r) => r.value ?? 0), 0);
  const top = options.caption ? 40 : 18;
  const rowHeight = 32;
  const body: string[] = options.caption ? [text(PAD, 26, options.caption, { size: 13, fill: colour.panelMuted })] : [];
  rows.forEach((row, i) => {
    const y = top + i * rowHeight;
    body.push(text(PAD, y + 15, clip(row.label, labelWidth - 12, 13), { size: 13 }));
    if (row.value === null) {
      body.push(text(left, y + 15, row.text, { size: 12, fill: colour.panelMuted }));
      return;
    }
    const length = largest > 0 ? (Math.max(row.value, 0) / largest) * trackWidth : 0;
    body.push(bar(left, y + 2, length, BAR, colour.chartreuse));
    body.push(text(left + length + 8, y + 15, row.text, { size: 13, weight: font.weights.medium }));
  });
  return frame(id, title, options.alt, top + rows.length * rowHeight + 8, body.join(""), width);
}

type Column = { label: string; value: number | null; text: string; sub?: string };

/** Vertical bars along time. A gap is a period we couldn't measure, never a zero. */
export function columnChart(id: string, title: string, columns: Column[], options: { alt: string; width?: number; height?: number; caption?: string }): Chart {
  if (columns.length === 0) return gap(id, title, "There's nothing to show.");
  const width = options.width ?? WIDTH;
  const height = options.height ?? 280;
  const top = options.caption ? 58 : 36;
  const bottom = height - (columns.some((c) => c.sub) ? 52 : 34);
  const slot = (width - 2 * PAD) / columns.length;
  const barWidth = Math.min(64, slot * 0.6);
  const largest = Math.max(...columns.map((c) => c.value ?? 0), 0);
  const body: string[] = options.caption ? [text(PAD, 26, options.caption, { size: 13, fill: colour.panelMuted })] : [];
  body.push(`<line x1="${PAD}" x2="${width - PAD}" y1="${bottom}" y2="${bottom}" stroke="${colour.panelRule}" stroke-width="1"/>`);
  columns.forEach((column, i) => {
    const centre = PAD + slot * i + slot / 2;
    body.push(text(centre, bottom + 18, column.label, { size: 12, fill: colour.panelMuted, anchor: "middle" }));
    if (column.sub) body.push(text(centre, bottom + 36, column.sub, { size: 12, fill: colour.panelMuted, anchor: "middle" }));
    if (column.value === null) {
      body.push(text(centre, bottom - 8, column.text, { size: 11, fill: colour.panelMuted, anchor: "middle" }));
      return;
    }
    const h = Math.max(largest > 0 ? (Math.max(column.value, 0) / largest) * (bottom - top) : 0, 1);
    const r = Math.min(radius.small, barWidth / 2, h);
    const x = centre - barWidth / 2;
    body.push(`<path d="M${x} ${bottom}v${-(h - r)}a${r} ${r} 0 0 1 ${r} ${-r}h${barWidth - 2 * r}a${r} ${r} 0 0 1 ${r} ${r}v${h - r}z" fill="${colour.chartreuse}"/>`);
    body.push(text(centre, bottom - h - 8, column.text, { size: 13, weight: font.weights.medium, anchor: "middle" }));
  });
  return frame(id, title, options.alt, height, body.join(""), width);
}

type Side = { value: number | null; text: string };
type PairedRow = { label: string; a: Side; b: Side };

/** Two bars per row, each written with its series name, so colour is never the only key. */
export function pairedBarChart(id: string, title: string, rows: PairedRow[], legend: [string, string], options: { alt: string; width?: number }): Chart {
  if (rows.length === 0) return gap(id, title, "There's nothing to show.");
  const width = options.width ?? WIDTH;
  const labelWidth = 130;
  const left = PAD + labelWidth;
  const trackWidth = width - left - PAD - 190;
  const largest = Math.max(...rows.flatMap((r) => [r.a.value ?? 0, r.b.value ?? 0]), 0);
  const top = 40;
  const thin = 12;
  const rowHeight = 2 * thin + 18;
  const second = PAD + 34 + legend[0].length * 8;
  const body: string[] = [bar(PAD, 16, 14, 12, colour.chartreuse), text(PAD + 20, 27, legend[0], { size: 12 }), bar(second, 16, 14, 12, colour.panelMuted), text(second + 20, 27, legend[1], { size: 12 })];
  rows.forEach((row, i) => {
    const y = top + i * rowHeight;
    body.push(text(PAD, y + thin + 5, clip(row.label, labelWidth - 12, 13), { size: 13 }));
    const sides: Array<[Side, string, string, number]> = [[row.a, colour.chartreuse, legend[0], 0], [row.b, colour.panelMuted, legend[1], thin + 2]];
    for (const [side, fill, name, offset] of sides) {
      if (side.value === null) {
        body.push(text(left, y + offset + 10, `${name}: ${side.text}`, { size: 11, fill: colour.panelMuted }));
        continue;
      }
      const length = largest > 0 ? (Math.max(side.value, 0) / largest) * trackWidth : 0;
      body.push(bar(left, y + offset, length, thin, fill));
      body.push(text(left + length + 6, y + offset + 10, `${name}: ${side.text}`, { size: 11 }));
    }
  });
  return frame(id, title, options.alt, top + rows.length * rowHeight + 4, body.join(""), width);
}

const dealCount = (n: number): string => `${n} ${n === 1 ? "deal" : "deals"}`;

/** Expected won value by month, with the expected number of deals under each month. */
export function deliveryOutlookChart(value: Metric | undefined, count: Metric | undefined, currency: string, width = WIDTH): Chart {
  const id = "delivery-outlook";
  const title = "Delivery outlook";
  const m = measuredOr(id, title, value);
  if (isChart(m)) return m;
  const expected = new Map(count?.status === "measured" ? count.series.map((p) => [p.date, p.value]) : []);
  const columns = m.series.map((p) => {
    const n = expected.get(p.date);
    // Deals are whole (DECISIONS.md 404), so a month with some value can round to none.
    const sub = typeof n !== "number" ? null : n === 0 && (p.value ?? 0) > 0 ? "under 1 deal" : dealCount(n);
    return { label: monthYear(p.date), value: p.value, text: p.value === null ? "Not measured" : compactMoney(p.value, currency), ...(sub ? { sub } : {}) };
  });
  return columnChart(id, title, columns, { alt: `Expected won value by month, ${compactMoney(m.value, currency)} in all.`, width });
}

/** New and repeat customers' part of what was won. */
export function winsByCustomerChart(metric: Metric | undefined, currency: string): Chart {
  const id = "wins-by-customer";
  const title = "New and repeat customers";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const rows = segmentsOf(m, "customer").map((s) => ({ label: s.key, value: s.value, text: `${compactMoney(s.value ?? 0, currency)} from ${s.n} wins` }));
  return barChart(id, title, rows, { alt: "Won value from new and repeat customers.", width: HALF_WIDTH, labelWidth: 150, textWidth: 160, caption: "Won value" });
}

/** How much of won value the largest accounts hold. Accounts are counted, never named. */
export function topAccountsChart(metric: Metric | undefined): Chart {
  const id = "top-accounts";
  const title = "Top accounts";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const rows = segmentsOf(m, "accounts").map((s) => ({ label: s.key, value: s.value, text: s.value === null ? "Not measured" : percent(s.value) }));
  return barChart(id, title, rows, { alt: "The largest accounts' share of won value.", width: HALF_WIDTH, labelWidth: 150, textWidth: 70, caption: "Share of won value" });
}

/** Won value by one of the deal's own fields, largest first. */
const WON_VALUE_ROWS = 5;

export function wonValueByChart(metric: Metric | undefined, dimension: string, id: string, title: string, currency: string): Chart {
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  // A real org had more lead sources than a half-width chart can hold. The rest
  // are in the raw notes, and summing them here would be a figure we didn't compute.
  const all = segmentsOf(m, dimension).sort((a, b) => (b.value ?? 0) - (a.value ?? 0));
  const rows = all.slice(0, WON_VALUE_ROWS).map((s) => ({ label: s.key, value: s.value, text: `${compactMoney(s.value ?? 0, currency)}, ${s.n} wins` }));
  const caption = all.length > WON_VALUE_ROWS ? `Won value by ${dimension}, largest first. The raw notes list the rest.` : `Won value by ${dimension}`;
  return barChart(id, title, rows, { alt: `Won value by ${dimension}.`, width: HALF_WIDTH, labelWidth: 150, textWidth: 130, caption });
}

/** Lost and no decision deals by the stage they left from, with the value that went with them. */
export function lostByStageChart(count: Metric | undefined, value: Metric | undefined, currency: string): Chart {
  const id = "lost-by-stage";
  const title = "Where deals are lost";
  const m = measuredOr(id, title, count);
  if (isChart(m)) return m;
  const amounts = new Map(value?.status === "measured" ? segmentsOf(value, "stage left").map((s) => [s.key, s.value]) : []);
  const rows = segmentsOf(m, "stage left").map((s) => {
    const amount = amounts.get(s.key);
    return { label: s.key, value: s.value, text: typeof amount === "number" ? `${dealCount(s.n)}, ${compactMoney(amount, currency)}` : dealCount(s.n) };
  });
  return barChart(id, title, rows, { alt: "Lost and no decision deals by the last open stage they were in.", width: HALF_WIDTH, labelWidth: 130, textWidth: 140, caption: "Last open stage before closing" });
}

/** How deals were closed when they didn't win: by closing stage where there's more than one, otherwise by outcome. */
export function closedAsChart(count: Metric | undefined): Chart {
  const id = "closed-as";
  const title = "How they were closed";
  const m = measuredOr(id, title, count);
  if (isChart(m)) return m;
  const byStage = segmentsOf(m, "closed as");
  const rows = (byStage.length ? byStage : segmentsOf(m, "outcome")).map((s) => ({ label: s.key, value: s.value, text: dealCount(s.n) }));
  return barChart(id, title, rows, { alt: "Lost and no decision deals by how they were closed.", width: HALF_WIDTH, labelWidth: 190, textWidth: 80, caption: byStage.length ? "Closing stage" : "Outcome" });
}

/** Median days in each open stage, won against lost, in sales order. */
export function stageVelocityChart(won: Metric | undefined, lost: Metric | undefined, order: string[]): Chart {
  const id = "stage-velocity";
  const title = "Time in each stage";
  const w = measuredOr(id, title, won);
  if (isChart(w)) return w;
  const l = lost?.status === "measured" ? lost : null;
  const find = (m: Measured | null, stage: string): MetricSegment | undefined => (m ? segmentsOf(m, "stage").find((s) => s.key === stage) : undefined);
  const side = (s: MetricSegment | undefined): Side => (s && s.value !== null ? { value: s.value, text: `${s.value} days` } : { value: null, text: s ? `too few to measure (${s.n})` : "none" });
  const stages = order.filter((stage) => (find(w, stage)?.n ?? 0) > 0 || (find(l, stage)?.n ?? 0) > 0);
  const rows = stages.map((stage) => ({ label: stage, a: side(find(w, stage)), b: side(find(l, stage)) }));
  return pairedBarChart(id, title, rows, ["Won", "Lost or no decision"], { alt: "Median days in each open stage for won deals against lost and no decision deals.", width: HALF_WIDTH });
}

// Adoption (roadmap 3.4). Every figure is the adoption metrics' own: a month's
// count, a channel's share, or the Lightning metric's stats.

/** "profile Standard User" as a reader would say it: "Standard User profile". */
const groupName = (group: string): string => {
  const [kind, ...rest] = group.split(" ");
  return kind === "profile" || kind === "role" ? `${rest.join(" ")} ${kind}` : group;
};

/** Monthly active users, with the group that fell furthest from its peak as a second, named line. */
export function activeUsersChart(metric: Metric | undefined, width = HALF_WIDTH): Chart {
  const id = "active-users";
  const title = "Monthly active users";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const series = m.series.filter((p) => p.value !== null);
  if (series.length < 2) return gap(id, title, "The login history doesn't cover enough whole months to draw a line.");
  const fell = m.segments.filter((s) => s.dimension.startsWith(FELL_MOST));
  const group = fell[0]?.dimension.slice(FELL_MOST.length) ?? null;
  const height = 290;
  const [left, right, top, bottom] = [PAD + 30, width - PAD - 150, 30, height - 44];
  const ceiling = niceCeiling(Math.max(...series.map((p) => p.value!), ...fell.map((s) => s.value ?? 0)));
  const x = (i: number): number => left + (i / Math.max(series.length - 1, 1)) * (right - left);
  const y = (value: number): number => bottom - (value / ceiling) * (bottom - top);
  const body: string[] = [];
  for (let t = 0; t <= 4; t++) {
    const value = Math.round((ceiling / 4) * t);
    body.push(`<line x1="${left}" x2="${right}" y1="${y(value)}" y2="${y(value)}" stroke="${colour.panelRule}" stroke-width="1"/>`);
    body.push(text(left - 8, y(value) + 4, String(value), { size: 11, fill: colour.panelMuted, anchor: "end", cls: "tick" }));
  }
  const line = (values: number[], stroke: string, dashed: boolean): string =>
    `<path d="${values.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`).join("")}" fill="none" stroke="${stroke}" stroke-width="${dashed ? 2 : 2.5}"${dashed ? ' stroke-dasharray="6 4"' : ""} stroke-linejoin="round" stroke-linecap="round"/>`;
  const all = series.map((p) => p.value!);
  body.push(line(all, colour.chartreuse, false));
  body.push(text(right + 10, y(all.at(-1)!) + 4, `Everyone: ${all.at(-1)}`, { size: 12, weight: font.weights.medium }));
  if (group && fell.length === series.length) {
    const values = fell.map((s) => s.value ?? 0);
    const peak = Math.max(...values.slice(0, -1));
    body.push(line(values, colour.ivory, true));
    const labelY = Math.abs(y(values.at(-1)!) - y(all.at(-1)!)) < 16 ? y(values.at(-1)!) + 18 : y(values.at(-1)!) + 4;
    const name = wrap(groupName(group), 20).slice(0, 3);
    name.forEach((part, i) => body.push(text(right + 10, labelY + i * 15, part, { size: 12 })));
    body.push(text(right + 10, labelY + name.length * 15, `${peak} at its peak, ${values.at(-1)} now`, { size: 11, fill: colour.panelMuted }));
  }
  body.push(text(x(0), bottom + 22, monthYear(series[0]!.date), { size: 11, fill: colour.panelMuted, cls: "tick" }));
  body.push(text(x(series.length - 1), bottom + 22, monthYear(series.at(-1)!.date), { size: 11, fill: colour.panelMuted, anchor: "end", cls: "tick" }));
  if (group) body.push(text(PAD, height - 8, "The dashed line is the group that fell furthest from its peak.", { size: 11, fill: colour.panelMuted, cls: "tick" }));
  return frame(id, title, `People who logged in each month, ${all.at(-1)} in the latest.${group ? ` The ${groupName(group)} fell furthest.` : ""}`, height, body.join(""), width);
}

const SHARE_FILLS = [colour.chartreuse, colour.ivory, colour.panelMuted, colour.snow, colour.panelRule];

/** One bar of a whole, split into its parts, each named with its share below, so colour is never the only key. */
function shareBar(id: string, title: string, parts: Array<{ label: string; share: number; text: string }>, options: { alt: string; caption: string; width?: number }): Chart {
  const shown = parts.filter((p) => p.share > 0);
  if (shown.length === 0) return gap(id, title, "There's nothing to show.");
  const width = options.width ?? HALF_WIDTH;
  const track = width - 2 * PAD;
  const total = shown.reduce((sum, p) => sum + p.share, 0);
  const body: string[] = [text(PAD, 26, options.caption, { size: 13, fill: colour.panelMuted })];
  let at = PAD;
  shown.forEach((p, i) => {
    const w = (p.share / total) * track;
    body.push(`<rect x="${at.toFixed(1)}" y="40" width="${Math.max(w, 1).toFixed(1)}" height="22" fill="${SHARE_FILLS[i % SHARE_FILLS.length]}"/>`);
    at += w;
  });
  const columns = 2;
  shown.forEach((p, i) => {
    const cx = PAD + (i % columns) * (track / columns);
    const cy = 86 + Math.floor(i / columns) * 22;
    body.push(`<rect x="${cx}" y="${cy - 10}" width="12" height="12" fill="${SHARE_FILLS[i % SHARE_FILLS.length]}"/>`);
    body.push(text(cx + 18, cy, clip(`${p.label}: ${p.text}`, track / columns - 24, 12), { size: 12 }));
  });
  return frame(id, title, options.alt, 86 + Math.ceil(shown.length / columns) * 22, body.join(""), width);
}

const CHANNEL_LABELS: Record<string, string> = { browser: "Browser", "mobile app": "Mobile app", "single sign-on": "Single sign-on", api: "API", other: "Other" };

/** How people signed in, as shares of successful logins. */
export function loginChannelsChart(metric: Metric | undefined, width = HALF_WIDTH): Chart {
  const id = "login-channels";
  const title = "How people sign in";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const parts = segmentsOf(m, "channel").map((s) => ({ label: CHANNEL_LABELS[s.key] ?? s.key, share: s.value ?? 0, text: percent(s.value ?? 0) }));
  return shareBar(id, title, parts, { alt: "Successful logins by how people signed in.", caption: `Logins by channel, ${m.n} in all`, width });
}

/** Lightning Experience against Classic, by active users. */
export function lightningChart(metric: Metric | undefined, width = HALF_WIDTH): Chart {
  const id = "lightning";
  const title = "Lightning and Classic";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const { lightning, classic, classicShare } = m.stats;
  if (lightning === undefined || classic === undefined || classicShare === undefined) return gap(id, title, "How many users are on each wasn't recorded in this run.");
  const users = (n: number) => `${n} ${n === 1 ? "user" : "users"}`;
  const parts = [
    { label: "Lightning", share: m.value, text: `${percent(m.value)}, ${users(lightning)}` },
    { label: "Classic", share: classicShare, text: `${percent(classicShare)}, ${users(classic)}` },
  ];
  return shareBar(id, title, parts, { alt: `${percent(m.value)} of active users are on Lightning Experience.`, caption: "Active users by the interface they're set to", width });
}

// The money (roadmap 3.4): each kind building to the total. Exposure is kept
// apart, in its own box at no scale, because it's never part of the total.

// Short enough to sit under a bar at half a slide. The table beside it has the full names.
const SHORT_KIND_LABELS: Partial<Record<keyof typeof KIND_LABELS, string>> = {
  revenue_recoverable: "Recoverable revenue",
  conversion_uplift: "Closing the gap",
  licence_waste: "Unused licences",
  time_cost: "Manual work",
};

/** A waterfall of the expected value by kind, then the total, with the largest exposure beside it and never stacked. */
/** Kinds smaller than this stay in the money table but get no bar: a sliver beside six figures is noise (DECISIONS.md 402). */
export const WATERFALL_MIN = 1000;

export function waterfallChart(value: ValueModel, width = WIDTH, height = 320): Chart {
  const id = "value-waterfall";
  const title = "What fixing this is worth";
  const kinds = HEADLINE_KINDS.filter((k) => value.byKind[k].expected >= WATERFALL_MIN);
  if (value.total.expected <= 0 || kinds.length === 0) return gap(id, title, "Nothing could be given a figure in money.");
  const m = (n: number) => compactMoney(n, value.currency);
  const exposure = value.byKind.exposure.expected > 0 ? value.byKind.exposure.expected : null;
  const box = width < WIDTH ? 140 : 170;
  const plotRight = exposure ? width - PAD - box - 20 : width - PAD;
  // Room under the bars for a kind's name over four short lines.
  const [top, bottom] = [36, height - 78];
  const columns = [...kinds.map((k) => ({ label: SHORT_KIND_LABELS[k] ?? KIND_LABELS[k], amount: value.byKind[k].expected })), { label: "Total", amount: value.total.expected }];
  const slot = (plotRight - PAD) / columns.length;
  const barWidth = Math.min(90, slot * 0.62);
  const y = (n: number): number => bottom - (n / value.total.expected) * (bottom - top);
  const body: string[] = [`<line x1="${PAD}" x2="${plotRight}" y1="${bottom}" y2="${bottom}" stroke="${colour.panelRule}" stroke-width="1"/>`];
  let running = 0;
  columns.forEach((c, i) => {
    const isTotal = i === columns.length - 1;
    const from = isTotal ? 0 : running;
    const to = isTotal ? c.amount : running + c.amount;
    const centre = PAD + slot * i + slot / 2;
    const h = Math.max(y(from) - y(to), 1.5);
    body.push(`<rect x="${(centre - barWidth / 2).toFixed(1)}" y="${y(to).toFixed(1)}" width="${barWidth.toFixed(1)}" height="${h.toFixed(1)}" rx="${radius.small}" fill="${isTotal ? colour.ivory : colour.chartreuse}"/>`);
    // A step joins the top of one bar to the bottom of the next, so the build is read left to right.
    if (!isTotal) body.push(`<line x1="${(centre + barWidth / 2).toFixed(1)}" x2="${(centre + slot - barWidth / 2).toFixed(1)}" y1="${y(to).toFixed(1)}" y2="${y(to).toFixed(1)}" stroke="${colour.panelMuted}" stroke-width="1" stroke-dasharray="3 3"/>`);
    body.push(text(centre, y(to) - 8, m(c.amount), { size: 13, weight: font.weights.medium, anchor: "middle" }));
    for (const [n, part] of wrap(c.label, Math.floor(slot / 7)).entries()) body.push(text(centre, bottom + 18 + n * 14, part, { size: 11.5, fill: colour.panelMuted, anchor: "middle" }));
    if (!isTotal) running = to;
  });
  if (exposure) {
    const bx = width - PAD - box;
    body.push(`<rect x="${bx}" y="${top}" width="${box}" height="${bottom - top}" rx="${radius.medium}" fill="none" stroke="${colour.panelMuted}" stroke-width="1.2" stroke-dasharray="5 4"/>`);
    body.push(text(bx + 14, top + 26, "Kept apart", { size: 12, fill: colour.panelMuted }));
    body.push(text(bx + 14, top + 52, m(exposure), { size: 18, weight: font.weights.semibold }));
    for (const [n, part] of wrap("At risk, not money found, so never added to the total.", box < 170 ? 19 : 24).entries()) body.push(text(bx + 14, top + 78 + n * 15, part, { size: 11.5, fill: colour.panelMuted }));
  }
  return frame(id, title, `Each kind of value building to ${m(value.total.expected)} expected over 12 months${exposure ? `, with ${m(exposure)} of exposure kept apart` : ""}.`, height, body.join(""), width);
}

/** Words split into lines of about `chars` characters. */
function wrap(value: string, chars: number): string[] {
  const lines: string[] = [];
  for (const word of value.split(" ")) {
    const last = lines.at(-1);
    if (last && `${last} ${word}`.length <= chars) lines[lines.length - 1] = `${last} ${word}`;
    else lines.push(word);
  }
  return lines;
}

// The sales process as a flow (roadmap 3.4). Every count printed is one cell of
// the transitions metric. Moves into a lost stage are drawn, not counted, because
// adding cells together would be a figure the engine didn't work out.

type FlowStage = { name: string; category: "open" | "won" | "lost" };
/** A stage carrying less than this share of the busiest stage's deals is left off the flow. */
const RARE_STAGE = 0.1;

/**
 * Open stages left to right in sales order, then Won. A move to the next stage
 * is a chartreuse arrow, its width by deals. A skip arcs above and a move back
 * arcs below, both in ivory, a move back dashed. A move into a lost or no
 * decision stage drops to the bar along the bottom.
 */
export function processFlowChart(metric: Metric | undefined, stages: FlowStage[]): Chart {
  const id = "process-flow";
  const title = "How deals move";
  const m = measuredOr(id, title, metric);
  if (isChart(m)) return m;
  const kind = new Map(stages.map((s) => [s.name, s.category]));
  const cells = m.segments
    .filter((s) => s.dimension.startsWith("from ") && s.value !== null && s.value > 0)
    .map((s) => ({ from: s.dimension.slice("from ".length), to: s.key, n: s.value! }))
    .filter((c) => kind.get(c.from) === "open" && kind.has(c.to));
  if (cells.length === 0) return gap(id, title, "No moves between stages were recorded.");
  // A stage few deals pass through, such as a retired one, would turn the main
  // flow around it into a "skip". It's left out, named, and in the raw notes.
  const through = (stage: string): number => Math.max(cells.filter((c) => c.to === stage).reduce((sum, c) => sum + c.n, 0), cells.filter((c) => c.from === stage).reduce((sum, c) => sum + c.n, 0));
  const used = stages.filter((s) => s.category === "open" && through(s.name) > 0).map((s) => s.name);
  const busiest = Math.max(...used.map(through));
  const open = used.filter((stage) => through(stage) >= busiest * RARE_STAGE);
  const rare = used.filter((stage) => !open.includes(stage));
  const nodes = [...open, "Won"];
  const width = WIDTH;
  const height = 330;
  const row = 150;
  const slot = (width - 2 * PAD) / nodes.length;
  const boxW = Math.min(104, slot - 36);
  const boxH = 40;
  const cx = (name: string): number => PAD + slot * nodes.indexOf(name) + slot / 2;
  const nodeOf = (stage: string): string | null => (kind.get(stage) === "won" ? "Won" : open.includes(stage) ? stage : null);
  const most = Math.max(...cells.map((c) => c.n));
  const thickness = (n: number): number => 1.5 + (n / most) * 14;
  const lostY = height - 34;
  const body: string[] = [];
  const arrowHead = (x: number, y: number, dir: 1 | -1, fill: string): string => `<path d="M${x} ${y}l${-8 * dir} -5v10z" fill="${fill}"/>`;
  // Drops into lost and no decision first, so arrows sit on top of them.
  const drops = new Map<string, number>();
  for (const c of cells) if (kind.get(c.to) === "lost") drops.set(c.from, (drops.get(c.from) ?? 0) + c.n);
  body.push(`<rect x="${PAD}" y="${lostY}" width="${width - 2 * PAD - slot}" height="8" rx="4" fill="${colour.panelRule}"/>`);
  body.push(text(PAD, lostY + 26, "Lost or no decision", { size: 11.5, fill: colour.panelMuted }));
  for (const [from, n] of drops) if (open.includes(from)) body.push(`<line x1="${cx(from)}" x2="${cx(from)}" y1="${row + boxH / 2}" y2="${lostY}" stroke="${colour.panelMuted}" stroke-width="${thickness(n).toFixed(1)}" stroke-opacity="0.55"/>`);
  // Each move back leaves and arrives at its own point along the box, wider
  // arcs outside narrower ones, so none share a start or lie on another.
  const backs = cells
    .map((c) => ({ c, to: nodeOf(c.to) }))
    .filter((x): x is { c: (typeof cells)[number]; to: string } => x.to !== null && open.includes(x.c.from) && nodes.indexOf(x.to) < nodes.indexOf(x.c.from));
  const ends = new Map<string, Array<{ key: string; order: number }>>();
  for (const { c, to } of backs) {
    const span = nodes.indexOf(c.from) - nodes.indexOf(to);
    const key = `${c.from}>${c.to}`;
    // Where an arc arrives (its left end), the widest sits furthest left; where it leaves, furthest right.
    ends.set(to, [...(ends.get(to) ?? []), { key: `${key}:in`, order: -span }]);
    ends.set(c.from, [...(ends.get(c.from) ?? []), { key: `${key}:out`, order: span }]);
  }
  const offset = new Map<string, number>();
  for (const list of ends.values()) {
    list.sort((p, q) => p.order - q.order || p.key.localeCompare(q.key));
    const usable = boxW - 24;
    list.forEach((e, i) => offset.set(e.key, list.length === 1 ? 0 : -usable / 2 + (usable * i) / (list.length - 1)));
  }
  const pills: string[] = [];
  for (const c of cells) {
    const to = nodeOf(c.to);
    if (!to || to === c.from || !open.includes(c.from)) continue;
    const [a, b] = [nodes.indexOf(c.from), nodes.indexOf(to)];
    const w = thickness(c.n).toFixed(1);
    if (b === a + 1) {
      const x1 = cx(c.from) + boxW / 2;
      const x2 = cx(to) - boxW / 2 - 8;
      body.push(`<line x1="${x1}" x2="${x2}" y1="${row}" y2="${row}" stroke="${colour.chartreuse}" stroke-width="${w}"/>`, arrowHead(x2 + 8, row, 1, colour.chartreuse));
      body.push(text((x1 + x2) / 2, row - Number(w) / 2 - 7, String(c.n), { size: 11.5, anchor: "middle", weight: font.weights.medium }));
      continue;
    }
    // Skips arc above, moves back arc below, each as deep as the stages it spans.
    const back = b < a;
    const span = Math.abs(b - a);
    const yEdge = row + (back ? boxH / 2 : -boxH / 2);
    if (!back) {
      const lift = -Math.min(28 + span * 18, 100);
      const [x1, x2] = [cx(c.from), cx(to)];
      body.push(`<path d="M${x1} ${yEdge}Q${(x1 + x2) / 2} ${yEdge + lift * 2} ${x2} ${yEdge}" fill="none" stroke="${colour.ivory}" stroke-width="${w}"/>`);
      body.push(text((x1 + x2) / 2, yEdge + lift - (Number(w) / 2 + 5), String(c.n), { size: 11.5, anchor: "middle" }));
      continue;
    }
    // Deep enough to clear the arcs it spans, and never as low as the lost bar.
    const lift = Math.min(22 + span * 26, lostY - 20 - yEdge);
    const key = `${c.from}>${c.to}`;
    const x1 = cx(c.from) + (offset.get(`${key}:out`) ?? 0);
    const x2 = cx(to) + (offset.get(`${key}:in`) ?? 0);
    body.push(`<path d="M${x1.toFixed(1)} ${yEdge}Q${((x1 + x2) / 2).toFixed(1)} ${yEdge + lift * 2} ${x2.toFixed(1)} ${yEdge}" fill="none" stroke="${colour.ivory}" stroke-width="${w}" stroke-dasharray="6 4"/>`);
    // The count on a pill at the arc's lowest point, drawn over every line, so it never sits on one.
    const label = String(c.n);
    const pw = 12 + label.length * 7;
    pills.push(`<rect x="${((x1 + x2) / 2 - pw / 2).toFixed(1)}" y="${(yEdge + lift - 9).toFixed(1)}" width="${pw}" height="18" rx="9" fill="${colour.carbon}" stroke="${colour.panelMuted}" stroke-width="1"/>`, text((x1 + x2) / 2, yEdge + lift + 4, label, { size: 11.5, anchor: "middle" }));
  }
  body.push(...pills);
  for (const name of nodes) {
    const x = cx(name) - boxW / 2;
    const won = name === "Won";
    body.push(`<rect x="${x.toFixed(1)}" y="${row - boxH / 2}" width="${boxW.toFixed(1)}" height="${boxH}" rx="${radius.small}" fill="${won ? colour.chartreuse : colour.carbon}" stroke="${colour.ivory}" stroke-width="1.2"/>`);
    body.push(text(cx(name), row + 5, clip(name, boxW - 8, 12), { size: 12, weight: font.weights.semibold, anchor: "middle", fill: won ? colour.carbon : colour.ivory }));
  }
  body.push(text(PAD, 20, "Above: deals that skipped a stage. Below, dashed: deals that went back.", { size: 11.5, fill: colour.panelMuted }));
  if (rare.length) body.push(text(width - PAD, lostY + 26, clip(`Rarely used, left out: ${rare.join(", ")}. The raw notes have every move.`, width - 2 * PAD - 160, 11.5), { size: 11.5, fill: colour.panelMuted, anchor: "end" }));
  return frame(id, title, "How deals moved between stages, with skips above and moves back below.", height, body.join(""), width);
}
