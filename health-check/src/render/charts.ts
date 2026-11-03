import { colour, font, radius } from "../design/tokens.ts";
import type { ScorecardRow } from "../narrative/select.ts";
import type { Metric, MetricSegment } from "../schemas/index.ts";
import { compactMoney, escapeXml, monthYear, percent, ukDates } from "./format.ts";

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
  return `<text x="${x}" y="${y}" font-size="${size}" font-weight="${weight}" fill="${fill}" text-anchor="${anchor}"${cls ? ` class="${cls}"` : ""}>${escapeXml(ukDates(value))}</text>`;
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

/** A domain with no score shows why. It's never drawn as a zero. */
export function scorecardChart(rows: ScorecardRow[]): Chart {
  const id = "scorecard";
  const title = "Scorecard";
  if (rows.length === 0) return gap(id, title, "No domains were assessed.");
  const labelWidth = 200;
  const left = PAD + labelWidth;
  const trackWidth = WIDTH - left - PAD - 170;
  const rowHeight = 34;
  const body: string[] = [];
  rows.forEach((row, i) => {
    const y = 18 + i * rowHeight;
    body.push(text(PAD, y + 15, clip(row.label, labelWidth - 12, 13), { size: 13 }));
    if (row.score === null) {
      body.push(text(left, y + 15, clip(row.note, trackWidth + 40, 12), { size: 12, fill: colour.panelMuted }));
    } else {
      body.push(bar(left, y + 2, (row.score / 100) * trackWidth, BAR, colour.chartreuse));
      body.push(text(left + (row.score / 100) * trackWidth + 8, y + 15, `${row.score}`, { size: 13, weight: font.weights.medium }));
    }
    // "0 issues" would read as a clean result for a domain nobody looked at.
    const count = row.assessed ? `${row.issues} ${row.issues === 1 ? "issue" : "issues"}` : "";
    if (count) body.push(text(WIDTH - PAD - (row.high > 0 ? 62 : 0), y + 15, count, { size: 12, fill: colour.panelMuted, anchor: "end" }));
    if (row.high > 0) {
      body.push(`<circle cx="${WIDTH - PAD - 52}" cy="${y + 11}" r="4" fill="${colour.alert}"/>`);
      body.push(text(WIDTH - PAD, y + 15, `${row.high} high`, { size: 12, anchor: "end" }));
    }
  });
  return frame(id, title, "Score and issue count for each domain.", 18 + rows.length * rowHeight + 8, body.join(""));
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
    return { label: monthYear(p.date), value: p.value, text: p.value === null ? "Not measured" : compactMoney(p.value, currency), ...(typeof n === "number" ? { sub: dealCount(n) } : {}) };
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
