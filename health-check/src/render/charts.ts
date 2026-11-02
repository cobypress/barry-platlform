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

function frame(id: string, title: string, alt: string, height: number, body: string): Chart {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${WIDTH} ${height}" role="img" aria-label="${escapeXml(alt)}" font-family="${escapeXml(font.stack)}" data-chart="${id}"><rect width="${WIDTH}" height="${height}" rx="${radius.medium}" fill="${colour.carbon}"/>${body}</svg>`;
  return { kind: "chart", id, title, alt, svg };
}

function clip(value: string, width: number, size: number): string {
  const max = Math.floor(width / (size * CHAR_WIDTH));
  return value.length <= max ? value : `${value.slice(0, Math.max(max - 1, 1)).trimEnd()}…`;
}

const gap = (id: string, title: string, reason: string): Chart => ({ kind: "gap", id, title, reason });

const missing = (metric: Metric | undefined): string | null => (!metric ? "This metric wasn't part of the run." : metric.status === "not_measurable" ? metric.reason : null);

/** Team and record type only: executive reports stay at team level. Rep segments stay in the raw notes. */
export function winRateBySegment(metric: Metric | undefined, dimensions: string[] = ["team", "record type"]): Chart {
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

  const labelWidth = 200;
  const left = PAD + labelWidth;
  const trackWidth = WIDTH - left - PAD - 90;
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
  return frame(id, title, alt, y + 14, body.join(""));
}

/** Bars show deals reaching each stage. The label says what share went on to the next. */
export function stageFunnel(metric: Metric | undefined): Chart {
  const id = "stage-funnel";
  const title = "Stage funnel";
  const reason = missing(metric);
  if (reason) return gap(id, title, reason);
  if (metric!.status !== "measured") return gap(id, title, "Not measured.");
  const steps = metric!.segments.filter((s) => s.dimension === "step");
  if (steps.length === 0) return gap(id, title, "No stage steps were measured.");

  const labelWidth = 170;
  const left = PAD + labelWidth;
  const trackWidth = WIDTH - left - PAD - 220;
  const largest = Math.max(...steps.map((s) => s.n), 1);
  const rowHeight = 36;
  const body: string[] = [];
  steps.forEach((step, i) => {
    const y = 20 + i * rowHeight;
    const [from, to] = step.key.split(" to ");
    const width = (step.n / largest) * trackWidth;
    body.push(text(PAD, y + 15, clip(from ?? step.key, labelWidth - 12, 13), { size: 13 }));
    body.push(bar(left, y + 2, width, BAR, colour.chartreuse));
    body.push(text(left + width + 8, y + 15, `${step.n}`, { size: 13, weight: font.weights.medium }));
    if (step.value !== null) body.push(text(WIDTH - PAD, y + 15, `${percent(step.value)} to ${clip(to ?? "next", 120, 12)}`, { size: 12, fill: colour.panelMuted, anchor: "end" }));
    else body.push(text(WIDTH - PAD, y + 15, "Too few to measure", { size: 12, fill: colour.panelMuted, anchor: "end" }));
  });
  return frame(id, title, "Deals reaching each stage, and the share that reached the next stage.", 20 + steps.length * rowHeight + 10, body.join(""));
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
