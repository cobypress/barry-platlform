import { currentBody, TOP_ISSUE_COUNT, type BuiltOutput, type BuiltReport } from "../narrative/sections.ts";
import { buildPlan, domainLabel, domainsInOrder, issuesOf, scorecard, topIssues, verdictOf } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Finding, type Metric } from "../schemas/index.ts";
import { closedAsChart, deliveryOutlookChart, HALF_WIDTH, lostByStageChart, pipelineOverTime, scorecardChart, stageFunnel, stageVelocityChart, topAccountsChart, winRateBySegment, winsByCustomerChart, wonValueByChart } from "./charts.ts";
import { compactMoney, longDate, monthYear, percent } from "./format.ts";
import type { Calibration } from "../schemas/index.ts";
import { chartSlot, document, effortText, estimateLines, paginate, paragraphs, renderContext, severityChip, valueFigure, valueText, type Render } from "./html.ts";

// The full deck: 16:9, 40 to 60 slides. Issue slides flex to keep it in range,
// because a thin org and a messy one have very different numbers of findings.

export const DECK_RANGE = { min: 40, max: 60 } as const;

const CSS = `
.page { font-size: 20px; line-height: 1.45; }
.page p + p { margin-top: 10px; }
.footer { left: 0.55in; right: 0.55in; bottom: 0.24in; font-size: 12px; }
.cover { justify-content: space-between; }
.cover h1 { font-size: 68px; font-weight: 700; line-height: 1.1; margin-top: 14px; }
.cover .sub { font-size: 26px; color: var(--panelMuted); }
.divider { justify-content: center; }
.divider h1 { font-size: 56px; font-weight: 700; line-height: 1.1; margin: 10px 0 18px; }
.divider .body { max-width: 1000px; font-size: 22px; }
h2 { font-size: 36px; font-weight: 600; line-height: 1.2; margin: 4px 0 16px; }
.eyebrow { font-size: 15px; }
.stat b { font-size: 38px; line-height: 1.2; }
.stat span { font-size: 17px; }
.columns { display: grid; grid-template-columns: 1.5fr 1fr; gap: 32px; align-items: start; }
.panel { background: var(--snow); border-radius: var(--radius-m); padding: 16px 20px; font-size: 18px; }
.panel h3 { font-size: 14px; font-weight: 600; color: var(--muted); margin: 0 0 2px; }
.panel p + h3 { margin-top: 12px; }
.panel .note { font-size: 14.5px; margin-top: 2px; }
.card { background: var(--snow); border-radius: var(--radius-m); padding: 12px 18px; margin-bottom: 10px; font-size: 16.5px; line-height: 1.4; }
.card p + p { margin-top: 4px; }
.card h3 { font-size: 18px; font-weight: 600; margin: 4px 0; }
.two-up { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; }
.two-up .card { font-size: 13.5px; line-height: 1.38; padding: 9px 13px; margin-bottom: 8px; }
.two-up .card h3 { font-size: 14.5px; margin: 3px 0; }
.two-up .card .meta { font-size: 11.5px; }
.two-up.roomy .card { font-size: 16.5px; line-height: 1.42; padding: 12px 16px; margin-bottom: 12px; }
.two-up.roomy .card h3 { font-size: 18px; margin: 4px 0; }
.two-up.roomy .card .meta { font-size: 13.5px; }
.two-text { columns: 2; column-gap: 28px; }
.two-text li, .two-text tr { break-inside: avoid; }
.action-list.two-text { font-size: 15px; }
.action-list.two-text .owner { font-size: 12.5px; }
.action-list.two-text li { margin-bottom: 6px; }
.two-text .definition { break-inside: avoid; margin-bottom: 9px; font-size: 13.5px; line-height: 1.35; }
.meta { display: flex; gap: 12px; align-items: center; color: var(--muted); font-size: 15px; }
.card .meta { font-size: 13.5px; }
.chart-wrap { max-width: 1040px; }
table.grid { font-size: 18px; }
.tight table.grid { font-size: 16px; }
.tight table.grid td, .tight table.grid th { padding: 5px 8px; }
ol.contents { columns: 2; margin: 0; padding-left: 26px; }
ol.contents li { margin-bottom: 8px; }
.action-list { margin: 0; padding-left: 22px; font-size: 18px; }
.action-list li { margin-bottom: 8px; }
.action-list .owner { color: var(--muted); font-size: 14.5px; }
.chart-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; align-items: start; }
.lede { font-size: 17px; margin-bottom: 14px; }
.small-note { font-size: 13.5px; color: var(--muted); margin-top: 10px; }
.data td.num, .data th.num { text-align: right; }
.tight table.data { font-size: 15px; }
.tight table.data td, .tight table.data th { padding: 5px 7px; }
table.data tr.total td { font-weight: 600; border-top: 1px solid var(--carbon); }
.panel .big { font-size: 26px; font-weight: 600; line-height: 1.2; }
h3.cap { font-size: 15px; font-weight: 600; color: var(--muted); margin: 0 0 6px; }
.three-text { columns: 3; column-gap: 24px; }
.three-text .definition { break-inside: avoid; margin-bottom: 8px; font-size: 12px; line-height: 1.32; }
.compact-rows table.grid { font-size: 13px; line-height: 1.3; }
.compact-rows table.grid td, .compact-rows table.grid th { padding: 4px 7px; }
`;

type Slide = { html: string; classes?: string };

// In pixels of a 720px slide, less margins, heading and footer. Deliberately
// cautious: the layout check measures the real thing afterwards.
const ISSUE_SLIDE_BUDGET = 500;
const CARD_CHARS = 105;
const SMALL_CARD_CHARS = 70;
const ROOMY_SCALE = 1.5;
const LIST_BUDGET = 470;

function issueSlide(r: Render, label: string, f: Finding): Slide {
  const value = valueText(r, f);
  const meta = `<div class="meta">${severityChip(f)}<span>Confidence ${f.confidence}</span><span>${effortText(f)}</span></div>`;
  return {
    html: `<div class="eyebrow">${r.t(label)}</div><h2>${r.t(f.title)}</h2>${meta}<div class="columns" style="margin-top:14px"><div><p>${r.t(f.summary)}</p>${f.recommendation ? `<p style="margin-top:12px"><strong>What to do:</strong> ${r.t(f.recommendation)}</p>` : ""}</div><div class="panel"><h3>Value</h3><p><strong>${value.amount}</strong></p><p class="note">${value.note}</p><h3>Owner</h3><p>${r.t(f.owner)}</p>${f.dependencies.length ? `<h3>Depends on</h3><p class="note">${r.t(f.dependencies.join("; "))}</p>` : ""}</div></div>`,
  };
}

function cardHeight(f: Finding): number {
  return 84 + 24 * estimateLines(f.title, CARD_CHARS) + 24 * estimateLines(f.summary, CARD_CHARS) + 24 * estimateLines(`What to do: ${f.recommendation}`, CARD_CHARS);
}

function smallCardHeight(f: Finding): number {
  return 62 + 19 * estimateLines(f.title, SMALL_CARD_CHARS - 8) + 19 * estimateLines(f.summary, SMALL_CARD_CHARS) + 19 * estimateLines(`What to do: ${f.recommendation}`, SMALL_CARD_CHARS);
}

function card(r: Render, f: Finding): string {
  return `<article class="card"><div class="meta">${severityChip(f)}<span>${effortText(f)}</span><span>${valueText(r, f).amount}</span></div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p><p><strong>What to do:</strong> ${r.t(f.recommendation)}</p></article>`;
}

const groupTitle = (f: Finding): string => (f.severity === "low" || f.severity === "info" ? "Smaller issues" : "Other issues");

function issueCards(r: Render, label: string, findings: Finding[]): Slide {
  return { html: `<div class="eyebrow">${r.t(label)}</div><h2>${groupTitle(findings[0]!)}</h2>${findings.map((f) => card(r, f)).join("")}` };
}

/** Keeps reading order, and splits where the taller column is as short as it can be. */
function balance(findings: Finding[]): [Finding[], Finding[]] {
  const height = (group: Finding[]): number => group.reduce((sum, f) => sum + smallCardHeight(f), 0);
  let best = 1;
  for (let i = 1; i < findings.length; i++) {
    if (Math.max(height(findings.slice(0, i)), height(findings.slice(i))) < Math.max(height(findings.slice(0, best)), height(findings.slice(best)))) best = i;
  }
  return [findings.slice(0, best), findings.slice(best)];
}

function twoColumnSlides(r: Render, label: string, findings: Finding[]): Slide[] {
  const columns = paginate(findings, smallCardHeight, ISSUE_SLIDE_BUDGET);
  const slides: Slide[] = [];
  for (let i = 0; i < columns.length; i += 2) {
    const group = columns.slice(i, i + 2).flat();
    // A lone card takes the full width. Half an empty slide looks like a mistake.
    if (group.length === 1) {
      slides.push(issueCards(r, label, group));
      continue;
    }
    const sides = balance(group);
    // Few cards get larger type, so a short slide doesn't look unfinished.
    const roomy = Math.max(...sides.map((side) => side.reduce((sum, f) => sum + smallCardHeight(f), 0))) * ROOMY_SCALE <= ISSUE_SLIDE_BUDGET;
    slides.push({ html: `<div class="eyebrow">${r.t(label)}</div><h2>${groupTitle(group[0]!)}</h2><div class="two-up${roomy ? " roomy" : ""}">${sides.map((column) => `<div class="col">${column.map((f) => card(r, f)).join("")}</div>`).join("")}</div>` });
  }
  return slides;
}

/** `columns` sets smaller cards in two columns, so an ordinary org keeps all its detail. */
type Density = Record<Finding["severity"], number> & { columns?: 2; domainSummary?: boolean };

/** How many issues share a slide, by severity. Tried in order until the deck fits. */
const DENSITIES: Density[] = [
  { critical: 1, high: 1, medium: 1, low: 1, info: 1 },
  { critical: 1, high: 1, medium: 1, low: 2, info: 2 },
  { critical: 1, high: 1, medium: 2, low: 2, info: 2 },
  { critical: 1, high: 1, medium: 2, low: 3, info: 3 },
  { critical: 1, high: 2, medium: 3, low: 4, info: 4 },
  { critical: 2, high: 3, medium: 4, low: 6, info: 6 },
  { critical: 1, high: 1, medium: 6, low: 6, info: 6, columns: 2 },
  { critical: 1, high: 6, medium: 6, low: 6, info: 6, columns: 2 },
  { critical: 24, high: 24, medium: 24, low: 24, info: 24, columns: 2, domainSummary: true },
];

const severityWord = (severity: string): string => `${severity.charAt(0).toUpperCase()}${severity.slice(1)} severity`;

const MANUAL_STATUS: Record<string, string> = { pending: "Not answered yet", answered: "Answered", skipped: "Skipped" };

/** The last resort for a very large org: one table per domain, so the deck has a fixed ceiling. */
const TABLE_ROWS = 7;
const COMPACT_NOTE = "This list is cut short to keep the report readable. The raw notes have all of it.";

function issueTable(r: Render, label: string, issues: Finding[]): Slide[] {
  if (issues.length === 0) return [];
  const rows = issues.slice(0, TABLE_ROWS).map((f) => `<tr><td>${r.t(f.title)}</td><td>${severityChip(f)}</td><td>${valueFigure(r, f)}</td><td>${r.t(f.owner)}</td></tr>`);
  const note = issues.length > TABLE_ROWS ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : "";
  return [{ classes: "tight", html: `<div class="eyebrow">${r.t(label)}</div><h2>Issues</h2>${note}<table class="grid"><thead><tr><th>Issue</th><th style="width:10%">Severity</th><th style="width:24%">Value</th><th style="width:18%">Owner</th></tr></thead><tbody>${rows.join("")}</tbody></table>` }];
}

function domainIssueSlides(r: Render, label: string, issues: Finding[], density: Density): Slide[] {
  const slides: Slide[] = [];
  let queue: Finding[] = [];
  const flush = (): void => {
    if (density.columns) slides.push(...twoColumnSlides(r, label, queue));
    else for (const group of paginate(queue, cardHeight, ISSUE_SLIDE_BUDGET)) slides.push(issueCards(r, label, group));
    queue = [];
  };
  for (const f of issues) {
    const perSlide = density[f.severity];
    if (perSlide === 1) {
      flush();
      slides.push(issueSlide(r, label, f));
    } else {
      queue.push(f);
      if (queue.length >= perSlide) flush();
    }
  }
  flush();
  return slides;
}

function metricRows(r: Render, metrics: Metric[], currency: string): string {
  const rows = metrics.map((m) => {
    if (m.status === "not_measurable") return `<tr><td>${r.t(m.label)}</td><td class="num">Not measurable</td><td>${r.t(m.reason)}</td></tr>`;
    const value = m.unit === "percent" ? percent(m.value) : m.unit === "days" ? `${m.value} days` : m.unit === "currency" ? compactMoney(m.value, currency) : String(m.value);
    return `<tr><td>${r.t(m.label)}</td><td class="num">${value}</td><td>${r.t(m.definition)}</td></tr>`;
  });
  return `<table class="grid"><thead><tr><th>Measure</th><th class="num">Result</th><th>What it means</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
}

// The pipeline deep dive. Every figure on these slides is a metric's value,
// segment, series point or stat, so the number check can trace each one.

type Measured = Extract<Metric, { status: "measured" }>;
const asMeasured = (m: Metric | undefined): Measured | null => (m?.status === "measured" ? m : null);
const segmentValue = (m: Measured | null, dimension: string, key: string): MetricSegmentLike | undefined => m?.segments.find((s) => s.dimension === dimension && s.key === key);
type MetricSegmentLike = Measured["segments"][number];
const notMeasured = (r: Render, m: Metric | undefined): string => `<p class="chart-gap"><strong>Not measurable.</strong> ${r.t(m?.status === "not_measurable" ? m.reason : "This metric wasn't part of the run.")}</p>`;

/** Open stages in the calibration's sales order, then anything else in name order. */
function stageOrder(calibration: Calibration): (a: string, b: string) => number {
  const order = new Map(calibration.stages.filter((s) => s.category === "open").map((s) => [s.name, s.order ?? 0]));
  return (a, b) => (order.get(a) ?? Number.MAX_SAFE_INTEGER) - (order.get(b) ?? Number.MAX_SAFE_INTEGER) || a.localeCompare(b);
}

function pipelineTodaySlide(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const head = `<div class="eyebrow">Pipeline today</div><h2>Open pipeline by stage</h2>`;
  const count = asMeasured(metric("current.open_deals"));
  if (!count) return { html: `${head}${notMeasured(r, metric("current.open_deals"))}` };
  const [amount, blank, age, late, weighted, rates] = ["current.open_pipeline", "current.open_no_amount", "current.open_median_age", "current.open_past_close", "current.weighted_pipeline", "historic.stage_win_rate"].map((id) => asMeasured(metric(id)));
  const retired = new Set(count.segments.filter((s) => s.dimension === "retired stage").map((s) => s.key));
  const stages = count.segments.filter((s) => s.dimension === "stage").map((s) => s.key).sort(stageOrder(calibration));
  const money = (v: number | null | undefined): string => (typeof v === "number" ? compactMoney(v, r.currency) : "Not measured");
  const plain = (v: number | null | undefined, fallback = "Not measured"): string => (typeof v === "number" ? String(v) : fallback);
  const rate = (stage: string): string => {
    const s = segmentValue(rates, "stage", stage);
    return s?.value != null ? percent(s.value) : s ? "Too few" : "Not measured";
  };
  const rows = stages.map((stage) => {
    const cell = (m: Measured | null): MetricSegmentLike | undefined => segmentValue(m, "stage", stage);
    const ageCell = cell(age);
    return `<tr><td>${r.t(stage)}${retired.has(stage) ? " (retired)" : ""}</td><td class="num">${cell(count)!.value}</td><td class="num">${money(cell(amount)?.value)}</td><td class="num">${plain(cell(blank)?.value)}</td><td class="num">${ageCell ? plain(ageCell.value, "Too few") : "Not measured"}</td><td class="num">${plain(cell(late)?.value)}</td><td class="num">${rate(stage)}</td><td class="num">${cell(weighted) ? (cell(weighted)!.value === null ? "Not weighted" : money(cell(weighted)!.value)) : "Not measured"}</td></tr>`;
  });
  rows.push(`<tr class="total"><td>All open deals</td><td class="num">${count.value}</td><td class="num">${money(amount?.value)}</td><td class="num">${plain(blank?.value)}</td><td class="num">${plain(age?.value)}</td><td class="num">${plain(late?.value)}</td><td class="num">${rates ? percent(rates.value) : "Not measured"}</td><td class="num">${money(weighted?.value)}</td></tr>`);
  const notes = [
    "Weighted value is each deal's amount at the share of closed deals from that stage that went on to win. Median age is days since the deal was created.",
    ...(weighted?.exclusions.filter((e) => /too few closed deals/.test(e)) ?? []).map((e) => `Not weighted: ${e}.`),
    ...(retired.size ? [`A retired stage is no longer active in Salesforce but still holds open deals.`] : []),
  ];
  const table = `<table class="grid data"><thead><tr><th>Stage</th><th class="num">Open deals</th><th class="num">Amount</th><th class="num">No amount</th><th class="num">Median age, days</th><th class="num">Past close date</th><th class="num">Won from here</th><th class="num">Weighted value</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
  return { classes: "tight", html: `${head}${table}${notes.map((n) => `<p class="small-note">${r.t(n)}</p>`).join("")}` };
}

function outlookSlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const value = metric("current.delivery_outlook_value");
  const count = asMeasured(metric("current.delivery_outlook_deals"));
  const m = asMeasured(value);
  const head = `<div class="eyebrow">Pipeline today</div><h2>Delivery outlook: what's likely to be won, month by month</h2>`;
  if (!m) return { html: `${head}${chartSlot(deliveryOutlookChart(value, count ?? undefined, r.currency))}` };
  const first = m.series[0]!.date;
  const last = m.series.at(-1)!.date;
  const timing = (key: string): MetricSegmentLike | undefined => segmentValue(m, "timing", key);
  const passed = timing("Expected date already passed");
  const later = timing("Expected after the outlook");
  const panel = [
    `<h3>Expected from ${monthYear(first)} to ${monthYear(last)}</h3><p class="big">${compactMoney(m.value, r.currency)}</p>${count ? `<p class="note">About ${count.value} deals</p>` : ""}`,
    passed && passed.n > 0 ? `<h3>Expected date already passed</h3><p><strong>${compactMoney(passed.value ?? 0, r.currency)}</strong> from ${passed.n} deals with no month</p>` : "",
    later && later.n > 0 ? `<h3>Expected after ${monthYear(last)}</h3><p><strong>${compactMoney(later.value ?? 0, r.currency)}</strong> from ${later.n} deals</p>` : "",
    `<h3>How we work it out</h3><p class="note">${m.assumptions.slice(1).map((a) => r.t(a)).join(". ")}.</p>`,
    m.exclusions.length ? `<p class="note">Left out: ${m.exclusions.map((e) => r.t(e)).join("; ")}.</p>` : "",
  ].join("");
  return {
    html: `${head}<p class="lede">For planning delivery capacity: the work you can expect to win, by when it's likely to close.</p><div class="columns"><div class="chart-wrap">${chartSlot(deliveryOutlookChart(value, count ?? undefined, r.currency))}</div><div class="panel">${panel}</div></div>`,
  };
}

function quarterlySlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const head = `<div class="eyebrow">Pipeline over time</div><h2>Quarter by quarter</h2>`;
  const ids = ["historic.quarterly_created", "historic.quarterly_won", "historic.quarterly_won_value", "historic.quarterly_win_rate", "historic.quarterly_median_won_size", "historic.quarterly_median_cycle"];
  const all = ids.map((id) => asMeasured(metric(id)));
  const spine = all.find((m) => m !== null);
  if (!spine) return { html: `${head}${notMeasured(r, metric(ids[0]!))}` };
  const format = [(v: number) => String(v), (v: number) => String(v), (v: number) => compactMoney(v, r.currency), (v: number) => percent(v), (v: number) => compactMoney(v, r.currency), (v: number) => String(v)];
  const rows = spine.series.map((point, i) => {
    const cells = all.map((m, k) => {
      if (!m) return "Not measured";
      const p = m.series.find((x) => x.date === point.date);
      return p?.value != null ? format[k]!(p.value) : p?.notMeasurableReason?.startsWith("Starts before") ? "Before the window" : "Too few";
    });
    const label = `${monthYear(point.date)}${i === spine.series.length - 1 ? ", to date" : ""}`;
    return `<tr><td>${label}</td>${cells.map((c) => `<td class="num">${c}</td>`).join("")}</tr>`;
  });
  const missing = ids.filter((_, k) => !all[k]).map((id) => metric(id)).filter((m): m is Extract<Metric, { status: "not_measurable" }> => m?.status === "not_measurable");
  const notes = ["Too few means fewer closed deals than the calibrated minimum. The last quarter is still running.", ...missing.map((m) => `${m.label}: ${m.reason}`)];
  return { classes: "tight", html: `${head}<table class="grid data"><thead><tr><th>Quarter starting</th><th class="num">Deals created</th><th class="num">Deals won</th><th class="num">Won value</th><th class="num">Win rate</th><th class="num">Median won deal</th><th class="num">Median cycle, days</th></tr></thead><tbody>${rows.join("")}</tbody></table>${notes.map((n) => `<p class="small-note">${r.t(n)}</p>`).join("")}` };
}

function winsSlides(r: Render, metric: (id: string) => Metric | undefined): Slide[] {
  const repeat = asMeasured(metric("historic.repeat_win_share"));
  const top = asMeasured(metric("historic.top_account_share"));
  const lines = [
    repeat ? `${percent(repeat.value)} of won value came from repeat customers: accounts that had already won a deal with you.` : "",
    top ? `Your top ${top.stats.top} accounts brought in ${percent(top.value)} of won value. Accounts are counted here, not named.` : "",
  ].filter(Boolean);
  return [
    { html: `<div class="eyebrow">Where wins come from</div><h2>New and repeat customers</h2>${lines.map((l) => `<p class="lede">${r.t(l)}</p>`).join("")}<div class="chart-pair">${chartSlot(winsByCustomerChart(metric("historic.won_value_by_customer"), r.currency))}${chartSlot(topAccountsChart(metric("historic.top_account_share")))}</div>` },
    { html: `<div class="eyebrow">Where wins come from</div><h2>Wins by type and lead source</h2><div class="chart-pair">${chartSlot(wonValueByChart(metric("historic.won_value_by_type"), "type", "won-by-type", "Won value by type", r.currency))}${chartSlot(wonValueByChart(metric("historic.won_value_by_source"), "lead source", "won-by-source", "Won value by lead source", r.currency))}</div>` },
  ];
}

function lostSlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const count = asMeasured(metric("historic.lost_by_stage"));
  const lede = count ? `<p class="lede">${count.value} lost and no decision deals closed in the window. This is the last open stage each was in.</p>` : "";
  return { html: `<div class="eyebrow">Where deals are lost</div><h2>Where deals drop out</h2>${lede}<div class="chart-pair">${chartSlot(lostByStageChart(metric("historic.lost_by_stage"), metric("historic.lost_value_by_stage"), r.currency))}${chartSlot(closedAsChart(metric("historic.lost_by_stage")))}</div>` };
}

function closeDatesSlide(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const share = metric("historic.close_date_reliability");
  const late = metric("historic.close_days_late");
  const block = (m: Metric | undefined, body: (x: Measured) => string): string => (m?.status === "measured" ? body(m) : `<h3>${r.t(m?.label ?? "Not measured")}</h3><p class="note">${r.t(m?.status === "not_measurable" ? m.reason : "This metric wasn't part of the run.")}</p>`);
  const panel = [
    block(share, (m) => `<h3>Won in the quarter first forecast</h3><p class="big">${percent(m.value)}</p><p class="note">of ${m.n} won deals</p>`),
    block(late, (m) => `<h3>Days late against the first close date</h3><p class="big">${m.value} days</p><p class="note">For the median won deal. The typical range is ${m.stats.p25} to ${m.stats.p75} days.</p>`),
  ].join("");
  const order = calibration.stages.filter((s) => s.category === "open").sort((a, b) => (a.order ?? 0) - (b.order ?? 0)).map((s) => s.name);
  return { html: `<div class="eyebrow">Close dates and stage speed</div><h2>How far close dates hold, and where time goes</h2><div class="columns" style="grid-template-columns: 1fr 1.5fr"><div class="panel">${panel}</div>${chartSlot(stageVelocityChart(metric("historic.stage_velocity_won"), metric("historic.stage_velocity_lost"), order))}</div>` };
}

// A dense deck keeps every chart that answers a question, on fewer slides.

function winsSlideDense(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const [first] = winsSlides(r, metric);
  const lede = /<\/h2>((?:<p class="lede">[\s\S]*?<\/p>)*)/.exec(first!.html)?.[1] ?? "";
  const grid = [
    winsByCustomerChart(metric("historic.won_value_by_customer"), r.currency),
    topAccountsChart(metric("historic.top_account_share")),
    wonValueByChart(metric("historic.won_value_by_type"), "type", "won-by-type", "Won value by type", r.currency),
    wonValueByChart(metric("historic.won_value_by_source"), "lead source", "won-by-source", "Won value by lead source", r.currency),
  ].map(chartSlot);
  return { html: `<div class="eyebrow">Where wins come from</div><h2>New and repeat customers, type and lead source</h2>${lede.replace(/class="lede"/g, 'class="lede" style="font-size:15px;margin-bottom:8px"')}<div class="chart-pair" style="row-gap:12px">${grid.join("")}</div>` };
}

function lostAndSpeedSlideDense(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const count = asMeasured(metric("historic.lost_by_stage"));
  const share = asMeasured(metric("historic.close_date_reliability"));
  const late = asMeasured(metric("historic.close_days_late"));
  const closedAs = count ? count.segments.filter((s) => s.dimension === "closed as") : [];
  const lines = [
    count ? `${count.value} lost and no decision deals closed in the window, shown by the last open stage each was in.${closedAs.length ? ` By how they closed: ${closedAs.map((s) => `${s.key} ${s.value}`).join(", ")}.` : ""}` : "",
    share && late ? `${percent(share.value)} of ${share.n} won deals closed in the quarter first forecast, and the median won deal closed ${late.value} days after its first close date.` : "",
  ].filter(Boolean);
  const order = calibration.stages.filter((s) => s.category === "open").sort((a, b) => (a.order ?? 0) - (b.order ?? 0)).map((s) => s.name);
  return { html: `<div class="eyebrow">Where deals are lost</div><h2>Where deals drop out, and where time goes</h2>${lines.map((l) => `<p class="lede" style="font-size:15px;margin-bottom:8px">${r.t(l)}</p>`).join("")}<div class="chart-pair">${chartSlot(lostByStageChart(metric("historic.lost_by_stage"), metric("historic.lost_value_by_stage"), r.currency))}${chartSlot(stageVelocityChart(metric("historic.stage_velocity_won"), metric("historic.stage_velocity_lost"), order))}</div>` };
}

/** Definitions are reference, so a dense deck sets them small, in three columns. */
function threeColumnList(r: Render, eyebrow: string, title: string, items: Array<{ title: string; detail: string }>): Slide[] {
  const pages = paginate(items, (x) => 26 + 17 * estimateLines(x.detail, 56), 3 * 490);
  return pages.map((group, i) => ({
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2><div class="three-text">${group.map((x) => `<p class="definition"><strong>${r.t(x.title)}</strong><br>${r.t(x.detail)}</p>`).join("")}</div>`,
  }));
}

const sectionBody = (r: Render, built: BuiltOutput, key: string): string => paragraphs(r, currentBody(built, key));

const needsInput = (built: BuiltOutput, key: string): boolean => built.sections.find((s) => s.key === key)?.status === "needs_input";

const inputChip = '<p style="margin-bottom:10px"><span class="chip input">Needs input from a consultant</span></p>';

function listSlides(r: Render, eyebrow: string, title: string, items: Array<{ title: string; detail: string }>, compact: boolean): Slide[] {
  if (items.length === 0) return [{ html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}</h2><p>Nothing to list.</p>` }];
  const pages = paginate(items, (x) => 16 + 24 * Math.max(estimateLines(x.detail, 85), estimateLines(x.title, 34)), LIST_BUDGET);
  const cut = compact && pages.length > 1;
  return (cut ? pages.slice(0, 1) : pages).map((group, i) => ({
    classes: "tight",
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2>${cut ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : ""}<table class="grid"><tbody>${(cut ? group.slice(0, -1) : group).map((x) => `<tr><td style="width:30%"><strong>${r.t(x.title)}</strong></td><td>${r.t(x.detail)}</td></tr>`).join("")}</tbody></table>`,
  }));
}

function twoColumnList(r: Render, eyebrow: string, title: string, items: Array<{ title: string; detail: string }>): Slide[] {
  const pages = paginate(items, (x) => 28 + 19 * estimateLines(x.detail, 72), 2 * LIST_BUDGET);
  return pages.map((group, i) => ({
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2><div class="two-text">${group.map((x) => `<p class="definition"><strong>${r.t(x.title)}</strong><br>${r.t(x.detail)}</p>`).join("")}</div>`,
  }));
}

function buildSlides(report: BuiltReport, r: Render, density: Density | null): Slide[] {
  const compact = density === null;
  const { run, intake, calibration, scores = [], manualSteps } = report.input;
  const built = report.outputs.full_health_check;
  const team = report.teamFindings;
  const metrics = report.metrics;
  const metric = (id: string): Metric | undefined => metrics.find((m) => m.id === id);
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const { phases: plan, later } = buildPlan(team);
  const domains = domainsInOrder(team);
  const assessed = domains.filter((d) => team.some((f) => f.domain === d));
  const unassessed = domains.filter((d) => !assessed.includes(d));
  const customer = intake?.customerName ?? run.customerRef ?? "Customer";
  const slides: Slide[] = [];
  const add = (html: string, classes?: string): void => void slides.push({ html, ...(classes ? { classes } : {}) });

  add(`<div><div class="eyebrow">Black Cloud Barometer</div><h1>Salesforce health check</h1><div class="sub">Full report</div></div><div>${r.draft ? '<p><span class="mark">Draft, not approved for release</span></p>' : ""}<p>${r.t(customer)}</p><p>As of ${longDate(run.asOf)}</p></div>`, "dark cover");

  const contents = ["Stated problems", "Overview and scorecard", ...assessed.map(domainLabel), "Pipeline deep dive", "Stated vs actual", "Roadmap", "Appendix"];
  add(`<div class="eyebrow">Contents</div><h2>What's in this report</h2><ol class="contents">${contents.map((c) => `<li>${r.t(c)}</li>`).join("")}</ol>`);

  add(`<div class="eyebrow">How to read this report</div><h2>What the ratings mean</h2><table class="grid"><tbody>
    <tr><td style="width:24%"><strong>Severity</strong></td><td>How much damage this does if it's left alone.</td></tr>
    <tr><td><strong>Confidence</strong></td><td>How sure we are, given what we could read from your org.</td></tr>
    <tr><td><strong>Effort</strong></td><td>Our sizing of the fix, from very small to very large.</td></tr>
    <tr><td><strong>Value</strong></td><td>Revenue, licence cost or hours, with its assumptions. If we can't size something, we say so.</td></tr>
    <tr><td><strong>Not measurable</strong></td><td>The data can't support the figure, so we report that rather than estimate it.</td></tr></tbody></table>`);

  add(`<div class="eyebrow">Stated problems</div><h2>What you told us</h2>${needsInput(built, "stated_problems") ? inputChip : ""}${sectionBody(r, built, "stated_problems")}`);

  add(`<div class="eyebrow">Overview</div><h2>${r.t(verdict.headline)}</h2><div>${sectionBody(r, built, "overview").replace(`<p>${r.t(verdict.headline)}.</p>`, "")}</div><div class="stat-row" style="margin-top:18px"><div class="stat"><b>${verdict.issues}</b><span>Issues</span></div><div class="stat"><b>${verdict.critical}</b><span>Critical</span></div><div class="stat"><b>${verdict.high}</b><span>High</span></div><div class="stat"><b>${team.filter((f) => f.status === "not_measurable").length}</b><span>Not measurable</span></div><div class="stat"><b>${team.filter((f) => f.status === "passed").length}</b><span>Passed</span></div></div>`);

  add(`<div class="eyebrow">Scorecard</div><h2>How each area stands</h2><div class="chart-wrap" style="max-width:880px">${chartSlot(scorecardChart(scorecard(team, scores)))}</div>`);

  add(`<div class="eyebrow">Top issues</div><h2>The ${TOP_ISSUE_COUNT} that matter most</h2><table class="grid"><thead><tr><th style="width:6%"></th><th>Issue</th><th style="width:12%">Severity</th><th style="width:26%">Value</th><th style="width:20%">Owner</th></tr></thead><tbody>${top.map((f, i) => `<tr><td>${i + 1}</td><td>${r.t(f.title)}</td><td>${severityChip(f)}</td><td>${valueText(r, f).amount}</td><td>${r.t(f.owner)}</td></tr>`).join("")}</tbody></table>`);

  const dense = density?.columns === 2;
  if (density?.domainSummary) {
    slides.push(...twoColumnList(r, "Domains", "Each area at a glance", assessed.map((domain) => {
      const own = team.filter((f) => f.domain === domain);
      return { title: `${domainLabel(domain)}: ${issuesOf(own).length} issues, ${own.filter((f) => f.status === "passed").length} passed`, detail: currentBody(built, `domain_${domain}`) };
    })));
  }
  for (const domain of assessed) {
    const label = domainLabel(domain);
    const own = team.filter((f) => f.domain === domain);
    const issues = issuesOf(own);
    if (!density?.domainSummary) add(`<div class="eyebrow">Domain</div><h1>${r.t(label)}</h1><div class="body">${sectionBody(r, built, `domain_${domain}`)}</div><div class="stat-row" style="max-width:640px;margin-top:20px;color:var(--carbon)"><div class="stat"><b>${issues.length}</b><span>Issues</span></div><div class="stat"><b>${own.filter((f) => f.status === "passed").length}</b><span>Passed</span></div><div class="stat"><b>${own.filter((f) => f.status === "not_measurable").length}</b><span>Not measurable</span></div></div>`, "dark divider");
    slides.push(...(density ? domainIssueSlides(r, label, issues, density) : issueTable(r, label, issues)));
  }

  if (unassessed.length) {
    add(`<div class="eyebrow">Domains</div><h2>Not assessed in this run</h2><p style="margin-bottom:12px">We make no claim about these areas, good or bad.</p><ul>${unassessed.map((d) => `<li>${r.t(domainLabel(d))}</li>`).join("")}</ul>`);
  }

  add(`<div class="eyebrow">Deep dive</div><h1>Pipeline</h1><div class="body">${sectionBody(r, built, "pipeline_deep_dive")}</div>`, "dark divider");
  slides.push(pipelineTodaySlide(r, metric, calibration), outlookSlide(r, metric));
  if (dense) add(`<div class="eyebrow">Pipeline</div><h2>How deals convert</h2><div class="chart-pair"><div><h3 class="cap">Win rate by segment</h3>${chartSlot(winRateBySegment(metric("historic.win_rate"), undefined, HALF_WIDTH))}</div><div><h3 class="cap">Stage funnel</h3>${chartSlot(stageFunnel(metric("historic.stage_conversion"), HALF_WIDTH))}</div></div>`);
  else add(`<div class="eyebrow">Pipeline</div><h2>Win rate by segment</h2><div class="chart-wrap">${chartSlot(winRateBySegment(metric("historic.win_rate")))}</div>`);
  const readings = ["historic.win_rate", "historic.win_rate_by_value", "historic.win_rate_including_no_decision", "historic.no_decision_rate"].flatMap((id) => metric(id) ?? []);
  const cycle = ["historic.sales_cycle_won", "historic.sales_cycle_lost"].flatMap((id) => metric(id) ?? []);
  const slippage = ["historic.slippage_rate", "historic.slippage_quarter_cross_rate", "historic.slippage_days"].flatMap((id) => metric(id) ?? []);
  if (!dense) add(`<div class="eyebrow">Pipeline</div><h2>Three ways of reading the win rate</h2>${metricRows(r, readings, r.currency)}`, "tight");
  if (!dense) add(`<div class="eyebrow">Pipeline</div><h2>Stage funnel</h2><div class="chart-wrap">${chartSlot(stageFunnel(metric("historic.stage_conversion")))}</div>`);
  slides.push(quarterlySlide(r, metric), ...(dense ? [winsSlideDense(r, metric), lostAndSpeedSlideDense(r, metric, calibration)] : [...winsSlides(r, metric), lostSlide(r, metric), closeDatesSlide(r, metric, calibration)]));
  if (dense) {
    // One table, so the deep dive keeps its new slides in a dense deck.
    add(`<div class="eyebrow">Pipeline</div><h2>Win rate, sales cycle and slippage</h2>${metricRows(r, [...readings, ...cycle, ...slippage], r.currency)}`, "tight compact-rows");
  } else {
    add(`<div class="eyebrow">Pipeline</div><h2>Sales cycle and slippage</h2>${metricRows(r, [...cycle, ...slippage], r.currency)}`, "tight");
  }
  add(`<div class="eyebrow">Pipeline</div><h2>Pipeline over time</h2><div class="chart-wrap" style="max-width:1000px">${chartSlot(pipelineOverTime(metric("historic.pipeline_point_in_time_value"), r.currency))}</div>`);

  add(`<div class="eyebrow">Stated vs actual</div><h2>What you told us, against what we found</h2>${needsInput(built, "stated_vs_actual") ? inputChip : ""}${sectionBody(r, built, "stated_vs_actual")}`);

  if (!dense) add(`<div class="eyebrow">Roadmap</div><h2>The first 90 days</h2><div class="stat-row">${plan.map((p) => `<div class="stat"><b>${r.t(p.title)}</b><span>Days ${p.from} to ${p.to}</span><p style="margin-top:8px">${p.findings.length} ${p.findings.length === 1 ? "action" : "actions"}</p></div>`).join("")}</div>`);
  for (const phase of plan) {
    const all = paginate(phase.findings, (f) => (dense ? 10 + 23 * estimateLines(`${f.title}. ${f.recommendation} ${f.owner}`, 58) : 12 + 28 * estimateLines(`${f.title}. ${f.recommendation} ${f.owner}`, 95)), dense ? 2 * LIST_BUDGET : LIST_BUDGET);
    const cut = compact && all.length > 1;
    const pages = cut ? [all[0]!.slice(0, -1)] : all;
    (pages.length ? pages : [[]]).forEach((group, i) => {
      add(`<div class="eyebrow">Roadmap</div><h2>Days ${phase.from} to ${phase.to}: ${phase.title}${i ? ", continued" : ""}</h2>${cut ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : ""}${group.length ? `<ul class="action-list${dense ? " two-text" : ""}">${group.map((f) => `<li><strong>${r.t(f.title)}.</strong> ${r.t(f.recommendation)} <span class="owner">${r.t(f.owner)}</span></li>`).join("")}</ul>` : "<p>Nothing planned for this phase.</p>"}`);
    });
  }

  // Titles only: each of these already has its card in its domain's section.
  const afterPlan = later.map((f) => ({ title: f.title, detail: `${severityWord(f.severity)}. ${effortText(f)}. ${f.owner}` }));
  if (later.length) slides.push(...(dense ? twoColumnList(r, "Roadmap", "After 90 days", afterPlan) : listSlides(r, "Roadmap", "After 90 days", afterPlan, compact)));

  if (!dense) add(`<div class="eyebrow">Appendix</div><h1>How we worked</h1><div class="body">${sectionBody(r, built, "appendix")}</div>`, "dark divider");
  slides.push(...listSlides(r, "Appendix", "Assumptions", [
    ...(dense ? [{ title: "How we worked", detail: currentBody(built, "appendix").replace(/\n+/g, " ") }] : []),
    { title: "Analysis window", detail: `${longDate(run.analysisWindow.from)} to ${longDate(run.analysisWindow.to)}` },
    { title: "Stage meanings", detail: calibration.stages.map((s) => `${s.name} (${s.category.replace("_", " ")})`).join(", ") },
    { title: "Dormant users", detail: `No login for ${calibration.dormantDays} days` },
    { title: "Smallest sample", detail: `At least ${calibration.minSample} deals behind a figure` },
    { title: "Currency", detail: calibration.currency },
    { title: "Rule set", detail: run.ruleSetVersion },
  ], compact));
  const definitions = metrics.map((m) => ({ title: m.label, detail: m.definition }));
  // Two columns at every density but the last: definitions are reference, and
  // one to a line took a slide for every few metrics.
  slides.push(...(compact ? listSlides(r, "Appendix", "Metric definitions", definitions, compact) : dense ? threeColumnList(r, "Appendix", "Metric definitions", definitions) : twoColumnList(r, "Appendix", "Metric definitions", definitions)));
  slides.push(...listSlides(r, "Appendix", "What we couldn't measure", team.filter((f) => f.status === "not_measurable").map((f) => ({ title: f.title, detail: f.notMeasurableReason ?? f.summary })), compact));
  slides.push(...listSlides(r, "Appendix", "Checks that passed", team.filter((f) => f.status === "passed").map((f) => ({ title: f.title, detail: f.summary })), compact));
  slides.push(...listSlides(r, "Appendix", "Manual steps", manualSteps.map((s) => ({ title: s.title, detail: `${MANUAL_STATUS[s.status] ?? s.status}. ${s.whyItMatters}` })), compact));
  return slides;
}

/** Slides at each density in turn, so a test can see how close a deck is to its ceiling. */
export function slideCounts(report: BuiltReport, pseudonyms: Pseudonymiser): number[] {
  const r = renderContext(pseudonyms, report.input.calibration.currency, true);
  return [...DENSITIES, null].map((density) => buildSlides(report, r, density).length);
}

export function fullReportHtml(report: BuiltReport, pseudonyms: Pseudonymiser): string {
  const built = report.outputs.full_health_check;
  const r = renderContext(pseudonyms, report.input.calibration.currency, !canRelease(built.sections).ok);
  let slides: Slide[] = [];
  for (const density of [...DENSITIES, null]) {
    slides = buildSlides(report, r, density);
    if (slides.length <= DECK_RANGE.max) break;
  }
  const pages = slides.map((slide, i) => {
    const chrome = slide.classes?.includes("cover") ? "" : `<div class="footer" data-chrome><span>Black Cloud Barometer${r.draft ? " | Draft, not approved for release" : ""}</span><span>${i + 1}</span></div>`;
    return `<section class="page ${slide.classes ?? ""}" data-page="${i + 1}">${slide.html}${chrome}</section>`;
  });
  return document("Full health check", "slide", CSS, pages);
}
