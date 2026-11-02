import type { RuleContext } from "../../engine/rule.ts";
import { pct } from "../../engine/rule.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import type { DateRange, Evidence, Metric, MetricSegment, SeriesPoint } from "../../schemas/index.ts";
import { loadPipeline, type Deal, type PipelineData } from "./cohort.ts";

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

const DAY = 86_400_000;
const DOMAIN = "historic_pipeline";
const SEGMENT_NOTE = "Sample below the calibrated minimum";
const MAX_MISSING_AMOUNT_PCT = 20;
const MAX_MISSING_HISTORY_PCT = 20;
const UTC_NOTE = "Days, months and quarters are in UTC, not the org's time zone";

export function median(values: number[]): number {
  return quantile(values, 0.5);
}

/** Linear interpolation between ranks, the same method as Excel's PERCENTILE.INC. */
export function quantile(values: number[], q: number): number {
  if (values.length === 0) return NaN;
  const sorted = [...values].sort((a, b) => a - b);
  const position = (sorted.length - 1) * q;
  const low = Math.floor(position);
  const high = Math.ceil(position);
  return sorted[low]! + (sorted[high]! - sorted[low]!) * (position - low);
}

const round1 = (n: number): number => Math.round(n * 10) / 10;

function groupBy<R>(rows: R[], keyOf: (row: R) => string): Map<string, R[]> {
  const groups = new Map<string, R[]>();
  for (const row of rows) {
    const key = keyOf(row);
    const group = groups.get(key);
    if (group) group.push(row);
    else groups.set(key, [row]);
  }
  return groups;
}

export function fiscalQuarter(date: Date, startMonth: number): string {
  const offset = (date.getUTCMonth() + 1 - startMonth + 12) % 12;
  const quarter = Math.floor(offset / 3) + 1;
  // Fiscal year is named for the calendar year it ends in, as Salesforce does by default.
  const year = date.getUTCFullYear() + (startMonth > 1 && date.getUTCMonth() + 1 >= startMonth ? 1 : 0);
  return `FY${year}-Q${quarter}`;
}

export const closedInWindow = (d: Deal, window: DateRange): boolean => {
  const day = d.closedAt.toISOString().slice(0, 10);
  return day >= window.from && day <= window.to;
};

/** The calibration's fiscal year, or the org's own. Null if neither can be read, because a guess moves every quarter. */
export function fiscalStartOf(ctx: Ctx): number | null {
  if (ctx.calibration.fiscalYearStartMonth) return ctx.calibration.fiscalYearStartMonth;
  const org = ctx.data(orgExtractor);
  return org.status === "ok" ? (org.data[0]?.FiscalYearStartMonth ?? null) : null;
}

const hasOpenStage = (d: Deal, data: Ready): boolean => d.history.some((h) => data.stages.orderOf(h.StageName) !== null);

function dimensions(ctx: Ctx): Record<string, (d: Deal) => string> {
  const fiscalStart = fiscalStartOf(ctx);
  return {
    "record type": (d) => d.RecordType ?? "No record type",
    team: (d) => d.team,
    rep: (d) => ctx.userLabel(d.OwnerId),
    "lead source": (d) => d.LeadSource ?? "No lead source",
    type: (d) => d.Type ?? "No type",
    ...(fiscalStart ? { "closed quarter": (d: Deal) => fiscalQuarter(d.closedAt, fiscalStart) } : {}),
  };
}

function segment(rows: Deal[], dims: Record<string, (d: Deal) => string>, minSample: number, calculate: (group: Deal[]) => number | null): MetricSegment[] {
  const segments: MetricSegment[] = [];
  for (const [dimension, keyOf] of Object.entries(dims)) {
    for (const [key, group] of [...groupBy(rows, keyOf)].sort(([a], [b]) => a.localeCompare(b))) {
      const thin = group.length < minSample;
      const value = thin ? null : calculate(group);
      segments.push({
        dimension,
        key,
        n: group.length,
        value,
        notMeasurableReason: thin ? `${SEGMENT_NOTE} (${group.length} of ${minSample})` : value === null ? "No usable values in this segment" : null,
      });
    }
  }
  return segments;
}

type Base = { id: string; label: string; definition: string; unit: Metric["unit"]; evidence: Evidence[]; exclusions: string[]; assumptions: string[] };

const notMeasurable = (base: Base, reason: string): Metric => ({ status: "not_measurable", domain: DOMAIN, ...base, reason });

function measured(base: Base, fields: { value: number; n: number; dateRange: DateRange | null; segments?: MetricSegment[]; series?: SeriesPoint[]; stats?: Record<string, number>; segmentExemptReason?: string }): Metric {
  return {
    status: "measured",
    domain: DOMAIN,
    ...base,
    value: fields.value,
    n: fields.n,
    dateRange: fields.dateRange,
    segments: fields.segments ?? [],
    segmentExemptReason: fields.segmentExemptReason ?? null,
    series: fields.series ?? [],
    stats: fields.stats ?? {},
  };
}

function base(data: Ready | Extract<PipelineData, { status: "unavailable" }>, fields: Pick<Base, "id" | "label" | "definition" | "unit"> & { assumptions?: string[] }): Base {
  return { ...fields, evidence: data.evidence, exclusions: data.status === "ok" ? data.exclusions : [], assumptions: [UTC_NOTE, ...(fields.assumptions ?? [])] };
}

const unmappedReason = (kind: string, stages: string[]): string => `The calibration doesn't say what these ${kind} mean: ${stages.join(", ")}.`;

function winRate(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const definitions = {
    count: base(data, { id: "historic.win_rate", label: "Win rate", unit: "percent", definition: "Won deals as a share of won and lost deals that closed in the window. No decision deals are left out, and reported separately." }),
    value: base(data, { id: "historic.win_rate_by_value", label: "Win rate by value", unit: "percent", definition: "Amount of won deals as a share of the amount of won and lost deals that closed in the window." }),
    withNoDecision: base(data, { id: "historic.win_rate_including_no_decision", label: "Win rate including no decision", unit: "percent", definition: "Won deals as a share of all deals that closed in the window, counting no decision as a loss." }),
    noDecision: base(data, { id: "historic.no_decision_rate", label: "No decision rate", unit: "percent", definition: "Deals closed as no decision, as a share of all deals that closed in the window." }),
  };
  if (data.unmapped.finalStages.length > 0) {
    const reason = unmappedReason("stages", data.unmapped.finalStages);
    return Object.values(definitions).map((d) => notMeasurable(d, reason));
  }
  const closed = data.deals.filter((d) => closedInWindow(d, ctx.window) && (d.category === "won" || d.category === "lost" || d.category === "no_decision"));
  const decided = closed.filter((d) => d.category !== "no_decision");
  const won = decided.filter((d) => d.category === "won");
  const rate = (group: Deal[]): number => pct(group.filter((d) => d.category === "won").length, group.length);
  const thin = (n: number): string => `${n} closed deals is below the calibrated minimum of ${minSample}.`;
  const dims = dimensions(ctx);
  const metrics: Metric[] = [];

  if (decided.length < minSample) metrics.push(notMeasurable(definitions.count, thin(decided.length)));
  else metrics.push(measured(definitions.count, { value: rate(decided), n: decided.length, dateRange: ctx.window, segments: segment(decided, dims, minSample, rate), stats: { won: won.length, lost: decided.length - won.length } }));

  const missingAmount = decided.filter((d) => d.Amount === null).length;
  if (data.multiCurrency) metrics.push(notMeasurable(definitions.value, "The org is multi-currency, and amounts can't be added until historic rates are supplied."));
  else if (decided.length < minSample) metrics.push(notMeasurable(definitions.value, thin(decided.length)));
  else if (pct(missingAmount, decided.length) > MAX_MISSING_AMOUNT_PCT) metrics.push(notMeasurable(definitions.value, `${pct(missingAmount, decided.length)}% of closed deals have no amount, so a value-weighted rate would mislead.`));
  else {
    const valueRate = (group: Deal[]): number | null => {
      if (pct(group.filter((d) => d.Amount === null).length, group.length) > MAX_MISSING_AMOUNT_PCT) return null;
      const total = group.reduce((sum, d) => sum + (d.Amount ?? 0), 0);
      return total === 0 ? null : pct(group.filter((d) => d.category === "won").reduce((sum, d) => sum + (d.Amount ?? 0), 0), total);
    };
    const value = valueRate(decided);
    if (value === null) metrics.push(notMeasurable(definitions.value, "Closed deals have no amounts."));
    else {
      const assumptions = [...definitions.value.assumptions, ...(missingAmount > 0 ? [`${missingAmount} closed deals have no amount and count as zero`] : [])];
      metrics.push(measured({ ...definitions.value, assumptions }, { value, n: decided.length, dateRange: ctx.window, segments: segment(decided, dims, minSample, valueRate) }));
    }
  }

  if (closed.length < minSample) {
    metrics.push(notMeasurable(definitions.withNoDecision, thin(closed.length)), notMeasurable(definitions.noDecision, thin(closed.length)));
  } else {
    const noDecisionRate = (group: Deal[]): number => pct(group.filter((d) => d.category === "no_decision").length, group.length);
    metrics.push(
      measured(definitions.withNoDecision, { value: rate(closed), n: closed.length, dateRange: ctx.window, segments: segment(closed, dims, minSample, rate) }),
      measured(definitions.noDecision, { value: noDecisionRate(closed), n: closed.length, dateRange: ctx.window, segments: segment(closed, dims, minSample, noDecisionRate) }),
    );
  }
  return metrics;
}

function stageConversion(ctx: Ctx, data: Ready): Metric[] {
  const definition = base(data, {
    id: "historic.stage_conversion",
    label: "Stage conversion",
    unit: "percent",
    definition: "Of deals closed in the window that reached the first stage, the share won. Segments show each step: of deals that reached a stage, the share that reached the next one.",
    assumptions: ["A deal that skipped a stage counts as having passed through it, and skips are shown separately"],
  });
  const gaps = [...data.unmapped.finalStages, ...data.unmapped.historyStages];
  if (gaps.length > 0) return [notMeasurable(definition, unmappedReason("stages", [...new Set(gaps)].sort()))];
  const { openStages, orderOf } = data.stages;
  if (openStages.length < 2) return [notMeasurable(definition, "The calibration needs at least two ordered open stages.")];

  const closed = data.deals.filter((d) => closedInWindow(d, ctx.window) && d.category !== "open" && d.category !== null);
  // How far each deal got. A won deal went all the way, but only if it was ever
  // open: one created as Closed Won says nothing about any stage, and counting it
  // while dropping one created as Closed Lost would inflate every step.
  const furthest = (d: Deal): number | null => {
    const reached = d.history.map((h) => orderOf(h.StageName)).filter((i): i is number => i !== null);
    if (reached.length === 0) return null;
    return d.category === "won" ? openStages.length : Math.max(...reached);
  };
  const withStages = closed.filter((d) => furthest(d) !== null);
  const neverOpen = closed.length - withStages.length;
  const exclusions = [...definition.exclusions, ...(neverOpen ? [`${neverOpen} deals with no recorded open stage`] : [])];
  const { minSample } = ctx.calibration;
  if (withStages.length < minSample) return [notMeasurable({ ...definition, exclusions }, `${withStages.length} closed deals with stage history is below the calibrated minimum of ${minSample}.`)];

  const reaching = (index: number): Deal[] => withStages.filter((d) => furthest(d)! >= index);
  const segments: MetricSegment[] = [];
  for (let i = 0; i < openStages.length; i++) {
    const entered = reaching(i);
    const next = reaching(i + 1);
    const label = i + 1 < openStages.length ? `${openStages[i]} to ${openStages[i + 1]}` : `${openStages[i]} to won`;
    const thin = entered.length < minSample;
    segments.push({
      dimension: "step",
      key: label,
      n: entered.length,
      value: thin ? null : pct(next.length, entered.length),
      notMeasurableReason: thin ? `${SEGMENT_NOTE} (${entered.length} of ${minSample})` : null,
    });
    const skipped = entered.filter((d) => !d.history.some((h) => h.StageName === openStages[i])).length;
    segments.push({
      dimension: "skipped stage",
      key: openStages[i]!,
      n: entered.length,
      value: thin ? null : pct(skipped, entered.length),
      notMeasurableReason: thin ? `${SEGMENT_NOTE} (${entered.length} of ${minSample})` : null,
    });
  }
  const won = withStages.filter((d) => d.category === "won").length;
  return [measured({ ...definition, exclusions }, { value: pct(won, reaching(0).length), n: reaching(0).length, dateRange: ctx.window, segments, stats: { won } })];
}

function salesCycle(ctx: Ctx, data: Ready): Metric[] {
  const { minSample, salesCycleAssumptionDays } = ctx.calibration;
  const make = (outcome: "won" | "lost", id: string, label: string): Metric => {
    const definition = base(data, {
      id,
      label,
      unit: "days",
      definition: `Median days from creation to the deal's final close, for ${outcome} deals closed in the window. The close comes from stage history, with CloseDate used where history has none.`,
      assumptions: salesCycleAssumptionDays ? [`The calibration assumes a sales cycle of ${salesCycleAssumptionDays} days`] : [],
    });
    if (data.unmapped.finalStages.length > 0) return notMeasurable(definition, unmappedReason("stages", data.unmapped.finalStages));
    const rawDays = (d: Deal): number => (d.closedAt.getTime() - new Date(d.CreatedDate).getTime()) / DAY;
    const inWindow = data.deals.filter((d) => d.category === outcome && closedInWindow(d, ctx.window));
    // A deal created already closed, or closed before it was created, has no cycle
    // to measure: it's a data load, and counting it as 0 days flatters the median.
    const deals = inWindow.filter((d) => (d.history.length === 0 || hasOpenStage(d, data)) && rawDays(d) >= -1);
    const unusable = inWindow.length - deals.length;
    const exclusions = [...definition.exclusions, ...(unusable ? [`${unusable} deals created already closed, or closed before they were created`] : [])];
    if (deals.length < minSample) return notMeasurable({ ...definition, exclusions }, `${deals.length} ${outcome} deals is below the calibrated minimum of ${minSample}.`);
    const days = (d: Deal): number => Math.max(0, rawDays(d));
    const all = deals.map(days);
    const fallback = deals.filter((d) => d.closedAtSource === "close_date").length;
    const assumptions = [...definition.assumptions, ...(fallback ? [`${fallback} deals have no closing history row, so CloseDate was used`] : [])];
    return measured(
      { ...definition, assumptions, exclusions },
      {
        value: round1(median(all)),
        n: deals.length,
        dateRange: ctx.window,
        segments: segment(deals, dimensions(ctx), minSample, (group) => round1(median(group.map(days)))),
        stats: { p25: round1(quantile(all, 0.25)), p75: round1(quantile(all, 0.75)), ...(salesCycleAssumptionDays ? { assumedDays: salesCycleAssumptionDays } : {}) },
      },
    );
  };
  return [make("won", "historic.sales_cycle_won", "Sales cycle, won deals"), make("lost", "historic.sales_cycle_lost", "Sales cycle, lost deals")];
}

type Push = { days: number; crossedQuarter: boolean };

function pushesOf(deal: Deal, fiscalStart: number | null): Push[] {
  const pushes: Push[] = [];
  for (let i = 1; i < deal.history.length; i++) {
    const before = new Date(`${deal.history[i - 1]!.CloseDate}T00:00:00Z`);
    const after = new Date(`${deal.history[i]!.CloseDate}T00:00:00Z`);
    if (after > before) pushes.push({ days: (after.getTime() - before.getTime()) / DAY, crossedQuarter: fiscalStart !== null && fiscalQuarter(before, fiscalStart) !== fiscalQuarter(after, fiscalStart) });
  }
  return pushes;
}

function slippage(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const fiscalYearStartMonth = fiscalStartOf(ctx);
  const rateDefinition = base(data, { id: "historic.slippage_rate", label: "Slippage rate", unit: "percent", definition: "Deals closed in the window whose close date was moved later at least once, as a share of those deals." });
  const quarterDefinition = base(data, { id: "historic.slippage_quarter_cross_rate", label: "Deals pushed across a quarter", unit: "percent", definition: "Deals closed in the window whose close date moved into a later fiscal quarter, as a share of those deals." });
  const daysDefinition = base(data, { id: "historic.slippage_days", label: "Days pushed per slipped deal", unit: "days", definition: "Median total days of later moves on deals that slipped. Moves to an earlier date aren't netted off." });
  if (data.unmapped.finalStages.length > 0) {
    const reason = unmappedReason("stages", data.unmapped.finalStages);
    return [rateDefinition, quarterDefinition, daysDefinition].map((d) => notMeasurable(d, reason));
  }
  const deals = data.deals.filter((d) => d.category !== "open" && d.category !== null && closedInWindow(d, ctx.window));
  // A deal that was never open had no close date to slip.
  const withHistory = deals.filter((d) => hasOpenStage(d, data));
  const missing = deals.length - withHistory.length;
  const exclusions = [...data.exclusions, ...(missing ? [`${missing} deals with no recorded open stage`] : [])];
  if (withHistory.length < minSample) {
    const reason = `${withHistory.length} closed deals with history is below the calibrated minimum of ${minSample}.`;
    return [rateDefinition, quarterDefinition, daysDefinition].map((d) => notMeasurable({ ...d, exclusions }, reason));
  }
  const dims = dimensions(ctx);
  const slipRate = (group: Deal[]): number => pct(group.filter((d) => pushesOf(d, fiscalYearStartMonth).length > 0).length, group.length);
  const crossRate = (group: Deal[]): number => pct(group.filter((d) => pushesOf(d, fiscalYearStartMonth).some((p) => p.crossedQuarter)).length, group.length);
  const slipped = withHistory.filter((d) => pushesOf(d, fiscalYearStartMonth).length > 0);
  const totalDays = (d: Deal): number => pushesOf(d, fiscalYearStartMonth).reduce((sum, p) => sum + p.days, 0);
  const metrics: Metric[] = [
    measured({ ...rateDefinition, exclusions }, { value: slipRate(withHistory), n: withHistory.length, dateRange: ctx.window, segments: segment(withHistory, dims, minSample, slipRate), stats: { slipped: slipped.length } }),
    fiscalYearStartMonth === null
      ? notMeasurable({ ...quarterDefinition, exclusions }, "The org's fiscal year couldn't be read, and the calibration doesn't set one.")
      : measured({ ...quarterDefinition, exclusions }, { value: crossRate(withHistory), n: withHistory.length, dateRange: ctx.window, segments: segment(withHistory, dims, minSample, crossRate) }),
  ];
  if (slipped.length < minSample) metrics.push(notMeasurable({ ...daysDefinition, exclusions }, `${slipped.length} slipped deals is below the calibrated minimum of ${minSample}.`));
  else metrics.push(measured({ ...daysDefinition, exclusions }, { value: round1(median(slipped.map(totalDays))), n: slipped.length, dateRange: ctx.window, segments: segment(slipped, dims, minSample, (group) => round1(median(group.map(totalDays)))) }));
  return metrics;
}

/** Each month end from the window start up to `now`. */
export function monthEnds(window: DateRange): string[] {
  const out: string[] = [];
  const cursor = new Date(`${window.from.slice(0, 7)}-01T00:00:00Z`);
  for (;;) {
    const end = new Date(Date.UTC(cursor.getUTCFullYear(), cursor.getUTCMonth() + 1, 0)).toISOString().slice(0, 10);
    if (end > window.to) return out;
    if (end >= window.from) out.push(end);
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }
}

type Snapshot = { stage: string; amount: number | null };

/** What a deal looked like at the end of `day`, or null if it had no history by then. */
export function stateAt(deal: Deal, day: string): Snapshot | null {
  const cutoff = `${day}T23:59:59.999Z`;
  let latest: { StageName: string; Amount: number | null } | null = null;
  for (const row of deal.history) {
    if (new Date(row.CreatedDate).toISOString() <= cutoff) latest = row;
    else break;
  }
  return latest ? { stage: latest.StageName, amount: latest.Amount } : null;
}

function pointInTime(ctx: Ctx, data: Ready): Metric[] {
  const countDefinition = base(data, {
    id: "historic.pipeline_point_in_time_count",
    label: "Open deals at each month end",
    unit: "count",
    definition: "Deals whose stage at each month end was open, rebuilt from stage history. The value is the latest month end.",
  });
  const valueDefinition = base(data, {
    id: "historic.pipeline_point_in_time_value",
    label: "Open pipeline at each month end",
    unit: "currency",
    definition: "Total amount of deals whose stage at each month end was open, using the amount recorded then. The value is the latest month end.",
    assumptions: ["A deal with no amount at the time counts as zero"],
  });
  const gaps = [...data.unmapped.finalStages, ...data.unmapped.historyStages];
  if (gaps.length > 0) {
    const reason = unmappedReason("stages", [...new Set(gaps)].sort());
    return [notMeasurable(countDefinition, reason), notMeasurable(valueDefinition, reason)];
  }
  if (!data.historyStart) return [notMeasurable(countDefinition, "OpportunityHistory has no rows."), notMeasurable(valueDefinition, "OpportunityHistory has no rows.")];

  const days = monthEnds(ctx.window);
  const countSeries: SeriesPoint[] = [];
  const valueSeries: SeriesPoint[] = [];
  let latestOpen: Array<{ deal: Deal; snapshot: Snapshot }> = [];
  const historyStart = data.historyStart;
  let missingHistory = 0;

  for (const day of days) {
    if (day < historyStart) {
      const reason = `Before the earliest stage history (${historyStart})`;
      countSeries.push({ date: day, value: null, n: 0, notMeasurableReason: reason });
      valueSeries.push({ date: day, value: null, n: 0, notMeasurableReason: reason });
      continue;
    }
    const open: Array<{ deal: Deal; snapshot: Snapshot }> = [];
    let gapped = 0;
    let placed = 0;
    for (const deal of data.deals) {
      if (deal.CreatedDate.slice(0, 10) > day) continue;
      const snapshot = stateAt(deal, day);
      if (!snapshot) {
        // Created by then, with no history: a migrated or bulk-loaded deal we can't place.
        gapped++;
        continue;
      }
      placed++;
      if (data.stages.categoryOf(snapshot.stage) === "open") open.push({ deal, snapshot });
    }
    // Judged month by month. One old row shouldn't make earlier months look
    // measured when most deals that existed then have no history yet.
    if (pct(gapped, gapped + placed) > MAX_MISSING_HISTORY_PCT) {
      const reason = `${gapped} of ${gapped + placed} deals that existed then have no stage history yet`;
      countSeries.push({ date: day, value: null, n: 0, notMeasurableReason: reason });
      valueSeries.push({ date: day, value: null, n: 0, notMeasurableReason: reason });
      continue;
    }
    missingHistory = gapped;
    countSeries.push({ date: day, value: open.length, n: open.length, notMeasurableReason: null });
    const blank = open.filter((o) => o.snapshot.amount === null).length;
    if (pct(blank, open.length) > MAX_MISSING_AMOUNT_PCT) {
      valueSeries.push({ date: day, value: null, n: open.length, notMeasurableReason: `${blank} of ${open.length} open deals had no amount` });
    } else {
      valueSeries.push({ date: day, value: Math.round(open.reduce((sum, o) => sum + (o.snapshot.amount ?? 0), 0)), n: open.length, notMeasurableReason: null });
    }
    latestOpen = open;
  }

  const measuredPoints = countSeries.filter((p) => p.value !== null);
  if (measuredPoints.length === 0) return [notMeasurable(countDefinition, "No month end falls inside the stage history."), notMeasurable(valueDefinition, "No month end falls inside the stage history.")];
  const exclusions = [...data.exclusions, ...(missingHistory ? [`${missingHistory} deals with no stage history by the latest month end`] : [])];
  const range: DateRange = { from: measuredPoints[0]!.date, to: measuredPoints.at(-1)!.date };

  const byStage = (rows: typeof latestOpen, calculate: (group: typeof latestOpen) => number): MetricSegment[] => {
    return [...groupBy(rows, (r) => r.snapshot.stage)].sort(([a], [b]) => a.localeCompare(b)).map(([key, group]) => ({ dimension: "stage", key, n: group.length, value: calculate(group), notMeasurableReason: null }));
  };

  const metrics: Metric[] = [
    measured({ ...countDefinition, exclusions }, { value: measuredPoints.at(-1)!.value!, n: latestOpen.length, dateRange: range, series: countSeries, segments: byStage(latestOpen, (g) => g.length), segmentExemptReason: latestOpen.length === 0 ? "No deals were open at the latest month end." : undefined }),
  ];
  if (data.multiCurrency) metrics.push(notMeasurable(valueDefinition, "The org is multi-currency, and amounts can't be added until historic rates are supplied."));
  else {
    const total = (group: typeof latestOpen): number => Math.round(group.reduce((sum, o) => sum + (o.snapshot.amount ?? 0), 0));
    const latest = valueSeries.find((p) => p.date === range.to)!;
    if (latest.value === null) {
      metrics.push(notMeasurable({ ...valueDefinition, exclusions }, `At ${range.to}: ${latest.notMeasurableReason}.`));
      return metrics;
    }
    metrics.push(measured({ ...valueDefinition, exclusions }, { value: latest.value, n: latestOpen.length, dateRange: range, series: valueSeries, segments: byStage(latestOpen, total), segmentExemptReason: latestOpen.length === 0 ? "No deals were open at the latest month end." : undefined }));
  }
  return metrics;
}

const ALL_METRIC_LABELS: Array<Pick<Base, "id" | "label" | "definition" | "unit">> = [
  { id: "historic.win_rate", label: "Win rate", unit: "percent", definition: "Won deals as a share of won and lost deals." },
  { id: "historic.stage_conversion", label: "Stage conversion", unit: "percent", definition: "Share of deals progressing between stages." },
  { id: "historic.sales_cycle_won", label: "Sales cycle, won deals", unit: "days", definition: "Median days from creation to close." },
  { id: "historic.slippage_rate", label: "Slippage rate", unit: "percent", definition: "Share of deals whose close date moved later." },
  { id: "historic.pipeline_point_in_time_count", label: "Open deals at each month end", unit: "count", definition: "Deals open at each month end." },
];

export function pipelineMetrics(ctx: Ctx): Metric[] {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return ALL_METRIC_LABELS.map((m) => notMeasurable(base(data, m), data.reason));
  return [...winRate(ctx, data), ...stageConversion(ctx, data), ...salesCycle(ctx, data), ...slippage(ctx, data), ...pointInTime(ctx, data)];
}
