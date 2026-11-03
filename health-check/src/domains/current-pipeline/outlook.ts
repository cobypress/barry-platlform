import type { RuleContext } from "../../engine/rule.ts";
import { stagesExtractor } from "../../extract/opportunities.ts";
import type { Evidence, Metric, MetricSegment, SeriesPoint } from "../../schemas/index.ts";
import type { Deal, PipelineData } from "../historic-pipeline/cohort.ts";
import { median, round1, segment, SEGMENT_NOTE } from "../historic-pipeline/metrics.ts";
import { daysLate, firstForecast, stageWinRates, wonFromOpen, type Cohort } from "../historic-pipeline/outcomes.ts";
import { todayIn, zoneNote } from "../time-zone.ts";

// Today's open pipeline by stage, and what it's likely to turn into by month.
// The outlook is written for whoever has to staff the work once it's won, so it
// is a weighted expectation by month, not a sales forecast by rep.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

const DOMAIN = "current_pipeline";
const DAY = 86_400_000;
const OUTLOOK_MONTHS = 6;
// Today's open deals are every deal, not a sample, so a stage's median age
// needs fewer of them than a rate does. Below this, one deal moves it too far.
const MIN_STAGE_DEALS = 10;
const MULTI_CURRENCY = "The org is multi-currency, and amounts can't be added until historic rates are supplied.";

const hasAmount = (d: Deal): boolean => (d.Amount ?? 0) > 0;
const ageDays = (d: Deal, now: Date): number => (now.getTime() - new Date(d.CreatedDate).getTime()) / DAY;
const addDays = (day: string, days: number): string => new Date(new Date(`${day}T00:00:00Z`).getTime() + Math.round(days) * DAY).toISOString().slice(0, 10);

/** The calendar month a day falls in, and the months after it. */
export function monthsFrom(today: string, count: number): string[] {
  const [year, month] = today.split("-").map(Number);
  return Array.from({ length: count }, (_, i) => new Date(Date.UTC(year!, month! - 1 + i, 1)).toISOString().slice(0, 10));
}

/** Inactive stages that still hold open deals. A retired stage often has no history to weight it by. */
export function retiredStages(ctx: Ctx, deals: Deal[]): MetricSegment[] {
  const stages = ctx.data(stagesExtractor);
  if (stages.status !== "ok") return [];
  const inactive = new Set(stages.data.filter((s) => !s.IsActive).map((s) => s.ApiName));
  const counts = new Map<string, number>();
  for (const d of deals) if (inactive.has(d.StageName)) counts.set(d.StageName, (counts.get(d.StageName) ?? 0) + 1);
  return [...counts].sort(([a], [b]) => a.localeCompare(b)).map(([key, n]) => ({ dimension: "retired stage", key, n, value: n, notMeasurableReason: null }));
}

/** The expected close of an open deal: its current date, or its first date moved by the org's median lateness, whichever is later. */
export const expectedClose = (deal: Deal, lateDays: number): string => {
  const shifted = addDays(firstForecast(deal), lateDays);
  return shifted > deal.CloseDate ? shifted : deal.CloseDate;
};

export function openStageMetrics(ctx: Ctx, data: Ready, deals: Deal[], evidence: Evidence[]): Metric[] {
  const { minSample } = ctx.calibration;
  const today = todayIn(ctx);
  const day = { from: ctx.window.to, to: ctx.window.to };
  const shared = { domain: DOMAIN, evidence, assumptions: [zoneNote(ctx)], exclusions: data.exclusions };
  const byStage = { stage: (d: Deal) => d.StageName };
  const dims: Record<string, (d: Deal) => string> = { ...byStage, "record type": (d) => d.RecordType ?? "No record type", team: (d) => d.team };
  const rest = { dateRange: day, series: [] as SeriesPoint[], stats: {} as Record<string, number>, segmentExemptReason: deals.length === 0 ? "There are no open deals." : null };
  const count = (group: Deal[]): number => group.length;
  const definitions = {
    noAmount: { id: "current.open_no_amount", label: "Open deals with no amount", unit: "count" as const, definition: "Open deals today with no Amount, or zero. We never impute one." },
    pastClose: { id: "current.open_past_close", label: "Open deals past their close date", unit: "count" as const, definition: "Open deals today whose close date has passed." },
    age: { id: "current.open_median_age", label: "Age of open deals", unit: "days" as const, definition: `Median days since open deals were created. A stage needs ${MIN_STAGE_DEALS} open deals for its own median.` },
    weighted: {
      id: "current.weighted_pipeline",
      label: "Weighted open pipeline",
      unit: "currency" as const,
      definition: "Each open deal's Amount times the win rate from its stage. Stages with too few closed deals aren't weighted.",
    },
  };
  const metrics: Metric[] = [
    { ...definitions.noAmount, ...shared, status: "measured", value: deals.filter((d) => !hasAmount(d)).length, n: deals.length, ...rest, segments: segment(deals, dims, 1, (group) => group.filter((d) => !hasAmount(d)).length) },
    { ...definitions.pastClose, ...shared, status: "measured", value: deals.filter((d) => d.CloseDate < today).length, n: deals.length, ...rest, segments: segment(deals, dims, 1, (group) => group.filter((d) => d.CloseDate < today).length) },
  ];
  const age = (group: Deal[]): number => round1(median(group.map((d) => ageDays(d, ctx.now))));
  if (deals.length < minSample) metrics.push({ ...definitions.age, ...shared, status: "not_measurable", reason: `${deals.length} open deals is below the calibrated minimum of ${minSample}.` });
  else metrics.push({ ...definitions.age, ...shared, status: "measured", value: age(deals), n: deals.length, ...rest, segments: segment(deals, dims, MIN_STAGE_DEALS, age) });

  const rates = stageWinRates(ctx, data);
  if (data.multiCurrency) metrics.push({ ...definitions.weighted, ...shared, status: "not_measurable", reason: MULTI_CURRENCY });
  else {
    const weighted = weightedPipeline(deals, rates, minSample);
    const exclusions = [...data.exclusions, ...weighted.unweighted];
    if (weighted.stages === 0) metrics.push({ ...definitions.weighted, ...shared, exclusions, status: "not_measurable", reason: `No stage with open deals has ${minSample} closed deals to weight it by.` });
    else metrics.push({ ...definitions.weighted, ...shared, exclusions, status: "measured", value: weighted.value, n: weighted.n, ...rest, segments: weighted.segments, stats: { stagesWeighted: weighted.stages } });
  }
  return metrics;
}

function weightedPipeline(deals: Deal[], rates: Map<string, Cohort>, minSample: number): { value: number; n: number; stages: number; segments: MetricSegment[]; unweighted: string[] } {
  const segments: MetricSegment[] = [];
  const unweighted: string[] = [];
  let value = 0;
  let n = 0;
  let stages = 0;
  const groups = new Map<string, Deal[]>();
  for (const d of deals) groups.set(d.StageName, [...(groups.get(d.StageName) ?? []), d]);
  for (const [stage, group] of [...groups].sort(([a], [b]) => a.localeCompare(b))) {
    const cohort = rates.get(stage);
    if (!cohort || cohort.rate === null) {
      const closed = cohort?.n ?? 0;
      segments.push({ dimension: "stage", key: stage, n: group.length, value: null, notMeasurableReason: `${SEGMENT_NOTE}: ${closed} closed deals were in this stage, of ${minSample}` });
      unweighted.push(`${group.length} open deals in ${stage}, where too few closed deals were in the stage to weight it`);
      continue;
    }
    const weighted = (group.reduce((sum, d) => sum + (d.Amount ?? 0), 0) * cohort.rate) / 100;
    segments.push({ dimension: "stage", key: stage, n: group.length, value: Math.round(weighted), notMeasurableReason: null });
    value += weighted;
    n += group.length;
    stages++;
  }
  return { value: Math.round(value), n, stages, segments, unweighted };
}

export function deliveryOutlook(ctx: Ctx, data: Ready, deals: Deal[], evidence: Evidence[]): Metric[] {
  const { minSample, currency } = ctx.calibration;
  const today = todayIn(ctx);
  const months = monthsFrom(today, OUTLOOK_MONTHS);
  const last = months.at(-1)!;
  const endOfOutlook = new Date(Date.UTC(Number(last.slice(0, 4)), Number(last.slice(5, 7)), 0)).toISOString().slice(0, 10);
  const method = "Each open deal counts at the win rate from its stage. It lands no earlier than its close date, and no earlier than its first close date moved by your median days late on won deals. A deal whose expected date has already passed has no month";
  const shared = { domain: DOMAIN, evidence, exclusions: data.exclusions, assumptions: [zoneNote(ctx), method] };
  const definitions = {
    value: { id: "current.delivery_outlook_value", label: "Expected won value by month", unit: "currency" as const, definition: `Open pipeline weighted by stage win rates, in the month each deal is expected to close. The value is the total for the months shown.` },
    deals: { id: "current.delivery_outlook_deals", label: "Expected won deals by month", unit: "count" as const, definition: "The same outlook in deals. Each counts as its stage win rate, so the figure can be a fraction." },
  };
  const won = wonFromOpen(ctx, data);
  const gate = data.unmapped.finalStages.length > 0 ? `The calibration doesn't say what these stages mean: ${data.unmapped.finalStages.join(", ")}.` : won.length < minSample ? `${won.length} won deals with stage history is below the calibrated minimum of ${minSample}, so we can't say how late close dates run.` : null;
  if (gate) return [{ ...definitions.value, ...shared, status: "not_measurable", reason: gate }, { ...definitions.deals, ...shared, status: "not_measurable", reason: gate }];

  const late = round1(median(won.map(daysLate)));
  const rates = stageWinRates(ctx, data);
  const weighted = deals.filter((d) => typeof rates.get(d.StageName)?.rate === "number");
  const unweighted = deals.length - weighted.length;
  const exclusions = [...data.exclusions, ...(unweighted ? [`${unweighted} open deals in stages with too few closed deals to weight`] : [])];
  const chance = (d: Deal): number => rates.get(d.StageName)!.rate! / 100;
  const timing = (d: Deal): string => {
    const expected = expectedClose(d, late);
    if (expected < today) return "past";
    return expected > endOfOutlook ? "later" : `${expected.slice(0, 7)}-01`;
  };
  const placed = new Map<string, Deal[]>();
  for (const d of weighted) placed.set(timing(d), [...(placed.get(timing(d)) ?? []), d]);
  const valueOf = (group: Deal[]): number => Math.round(group.reduce((sum, d) => sum + chance(d) * (d.Amount ?? 0), 0));
  const dealsOf = (group: Deal[]): number => round1(group.reduce((sum, d) => sum + chance(d), 0));
  const timingKeys: Array<[string, string]> = [["past", "Expected date already passed"], ["later", "Expected after the outlook"]];
  const assumptions = [...shared.assumptions, `Your won deals closed a median of ${late} days after their first close date`];
  const stats = { lateDays: late, months: OUTLOOK_MONTHS };
  const within = months.flatMap((m) => placed.get(m) ?? []);
  if (weighted.length === 0) {
    const reason = `No open deal is in a stage with ${minSample} closed deals to weight it by.`;
    return [{ ...definitions.value, ...shared, exclusions, status: "not_measurable", reason }, { ...definitions.deals, ...shared, exclusions, status: "not_measurable", reason }];
  }
  const build = (calculate: (group: Deal[]) => number) => ({
    value: calculate(within),
    n: within.length,
    dateRange: { from: months[0]!, to: endOfOutlook },
    series: months.map((m) => ({ date: m, value: calculate(placed.get(m) ?? []), n: (placed.get(m) ?? []).length, notMeasurableReason: null })),
    segments: [
      ...timingKeys.map(([key, label]) => ({ dimension: "timing", key: label, n: (placed.get(key) ?? []).length, value: calculate(placed.get(key) ?? []), notMeasurableReason: null })),
      ...segment(within, { stage: (d) => d.StageName, team: (d) => d.team }, 1, calculate),
    ],
    segmentExemptReason: null,
    stats,
  });
  const priced = weighted.filter(hasAmount);
  const noAmount = weighted.length - priced.length;
  const metrics: Metric[] = [];
  if (data.multiCurrency) metrics.push({ ...definitions.value, ...shared, exclusions, status: "not_measurable", reason: MULTI_CURRENCY });
  else metrics.push({ ...definitions.value, ...shared, exclusions: [...exclusions, ...(noAmount ? [`${noAmount} weighted open deals with no amount add nothing to value`] : [])], assumptions: [...assumptions, `Amounts in ${currency}`], status: "measured", ...build(valueOf) });
  metrics.push({ ...definitions.deals, ...shared, exclusions, assumptions, status: "measured", ...build(dealsOf) });
  return metrics;
}

/** What each of these metrics is, for a run where open deals couldn't be read. */
export const OUTLOOK_LABELS = [
  { id: "current.open_no_amount", label: "Open deals with no amount", unit: "count" as const, definition: "Open deals today with no Amount, or an Amount of zero." },
  { id: "current.open_past_close", label: "Open deals past their close date", unit: "count" as const, definition: "Open deals today whose close date has passed." },
  { id: "current.open_median_age", label: "Age of open deals", unit: "days" as const, definition: "Median days since open deals were created." },
  { id: "current.weighted_pipeline", label: "Weighted open pipeline", unit: "currency" as const, definition: "Amount of each open deal times the win rate from its stage." },
  { id: "current.delivery_outlook_value", label: "Expected won value by month", unit: "currency" as const, definition: "Open pipeline weighted by stage win rates, by the month each deal is expected to close." },
  { id: "current.delivery_outlook_deals", label: "Expected won deals by month", unit: "count" as const, definition: "The same outlook as a number of deals." },
];

