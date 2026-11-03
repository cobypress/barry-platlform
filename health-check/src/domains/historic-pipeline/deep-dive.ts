import { pct, type RuleContext } from "../../engine/rule.ts";
import { priorWinsExtractor, type PriorWinRow } from "../../extract/prior-wins.ts";
import type { Sourced } from "../../extract/types.ts";
import type { DateRange, Metric, MetricSegment, SeriesPoint } from "../../schemas/index.ts";
import { quarterBounds, stageRuns } from "../current-pipeline/stages.ts";
import { localDay, todayIn, zoneOf } from "../time-zone.ts";
import { loadPipeline, type Deal, type PipelineData } from "./cohort.ts";
import { base, closedInWindow, dimensions, fiscalQuarter, fiscalStartOf, groupBy, hasOpenStage, MAX_MISSING_AMOUNT_PCT, measured, median, notMeasurable, quantile, round1, segment, SEGMENT_NOTE, unmappedReason, type Base } from "./metrics.ts";
import { closedFromOpen, cohortOf, daysLate, firstForecast, stageWinRates, wonFromOpen } from "./outcomes.ts";

// The deeper pipeline figures: where wins come from, where deals are lost, how
// the business has moved quarter by quarter, and how far close dates hold.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;
type Definition = Pick<Base, "id" | "label" | "definition" | "unit"> & { assumptions?: string[] };

const DAY = 86_400_000;
const QUARTERS = 8;
const TOP_ACCOUNTS = [1, 3, 5, 10];
const MULTI_CURRENCY = "The org is multi-currency, and amounts can't be added until historic rates are supplied.";

const thinReason = (n: number, noun: string, minSample: number): string => `${n} ${noun} is below the calibrated minimum of ${minSample}.`;
const amountOf = (d: Deal): number => d.Amount ?? 0;
const total = (deals: Deal[]): number => Math.round(deals.reduce((sum, d) => sum + amountOf(d), 0));
const tooManyBlank = (deals: Deal[]): boolean => pct(deals.filter((d) => d.Amount === null).length, deals.length) > MAX_MISSING_AMOUNT_PCT;
const wonIn = (ctx: Ctx, data: Ready): Deal[] => data.deals.filter((d) => d.category === "won" && closedInWindow(d, ctx.window));

function stageWinRate(ctx: Ctx, data: Ready): Metric {
  const definition = base(ctx, data, {
    id: "historic.stage_win_rate",
    label: "Win rate from each stage",
    unit: "percent",
    definition: "Of deals closed in the window that were in an open stage at some point, the share that won. Each stage counts the deals that were in it at some point. No decision counts as not won. These rates weight open pipeline.",
  });
  if (data.unmapped.finalStages.length > 0) return notMeasurable(definition, unmappedReason("stages", data.unmapped.finalStages));
  const { minSample } = ctx.calibration;
  const closed = closedFromOpen(ctx, data);
  const neverOpen = data.deals.filter((d) => d.category !== "open" && d.category !== null && closedInWindow(d, ctx.window)).length - closed.length;
  const exclusions = [...definition.exclusions, ...(neverOpen > 0 ? [`${neverOpen} deals with no recorded open stage`] : [])];
  if (closed.length < minSample) return notMeasurable({ ...definition, exclusions }, thinReason(closed.length, "closed deals with stage history", minSample));
  const rates = stageWinRates(ctx, data);
  const stages: MetricSegment[] = [...rates].map(([stage, cohort]) => ({ dimension: "stage", key: stage, n: cohort.n, value: cohort.rate, notMeasurableReason: cohort.rate === null ? `${SEGMENT_NOTE} (${cohort.n} of ${minSample})` : null }));
  const rate = (group: Deal[]): number => cohortOf(group, 0).rate!;
  const { team, "record type": recordType } = dimensions(ctx);
  return measured({ ...definition, exclusions }, { value: rate(closed), n: closed.length, dateRange: ctx.window, segments: [...stages, ...segment(closed, { team: team!, "record type": recordType! }, minSample, rate)], stats: { won: closed.filter((d) => d.category === "won").length } });
}

/** The current fiscal quarter and the ones before it, oldest first. */
export function recentQuarters(today: string, fiscalStart: number, count: number): DateRange[] {
  const quarters: DateRange[] = [];
  let cursor = new Date(`${today}T00:00:00Z`);
  for (let i = 0; i < count; i++) {
    const quarter = quarterBounds(cursor, fiscalStart);
    quarters.unshift(quarter);
    cursor = new Date(new Date(`${quarter.from}T00:00:00Z`).getTime() - DAY);
  }
  return quarters;
}

function quarterlyTrends(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const shared = "The last 8 fiscal quarters, the last of them to date. The value is the latest whole quarter.";
  const definitions: Record<string, Definition> = {
    created: { id: "historic.quarterly_created", label: "Deals created by quarter", unit: "count", definition: `Deals created in each quarter. ${shared}` },
    won: { id: "historic.quarterly_won", label: "Deals won by quarter", unit: "count", definition: `Deals won in each quarter, by when they closed. ${shared}` },
    wonValue: { id: "historic.quarterly_won_value", label: "Won value by quarter", unit: "currency", definition: `Total Amount of deals won in each quarter. ${shared}` },
    winRate: { id: "historic.quarterly_win_rate", label: "Win rate by quarter", unit: "percent", definition: `Won deals as a share of won and lost deals that closed in each quarter. ${shared}` },
    size: { id: "historic.quarterly_median_won_size", label: "Median won deal by quarter", unit: "currency", definition: `Median Amount of deals won in each quarter, leaving out deals with no amount. ${shared}` },
    cycle: { id: "historic.quarterly_median_cycle", label: "Median sales cycle by quarter", unit: "days", definition: `Median days from creation to close for deals won in each quarter. ${shared}` },
  };
  const all = Object.values(definitions).map((d) => base(ctx, data, d));
  const fiscal = fiscalStartOf(ctx);
  if (!fiscal) return all.map((d) => notMeasurable(d, "The org's fiscal year couldn't be read, and the calibration doesn't set one."));
  const zone = zoneOf(ctx);
  const today = todayIn(ctx);
  const quarters = recentQuarters(today, fiscal, QUARTERS);
  const mapped = data.unmapped.finalStages.length === 0;
  const before = (q: DateRange): string | null => (q.from < ctx.window.from ? "Starts before the analysis window" : null);
  const inQuarter = (day: string, q: DateRange): boolean => day >= q.from && day <= q.to;
  const won = (q: DateRange): Deal[] => data.deals.filter((d) => d.category === "won" && inQuarter(d.closedDay, q));
  const usableCycle = (d: Deal): boolean => (d.history.length === 0 || hasOpenStage(d, data)) && (d.closedAt.getTime() - new Date(d.CreatedDate).getTime()) / DAY >= -1;
  const cycleDays = (d: Deal): number => Math.max(0, (d.closedAt.getTime() - new Date(d.CreatedDate).getTime()) / DAY);

  type Point = { value: number | null; n: number; reason: string | null };
  const build = (definition: Base, point: (q: DateRange) => Point, gate: string | null): Metric => {
    if (gate) return notMeasurable(definition, gate);
    const series: SeriesPoint[] = quarters.map((q) => {
      const early = before(q);
      if (early) return { date: q.from, value: null, n: 0, notMeasurableReason: early };
      const p = point(q);
      return { date: q.from, value: p.value, n: p.n, notMeasurableReason: p.value === null ? p.reason : null };
    });
    // The current quarter is still running, so the headline is the last whole one.
    const latest = series.slice(0, -1).filter((p) => p.value !== null).at(-1);
    if (!latest) return notMeasurable(definition, "No whole quarter in the last 8 could be measured.");
    return measured(definition, {
      value: latest.value!,
      n: latest.n,
      dateRange: { from: quarters[0]!.from, to: today },
      series,
      segmentExemptReason: "Shown by quarter. Team, rep and record type cuts are in the win rate and sales cycle metrics.",
    });
  };
  const thin = (n: number, noun: string): Point => ({ value: null, n, reason: `${n} ${noun} is below the calibrated minimum of ${minSample}` });
  const [created, wonCount, wonValue, winRate, size, cycle] = all as [Base, Base, Base, Base, Base, Base];
  const unmapped = mapped ? null : unmappedReason("stages", data.unmapped.finalStages);
  return [
    build(created, (q) => {
      const n = data.deals.filter((d) => inQuarter(localDay(d.CreatedDate, zone), q)).length;
      return { value: n, n, reason: null };
    }, null),
    build(wonCount, (q) => ({ value: won(q).length, n: won(q).length, reason: null }), unmapped),
    build(wonValue, (q) => {
      const rows = won(q);
      if (rows.length > 0 && tooManyBlank(rows)) return { value: null, n: rows.length, reason: `${rows.filter((d) => d.Amount === null).length} of ${rows.length} won deals have no amount` };
      return { value: total(rows), n: rows.length, reason: null };
    }, unmapped ?? (data.multiCurrency ? MULTI_CURRENCY : null)),
    build(winRate, (q) => {
      const decided = data.deals.filter((d) => (d.category === "won" || d.category === "lost") && inQuarter(d.closedDay, q));
      return decided.length < minSample ? thin(decided.length, "won and lost deals") : { value: pct(decided.filter((d) => d.category === "won").length, decided.length), n: decided.length, reason: null };
    }, unmapped),
    build(size, (q) => {
      const amounts = won(q).filter((d) => (d.Amount ?? 0) > 0).map(amountOf);
      return amounts.length < minSample ? thin(amounts.length, "won deals with an amount") : { value: Math.round(median(amounts)), n: amounts.length, reason: null };
    }, unmapped ?? (data.multiCurrency ? MULTI_CURRENCY : null)),
    build(cycle, (q) => {
      const rows = won(q).filter(usableCycle);
      return rows.length < minSample ? thin(rows.length, "won deals") : { value: round1(median(rows.map(cycleDays))), n: rows.length, reason: null };
    }, unmapped),
  ];
}

/** A won deal is a repeat when its account had already won before it, in the window or earlier. */
export function repeatFlags(won: Deal[], earlier: Array<{ Id: string; AccountId: string; day: string }>): Map<string, boolean> {
  const firsts = new Map<string, { day: string; id: string }>();
  const consider = (account: string, day: string, id: string): void => {
    const known = firsts.get(account);
    if (!known || day < known.day || (day === known.day && id < known.id)) firsts.set(account, { day, id });
  };
  for (const w of earlier) consider(w.AccountId, w.day, w.Id);
  for (const d of won) if (d.AccountId) consider(d.AccountId, d.closedDay, d.Id);
  return new Map(won.filter((d) => d.AccountId).map((d) => [d.Id, firsts.get(d.AccountId!)!.id !== d.Id]));
}

function priorWins(ctx: Ctx): Sourced<PriorWinRow[]> | null {
  try {
    return ctx.data(priorWinsExtractor);
  } catch {
    // A run from before this step existed. It's treated like a read that failed.
    return null;
  }
}

function winSources(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const definitions = {
    customer: base(ctx, data, { id: "historic.won_value_by_customer", label: "Won value, new and repeat customers", unit: "currency", definition: "Total Amount of deals won in the window, split by whether the account was a new customer or had won before, in the window or earlier." }),
    repeat: base(ctx, data, { id: "historic.repeat_win_share", label: "Won value from repeat customers", unit: "percent", definition: "Amount of deals won in the window on accounts that had already won before, as a share of all won Amount on accounts." }),
    type: base(ctx, data, { id: "historic.won_value_by_type", label: "Won value by type", unit: "currency", definition: "Total Amount of deals won in the window, by the deal's Type." }),
    source: base(ctx, data, { id: "historic.won_value_by_source", label: "Won value by lead source", unit: "currency", definition: "Total Amount of deals won in the window, by the deal's Lead Source." }),
    top: base(ctx, data, { id: "historic.top_account_share", label: "Won value from the top 10 accounts", unit: "percent", definition: "Amount of deals won in the window on the 10 accounts with the most, as a share of all won Amount on accounts. Accounts are shown as tokens, never names." }),
  };
  const every = Object.values(definitions);
  if (data.unmapped.finalStages.length > 0) return every.map((d) => notMeasurable(d, unmappedReason("stages", data.unmapped.finalStages)));
  if (data.multiCurrency) return every.map((d) => notMeasurable(d, MULTI_CURRENCY));
  const won = wonIn(ctx, data);
  if (won.length < minSample) return every.map((d) => notMeasurable(d, thinReason(won.length, "won deals", minSample)));
  const blank = won.filter((d) => d.Amount === null).length;
  if (tooManyBlank(won)) return every.map((d) => notMeasurable(d, `${blank} of ${won.length} won deals have no amount, so shares of won value would mislead.`));
  const blankNote = blank > 0 ? [`${blank} won deals with no amount count as zero`] : [];
  const withAccount = won.filter((d) => d.AccountId);
  const noAccount = won.length - withAccount.length;
  const accountExclusions = noAccount > 0 ? [`${noAccount} won deals with no account`] : [];
  const metrics: Metric[] = [];

  const prior = priorWins(ctx);
  if (!prior || prior.status !== "ok") {
    const reason = prior ? `Wins before the window couldn't be read, so a first win can't be told from a repeat: ${prior.reason}` : "Wins before the window weren't read in this run, so a first win can't be told from a repeat.";
    metrics.push(notMeasurable(definitions.customer, reason), notMeasurable(definitions.repeat, reason));
  } else {
    // A deal can be in both reads if it closed before the window and was edited inside it.
    const inExtract = new Set(data.deals.map((d) => d.Id));
    const earlier = [
      ...prior.data.filter((w) => !inExtract.has(w.Id)).map((w) => ({ Id: w.Id, AccountId: w.AccountId, day: w.CloseDate })),
      ...data.deals.filter((d) => d.category === "won" && d.AccountId && d.closedDay < ctx.window.from).map((d) => ({ Id: d.Id, AccountId: d.AccountId!, day: d.closedDay })),
    ];
    const flags = repeatFlags(withAccount, earlier);
    const kind = (d: Deal): string => (flags.get(d.Id) ? "Repeat customer" : "New customer");
    const evidence = [...definitions.customer.evidence, ctx.ev(prior.source, { count: prior.data.length, note: "Wins before the analysis window: ids, accounts and dates only" })];
    const assumptions = [...definitions.customer.assumptions, "A won deal is from a repeat customer when its account had already won a deal, in the window or before it", ...blankNote];
    const byKind = [...groupBy(withAccount, kind)].sort(([a], [b]) => a.localeCompare(b));
    metrics.push(measured({ ...definitions.customer, evidence, assumptions, exclusions: [...definitions.customer.exclusions, ...accountExclusions] }, { value: total(withAccount), n: withAccount.length, dateRange: ctx.window, segments: byKind.map(([key, group]) => ({ dimension: "customer", key, n: group.length, value: total(group), notMeasurableReason: null })), stats: { repeatWins: withAccount.filter((d) => flags.get(d.Id)).length, newWins: withAccount.filter((d) => !flags.get(d.Id)).length, accounts: new Set(withAccount.map((d) => d.AccountId)).size } }));
    const share = (group: Deal[]): number | null => {
      const sum = total(group);
      return sum === 0 ? null : pct(total(group.filter((d) => flags.get(d.Id))), sum);
    };
    const value = share(withAccount);
    if (value === null) metrics.push(notMeasurable({ ...definitions.repeat, evidence }, "Won deals on accounts have no amounts."));
    else {
      const { team, "record type": recordType } = dimensions(ctx);
      metrics.push(measured({ ...definitions.repeat, evidence, assumptions, exclusions: [...definitions.repeat.exclusions, ...accountExclusions] }, { value, n: withAccount.length, dateRange: ctx.window, segments: segment(withAccount, { team: team!, "record type": recordType! }, minSample, share), stats: { repeatWins: withAccount.filter((d) => flags.get(d.Id)).length } }));
    }
  }

  for (const [definition, field, noun] of [[definitions.type, "Type", "type"], [definitions.source, "LeadSource", "lead source"]] as const) {
    const filled = won.filter((d) => d[field]);
    if (filled.length === 0) {
      metrics.push(notMeasurable(definition, `No won deal has a ${noun}, so wins can't be split by it.`));
      continue;
    }
    const keyOf = (d: Deal): string => d[field] ?? `No ${noun}`;
    const groups = [...groupBy(won, keyOf)].sort(([, a], [, b]) => total(b) - total(a));
    metrics.push(measured({ ...definition, assumptions: [...definition.assumptions, ...blankNote] }, { value: total(won), n: won.length, dateRange: ctx.window, segments: groups.map(([key, group]) => ({ dimension: noun, key, n: group.length, value: total(group), notMeasurableReason: null })), stats: { filled: filled.length } }));
  }

  const byAccount = [...groupBy(withAccount, (d) => d.AccountId!)].map(([id, group]) => ({ id, amount: total(group), deals: group.length })).sort((a, b) => b.amount - a.amount || a.id.localeCompare(b.id));
  const accountTotal = byAccount.reduce((sum, a) => sum + a.amount, 0);
  if (accountTotal === 0) metrics.push(notMeasurable(definitions.top, "Won deals on accounts have no amounts."));
  else {
    const top = Math.min(10, byAccount.length);
    const cumulative: MetricSegment[] = TOP_ACCOUNTS.filter((k) => k <= byAccount.length).map((k) => ({ dimension: "accounts", key: k === 1 ? "Top account" : `Top ${k} accounts`, n: byAccount.slice(0, k).reduce((sum, a) => sum + a.deals, 0), value: pct(byAccount.slice(0, k).reduce((sum, a) => sum + a.amount, 0), accountTotal), notMeasurableReason: null }));
    // The name isn't read, so a token restores to the account's id for the consultant to look up.
    const accounts: MetricSegment[] = byAccount.slice(0, top).map((a) => ({ dimension: "account", key: ctx.pseudonyms.label("account", a.id, a.id), n: a.deals, value: pct(a.amount, accountTotal), notMeasurableReason: null }));
    metrics.push(measured({ ...definitions.top, assumptions: [...definitions.top.assumptions, ...blankNote], exclusions: [...definitions.top.exclusions, ...accountExclusions] }, { value: pct(byAccount.slice(0, top).reduce((sum, a) => sum + a.amount, 0), accountTotal), n: withAccount.length, dateRange: ctx.window, segments: [...cumulative, ...accounts], stats: { accounts: byAccount.length, top } }));
  }
  return metrics;
}

/** The last open stage before the deal's final close. */
export function stageLeftFrom(deal: Deal, data: Ready): string | null {
  let i = deal.history.length - 1;
  while (i >= 0 && data.stages.categoryOf(deal.history[i]!.StageName) !== "open") i--;
  return i >= 0 ? deal.history[i]!.StageName : null;
}

function lostFrom(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const countDefinition = base(ctx, data, { id: "historic.lost_by_stage", label: "Lost deals by the stage they left", unit: "count", definition: "Lost and no decision deals closed in the window, by the last open stage they were in before they closed. Also split by outcome, and by the stage they closed in where there's more than one." });
  const valueDefinition = base(ctx, data, { id: "historic.lost_value_by_stage", label: "Lost value by the stage it left", unit: "currency", definition: "Total Amount of lost and no decision deals closed in the window, by the last open stage they were in." });
  if (data.unmapped.finalStages.length > 0) return [countDefinition, valueDefinition].map((d) => notMeasurable(d, unmappedReason("stages", data.unmapped.finalStages)));
  const all = data.deals.filter((d) => (d.category === "lost" || d.category === "no_decision") && closedInWindow(d, ctx.window));
  const lost = all.filter((d) => stageLeftFrom(d, data) !== null);
  const neverOpen = all.length - lost.length;
  const exclusions = [...countDefinition.exclusions, ...(neverOpen > 0 ? [`${neverOpen} deals with no recorded open stage`] : [])];
  if (lost.length < minSample) return [countDefinition, valueDefinition].map((d) => notMeasurable({ ...d, exclusions }, thinReason(lost.length, "lost and no decision deals with stage history", minSample)));
  const order = (stage: string): number => data.stages.orderOf(stage) ?? Number.MAX_SAFE_INTEGER;
  const byStage = [...groupBy(lost, (d) => stageLeftFrom(d, data)!)].sort(([a], [b]) => order(a) - order(b) || a.localeCompare(b));
  const closedStages = ctx.calibration.stages.filter((s) => s.category === "lost" || s.category === "no_decision");
  const outcome = (d: Deal): string => (d.category === "lost" ? "Lost" : "No decision");
  const countSegments: MetricSegment[] = [
    ...byStage.map(([key, group]) => ({ dimension: "stage left", key, n: group.length, value: group.length, notMeasurableReason: null })),
    ...[...groupBy(lost, outcome)].sort(([a], [b]) => a.localeCompare(b)).map(([key, group]) => ({ dimension: "outcome", key, n: group.length, value: group.length, notMeasurableReason: null })),
    // Only worth showing where the customer closes deals in more than one way.
    ...(closedStages.length > 1 ? [...groupBy(lost, (d) => d.StageName)].sort(([, a], [, b]) => b.length - a.length).map(([key, group]) => ({ dimension: "closed as", key, n: group.length, value: group.length, notMeasurableReason: null })) : []),
  ];
  const metrics = [measured({ ...countDefinition, exclusions }, { value: lost.length, n: lost.length, dateRange: ctx.window, segments: countSegments })];
  if (data.multiCurrency) metrics.push(notMeasurable({ ...valueDefinition, exclusions }, MULTI_CURRENCY));
  else if (tooManyBlank(lost)) metrics.push(notMeasurable({ ...valueDefinition, exclusions }, `${lost.filter((d) => d.Amount === null).length} of ${lost.length} lost deals have no amount, so the value lost would mislead.`));
  else {
    const blank = lost.filter((d) => d.Amount === null).length;
    metrics.push(measured({ ...valueDefinition, exclusions, assumptions: [...valueDefinition.assumptions, ...(blank ? [`${blank} lost deals with no amount count as zero`] : [])] }, { value: total(lost), n: lost.length, dateRange: ctx.window, segments: byStage.map(([key, group]) => ({ dimension: "stage left", key, n: group.length, value: total(group), notMeasurableReason: null })) }));
  }
  return metrics;
}

function closeDateReliability(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const shareDefinition = base(ctx, data, { id: "historic.close_date_reliability", label: "Won in the quarter first forecast", unit: "percent", definition: "Won deals closed in the window that closed in the fiscal quarter of the first close date they were given, as a share of won deals with stage history." });
  const lateDefinition = base(ctx, data, { id: "historic.close_days_late", label: "Days late against the first close date", unit: "days", definition: "Median days from the first close date a won deal was given to the day it closed. A negative figure means early." });
  if (data.unmapped.finalStages.length > 0) return [shareDefinition, lateDefinition].map((d) => notMeasurable(d, unmappedReason("stages", data.unmapped.finalStages)));
  const won = wonFromOpen(ctx, data);
  const neverOpen = wonIn(ctx, data).length - won.length;
  const exclusions = [...shareDefinition.exclusions, ...(neverOpen > 0 ? [`${neverOpen} won deals with no recorded open stage`] : [])];
  if (won.length < minSample) return [shareDefinition, lateDefinition].map((d) => notMeasurable({ ...d, exclusions }, thinReason(won.length, "won deals with stage history", minSample)));
  const dims = dimensions(ctx);
  delete dims["closed quarter"];
  const metrics: Metric[] = [];
  const fiscal = fiscalStartOf(ctx);
  if (!fiscal) metrics.push(notMeasurable({ ...shareDefinition, exclusions }, "The org's fiscal year couldn't be read, and the calibration doesn't set one."));
  else {
    const quarter = (day: string): string => fiscalQuarter(new Date(`${day}T00:00:00Z`), fiscal);
    const onTime = (group: Deal[]): number => pct(group.filter((d) => quarter(d.closedDay) === quarter(firstForecast(d))).length, group.length);
    metrics.push(measured({ ...shareDefinition, exclusions }, { value: onTime(won), n: won.length, dateRange: ctx.window, segments: segment(won, dims, minSample, onTime), stats: { inQuarter: won.filter((d) => quarter(d.closedDay) === quarter(firstForecast(d))).length } }));
  }
  const days = won.map(daysLate);
  metrics.push(measured({ ...lateDefinition, exclusions }, { value: round1(median(days)), n: won.length, dateRange: ctx.window, segments: segment(won, dims, minSample, (group) => round1(median(group.map(daysLate)))), stats: { p25: round1(quantile(days, 0.25)), p75: round1(quantile(days, 0.75)), lateDeals: days.filter((d) => d > 0).length } }));
  return metrics;
}

/** Total days a deal spent in each open stage, over every run of it. */
export function daysByStage(deal: Deal, data: Ready): Map<string, number> {
  const out = new Map<string, number>();
  for (const run of stageRuns(deal)) if (data.stages.categoryOf(run.stage) === "open") out.set(run.stage, (out.get(run.stage) ?? 0) + run.days);
  return out;
}

function stageVelocity(ctx: Ctx, data: Ready): Metric[] {
  const { minSample } = ctx.calibration;
  const make = (outcome: "won" | "lost", id: string, label: string, phrase: string): Metric => {
    const definition = base(ctx, data, { id, label, unit: "days", definition: `Median days ${phrase} deals closed in the window spent in open stages in total. Each stage segment is the median days in that stage, for deals that were in it.` });
    if (data.unmapped.finalStages.length > 0) return notMeasurable(definition, unmappedReason("stages", data.unmapped.finalStages));
    const deals = closedFromOpen(ctx, data).filter((d) => (outcome === "won" ? d.category === "won" : d.category === "lost" || d.category === "no_decision"));
    if (deals.length < minSample) return notMeasurable(definition, thinReason(deals.length, `${phrase} deals with stage history`, minSample));
    const timings = new Map(deals.map((d) => [d.Id, daysByStage(d, data)]));
    const totalDays = (d: Deal): number => [...timings.get(d.Id)!.values()].reduce((a, b) => a + b, 0);
    const segments: MetricSegment[] = data.stages.openStages.map((stage) => {
      const days = deals.flatMap((d) => (timings.get(d.Id)!.has(stage) ? [timings.get(d.Id)!.get(stage)!] : []));
      const thin = days.length < minSample;
      return { dimension: "stage", key: stage, n: days.length, value: thin ? null : round1(median(days)), notMeasurableReason: thin ? `${SEGMENT_NOTE} (${days.length} of ${minSample})` : null };
    });
    return measured(definition, { value: round1(median(deals.map(totalDays))), n: deals.length, dateRange: ctx.window, segments });
  };
  return [make("won", "historic.stage_velocity_won", "Time in each stage, won deals", "won"), make("lost", "historic.stage_velocity_lost", "Time in each stage, lost deals", "lost and no decision")];
}

const LABELS: Definition[] = [
  { id: "historic.stage_win_rate", label: "Win rate from each stage", unit: "percent", definition: "Share of deals that won, by the open stages they were in." },
  { id: "historic.quarterly_won_value", label: "Won value by quarter", unit: "currency", definition: "Total Amount won in each quarter." },
  { id: "historic.won_value_by_customer", label: "Won value, new and repeat customers", unit: "currency", definition: "Won Amount split by new and repeat customers." },
  { id: "historic.lost_by_stage", label: "Lost deals by the stage they left", unit: "count", definition: "Lost deals by the last open stage they were in." },
  { id: "historic.close_date_reliability", label: "Won in the quarter first forecast", unit: "percent", definition: "Share of won deals closed in the quarter first forecast." },
  { id: "historic.stage_velocity_won", label: "Time in each stage, won deals", unit: "days", definition: "Median days won deals spent in each stage." },
];

export function deepDiveMetrics(ctx: Ctx): Metric[] {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return LABELS.map((m) => notMeasurable(base(ctx, data, m), data.reason));
  return [stageWinRate(ctx, data), ...quarterlyTrends(ctx, data), ...winSources(ctx, data), ...lostFrom(ctx, data), ...closeDateReliability(ctx, data), ...stageVelocity(ctx, data)];
}
