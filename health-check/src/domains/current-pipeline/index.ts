import { money } from "../../render/format.ts";
import { schemaExtractor } from "../../extract/configuration.ts";
import { quotasExtractor, revenueForecastType } from "../../extract/forecast.ts";
import { nextStepsExtractor, opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { exposure } from "../data-quality/index.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { potentialRevenue, SITUATIONS, type Situation } from "../historic-pipeline/potential.ts";
import { fiscalStartOf, median, pushesOf, quantile, segment } from "../historic-pipeline/metrics.ts";
import type { DateRange, Evidence, Metric } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { localDay, todayIn, zoneNote, zoneOf } from "../time-zone.ts";
import { daysSince, enteredCurrentStage, quarterBounds, stageRuns } from "./stages.ts";
import { deliveryOutlook, openStageMetrics, OUTLOOK_LABELS, retiredStages } from "./outlook.ts";

const DOMAIN = "current_pipeline";
const MIN_SEGMENT = 10;
const STUCK_PERCENTILE = 0.75;
const QUIET_DAYS = 30;
const REPEAT_PUSHES = 2;
const NEXT_STEP_LIMIT = 30;
const NEXT_STEP_SEGMENT_LIMIT = 60;
const ACCOUNT_SHARE_LIMIT = 25;
const REP_SHARE_LIMIT = 40;
const MIN_REPS = 3;
const MAX_BLANK_AMOUNT_SHARE = 20;
const COVERAGE_TARGET = 3;

type Ready = Extract<ReturnType<typeof loadPipeline>, { status: "ok" }>;

function openDeals(ctx: RuleContext | Omit<RuleContext, "metrics">): { data: Ready; deals: Deal[] } | NotMeasurableResult {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
  if (data.unmapped.finalStages.length > 0) return { outcome: "not_measurable", reason: `The calibration doesn't say whether these stages are open: ${data.unmapped.finalStages.join(", ")}.`, evidence: data.evidence };
  return { data, deals: data.deals.filter((d) => d.category === "open") };
}

const dimensions = (ctx: RuleContext): Record<string, (d: Deal) => string> => ({
  stage: (d) => d.StageName,
  "record type": (d) => d.RecordType ?? "No record type",
  team: (d) => d.team,
  rep: (d) => ctx.userLabel(d.OwnerId),
});

/** `problem` is an affected deal's line in the action list, in words that hold no name. */
export type Scoped = { deals: Deal[]; isHit: (d: Deal) => boolean; problem?: (d: Deal) => string; exclusions?: string[]; evidence?: Evidence[] } | { reason: string; evidence?: Evidence[] };

type ShareRule = {
  id: string;
  /** Defaults to current pipeline. Forecast rules share this shape. */
  domain?: string;
  title: string;
  totalLimit: number;
  segmentLimit: number;
  severity: "high" | "medium" | "low";
  scope: (ctx: RuleContext, data: Ready, open: Deal[]) => Scoped;
  scopeLabel: string;
  issueTitle: string;
  sentence: (hits: number, n: number, rate: number) => string;
  recommendation: string;
  owner: string;
  /** True sizes the raw exposure. A situation sizes potential revenue at how often comparable deals still won. */
  sized: boolean | ((ctx: RuleContext, data: Ready) => Situation);
  /** What to do to each affected deal, for the action list. With the scope's `problem`, the issue lists its deals. */
  fix?: string;
};

// The shape every share-of-open-deals rule takes: scope, hit test, then the
// total and each segment against a limit, so a bad rep can't hide in a good total.
export function shareRule(options: ShareRule): Rule {
  return {
    id: options.id,
    version: 1,
    domain: options.domain ?? DOMAIN,
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const loaded = openDeals(ctx);
      if ("outcome" in loaded) return loaded;
      const { data } = loaded;
      const scoped = options.scope(ctx, data, loaded.deals);
      const base = { ...data.evidence[0]!, dateRange: { from: ctx.window.to, to: ctx.window.to } };
      if ("reason" in scoped) return { outcome: "not_measurable", reason: scoped.reason, evidence: [{ ...base, count: null }, ...(scoped.evidence ?? [])] };
      const evidence: Evidence[] = [{ ...base, count: scoped.deals.length, exclusions: [...data.exclusions, ...(scoped.exclusions ?? [])], note: options.scopeLabel }, ...(scoped.evidence ?? [])];
      if (scoped.deals.length === 0) return { outcome: "pass", summary: `There are no ${options.scopeLabel} to check.`, evidence, segmentExemptReason: "There are no deals to segment." };
      const result = segmentRate(scoped.deals, scoped.isHit, dimensions(ctx), { segmentLimit: options.segmentLimit, minSegment: MIN_SEGMENT });
      const overLimit = result.rate > options.totalLimit;
      if (!overLimit && result.hotspots.length === 0) {
        return { outcome: "pass", summary: `Affected: ${result.hits} of ${result.n} ${options.scopeLabel} (${result.rate}%), with no segment above ${options.segmentLimit}%.`, evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
      }
      const worst = result.hotspots[0];
      const hidden = !overLimit ? ` The total looks acceptable, but by ${worst!.dimension}, ${worst!.key} is at ${worst!.rate}% (${worst!.hits} of ${worst!.n}).` : worst ? ` Worst by ${worst.dimension}: ${worst.key}, at ${worst.rate}%.` : "";
      return {
        outcome: "issue",
        title: options.issueTitle,
        summary: `${options.sentence(result.hits, result.n, result.rate)}${hidden}`,
        recommendation: options.recommendation,
        recommendationType: "change",
        severity: options.severity,
        confidence: "high",
        evidence,
        value:
          typeof options.sized === "function"
            ? potentialRevenue(ctx, data, scoped.deals.filter(scoped.isHit), () => (options.sized as (c: RuleContext, d: Ready) => Situation)(ctx, data))
            : options.sized
              ? exposure(ctx, scoped.deals.filter(scoped.isHit), data.multiCurrency, `${result.hits} deals affected`)
              : notSized("This affects how far the pipeline can be trusted, which has no direct figure."),
        effort: { size: "S" },
        owner: options.owner,
        segmentsChecked: result.segmentsChecked,
        subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
        facts: { hits: result.hits, n: result.n, rate: result.rate, ...(worst ? { worstSegmentRate: worst.rate } : {}) },
        ...(scoped.problem && options.fix ? { records: { object: "Opportunity" as const, rows: scoped.deals.filter(scoped.isHit).map((d) => ({ id: d.Id, problem: scoped.problem!(d) })), fix: options.fix } } : {}),
      };
    },
  };
}

/** How long won deals took in each stage, at the 75th percentile. A stage with too few won deals has no line. */
function stageLines(ctx: RuleContext, data: Ready): Map<string, number> {
  const byStage = new Map<string, number[]>();
  for (const deal of data.deals.filter((d) => d.category === "won")) {
    for (const run of stageRuns(deal)) if (data.stages.orderOf(run.stage) !== null) byStage.set(run.stage, [...(byStage.get(run.stage) ?? []), run.days]);
  }
  return new Map([...byStage].filter(([, days]) => days.length >= ctx.calibration.minSample).map(([stage, days]) => [stage, quantile(days, STUCK_PERCENTILE)]));
}

const stuckInStage = shareRule({
  id: "cur.stuck-in-stage",
  title: "Open deals stuck in their stage",
  totalLimit: 40,
  segmentLimit: 60,
  severity: "high",
  scopeLabel: "open deals with a stage to compare",
  scope(ctx, data, open) {
    const lines = stageLines(ctx, data);
    if (lines.size === 0) return { reason: `No open stage has ${ctx.calibration.minSample} won deals to compare with.` };
    const deals = open.filter((d) => lines.has(d.StageName) && enteredCurrentStage(d));
    const noLine = open.filter((d) => !lines.has(d.StageName)).length;
    const noEntry = open.filter((d) => lines.has(d.StageName) && !enteredCurrentStage(d)).length;
    return {
      deals,
      isHit: (d) => daysSince(enteredCurrentStage(d)!, ctx.now) > lines.get(d.StageName)!,
      problem: (d) => `In ${d.StageName} for ${daysSince(enteredCurrentStage(d)!, ctx.now)} days, where three quarters of won deals moved on within ${Math.round(lines.get(d.StageName)!)} days`,
      exclusions: [...(noLine ? [`${plural(noLine, "deal")} in stages with too few won deals to compare`] : []), ...(noEntry ? [`${plural(noEntry, "deal")} whose history doesn't show when they entered this stage`] : [])],
    };
  },
  issueTitle: "Deals are sitting in stages for longer than your wins do",
  sentence: (hits, n, rate) => `Open deals that have been in their stage longer than three quarters of your won deals took: ${hits} of ${n} (${rate}%).`,
  recommendation: "Go through these in pipeline reviews, and either move each deal on with a dated next step or close it.",
  owner: "Head of sales",
  sized: true,
  fix: "Move it on with a dated next step, or close it",
});

const staleDeals = shareRule({
  id: "cur.stale-deals",
  title: "Open deals with no movement and no activity",
  totalLimit: 25,
  segmentLimit: 50,
  severity: "high",
  scopeLabel: "open deals",
  scope(ctx, _data, open) {
    const cutoff = daysBefore(ctx.now, QUIET_DAYS);
    const day = localDay(cutoff, zoneOf(ctx));
    const moved = (d: Deal): boolean => {
      const entered = enteredCurrentStage(d) ?? new Date(d.CreatedDate);
      return entered >= cutoff;
    };
    return {
      deals: open,
      isHit: (d) => !moved(d) && (d.LastActivityDate === null || d.LastActivityDate < day),
      problem: (d) => `No stage change in ${QUIET_DAYS} days, and ${d.LastActivityDate ? `last activity ${d.LastActivityDate}` : "no activity logged"}`,
    };
  },
  issueTitle: "Part of your open pipeline isn't moving",
  sentence: (hits, n, rate) => `Open deals with no stage change and no logged activity in ${QUIET_DAYS} days: ${hits} of ${n} (${rate}%).`,
  recommendation: "Check that activity is being captured, then re-engage these deals or close them.",
  owner: "Head of sales",
  sized: (_ctx, data) => SITUATIONS.inStageFor(QUIET_DAYS, data),
  fix: "Re-engage the customer and log it, or close the deal",
});

const repeatedPushes = shareRule({
  id: "cur.close-date-pushes",
  title: "Open deals whose close date keeps moving",
  totalLimit: 15,
  segmentLimit: 40,
  severity: "medium",
  scopeLabel: "open deals with stage history",
  scope(ctx, _data, open) {
    const fiscal = fiscalStartOf(ctx);
    const deals = open.filter((d) => d.history.length > 0);
    const noHistory = open.length - deals.length;
    return { deals, isHit: (d) => pushesOf(d, fiscal).length >= REPEAT_PUSHES, problem: (d) => `Close date moved later ${pushesOf(d, fiscal).length} times`, exclusions: noHistory ? [`${plural(noHistory, "deal")} with no stage history`] : [] };
  },
  issueTitle: "Close dates are being pushed again and again",
  sentence: (hits, n, rate) => `Open deals whose close date has moved later ${REPEAT_PUSHES} or more times: ${hits} of ${n} (${rate}%). A date that keeps moving usually means the deal isn't really on track.`,
  recommendation: "Treat a second push as a trigger for a manager to review the deal.",
  owner: "Head of sales",
  sized: true,
  fix: "Have a manager review it and agree a date the customer has confirmed",
});

const missingNextSteps = shareRule({
  id: "cur.missing-next-steps",
  title: "Open deals with no next step",
  totalLimit: NEXT_STEP_LIMIT,
  segmentLimit: NEXT_STEP_SEGMENT_LIMIT,
  severity: "medium",
  scopeLabel: "open deals",
  scope(ctx, _data, open) {
    const steps = ctx.data(nextStepsExtractor);
    if (steps.status !== "ok") return { reason: `Next steps couldn't be read: ${steps.reason}`, evidence: [ctx.ev(steps.source, { count: null })] };
    const filled = new Set(steps.data);
    return { deals: open, isHit: (d) => !filled.has(d.Id), problem: () => "Next step is empty", evidence: [ctx.ev(steps.source, { count: steps.data.length, note: "Read as filled or empty only. The text itself is never fetched" })] };
  },
  issueTitle: "Deals are missing a next step",
  sentence: (hits, n, rate) => `Open deals with an empty next step: ${hits} of ${n} (${rate}%).`,
  recommendation: "Make a next step part of how a deal stays in the forecast, and check it in pipeline reviews.",
  owner: "Head of sales",
  sized: false,
  fix: "Write the next step, with a date",
});

function concentrationRule(options: { id: string; title: string; limit: number; keyOf: (d: Deal) => string | null | undefined; subject: string; labelOf: (ctx: RuleContext, key: string) => string; minKeys: number; issueTitle: string; sentence: (share: number) => string; recommendation: string }): Rule {
  return {
    id: options.id,
    version: 1,
    domain: DOMAIN,
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const loaded = openDeals(ctx);
      if ("outcome" in loaded) return loaded;
      const { data, deals } = loaded;
      if (data.multiCurrency) return { outcome: "not_measurable", reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied.", evidence: data.evidence };
      const blank = deals.filter((d) => d.Amount === null).length;
      if (pct(blank, deals.length) > MAX_BLANK_AMOUNT_SHARE) return { outcome: "not_measurable", reason: `${blank} of ${deals.length} open deals have no amount, so shares of the total would mislead.`, evidence: data.evidence };
      const byKey = new Map<string, number>();
      let unkeyed = 0;
      for (const deal of deals) {
        const key = options.keyOf(deal);
        if (!key) unkeyed++;
        else byKey.set(key, (byKey.get(key) ?? 0) + (deal.Amount ?? 0));
      }
      const total = [...byKey.values()].reduce((a, b) => a + b, 0);
      const exclusions = [...data.exclusions, ...(blank ? [`${plural(blank, "deal")} with no amount`] : []), ...(unkeyed ? [`${plural(unkeyed, "deal")} with no ${options.subject}`] : [])];
      const evidence = [{ ...data.evidence[0]!, count: deals.length, dateRange: { from: ctx.window.to, to: ctx.window.to }, exclusions, note: "Open deals" }];
      if (total === 0) return { outcome: "not_measurable", reason: "Open deals have no amounts to compare.", evidence };
      if (byKey.size < options.minKeys) return { outcome: "not_measurable", reason: `Only ${plural(byKey.size, options.subject)} ${byKey.size === 1 ? "holds" : "hold"} open pipeline, so a share means little.`, evidence };
      const [top, amount] = [...byKey].sort(([ka, a], [kb, b]) => b - a || ka.localeCompare(kb))[0]!;
      const share = pct(amount, total);
      const segments = [{ dimension: options.subject, segments: byKey.size }];
      if (share <= options.limit) return { outcome: "pass", summary: `The largest ${options.subject} holds ${share}% of open pipeline, at or under our working limit of ${options.limit}%.`, evidence, segmentsChecked: segments, facts: { share } };
      return {
        outcome: "issue",
        title: options.issueTitle,
        summary: options.sentence(share),
        recommendation: options.recommendation,
        recommendationType: "investigate",
        severity: "medium",
        confidence: "high",
        evidence,
        value: { kind: "revenue", amount: Math.round(amount), unit: ctx.calibration.currency, period: "open pipeline today", assumptions: [`The Amount on open deals for the largest ${options.subject}: exposure, not expected loss`, `${options.limit}% is our working limit, not a Salesforce figure`] },
        effort: { size: "M" },
        owner: "Head of sales",
        segmentsChecked: segments,
        subjects: [{ type: options.subject, label: options.labelOf(ctx, top) }],
        facts: { share, limit: options.limit, keys: byKey.size },
      };
    },
  };
}

const accountConcentration = concentrationRule({
  id: "cur.account-concentration",
  title: "Open pipeline resting on one account",
  limit: ACCOUNT_SHARE_LIMIT,
  keyOf: (d) => d.AccountId,
  subject: "account",
  // The account's name isn't read, so the token restores to its id for the consultant to look up.
  labelOf: (ctx, key) => ctx.pseudonyms.label("account", key, key),
  minKeys: 2,
  issueTitle: "Too much of your pipeline rests on one account",
  sentence: (share) => `One account holds ${share}% of open pipeline. If that relationship stalls, the number goes with it.`,
  recommendation: "Check how much of this is real, then build pipeline elsewhere so one account can't decide the quarter.",
});

const repConcentration = concentrationRule({
  id: "cur.rep-concentration",
  title: "Open pipeline resting on one rep",
  limit: REP_SHARE_LIMIT,
  keyOf: (d) => d.OwnerId,
  subject: "rep",
  labelOf: (ctx, key) => ctx.userLabel(key),
  minKeys: MIN_REPS,
  issueTitle: "Too much of your pipeline rests on one rep",
  sentence: (share) => `One rep holds ${share}% of open pipeline, so an absence or a departure would leave a large gap.`,
  recommendation: "Check the territory split, and make sure someone else knows these deals.",
});

type Coverage = { coverage: number; quota: number; won: number; remaining: number; open: number; quarter: DateRange; reps: Array<{ owner: string; quota: number; won: number; open: number }> };

/** Open pipeline closing this fiscal quarter against what's left of quota, for the people who own deals. */
export function coverageOf(ctx: RuleContext | Omit<RuleContext, "metrics">, deals: Deal[]): Coverage | { reason: string; evidence: Evidence[] } {
  const { source } = ctx.calibration.targets;
  const { quotas, types } = ctx.data(quotasExtractor);
  const noTarget = [ctx.ev(quotas.source, { count: null })];
  if (source === "none") return { reason: "The calibration says there are no targets, so coverage can't be worked out.", evidence: noTarget };
  if (source !== "forecasting_quota") return { reason: `The calibration keeps targets in ${source === "custom_object" ? "a custom object" : "a spreadsheet"}, and we don't read that yet.`, evidence: noTarget };
  if (types.status !== "ok") return { reason: `Forecast types couldn't be read: ${types.reason}`, evidence: [ctx.ev(types.source, { count: null })] };
  if (quotas.status !== "ok") return { reason: `Quotas couldn't be read: ${quotas.reason}`, evidence: noTarget };
  const type = revenueForecastType(types.data);
  if ("reason" in type) return { reason: type.reason, evidence: noTarget };
  const fiscal = fiscalStartOf(ctx);
  if (!fiscal) return { reason: "The org's fiscal year couldn't be read, and the calibration doesn't set one.", evidence: noTarget };

  const today = todayIn(ctx);
  const quarter = quarterBounds(new Date(`${today}T00:00:00Z`), fiscal);
  const data = loadPipeline(ctx);
  const everyone = data.status === "ok" ? data.deals : deals;
  const owners = new Set(everyone.filter((d) => d.category === "open" || d.category === "won").map((d) => d.OwnerId));
  const reps = new Map<string, number>();
  // Only people who own deals: a manager's quota is the sum of the team's, and
  // adding it in would double the target.
  for (const quota of quotas.data) {
    if (quota.ForecastingTypeId !== type.id || quota.QuotaAmount === null || quota.StartDate < quarter.from || quota.StartDate > quarter.to || !owners.has(quota.QuotaOwnerId)) continue;
    reps.set(quota.QuotaOwnerId, (reps.get(quota.QuotaOwnerId) ?? 0) + quota.QuotaAmount);
  }
  if (reps.size === 0) return { reason: `No deal owner has a revenue quota for the quarter starting ${quarter.from}.`, evidence: noTarget };
  const inQuarter = (day: string): boolean => day >= quarter.from && day <= quarter.to;
  const rows = [...reps].map(([owner, quota]) => ({
    owner,
    quota,
    won: everyone.filter((d) => d.OwnerId === owner && d.category === "won" && inQuarter(d.closedDay)).reduce((sum, d) => sum + (d.Amount ?? 0), 0),
    open: deals.filter((d) => d.OwnerId === owner && d.CloseDate >= today && inQuarter(d.CloseDate)).reduce((sum, d) => sum + (d.Amount ?? 0), 0),
  }));
  const quota = rows.reduce((sum, r) => sum + r.quota, 0);
  const won = rows.reduce((sum, r) => sum + r.won, 0);
  const open = rows.reduce((sum, r) => sum + r.open, 0);
  const remaining = quota - won;
  return { coverage: remaining > 0 ? Math.round((open / remaining) * 100) / 100 : Infinity, quota, won, remaining, open, quarter, reps: rows };
}

const coverage: Rule = {
  id: "cur.coverage",
  version: 1,
  domain: DOMAIN,
  title: "Pipeline coverage against quota",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = openDeals(ctx);
    if ("outcome" in loaded) return loaded;
    if (loaded.data.multiCurrency) return { outcome: "not_measurable", reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied.", evidence: loaded.data.evidence };
    const result = coverageOf(ctx, loaded.deals);
    if ("reason" in result) return { outcome: "not_measurable", reason: result.reason, evidence: result.evidence };
    const { quotas } = ctx.data(quotasExtractor);
    const evidence = [ctx.ev(quotas.source, { count: result.reps.length, dateRange: result.quarter, exclusions: ["Quotas held by people who own no deals, because a manager's quota repeats the team's", "Open deals closing outside this quarter or already past their close date"] }), { ...loaded.data.evidence[0]!, count: loaded.deals.length }];
    const segments = [{ dimension: "rep", segments: result.reps.length }];
    const facts = { quota: result.quota, won: result.won, remaining: result.remaining, openThisQuarter: result.open };
    if (result.remaining <= 0) return { outcome: "pass", summary: `Won deals have already covered this quarter's quota of ${result.quota}.`, evidence, segmentsChecked: segments, facts };
    const low = result.reps.filter((r) => r.quota > r.won && r.open / (r.quota - r.won) < COVERAGE_TARGET).sort((a, b) => a.open / (a.quota - a.won) - b.open / (b.quota - b.won) || a.owner.localeCompare(b.owner));
    if (result.coverage >= COVERAGE_TARGET && low.length === 0) {
      return { outcome: "pass", summary: `Coverage is ${result.coverage} times what's left of quota this quarter, at or above our working target of ${COVERAGE_TARGET}.`, evidence, segmentsChecked: segments, facts: { ...facts, coverage: result.coverage } };
    }
    const shortfall = Math.max(0, Math.round(result.remaining * COVERAGE_TARGET - result.open));
    return {
      outcome: "issue",
      title: "There isn't enough pipeline to cover this quarter's quota",
      summary: `Open pipeline closing this quarter is ${result.coverage} times what's left of quota, against our working target of ${COVERAGE_TARGET}. Quota ${money(result.quota, ctx.calibration.currency)}, won ${money(result.won, ctx.calibration.currency)}, open ${money(result.open, ctx.calibration.currency)}.`,
      recommendation: "Decide this month whether to build more pipeline or reset the number. Coverage can't be fixed in the last weeks of a quarter.",
      recommendationType: "investigate",
      severity: result.coverage < COVERAGE_TARGET / 2 ? "high" : "medium",
      confidence: "medium",
      evidence,
      value: { kind: "revenue", amount: shortfall, unit: ctx.calibration.currency, period: `more pipeline needed for the quarter starting ${result.quarter.from}`, assumptions: [`A target of ${COVERAGE_TARGET} times the remaining quota, which is our working figure`, "Quota comes from Collaborative Forecasts, for people who own deals", "Open pipeline in the quarter is taken at full Amount, with no stage weighting"] },
      // The decision is the small part, and it's the urgent part. Building pipeline afterwards is the dependency.
      effort: { size: "S" },
      owner: "Head of sales",
      dependencies: ["Building pipeline, if that's the decision, is a larger piece of work"],
      segmentsChecked: segments,
      subjects: low.slice(0, 5).map((r) => ({ type: "rep", label: ctx.userLabel(r.owner) })),
      facts: { ...facts, coverage: result.coverage, target: COVERAGE_TARGET, shortfall },
    };
  },
};

type Ctx = Omit<RuleContext, "metrics">;

function currentMetrics(ctx: Ctx): Metric[] {
  const loaded = openDeals(ctx);
  const definitions = {
    count: { id: "current.open_deals", domain: DOMAIN, label: "Open deals", unit: "count" as const, definition: "Deals in a stage the calibration calls open, today." },
    value: { id: "current.open_pipeline", domain: DOMAIN, label: "Open pipeline", unit: "currency" as const, definition: "Total Amount of open deals today. A deal with no amount counts as zero." },
    age: { id: "current.median_days_in_stage", domain: DOMAIN, label: "Days in current stage", unit: "days" as const, definition: "Median days open deals have been in their current stage." },
  };
  const shared = { exclusions: [] as string[], assumptions: [zoneNote(ctx)] };
  if ("outcome" in loaded) return [...Object.values(definitions), ...OUTLOOK_LABELS.map((m) => ({ ...m, domain: DOMAIN }))].map((d) => ({ ...d, ...shared, evidence: loaded.evidence, status: "not_measurable", reason: loaded.reason }));
  const { data, deals } = loaded;
  const today = { from: ctx.window.to, to: ctx.window.to };
  const evidence = [{ ...data.evidence[0]!, count: deals.length, dateRange: today }];
  const common = { ...shared, exclusions: data.exclusions, evidence };
  const { minSample } = ctx.calibration;
  const dims: Record<string, (d: Deal) => string> = { stage: (d) => d.StageName, "record type": (d) => d.RecordType ?? "No record type", team: (d) => d.team, rep: (d) => ctx.userLabel(d.OwnerId) };
  const rest = { dateRange: today, series: [], stats: {}, segmentExemptReason: deals.length === 0 ? "There are no open deals." : null };
  const metrics: Metric[] = [{ ...definitions.count, ...common, status: "measured", value: deals.length, n: deals.length, segments: [...segment(deals, dims, 1, (group) => group.length), ...retiredStages(ctx, deals)], ...rest }];
  if (data.multiCurrency) metrics.push({ ...definitions.value, ...common, status: "not_measurable", reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied." });
  else {
    const total = (group: Deal[]): number => Math.round(group.reduce((sum, d) => sum + (d.Amount ?? 0), 0));
    metrics.push({ ...definitions.value, ...common, status: "measured", value: total(deals), n: deals.length, segments: segment(deals, dims, 1, total), ...rest });
  }
  const placed = deals.filter((d) => enteredCurrentStage(d));
  const days = (d: Deal): number => daysSince(enteredCurrentStage(d)!, ctx.now);
  const round = (n: number): number => Math.round(n * 10) / 10;
  if (placed.length < minSample) metrics.push({ ...definitions.age, ...common, status: "not_measurable", reason: `${placed.length} open deals with stage history is below the calibrated minimum of ${minSample}.` });
  else metrics.push({ ...definitions.age, ...common, status: "measured", value: round(median(placed.map(days))), n: placed.length, segments: segment(placed, dims, minSample, (group) => round(median(group.map(days)))), ...rest });
  metrics.push(...openStageMetrics(ctx, data, deals, evidence), ...deliveryOutlook(ctx, data, deals, evidence));
  return metrics;
}

export const currentPipelineModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Current pipeline",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, nextStepsExtractor, quotasExtractor],
  metrics: [currentMetrics],
  rules: [stuckInStage, staleDeals, repeatedPushes, missingNextSteps, accountConcentration, repConcentration, coverage],
};
