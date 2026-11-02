import { schemaExtractor } from "../../extract/configuration.ts";
import { forecastExtractor, quotasExtractor, revenueForecastType, type ForecastItemRow } from "../../extract/forecast.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { coverageOf, shareRule, type Scoped } from "../current-pipeline/index.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { fiscalStartOf, pushesOf } from "../historic-pipeline/metrics.ts";
import type { Evidence, Metric } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";

const DOMAIN = "forecast";
const COMMIT = "Commit";
const ADJUSTMENT_LIMIT = 20;
const AT_RISK_LIMIT = 20;
const AT_RISK_SEGMENT_LIMIT = 50;
const QUIET_DAYS = 30;
const REPEAT_PUSHES = 2;
const QUOTA_GAP_LIMIT = 10;
const EARLY_STAGES = 2;
const MIN_TEAM = 2;

type Ctx = Omit<RuleContext, "metrics">;

type InUse = NotMeasurableResult & { inUse: false };

/** Forecasting counts as in use only if the org has an active revenue type and forecast rows for today. */
function forecastInUse(ctx: Ctx): { inUse: true; typeId: string; items: ForecastItemRow[] } | InUse {
  const { types } = ctx.data(quotasExtractor);
  const { items } = ctx.data(forecastExtractor);
  const off = (reason: string, evidence: Evidence[]): InUse => ({ inUse: false, outcome: "not_measurable", reason: `Forecasting isn't in use, or can't be read, so we don't report a forecast figure. ${reason}`, evidence });
  if (types.status !== "ok") return off(types.reason, [ctx.ev(types.source, { count: null })]);
  const type = revenueForecastType(types.data);
  if ("reason" in type) return off(type.reason, [ctx.ev(types.source, { count: types.data.length })]);
  if (items.status !== "ok") return off(items.reason, [ctx.ev(items.source, { count: null })]);
  const rows = items.data.filter((i) => i.ForecastingTypeId === type.id);
  if (rows.length === 0) return off("Salesforce holds no forecast rows for this period.", [ctx.ev(items.source, { count: 0 })]);
  return { inUse: true, typeId: type.id, items: rows };
}

const inUseRule: Rule = {
  id: "fct.forecasting-in-use",
  version: 1,
  domain: DOMAIN,
  title: "Whether Collaborative Forecasts is in use",
  evaluate(ctx): RuleResult | RuleResult[] {
    const state = forecastInUse(ctx);
    if (!state.inUse) return { outcome: "not_measurable", reason: state.reason, evidence: state.evidence };
    const { items } = ctx.data(forecastExtractor);
    const rows = state.items;
    return {
      outcome: "pass",
      summary: `Collaborative Forecasts is in use: ${rows.length} forecast rows for ${plural(new Set(rows.map((r) => r.OwnerId)).size, "person", "people")} in the current period.`,
      evidence: [ctx.ev(items.source, { count: rows.length })],
      segmentsChecked: [{ dimension: "forecast owner", segments: new Set(rows.map((r) => r.OwnerId)).size }, { dimension: "forecast category", segments: new Set(rows.map((r) => r.ForecastCategoryName ?? "None")).size }],
      facts: { rows: rows.length },
    };
  },
};

// Rule 5, as its own rule because the obvious version is wrong. ForecastingItem
// is the forecast as it stands today, and these rows have no snapshot of what
// was called at the time, so comparing them with results would flatter the org.
const accuracyRule: Rule = {
  id: "fct.forecast-accuracy",
  version: 1,
  domain: DOMAIN,
  title: "Forecast accuracy",
  evaluate(ctx): RuleResult | RuleResult[] {
    const state = forecastInUse(ctx);
    if (!state.inUse) return { outcome: "not_measurable", reason: state.reason, evidence: state.evidence };
    const { items } = ctx.data(forecastExtractor);
    return {
      outcome: "not_measurable",
      reason: "Forecast rows show today's position, not what was called at the time, so accuracy can't be measured from them. It needs a snapshot taken each period.",
      evidence: [ctx.ev(items.source, { count: state.items.length })],
    };
  },
};

const adjustmentsRule: Rule = {
  id: "fct.manager-adjustments",
  version: 1,
  domain: DOMAIN,
  title: "Forecast changed by hand",
  evaluate(ctx): RuleResult | RuleResult[] {
    const state = forecastInUse(ctx);
    if (!state.inUse) return { outcome: "not_measurable", reason: state.reason, evidence: state.evidence };
    const { items } = ctx.data(forecastExtractor);
    const rows = state.items.filter((r) => r.ForecastCategoryName === COMMIT && r.AmountWithoutAdjustments !== null && r.ForecastAmount !== null);
    const evidence = [ctx.ev(items.source, { count: rows.length, exclusions: ["Forecast categories other than Commit", "Rows with no amount"] })];
    if (rows.length === 0) return { outcome: "not_measurable", reason: "There are no Commit forecast rows with amounts to compare.", evidence };
    const before = rows.reduce((sum, r) => sum + r.AmountWithoutAdjustments!, 0);
    const moved = rows.reduce((sum, r) => sum + Math.abs(r.ForecastAmount! - r.AmountWithoutAdjustments!), 0);
    if (before === 0) return { outcome: "not_measurable", reason: "Commit forecast rows are all zero before adjustment.", evidence };
    const share = pct(moved, before);
    const adjusted = rows.filter((r) => r.HasAdjustment && r.ForecastAmount !== r.AmountWithoutAdjustments);
    const segments = [{ dimension: "forecast owner", segments: new Set(rows.map((r) => r.OwnerId)).size }];
    const facts = { share, adjustedOwners: new Set(adjusted.map((r) => r.OwnerId)).size, owners: new Set(rows.map((r) => r.OwnerId)).size };
    if (share <= ADJUSTMENT_LIMIT) return { outcome: "pass", summary: `Hand adjustments move the Commit forecast by ${share}%, at or under our working limit of ${ADJUSTMENT_LIMIT}%.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Much of the Commit forecast comes from hand adjustments",
      summary: `Hand adjustments move the Commit forecast by ${share}% of its unadjusted amount, across ${plural(facts.adjustedOwners, "person", "people")}. The difference is judgement that isn't backed by a deal in Salesforce.`,
      recommendation: "Ask for a reason behind each large adjustment, and check whether the deals should be recorded differently.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("We can't say whether the adjustments are right until a snapshot shows what was called each period."),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: segments,
      subjects: [...new Set(adjusted.map((r) => r.OwnerId))].map((id) => ({ type: "user", label: ctx.userLabel(id) })),
      facts: { ...facts, limit: ADJUSTMENT_LIMIT },
    };
  },
};

const quotaGaps: Rule = {
  id: "fct.quota-gaps",
  version: 1,
  domain: DOMAIN,
  title: "Deal owners with no quota",
  evaluate(ctx): RuleResult | RuleResult[] {
    const state = forecastInUse(ctx);
    if (!state.inUse) return { outcome: "not_measurable", reason: state.reason, evidence: state.evidence };
    const { quotas } = ctx.data(quotasExtractor);
    const { periods } = ctx.data(forecastExtractor);
    const users = ctx.data(usersExtractor);
    const data = loadPipeline(ctx);
    if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
    if (quotas.status !== "ok") return { outcome: "not_measurable", reason: `Quotas couldn't be read: ${quotas.reason}`, evidence: [ctx.ev(quotas.source, { count: null })] };
    if (periods.status !== "ok") return { outcome: "not_measurable", reason: `Periods couldn't be read: ${periods.reason}`, evidence: [ctx.ev(periods.source, { count: null })] };
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const active = new Map(users.data.filter((u) => u.IsActive).map((u) => [u.Id, u]));
    const owners = [...new Set(data.deals.filter((d) => d.category === "open" && active.has(d.OwnerId)).map((d) => d.OwnerId))];
    const earliest = periods.data.map((p) => p.StartDate).sort()[0];
    const latest = periods.data.map((p) => p.EndDate).sort().at(-1);
    const holders = new Set(quotas.data.filter((q) => q.ForecastingTypeId === state.typeId && q.QuotaAmount !== null && earliest !== undefined && latest !== undefined && q.StartDate >= earliest && q.StartDate <= latest).map((q) => q.QuotaOwnerId));
    const evidence = [ctx.ev(quotas.source, { count: holders.size, exclusions: ["Inactive deal owners, which the data quality checks cover"] })];
    if (owners.length === 0) return { outcome: "pass", summary: "No active user owns an open deal.", evidence, segmentExemptReason: "There are no owners to segment." };
    const result = segmentRate(owners, (id) => !holders.has(id), { team: (id) => active.get(id)!.UserRole?.Name ?? "No role" }, { segmentLimit: 50, minSegment: MIN_TEAM });
    if (result.rate <= QUOTA_GAP_LIMIT && result.hotspots.length === 0) {
      return { outcome: "pass", summary: `Deal owners with no quota this period: ${result.hits} of ${result.n} (${result.rate}%).`, evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
    }
    return {
      outcome: "issue",
      title: "Some deal owners have no quota",
      summary: `Active users who own open deals but have no quota this period: ${result.hits} of ${result.n} (${result.rate}%). Their deals count towards the forecast with nothing to measure them against.`,
      recommendation: "Load a quota for each person who carries a number, or move their deals to someone who does.",
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("This affects how far quota attainment can be trusted, which has no direct figure."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: result.segmentsChecked,
      subjects: owners.filter((id) => !holders.has(id)).map((id) => ({ type: "user", label: ctx.userLabel(id) })),
      facts: { hits: result.hits, n: result.n, rate: result.rate },
    };
  },
};

function commitDeals(ctx: RuleContext, open: Deal[]): { commit: Deal[]; evidence: Evidence[] } | { reason: string; evidence: Evidence[] } {
  const state = forecastInUse(ctx);
  if (!state.inUse) return { reason: state.reason, evidence: state.evidence };
  const { facts } = ctx.data(forecastExtractor);
  if (facts.status !== "ok") return { reason: `Forecast facts couldn't be read: ${facts.reason}`, evidence: [ctx.ev(facts.source, { count: null })] };
  const committed = new Set(facts.data.filter((f) => f.ForecastCategoryName === COMMIT).map((f) => f.OpportunityId));
  const evidence = [ctx.ev(facts.source, { count: committed.size, note: "Deals in the Commit forecast category this period" })];
  // Admins can rename forecast categories, so finding none can't be read as "nothing at risk".
  if (committed.size === 0) return { reason: "No deal is in a forecast category called Commit this period, so we can't tell a clean Commit from a renamed one.", evidence };
  return { commit: open.filter((d) => committed.has(d.Id)), evidence };
}

function commitScope(ctx: RuleContext, open: Deal[], isHit: (d: Deal) => boolean): Scoped {
  const result = commitDeals(ctx, open);
  if ("reason" in result) return result;
  return { deals: result.commit, isHit, evidence: result.evidence };
}

const commitAtRisk = shareRule({
  id: "fct.commit-at-risk",
  domain: DOMAIN,
  title: "Committed deals showing warning signs",
  totalLimit: AT_RISK_LIMIT,
  segmentLimit: AT_RISK_SEGMENT_LIMIT,
  severity: "high",
  scopeLabel: "deals in Commit",
  scope(ctx, _data, open) {
    const today = ctx.window.to;
    const quietDay = daysBefore(ctx.now, QUIET_DAYS).toISOString().slice(0, 10);
    const fiscal = fiscalStartOf(ctx);
    return commitScope(ctx, open, (d) => d.CloseDate < today || d.LastActivityDate === null || d.LastActivityDate < quietDay || pushesOf(d, fiscal).length >= REPEAT_PUSHES);
  },
  issueTitle: "Your Commit forecast includes deals showing warning signs",
  sentence: (hits, n, rate) => `Deals in Commit that are past their close date, quiet for ${QUIET_DAYS} days, or pushed ${REPEAT_PUSHES} or more times: ${hits} of ${n} (${rate}%).`,
  recommendation: "Review each one before the next forecast call, and move it out of Commit unless there's a dated reason to keep it.",
  owner: "Head of sales",
  sized: true,
});

const earlyStageCommits = shareRule({
  id: "fct.early-stage-commits",
  domain: DOMAIN,
  title: "Committed deals still in early stages",
  totalLimit: 10,
  segmentLimit: 25,
  severity: "medium",
  scopeLabel: "deals in Commit",
  scope(ctx, data, open) {
    const early = new Set(data.stages.openStages.slice(0, EARLY_STAGES));
    return commitScope(ctx, open, (d) => early.has(d.StageName));
  },
  issueTitle: "Early-stage deals are being counted as Commit",
  sentence: (hits, n, rate) => `Deals in Commit that are still in the first ${EARLY_STAGES} open stages: ${hits} of ${n} (${rate}%).`,
  recommendation: "Check whether these are real commitments. If they are, the stage is out of date. If not, take them out of Commit.",
  owner: "Sales operations",
  sized: true,
});

function forecastMetrics(ctx: Ctx): Metric[] {
  const definition = {
    id: "forecast.commit_against_quota",
    domain: DOMAIN,
    label: "Won and Commit against quota",
    unit: "percent" as const,
    definition: "Won this quarter plus open deals in Commit, as a share of quota, for people who own deals. Worked out from deals rather than forecast roll-ups, so it isn't affected by how categories are rolled up.",
    exclusions: ["Quotas held by people who own no deals"],
    assumptions: ["Days are in UTC, not the org's time zone"],
  };
  const notMeasurable = (reason: string, evidence: Evidence[]): Metric => ({ ...definition, status: "not_measurable", reason, evidence });
  const state = forecastInUse(ctx);
  if (!state.inUse) return [notMeasurable(state.reason, state.evidence)];
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return [notMeasurable(data.reason, data.evidence)];
  if (data.multiCurrency) return [notMeasurable("The org is multi-currency, and amounts can't be added until historic rates are supplied.", data.evidence)];
  const open = data.deals.filter((d) => d.category === "open");
  const covered = coverageOf(ctx, open);
  if ("reason" in covered) return [notMeasurable(covered.reason, covered.evidence)];
  const { facts } = ctx.data(forecastExtractor);
  if (facts.status !== "ok") return [notMeasurable(`Forecast facts couldn't be read: ${facts.reason}`, [ctx.ev(facts.source, { count: null })])];
  const committed = new Set(facts.data.filter((f) => f.ForecastCategoryName === COMMIT).map((f) => f.OpportunityId));
  const commitOf = (owner: string): number => open.filter((d) => d.OwnerId === owner && committed.has(d.Id)).reduce((sum, d) => sum + (d.Amount ?? 0), 0);
  const round = (n: number): number => Math.round(n * 10) / 10;
  const attainment = (quota: number, amount: number): number | null => (quota > 0 ? round((amount / quota) * 100) : null);
  const total = covered.reps.reduce((sum, r) => sum + r.won + commitOf(r.owner), 0);
  const value = attainment(covered.quota, total);
  if (value === null) return [notMeasurable("The quota for this quarter is zero.", [ctx.ev(facts.source, { count: facts.data.length })])];
  return [
    {
      ...definition,
      status: "measured",
      value,
      n: covered.reps.length,
      dateRange: covered.quarter,
      evidence: [ctx.ev(facts.source, { count: facts.data.length, dateRange: covered.quarter })],
      segments: covered.reps.map((r) => ({ dimension: "rep", key: ctx.userLabel(r.owner), n: 1, value: attainment(r.quota, r.won + commitOf(r.owner)), notMeasurableReason: r.quota > 0 ? null : "No quota" })),
      segmentExemptReason: null,
      series: [],
      stats: { quota: covered.quota, won: covered.won, commit: total - covered.won },
    },
  ];
}

export const forecastModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Forecast",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, quotasExtractor, forecastExtractor],
  metrics: [forecastMetrics],
  rules: [inUseRule, accuracyRule, adjustmentsRule, quotaGaps, commitAtRisk, earlyStageCommits],
};
