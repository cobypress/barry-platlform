import { money } from "../../render/format.ts";
import { activityExtractor } from "../../extract/activity.ts";
import { schemaExtractor } from "../../extract/configuration.ts";
import { leadsExtractor } from "../../extract/leads.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { loadPipeline } from "../historic-pipeline/cohort.ts";
import { median } from "../historic-pipeline/metrics.ts";
import { zoneNote } from "../time-zone.ts";
import { atOwnLeadReturn } from "../value.ts";
import type { Metric, MetricSegment } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { loadLeads, type Lead, type LeadData } from "./load.ts";

const DOMAIN = "lead_funnel";
const CONVERSION_AGE_DAYS = 30;
const RESPONSE_GRACE_DAYS = 7;
const RESPONSE_TARGET_HOURS = 24;
const RESPONSE_SEGMENT_HOURS = 72;
const NO_TOUCH_LIMIT = 15;
const NO_TOUCH_SEGMENT_LIMIT = 30;
const MIN_SEGMENT = 10;
const CONVERSION_FLOOR = 0.5;
const NO_OPPORTUNITY_LIMIT = 20;
const HOUR = 3_600_000;

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<LeadData, { status: "ok" }>;

function leadsOr(ctx: Ctx): Ready | NotMeasurableResult {
  const data = loadLeads(ctx);
  return data.status === "ok" ? data : { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
}

const dimensions = (ctx: Ctx): Record<string, (l: Lead) => string> => ({ source: (l) => l.source, team: (l) => l.team, rep: (l) => ctx.userLabel(l.OwnerId) });
const hoursToTouch = (l: Lead): number => (l.firstTouch!.getTime() - new Date(l.CreatedDate.replace(/\+0000$/, "Z")).getTime()) / HOUR;
const created = (l: Lead): Date => new Date(l.CreatedDate.replace(/\+0000$/, "Z"));
const round1 = (n: number): number => Math.round(n * 10) / 10;

/** Old enough to have had a fair chance of converting. */
const settled = (ctx: Ctx, data: Ready): Lead[] => data.leads.filter((l) => created(l) < daysBefore(ctx.now, CONVERSION_AGE_DAYS));

function groupsOf(leads: Lead[], keyOf: (l: Lead) => string): Array<[string, Lead[]]> {
  const groups = new Map<string, Lead[]>();
  for (const lead of leads) groups.set(keyOf(lead), [...(groups.get(keyOf(lead)) ?? []), lead]);
  return [...groups].sort(([a], [b]) => a.localeCompare(b));
}

const conversionBySource: Rule = {
  id: "lead.conversion-by-source",
  version: 1,
  domain: DOMAIN,
  title: "Lead conversion by source",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    const leads = settled(ctx, data);
    const { minSample } = ctx.calibration;
    const evidence = [{ ...data.evidence[0]!, count: leads.length, exclusions: [...data.exclusions, `Leads created in the last ${CONVERSION_AGE_DAYS} days, which haven't had time to convert`] }];
    if (leads.length < minSample) return { outcome: "not_measurable", reason: `${leads.length} leads old enough to judge is below the calibrated minimum of ${minSample}.`, evidence };
    const rate = (group: Lead[]): number => pct(group.filter((l) => l.IsConverted).length, group.length);
    const overall = rate(leads);
    const sources = groupsOf(leads, (l) => l.source);
    const low = sources.filter(([, group]) => group.length >= minSample && rate(group) < overall * CONVERSION_FLOOR).sort(([a, x], [b, y]) => rate(x) - rate(y) || a.localeCompare(b));
    const segments = [{ dimension: "source", segments: sources.length }];
    if (low.length === 0) return { outcome: "pass", summary: `${overall}% of leads convert, and no source with ${minSample} or more leads is under half of that.`, evidence, segmentsChecked: segments, facts: { overall } };
    const [name, group] = low[0]!;
    return {
      outcome: "issue",
      title: "Some lead sources barely convert",
      summary: `${overall}% of leads convert overall, but ${name} converts ${rate(group)}% of ${group.length} leads.`,
      recommendation: "Find out whether these leads are poor or just poorly handled before changing what you spend on the source.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("We can't say what a better rate would be worth until we know why it's low."),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: segments,
      subjects: low.slice(0, 5).map(([label]) => ({ type: "lead source", label })),
      facts: { overall, worstRate: rate(group), worstLeads: group.length },
    };
  },
};

/** Leads we can judge for response: old enough to have been reached, and young enough that their activity is readable. */
function judgeable(ctx: Ctx, data: Ready): Lead[] {
  return data.leads.filter((l) => l.touchKnowable && created(l) < daysBefore(ctx.now, RESPONSE_GRACE_DAYS));
}

const responseTime: Rule = {
  id: "lead.response-time",
  version: 1,
  domain: DOMAIN,
  title: "How fast leads get a first response",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    if (!data.activityReadable) return { outcome: "not_measurable", reason: "Activities couldn't be read in full, so we can't see first responses.", evidence: data.evidence };
    const touched = judgeable(ctx, data).filter((l) => l.firstTouch);
    const { minSample } = ctx.calibration;
    const exclusions = [...data.exclusions, "Leads with no response at all, which the next check covers", `Leads created before the readable year or in the last ${RESPONSE_GRACE_DAYS} days`];
    const evidence = [{ ...data.evidence[0]!, count: touched.length, exclusions, note: "First response is the earliest task, event or status change after creation, on the lead or its converted contact" }];
    if (touched.length < minSample) return { outcome: "not_measurable", reason: `${touched.length} leads with a response is below the calibrated minimum of ${minSample}.`, evidence };
    const overall = round1(median(touched.map(hoursToTouch)));
    const dims = dimensions(ctx);
    const hotspots = Object.entries(dims).flatMap(([dimension, keyOf]) =>
      groupsOf(touched, keyOf).flatMap(([key, group]) => {
        const hours = round1(median(group.map(hoursToTouch)));
        return group.length >= MIN_SEGMENT && hours > RESPONSE_SEGMENT_HOURS ? [{ dimension, key, hours, n: group.length }] : [];
      }),
    ).sort((a, b) => b.hours - a.hours || a.key.localeCompare(b.key));
    const segmentsChecked = Object.entries(dims).map(([dimension, keyOf]) => ({ dimension, segments: new Set(touched.map(keyOf)).size }));
    if (overall <= RESPONSE_TARGET_HOURS && hotspots.length === 0) {
      return { outcome: "pass", summary: `The median first response is ${overall} hours, with no segment over ${RESPONSE_SEGMENT_HOURS} hours.`, evidence, segmentsChecked, facts: { medianHours: overall, leads: touched.length } };
    }
    const worst = hotspots[0];
    return {
      outcome: "issue",
      title: "Some leads wait days for a first response",
      summary: `The median first response is ${overall} hours.${worst ? ` ${worst.dimension === "source" ? `${worst.key} leads wait` : `For ${worst.dimension} ${worst.key} the wait is`} ${worst.hours} hours.` : ""} Our working targets are ${RESPONSE_TARGET_HOURS} hours overall and ${RESPONSE_SEGMENT_HOURS} for any segment.`,
      recommendation: "Set a response rule by source, and route the slow ones to someone who can meet it.",
      recommendationType: "change",
      severity: "medium",
      // First response is read from what's logged. Unlogged calls would make this look slower than it is.
      confidence: "medium",
      evidence,
      value: notSized("The link between response time and conversion here hasn't been measured."),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked,
      subjects: hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts: { medianHours: overall, leads: touched.length, ...(worst ? { worstHours: worst.hours } : {}) },
    };
  },
};

const noResponse: Rule = {
  id: "lead.no-first-response",
  version: 2,
  domain: DOMAIN,
  title: "Leads with no response at all",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    if (!data.activityReadable) return { outcome: "not_measurable", reason: "Activities couldn't be read in full, so we can't see first responses.", evidence: data.evidence };
    const leads = judgeable(ctx, data);
    const evidence = [{ ...data.evidence[0]!, count: leads.length, exclusions: [...data.exclusions, `Leads created before the readable year or in the last ${RESPONSE_GRACE_DAYS} days`] }];
    if (leads.length < ctx.calibration.minSample) return { outcome: "not_measurable", reason: `${leads.length} leads is below the calibrated minimum of ${ctx.calibration.minSample}.`, evidence };
    const result = segmentRate(leads, (l) => l.firstTouch === null, dimensions(ctx), { segmentLimit: NO_TOUCH_SEGMENT_LIMIT, minSegment: MIN_SEGMENT });
    const facts = { hits: result.hits, n: result.n, rate: result.rate };
    if (result.rate <= NO_TOUCH_LIMIT && result.hotspots.length === 0) return { outcome: "pass", summary: `Leads with no logged response: ${result.hits} of ${result.n} (${result.rate}%), with no segment above ${NO_TOUCH_SEGMENT_LIMIT}%.`, evidence, segmentsChecked: result.segmentsChecked, facts };
    const worst = result.hotspots[0];
    return {
      outcome: "issue",
      title: "Leads are going unanswered",
      summary: `Leads with no logged response after ${RESPONSE_GRACE_DAYS} days: ${result.hits} of ${result.n} (${result.rate}%).${worst ? ` The worst segment is ${worst.dimension} ${worst.key} at ${worst.rate}%.` : ""}`,
      recommendation: "Check routing first: unassigned or queue-held leads are the usual cause.",
      recommendationType: "change",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: atOwnLeadReturn(ctx, result.hits, { period: "one-off, if these leads are worked", caveat: "Assumes an unanswered lead would return what an average lead does. Some may be poor leads, so treat this as a ceiling" }),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts,
    };
  },
};

const convertedWithoutOpportunity: Rule = {
  id: "lead.converted-no-opportunity",
  version: 1,
  domain: DOMAIN,
  title: "Leads converted with no opportunity",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    const converted = data.leads.filter((l) => l.IsConverted);
    const evidence = [{ ...data.evidence[0]!, count: converted.length, exclusions: data.exclusions, note: "Converted leads" }];
    if (converted.length === 0) return { outcome: "pass", summary: "No leads were converted in the window.", evidence, segmentExemptReason: "There are no converted leads to segment." };
    const result = segmentRate(converted, (l) => l.ConvertedOpportunityId === null, dimensions(ctx), { segmentLimit: 40, minSegment: MIN_SEGMENT });
    const facts = { hits: result.hits, n: result.n, rate: result.rate };
    if (result.rate <= NO_OPPORTUNITY_LIMIT && result.hotspots.length === 0) return { outcome: "pass", summary: `Converted with no opportunity: ${result.hits} of ${result.n} (${result.rate}%), with no segment above 40%.`, evidence, segmentsChecked: result.segmentsChecked, facts };
    const worst = result.hotspots[0];
    return {
      outcome: "issue",
      title: "Converted leads aren't becoming opportunities",
      summary: `Converted with no opportunity: ${result.hits} of ${result.n} (${result.rate}%).${worst ? ` The worst segment is ${worst.dimension} ${worst.key} at ${worst.rate}%.` : ""} Conversion looks better than the pipeline it produced.`,
      recommendation: "Agree when a lead should be converted. If it's only to tidy up, take these out of your conversion figures.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("We can't say whether these leads should have produced a deal."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts,
    };
  },
};

type SourceReturn = { source: string; leads: number; won: number; deals: number };

/** Won Amount from converted leads, by source. Null if amounts can't be added. */
function sourceReturns(ctx: Ctx, data: Ready): { rows: SourceReturn[] } | { reason: string } {
  const pipeline = loadPipeline(ctx);
  if (pipeline.status !== "ok") return { reason: pipeline.reason };
  if (pipeline.multiCurrency) return { reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied." };
  const wonById = new Map(pipeline.deals.filter((d) => d.category === "won").map((d) => [d.Id, d.Amount ?? 0]));
  const rows = groupsOf(data.leads, (l) => l.source).map(([source, group]): SourceReturn => {
    const deals = new Set(group.flatMap((l) => (l.ConvertedOpportunityId && wonById.has(l.ConvertedOpportunityId) ? [l.ConvertedOpportunityId] : [])));
    return { source, leads: group.length, deals: deals.size, won: [...deals].reduce((sum, id) => sum + wonById.get(id)!, 0) };
  });
  return { rows };
}

const sourceReturn: Rule = {
  id: "lead.source-return",
  version: 1,
  domain: DOMAIN,
  title: "What each lead source returns",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    const returns = sourceReturns(ctx, data);
    if ("reason" in returns) return { outcome: "not_measurable", reason: returns.reason, evidence: data.evidence };
    const { minSample } = ctx.calibration;
    const big = returns.rows.filter((r) => r.leads >= minSample);
    const evidence = [{ ...data.evidence[0]!, exclusions: [...data.exclusions, "Won deals that didn't come from a converted lead"], note: "Won Amount on the opportunity each lead converted to" }];
    if (big.length === 0) return { outcome: "not_measurable", reason: `No source has the calibrated minimum of ${minSample} leads.`, evidence };
    const dry = big.filter((r) => r.deals === 0).sort((a, b) => b.leads - a.leads || a.source.localeCompare(b.source));
    const segments = [{ dimension: "source", segments: returns.rows.length }];
    const total = returns.rows.reduce((sum, r) => sum + r.won, 0);
    if (dry.length === 0) return { outcome: "pass", summary: `Every source with ${minSample} or more leads has produced at least one win.`, evidence, segmentsChecked: segments, facts: { wonFromLeads: Math.round(total) } };
    const leads = dry.reduce((sum, r) => sum + r.leads, 0);
    return {
      outcome: "issue",
      title: "Some lead sources have produced no wins",
      summary: `${dry.map((r) => r.source).join(", ")} brought in ${leads} leads and no won deals, against ${money(total, ctx.calibration.currency)} won from other sources.`,
      recommendation: "Check how these leads are handled, then decide whether the source is worth keeping.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The cost of each source isn't in Salesforce, so we can't size a return."),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: segments,
      subjects: dry.slice(0, 5).map((r) => ({ type: "lead source", label: r.source })),
      facts: { leads, sources: dry.length, wonFromLeads: Math.round(total) },
    };
  },
};

const campaignReturn: Rule = {
  id: "lead.campaign-return",
  version: 1,
  domain: DOMAIN,
  title: "Campaign spend against what it returned",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = leadsOr(ctx);
    if ("outcome" in data) return data;
    const { campaigns, members } = ctx.data(leadsExtractor);
    if (campaigns.status !== "ok") return { outcome: "not_measurable", reason: `Campaigns couldn't be read: ${campaigns.reason}`, evidence: [ctx.ev(campaigns.source, { count: null })] };
    if (members.status !== "ok") return { outcome: "not_measurable", reason: `Campaign members couldn't be read: ${members.reason}`, evidence: [ctx.ev(members.source, { count: null })] };
    if (members.truncated) return { outcome: "not_measurable", reason: "There are too many campaign members to read in full.", evidence: [ctx.ev(members.source, { count: members.data.length })] };
    const costed = campaigns.data.filter((c) => c.ActualCost !== null && c.ActualCost > 0);
    const unpriced = campaigns.data.length - costed.length;
    const exclusions = unpriced ? [`${plural(unpriced, "campaign")} with no actual cost recorded`] : [];
    const evidence = [ctx.ev(campaigns.source, { count: campaigns.data.length, exclusions }), ctx.ev(members.source, { count: members.data.length, note: "A deal is credited to every campaign its lead was in, so returns overlap" })];
    if (costed.length === 0) return { outcome: "not_measurable", reason: "No campaign has an actual cost recorded, so a return can't be worked out.", evidence };
    const pipeline = loadPipeline(ctx);
    if (pipeline.status !== "ok") return { outcome: "not_measurable", reason: pipeline.reason, evidence };
    if (pipeline.multiCurrency) return { outcome: "not_measurable", reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied.", evidence };
    const won = new Map(pipeline.deals.filter((d) => d.category === "won").map((d) => [d.Id, d.Amount ?? 0]));
    const dealOf = new Map(data.leads.flatMap((l) => (l.ConvertedOpportunityId ? [[l.Id, l.ConvertedOpportunityId] as const] : [])));
    const rows = groupByType(costed).map(([type, group]) => {
      const ids = new Set(group.map((c) => c.Id));
      const deals = new Set(members.data.flatMap((m) => {
        const deal = m.LeadId && ids.has(m.CampaignId) ? dealOf.get(m.LeadId) : undefined;
        return deal && won.has(deal) ? [deal] : [];
      }));
      return { type, cost: group.reduce((sum, c) => sum + c.ActualCost!, 0), returned: [...deals].reduce((sum, id) => sum + won.get(id)!, 0) };
    });
    const under = rows.filter((r) => r.returned < r.cost).sort((a, b) => a.returned / a.cost - b.returned / b.cost || a.type.localeCompare(b.type));
    const spend = rows.reduce((sum, r) => sum + r.cost, 0);
    const segments = [{ dimension: "campaign type", segments: rows.length }];
    if (under.length === 0) return { outcome: "pass", summary: `Every campaign type with a recorded cost returned more than it cost, across ${money(spend, ctx.calibration.currency)} of spend.`, evidence, segmentsChecked: segments, facts: { spend, types: rows.length } };
    const gap = Math.round(under.reduce((sum, r) => sum + r.cost - r.returned, 0));
    return {
      outcome: "issue",
      title: "Some campaign spend isn't coming back",
      summary: `${under.map((r) => r.type).join(", ")} cost ${money(under.reduce((sum, r) => sum + r.cost, 0), ctx.calibration.currency)} and returned ${money(under.reduce((sum, r) => sum + r.returned, 0), ctx.calibration.currency)} in won deals from its leads.`,
      recommendation: "Check the attribution first, because deals with contacts on a campaign aren't counted here. Then decide whether to keep this spend.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "medium",
      evidence,
      value: { kind: "revenue", amount: gap, unit: ctx.calibration.currency, period: "campaign spend to date", assumptions: ["Actual Cost as recorded on each campaign", "Return is the Amount of won deals converted from leads on the campaign", "A deal is credited in full to every campaign its lead was in", "Influence on existing contacts isn't counted, so return may be understated"] },
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: segments,
      subjects: under.map((r) => ({ type: "campaign type", label: r.type })),
      facts: { spend, gap, types: under.length },
    };
  },
};

function groupByType<C extends { Type: string | null }>(campaigns: C[]): Array<[string, C[]]> {
  const groups = new Map<string, C[]>();
  for (const campaign of campaigns) groups.set(campaign.Type ?? "No type", [...(groups.get(campaign.Type ?? "No type") ?? []), campaign]);
  return [...groups].sort(([a], [b]) => a.localeCompare(b));
}

function funnelMetrics(ctx: Ctx): Metric[] {
  const shared = { domain: DOMAIN, exclusions: [] as string[], assumptions: [zoneNote(ctx)] };
  const definitions = {
    conversion: { ...shared, id: "lead.conversion_rate", label: "Lead conversion", unit: "percent" as const, definition: `Leads converted, as a share of leads at least ${CONVERSION_AGE_DAYS} days old.` },
    response: { ...shared, id: "lead.median_response_hours", label: "First response time", unit: "count" as const, definition: "Median hours from a lead's creation to its first task, event or status change. The unit is hours." },
    wonPerLead: { ...shared, id: "lead.won_per_lead", label: "Won per lead", unit: "currency" as const, definition: "Amount of won deals converted from leads, divided by leads." },
  };
  const data = loadLeads(ctx);
  if (data.status !== "ok") return Object.values(definitions).map((d) => ({ ...d, status: "not_measurable", reason: data.reason, evidence: data.evidence }));
  // No metric is published without the deals behind the funnel. Conversion alone
  // could be counted, but a funnel that stops at conversion invites the wrong read.
  const deals = loadPipeline(ctx);
  if (deals.status !== "ok") return Object.values(definitions).map((d) => ({ ...d, status: "not_measurable", reason: `The funnel is only reported through to deals, and they couldn't be read. ${deals.reason}`, evidence: data.evidence }));
  const { minSample } = ctx.calibration;
  const evidence = data.evidence;
  const metrics: Metric[] = [];
  const rest = { dateRange: ctx.window, segmentExemptReason: null, series: [], stats: {} };
  const dims = dimensions(ctx);
  const cuts = (rows: Lead[], calculate: (group: Lead[]) => number): MetricSegment[] =>
    Object.entries(dims).flatMap(([dimension, keyOf]) =>
      groupsOf(rows, keyOf).map(([key, group]) => {
        const thin = group.length < minSample;
        return { dimension, key, n: group.length, value: thin ? null : calculate(group), notMeasurableReason: thin ? `Sample below the calibrated minimum (${group.length} of ${minSample})` : null };
      }),
    );
  const rate = (group: Lead[]): number => pct(group.filter((l) => l.IsConverted).length, group.length);
  const leads = settled(ctx, data);
  if (leads.length < minSample) metrics.push({ ...definitions.conversion, evidence, status: "not_measurable", reason: `${leads.length} leads is below the calibrated minimum of ${minSample}.` });
  else metrics.push({ ...definitions.conversion, evidence, status: "measured", value: rate(leads), n: leads.length, segments: cuts(leads, rate), ...rest });

  const touched = judgeable(ctx, data).filter((l) => l.firstTouch);
  if (!data.activityReadable) metrics.push({ ...definitions.response, evidence, status: "not_measurable", reason: "Activities couldn't be read in full." });
  else if (touched.length < minSample) metrics.push({ ...definitions.response, evidence, status: "not_measurable", reason: `${touched.length} leads with a response is below the calibrated minimum of ${minSample}.` });
  else {
    const hours = (group: Lead[]): number => round1(median(group.map(hoursToTouch)));
    metrics.push({ ...definitions.response, evidence, status: "measured", value: hours(touched), n: touched.length, segments: cuts(touched, hours), ...rest });
  }

  const returns = sourceReturns(ctx, data);
  if ("reason" in returns) metrics.push({ ...definitions.wonPerLead, evidence, status: "not_measurable", reason: returns.reason });
  else {
    const per = (r: { won: number; leads: number }): number => Math.round(r.won / r.leads);
    const total = returns.rows.reduce((acc, r) => ({ won: acc.won + r.won, leads: acc.leads + r.leads }), { won: 0, leads: 0 });
    const segments: MetricSegment[] = returns.rows.map((r) => ({ dimension: "source", key: r.source, n: r.leads, value: r.leads < minSample ? null : per(r), notMeasurableReason: r.leads < minSample ? `Sample below the calibrated minimum (${r.leads} of ${minSample})` : null }));
    metrics.push({ ...definitions.wonPerLead, evidence, status: "measured", value: per(total), n: total.leads, segments, ...rest });
  }
  return metrics;
}

export const leadFunnelModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Lead funnel",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, activityExtractor, leadsExtractor],
  metrics: [funnelMetrics],
  rules: [conversionBySource, responseTime, noResponse, convertedWithoutOpportunity, sourceReturn, campaignReturn],
};
