import { activityExtractor } from "../../extract/activity.ts";
import { schemaExtractor } from "../../extract/configuration.ts";
import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { activityCaptureExtractor, usersExtractor } from "../../extract/users.ts";
import { notSized, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { shareRule } from "../current-pipeline/index.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { closedInWindow, median, segment } from "../historic-pipeline/metrics.ts";
import { localDay, todayIn, zoneNote, zoneOf } from "../time-zone.ts";
import type { Evidence, Metric, SeriesPoint } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { loadActivity, type ActivityData, type Touch } from "./load.ts";

const DOMAIN = "activity";
const RECENT_MONTHS = 3;
const PRIOR_MONTHS = 6;
const COLLAPSE_RATIO = 0.5;
const STEADY_VOLUME = 0.25;
const MIN_GROUP_EVENTS = 10;
const RECENT_DAYS = 90;
const REP_FLOOR_SHARE = 0.25;
const MIN_REPS = 3;
const UNLINKED_LIMIT = 30;
const UNLINKED_SEGMENT_LIMIT = 60;

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<ActivityData, { status: "ok" }>;

function activityOr(ctx: Ctx): Ready | NotMeasurableResult {
  const data = loadActivity(ctx);
  if (data.status === "ok") return data;
  return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
}

const stamped = (ctx: Ctx, data: Ready, count: number | null, note?: string): Evidence => ({ ...data.evidence[1]!, count, exclusions: data.exclusions, ...(note ? { note } : {}) });
const confidence = (data: Ready): "high" | "medium" => (data.captureInUse ? "medium" : "high");

/** Whole months before the one holding `today`, oldest first. A part month can't be compared. */
function fullMonths(from: string, today: string): string[] {
  const months: string[] = [];
  const cursor = new Date(`${from.slice(0, 7)}-01T00:00:00Z`);
  // The first month is only whole if the window starts on its first day.
  if (from.slice(8, 10) !== "01") cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  while (cursor.toISOString().slice(0, 7) < today.slice(0, 7)) {
    months.push(cursor.toISOString().slice(0, 7));
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }
  return months;
}

type Window = { total: number; linked: number; share: number };
type Comparison = { prior: Window; recent: Window; collapsed: boolean; volumeFell: boolean };

function compare(events: Array<{ month: string; WhatId: string | null }>, prior: Set<string>, recent: Set<string>): Comparison {
  const window = (months: Set<string>): Window => {
    const rows = events.filter((e) => months.has(e.month));
    const linked = rows.filter((e) => e.WhatId !== null).length;
    return { total: rows.length, linked, share: rows.length === 0 ? 0 : Math.round((linked / rows.length) * 1000) / 10 };
  };
  const [before, after] = [window(prior), window(recent)];
  const perMonth = (w: Window, months: Set<string>): number => w.total / months.size;
  const volumeFell = perMonth(after, recent) < perMonth(before, prior) * (1 - STEADY_VOLUME);
  return { prior: before, recent: after, collapsed: before.share > 0 && after.share <= before.share * COLLAPSE_RATIO, volumeFell };
}

// Rule 4's reason for existing. A real check once missed a collapse here because
// total events looked normal: they were still syncing, but no longer linked to
// any record, so deals went dark while the chart stayed flat. So this watches the
// linked share over time, overall and for each team and rep, never the total.
const eventLinkCollapse: Rule = {
  id: "act.event-link-collapse",
  version: 1,
  domain: DOMAIN,
  title: "Events no longer linked to records",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = activityOr(ctx);
    if ("outcome" in data) return data;
    const users = ctx.data(usersExtractor);
    const months = fullMonths(data.from, todayIn(ctx));
    if (months.length < RECENT_MONTHS + PRIOR_MONTHS) return { outcome: "not_measurable", reason: `${plural(months.length, "whole month")} of events is too few to compare ${RECENT_MONTHS} recent months with the ${PRIOR_MONTHS} before them.`, evidence: data.evidence };
    const recent = new Set(months.slice(-RECENT_MONTHS));
    const prior = new Set(months.slice(-(RECENT_MONTHS + PRIOR_MONTHS), -RECENT_MONTHS));
    const overall = compare(data.events, prior, recent);
    const { minSample } = ctx.calibration;
    const evidence = [stamped(ctx, data, overall.prior.total + overall.recent.total, `Linked means the event has a related record. Compares ${[...prior][0]} to ${[...prior].at(-1)} with ${[...recent][0]} to ${[...recent].at(-1)}`)];
    if (overall.prior.total < minSample) return { outcome: "not_measurable", reason: `${overall.prior.total} events in the comparison months is below the calibrated minimum of ${minSample}.`, evidence };

    const roles = new Map(users.status === "ok" ? users.data.map((u) => [u.Id, u.UserRole?.Name ?? "No role"]) : []);
    const dimensions: Record<string, (e: { OwnerId: string }) => string> = { team: (e) => roles.get(e.OwnerId) ?? "No role", rep: (e) => ctx.userLabel(e.OwnerId) };
    const segmentsChecked = Object.entries(dimensions).map(([dimension, keyOf]) => ({ dimension, segments: new Set(data.events.map(keyOf)).size }));
    const hotspots = Object.entries(dimensions).flatMap(([dimension, keyOf]) => {
      const groups = new Map<string, typeof data.events>();
      for (const event of data.events) groups.set(keyOf(event), [...(groups.get(keyOf(event)) ?? []), event]);
      return [...groups].flatMap(([key, rows]) => {
        const comparison = compare(rows, prior, recent);
        return comparison.prior.total >= MIN_GROUP_EVENTS && comparison.collapsed ? [{ dimension, key, comparison }] : [];
      });
    }).sort((a, b) => a.comparison.recent.share - b.comparison.recent.share || a.dimension.localeCompare(b.dimension) || a.key.localeCompare(b.key));

    // Month by month, so a narrative can quote the fall and the number check can trace it.
    const byMonth = Object.fromEntries(months.map((month) => [`linkedShare ${month}`, compare(data.events, new Set([month]), new Set([month])).prior.share]));
    const facts = { priorShare: overall.prior.share, recentShare: overall.recent.share, priorEvents: overall.prior.total, recentEvents: overall.recent.total, ...byMonth };
    if (!overall.collapsed && hotspots.length === 0) {
      return { outcome: "pass", summary: `Events linked to a record were ${overall.prior.share}% before and ${overall.recent.share}% in the last ${RECENT_MONTHS} whole months, with no team or rep falling by half.`, evidence, segmentsChecked, facts };
    }
    const volume = overall.volumeFell ? "Total events fell as well, so check whether events are syncing at all." : "Total events held steady, so a monthly total won't show this: events are still arriving, but they aren't reaching the records they belong to.";
    const capture = ctx.data(activityCaptureExtractor);
    const manualStepIds = capture.status === "ok" && capture.data.length === 0 ? [] : [ctx.manual("eac-advanced-sync").step.id];
    return {
      outcome: "issue",
      title: overall.collapsed ? "Events have stopped being linked to your deals" : "Some teams' events have stopped being linked to deals",
      summary: overall.collapsed
        ? `Events linked to a record fell from ${overall.prior.share}% to ${overall.recent.share}% in the last ${RECENT_MONTHS} whole months. ${volume}`
        : `The overall linked share looks fine at ${overall.recent.share}%, but it has fallen by at least half for ${hotspots[0]!.dimension} ${hotspots[0]!.key}, from ${hotspots[0]!.comparison.prior.share}% to ${hotspots[0]!.comparison.recent.share}%.`,
      recommendation: "Find what changed at the start of the fall. Check Einstein Activity Capture's Relate Synced Events setting, and any change to how calendars sync.",
      recommendationType: "investigate",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("Lost links make deals look quiet, and we can't tell which deals they belonged to."),
      effort: { size: "S" },
      owner: "Salesforce admin",
      dependencies: ["Activity won't relink itself: ask Salesforce support whether synced events can be related after the fix"],
      segmentsChecked,
      subjects: hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts,
      manualStepIds,
    };
  },
};

const touchesByDeal = (data: Ready): Map<string, Touch[]> => {
  const byDeal = new Map<string, Touch[]>();
  for (const touch of data.touches) {
    if (!touch.dealId) continue;
    const list = byDeal.get(touch.dealId);
    if (list) list.push(touch);
    else byDeal.set(touch.dealId, [touch]);
  }
  return byDeal;
};

// Only deals whose whole life is inside the readable year. An older deal has lost
// its early activity to archiving, which would read as a deal nobody worked.
const closedThisYear = (ctx: Ctx, deals: Deal[], from: string): Deal[] =>
  deals.filter((d) => (d.category === "won" || d.category === "lost") && localDay(d.CreatedDate, zoneOf(ctx)) >= from && closedInWindow(d, ctx.window));

const dealsWithoutActivity = shareRule({
  id: "act.deals-without-activity",
  domain: DOMAIN,
  title: "Closed deals with nothing logged",
  totalLimit: 25,
  segmentLimit: 50,
  severity: "medium",
  scopeLabel: "won and lost deals created in the last year",
  scope(ctx, pipeline) {
    const data = loadActivity(ctx);
    if (data.status !== "ok") return { reason: data.reason, evidence: data.evidence };
    const touches = touchesByDeal(data);
    return { deals: closedThisYear(ctx, pipeline.deals, data.from), isHit: (d) => !touches.has(d.Id), exclusions: [...data.exclusions, "Deals created before the readable year"], evidence: data.evidence.slice(0, 1) };
  },
  issueTitle: "Deals are closing with nothing logged against them",
  sentence: (hits, n, rate) => `Won and lost deals with no task, event or email against them: ${hits} of ${n} (${rate}%). What happened on those deals isn't in Salesforce.`,
  recommendation: "Find out whether the work isn't happening or isn't being logged, then fix capture before judging anyone on activity.",
  owner: "Head of sales",
  sized: false,
});

const wonAgainstLost: Rule = {
  id: "act.won-vs-lost-activity",
  version: 1,
  domain: DOMAIN,
  title: "Activity on won deals against lost deals",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = activityOr(ctx);
    if ("outcome" in data) return data;
    const pipeline = loadPipeline(ctx);
    if (pipeline.status !== "ok") return { outcome: "not_measurable", reason: pipeline.reason, evidence: pipeline.evidence };
    const touches = touchesByDeal(data);
    const count = (d: Deal): number => touches.get(d.Id)?.length ?? 0;
    const deals = closedThisYear(ctx, pipeline.deals, data.from);
    const won = deals.filter((d) => d.category === "won");
    const lost = deals.filter((d) => d.category === "lost");
    const { minSample } = ctx.calibration;
    const evidence = [stamped(ctx, data, deals.length, "Won and lost deals created in the readable year")];
    if (won.length < minSample || lost.length < minSample) return { outcome: "not_measurable", reason: `${won.length} won and ${lost.length} lost deals is below the calibrated minimum of ${minSample} each.`, evidence };
    const wonMedian = median(won.map(count));
    const lostMedian = median(lost.map(count));
    const teams = new Set(deals.map((d) => d.team)).size;
    const segments = [{ dimension: "outcome", segments: 2 }, { dimension: "team", segments: teams }];
    const facts = { wonMedian, lostMedian, won: won.length, lost: lost.length };
    if (wonMedian > lostMedian) return { outcome: "pass", summary: `Won deals have a median of ${wonMedian} logged activities against ${lostMedian} on lost deals.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Lost deals get as much logged effort as won ones",
      summary: `Lost deals have a median of ${lostMedian} logged activities against ${wonMedian} on won deals, so effort isn't going where deals are won, or isn't being logged evenly.`,
      recommendation: "Look at when deals are qualified out. If they're dropped late, that's where the time goes.",
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence,
      value: notSized("We can't convert logged activity into hours without knowing what each one took."),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: segments,
      facts,
    };
  },
};

const repActivity: Rule = {
  id: "act.rep-activity-spread",
  version: 1,
  domain: DOMAIN,
  title: "Reps logging far less than their peers",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = activityOr(ctx);
    if ("outcome" in data) return data;
    const pipeline = loadPipeline(ctx);
    if (pipeline.status !== "ok") return { outcome: "not_measurable", reason: pipeline.reason, evidence: pipeline.evidence };
    const users = ctx.data(usersExtractor);
    if (users.status !== "ok") return { outcome: "not_measurable", reason: `Users couldn't be read: ${users.reason}`, evidence: [ctx.ev(users.source, { count: null })] };
    const active = new Set(users.data.filter((u) => u.IsActive).map((u) => u.Id));
    const cutoff = daysBefore(ctx.now, RECENT_DAYS).toISOString();
    const openByRep = new Map<string, number>();
    for (const deal of pipeline.deals) if (deal.category === "open" && active.has(deal.OwnerId)) openByRep.set(deal.OwnerId, (openByRep.get(deal.OwnerId) ?? 0) + 1);
    const evidence = [stamped(ctx, data, openByRep.size, `Logged in the last ${RECENT_DAYS} days, per open deal`)];
    if (openByRep.size < MIN_REPS) return { outcome: "not_measurable", reason: `${plural(openByRep.size, "rep")} with open deals is too few to compare.`, evidence };
    const recent = (touch: Touch): boolean => new Date(touch.at).toISOString() >= cutoff;
    const rates = [...openByRep].map(([rep, deals]) => ({ rep, deals, rate: Math.round((data.touches.filter((t) => t.ownerId === rep && recent(t)).length / deals) * 100) / 100 }));
    const typical = median(rates.map((r) => r.rate));
    const low = rates.filter((r) => r.rate < typical * REP_FLOOR_SHARE).sort((a, b) => a.rate - b.rate || a.rep.localeCompare(b.rep));
    const segments = [{ dimension: "rep", segments: rates.length }];
    const facts = { typicalPerDeal: typical, reps: rates.length, low: low.length };
    if (low.length === 0) return { outcome: "pass", summary: `No rep logs under ${REP_FLOOR_SHARE * 100}% of the typical ${typical} activities per open deal.`, evidence, segmentsChecked: segments, facts };
    return {
      outcome: "issue",
      title: "Some reps are logging far less than their peers",
      summary: `${plural(low.length, "rep")} logged under ${REP_FLOOR_SHARE * 100}% of the typical ${typical} activities per open deal in the last ${RECENT_DAYS} days. This may be a logging gap rather than a performance one.`,
      recommendation: "Check their email and calendar connections first, then look at how they're working these deals.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: confidence(data),
      evidence,
      value: notSized("A low count may be a logging gap, so it can't be sized as lost revenue."),
      effort: { size: "XS" },
      owner: "Head of sales",
      segmentsChecked: segments,
      subjects: low.map((r) => ({ type: "user", label: ctx.userLabel(r.rep) })),
      facts,
    };
  },
};

const unlinkedTasks: Rule = {
  id: "act.unlinked-tasks",
  version: 1,
  domain: DOMAIN,
  title: "Tasks linked to nothing",
  evaluate(ctx): RuleResult | RuleResult[] {
    const data = activityOr(ctx);
    if ("outcome" in data) return data;
    const evidence = [{ ...data.evidence[0]!, count: data.tasks.length, exclusions: data.exclusions }];
    if (data.tasks.length === 0) return { outcome: "not_measurable", reason: "No tasks were logged in the last year, so we can't tell a clean record from an unused one.", evidence };
    const result = segmentRate(data.tasks, (t) => t.WhatId === null && t.WhoId === null, { rep: (t) => ctx.userLabel(t.OwnerId), "task type": (t) => t.TaskSubtype ?? "No type" }, { segmentLimit: UNLINKED_SEGMENT_LIMIT, minSegment: MIN_GROUP_EVENTS });
    const facts = { hits: result.hits, n: result.n, rate: result.rate };
    if (result.rate <= UNLINKED_LIMIT && result.hotspots.length === 0) return { outcome: "pass", summary: `Tasks linked to no record: ${result.hits} of ${result.n} (${result.rate}%), with no rep above ${UNLINKED_SEGMENT_LIMIT}%.`, evidence, segmentsChecked: result.segmentsChecked, facts };
    const worst = result.hotspots[0];
    return {
      outcome: "issue",
      title: "Tasks are being logged against nothing",
      summary: `Tasks linked to no record: ${result.hits} of ${result.n} (${result.rate}%).${worst ? ` The worst segment is ${worst.dimension} ${worst.key} at ${worst.rate}%.` : ""} This work won't show on any deal, contact or account.`,
      recommendation: "Show these people how to log against the deal, and check any tool that's creating tasks automatically.",
      recommendationType: "change",
      severity: "low",
      confidence: "high",
      evidence,
      value: notSized("We can't say which deals these tasks belonged to."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts,
    };
  },
};

function activityMetrics(ctx: Ctx): Metric[] {
  const shared = (label: string) => ({ domain: DOMAIN, label, exclusions: [] as string[], assumptions: [zoneNote(ctx)] });
  const definitions = {
    linked: { ...shared("Events linked to a record"), id: "activity.event_linked_share", unit: "percent" as const, definition: "Events with a related record, as a share of all events, by month. The value is the latest whole month." },
    won: { ...shared("Activities per won deal"), id: "activity.per_won_deal", unit: "count" as const, definition: "Median tasks, events and emails logged on won deals created in the readable year." },
    lost: { ...shared("Activities per lost deal"), id: "activity.per_lost_deal", unit: "count" as const, definition: "Median tasks, events and emails logged on lost deals created in the readable year." },
  };
  const data = loadActivity(ctx);
  if (data.status !== "ok") return Object.values(definitions).map((d) => ({ ...d, status: "not_measurable", reason: data.reason, evidence: data.evidence }));
  const common = { exclusions: data.exclusions, assumptions: [...definitions.won.assumptions, ...data.assumptions], evidence: data.evidence.slice(0, 2) };
  const { minSample } = ctx.calibration;
  const metrics: Metric[] = [];
  // No metric is published without the deals it's about, the link share included:
  // it says deals are going dark, which can't be stood behind with no deals to hand.
  const pipeline = loadPipeline(ctx);
  if (pipeline.status !== "ok") return Object.values(definitions).map((d) => ({ ...d, ...common, status: "not_measurable", reason: pipeline.reason }));
  const months = fullMonths(data.from, todayIn(ctx));
  const series: SeriesPoint[] = months.map((month) => {
    const rows = data.events.filter((e) => e.month === month);
    const thin = rows.length < minSample;
    const last = new Date(Date.UTC(Number(month.slice(0, 4)), Number(month.slice(5, 7)), 0)).toISOString().slice(0, 10);
    return { date: last, value: thin ? null : Math.round((rows.filter((e) => e.WhatId !== null).length / rows.length) * 1000) / 10, n: rows.length, notMeasurableReason: thin ? `${rows.length} events is below the calibrated minimum of ${minSample}` : null };
  });
  const latest = series.filter((p) => p.value !== null).at(-1);
  if (!latest) metrics.push({ ...definitions.linked, ...common, status: "not_measurable", reason: `No whole month has ${minSample} events.` });
  else metrics.push({ ...definitions.linked, ...common, status: "measured", value: latest.value!, n: data.events.length, dateRange: { from: data.from, to: ctx.window.to }, segments: [], segmentExemptReason: "Shown by month. Team and rep cuts are in the event link check.", series, stats: {} });

  const touches = touchesByDeal(data);
  const count = (d: Deal): number => touches.get(d.Id)?.length ?? 0;
  const deals = closedThisYear(ctx, pipeline.deals, data.from);
  const dims: Record<string, (d: Deal) => string> = { team: (d) => d.team, rep: (d) => ctx.userLabel(d.OwnerId) };
  for (const [category, definition] of [["won", definitions.won], ["lost", definitions.lost]] as const) {
    const rows = deals.filter((d) => d.category === category);
    if (rows.length < minSample) metrics.push({ ...definition, ...common, status: "not_measurable", reason: `${rows.length} ${category} deals is below the calibrated minimum of ${minSample}.` });
    else metrics.push({ ...definition, ...common, status: "measured", value: median(rows.map(count)), n: rows.length, dateRange: { from: data.from, to: ctx.window.to }, segments: segment(rows, dims, minSample, (group) => median(group.map(count))), segmentExemptReason: null, series: [], stats: {} });
  }
  return metrics;
}

export const activityModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Activity",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, activityCaptureExtractor, activityExtractor],
  metrics: [activityMetrics],
  rules: [eventLinkCollapse, dealsWithoutActivity, wonAgainstLost, repActivity, unlinkedTasks],
};
