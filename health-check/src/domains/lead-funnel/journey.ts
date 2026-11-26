import { pct, plural, notSized, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { org, said, type Said } from "../../engine/org-words.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck, SeriesPoint } from "../../schemas/index.ts";
import { meanStat } from "../average.ts";
import { loadPipeline } from "../historic-pipeline/cohort.ts";
import { groupBy, median, quantile, round1 } from "../historic-pipeline/metrics.ts";
import { bandedZ, MIN_EACH_OUTCOME, NOT_A_CAUSE, WHOLE_SHARE, MIN_SEGMENTS, WIN_GAP, zBar, zOf } from "../historic-pipeline/paths/insights.ts";
import { dayPlus, daySaid, LAG_QUANTILE, OPEN_SHARE } from "../historic-pipeline/reached.ts";
import { localDay, zoneNote, zoneOf } from "../time-zone.ts";
import { loadLeads, type Lead } from "./load.ts";

// The lead journey (KAN-46 slice 1, DECISIONS.md 770), from what the engine
// already reads: a lead's creation, its conversion and the deal it converted
// into, followed to that deal's close. No new read. Behind HC_LEAD_JOURNEY
// (index.ts leadJourneyModule): off, none of this runs.
//
// Who is counted is chosen by the day a lead was made, never by when its deal
// closed (712): leads made from the window's start to the run's date less the
// journey's maturity lag, the days within which 9 in 10 of the leads that
// became closed deals got there, and never fewer than the 30 days a lead is
// given to convert. A lead made later hasn't had the time, so it is left out by
// that rule, never counted as a lead that failed. Not read while more than 1 in
// 20 of the deals from those leads are still open; the few that are go each way
// in every comparison. Days are whole calendar days: ConvertedDate is a day with
// no time, and a deal's close is a day, so a lead's creation is taken as its day
// in the org's time zone and nothing finer is claimed.
//
// A deal converted from more than one lead is one deal, counted once from its
// first lead, so no deal is two results. Both sides of every comparison are
// deals from converted leads made in the same period: never a group against
// survivors (703), never converted leads against leads that didn't convert.

type Ctx = Omit<RuleContext, "metrics">;

const DOMAIN = "lead_funnel";
const DAY = 86_400_000;
/** A lead's least time to convert, as lead.conversion-by-source gives it: the lag is never shorter. */
const LEAST_LAG_DAYS = 30;
/** "Converted quickly", for the stored insight: within a week of the lead's creation, fixed before looking. */
export const FAST_DAYS = 7;
/** Sources raised at most by the journey's check, the widest gaps first. */
const MOST_RAISED = 3;

type Outcome = "won" | "lost" | "open";

/** A deal from a lead made in the period: its first lead, how it stands, and the days from that lead to its close. */
export type JourneyDeal = { id: string; lead: Lead; madeAt: number; outcome: Outcome; won: boolean; open: boolean; days: number | null; convertedDays: number | null };

/** A lead made in the period, with how its deal stands where it can be followed. */
type JourneyLead = { lead: Lead; outcome: Outcome | null };

export type Cohort =
  | { status: "not_measurable"; reason: string; evidence: Evidence[] }
  | { status: "ok"; from: string; to: string; lagDays: number; leads: JourneyLead[]; deals: JourneyDeal[]; exclusions: string[]; evidence: Evidence[] };

/** Whole days from one calendar day to another. */
export const daysBetween = (from: string, to: string): number => Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / DAY);
const created = (l: Lead): Date => new Date(l.CreatedDate.replace(/\+0000$/, "Z"));

/**
 * The leads of the period and the deals they became, or not measurable with the
 * reason. Made afresh for each metric builder and rule, as the pipeline is read,
 * so each carries what the leads' and deals' reads say of themselves.
 */
export function journeyOf(ctx: Ctx): Cohort {
  const data = loadLeads(ctx, true);
  if (data.status !== "ok") return { status: "not_measurable", reason: data.reason, evidence: data.evidence };
  const pipeline = loadPipeline(ctx);
  if (pipeline.status !== "ok") return { status: "not_measurable", reason: pipeline.reason, evidence: data.evidence };
  const { minSample } = ctx.calibration;
  const zone = zoneOf(ctx);
  const dayOf = (l: Lead): string => localDay(created(l), zone);
  const deals = new Map(pipeline.deals.map((d) => [d.Id, d]));

  // Each deal's first lead, earliest made, then by id.
  const first = new Map<string, Lead>();
  for (const lead of data.leads) {
    const id = lead.IsConverted ? lead.ConvertedOpportunityId : null;
    if (!id) continue;
    const seen = first.get(id);
    if (!seen || created(lead) < created(seen) || (created(lead).getTime() === created(seen).getTime() && lead.Id < seen.Id)) first.set(id, lead);
  }
  const followed = new Map<string, JourneyDeal>();
  for (const [id, lead] of first) {
    const deal = deals.get(id);
    if (!deal) continue;
    const outcome: Outcome | null = deal.category === "won" ? "won" : deal.category === "lost" || deal.category === "no_decision" ? "lost" : deal.category === "open" ? "open" : null;
    if (outcome === null) continue;
    const days = outcome === "open" ? null : daysBetween(dayOf(lead), deal.closedDay);
    // A deal that closed before its first lead was made isn't a journey from that lead.
    if (days !== null && days < 0) continue;
    const convertedDays = lead.ConvertedDate ? Math.max(0, daysBetween(dayOf(lead), lead.ConvertedDate)) : null;
    followed.set(id, { id, lead, madeAt: created(lead).getTime(), outcome, won: outcome === "won", open: outcome === "open", days, convertedDays });
  }

  const closedDays = [...followed.values()].flatMap((d) => (d.days === null ? [] : [d.days]));
  const evidence = data.evidence;
  if (closedDays.length < minSample) return { status: "not_measurable", reason: said`${closedDays.length} closed deals from leads is below the calibrated minimum of ${minSample}.`, evidence };
  const lagDays = Math.max(LEAST_LAG_DAYS, Math.ceil(quantile(closedDays, LAG_QUANTILE)));
  const from = ctx.window.from;
  const to = dayPlus(localDay(ctx.now, zone), -lagDays);
  if (to < from) return { status: "not_measurable", reason: said`Leads take so long to become closed deals here that none created since ${daySaid(from)} has had the ${lagDays} days it takes, so none can be counted yet.`, evidence };

  const inPeriod = (l: Lead): boolean => dayOf(l) >= from && dayOf(l) <= to;
  const recent = data.leads.filter((l) => dayOf(l) > to).length;
  const dealOfLead = (l: Lead): JourneyDeal | undefined => (l.IsConverted && l.ConvertedOpportunityId ? followed.get(l.ConvertedOpportunityId) : undefined);
  const notFollowed = (l: Lead): boolean => l.IsConverted && l.ConvertedOpportunityId !== null && !dealOfLead(l);
  const leads: JourneyLead[] = data.leads.filter((l) => inPeriod(l) && !notFollowed(l)).map((lead) => ({ lead, outcome: dealOfLead(lead)?.outcome ?? null }));
  const left = data.leads.filter((l) => inPeriod(l) && notFollowed(l)).length;
  const exclusions = [
    ...data.exclusions,
    ...(recent ? [said`${plural(recent, "lead")} created after ${daySaid(to)}, too recent to have had the ${lagDays} days within which 9 in 10 of the leads that became closed deals got there`] : []),
    ...(left ? [said`${plural(left, "lead")} converted into a deal we couldn't follow: not read, at a stage with no meaning, or closed before the lead was made`] : []),
  ];
  const kept = [...followed.values()].filter((d) => inPeriod(d.lead));
  return { status: "ok", from, to, lagDays, leads, deals: kept, exclusions, evidence: [{ ...evidence[0]!, count: leads.length, exclusions }] };
}


/** Too many still open to say how the period's deals end, or null when few enough are. */
function tooOpen(c: Extract<Cohort, { status: "ok" }>): Said | null {
  const open = c.deals.filter((d) => d.open).length;
  return c.deals.length > 0 && open > OPEN_SHARE * c.deals.length ? said`Of the ${c.deals.length} deals from leads made early enough to judge, ${open} are still open, too many to say how they end.` : null;
}

/** The cohort, or not measurable with the reason: a reason of its own, too many still open, or too few leads in it. */
function readyOrWhy(ctx: Ctx): Extract<Cohort, { status: "ok" }> | { reason: string; evidence: Evidence[] } {
  const c = journeyOf(ctx);
  if (c.status !== "ok") return { reason: c.reason, evidence: c.evidence };
  const { minSample } = ctx.calibration;
  if (c.leads.length < minSample) return { reason: said`${c.leads.length} leads made early enough to judge is below the calibrated minimum of ${minSample}.`, evidence: c.evidence };
  const open = tooOpen(c);
  return open ? { reason: open, evidence: c.evidence } : c;
}

const periodSaid = (c: { from: string; to: string }): Said => said`between ${daySaid(c.from)} and ${daySaid(c.to)}`;
const thin = (n: number, minSample: number): string => said`Sample below the calibrated minimum (${n} of ${minSample})`;

// ─── The metrics ─────────────────────────────────────────────────────────────

/** The first day of the calendar quarter a day is in. */
const quarterOf = (day: string): string => `${day.slice(0, 4)}-${String(Math.floor((Number(day.slice(5, 7)) - 1) / 3) * 3 + 1).padStart(2, "0")}-01`;

/** Lead to won deal (median days, the average beside it, 709) and the share of leads that became won deals, by source and by the quarter they were made. */
export function journeyMetrics(ctx: Ctx): Metric[] {
  const shared = { domain: DOMAIN, exclusions: [] as string[], assumptions: [zoneNote(ctx)] };
  const definitions = {
    days: { ...shared, id: "lead.days_to_won", label: said`Lead to won deal`, unit: "days" as const, definition: said`Median calendar days from a lead's creation to the close of the won deal it was converted into, for leads made early enough to have closed.` },
    won: { ...shared, id: "lead.won_lead_rate", label: said`Leads that became won deals`, unit: "percent" as const, definition: said`Leads converted into a deal that was won, as a share of the leads made early enough to judge. A lead not converted counts as not won.` },
  };
  const ready = readyOrWhy(ctx);
  if (!("leads" in ready)) return Object.values(definitions).map((d) => ({ ...d, status: "not_measurable", reason: ready.reason, evidence: ready.evidence }));
  const c = ready;
  const { minSample } = ctx.calibration;
  const assumptions = [
    zoneNote(ctx),
    said`Leads made ${periodSaid(c)} are counted: the run's date less ${c.lagDays} days, the time within which 9 in 10 of the leads that became closed deals got there`,
    said`Days are whole calendar days, from the day a lead was made to the day its deal closed, because Salesforce keeps the day a lead converted without a time`,
    said`A deal converted from more than one lead is counted once, from the first of them`,
  ];
  const rest = { dateRange: { from: c.from, to: c.to }, segmentExemptReason: null, series: [] as SeriesPoint[], exclusions: c.exclusions, assumptions };
  const metrics: Metric[] = [];
  const evidence = c.evidence;

  const won = c.deals.filter((d) => d.won);
  if (won.length < minSample) metrics.push({ ...definitions.days, evidence, status: "not_measurable", reason: said`${won.length} won deals from leads is below the calibrated minimum of ${minSample}.` });
  else {
    const days = (group: JourneyDeal[]): number => round1(median(group.map((d) => d.days!)));
    const segments: MetricSegment[] = [...groupBy(won, (d) => d.lead.source)].sort(([a], [b]) => a.localeCompare(b)).map(([key, group]) => ({ dimension: "source", key, n: group.length, value: group.length < minSample ? null : days(group), notMeasurableReason: group.length < minSample ? thin(group.length, minSample) : null }));
    metrics.push({ ...definitions.days, ...rest, evidence, status: "measured", value: days(won), n: won.length, segments, stats: meanStat(won.map((d) => d.days!)) });
  }

  const rate = (group: JourneyLead[]): number => pct(group.filter((l) => l.outcome === "won").length, group.length);
  const segments: MetricSegment[] = [...groupBy(c.leads, (l) => l.lead.source)].sort(([a], [b]) => a.localeCompare(b)).map(([key, group]) => ({ dimension: "source", key, n: group.length, value: group.length < minSample ? null : rate(group), notMeasurableReason: group.length < minSample ? thin(group.length, minSample) : null }));
  const series: SeriesPoint[] = [...groupBy(c.leads, (l) => quarterOf(localDay(created(l.lead), zoneOf(ctx))))].sort(([a], [b]) => a.localeCompare(b)).map(([date, group]) => ({ date, n: group.length, value: group.length < minSample ? null : rate(group), notMeasurableReason: group.length < minSample ? thin(group.length, minSample) : null }));
  const converted = c.leads.filter((l) => l.lead.IsConverted).length;
  const closed = c.deals.filter((d) => !d.open);
  const stats = { converted, conversionRate: pct(converted, c.leads.length), closedDeals: closed.length, wonDeals: won.length, winRate: pct(won.length, closed.length), openDeals: c.deals.length - closed.length, lagDays: c.lagDays };
  metrics.push({ ...definitions.won, ...rest, assumptions: [...assumptions, said`Quarters are calendar quarters, by the day each lead was made`], evidence, status: "measured", value: rate(c.leads), n: c.leads.length, segments, series, stats });
  return metrics;
}

// ─── Comparisons ─────────────────────────────────────────────────────────────

type Side = { n: number; won: number; rate: number };
const sideOf = (deals: JourneyDeal[]): Side => ({ n: deals.length, won: deals.filter((d) => d.won).length, rate: pct(deals.filter((d) => d.won).length, deals.length) });
type Bound = { gap: number; z: number; cmh: number };
type Compared = { group: Side; other: Side; gap: number; open: number; low: Bound; high: Bound; compared: number; held: number };

/** The group's open deals counted as lost and the other's as won (`groupOpenWon` false), or the other way round. */
function boundOf(groupAll: JourneyDeal[], otherAll: JourneyDeal[], groupOpenWon: boolean, from: string): Bound {
  const inGroup = new Set(groupAll);
  const wonOf = (d: JourneyDeal): boolean => (d.open ? inGroup.has(d) === groupOpenWon : d.won);
  const a = { n: groupAll.length, won: groupAll.filter(wonOf).length };
  const b = { n: otherAll.length, won: otherAll.filter(wonOf).length };
  return { gap: round1(pct(a.won, a.n) - pct(b.won, b.n)), z: Math.round(zOf(a, b) * 100) / 100, cmh: Math.round(bandedZ(groupAll, otherAll, wonOf, (d) => d.madeAt, from) * 100) / 100 };
}

/** Both sides' closed deals at the calibrated minimum, with enough won and lost, as the path insights hold them (703). */
const testable = (ctx: Ctx, a: Side, b: Side): boolean => [a, b].every((s) => s.n >= ctx.calibration.minSample && s.won >= MIN_EACH_OUTCOME && s.n - s.won >= MIN_EACH_OUTCOME);

/** Rule 4 as the path insights check it: only groups that can check something, in dimensions with two or more values. */
function withinGroups(ctx: Ctx, group: JourneyDeal[], other: JourneyDeal[], gap: number, keys: Array<(d: JourneyDeal) => string>): { compared: number; held: number } {
  const { minSample } = ctx.calibration;
  let compared = 0;
  let held = 0;
  for (const keyOf of keys) {
    const a = groupBy(group, keyOf);
    const b = groupBy(other, keyOf);
    if (new Set([...a.keys(), ...b.keys()]).size < 2) continue;
    for (const [key, inGroup] of a) {
      const inOther = b.get(key) ?? [];
      if (inGroup.length < minSample || inOther.length < minSample) continue;
      if (inGroup.length >= WHOLE_SHARE * group.length || inOther.length >= WHOLE_SHARE * other.length) continue;
      compared++;
      if (Math.sign(sideOf(inGroup).rate - sideOf(inOther).rate) === Math.sign(gap)) held++;
    }
  }
  return { compared, held };
}

/** A group's deals against the other side's, or null when either side's closed deals are too few to test. */
function compare(ctx: Ctx, from: string, groupAll: JourneyDeal[], otherAll: JourneyDeal[], keys: Array<(d: JourneyDeal) => string>): Compared | null {
  const [group, other] = [groupAll.filter((d) => !d.open), otherAll.filter((d) => !d.open)];
  const [g, o] = [sideOf(group), sideOf(other)];
  if (!testable(ctx, g, o)) return null;
  const gap = round1(g.rate - o.rate);
  return { group: g, other: o, gap, open: groupAll.length + otherAll.length - group.length - other.length, low: boundOf(groupAll, otherAll, false, from), high: boundOf(groupAll, otherAll, true, from), ...withinGroups(ctx, group, other, gap, keys) };
}

/** Every bar the path insights hold a gap to (703, 712), with the open counted both ways, inside the creation bands and the groups. */
function clears(c: Compared | null, bar: number): boolean {
  if (!c) return false;
  const side = Math.sign(c.low.gap);
  if (side === 0 || Math.sign(c.high.gap) !== side) return false;
  if ([c.low, c.high].some((b) => Math.abs(b.gap) < WIN_GAP || Math.abs(b.z) < bar || Math.sign(b.cmh) !== side || Math.abs(b.cmh) < bar)) return false;
  if (c.compared === 0) return true;
  return c.compared >= MIN_SEGMENTS && c.held * 2 > c.compared;
}

/** The smallest gap, in whole points, a comparison could have told from chance at the bar (as the path insights say it). */
function smallestTellable(c: Compared, bar: number): number {
  const p = (c.group.won + c.other.won) / (c.group.n + c.other.n);
  return Math.max(WIN_GAP, Math.ceil(bar * Math.sqrt(p * (1 - p) * (1 / c.group.n + 1 / c.other.n)) * 100));
}

const confidenceOf = (ctx: Ctx, c: Compared): "high" | "medium" | "low" => (c.compared === 0 ? "low" : Math.min(c.group.n, c.other.n) >= ctx.calibration.minSample * 2 ? "high" : "medium");

const stillOpen = (open: number): Said =>
  open === 0 ? said`` : open === 1 ? said` 1 deal from these leads is still open, and the gap holds whichever way it goes.` : said` ${open} deals from these leads are still open, and the gap holds whichever way they go.`;

const segmentsOf = (deals: JourneyDeal[], dims: Record<string, (d: JourneyDeal) => string>): SegmentCheck[] => Object.entries(dims).map(([dimension, keyOf]) => ({ dimension, segments: new Set(deals.map(keyOf)).size }));

// ─── The check: sources that convert well and win poorly, or the reverse ─────

export const sourceWinRate: Rule = {
  id: "lead.source-win-rate",
  version: 1,
  domain: DOMAIN,
  title: "Lead sources whose deals are won more or less often than they convert",
  records: true,
  evaluate(ctx): RuleResult | RuleResult[] {
    const ready = readyOrWhy(ctx);
    if (!("leads" in ready)) return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    const team = (d: JourneyDeal): string => d.lead.team;
    const sources = [...groupBy(c.leads, (l) => l.lead.source)].sort(([a], [b]) => a.localeCompare(b));
    const checked = segmentsOf(c.deals, { source: (d) => d.lead.source, team });
    if (sources.length < 2) return { outcome: "not_measurable", reason: said`Every lead made early enough to judge came from ${org(sources[0]![0])}, so there's no other source to compare with.`, evidence: c.evidence };
    const converted = (group: JourneyLead[]): number => pct(group.filter((l) => l.lead.IsConverted).length, group.length);
    const tried = sources.flatMap(([source, group]) => {
      const mine = c.deals.filter((d) => d.lead.source === source);
      const others = c.deals.filter((d) => d.lead.source !== source);
      const rest = c.leads.filter((l) => l.lead.source !== source);
      const cmp = group.length >= ctx.calibration.minSample ? compare(ctx, c.from, mine, others, [team]) : null;
      return cmp ? [{ source, leads: group.length, otherLeads: rest.length, conversion: converted(group), otherConversion: converted(rest), c: cmp }] : [];
    });
    if (tried.length === 0) {
      return { outcome: "not_measurable", reason: said`No lead source had ${ctx.calibration.minSample} or more leads and closed deals, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, to compare with the deals from other sources.`, evidence: c.evidence };
    }
    const bar = zBar(tried.length);
    // Only the two that say something new: converting at least as well and winning less, or converting less and winning more.
    const raised = tried
      .filter((t) => clears(t.c, bar) && (t.c.gap < 0 ? t.conversion >= t.otherConversion : t.conversion < t.otherConversion))
      .sort((a, b) => Math.abs(b.c.gap) - Math.abs(a.c.gap) || a.source.localeCompare(b.source))
      .slice(0, MOST_RAISED);
    if (raised.length === 0) {
      return {
        outcome: "pass",
        summary: said`We compared the win rate of deals from ${plural(tried.length, "lead source")} that had enough closed deals with the win rate of deals from the other sources. None converted well but won less often, or converted less often but won more, by a gap we could tell from chance.`,
        evidence: c.evidence,
        segmentsChecked: checked,
        facts: { sources: tried.length, leads: c.leads.length, deals: c.deals.length },
      };
    }
    return raised.map(({ source, leads, otherLeads, conversion, otherConversion, c: cmp }): RuleResult => {
      const winsLess = cmp.gap < 0;
      const within = cmp.compared
        ? said` The gap pointed the same way in ${cmp.held} of the ${cmp.compared} teams we could check it in.`
        : said` We couldn't check this within teams: these deals couldn't be split into teams big enough to compare on both sides.`;
      return {
        outcome: "issue",
        key: `source:${source}`,
        title: winsLess ? said`Leads from ${org(source)} convert well, but their deals are won less often` : said`Leads from ${org(source)} convert less often, but their deals are won more often`,
        summary: said`Of the leads created ${periodSaid(c)}, ${conversion}% of the ${leads} from ${org(source)} were converted, against ${otherConversion}% of the ${otherLeads} from other sources. Their ${cmp.group.n} closed deals were won ${cmp.group.rate}% of the time, against ${cmp.other.rate}% of the ${cmp.other.n} closed deals from other sources.${stillOpen(cmp.open)}${within}${NOT_A_CAUSE}`,
        recommendation: winsLess
          ? said`Check how leads from ${org(source)} are qualified before they're converted, and whether their deals are a good fit, before putting more into the source.`
          : said`Look at what makes deals from ${org(source)} win, and whether more of its leads are worth converting.`,
        recommendationType: "investigate",
        severity: "low",
        confidence: confidenceOf(ctx, cmp),
        evidence: c.evidence,
        value: notSized(said`We can't say what a different win rate from one source would be worth until we know why it differs.`),
        effort: { size: "S" },
        owner: "Head of marketing",
        segmentsChecked: checked,
        subjects: [{ type: "lead source", label: source }],
        facts: { leads, otherLeads, conversionRate: conversion, otherConversionRate: otherConversion, deals: cmp.group.n, otherDeals: cmp.other.n, winRate: cmp.group.rate, otherWinRate: cmp.other.rate, open: cmp.open, ...(cmp.compared ? { compared: cmp.compared, held: cmp.held } : {}) },
      };
    });
  },
};

// ─── The stored insight: fast conversion and winning (703, 708) ──────────────

/**
 * Of the deals from leads made in the period, those whose lead converted within
 * FAST_DAYS of its creation against those converted later, on the win rate of
 * the deals. Both sides converted and became deals, so neither is a group of
 * survivors. Stored, never served: in INSIGHT_RULES (scoring.ts), so it is kept
 * apart from the findings, scored nowhere and left out of every document and
 * digest, as pipe.path-insights is.
 */
export const fastConversion: Rule = {
  id: "lead.fast-conversion",
  version: 1,
  domain: DOMAIN,
  title: "Deals from leads converted quickly, against deals from leads converted later",
  records: true,
  evaluate(ctx): RuleResult | RuleResult[] {
    const ready = readyOrWhy(ctx);
    if (!("leads" in ready)) return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    const dims = { source: (d: JourneyDeal) => d.lead.source, team: (d: JourneyDeal) => d.lead.team };
    const timed = c.deals.filter((d) => d.convertedDays !== null);
    const fast = timed.filter((d) => d.convertedDays! <= FAST_DAYS);
    const later = timed.filter((d) => d.convertedDays! > FAST_DAYS);
    const cmp = compare(ctx, c.from, fast, later, Object.values(dims));
    const checked = segmentsOf(timed, dims);
    if (!cmp) {
      return { outcome: "not_measurable", reason: said`Fewer than ${ctx.calibration.minSample} closed deals, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, came from leads converted within ${FAST_DAYS} days, or from leads converted later, so the two can't be compared.`, evidence: c.evidence };
    }
    const bar = zBar(1);
    if (!clears(cmp, bar)) {
      return {
        outcome: "pass",
        summary: said`We couldn't tell whether deals from leads converted within ${FAST_DAYS} days were won more or less often than deals from leads converted later: a gap in win rate would have had to be about ${smallestTellable(cmp, bar)} points or more to tell it from chance, and it wasn't.`,
        evidence: c.evidence,
        segmentsChecked: checked,
        facts: { fast: cmp.group.n, later: cmp.other.n },
      };
    }
    const more = cmp.gap > 0;
    const closed = cmp.group.n + cmp.other.n;
    const who = cmp.open === 0 ? said`Of the ${closed} deals from leads created ${periodSaid(c)},` : said`Of the ${closed + cmp.open} deals from leads created ${periodSaid(c)}, ${cmp.open} ${cmp.open === 1 ? said`is` : said`are`} still open. Of the other ${closed},`;
    const within = cmp.compared
      ? said` The gap pointed the same way in ${cmp.held} of the ${cmp.compared} sources and teams we could check it in.`
      : said` We couldn't check this within sources or teams: these deals couldn't be split into groups big enough to compare on both sides.`;
    return {
      outcome: "issue",
      key: "fast",
      title: more ? said`Deals from leads converted within ${FAST_DAYS} days were won more often than deals from leads converted later` : said`Deals from leads converted within ${FAST_DAYS} days were won less often than deals from leads converted later`,
      summary: said`${who} the ${cmp.group.n} whose lead was converted within ${FAST_DAYS} days were won ${cmp.group.rate}% of the time, against ${cmp.other.rate}% of the ${cmp.other.n} whose lead was converted later.${within}${NOT_A_CAUSE}`,
      recommendation: more
        ? said`Look at what happens to the leads converted quickly, and whether the leads that wait could be worked sooner.`
        : said`Look at why leads converted quickly go on to win less often, and whether they're converted before they're ready.`,
      recommendationType: "investigate",
      // For information, as every insight: kept out of the plan, and not scored (scoring.ts INSIGHT_RULES).
      severity: "info",
      confidence: confidenceOf(ctx, cmp),
      evidence: c.evidence,
      value: notSized(said`It compares how quickly leads were converted, which has no direct figure.`),
      effort: { size: "S" },
      owner: "Head of marketing",
      segmentsChecked: checked,
      facts: { reached: closed, open: cmp.open, deals: cmp.group.n, otherDeals: cmp.other.n, winRate: cmp.group.rate, otherWinRate: cmp.other.rate, ...(cmp.compared ? { compared: cmp.compared, held: cmp.held } : {}) },
    };
  },
};
