import { notSized, pct, plural, segmentRate, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { org, passedOn, said, type Said } from "../../engine/org-words.ts";
import { named } from "../../engine/named.ts";
import { serviceActivityExtractor, serviceCasesExtractor, serviceSetupExtractor, SERVICE_MONTHS, type CaseStatusRow, type ClosedCaseRow, type OpenCaseRow, type StatusChangeRow } from "../../extract/service.ts";
import { monthsBefore, type SourceInfo, type Sourced } from "../../extract/types.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck } from "../../schemas/index.ts";
import { averageApart, meanStat, MIN_AVERAGE_VALUES } from "../average.ts";
import { groupBy } from "../historic-pipeline/metrics.ts";
import { bandedZ, MIN_EACH_OUTCOME, MIN_SEGMENTS, NOT_A_CAUSE, WHOLE_SHARE, WIN_GAP, zBar, zOf } from "../historic-pipeline/paths/insights.ts";
import { sendBacks } from "../historic-pipeline/paths/variants.ts";
import { dayPlus, daySaid, LAG_QUANTILE, OPEN_SHARE } from "../historic-pipeline/reached.ts";
import { daysBetween, nearestRank } from "../lead-funnel/journey.ts";
import { localDay, zoneNote, zoneOf } from "../time-zone.ts";
import { MIN_SEGMENT, median, originOf, priorityOf, round1 } from "./shared.ts";

// Case status paths (KAN-46 slice 1 lane B, DECISIONS.md 776), from what the
// service module already reads: the case statuses and which are closed, the
// open cases with their current Status, the cases closed in the year, and the
// year's Status changes from CaseHistory. No new read. Behind HC_CASE_PATHS
// (index.ts casePathsModule): off, none of this runs, and it only ever runs on
// a run that includes the service module.
//
// Who is counted is chosen by the day a case was made, never by when it closed
// (712): cases made from the first full day of the year read (or the day after
// the first Status change found, if later) to the run's day less the cases'
// maturity lag, the days within which 9 in 10 of the closed cases made in that
// time had closed, and never fewer than LEAST_LAG_DAYS. A case made later
// hasn't had the time, so it is left out by that rule, never counted as a case
// that failed. Not read while more than 1 in 20 of the period's cases are still
// open; the few that are go each way where their ending would change an answer.
//
// Every case read, open or closed, is either counted or in a named exclusion.
// Days are calendar days: business hours are read but not applied.
//
// Statuses are the org's words. A value in the history is matched to the org's
// case status settings by API name or label, ignoring case, as the reopened
// check matches it; a value matching none, or both an open and a closed one,
// has no meaning here, and its case is left out and said.

type Ctx = Omit<RuleContext, "metrics">;

const DOMAIN = "service_operations";
const OWNER = "Head of customer service";
const DAY = 86_400_000;
/** A case's least time to close, for the period's lag: never shorter, and said as ours when it applies. */
export const LEAST_LAG_DAYS = 14;
/** The share of the closed cases' times to close the lag is set to, as a sentence says it ("90%"). */
const LAG_SHARE = Math.round(LAG_QUANTILE * 100);
/** An open case is held to the time this share of the period's stays in its status ended within. */
export const STAY_QUANTILE = 0.75;
const STAY_SHARE = Math.round(STAY_QUANTILE * 100);
/** Stuck cases: the share of open cases past the line, overall and in any one segment, as cur.stuck-in-stage holds deals. */
const STUCK_LIMIT = 40;
const STUCK_SEGMENT_LIMIT = 60;
/** Cases closed straight from where they started, overall and in any one segment. */
const STRAIGHT_LIMIT = 25;
const STRAIGHT_SEGMENT_LIMIT = 50;
/** The paths listed by name in the stored metric, with every other path in one row. */
const TOP_PATHS = 5;
const OTHER_PATHS = "All other paths";
const PATH_JOIN = " > ";

// ─── Reading the cases ───────────────────────────────────────────────────────

/** A value Salesforce gave as empty is no value at all. */
const valueOf = (v: string | null | undefined): string | null => (typeof v === "string" && v.trim() !== "" ? v : null);
const casePriority = priorityOf;
const caseOrigin = originOf;

/** A status as the paths read it: the org's settings row it matches, by API name. */
type Status = { key: string; label: string; closed: boolean };

/** The org's statuses by every name a value can carry; a name two statuses of different kinds share is left out. */
function statusTable(rows: CaseStatusRow[]): (value: string | null) => Status | null {
  const byName = new Map<string, Status | null>();
  for (const row of rows) {
    const s: Status = { key: row.ApiName, label: valueOf(row.MasterLabel) ?? row.ApiName, closed: row.IsClosed === true };
    for (const name of new Set([row.ApiName, row.MasterLabel].flatMap((n) => (valueOf(n) ? [n!.toLowerCase()] : [])))) {
      const seen = byName.get(name);
      if (seen === undefined) byName.set(name, s);
      else if (seen !== null && seen.key !== s.key && seen.closed !== s.closed) byName.set(name, null);
    }
  }
  return (value) => {
    const v = valueOf(value);
    return v === null ? null : (byName.get(v.toLowerCase()) ?? null);
  };
}

/** A time a case spent in one status, from entering it to leaving it; `end` null while it is still there. */
export type Visit = { status: Status; start: number; end: number | null };

export type PathCase = {
  id: string;
  madeAt: number;
  madeDay: string;
  open: boolean;
  /** When it closed, for a case closed now. */
  closedAt: number | null;
  visits: Visit[];
  /** Every status in order, a run in one status once. */
  path: string[];
  /** The open statuses before its first close, in order, a run once. */
  beforeClose: string[];
  firstCloseAt: number | null;
  reopened: boolean;
  priority: string;
  origin: string;
};

type Ready = {
  status: "ok";
  start: string;
  to: string;
  lagDays: number;
  floored: boolean;
  /** The day the Status changes found start, when later than the first full day read. */
  lateHistory: string | null;
  /** The first instant a Status change was found, for an open case with none of its own. */
  historyFrom: number;
  cases: PathCase[];
  /** Open cases read, as C1 reads them (every open case, whenever made). */
  openNow: Array<{ row: OpenCaseRow; visits: Visit[] | null; status: Status | null; consistent: boolean }>;
  openRead: number;
  hasOrigin: boolean;
  openSource: SourceInfo;
  exclusions: string[];
  /** Cases read, open and closed, each once. */
  read: number;
  evidence: Evidence[];
};
type NotReady = { status: "not_measurable"; reason: string; evidence: Evidence[] };
export type CaseCohort = Ready | NotReady;

const instant = (s: string): number => Date.parse(s.replace(/\+0000$/, "Z"));

/** The changes of one case in order: by time, and within the same second by chaining one change's new value to the next's old. */
function ordered(changes: StatusChangeRow[], table: (v: string | null) => Status | null): StatusChangeRow[] {
  const byTime = [...changes].sort((a, b) => instant(a.CreatedDate) - instant(b.CreatedDate));
  const out: StatusChangeRow[] = [];
  for (let i = 0; i < byTime.length; ) {
    let j = i;
    while (j < byTime.length && instant(byTime[j]!.CreatedDate) === instant(byTime[i]!.CreatedDate)) j++;
    const tied = byTime.slice(i, j);
    while (tied.length) {
      const last = out.at(-1);
      const next = last ? tied.findIndex((c) => table(c.OldValue)?.key === table(last.NewValue)?.key) : -1;
      out.push(tied.splice(next >= 0 ? next : 0, 1)[0]!);
    }
    i = j;
  }
  return out;
}

/** A case's visits from its changes, starting in the first change's old value (or its current status with none), or null when a value has no meaning. */
function visitsOf(madeAt: number, changes: StatusChangeRow[], current: Status | null, table: (v: string | null) => Status | null): Visit[] | null {
  const first = changes.length ? table(changes[0]!.OldValue) : current;
  if (!first) return null;
  const visits: Visit[] = [{ status: first, start: madeAt, end: null }];
  for (const c of changes) {
    const to = table(c.NewValue);
    if (!to) return null;
    const at = Math.max(instant(c.CreatedDate), visits.at(-1)!.start);
    if (to.key === visits.at(-1)!.status.key) continue;
    visits.at(-1)!.end = at;
    visits.push({ status: to, start: at, end: null });
  }
  return visits;
}

const runsOf = (statuses: string[]): string[] => statuses.filter((s, i) => i === 0 || statuses[i - 1] !== s);

const unreadable = (ctx: Ctx, what: Said, source: Extract<Sourced<unknown>, { status: "unavailable" }>): NotReady => ({ status: "not_measurable", reason: said`${what} couldn't be read: ${passedOn(source.reason)}`, evidence: [ctx.ev(source.source, { count: null })] });

/**
 * The cases made in the period and what became of them, or not measurable with
 * the reason. Made afresh for each metric builder and rule, as the pipeline is,
 * so each carries what the reads say of themselves.
 */
export function caseCohortOf(ctx: Ctx): CaseCohort {
  const setup = ctx.data(serviceSetupExtractor);
  const data = ctx.data(serviceCasesExtractor);
  const { statusChanges } = ctx.data(serviceActivityExtractor);
  if (setup.statuses.status !== "ok") return unreadable(ctx, said`Case statuses`, setup.statuses);
  if (data.open.status !== "ok") return unreadable(ctx, said`Open cases`, data.open);
  if (data.closedCases.status !== "ok") return unreadable(ctx, said`Closed cases`, data.closedCases);
  if (statusChanges.status !== "ok") return unreadable(ctx, said`Case history`, statusChanges);
  const { minSample } = ctx.calibration;
  const zone = zoneOf(ctx);
  const yearStart = monthsBefore(ctx.now, SERVICE_MONTHS);
  const range = { from: localDay(yearStart, zone), to: localDay(ctx.now, zone) };
  const evidence: Evidence[] = [
    ctx.ev(statusChanges.source, { count: statusChanges.data.length, dateRange: range, note: "Status changes in the last year" }),
    ctx.ev(data.open.source, { count: data.open.data.length, note: "Every open case" }),
    ctx.ev(data.closedCases.source, { count: data.closedCases.data.length, dateRange: range, note: "Cases closed in the last year" }),
  ];
  // A read stopped at its row limit is an arbitrary part, so no path can be read from it.
  const cut = [statusChanges, data.open, data.closedCases].find((s) => s.truncated);
  if (cut) {
    const what = cut === statusChanges ? said`status changes` : cut === data.open ? said`open cases` : said`closed cases`;
    return { status: "not_measurable", reason: said`The read of ${what} stopped at its row limit, so we can't follow every case's statuses.`, evidence };
  }
  const table = statusTable(setup.statuses.data);
  const closedRows = data.closedCases.data;
  if (statusChanges.data.length === 0) {
    const closed = closedRows.length;
    if (closed < minSample) return { status: "not_measurable", reason: named(said`${plural(closed, "case")} closed in the last year, below the calibrated minimum of ${minSample}, and we found no recorded change to a case's Status.`, { closed, minSample }), evidence };
    return {
      status: "not_measurable",
      reason: named(
        said`We couldn't follow your cases' statuses: we found no recorded change to a case's Status in the last year, though ${closed} cases were closed in it. Either Salesforce isn't keeping a history of the Status field, or we can't see it. If field history isn't switched on for Case Status, switching it on lets later runs measure from that day.`,
        { closed },
      ),
      evidence,
    };
  }

  // The first full day read: the year starts at an instant, so its own day is only partly read.
  const firstFull = dayPlus(range.from, 1);
  const historyFrom = Math.min(...statusChanges.data.map((c) => instant(c.CreatedDate)));
  const historyDay = localDay(new Date(historyFrom), zone);
  // History found to start later than the year read: tracking began then, so only cases made after that day show every status.
  const lateHistory = historyDay > firstFull ? dayPlus(historyDay, 1) : null;
  const start = lateHistory ?? firstFull;

  const changes = groupBy(statusChanges.data, (c) => c.CaseId);
  const openRows = data.open.data;
  const openIds = new Set(openRows.map((c) => c.Id));
  const closedIds = new Set(closedRows.map((c) => c.Id));
  const both = new Set([...openIds].filter((id) => closedIds.has(id)));
  type Row = { id: string; made: number; priority: string; origin: string; open: OpenCaseRow | null; closed: ClosedCaseRow | null };
  const rows: Row[] = [
    ...openRows.filter((c) => !both.has(c.Id)).map((c): Row => ({ id: c.Id, made: instant(c.CreatedDate), priority: casePriority(c), origin: caseOrigin(c), open: c, closed: null })),
    ...closedRows.filter((c) => !both.has(c.Id)).map((c): Row => ({ id: c.Id, made: instant(c.CreatedDate), priority: casePriority(c), origin: caseOrigin(c), open: null, closed: c })),
  ];
  const read = rows.length + both.size;
  const dayOf = (r: Row): string => localDay(new Date(r.made), zone);

  // The lag: the closed cases made since the start, by the days each took from its day to its close day.
  const closedDays = rows.filter((r) => r.closed && dayOf(r) >= start).map((r) => Math.max(0, daysBetween(dayOf(r), localDay(new Date(instant(r.closed!.ClosedDate)), zone))));
  const closedCount = closedDays.length;
  if (closedCount < minSample) return { status: "not_measurable", reason: named(said`${closedCount} closed cases made since ${daySaid(start)} is below the calibrated minimum of ${minSample}.`, { closedCount, start: [start, "date"], minSample }), evidence };
  const percentile = nearestRank(closedDays, LAG_QUANTILE);
  const floored = percentile < LEAST_LAG_DAYS;
  const lagDays = Math.max(LEAST_LAG_DAYS, percentile);
  const to = dayPlus(range.to, -lagDays);
  if (to < start) return { status: "not_measurable", reason: named(said`Cases here can take ${lagDays} days to close, so none made since ${daySaid(start)} has had the time yet.`, { lagDays: [lagDays, "day"], start: [start, "date"] }), evidence };

  const counts = { beforeRead: 0, beforeHistory: 0, recent: 0, noChange: 0, unknown: 0, mismatch: 0 };
  const cases: PathCase[] = [];
  const visitsById = new Map<string, Visit[] | null>();
  for (const r of rows) {
    const own = ordered(changes.get(r.id) ?? [], table);
    const current = r.open ? table(r.open.Status) : null;
    const visits = r.open && !current ? null : visitsOf(r.made, own, current, table);
    visitsById.set(r.id, visits);
    const day = dayOf(r);
    if (day < firstFull) {
      counts.beforeRead++;
      continue;
    }
    if (day < start) {
      counts.beforeHistory++;
      continue;
    }
    if (day > to) {
      counts.recent++;
      continue;
    }
    if (r.closed && own.length === 0) {
      counts.noChange++;
      continue;
    }
    if (!visits) {
      counts.unknown++;
      continue;
    }
    const last = visits.at(-1)!.status;
    // How the case stands now must be where its changes end: a closed case in a closed status, an open one in its Status.
    if (r.closed ? !last.closed : current!.closed || last.key !== current!.key) {
      counts.mismatch++;
      continue;
    }
    const firstClose = visits.find((v) => v.status.closed) ?? null;
    const beforeClose = runsOf(visits.filter((v) => !v.status.closed && (firstClose === null || v.start < firstClose.start)).map((v) => v.status.key));
    cases.push({
      id: r.id,
      madeAt: r.made,
      madeDay: day,
      open: r.open !== null,
      closedAt: r.closed ? instant(r.closed.ClosedDate) : null,
      visits,
      path: runsOf(visits.map((v) => v.status.key)),
      beforeClose,
      firstCloseAt: firstClose ? firstClose.start : null,
      reopened: firstClose !== null && visits.some((v) => !v.status.closed && v.start > firstClose.start),
      priority: r.priority,
      origin: r.origin,
    });
  }
  const exclusions = [
    ...(counts.beforeRead ? [named(said`${plural(counts.beforeRead, "case")} made before ${daySaid(firstFull)}, the first full day of the year we read`, { beforeRead: counts.beforeRead, firstFull: [firstFull, "date"] })] : []),
    ...(counts.beforeHistory ? [named(said`${plural(counts.beforeHistory, "case")} made before ${daySaid(start)}, the day after the first status change we found, so their earlier statuses may not be recorded`, { beforeHistory: counts.beforeHistory, start: [start, "date"] })] : []),
    ...(counts.recent
      ? [floored
          ? named(said`${plural(counts.recent, "case")} made after ${daySaid(to)}, too recent to have had ${lagDays} days, the least time we allow for a case to close`, { recent: counts.recent, to: [to, "date"], lagDays: [lagDays, "day"] })
          : named(said`${plural(counts.recent, "case")} made after ${daySaid(to)}, too recent to have had the ${lagDays} days that ${LAG_SHARE}% of closed cases took`, { recent: counts.recent, to: [to, "date"], lagDays: [lagDays, "day"], lagShare: [LAG_SHARE, "percent"] })]
      : []),
    ...(both.size ? [named(said`${plural(both.size, "case")} read as both open and closed, as it changed while we read`, { both: both.size })] : []),
    ...(counts.noChange ? [named(said`${plural(counts.noChange, "closed case")} with no recorded status change, so we can't tell which statuses it went through`, { noChange: counts.noChange })] : []),
    ...(counts.unknown ? [named(said`${plural(counts.unknown, "case")} with a status we couldn't find in your case status settings, or with none`, { unknown: counts.unknown })] : []),
    ...(counts.mismatch ? [named(said`${plural(counts.mismatch, "case")} whose recorded status changes don't end where the case stands now`, { mismatch: counts.mismatch })] : []),
  ];
  const openNow = openRows.map((row) => {
    const status = table(row.Status);
    const visits = both.has(row.Id) ? null : (visitsById.get(row.Id) ?? null);
    const last = visits?.at(-1)?.status ?? null;
    return { row, visits, status, consistent: status !== null && !status.closed && last !== null && last.key === status.key };
  });
  const hasOrigin = !(data.open.missingFields ?? []).includes("Case.Origin") && !(data.closedCases.missingFields ?? []).includes("Case.Origin");
  return {
    status: "ok",
    start,
    to,
    lagDays,
    floored,
    lateHistory,
    historyFrom,
    cases,
    openNow,
    openRead: openRows.length,
    hasOrigin,
    openSource: data.open.source,
    exclusions,
    read,
    evidence: [evidence[0]!, { ...evidence[2]!, count: cases.length, dateRange: { from: start, to }, note: "Cases made in the period, open or closed", exclusions }, evidence[1]!],
  };
}

/**
 * The cohort, or not measurable: a reason of its own, too few cases made in the
 * period, or too many still open. The stuck check alone reads the period while
 * many are open (`openGate` false): its line counts every stay not yet ended as
 * the longest there is, so open cases can only raise the line, never lower it.
 */
function readyOrWhy(ctx: Ctx, openGate = true): Ready | NotReady {
  const c = caseCohortOf(ctx);
  if (c.status !== "ok") return c;
  const { minSample } = ctx.calibration;
  const made = c.cases.length;
  if (made < minSample) return { status: "not_measurable", reason: named(said`${plural(made, "case")} made ${periodSaid(c)} could be followed, below the calibrated minimum of ${minSample}.`, { made, minSample }), evidence: c.evidence };
  const open = c.cases.filter((x) => x.open).length;
  if (openGate && open > OPEN_SHARE * made) return { status: "not_measurable", reason: named(said`Of the ${made} cases made ${periodSaid(c)}, ${open} ${open === 1 ? said`is` : said`are`} still open, too many to say how these cases end.`, { made, open }), evidence: c.evidence };
  return c;
}

const periodSaid = (c: { start: string; to: string }): Said => named(said`between ${daySaid(c.start)} and ${daySaid(c.to)}`, { start: [c.start, "date"], to: [c.to, "date"] });
const daysOf = (ms: number): number => ms / DAY;

/** A median of calendar days, with the average beside it where it says something (709), or nothing to add. */
function daysSaid(values: number[]): Said {
  if (values.length === 1) {
    const only = round1(values[0]!);
    return named(said`${only} ${only === 1 ? said`calendar day` : said`calendar days`}`, { only: [only, "day"] });
  }
  const mid = round1(median(values));
  const mean = meanStat(values).mean;
  const unit = mid === 1 ? said`calendar day` : said`calendar days`;
  if (mean === undefined || values.length < MIN_AVERAGE_VALUES || mid <= 0 || !averageApart(mid, mean)) return named(said`a median of ${mid} ${unit}`, { mid: [mid, "day"] });
  return named(said`a median of ${mid} ${unit} (an average of ${mean})`, { mid: [mid, "day"], mean: [mean, "day"] });
}

/** Rule 4: the segments checked, in dimensions with two or more values. */
function dimensionsOf(c: Ready): Record<string, (x: PathCase) => string> {
  return { priority: (x) => x.priority, ...(c.hasOrigin ? { origin: (x: PathCase) => x.origin } : {}) };
}

const value = notSized(said`How cases are handled has no honest figure in money: it shows in customers' experience, not on an invoice.`);

/** Our name for a segment's dimension, or the org's words for any other (657). */
const DIMENSION_WORDS = ["priority", "origin", "status", "starting status"] as const;
const dimensionWords = (dimension: string) => DIMENSION_WORDS.find((k) => k === dimension) ?? org(dimension);

// ─── C1: open cases sitting in a status longer than most cases did ───────────

export const stuckInStatus: Rule = {
  id: "svc.stuck-in-status",
  version: 1,
  domain: DOMAIN,
  title: "Open cases sitting in a status longer than most cases did",
  records: true,
  evaluate(ctx): RuleResult {
    const ready = readyOrWhy(ctx, false);
    if (ready.status !== "ok") return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    const { minSample } = ctx.calibration;
    // Every stay in each open status by the period's cases. A stay not yet ended is counted as the longest there is, so the
    // line is one at least STAY_SHARE% of the stays really ended within, however the open ones end.
    const stays = new Map<string, number[]>();
    for (const x of c.cases) for (const v of x.visits) if (!v.status.closed) stays.set(v.status.key, [...(stays.get(v.status.key) ?? []), v.end === null ? Infinity : daysOf(v.end - v.start)]);
    const lines = new Map<string, number>();
    for (const [key, days] of stays) {
      if (days.length < minSample) continue;
      const line = nearestRank(days, STAY_QUANTILE);
      if (Number.isFinite(line)) lines.set(key, round1(line));
    }
    const now = ctx.now.getTime();
    type Target = { row: OpenCaseRow; status: Status; since: number; days: number; priority: string; origin: string; label: string };
    const targets: Target[] = [];
    let noStatus = 0;
    let noLine = 0;
    for (const o of c.openNow) {
      if (!o.status || o.status.closed || !o.consistent || !o.visits) {
        noStatus++;
        continue;
      }
      if (!lines.has(o.status.key)) {
        noLine++;
        continue;
      }
      // With no change of its own, the case has been in its status since it was made, or at least since the first change we found.
      const since = o.visits.length > 1 ? o.visits.at(-1)!.start : Math.max(instant(o.row.CreatedDate), c.historyFrom);
      targets.push({ row: o.row, status: o.status, since, days: daysOf(now - since), priority: casePriority(o.row), origin: caseOrigin(o.row), label: o.status.label });
    }
    const exclusions = [
      ...(noStatus ? [named(said`${plural(noStatus, "open case")} whose status we couldn't place: not in your case status settings, a closed status, or not where its recorded changes end`, { noStatus })] : []),
      ...(noLine ? [named(said`${plural(noLine, "open case")} in a status too few cases made ${periodSaid(c)} spent time in, or left, to set a line`, { noLine, minSample })] : []),
    ];
    const evidence = [...c.evidence, ctx.ev(c.openSource, { count: targets.length, note: "Open cases now, in a status with a line to compare", exclusions })];
    if (targets.length === 0) return { outcome: "not_measurable", reason: named(said`No open case is in a status that ${minSample} or more stays by cases made ${periodSaid(c)} were in and left.`, { minSample }), evidence };
    const dims: Record<string, (t: Target) => string> = { status: (t) => t.label, priority: (t) => t.priority, ...(c.hasOrigin ? { origin: (t: Target) => t.origin } : {}) };
    const result = segmentRate(targets, (t) => t.days > lines.get(t.status.key)!, dims, { segmentLimit: STUCK_SEGMENT_LIMIT, minSegment: MIN_SEGMENT });
    const { hits, n, rate } = result;
    const lead = named(said`We set each open case beside the cases made ${periodSaid(c)}. ${hits} of ${n} open cases (${rate}%) have been in their current status longer than ${STAY_SHARE}% of those cases' stays in it lasted. Days are calendar days.`, { stayShare: [STAY_SHARE, "percent"], hits, n, rate: [rate, "percent"] });
    const facts = { hits, n, rate, statusesWithLine: lines.size, stayShare: STAY_SHARE, segmentLimit: STUCK_SEGMENT_LIMIT };
    const over = rate > STUCK_LIMIT;
    if (!over && result.hotspots.length === 0) {
      return { outcome: "pass", summary: named(said`${lead} No segment is above ${STUCK_SEGMENT_LIMIT}%.`, { segmentLimit: [STUCK_SEGMENT_LIMIT, "percent"] }), evidence, segmentsChecked: result.segmentsChecked, facts };
    }
    const worst = result.hotspots[0];
    const worstLine = worst && worst.dimension === "status" ? lines.get(targets.find((t) => t.label === worst.key)!.status.key)! : null;
    const lineSaid = worst && worstLine !== null ? named(said` ${STAY_SHARE}% of the stays in ${org(worst.key)} ended within ${worstLine} days.`, { stayShare: [STAY_SHARE, "percent"], worstLine: [worstLine, "day"] }) : said``;
    const hidden = !worst
      ? said``
      : !over
        ? named(said` The total is within our line of ${STUCK_LIMIT}%, but by ${dimensionWords(worst.dimension)}, ${org(worst.key)} is at ${worst.rate}% (${worst.hits} of ${worst.n}).`, { stuckLimit: [STUCK_LIMIT, "percent"], worstPct: [worst.rate, "percent"], worstHits: worst.hits, worstN: worst.n })
        : named(said` Worst by ${dimensionWords(worst.dimension)}: ${org(worst.key)}, at ${worst.rate}%.`, { worstPct: [worst.rate, "percent"] });
    return {
      outcome: "issue",
      title: said`Open cases are sitting in a status longer than most cases did`,
      summary: said`${lead}${hidden}${lineSaid}`,
      recommendation: said`Go through the open cases that have waited longest in each status and move each one on or close it, then look at why cases wait there.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence,
      value,
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts: { ...facts, ...(worst ? { worstSegmentRate: worst.rate, worstSegmentHits: worst.hits, worstSegmentN: worst.n } : {}) },
    };
  },
};

// ─── C2: reopened cases, the time before their first close and after it ─────

export const reopenedTime: Rule = {
  id: "svc.reopened-time",
  version: 1,
  domain: DOMAIN,
  title: "Time reopened cases spend after their first close",
  records: true,
  evaluate(ctx): RuleResult {
    const ready = readyOrWhy(ctx);
    if (ready.status !== "ok") return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    const { minSample } = ctx.calibration;
    const now = ctx.now.getTime();
    const reopened = c.cases.filter((x) => x.reopened);
    const n = reopened.length;
    if (n === 0) return { outcome: "not_measurable", reason: named(said`None of the ${c.cases.length} cases made ${periodSaid(c)} was closed and then reopened, so there is no time after a first close to measure.`, { made: c.cases.length }), evidence: c.evidence };
    if (n < minSample) {
      const few = n === 1 ? said`1 case made ${periodSaid(c)} was closed and then reopened` : named(said`${n} cases made ${periodSaid(c)} were closed and then reopened`, { n });
      return { outcome: "not_measurable", reason: named(said`${few}, below the calibrated minimum of ${minSample}.`, { minSample }), evidence: c.evidence };
    }
    // Same cases on both sides: the time to the first close against the time from it to the final close.
    const before = (x: PathCase): number => daysOf(x.firstCloseAt! - x.madeAt);
    const after = (x: PathCase): number => daysOf((x.closedAt ?? now) - x.firstCloseAt!);
    const closedAgain = reopened.filter((x) => !x.open);
    const stillOpen = reopened.filter((x) => x.open);
    // A case still open has had at least this long since its first close: longer already, or not yet known.
    const longer = reopened.filter((x) => after(x) > before(x)).length;
    const unknown = stillOpen.filter((x) => after(x) <= before(x)).length;
    const open = stillOpen.length;
    const dims = dimensionsOf(c);
    const segments = segmentRate(reopened, (x) => after(x) > before(x), dims, { segmentLimit: 100, minSegment: MIN_SEGMENT }).segmentsChecked;
    const who = named(said`Of the ${n} cases made ${periodSaid(c)} that were closed and then reopened`, { n });
    const openSaid = open === 0 ? said`` : open === 1 ? said` 1 of them is still open.` : named(said` ${open} of them are still open.`, { open });
    const timesSaid = closedAgain.length
      ? closedAgain.length === 1
        ? said` Before their first close they took ${daysSaid(reopened.map(before))}. The 1 closed again took ${daysSaid(closedAgain.map(after))} from its first close to its final close.`
        : named(said` Before their first close they took ${daysSaid(reopened.map(before))}. The ${closedAgain.length} closed again took ${daysSaid(closedAgain.map(after))} from their first close to their final close.`, { closedAgain: closedAgain.length })
      : said` Before their first close they took ${daysSaid(reopened.map(before))}. None has been closed again yet.`;
    const facts = { reopened: n, longer, open, unknown, closedAgain: closedAgain.length };
    if (longer * 2 > n) {
      return {
        outcome: "issue",
        title: said`Reopened cases spend longer after their first close than before it`,
        summary: named(said`${who}, ${longer} spent longer from their first close to their final close, or to today, than they had taken to be closed the first time.${openSaid}${timesSaid} Days are calendar days.`, { longer }),
        recommendation: said`Look at what these cases had in common when they were first closed, and confirm with the customer before closing cases like them.`,
        recommendationType: "investigate",
        severity: "low",
        confidence: open === 0 ? "high" : "medium",
        evidence: c.evidence,
        value,
        effort: { size: "S" },
        owner: OWNER,
        segmentsChecked: segments,
        facts,
      };
    }
    if ((longer + unknown) * 2 > n) {
      return { outcome: "not_measurable", reason: named(said`${who}, so far ${longer} of them spent longer after their first close than before it, and ${unknown} still open could take that past half, so we can't say yet whether most of them did.`, { longer, unknown }), evidence: c.evidence };
    }
    return {
      outcome: "pass",
      summary: named(said`${who}, ${longer} spent longer from their first close to their final close, or to today, than they had taken to be closed the first time, which isn't more than half of them.${openSaid}${timesSaid} Days are calendar days.`, { longer }),
      evidence: c.evidence,
      segmentsChecked: segments,
      facts,
    };
  },
};

// ─── C3: cases closed straight from the status they started in ───────────────

export const closedStraight: Rule = {
  id: "svc.closed-straight",
  version: 1,
  domain: DOMAIN,
  title: "Cases closed straight from the status they started in",
  records: true,
  evaluate(ctx): RuleResult {
    const ready = readyOrWhy(ctx);
    if (ready.status !== "ok") return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    // A case is closed straight when the status it was first closed from is the one it started in. A case not yet closed
    // isn't, which is true today; the few still open are said.
    const straight = (x: PathCase): boolean => x.firstCloseAt !== null && x.beforeClose.length === 1;
    const starting = (x: PathCase): string => x.visits[0]!.status.label;
    const dims: Record<string, (x: PathCase) => string> = { "starting status": starting, ...dimensionsOf(c) };
    const result = segmentRate(c.cases, straight, dims, { segmentLimit: STRAIGHT_SEGMENT_LIMIT, minSegment: MIN_SEGMENT });
    const { hits, n, rate } = result;
    const open = c.cases.filter((x) => x.firstCloseAt === null).length;
    const lead = named(said`Of the ${n} cases made ${periodSaid(c)}, ${hits} (${rate}%) were closed straight from the status they started in, with no other status between.`, { n, hits, rate: [rate, "percent"] });
    const openSaid = open === 0 ? said`` : open === 1 ? said` 1 case has never been closed, and isn't counted as closed straight.` : named(said` ${open} cases have never been closed, and aren't counted as closed straight.`, { open });
    const facts = { hits, n, rate, open, segmentLimit: STRAIGHT_SEGMENT_LIMIT };
    const over = rate > STRAIGHT_LIMIT;
    if (!over && result.hotspots.length === 0) {
      return { outcome: "pass", summary: named(said`${lead}${openSaid} No segment is above ${STRAIGHT_SEGMENT_LIMIT}%.`, { segmentLimit: [STRAIGHT_SEGMENT_LIMIT, "percent"] }), evidence: c.evidence, segmentsChecked: result.segmentsChecked, facts };
    }
    const worst = result.hotspots[0];
    const hidden = !worst
      ? said``
      : !over
        ? named(said` The total is within our line of ${STRAIGHT_LIMIT}%, but by ${dimensionWords(worst.dimension)}, ${org(worst.key)} is at ${worst.rate}% (${worst.hits} of ${worst.n}).`, { straightLimit: [STRAIGHT_LIMIT, "percent"], worstPct: [worst.rate, "percent"], worstHits: worst.hits, worstN: worst.n })
        : named(said` Worst by ${dimensionWords(worst.dimension)}: ${org(worst.key)}, at ${worst.rate}%.`, { worstPct: [worst.rate, "percent"] });
    return {
      outcome: "issue",
      title: said`Many cases are closed straight from the status they started in`,
      summary: said`${lead}${hidden}${openSaid} Some may be duplicates, spam or cases closed by automation, and some may be questions answered at once: the statuses alone can't tell them apart.`,
      recommendation: said`Look at a sample of these cases to see which they are. Filter duplicates and spam out where they come in, check that any automation closing cases should be, and leave quick answers as they are.`,
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence: c.evidence,
      value,
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts: { ...facts, ...(worst ? { worstSegmentRate: worst.rate, worstSegmentHits: worst.hits, worstSegmentN: worst.n } : {}) },
    };
  },
};

// ─── C4, stored and never served: sent back, and closed in time ──────────────

type Side = { n: number; won: number; rate: number };
type Bound = { gap: number; z: number; cmh: number };
type Compared = { from: string; to: string; group: Side; other: Side; gap: number; open: number; low: Bound; high: Bound; compared: number; held: number };

/**
 * Closed within the lag of being made: true, false, or null for a case still
 * open that could yet close within it (made on the period's last day).
 */
function resolvedOf(c: Ready, today: string, zone: string | null): (x: PathCase) => boolean | null {
  return (x) => {
    if (!x.open) return daysBetween(x.madeDay, localDay(new Date(x.closedAt!), zone)) <= c.lagDays;
    return daysBetween(x.madeDay, today) <= c.lagDays ? null : false;
  };
}

function compareCases(ctx: Ctx, c: Ready, group: PathCase[], other: PathCase[], resolved: (x: PathCase) => boolean | null): Compared | null {
  const { minSample } = ctx.calibration;
  const known = (xs: PathCase[]) => xs.filter((x) => resolved(x) !== null);
  const sideOf = (xs: PathCase[]): Side => ({ n: xs.length, won: xs.filter((x) => resolved(x) === true).length, rate: pct(xs.filter((x) => resolved(x) === true).length, xs.length) });
  const [g, o] = [sideOf(known(group)), sideOf(known(other))];
  if (![g, o].every((s) => s.n >= minSample && s.won >= MIN_EACH_OUTCOME && s.n - s.won >= MIN_EACH_OUTCOME)) return null;
  const inGroup = new Set(group);
  const bound = (groupOpenWon: boolean): Bound => {
    const wonOf = (x: PathCase): boolean => resolved(x) ?? inGroup.has(x) === groupOpenWon;
    const a = { n: group.length, won: group.filter(wonOf).length };
    const b = { n: other.length, won: other.filter(wonOf).length };
    return { gap: round1(pct(a.won, a.n) - pct(b.won, b.n)), z: Math.round(zOf(a, b) * 100) / 100, cmh: Math.round(bandedZ(group, other, wonOf, (x) => x.madeAt, c.start) * 100) / 100 };
  };
  const gap = round1(g.rate - o.rate);
  let compared = 0;
  let held = 0;
  for (const keyOf of Object.values(dimensionsOf(c))) {
    const a = groupBy(group, keyOf);
    const b = groupBy(other, keyOf);
    if (new Set([...a.keys(), ...b.keys()]).size < 2) continue;
    for (const [key, inA] of a) {
      const inB = b.get(key) ?? [];
      if (inA.length < minSample || inB.length < minSample || inA.length >= WHOLE_SHARE * group.length || inB.length >= WHOLE_SHARE * other.length) continue;
      compared++;
      if (Math.sign(sideOf(known(inA)).rate - sideOf(known(inB)).rate) === Math.sign(gap)) held++;
    }
  }
  return { from: c.start, to: c.to, group: g, other: o, gap, open: group.length + other.length - g.n - o.n, low: bound(false), high: bound(true), compared, held };
}

function clears(x: Compared | null, bar: number): boolean {
  if (!x) return false;
  const side = Math.sign(x.low.gap);
  if (side === 0 || Math.sign(x.high.gap) !== side) return false;
  if ([x.low, x.high].some((b) => Math.abs(b.gap) < WIN_GAP || Math.abs(b.z) < bar || Math.sign(b.cmh) !== side || Math.abs(b.cmh) < bar)) return false;
  if (x.compared === 0) return true;
  return x.compared >= MIN_SEGMENTS && x.held * 2 > x.compared;
}

/** Each send-back seen: from a status to one the case first entered before it, among the open statuses before its first close. */
export function sendBackPairs(cases: PathCase[]): Array<{ from: string; to: string; group: PathCase[]; other: PathCase[] }> {
  const pairs = new Map<string, { from: string; to: string }>();
  for (const x of cases) for (const m of sendBacks(x.beforeClose)) pairs.set(`${m.from}\u0000${m.to}`, m);
  return [...pairs.values()]
    .sort((a, b) => (a.from === b.from ? (a.to < b.to ? -1 : 1) : a.from < b.from ? -1 : 1))
    .map(({ from, to }) => {
      // Both sides were in `to` before they reached `from`: only they could have been sent back from one to the other.
      const there = cases.filter((x) => x.beforeClose.includes(from) && x.beforeClose.includes(to) && x.beforeClose.indexOf(to) < x.beforeClose.indexOf(from));
      const sent = (x: PathCase): boolean => sendBacks(x.beforeClose).some((m) => m.from === from && m.to === to);
      return { from, to, group: there.filter(sent), other: there.filter((x) => !sent(x)) };
    });
}

/**
 * Of the cases made in the period that went from a status T on to a later
 * status F before their first close, those sent back from F to T against the
 * rest, on how often each was closed within the period's lag of being made. Both
 * sides reached the same point. Being sent back takes time of its own, so the
 * gap leans one way by construction: stored, never served (INSIGHT_RULES), and
 * not to be served before its own noise probe and a read on a real org.
 */
export const sentBackCases: Rule = {
  id: "svc.sent-back-cases",
  version: 1,
  domain: DOMAIN,
  title: "Cases sent back to an earlier status, against cases that reached the same point",
  records: true,
  evaluate(ctx): RuleResult | RuleResult[] {
    const ready = readyOrWhy(ctx);
    if (ready.status !== "ok") return { outcome: "not_measurable", reason: ready.reason, evidence: ready.evidence };
    const c = ready;
    const zone = zoneOf(ctx);
    const resolved = resolvedOf(c, localDay(ctx.now, zone), zone);
    const tried = sendBackPairs(c.cases).flatMap((p) => {
      const cmp = compareCases(ctx, c, p.group, p.other, resolved);
      return cmp ? [{ ...p, cmp }] : [];
    });
    const { minSample } = ctx.calibration;
    const lagDays = c.lagDays;
    if (tried.length === 0) return { outcome: "not_measurable", reason: named(said`No status had ${minSample} or more cases sent back to an earlier one, and as many that reached the same point without it, each side with ${MIN_EACH_OUTCOME} closed within ${lagDays} days and ${MIN_EACH_OUTCOME} not.`, { minSample, eachOutcome: MIN_EACH_OUTCOME, lagDays: [lagDays, "day"] }), evidence: c.evidence };
    // Bonferroni over every comparison made.
    const bar = zBar(tried.length);
    const raised = tried.filter((t) => clears(t.cmp, bar));
    const segmentsChecked: SegmentCheck[] = Object.entries(dimensionsOf(c)).map(([dimension, keyOf]) => ({ dimension, segments: new Set(c.cases.map(keyOf)).size }));
    if (raised.length === 0) {
      const tests = tried.length;
      return { outcome: "pass", summary: named(said`We compared ${plural(tests, "status", "statuses")} that cases were sent back from with the cases that reached them without being sent back. We couldn't tell whether those sent back were closed within ${lagDays} days of being made more or less often.`, { tests, lagDays: [lagDays, "day"] }), evidence: c.evidence, segmentsChecked, facts: { tested: tests, bar } };
    }
    const labels = new Map(c.cases.flatMap((x) => x.visits.map((v) => [v.status.key, v.status.label] as const)));
    return raised.map(({ from, to, cmp }): RuleResult => {
      const more = cmp.gap > 0;
      const [fromLabel, toLabel] = [labels.get(from)!, labels.get(to)!];
      const reached = cmp.group.n + cmp.other.n;
      const openSaid = cmp.open === 0 ? said`` : named(said` ${cmp.open} more are still open and could yet close in time, and the gap holds whichever way they go.`, { open: cmp.open });
      const within = cmp.compared ? named(said` The gap pointed the same way in ${cmp.held} of the ${cmp.compared} priorities and origins we could check it in.`, { held: cmp.held, compared: cmp.compared }) : said` We couldn't check this within priorities or origins: these cases couldn't be split into groups big enough to compare on both sides.`;
      return {
        outcome: "issue",
        key: `sent-back:${from}${PATH_JOIN}${to}`,
        title: more ? said`Cases sent back from ${org(fromLabel)} to ${org(toLabel)} were closed in time more often` : said`Cases sent back from ${org(fromLabel)} to ${org(toLabel)} were closed in time less often`,
        summary: named(
          said`Of the ${reached} cases made ${periodSaid(c)} that went from ${org(toLabel)} on to ${org(fromLabel)}, the ${cmp.group.n} sent back from ${org(fromLabel)} to ${org(toLabel)} were closed within ${lagDays} days of being made ${cmp.group.rate}% of the time, against ${cmp.other.rate}% of the ${cmp.other.n} that weren't.${openSaid}${within}${NOT_A_CAUSE}`,
          { reached, n: cmp.group.n, lagDays: [lagDays, "day"], rate: [cmp.group.rate, "percent"], rate2: [cmp.other.rate, "percent"], n2: cmp.other.n },
        ),
        recommendation: said`Look at why cases are sent back between these statuses, and what happens to them after.`,
        recommendationType: "investigate",
        severity: "info",
        confidence: cmp.compared === 0 ? "low" : Math.min(cmp.group.n, cmp.other.n) >= minSample * 2 ? "high" : "medium",
        evidence: c.evidence,
        value: notSized(said`It compares the paths cases take, which has no direct figure.`),
        effort: { size: "S" },
        owner: OWNER,
        segmentsChecked,
        subjects: [{ type: "case status", label: fromLabel }, { type: "case status", label: toLabel }],
        facts: { reached, open: cmp.open, cases: cmp.group.n, otherCases: cmp.other.n, rate: cmp.group.rate, otherRate: cmp.other.rate, bar, tested: tried.length, ...(cmp.compared ? { compared: cmp.compared, held: cmp.held } : {}) },
      };
    });
  },
};

// ─── The stored metric: the status paths closed cases took ───────────────────

export const STATUS_PATHS = "service.status_paths";

/**
 * The paths the period's closed cases took through every status, open and
 * closed, a run in one status once: counts and days only, the five most common
 * and every other path in one row. Stored, never served (INSIGHT_METRICS), as
 * historic.path_variants is: a table of paths invites setting one against the
 * usual one, which compares with survivors (703).
 */
export function statusPathMetrics(ctx: Ctx): Metric[] {
  const def = {
    id: STATUS_PATHS,
    domain: DOMAIN,
    label: said`The status paths your cases take`,
    unit: "percent" as const,
    definition: said`Closed cases grouped by the run of statuses each was in, open and closed. The value is the share of closed cases on the most common path.`,
    exclusions: [] as string[],
    assumptions: [zoneNote(ctx)],
  };
  const ready = readyOrWhy(ctx);
  if (ready.status !== "ok") return [{ ...def, status: "not_measurable", reason: ready.reason, evidence: ready.evidence }];
  const c = ready;
  const closed = c.cases.filter((x) => !x.open);
  const days = (x: PathCase): number => daysOf(x.closedAt! - x.madeAt);
  const groups = [...groupBy(closed, (x) => x.path.join(PATH_JOIN))].sort(([a, x], [b, y]) => y.length - x.length || (a < b ? -1 : a > b ? 1 : 0));
  const shown = groups.slice(0, TOP_PATHS);
  const rest = groups.slice(TOP_PATHS).flatMap(([, xs]) => xs);
  const segments: MetricSegment[] = [];
  const stats: Record<string, number> = { closed: closed.length, paths: groups.length, listed: shown.length, lagDays: c.lagDays };
  const row = (key: string, xs: PathCase[], i: number | null): void => {
    const n = xs.length;
    segments.push({ dimension: "path", key, n, value: pct(n, closed.length), notMeasurableReason: null }, { dimension: "path median days", key, n, value: round1(median(xs.map(days))), notMeasurableReason: null });
    const prefix = i === null ? "other" : `path${i + 1}`;
    const mean = meanStat(xs.map(days)).mean;
    Object.assign(stats, { [`${prefix}Cases`]: n, [`${prefix}Reopened`]: xs.filter((x) => x.reopened).length, [`${prefix}SentBack`]: xs.filter((x) => sendBacks(x.beforeClose).length > 0).length, ...(mean === undefined ? {} : { [`${prefix}MeanDays`]: mean }) });
  };
  shown.forEach(([key, xs], i) => row(key, xs, i));
  if (rest.length) {
    row(OTHER_PATHS, rest, null);
    stats.otherPaths = groups.length - shown.length;
  }
  const assumptions = [
    zoneNote(ctx),
    c.floored
      ? named(said`Cases made ${periodSaid(c)} are counted. The period ends ${c.lagDays} days before the run, our minimum, so recent cases have time to close`, { lagDays: [c.lagDays, "day"] })
      : named(said`Cases made ${periodSaid(c)} are counted. The period ends ${c.lagDays} days before the run: the time ${LAG_SHARE}% of closed cases took to close`, { lagDays: [c.lagDays, "day"], lagShare: [LAG_SHARE, "percent"] }),
    said`Days are calendar days from a case being made to its close, the median, not business hours`,
    said`A run of changes into the same status counts once, and a return to an earlier status is kept`,
  ];
  return [{ ...def, assumptions, status: "measured", value: shown.length ? pct(shown[0]![1].length, closed.length) : 0, n: closed.length, dateRange: { from: c.start, to: c.to }, segments, segmentExemptReason: "The paths are counts and days only, kept for the record", series: [], stats, evidence: c.evidence, exclusions: c.exclusions }];
}
