import { notSized, pct, plural, segmentRate, type Hotspot, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { org, passedOn, said, type Said } from "../../engine/org-words.ts";
import { named } from "../../engine/named.ts";
import { holdsNameOrSecret } from "../../narrative/guard.ts";
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

/**
 * A status's line is set from the stays that began in it in LOOKBACK_DAYS, ending GAP_DAYS before the day judged:
 * so a backlog building in the last weeks doesn't raise its own line, and nothing the day didn't know is used.
 */
export const LOOKBACK_DAYS = 90;
export const GAP_DAYS = 30;
/** How far back the oldest stay a line reads began, as a sentence says it: the lookback and the gap together. */
const LINE_FROM_DAYS = LOOKBACK_DAYS + GAP_DAYS;
/** Today is set beside the same weekday and time in each earlier week. */
const SNAPSHOT_EVERY_DAYS = 7;
/** Earlier weeks, each with the calibrated minimum of open cases with a line, needed to say what is usual. */
export const MIN_SNAPSHOTS = 12;
/**
 * Today must be above the share this share of the earlier weeks were at or below, and above their median by at
 * least `points`, and by `z` standard errors of a share of today's open cases at that median: the margin grows as
 * the open cases get fewer (set by the noise probe, DECISIONS.md 776; a test may change it).
 */
export const STUCK_BAR: { quantile: number; points: number; z: number; relative: number; mode: "both" | "weeks" | "steady" } = { quantile: 0.9, points: 10, z: 3, relative: 0.3, mode: "both" };
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

/**
 * The org's statuses by the names a value can carry: its API name exactly
 * first, then any API name or label ignoring case. A name two statuses of
 * different kinds share at the same step is left out, so it has no meaning.
 */
function statusTable(rows: CaseStatusRow[]): (value: string | null) => Status | null {
  const exact = new Map<string, Status | null>();
  const loose = new Map<string, Status | null>();
  const add = (map: Map<string, Status | null>, name: string, s: Status): void => {
    const seen = map.get(name);
    if (seen === undefined) map.set(name, s);
    else if (seen !== null && seen.key !== s.key && seen.closed !== s.closed) map.set(name, null);
  };
  for (const row of rows) {
    if (!valueOf(row.ApiName)) continue;
    const s: Status = { key: row.ApiName, label: valueOf(row.MasterLabel) ?? row.ApiName, closed: row.IsClosed === true };
    add(exact, row.ApiName, s);
    for (const name of new Set([row.ApiName, row.MasterLabel].flatMap((n) => (valueOf(n) ? [n!.toLowerCase()] : [])))) add(loose, name, s);
  }
  return (value) => {
    const v = valueOf(value);
    if (v === null) return null;
    const hit = exact.get(v);
    return hit !== undefined ? hit : (loose.get(v.toLowerCase()) ?? null);
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
  /** Every case read whose statuses can be followed, whenever made: what the stuck check's days are read from. */
  universe: SnapCase[];
  openRead: number;
  /** The year read starts at this instant. */
  yearStart: number;
  /** Cases made in the period, closed, with no recorded status change: not counted, and said where it matters. */
  noChange: number;
  hasOrigin: boolean;
  openSource: SourceInfo;
  exclusions: string[];
  /** Cases read, open and closed, each once. */
  read: number;
  evidence: Evidence[];
};
type NotReady = { status: "not_measurable"; reason: string; evidence: Evidence[] };
export type CaseCohort = Ready | NotReady;

/** A case as the stuck check reads it on any day: when it was made, its visits, and how it is segmented. */
export type SnapCase = { id: string; madeAt: number; visits: Visit[]; priority: string; origin: string };

const instant = (s: string): number => Date.parse(s.replace(/\+0000$/, "Z"));

/**
 * The changes of one case in order: by time, and within the same second by
 * chaining, never by the order the read returned them (it has no ORDER BY).
 * The chain starts from the tied change whose old value is no other tied
 * change's new value, and each next is the one tied change whose old value is
 * the last one's new. Null when the ties can't be put in one order (no such
 * start, two of them, a branch or a cycle): the case is then left out and said.
 */
export function ordered(changes: StatusChangeRow[], table: (v: string | null) => Status | null): StatusChangeRow[] | null {
  const byTime = [...changes].sort((a, b) => instant(a.CreatedDate) - instant(b.CreatedDate));
  const keyOf = (v: string | null): string | null => (valueOf(v) === null ? null : (table(v)?.key ?? `?${v}`));
  const out: StatusChangeRow[] = [];
  for (let i = 0; i < byTime.length; ) {
    let j = i;
    while (j < byTime.length && instant(byTime[j]!.CreatedDate) === instant(byTime[i]!.CreatedDate)) j++;
    const tied = byTime.slice(i, j);
    i = j;
    if (tied.length === 1) {
      out.push(tied[0]!);
      continue;
    }
    const news = tied.map((c) => keyOf(c.NewValue));
    const starts = tied.filter((c) => !news.includes(keyOf(c.OldValue)) || keyOf(c.OldValue) === null);
    if (starts.length !== 1) return null;
    let last = starts[0]!;
    const left = tied.filter((c) => c !== last);
    out.push(last);
    while (left.length) {
      const next = left.filter((c) => keyOf(c.OldValue) === keyOf(last.NewValue));
      if (next.length !== 1) return null;
      last = next[0]!;
      left.splice(left.indexOf(last), 1);
      out.push(last);
    }
  }
  return out;
}

/**
 * A case's visits from its changes, starting in the first change's old value (or
 * its current status with none), or null when a value has no meaning. A first
 * change with no old value is the case being made: its new value is the status
 * it was made in, and it is no move.
 */
function visitsOf(madeAt: number, changes: StatusChangeRow[], current: Status | null, table: (v: string | null) => Status | null): Visit[] | null {
  const made = changes.length > 0 && valueOf(changes[0]!.OldValue) === null;
  const moves = made ? changes.slice(1) : changes;
  const first = made ? table(changes[0]!.NewValue) : moves.length ? table(moves[0]!.OldValue) : current;
  if (!first) return null;
  const visits: Visit[] = [{ status: first, start: madeAt, end: null }];
  for (const c of moves) {
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
        said`We couldn't follow your cases' statuses: we found no recorded change to a case's Status in the last year, though ${closed} cases were closed in it. Either Salesforce isn't keeping a history of the Status field, or we can't see it. If field history isn't switched on for Case Status, switching it on lets later runs measure from that day. It can also be that every one of these cases was made already closed.`,
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

  const counts = { beforeRead: 0, beforeHistory: 0, recent: 0, noChange: 0, unordered: 0, unknown: 0, mismatch: 0 };
  const cases: PathCase[] = [];
  const universe: SnapCase[] = [];
  for (const r of rows) {
    const raw = changes.get(r.id) ?? [];
    const own = ordered(raw, table);
    const current = r.open ? table(r.open.Status) : null;
    const visits = own === null || (r.open && !current) ? null : visitsOf(r.made, own, current, table);
    const lastOf = visits?.at(-1)?.status ?? null;
    // Where the case stands now must be where its changes end: a closed case in a closed status, an open one in its Status.
    const consistent = visits !== null && lastOf !== null && (r.closed ? lastOf.closed : !current!.closed && lastOf.key === current!.key);
    if (consistent && !(r.closed && raw.length === 0)) universe.push({ id: r.id, madeAt: r.made, visits: visits!, priority: r.priority, origin: r.origin });
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
    if (r.closed && raw.length === 0) {
      counts.noChange++;
      continue;
    }
    if (own === null) {
      counts.unordered++;
      continue;
    }
    if (!visits) {
      counts.unknown++;
      continue;
    }
    if (!consistent) {
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
    ...(both.size ? [named(said`${plural(both.size, "case")} read as both open and closed, as ${both.size === 1 ? said`it` : said`they`} changed while we read`, { both: both.size })] : []),
    ...(counts.noChange ? [named(said`${plural(counts.noChange, "closed case")} with no recorded status change, so we can't tell which statuses ${counts.noChange === 1 ? said`it` : said`they`} went through`, { noChange: counts.noChange })] : []),
    ...(counts.unordered ? [named(said`${plural(counts.unordered, "case")} with status changes in the same second that we couldn't put in one order`, { unordered: counts.unordered })] : []),
    ...(counts.unknown ? [named(said`${plural(counts.unknown, "case")} with a status we couldn't find in your case status settings, or with none`, { unknown: counts.unknown })] : []),
    ...(counts.mismatch ? [named(said`${plural(counts.mismatch, "case")} whose recorded status changes don't end where the case stands now`, { mismatch: counts.mismatch })] : []),
  ];
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
    universe,
    openRead: openRows.length,
    yearStart: yearStart.getTime(),
    noChange: counts.noChange,
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
  const exact = median(values);
  if (exact < 1) {
    // Under a day, in hours, so a median is never "0 calendar days".
    const hours = Math.round(exact * 24);
    return hours === 0 ? said`a median of under an hour` : hours === 1 ? said`a median of 1 hour` : named(said`a median of ${hours} hours`, { hours: [hours, "hour"] });
  }
  const mid = round1(exact);
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

/** Any pseudonym token, an email, or a Salesforce id's shape: what the digest's name guard reads as a person or a secret. */
const GUARDED = [/\[\[[a-z]+[:\]]/, /@/, /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/];
/**
 * Whether an org's name can be written into a sentence: never one the privacy
 * pass or the digest's name guard would take for a person or a secret. A
 * status named like an email or an id is no person, and saying so would be
 * false, so the sentence leaves that name out instead.
 */
export const sayable = (ctx: Ctx, name: string): boolean => !GUARDED.some((re) => re.test(name)) && !holdsNameOrSecret(name, ctx.pseudonyms) && ctx.pseudonyms.scrub(name) === name;

/** The hotspots worth naming: in a dimension with two or more values among these rows, and a name we can write. */
function nameable<R>(ctx: Ctx, rows: R[], dims: Record<string, (r: R) => string>, hotspots: Hotspot[]): Hotspot[] {
  const many = new Set(Object.entries(dims).filter(([, keyOf]) => new Set(rows.map(keyOf)).size >= 2).map(([d]) => d));
  return hotspots.filter((h) => many.has(h.dimension) && sayable(ctx, h.key));
}

// ─── C1: more open cases past their status's line today than usual ─────────

/** A status's line on a day, as said: rounded up, so "ended within" stays true; in hours under a day, never "0 days". */
export function lineSaid(days: number): Said {
  if (days < 1) {
    const hours = Math.max(1, Math.ceil(days * 24));
    return hours === 1 ? said`an hour` : named(said`${hours} hours`, { hours: [hours, "hour"] });
  }
  const up = Math.ceil(days * 10) / 10;
  return up === 1 ? said`1 day` : named(said`${up} days`, { up: [up, "day"] });
}

/** One open case on a day: its status, since when it was there, and whether it was past that status's line then. */
export type Target = { id: string; status: Status; since: number; days: number; past: boolean; priority: string; origin: string };
export type Snapshot = { at: number; lines: Map<string, number>; targets: Target[]; noLine: number; open: number; expected: Map<string, number> };

/** The points today must be above the steady share by: our least, the z bar's at `n` open cases, or a share of it, whichever is most. */
export const steadyMarginOf = (steady: number, n: number): number => Math.max(STUCK_BAR.points, Math.ceil(STUCK_BAR.z * Math.sqrt((steady * (100 - steady)) / n)), Math.ceil(STUCK_BAR.relative * steady));

/** A stay as the line reads it: its length in days, and whether it ended (else it is still running, at that age). */
export type Stay = { days: number; ended: boolean };

/** The share of stays still running past each time, Kaplan–Meier: a running stay counts until its age, then drops out. */
export function survival(stays: Stay[]): Array<{ t: number; s: number }> {
  const sorted = [...stays].sort((a, b) => a.days - b.days || Number(b.ended) - Number(a.ended));
  const out: Array<{ t: number; s: number }> = [];
  let atRisk = sorted.length;
  let s = 1;
  for (let i = 0; i < sorted.length; ) {
    const t = sorted[i]!.days;
    let ended = 0;
    let gone = 0;
    while (i < sorted.length && sorted[i]!.days === t) {
      if (sorted[i]!.ended) ended++;
      gone++;
      i++;
    }
    if (ended > 0) {
      s *= 1 - ended / atRisk;
      out.push({ t, s });
    }
    atRisk -= gone;
  }
  return out;
}

/** The time by which the share still running first falls to or below `left`, or null when the stays seen never show it. */
export function survivalAt(curve: Array<{ t: number; s: number }>, left: number): number | null {
  return curve.find((p) => p.s <= left + 1e-12)?.t ?? null;
}

/**
 * The share of open cases a steady queue would have past a line, from the stays' survival: a case open on a given day
 * is in a stay with probability in proportion to its length, so the share past the line is the area under the survival
 * curve beyond the line over the whole area (length bias), E[max(0, X − line)] / E[X]. Stays still running are counted
 * to their age and then censored (Kaplan–Meier), never as ended, so the curve doesn't read short; the area stops where
 * the stays seen stop, which the line's tail rule keeps to a small remainder. Given plain lengths, every one ended.
 * A shared helper for the deal check's version is being built apart (hc/stuck-expected); the two are to be reconciled.
 */
export function steadyShare(stays: Array<number | Stay>, line: number): number {
  const list = stays.map((x) => (typeof x === "number" ? { days: x, ended: true } : x));
  const curve = survival(list);
  // To the longest stay seen, ended or not: beyond the last end, the share still running carries on to there.
  const tau = Math.max(0, ...list.map((x) => x.days));
  let area = 0;
  let beyond = 0;
  let prevT = 0;
  let prevS = 1;
  for (const p of [...curve, { t: tau, s: curve.at(-1)?.s ?? 1 }]) {
    if (p.t < prevT) continue;
    area += prevS * (p.t - prevT);
    beyond += prevS * Math.max(0, p.t - Math.max(prevT, line));
    prevT = p.t;
    prevS = p.s;
  }
  return area > 0 ? beyond / area : 0;
}

/**
 * Open cases in a status this many times longer than the longest stay in it that ended in the year read, with the
 * calibrated minimum of ended stays: raised whatever the line says, so a status nobody has entered lately, whose open
 * cases are far older than any stay there ever ended, is never read as usual. At least FAR_MIN such cases.
 */
export const FAR_TIMES = 2;
export const FAR_MIN = 5;

/** Open cases far older than any ended stay in their status (FAR_TIMES), with that status's longest ended stay. */
export function farPast(cases: SnapCase[], at: number, since: number, historyFrom: number, minSample: number): Array<{ id: string; status: Status; days: number; longest: number }> {
  const ended = new Map<string, number[]>();
  for (const x of cases) x.visits.forEach((v, i) => {
    if (v.status.closed || v.start < since || v.end === null || v.end > at || (i === 0 && x.madeAt < historyFrom)) return;
    ended.set(v.status.key, [...(ended.get(v.status.key) ?? []), daysOf(v.end - v.start)]);
  });
  const out: Array<{ id: string; status: Status; days: number; longest: number }> = [];
  for (const x of cases) {
    if (x.madeAt > at) continue;
    const i = x.visits.findIndex((v) => v.start <= at && (v.end === null || v.end > at));
    const v = i >= 0 ? x.visits[i]! : null;
    if (!v || v.status.closed) continue;
    const list = ended.get(v.status.key) ?? [];
    if (list.length < minSample) continue;
    const longest = Math.max(...list);
    const days = daysOf(at - (i > 0 ? v.start : Math.max(x.madeAt, historyFrom)));
    if (days > FAR_TIMES * longest) out.push({ id: x.id, status: v.status, days, longest });
  }
  return out;
}

/** A status's stays may leave at most this share still running beyond the longest seen; more, and its tail isn't covered. */
export const TAIL_LEFT = 0.05;

/**
 * The open cases at an instant and which were past their status's line then.
 * A status's line is the 75th percentile of the stays that began in it in the
 * LOOKBACK_DAYS ending GAP_DAYS before the instant, read from their survival
 * (Kaplan–Meier: a stay still running then counts to its age, never as ended),
 * so the line uses nothing the day didn't know and doesn't read short. A status
 * whose stays leave more than TAIL_LEFT still running past the longest seen has
 * no line: the history doesn't cover its tail. A case with no change of its own
 * before the instant has been in its status since it was made or, if it was
 * made before the first change found, at least since then. Compared unrounded.
 */
export function snapshotAt(cases: SnapCase[], at: number, historyFrom: number, minSample: number): Snapshot {
  const until = at - GAP_DAYS * DAY;
  const from = until - LOOKBACK_DAYS * DAY;
  const stays = new Map<string, Stay[]>();
  for (const x of cases) {
    x.visits.forEach((v, i) => {
      if (v.status.closed || v.start >= until || v.start < from) return;
      // A first stay of a case made before the history starts began at a time we can't see.
      if (i === 0 && x.madeAt < historyFrom) return;
      const ended = v.end !== null && v.end <= at;
      stays.set(v.status.key, [...(stays.get(v.status.key) ?? []), { days: daysOf((ended ? v.end! : at) - v.start), ended }]);
    });
  }
  const lines = new Map<string, number>();
  const expected = new Map<string, number>();
  for (const [key, list] of stays) {
    if (list.length < minSample) continue;
    const curve = survival(list);
    const line = survivalAt(curve, 1 - STAY_QUANTILE);
    // Too many still running past the longest stay seen: the history doesn't show how long this status's stays last.
    if (line === null || (curve.at(-1)?.s ?? 1) > TAIL_LEFT) continue;
    lines.set(key, line);
    expected.set(key, steadyShare(list, line));
  }
  const targets: Target[] = [];
  let noLine = 0;
  let open = 0;
  for (const x of cases) {
    if (x.madeAt > at) continue;
    const i = x.visits.findIndex((v) => v.start <= at && (v.end === null || v.end > at));
    const v = i >= 0 ? x.visits[i]! : null;
    if (!v || v.status.closed) continue;
    open++;
    const line = lines.get(v.status.key);
    if (line === undefined) {
      noLine++;
      continue;
    }
    const since = i > 0 ? v.start : Math.max(x.madeAt, historyFrom);
    const days = daysOf(at - since);
    targets.push({ id: x.id, status: v.status, since, days, past: days > line, priority: x.priority, origin: x.origin });
  }
  return { at, lines, targets, noLine, open, expected };
}

/**
 * Today's open cases against the same org's earlier weeks, never against
 * completed stays: a snapshot of open cases holds more long stays than a list
 * of ended ones, so "past the line" is set beside how often it was so on the
 * same weekday and time in each earlier week of the year read, each judged
 * with only what it knew then. Raised only when today is above what 90% of
 * those weeks were at or below and at least STUCK_MARGIN points above their
 * median, with the calibrated minimum of open cases today and in each week
 * counted, and MIN_SNAPSHOTS such weeks.
 */
export const stuckInStatus: Rule = {
  id: "svc.stuck-in-status",
  version: 1,
  domain: DOMAIN,
  title: "Open cases past the usual time in their status",
  records: true,
  evaluate(ctx): RuleResult {
    const c = caseCohortOf(ctx);
    if (c.status !== "ok") return { outcome: "not_measurable", reason: c.reason, evidence: c.evidence };
    const { minSample } = ctx.calibration;
    const now = ctx.now.getTime();
    const today = snapshotAt(c.universe, now, c.historyFrom, minSample);
    const noStatus = c.openRead - today.open;
    const noLine = today.noLine;
    const exclusions = [
      ...(noStatus ? [named(said`${plural(noStatus, "open case")} whose status we couldn't place: not in your case status settings, a closed status, or not where ${noStatus === 1 ? said`its` : said`their`} recorded changes end`, { noStatus })] : []),
      ...(noLine ? [named(said`${plural(noLine, "open case")} in a status with fewer than ${minSample} stays begun between ${LINE_FROM_DAYS} and ${GAP_DAYS} days earlier, or too many of them still running to show how long they last, to set a line`, { noLine, minSample, lineFrom: [LINE_FROM_DAYS, "day"], gap: [GAP_DAYS, "day"] })] : []),
    ];
    const evidence = [...c.evidence, ctx.ev(c.openSource, { count: today.targets.length, note: "Open cases now, in a status with a line to compare", exclusions })];
    const n = today.targets.length;
    // Open cases far older than any stay in their status that ended in the year: raised whatever the lines say.
    const far = farPast(c.universe, now, Math.max(c.yearStart, c.historyFrom), c.historyFrom, minSample);
    const farCount = far.length;
    const overFar = farCount >= FAR_MIN;
    const farStatus = [...groupBy(far, (x) => x.status.key)].map(([, xs]) => xs).sort((a, b) => b.length - a.length)[0];
    const farLabel = farStatus && sayable(ctx, farStatus[0]!.status.label) ? farStatus[0]!.status.label : null;
    const farCore = !overFar
      ? said``
      : farLabel
        ? farStatus!.length === farCount
          ? named(said`${farCount} open cases have been in their status more than ${FAR_TIMES} times as long as the longest stay there that ended in the year we read, all of them in ${org(farLabel)}.`, { farCount, times: [FAR_TIMES, "ratio"] })
          : named(said`${farCount} open cases have been in their status more than ${FAR_TIMES} times as long as the longest stay there that ended in the year we read, ${farStatus!.length} of them in ${org(farLabel)}.`, { farCount, times: [FAR_TIMES, "ratio"], farThere: farStatus!.length })
        : named(said`${farCount} open cases have been in their status more than ${FAR_TIMES} times as long as the longest stay there that ended in the year we read.`, { farCount, times: [FAR_TIMES, "ratio"] });
    const farSaid = overFar ? said` ${farCore}` : said``;
    const farLine = named(said`${FAR_MIN} or more open cases have been in their status more than ${FAR_TIMES} times as long as any stay there that ended`, { farMin: FAR_MIN, times: [FAR_TIMES, "ratio"] });
    if (n < minSample) {
      const few = n === 0 ? said`No open case is` : n === 1 ? said`1 open case is` : named(said`${n} open cases are`, { n });
      const thin = named(said`${few} in a status with a line today, below the calibrated minimum of ${minSample}.`, { minSample });
      if (!overFar) return { outcome: "not_measurable", reason: thin, evidence };
      return { ...farIssue(said`${farCore} ${thin} We raise this when ${farLine}: that line is ours.`, evidence, farLabel, new Set(far.map((x) => x.status.key)).size), facts: { far: farCount, n } };
    }
    const hits = today.targets.filter((t) => t.past).length;
    const rate = pct(hits, n);
    // What a steady queue with these stay times would have past the lines, weighted by today's open cases in each status.
    const steady = round1((today.targets.reduce((t, x) => t + today.expected.get(x.status.key)!, 0) / n) * 100);
    // Its margin: our least points, the z bar's points at today's count, or a share of the steady share itself, whichever is
    // most: a status with long stays has its steady share read a little short from stays that began in the lookback, and the
    // probe's heavy-tailed orgs read 13 to 16 points over it with nothing wrong.
    const steadyMargin = steadyMarginOf(steady, n);
    // The earlier weeks: the same weekday and time, back while a whole lookback and gap lie inside what we read.
    const earliest = Math.max(c.yearStart, c.historyFrom) + (LOOKBACK_DAYS + GAP_DAYS) * DAY;
    const weeks: Snapshot[] = [];
    // From a whole gap back, so the weeks today is set beside aren't the weeks in which a backlog may be building.
    for (let at = now - Math.ceil(GAP_DAYS / SNAPSHOT_EVERY_DAYS) * SNAPSHOT_EVERY_DAYS * DAY; at >= earliest; at -= SNAPSHOT_EVERY_DAYS * DAY) {
      const snap = snapshotAt(c.universe, at, c.historyFrom, minSample);
      if (snap.targets.length >= minSample) weeks.push(snap);
    }
    const counted = weeks.length;
    const useWeeks = STUCK_BAR.mode !== "steady";
    const useSteady = STUCK_BAR.mode !== "weeks";
    const shareOf = (snap: Snapshot): number => pct(snap.targets.filter((t) => t.past).length, snap.targets.length);
    const shares = weeks.map(shareOf);
    const enoughWeeks = useWeeks && counted >= MIN_SNAPSHOTS;
    if (STUCK_BAR.mode === "weeks" && !enoughWeeks) {
      const fewWeeks = counted === 0 ? said`no earlier week` : counted === 1 ? said`only 1 earlier week` : named(said`only ${counted} earlier weeks`, { counted });
      return { outcome: "not_measurable", reason: named(said`We could set today's open cases beside ${fewWeeks} with ${minSample} or more open cases in a status with a line, and we need ${MIN_SNAPSHOTS} to say what is usual for your cases.`, { minSample, minSnapshots: MIN_SNAPSHOTS }), evidence };
    }
    const usual = enoughWeeks ? nearestRank(shares, STUCK_BAR.quantile) : 0;
    const USUAL_SHARE = Math.round(STUCK_BAR.quantile * 100);
    const middle = enoughWeeks ? round1(median(shares)) : 0;
    // The margin over the weeks: our least points, or the z bar's points at today's count, whichever is more, whole points up.
    const margin = Math.max(STUCK_BAR.points, Math.ceil(STUCK_BAR.z * Math.sqrt((middle * (100 - middle)) / n)));
    const dims: Record<string, (t: Target) => string> = { status: (t) => t.status.label, priority: (t) => t.priority, ...(c.hasOrigin ? { origin: (t: Target) => t.origin } : {}) };
    const segmentsChecked: SegmentCheck[] = Object.entries(dims).map(([dimension, keyOf]) => ({ dimension, segments: new Set(today.targets.map(keyOf)).size }));
    const todaySaid = named(said`Today ${hits} of ${n} open cases (${rate}%) ${hits === 1 ? said`has been in its` : said`have been in their`} status longer than ${STAY_SHARE}% of the stays that began there between ${LINE_FROM_DAYS} and ${GAP_DAYS} days earlier.`, { hits, n, rate: [rate, "percent"], stayShare: [STAY_SHARE, "percent"], lineFrom: [LINE_FROM_DAYS, "day"], gap: [GAP_DAYS, "day"] });
    const steadySaid = useSteady ? named(said` With your cases' own stay times, a steady queue would have about ${steady}% past that point.`, { steady: [steady, "percent"] }) : said``;
    const firstWeek = enoughWeeks ? localDay(new Date(weeks.at(-1)!.at), zoneOf(ctx)) : "";
    const weeksSaid = !useWeeks
      ? said``
      : enoughWeeks
        ? named(said` On the same day of the week in the ${counted} earlier weeks we could compare, back to ${daySaid(firstWeek)}, the share was a median of ${middle}%, and ${USUAL_SHARE}% of those weeks were at or below ${usual}%.`, { counted, firstWeek: [firstWeek, "date"], middle: [middle, "percent"], usualShare: [USUAL_SHARE, "percent"], usual: [usual, "percent"] })
        : counted === 0
          ? named(said` No earlier week had ${minSample} or more open cases with a line to compare with.`, { minSample })
          : counted === 1
            ? named(said` Only 1 earlier week had ${minSample} or more open cases with a line, too few to say what is usual.`, { minSample })
            : named(said` Only ${counted} earlier weeks had ${minSample} or more open cases with a line, too few to say what is usual.`, { counted, minSample });
    const lead = said`${todaySaid}${steadySaid}${weeksSaid} Days are calendar days.`;
    const overWeeks = enoughWeeks && rate > usual && rate - middle >= margin;
    const overSteady = useSteady && rate - steady >= steadyMargin;
    // Raised on the shares only when today is over BOTH: a steady queue alone reads a seasonal org's quiet weeks as stuck
    // (the second generator: seasonal arrivals raised in 67% of orgs on the steady share alone), and earlier weeks alone
    // read a block that has sat all year as usual (which the far clause catches). In the probes' "weeks" or "steady" mode,
    // that comparison alone.
    const overShare = STUCK_BAR.mode === "both" ? overWeeks && overSteady : STUCK_BAR.mode === "weeks" ? overWeeks : overSteady;
    const weeksLine = named(said`above ${usual}% and at least ${margin} points above the median of ${middle}%`, { usual: [usual, "percent"], margin: [margin, "points"], middle: [middle, "percent"] });
    const steadyLine = named(said`at least ${steadyMargin} points above the steady ${steady}%`, { steadyMargin: [steadyMargin, "points"], steady: [steady, "percent"] });
    const lines = STUCK_BAR.mode === "both" ? said`${weeksLine}, and ${steadyLine}` : STUCK_BAR.mode === "weeks" ? weeksLine : steadyLine;
    const facts = { hits, n, rate, steady, steadyMargin, weeks: counted, ...(enoughWeeks ? { median: middle, usual, margin } : {}), stayShare: STAY_SHARE, lookbackDays: LOOKBACK_DAYS };
    if (!overShare && !overFar) {
      const tail = STUCK_BAR.mode === "both" && !enoughWeeks ? said`We compare today's share only beside enough earlier weeks, so we raise this now only when ${farLine}, and it isn't so.` : said`We raise this only when today's share is ${lines}, or when ${farLine}, and neither is so.`;
      return { outcome: "pass", summary: said`${lead} ${tail}`, evidence, segmentsChecked, facts: { ...facts, far: farCount } };
    }
    // The status with the most open cases past its line today, when its name can be written.
    const byStatus = [...groupBy(today.targets.filter((t) => t.past), (t) => t.status.key)].map(([key, ts]) => ({ key, past: ts.length, status: ts[0]!.status, there: today.targets.filter((t) => t.status.key === key).length }));
    const top = byStatus.filter((x) => sayable(ctx, x.status.label)).sort((a, b) => b.past - a.past || (a.status.label < b.status.label ? -1 : 1))[0];
    const statusSaid = top
      ? named(said` In ${org(top.status.label)}, ${top.past} of ${top.there} open ${top.there === 1 ? said`case is` : said`cases are`} past that status's line: ${STAY_SHARE}% of the stays that began there between ${LINE_FROM_DAYS} and ${GAP_DAYS} days earlier ended within ${lineSaid(today.lines.get(top.key)!)}.`, { topPast: top.past, topThere: top.there, stayShare: [STAY_SHARE, "percent"], lineFrom: [LINE_FROM_DAYS, "day"], gap: [GAP_DAYS, "day"] })
      : said``;
    const which = !overShare ? said`` : overWeeks && overSteady ? said` That is more than both what was usual in earlier weeks and what a steady queue would have.` : overWeeks ? said` That is more than what was usual in earlier weeks.` : said` That is more than what a steady queue would have.`;
    return {
      outcome: "issue",
      title: said`More of your open cases are past the usual time in their status than we would expect`,
      summary: said`${lead}${overShare ? statusSaid : said``}${which}${farSaid} ${STUCK_BAR.mode === "both" && !enoughWeeks ? said`We compare today's share only beside enough earlier weeks, so we raise this now only when ${farLine}: that line is ours.` : said`We raise this when today's share is ${lines}, or when ${farLine}: these lines are ours.`}`,
      recommendation: said`Go through the open cases that have waited longest in each status and move each one on or close it, then look at why cases wait there.`,
      recommendationType: "change",
      severity: "medium",
      confidence: n < minSample * 2 ? "medium" : "high",
      evidence,
      value,
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked,
      ...(top ? { subjects: [{ type: "status", label: top.status.label }] } : {}),
      facts: { ...facts, far: farCount },
    };
  },
};

/** The stuck check raised on open cases far older than any ended stay alone, with too few others to compare. */
function farIssue(summary: Said, evidence: Evidence[], label: string | null, statuses: number): Omit<Extract<RuleResult, { outcome: "issue" }>, "facts"> {
  return {
    outcome: "issue",
    title: said`Some open cases have been in their status far longer than any case before them`,
    summary,
    recommendation: said`Go through the open cases that have waited longest in each status and move each one on or close it, then look at why cases wait there.`,
    recommendationType: "change",
    severity: "medium",
    confidence: "medium",
    evidence,
    value,
    effort: { size: "S" },
    owner: OWNER,
    segmentsChecked: [{ dimension: "status", segments: statuses }],
    ...(label ? { subjects: [{ type: "status", label }] } : {}),
  };
}

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
    // A reopened case whose close date is its first close, though a later close is recorded: the close date we read would
    // then not be the final close, and every time after a first close would read short. Direct evidence, so nothing is said.
    const keptFirst = reopened.filter((x) => !x.open && Math.abs(x.closedAt! - x.firstCloseAt!) < 1000 && x.visits.some((v) => v.status.closed && v.start > x.firstCloseAt!)).length;
    if (keptFirst > 0) {
      const which = keptFirst === 1 ? said`On 1 reopened case, the close date we read is its first close, though a later close is recorded` : named(said`On ${keptFirst} reopened cases, the close date we read is their first close, though a later close is recorded`, { keptFirst });
      return { outcome: "not_measurable", reason: said`${which}, so we can't tell from the close date how long these cases took after their first close.`, evidence: c.evidence };
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
    const lead = named(said`Of the ${n} cases made ${periodSaid(c)}, ${hits} (${rate}%) ${hits === 1 ? said`was` : said`were`} closed straight from the status they started in, with no other status between.`, { n, hits, rate: [rate, "percent"] });
    const openSaid = open === 0 ? said`` : open === 1 ? said` 1 case has never been closed, and isn't counted as closed straight.` : named(said` ${open} cases have never been closed, and aren't counted as closed straight.`, { open });
    // Cases closed with no recorded change may be the very cases made already closed: we can't tell, so they are said.
    const noChange = c.noChange;
    const noChangeSaid = noChange === 0 ? said`` : noChange === 1 ? said` 1 case closed with no recorded status change isn't counted.` : named(said` ${noChange} cases closed with no recorded status change aren't counted.`, { noChange });
    const facts = { hits, n, rate, open, noChange, limit: STRAIGHT_LIMIT, segmentLimit: STRAIGHT_SEGMENT_LIMIT };
    const over = rate > STRAIGHT_LIMIT;
    // Only a segment worth naming: in a dimension with two or more values here, and a name we can write.
    const hotspots = nameable(ctx, c.cases, dims, result.hotspots);
    if (!over && hotspots.length === 0) {
      return { outcome: "pass", summary: named(said`${lead}${openSaid}${noChangeSaid} Our lines are ${STRAIGHT_LIMIT}% overall and ${STRAIGHT_SEGMENT_LIMIT}% in any one starting status, priority or origin, and none is crossed.`, { limit: [STRAIGHT_LIMIT, "percent"], segmentLimit: [STRAIGHT_SEGMENT_LIMIT, "percent"] }), evidence: c.evidence, segmentsChecked: result.segmentsChecked, facts };
    }
    const worst = hotspots[0];
    const hidden = !worst
      ? said``
      : !over
        ? named(said` The total is within our line of ${STRAIGHT_LIMIT}%, but by ${dimensionWords(worst.dimension)}, ${org(worst.key)} is at ${worst.rate}% (${worst.hits} of ${worst.n}), over our line of ${STRAIGHT_SEGMENT_LIMIT}% for any one.`, { straightLimit: [STRAIGHT_LIMIT, "percent"], worstPct: [worst.rate, "percent"], worstHits: worst.hits, worstN: worst.n, segmentLimit: [STRAIGHT_SEGMENT_LIMIT, "percent"] })
        : worst
          ? named(said` That is over our line of ${STRAIGHT_LIMIT}%. Highest by ${dimensionWords(worst.dimension)}: ${org(worst.key)}, at ${worst.rate}%.`, { straightLimit: [STRAIGHT_LIMIT, "percent"], worstPct: [worst.rate, "percent"] })
          : said``;
    const overSaid = over && !worst ? named(said` That is over our line of ${STRAIGHT_LIMIT}%.`, { straightLimit: [STRAIGHT_LIMIT, "percent"] }) : said``;
    // Titled by what is true: over the line overall, or in one segment.
    const title = over
      ? named(said`More than ${STRAIGHT_LIMIT}% of cases are closed straight from the status they started in`, { straightLimit: [STRAIGHT_LIMIT, "percent"] })
      : named(said`More than ${STRAIGHT_SEGMENT_LIMIT}% of cases with ${dimensionWords(worst!.dimension)} ${org(worst!.key)} are closed straight from the status they started in`, { segmentLimit: [STRAIGHT_SEGMENT_LIMIT, "percent"] });
    return {
      outcome: "issue",
      title,
      summary: said`${lead}${hidden}${overSaid}${openSaid}${noChangeSaid} Some may be duplicates, spam or cases closed by automation, and some may be questions answered at once: the statuses alone can't tell them apart.`,
      recommendation: said`Look at a sample of these cases to see which they are. Filter duplicates and spam out where they come in, check that any automation closing cases should be, and leave quick answers as they are.`,
      recommendationType: "investigate",
      severity: "low",
      confidence: "medium",
      evidence: c.evidence,
      value,
      effort: { size: "S" },
      owner: OWNER,
      segmentsChecked: result.segmentsChecked,
      subjects: hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
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
      return { outcome: "pass", summary: named(said`We compared ${plural(tests, "status", "statuses")} that cases were sent back from with the cases that reached ${tests === 1 ? said`it` : said`them`} without being sent back. We couldn't tell whether those sent back were closed within ${lagDays} days of being made more or less often.`, { tests, lagDays: [lagDays, "day"] }), evidence: c.evidence, segmentsChecked, facts: { tested: tests, bar } };
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
