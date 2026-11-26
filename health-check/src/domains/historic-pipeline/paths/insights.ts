import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { org, said, type Said } from "../../../engine/org-words.ts";
import { andList } from "../../../engine/said-forms.ts";
import type { SegmentCheck } from "../../../schemas/index.ts";
import { dimensions, groupBy, median, round1 } from "../metrics.ts";
import { DRIVER_GAP } from "../drivers.ts";
import { cohortDeals, comparable, daySaid, periodOf, type CohortDeal, type Period } from "../reached.ts";
import { closingOut, loadPaths, sendBacks, type PathDeal, type Paths } from "./variants.ts";

// The insights read from the paths deals take (DECISIONS.md 703): each a
// sentence about the customer's deals first, with its figures as the evidence.
//
// Both sides of every comparison reached the same point, so neither is a group
// of survivors: deals that skipped a stage against deals that went through it,
// among those that reached the stage after it; deals sent back from a stage
// against the rest that reached it. The usual path is a label in the variant
// table and never a side. An insight is raised on win rate alone, and only when
// it is honest to: enough deals, enough won and lost on each side, a gap big
// enough to matter and unlikely to be chance once every comparison tried is
// counted, the gap inside the record types and segments that can check it.
// Below that nothing is said; the figures stay in the variant table. Every
// comparison says how often two things went together, never that one caused the other.
//
// Who is compared is chosen by when the deals were made, in a period that ends
// a maturity lag before the run (reached.ts; DECISIONS.md 712), never by when
// they closed: when a path changes how fast won deals close but not lost ones,
// a sample by close date made a gap that wasn't there whenever deal volume
// changed (up to 66% of simulated orgs in a cell). Not by when they reached the
// stage either, as the stage probabilities are (706): when a path changes how
// fast won deals reach the stage, that still made one (7.5%). When a deal was
// made comes before its path and its outcome, so choosing by it favours
// neither. The few still open are counted both ways, and both must clear.

type Ctx = Omit<RuleContext, "metrics">;

const DAY = 86_400_000;

/** Points of win rate, as a win-rate driver needs (drivers.ts). */
export const WIN_GAP = DRIVER_GAP;
/** Won deals and lost deals each side needs, so a rate isn't one or two deals either way. */
export const MIN_EACH_OUTCOME = 5;
/**
 * The two-proportion z a gap needs before the count of comparisons raises it
 * (Bonferroni). At 2, one comparison is a two-sided 4.55%, and where the 10-point
 * gap is about 2 standard errors no other bar adds a margin: on dated orgs with
 * nothing to find, 4.3% of orgs of 1,000 deals got an insight, single cells of
 * 1,000 orgs up to 6.3%, and with deals chosen by when they were made one cell
 * still reached 5.3% (DECISIONS.md 712). At 2.15, a two-sided 3.2% and the stage
 * probabilities' floor (706), the worst cell is 3.6%.
 */
export const Z_FLOOR = 2.15;
/** The family-wise chance of a false insight the z bar is set to, across every comparison a run tries. */
export const FAMILY_ALPHA = 0.05;
/** A won deals' days gap is said only when it is at least this share of the other side's median... */
export const DAYS_GAP_SHARE = 25;
/** ...and at least this many days. Days never raise an insight on their own. */
export const DAYS_GAP_MIN = 7;
/** A segment holding this share of either side or more is the whole side over again, and checks nothing. */
export const WHOLE_SHARE = 0.9;
/** Segments an insight needs compared, with the gap pointing the same way in most of them. */
export const MIN_SEGMENTS = 2;

type Side = { deals: PathDeal[]; n: number; won: number; rate: number };
const sideOf = (deals: PathDeal[]): Side => ({ deals, n: deals.length, won: deals.filter((d) => d.won).length, rate: pct(deals.filter((d) => d.won).length, deals.length) });

/** The two-proportion z of a group's win rate against the other side's, pooled. Zero when it can't be worked out. */
export function zOf(a: Pick<Side, "n" | "won">, b: Pick<Side, "n" | "won">): number {
  const p = (a.won + b.won) / (a.n + b.n);
  const se = Math.sqrt(p * (1 - p) * (1 / a.n + 1 / b.n));
  return se === 0 ? 0 : (a.won / a.n - b.won / b.n) / se;
}

/** The standard normal's upper tail, by the complementary error function (Abramowitz and Stegun 7.1.26). */
function upperTail(z: number): number {
  const x = Math.abs(z) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * x);
  const erfc = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429)))) * Math.exp(-x * x);
  return z >= 0 ? erfc / 2 : 1 - erfc / 2;
}

/** The z a gap needs when `tests` comparisons were tried: two-sided at FAMILY_ALPHA / tests, never below Z_FLOOR. */
export function zBar(tests: number): number {
  const target = FAMILY_ALPHA / 2 / Math.max(1, tests);
  let [low, high] = [0, 10];
  for (let i = 0; i < 60; i++) {
    const mid = (low + high) / 2;
    if (upperTail(mid) > target) low = mid;
    else high = mid;
  }
  return Math.max(Z_FLOOR, Math.ceil(high * 100) / 100);
}

/** The gap with the open deals counted one way, over every deal in the period: points, its z, and its z within creation-date bands. */
export type Bound = { gap: number; z: number; cmh: number };

/** A comparison: the group against the other side, deals made in the stage's period that reached it. Sides are the closed deals. */
export type Comparison = {
  reached: string;
  group: Side;
  other: Side;
  gap: number;
  z: number;
  /** The period the deals were made in, as local days. */
  from: string;
  to: string;
  /** Deals still open on each side, which can go either way. */
  open: { group: number; other: number };
  /** The group's open counted as lost and the other's as won (low), and the other way round (high). */
  low: Bound;
  high: Bound;
  /** Won deals' median days to win on each side, when each has the calibrated minimum of won deals. */
  days: { group: number; other: number; clears: boolean } | null;
  compared: number;
  held: number;
  /** Record types with both sides over the minimum, and how many the gap holds in. Zero of zero with one record type. */
  types: { many: boolean; compared: number; held: number };
};

/** Both sides big enough to test: the calibrated minimum, and enough won and lost deals in each. */
const testable = (ctx: Ctx, a: Side, b: Side): boolean =>
  [a, b].every((s) => s.n >= ctx.calibration.minSample && s.won >= MIN_EACH_OUTCOME && s.n - s.won >= MIN_EACH_OUTCOME);

/**
 * Rule 4 for a comparison, as a driver is checked (drivers.ts), counting only
 * segments that can check something: a dimension with two or more values among
 * these deals, and a segment holding less than WHOLE_SHARE of either side.
 */
function withinSegments(ctx: Ctx, group: PathDeal[], other: PathDeal[], gap: number): { compared: number; held: number } {
  const { minSample } = ctx.calibration;
  let compared = 0;
  let held = 0;
  for (const keyOf of Object.values(dimensions(ctx))) {
    const a = groupBy(group, (d) => keyOf(d.deal));
    const b = groupBy(other, (d) => keyOf(d.deal));
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

/** With more than one record type, the gap must hold inside one that has both sides over the minimum: a process without the stage isn't skipping it. */
function withinRecordTypes(ctx: Ctx, group: PathDeal[], other: PathDeal[], gap: number): Comparison["types"] {
  const typeOf = (d: PathDeal): string => d.deal.RecordType ?? "No record type";
  const types = new Set([...group, ...other].map(typeOf));
  if (types.size < 2) return { many: false, compared: 0, held: 0 };
  let compared = 0;
  let held = 0;
  for (const type of types) {
    const [a, b] = [group.filter((d) => typeOf(d) === type), other.filter((d) => typeOf(d) === type)];
    if (a.length < ctx.calibration.minSample || b.length < ctx.calibration.minSample) continue;
    compared++;
    const inside = sideOf(a).rate - sideOf(b).rate;
    if (Math.sign(inside) === Math.sign(gap) && Math.abs(inside) >= WIN_GAP) held++;
  }
  return { many: true, compared, held };
}

const medianDays = (deals: PathDeal[]): number => round1(median(deals.map((d) => d.days)));

/** Days in each creation-date band the stratified test sets deals side by side in. */
export const BAND_DAYS = 91;

/**
 * The Cochran–Mantel–Haenszel z of the group's wins against the other side's,
 * with the deals split into BAND_DAYS bands by the day they were made (712).
 * A gap that is only the mix changing over the period (more skippers in a year
 * when more deals were won, or a season) is set side by side band by band and
 * goes away; a gap inside the bands stays. Bands with one side empty, or with
 * every deal won or every deal lost, carry nothing and are passed over; with
 * none left the z is 0, which clears no bar.
 */
export function stratifiedZ(group: PathDeal[], other: PathDeal[], wonOf: (d: PathDeal) => boolean, from: string): number {
  return bandedZ(group, other, wonOf, (d) => new Date(d.deal.CreatedDate).getTime(), from);
}

/** The same test for any records, given when each was made (the lead journey's, DECISIONS.md 770). */
export function bandedZ<T>(group: T[], other: T[], wonOf: (d: T) => boolean, madeAt: (d: T) => number, from: string): number {
  const start = Date.parse(`${from}T00:00:00Z`);
  const bands = new Map<number, { won: number; n1: number; n2: number; m1: number }>();
  const add = (d: T, inGroup: boolean): void => {
    const k = Math.floor((madeAt(d) - start) / (BAND_DAYS * DAY));
    const b = bands.get(k) ?? { won: 0, n1: 0, n2: 0, m1: 0 };
    const won = wonOf(d);
    if (inGroup) {
      b.n1++;
      if (won) b.won++;
    } else b.n2++;
    if (won) b.m1++;
    bands.set(k, b);
  };
  for (const d of group) add(d, true);
  for (const d of other) add(d, false);
  let num = 0;
  let den = 0;
  for (const { won, n1, n2, m1 } of bands.values()) {
    const n = n1 + n2;
    const m2 = n - m1;
    if (n < 2 || n1 === 0 || n2 === 0 || m1 === 0 || m2 === 0) continue;
    num += won - (n1 * m1) / n;
    den += (n1 * n2 * m1 * m2) / (n * n * (n - 1));
  }
  return den > 0 ? num / Math.sqrt(den) : 0;
}

const isOpen = (d: PathDeal): boolean => (d as Partial<CohortDeal>).open === true;

/**
 * The gap, its z and its stratified z over every deal on each side, the group's
 * open deals counted as lost and the other's as won (`groupOpenWon` false), or the
 * other way round: the few still open go each way in both tests at once.
 */
function boundOf(groupAll: PathDeal[], otherAll: PathDeal[], groupOpenWon: boolean, from: string): Bound {
  const inGroup = new Set(groupAll);
  const wonOf = (d: PathDeal): boolean => (isOpen(d) ? inGroup.has(d) === groupOpenWon : d.won);
  const a = { n: groupAll.length, won: groupAll.filter(wonOf).length };
  const b = { n: otherAll.length, won: otherAll.filter(wonOf).length };
  return { gap: round1(pct(a.won, a.n) - pct(b.won, b.n)), z: Math.round(zOf(a, b) * 100) / 100, cmh: Math.round(stratifiedZ(groupAll, otherAll, wonOf, from) * 100) / 100 };
}

/** A group against the other side in a period, or null when the period has too many still open or either side's closed deals are too few to test. Days only on won deals. */
export function compare(ctx: Ctx, reached: string, groupAll: PathDeal[], otherAll: PathDeal[], period: Pick<Period, "from" | "to" | "tooOpen">): Comparison | null {
  const [group, other] = [groupAll.filter((d) => !isOpen(d)), otherAll.filter((d) => !isOpen(d))];
  const [g, o] = [sideOf(group), sideOf(other)];
  if (period.tooOpen || !period.from || !period.to || !testable(ctx, g, o)) return null;
  const open = { group: groupAll.length - group.length, other: otherAll.length - other.length };
  const gap = round1(g.rate - o.rate);
  const wonG = group.filter((d) => d.won);
  const wonO = other.filter((d) => d.won);
  const days = wonG.length >= ctx.calibration.minSample && wonO.length >= ctx.calibration.minSample ? (() => {
    const [dg, dother] = [medianDays(wonG), medianDays(wonO)];
    const diff = Math.abs(round1(dg - dother));
    return { group: dg, other: dother, clears: diff >= DAYS_GAP_MIN && diff * 100 >= DAYS_GAP_SHARE * dother };
  })() : null;
  return {
    reached,
    group: g,
    other: o,
    gap,
    z: Math.round(zOf(g, o) * 100) / 100,
    from: period.from,
    to: period.to,
    open,
    low: boundOf(groupAll, otherAll, false, period.from),
    high: boundOf(groupAll, otherAll, true, period.from),
    days,
    ...withinSegments(ctx, group, other, gap),
    types: withinRecordTypes(ctx, group, other, gap),
  };
}

/** Too many still open in the period to test, though its closed deals alone would have been enough. */
export function tooOpenToTest(ctx: Ctx, groupAll: PathDeal[], otherAll: PathDeal[], period: Pick<Period, "tooOpen">): boolean {
  return period.tooOpen && testable(ctx, sideOf(groupAll.filter((d) => !isOpen(d))), sideOf(otherAll.filter((d) => !isOpen(d))));
}

/** Raised: the win-rate gap clears every bar. Days never raise one. */
export function raised(c: Comparison | null, bar: number): c is Comparison {
  if (!c) return false;
  // The open deals counted both ways: each must clear the gap, the bar and the bar within the bands, on the same side.
  const side = Math.sign(c.low.gap);
  if (side === 0 || Math.sign(c.high.gap) !== side) return false;
  if ([c.low, c.high].some((b) => Math.abs(b.gap) < WIN_GAP || Math.abs(b.z) < bar || Math.sign(b.cmh) !== side || Math.abs(b.cmh) < bar)) return false;
  if (c.types.many && c.types.held === 0) return false;
  // No segment can check it: said at the lowest confidence. One alone, or gaps that turn round in most: not raised.
  if (c.compared === 0) return true;
  return c.compared >= MIN_SEGMENTS && c.held * 2 > c.compared;
}

/**
 * The smallest gap in win rate, in whole points, that the biggest comparison
 * tried could have told from chance at the bar: the bar times the pooled
 * standard error of its closed deals, and never under WIN_GAP. A gap smaller
 * than this may be there unseen.
 */
export function smallestTellable(cs: Comparison[], bar: number): number {
  const biggest = cs.reduce((a, c) => (c.group.n + c.other.n > a.group.n + a.other.n ? c : a));
  const p = (biggest.group.won + biggest.other.won) / (biggest.group.n + biggest.other.n);
  const se = Math.sqrt(p * (1 - p) * (1 / biggest.group.n + 1 / biggest.other.n));
  return Math.max(WIN_GAP, Math.ceil(bar * se * 100));
}

// ─── Who is compared ─────────────────────────────────────────────────────────

const at = (standard: string[], stage: string): number => standard.indexOf(stage);

/**
 * For each stage X of the usual path, with S the next: among deals that started
 * at a stage of the usual path before X and entered S, those that never entered
 * X against those that went through it. Both sides started before X, so history
 * starting late (at X or after) is neither a skip nor a way through it.
 */
export function skipComparisons(p: Paths, deals: PathDeal[]): Array<{ stage: string; reached: string; group: PathDeal[]; other: PathDeal[] }> {
  return p.standard.slice(0, -1).flatMap((stage, i) => {
    const reached = p.standard[i + 1]!;
    const first = (d: PathDeal): number => at(p.standard, d.path[0]!);
    const there = deals.filter((d) => d.path.includes(reached) && first(d) !== -1 && first(d) < i);
    const group = there.filter((d) => !d.path.includes(stage));
    const other = there.filter((d) => d.path.includes(stage));
    return [{ stage, reached, group, other }];
  });
}

export { closingOut };

/** Send-backs that count: every one but a closing-out move. */
export const countedSendBacks = (d: PathDeal): Array<{ from: string; to: string }> => (closingOut(d) ? sendBacks(d.path).slice(0, -1) : sendBacks(d.path));

/**
 * For each move back from F to T: among deals that reached F, those sent back to
 * T against the rest. A deal whose only such move closed it out is in neither.
 */
export function sendBackComparisons(deals: PathDeal[]): Array<{ from: string; to: string; group: PathDeal[]; other: PathDeal[]; closedOut: PathDeal[] }> {
  const moves = new Map<string, { from: string; to: string }>();
  for (const d of deals) for (const m of countedSendBacks(d)) moves.set(JSON.stringify([m.from, m.to]), m);
  return [...moves].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([key, m]) => {
    const there = deals.filter((d) => d.path.includes(m.from));
    const sent = (d: PathDeal): boolean => countedSendBacks(d).some((x) => JSON.stringify([x.from, x.to]) === key);
    const closedOut = (d: PathDeal): boolean => !sent(d) && sendBacks(d.path).some((x) => JSON.stringify([x.from, x.to]) === key);
    return { ...m, group: there.filter(sent), other: there.filter((d) => !sent(d) && !closedOut(d)), closedOut: there.filter(closedOut) };
  });
}

/** Why a comparison wasn't tested: its period ends before it starts, too many are still open, or too few were won and lost. */
export type Untested = "no period" | "too open" | "too few";

type Tried<T> = T & { c: Comparison | null; untested: Untested | null; period: Period };

/**
 * Every comparison a run makes, each on the deals made in its stage's period
 * that reached the stage, with what stopped it when it wasn't tested; the bar
 * every comparison tried must clear (Bonferroni over them all).
 */
export function comparisonsOf(ctx: Ctx, data: Extract<ReturnType<typeof loadPaths>, { status: "ok" }>["data"], p: Paths) {
  // Every deal, open or closed; each comparison takes those made in its stage's period that reached the stage.
  const { deals, before } = cohortDeals(ctx, data);
  const periods = new Map<string, Period>();
  const periodAt = (stage: string): Period => periods.get(stage) ?? periods.set(stage, periodOf(ctx, deals, stage, "created", { start: data.historyStart, before })).get(stage)!;
  const inPeriod = (stage: string, list: PathDeal[]): PathDeal[] => {
    const ids = new Set(periodAt(stage).cohort.map((d) => d.id));
    return list.filter((d) => ids.has(d.id));
  };
  const why = (c: Comparison | null, group: PathDeal[], other: PathDeal[], period: Period): Untested | null =>
    c ? null : !period.from && period.lagDays !== null ? "no period" : tooOpenToTest(ctx, group, other, period) ? "too open" : "too few";
  // A skip no deal took is no comparison at all.
  const skips: Array<Tried<ReturnType<typeof skipComparisons>[number]>> = skipComparisons(p, deals)
    .filter((s) => s.group.length > 0)
    .map((s) => {
      const period = periodAt(s.reached);
      const [group, other] = [inPeriod(s.reached, s.group), inPeriod(s.reached, s.other)];
      const c = compare(ctx, s.reached, group, other, period);
      return { ...s, group, other, c, untested: why(c, group, other, period), period };
    });
  const backs: Array<Tried<Omit<ReturnType<typeof sendBackComparisons>[number], "closedOut"> & { closedOut: number }>> = sendBackComparisons(deals).map((s) => {
    const period = periodAt(s.from);
    const [group, other] = [inPeriod(s.from, s.group), inPeriod(s.from, s.other)];
    const c = compare(ctx, s.from, group, other, period);
    return { ...s, group, other, closedOut: inPeriod(s.from, s.closedOut).length, c, untested: why(c, group, other, period), period };
  });
  const tried = [...skips, ...backs].filter((s) => s.c !== null).length;
  return { skips, backs, tried, bar: zBar(tried) };
}

// ─── Sentences ───────────────────────────────────────────────────────────────

const within = (c: Comparison): Said =>
  c.compared
    ? said` The gap pointed the same way in ${c.held} of the ${c.compared} teams, record types and other groups we could check it in.`
    : said` We couldn't check this within teams, record types or other groups: these deals couldn't be split into groups big enough to compare on both sides.`;

/**
 * The period and the deals in it, said first, every count adding up: "Of the 92
 * deals created between 31 October 2024 and 17 March 2026 that started before
 * Discovery and reached Proposal, 2 are still open. Of the other 90,". A
 * send-back's comparison leaves out the deals whose last move, back to the same
 * stage on closing, closed them out, and says so.
 */
function whoSaid(c: Comparison, stage: string, startedBefore: string | null, closedOut: { count: number; to: string } | null = null): Said {
  const open = c.open.group + c.open.other;
  const closed = c.group.n + c.other.n;
  const out = closedOut?.count ?? 0;
  const between = said`between ${daySaid(c.from)} and ${daySaid(c.to)}`;
  const who = startedBefore ? said`deals created ${between} that started before ${org(startedBefore)} and reached ${org(stage)}` : said`deals created ${between} that reached ${org(stage)}`;
  if (open === 0 && out === 0) return said`Of the ${closed} ${who},`;
  const left =
    out === 0 ? said``
    : out === 1 ? said`1 whose last move, back to ${org(closedOut!.to)} on closing, closed it out is left out`
    : said`${out} whose last move, back to ${org(closedOut!.to)} on closing, closed them out are left out`;
  const still = open === 0 ? said`` : open === 1 ? said`1 is still open` : said`${open} are still open`;
  return said`Of the ${closed + open + out} ${who}, ${left}${out && open ? said`, and ` : said``}${still}. Of the other ${closed},`;
}

const stagesSaid = (stages: string[]): Said => andList(stages.map((s) => said`${org(s)}`));

/** What kept comparisons from being tested, each reason said once with its stages. Empty when nothing was kept back. */
function untestedSaid(all: Array<{ untested: Untested | null; period: Period }>, others: boolean): Said {
  const stagesWith = (u: Untested): string[] => [...new Set(all.filter((s) => s.untested === u).map((s) => s.period.stage))];
  const late = stagesWith("no period");
  const open = stagesWith("too open");
  const few = all.some((s) => s.untested === "too few");
  const lateSaid = late.length
    ? said` Deals that reached ${stagesSaid(late)} took so long to close that none made since ${daySaid(all.find((s) => s.untested === "no period")!.period.start)} has had time to, so none could be counted yet.`
    : said``;
  const openSaid = open.length ? said` Of the deals made early enough to compare that reached ${stagesSaid(open)}, too many are still open.` : said``;
  const fewSaid = few ? (late.length || open.length || others ? said` The others had too few won and lost deals on both sides.` : said``) : said``;
  return said`${lateSaid}${openSaid}${fewSaid}`;
}

export const NOT_A_CAUSE = said` This shows how often they went together, not that one is the reason for the other.`;
const value = (): ReturnType<typeof notSized> => notSized(said`It compares the paths your deals took, which has no direct figure.`);

/** Lowest with no segment to check it in; high only with segments and both sides at twice the minimum. */
export const confidenceOf = (ctx: Ctx, c: Comparison): "high" | "medium" | "low" =>
  c.compared === 0 ? "low" : Math.min(c.group.n, c.other.n) >= ctx.calibration.minSample * 2 ? "high" : "medium";

const factsOf = (c: Comparison): Record<string, number> => ({
  reached: c.group.n + c.other.n,
  open: c.open.group + c.open.other,
  deals: c.group.n,
  otherDeals: c.other.n,
  winRate: c.group.rate,
  otherWinRate: c.other.rate,
  ...(c.days?.clears ? { wonMedianDays: c.days.group, otherWonMedianDays: c.days.other } : {}),
  ...(c.compared ? { compared: c.compared, held: c.held } : {}),
});

const segmentsChecked = (ctx: Ctx, deals: PathDeal[]): SegmentCheck[] =>
  Object.entries(dimensions(ctx)).map(([dimension, keyOf]) => ({ dimension, segments: new Set(deals.map((d) => keyOf(d.deal))).size }));

export const pathInsights: Rule = {
  id: "pipe.path-insights",
  version: 1,
  domain: "historic_pipeline",
  title: "Deals that skip a stage or are sent back, against deals that reached the same point",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = loadPaths(ctx);
    if (loaded.status !== "ok") return { outcome: "not_measurable", reason: loaded.reason, evidence: loaded.data.evidence };
    const { data, paths: p } = loaded;
    const { skips, backs, tried, bar } = comparisonsOf(ctx, data, p);
    // The closed deals of the window, as every pipeline figure counts them: an open deal has no closed quarter.
    const closedDeals = comparable(ctx, p, data.historyStart);
    const checked = segmentsChecked(ctx, closedDeals);
    const results: RuleResult[] = [];
    // Each insight is for information: a comparison of how deals went, never a fault in the org's set-up. So it is
    // severity info, which keeps it out of the 90-day plan of fixes, and it isn't scored (scoring.ts).
    const common = (c: Comparison) => ({
      outcome: "issue" as const,
      recommendationType: "investigate" as const,
      severity: "info" as const,
      confidence: confidenceOf(ctx, c),
      evidence: data.evidence,
      value: value(),
      effort: { size: "S" as const },
      owner: "Head of sales",
      segmentsChecked: checked,
      facts: factsOf(c),
    });

    for (const { stage, reached, c } of skips) {
      if (!raised(c, bar)) continue;
      const more = c.gap > 0;
      const before = p.standard[p.standard.indexOf(stage) - 1] ?? null;
      const days = c.days?.clears ? said` Won deals that skipped ${org(stage)} took a median of ${c.days.group} days to win, against ${c.days.other} for won deals that went through it.` : said``;
      results.push({
        ...common(c),
        key: `skip:${stage}`,
        title: more ? said`Deals that skipped ${org(stage)} were won more often than deals that went through it` : said`Deals that skipped ${org(stage)} were won less often than deals that went through it`,
        summary: said`${whoSaid(c, reached, before ? stage : null)} the ${c.group.n} that skipped ${org(stage)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the ${c.other.n} that went through it.${days}${within(c)}${NOT_A_CAUSE}`,
        recommendation: more
          ? said`Look at how the deals that skip ${org(stage)} are sold, and what they do instead, before deciding whether the stage is needed for them.`
          : said`Look at why deals move past ${org(stage)} without entering it, and whether the work the stage stands for is being done.`,
        subjects: [{ type: "stage", label: stage }],
      });
    }

    for (const { from, to, c, closedOut } of backs) {
      if (!raised(c, bar)) continue;
      const more = c.gap > 0;
      const days = c.days?.clears ? said` Won deals sent back to ${org(to)} took a median of ${c.days.group} days to win, against ${c.days.other} for the rest.` : said``;
      results.push({
        ...common(c),
        key: `rework:${from}:${to}`,
        title: more ? said`Deals sent back from ${org(from)} to ${org(to)} were won more often than the rest that reached ${org(from)}` : said`Deals sent back from ${org(from)} to ${org(to)} were won less often than the rest that reached ${org(from)}`,
        summary: said`${whoSaid(c, from, null, { count: closedOut, to })} the ${c.group.n} sent back to ${org(to)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the rest.${days}${within(c)}${NOT_A_CAUSE}`,
        recommendation: said`Look at what sends these deals back, and agree what has to be true before a deal leaves ${org(to)}.`,
        subjects: [{ type: "stage", label: from }, { type: "stage", label: to }],
      });
    }

    if (results.length > 0) return results;
    // Nothing raised is never "no difference": what we could and couldn't tell, in the state the run is in.
    const all = [...skips, ...backs];
    const testedCs = all.flatMap((s) => (s.c ? [s.c] : []));
    const summary =
      testedCs.length > 0
        ? said`We couldn't tell whether skipping a stage or being sent back went with winning more or less often. In the ${plural(tried, "comparison")} we could make, between deals that reached the same point, a gap in win rate would have had to be about ${smallestTellable(testedCs, bar)} points or more to tell it from chance, and none was, so a smaller difference may be there unseen.${untestedSaid(all, true)}`
        : all.length === 0
          ? said`No deal skipped a stage of the usual path or was sent back to an earlier stage, so there was nothing to compare.`
          : all.every((s) => s.untested === "too few")
            ? said`No skipped stage or move back had enough won and lost deals on both sides, among deals that reached the same point, to compare.`
            : said`We couldn't compare any skipped stage or move back.${untestedSaid(all, false)}`;
    return {
      outcome: "pass",
      summary,
      evidence: data.evidence,
      segmentsChecked: checked,
      facts: { closed: closedDeals.length, tried },
    };
  },
};
