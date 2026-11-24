import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { org, said, type Said } from "../../../engine/org-words.ts";
import { andList } from "../../../engine/said-forms.ts";
import type { SegmentCheck } from "../../../schemas/index.ts";
import { dimensions, groupBy, median, round1 } from "../metrics.ts";
import { DRIVER_GAP } from "../drivers.ts";
import { cohortDeals, comparable, daySaid, periodOf, type CohortDeal, type Period } from "../reached.ts";
import { loadPaths, sendBacks, type PathDeal, type Paths } from "./variants.ts";

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

/** The gap with the open deals counted one way, over every deal in the period: points, and its z. */
export type Bound = { gap: number; z: number };

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

/** The gap and its z over every deal on each side, the group's open counted as won and the other's as lost, or the other way round. */
function boundOf(g: Side, o: Side, open: Comparison["open"], groupOpenWon: boolean): Bound {
  const a = { n: g.n + open.group, won: g.won + (groupOpenWon ? open.group : 0) };
  const b = { n: o.n + open.other, won: o.won + (groupOpenWon ? 0 : open.other) };
  return { gap: round1(pct(a.won, a.n) - pct(b.won, b.n)), z: Math.round(zOf(a, b) * 100) / 100 };
}

const isOpen = (d: PathDeal): boolean => (d as Partial<CohortDeal>).open === true;

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
    low: boundOf(g, o, open, false),
    high: boundOf(g, o, open, true),
    days,
    ...withinSegments(ctx, group, other, gap),
    types: withinRecordTypes(ctx, group, other, gap),
  };
}

/** Too many still open in the period to test, though its closed deals alone would have been enough: said beside a pass. */
export function tooOpenToTest(ctx: Ctx, groupAll: PathDeal[], otherAll: PathDeal[], period: Pick<Period, "tooOpen">): boolean {
  return period.tooOpen && testable(ctx, sideOf(groupAll.filter((d) => !isOpen(d))), sideOf(otherAll.filter((d) => !isOpen(d))));
}

/** Raised: the win-rate gap clears every bar. Days never raise one. */
export function raised(c: Comparison | null, bar: number): c is Comparison {
  if (!c) return false;
  // The open deals counted both ways: each must clear the gap and the bar, on the same side.
  const side = Math.sign(c.low.gap);
  if (side === 0 || Math.sign(c.high.gap) !== side) return false;
  if ([c.low, c.high].some((b) => Math.abs(b.gap) < WIN_GAP || Math.abs(b.z) < bar)) return false;
  if (c.types.many && c.types.held === 0) return false;
  // No segment can check it: said at the lowest confidence. One alone, or gaps that turn round in most: not raised.
  if (c.compared === 0) return true;
  return c.compared >= MIN_SEGMENTS && c.held * 2 > c.compared;
}

// ─── Who is compared ─────────────────────────────────────────────────────────

const at = (standard: string[], stage: string): number => standard.indexOf(stage);

/**
 * For each stage X of the usual path, with S the next: among deals that entered
 * S, those that never entered X though their first recorded stage came before it
 * (history starting late isn't a skip), against those that went through X.
 */
export function skipComparisons(p: Paths, deals: PathDeal[]): Array<{ stage: string; reached: string; group: PathDeal[]; other: PathDeal[] }> {
  return p.standard.slice(0, -1).flatMap((stage, i) => {
    const reached = p.standard[i + 1]!;
    const there = deals.filter((d) => d.path.includes(reached));
    const first = (d: PathDeal): number => at(p.standard, d.path[0]!);
    const group = there.filter((d) => !d.path.includes(stage) && first(d) !== -1 && first(d) < i);
    const other = there.filter((d) => d.path.includes(stage));
    return [{ stage, reached, group, other }];
  });
}

/** A send-back that is a lost deal's last move is how it was closed out, not rework. */
export const closingOut = (d: PathDeal): boolean => !d.won && !isOpen(d) && d.path.length >= 2 && d.path.indexOf(d.path.at(-1)!) < d.path.indexOf(d.path.at(-2)!);

/** Send-backs that count: every one but a lost deal's closing-out move. */
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

// ─── Sentences ───────────────────────────────────────────────────────────────

const within = (c: Comparison): Said =>
  c.compared
    ? said` The gap pointed the same way in ${c.held} of the ${c.compared} teams, record types and other groups we could check it in.`
    : said` We couldn't check this within teams, record types or other groups: these deals couldn't be split into groups big enough to compare on both sides.`;

/**
 * The period and the deals in it, said first, every count adding up: "Of the 92
 * deals created between 31 October 2024 and 17 March 2026 that reached Proposal,
 * 2 are still open. Of the other 90,". A send-back's comparison leaves out the lost
 * deals whose last move, back to the same stage, closed them out, and says so.
 */
function whoSaid(c: Comparison, stage: string, closedOut: { count: number; to: string } | null = null): Said {
  const open = c.open.group + c.open.other;
  const closed = c.group.n + c.other.n;
  const out = closedOut?.count ?? 0;
  const between = said`between ${daySaid(c.from)} and ${daySaid(c.to)}`;
  const who = said`deals created ${between} that reached ${org(stage)}`;
  if (open === 0 && out === 0) return said`Of the ${closed} ${who},`;
  const left = out === 0 ? said`` : out === 1 ? said`1 lost deal whose last move back to ${org(closedOut!.to)} closed it out is left out` : said`${out} lost deals whose last move back to ${org(closedOut!.to)} closed them out are left out`;
  const still = open === 0 ? said`` : open === 1 ? said`1 is still open` : said`${open} are still open`;
  return said`Of the ${closed + open + out} ${who}, ${left}${out && open ? said`, and ` : said``}${still}. Of the other ${closed},`;
}

/** The stages whose deals couldn't be compared because too many of those made in the period are still open. */
const tooOpenSaid = (stages: string[]): Said =>
  stages.length === 0
    ? said``
    : said` Of the deals made early enough to compare that reached ${andList(stages.map((s) => said`${org(s)}`))}, too many are still open.`;

const NOT_A_CAUSE = said` This shows how often they went together, not that one is the reason for the other.`;
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
    // Every deal, open or closed; each comparison takes those made in its stage's period that reached the stage.
    const { deals } = cohortDeals(ctx, data);
    const periods = new Map<string, Period>();
    const periodAt = (stage: string): Period => periods.get(stage) ?? periods.set(stage, periodOf(ctx, deals, stage, "created")).get(stage)!;
    const inPeriod = (stage: string, list: PathDeal[]): PathDeal[] => {
      const ids = new Set(periodAt(stage).cohort.map((d) => d.id));
      return list.filter((d) => ids.has(d.id));
    };
    const skips = skipComparisons(p, deals).map((s) => {
      const [group, other] = [inPeriod(s.reached, s.group), inPeriod(s.reached, s.other)];
      return { ...s, group, other, c: compare(ctx, s.reached, group, other, periodAt(s.reached)), tooOpen: tooOpenToTest(ctx, group, other, periodAt(s.reached)) };
    });
    const backs = sendBackComparisons(deals).map((s) => {
      const [group, other] = [inPeriod(s.from, s.group), inPeriod(s.from, s.other)];
      return { ...s, group, other, closedOut: inPeriod(s.from, s.closedOut).length, c: compare(ctx, s.from, group, other, periodAt(s.from)), tooOpen: tooOpenToTest(ctx, group, other, periodAt(s.from)) };
    });
    // Every comparison with both sides big enough to test is one tried, and each raises the bar for all (Bonferroni).
    const tried = [...skips, ...backs].filter((s) => s.c !== null).length;
    const tooOpen = [...new Set([...skips, ...backs].filter((s) => s.tooOpen).map((s) => ("reached" in s ? s.reached : s.from)))];
    const bar = zBar(tried);
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
      const days = c.days?.clears ? said` Won deals that skipped ${org(stage)} took a median of ${c.days.group} days to win, against ${c.days.other} for won deals that went through it.` : said``;
      results.push({
        ...common(c),
        key: `skip:${stage}`,
        title: more ? said`Deals that skipped ${org(stage)} were won more often than deals that went through it` : said`Deals that skipped ${org(stage)} were won less often than deals that went through it`,
        summary: said`${whoSaid(c, reached)} the ${c.group.n} that skipped ${org(stage)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the ${c.other.n} that went through it.${days}${within(c)}${NOT_A_CAUSE}`,
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
        summary: said`${whoSaid(c, from, { count: closedOut, to })} the ${c.group.n} sent back to ${org(to)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the rest.${days}${within(c)}${NOT_A_CAUSE}`,
        recommendation: said`Look at what sends these deals back, and agree what has to be true before a deal leaves ${org(to)}.`,
        subjects: [{ type: "stage", label: from }, { type: "stage", label: to }],
      });
    }

    if (results.length > 0) return results;
    return {
      outcome: "pass",
      summary: tried
        ? said`No skipped stage or move back separated deals that reached the same point by ${WIN_GAP} points of win rate or more, beyond what chance could make of the ${plural(tried, "comparison")} we could test.${tooOpenSaid(tooOpen)}`
        : tooOpen.length
          ? said`No skipped stage or move back could be compared.${tooOpenSaid(tooOpen)} No other had enough won and lost deals on both sides, among deals that reached the same point.`
          : said`No skipped stage or move back had enough won and lost deals on both sides, among deals that reached the same point, to compare.`,
      evidence: data.evidence,
      segmentsChecked: checked,
      facts: { closed: closedDeals.length, tried },
    };
  },
};
