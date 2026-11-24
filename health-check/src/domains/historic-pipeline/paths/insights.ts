import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { org, said, type Said } from "../../../engine/org-words.ts";
import type { SegmentCheck } from "../../../schemas/index.ts";
import { localDay, zoneOf } from "../../time-zone.ts";
import { dimensions, groupBy, median, round1 } from "../metrics.ts";
import { DRIVER_GAP } from "../drivers.ts";
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

type Ctx = Omit<RuleContext, "metrics">;

/** Points of win rate, as a win-rate driver needs (drivers.ts). */
export const WIN_GAP = DRIVER_GAP;
/** Won deals and lost deals each side needs, so a rate isn't one or two deals either way. */
export const MIN_EACH_OUTCOME = 5;
/** The two-proportion z a gap needs before the count of comparisons raises it (Bonferroni). */
export const Z_FLOOR = 2;
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

/** A comparison: the group against the other side, both having reached the same stage. */
export type Comparison = {
  reached: string;
  group: Side;
  other: Side;
  gap: number;
  z: number;
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

/** A group against the other side, or null when either side is too small to test. Days only on won deals. */
export function compare(ctx: Ctx, reached: string, group: PathDeal[], other: PathDeal[]): Comparison | null {
  const [g, o] = [sideOf(group), sideOf(other)];
  if (!testable(ctx, g, o)) return null;
  const gap = round1(g.rate - o.rate);
  const wonG = group.filter((d) => d.won);
  const wonO = other.filter((d) => d.won);
  const days = wonG.length >= ctx.calibration.minSample && wonO.length >= ctx.calibration.minSample ? (() => {
    const [dg, dother] = [medianDays(wonG), medianDays(wonO)];
    const diff = Math.abs(round1(dg - dother));
    return { group: dg, other: dother, clears: diff >= DAYS_GAP_MIN && diff * 100 >= DAYS_GAP_SHARE * dother };
  })() : null;
  return { reached, group: g, other: o, gap, z: Math.round(zOf(g, o) * 100) / 100, days, ...withinSegments(ctx, group, other, gap), types: withinRecordTypes(ctx, group, other, gap) };
}

/** Raised: the win-rate gap clears every bar. Days never raise one. */
export function raised(c: Comparison | null, bar: number): c is Comparison {
  if (!c || Math.abs(c.gap) < WIN_GAP || Math.abs(c.z) < bar) return false;
  if (c.types.many && c.types.held === 0) return false;
  // No segment can check it: said at the lowest confidence. One alone, or gaps that turn round in most: not raised.
  if (c.compared === 0) return true;
  return c.compared >= MIN_SEGMENTS && c.held * 2 > c.compared;
}

// ─── Who is compared ─────────────────────────────────────────────────────────

/** Deals created before the org's stage history starts can't show every stage they were in, so they aren't compared. */
export function comparable(ctx: Ctx, p: Pick<Paths, "closed">, historyStart: string | null): PathDeal[] {
  if (!historyStart) return p.closed;
  const zone = zoneOf(ctx);
  return p.closed.filter((d) => localDay(new Date(d.deal.CreatedDate), zone) >= historyStart);
}

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
export const closingOut = (d: PathDeal): boolean => !d.won && d.path.length >= 2 && d.path.indexOf(d.path.at(-1)!) < d.path.indexOf(d.path.at(-2)!);

/** Send-backs that count: every one but a lost deal's closing-out move. */
export const countedSendBacks = (d: PathDeal): Array<{ from: string; to: string }> => (closingOut(d) ? sendBacks(d.path).slice(0, -1) : sendBacks(d.path));

/**
 * For each move back from F to T: among deals that reached F, those sent back to
 * T against the rest. A deal whose only such move closed it out is in neither.
 */
export function sendBackComparisons(deals: PathDeal[]): Array<{ from: string; to: string; group: PathDeal[]; other: PathDeal[] }> {
  const moves = new Map<string, { from: string; to: string }>();
  for (const d of deals) for (const m of countedSendBacks(d)) moves.set(JSON.stringify([m.from, m.to]), m);
  return [...moves].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([key, m]) => {
    const there = deals.filter((d) => d.path.includes(m.from));
    const sent = (d: PathDeal): boolean => countedSendBacks(d).some((x) => JSON.stringify([x.from, x.to]) === key);
    const closedOut = (d: PathDeal): boolean => !sent(d) && sendBacks(d.path).some((x) => JSON.stringify([x.from, x.to]) === key);
    return { ...m, group: there.filter(sent), other: there.filter((d) => !sent(d) && !closedOut(d)) };
  });
}

// ─── Sentences ───────────────────────────────────────────────────────────────

const within = (c: Comparison): Said =>
  c.compared
    ? said` The gap points the same way in ${c.held} of ${c.compared} teams, record types and other segments that could check it.`
    : said` We couldn't check this within teams or record types: none had enough of these deals on both sides.`;

const NOT_A_CAUSE = said` This shows how often they went together, not that one is the reason for the other.`;
const value = (): ReturnType<typeof notSized> => notSized(said`It compares the paths your deals took, which has no direct figure.`);

/** Lowest with no segment to check it in; high only with segments and both sides at twice the minimum. */
export const confidenceOf = (ctx: Ctx, c: Comparison): "high" | "medium" | "low" =>
  c.compared === 0 ? "low" : Math.min(c.group.n, c.other.n) >= ctx.calibration.minSample * 2 ? "high" : "medium";

const factsOf = (c: Comparison): Record<string, number> => ({
  reached: c.group.n + c.other.n,
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
    const deals = comparable(ctx, p, data.historyStart);
    const skips = skipComparisons(p, deals).map((s) => ({ ...s, c: compare(ctx, s.reached, s.group, s.other) }));
    const backs = sendBackComparisons(deals).map((s) => ({ ...s, c: compare(ctx, s.from, s.group, s.other) }));
    // Every comparison with both sides big enough to test is one tried, and each raises the bar for all (Bonferroni).
    const tried = [...skips, ...backs].filter((s) => s.c !== null).length;
    const bar = zBar(tried);
    const checked = segmentsChecked(ctx, deals);
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
        title: more ? said`Deals that skip ${org(stage)} are won more often than deals that went through it` : said`Deals that skip ${org(stage)} are won less often than deals that went through it`,
        summary: said`Of the ${c.group.n + c.other.n} closed deals that reached ${org(reached)}, the ${c.group.n} that skipped ${org(stage)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the ${c.other.n} that went through it.${days}${within(c)}${NOT_A_CAUSE}`,
        recommendation: more
          ? said`Look at how the deals that skip ${org(stage)} are sold, and what they do instead, before deciding whether the stage is needed for them.`
          : said`Look at why deals move past ${org(stage)} without entering it, and whether the work the stage stands for is being done.`,
        subjects: [{ type: "stage", label: stage }],
      });
    }

    for (const { from, to, c } of backs) {
      if (!raised(c, bar)) continue;
      const more = c.gap > 0;
      const days = c.days?.clears ? said` Won deals sent back to ${org(to)} took a median of ${c.days.group} days to win, against ${c.days.other} for the rest.` : said``;
      results.push({
        ...common(c),
        key: `rework:${from}:${to}`,
        title: more ? said`Deals sent back from ${org(from)} to ${org(to)} are won more often than the rest that reached ${org(from)}` : said`Deals sent back from ${org(from)} to ${org(to)} are won less often than the rest that reached ${org(from)}`,
        summary: said`Of the ${c.group.n + c.other.n} closed deals that reached ${org(from)}, the ${c.group.n} sent back to ${org(to)} were won ${c.group.rate}% of the time, against ${c.other.rate}% of the rest.${days}${within(c)}${NOT_A_CAUSE}`,
        recommendation: said`Look at what sends these deals back, and agree what has to be true before a deal leaves ${org(to)}.`,
        subjects: [{ type: "stage", label: from }, { type: "stage", label: to }],
      });
    }

    if (results.length > 0) return results;
    return {
      outcome: "pass",
      summary: tried
        ? said`No skipped stage or move back separates deals that reached the same point by ${WIN_GAP} points of win rate or more, beyond what chance could make of the ${plural(tried, "comparison")} we could test.`
        : said`No skipped stage or move back had enough won and lost deals on both sides, among deals that reached the same point, to compare.`,
      evidence: data.evidence,
      segmentsChecked: checked,
      facts: { closed: deals.length, tried },
    };
  },
};
