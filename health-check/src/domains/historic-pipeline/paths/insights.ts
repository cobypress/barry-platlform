import { notSized, pct, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { org, said, type Said } from "../../../engine/org-words.ts";
import type { SegmentCheck } from "../../../schemas/index.ts";
import { dimensions, groupBy, median, round1 } from "../metrics.ts";
import { DEAD_END_SHARE } from "../process.ts";
import { DRIVER_GAP } from "../drivers.ts";
import { loadPaths, sendBacks, skipsOf, type PathDeal, type Paths } from "./variants.ts";

// The insights read from the variant table (DECISIONS.md 703): each a sentence
// about the customer's deals first, with its figures as the evidence. One is
// raised only when it is honest to: enough deals on each side, a gap big enough
// to matter, and the gap pointing the same way inside most of the teams, record
// types and other segments that can check it (rule 4). Below that, nothing is
// said, and the figures stay in the variant table. Every comparison says how
// often two things went together, never that one caused the other.

type Ctx = Omit<RuleContext, "metrics">;

/** Points of win rate, as a win-rate driver needs (drivers.ts). */
export const WIN_GAP = DRIVER_GAP;
/** A difference in median days to close of at least this share of the usual path's median... */
export const DAYS_GAP_SHARE = 25;
/** ...and at least this many days, so two short medians a day or two apart aren't called a difference. */
export const DAYS_GAP_MIN = 7;

type Side = { n: number; won: number; rate: number; days: number };
const sideOf = (deals: PathDeal[]): Side => ({ n: deals.length, won: deals.filter((d) => d.won).length, rate: pct(deals.filter((d) => d.won).length, deals.length), days: round1(median(deals.map((d) => d.days))) });

/** The gaps between a group and the usual path that clear the bar, from the figures as printed. */
export type Comparison = { group: Side; usual: Side; winGap: number; daysGap: number; winClears: boolean; daysClears: boolean; compared: number; held: number };

/** Whether a days gap is big enough, against the usual path's median. */
export const daysClear = (gap: number, usualDays: number): boolean => Math.abs(gap) >= DAYS_GAP_MIN && Math.abs(gap) * 100 >= DAYS_GAP_SHARE * usualDays;

/**
 * Rule 4 for a comparison, as drivers.ts checks a driver: the same comparison
 * inside each segment where both sides have enough deals, and how many of those
 * point the same way as the whole for each measure that cleared.
 */
function withinSegments(ctx: Ctx, group: PathDeal[], usual: PathDeal[], c: Pick<Comparison, "winGap" | "daysGap" | "winClears" | "daysClears">): { compared: number; held: number } {
  const { minSample } = ctx.calibration;
  let compared = 0;
  let held = 0;
  for (const keyOf of Object.values(dimensions(ctx))) {
    const a = groupBy(group, (d) => keyOf(d.deal));
    const b = groupBy(usual, (d) => keyOf(d.deal));
    for (const [key, inGroup] of a) {
      const inUsual = b.get(key) ?? [];
      if (inGroup.length < minSample || inUsual.length < minSample) continue;
      compared++;
      const [x, y] = [sideOf(inGroup), sideOf(inUsual)];
      const winHolds = !c.winClears || Math.sign(x.rate - y.rate) === Math.sign(c.winGap);
      const daysHolds = !c.daysClears || Math.sign(x.days - y.days) === Math.sign(c.daysGap);
      if (winHolds && daysHolds) held++;
    }
  }
  return { compared, held };
}

/** A group against the usual path, or null when either side is below the calibrated minimum. */
export function compare(ctx: Ctx, group: PathDeal[], usual: PathDeal[]): Comparison | null {
  const { minSample } = ctx.calibration;
  if (group.length < minSample || usual.length < minSample) return null;
  const [g, u] = [sideOf(group), sideOf(usual)];
  const winGap = round1(g.rate - u.rate);
  const daysGap = round1(g.days - u.days);
  const gaps = { winGap, daysGap, winClears: Math.abs(winGap) >= WIN_GAP, daysClears: daysClear(daysGap, u.days) };
  const { compared, held } = gaps.winClears || gaps.daysClears ? withinSegments(ctx, group, usual, gaps) : { compared: 0, held: 0 };
  return { group: g, usual: u, ...gaps, compared, held };
}

/** Raised: a measure cleared, and the gap doesn't turn round in most of the segments that could check it. */
export const raised = (c: Comparison | null): c is Comparison => c !== null && (c.winClears || c.daysClears) && c.held * 2 >= c.compared;

// ─── Sentences ───────────────────────────────────────────────────────────────

const wonWords = (c: Comparison): Said => (c.winGap > 0 ? said`are won more often` : said`are won less often`);
const daysWords = (c: Comparison): Said => (c.daysGap > 0 ? said`take longer to close` : said`close sooner`);
/** What a title says the group does: only the measures that cleared. */
const doWords = (c: Comparison): Said => (c.winClears && c.daysClears ? said`${wonWords(c)} and ${daysWords(c)}` : c.winClears ? wonWords(c) : daysWords(c));

/** Both measures, as evidence, whichever cleared. */
const figures = (c: Comparison, who: Said): Said =>
  said`${who} are won ${c.group.rate}% of the time, against ${c.usual.rate}% on your usual path, and close in a median of ${c.group.days} days from creation, against ${c.usual.days} days.`;

const within = (c: Comparison): Said =>
  c.compared ? said` The gap points the same way in ${c.held} of ${c.compared} segments, such as teams and record types, with enough deals on both sides.` : said` No team, record type or other segment had enough deals on both sides to check it within.`;

// The usual path itself is named once, in the variant table's assumptions: a stage name repeated in every
// sentence would only repeat the org's words, and each is one more place they must be marked.
const basis = (c: Comparison, groupWords: Said, p: Paths): Said =>
  said` Based on ${c.group.n} closed deals that ${groupWords} and ${c.usual.n} that took your usual path, of ${p.closed.length} closed deals.`;

const NOT_A_CAUSE = said` This shows how often they went together, not that one is the reason for the other.`;
const value = (): ReturnType<typeof notSized> => notSized(said`It compares the paths your deals took, which has no direct figure.`);

const confidenceOf = (ctx: Ctx, c: Comparison): "high" | "medium" =>
  c.compared > 0 && Math.min(c.group.n, c.usual.n) >= ctx.calibration.minSample * 2 ? "high" : "medium";

const factsOf = (c: Comparison, closed: number): Record<string, number> => ({
  deals: c.group.n,
  usualDeals: c.usual.n,
  closed,
  winRate: c.group.rate,
  usualWinRate: c.usual.rate,
  winGap: Math.abs(c.winGap),
  medianDays: c.group.days,
  usualMedianDays: c.usual.days,
  daysGap: Math.abs(c.daysGap),
  compared: c.compared,
  held: c.held,
});

// ─── The three insights ──────────────────────────────────────────────────────

/** Deals that moved past each stage of the usual path without entering it, against the usual path. */
export function skipInsights(ctx: Ctx, p: Paths): Array<{ stage: string; c: Comparison }> {
  return p.standard.flatMap((stage) => {
    const c = compare(ctx, p.closed.filter((d) => skipsOf(d.path, p.standard).includes(stage)), p.onStandard);
    return raised(c) ? [{ stage, c }] : [];
  });
}

/** Deals sent back from one stage to another they'd been in, for each such move, against the usual path. */
export function reworkInsights(ctx: Ctx, p: Paths): Array<{ from: string; to: string; c: Comparison }> {
  const moves = new Map<string, { from: string; to: string }>();
  for (const d of p.closed) for (const m of sendBacks(d.path)) moves.set(JSON.stringify([m.from, m.to]), m);
  // A move the usual path itself makes isn't a departure from it.
  const usualMoves = new Set(sendBacks(p.standard).map((m) => JSON.stringify([m.from, m.to])));
  return [...moves]
    .filter(([key]) => !usualMoves.has(key))
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .flatMap(([key, m]) => {
      const c = compare(ctx, p.closed.filter((d) => sendBacks(d.path).some((x) => JSON.stringify([x.from, x.to]) === key)), p.onStandard);
      return raised(c) ? [{ ...m, c }] : [];
    });
}

export type DeadEnd = { stage: string; lost: number; dying: number; share: number; days: number; wonDays: number };

/**
 * Where deals that weren't won were lost: the open stage they were last in.
 * Lost and no decision together, so an inferred lost stage changes nothing
 * (DECISIONS.md 600). Raised when that stage holds at least a third of them, the
 * line pipe.dead-end-stage draws, and there are enough of them to say so.
 */
export function deadEnd(ctx: Ctx, p: Paths): DeadEnd | null {
  const { minSample } = ctx.calibration;
  const lost = p.closed.filter((d) => !d.won && d.path.length > 0);
  if (lost.length < minSample) return null;
  const counts = groupBy(lost, (d) => d.path.at(-1)!);
  const place = (s: string): number => (p.standard.includes(s) ? p.standard.indexOf(s) : Number.MAX_SAFE_INTEGER);
  const [stage, dying] = [...counts].sort(([a, x], [b, y]) => y.length - x.length || place(a) - place(b) || (a < b ? -1 : a > b ? 1 : 0))[0]!;
  const share = pct(dying.length, lost.length);
  if (share < DEAD_END_SHARE || dying.length < minSample) return null;
  const won = p.closed.filter((d) => d.won);
  return { stage, lost: lost.length, dying: dying.length, share, days: round1(median(dying.map((d) => d.days))), wonDays: round1(median(won.map((d) => d.days))) };
}

const segmentsChecked = (ctx: Ctx, p: Paths): SegmentCheck[] =>
  Object.entries(dimensions(ctx)).map(([dimension, keyOf]) => ({ dimension, segments: new Set(p.closed.map((d) => keyOf(d.deal))).size }));

export const pathInsights: Rule = {
  id: "pipe.path-insights",
  version: 1,
  domain: "historic_pipeline",
  title: "How the paths deals take compare with your usual path",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = loadPaths(ctx);
    if (loaded.status !== "ok") return { outcome: "not_measurable", reason: loaded.reason, evidence: loaded.data.evidence };
    const { data, paths: p } = loaded;
    const total = p.closed.length;
    const checked = segmentsChecked(ctx, p);
    const results: RuleResult[] = [];

    for (const { stage, c } of skipInsights(ctx, p)) {
      const better = (c.winClears ? c.winGap > 0 : c.daysGap < 0);
      results.push({
        outcome: "issue",
        key: `skip:${stage}`,
        title: said`Deals that skip ${org(stage)} ${doWords(c)} than on your usual path`,
        summary: said`${figures(c, said`Deals that skip ${org(stage)}`)}${basis(c, said`skipped it`, p)}${within(c)}${NOT_A_CAUSE}`,
        recommendation: better
          ? said`Look at how the deals that skip ${org(stage)} are sold. If the stage adds nothing for them, agree when it can be left out; if it does, find out what they do instead.`
          : said`Look at why deals move past ${org(stage)} without entering it, and whether the work the stage stands for is being done.`,
        recommendationType: "investigate",
        severity: better ? "low" : "medium",
        confidence: confidenceOf(ctx, c),
        evidence: data.evidence,
        value: value(),
        effort: { size: "S" },
        owner: "Head of sales",
        segmentsChecked: checked,
        subjects: [{ type: "stage", label: stage }],
        facts: factsOf(c, total),
      });
    }

    for (const { from, to, c } of reworkInsights(ctx, p)) {
      results.push({
        outcome: "issue",
        key: `rework:${from}:${to}`,
        title: said`Deals sent back from ${org(from)} to ${org(to)} ${doWords(c)}`,
        summary: said`${figures(c, said`Deals sent back from ${org(from)} to ${org(to)}`)}${basis(c, said`went back this way`, p)}${within(c)}${NOT_A_CAUSE}`,
        recommendation: said`Look at what sends these deals back, and agree what has to be true before a deal leaves ${org(to)}.`,
        recommendationType: "investigate",
        severity: c.winGap < 0 || c.daysGap > 0 ? "medium" : "low",
        confidence: confidenceOf(ctx, c),
        evidence: data.evidence,
        value: value(),
        effort: { size: "S" },
        owner: "Head of sales",
        segmentsChecked: checked,
        subjects: [{ type: "stage", label: from }, { type: "stage", label: to }],
        facts: factsOf(c, total),
      });
    }

    const dead = deadEnd(ctx, p);
    if (dead) {
      results.push({
        outcome: "issue",
        key: `dead-end:${dead.stage}`,
        title: said`More deals are lost after ${org(dead.stage)} than after any other stage`,
        summary: said`${dead.dying} of the ${dead.lost} lost and no decision deals (${dead.share}%) were last open in ${org(dead.stage)}, after a median of ${dead.days} days from creation, against ${dead.wonDays} days for a won deal to close. Based on ${dead.lost} of ${total} closed deals.`,
        recommendation: said`Look at what deals in ${org(dead.stage)} are waiting for, and whether the ones that won't close are found and closed sooner.`,
        recommendationType: "investigate",
        severity: "medium",
        confidence: dead.lost >= ctx.calibration.minSample * 2 ? "high" : "medium",
        evidence: data.evidence,
        value: value(),
        effort: { size: "S" },
        owner: "Head of sales",
        segmentsChecked: checked,
        subjects: [{ type: "stage", label: dead.stage }],
        facts: { lost: dead.lost, dying: dead.dying, share: dead.share, medianDays: dead.days, wonMedianDays: dead.wonDays, closed: total },
      });
    }

    if (results.length > 0) return results;
    return {
      outcome: "pass",
      summary: said`No path your ${total} closed deals took differs from your usual path by ${WIN_GAP} points of win rate, or by ${DAYS_GAP_SHARE}% and ${DAYS_GAP_MIN} days of its median time to close, with ${ctx.calibration.minSample} deals or more on both sides, and no one stage holds a third of lost deals.`,
      evidence: data.evidence,
      segmentsChecked: checked,
      facts: { closed: total, paths: p.all.length },
    };
  },
};
