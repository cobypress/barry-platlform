import { pct, type RuleContext } from "../../../engine/rule.ts";
import { org, said, type Said } from "../../../engine/org-words.ts";
import type { Metric, MetricSegment } from "../../../schemas/index.ts";
import { loadPipeline, type Deal, type PipelineData, type StageMap } from "../cohort.ts";
import { base, closedInWindow, measured, median, notMeasurable, round1, unmappedReason } from "../metrics.ts";
import { closedFromOpen } from "../outcomes.ts";

// Roadmap stage 2, item 1 (DECISIONS.md 703): the paths closed deals really
// took through the open stages, grouped into variants, each against the usual
// path on win rate and time. Read only from the opportunities and stage history
// the pipeline already holds, and never from the stage order in the settings:
// the usual path is what won deals did, so it can be held against that order.
//
// Counts, rates and days only. No amount is read here.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

const DAY = 86_400_000;
const DOMAIN_METRIC = "historic.path_variants";
export const PATH_VARIANTS = DOMAIN_METRIC;
/**
 * The variants listed by name: the five most common, as the paths to won and
 * to lost list theirs (process.ts), with every other path in one row. The usual
 * path is always listed, as a sixth row when it isn't among the five.
 */
export const TOP_VARIANTS = 5;
export const OTHER_PATHS = "All other paths";
/** How a path is written in a segment key. Stage names are the org's: only the joins are ours. */
export const PATH_JOIN = " > ";

/** One closed deal as the paths read it. */
export type PathDeal = { id: string; deal: Deal; path: string[]; won: boolean; days: number };

export type Variant = {
  path: string[];
  deals: PathDeal[];
  won: number;
  share: number;
  winRate: number;
  medianDays: number;
  /** Stages of the usual path this path moved past without entering. */
  skips: string[];
  rework: boolean;
  standard: boolean;
};

export type Paths = {
  closed: PathDeal[];
  standard: string[];
  /** Closed deals on exactly the usual path. */
  onStandard: PathDeal[];
  /** Every distinct path, most deals first. */
  all: Variant[];
  /** The rows listed: the top variants, and the usual path if it isn't among them. */
  shown: Variant[];
  /** Every closed deal on a path not listed, or null when there are none. */
  other: { deals: PathDeal[]; won: number; share: number; winRate: number; medianDays: number; rework: number; paths: number } | null;
};

/**
 * The open stages a deal was in, in the order it was in them, with a run of rows
 * in the same stage (a close date or amount change) counted once. A return to a
 * stage it was in before is kept: that's rework. Closed stages aren't part of a
 * path, since the outcome is what a path is compared on.
 */
export function openPathOf(deal: Deal, stages: Pick<StageMap, "categoryOf">): string[] {
  const path: string[] = [];
  for (const row of deal.history) {
    if (stages.categoryOf(row.StageName) !== "open") continue;
    if (path.at(-1) !== row.StageName) path.push(row.StageName);
  }
  return path;
}

/** Days from the deal's creation to its close, never below zero. */
export const daysToClose = (deal: Deal): number => Math.max(0, (deal.closedAt.getTime() - new Date(deal.CreatedDate).getTime()) / DAY);

/**
 * Each move back: into a stage the deal first entered before the one it moved
 * from. Earlier means earlier in the deal's own path, never in the settings'
 * order. Going on again after a move back (A > B > A > B) is a move forward.
 */
export function sendBacks(path: string[]): Array<{ from: string; to: string }> {
  const moves: Array<{ from: string; to: string }> = [];
  for (let i = 1; i < path.length; i++) if (path.indexOf(path[i]!) < path.indexOf(path[i - 1]!)) moves.push({ from: path[i - 1]!, to: path[i]! });
  return moves;
}

export const hasRework = (path: string[]): boolean => sendBacks(path).length > 0;

/**
 * Stages of the usual path this path moved past without entering: a later stage
 * of the usual path was entered and this one never was. A deal that closed
 * before it reached a stage didn't skip it, so a deal lost early isn't counted
 * as skipping everything after.
 */
export function skipsOf(path: string[], standard: string[]): string[] {
  const furthest = Math.max(-1, ...path.map((s) => standard.indexOf(s)));
  return standard.filter((stage, i) => i < furthest && !path.includes(stage));
}

const keyOf = (path: string[]): string => JSON.stringify(path);
const plainCompare = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0);

/**
 * The usual path: the one won deals took most often. A tie goes to the path
 * more closed deals took, then to the first by its stage names, so the same
 * deals always give the same path.
 */
export function standardPathOf(closed: PathDeal[]): string[] | null {
  const won = new Map<string, number>();
  const all = new Map<string, number>();
  for (const d of closed) {
    const key = keyOf(d.path);
    all.set(key, (all.get(key) ?? 0) + 1);
    if (d.won) won.set(key, (won.get(key) ?? 0) + 1);
  }
  const ranked = [...won].sort(([a, x], [b, y]) => y - x || all.get(b)! - all.get(a)! || plainCompare(a, b));
  return ranked.length ? (JSON.parse(ranked[0]![0]) as string[]) : null;
}

const winRateOf = (deals: PathDeal[]): number => pct(deals.filter((d) => d.won).length, deals.length);
const medianDaysOf = (deals: PathDeal[]): number => round1(median(deals.map((d) => d.days)));

/** Every distinct path with its figures, the listed rows and the rest, against a usual path already chosen. */
export function variantsOf(closed: PathDeal[], standard: string[], top = TOP_VARIANTS): Paths {
  const groups = new Map<string, PathDeal[]>();
  for (const d of closed) {
    const key = keyOf(d.path);
    const group = groups.get(key);
    if (group) group.push(d);
    else groups.set(key, [d]);
  }
  const standardKey = keyOf(standard);
  const all: Variant[] = [...groups]
    .sort(([a, x], [b, y]) => y.length - x.length || plainCompare(a, b))
    .map(([key, deals]) => {
      const path = deals[0]!.path;
      return {
        path,
        deals,
        won: deals.filter((d) => d.won).length,
        share: pct(deals.length, closed.length),
        winRate: winRateOf(deals),
        medianDays: medianDaysOf(deals),
        skips: skipsOf(path, standard),
        rework: hasRework(path),
        standard: key === standardKey,
      };
    });
  const shown = all.slice(0, top);
  if (!shown.some((v) => v.standard)) {
    const usual = all.find((v) => v.standard);
    if (usual) shown.push(usual);
  }
  const listed = new Set(shown);
  const rest = all.filter((v) => !listed.has(v));
  const deals = rest.flatMap((v) => v.deals);
  const other = deals.length
    ? { deals, won: deals.filter((d) => d.won).length, share: pct(deals.length, closed.length), winRate: winRateOf(deals), medianDays: medianDaysOf(deals), rework: deals.filter((d) => hasRework(d.path)).length, paths: rest.length }
    : null;
  return { closed, standard, onStandard: groups.get(standardKey) ?? [], all, shown, other };
}

/** A path as a sentence writes it, each stage the org's words. */
export function pathSaid(path: string[]): Said {
  let text = said`${org(path[0]!)}`;
  for (const stage of path.slice(1)) text = said`${text} > ${org(stage)}`;
  return text;
}

export type Loaded =
  | { status: "ok"; data: Ready; paths: Paths }
  | { status: "not_measurable"; reason: string; data: PipelineData };

const built = new WeakMap<RuleContext["data"], Loaded>();

/**
 * The paths, or not measurable with the reason. Built once per analysis, as the
 * pipeline is; loadPipeline is still called each time, so each metric and rule
 * that reads it carries the pipeline's own assumptions.
 */
export function loadPaths(ctx: Ctx): Loaded {
  const data = loadPipeline(ctx);
  let loaded = built.get(ctx.data);
  if (!loaded) {
    loaded = buildPaths(ctx, data);
    built.set(ctx.data, loaded);
  }
  return loaded.status === "ok" && data.status === "ok" ? { ...loaded, data } : loaded;
}

function buildPaths(ctx: Ctx, data: PipelineData): Loaded {
  if (data.status !== "ok") return { status: "not_measurable", reason: data.reason, data };
  // A stage with no meaning could be open or closed, so no path through it can be read.
  const gaps = [...new Set([...data.unmapped.finalStages, ...data.unmapped.historyStages])].sort();
  if (gaps.length > 0) return { status: "not_measurable", reason: unmappedReason("stages", gaps), data };
  const { minSample } = ctx.calibration;
  const closedAll = data.deals.filter((d) => (d.category === "won" || d.category === "lost" || d.category === "no_decision") && closedInWindow(d, ctx.window));
  const fromOpen = closedFromOpen(ctx, data);
  // Deals closed, and not one with a stage from while it was open: the stage history isn't kept for them.
  if (closedAll.length > 0 && fromOpen.length === 0) {
    return { status: "not_measurable", reason: said`Stage history isn't tracked for these deals: none of the ${closedAll.length} deals that closed in the window has a recorded open stage.`, data };
  }
  if (fromOpen.length < minSample) return { status: "not_measurable", reason: said`${fromOpen.length} closed deals with stage history is below the calibrated minimum of ${minSample}.`, data };
  const closed: PathDeal[] = fromOpen.map((deal) => ({ id: deal.Id, deal, path: openPathOf(deal, data.stages), won: deal.category === "won", days: daysToClose(deal) }));
  const distinct = new Set(closed.map((d) => keyOf(d.path))).size;
  if (distinct === closed.length) return { status: "not_measurable", reason: said`Each of the ${closed.length} closed deals took a different path through your stages, so no path is common enough to compare with.`, data };
  const won = closed.filter((d) => d.won);
  if (won.length < minSample) return { status: "not_measurable", reason: said`${won.length} won deals with stage history is below the calibrated minimum of ${minSample}.`, data };
  if (new Set(won.map((d) => keyOf(d.path))).size === won.length) {
    return { status: "not_measurable", reason: said`Each of the ${won.length} won deals took a different path through your stages, so there's no usual path to compare with.`, data };
  }
  return { status: "ok", data, paths: variantsOf(closed, standardPathOf(closed)!) };
}

// ─── The metric ──────────────────────────────────────────────────────────────

// Short, because the deck's definitions page prints it.
const DEFINITION =
  "Closed deals grouped by the open stages each was in, in order, from its stage history: a run of rows in the same stage counts once, and a return to an earlier stage is kept. Your usual path is the one won deals took most often, not the stage order in your settings. For the five most common paths, and all others together: deals, share of closed deals, win rate (won as a share of every closed deal on the path, so no decision counts as not won), the median days from creation to close (the median, not the mean), stages of the usual path it moved past without entering, and whether it went back to a stage. The value is the share of closed deals on the usual path.";

const DEF = { id: DOMAIN_METRIC, label: "The paths your deals take", unit: "percent" as const, definition: DEFINITION };

/**
 * The full variant table, kept in the run's metrics whatever the insights say
 * (the owner's data policy: store broadly, show selectively). One segment per
 * figure per row, keyed by the path; what a segment can't hold is in the stats,
 * by the row's place in the list (path1 is the most common).
 */
export function pathMetrics(ctx: Ctx): Metric[] {
  const loaded = loadPaths(ctx);
  if (loaded.status !== "ok") return [notMeasurable(base(ctx, loaded.data, DEF), loaded.reason)];
  const { data, paths: p } = loaded;
  const assumptions = [
    said`Your usual path, the one won deals took most often: ${pathSaid(p.standard)}`,
    said`A stage counts as skipped only where a deal went on to a later stage of the usual path without entering it`,
  ];
  const segments: MetricSegment[] = [];
  const stats: Record<string, number> = {
    closed: p.closed.length,
    won: p.closed.filter((d) => d.won).length,
    paths: p.all.length,
    listed: p.shown.length,
    standardDeals: p.onStandard.length,
    standardWon: p.onStandard.filter((d) => d.won).length,
    standardStages: p.standard.length,
  };
  p.shown.forEach((v, i) => {
    const key = v.path.join(PATH_JOIN);
    const n = v.deals.length;
    segments.push(
      { dimension: "path", key, n, value: v.share, notMeasurableReason: null },
      { dimension: "path win rate", key, n, value: v.winRate, notMeasurableReason: null },
      { dimension: "path median days", key, n, value: v.medianDays, notMeasurableReason: null },
      ...v.skips.map((stage) => ({ dimension: `path ${i + 1} skipped stage`, key: stage, n, value: n, notMeasurableReason: null })),
    );
    Object.assign(stats, { [`path${i + 1}Deals`]: n, [`path${i + 1}Won`]: v.won, [`path${i + 1}Skips`]: v.skips.length, [`path${i + 1}Rework`]: v.rework ? 1 : 0, [`path${i + 1}Standard`]: v.standard ? 1 : 0 });
  });
  if (p.other) {
    const n = p.other.deals.length;
    segments.push(
      { dimension: "path", key: OTHER_PATHS, n, value: p.other.share, notMeasurableReason: null },
      { dimension: "path win rate", key: OTHER_PATHS, n, value: p.other.winRate, notMeasurableReason: null },
      { dimension: "path median days", key: OTHER_PATHS, n, value: p.other.medianDays, notMeasurableReason: null },
    );
    Object.assign(stats, { otherDeals: n, otherWon: p.other.won, otherPaths: p.other.paths, otherRework: p.other.rework });
  }
  return [measured(base(ctx, data, { ...DEF, assumptions }), { value: pct(p.onStandard.length, p.closed.length), n: p.closed.length, dateRange: ctx.window, segments, stats })];
}
