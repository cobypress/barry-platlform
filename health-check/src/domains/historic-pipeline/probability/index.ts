import { joinSaid, org, said, type Said } from "../../../engine/org-words.ts";
import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { andList } from "../../../engine/said-forms.ts";
import { PROBABILITY_FIELD, stagesExtractor, type StageRow, type StagesRead } from "../../../extract/opportunities.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck } from "../../../schemas/index.ts";
import { loadPipeline, type PipelineData } from "../cohort.ts";
import { base, closedInWindow, dimensions, groupBy, measured, notMeasurable, round1, SEGMENT_NOTE, unmappedReason } from "../metrics.ts";
import { closedFromOpen } from "../outcomes.ts";
import { MIN_EACH_OUTCOME, WHOLE_SHARE, WIN_GAP, Z_FLOOR, zBar } from "../paths/insights.ts";
import { cohortDeals, dayPlus, daySaid, LAG_QUANTILE, OPEN_SHARE, periodOf, reached, type CohortDeal } from "../reached.ts";
import { outside, wilson, type Interval } from "./wilson.ts";

// Roadmap stage 2, item 2 (DECISIONS.md 706): for each open stage, the
// probability set on it in Salesforce beside the share of the deals that
// reached the stage that were won. A stage probability is the chance a deal at
// that stage closes won, so "reached, then won" is its honest counterpart: the
// deals that were once where a deal at that stage is now, and how they ended.
// It is not the win rate of the deals in the stage today, which are still open.
//
// Who is counted: the deals that REACHED the stage in a period, open or closed,
// never the deals that closed in one. Chosen by when they closed, a sample
// over-counts whichever outcome closes faster whenever deals arrive faster or
// slower than before. Each stage's period runs from the start of the window to
// the run's date less the stage's maturity lag, the time within which 90% of
// the closed deals that reached it had closed, and a stage is named only when
// almost all of the deals that reached it in that period have closed
// (OPEN_SHARE). The few still open could each go either way, so the share is
// worked out both ways, the open as all lost and as all won, and a stage is
// named only when both clear every bar on the same side (review round 2).
//
// A setting can be wrong for last year and right now. So the period is cut in
// PARTS by when deals reached the stage, and each part must show the gap on its
// own: the same side, and the probability outside the part's interval. Two
// halves weren't enough: the lag ends the period months before the run, so a
// setting put right in the last year still sat mostly inside the second half
// (named in 43 of 100 simulated orgs of 1,000 deals; in thirds, 3). What is said
// is about the period, in the past tense.
//
// "Reached" follows the path insights (703): a stage is reached when the deal's
// own history shows it there, so a stage before its first recorded stage isn't
// reached, and deals made before the org's stage history starts are left out.
//
// Direction only: never a number the probability should be, never what the
// forecast is off by, never money, never why.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

export const PROBABILITY_METRIC = "historic.stage_probability_vs_won";
export const PROBABILITY_RULE = "pipe.probability-vs-reality";

// Who is counted is shared with the path insights (reached.ts, DECISIONS.md 712).
export { cohortDeals, daySaid, LAG_QUANTILE, OPEN_SHARE, reached, type CohortDeal };
/**
 * The lowest z the interval is drawn at, whatever the number of stages tested.
 * With one stage tested, a floor of 2 named a true probability in up to 6.4% of
 * orgs of some size (the worst, 30 deals at a true 55%, worked out exactly); at
 * 2.15 the worst is 4.4%. The path insights' floor, which is now the same
 * (DECISIONS.md 712), so the two can't drift apart.
 */
export const Z_MIN = Z_FLOOR;
/** Within a record type, the gap that counts as holding there: the same as overall. */
const TYPE_GAP = WIN_GAP;

/**
 * The ways a stage's deals are split to check a gap (rule 4), each said in our
 * words. Record type has its own check, and the quarter a deal closed isn't a
 * way to split deals chosen by when they reached a stage. Functions, so each
 * phrase is made inside the analysis that says it, where the org-word ledger
 * can vouch for it.
 */
const WAYS: Record<string, () => Said> = { team: () => said`team`, rep: () => said`rep`, "lead source": () => said`lead source`, type: () => said`type` };


/** How one way of splitting a stage's deals went: segments compared, and how many sit on the same side of the setting. */
export type Way = { dimension: string; compared: number; held: number; holds: boolean };

/** The share with the open deals counted one way: all lost (low) or all won (high), over every deal in the period. */
export type Bound = { won: number; n: number; share: number; gap: number; interval: Interval | null; outside: boolean };

/** The parts the period is cut into, by when deals reached the stage; each must show the gap alone. */
export const PARTS = 3;

/** One part of the period, by when deals reached the stage: its closed deals, how many were won, and whether it shows the gap alone. */
export type Part = { from: string; to: string; closed: number; won: number; share: number; enough: boolean; holds: boolean };

/** One open stage's figures. Shares and intervals are percentages; days are local calendar days. */
export type StageFigures = {
  stage: string;
  /** The probability set on the stage in Salesforce, or null where it has none. */
  probability: number | null;
  /** Days within which LAG_QUANTILE of the closed deals that reached it had closed; null with none closed. */
  lagDays: number | null;
  /** The period deals are counted in: reached the stage from `from` to `to`. Null when no day fits. */
  from: string | null;
  to: string | null;
  /** Deals that reached it in the period, open or closed. */
  cohort: number;
  open: number;
  /** The closed ones, and how many were won. */
  reached: number;
  won: number;
  /** Won as a share of the closed deals that reached it in the period: the figure said. */
  share: number;
  tooOpen: boolean;
  /** Why the stage can't be tested, in plain words, or null when it can. */
  untested: string | null;
  testable: boolean;
  /** The plain share's interval, for the chart. The bars are on the bounds. */
  interval: Interval | null;
  /** Won share less the probability, in points. Null with no probability. */
  gap: number | null;
  low: Bound | null;
  high: Bound | null;
  parts: Part[] | null;
  ways: Way[];
  types: { many: boolean; compared: number; held: number };
  named: boolean;
  /** Deals made before the stage history starts that reached it in the period, left out (712). */
  leftOut: number;
};

export type Loaded =
  | { status: "ok"; data: Ready; stagesEvidence: Evidence; deals: CohortDeal[]; beforeHistory: number; stages: StageFigures[]; tested: number; z: number }
  | { status: "not_measurable"; reason: string; evidence: Evidence[]; data: PipelineData };


/** Both outcomes often enough, and enough deals: the path insights' bars for one side. */
const enough = (minSample: number, closed: number, won: number): boolean => closed >= minSample && won >= MIN_EACH_OUTCOME && closed - won >= MIN_EACH_OUTCOME;

/**
 * Rule 4 for one stage, a way of splitting at a time: a way counts when it has
 * two or more segments with the calibrated minimum and under WHOLE_SHARE of
 * the deals (a segment holding nearly all of them is the whole again), and it
 * holds when most of those segments sit on the same side of the setting.
 */
export function waysOf(ctx: Ctx, closed: CohortDeal[], probability: number, gap: number): Way[] {
  const dims = dimensions(ctx);
  const ways: Way[] = [];
  for (const dimension of Object.keys(WAYS)) {
    const keyOf = dims[dimension];
    if (!keyOf) continue;
    let compared = 0;
    let held = 0;
    for (const inside of groupBy(closed, (d) => keyOf(d.deal)).values()) {
      if (inside.length < ctx.calibration.minSample || inside.length >= WHOLE_SHARE * closed.length) continue;
      compared++;
      if (Math.sign(pct(inside.filter((d) => d.won).length, inside.length) - probability) === Math.sign(gap)) held++;
    }
    if (compared >= 2) ways.push({ dimension, compared, held, holds: held * 2 > compared });
  }
  return ways;
}

/** With more than one record type, the gap must hold, by TYPE_GAP the same way, inside one with the minimum of deals: a process without the stage, or with its own probabilities, isn't blended into it. */
function withinRecordTypes(ctx: Ctx, closed: CohortDeal[], probability: number, gap: number): StageFigures["types"] {
  const types = groupBy(closed, (d) => d.deal.RecordType ?? "No record type");
  if (types.size < 2) return { many: false, compared: 0, held: 0 };
  let compared = 0;
  let held = 0;
  for (const inside of types.values()) {
    if (inside.length < ctx.calibration.minSample) continue;
    compared++;
    const g = pct(inside.filter((d) => d.won).length, inside.length) - probability;
    if (Math.sign(g) === Math.sign(gap) && Math.abs(g) >= TYPE_GAP) held++;
  }
  return { many: true, compared, held };
}

/** The share one way, its gap from the setting, and whether the setting lies outside its interval. */
function boundOf(won: number, n: number, probability: number, z: number): Bound {
  const share = pct(won, n);
  const interval = wilson(won, n, z);
  return { won, n, share, gap: round1(share - probability), interval: interval ? { low: round1(interval.low * 100), high: round1(interval.high * 100) } : null, outside: interval !== null && outside(probability / 100, interval) };
}

/** A bound clears the gap and the interval, on the side given. */
const clears = (b: Bound, side: number): boolean => Math.sign(b.gap) === side && Math.abs(b.gap) >= WIN_GAP && b.outside;

/** Named: every bar cleared. */
export function named(s: Omit<StageFigures, "named">): boolean {
  if (!s.testable || s.tooOpen || s.gap === null || !s.low || !s.high || !s.parts) return false;
  // The open deals counted as all lost and as all won: both must clear, on the same side.
  const side = Math.sign(s.low.gap);
  if (side === 0 || !clears(s.low, side) || !clears(s.high, side)) return false;
  // The gap in each part of the period alone, so a setting wrong only earlier isn't said of the whole.
  if (!s.parts.every((h) => h.enough && h.holds)) return false;
  if (s.types.many && s.types.held === 0) return false;
  // No way to split them: said at the lowest confidence. Otherwise it must hold in most of the ways there are.
  if (s.ways.length === 0) return true;
  return s.ways.filter((w) => w.holds).length * 2 > s.ways.length;
}

/** The open stages listed: every active open stage in the settings and any other a deal reached, in Salesforce's own order. */
function stagesListed(data: Ready, rows: StageRow[], deals: CohortDeal[]): Array<{ stage: string; probability: number | null }> {
  const settings = new Map(rows.map((r) => [r.ApiName, r]));
  const open = (stage: string): boolean => data.stages.categoryOf(stage) === "open";
  const names = new Set([...rows.filter((r) => r.IsActive && open(r.ApiName)).map((r) => r.ApiName), ...deals.flatMap((d) => d.path).filter(open)]);
  // The listing order only: no figure here rests on it, so the order is assumed for nothing (692).
  const order = (stage: string): number => settings.get(stage)?.SortOrder ?? Number.MAX_SAFE_INTEGER;
  return [...names]
    .sort((a, b) => order(a) - order(b) || (a < b ? -1 : a > b ? 1 : 0))
    .map((stage) => {
      const p = settings.get(stage)?.DefaultProbability;
      return { stage, probability: typeof p === "number" && Number.isFinite(p) ? p : null };
    });
}

/** Why the probabilities weren't read, by how Salesforce refused them. */
function refusedReason(read: StagesRead): Said {
  return read.probabilityRefused === "no_access"
    ? said`Salesforce refused the connected user the probability set on each stage, so there's nothing to hold how often deals are won against.`
    : said`Salesforce didn't give us the probability set on each stage: it answered that the field isn't there for the connected user, so there's nothing to hold how often deals are won against.`;
}

const built = new WeakMap<RuleContext["data"], Loaded>();

/**
 * The figures, or not measurable with the reason. Built once per analysis;
 * loadPipeline is still called each time, so the metric and the rule each carry
 * the pipeline's own assumptions (inferred stages, blended record types).
 */
export function loadProbabilities(ctx: Ctx): Loaded {
  const data = loadPipeline(ctx);
  let loaded = built.get(ctx.data);
  if (!loaded) {
    loaded = build(ctx, data);
    built.set(ctx.data, loaded);
  }
  return loaded.status === "ok" && data.status === "ok" ? { ...loaded, data } : loaded;
}


/** One stage's deals in its period, and its figures but for the bars that count every stage tested. */
function stageOf(ctx: Ctx, s: { stage: string; probability: number | null }, deals: CohortDeal[], history: { start: string | null; before: CohortDeal[] }) {
  const { lagDays, from, to, cohort, closed, open, tooOpen, dayOf, leftOut } = periodOf(ctx, deals, s.stage, "reached", history);
  const won = closed.filter((d) => d.won).length;
  const { minSample } = ctx.calibration;
  const untested =
    s.probability === null ? "No probability is set on this stage"
    : lagDays === null ? "No closed deal has reached this stage, so how long its deals take to close isn't known"
    : !from ? "No deal could have reached this stage early enough in the window to have closed by now"
    : tooOpen ? `Too many of the deals that reached ${s.stage} in this period are still open to say how often they are won`
    : !enough(minSample, closed.length, won) ? `${SEGMENT_NOTE}, or fewer than ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost`
    : null;
  // The parts of the period, cut at whole days: each from the day after the last one ends.
  const span = from && to ? Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86_400_000) : 0;
  const bounds = from && to ? Array.from({ length: PARTS }, (_, k) => ({ from: k === 0 ? from : dayPlus(from, Math.floor((span * k) / PARTS) + 1), to: k === PARTS - 1 ? to : dayPlus(from, Math.floor((span * (k + 1)) / PARTS)) })) : null;
  const partDeals = bounds ? bounds.map((b) => ({ ...b, deals: closed.filter((d) => dayOf(d) >= b.from && dayOf(d) <= b.to) })) : null;
  return { ...s, lagDays, from, to, cohort: cohort.length, open, closed, partDeals, leftOut, reached: closed.length, won, share: pct(won, closed.length), tooOpen, untested, testable: untested === null };
}

/** A part of the period, and whether it shows the gap alone: on the same side, with the setting outside its own interval. */
function partOf(ctx: Ctx, deals: CohortDeal[], from: string, to: string, probability: number, side: number, z: number): Part {
  const won = deals.filter((d) => d.won).length;
  const share = pct(won, deals.length);
  const interval = wilson(won, deals.length, z);
  const isEnough = deals.length >= ctx.calibration.minSample;
  return { from, to, closed: deals.length, won, share, enough: isEnough, holds: isEnough && Math.sign(share - probability) === side && interval !== null && outside(probability / 100, interval) };
}

function build(ctx: Ctx, data: PipelineData): Loaded {
  const read = ctx.data(stagesExtractor);
  const stagesEvidence = ctx.ev(read.source, { count: read.status === "ok" ? read.data.length : null });
  const evidence = [...data.evidence, stagesEvidence];
  const nm = (reason: string): Loaded => ({ status: "not_measurable", reason, evidence, data });
  if (data.status !== "ok") return nm(data.reason);
  if (read.status !== "ok") return nm(`Salesforce's stage settings couldn't be read, so the probability set on each stage isn't known: ${read.reason}`);
  if (read.missingFields?.includes(PROBABILITY_FIELD)) return nm(refusedReason(read));
  // A stage with no meaning could be open or closed, so which stages a deal reached can't be read.
  const gaps = [...new Set([...data.unmapped.finalStages, ...data.unmapped.historyStages])].sort();
  if (gaps.length > 0) return nm(unmappedReason("stages", gaps));
  const { minSample } = ctx.calibration;
  const closedAll = data.deals.filter((d) => (d.category === "won" || d.category === "lost" || d.category === "no_decision") && closedInWindow(d, ctx.window));
  const fromOpen = closedFromOpen(ctx, data);
  if (closedAll.length > 0 && fromOpen.length === 0) {
    return nm(said`Stage history isn't tracked for these deals: none of the ${closedAll.length} deals that closed in the window has a recorded open stage.`);
  }
  // Every deal with a recorded open stage, open or closed, whenever it closed: each stage picks its own by when they reached it.
  const { deals, beforeHistory, before } = cohortDeals(ctx, data);
  const closedInIt = fromOpen.filter((d) => deals.some((x) => x.id === d.Id)).length;
  if (closedInIt < minSample) {
    return nm(
      beforeHistory
        ? said`${closedInIt} closed deals created since your stage history starts is below the calibrated minimum of ${minSample}.`
        : said`${closedInIt} closed deals with stage history is below the calibrated minimum of ${minSample}.`,
    );
  }
  const listed = stagesListed(data, read.data, deals);
  if (!listed.some((s) => s.probability !== null && deals.some((d) => reached(d, s.stage)))) {
    return nm(said`None of the open stages your closed deals reached has a probability set in Salesforce.`);
  }
  const counted = listed.map((s) => stageOf(ctx, s, deals, { start: data.historyStart, before }));
  const tested = counted.filter((s) => s.testable).length;
  // Nothing to test: not measurable, with what stood in the way.
  if (tested === 0) {
    const open = counted.filter((s) => s.probability !== null && s.tooOpen).map((s) => s.stage);
    return nm(
      open.length
        ? said`No open stage could be tested: too many of the deals that reached ${andList(open.map((s) => said`${org(s)}`))} in their period are still open to say how often they are won, and no other stage had enough deals that reached it and have since closed, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost.`
        : said`No open stage could be tested: none with a probability set had enough deals that reached it and have since closed, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost.`,
    );
  }
  // Every stage tested raises the bar for all, as every comparison does for the path insights (703), never below Z_MIN.
  const z = Math.max(Z_MIN, zBar(tested));
  const stages = counted.map(({ closed, partDeals, ...s }): StageFigures => {
    const p = s.probability;
    const interval = s.testable ? wilson(s.won, s.reached, z) : null;
    const gap = p === null || s.reached === 0 ? null : round1(s.share - p);
    const checked = s.testable && gap !== null && p !== null;
    const low = checked ? boundOf(s.won, s.cohort, p, z) : null;
    const high = checked ? boundOf(s.won + s.open, s.cohort, p, z) : null;
    const side = low ? Math.sign(low.gap) : 0;
    const parts = checked && partDeals ? partDeals.map((b) => partOf(ctx, b.deals, b.from, b.to, p, side, z)) : null;
    const figures = {
      ...s,
      interval: interval ? { low: round1(interval.low * 100), high: round1(interval.high * 100) } : null,
      gap,
      low,
      high,
      parts,
      ways: checked ? waysOf(ctx, closed, p, gap) : [],
      types: checked ? withinRecordTypes(ctx, closed, p, gap) : { many: false, compared: 0, held: 0 },
    };
    return { ...figures, named: named(figures) };
  });
  return { status: "ok", data, stagesEvidence, deals, beforeHistory, stages, tested, z };
}

// ─── The metric ──────────────────────────────────────────────────────────────

// Short, because the deck's definitions page prints it. How it's worked out is in the assumptions.
const DEFINITION =
  "For each open stage, the probability set on it in Salesforce beside the share of the deals that reached the stage that were won, which is what a stage probability claims: the chance a deal at that stage closes won. It isn't the win rate of the deals in the stage today. The value is how many stages were set clearly above or below what happened.";

const DEF = { id: PROBABILITY_METRIC, label: "Stage probability against how often deals are won", unit: "count" as const, definition: DEFINITION };

const HOW = [
  "A deal reached a stage when its stage history shows it there; a stage before its first recorded stage isn't counted as reached",
  "Each stage counts the deals, open or closed, that reached it from the start of the window to a cut-off: the run's date less the time within which 90% of the closed deals that reached the stage had closed. Deals are chosen by when they reached the stage, not when they closed, which would favour whichever outcome closes faster whenever the number of deals is changing",
  "A stage isn't named while more than 5% of the deals that reached it in its period are still open, and only when the gap holds with those still open counted as all lost and as all won",
  "A stage is named only when the gap also holds in each third of its period on its own, so a setting that was wrong only earlier isn't said of the whole",
  "The won share said is won deals as a share of the closed deals that reached the stage in its period, so no decision counts as not won",
  "Stages are listed in Salesforce's own stage order, which no figure here depends on",
];

/**
 * Every listed stage's figures, kept in the run's metrics whatever the rule
 * says (store broadly, show selectively): per stage, the probability, the
 * period and its lag, the deals that reached it then, how many are still open,
 * how many were won, the won share both ways and, where tested, its interval
 * and each third of its period. Enough for a chart of each stage's setting against what happened.
 */
export function probabilityMetrics(ctx: Ctx): Metric[] {
  const loaded = loadProbabilities(ctx);
  if (loaded.status !== "ok") return [notMeasurable({ ...base(ctx, loaded.data, DEF), evidence: loaded.evidence }, loaded.reason)];
  const { data, stages, tested, deals, beforeHistory } = loaded;
  const { minSample } = ctx.calibration;
  const assumptions = [
    ...HOW,
    ...stages.filter((s) => s.lagDays !== null && s.to !== null).map((s) => said`${org(s.stage)}: 90% of the closed deals that reached it had closed within ${s.lagDays!} days, so its period ends on ${daySaid(s.to!)}`),
    ...(beforeHistory ? [said`${beforeHistory} deals created before your stage history starts are left out, since it can't show every stage they were in`] : []),
    said`A stage is named only when at least ${minSample} of those deals have closed, ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, its probability is ${WIN_GAP} points or more from the won share, and outside the range chance could give it across the ${plural(tested, "stage")} we could test`,
  ];
  const segments: MetricSegment[] = [];
  const stats: Record<string, number> = { deals: deals.length, beforeHistory, stages: stages.length, tested, named: stages.filter((s) => s.named).length, z: loaded.z };
  stages.forEach((s, i) => {
    const shareReason = s.reached < minSample && !s.tooOpen ? SEGMENT_NOTE : s.tooOpen ? s.untested : null;
    segments.push(
      { dimension: "probability in Salesforce", key: s.stage, n: s.cohort, value: s.probability, notMeasurableReason: s.probability === null ? "No probability is set on this stage" : null },
      { dimension: "reached in the period", key: s.stage, n: s.cohort, value: s.cohort, notMeasurableReason: s.lagDays === null || s.from === null ? s.untested : null },
      { dimension: "still open", key: s.stage, n: s.cohort, value: s.open, notMeasurableReason: null },
      { dimension: "reached, then won", key: s.stage, n: s.reached, value: s.won, notMeasurableReason: null },
      { dimension: "won share", key: s.stage, n: s.reached, value: shareReason ? null : s.share, notMeasurableReason: shareReason },
    );
    if (s.interval) {
      segments.push(
        { dimension: "won share interval low", key: s.stage, n: s.reached, value: s.interval.low, notMeasurableReason: null },
        { dimension: "won share interval high", key: s.stage, n: s.reached, value: s.interval.high, notMeasurableReason: null },
      );
    }
    if (s.low && s.high) {
      segments.push(
        { dimension: "won share with the open as lost", key: s.stage, n: s.cohort, value: s.low.share, notMeasurableReason: null },
        { dimension: "won share with the open as won", key: s.stage, n: s.cohort, value: s.high.share, notMeasurableReason: null },
      );
    }
    s.parts?.forEach((h, k) => segments.push({ dimension: `won share, part ${k + 1} of ${PARTS} of the period`, key: s.stage, n: h.closed, value: h.enough ? h.share : null, notMeasurableReason: h.enough ? null : SEGMENT_NOTE }));
    Object.assign(stats, {
      [`stage${i + 1}LagDays`]: s.lagDays ?? -1,
      [`stage${i + 1}Cohort`]: s.cohort,
      [`stage${i + 1}Open`]: s.open,
      [`stage${i + 1}Reached`]: s.reached,
      [`stage${i + 1}Won`]: s.won,
      [`stage${i + 1}Tested`]: s.testable ? 1 : 0,
      [`stage${i + 1}Named`]: s.named ? 1 : 0,
    });
  });
  const fields = { value: stats.named!, n: deals.length, dateRange: ctx.window, segments, stats };
  return [measured({ ...base(ctx, data, { ...DEF, assumptions }), evidence: [...data.evidence, loaded.stagesEvidence] }, fields)];
}

// ─── The rule ────────────────────────────────────────────────────────────────

const direction = (s: StageFigures): Said => (s.gap! < 0 ? said`set higher than what happened` : said`set lower than what happened`);

/**
 * Each way of splitting, said once by name, and only what is true of it:
 * "every group" when every group in every way sits on the setting's side; else
 * "most groups", in each way or in how many of them.
 */
function waysSaid(s: StageFigures): Said {
  if (s.ways.length === 0) return said` We couldn't check this within groups: these deals couldn't be split by team, rep, lead source or type into groups big enough to compare.`;
  const names = andList(s.ways.map((w) => WAYS[w.dimension]!()));
  const holding = s.ways.filter((w) => w.holds).length;
  const every = s.ways.every((w) => w.held === w.compared);
  const ways = s.ways.length === 1 ? said`the one way we could split these deals (by ${names})` : said`${s.ways.length} ways we could split these deals (by ${names})`;
  if (every) return said` It was on the same side of the setting in every group we could compare, in ${s.ways.length === 1 ? said`` : said`each of the `}${ways}.`;
  if (holding === s.ways.length) return said` It was on the same side of the setting in most groups, in ${s.ways.length === 1 ? said`` : said`each of the `}${ways}.`;
  return said` It was on the same side of the setting in most groups in ${holding} of the ${ways}.`;
}

const sentenceOf = (s: StageFigures): Said => {
  // Deals made before the stage history starts are left out; when any reached it in the period, the count says whose it is (712).
  const period = s.leftOut ? said`made since your stage history starts that reached it between ${daySaid(s.from!)} and ${daySaid(s.to!)}` : said`that reached it between ${daySaid(s.from!)} and ${daySaid(s.to!)}`;
  const outcome =
    s.open === 0 ? said`Of the ${s.reached} deals ${period}, ${s.share}% were won`
    : s.open === 1 ? said`Of the ${s.cohort} deals ${period}, 1 is still open and ${s.share}% of the other ${s.reached} were won`
    : said`Of the ${s.cohort} deals ${period}, ${s.open} are still open and ${s.share}% of the other ${s.reached} were won`;
  return said`${org(s.stage)} is set to ${s.probability!}% in Salesforce. ${outcome}, so in that period it was ${direction(s)}.${waysSaid(s)}`;
};

function titleOf(stages: StageFigures[]): Said {
  const higher = stages.every((s) => s.gap! < 0);
  const lower = stages.every((s) => s.gap! > 0);
  if (stages.length === 1) return said`The probability on ${org(stages[0]!.stage)} in Salesforce was ${higher ? said`higher` : said`lower`} than how often its deals were won`;
  const way = higher ? said`higher` : lower ? said`lower` : said`higher or lower`;
  return said`The probabilities on ${plural(stages.length, "stage")} in Salesforce were ${way} than how often their deals were won`;
}

const NOT_A_CAUSE = said` This compares each setting with what happened to the deals that reached the stage; it doesn't say why they differ.`;

/** Lowest with no way to split a stage named; high only with ways for each and every stage named at twice the minimum. */
export const confidenceOf = (ctx: Ctx, stages: StageFigures[]): "high" | "medium" | "low" =>
  stages.some((s) => s.ways.length === 0) ? "low" : stages.every((s) => s.reached >= ctx.calibration.minSample * 2) ? "high" : "medium";

const segmentsChecked = (ctx: Ctx, deals: CohortDeal[]): SegmentCheck[] =>
  Object.entries(dimensions(ctx))
    .filter(([dimension]) => dimension in WAYS || dimension === "record type")
    .map(([dimension, keyOf]) => ({ dimension, segments: new Set(deals.map((d) => keyOf(d.deal))).size }));

/** What kept stages with a probability from being tested because too many of their deals are still open, said beside a pass. */
function tooOpenSaid(stages: StageFigures[]): Said {
  const open = stages.filter((s) => s.probability !== null && s.tooOpen).map((s) => s.stage);
  if (open.length === 0) return said``;
  return said` For ${andList(open.map((s) => said`${org(s)}`))}, too many of the deals that reached ${open.length === 1 ? said`it` : said`them`} in ${open.length === 1 ? said`its` : said`their`} period are still open to say how often they are won.`;
}

export const probabilityVsReality: Rule = {
  id: PROBABILITY_RULE,
  version: 1,
  domain: "historic_pipeline",
  title: "Stage probability in Salesforce against how often those deals are won",
  evaluate(ctx): RuleResult {
    const loaded = loadProbabilities(ctx);
    if (loaded.status !== "ok") return { outcome: "not_measurable", reason: loaded.reason, evidence: loaded.evidence };
    const { data, stages, tested, deals } = loaded;
    const evidence = [...data.evidence, loaded.stagesEvidence];
    const checked = segmentsChecked(ctx, deals);
    const shown = stages.filter((s) => s.named);
    if (shown.length === 0) {
      return {
        outcome: "pass",
        summary: said`None of the ${plural(tested, "stage")} we could test had a probability in Salesforce ${WIN_GAP} points or more from how often the deals that reached it were won, beyond what chance could make of them.${tooOpenSaid(stages)}`,
        evidence,
        segmentsChecked: checked,
        facts: { deals: deals.length, tested },
      };
    }
    const facts: Record<string, number> = { deals: deals.length, tested, named: shown.length };
    shown.forEach((s, i) => Object.assign(facts, { [`stage${i + 1}Probability`]: s.probability!, [`stage${i + 1}Reached`]: s.reached, [`stage${i + 1}Open`]: s.open, [`stage${i + 1}WonShare`]: s.share, [`stage${i + 1}LagDays`]: s.lagDays! }));
    // For information: a setting set against what happened, never a fault in itself. Severity info keeps it out
    // of the 90-day plan of fixes, and it isn't scored (scoring.ts); stored, and not served (DECISIONS.md 706).
    return {
      outcome: "issue",
      recommendationType: "investigate",
      severity: "info",
      confidence: confidenceOf(ctx, shown),
      evidence,
      value: notSized(said`It compares a setting with what happened to the deals that reached the stage, which has no direct figure.`),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: checked,
      facts,
      title: titleOf(shown),
      summary: said`${joinSaid(shown.map(sentenceOf), " ")}${NOT_A_CAUSE}`,
      recommendation: said`Agree with the people who own the sales process what the probability on ${shown.length === 1 ? said`this stage` : said`each of these stages`} is meant to say, and whether anything that relies on it, such as a weighted forecast, should.`,
      subjects: shown.map((s) => ({ type: "stage", label: s.stage })),
    };
  },
};
