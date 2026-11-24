import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { joinSaid, org, said, type Said } from "../../../engine/org-words.ts";
import { PROBABILITY_FIELD, stagesExtractor, type StageRow } from "../../../extract/opportunities.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck } from "../../../schemas/index.ts";
import { loadPipeline, type PipelineData } from "../cohort.ts";
import { base, closedInWindow, dimensions, groupBy, measured, notMeasurable, round1, SEGMENT_NOTE, unmappedReason } from "../metrics.ts";
import { closedFromOpen } from "../outcomes.ts";
import { comparable, MIN_EACH_OUTCOME, MIN_SEGMENTS, WHOLE_SHARE, WIN_GAP, zBar } from "../paths/insights.ts";
import { daysToClose, openPathOf, type PathDeal } from "../paths/variants.ts";
import { outside, wilson, type Interval } from "./wilson.ts";

// Roadmap stage 2, item 2 (DECISIONS.md 706): for each open stage, the
// probability set on it in Salesforce beside the share of closed deals that
// reached the stage and were won. A stage probability is the chance a deal at
// that stage closes won, so "reached, then won" is its honest counterpart: the
// deals that were once where a deal at that stage is now, and how they ended.
// It is not the win rate of the deals in the stage today, which are still open.
//
// "Reached" follows the path insights (703): a stage is reached when the deal's
// own stage history shows it there. A stage before the deal's first recorded
// stage isn't reached, since the history may only start late; deals created
// before the org's stage history starts are left out. The same deals the paths
// read (closed in the window, with a recorded open stage), never through
// loadPaths, which stops when every deal took a different path.
//
// A stage is named only when it is honest to: enough deals, enough won and
// lost, a gap of WIN_GAP points or more, the probability outside a Wilson
// interval wide enough for every stage tested at once (the path insights' z bar),
// and, as there, the gap holding inside a record type and most segments that can
// check it. Direction only: never a number the probability should be, never
// what the forecast is off by, never money, never why.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

export const PROBABILITY_METRIC = "historic.stage_probability_vs_won";
export const PROBABILITY_RULE = "pipe.probability-vs-reality";

/** Within a record type, the gap that counts as holding there: the same as overall. */
const TYPE_GAP = WIN_GAP;

/** One open stage's figures. Shares and the interval are percentages. */
export type StageFigures = {
  stage: string;
  /** The probability set on the stage in Salesforce, or null where it has none. */
  probability: number | null;
  reached: number;
  won: number;
  /** Won as a share of every closed deal that reached the stage. */
  share: number;
  /** Enough deals, enough won and enough lost to test, with a probability to test against. */
  testable: boolean;
  interval: Interval | null;
  /** Won share less the probability, in points. Null with no probability. */
  gap: number | null;
  outside: boolean;
  compared: number;
  held: number;
  types: { many: boolean; compared: number; held: number };
  named: boolean;
};

export type Loaded =
  | { status: "ok"; data: Ready; stagesEvidence: Evidence; deals: PathDeal[]; beforeHistory: number; stages: StageFigures[]; tested: number; z: number }
  | { status: "not_measurable"; reason: string; evidence: Evidence[]; data: PipelineData };

/** A deal reached a stage when its own stage history shows it there. Nothing before the first recorded stage is inferred. */
export const reached = (d: PathDeal, stage: string): boolean => d.path.includes(stage);

/** Both outcomes often enough, and enough deals: the path insights' bars for one side. */
const testable = (minSample: number, reachedN: number, won: number): boolean => reachedN >= minSample && won >= MIN_EACH_OUTCOME && reachedN - won >= MIN_EACH_OUTCOME;

/**
 * Rule 4, as the path insights check it: segments in dimensions with two or more
 * values among these deals, each with the calibrated minimum and under
 * WHOLE_SHARE of them, and whether the share sits on the same side of the
 * probability there.
 */
function withinSegments(ctx: Ctx, there: PathDeal[], probability: number, gap: number): { compared: number; held: number } {
  let compared = 0;
  let held = 0;
  for (const keyOf of Object.values(dimensions(ctx))) {
    const groups = groupBy(there, (d) => keyOf(d.deal));
    if (groups.size < 2) continue;
    for (const inside of groups.values()) {
      if (inside.length < ctx.calibration.minSample || inside.length >= WHOLE_SHARE * there.length) continue;
      compared++;
      if (Math.sign(pct(inside.filter((d) => d.won).length, inside.length) - probability) === Math.sign(gap)) held++;
    }
  }
  return { compared, held };
}

/** With more than one record type, the gap must hold, by TYPE_GAP the same way, inside one with the minimum of deals: a process without the stage, or with its own probabilities, isn't blended into it. */
function withinRecordTypes(ctx: Ctx, there: PathDeal[], probability: number, gap: number): StageFigures["types"] {
  const typeOf = (d: PathDeal): string => d.deal.RecordType ?? "No record type";
  const types = groupBy(there, typeOf);
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

/** Named: every bar cleared. */
export function named(s: Omit<StageFigures, "named">): boolean {
  if (!s.testable || s.gap === null || Math.abs(s.gap) < WIN_GAP || !s.outside) return false;
  if (s.types.many && s.types.held === 0) return false;
  // No segment can check it: said at the lowest confidence. One alone, or a gap that turns round in most: not named.
  if (s.compared === 0) return true;
  return s.compared >= MIN_SEGMENTS && s.held * 2 > s.compared;
}

/** The open stages listed: every active open stage in the settings and any other a deal reached, in Salesforce's own order. */
function stagesListed(data: Ready, rows: StageRow[], deals: PathDeal[]): Array<{ stage: string; probability: number | null }> {
  const settings = new Map(rows.map((r) => [r.ApiName, r]));
  const open = (stage: string): boolean => data.stages.categoryOf(stage) === "open";
  const names = new Set([...rows.filter((r) => r.IsActive && open(r.ApiName)).map((r) => r.ApiName), ...deals.flatMap((d) => d.path).filter(open)]);
  // The listing order only: no figure here rests on it, so the order isn't assumed for anything (692).
  const order = (stage: string): number => settings.get(stage)?.SortOrder ?? Number.MAX_SAFE_INTEGER;
  return [...names]
    .sort((a, b) => order(a) - order(b) || (a < b ? -1 : a > b ? 1 : 0))
    .map((stage) => {
      const p = settings.get(stage)?.DefaultProbability;
      return { stage, probability: typeof p === "number" && Number.isFinite(p) ? p : null };
    });
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

function build(ctx: Ctx, data: PipelineData): Loaded {
  const read = ctx.data(stagesExtractor);
  const stagesEvidence = ctx.ev(read.source, { count: read.status === "ok" ? read.data.length : null });
  const evidence = [...data.evidence, stagesEvidence];
  const nm = (reason: string): Loaded => ({ status: "not_measurable", reason, evidence, data });
  if (data.status !== "ok") return nm(data.reason);
  if (read.status !== "ok") return nm(`Salesforce's stage settings couldn't be read, so the probability set on each stage isn't known: ${read.reason}`);
  if (read.missingFields?.includes(PROBABILITY_FIELD)) return nm(said`Salesforce didn't give us the probability set on each stage, so there's nothing to hold how often deals are won against.`);
  // A stage with no meaning could be open or closed, so which stages a deal reached can't be read.
  const gaps = [...new Set([...data.unmapped.finalStages, ...data.unmapped.historyStages])].sort();
  if (gaps.length > 0) return nm(unmappedReason("stages", gaps));
  const { minSample } = ctx.calibration;
  const closedAll = data.deals.filter((d) => (d.category === "won" || d.category === "lost" || d.category === "no_decision") && closedInWindow(d, ctx.window));
  const fromOpen = closedFromOpen(ctx, data);
  if (closedAll.length > 0 && fromOpen.length === 0) {
    return nm(said`Stage history isn't tracked for these deals: none of the ${closedAll.length} deals that closed in the window has a recorded open stage.`);
  }
  const all: PathDeal[] = fromOpen.map((deal) => ({ id: deal.Id, deal, path: openPathOf(deal, data.stages), won: deal.category === "won", days: daysToClose(deal) }));
  const deals = comparable(ctx, { closed: all }, data.historyStart);
  const beforeHistory = all.length - deals.length;
  if (deals.length < minSample) {
    return nm(
      beforeHistory
        ? said`${deals.length} closed deals created since your stage history starts is below the calibrated minimum of ${minSample}.`
        : said`${deals.length} closed deals with stage history is below the calibrated minimum of ${minSample}.`,
    );
  }
  const listed = stagesListed(data, read.data, deals);
  if (!listed.some((s) => s.probability !== null && deals.some((d) => reached(d, s.stage)))) {
    return nm(said`None of the open stages your closed deals reached has a probability set in Salesforce.`);
  }
  const counted = listed.map((s) => {
    const there = deals.filter((d) => reached(d, s.stage));
    const won = there.filter((d) => d.won).length;
    return { ...s, there, won, testable: s.probability !== null && testable(minSample, there.length, won) };
  });
  // Every stage tested raises the bar for all, as every comparison does for the path insights (703).
  const tested = counted.filter((s) => s.testable).length;
  const z = zBar(tested);
  const stages = counted.map(({ there, ...s }): StageFigures => {
    const share = pct(s.won, there.length);
    const interval = s.testable ? wilson(s.won, there.length, z) : null;
    const gap = s.probability === null ? null : round1(share - s.probability);
    const figures = {
      stage: s.stage,
      probability: s.probability,
      reached: there.length,
      won: s.won,
      share,
      testable: s.testable,
      interval: interval ? { low: round1(interval.low * 100), high: round1(interval.high * 100) } : null,
      gap,
      outside: interval !== null && s.probability !== null && outside(s.probability / 100, interval),
      ...(s.testable && gap !== null ? withinSegments(ctx, there, s.probability!, gap) : { compared: 0, held: 0 }),
      types: s.testable && gap !== null ? withinRecordTypes(ctx, there, s.probability!, gap) : { many: false, compared: 0, held: 0 },
    };
    return { ...figures, named: named(figures) };
  });
  return { status: "ok", data, stagesEvidence, deals, beforeHistory, stages, tested, z };
}

// ─── The metric ──────────────────────────────────────────────────────────────

// Short, because the deck's definitions page prints it. How it's worked out is in the assumptions.
const DEFINITION =
  "For each open stage, the probability set on it in Salesforce beside the share of closed deals that reached the stage and were won, which is what a stage probability claims: the chance a deal at that stage closes won. It isn't the win rate of the deals in the stage today. The value is how many stages are set clearly above or below what happens.";

const DEF = { id: PROBABILITY_METRIC, label: "Stage probability against how often deals are won", unit: "count" as const, definition: DEFINITION };

const HOW = [
  "A deal reached a stage when its stage history shows it there; a stage before its first recorded stage isn't counted as reached",
  "The won share is won deals as a share of every closed deal that reached the stage, so no decision counts as not won",
  "Only deals that closed in the window are counted, so this says what happened to closed deals, not what open deals will do",
  "Stages are listed in Salesforce's own stage order, which no figure here depends on",
];

/**
 * Every listed stage's figures, kept in the run's metrics whatever the rule
 * says (store broadly, show selectively): per stage, the probability, the deals
 * that reached it, how many were won, the won share and, where tested, its
 * interval. Enough for a chart of each stage's setting against what happened.
 */
export function probabilityMetrics(ctx: Ctx): Metric[] {
  const loaded = loadProbabilities(ctx);
  if (loaded.status !== "ok") return [notMeasurable({ ...base(ctx, loaded.data, DEF), evidence: loaded.evidence }, loaded.reason)];
  const { data, stages, tested, deals, beforeHistory } = loaded;
  const { minSample } = ctx.calibration;
  const assumptions = [
    ...HOW,
    ...(beforeHistory ? [said`${beforeHistory} closed deals created before your stage history starts are left out, since it can't show every stage they were in`] : []),
    said`A stage is named only when at least ${minSample} closed deals reached it, ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, its probability is ${WIN_GAP} points or more from the won share, and outside the range chance could give it across the ${plural(tested, "stage")} we could test`,
  ];
  const segments: MetricSegment[] = [];
  const stats: Record<string, number> = { closed: deals.length, won: deals.filter((d) => d.won).length, beforeHistory, stages: stages.length, tested, named: stages.filter((s) => s.named).length, z: loaded.z };
  stages.forEach((s, i) => {
    const thin = s.reached < minSample ? SEGMENT_NOTE : null;
    segments.push(
      { dimension: "probability in Salesforce", key: s.stage, n: s.reached, value: s.probability, notMeasurableReason: s.probability === null ? "No probability is set on this stage" : null },
      { dimension: "reached, then won", key: s.stage, n: s.reached, value: s.won, notMeasurableReason: null },
      { dimension: "won share", key: s.stage, n: s.reached, value: thin ? null : s.share, notMeasurableReason: thin },
    );
    if (s.interval) {
      segments.push(
        { dimension: "won share interval low", key: s.stage, n: s.reached, value: s.interval.low, notMeasurableReason: null },
        { dimension: "won share interval high", key: s.stage, n: s.reached, value: s.interval.high, notMeasurableReason: null },
      );
    }
    Object.assign(stats, { [`stage${i + 1}Reached`]: s.reached, [`stage${i + 1}Won`]: s.won, [`stage${i + 1}Tested`]: s.testable ? 1 : 0, [`stage${i + 1}Named`]: s.named ? 1 : 0 });
  });
  const fields = { value: stats.named!, n: deals.length, dateRange: ctx.window, segments, stats };
  return [measured({ ...base(ctx, data, { ...DEF, assumptions }), evidence: [...data.evidence, loaded.stagesEvidence] }, fields)];
}

// ─── The rule ────────────────────────────────────────────────────────────────

const direction = (s: StageFigures): Said => (s.gap! < 0 ? said`set higher than what happens` : said`set lower than what happens`);

const sentenceOf = (s: StageFigures): Said =>
  said`${org(s.stage)} is set to ${s.probability!}% in Salesforce. Of the ${s.reached} closed deals that reached it, ${s.share}% were won, so it is ${direction(s)}.`;

function titleOf(stages: StageFigures[]): Said {
  const higher = stages.every((s) => s.gap! < 0);
  const lower = stages.every((s) => s.gap! > 0);
  if (stages.length === 1) return said`The probability on ${org(stages[0]!.stage)} in Salesforce is ${higher ? said`higher` : said`lower`} than how often its deals are won`;
  const way = higher ? said`higher` : lower ? said`lower` : said`higher or lower`;
  return said`The probabilities on ${plural(stages.length, "stage")} in Salesforce are ${way} than how often their deals are won`;
}

const within = (stages: StageFigures[]): Said => {
  const compared = stages.reduce((n, s) => n + s.compared, 0);
  const held = stages.reduce((n, s) => n + s.held, 0);
  return compared
    ? said` The won share is on the same side of the probability in ${held} of ${compared} teams, record types and other segments that could check it.`
    : said` We couldn't check this within teams or record types: none had enough of these deals.`;
};

const NOT_A_CAUSE = said` This compares each setting with what happened to closed deals; it doesn't say why they differ.`;

/** Lowest with no segment to check any stage named; high only with segments and every stage named at twice the minimum. */
export const confidenceOf = (ctx: Ctx, stages: StageFigures[]): "high" | "medium" | "low" =>
  stages.some((s) => s.compared === 0) ? "low" : stages.every((s) => s.reached >= ctx.calibration.minSample * 2) ? "high" : "medium";

const segmentsChecked = (ctx: Ctx, deals: PathDeal[]): SegmentCheck[] =>
  Object.entries(dimensions(ctx)).map(([dimension, keyOf]) => ({ dimension, segments: new Set(deals.map((d) => keyOf(d.deal))).size }));

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
        summary: tested
          ? said`No open stage's probability in Salesforce is ${WIN_GAP} points or more from how often the closed deals that reached it were won, beyond what chance could make of the ${plural(tested, "stage")} we could test.`
          : said`No open stage with a probability set had enough closed deals, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, to hold against its probability.`,
        evidence,
        segmentsChecked: checked,
        facts: { closed: deals.length, tested },
      };
    }
    const facts: Record<string, number> = { closed: deals.length, tested, named: shown.length };
    shown.forEach((s, i) => Object.assign(facts, { [`stage${i + 1}Probability`]: s.probability!, [`stage${i + 1}Reached`]: s.reached, [`stage${i + 1}WonShare`]: s.share }));
    // For information: a setting set against what happened, never a fault in itself. Severity info keeps it out
    // of the 90-day plan of fixes, and it isn't scored (scoring.ts); stored, and not served (DECISIONS.md 706).
    return {
      outcome: "issue",
      recommendationType: "investigate",
      severity: "info",
      confidence: confidenceOf(ctx, shown),
      evidence,
      value: notSized(said`It compares a setting with what happened to closed deals, which has no direct figure.`),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: checked,
      facts,
      title: titleOf(shown),
      summary: said`${joinSaid(shown.map(sentenceOf), " ")}${within(shown)}${NOT_A_CAUSE}`,
      recommendation: said`Agree with the people who own the sales process what the probability on ${shown.length === 1 ? said`this stage` : said`each of these stages`} is meant to say, and whether anything that relies on it, such as a weighted forecast, should.`,
      subjects: shown.map((s) => ({ type: "stage", label: s.stage })),
    };
  },
};
