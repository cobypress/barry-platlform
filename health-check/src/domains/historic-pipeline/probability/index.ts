import { eng, joinSaid, org, said, type Said } from "../../../engine/org-words.ts";
import { notSized, pct, plural, type Rule, type RuleContext, type RuleResult } from "../../../engine/rule.ts";
import { andList } from "../../../engine/said-forms.ts";
import { PROBABILITY_FIELD, stagesExtractor, type StageRow, type StagesRead } from "../../../extract/opportunities.ts";
import type { Evidence, Metric, MetricSegment, SegmentCheck } from "../../../schemas/index.ts";
import { localDay, zoneOf } from "../../time-zone.ts";
import { loadPipeline, type Deal, type PipelineData } from "../cohort.ts";
import { base, closedInWindow, dimensions, groupBy, hasOpenStage, measured, notMeasurable, quantile, round1, SEGMENT_NOTE, unmappedReason } from "../metrics.ts";
import { closedFromOpen } from "../outcomes.ts";
import { comparable, MIN_EACH_OUTCOME, WHOLE_SHARE, WIN_GAP, zBar } from "../paths/insights.ts";
import { daysToClose, openPathOf, type PathDeal } from "../paths/variants.ts";
import { outside, wilson, type Interval } from "./wilson.ts";

// Roadmap stage 2, item 2 (DECISIONS.md 706): for each open stage, the
// probability set on it in Salesforce beside the share of the deals that
// reached the stage that were won. A stage probability is the chance a deal at
// that stage closes won, so "reached, then won" is its honest counterpart: the
// deals that were once where a deal at that stage is now, and how they ended.
// It is not the win rate of the deals in the stage today, which are still open.
//
// Who is counted (fix round 1): the deals that REACHED the stage in a period,
// open or closed, never the deals that closed in one. Chosen by when they
// closed, a sample over-counts whichever outcome closes faster whenever deals
// arrive faster or slower than before: in a shrinking org whose wins take longer
// than its losses, a true probability was named in a third of simulated orgs. So
// each stage's period runs from the start of the window to the run's date less
// the stage's maturity lag, the time within which 90% of the closed deals that
// reached it had closed, and a stage is named only when almost all of the deals
// that reached it in that period have closed (OPEN_SHARE).
//
// "Reached" follows the path insights (703): a stage is reached when the deal's
// own history shows it there, so a stage before its first recorded stage isn't
// reached, and deals made before the org's stage history starts are left out.
//
// A stage is named only when it is honest to: enough closed deals, enough won
// and lost, a gap of WIN_GAP points or more, the probability outside a Wilson
// interval wide enough for every stage tested at once (the path insights' z
// bar), the gap holding inside a record type, and in most of the ways the deals
// can be split. Direction only: never a number the probability should be, never
// what the forecast is off by, never money, never why.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

export const PROBABILITY_METRIC = "historic.stage_probability_vs_won";
export const PROBABILITY_RULE = "pipe.probability-vs-reality";

const DAY = 86_400_000;
/** The share of closed deals that reached a stage whose time to close sets the stage's maturity lag. */
export const LAG_QUANTILE = 0.9;
/** A stage's period may hold at most this share of deals still open; more, and the stage isn't named. */
export const OPEN_SHARE = 0.05;
/** Within a record type, the gap that counts as holding there: the same as overall. */
const TYPE_GAP = WIN_GAP;

/**
 * The ways a stage's deals are split to check a gap (rule 4), each said in our
 * words. Record type has its own check, and the quarter a deal closed isn't a
 * way to split deals chosen by when they reached a stage.
 */
const WAYS: Record<string, Said> = { team: said`team`, rep: said`rep`, "lead source": said`lead source`, type: said`type` };

/** A deal as the comparison reads it: open or closed, with when it first entered each open stage. */
export type CohortDeal = PathDeal & { open: boolean; reachedAt: Map<string, number>; closedAtMs: number | null };

/** How one way of splitting a stage's deals went: segments compared, and how many sit on the same side of the setting. */
export type Way = { dimension: string; compared: number; held: number; holds: boolean };

/** One open stage's figures. Shares and the interval are percentages; days are local calendar days. */
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
  /** Won as a share of the closed deals that reached it in the period. */
  share: number;
  tooOpen: boolean;
  /** Why the stage can't be tested, in plain words, or null when it can. */
  untested: string | null;
  testable: boolean;
  interval: Interval | null;
  /** Won share less the probability, in points. Null with no probability. */
  gap: number | null;
  outside: boolean;
  ways: Way[];
  types: { many: boolean; compared: number; held: number };
  named: boolean;
};

export type Loaded =
  | { status: "ok"; data: Ready; stagesEvidence: Evidence; deals: CohortDeal[]; beforeHistory: number; stages: StageFigures[]; tested: number; z: number }
  | { status: "not_measurable"; reason: string; evidence: Evidence[]; data: PipelineData };

/** A deal reached a stage when its own stage history shows it there. Nothing before the first recorded stage is inferred. */
export const reached = (d: Pick<PathDeal, "path">, stage: string): boolean => d.path.includes(stage);

/** When the deal first entered each open stage, from its stage history. */
function reachedTimes(deal: Deal, open: (stage: string) => boolean): Map<string, number> {
  const at = new Map<string, number>();
  for (const row of deal.history) if (open(row.StageName) && !at.has(row.StageName)) at.set(row.StageName, new Date(row.CreatedDate).getTime());
  return at;
}

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

/** Named: every bar cleared. */
export function named(s: Omit<StageFigures, "named">): boolean {
  if (!s.testable || s.tooOpen || s.gap === null || Math.abs(s.gap) < WIN_GAP || !s.outside) return false;
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

/** One stage's deals in its period, and its figures but for the bar that counts every stage tested. */
function stageOf(ctx: Ctx, s: { stage: string; probability: number | null }, deals: CohortDeal[]) {
  const zone = zoneOf(ctx);
  const there = deals.filter((d) => reached(d, s.stage));
  const durations = there.filter((d) => d.closedAtMs !== null).map((d) => Math.max(0, (d.closedAtMs! - d.reachedAt.get(s.stage)!) / DAY));
  const lagDays = durations.length ? Math.ceil(quantile(durations, LAG_QUANTILE)) : null;
  const to = lagDays === null ? null : localDay(new Date(ctx.now.getTime() - lagDays * DAY), zone);
  const from = to !== null && to >= ctx.window.from ? ctx.window.from : null;
  const cohort = from && to ? there.filter((d) => {
    const day = localDay(new Date(d.reachedAt.get(s.stage)!), zone);
    return day >= from && day <= to;
  }) : [];
  const closed = cohort.filter((d) => !d.open);
  const won = closed.filter((d) => d.won).length;
  const open = cohort.length - closed.length;
  const tooOpen = cohort.length > 0 && open > OPEN_SHARE * cohort.length;
  const { minSample } = ctx.calibration;
  const untested =
    s.probability === null ? "No probability is set on this stage"
    : lagDays === null ? "No closed deal has reached this stage, so how long its deals take to close isn't known"
    : !from ? "No deal could have reached this stage early enough in the window to have closed by now"
    : tooOpen ? `Too many of the deals that reached ${s.stage} in this period are still open to say how often they are won`
    : !enough(minSample, closed.length, won) ? `${SEGMENT_NOTE}, or fewer than ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost`
    : null;
  return { ...s, lagDays, from, to, cohort: cohort.length, open, closed, reached: closed.length, won, share: pct(won, closed.length), tooOpen, untested, testable: untested === null };
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
  const open = (stage: string): boolean => data.stages.categoryOf(stage) === "open";
  // Every deal with a recorded open stage, open or closed, whenever it closed: each stage picks its own by when they reached it.
  const all: CohortDeal[] = data.deals.filter((d) => hasOpenStage(d, data)).map((deal) => {
    const isOpen = deal.category === "open";
    return { id: deal.Id, deal, path: openPathOf(deal, data.stages), won: deal.category === "won", days: isOpen ? 0 : daysToClose(deal), open: isOpen, reachedAt: reachedTimes(deal, open), closedAtMs: isOpen ? null : deal.closedAt.getTime() };
  });
  const deals = comparable(ctx, { closed: all }, data.historyStart) as CohortDeal[];
  const closedInIt = fromOpen.filter((d) => deals.some((x) => x.id === d.Id)).length;
  const beforeHistory = all.length - deals.length;
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
  const counted = listed.map((s) => stageOf(ctx, s, deals));
  // Every stage tested raises the bar for all, as every comparison does for the path insights (703).
  const tested = counted.filter((s) => s.testable).length;
  const z = zBar(tested);
  const stages = counted.map(({ closed, ...s }): StageFigures => {
    const interval = s.testable ? wilson(s.won, s.reached, z) : null;
    const gap = s.probability === null || s.reached === 0 ? null : round1(s.share - s.probability);
    const checked = s.testable && gap !== null;
    const figures = {
      ...s,
      interval: interval ? { low: round1(interval.low * 100), high: round1(interval.high * 100) } : null,
      gap,
      outside: interval !== null && s.probability !== null && outside(s.probability / 100, interval),
      ways: checked ? waysOf(ctx, closed, s.probability!, gap) : [],
      types: checked ? withinRecordTypes(ctx, closed, s.probability!, gap) : { many: false, compared: 0, held: 0 },
    };
    return { ...figures, named: named(figures) };
  });
  return { status: "ok", data, stagesEvidence, deals, beforeHistory, stages, tested, z };
}

// ─── Words ───────────────────────────────────────────────────────────────────

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"] as const;

/** A calendar day as its month and year, so the period is one the customer can check. */
export const monthSaid = (day: string): Said => eng(`${MONTHS[Number(day.slice(5, 7)) - 1]} ${day.slice(0, 4)}`);

// ─── The metric ──────────────────────────────────────────────────────────────

// Short, because the deck's definitions page prints it. How it's worked out is in the assumptions.
const DEFINITION =
  "For each open stage, the probability set on it in Salesforce beside the share of the deals that reached the stage that were won, which is what a stage probability claims: the chance a deal at that stage closes won. It isn't the win rate of the deals in the stage today. The value is how many stages are set clearly above or below what happens.";

const DEF = { id: PROBABILITY_METRIC, label: "Stage probability against how often deals are won", unit: "count" as const, definition: DEFINITION };

const HOW = [
  "A deal reached a stage when its stage history shows it there; a stage before its first recorded stage isn't counted as reached",
  "Each stage counts the deals, open or closed, that reached it from the start of the window to a cut-off: the run's date less the time within which 90% of the closed deals that reached the stage had closed. Deals are chosen by when they reached the stage, not when they closed, which would favour whichever outcome closes faster whenever the number of deals is changing",
  "A stage isn't named while more than 5% of the deals that reached it in its period are still open",
  "The won share is won deals as a share of the closed deals that reached the stage in its period, so no decision counts as not won",
  "Stages are listed in Salesforce's own stage order, which no figure here depends on",
];

/**
 * Every listed stage's figures, kept in the run's metrics whatever the rule
 * says (store broadly, show selectively): per stage, the probability, the
 * period and its lag, the deals that reached it then, how many are still open,
 * how many were won, the won share and, where tested, its interval. Enough for a
 * chart of each stage's setting against what happened.
 */
export function probabilityMetrics(ctx: Ctx): Metric[] {
  const loaded = loadProbabilities(ctx);
  if (loaded.status !== "ok") return [notMeasurable({ ...base(ctx, loaded.data, DEF), evidence: loaded.evidence }, loaded.reason)];
  const { data, stages, tested, deals, beforeHistory } = loaded;
  const { minSample } = ctx.calibration;
  const assumptions = [
    ...HOW,
    ...stages.filter((s) => s.lagDays !== null && s.to !== null).map((s) => said`${org(s.stage)}: 90% of the closed deals that reached it had closed within ${s.lagDays!} days, so its period ends on ${eng(s.to!)}`),
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

const direction = (s: StageFigures): Said => (s.gap! < 0 ? said`set higher than what happens` : said`set lower than what happens`);

/** Each way of splitting, said once by name, and how many it holds in. */
function waysSaid(s: StageFigures): Said {
  if (s.ways.length === 0) return said` We couldn't check this within teams, reps or other groups: none had enough of these deals.`;
  const names = andList(s.ways.map((w) => WAYS[w.dimension]!));
  const holding = s.ways.filter((w) => w.holds).length;
  if (s.ways.length === 1) return said` It is on the same side of the setting in the one way we could split these deals (by ${names}).`;
  return holding === s.ways.length
    ? said` It is on the same side of the setting in each of the ${s.ways.length} ways we could split these deals (by ${names}).`
    : said` It is on the same side of the setting in ${holding} of the ${s.ways.length} ways we could split these deals (by ${names}).`;
}

const sentenceOf = (s: StageFigures): Said => {
  const period = said`between ${monthSaid(s.from!)} and ${monthSaid(s.to!)}`;
  const outcome = s.open
    ? said`Of the ${s.cohort} deals that reached it ${period}, ${s.open} are still open and ${s.share}% of the other ${s.reached} were won`
    : said`Of the ${s.reached} deals that reached it ${period}, ${s.share}% were won`;
  return said`${org(s.stage)} is set to ${s.probability!}% in Salesforce. ${outcome}, so it is ${direction(s)}.${waysSaid(s)}`;
};

function titleOf(stages: StageFigures[]): Said {
  const higher = stages.every((s) => s.gap! < 0);
  const lower = stages.every((s) => s.gap! > 0);
  if (stages.length === 1) return said`The probability on ${org(stages[0]!.stage)} in Salesforce is ${higher ? said`higher` : said`lower`} than how often its deals are won`;
  const way = higher ? said`higher` : lower ? said`lower` : said`higher or lower`;
  return said`The probabilities on ${plural(stages.length, "stage")} in Salesforce are ${way} than how often their deals are won`;
}

const NOT_A_CAUSE = said` This compares each setting with what happened to the deals that reached the stage; it doesn't say why they differ.`;

/** Lowest with no way to split a stage named; high only with ways for each and every stage named at twice the minimum. */
export const confidenceOf = (ctx: Ctx, stages: StageFigures[]): "high" | "medium" | "low" =>
  stages.some((s) => s.ways.length === 0) ? "low" : stages.every((s) => s.reached >= ctx.calibration.minSample * 2) ? "high" : "medium";

const segmentsChecked = (ctx: Ctx, deals: CohortDeal[]): SegmentCheck[] =>
  Object.entries(dimensions(ctx))
    .filter(([dimension]) => dimension in WAYS || dimension === "record type")
    .map(([dimension, keyOf]) => ({ dimension, segments: new Set(deals.map((d) => keyOf(d.deal))).size }));

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
          ? said`No open stage's probability in Salesforce is ${WIN_GAP} points or more from how often the deals that reached it were won, beyond what chance could make of the ${plural(tested, "stage")} we could test.`
          : said`No open stage with a probability set had enough deals that reached it and have since closed, with ${MIN_EACH_OUTCOME} won and ${MIN_EACH_OUTCOME} lost, to hold against its probability.`,
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
