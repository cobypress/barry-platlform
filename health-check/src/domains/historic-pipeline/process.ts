import { notSized, pct, plural, segmentRate, type Rule, type RuleContext, type RuleResult, type SegmentedRate } from "../../engine/rule.ts";
import type { Calibration, Metric, MetricSegment } from "../../schemas/index.ts";
import { loadPipeline, type Deal, type PipelineData } from "./cohort.ts";
import { daysByStage, stageLeftFrom } from "./deep-dive.ts";
import { base, dimensions, measured, median, notMeasurable, quantile, round1, segment, unmappedReason, type Base } from "./metrics.ts";
import { closedFromOpen } from "./outcomes.ts";

// Roadmap 3.2: the sales process as deals really run it, read from stage
// history. Every deal here closed in the window after being open, so a deal
// loaded straight into a closed stage says nothing about the process.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;
type Definition = Pick<Base, "id" | "label" | "definition" | "unit"> & { assumptions?: string[] };

const DOMAIN = "historic_pipeline";
/** A stage more than this share of won deals passed through is part of the process, unless the calibration names the required stages. */
export const USUAL_SHARE = 50;
/** Won deals that skipped an expected stage: over this share overall, or in a segment, is an issue. */
export const SKIP_LIMIT = 10;
export const SKIP_SEGMENT_LIMIT = 25;
/** Deals that went back a stage at least once. */
export const BACK_LIMIT = 15;
export const BACK_SEGMENT_LIMIT = 30;
/** The stage lost deals leave from most, holding at least this share of them. */
export const DEAD_END_SHARE = 33;
/** Longer in a stage than this share of won deals took there is a long wait, as for open deals stuck in a stage. */
export const WAIT_PERCENTILE = 0.75;
const TOP_PATHS = 5;

const DEF = {
  transitions: { id: "historic.stage_transitions", label: "How deals move between stages", unit: "count", definition: "Each move from one stage to the next in the stage history of deals that closed in the window, counted from stage to stage. Repeated rows in the same stage, such as a close date change, aren't moves." },
  skipped: { id: "historic.won_skipped_stage", label: "Won deals that skipped a stage", unit: "percent", definition: "Won deals that never entered one of the expected stages, as a share of won deals. The expected stages are the ones the calibration marks as required, or else those most won deals passed through." },
  backwards: { id: "historic.went_backwards", label: "Deals that went back a stage", unit: "percent", definition: "Deals that moved to an earlier open stage at least once, as a share of deals that closed in the window." },
  wonPath: { id: "historic.path_to_won", label: "Most common path to won", unit: "percent", definition: "The run of stages won deals most often took, as a share of won deals, with the next most common paths." },
  lostPath: { id: "historic.path_to_lost", label: "Most common path to lost", unit: "percent", definition: "The run of stages lost and no decision deals most often took, as a share of those deals, with the next most common paths." },
} satisfies Record<string, Definition>;

export const PROCESS_METRIC_IDS = Object.values(DEF).map((d) => d.id);
export const TRANSITIONS = DEF.transitions.id;

/** The stages a deal was in, in order, with a repeated row in the same stage counted once. */
export function pathOf(deal: Deal): string[] {
  const path: string[] = [];
  for (const row of deal.history) if (path.at(-1) !== row.StageName) path.push(row.StageName);
  return path;
}

/** Moves between two open stages where the second comes earlier in the sales order. */
export function backwardMoves(path: string[], orderOf: (stage: string) => number | null): number {
  let moves = 0;
  for (let i = 1; i < path.length; i++) {
    const [from, to] = [orderOf(path[i - 1]!), orderOf(path[i]!)];
    if (from !== null && to !== null && to < from) moves++;
  }
  return moves;
}

const isWon = (d: Deal): boolean => d.category === "won";
const isLost = (d: Deal): boolean => d.category === "lost" || d.category === "no_decision";

type Process = { closed: Deal[]; won: Deal[]; lost: Deal[]; expected: string[]; expectedFrom: "calibration" | "won deals"; paths: Map<string, string[]> };

/** Null with the reason when the history can't support any of this. */
function load(ctx: Ctx): { status: "ok"; data: Ready; process: Process } | { status: "not_measurable"; reason: string; data: PipelineData } {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return { status: "not_measurable", reason: data.reason, data };
  const gaps = [...new Set([...data.unmapped.finalStages, ...data.unmapped.historyStages])].sort();
  if (gaps.length > 0) return { status: "not_measurable", reason: unmappedReason("stages", gaps), data };
  const closed = closedFromOpen(ctx, data);
  const { minSample } = ctx.calibration;
  if (closed.length < minSample) return { status: "not_measurable", reason: `${closed.length} closed deals with stage history is below the calibrated minimum of ${minSample}.`, data };
  const paths = new Map(closed.map((d) => [d.Id, pathOf(d)]));
  const won = closed.filter(isWon);
  const lost = closed.filter(isLost);
  const required = ctx.calibration.stages.filter((s) => s.category === "open" && s.required).map((s) => s.name);
  const usual = data.stages.openStages.filter((stage) => won.length > 0 && pct(won.filter((d) => paths.get(d.Id)!.includes(stage)).length, won.length) > USUAL_SHARE);
  const expected = required.length > 0 ? required : usual;
  return { status: "ok", data, process: { closed, won, lost, expected, expectedFrom: required.length > 0 ? "calibration" : "won deals", paths } };
}

const skippedOf = (p: Process, d: Deal): string[] => p.expected.filter((stage) => !p.paths.get(d.Id)!.includes(stage));
const expectedNote = (p: Process): string =>
  p.expectedFrom === "calibration" ? `The expected stages are the ones the calibration marks as required: ${p.expected.join(", ")}` : `The expected stages are those more than ${USUAL_SHARE}% of won deals passed through: ${p.expected.join(", ")}`;

// ─── Metrics ─────────────────────────────────────────────────────────────────

function transitions(ctx: Ctx, data: Ready, p: Process): Metric {
  const definition = base(ctx, data, DEF.transitions);
  const counts = new Map<string, Map<string, number>>();
  let moves = 0;
  for (const d of p.closed) {
    const path = p.paths.get(d.Id)!;
    for (let i = 1; i < path.length; i++) {
      const from = counts.get(path[i - 1]!) ?? new Map<string, number>();
      from.set(path[i]!, (from.get(path[i]!) ?? 0) + 1);
      counts.set(path[i - 1]!, from);
      moves++;
    }
  }
  const order = stageOrder(ctx.calibration);
  const sorted = (stages: Iterable<string>): string[] => [...stages].sort((a, b) => order(a) - order(b) || a.localeCompare(b));
  // One segment per cell, from stage to stage: the raw notes and the deck build the table from these.
  const segments: MetricSegment[] = sorted(counts.keys()).flatMap((from) => sorted(counts.get(from)!.keys()).map((to) => ({ dimension: `${FROM}${from}`, key: to, n: counts.get(from)!.get(to)!, value: counts.get(from)!.get(to)!, notMeasurableReason: null })));
  const backwards = p.closed.reduce((sum, d) => sum + backwardMoves(p.paths.get(d.Id)!, data.stages.orderOf), 0);
  return measured(definition, { value: moves, n: p.closed.length, dateRange: ctx.window, segments, stats: { moves, backwards, stages: counts.size } });
}

/** Open stages in sales order, then the closed ones in the calibration's order. */
function stageOrder(calibration: Calibration): (stage: string) => number {
  const open = calibration.stages.filter((s) => s.category === "open").sort((a, b) => a.order! - b.order!);
  const rest = calibration.stages.filter((s) => s.category !== "open");
  const position = new Map([...open, ...rest].map((s, i) => [s.name, i]));
  return (stage) => position.get(stage) ?? Number.MAX_SAFE_INTEGER;
}

const FROM = "from ";

/** The transitions metric as a table: a row for each stage moved from, a column for each stage moved to. */
export function transitionMatrix(m: Extract<Metric, { status: "measured" }>, calibration: Calibration): { from: string[]; to: string[]; count: (from: string, to: string) => number | null } {
  const order = stageOrder(calibration);
  const sorted = (stages: Iterable<string>): string[] => [...new Set(stages)].sort((a, b) => order(a) - order(b) || a.localeCompare(b));
  const cells = m.segments.filter((x) => x.dimension.startsWith(FROM));
  const cell = new Map(cells.map((x) => [`${x.dimension.slice(FROM.length)}\u0000${x.key}`, x.value]));
  return { from: sorted(cells.map((x) => x.dimension.slice(FROM.length))), to: sorted(cells.map((x) => x.key)), count: (from, to) => cell.get(`${from}\u0000${to}`) ?? null };
}

function skipped(ctx: Ctx, data: Ready, p: Process): Metric {
  const definition = base(ctx, data, { ...DEF.skipped, assumptions: [expectedNote(p)] });
  const { minSample } = ctx.calibration;
  if (p.won.length < minSample) return notMeasurable(definition, `${p.won.length} won deals with stage history is below the calibrated minimum of ${minSample}.`);
  if (p.expected.length === 0) return notMeasurable(definition, `No open stage was passed through by more than ${USUAL_SHARE}% of won deals, and the calibration marks none as required.`);
  const rate = (group: Deal[]): number => pct(group.filter((d) => skippedOf(p, d).length > 0).length, group.length);
  const byStage: MetricSegment[] = p.expected.map((stage) => ({ dimension: "skipped stage", key: stage, n: p.won.length, value: pct(p.won.filter((d) => !p.paths.get(d.Id)!.includes(stage)).length, p.won.length), notMeasurableReason: null }));
  return measured(definition, { value: rate(p.won), n: p.won.length, dateRange: ctx.window, segments: [...byStage, ...segment(p.won, dimensions(ctx), minSample, rate)], stats: { skipped: p.won.filter((d) => skippedOf(p, d).length > 0).length } });
}

function backwards(ctx: Ctx, data: Ready, p: Process): Metric {
  const definition = base(ctx, data, { ...DEF.backwards, assumptions: ["Earlier means earlier in the calibration's order of open stages"] });
  const back = (d: Deal): number => backwardMoves(p.paths.get(d.Id)!, data.stages.orderOf);
  const rate = (group: Deal[]): number => pct(group.filter((d) => back(d) > 0).length, group.length);
  const outcome = (d: Deal): string => (isWon(d) ? "Won" : d.category === "lost" ? "Lost" : "No decision");
  return measured(definition, {
    value: rate(p.closed),
    n: p.closed.length,
    dateRange: ctx.window,
    segments: segment(p.closed, { outcome, ...dimensions(ctx) }, ctx.calibration.minSample, rate),
    stats: { wentBack: p.closed.filter((d) => back(d) > 0).length, twiceOrMore: p.closed.filter((d) => back(d) >= 2).length },
  });
}

function commonPath(ctx: Ctx, data: Ready, p: Process, deals: Deal[], d: Definition, noun: string): Metric {
  const definition = base(ctx, data, d);
  const { minSample } = ctx.calibration;
  if (deals.length < minSample) return notMeasurable(definition, `${deals.length} ${noun} with stage history is below the calibrated minimum of ${minSample}.`);
  const counts = new Map<string, number>();
  for (const deal of deals) {
    const key = p.paths.get(deal.Id)!.join(" > ");
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  const ranked = [...counts].sort(([a, x], [b, y]) => y - x || a.localeCompare(b));
  const segments: MetricSegment[] = ranked.slice(0, TOP_PATHS).map(([key, n]) => ({ dimension: "path", key, n, value: pct(n, deals.length), notMeasurableReason: null }));
  return measured(definition, { value: pct(ranked[0]![1], deals.length), n: deals.length, dateRange: ctx.window, segments, stats: { paths: counts.size, steps: p.paths.get(deals.find((x) => p.paths.get(x.Id)!.join(" > ") === ranked[0]![0])!.Id)!.length } });
}

const NOT_MEASURABLE_IDS: Definition[] = Object.values(DEF);

export function processMetrics(ctx: Ctx): Metric[] {
  const loaded = load(ctx);
  if (loaded.status !== "ok") return NOT_MEASURABLE_IDS.map((d) => notMeasurable(base(ctx, loaded.data, d), loaded.reason));
  const { data, process: p } = loaded;
  return [transitions(ctx, data, p), skipped(ctx, data, p), backwards(ctx, data, p), commonPath(ctx, data, p, p.won, DEF.wonPath, "won deals"), commonPath(ctx, data, p, p.lost, DEF.lostPath, "lost and no decision deals")];
}

// ─── Rules ───────────────────────────────────────────────────────────────────

/** The segments worse than the whole, which a total alone would hide. Nothing when every segment matches it. */
function worst(result: SegmentedRate): string {
  const worse = result.hotspots.filter((h) => h.rate > result.rate).slice(0, 3);
  return worse.length ? ` Highest: ${worse.map((h) => `${h.dimension} ${h.key}, ${h.hits} of ${h.n} (${h.rate}%)`).join("; ")}.` : "";
}

const NOT_SIZED = "We don't put a figure on it: it shows how far the stages can be trusted, and that has no direct value.";

function notMeasurableResult(loaded: Exclude<ReturnType<typeof load>, { status: "ok" }>): RuleResult {
  return { outcome: "not_measurable", reason: loaded.reason, evidence: loaded.data.evidence };
}

const skippedStages: Rule = {
  id: "pipe.skipped-stages",
  version: 1,
  domain: DOMAIN,
  title: "Won deals that skip a stage",
  evaluate(ctx): RuleResult {
    const loaded = load(ctx);
    if (loaded.status !== "ok") return notMeasurableResult(loaded);
    const { data, process: p } = loaded;
    const { minSample } = ctx.calibration;
    if (p.won.length < minSample) return { outcome: "not_measurable", reason: `${p.won.length} won deals with stage history is below the calibrated minimum of ${minSample}.`, evidence: data.evidence };
    if (p.expected.length === 0) return { outcome: "not_measurable", reason: `No open stage was passed through by more than ${USUAL_SHARE}% of won deals, and the calibration marks none as required.`, evidence: data.evidence };
    const result = segmentRate(p.won, (d) => skippedOf(p, d).length > 0, dimensions(ctx), { segmentLimit: SKIP_SEGMENT_LIMIT, minSegment: minSample });
    if (result.rate <= SKIP_LIMIT && result.hotspots.length === 0) {
      return { outcome: "pass", summary: `${result.hits} of ${result.n} won deals (${result.rate}%) skipped an expected stage, within our limit of ${SKIP_LIMIT}%, and no segment is over ${SKIP_SEGMENT_LIMIT}%. ${expectedNote(p)}.`, evidence: data.evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
    }
    const byStage = p.expected.map((stage) => ({ stage, n: p.won.filter((d) => !p.paths.get(d.Id)!.includes(stage)).length })).filter((s) => s.n > 0).sort((a, b) => b.n - a.n);
    const where = result.rate > SKIP_LIMIT ? `${result.hits} of ${result.n} won deals (${result.rate}%)` : `${result.hits} of ${result.n} won deals overall (${result.rate}%), and more in some segments`;
    return {
      outcome: "issue",
      title: "Won deals skip stages your process expects",
      summary: `${where} never entered a stage we'd expect them to: ${byStage.map((s) => `${s.stage} (${s.n})`).join(", ")}. ${expectedNote(p)}.${worst(result)}`,
      recommendation: "Ask the teams that skip these stages why. Either the stage isn't part of how they sell, and it can go from their path, or it's being skipped in Salesforce, and the stage's exit criteria need agreeing.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: p.expectedFrom === "calibration" ? "high" : "medium",
      evidence: data.evidence,
      value: notSized(NOT_SIZED),
      effort: { size: "S" },
      owner: "Sales operations",
      segmentsChecked: result.segmentsChecked,
      subjects: [...byStage.map((s) => ({ type: "stage", label: s.stage })), ...result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key }))],
      facts: { hits: result.hits, n: result.n, rate: result.rate, stages: byStage.length },
    };
  },
};

const goingBackwards: Rule = {
  id: "pipe.going-backwards",
  version: 1,
  domain: DOMAIN,
  title: "Deals that go back a stage",
  evaluate(ctx): RuleResult {
    const loaded = load(ctx);
    if (loaded.status !== "ok") return notMeasurableResult(loaded);
    const { data, process: p } = loaded;
    const back = (d: Deal): number => backwardMoves(p.paths.get(d.Id)!, data.stages.orderOf);
    const result = segmentRate(p.closed, (d) => back(d) > 0, dimensions(ctx), { segmentLimit: BACK_SEGMENT_LIMIT, minSegment: ctx.calibration.minSample });
    if (result.rate <= BACK_LIMIT && result.hotspots.length === 0) {
      return { outcome: "pass", summary: `${result.hits} of ${result.n} closed deals (${result.rate}%) went back a stage, within our limit of ${BACK_LIMIT}%, and no segment is over ${BACK_SEGMENT_LIMIT}%.`, evidence: data.evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
    }
    const twice = p.closed.filter((d) => back(d) >= 2).length;
    return {
      outcome: "issue",
      title: "Deals often go back a stage",
      summary: `${result.hits} of ${result.n} deals that closed in the window (${result.rate}%) went back to an earlier stage at least once${twice ? `, and ${plural(twice, "deal")} twice or more` : ""}.${worst(result)} A deal moved forward too early makes the open pipeline look further on than it is.`,
      recommendation: "Agree what has to be true for a deal to leave each stage, and check new moves against it in pipeline reviews.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence: data.evidence,
      value: notSized(NOT_SIZED),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: result.segmentsChecked,
      subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
      facts: { hits: result.hits, n: result.n, rate: result.rate, twiceOrMore: twice },
    };
  },
};

/**
 * Not the lost-by-stage figure, which counts where lost deals leave from, nor
 * cur.stuck-in-stage, which is about open deals. This joins the two: the stage
 * lost deals leave from most, when they sit there far longer than wins do.
 */
const deadEndStage: Rule = {
  id: "pipe.dead-end-stage",
  version: 1,
  domain: DOMAIN,
  title: "A stage where lost deals wait before they die",
  evaluate(ctx): RuleResult {
    const loaded = load(ctx);
    if (loaded.status !== "ok") return notMeasurableResult(loaded);
    const { data, process: p } = loaded;
    const { minSample } = ctx.calibration;
    const lost = p.lost.filter((d) => stageLeftFrom(d, data) !== null);
    if (lost.length < minSample) return { outcome: "not_measurable", reason: `${lost.length} lost and no decision deals with stage history is below the calibrated minimum of ${minSample}.`, evidence: data.evidence };
    const counts = new Map<string, Deal[]>();
    for (const d of lost) counts.set(stageLeftFrom(d, data)!, [...(counts.get(stageLeftFrom(d, data)!) ?? []), d]);
    const [stage, dying] = [...counts].sort(([a, x], [b, y]) => y.length - x.length || a.localeCompare(b))[0]!;
    const share = pct(dying.length, lost.length);
    const wonDays = p.won.flatMap((d) => (daysByStage(d, data).has(stage) ? [daysByStage(d, data).get(stage)!] : []));
    const segments = [{ dimension: "stage left", segments: counts.size }];
    if (share < DEAD_END_SHARE) return { outcome: "pass", summary: `Lost deals leave from no one stage in particular: the most, ${stage}, holds ${dying.length} of ${lost.length} (${share}%).`, evidence: data.evidence, segmentsChecked: segments, facts: { share } };
    if (wonDays.length < minSample) return { outcome: "not_measurable", reason: `${wonDays.length} won deals were in ${stage}, below the calibrated minimum of ${minSample}, so there's no usual time to compare with.`, evidence: data.evidence };
    const line = round1(quantile(wonDays, WAIT_PERCENTILE));
    const waitOf = (d: Deal): number => daysByStage(d, data).get(stage) ?? 0;
    const wait = round1(median(dying.map(waitOf)));
    const result = segmentRate(dying, (d) => waitOf(d) > line, dimensions(ctx), { segmentLimit: 50, minSegment: minSample });
    const checked = [...segments, ...result.segmentsChecked];
    if (wait <= line) return { outcome: "pass", summary: `More lost deals leave from ${stage} than from any other stage (${dying.length} of ${lost.length}, ${share}%), but after a median of ${wait} days there, no longer than three quarters of won deals took (${line} days).`, evidence: data.evidence, segmentsChecked: checked, facts: { share, wait, line } };
    return {
      outcome: "issue",
      title: `Lost deals wait in ${stage} before they're closed`,
      summary: `${dying.length} of ${lost.length} lost and no decision deals (${share}%) were last open in ${stage}, and spent a median of ${wait} days there. Three quarters of won deals left ${stage} within ${line} days. ${result.hits} of these lost deals (${result.rate}%) waited longer than that.${worst(result)}`,
      recommendation: `Set a time limit for ${stage} from your won deals: past it, a deal is reviewed and either moved on with a dated next step or closed, so lost deals stop sitting in the pipeline.`,
      recommendationType: "change",
      severity: "medium",
      confidence: "high",
      evidence: data.evidence,
      value: notSized("These deals were lost either way. Closing them sooner makes the pipeline honest, which has no direct figure."),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: checked,
      subjects: [{ type: "stage", label: stage }, ...result.hotspots.slice(0, 4).map((h) => ({ type: h.dimension, label: h.key }))],
      facts: { lost: dying.length, share, wait, line, waitedLonger: result.hits },
    };
  },
};

export const PROCESS_RULES: Rule[] = [skippedStages, goingBackwards, deadEndStage];
