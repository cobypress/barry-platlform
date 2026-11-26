// cur.stuck-in-stage version 3's comparison (HC_STUCK_EXPECTED, DECISIONS.md 777), with no Salesforce in it, so a
// probe can run it on simulated orgs as the rule does on a real one.
//
// Version 2 counted the open deals that had sat in their stage longer than the
// line (the 75th percentile of won deals' completed stays) and raised an issue
// above a fixed share. A snapshot of open deals over-samples long stays: on a
// steady pipeline the share past the q-th percentile of stays X is
// E[max(0, X - q)] / E[X], 25% only for exponential stays and 57% or more for the
// long tails B2B stages usually have, so the fixed share raised an issue on every
// run of a steady long-tailed org.
//
// The stay in a stage. Every entry into the stage since the history we read
// starts is one stay: finished when the deal moved on or closed, still running
// (censored at its age today) when the deal is still there. A deal that re-enters
// the stage has a stay each time; a deal that skips it has none. The share of
// stays lasting longer than t, S(t), is the Kaplan-Meier estimate from all of
// them, so stays still running count for as long as they have run: a long cycle
// or a short history doesn't read short, as finished stays alone did (the e45
// review: 8 points low on a 9-month cycle, 15 on a 180-day history). Only entries
// since the read's start are used, since an older entry is in the read only if
// its deal was still open or closed inside it, which favours long stays.
//
// The tail. After the longest finished stay, S stays where it was (some stays
// still running are longer), unless the history covers twice the longest finished
// stay: then deals were followed long enough to have been seen leaving later, none
// did, and those still there are past every stay the stage's times explain, so S is
// taken as none beyond it. Otherwise a stage whose estimate still has more than
// TAIL_LIMIT of stays running at its longest finished stay is not measurable: the
// history doesn't yet cover how long deals stay there, and an estimate would be a
// guess.
//
// The expectation. Each entry at age a today is still in the stage with chance
// S(a); so, given the deals that really entered, the stage would hold sum S(a) of
// them, and sum S(a) over a past the line of those past it. Each open deal is then
// past the line with that share, independently (stays in progress at a moment are
// a Poisson sample), so a group's count past the line has mean sum(e) and variance
// sum(e(1 - e)); the share's own uncertainty, from Greenwood's variance of the
// Kaplan-Meier estimate carried through the ratio by the delta method (a bootstrap
// would cost a hundred estimates a stage a group, and Greenwood is exact to the
// same first order), is added, scaled by the group's deals in that stage, since
// they share the one estimate. When no entry would still be there at all, none of
// the open deals there is expected: every one is past what the stage's times
// explain. A group is raised only when it is clearly above (z) and far enough
// above (points) its own expected share: the total and every segment alike.

/** One stay in a stage: finished (`done`) after `days`, or still running after `days`. */
export type Stay = { days: number; done: boolean };

/** A stage's estimate of how long stays last. */
export type StageModel = {
  /** Days: the percentile of stays, by the estimate. */
  line: number;
  /** The longest finished stay, in days. */
  longest: number;
  /** The share of stays still running after the longest finished one (before any cut). */
  plateau: number;
  /** True when the history covers twice the longest finished stay, so nothing is expected past it. */
  cut: boolean;
  /** Distinct finished stays, ascending, with S just after each and Greenwood's sum up to it. */
  times: number[];
  surv: number[];
  greenwood: number[];
  finished: number;
};

/** The most of a stage's stays the estimate may leave running at its longest finished stay, short of the cut. */
export const TAIL_LIMIT = 0.05;
/** The history must cover this many times the longest finished stay for nothing to be expected past it. */
export const CUT_COVER = 2;

export type ModelOutcome = StageModel | { why: "few" | "uncovered" };

/** A stage's estimate from its stays, given how many days of history the read covers for it, at `percentile` (0 to 1). */
export function stageModel(stays: readonly Stay[], coverDays: number, percentile: number, minFinished: number): ModelOutcome {
  const finished = stays.filter((s) => s.done).length;
  if (finished < minFinished) return { why: "few" };
  const sorted = [...stays].sort((a, b) => a.days - b.days || Number(b.done) - Number(a.done));
  const times: number[] = [];
  const surv: number[] = [];
  const greenwood: number[] = [];
  let atRisk = sorted.length;
  let s = 1;
  let g = 0;
  for (let i = 0; i < sorted.length; ) {
    const t = sorted[i]!.days;
    let d = 0;
    let c = 0;
    while (i < sorted.length && sorted[i]!.days === t) {
      if (sorted[i]!.done) d++;
      else c++;
      i++;
    }
    if (d > 0) {
      s *= 1 - d / atRisk;
      if (atRisk > d) g += d / (atRisk * (atRisk - d));
      times.push(t);
      surv.push(s);
      greenwood.push(g);
    }
    atRisk -= d + c;
  }
  const longest = times.at(-1)!;
  const plateau = surv.at(-1)!;
  const cut = coverDays >= CUT_COVER * longest;
  if (!cut && plateau > TAIL_LIMIT) return { why: "uncovered" };
  const at = surv.findIndex((x) => x <= 1 - percentile);
  // With the cut, the stays still running past the longest finished one are past the line, which is that stay.
  const line = at >= 0 ? times[at]! : longest;
  return { line, longest, plateau, cut, times, surv, greenwood, finished };
}

/** Count of sorted values at or below x. */
function upTo(sorted: readonly number[], x: number): number {
  let lo = 0;
  let hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sorted[mid]! <= x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** S(age), the share of stays longer than `age`, and Greenwood's sum at it. */
export function survival(model: StageModel, age: number): { s: number; g: number } {
  if (model.cut && age > model.longest) return { s: 0, g: 0 };
  const k = upTo(model.times, age);
  return k === 0 ? { s: 1, g: 0 } : { s: model.surv[k - 1]!, g: model.greenwood[k - 1]! };
}

/** The share of a stage's deals past its line, given the ages today of the entries into it, and its variance. */
export type Expected = { share: number; spread: number };

export function expectedShare(model: StageModel, ages: readonly number[]): Expected {
  const rows = [...ages].sort((a, b) => a - b).map((a) => ({ past: a > model.line, ...survival(model, a) }));
  const all = rows.reduce((t, r) => t + r.s, 0);
  // No entry would still be there: none of the deals there is expected, past the line or not.
  if (!(all > 0)) return { share: 0, spread: 0 };
  const share = rows.reduce((t, r) => t + (r.past ? r.s : 0), 0) / all;
  // Var(sum h_j S_j-hat) with Cov(S(a), S(b)) = S(a) S(b) g(min(a, b)), the entries in age order.
  const h = rows.map((r) => ((r.past ? 1 : 0) - share) * r.s);
  let after = 0;
  let v = 0;
  for (let j = rows.length - 1; j >= 0; j--) {
    v += h[j]! * rows[j]!.g * (h[j]! + 2 * after);
    after += h[j]!;
  }
  return { share, spread: Math.max(0, v) / all ** 2 };
}

/** One group's count past the line against what it would be. Shares are raw (0 to 100), never rounded. */
export type Comparison = {
  n: number;
  hits: number;
  share: number;
  expectedShare: number;
  excess: number;
  z: number;
};

export type Margin = { points: number; z: number };

/**
 * The margins version 3 ships (DECISIONS.md 777), chosen on the e45 fix round's probes on two generators (the
 * builder's and the reviewer's, 99 quiet cells of 200 orgs): at most 0.5% raised in any. The z tests keep steady orgs
 * quiet; the points keep a large org from being raised for a gap too small to act on. A segment is held to a higher
 * z and more points, since about a dozen are tested: at z 3.5, 50 reps raised 1.5% of steady orgs.
 */
export const TOTAL_MARGIN: Margin = { points: 5, z: 3 };
export const SEGMENT_MARGIN: Margin = { points: 8, z: 4 };

export const above = (c: Comparison, margin: Margin): boolean => c.n > 0 && c.excess > margin.points && c.z > margin.z;

export type SegmentComparison = Comparison & { dimension: string; key: string };

export type Judged = {
  total: Comparison & { raised: boolean };
  segments: Array<SegmentComparison & { raised: boolean }>;
  raised: SegmentComparison[];
  segmentsChecked: Array<{ dimension: string; segments: number }>;
};

export type Inputs<D> = {
  open: readonly D[];
  stageOf: (deal: D) => string;
  ageOf: (deal: D) => number;
  /** Every entry since the read's start into a stage with a model, by any deal, with its age today in days. */
  entries: ReadonlyArray<{ deal: D; stage: string; age: number }>;
  models: ReadonlyMap<string, StageModel>;
  dimensions: Record<string, (deal: D) => string>;
  /** The dimension that is the deal's stage: its segment's entries are every entry into that stage. */
  stageDimension: string;
  minSegment: number;
  total?: Margin;
  segment?: Margin;
};

/** Whether an open deal is past its stage's line: strictly longer, as version 2 counts. */
export const pastLine = (age: number, model: StageModel): boolean => age > model.line;

function compareGroup<D>(open: readonly D[], entries: Inputs<D>["entries"], input: Inputs<D>): Comparison {
  let expected = 0;
  let binomial = 0;
  let estimate = 0;
  const inStage = new Map<string, number>();
  for (const deal of open) inStage.set(input.stageOf(deal), (inStage.get(input.stageOf(deal)) ?? 0) + 1);
  for (const [stage, k] of inStage) {
    const model = input.models.get(stage);
    if (!model) throw new Error("No model for the stage of a compared deal");
    const e = expectedShare(model, entries.filter((x) => x.stage === stage).map((x) => x.age));
    expected += k * e.share;
    binomial += k * e.share * (1 - e.share);
    estimate += k * k * e.spread;
  }
  const hits = open.filter((d) => pastLine(input.ageOf(d), input.models.get(input.stageOf(d))!)).length;
  const n = open.length;
  const variance = binomial + estimate;
  const diff = hits - expected;
  const z = variance > 0 ? diff / Math.sqrt(variance) : diff > 0 ? Infinity : 0;
  const share = n ? (100 * hits) / n : 0;
  const expectedPct = n ? (100 * expected) / n : 0;
  return { n, hits, share, expectedShare: expectedPct, excess: share - expectedPct, z };
}

/** The total and each segment, each against its own deals' expectation, as rule 4 asks. */
export function judge<D>(input: Inputs<D>): Judged {
  const [totalMargin, segmentMargin] = [input.total ?? TOTAL_MARGIN, input.segment ?? SEGMENT_MARGIN];
  const total = compareGroup(input.open, input.entries, input);
  const segments: Judged["segments"] = [];
  const segmentsChecked: Judged["segmentsChecked"] = [];
  for (const [dimension, keyOf] of Object.entries(input.dimensions)) {
    const groups = new Map<string, D[]>();
    for (const deal of input.open) {
      const key = keyOf(deal);
      const group = groups.get(key);
      if (group) group.push(deal);
      else groups.set(key, [deal]);
    }
    segmentsChecked.push({ dimension, segments: groups.size });
    for (const [key, group] of groups) {
      // A segment holding every compared deal is the total, said once as the total.
      if (group.length < input.minSegment || group.length === input.open.length) continue;
      const c = compareGroup(group, input.entries.filter((x) => (dimension === input.stageDimension ? x.stage : keyOf(x.deal)) === key), input);
      segments.push({ ...c, dimension, key, raised: above(c, segmentMargin) });
    }
  }
  const raised = segments
    .filter((s) => s.raised)
    .map(({ raised: _, ...s }) => s)
    .sort((a, b) => b.excess - a.excess || b.z - a.z || a.dimension.localeCompare(b.dimension) || a.key.localeCompare(b.key));
  return { total: { ...total, raised: above(total, totalMargin) }, segments, raised, segmentsChecked };
}
