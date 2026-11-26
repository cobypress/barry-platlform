// cur.stuck-in-stage version 3's comparison (HC_STUCK_EXPECTED, DECISIONS.md 777), with no Salesforce in it, so a
// probe can run it on simulated orgs as the rule does on a real one.
//
// Version 2 counted the open deals that had sat in their stage longer than the
// line (the 75th percentile of completed stays) and raised an issue above a fixed
// share. A snapshot of open deals over-samples long stays: on a steady pipeline,
// where nothing gets worse, the share of open deals past the q-th percentile of
// completed stays X is E[max(0, X - q)] / E[X], which is 25% only for exponential
// stays and 57% or more for the heavy tails B2B stages usually have. So the fixed
// share raised an issue on every run of a steady heavy-tailed org.
//
// Version 3 keeps the line and the count, and compares the count with the share
// the same deals would show if they had moved through each stage as the completed
// stays did. Every entry into a stage the history shows, at age a today, would
// still be in it with chance S(a), the share of completed stays longer than a. So,
// given the deals that really entered, the stage would hold sum S(a) of them, and
// sum S(a) over a past the line of those past it. With entries arriving evenly
// that ratio is E[max(0, X - q)] / E[X]; reading the real entries also keeps a
// pipeline whose intake has grown or shrunk, or an org whose history is short,
// from looking stuck when nothing is (the e45 probes: arrivals falling to a third
// over a year raised up to 23% of steady heavy-tailed orgs under the even-flow
// formula, and none this way).
//
// Each open deal in a stage is then past the line with that chance, independently
// (the stays in progress at a moment of a Poisson flow are a Poisson sample), so a
// group's count past the line has mean sum(e) and variance sum(e(1 - e)). The
// expected share comes from a finite sample of stays, and its spread (the delta
// method for a ratio of means over the stays) is added, scaled by the group's
// deals in that stage, since they all share the one estimate. A group is raised
// only when it is clearly above (z) and far enough above (points) its own expected
// share: the total and every segment alike, each against its own entries, since a
// segment tested against a fixed share would bring the fault back.

/** Count of sorted values strictly below x. */
function below(sorted: readonly number[], x: number): number {
  let lo = 0;
  let hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (sorted[mid]! < x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** A stage's line and the completed stays it was read from. */
export type StageLine = {
  /** Days: the percentile of the completed stays. */
  line: number;
  /** The completed stays, in days. */
  stays: readonly number[];
};

export const stageLine = (stays: readonly number[], percentile: number, quantile: (values: number[], q: number) => number): StageLine => ({ line: quantile([...stays], percentile), stays });

/** The share of a stage's deals past its line in a pipeline whose stays were the completed ones, and its spread. */
export type Expected = {
  /** 0 to 1. */
  share: number;
  /** The variance of `share` as an estimate, from the stays' own spread. */
  spread: number;
};

/**
 * The expected share past the line, given the ages today of every entry into the stage (in days), or null when no
 * entry would still be in the stage at all (every one is older than every completed stay).
 */
export function expectedShare(ref: StageLine, ages: readonly number[]): Expected | null {
  const sorted = [...ages].sort((a, b) => a - b);
  // Entries at or under the line: never past it, however long they stay.
  const notPast = below(sorted, ref.line) + sorted.filter((a) => a === ref.line).length;
  // For each stay x, the entries it would outlast (age < x), and those of them past the line: their means over the
  // stays are sum S(a) and sum S(a) over a past the line.
  let all = 0;
  let past = 0;
  const pairs: Array<[number, number]> = [];
  for (const x of ref.stays) {
    const c = below(sorted, x);
    const p = Math.max(0, c - notPast);
    all += c;
    past += p;
    pairs.push([c, p]);
  }
  const m = ref.stays.length;
  if (!(all > 0)) return null;
  const share = past / all;
  const mean = all / m;
  const residual = m > 1 ? pairs.reduce((a, [c, p]) => a + (p - share * c) ** 2, 0) / (m - 1) : 0;
  return { share, spread: residual / m / mean ** 2 };
}

/** One group's count past the line against what it would be. Shares are raw (0 to 100), never rounded. */
export type Comparison = {
  n: number;
  hits: number;
  /** Raw share past the line, in percent. */
  share: number;
  /** Raw expected share, in percent. */
  expectedShare: number;
  /** Raw points above the expected share (negative below it). */
  excess: number;
  z: number;
};

/** How far above its expectation a group must be, by both tests, to be raised. */
export type Margin = { points: number; z: number };

/**
 * The margins version 3 ships (DECISIONS.md 777), chosen on the e45 probes (orchestration/health-check/probes/
 * e45-stuck-expected): four stay shapes at 100, 300 and 1,000 open deals, 200 orgs a cell. The z tests keep steady
 * orgs quiet; the points keep a large org from being raised for a gap too small to act on. The total's z is the
 * higher: the closed deals lack the longest stays, still going, so the expectation reads up to 2 points low on long
 * tails (3 on a growing pipeline), which a large org's total turns into z. A segment needs more points, since about
 * a dozen are tested.
 */
export const TOTAL_MARGIN: Margin = { points: 5, z: 4 };
export const SEGMENT_MARGIN: Margin = { points: 8, z: 3.5 };

/** Whether a comparison is raised at a margin: above by more than its points and its z, on the raw figures. */
export const above = (c: Comparison, margin: Margin): boolean => c.n > 0 && c.excess > margin.points && c.z > margin.z;

export type SegmentComparison = Comparison & { dimension: string; key: string };

export type Judged = {
  total: Comparison & { raised: boolean };
  /** Every segment of at least `minSegment` deals, with whether it was raised. */
  segments: Array<SegmentComparison & { raised: boolean }>;
  /** The raised segments, furthest above their own expected share first. */
  raised: SegmentComparison[];
  segmentsChecked: Array<{ dimension: string; segments: number }>;
};

export type Inputs<D> = {
  /** The open deals compared: each in a stage with a line, with its entry shown. */
  open: readonly D[];
  stageOf: (deal: D) => string;
  /** Days in its current stage today. */
  ageOf: (deal: D) => number;
  /** Every entry into a stage with a line, by any deal, with its age today in days. */
  entries: ReadonlyArray<{ deal: D; stage: string; age: number }>;
  lines: ReadonlyMap<string, StageLine>;
  dimensions: Record<string, (deal: D) => string>;
  /**
   * The dimension that is the deal's stage: its segment's entries are every entry into that stage, by any deal, and
   * not the entries of the deals in it now, which would leave out every deal that has moved on.
   */
  stageDimension: string;
  minSegment: number;
  total?: Margin;
  segment?: Margin;
};

/** One group of open deals against its own entries' expectation. */
function compareGroup<D>(open: readonly D[], entries: Inputs<D>["entries"], input: Inputs<D>): Comparison {
  let hits = 0;
  let expected = 0;
  let binomial = 0;
  let estimate = 0;
  const inStage = new Map<string, number>();
  for (const deal of open) inStage.set(input.stageOf(deal), (inStage.get(input.stageOf(deal)) ?? 0) + 1);
  for (const [stage, k] of inStage) {
    const ref = input.lines.get(stage);
    if (!ref) throw new Error("No line for the stage of a compared deal");
    // Every entry older than every completed stay: none would still be there, and as S(a) falls to nothing the share
    // of those left that are past the line rises to all of them, its limit, so an open deal there is expected past it.
    const e = expectedShare(ref, entries.filter((x) => x.stage === stage).map((x) => x.age)) ?? { share: 1, spread: 0 };
    expected += k * e.share;
    binomial += k * e.share * (1 - e.share);
    estimate += k * k * e.spread;
  }
  for (const deal of open) if (input.ageOf(deal) > input.lines.get(input.stageOf(deal))!.line) hits++;
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
      if (group.length < input.minSegment) continue;
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
