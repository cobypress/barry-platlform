import type { Extractor, SourceInfo } from "../extract/types.ts";
import type { Calibration, DateRange, RunScope, Effort, Evidence, Guard, ManualAnswer, ManualStep, Metric, RecommendationType, SegmentCheck, Subject, Value } from "../schemas/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";

type Severity = "critical" | "high" | "medium" | "low" | "info";
type Confidence = "high" | "medium" | "low";

/** The objects an action list can hold records of. */
export type ActionObject = "Opportunity" | "Lead" | "Contact" | "Account" | "SBQQ__Quote__c" | "Contract" | "User";

/**
 * The records an issue is about, for the action lists (roadmap Stage 4). Ids
 * only, with what's wrong in words that hold no name: the engine keeps these out
 * of the finding (rule 7) and joins each record's name in the worker.
 */
export type RecordList = {
  object: ActionObject;
  rows: Array<{ id: string; problem: string }>;
  /** What to do to each record. */
  fix: string;
  /**
   * When the rows are a part of the whole: how many there are, and how to find
   * the rest. `label` says what the part is when it isn't simply the first rows
   * in order of Salesforce id, such as two lists of which one stopped short.
   */
  part?: { total: number; rest: string; label?: string };
};

export type IssueResult = {
  outcome: "issue";
  /** Tells apart several findings from one rule. Stable across runs. */
  key?: string;
  title: string;
  summary: string;
  recommendation: string;
  recommendationType: RecommendationType;
  /** What to say instead if a guard blocks a destructive recommendation. Required with guards. */
  blockedRecommendation?: string;
  severity: Severity;
  confidence: Confidence;
  evidence: Evidence[];
  value: Value;
  effort: Effort;
  owner: string;
  dependencies?: string[];
  guards?: Guard[];
  segmentsChecked?: SegmentCheck[];
  subjects?: Subject[];
  facts?: Record<string, number>;
  manualStepIds?: string[];
  records?: RecordList;
};

export type PassResult = {
  outcome: "pass";
  key?: string;
  summary: string;
  evidence: Evidence[];
  segmentsChecked?: SegmentCheck[];
  /** The only way to pass without segments, and it has to give a reason. */
  segmentExemptReason?: string;
  facts?: Record<string, number>;
  manualStepIds?: string[];
};

export type NotMeasurableResult = {
  outcome: "not_measurable";
  key?: string;
  reason: string;
  evidence: Evidence[];
  manualStepIds?: string[];
};

export type RuleResult = IssueResult | PassResult | NotMeasurableResult;

export type ManualState = {
  step: ManualStep;
  status: ManualStep["status"];
  answer: ManualAnswer | null;
  evidence: Evidence;
};

export type EvidenceInput = { count?: number | null; dateRange?: DateRange | null; exclusions?: string[]; note?: string };

export type RuleContext = {
  calibration: Calibration;
  now: Date;
  window: DateRange;
  metrics: Metric[];
  pseudonyms: Pseudonymiser;
  /** The output of an extractor. */
  data<T>(extractor: Extractor<T>): T;
  /** Complete unless the run is a set-up review, which reads no CRM records. */
  scope?: RunScope;
  /** Turns a source into evidence, stamped with this run's time. */
  ev(source: SourceInfo, input?: EvidenceInput): Evidence;
  /** Raises a manual step (once) and returns where it stands. */
  manual(key: string): ManualState;
  /** A user's token. Never put a real name in a finding. */
  userLabel(userId: string): string;
};

export type Rule = {
  id: string;
  version: number;
  domain: string;
  title: string;
  /** Needs an extract that reads CRM records, so a set-up review leaves the rule out rather than report it not measurable. */
  records?: true;
  evaluate(ctx: RuleContext): RuleResult | RuleResult[];
};

export const notSized = (reason: string): Value => ({ kind: "not_sized", reason });

/** "1 stage", "2 stages". Pass `many` where adding an s is wrong. */
export const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** A pct with one decimal place, for text. The exact figure stays in `facts`. */
export const pct = (part: number, whole: number): number => (whole === 0 ? 0 : Math.round((part / whole) * 1000) / 10);

export type Hotspot = { dimension: string; key: string; n: number; hits: number; rate: number };

export type SegmentedRate = {
  n: number;
  hits: number;
  rate: number;
  segmentsChecked: SegmentCheck[];
  hotspots: Hotspot[];
};

/**
 * Rule 4 as a helper: a rate overall and across each cut of the data, with the
 * segments that break a limit even when the total looks fine.
 */
export function segmentRate<R>(
  rows: R[],
  isHit: (row: R) => boolean,
  dimensions: Record<string, (row: R) => string>,
  options: { segmentLimit: number; minSegment: number },
): SegmentedRate {
  const hits = rows.filter(isHit).length;
  const segmentsChecked: SegmentCheck[] = [];
  const hotspots: Hotspot[] = [];
  for (const [dimension, keyOf] of Object.entries(dimensions)) {
    const groups = new Map<string, { n: number; hits: number }>();
    for (const row of rows) {
      const group = groups.get(keyOf(row)) ?? { n: 0, hits: 0 };
      group.n++;
      if (isHit(row)) group.hits++;
      groups.set(keyOf(row), group);
    }
    segmentsChecked.push({ dimension, segments: groups.size });
    for (const [key, group] of groups) {
      const rate = pct(group.hits, group.n);
      if (group.n >= options.minSegment && rate > options.segmentLimit) hotspots.push({ dimension, key, n: group.n, hits: group.hits, rate });
    }
  }
  hotspots.sort((a, b) => b.rate - a.rate || a.dimension.localeCompare(b.dimension) || a.key.localeCompare(b.key));
  return { n: rows.length, hits, rate: pct(hits, rows.length), segmentsChecked, hotspots };
}
