import { z } from "zod";
import { CalendarDate, DomainId, NotMeasurableKind, Timestamp } from "./common.ts";

// The run digest (DECISIONS.md 632): what the customer's connector may serve of
// one run, made by code on the worker from the same built report the documents
// are made from, and kept in the console's HealthCheckRunDigest row. The
// console copies this file word for word.
//
// Every figure sits under what it traces to: the digest names its run, a
// finding's figures sit under its finding id, a measure's under its metric id,
// an area's under its domain, and every total lists the ids it adds up. Text
// that could name a person, an email, a record id or a secret has been
// replaced by NAMED_PEOPLE_SENTENCE. Nothing record-level is kept: no subjects,
// evidence, guards, segments, record ids, tokens or the pseudonym map.
//
// Objects are not strict, so a later engine can add optional fields (the
// hours and fix prompts of decision 628 among them) and a console still on
// this copy reads the row, dropping what it doesn't know.

export const DIGEST_VERSION = 1;

/** What a title, summary, recommendation or reason says in place of one that holds a name, an email, an id or a secret. */
export const NAMED_PEOPLE_SENTENCE = "This issue involves named people. It's described in full in your report.";

const Text = z.string().min(1);
const Id = z.string().min(1).max(200);
const Amount = z.number().finite();
const Ids = z.array(Id);
const Severity = z.enum(["critical", "high", "medium", "low", "info"]);

/** Low, expected and high, in the digest's currency. */
export const DigestRange = z.object({ low: Amount, expected: Amount, high: Amount });
export type DigestRange = z.infer<typeof DigestRange>;

/** A finding's own figure as corrected, without its assumptions' working beyond the guarded text. */
export const DigestValue = z.discriminatedUnion("kind", [
  z.object({ kind: z.enum(["revenue", "licence_cost", "hours"]), amount: Amount.nonnegative(), unit: Text, period: Text, assumptions: z.array(Text) }),
  z.object({ kind: z.literal("not_sized"), reason: Text }),
]);

export const DigestFinding = z.object({
  id: Id,
  ruleId: Id,
  domain: DomainId,
  status: z.enum(["issue", "passed", "not_measurable"]),
  severity: Severity,
  confidence: z.enum(["high", "medium", "low"]),
  /** At team level, as the executive report says it, or NAMED_PEOPLE_SENTENCE. */
  title: Text,
  summary: Text,
  recommendation: z.string(),
  recommendationType: z.enum(["remove", "deactivate", "merge", "change", "investigate", "none"]),
  value: DigestValue,
  effortSize: z.enum(["XS", "S", "M", "L", "XL"]),
  notMeasurableReason: Text.nullable(),
  notMeasurableKind: NotMeasurableKind.optional(),
});
export type DigestFinding = z.infer<typeof DigestFinding>;

/** A measure's headline only, as corrected: never its segments, series or stats. */
export const DigestMetric = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("measured"),
    id: Id,
    domain: DomainId,
    label: Text,
    definition: Text,
    unit: z.enum(["percent", "days", "count", "currency", "ratio"]),
    value: Amount,
    n: z.number().int().nonnegative(),
    dateRange: z.object({ from: CalendarDate, to: CalendarDate }).nullable(),
    assumptions: z.array(Text),
    exclusions: z.array(Text),
  }),
  z.object({
    status: z.literal("not_measurable"),
    id: Id,
    domain: DomainId,
    label: Text,
    definition: Text,
    unit: z.enum(["percent", "days", "count", "currency", "ratio"]),
    reason: Text,
    notMeasurableKind: NotMeasurableKind.optional(),
  }),
]);
export type DigestMetric = z.infer<typeof DigestMetric>;

export const DigestArea = z.discriminatedUnion("status", [
  z.object({ status: z.literal("measured"), domain: DomainId, label: Text, score: z.number().min(0).max(100), findingIds: Ids }),
  z.object({ status: z.literal("not_measurable"), domain: DomainId, label: Text, reason: Text, notMeasurableKind: NotMeasurableKind.optional() }),
]);
export type DigestArea = z.infer<typeof DigestArea>;

const ValueKind = z.enum(["revenue_recoverable", "conversion_uplift", "licence_waste", "time_cost", "exposure"]);

/**
 * One finding's line in the value model: its own figure over 12 months, the
 * share of it counted at each case, and what it adds to the total after the
 * overlap rules (DECISIONS.md 362). `id` is the finding id, or the survey's
 * own line for the time the team says it loses.
 */
export const DigestValueLine = z.object({ id: Id, ruleId: Id, kind: ValueKind, group: Id, annual: Amount, low: Amount, expected: Amount, high: Amount, added: Amount });

/** A group of figures that describe the same money: only `counted` is added, at the group's realisation rates. */
export const DigestValueGroup = z.object({
  id: Id,
  kind: ValueKind,
  label: Text,
  realisation: DigestRange,
  counted: Id,
  lineIds: Ids,
});

/** A total and the value lines it adds up (each the counted line of its group). */
export const DigestTotal = DigestRange.extend({ lineIds: Ids });
export type DigestTotal = z.infer<typeof DigestTotal>;

export const DigestValueModel = z.object({
  currency: z.string().regex(/^[A-Z]{3}$/),
  /** Over the next 12 months: the headline kinds, each group counted once. Exposure is never in it. */
  total: DigestTotal,
  byKind: z.object({ revenue_recoverable: DigestTotal, conversion_uplift: DigestTotal, licence_waste: DigestTotal, time_cost: DigestTotal }),
  /** Shown beside the total and never added: the largest exposure stands for them all. */
  exposure: DigestTotal,
  groups: z.array(DigestValueGroup),
  lines: z.array(DigestValueLine),
  /** Findings with a figure the model couldn't place, by id. Their reasons are in the report. */
  leftOut: Ids,
});
export type DigestValueModel = z.infer<typeof DigestValueModel>;

/** The engine's own 90-day plan, by finding id, in its order. */
export const DigestPlan = z.object({
  phases: z.array(z.object({ key: z.enum(["stabilise", "fix", "build"]), title: Text, from: z.number().int().positive(), to: z.number().int().positive(), findingIds: Ids })),
  later: Ids,
  quickWins: Ids,
  /** By an action's finding id, the other issues from the same rule the same action covers. */
  alsoFor: z.record(Id, Ids),
});
export type DigestPlan = z.infer<typeof DigestPlan>;

export const DigestOverall = z.discriminatedUnion("status", [
  z.object({ status: z.literal("measured"), score: z.number().int().min(0).max(100), grade: z.enum(["A", "B", "C", "D", "E"]), areaDomains: z.array(DomainId) }),
  z.object({ status: z.literal("not_measurable"), reason: Text }),
]);

export const RunDigest = z.object({
  version: z.literal(DIGEST_VERSION),
  runId: Id,
  product: z.enum(["barometer", "pulse"]),
  /** False for a pulse, which nobody reviews; true for a check or re-check, made once its documents are. */
  reviewed: z.boolean(),
  asOf: Timestamp,
  madeAt: Timestamp,
  baselineRunId: Id.nullable(),
  overall: DigestOverall,
  areas: z.array(DigestArea),
  findings: z.array(DigestFinding),
  metrics: z.array(DigestMetric),
  value: DigestValueModel,
  plan: DigestPlan,
  /** SHA-256, in lower-case hex, of each inventory name that holds a known person's name. Never the names. */
  withheldNames: z.array(z.string().regex(/^[0-9a-f]{64}$/)),
});
export type RunDigest = z.infer<typeof RunDigest>;
