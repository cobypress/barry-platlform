import { z } from "zod";
import { CalendarDate, DomainId, NotMeasurableKind, Timestamp } from "./common.ts";
import { FIX_PROMPT_MAX } from "./finding.ts";

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
// Objects are not strict, so a later engine can add optional fields and a
// console still on this copy reads the row, dropping what it doesn't know. The
// hours and bands of decisions 628 and 636 are here, and an issue's fix prompt
// with its org words marked and the hash of the text they were made for
// (DECISIONS.md 672): a prompt without marks is never served. A finding's
// `orgWords` (DECISIONS.md 655) marks which characters of its sentences the org
// chose: positions, not figures, so nothing traces them. Its value's reason and
// assumptions, its hours' basis, a plan phase's title, an area's label and a
// value group's label are marked the same way (DECISIONS.md 663).

export const DIGEST_VERSION = 1;

/** What a title, summary, recommendation or reason says in place of one that holds a name, an email, an id or a secret. */
export const NAMED_PEOPLE_SENTENCE = "This issue involves named people. It's described in full in your report.";

const Text = z.string().min(1);
/** An engine id: a finding, rule, metric, group or run id. Letters, digits and . _ : - only, so no sentence fits. */
const Id = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/);
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

/** A finding's hours to fix (DECISIONS.md 628), whole hours, with what the range covers. */
export const DigestHours = z.object({ low: z.number().int().min(1), high: z.number().int().min(1), basis: Text, partial: z.literal(true).optional() });

/** Characters a sentence took from the org (DECISIONS.md 655): UTF-16 code units into the sentence as the digest holds it, end exclusive. */
export const DigestSpan = z.object({ start: z.number().int().nonnegative(), end: z.number().int().positive() });
export type DigestSpan = z.infer<typeof DigestSpan>;

/**
 * A sentence the engine vouches for: every character the org chose lies in one
 * of `orgWords`, and every other is the engine's own. No spans: the engine's
 * words only. A sentence with no marking is unknown, as every one was before.
 */
export const SentenceOrgWords = z.object({ orgWordsMarked: z.literal(true), orgWords: z.array(DigestSpan) });
export type SentenceOrgWords = z.infer<typeof SentenceOrgWords>;

/**
 * A fix prompt's marks (DECISIONS.md 672): as a sentence's, with the SHA-256, in
 * lower-case hex, of the prompt as UTF-8 that they were made for, so a reader
 * whose text differs by a character uses none of them.
 */
export const FixPromptOrgWords = SentenceOrgWords.extend({ textSha256: z.string().regex(/^[0-9a-f]{64}$/) });
export type FixPromptOrgWords = z.infer<typeof FixPromptOrgWords>;

/** Spans each non-empty, inside the sentence, sorted and apart. */
export function spansFitSentence(text: string, spans: readonly DigestSpan[]): boolean {
  let from = 0;
  for (const s of spans) {
    if (s.start < from || s.end <= s.start || s.end > text.length) return false;
    from = s.end + 1;
  }
  return true;
}

const MARKED_SENTENCES = ["title", "summary", "recommendation", "notMeasurableReason"] as const;

/** A sentence's marks, when it has them, fit it. */
const fits = (text: string | undefined, marked: SentenceOrgWords | undefined): boolean => !marked || (typeof text === "string" && spansFitSentence(text, marked.orgWords));

/** A label or a title marked (DECISIONS.md 663): absent, it is unknown. */
const LabelOrgWords = z.object({ label: SentenceOrgWords.optional() });
const TitleOrgWords = z.object({ title: SentenceOrgWords.optional() });

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
  /** An issue's hours to fix, and the band worked out from their high end (DECISIONS.md 628, 636). */
  effortHours: DigestHours.optional(),
  timeBand: z.enum(["small", "medium", "large"]).optional(),
  /** How risky or skilled the fix is, by rule (DECISIONS.md 636). Not the org's complexity score. */
  fixComplexity: z.enum(["simple", "moderate", "complex"]).optional(),
  /**
   * An issue's fix prompt as the finding holds it (DECISIONS.md 628, 672), whole,
   * never cut: one longer than the limit, or one the digest's guard would change,
   * is left out. Served only with its marks at `orgWords.fixPrompt`.
   */
  fixPrompt: z.string().min(1).max(FIX_PROMPT_MAX).optional(),
  /**
   * On an issue or a pass of the code checker's six groups only (DECISIONS.md
   * 673): what it read and how many findings went to our own scan's checks, so a
   * re-check calls a pass fixed only when as much was read and none went there.
   * Counts only. Absent everywhere else, and when the finding's facts lack them.
   */
  checkerRead: z.object({ readClasses: z.number().int().nonnegative(), readTriggers: z.number().int().nonnegative(), alreadyListed: z.number().int().nonnegative() }).optional(),
  /** By sentence, the ones the engine can vouch for, with the org's characters marked (DECISIONS.md 655). */
  orgWords: z
    .object({
      title: SentenceOrgWords.optional(),
      summary: SentenceOrgWords.optional(),
      recommendation: SentenceOrgWords.optional(),
      notMeasurableReason: SentenceOrgWords.optional(),
      /** A not sized value's reason (DECISIONS.md 663). */
      valueReason: SentenceOrgWords.optional(),
      /** A sized value's assumptions, one marking each in order, present only when every one is marked (DECISIONS.md 663). */
      valueAssumptions: z.array(SentenceOrgWords).optional(),
      /** The hours' basis (DECISIONS.md 663). */
      hoursBasis: SentenceOrgWords.optional(),
      /** The fix prompt, with its text's hash (DECISIONS.md 672). */
      fixPrompt: FixPromptOrgWords.optional(),
    })
    .optional(),
}).superRefine((f, ctx) => {
  for (const key of MARKED_SENTENCES) {
    const marked = f.orgWords?.[key];
    if (!marked) continue;
    const text = f[key];
    if (typeof text !== "string" || !spansFitSentence(text, marked.orgWords)) ctx.addIssue({ code: "custom", path: ["orgWords", key], message: "Spans must fit their sentence" });
  }
  const marks = f.orgWords;
  if (!marks) return;
  const v = f.value;
  if (marks.valueReason && (v.kind !== "not_sized" || !fits(v.reason, marks.valueReason))) ctx.addIssue({ code: "custom", path: ["orgWords", "valueReason"], message: "Spans must fit a not sized value's reason" });
  if (marks.valueAssumptions) {
    const held = v.kind === "not_sized" ? null : v.assumptions;
    if (!held || held.length !== marks.valueAssumptions.length || !marks.valueAssumptions.every((m, i) => fits(held[i], m)))
      ctx.addIssue({ code: "custom", path: ["orgWords", "valueAssumptions"], message: "One marking for each assumption, each fitting it" });
  }
  if (marks.hoursBasis && !fits(f.effortHours?.basis, marks.hoursBasis)) ctx.addIssue({ code: "custom", path: ["orgWords", "hoursBasis"], message: "Spans must fit the hours' basis" });
  if (marks.fixPrompt && !fits(f.fixPrompt, marks.fixPrompt)) ctx.addIssue({ code: "custom", path: ["orgWords", "fixPrompt"], message: "Spans must fit the fix prompt" });
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

export const DigestArea = z
  .discriminatedUnion("status", [
    z.object({ status: z.literal("measured"), domain: DomainId, label: Text, score: z.number().min(0).max(100), findingIds: Ids, orgWords: LabelOrgWords.optional() }),
    // The reason is unknown, never marked.
    z.object({ status: z.literal("not_measurable"), domain: DomainId, label: Text, reason: Text, notMeasurableKind: NotMeasurableKind.optional(), orgWords: LabelOrgWords.optional() }),
  ])
  .superRefine((a, ctx) => {
    if (!fits(a.label, a.orgWords?.label)) ctx.addIssue({ code: "custom", path: ["orgWords", "label"], message: "Spans must fit the label" });
  });
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
export const DigestValueGroup = z
  .object({
    id: Id,
    kind: ValueKind,
    label: Text,
    realisation: DigestRange,
    counted: Id,
    lineIds: Ids,
    orgWords: LabelOrgWords.optional(),
  })
  .superRefine((g, ctx) => {
    if (!fits(g.label, g.orgWords?.label)) ctx.addIssue({ code: "custom", path: ["orgWords", "label"], message: "Spans must fit the label" });
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
  phases: z.array(
    z
      .object({ key: z.enum(["stabilise", "fix", "build"]), title: Text, from: z.number().int().positive(), to: z.number().int().positive(), findingIds: Ids, orgWords: TitleOrgWords.optional() })
      .superRefine((p, ctx) => {
        if (!fits(p.title, p.orgWords?.title)) ctx.addIssue({ code: "custom", path: ["orgWords", "title"], message: "Spans must fit the title" });
      }),
  ),
  later: Ids,
  quickWins: Ids,
  /** By an action's finding id, the other issues from the same rule the same action covers. */
  alsoFor: z.record(Id, Ids),
  /**
   * The hours to fix every finding the plan holds (its quick wins, phases and
   * later list, and the issues each action also covers), each once, summed by
   * code, with the finding ids it adds (DECISIONS.md 636). Never the whole on its
   * own: `partialFindingIds` are those whose hours cover only part of their work,
   * and `withoutHoursFindingIds` the plan's issues with no hours at all, both
   * always listed, empty or not. Absent when no finding in the plan has hours.
   */
  hoursTotal: z
    .object({
      low: z.number().int().nonnegative(),
      high: z.number().int().nonnegative(),
      findingIds: Ids,
      partialFindingIds: Ids,
      withoutHoursFindingIds: Ids,
      // The code checker's findings, each with its own hours and left out of the total, and the fixed sentence saying so (DECISIONS.md 673). Both or neither.
      notTotalledFindingIds: Ids.optional(),
      note: Text.optional(),
    })
    .refine((h) => (h.notTotalledFindingIds === undefined) === (h.note === undefined), { message: "The findings left out of the total and the sentence saying so go together" })
    .optional(),
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
  /** False for a pulse, which nobody reviews, and for an automatic run, which no person reviewed (DECISIONS.md 688); true for a consultant-led check or re-check, made once its documents are. */
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

/** The schema's own keys, so a path into a digest can be logged by these and indices alone, never by a key read from it. */
export const DIGEST_KEYS: ReadonlySet<string> = new Set([
  "version", "runId", "product", "reviewed", "asOf", "madeAt", "baselineRunId", "overall", "areas", "findings", "metrics", "value", "plan", "withheldNames",
  "status", "score", "grade", "areaDomains", "reason", "domain", "label", "findingIds", "notMeasurableKind", "id", "ruleId", "severity", "confidence",
  "title", "summary", "recommendation", "recommendationType", "kind", "amount", "unit", "period", "assumptions", "effortSize", "notMeasurableReason",
  "definition", "n", "dateRange", "from", "to", "exclusions", "currency", "total", "byKind", "revenue_recoverable", "conversion_uplift", "licence_waste",
  "time_cost", "exposure", "groups", "lines", "leftOut", "realisation", "counted", "lineIds", "low", "expected", "high", "group", "annual", "added",
  "phases", "later", "quickWins", "alsoFor", "key", "effortHours", "basis", "partial", "timeBand", "fixComplexity", "hoursTotal", "partialFindingIds", "withoutHoursFindingIds", "notTotalledFindingIds", "note",
  "checkerRead", "readClasses", "readTriggers", "alreadyListed", "orgWords", "orgWordsMarked", "start", "end", "valueReason", "valueAssumptions", "hoursBasis", "fixPrompt", "textSha256",
]);
