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
/** An area's label, and a not measurable area's reason (DECISIONS.md 693). */
const AreaOrgWords = LabelOrgWords.extend({ reason: SentenceOrgWords.optional() });
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
   * How hard the fix is, worked out by code from the hours above and the fix
   * complexity (DECISIONS.md 714): "quick_fix", "needs_planning" or "project",
   * said as "Quick fix", "Needs planning" and "A project". Absent with no hours:
   * not estimated, never a guess.
   */
  difficulty: z.enum(["quick_fix", "needs_planning", "project"]).optional(),
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
  /**
   * Only on a rule we have loosened since an earlier version (DECISIONS.md 732): the
   * rule's version, on every status. Absent everywhere else, so a digest of any other
   * rule is as before.
   */
  ruleVersion: z.number().int().positive().optional(),
  /**
   * Only on such a rule's pass, where the engine can judge it: the earlier versions,
   * ascending, under which this pass's reading would still have been an issue. Empty: it
   * would have passed under every one, so a pass after any earlier issue is a fix. Absent:
   * unknown, never a fix we can show.
   */
  issueUnder: z.array(z.number().int().positive()).optional(),
  /** Its amounts are plain numbers in a currency not known by name (DECISIONS.md 700): show value.currencyNote beside them. Absent otherwise. */
  amountsUnnamed: z.literal(true).optional(),
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
      /** A sized value's unit and period (DECISIONS.md 693): optional, so a digest made before reads as before. */
      valueUnit: SentenceOrgWords.optional(),
      valuePeriod: SentenceOrgWords.optional(),
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
  if (marks.valueUnit && (v.kind === "not_sized" || !fits(v.unit, marks.valueUnit))) ctx.addIssue({ code: "custom", path: ["orgWords", "valueUnit"], message: "Spans must fit the value's unit" });
  if (marks.valuePeriod && (v.kind === "not_sized" || !fits(v.period, marks.valuePeriod))) ctx.addIssue({ code: "custom", path: ["orgWords", "valuePeriod"], message: "Spans must fit the value's period" });
});
export type DigestFinding = z.infer<typeof DigestFinding>;

/**
 * A metric's texts marked (DECISIONS.md 693): optional, so a digest made before
 * it reads as before; an item of a list is null where its marks are unknown.
 */
export const MetricOrgWords = z.object({
  label: SentenceOrgWords.optional(),
  definition: SentenceOrgWords.optional(),
  reason: SentenceOrgWords.optional(),
  assumptions: z.array(SentenceOrgWords.nullable()).optional(),
  exclusions: z.array(SentenceOrgWords.nullable()).optional(),
});

/**
 * A measure's headline only, as corrected: never its segments, series or stats,
 * save one: a duration's average, beside the median that is its value, for
 * every such duration whose average is more than 10% from its median, from five
 * values or more, with a median above 0, whether or not a document shows that
 * measure (DECISIONS.md 709). Optional, so a digest made before it reads as before.
 */
export const DigestMetric = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("measured"),
    id: Id,
    domain: DomainId,
    label: Text,
    definition: Text,
    unit: z.enum(["percent", "days", "count", "currency", "ratio"]),
    value: Amount,
    average: Amount.optional(),
    n: z.number().int().nonnegative(),
    dateRange: z.object({ from: CalendarDate, to: CalendarDate }).nullable(),
    assumptions: z.array(Text),
    exclusions: z.array(Text),
    orgWords: MetricOrgWords.optional(),
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
    orgWords: MetricOrgWords.optional(),
  }),
]).superRefine((m, ctx) => {
  const marks = m.orgWords;
  if (!marks) return;
  const bad = (path: string, text: string | undefined, held: SentenceOrgWords | null | undefined) => {
    if (held && !fits(text, held ?? undefined)) ctx.addIssue({ code: "custom", path: ["orgWords", path], message: "Spans must fit their text" });
  };
  bad("label", m.label, marks.label);
  bad("definition", m.definition, marks.definition);
  if (marks.reason && m.status !== "not_measurable") ctx.addIssue({ code: "custom", path: ["orgWords", "reason"], message: "Only a not measurable metric has a reason" });
  if (m.status === "not_measurable") bad("reason", m.reason, marks.reason);
  const lists = m.status === "measured" ? { assumptions: m.assumptions, exclusions: m.exclusions } : { assumptions: [], exclusions: [] };
  for (const key of ["assumptions", "exclusions"] as const) {
    const held = marks[key];
    if (!held) continue;
    if (held.length !== lists[key].length) ctx.addIssue({ code: "custom", path: ["orgWords", key], message: "One marking, or null, for each text" });
    else held.forEach((h, i) => bad(key, lists[key][i], h));
  }
});
export type DigestMetric = z.infer<typeof DigestMetric>;

export const DigestArea = z
  .discriminatedUnion("status", [
    z.object({ status: z.literal("measured"), domain: DomainId, label: Text, score: z.number().min(0).max(100), findingIds: Ids, orgWords: LabelOrgWords.optional() }),
    // The reason is unknown, never marked.
    z.object({ status: z.literal("not_measurable"), domain: DomainId, label: Text, reason: Text, notMeasurableKind: NotMeasurableKind.optional(), orgWords: AreaOrgWords.optional() }),
  ])
  .superRefine((a, ctx) => {
    if (!fits(a.label, a.orgWords?.label)) ctx.addIssue({ code: "custom", path: ["orgWords", "label"], message: "Spans must fit the label" });
    const reason = a.orgWords && "reason" in a.orgWords ? a.orgWords.reason : undefined;
    if (reason && (a.status !== "not_measurable" || !fits(a.reason, reason))) ctx.addIssue({ code: "custom", path: ["orgWords", "reason"], message: "Spans must fit the reason" });
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
  /**
   * On an automatic run whose currency isn't known by name (`currency` then
   * "XXX", never printed), the line said beside its amounts, all ours
   * (DECISIONS.md 700). Absent on every other run.
   */
  currencyNote: Text.optional(),
  /** The currency the org's default locale suggests, for the portal's question: the org's word, never the currency. Only with currencyNote. */
  currencySuggested: z.string().regex(/^[A-Z]{3}$/).optional(),
  orgWords: z.object({ currencyNote: SentenceOrgWords.optional(), currencySuggested: SentenceOrgWords.optional() }).optional(),
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
  /**
   * The plan's critical and high issues whose difficulty is a quick fix
   * (DECISIONS.md 714), in the plan's order (its quick wins, phases, then after
   * day 90), for a reader to start with. Absent when there are none. It moves
   * nothing: every id is still where the plan put it.
   */
  quickFixes: Ids.optional(),
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
      /** The note marked (DECISIONS.md 693): a fixed sentence of ours. */
      orgWords: z.object({ note: SentenceOrgWords.optional() }).optional(),
    })
    .refine((h) => (h.notTotalledFindingIds === undefined) === (h.note === undefined), { message: "The findings left out of the total and the sentence saying so go together" })
    .optional(),
});
export type DigestPlan = z.infer<typeof DigestPlan>;

export const DigestOverall = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("measured"),
    score: z.number().int().min(0).max(100),
    grade: z.enum(["A", "B", "C", "D", "E"]),
    areaDomains: z.array(DomainId),
    /**
     * Said under the score when it leaves out areas we couldn't score (DECISIONS.md
     * 710), all ours: "Your Barometer score leaves out the areas we couldn't
     * score: <areas>." Absent when no area is left out.
     */
    caveat: Text.optional(),
    orgWords: z.object({ caveat: SentenceOrgWords.optional() }).optional(),
  }),
  z.object({ status: z.literal("not_measurable"), reason: Text, orgWords: z.object({ reason: SentenceOrgWords.optional() }).optional() }),
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
  /**
   * What the org's edition doesn't include, said once for the run (DECISIONS.md
   * 710), all ours: the checks it left unmeasured say only "Needs X, which
   * Professional Edition doesn't include." Show it once wherever such a check
   * is listed. Absent for an org the edition left nothing out of.
   */
  editionNote: Text.optional(),
  orgWords: z.object({ editionNote: SentenceOrgWords.optional() }).optional(),
  /**
   * Every rule this run checked (DECISIONS.md 718): ran to a result of any
   * status, not measurable included, so each has a finding. Sorted by code
   * unit, each once. Never an insight's, never a rule the run's scope or
   * modules left out or that had nothing to compare, and the code checker's
   * six only on a run that had its step. Ids, never text. Absent on a digest
   * of a run analysed before it was kept: then nothing is known of what didn't
   * run. A digest made before 25 November 2026 can list the calibration check
   * on an automatic run with no finding: that silence is not a pass.
   */
  rulesRun: Ids.optional(),
  /**
   * Every served rule the engine knew when this run was analysed, sorted, each
   * once, always holding rulesRun (DECISIONS.md 718). A rule an earlier run
   * checked that is not here has been retired. Absent with rulesRun.
   */
  rulesKnown: Ids.optional(),
});
export type RunDigest = z.infer<typeof RunDigest>;

/** The schema's own keys, so a path into a digest can be logged by these and indices alone, never by a key read from it. */
export const DIGEST_KEYS: ReadonlySet<string> = new Set([
  "version", "runId", "product", "reviewed", "asOf", "madeAt", "baselineRunId", "overall", "areas", "findings", "metrics", "value", "plan", "withheldNames",
  "status", "score", "grade", "areaDomains", "reason", "domain", "label", "findingIds", "notMeasurableKind", "id", "ruleId", "severity", "confidence",
  "title", "summary", "recommendation", "recommendationType", "kind", "amount", "unit", "period", "assumptions", "effortSize", "notMeasurableReason",
  "definition", "n", "dateRange", "from", "to", "exclusions", "currency", "total", "byKind", "revenue_recoverable", "conversion_uplift", "licence_waste",
  "time_cost", "exposure", "groups", "lines", "leftOut", "realisation", "counted", "lineIds", "low", "expected", "high", "group", "annual", "added",
  "phases", "later", "quickWins", "quickFixes", "difficulty", "alsoFor", "key", "effortHours", "basis", "partial", "timeBand", "fixComplexity", "hoursTotal", "partialFindingIds", "withoutHoursFindingIds", "notTotalledFindingIds", "note",
  "checkerRead", "readClasses", "readTriggers", "alreadyListed", "orgWords", "orgWordsMarked", "start", "end", "valueReason", "valueAssumptions", "hoursBasis", "fixPrompt", "textSha256",
  "valueUnit", "valuePeriod", "editionNote", "caveat", "rulesRun", "rulesKnown", "ruleVersion", "issueUnder",
]);
