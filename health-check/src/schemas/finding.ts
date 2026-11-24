import { z } from "zod";
import { DomainId, NotMeasurableKind, SegmentCheck } from "./common.ts";
import { Evidence } from "./evidence.ts";

export const Severity = z.enum(["critical", "high", "medium", "low", "info"]);
export const Confidence = z.enum(["high", "medium", "low"]);

export const RecommendationType = z.enum(["remove", "deactivate", "merge", "change", "investigate", "none"]);
export type RecommendationType = z.infer<typeof RecommendationType>;

// Anything in this set can damage a customer org if it's wrong.
export const DESTRUCTIVE_RECOMMENDATIONS: ReadonlySet<RecommendationType> = new Set(["remove", "deactivate", "merge"]);

export const GuardStatus = z.enum(["passed", "failed", "unknown"]);
export type GuardStatus = z.infer<typeof GuardStatus>;

// "unknown" means the check couldn't be completed. It blocks exactly as a
// failure does: a guard we couldn't run has told us nothing.
export const Guard = z.object({
  guardId: z.string().min(1),
  subject: z.string().min(1),
  status: GuardStatus,
  detail: z.string().min(1),
  evidence: z.array(Evidence).min(1),
});
export type Guard = z.infer<typeof Guard>;

export const Value = z.discriminatedUnion("kind", [
  z.object({
    kind: z.enum(["revenue", "licence_cost", "hours"]),
    amount: z.number().nonnegative(),
    unit: z.string().min(1),
    period: z.string().min(1),
    assumptions: z.array(z.string().min(1)).min(1),
  }),
  z.object({ kind: z.literal("not_sized"), reason: z.string().min(1) }),
]);
export type Value = z.infer<typeof Value>;

export const Effort = z.object({
  size: z.enum(["XS", "S", "M", "L", "XL"]),
  note: z.string().optional(),
});
export type Effort = z.infer<typeof Effort>;

// Hours to fix an issue, as a range in whole hours with what it covers (DECISIONS.md
// 628). Estimates awaiting the owner's review: data only, printed in no document yet.
export const EffortHours = z
  .object({
    low: z.number().int().min(1),
    high: z.number().int().min(1),
    basis: z.string().min(1),
    // Set when the cap left units to be scoped separately: the range covers only part of the work (DECISIONS.md 636).
    partial: z.literal(true).optional(),
  })
  .refine((h) => h.high >= h.low, { message: "The high end of an hours range can't be below the low end", path: ["high"] });
export type EffortHours = z.infer<typeof EffortHours>;

// How long a fix takes, by band, worked out by code from effortHours.high
// (DECISIONS.md 636): never set by hand, so it can't disagree with the hours.
export const TimeBand = z.enum(["small", "medium", "large"]);
export type TimeBand = z.infer<typeof TimeBand>;

// How risky or skilled a fix is, declared per rule (DECISIONS.md 636). This is
// not the org's complexity score (DECISIONS.md 614, complexity.json), which
// describes the whole org: it describes one finding's fix.
export const FixComplexity = z.enum(["simple", "moderate", "complex"]);
export type FixComplexity = z.infer<typeof FixComplexity>;

/** The longest a fix prompt may be, in characters (DECISIONS.md 628). */
export const FIX_PROMPT_MAX = 2500;

// Which characters of a sentence were chosen inside the org (DECISIONS.md 655), in
// UTF-16 code units, end exclusive, kept with the SHA-256 of the sentence they
// were made for: a marking whose sentence has changed since is never used.
export const OrgWordSpan = z.object({ start: z.number().int().nonnegative(), end: z.number().int().positive() });
export type OrgWordSpan = z.infer<typeof OrgWordSpan>;
export const MarkedSentence = z.object({ spans: z.array(OrgWordSpan), textSha256: z.string().regex(/^[0-9a-f]{64}$/) });
export const FindingOrgWords = z.object({
  title: MarkedSentence.optional(),
  summary: MarkedSentence.optional(),
  recommendation: MarkedSentence.optional(),
  notMeasurableReason: MarkedSentence.optional(),
  // A value's reason, and its assumptions all together, one marking each in order, or none (DECISIONS.md 663).
  valueReason: MarkedSentence.optional(),
  valueAssumptions: z.array(MarkedSentence).optional(),
  // The hours' basis (DECISIONS.md 628): the fix table's words and counts only, so marked with no spans when made.
  hoursBasis: MarkedSentence.optional(),
  // The fix prompt (DECISIONS.md 672): its subjects' labels and the finding's own marked sentences, kept only when every part is vouched for.
  fixPrompt: MarkedSentence.optional(),
});
export type FindingOrgWords = z.infer<typeof FindingOrgWords>;

// Labels are pseudonymised tokens (see src/privacy). A real name in a finding is a bug.
export const Subject = z.object({ type: z.string().min(1), label: z.string().min(1) });
export type Subject = z.infer<typeof Subject>;

export const FindingStatus = z.enum(["issue", "passed", "not_measurable"]);

export const Finding = z
  .object({
    id: z.string().min(1),
    ruleId: z.string().min(1),
    ruleVersion: z.number().int().positive(),
    domain: DomainId,
    status: FindingStatus,
    title: z.string().min(1),
    summary: z.string().min(1),
    recommendation: z.string(),
    recommendationType: RecommendationType,
    severity: Severity,
    confidence: Confidence,
    evidence: z.array(Evidence).min(1),
    value: Value,
    effort: Effort,
    owner: z.string().min(1),
    dependencies: z.array(z.string()),
    guards: z.array(Guard),
    segmentsChecked: z.array(SegmentCheck),
    segmentExemptReason: z.string().nullable(),
    subjects: z.array(Subject),
    // Every figure in the summary, by name. The Milestone 3 number check reads these.
    facts: z.record(z.string(), z.number()),
    notMeasurableReason: z.string().nullable(),
    // Which of the four kinds the reason is (DECISIONS.md 627). Only on a not measurable finding.
    notMeasurableKind: NotMeasurableKind.optional(),
    manualStepIds: z.array(z.string()),
    // The engine sets these on an issue only, where the rule declares them (DECISIONS.md 628).
    // Not enforced here, so a stored result read back is never refused for them.
    effortHours: EffortHours.optional(),
    // From effortHours.high, present exactly when it is (DECISIONS.md 636).
    timeBand: TimeBand.optional(),
    // The rule's fix complexity, on an issue only (DECISIONS.md 636). Not the org's complexity score.
    fixComplexity: FixComplexity.optional(),
    // Made by code from fixed text and the finding's own facts, never by Claude, and never naming a person or a record.
    fixPrompt: z.string().min(1).max(FIX_PROMPT_MAX).optional(),
    // The sentences the engine can vouch for, each with the org's characters marked (DECISIONS.md 655). Absent: unknown.
    orgWords: FindingOrgWords.optional(),
    // Its amounts are in a currency not known by name, printed as plain numbers (DECISIONS.md 700): the line saying so is
    // the page's, the section's and the digest's (value.currencyNote), never the finding's own text. Absent otherwise.
    amountsUnnamed: z.literal(true).optional(),
  })
  .superRefine((f, ctx) => {
    if (f.status === "not_measurable" && !f.notMeasurableReason) {
      ctx.addIssue({ code: "custom", path: ["notMeasurableReason"], message: "A not measurable finding must give the reason" });
    }
    if (f.status !== "not_measurable" && f.notMeasurableKind !== undefined) {
      ctx.addIssue({ code: "custom", path: ["notMeasurableKind"], message: "Only a not measurable finding has a kind" });
    }
    if (DESTRUCTIVE_RECOMMENDATIONS.has(f.recommendationType)) {
      if (f.status !== "issue") {
        ctx.addIssue({ code: "custom", path: ["recommendationType"], message: "Only an issue can recommend a destructive change" });
      }
      if (f.guards.length === 0) {
        ctx.addIssue({ code: "custom", path: ["guards"], message: `A ${f.recommendationType} recommendation needs guards` });
      }
      if (f.guards.some((g) => g.status !== "passed")) {
        ctx.addIssue({ code: "custom", path: ["guards"], message: `A ${f.recommendationType} recommendation needs every guard to pass` });
      }
    }
    if (f.status === "passed" && f.segmentsChecked.length === 0 && !f.segmentExemptReason) {
      ctx.addIssue({ code: "custom", path: ["segmentsChecked"], message: "A pass must show its segments or say why it can't be segmented" });
    }
  });
export type Finding = z.infer<typeof Finding>;
