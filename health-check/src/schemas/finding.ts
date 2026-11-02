import { z } from "zod";
import { DomainId, SegmentCheck } from "./common.ts";
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
    manualStepIds: z.array(z.string()),
  })
  .superRefine((f, ctx) => {
    if (f.status === "not_measurable" && !f.notMeasurableReason) {
      ctx.addIssue({ code: "custom", path: ["notMeasurableReason"], message: "A not measurable finding must give the reason" });
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
