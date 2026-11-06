import { z } from "zod";
import { DomainId, Timestamp } from "./common.ts";

// Everything here is built in Milestones 3 to 5. The shapes are fixed now so
// the review console doesn't force a change to how findings are stored.

export const Score = z.discriminatedUnion("status", [
  z.object({
    status: z.literal("measured"),
    domain: DomainId,
    score: z.number().min(0).max(100),
    ruleSetVersion: z.string().min(1),
    weights: z.record(z.string(), z.number()),
    findingIds: z.array(z.string()),
  }),
  z.object({ status: z.literal("not_measurable"), domain: DomainId, ruleSetVersion: z.string().min(1), reason: z.string().min(1) }),
]);
export type Score = z.infer<typeof Score>;

export const ReportOutput = z.enum(["raw_notes", "executive_summary", "full_health_check"]);
export const SectionStatus = z.enum(["draft", "needs_input", "in_review", "approved"]);

const Approval = z.object({ approvedBy: z.string().min(1), approvedAt: Timestamp });

export const ReportSection = z
  .object({
    id: z.string().min(1),
    runId: z.string().min(1),
    output: ReportOutput,
    key: z.string().min(1),
    title: z.string().min(1),
    order: z.number().int().nonnegative(),
    status: SectionStatus,
    currentVersionId: z.string().nullable(),
    findingIds: z.array(z.string()),
    metricIds: z.array(z.string()),
    manualStepIds: z.array(z.string()),
    approval: Approval.nullable(),
  })
  .superRefine((s, ctx) => {
    if (s.status === "approved" && !s.approval) {
      ctx.addIssue({ code: "custom", path: ["approval"], message: "An approved section must record who approved it and when" });
    }
    if (s.status !== "approved" && s.approval) {
      ctx.addIssue({ code: "custom", path: ["approval"], message: "Only an approved section can hold an approval" });
    }
  });
export type ReportSection = z.infer<typeof ReportSection>;

export const NumberCheckResult = z.object({
  passed: z.boolean(),
  checkedAt: Timestamp,
  unmatched: z.array(z.string()),
  /** Figures only someone told us, each with where it was said ("40%: said in interview, Sales director"). */
  said: z.array(z.string()).optional(),
});

export const SectionVersion = z.object({
  id: z.string().min(1),
  sectionId: z.string().min(1),
  version: z.number().int().positive(),
  body: z.string(),
  source: z.enum(["engine", "claude_draft", "claude_proposal", "consultant_edit"]),
  changedBy: z.string().min(1),
  changedAt: Timestamp,
  changeSummary: z.string(),
  numberCheck: NumberCheckResult,
});
export type SectionVersion = z.infer<typeof SectionVersion>;

// A wording change from the Claude panel, held until a consultant decides.
export const WordingProposal = z
  .object({
    id: z.string().min(1),
    sectionId: z.string().min(1),
    baseVersionId: z.string().min(1),
    before: z.string(),
    after: z.string(),
    rationale: z.string(),
    model: z.string().min(1),
    status: z.enum(["proposed", "accepted", "rejected"]),
    decidedBy: z.string().nullable(),
    decidedAt: Timestamp.nullable(),
    numberCheck: NumberCheckResult,
  })
  .superRefine((p, ctx) => {
    if (p.status === "accepted" && !p.numberCheck.passed) {
      ctx.addIssue({ code: "custom", path: ["numberCheck"], message: "A proposal that fails the number check can't be accepted" });
    }
    if (p.status !== "proposed" && (!p.decidedBy || !p.decidedAt)) {
      ctx.addIssue({ code: "custom", path: ["decidedBy"], message: "A decided proposal must record who decided and when" });
    }
  });
export type WordingProposal = z.infer<typeof WordingProposal>;

// The only way a number changes without a re-run. Rendered in the raw notes.
export const NumberOverride = z.object({
  id: z.string().min(1),
  runId: z.string().min(1),
  target: z.object({ type: z.enum(["finding", "metric"]), id: z.string().min(1), field: z.string().min(1) }),
  previousValue: z.number(),
  newValue: z.number(),
  reason: z.string().min(1),
  overriddenBy: z.string().min(1),
  overriddenAt: Timestamp,
});
export type NumberOverride = z.infer<typeof NumberOverride>;

export const Release = z.object({
  id: z.string().min(1),
  runId: z.string().min(1),
  output: ReportOutput,
  sectionIds: z.array(z.string()).min(1),
  releasedBy: z.string().min(1),
  releasedAt: Timestamp,
});
export type Release = z.infer<typeof Release>;

/** Rule 10, as a function the release action has to call. */
export function canRelease(sections: ReportSection[]): { ok: true } | { ok: false; blockedBy: string[] } {
  if (sections.length === 0) return { ok: false, blockedBy: ["no sections"] };
  const blockedBy = sections.filter((s) => s.status !== "approved" || !s.approval).map((s) => s.key);
  return blockedBy.length === 0 ? { ok: true } : { ok: false, blockedBy };
}

// Links a customer's later run to the one it's compared against. Holds no
// values of its own: those stay in the run's findings and metrics.
export const CustomerBaseline = z.object({
  customerRef: z.string().min(1),
  runId: z.string().min(1),
  ruleSetVersion: z.string().min(1),
  capturedAt: Timestamp,
  deleteAfter: Timestamp,
});
export type CustomerBaseline = z.infer<typeof CustomerBaseline>;
