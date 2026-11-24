import { z } from "zod";
import { DomainId, NotMeasurableKind, Timestamp } from "./common.ts";
import { ReviewMode } from "./run.ts";

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
  // The kind only when most of the area doesn't apply to the org (DECISIONS.md 627), and optional, so a score written before it reads as before.
  z.object({ status: z.literal("not_measurable"), domain: DomainId, ruleSetVersion: z.string().min(1), reason: z.string().min(1), notMeasurableKind: NotMeasurableKind.optional() }),
]);
export type Score = z.infer<typeof Score>;

export const ReportOutput = z.enum(["raw_notes", "executive_summary", "full_health_check"]);
// "checked" passed the machine checks and no person approved it: an automatic
// run's only (DECISIONS.md 688). It never holds an approval, so no name is made up for it.
export const SectionStatus = z.enum(["draft", "needs_input", "in_review", "approved", "checked"]);

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
    if (s.status === "checked" && s.approval) {
      ctx.addIssue({ code: "custom", path: ["approval"], message: "A checked section was approved by no one, so it can't name an approver" });
    } else if (s.status !== "approved" && s.approval) {
      ctx.addIssue({ code: "custom", path: ["approval"], message: "Only an approved section can hold an approval" });
    }
  });
export type ReportSection = z.infer<typeof ReportSection>;

export const CHECKED_OUTSIDE_AUTOMATIC = "Only an automatic run's sections can be checked: a consultant-led run's are approved by a person";

/**
 * A run's sections held to its review mode, wherever the two are read
 * together: a checked section on a consultant-led run would pass for reviewed
 * with nobody having looked (DECISIONS.md 688).
 */
export const SectionsOfRun = z
  .object({ reviewMode: ReviewMode, sections: z.array(ReportSection) })
  .superRefine((r, ctx) => {
    if (r.reviewMode === "automatic") return;
    r.sections.forEach((s, i) => {
      if (s.status === "checked") ctx.addIssue({ code: "custom", path: ["sections", i, "status"], message: CHECKED_OUTSIDE_AUTOMATIC });
    });
  });

/** Throws, naming the first section, when a run's sections don't fit its review mode. */
export function assertSectionsFitRun(reviewMode: ReviewMode, sections: ReportSection[]): void {
  const fit = SectionsOfRun.safeParse({ reviewMode, sections });
  if (!fit.success) throw new Error(fit.error.issues[0]?.message ?? CHECKED_OUTSIDE_AUTOMATIC);
}

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

// A person's release names them. An automatic one names no one, never a made-up
// releaser (DECISIONS.md 688). Rows from before 688 were all a person's.
export const Release = z
  .object({
    id: z.string().min(1),
    runId: z.string().min(1),
    output: ReportOutput,
    sectionIds: z.array(z.string()).min(1),
    kind: z.enum(["person", "automatic"]).default("person"),
    releasedBy: z.string().min(1).nullable().default(null),
    releasedAt: Timestamp,
  })
  .superRefine((r, ctx) => {
    if (r.kind === "person" && r.releasedBy === null) ctx.addIssue({ code: "custom", path: ["releasedBy"], message: "A person's release must name who released it" });
    if (r.kind === "automatic" && r.releasedBy !== null) ctx.addIssue({ code: "custom", path: ["releasedBy"], message: "An automatic release was made by no one, so it can't name a person" });
  });
export type Release = z.infer<typeof Release>;

/** Why an automatic run's sections can't be released: one isn't checked, or names an approver no person was (DECISIONS.md 693). */
export const AUTOMATIC_NOT_ALL_CHECKED = "Not every section passed its checks, so the automatic run can't be released";

/**
 * Rule 10, as a function the release action has to call. A consultant-led run
 * is released only when every section is approved, with who and when, so a
 * checked section never passes for one. An automatic run is released by code
 * only when every section is checked and names no approver (DECISIONS.md 693);
 * the run's digest and its other checks are the release's own (src/worker/release.ts).
 */
export function canRelease(sections: ReportSection[], mode: ReviewMode): { ok: true } | { ok: false; blockedBy: string[]; reason?: string } {
  if (sections.length === 0) return { ok: false, blockedBy: ["no sections"] };
  if (mode === "automatic") {
    const blockedBy = sections.filter((s) => s.status !== "checked" || s.approval !== null).map((s) => s.key);
    return blockedBy.length === 0 ? { ok: true } : { ok: false, blockedBy, reason: AUTOMATIC_NOT_ALL_CHECKED };
  }
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
