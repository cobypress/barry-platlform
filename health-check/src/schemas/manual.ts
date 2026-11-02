import { z } from "zod";
import { DomainId, Timestamp } from "./common.ts";

export const ManualField = z.object({
  key: z.string().min(1),
  label: z.string().min(1),
  type: z.enum(["boolean", "number", "text", "select", "date"]),
  options: z.array(z.string()).optional(),
  required: z.boolean(),
});
export type ManualField = z.infer<typeof ManualField>;

export const ManualAnswerMethod = z.enum(["fields", "paste", "screenshot"]);

export const ManualStep = z.object({
  id: z.string().min(1),
  key: z.string().min(1),
  domain: DomainId,
  raisedBy: z.enum(["engine", "claude", "consultant"]),
  ruleIds: z.array(z.string()),
  title: z.string().min(1),
  whyItMatters: z.string().min(1),
  whereToGo: z.string().min(1),
  whatToCapture: z.string().min(1),
  fields: z.array(ManualField).min(1),
  answerMethods: z.array(ManualAnswerMethod).min(1),
  status: z.enum(["pending", "answered", "skipped"]),
  ifSkipped: z.string().min(1),
});
export type ManualStep = z.infer<typeof ManualStep>;

// What Claude read from a screenshot. Never evidence until a consultant confirms it.
export const ManualPrefill = z.object({
  values: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])),
  model: z.string().min(1),
  screenshotRef: z.string().min(1),
  createdAt: Timestamp,
  confirmedBy: z.string().nullable(),
  confirmedAt: Timestamp.nullable(),
});

export const ManualAnswer = z
  .object({
    stepKey: z.string().min(1),
    outcome: z.enum(["answered", "skipped"]),
    method: ManualAnswerMethod.optional(),
    values: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])).default({}),
    pastedText: z.string().optional(),
    screenshotRef: z.string().optional(),
    prefill: ManualPrefill.optional(),
    skipReason: z.string().optional(),
    answeredBy: z.string().min(1),
    answeredAt: Timestamp,
  })
  .superRefine((a, ctx) => {
    if (a.outcome === "answered" && !a.method) {
      ctx.addIssue({ code: "custom", path: ["method"], message: "An answer must say how it was given" });
    }
    if (a.outcome === "skipped" && !a.skipReason) {
      ctx.addIssue({ code: "custom", path: ["skipReason"], message: "A skipped step must give the reason" });
    }
    if (a.method === "screenshot" && a.prefill && (!a.prefill.confirmedBy || !a.prefill.confirmedAt)) {
      ctx.addIssue({ code: "custom", path: ["prefill"], message: "Claude's reading of a screenshot needs a consultant's confirmation" });
    }
  });
export type ManualAnswer = z.infer<typeof ManualAnswer>;
