import { z } from "zod";
import { DateRange, Timestamp } from "./common.ts";

export const RunStatus = z.enum([
  "extracting",
  "stopped_budget",
  "analysing",
  "awaiting_manual",
  "in_review",
  "released",
  "failed",
]);
export type RunStatus = z.infer<typeof RunStatus>;

// One step maps to one BullMQ job in Milestone 5.
export const RunStep = z.object({
  id: z.string().min(1),
  version: z.number().int().positive(),
  status: z.enum(["pending", "done", "failed"]),
  calls: z.number().int().nonnegative(),
  finishedAt: Timestamp.nullable(),
  error: z.string().nullable(),
});
export type RunStep = z.infer<typeof RunStep>;

export const RunBudget = z.object({
  stopAtShare: z.number().gt(0).lte(1),
  dailyLimit: z.number().int().nonnegative().nullable(),
  usedAtStart: z.number().int().nonnegative().nullable(),
  usedAtLastCheck: z.number().int().nonnegative().nullable(),
  callsMade: z.number().int().nonnegative(),
  stoppedReason: z.string().nullable(),
});
export type RunBudget = z.infer<typeof RunBudget>;

/** Complete reads set-up, records and usage. Essentials, the set-up review, never reads CRM records (roadmap 2.2). */
export const RunScope = z.enum(["complete", "essentials"]);
export type RunScope = z.infer<typeof RunScope>;

/**
 * The wider metadata retrieve failed and the four standard objects were read
 * instead (DECISIONS.md 460): the objects it also named, and Salesforce's
 * reason. Object API names and an error, never a record, so the raw notes can
 * say so (DECISIONS.md 425).
 */
export const RetrieveFallback = z.object({ named: z.array(z.string()), reason: z.string() });
export type RetrieveFallback = z.infer<typeof RetrieveFallback>;

export const Run = z.object({
  id: z.string().min(1),
  // A scan is the free set-up teaser (roadmap Stage 5): never reviewed, released or made into documents.
  // A pulse is Barometer Ongoing's monthly re-check against its baseline: never reviewed, drafted or made into documents (DECISIONS.md 135).
  product: z.enum(["barometer", "scan", "pulse"]).default("barometer"),
  customerRef: z.string().nullable(),
  orgId: z.string().min(15).max(18),
  isSandbox: z.boolean(),
  connectionKind: z.enum(["sf-cli", "oauth"]),
  status: RunStatus,
  modules: z.array(z.string()).min(1),
  // Runs from before scopes existed were all complete.
  scope: RunScope.default("complete"),
  ruleSetVersion: z.string().min(1),
  calibrationHash: z.string().min(1),
  baselineRunId: z.string().nullable(),
  asOf: Timestamp,
  analysisWindow: DateRange,
  apiVersion: z.string().min(1),
  budget: RunBudget,
  steps: z.array(RunStep),
  benchmarkOptIn: z.boolean(),
  createdBy: z.string().min(1),
  startedAt: Timestamp,
  finishedAt: Timestamp.nullable(),
  // Absent when the wider retrieve worked, or wasn't tried.
  retrieveFallback: RetrieveFallback.optional(),
});
export type Run = z.infer<typeof Run>;
