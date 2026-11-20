import { z } from "zod";
import { Timestamp } from "./common.ts";

// The org's complexity (DECISIONS.md 614): complexity.json in the run folder,
// and the console's HealthCheckComplexity row. How big and tangled the build
// is, never whether that's good: counts of set-up only, with the working that
// turns them into the score, so a consultant can explain every point.

export const COMPLEXITY_VERSION = 1;

export const COMPLEXITY_BANDS = ["Simple", "Moderate", "Complex", "Very complex"] as const;

const Id = z.string().regex(/^[a-z][a-zA-Z]*$/);
const Label = z.string().min(1).max(200);
const Points = z.number().nonnegative();

/** One count, its top (where it stops earning) and the points it earned of its most. */
export const ComplexityCount = z.object({ id: Id, label: Label, count: z.number().int().nonnegative(), top: z.number().int().positive(), maxPoints: Points, points: Points }).strict();

export const ComplexityComponent = z.discriminatedUnion("status", [
  z
    .object({
      status: z.literal("measured"),
      id: Id,
      label: Label,
      weight: z.number().int().positive(),
      counts: z.array(ComplexityCount).min(1),
      points: Points,
      /** This component's part of the points earned, in whole percent. */
      shareOfScore: z.number().int().min(0).max(100),
      notes: z.array(z.string().min(1)),
    })
    .strict(),
  z.object({ status: z.literal("not_read"), id: Id, label: Label, weight: z.number().int().positive(), reason: z.string().min(1) }).strict(),
]);
export type ComplexityComponent = z.infer<typeof ComplexityComponent>;

const common = {
  version: z.literal(COMPLEXITY_VERSION),
  /** Which table of tops and weights made it. A change to either is a new formula. */
  formula: z.number().int().positive(),
  madeAt: Timestamp,
  components: z.array(ComplexityComponent).min(1),
  /** The components not read, by id. */
  leftOut: z.array(Id),
};

export const Complexity = z.discriminatedUnion("status", [
  z.object({ status: z.literal("measured"), ...common, score: z.number().int().min(0).max(100), band: z.enum(COMPLEXITY_BANDS), weightRead: z.number().int().positive().max(100) }).strict(),
  z.object({ status: z.literal("not_measurable"), ...common, reason: z.string().min(1) }).strict(),
]);
export type Complexity = z.infer<typeof Complexity>;
