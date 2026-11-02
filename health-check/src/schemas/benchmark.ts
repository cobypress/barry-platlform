import { z } from "zod";

export const HeadcountBand = z.enum(["1-49", "50-249", "250-999", "1000+"]);
export const UserBand = z.enum(["1-50", "51-200", "201+"]);
export const DealSizeBand = z.enum(["under-5k", "5k-25k", "25k-100k", "100k+"]);

// Strict, so a name, org id or run id can't be carried in as an extra field.
// Nothing here links back to a customer, and nothing may: rows can't be deleted
// individually for the same reason, which the customer terms have to state.
export const BenchmarkRow = z.strictObject({
  metricKey: z.string().regex(/^[a-z][a-z0-9_.]*$/),
  ruleSetVersion: z.string().min(1),
  // Whole units only, because an exact ratio can identify an unusual org.
  value: z.number().int(),
  unit: z.enum(["percent", "days", "score"]),
  industry: z.string().min(1),
  headcountBand: HeadcountBand,
  userBand: UserBand,
  dealSizeBand: DealSizeBand,
  // A quarter, never a date: a run date could be matched to an engagement.
  period: z.string().regex(/^\d{4}-Q[1-4]$/),
});
export type BenchmarkRow = z.infer<typeof BenchmarkRow>;

/** Below this many orgs a comparison isn't shown, and best-practice thresholds are used instead. */
export const BENCHMARK_MIN_GROUP = 10;
