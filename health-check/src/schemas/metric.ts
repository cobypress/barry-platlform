import { z } from "zod";
import { CalendarDate, DateRange, DomainId, NotMeasurableKind } from "./common.ts";
import { Evidence } from "./evidence.ts";
import { MarkedSentence } from "./finding.ts";

/**
 * A metric's texts with the characters the org chose marked (DECISIONS.md 693),
 * each kept only while its text still hashes to it; an item of a list is null
 * where its marks are unknown. Optional, and written only on an automatic run,
 * whose digest serves every text marked: a consultant-led run's metrics are
 * stored exactly as before.
 */
export const MetricOrgWords = z.object({
  label: MarkedSentence.optional(),
  definition: MarkedSentence.optional(),
  reason: MarkedSentence.optional(),
  assumptions: z.array(MarkedSentence.nullable()).optional(),
  exclusions: z.array(MarkedSentence.nullable()).optional(),
});
export type MetricOrgWords = z.infer<typeof MetricOrgWords>;

export const MetricUnit = z.enum(["percent", "days", "count", "currency", "ratio"]);

export const MetricSegment = z.object({
  dimension: z.string().min(1),
  key: z.string().min(1),
  n: z.number().int().nonnegative(),
  value: z.number().nullable(),
  // Thin segments stay listed, with the reason, so a gap in a chart is explained.
  notMeasurableReason: z.string().nullable(),
});
export type MetricSegment = z.infer<typeof MetricSegment>;

export const SeriesPoint = z.object({
  date: CalendarDate,
  value: z.number().nullable(),
  n: z.number().int().nonnegative(),
  notMeasurableReason: z.string().nullable(),
});
export type SeriesPoint = z.infer<typeof SeriesPoint>;

const base = {
  id: z.string().min(1),
  domain: DomainId,
  label: z.string().min(1),
  definition: z.string().min(1),
  unit: MetricUnit,
  evidence: z.array(Evidence).min(1),
  exclusions: z.array(z.string()),
  assumptions: z.array(z.string()),
  orgWords: MetricOrgWords.optional(),
};

export const Metric = z
  .discriminatedUnion("status", [
    z.object({
      status: z.literal("measured"),
      ...base,
      value: z.number(),
      n: z.number().int().nonnegative(),
      dateRange: DateRange.nullable(),
      segments: z.array(MetricSegment),
      segmentExemptReason: z.string().nullable(),
      series: z.array(SeriesPoint),
      stats: z.record(z.string(), z.number()),
    }),
    // A distinct shape on purpose: there's no value field to fill with a guess.
    // The kind is optional (DECISIONS.md 627): a metric written before it has none.
    z.object({ status: z.literal("not_measurable"), ...base, reason: z.string().min(1), notMeasurableKind: NotMeasurableKind.optional() }),
  ])
  .superRefine((m, ctx) => {
    if (m.status === "measured" && m.segments.length === 0 && !m.segmentExemptReason) {
      ctx.addIssue({ code: "custom", path: ["segments"], message: "A measured metric must be segmented or say why it can't be" });
    }
  });
export type Metric = z.infer<typeof Metric>;
