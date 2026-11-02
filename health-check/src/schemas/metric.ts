import { z } from "zod";
import { CalendarDate, DateRange, DomainId } from "./common.ts";
import { Evidence } from "./evidence.ts";

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
    z.object({ status: z.literal("not_measurable"), ...base, reason: z.string().min(1) }),
  ])
  .superRefine((m, ctx) => {
    if (m.status === "measured" && m.segments.length === 0 && !m.segmentExemptReason) {
      ctx.addIssue({ code: "custom", path: ["segments"], message: "A measured metric must be segmented or say why it can't be" });
    }
  });
export type Metric = z.infer<typeof Metric>;
