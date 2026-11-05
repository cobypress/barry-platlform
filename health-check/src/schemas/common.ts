import { z } from "zod";

// Domains are plug-ins, so the id is an open slug. SALES_CLOUD_DOMAINS is the
// version 1 coverage list, not a closed set: Service Cloud adds its own ids.
export const DomainId = z.string().regex(/^[a-z][a-z0-9_]*$/);
export type DomainId = z.infer<typeof DomainId>;

export const SALES_CLOUD_DOMAINS = [
  "configuration",
  "automation",
  "security",
  "best_practice",
  "data_quality",
  "utilisation",
  "adoption",
  "current_pipeline",
  "historic_pipeline",
  "forecast",
  "activity",
  "lead_funnel",
  "revenue_opportunities",
] as const;

export const Timestamp = z.iso.datetime();
export const CalendarDate = z.iso.date();

export const DateRange = z.object({ from: CalendarDate, to: CalendarDate });
export type DateRange = z.infer<typeof DateRange>;

// Rule 4: a conclusion must show which cuts of the data were looked at.
export const SegmentCheck = z.object({
  dimension: z.string().min(1),
  segments: z.number().int().nonnegative(),
  note: z.string().optional(),
});
export type SegmentCheck = z.infer<typeof SegmentCheck>;
