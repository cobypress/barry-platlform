import { z } from "zod";

// Domains are plug-ins, so the id is an open slug. SALES_CLOUD_DOMAINS is the
// version 1 coverage list, not a closed set: Service Cloud adds its own ids.
export const DomainId = z.string().regex(/^[a-z][a-z0-9_]*$/);
export type DomainId = z.infer<typeof DomainId>;

export const SALES_CLOUD_DOMAINS = [
  "configuration",
  "automation",
  "technical_health",
  "security",
  "integrations",
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

/** Service Cloud's areas (roadmap Stage 6). A run has them only when its modules include "service". */
export const SERVICE_CLOUD_DOMAINS = ["service_operations", "service_setup"] as const;

/** Salesforce CPQ's areas (roadmap Stage 6). A run has them only when its modules include "cpq". */
export const CPQ_DOMAINS = ["cpq_quoting", "cpq_setup"] as const;

/** Marketing Cloud Account Engagement's area (roadmap Stage 6). A run has it only when its modules include it. */
export const ACCOUNT_ENGAGEMENT_DOMAINS = ["account_engagement"] as const;

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
