import type { Metric } from "../schemas/index.ts";

// The average beside a median (DECISIONS.md 16, 709). Every duration the
// engine reports leads with its median, because a few very old records drag
// an average. The average is kept beside it as the stat `mean`, so the two can
// be read together, and shown in brackets after the median only where it
// says something the median doesn't. No rule reads it.

/** Fewer values than this and no average is kept, whatever the calibrated minimum. */
export const MIN_AVERAGE_VALUES = 5;

/** An average within this share of its median adds nothing, so only the median is shown. */
export const AVERAGE_APART = 0.1;

/** The durations whose value is a median of one value per record, and so carry the average beside it. */
export const DURATION_MEDIANS: ReadonlySet<string> = new Set([
  "historic.sales_cycle_won",
  "historic.sales_cycle_lost",
  "historic.slippage_days",
  "historic.close_days_late",
  "historic.stage_velocity_won",
  "historic.stage_velocity_lost",
  "current.median_days_in_stage",
  "current.open_median_age",
  "service.resolution_days",
  // Behind HC_LEAD_JOURNEY (DECISIONS.md 770): days from a won deal's first lead to its close.
  "lead.days_to_won",
]);

const round1 = (n: number): number => Math.round(n * 10) / 10;

/** An average as the documents print it, a thousand and over with a comma: "79.9", "1,204.5". */
export const averageFigure = (n: number): string => n.toLocaleString("en-GB", { maximumFractionDigits: 1 });

/** The average in brackets after a median, or nothing (see averageBeside). */
export const averageBracket = (m: Metric | null | undefined): string => {
  const average = averageBeside(m);
  return average === null ? "" : ` (average ${averageFigure(average)})`;
};

/** The mean of the values a median was taken from, to one decimal place as the medians are, as a stat; nothing below five values. */
export function meanStat(values: number[]): { mean?: number } {
  if (values.length < MIN_AVERAGE_VALUES) return {};
  return { mean: round1(values.reduce((sum, v) => sum + v, 0) / values.length) };
}

/** Whether an average is far enough from its median to be worth showing: more than 10% of the median apart. */
export const averageApart = (median: number, mean: number): boolean => Math.abs(mean - median) > AVERAGE_APART * Math.abs(median);

/**
 * The average to show beside a duration's median, or null: when the metric
 * isn't such a duration, isn't measured, rests on fewer than five values, has
 * no average kept, has one within 10% of the median, or has a median of 0 or
 * below (deals closed early), where "an average of" reads as a duration it isn't.
 */
export function averageBeside(m: Metric | null | undefined): number | null {
  if (!m || m.status !== "measured" || !DURATION_MEDIANS.has(m.id) || m.n < MIN_AVERAGE_VALUES || m.value <= 0) return null;
  // A metric made by hand in a test may carry no stats at all.
  const mean = (m.stats as Record<string, number> | undefined)?.mean;
  return typeof mean === "number" && averageApart(m.value, mean) ? mean : null;
}
