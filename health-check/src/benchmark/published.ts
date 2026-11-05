// Published research we compare a run with, beside our own customers' figures
// (roadmap decision 2). Every figure is as its source states it: never rounded,
// converted or filled in. Each says exactly what it measures and how that
// differs from our metric, because a figure measured another way can only be
// compared with care, and the reader has to be told what the care is.
// A figure we couldn't read on the source's own page isn't here. Anything
// doubtful is in QUESTIONS.md.

export type PublishedSource = {
  /** Who published it, as a reader would name them. */
  publisher: string;
  title: string;
  year: number;
  url: string;
  /** When we last read the figure at that address, YYYY-MM-DD. */
  checked: string;
};

export type PublishedFigure = {
  id: string;
  /** The benchmark key it's compared with, from BENCHMARK_METRICS. */
  metricKey: string;
  value: number;
  unit: "percent" | "days";
  /** Which statistic the source gives, in words: "average", "median". */
  statistic: string;
  /** Who it describes: industry, region, company size, sample and period. */
  population: string;
  /** What it measures, as the source defines it. */
  measures: string;
  /** Where it measures something other than our metric. Empty only when the definitions match. */
  differences: string[];
  /** Measured from deal records, or answered in a survey. */
  basis: "records" | "survey";
  source: PublishedSource;
};

export const PUBLISHED_FIGURES: PublishedFigure[] = [];
