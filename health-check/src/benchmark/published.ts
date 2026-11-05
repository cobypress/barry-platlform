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
  /** Which statistic the source gives, as words to follow the figure: "on average", "median", or "overall" when it doesn't say. */
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

// Ebsta's figures are measured from its customers' CRM records, which makes them
// the closest to ours. None of the surveys we found defines its figure closely
// enough to compare, and what we left out, and why, is in QUESTIONS.md.
const EBSTA_2025: PublishedSource = {
  publisher: "Ebsta and Pavilion",
  title: "2025 GTM Benchmarks",
  year: 2025,
  url: "https://benchmarks.ebsta.com/hubfs/V3%202025%20Benchmark%20Report/gtm_benchmarks_digital_report.pdf",
  checked: "2026-11-05",
};
const EBSTA_2025_POPULATION =
  "655K opportunities worth $48 billion, from 387 companies, analysed by Ebsta from its customers' CRM data. The report doesn't say which region the companies are in, or when the deals closed";

export const PUBLISHED_FIGURES: PublishedFigure[] = [
  {
    id: "ebsta-2025-new-logo-win-rate",
    metricKey: "historic.win_rate",
    value: 19,
    unit: "percent",
    statistic: "overall",
    population: EBSTA_2025_POPULATION,
    measures: "New logo win rate: the share of new business opportunities won.",
    differences: [
      "It covers new business only. We count every closed deal, and the same report gives 45% for expansion deals.",
      "The report doesn't say whether deals that ended with no decision are counted as lost. We leave them out.",
      "The report doesn't say whether it's pooled across deals or averaged across companies.",
      "A later page of the same report prints 18% for new business. We use the 19% its overview and its new business and expansion pages give.",
    ],
    basis: "records",
    source: EBSTA_2025,
  },
  {
    id: "ebsta-2025-new-business-sales-cycle",
    metricKey: "historic.sales_cycle_won",
    value: 91,
    unit: "days",
    statistic: "on average",
    population: EBSTA_2025_POPULATION,
    measures: "How long new business deals take to close.",
    differences: [
      "It covers new business only. We take every won deal, and the same report gives 52 days for expansion deals.",
      "It's an average, and ours is a median. A few very long deals pull an average up, so a median is usually lower.",
      "The report doesn't say whether lost deals are included, or when the clock starts. Ours runs from a won deal's creation to its close.",
    ],
    basis: "records",
    source: EBSTA_2025,
  },
  {
    id: "ebsta-2025-deals-slipped",
    metricKey: "historic.slippage_rate",
    value: 36,
    unit: "percent",
    statistic: "overall",
    population: EBSTA_2025_POPULATION,
    measures: "Deals slipped: Ebsta describes these as deals slipping past their close date.",
    differences: [
      "The report doesn't define the base. We count closed deals whose close date was moved later at least once.",
      "It's on the report's page about new business, so it may leave out expansion deals. We count every closed deal.",
      "The report gives no period. Ours is the deals that closed in the analysis window.",
    ],
    basis: "records",
    source: EBSTA_2025,
  },
];
