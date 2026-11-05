import { BENCHMARK_MIN_GROUP, type BenchmarkRow, type Finding, type Industry, type Metric, type Score } from "../schemas/index.ts";
import { BENCHMARK_METRICS, benchmarkFigures } from "./export.ts";
import { PUBLISHED_FIGURES, type PublishedFigure } from "./published.ts";
import { latestWindow } from "./summary.ts";

// Roadmap 1.6: where a run sits against published research and against our own
// customers. The code works out every position (rule 1), and the two kinds of
// figure are never combined: published research measures other companies in its
// own way, and our set measures Black Cloud customers in ours.

/** Our own customers' range is shown from this many orgs (roadmap decision 2), and never below. */
export const OWN_RANGE_MIN = 5;

export type Better = "higher" | "lower";

export type ComparedMetric = { key: string; label: string; unit: "percent" | "days"; better: Better };

// The headline figures a sales leader asks about. Each is a benchmark key, so a
// run's figure is rounded exactly as the rows it's set against were.
export const COMPARED_METRICS: ComparedMetric[] = [
  { key: "historic.win_rate", label: "Win rate", unit: "percent", better: "higher" },
  { key: "historic.win_rate_by_value", label: "Win rate by value", unit: "percent", better: "higher" },
  { key: "historic.sales_cycle_won", label: "Sales cycle, won deals", unit: "days", better: "lower" },
  { key: "historic.slippage_rate", label: "Deals whose close date slipped", unit: "percent", better: "lower" },
  { key: "historic.slippage_quarter_cross_rate", label: "Deals pushed into a later quarter", unit: "percent", better: "lower" },
  { key: "lead.conversion_rate", label: "Lead conversion", unit: "percent", better: "higher" },
  { key: "dq.open_deals_past_close_rate", label: "Open deals past their close date", unit: "percent", better: "lower" },
  { key: "dq.contacts_missing_email_rate", label: "Contacts with no email", unit: "percent", better: "lower" },
];

export type Position = "bottom quarter" | "middle half" | "top quarter";

export type GroupKind = "industry-and-size" | "industry" | "size" | "all";

/** One step of the widening: the group, how many orgs it held, and the floor it needed. Orgs is null when it couldn't be tried. */
export type GroupTried = { kind: GroupKind; label: string; orgs: number | null; floor: number; why?: string };

export type OwnComparison =
  | { status: "measured"; group: GroupTried & { orgs: number }; p25: number; median: number; p75: number; position: Position; tried: GroupTried[] }
  | { status: "not_measurable"; reason: string; tried: GroupTried[] };

export type PublishedComparison = { figure: PublishedFigure; difference: number; side: "better" | "worse" | "level" };

export type Comparison = {
  metric: ComparedMetric;
  /** The run's figure in whole units, as it went into the benchmark set. Null when the run didn't measure it. */
  value: number | null;
  /** Why value is null. */
  reason: string | null;
  published: PublishedComparison[];
  /** Why there's no published comparison, when there isn't one. */
  publishedReason: string | null;
  own: OwnComparison;
};

export type BenchmarkInput = {
  rows: BenchmarkRow[];
  /** The run's own rows are already in the set, as they are once its documents have been made. */
  includesThisRun: boolean;
};

export type CompareInput = {
  metrics: Metric[];
  scores: Score[];
  findings: Finding[];
  industry: Industry | null;
  headcountBand: BenchmarkRow["headcountBand"] | null;
  /** The run's quarter, from quarterOf(analysisWindow.to). */
  period: string;
  /** Null when the report is made without our own set. */
  benchmark: BenchmarkInput | null;
  published?: PublishedFigure[];
};

/** The source id a comparison's figures go under in the number check's dataset, and its raw notes item. */
export const comparisonId = (key: string): string => `compare:${key}`;

const INDUSTRY_WORDS: Record<Industry, string> = {
  "professional-services": "professional services",
  technology: "technology",
  manufacturing: "manufacturing",
  "financial-services": "financial services",
  healthcare: "healthcare",
  retail: "retail",
  education: "education",
  "public-sector": "the public sector",
  other: "other industries",
};

function sizeWords(band: BenchmarkRow["headcountBand"]): string {
  if (band.endsWith("+")) return `${Number(band.slice(0, -1)).toLocaleString("en-GB")} or more staff`;
  const [from, to] = band.split("-").map(Number) as [number, number];
  return `${from.toLocaleString("en-GB")} to ${to.toLocaleString("en-GB")} staff`;
}

/** The middle of the sorted values at p, between neighbours where it falls between two. */
function quantile(sorted: number[], p: number): number {
  const h = (sorted.length - 1) * p;
  const lo = Math.floor(h);
  const hi = Math.ceil(h);
  return sorted[lo]! + (h - lo) * (sorted[hi]! - sorted[lo]!);
}

// Shown in whole units, like the rows. The position is read against the figures
// as shown, so a reader never sees "30%, top quarter from 30%".
const whole = (n: number): number => Math.round(n) || 0;

export function positionOf(value: number, p25: number, p75: number, better: Better): Position {
  const [worse, best] = better === "higher" ? [value < p25, value > p75] : [value > p75, value < p25];
  return best ? "top quarter" : worse ? "bottom quarter" : "middle half";
}

/** Leaves out one row that is this run's own figure, so a run is never compared with itself. */
function withoutOwnRow(rows: BenchmarkRow[], value: number, input: CompareInput): BenchmarkRow[] | null {
  // Only the fields a tried group reads need to match: any row equal on those is
  // interchangeable with ours for every figure worked out here.
  const index = rows.findIndex(
    (r) => r.value === value && r.period === input.period && (input.industry === null || r.industry === input.industry) && (input.headcountBand === null || r.headcountBand === input.headcountBand),
  );
  return index === -1 ? null : rows.filter((_, i) => i !== index);
}

function ownComparison(metric: ComparedMetric, value: number, input: CompareInput, window: BenchmarkRow[]): OwnComparison {
  const { industry, headcountBand } = input;
  // "Other" isn't an industry, so it never makes a group of its own.
  const byIndustry = industry !== null && industry !== "other";
  const steps: Array<{ kind: GroupKind; label: string; floor: number; keep: (r: BenchmarkRow) => boolean; why: string | null }> = [
    {
      kind: "industry-and-size",
      label: byIndustry && headcountBand ? `Black Cloud customers in ${INDUSTRY_WORDS[industry]} with ${sizeWords(headcountBand)}` : "Black Cloud customers in your industry and size",
      floor: BENCHMARK_MIN_GROUP,
      keep: (r) => r.industry === industry && r.headcountBand === headcountBand,
      why: !byIndustry ? industryMissing(industry) : !headcountBand ? "The intake doesn't record your headcount." : null,
    },
    {
      kind: "industry",
      label: byIndustry ? `Black Cloud customers in ${INDUSTRY_WORDS[industry]}` : "Black Cloud customers in your industry",
      floor: BENCHMARK_MIN_GROUP,
      keep: (r) => r.industry === industry,
      why: byIndustry ? null : industryMissing(industry),
    },
    {
      kind: "size",
      label: headcountBand ? `Black Cloud customers with ${sizeWords(headcountBand)}` : "Black Cloud customers of your size",
      floor: BENCHMARK_MIN_GROUP,
      keep: (r) => r.headcountBand === headcountBand,
      why: headcountBand ? null : "The intake doesn't record your headcount.",
    },
    { kind: "all", label: "Black Cloud customers", floor: OWN_RANGE_MIN, keep: () => true, why: null },
  ];

  if (!input.benchmark) {
    return { status: "not_measurable", reason: "We didn't have the Black Cloud customer figures when this report was made.", tried: [] };
  }
  let rows = window.filter((r) => r.metricKey === metric.key && r.unit === metric.unit);
  if (input.benchmark.includesThisRun) {
    const others = withoutOwnRow(rows, value, input);
    // Comparing a run with a set that holds it, uncounted, would flatter it.
    if (!others) return { status: "not_measurable", reason: "Your own figures are in the Black Cloud customer set, and we couldn't find this one to leave it out.", tried: [] };
    rows = others;
  }

  const tried: GroupTried[] = [];
  for (const step of steps) {
    if (step.why) {
      tried.push({ kind: step.kind, label: step.label, orgs: null, floor: step.floor, why: step.why });
      continue;
    }
    // One row per org per metric in the window (DECISIONS 47), so rows are orgs.
    const values = rows.filter(step.keep).map((r) => r.value).sort((a, b) => a - b);
    const group: GroupTried = { kind: step.kind, label: step.label, orgs: values.length, floor: step.floor };
    tried.push(group);
    if (values.length < step.floor) continue;
    const p25 = whole(quantile(values, 0.25));
    const p75 = whole(quantile(values, 0.75));
    return { status: "measured", group: { ...group, orgs: values.length }, p25, median: whole(quantile(values, 0.5)), p75, position: positionOf(value, p25, p75, metric.better), tried };
  }
  const all = tried.at(-1)!.orgs!;
  return {
    status: "not_measurable",
    reason:
      all === 0
        ? "No Black Cloud customer has contributed this figure yet."
        : `We have this figure from ${all} Black Cloud ${all === 1 ? "customer" : "customers"}. We show a range from ${OWN_RANGE_MIN}, so that no one customer's figure can be read from it.`,
    tried,
  };
}

function industryMissing(industry: Industry | null): string {
  return industry === "other" ? "Your industry is recorded as other, which isn't a group we compare within." : "The intake doesn't record your industry.";
}

/** To one decimal place, half away from zero, so a difference reads the same whichever way round it is. */
const tenth = (n: number): number => (Math.sign(n) * Math.round(Math.abs(n) * 10)) / 10 || 0;

function publishedComparisons(metric: ComparedMetric, value: number, figures: PublishedFigure[]): PublishedComparison[] {
  return figures
    .filter((f) => f.metricKey === metric.key && f.unit === metric.unit)
    .map((figure) => {
      const difference = tenth(value - figure.value);
      const better = metric.better === "higher" ? difference > 0 : difference < 0;
      return { figure, difference, side: difference === 0 ? "level" : better ? "better" : "worse" };
    });
}

/** Every headline figure, set against published research and our own customers, or not measurable with the reason. */
export function compareRun(input: CompareInput): Comparison[] {
  const { figures, skipped } = benchmarkFigures(input);
  const window = latestWindow(input.benchmark?.rows ?? []);
  const published = input.published ?? PUBLISHED_FIGURES;
  return COMPARED_METRICS.map((metric): Comparison => {
    const figure = figures.find((f) => f.metricKey === metric.key);
    if (!figure) {
      const reason = skipped.find((s) => s.metricKey === metric.key)?.reason ?? "This run didn't measure it.";
      const own: OwnComparison = { status: "not_measurable", reason: "There's no figure of yours to compare.", tried: [] };
      return { metric, value: null, reason, published: [], publishedReason: "There's no figure of yours to compare.", own };
    }
    const found = publishedComparisons(metric, figure.value, published);
    return {
      metric,
      value: figure.value,
      reason: null,
      published: found,
      publishedReason: found.length ? null : "We haven't found published research that measures this the way we do.",
      own: ownComparison(metric, figure.value, input, window),
    };
  });
}

// A compared metric is always a benchmark key in the same unit, so the run's
// figure and the rows it's set against were rounded the same way.
for (const metric of COMPARED_METRICS) {
  const listed = BENCHMARK_METRICS.find((m) => m.key === metric.key);
  if (!listed || listed.unit !== metric.unit) throw new Error(`${metric.key} isn't a ${metric.unit} benchmark figure.`);
}

const orgs = (n: number): string => `${n} ${n === 1 ? "org" : "orgs"}`;

export const formatFigure = (value: number, unit: ComparedMetric["unit"]): string => (unit === "percent" ? `${value}%` : `${value} ${value === 1 ? "day" : "days"}`);

/** How far apart two figures are, in words: "4 points higher", "12 days shorter". */
export function differenceWords(c: Comparison, p: PublishedComparison): string {
  if (p.side === "level") return "the same";
  const size = Math.abs(p.difference);
  const more = p.difference > 0;
  return c.metric.unit === "percent" ? `${size} ${size === 1 ? "point" : "points"} ${more ? "higher" : "lower"}` : `${size} ${size === 1 ? "day" : "days"} ${more ? "longer" : "shorter"}`;
}

/** The working for one comparison, as labelled lines: the raw notes print them, and the number check reads them. */
export function comparisonNotes(c: Comparison): Array<{ label: string; text: string }> {
  const fmt = (v: number): string => formatFigure(v, c.metric.unit);
  const lines: Array<{ label: string; text: string }> = [];
  lines.push({ label: "Yours", text: c.value === null ? `Not measurable. ${c.reason}` : `${fmt(c.value)}, rounded to whole units as in the benchmark set.` });
  lines.push({ label: "Better is", text: c.metric.better === "higher" ? "Higher." : c.metric.unit === "days" ? "Shorter." : "Lower." });
  if (c.published.length === 0) lines.push({ label: "Published research", text: c.publishedReason ?? "None." });
  for (const p of c.published) {
    const f = p.figure;
    lines.push({ label: "Published research", text: `${f.source.publisher}, ${f.source.title} (${f.source.year}): ${fmt(f.value)}, ${f.statistic}.` });
    lines.push({ label: "What it measures", text: f.measures });
    lines.push({ label: "Who it describes", text: `${f.population}.` });
    lines.push({ label: "How it differs from ours", text: f.differences.length ? f.differences.join(" ") : "It measures the same thing." });
    lines.push({ label: "Basis", text: f.basis === "records" ? "Measured from deal records." : "Answered in a survey." });
    lines.push({ label: "Source", text: `${f.source.url}, read on ${f.source.checked}.` });
    lines.push({ label: "Against it", text: `Yours is ${differenceWords(c, p)}${p.side === "level" ? "" : `, which is ${p.side}`}.` });
  }
  const own = c.own;
  if (own.status === "measured") {
    lines.push({
      label: "Black Cloud customers",
      text: `${own.group.label}, from ${orgs(own.group.orgs)}: the middle half runs from ${fmt(own.p25)} to ${fmt(own.p75)}, and the median is ${fmt(own.median)}. Yours is in the ${own.position}.`,
    });
  } else lines.push({ label: "Black Cloud customers", text: `Not measurable. ${own.reason}` });
  if (own.tried.length) {
    lines.push({ label: "Groups tried, narrowest first", text: own.tried.map((g) => `${g.label}: ${g.orgs === null ? `not tried. ${g.why}` : `${orgs(g.orgs)}, needs ${g.floor}.`}`).join(" ") });
  }
  return lines;
}
