import { buildValueModel, HEADLINE_KINDS, KIND_LABELS, UK_HOURLY_BENCHMARK, type ValueModel } from "../value/model.ts";
import { AREA_WEIGHTS, CRITICAL_CAP, GRADES, overallScore, type OverallScore } from "../domains/scoring.ts";
import { comparisonId, comparisonNotes, compareRun, OWN_RANGE_MIN, type BenchmarkInput, type Comparison } from "../benchmark/compare.ts";
import { quarterOf } from "../benchmark/export.ts";
import { buildInvestment, investmentText, INVESTMENT_METRIC_IDS, type Investment } from "../money/investment.ts";
import { buildDataset, checkNumbers, scopeDataset, SOURCE, toNumberCheckResult, type FigureSet, type StructuralFigure } from "../numbercheck/index.ts";
import { compactMoney, money, monthYear } from "../render/format.ts";
import { BENCHMARK_MIN_GROUP, type Calibration, type Finding, type ManualStep, type Metric, type NumberOverride, type ReportSection, type Run, type Score, type SectionVersion } from "../schemas/index.ts";
import { ReportSection as ReportSectionSchema, SectionVersion as SectionVersionSchema } from "../schemas/index.ts";
import type { SectionBrief } from "./draft.ts";
import type { Intake } from "./intake.ts";
import { applyOverrides } from "./overrides.ts";
import { assumptionsSource, findingSource, metricSource, planSource, statedProblemsSource, type PromptSource } from "./prompt.ts";
import { buildPlan, bySeverity, domainLabel, domainsInOrder, issuesOf, scorecard, teamLevelFinding, topIssues, verdictOf, type Plan } from "./select.ts";

// The three outputs, built from one findings dataset. Each section gets a first
// version written by the engine, so a report always exists even if Claude never
// runs. Sections start as drafts: only a person can approve one (rule 10).

export type ReportInput = {
  run: Run;
  calibration: Calibration;
  findings: Finding[];
  metrics: Metric[];
  manualSteps: ManualStep[];
  scores?: Score[];
  overrides?: NumberOverride[];
  intake?: Intake | null;
  /** The consultant the documents' cover names. Only the documents job sets it, and it never calls Claude. */
  preparedBy?: string | null;
  /** Our own customers' benchmark rows. Without them, only published research is compared. */
  benchmark?: BenchmarkInput | null;
  now?: Date;
};

export type ReportOutputKey = "raw_notes" | "executive_summary" | "full_health_check";

export type BuiltOutput = { output: ReportOutputKey; sections: ReportSection[]; versions: SectionVersion[] };

export type BuiltReport = {
  input: ReportInput;
  dataset: FigureSet;
  /** Findings as an executive reader sees them: overrides applied, at team level. */
  teamFindings: Finding[];
  /** Metrics with overrides applied. */
  metrics: Metric[];
  /** The Barometer score: one figure for the org, from the area scores. */
  overall: OverallScore;
  /** What the issues are worth over the next 12 months (roadmap 1.4). */
  value: ValueModel;
  /** Where the headline figures sit against published research and our own customers. */
  comparisons: Comparison[];
  /** What the customer pays a year, by precedence, with list price for context. */
  investment: Investment;
  outputs: Record<ReportOutputKey, BuiltOutput>;
  /** What Claude would be asked for, by section id. Sections with no entry stay engine text. */
  briefs: Map<string, SectionBrief>;
  /** The dataset sources each section's number check reads, by section id. A brief's sources are always among them. */
  scopes: Map<string, string[]>;
};

export const TOP_ISSUE_COUNT = 5;

// Figures the structure itself needs. These are report shape, not findings, and
// they're never in a scope Claude's text is checked against.
const STRUCTURE: StructuralFigure[] = [
  ...[1, 30, 31, 60, 61, 90].map((value): StructuralFigure => ({ kind: "number", value })),
  { kind: "duration", value: 90, unit: "day" },
  { kind: "number", value: TOP_ISSUE_COUNT },
];

type Spec = {
  key: string;
  title: string;
  body: string;
  findingIds?: string[];
  metricIds?: string[];
  manualStepIds?: string[];
  /** Other dataset sources this section's text may quote. */
  sources?: string[];
  needsInput?: boolean;
  brief?: { purpose: string; sources: PromptSource[]; fillsPlaceholder?: boolean };
};

const SUMMARY_SOURCES = [SOURCE.run, SOURCE.summary, SOURCE.structure];
const INTAKE = "intake";
const CALIBRATION = "calibration";
const OVERRIDES = "overrides";
const COVER = "cover";
const PLAN = "plan";
export const OVERALL = "score:overall";
export const VALUE_TOTAL = "value:total";
export const valueSource = (group: string): string => `value:${group}`;
/** The investment section's own figures: what the customer told us they spend. */
export const INVESTMENT_SOURCE = "investment";
const COMPARISONS = "comparisons";

const lines = (...parts: Array<string | null | undefined | false>): string => parts.filter(Boolean).join("\n\n");
const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

/** "1 high, none critical": a count of nothing is said in words. */
function severityCounts({ critical, high }: { critical: number; high: number }): string {
  if (critical === 0 && high === 0) return "none of them critical or high";
  if (critical === 0) return `${high} high, none critical`;
  if (high === 0) return `${critical} critical, none high`;
  return `${critical} critical and ${high} high`;
}

function calibrationText(run: Run, c: Calibration): string {
  return [
    `Analysis window: ${run.analysisWindow.from} to ${run.analysisWindow.to}.`,
    `Calibration confirmed by ${c.confirmedBy} on ${c.confirmedAt.slice(0, 10)}.`,
    `Record types in scope: ${c.scope.recordTypes === "all" ? "all" : c.scope.recordTypes.join(", ")}.`,
    `Stage meanings: ${c.stages.map((s) => `${s.name} (${s.category})`).join(", ")}.`,
    `A user counts as dormant after ${c.dormantDays} days without logging in.`,
    `A figure needs at least ${c.minSample} deals behind it.`,
    `Values are in ${c.currency}.`,
  ].join("\n");
}

function overrideText(overrides: NumberOverride[]): string {
  return overrides.map((o) => `${o.target.type} ${o.target.id}, ${o.target.field}: changed from ${o.previousValue} to ${o.newValue} by ${o.overriddenBy} on ${o.overriddenAt.slice(0, 10)}. Reason: ${o.reason}`).join("\n");
}

function intakeText(intake: Intake): string {
  return lines(intake.summary, ...intake.statedProblems.map((p) => `${p.statement}${p.assessment ? ` ${p.assessment}` : ""}`));
}

/** The score's working, figure by figure, for the raw notes. */
function overallText(overall: OverallScore): string {
  if (overall.status === "not_measurable") return `We couldn't give the org one score. ${overall.reason}`;
  return lines(
    `Your Barometer score is ${overall.score} out of 100: grade ${overall.grade}, ${overall.word.toLowerCase()}.`,
    `It's the average of the area scores, each weighted by how directly the area affects revenue:\n${overall.areas.map((a) => `${domainLabel(a.domain)}: ${a.score}, weighted ${a.weight}`).join("\n")}`,
    "Areas we couldn't score are left out, rather than counted as zero.",
    overall.capped ? `A critical issue holds the score at ${CRITICAL_CAP}, however well the rest does. Without it, the score would be ${overall.uncapped}.` : null,
    `Grades: ${GRADES.map((g, i) => (i === GRADES.length - 1 ? `${g.grade} below ${GRADES[i - 1]!.min}` : `${g.grade} from ${g.min}`)).join(", ")}.`,
  );
}

/** Every figure the score's working quotes, under one source. */
function overallFigures(overall: OverallScore): FigureSet["figures"] {
  const number = (value: number) => ({ source: OVERALL, kind: "number" as const, value });
  const fixed = [100, CRITICAL_CAP, ...GRADES.map((g) => g.min), ...Object.values(AREA_WEIGHTS)].map(number);
  if (overall.status === "not_measurable") return fixed;
  return [...fixed, number(overall.score), { ...number(overall.score), unit: "points" }, number(overall.uncapped), ...overall.areas.flatMap((a) => [number(a.score), number(a.weight)])];
}

/** The money in a few sentences: the total, each kind, and how it was counted. */
function valueText(value: ValueModel): string {
  const m = (n: number) => money(n, value.currency);
  const { total, byKind } = value;
  const exposure = value.groups.filter((g) => g.kind === "exposure").sort((x, y) => y.counted.annual - x.counted.annual)[0];
  if (total.expected === 0) return "None of the issues we found could be given a figure in money, so there's no total. Each one is rated by the risk it carries instead.";
  const kinds = HEADLINE_KINDS.filter((k) => byKind[k].expected > 0).map((k) => `${KIND_LABELS[k]}: ${m(byKind[k].expected)}, between ${m(byKind[k].low)} and ${m(byKind[k].high)}`);
  return lines(
    `Over the next 12 months, the issues we found are worth about ${m(total.expected)} to you, and between ${m(total.low)} and ${m(total.high)}.`,
    kinds.join("\n"),
    "Figures that describe the same money are counted once: in each group below, only the largest counts. Each issue's figure is what's there if every case were won back, so we count a share of it, and the expected case is the conservative one. The items below show each figure, its share and why.",
    byKind.time_cost.expected > 0
      ? value.hourly.source === "customer"
        ? `Time is costed at the hourly figure you gave us, £${value.hourly.rate}.`
        : `Time is costed at £${value.hourly.rate} an hour: ${value.hourly.working.join(" ")}`
      : null,
    exposure ? `Separately, the largest exposure we found is this: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). Pipeline at risk, a gap in coverage or spend that isn't coming back is exposure, not money found, so it's never added to the total.` : null,
  );
}

/** Every figure the money's working quotes: each group under its own source, and the totals. */
function valueFigures(value: ValueModel): FigureSet["figures"] {
  const cash = (source: string, amount: number) => ({ source, kind: "currency" as const, value: amount, currency: value.currency });
  const number = (source: string, n: number) => ({ source, kind: "number" as const, value: n });
  const percent = (source: string, n: number) => ({ source, kind: "percent" as const, value: n });
  const b = UK_HOURLY_BENCHMARK;
  const totals = [value.total, ...Object.values(value.byKind)].flatMap((r) => [r.low, r.expected, r.high]).map((n) => cash(VALUE_TOTAL, n));
  const duration = (n: number, unit: string) => ({ source: VALUE_TOTAL, kind: "duration" as const, value: n, unit });
  const worked = value.hourly.figures ? [value.hourly.figures.ni, value.hourly.figures.pension, value.hourly.figures.loaded].map((n) => cash(VALUE_TOTAL, n)) : [];
  const rate = [...worked, duration(12, "month"), duration(b.hoursPerWeek * b.weeksPerYear, "hour"), duration(b.hoursPerWeek, "hour"), number(VALUE_TOTAL, 1), cash(VALUE_TOTAL, value.hourly.rate), ...[b.salary, b.niThreshold, b.pensionFrom, b.pensionTo].map((n) => cash(VALUE_TOTAL, n)), ...[b.hoursPerWeek, b.hoursPerWeek * b.weeksPerYear, 12, 2025].map((n) => number(VALUE_TOTAL, n)), ...[b.employerNiRate, b.pensionRate].map((n) => percent(VALUE_TOTAL, n * 100))];
  const groups = value.groups.flatMap((g) => {
    const source = valueSource(g.id);
    const shares = [g.realisation.low, g.realisation.expected, g.realisation.high].map((n) => percent(source, n * 100));
    return [...shares, ...g.lines.flatMap((l) => [cash(source, l.annual), cash(source, l.low), cash(source, l.expected), cash(source, l.high)])];
  });
  return [...totals, ...rate, ...groups];
}

function rawNotesSpecs(input: ReportInput, overall: OverallScore, comparisons: Comparison[], value: ValueModel, investment: Investment, effectiveMetrics: Metric[]): Spec[] {
  const { run, calibration, findings, metrics, manualSteps, intake, overrides = [] } = input;
  const exclusions = [...new Set([...findings.flatMap((f) => f.evidence.flatMap((e) => e.exclusions)), ...metrics.flatMap((m) => m.exclusions)])];
  const notMeasurable = findings.filter((f) => f.status === "not_measurable");
  const domains = domainsInOrder(findings).filter((d) => findings.some((f) => f.domain === d));
  return [
    intake
      ? { key: "intake_summary", title: "Intake summary", body: intakeText(intake), sources: [INTAKE] }
      : { key: "intake_summary", title: "Intake summary", body: "No intake has been recorded for this run.", needsInput: true },
    { key: "assumptions", title: "Assumptions", body: calibrationText(run, calibration), sources: [SOURCE.run, CALIBRATION] },
    { key: "barometer_score", title: "Barometer score", body: overallText(overall), sources: [OVERALL] },
    {
      key: "investment",
      title: "Investment",
      // With overrides applied, as the dataset has them: this text quotes the figures.
      body: investmentText(investment, effectiveMetrics).join("\n\n"),
      metricIds: metrics.filter((m) => INVESTMENT_METRIC_IDS.includes(m.id)).map((m) => m.id),
      sources: [INVESTMENT_SOURCE],
    },
    { key: "value_model", title: "The money", body: valueText(value), sources: [VALUE_TOTAL, ...value.groups.map((g) => valueSource(g.id))] },
    ...domains.map((domain): Spec => {
      const own = findings.filter((f) => f.domain === domain);
      return { key: `findings_${domain}`, title: `Findings: ${domainLabel(domain)}`, body: `Every check we ran for ${domainLabel(domain).toLowerCase()}, with what it found and how to reproduce it.`, findingIds: own.map((f) => f.id) };
    }),
    // The investment figures have their own section above.
    { key: "pipeline_metrics", title: "Pipeline metrics", body: "How each pipeline figure was calculated, with its sample, exclusions and assumptions.", metricIds: metrics.filter((m) => !INVESTMENT_METRIC_IDS.includes(m.id)).map((m) => m.id) },
    {
      key: "comparisons",
      title: "How you compare",
      body: lines(
        "Where your headline figures sit against published research and against other Black Cloud customers. We never combine them: published research measures other companies in its own way, so each published figure says what it measures and how that differs from ours.",
        `Our own customers' range is shown from ${OWN_RANGE_MIN} orgs, and groups by industry and size from ${BENCHMARK_MIN_GROUP}. Where your group is too small, we widen it: industry and size, then industry, then size, then every customer.`,
      ),
      sources: [COMPARISONS, ...comparisons.map((c) => comparisonId(c.metric.key))],
    },
    { key: "queries", title: "Queries", body: "The queries and metadata reads behind these notes, so each finding can be reproduced.", findingIds: findings.map((f) => f.id), metricIds: metrics.map((m) => m.id) },
    {
      key: "exclusions_limits",
      title: "Exclusions and limits",
      body: lines(exclusions.length ? `What the checks left out:\n${exclusions.join("\n")}` : "No exclusions were applied.", notMeasurable.length ? `What we couldn't measure:\n${notMeasurable.map((f) => `${f.title}: ${f.notMeasurableReason}`).join("\n")}` : null),
      findingIds: findings.map((f) => f.id),
      metricIds: metrics.map((m) => m.id),
    },
    { key: "overrides", title: "Overrides", body: overrides.length ? overrideText(overrides) : "No figures were overridden.", sources: [OVERRIDES] },
    // Last, so the sections the decks point to are numbered without a gap.
    { key: "manual_steps", title: "Manual steps", body: manualSteps.length ? manualSteps.map((s) => `${s.title}: ${s.status}.`).join("\n") : "None raised.", manualStepIds: manualSteps.map((s) => s.id) },
  ];
}

// Rules whose value is potential revenue in open deals. The executive pipeline
// page lists them together, and they overlap, so they're never added up.
export const POTENTIAL_REVENUE_RULES = ["dq.open-opps-past-close", "dq.opps-inactive-owner", "dq.open-opps-no-activity", "cur.stale-deals", "rev.stalled-deals"];

/** The metrics the pipeline pages quote, in the order they're read. */
export const PIPELINE_TODAY_METRICS = ["current.open_deals", "current.open_pipeline", "current.open_no_amount", "current.weighted_pipeline", "current.delivery_outlook_value", "current.delivery_outlook_deals", "historic.stage_win_rate"];

type MeasuredMetric = Extract<Metric, { status: "measured" }>;

const measuredIn = (metrics: Metric[], id: string): MeasuredMetric | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m : null;
};

/** Today's pipeline in a few sentences, from metric figures only. Empty when open deals couldn't be read. */
export function pipelineTodayText(metrics: Metric[], currency: string): string[] {
  const open = measuredIn(metrics, "current.open_deals");
  if (!open) return [];
  const value = measuredIn(metrics, "current.open_pipeline");
  const blank = measuredIn(metrics, "current.open_no_amount");
  const weighted = measuredIn(metrics, "current.weighted_pipeline");
  const outlook = measuredIn(metrics, "current.delivery_outlook_value");
  const deals = measuredIn(metrics, "current.delivery_outlook_deals");
  const parts = [
    `You have ${plural(open.value, "open deal")}${value ? ` worth ${compactMoney(value.value, currency)}` : ""}.${blank && blank.value > 0 ? ` ${blank.value} of them have no amount, so they add nothing to that figure.` : ""}${weighted ? ` Weighted by how often deals at each stage have gone on to win, it's worth ${compactMoney(weighted.value, currency)}.` : ""}`,
  ];
  if (outlook) {
    const first = monthYear(outlook.series[0]!.date);
    const last = monthYear(outlook.series.at(-1)!.date);
    parts.push(`From ${first} to ${last}, we expect about ${compactMoney(outlook.value, currency)} of it to be won${deals ? `, across about ${deals.value} deals` : ""}. That's the figure to plan delivery capacity against, not the full pipeline.`);
  }
  return parts;
}

function deepDiveText(metrics: Metric[], currency: string): string {
  const repeat = measuredIn(metrics, "historic.repeat_win_share");
  const top = measuredIn(metrics, "historic.top_account_share");
  const reliable = measuredIn(metrics, "historic.close_date_reliability");
  const late = measuredIn(metrics, "historic.close_days_late");
  return lines(
    "How deals have really moved through your pipeline, measured from stage history rather than current field values.",
    ...pipelineTodayText(metrics, currency),
    repeat ? `${repeat.value}% of won value came from customers who had bought before.${top ? ` Your top ${top.stats.top} accounts brought in ${top.value}% of it.` : ""}` : null,
    reliable && late ? `${reliable.value}% of won deals closed in the quarter they were first forecast for, and the median won deal closed ${late.value} days after its first close date.` : null,
  );
}

function summaryCounts(findings: Finding[]): PromptSource {
  const issues = issuesOf(findings);
  return {
    id: SOURCE.summary,
    kind: "summary",
    checks: findings.length,
    issues: issues.length,
    critical: issues.filter((f) => f.severity === "critical").length,
    high: issues.filter((f) => f.severity === "high").length,
    passed: findings.filter((f) => f.status === "passed").length,
    notMeasurable: findings.filter((f) => f.status === "not_measurable").length,
  };
}

/** The plan's shape as figures, under the id planSource uses, so a draft of a plan section can say "the first 30 days". */
function planFigures({ phases, later }: Plan): FigureSet["figures"] {
  const number = (value: number) => ({ source: PLAN, kind: "number" as const, value });
  const days = (value: number) => ({ source: PLAN, kind: "duration" as const, value, unit: "day" });
  return [
    ...phases.flatMap((p) => [number(p.findings.length), number(p.from), number(p.to), days(p.to), days(p.to - p.from + 1)]),
    number(later.length),
    number(phases.length),
    // Each phase is a month, as the engine's own text says.
    { source: PLAN, kind: "duration" as const, value: phases.length, unit: "month" },
  ];
}

const planFindings = ({ phases }: Plan): Finding[] => phases.flatMap((p) => p.findings);

/** Stated vs actual. Claude may draft it from the stated problems and the issues; a consultant's own links and assessment come first. */
function statedVsActualSpec(intake: Intake | null | undefined, team: Finding[]): Spec {
  const key = "stated_vs_actual";
  const title = "Stated vs actual";
  const stated = intake?.statedProblems ?? [];
  const placeholder = "A consultant hasn't yet linked the stated problems to findings.";
  if (stated.length === 0) return { key, title, body: placeholder, needsInput: true };
  const linked = stated.filter((p) => p.findingIds.length > 0);
  const linkedIds = [...new Set(linked.flatMap((p) => p.findingIds))];
  // The linked findings, whatever their status, then every issue, so Claude can say what matters more.
  const shown = [...team.filter((f) => linkedIds.includes(f.id) && f.status !== "issue"), ...issuesOf(team)];
  return {
    key,
    title,
    body: linked.length ? linked.map((p) => `${p.statement}${p.assessment ? ` ${p.assessment}` : ""}`).join("\n\n") : placeholder,
    findingIds: linkedIds,
    // The issues are in scope without being the consultant's links.
    sources: [INTAKE, ...shown.map((f) => f.id)],
    needsInput: linked.length === 0,
    brief: {
      purpose:
        "Set what you told us against what we found. For each stated problem, say whether our findings support it, partly support it or don't support it, naming the issues that bear on it. Where a problem has our own assessment, follow it. Then say what we found that matters more than the problems you raised, if anything does.",
      sources: [statedProblemsSource(INTAKE, stated), ...shown.map(findingSource)],
      fillsPlaceholder: linked.length === 0,
    },
  };
}

/** The executive pipeline page: what's open, what it's likely to be worth, and the potential revenue in deals that have stalled. */
function pipelineSpec(input: ReportInput, team: Finding[]): Spec {
  const metrics = input.metrics.filter((m) => PIPELINE_TODAY_METRICS.includes(m.id));
  const potential = POTENTIAL_REVENUE_RULES.flatMap((id) => team.find((f) => f.ruleId === id && f.status === "issue") ?? []);
  const today = pipelineTodayText(input.metrics, input.calibration.currency);
  const sized = potential.filter((f) => f.value.kind === "revenue");
  return {
    key: "pipeline",
    title: "Pipeline",
    body: lines(
      ...(today.length ? today : ["Open deals couldn't be measured in this run, so we can't say what the pipeline holds."]),
      sized.length
        ? "Old deals are still potential deals. Each line below sizes one kind of stalled or neglected deal at how often deals like them have still gone on to win here. The lines overlap, so they shouldn't be added together."
        : null,
    ),
    findingIds: potential.map((f) => f.id),
    metricIds: metrics.map((m) => m.id),
    brief: { purpose: "Tell an executive reader what the open pipeline holds today, what it's likely to be worth and when, and the potential revenue in deals that have stalled.", sources: [...metrics.map(metricSource), ...potential.map(findingSource)] },
  };
}

function executiveSpecs(input: ReportInput, team: Finding[]): Spec[] {
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const plan = buildPlan(team);
  const planned = planFindings(plan);
  const rows = scorecard(team, input.scores ?? []);
  const scoreSources = rows.filter((r) => r.score !== null).map((r) => `score:${r.domain}`);
  const assessed = rows.filter((r) => r.issues > 0 || team.some((f) => f.domain === r.domain)).length;
  return [
    {
      key: "verdict",
      title: "Verdict",
      body: lines(
        `${verdict.headline}.`,
        `We ran ${plural(team.length, "check")} across ${plural(assessed, "area")} of your Salesforce and ${verdict.issues ? `found ${plural(verdict.issues, "issue")}: ${severityCounts(verdict)}` : "found no issues"}.`,
      ),
      findingIds: top.map((f) => f.id),
      sources: SUMMARY_SOURCES,
      brief: { purpose: "Tell an executive reader, in a few sentences, what state this Salesforce org is in and what matters most.", sources: [summaryCounts(team), ...top.map(findingSource)] },
    },
    { key: "scorecard", title: "Scorecard", body: "Each area of your Salesforce, scored out of 100. Where an area can't be scored fairly, we say why rather than guess.", findingIds: team.map((f) => f.id), sources: [...SUMMARY_SOURCES, ...scoreSources] },
    pipelineSpec(input, team),
    {
      key: "top_issues",
      title: `Top ${TOP_ISSUE_COUNT} issues`,
      body: top.length ? "The issues that matter most, ranked by severity and then by value." : "We found no issues.",
      findingIds: top.map((f) => f.id),
      sources: SUMMARY_SOURCES,
      ...(top.length
        ? {
            brief: {
              purpose: "Introduce the top issues to an executive reader in two or three sentences: what they have in common, and why they come before everything else. Each issue is set out in full straight after this text, so don't go through them one by one.",
              sources: [summaryCounts(team), ...top.map(findingSource)],
            },
          }
        : {}),
    },
    {
      key: "plan_90_day",
      title: "90-day plan",
      body: plan.later.length
        ? "Urgent fixes come first, and each month holds only what a team can take on. The full report has every action, and what comes after 90 days."
        : "Urgent fixes come first, and each month holds only what a team can take on. The full report has every action.",
      findingIds: planned.map((f) => f.id),
      sources: [...SUMMARY_SOURCES, PLAN],
      ...(planned.length
        ? {
            brief: {
              purpose:
                "Introduce the 90-day plan to an executive reader in two or three sentences: what each phase is for, and why the work comes in that order. Each phase's actions are listed straight after this text, so don't go through them one by one. If some actions come after the 90 days, say that the full report has them.",
              sources: [planSource(PLAN, plan), ...planned.map(findingSource)],
            },
          }
        : {}),
    },
  ];
}

function fullSpecs(input: ReportInput, team: Finding[], metrics: Metric[]): Spec[] {
  const { intake } = input;
  const built = buildPlan(team);
  const { phases: plan, later } = built;
  const roadmapFindings = [...planFindings(built), ...later];
  const verdict = verdictOf(team);
  const issues = issuesOf(team);
  const notMeasurable = team.filter((f) => f.status === "not_measurable");
  const passed = team.filter((f) => f.status === "passed");
  const pipelineMetrics = metrics.filter((m) => m.domain === "historic_pipeline" || m.domain === "current_pipeline");
  return [
    intake && intake.statedProblems.length
      ? { key: "stated_problems", title: "Stated problems", body: lines(intake.summary, ...intake.statedProblems.map((p) => p.statement)), sources: [INTAKE] }
      : { key: "stated_problems", title: "Stated problems", body: "The customer's stated problems haven't been recorded for this run.", needsInput: true },
    {
      key: "overview",
      title: "Overview",
      body: lines(`${verdict.headline}.`, verdict.issues ? `We found ${plural(verdict.issues, "issue")}: ${severityCounts(verdict)}.` : "We found no issues."),
      findingIds: issues.map((f) => f.id),
      sources: SUMMARY_SOURCES,
      brief: {
        purpose:
          "Open the full report. In two or three short paragraphs, tell the reader what state their Salesforce is in, the few themes that run through the issues and why they matter to the business, and where to start. The verdict and the counts of issues are shown beside this text, so don't open by repeating them.",
        sources: [summaryCounts(team), ...issues.map(findingSource)],
      },
    },
    ...domainsInOrder(team).map((domain): Spec => {
      const own = team.filter((f) => f.domain === domain);
      const issues = issuesOf(own);
      const label = domainLabel(domain);
      if (own.length === 0) return { key: `domain_${domain}`, title: label, body: "We didn't assess this area, so we make no claim about it." };
      const measured = metrics.filter((m) => m.domain === domain);
      return {
        key: `domain_${domain}`,
        title: label,
        body: issues.length ? `We found ${plural(issues.length, "issue")} here. Start with this: ${issues[0]!.title}.` : "We found no issues here.",
        findingIds: own.map((f) => f.id),
        metricIds: measured.map((m) => m.id),
        sources: [SOURCE.summary],
        brief: { purpose: `Explain what we found in ${label.toLowerCase()}, what it means for the customer, and what to do first.`, sources: [...own.map(findingSource), ...measured.map(metricSource)] },
      };
    }),
    {
      key: "pipeline_deep_dive",
      title: "Pipeline deep dive",
      body: pipelineMetrics.some((m) => m.status === "measured") ? deepDiveText(pipelineMetrics, input.calibration.currency) : "Pipeline metrics couldn't be measured in this run.",
      metricIds: pipelineMetrics.map((m) => m.id),
      brief: {
        purpose: "Explain the pipeline for someone planning delivery as well as someone selling: what's open today and what it's likely to be worth, when it's likely to land, where wins come from, where deals are lost, and how far close dates hold.",
        sources: pipelineMetrics.map(metricSource),
      },
    },
    statedVsActualSpec(intake, team),
    {
      key: "roadmap",
      title: "Roadmap",
      body: [...plan.map((p) => `Days ${p.from} to ${p.to}, ${p.title}: ${plural(p.findings.length, "action")}.`), ...(later.length ? [`After 90 days: ${plural(later.length, "action")}.`] : [])].join("\n"),
      findingIds: roadmapFindings.map((f) => f.id),
      sources: [...SUMMARY_SOURCES, PLAN],
      ...(roadmapFindings.length
        ? {
            brief: {
              purpose:
                "Walk the reader through the roadmap: what each phase is for, why the work comes in that order, and what's left for after the 90 days. Every action is listed in full after this text, so describe the shape of the plan and its priorities rather than each action.",
              sources: [planSource(PLAN, built), ...roadmapFindings.map(findingSource)],
            },
          }
        : {}),
    },
    {
      key: "appendix",
      title: "Appendix",
      body: lines("How we worked, what we couldn't measure, and what passed.", notMeasurable.length ? `We couldn't measure ${plural(notMeasurable.length, "check")}. We report these rather than estimate them.` : null, passed.length ? `${plural(passed.length, "check")} passed.` : null),
      findingIds: [...notMeasurable, ...passed].map((f) => f.id),
      sources: [...SUMMARY_SOURCES, CALIBRATION],
      brief: {
        purpose:
          "Explain how we carried out this health check: the period of data we looked at, the working assumptions behind the figures, what we couldn't measure and why, and how many checks passed. Where something couldn't be measured, say that we report it rather than estimate it.",
        sources: [summaryCounts(team), assumptionsSource(CALIBRATION, input.run, input.calibration), ...notMeasurable.map(findingSource)],
      },
    },
  ];
}

function record(run: Run, output: ReportOutputKey, specs: Spec[], dataset: FigureSet, now: Date, briefs: Map<string, SectionBrief>, scopes: Map<string, string[]>): BuiltOutput {
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  specs.forEach((spec, order) => {
    const id = `${run.id}:${output}:${spec.key}`;
    const ids = [...new Set([...(spec.findingIds ?? []), ...(spec.metricIds ?? []), ...(spec.manualStepIds ?? []), ...(spec.sources ?? [])])];
    scopes.set(id, ids);
    const scope = scopeDataset(dataset, ids);
    const numberCheck = toNumberCheckResult(checkNumbers(spec.body, scope), now);
    const version = SectionVersionSchema.parse({ id: `${id}:v1`, sectionId: id, version: 1, body: spec.body, source: "engine", changedBy: "engine", changedAt: now.toISOString(), changeSummary: "Engine draft", numberCheck });
    versions.push(version);
    sections.push(
      ReportSectionSchema.parse({
        id,
        runId: run.id,
        output,
        key: spec.key,
        title: spec.title,
        order,
        // A failed check on engine text is a bug, and it must stop for a person.
        status: spec.needsInput || !numberCheck.passed ? "needs_input" : "draft",
        currentVersionId: version.id,
        findingIds: spec.findingIds ?? [],
        metricIds: spec.metricIds ?? [],
        manualStepIds: spec.manualStepIds ?? [],
        approval: null,
      }),
    );
    if (spec.brief) briefs.set(id, { title: spec.title, ...spec.brief });
  });
  return { output, sections, versions };
}

export function buildReport(input: ReportInput): BuiltReport {
  const { run, calibration, findings, metrics, manualSteps, scores = [], overrides = [], intake = null } = input;
  const now = input.now ?? new Date(run.asOf);
  const effective = applyOverrides(findings, metrics, overrides);
  const team = effective.findings.map(teamLevelFinding);
  const overall = overallScore(scores, effective.findings);
  const value = buildValueModel(effective.findings, run, calibration.currency, intake?.hourlyCost);
  const comparisons = compareRun({
    metrics: effective.metrics,
    scores,
    findings: effective.findings,
    industry: intake?.industry ?? null,
    headcountBand: intake?.headcountBand ?? null,
    period: quarterOf(run.analysisWindow.to),
    benchmark: input.benchmark ?? null,
  });
  const investment = buildInvestment(effective.metrics, intake, calibration.currency);
  const dataset = buildDataset({
    findings: [...findings, ...team],
    metrics: effective.metrics,
    manualSteps,
    scores,
    run,
    currency: calibration.currency,
    overrides,
    structural: STRUCTURE,
    extraText: [
      { source: CALIBRATION, text: calibrationText(run, calibration) },
      ...(intake ? [{ source: INTAKE, text: intakeText(intake) }] : []),
      // The cover's customer name, which can be a reference such as "ACME-02", and
      // the consultant's, which can read as a number ("Ten Eyck"). No section's scope includes it.
      { source: COVER, text: [intake?.customerName, run.customerRef, input.preparedBy].filter(Boolean).join("\n") },
      ...(overrides.length ? [{ source: OVERRIDES, text: overrideText(overrides) }] : []),
      // The working the raw notes print, so every figure a comparison shows traces to it.
      ...comparisons.map((c) => ({ source: comparisonId(c.metric.key), text: comparisonNotes(c).map((l) => l.text).join("\n") })),
    ],
  });
  // The floors, and the half of a group a quartile range holds ("the middle half").
  dataset.figures.push(
    ...[OWN_RANGE_MIN, BENCHMARK_MIN_GROUP].map((value) => ({ source: COMPARISONS, kind: "number" as const, value })),
    ...[COMPARISONS, ...comparisons.map((c) => comparisonId(c.metric.key))].map((source) => ({ source, kind: "percent" as const, value: 50 })),
  );
  dataset.figures.push(...planFigures(buildPlan(team)), ...overallFigures(overall), ...valueFigures(value));
  // What fixing it is worth against what they pay, as the investment page says it.
  if (investment.annualCost.status === "measured" && investment.annualCost.value > 0 && value.total.expected > 0) {
    dataset.figures.push({ source: VALUE_TOTAL, kind: "ratio", value: Math.round((value.total.expected / investment.annualCost.value) * 10) / 10 });
  }
  // The customer's own total is the one investment figure no metric holds.
  if (investment.annualCost.status === "measured" && investment.annualCost.basis === "customer") {
    dataset.figures.push({ source: INVESTMENT_SOURCE, kind: "currency", value: investment.annualCost.value, currency: calibration.currency });
  }
  const briefs = new Map<string, SectionBrief>();
  const scopes = new Map<string, string[]>();
  const build = (output: ReportOutputKey, specs: Spec[]): BuiltOutput => record(run, output, specs, dataset, now, briefs, scopes);
  return {
    input,
    dataset,
    teamFindings: team.slice().sort(bySeverity),
    metrics: effective.metrics,
    overall,
    value,
    comparisons,
    investment,
    outputs: {
      raw_notes: build("raw_notes", rawNotesSpecs(input, overall, comparisons, value, investment, effective.metrics)),
      executive_summary: build("executive_summary", executiveSpecs(input, team)),
      full_health_check: build("full_health_check", fullSpecs(input, team, effective.metrics)),
    },
    briefs,
    scopes,
  };
}

/** The text of a section's current version. */
export function currentBody(built: BuiltOutput, key: string): string {
  const section = built.sections.find((s) => s.key === key);
  return built.versions.find((v) => v.id === section?.currentVersionId)?.body ?? "";
}
