import { buildDataset, checkNumbers, scopeDataset, SOURCE, toNumberCheckResult, type FigureSet, type StructuralFigure } from "../numbercheck/index.ts";
import type { Calibration, Finding, ManualStep, Metric, NumberOverride, ReportSection, Run, Score, SectionVersion } from "../schemas/index.ts";
import { ReportSection as ReportSectionSchema, SectionVersion as SectionVersionSchema } from "../schemas/index.ts";
import type { SectionBrief } from "./draft.ts";
import type { Intake } from "./intake.ts";
import { findingSource, metricSource, type PromptSource } from "./prompt.ts";
import { buildPlan, bySeverity, domainLabel, domainsInOrder, issuesOf, scorecard, teamLevelFinding, topIssues, verdictOf } from "./select.ts";

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
  now?: Date;
};

export type ReportOutputKey = "raw_notes" | "executive_summary" | "full_health_check";

export type BuiltOutput = { output: ReportOutputKey; sections: ReportSection[]; versions: SectionVersion[] };

export type BuiltReport = {
  input: ReportInput;
  dataset: FigureSet;
  /** Findings as an executive reader sees them. */
  teamFindings: Finding[];
  outputs: Record<ReportOutputKey, BuiltOutput>;
  /** What Claude would be asked for, by section id. Sections with no entry stay engine text. */
  briefs: Map<string, SectionBrief>;
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
  brief?: { purpose: string; sources: PromptSource[] };
};

const SUMMARY_SOURCES = [SOURCE.run, SOURCE.summary, SOURCE.structure];
const INTAKE = "intake";
const CALIBRATION = "calibration";
const OVERRIDES = "overrides";

const lines = (...parts: Array<string | null | undefined | false>): string => parts.filter(Boolean).join("\n\n");
const plural = (n: number, one: string, many = `${one}s`): string => `${n} ${n === 1 ? one : many}`;

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

function rawNotesSpecs(input: ReportInput): Spec[] {
  const { run, calibration, findings, metrics, manualSteps, intake, overrides = [] } = input;
  const exclusions = [...new Set([...findings.flatMap((f) => f.evidence.flatMap((e) => e.exclusions)), ...metrics.flatMap((m) => m.exclusions)])];
  const notMeasurable = findings.filter((f) => f.status === "not_measurable");
  const domains = domainsInOrder(findings).filter((d) => findings.some((f) => f.domain === d));
  return [
    intake
      ? { key: "intake_summary", title: "Intake summary", body: intakeText(intake), sources: [INTAKE] }
      : { key: "intake_summary", title: "Intake summary", body: "No intake has been recorded for this run.", needsInput: true },
    { key: "assumptions", title: "Assumptions", body: calibrationText(run, calibration), sources: [SOURCE.run, CALIBRATION] },
    { key: "manual_steps", title: "Manual steps", body: manualSteps.length ? manualSteps.map((s) => `${s.title}: ${s.status}.`).join("\n") : "None raised.", manualStepIds: manualSteps.map((s) => s.id) },
    ...domains.map((domain): Spec => {
      const own = findings.filter((f) => f.domain === domain);
      return { key: `findings_${domain}`, title: `Findings: ${domainLabel(domain)}`, body: `Every check we ran for ${domainLabel(domain).toLowerCase()}, with what it found and how to reproduce it.`, findingIds: own.map((f) => f.id) };
    }),
    { key: "pipeline_metrics", title: "Pipeline metrics", body: "How each pipeline figure was calculated, with its sample, exclusions and assumptions.", metricIds: metrics.map((m) => m.id) },
    { key: "queries", title: "Queries", body: "The queries and metadata reads behind these notes, so each finding can be reproduced.", findingIds: findings.map((f) => f.id), metricIds: metrics.map((m) => m.id) },
    {
      key: "exclusions_limits",
      title: "Exclusions and limits",
      body: lines(exclusions.length ? `What the checks left out:\n${exclusions.join("\n")}` : "No exclusions were applied.", notMeasurable.length ? `What we couldn't measure:\n${notMeasurable.map((f) => `${f.title}: ${f.notMeasurableReason}`).join("\n")}` : null),
      findingIds: findings.map((f) => f.id),
      metricIds: metrics.map((m) => m.id),
    },
    { key: "overrides", title: "Overrides", body: overrides.length ? overrideText(overrides) : "No figures were overridden.", sources: [OVERRIDES] },
  ];
}

function summaryCounts(findings: Finding[]): PromptSource {
  const issues = issuesOf(findings);
  return { id: SOURCE.summary, kind: "summary", checks: findings.length, issues: issues.length, critical: issues.filter((f) => f.severity === "critical").length, high: issues.filter((f) => f.severity === "high").length };
}

function executiveSpecs(input: ReportInput, team: Finding[]): Spec[] {
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const plan = buildPlan(team);
  const rows = scorecard(team, input.scores ?? []);
  const scoreSources = rows.filter((r) => r.score !== null).map((r) => `score:${r.domain}`);
  const assessed = rows.filter((r) => r.issues > 0 || team.some((f) => f.domain === r.domain)).length;
  return [
    {
      key: "verdict",
      title: "Verdict",
      body: lines(
        `${verdict.headline}.`,
        `We ran ${plural(team.length, "check")} across ${plural(assessed, "domain")} and found ${plural(verdict.issues, "issue")}: ${verdict.critical} critical and ${verdict.high} high.`,
      ),
      findingIds: top.map((f) => f.id),
      sources: SUMMARY_SOURCES,
      brief: { purpose: "Tell an executive reader, in a few sentences, what state this Salesforce org is in and what matters most.", sources: [summaryCounts(team), ...top.map(findingSource)] },
    },
    { key: "scorecard", title: "Scorecard", body: "How each domain stands. A domain with no score says why.", findingIds: team.map((f) => f.id), sources: [...SUMMARY_SOURCES, ...scoreSources] },
    {
      key: "top_issues",
      title: `Top ${TOP_ISSUE_COUNT} issues`,
      body: top.length ? top.map((f) => f.summary).join("\n\n") : "We found no issues.",
      findingIds: top.map((f) => f.id),
      sources: SUMMARY_SOURCES,
    },
    {
      key: "plan_90_day",
      title: "90-day plan",
      body: plan.map((p) => `Days ${p.from} to ${p.to}, ${p.title}: ${p.findings.length ? p.findings.slice(0, TOP_ISSUE_COUNT).map((f) => f.recommendation).join(" ") : "Nothing planned for this phase."}`).join("\n\n"),
      findingIds: plan.flatMap((p) => p.findings.map((f) => f.id)),
      sources: SUMMARY_SOURCES,
    },
  ];
}

function fullSpecs(input: ReportInput, team: Finding[]): Spec[] {
  const { metrics, intake } = input;
  const plan = buildPlan(team);
  const verdict = verdictOf(team);
  const notMeasurable = team.filter((f) => f.status === "not_measurable");
  const passed = team.filter((f) => f.status === "passed");
  const linked = intake?.statedProblems.filter((p) => p.findingIds.length > 0) ?? [];
  const pipelineMetrics = metrics.filter((m) => m.domain === "historic_pipeline" || m.domain === "current_pipeline");
  return [
    intake && intake.statedProblems.length
      ? { key: "stated_problems", title: "Stated problems", body: lines(intake.summary, ...intake.statedProblems.map((p) => p.statement)), sources: [INTAKE] }
      : { key: "stated_problems", title: "Stated problems", body: "The customer's stated problems haven't been recorded for this run.", needsInput: true },
    {
      key: "overview",
      title: "Overview",
      body: lines(`${verdict.headline}.`, `We found ${plural(verdict.issues, "issue")}, of which ${verdict.critical} are critical and ${verdict.high} are high.`),
      findingIds: issuesOf(team).map((f) => f.id),
      sources: SUMMARY_SOURCES,
    },
    ...domainsInOrder(team).map((domain): Spec => {
      const own = team.filter((f) => f.domain === domain);
      const issues = issuesOf(own);
      const label = domainLabel(domain);
      if (own.length === 0) return { key: `domain_${domain}`, title: label, body: "We didn't assess this domain in this run, so we make no claim about it." };
      const measured = metrics.filter((m) => m.domain === domain);
      return {
        key: `domain_${domain}`,
        title: label,
        body: issues.length ? `We found ${plural(issues.length, "issue")} here. ${issues[0]!.summary}` : "We found no issues here.",
        findingIds: own.map((f) => f.id),
        metricIds: measured.map((m) => m.id),
        sources: [SOURCE.summary],
        brief: { purpose: `Explain what we found in ${label.toLowerCase()}, what it means for the customer, and what to do first.`, sources: [...own.map(findingSource), ...measured.map(metricSource)] },
      };
    }),
    {
      key: "pipeline_deep_dive",
      title: "Pipeline deep dive",
      body: pipelineMetrics.some((m) => m.status === "measured") ? "How deals have really moved through your pipeline, measured from stage history rather than current field values." : "Pipeline metrics couldn't be measured in this run.",
      metricIds: pipelineMetrics.map((m) => m.id),
      brief: { purpose: "Explain how deals have really moved through this pipeline: win rate, conversion, cycle time and slippage.", sources: pipelineMetrics.map(metricSource) },
    },
    linked.length
      ? { key: "stated_vs_actual", title: "Stated vs actual", body: linked.map((p) => `${p.statement}${p.assessment ? ` ${p.assessment}` : ""}`).join("\n\n"), findingIds: linked.flatMap((p) => p.findingIds), sources: [INTAKE] }
      : { key: "stated_vs_actual", title: "Stated vs actual", body: "A consultant hasn't yet linked the stated problems to findings.", needsInput: true },
    {
      key: "roadmap",
      title: "Roadmap",
      body: plan.map((p) => `Days ${p.from} to ${p.to}, ${p.title}: ${plural(p.findings.length, "action")}.`).join("\n"),
      findingIds: plan.flatMap((p) => p.findings.map((f) => f.id)),
      sources: [...SUMMARY_SOURCES, "plan"],
    },
    {
      key: "appendix",
      title: "Appendix",
      body: lines("How we worked, what we couldn't measure, and what passed.", notMeasurable.length ? `We couldn't measure ${plural(notMeasurable.length, "check")}. We report these rather than estimate them.` : null, passed.length ? `${plural(passed.length, "check")} passed.` : null),
      findingIds: [...notMeasurable, ...passed].map((f) => f.id),
      sources: SUMMARY_SOURCES,
    },
  ];
}

function record(run: Run, output: ReportOutputKey, specs: Spec[], dataset: FigureSet, now: Date, briefs: Map<string, SectionBrief>): BuiltOutput {
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  specs.forEach((spec, order) => {
    const id = `${run.id}:${output}:${spec.key}`;
    const scope = scopeDataset(dataset, [...(spec.findingIds ?? []), ...(spec.metricIds ?? []), ...(spec.manualStepIds ?? []), ...(spec.sources ?? [])]);
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
  const team = findings.map(teamLevelFinding);
  const plan = buildPlan(team);
  const dataset = buildDataset({
    findings: [...findings, ...team],
    metrics,
    manualSteps,
    scores,
    run,
    currency: calibration.currency,
    overrides,
    structural: STRUCTURE,
    extraText: [
      { source: CALIBRATION, text: calibrationText(run, calibration) },
      ...(intake ? [{ source: INTAKE, text: intakeText(intake) }] : []),
      ...(overrides.length ? [{ source: OVERRIDES, text: overrideText(overrides) }] : []),
    ],
  });
  dataset.figures.push(...plan.map((p) => ({ source: "plan", kind: "number" as const, value: p.findings.length })));
  const briefs = new Map<string, SectionBrief>();
  const build = (output: ReportOutputKey, specs: Spec[]): BuiltOutput => record(run, output, specs, dataset, now, briefs);
  return {
    input,
    dataset,
    teamFindings: team.slice().sort(bySeverity),
    outputs: {
      raw_notes: build("raw_notes", rawNotesSpecs(input)),
      executive_summary: build("executive_summary", executiveSpecs(input, team)),
      full_health_check: build("full_health_check", fullSpecs(input, team)),
    },
    briefs,
  };
}

/** The text of a section's current version. */
export function currentBody(built: BuiltOutput, key: string): string {
  const section = built.sections.find((s) => s.key === key);
  return built.versions.find((v) => v.id === section?.currentVersionId)?.body ?? "";
}
