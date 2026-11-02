import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { Calibration, Evidence, Finding, ManualStep, Metric, Run, Score } from "../schemas/index.ts";

// The Markdown forerunner of the Word raw notes. This is the one place tokens
// become names, because this document is what the customer's admin reads.

const SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"];

function evidenceLine(e: Evidence): string {
  const source =
    e.kind === "soql"
      ? `${e.api === "tooling" ? "Tooling query" : "Query"}: \`${e.query}\``
      : e.kind === "manual"
        ? `Manual step ${e.manualStepId} (${e.status}${e.answeredBy ? `, by ${e.answeredBy} on ${e.answeredAt?.slice(0, 10)}` : ""})`
        : e.kind === "metadata"
          ? `Metadata: \`${e.path}\``
          : e.kind === "rest"
            ? `API: \`${e.path}\``
            : `Engine: ${e.path}`;
  const parts = [
    e.count === null ? "count: not available" : `count: ${e.count}`,
    e.dateRange ? `period: ${e.dateRange.from} to ${e.dateRange.to}` : "period: not date bound",
    e.exclusions.length ? `excludes: ${e.exclusions.join("; ")}` : "excludes: nothing",
    ...(e.note ? [e.note] : []),
  ];
  return `- ${source}\n  - ${parts.join(" | ")}`;
}

function valueLine(f: Finding): string {
  if (f.value.kind === "not_sized") return `Not sized. ${f.value.reason}`;
  return `${f.value.amount} ${f.value.unit} ${f.value.period} (${f.value.kind}). Assumptions: ${f.value.assumptions.join("; ")}.`;
}

function findingBlock(f: Finding): string {
  const lines = [`### ${f.title}`, "", `\`${f.ruleId}\` version ${f.ruleVersion} | Finding ${f.id}`, ""];
  if (f.status === "issue") {
    lines.push(`**Severity:** ${f.severity} | **Confidence:** ${f.confidence} | **Effort:** ${f.effort.size} | **Owner:** ${f.owner}`, "");
  }
  lines.push(f.summary, "");
  if (f.recommendation) lines.push(`**Recommendation (${f.recommendationType}):** ${f.recommendation}`, "");
  if (f.status === "issue") lines.push(`**Value:** ${valueLine(f)}`, "");
  if (f.dependencies.length) lines.push(`**Dependencies:** ${f.dependencies.join("; ")}`, "");
  if (f.subjects.length) lines.push(`**Applies to:** ${f.subjects.map((s) => s.label).join(", ")}`, "");
  if (f.segmentsChecked.length) lines.push(`**Segments checked:** ${f.segmentsChecked.map((s) => `${s.dimension} (${s.segments})`).join(", ")}`, "");
  if (f.segmentExemptReason) lines.push(`**Not segmented because:** ${f.segmentExemptReason}`, "");
  lines.push("**Evidence:**", ...f.evidence.map(evidenceLine), "");
  if (f.guards.length) {
    lines.push("**Guards:**", "", "| Subject | Guard | Result | Detail |", "| --- | --- | --- | --- |");
    for (const g of f.guards) lines.push(`| ${g.subject} | ${g.guardId} | ${g.status} | ${g.detail.replace(/\|/g, "/")} |`);
    lines.push("");
  }
  return lines.join("\n");
}

function metricBlock(m: Metric): string {
  if (m.status === "not_measurable") {
    return [`### ${m.label}`, "", `**Not measurable.** ${m.reason}`, "", `*Definition:* ${m.definition}`, ""].join("\n");
  }
  const unit = m.unit === "percent" ? "%" : m.unit === "days" ? " days" : "";
  const lines = [`### ${m.label}: ${m.value}${unit}`, "", `*Definition:* ${m.definition}`, "", `**Sample:** ${m.n} deals${m.dateRange ? `, ${m.dateRange.from} to ${m.dateRange.to}` : ""}`, ""];
  if (Object.keys(m.stats).length) lines.push(`**Detail:** ${Object.entries(m.stats).map(([k, v]) => `${k} ${v}`).join(", ")}`, "");
  if (m.exclusions.length) lines.push(`**Excludes:** ${m.exclusions.join("; ")}`, "");
  if (m.assumptions.length) lines.push(`**Assumptions:** ${m.assumptions.join("; ")}`, "");
  if (m.segments.length) {
    lines.push("| Segment | Key | Deals | Value |", "| --- | --- | --- | --- |");
    for (const s of m.segments) lines.push(`| ${s.dimension} | ${s.key} | ${s.n} | ${s.value ?? s.notMeasurableReason ?? "not measurable"} |`);
    lines.push("");
  }
  if (m.series.length) {
    lines.push("| Month end | Value | Deals |", "| --- | --- | --- |");
    for (const p of m.series) lines.push(`| ${p.date} | ${p.value ?? p.notMeasurableReason} | ${p.n} |`);
    lines.push("");
  }
  return lines.join("\n");
}

function manualBlock(s: ManualStep): string {
  return [
    `### ${s.title} (${s.status})`,
    "",
    `**Why it matters:** ${s.whyItMatters}`,
    "",
    `**Where to go:** ${s.whereToGo}`,
    "",
    `**What to capture:** ${s.whatToCapture}`,
    "",
    `**If skipped:** ${s.ifSkipped}`,
    "",
  ].join("\n");
}

const scoreLine = (s: Score): string => `| ${s.domain} | ${s.status === "measured" ? s.score : `Not measurable. ${s.reason}`} |`;

export function renderRawNotes(input: { run: Run; calibration: Calibration; findings: Finding[]; metrics: Metric[]; manualSteps: ManualStep[]; pseudonyms: Pseudonymiser; scores?: Score[] }): string {
  const { run, calibration, findings, metrics, manualSteps, pseudonyms, scores = [] } = input;
  const issues = findings.filter((f) => f.status === "issue").sort((a, b) => SEVERITY_ORDER.indexOf(a.severity) - SEVERITY_ORDER.indexOf(b.severity) || a.id.localeCompare(b.id));
  const passed = findings.filter((f) => f.status === "passed");
  const notMeasurable = findings.filter((f) => f.status === "not_measurable");
  const stopped = run.status === "stopped_budget";

  const body = [
    "# Black Cloud Barometer: raw notes",
    "",
    `Run ${run.id} | As of ${run.asOf.slice(0, 10)} | Rule set ${run.ruleSetVersion} | API version ${run.apiVersion}`,
    "",
    "These notes are the working record behind the reports: every check we ran, what it found and how to reproduce it.",
    "",
    "## Summary",
    "",
    `- ${issues.length} issues, ${passed.length} checks passed, ${notMeasurable.length} not measurable`,
    `- ${manualSteps.filter((s) => s.status === "pending").length} manual steps outstanding`,
    `- ${metrics.filter((m) => m.status === "measured").length} of ${metrics.length} pipeline metrics measured`,
    `- API calls: ${run.budget.callsMade} used, stopping at ${Math.round(run.budget.stopAtShare * 100)}% of the org's daily limit`,
    ...(stopped ? [`- **This run is incomplete:** ${run.budget.stoppedReason}`] : []),
    "",
    ...(scores.length ? ["## Scorecard", "", "A domain's score is the share of its weighted checks that came back clean. Checks we couldn't measure don't count either way.", "", "| Domain | Score |", "| --- | --- |", ...scores.map(scoreLine), ""] : []),
    "## Assumptions",
    "",
    `- Analysis window: ${run.analysisWindow.from} to ${run.analysisWindow.to}`,
    `- Calibration confirmed by ${calibration.confirmedBy} on ${calibration.confirmedAt.slice(0, 10)}`,
    `- Record types in scope: ${calibration.scope.recordTypes === "all" ? "all" : calibration.scope.recordTypes.join(", ")}`,
    `- Stage meanings: ${calibration.stages.map((s) => `${s.name} (${s.category})`).join(", ")}`,
    `- A user counts as dormant after ${calibration.dormantDays} days without logging in`,
    `- A figure needs at least ${calibration.minSample} deals behind it`,
    "",
    "## Manual steps",
    "",
    ...(manualSteps.length ? manualSteps.map(manualBlock) : ["None raised.", ""]),
    "## Issues",
    "",
    ...(issues.length ? issues.map(findingBlock) : ["None found.", ""]),
    "## Pipeline metrics",
    "",
    ...metrics.map(metricBlock),
    "## Not measurable",
    "",
    "We report these rather than estimate them.",
    "",
    ...(notMeasurable.length ? notMeasurable.map(findingBlock) : ["Nothing.", ""]),
    "## Checks passed",
    "",
    ...(passed.length ? passed.map(findingBlock) : ["None.", ""]),
  ].join("\n");
  return pseudonyms.restore(body);
}
