import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { usersExtractor } from "../../extract/users.ts";
import { orgExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { notSized, type Rule, type RuleResult } from "../../engine/rule.ts";
import type { DomainModule } from "../types.ts";
import type { Metric } from "../../schemas/index.ts";
import { pipelineMetrics } from "./metrics.ts";

const DIVERGENCE_POINTS = 15;
const WATCHED_METRICS = ["historic.win_rate", "historic.slippage_rate"];

const unmappedStages: Rule = {
  id: "pipe.unmapped-stages",
  version: 1,
  domain: "historic_pipeline",
  title: "Stages the calibration doesn't cover",
  evaluate(ctx): RuleResult | RuleResult[] {
    const stages = ctx.data(stagesExtractor);
    if (stages.status !== "ok") return { outcome: "not_measurable", reason: `Opportunity stages couldn't be read: ${stages.reason}`, evidence: [ctx.ev(stages.source, { count: null })] };
    const { history, opportunities } = ctx.data(opportunitiesExtractor);
    const known = new Set(ctx.calibration.stages.map((s) => s.name));
    const seen = new Set([
      ...stages.data.filter((s) => s.IsActive).map((s) => s.ApiName),
      ...(opportunities.status === "ok" ? opportunities.data.map((o) => o.StageName) : []),
      ...(history.status === "ok" ? history.data.map((h) => h.StageName) : []),
    ]);
    const missing = [...seen].filter((s) => !known.has(s)).sort();
    const evidence = [ctx.ev(stages.source, { count: stages.data.length })];
    if (missing.length === 0) return { outcome: "pass", summary: `The calibration covers all ${seen.size} stages seen in the org.`, evidence, segmentsChecked: [{ dimension: "stage", segments: seen.size }] };
    return {
      outcome: "issue",
      title: "The calibration is missing stages",
      summary: `${missing.length} stages in the org aren't in the calibration: ${missing.join(", ")}. Metrics that depend on them are reported as not measurable.`,
      recommendation: "Add these stages to the calibration, then re-run.",
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized("This is a gap in the check's setup, not in the customer's org."),
      effort: { size: "XS" },
      owner: "Black Cloud consultant",
      segmentsChecked: [{ dimension: "stage", segments: seen.size }],
      subjects: missing.map((label) => ({ type: "stage", label })),
      facts: { missing: missing.length },
    };
  },
};

const calibrationContradiction: Rule = {
  id: "pipe.calibration-contradicts-org",
  version: 1,
  domain: "historic_pipeline",
  title: "The calibration disagrees with Salesforce's own stage flags",
  evaluate(ctx): RuleResult | RuleResult[] {
    const stages = ctx.data(stagesExtractor);
    if (stages.status !== "ok") return { outcome: "not_measurable", reason: `Opportunity stages couldn't be read: ${stages.reason}`, evidence: [ctx.ev(stages.source, { count: null })] };
    const flags = new Map(stages.data.map((s) => [s.ApiName, s]));
    const conflicts: string[] = [];
    for (const stage of ctx.calibration.stages) {
      const org = flags.get(stage.name);
      if (!org) continue;
      if (stage.category === "won" && !org.IsWon) conflicts.push(`${stage.name} is calibrated as won, but Salesforce doesn't flag it as won`);
      if (stage.category === "open" && org.IsClosed) conflicts.push(`${stage.name} is calibrated as open, but Salesforce flags it as closed`);
      if ((stage.category === "lost" || stage.category === "no_decision") && !org.IsClosed) conflicts.push(`${stage.name} is calibrated as ${stage.category}, but Salesforce flags it as open`);
      if ((stage.category === "lost" || stage.category === "no_decision") && org.IsWon) conflicts.push(`${stage.name} is calibrated as ${stage.category}, but Salesforce flags it as won`);
    }
    const org = ctx.data(orgExtractor);
    const orgFiscal = org.status === "ok" ? org.data[0]?.FiscalYearStartMonth : undefined;
    const calibratedFiscal = ctx.calibration.fiscalYearStartMonth;
    if (calibratedFiscal && orgFiscal && calibratedFiscal !== orgFiscal) {
      conflicts.push(`The calibration starts the fiscal year in month ${calibratedFiscal}, but the org starts it in month ${orgFiscal}`);
    }
    const evidence = [ctx.ev(stages.source, { count: stages.data.length })];
    const segments = [{ dimension: "stage", segments: ctx.calibration.stages.length }];
    if (conflicts.length === 0) return { outcome: "pass", summary: "The calibration agrees with Salesforce's stage flags.", evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: "How stages are used differs from how they're set up",
      summary: `${conflicts.join(". ")}. We've followed the calibration, but Salesforce's standard reports won't match these figures.`,
      recommendation: "Confirm the calibration with the customer. If it's right, fix the stage settings so standard reports agree.",
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized("The effect depends on which reports people use."),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts: { conflicts: conflicts.length },
    };
  },
};

// Rule 4 for pipeline numbers: a healthy total can hide a team that has collapsed.
const segmentDivergence: Rule = {
  id: "pipe.segment-divergence",
  version: 1,
  domain: "historic_pipeline",
  title: "Segments far from the overall figure",
  evaluate(ctx): RuleResult | RuleResult[] {
    const watched = ctx.metrics.filter((m) => WATCHED_METRICS.includes(m.id));
    const measured = watched.filter((m): m is Extract<Metric, { status: "measured" }> => m.status === "measured");
    if (measured.length === 0) {
      const first = watched[0];
      const reason = first?.status === "not_measurable" ? first.reason : "No pipeline metric could be measured.";
      return { outcome: "not_measurable", reason, evidence: first?.evidence ?? [ctx.ev({ kind: "rest", path: "pipeline metrics" }, { count: null })] };
    }
    const outliers = measured.flatMap((metric) => {
      // A low win rate is the bad direction, and a high slippage rate is.
      const adverse = metric.id === "historic.slippage_rate" ? 1 : -1;
      return metric.segments
        .filter((s) => s.value !== null && s.dimension !== "closed quarter" && (s.value - metric.value) * adverse >= DIVERGENCE_POINTS)
        .map((s) => ({ metric, segment: s, gap: Math.round(Math.abs(s.value! - metric.value) * 10) / 10 }));
    });
    const dimensions = new Set(measured.flatMap((m) => m.segments.map((s) => s.dimension)));
    const segmentsChecked = [...dimensions].sort().map((dimension) => ({ dimension, segments: new Set(measured.flatMap((m) => m.segments.filter((s) => s.dimension === dimension).map((s) => s.key))).size }));
    const evidence = measured[0]!.evidence;
    if (outliers.length === 0) {
      return { outcome: "pass", summary: `No segment is ${DIVERGENCE_POINTS} points worse than the overall figure.`, evidence, segmentsChecked };
    }
    outliers.sort((a, b) => b.gap - a.gap);
    return outliers.map(({ metric, segment, gap }) => ({
      outcome: "issue" as const,
      key: `${metric.id}:${segment.dimension}:${segment.key}`,
      title: `${metric.label} for ${segment.dimension} ${segment.key} is well away from the overall figure`,
      summary: `${metric.label} is ${segment.value}% for ${segment.dimension} ${segment.key} (${segment.n} deals), against ${metric.value}% overall, a gap of ${gap} points.`,
      recommendation: "Look at what's different about these deals before drawing a conclusion: sample size, deal type and who's working them.",
      recommendationType: "investigate" as const,
      severity: "high" as const,
      confidence: segment.n >= ctx.calibration.minSample * 2 ? ("high" as const) : ("medium" as const),
      evidence,
      value: notSized("Sizing this needs the value model, which isn't built yet."),
      effort: { size: "S" as const },
      owner: "Head of sales",
      segmentsChecked,
      subjects: [{ type: segment.dimension, label: segment.key }],
      facts: { segmentValue: segment.value!, overallValue: metric.value, gap, deals: segment.n },
    }));
  },
};

export const historicPipelineModule: DomainModule = {
  id: "historic_pipeline",
  cloud: "sales",
  label: "Historic pipeline",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor],
  metrics: [pipelineMetrics],
  rules: [unmappedStages, calibrationContradiction, segmentDivergence],
};
