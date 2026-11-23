import { opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { usersExtractor } from "../../extract/users.ts";
import { orgExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { notSized, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import type { DomainModule } from "../types.ts";
import type { Metric, Value } from "../../schemas/index.ts";
import { loadPipeline } from "./cohort.ts";
import { closedInWindow, dimensions, pipelineMetrics } from "./metrics.ts";
import { deepDiveMetrics } from "./deep-dive.ts";
import { PROCESS_RULES, processMetrics } from "./process.ts";
import { driverMetrics } from "./drivers.ts";
import { priorWinsExtractor } from "../../extract/prior-wins.ts";
import { joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { andList, moneySaid } from "../../engine/said-forms.ts";
import { dimensionWords } from "./process.ts";

// The rules' titles, summaries, recommendations and reasons are written with
// `said`, every stage and segment key with `org`, so the digest can say which
// words are the org's (DECISIONS.md 655, 657). A reason passed on from an
// extract or a metric stays plain: unknown.

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
    // An automatic run's calibration was read from these same settings, and no consultant will add to it (DECISIONS.md 693):
    // the gap is the customer's to close if they choose, so it is said as what we couldn't measure, never as work for a consultant.
    if (missing.length > 0 && ctx.calibration.source === "org_settings") {
      return {
        outcome: "not_measurable",
        reason: said`The calibration doesn't say what these stages mean, as it was read from your org's own stage settings: ${stageList(missing)}. Where a deal is in one, we've used Salesforce's own setting for whether it's open, won or lost, and said so. If you tell us what each means, your next health check can use it.`,
        evidence,
      };
    }
    if (missing.length === 0) return { outcome: "pass", summary: said`The calibration covers all ${seen.size} stages seen in the org.`, evidence, segmentsChecked: [{ dimension: "stage", segments: seen.size }] };
    // Salesforce's flags stand in where a deal relies on one of them (DECISIONS.md 598), so the gap is still a gap to close.
    const flagged = new Set(stages.data.map((s) => s.ApiName));
    const unflagged = missing.filter((s) => !flagged.has(s));
    const consequence = unflagged.length === 0
      ? said`Where a deal in scope is in one, we've used Salesforce's own setting for whether it's open, won or lost, and said so; a check that needs its place in the sales order is reported as not measurable.`
      : said`Where a deal in scope is in one, we've used Salesforce's own setting for whether it's open, won or lost, and said so, except for ${stageList(unflagged)}, which Salesforce's stage settings don't hold; checks that depend on ${unflagged.length === 1 ? "that one" : "those"}, or on a stage's place in the sales order, are reported as not measurable.`;
    return {
      outcome: "issue",
      title: said`The calibration is missing stages`,
      summary: said`${missing.length} stages in the org aren't in the calibration: ${stageList(missing)}. ${consequence}`,
      recommendation: said`Add these stages to the calibration, then re-run.`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      value: notSized(said`This is a gap in the check's setup, not in the customer's org.`),
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
    // Each stage's name and calibrated category are the org's and the calibration's words.
    const conflicts: Said[] = [];
    for (const stage of ctx.calibration.stages) {
      const flag = flags.get(stage.name);
      if (!flag) continue;
      if (stage.category === "won" && !flag.IsWon) conflicts.push(said`${org(stage.name)} is calibrated as won, but Salesforce doesn't flag it as won`);
      if (stage.category === "open" && flag.IsClosed) conflicts.push(said`${org(stage.name)} is calibrated as open, but Salesforce flags it as closed`);
      if ((stage.category === "lost" || stage.category === "no_decision") && !flag.IsClosed) conflicts.push(said`${org(stage.name)} is calibrated as ${org(stage.category)}, but Salesforce flags it as open`);
      if ((stage.category === "lost" || stage.category === "no_decision") && flag.IsWon) conflicts.push(said`${org(stage.name)} is calibrated as ${org(stage.category)}, but Salesforce flags it as won`);
    }
    const orgRead = ctx.data(orgExtractor);
    const orgFiscal = orgRead.status === "ok" ? orgRead.data[0]?.FiscalYearStartMonth : undefined;
    const calibratedFiscal = ctx.calibration.fiscalYearStartMonth;
    if (calibratedFiscal && orgFiscal && calibratedFiscal !== orgFiscal) {
      conflicts.push(said`The calibration starts the fiscal year in month ${calibratedFiscal}, but the org starts it in month ${orgFiscal}`);
    }
    const evidence = [ctx.ev(stages.source, { count: stages.data.length })];
    const segments = [{ dimension: "stage", segments: ctx.calibration.stages.length }];
    if (conflicts.length === 0) return { outcome: "pass", summary: said`The calibration agrees with Salesforce's stage flags.`, evidence, segmentsChecked: segments };
    return {
      outcome: "issue",
      title: said`How stages are used differs from how they're set up`,
      summary: said`${joinSaid(conflicts, ". ")}. We've followed the calibration, but Salesforce's standard reports won't match these figures.`,
      recommendation: said`Confirm the calibration with the customer. If it's right, fix the stage settings so standard reports agree.`,
      recommendationType: "investigate",
      severity: "medium",
      confidence: "high",
      evidence,
      value: notSized(said`The effect depends on which reports people use.`),
      effort: { size: "XS" },
      owner: "Sales operations",
      segmentsChecked: segments,
      facts: { conflicts: conflicts.length },
    };
  },
};

function divergenceValue(ctx: RuleContext, metric: Extract<Metric, { status: "measured" }>, segment: { dimension: string; key: string; value: number | null }): Value {
  if (metric.id !== "historic.win_rate") return notSized(said`Slipped deals move revenue later rather than losing it, so this has no revenue figure.`);
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return notSized(data.reason);
  if (data.multiCurrency) return notSized(said`The org is multi-currency, so these amounts can't be added.`);
  const keyOf = dimensions(ctx)[segment.dimension];
  if (!keyOf) return notSized(said`We can't rebuild the ${org(segment.dimension)} segment to size it.`);
  const deals = data.deals.filter((d) => (d.category === "won" || d.category === "lost") && closedInWindow(d, ctx.window) && keyOf(d) === segment.key);
  const amount = deals.reduce((sum, d) => sum + (d.Amount ?? 0), 0);
  const points = metric.value - segment.value!;
  if (amount <= 0) return notSized(said`These deals have no amounts.`);
  return {
    kind: "revenue",
    amount: Math.round((amount * points) / 100),
    unit: ctx.calibration.currency,
    period: `in wins the gap may have cost, ${ctx.window.from} to ${ctx.window.to}`,
    assumptions: [said`${deals.length} won and lost deals in this segment, with a total Amount of ${moneySaid(amount, ctx.calibration.currency)}`, said`The gap of ${Math.round(points * 10) / 10} points to your overall win rate of ${metric.value}%`, said`Assumes these deals could win as often as the rest. Deal mix or territory may explain part of the gap, so treat this as a ceiling`],
  };
}

const stageList = (stages: string[]): Said => joinSaid(stages.map((s) => said`${org(s)}`), ", ");

/** A watched metric's label: our own words when it is one of the two we watch, as written in metrics.ts; anything else is treated as the org's. */
const metricWords = (label: string) => (label === "Win rate" ? "Win rate" : label === "Slippage rate" ? "Slippage rate" : org(label));

/**
 * For a rep, the listed teams their closed deals were sold in: the two figures
 * then share deals (DECISIONS.md 405). Empty for anything but a rep.
 */
function teamsAlsoListed(ctx: RuleContext, segment: { dimension: string; key: string }, listedTeams: string[]): string[] {
  if (segment.dimension !== "rep" || listedTeams.length === 0) return [];
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return [];
  const rep = dimensions(ctx).rep!;
  const teams = new Set(data.deals.filter((d) => (d.category === "won" || d.category === "lost") && closedInWindow(d, ctx.window) && rep(d) === segment.key).map((d) => d.team));
  return listedTeams.filter((t) => teams.has(t));
}

/** "the Sales EU team", "the Renewal record type". A rep keeps "rep" so team-level text can say "a single rep", and a default key reads as a group of deals. */
function segmentPhrase(dimension: string, key: string): Said {
  // The key is the org's (a team, a record type, a person's token), lower-cased or not.
  if (dimension === "rep") return said`rep ${org(key)}`;
  if (/^No /.test(key)) return said`deals with ${org(key.toLowerCase())}`;
  return said`the ${org(key)} ${dimensionWords(dimension)}`;
}

// Rule 4 for pipeline numbers: a healthy total can hide a team that has collapsed.
const segmentDivergence: Rule = {
  id: "pipe.segment-divergence",
  version: 2,
  domain: "historic_pipeline",
  title: "Segments far from the overall figure",
  evaluate(ctx): RuleResult | RuleResult[] {
    const watched = ctx.metrics.filter((m) => WATCHED_METRICS.includes(m.id));
    const measured = watched.filter((m): m is Extract<Metric, { status: "measured" }> => m.status === "measured");
    for (const m of measured) ctx.leanOn?.(m.id);
    if (measured.length === 0) {
      const first = watched[0];
      const reason = first?.status === "not_measurable" ? first.reason : said`No pipeline metric could be measured.`;
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
      return { outcome: "pass", summary: said`No segment is ${DIVERGENCE_POINTS} points worse than the overall figure.`, evidence, segmentsChecked };
    }
    outliers.sort((a, b) => b.gap - a.gap);
    const teamsListed = (metricId: string): string[] => outliers.filter((o) => o.metric.id === metricId && o.segment.dimension === "team").map((o) => o.segment.key);
    return outliers.map(({ metric, segment, gap }) => {
      const within = teamsAlsoListed(ctx, segment, teamsListed(metric.id));
      const overlap = within.length ? said` This rep sells in ${andList(within.map((t) => said`the ${org(t)} team`))}, which ${within.length === 1 ? "is" : "are"} also listed, so the two figures share deals.` : said``;
      return {
        outcome: "issue" as const,
        key: `${metric.id}:${segment.dimension}:${segment.key}`,
        title: said`${metricWords(metric.label)} for ${segmentPhrase(segment.dimension, segment.key)} is well ${(segment.value ?? 0) < (metric.value ?? 0) ? "below" : "above"} the overall figure`,
        summary: said`${metricWords(metric.label)} is ${segment.value!}% for ${segmentPhrase(segment.dimension, segment.key)} (${segment.n} deals), against ${metric.value}% overall, a gap of ${gap} points.${overlap}`,
        recommendation: said`Look at what's different about these deals before drawing a conclusion: sample size, deal type and who's working them.`,
        recommendationType: "investigate" as const,
        severity: "high" as const,
        confidence: segment.n >= ctx.calibration.minSample * 2 ? ("high" as const) : ("medium" as const),
        evidence,
        value: divergenceValue(ctx, metric, segment),
        effort: { size: "S" as const },
        owner: "Head of sales",
        segmentsChecked,
        subjects: [{ type: segment.dimension, label: segment.key }],
        facts: { segmentValue: segment.value!, overallValue: metric.value, gap, deals: segment.n },
      };
    });
  },
};

export const historicPipelineModule: DomainModule = {
  id: "historic_pipeline",
  cloud: "sales",
  label: "Historic pipeline",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, priorWinsExtractor],
  metrics: [pipelineMetrics, deepDiveMetrics, processMetrics, driverMetrics],
  rules: [unmappedStages, calibrationContradiction, segmentDivergence, ...PROCESS_RULES],
};
