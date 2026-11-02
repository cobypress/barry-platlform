import { createHash } from "node:crypto";
import type { DomainModule } from "../domains/types.ts";
import type { Extractor } from "../extract/types.ts";
import { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { DESTRUCTIVE_RECOMMENDATIONS, Finding, Metric, type Calibration, type DateRange, type Evidence, type ManualAnswer, type ManualStep } from "../schemas/index.ts";
import { MANUAL_STEPS } from "./manual-steps.ts";
import type { IssueResult, ManualState, Rule, RuleContext, RuleResult } from "./rule.ts";

export type AnalysisInput = {
  modules: DomainModule[];
  extracts: Map<string, unknown>;
  calibration: Calibration;
  now: Date;
  window: DateRange;
  answers: ManualAnswer[];
  pseudonyms: Pseudonymiser;
  /** Real names by user id. Only ever reaches the pseudonymiser. */
  userDirectory: Map<string, { name: string; aliases: string[] }>;
};

export type AnalysisOutput = { findings: Finding[]; metrics: Metric[]; manualSteps: ManualStep[] };

const NOT_SIZED = { kind: "not_sized", reason: "No finding to size." } as const;

export function findingId(ruleId: string, key: string): string {
  // Stable across runs, so a repeat check can line findings up with the baseline.
  return `f-${createHash("sha256").update(`${ruleId}|${key}`).digest("hex").slice(0, 12)}`;
}

/**
 * Rule 3's safety net, independent of how a rule is written. A destructive
 * recommendation with a guard that didn't pass is rewritten to "investigate"
 * and its saving dropped, so it can't reach a report in that form.
 */
export function applyGuards(result: IssueResult): IssueResult {
  if (!DESTRUCTIVE_RECOMMENDATIONS.has(result.recommendationType)) return result;
  const guards = result.guards ?? [];
  const clear = guards.length > 0 && guards.every((g) => g.status === "passed");
  if (clear) return result;
  return {
    ...result,
    recommendationType: "investigate",
    recommendation: result.blockedRecommendation ?? "Check this by hand before changing anything: not every guard passed.",
    value: { kind: "not_sized", reason: "Not counted as a saving, because not every guard passed." },
  };
}

function toFinding(rule: Rule, result: RuleResult, pseudonyms: Pseudonymiser): Finding {
  const shared = {
    id: findingId(rule.id, result.key ?? ""),
    ruleId: rule.id,
    ruleVersion: rule.version,
    domain: rule.domain,
    manualStepIds: result.manualStepIds ?? [],
    subjects: [] as Finding["subjects"],
    guards: [] as Finding["guards"],
    segmentsChecked: [] as Finding["segmentsChecked"],
    segmentExemptReason: null as string | null,
    facts: {} as Finding["facts"],
    dependencies: [] as string[],
    notMeasurableReason: null as string | null,
  };

  let draft: unknown;
  if (result.outcome === "issue") {
    const issue = applyGuards(result);
    draft = {
      ...shared,
      status: "issue",
      title: issue.title,
      summary: issue.summary,
      recommendation: issue.recommendation,
      recommendationType: issue.recommendationType,
      severity: issue.severity,
      confidence: issue.confidence,
      evidence: issue.evidence,
      value: issue.value,
      effort: issue.effort,
      owner: issue.owner,
      dependencies: issue.dependencies ?? [],
      guards: issue.guards ?? [],
      segmentsChecked: issue.segmentsChecked ?? [],
      subjects: issue.subjects ?? [],
      facts: issue.facts ?? {},
    };
  } else {
    draft = {
      ...shared,
      title: rule.title,
      recommendation: "",
      recommendationType: "none",
      severity: "info",
      confidence: "high",
      evidence: result.evidence,
      value: NOT_SIZED,
      effort: { size: "XS" },
      owner: "None",
      ...(result.outcome === "pass"
        ? {
            status: "passed",
            summary: result.summary,
            segmentsChecked: result.segmentsChecked ?? [],
            segmentExemptReason: result.segmentExemptReason ?? null,
            facts: result.facts ?? {},
          }
        : { status: "not_measurable", summary: result.reason, notMeasurableReason: result.reason }),
    };
  }
  // A last pass over the text, in case a rule let a name through.
  return Finding.parse(pseudonyms.scrubDeep(draft));
}

function engineEvidence(ruleId: string, note: string, now: Date): Evidence {
  return { kind: "engine", path: ruleId, count: null, dateRange: null, exclusions: [], capturedAt: now.toISOString(), note };
}

function runRule(rule: Rule, ctx: RuleContext, pseudonyms: Pseudonymiser): Finding[] {
  try {
    const raw = rule.evaluate(ctx);
    const results = Array.isArray(raw) ? raw : [raw];
    return results.map((result) => {
      // Rule 4: a rule isn't allowed to report "no problem" from a total alone.
      if (result.outcome === "pass" && !result.segmentsChecked?.length && !result.segmentExemptReason) {
        return toFinding(rule, { outcome: "not_measurable", key: result.key, reason: "The rule passed without segmenting its data, so the result can't be trusted.", evidence: result.evidence }, pseudonyms);
      }
      return toFinding(rule, result, pseudonyms);
    });
  } catch (err) {
    // One broken rule shouldn't lose the rest of the run, and it must show up.
    const message = err instanceof Error ? err.message : String(err);
    const reason = `This check failed to run: ${message}`;
    return [toFinding(rule, { outcome: "not_measurable", reason, evidence: [engineEvidence(rule.id, message, ctx.now)] }, pseudonyms)];
  }
}

export function analyse(input: AnalysisInput): AnalysisOutput {
  const { modules, extracts, calibration, now, window, answers, pseudonyms, userDirectory } = input;
  const raised = new Map<string, ManualStep>();
  let currentRule: string | null = null;

  const data = <T>(extractor: Extractor<T>): T => {
    if (!extracts.has(extractor.id)) throw new Error(`Extractor "${extractor.id}" didn't run`);
    return extracts.get(extractor.id) as T;
  };

  const manual = (key: string): ManualState => {
    const template = MANUAL_STEPS[key];
    if (!template) throw new Error(`Unknown manual step "${key}"`);
    const answer = answers.find((a) => a.stepKey === key) ?? null;
    const status: ManualStep["status"] = answer ? answer.outcome : "pending";
    const id = `ms-${key}`;
    const step = raised.get(key) ?? { ...template, id, status, ruleIds: [] };
    if (currentRule && !step.ruleIds.includes(currentRule)) step.ruleIds.push(currentRule);
    raised.set(key, step);
    const evidence: Evidence = {
      kind: "manual",
      manualStepId: id,
      status,
      answeredBy: answer?.outcome === "answered" ? answer.answeredBy : null,
      answeredAt: answer?.outcome === "answered" ? answer.answeredAt : null,
      count: null,
      dateRange: null,
      exclusions: [],
      capturedAt: now.toISOString(),
      note: answer?.outcome === "answered" ? `Given by ${answer.method}` : undefined,
    };
    return { step, status, answer, evidence };
  };

  const base: Omit<RuleContext, "metrics"> = {
    calibration,
    now,
    window,
    pseudonyms,
    data,
    manual,
    ev: (source, evidence = {}) => ({
      ...source,
      count: evidence.count ?? null,
      dateRange: evidence.dateRange ?? null,
      exclusions: evidence.exclusions ?? [],
      capturedAt: now.toISOString(),
      ...(evidence.note ? { note: evidence.note } : {}),
    }),
    userLabel: (userId) => {
      const known = userDirectory.get(userId);
      return known ? pseudonyms.label("user", userId, known.name, known.aliases) : pseudonyms.label("user", userId, "Unknown user");
    },
  };

  const metrics = modules.flatMap((m) => (m.metrics ?? []).flatMap((build) => build(base))).map((m) => Metric.parse(pseudonyms.scrubDeep(m)));
  const ctx: RuleContext = { ...base, metrics };
  const findings: Finding[] = [];
  for (const module of modules) {
    for (const rule of module.rules) {
      currentRule = rule.id;
      findings.push(...runRule(rule, ctx, pseudonyms));
    }
  }
  currentRule = null;
  return { findings, metrics, manualSteps: [...raised.values()] };
}
