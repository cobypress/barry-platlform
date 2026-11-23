import { createHash } from "node:crypto";
import { stagesExtractor } from "../extract/opportunities.ts";
import { marksOfMetric, orgValuesOf, rewordFinding, rewordMetric } from "./automatic-words.ts";
import type { DomainModule } from "../domains/types.ts";
import type { Extractor } from "../extract/types.ts";
import { Pseudonymiser, UNKNOWN_USER } from "../privacy/pseudonymiser.ts";
import { DESTRUCTIVE_RECOMMENDATIONS, Finding, Metric, type Calibration, type DateRange, type Evidence, type ManualAnswer, type ManualStep, type RunScope } from "../schemas/index.ts";
import { fixFields } from "../fix/index.ts";
import { MANUAL_STEPS } from "./manual-steps.ts";
import { notMeasurableKind } from "./not-measurable.ts";
import { joinSaid, markSentence, orgWordsOf, said, textSha256, withOrgWords, type MarkedSentence, type Said } from "./org-words.ts";
import type { IssueResult, ManualState, RecordList, Rule, RuleContext, RuleResult } from "./rule.ts";

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
  /** Complete unless said otherwise. */
  scope?: RunScope;
  /**
   * What a manual step nobody has answered is: `ask`, pending, so a
   * consultant-led run waits for a person; `skip`, skipped, so an automatic run
   * never waits and the rules that needed it say not measurable (DECISIONS.md 693).
   */
  unanswered?: "ask" | "skip";
};

/** An issue's records, by finding. Worker-only: never part of a finding or anything sent on. */
export type FindingRecords = RecordList & { findingId: string; ruleId: string };

export type AnalysisOutput = { findings: Finding[]; metrics: Metric[]; manualSteps: ManualStep[]; records: FindingRecords[] };

const NOT_SIZED = { kind: "not_sized", reason: "No finding to size." } as const;

export function findingId(ruleId: string, key: string): string {
  // Stable across runs, so a repeat check can line findings up with the baseline.
  return `f-${createHash("sha256").update(`${ruleId}|${key}`).digest("hex").slice(0, 12)}`;
}

/**
 * Rule 3's safety net, independent of how a rule is written. A destructive
 * recommendation with a guard that didn't pass is rewritten to "investigate"
 * and its saving dropped, so it can't reach a report in that form.
 *
 * A finding the rule gave no figure keeps its own reason (DECISIONS.md 498):
 * "not counted as a saving" would say a saving was waiting where none ever is.
 * One that sizes a saving and lacks a figure only for want of an agreed price
 * or time says the guards' reason, as one with a figure does: agreeing a price
 * wouldn't size a finding its guards hold back.
 */
/** What a held-back destructive recommendation says when its rule gave nothing else: the engine's own words. */
const GUARD_HELD = "Check this by hand before changing anything: not every guard passed.";
/** What a held-back destructive recommendation's value says when its rule sized a saving: the engine's own words. */
const GUARD_UNSIZED = "Not counted as a saving, because not every guard passed.";

export function applyGuards(result: IssueResult): IssueResult {
  if (!DESTRUCTIVE_RECOMMENDATIONS.has(result.recommendationType)) return result;
  const guards = result.guards ?? [];
  const clear = guards.length > 0 && guards.every((g) => g.status === "passed");
  if (clear) return result;
  const ownReason = result.value.kind === "not_sized" && !result.sizesASaving;
  return {
    ...result,
    recommendationType: "investigate",
    recommendation: result.blockedRecommendation ?? GUARD_HELD,
    value: ownReason ? result.value : { kind: "not_sized", reason: GUARD_UNSIZED },
  };
}

function toFinding(rule: Rule, result: RuleResult, pseudonyms: Pseudonymiser, assumed = false): Finding {
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
      // A pass that leaned on an assumption is no surer than medium (DECISIONS.md 598).
      confidence: assumed && result.outcome === "pass" ? "medium" : "high",
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
        : { status: "not_measurable", summary: result.reason, notMeasurableReason: result.reason, notMeasurableKind: notMeasurableKind(result.reason) }),
    };
  }
  // A last pass over the text, in case a rule let a name through.
  const cleaned = pseudonyms.scrubDeep(draft) as Finding;
  const scrubbed = { ...cleaned, ...orgWordsFor(rule, draft as Finding, cleaned) };
  if (result.outcome !== "issue") return Finding.parse(scrubbed);
  // Hours to fix and a fix prompt (DECISIONS.md 628), from the scrubbed text, so a
  // prompt is built from what the finding says and nothing the scrub took out.
  // A remove, deactivate or merge a guard held back is now "investigate", which the prompt reads as held back.
  const { fields: fix, promptWords } = fixFields(rule.id, scrubbed, pseudonyms);
  // The hours' basis is put together from the fix table's fixed words and the
  // finding's whole-number counts alone (effortHours is given nothing else), so
  // it holds no word the org chose: marked with no spans as it's made (DECISIONS.md 663).
  const basis = fix.effortHours ? { hoursBasis: { spans: [], textSha256: textSha256(fix.effortHours.basis) } } : {};
  // The fix prompt's org words, marked where it was put together (DECISIONS.md 672).
  const prompt = fix.fixPrompt && promptWords ? { fixPrompt: promptWords } : {};
  const marks = { ...basis, ...prompt };
  return Finding.parse({ ...scrubbed, ...fix, ...(Object.keys(marks).length > 0 ? { orgWords: { ...scrubbed.orgWords, ...marks } } : {}) });
}

const MARKED = ["title", "summary", "recommendation", "notMeasurableReason"] as const;

/**
 * The sentences this analysis can vouch for, each with the characters it took
 * from the org marked (DECISIONS.md 655): those `said` made, with their spans
 * from where they were put together. A pass's or not measurable finding's title
 * is the rule's own constant, and an empty recommendation holds no words. A
 * sentence the scrub changed loses its marks, so a span can never sit on a token.
 */
function orgWordsFor(rule: Rule, before: Finding, after: Finding): { orgWords?: Finding["orgWords"] } {
  const orgWords: NonNullable<Finding["orgWords"]> = {};
  for (const key of MARKED) {
    const text = before[key];
    if (typeof text !== "string" || text !== after[key]) continue;
    const ownWords = text === "" || (key === "title" && before.status !== "issue" && text === rule.title) || (key === "recommendation" && text === GUARD_HELD);
    const marked = markSentence(text, ownWords ? [] : orgWordsOf(text));
    if (marked) orgWords[key] = marked;
  }
  Object.assign(orgWords, valueOrgWords(before.value, after.value));
  return Object.keys(orgWords).length > 0 ? { orgWords } : {};
}

/**
 * A value's reason, or its assumptions, as this analysis can vouch for them
 * (DECISIONS.md 663). The assumptions are marked all together or not at all, so
 * a reader never takes one unmarked assumption among marked ones for the
 * engine's. The two fixed reasons the engine gives itself are its own words.
 */
function valueOrgWords(was: Finding["value"], now: Finding["value"]): Pick<NonNullable<Finding["orgWords"]>, "valueReason" | "valueAssumptions"> {
  if (was.kind === "not_sized") {
    if (now.kind !== "not_sized" || now.reason !== was.reason) return {};
    const marked = markSentence(was.reason, was.reason === NOT_SIZED.reason || was.reason === GUARD_UNSIZED ? [] : orgWordsOf(was.reason));
    return marked ? { valueReason: marked } : {};
  }
  if (now.kind === "not_sized" || now.assumptions.length !== was.assumptions.length || now.assumptions.some((a, i) => a !== was.assumptions[i])) return {};
  const marks = was.assumptions.map((a) => markSentence(a)).filter((m): m is MarkedSentence => m !== null);
  return marks.length > 0 && marks.length === was.assumptions.length ? { valueAssumptions: marks } : {};
}

function engineEvidence(ruleId: string, note: string, now: Date): Evidence {
  return { kind: "engine", path: ruleId, count: null, dateRange: null, exclusions: [], capturedAt: now.toISOString(), note };
}

const CONFIDENCE_RANK = { low: 0, medium: 1, high: 2 } as const;

/**
 * What a result says once the assumptions its rule leaned on are added
 * (DECISIONS.md 598): each one in its text, and a finding's confidence no
 * higher than medium. A not measurable result leaned on nothing.
 */
export function withAssumptions(result: RuleResult, notes: string[]): RuleResult {
  if (notes.length === 0 || result.outcome === "not_measurable") return result;
  // Kept marked only when the summary and every note were made by said (DECISIONS.md 655).
  const parts = [result.summary, ...notes];
  const summary = parts.every((p) => orgWordsOf(p) !== null) ? joinSaid(parts as Said[], " ") : parts.join(" ");
  if (result.outcome === "pass") return { ...result, summary };
  return { ...result, summary, confidence: CONFIDENCE_RANK[result.confidence] > CONFIDENCE_RANK.medium ? "medium" : result.confidence };
}

function runRule(rule: Rule, ctx: RuleContext, pseudonyms: Pseudonymiser, records: FindingRecords[], assumed: Set<string>): Finding[] {
  try {
    const raw = rule.evaluate(ctx);
    const results = (Array.isArray(raw) ? raw : [raw]).map((r) => withAssumptions(r, [...assumed]));
    for (const result of results) {
      if (result.outcome !== "issue" || !result.records?.rows.length) continue;
      // The same last pass as the finding's text, for the words a rule wrote. Not
      // the ids: the scrub would turn a user's id into their token.
      const { object, rows, fix, part } = result.records;
      records.push({
        object,
        rows: rows.map((r) => ({ id: r.id, problem: pseudonyms.scrub(r.problem) })),
        fix: pseudonyms.scrub(fix),
        ...(part ? { part: { total: part.total, rest: pseudonyms.scrub(part.rest), ...(part.label ? { label: pseudonyms.scrub(part.label) } : {}) } } : {}),
        findingId: findingId(rule.id, result.key ?? ""),
        ruleId: rule.id,
      });
    }
    return results.map((result) => {
      // Rule 4: a rule isn't allowed to report "no problem" from a total alone.
      if (result.outcome === "pass" && !result.segmentsChecked?.length && !result.segmentExemptReason) {
        return toFinding(rule, { outcome: "not_measurable", key: result.key, reason: said`The rule passed without segmenting its data, so the result can't be trusted.`, evidence: result.evidence }, pseudonyms);
      }
      return toFinding(rule, result, pseudonyms, assumed.size > 0);
    });
  } catch (err) {
    // One broken rule shouldn't lose the rest of the run, and it must show up.
    const message = err instanceof Error ? err.message : String(err);
    const reason = `This check failed to run: ${message}`;
    return [toFinding(rule, { outcome: "not_measurable", reason, evidence: [engineEvidence(rule.id, message, ctx.now)] }, pseudonyms)];
  }
}

export function analyse(input: AnalysisInput): AnalysisOutput {
  // Each analysis has its own ledger of the sentences said made (DECISIONS.md 655).
  return withOrgWords(() => analyseWithin(input));
}

function analyseWithin(input: AnalysisInput): AnalysisOutput {
  const { modules, extracts, calibration, now, window, answers, pseudonyms, userDirectory, scope = "complete", unanswered = "ask" } = input;
  const raised = new Map<string, ManualStep>();
  let currentRule: string | null = null;
  // What the rule or metric builder running now has leaned on (DECISIONS.md 598).
  let assumed = new Set<string>();

  const data = <T>(extractor: Extractor<T>): T => {
    if (!extracts.has(extractor.id)) throw new Error(`Extractor "${extractor.id}" didn't run`);
    return extracts.get(extractor.id) as T;
  };

  const manual = (key: string): ManualState => {
    const template = MANUAL_STEPS[key];
    if (!template) throw new Error(`Unknown manual step "${key}"`);
    const answer = answers.find((a) => a.stepKey === key) ?? null;
    const status: ManualStep["status"] = answer ? answer.outcome : unanswered === "skip" ? "skipped" : "pending";
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
    scope,
    calibration,
    now,
    window,
    pseudonyms,
    data,
    ran: (extractor) => extracts.has(extractor.id),
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
      return known ? pseudonyms.label("user", userId, known.name, known.aliases) : pseudonyms.label("user", userId, UNKNOWN_USER);
    },
    assume: (note) => {
      assumed.add(note);
    },
    leanOn: (metricId) => {
      for (const note of leanedOn.get(metricId) ?? []) assumed.add(note);
    },
  };
  const leanedOn = new Map<string, string[]>();

  const metrics = modules
    .flatMap((m) => (m.metrics ?? []).flatMap((build) => {
      assumed = new Set();
      const built = build(base);
      const notes = [...assumed];
      for (const metric of built) if (metric.status === "measured" && notes.length > 0) leanedOn.set(metric.id, notes);
      // A measured figure that leaned on an assumption says so among its own.
      return notes.length === 0 ? built : built.map((metric) => (metric.status === "measured" ? { ...metric, assumptions: [...metric.assumptions, ...notes.filter((n) => !metric.assumptions.includes(n))] } : metric));
    }))
    // Every metric that couldn't be measured says which kind of reason it is (DECISIONS.md 627).
    .map((m) => (m.status === "not_measurable" && m.notMeasurableKind === undefined ? { ...m, notMeasurableKind: notMeasurableKind(m.reason) } : m))
    .map((m) => Metric.parse(pseudonyms.scrubDeep(m)));
  assumed = new Set();
  const ctx: RuleContext = { ...base, metrics };
  const findings: Finding[] = [];
  const records: FindingRecords[] = [];
  for (const module of modules) {
    for (const rule of module.rules) {
      currentRule = rule.id;
      assumed = new Set();
      findings.push(...runRule(rule, ctx, pseudonyms, records, assumed));
    }
  }
  currentRule = null;
  assumed = new Set();
  // Only what ended as an issue keeps its records, so a list never outlives its finding.
  const issues = new Set(findings.filter((f) => f.status === "issue").map((f) => f.id));
  // An automatic run's texts said for what they are on it, and its metrics' texts marked (DECISIONS.md 693):
  // last, once every rule has read the metrics as built, so no reworded text is ever put into a sentence said makes.
  if (calibration.source === "org_settings") {
    const stageRows = extracts.get(stagesExtractor.id) as { status: string; data?: Array<{ ApiName: string; MasterLabel: string }> } | undefined;
    const orgValues = orgValuesOf({ metrics, findings, stages: [...calibration.stages.map((s) => s.name), ...(stageRows?.data ?? []).flatMap((s) => [s.ApiName, s.MasterLabel])] });
    return {
      findings: findings.map((f) => Finding.parse(rewordFinding(f))),
      metrics: metrics.map((m) => Metric.parse(rewordMetric(m, marksOfMetric(m, orgValues)))),
      manualSteps: [...raised.values()],
      records: records.filter((r) => issues.has(r.findingId)),
    };
  }
  return { findings, metrics, manualSteps: [...raised.values()], records: records.filter((r) => issues.has(r.findingId)) };
}
