import { createHash } from "node:crypto";
import { stagesExtractor } from "../extract/opportunities.ts";
import { guardPassedOn, marksOfMetric, orgValuesOf, rewordFinding, rewordMetric } from "./automatic-words.ts";
import type { DomainModule } from "../domains/types.ts";
import type { Extractor } from "../extract/types.ts";
import { Pseudonymiser, UNKNOWN_USER } from "../privacy/pseudonymiser.ts";
import { DESTRUCTIVE_RECOMMENDATIONS, Finding, Metric, type Calibration, type DateRange, type Evidence, type ManualAnswer, type ManualStep, type RunScope } from "../schemas/index.ts";
import { fixFields } from "../fix/index.ts";
import { MANUAL_STEPS } from "./manual-steps.ts";
import { currencyNotNamedNote, currencyStanding, currencyUnknownReason, NO_CURRENCY, UNREAD_MARK } from "./currency.ts";
import { opportunitiesExtractor, type OpportunityExtract } from "../extract/opportunities.ts";
import { moneySymbol } from "../render/format.ts";
import { notMeasurableKind } from "./not-measurable.ts";
import { INSIGHT_METRICS, INSIGHT_RULES } from "../domains/scoring.ts";
import { joinSaid, markSentence, orgWordsOf, said, saidMade, textSha256, withOrgWords, type MarkedSentence, type Said } from "./org-words.ts";
import { passedOnSpans } from "./passed-on.ts";
import { fromModel, fromName } from "../connect/stage-outcomes.ts";
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
  /**
   * Whether this run's digest is checked by code (an automatic run, or a pulse
   * shown through an automatic check's gate): a value's period said made is then
   * marked on its finding, for the digest (DECISIONS.md 756). Off, findings are as before.
   */
  markPeriods?: boolean;
  /**
   * HC_STAGE_NAMES (DECISIONS.md 756): off or absent, a stage the calibration
   * holds as read from its name is set aside for this analysis and Salesforce's
   * flags decide, whatever row a run or a copy of it still carries.
   */
  stageNames?: boolean;
  /**
   * HC_STAGE_NAMES_MODEL (DECISIONS.md 766): a stage read from its name by a
   * model is used only with this and stageNames both on; otherwise it is set
   * aside, as a name's reading is with stageNames off.
   */
  stageNamesModel?: boolean;
};

/** An issue's records, by finding. Worker-only: never part of a finding or anything sent on. */
export type FindingRecords = RecordList & { findingId: string; ruleId: string };

/**
 * `findings` are the checks the documents are made from; `insights` the path insights (DECISIONS.md 703), which the
 * run's findings.json, raw notes, the console and the digest keep and the documents leave out, so a run's documents
 * and everything made from its findings in memory are as they were before them.
 */
export type AnalysisOutput = {
  findings: Finding[];
  insights: Finding[];
  metrics: Metric[];
  insightMetrics: Metric[];
  manualSteps: ManualStep[];
  records: FindingRecords[];
  /**
   * The id of every rule this analysis ran to a result, sorted, each once
   * (DECISIONS.md 718): whatever it found, not measurable included. Never an insight's, which nothing
   * serves; never a rule its scope or modules left out, one whose step
   * (`onlyWith`) wasn't part of the run, or one the calibration gives nothing
   * to compare (`onlyIf`). Each one listed has a finding.
   */
  rulesRun: string[];
};

/** Every finding of an analysis, the insights last: what the run folder and the console keep. */
export const allFindings = (output: Pick<AnalysisOutput, "findings" | "insights">): Finding[] => [...output.findings, ...output.insights];
/** Every measure of an analysis, the insights' last. */
export const allMetrics = (output: Pick<AnalysisOutput, "metrics" | "insightMetrics">): Metric[] => [...output.metrics, ...output.insightMetrics];

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
    // A refused read's reason a rule passed on is known by its words, where said didn't make it (DECISIONS.md 702).
    const passed = !ownWords && !saidMade(text) && (key === "summary" || key === "notMeasurableReason") ? passedOnSpans(text) : null;
    const marked = markSentence(text, ownWords ? [] : (passed ?? orgWordsOf(text)));
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
function valueOrgWords(was: Finding["value"], now: Finding["value"]): Pick<NonNullable<Finding["orgWords"]>, "valueReason" | "valueAssumptions" | "valuePeriod"> {
  if (was.kind === "not_sized") {
    if (now.kind !== "not_sized" || now.reason !== was.reason) return {};
    const own = was.reason === NOT_SIZED.reason || was.reason === GUARD_UNSIZED;
    // A refused read's reason passed on is known by its words (DECISIONS.md 702).
    const marked = markSentence(was.reason, own ? [] : saidMade(was.reason) ? orgWordsOf(was.reason) : passedOnSpans(was.reason));
    return marked ? { valueReason: marked } : {};
  }
  if (now.kind === "not_sized") return {};
  // A period said made, on a run whose digest is checked (DECISIONS.md 756): marked by itself, since it isn't one of the assumptions.
  const spans = markingPeriods && now.period === was.period && saidMade(was.period) ? orgWordsOf(was.period) : null;
  const period = spans ? markSentence(was.period, spans) : null;
  const withPeriod = period ? { valuePeriod: period } : {};
  if (now.assumptions.length !== was.assumptions.length || now.assumptions.some((a, i) => a !== was.assumptions[i])) return withPeriod;
  const marks = was.assumptions.map((a) => markSentence(a)).filter((m): m is MarkedSentence => m !== null);
  return marks.length > 0 && marks.length === was.assumptions.length ? { valueAssumptions: marks, ...withPeriod } : withPeriod;
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

function runRule(rule: Rule, ctx: RuleContext, pseudonyms: Pseudonymiser, records: FindingRecords[], assumed: Set<string>, gate: () => ((r: RuleResult) => RuleResult) | null = () => null, noted: Set<string> = new Set()): Finding[] {
  try {
    const raw = rule.evaluate(ctx);
    // With no currency, whatever the rule said in money is taken out first (DECISIONS.md 700).
    const ungated = Array.isArray(raw) ? raw : [raw];
    const without = gate();
    const results = (without ? ungated.map(without) : ungated).map((r) => withAssumptions(r, [...assumed]));
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
      // Amounts in a currency not known by name are flagged, never said in the finding's own text (DECISIONS.md 700):
      // its page, section and the digest say it once, and its fix prompt, made from its summary, never repeats it.
      const finding = toFinding(rule, result, pseudonyms, assumed.size > 0);
      return noted.size > 0 && finding.status !== "not_measurable" ? Finding.parse({ ...finding, amountsUnnamed: true }) : finding;
    });
  } catch (err) {
    // One broken rule shouldn't lose the rest of the run, and it must show up.
    const message = err instanceof Error ? err.message : String(err);
    const reason = `This check failed to run: ${message}`;
    return [toFinding(rule, { outcome: "not_measurable", reason, evidence: [engineEvidence(rule.id, message, ctx.now)] }, pseudonyms)];
  }
}

/**
 * The calibration as a rule or metric builder reads it, telling the engine
 * each time the currency is read (DECISIONS.md 700), as the unconfirmed stage
 * order tells it each time the order is read (692). Only on an automatic run
 * whose currency isn't known by name, and with the code given: ISO 4217's
 * "XXX", printed as no code at all, or, where the run can't tell, its testing
 * code, which is found and taken out. Every other run reads the calibration
 * itself, so nothing about it changes.
 */
function watchingCurrency(calibration: Calibration, code: string, onRead: () => void): Calibration {
  const watched = { ...calibration };
  Object.defineProperty(watched, "currency", {
    enumerable: true,
    get: () => {
      onRead();
      return code;
    },
  });
  return watched;
}

/** Whether a result or metric says an amount in this currency anywhere: its code, or its symbol. Numbers are never read as one. */
function mentionsCurrency(value: unknown, code: string): boolean {
  const text = JSON.stringify(value);
  const symbol = moneySymbol(code).trim();
  return text.includes(code) || (symbol !== "" && text.includes(symbol));
}

/**
 * A rule's result with no money in it, when the run knows no currency
 * (DECISIONS.md 700): an issue's figure in money is not sized, for the plain
 * reason; a result that still says an amount anywhere is not measurable, for
 * the same reason. Counts, rates and everything else stand as they are.
 */
function withoutCurrency(result: RuleResult, reason: Said): RuleResult {
  let r = result;
  if (r.outcome === "issue" && r.value.kind !== "not_sized" && r.value.unit === UNREAD_MARK) r = { ...r, value: { kind: "not_sized", reason } };
  if (!mentionsCurrency(r, UNREAD_MARK)) return r;
  return { outcome: "not_measurable", ...(r.key !== undefined ? { key: r.key } : {}), reason, evidence: r.evidence.map(noAmountNote), ...(r.manualStepIds ? { manualStepIds: r.manualStepIds } : {}) };
}

/** Evidence kept with a figure taken out for want of a currency, without a note that says an amount. */
function noAmountNote(e: Evidence): Evidence {
  if (e.note === undefined || !mentionsCurrency(e.note, UNREAD_MARK)) return e;
  const { note: _, ...rest } = e;
  return rest as Evidence;
}

/** The same for a metric: one in money, or one that says an amount, is not measurable for the plain reason. */
function metricWithoutCurrency(metric: Metric, reason: Said): Metric {
  // One already not measurable for its own reason keeps it, unless that reason says an amount.
  if (metric.status === "not_measurable" ? !mentionsCurrency(metric, UNREAD_MARK) : metric.unit !== "currency" && !mentionsCurrency(metric, UNREAD_MARK)) return metric;
  const { id, domain, label, definition, unit, evidence, exclusions } = metric;
  return { status: "not_measurable", id, domain, label, definition, unit, evidence: evidence.map(noAmountNote), exclusions: exclusions.filter((e) => !mentionsCurrency(e, UNREAD_MARK)), assumptions: [], reason };
}

export function analyse(input: AnalysisInput): AnalysisOutput {
  // Each analysis has its own ledger of the sentences said made (DECISIONS.md 655).
  const outer = markingPeriods;
  markingPeriods = input.markPeriods === true;
  try {
    return withOrgWords(() => analyseWithin(input));
  } finally {
    markingPeriods = outer;
  }
}

/** Set for one analysis whose digest is checked (DECISIONS.md 756): its findings keep a period's marks. Analyses run one at a time, synchronously. */
let markingPeriods = false;

/** A calibration read from the org's settings without the stages the name rule read (DECISIONS.md 756), or a model (766), as `drop` says; any other, or one with none, as it is. */
function withoutNameReadings(calibration: Calibration, drop: (s: Calibration["stages"][number]) => boolean = fromName): Calibration {
  if (calibration.source !== "org_settings" || !calibration.stages.some(drop)) return calibration;
  return { ...calibration, stages: calibration.stages.filter((s) => !drop(s)) };
}

function analyseWithin(input: AnalysisInput): AnalysisOutput {
  const calibration =
    input.stageNames !== true
      ? withoutNameReadings(input.calibration, (s) => fromName(s) || fromModel(s))
      : input.stageNamesModel === true
        ? input.calibration
        : withoutNameReadings(input.calibration, fromModel);
  const { modules, extracts, now, window, answers, pseudonyms, userDirectory, scope = "complete", unanswered = "ask" } = input;
  const raised = new Map<string, ManualStep>();
  let currentRule: string | null = null;
  // What the rule or metric builder running now has leaned on (DECISIONS.md 598).
  let assumed = new Set<string>();

  const data = <T>(extractor: Extractor<T>): T => {
    if (!extracts.has(extractor.id)) throw new Error(`Extractor "${extractor.id}" didn't run`);
    const output = extracts.get(extractor.id) as T;
    // Whatever reads the step leans on what it says of itself, such as rows left out (DECISIONS.md 702).
    for (const note of extractor.notes?.(output) ?? []) assumed.add(note);
    return output;
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

  // What the run knows of its currency (DECISIONS.md 700). named: a code, as before. plain: one currency not
  // known by name, so amounts are plain numbers, each place that shows one saying so. none: the run can't tell
  // (a failed read at connect, and opportunities that couldn't be described or hold several currencies).
  const standing = currencyStanding(calibration);
  const opportunities = extracts.get(opportunitiesExtractor.id) as OpportunityExtract | undefined;
  const oneCurrencySeen = opportunities?.opportunities.status === "ok" && opportunities.multiCurrency === false;
  const mode: "named" | "plain" | "none" = standing === "org" || (standing === "unread" && oneCurrencySeen) ? "plain" : standing === "unread" ? "none" : "named";
  // Set while a rule runs, so reading the currency there says so on its result; a metric builder's is said by its metrics' unit instead.
  let inRule = false;
  let currencyRead = false;
  let noted = new Set<string>();
  let plainNote: Said | undefined;
  const notNamed = (): Said => (plainNote ??= currencyNotNamedNote());
  let unknownReason: Said | undefined;
  const noCurrency = (): Said => (unknownReason ??= currencyUnknownReason());
  const base: Omit<RuleContext, "metrics"> = {
    scope,
    calibration:
      mode === "named"
        ? calibration
        : watchingCurrency(calibration, mode === "none" ? UNREAD_MARK : NO_CURRENCY, () => {
            currencyRead = true;
            if (mode === "plain" && inRule) noted.add(notNamed());
          }),
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
    assumeOn: (metricIds, note) => {
      if (!inBuilder) return void assumed.add(note);
      for (const id of metricIds) onMetric.set(id, [...(onMetric.get(id) ?? []), note]);
    },
    leanOn: (metricId) => {
      for (const note of leanedOn.get(metricId) ?? []) assumed.add(note);
    },
  };
  const leanedOn = new Map<string, string[]>();
  // Notes said on named metrics of the builder running now (DECISIONS.md 756).
  let onMetric = new Map<string, string[]>();
  let inBuilder = false;

  const metrics = modules
    .flatMap((m) => (m.metrics ?? []).flatMap((build) => {
      assumed = new Set();
      onMetric = new Map();
      inBuilder = true;
      currencyRead = false;
      let made: Metric[];
      try {
        made = build(base);
      } finally {
        inBuilder = false;
      }
      // No currency: nothing in money is measured, for the plain reason (DECISIONS.md 700).
      const built = mode === "none" ? made.map((metric) => (metric.unit === "currency" || currencyRead ? metricWithoutCurrency(metric, noCurrency()) : metric)) : made;
      const builderNotes = [...assumed];
      // And the notes said on one metric alone (DECISIONS.md 756), after the builder's, as each was said.
      const ownNotes = (metric: Metric): string[] => [...builderNotes, ...(onMetric.get(metric.id) ?? []).filter((n) => !builderNotes.includes(n))];
      for (const metric of built) if (metric.status === "measured" && ownNotes(metric).length > 0) leanedOn.set(metric.id, ownNotes(metric));
      // A measured figure in money, its currency not known by name, says so among its assumptions (DECISIONS.md 700). It isn't leaned on: nothing is guessed.
      const notesOf = (metric: Metric): string[] => (mode === "plain" && metric.unit === "currency" ? [...ownNotes(metric), notNamed()] : ownNotes(metric));
      // A measured figure that leaned on an assumption says so among its own.
      return built.map((metric) => {
        const notes = metric.status === "measured" ? notesOf(metric) : [];
        return notes.length === 0 || metric.status !== "measured" ? metric : { ...metric, assumptions: [...metric.assumptions, ...notes.filter((n) => !metric.assumptions.includes(n))] };
      });
    }))
    // Every metric that couldn't be measured says which kind of reason it is (DECISIONS.md 627).
    .map((m) => (m.status === "not_measurable" && m.notMeasurableKind === undefined ? { ...m, notMeasurableKind: notMeasurableKind(m.reason) } : m))
    .map((m) => Metric.parse(pseudonyms.scrubDeep(m)));
  assumed = new Set();
  const ctx: RuleContext = { ...base, metrics };
  const findings: Finding[] = [];
  const records: FindingRecords[] = [];
  const ran = new Set<string>();
  for (const module of modules) {
    for (const rule of module.rules) {
      // A rule run only beside a step no module owns isn't run without it (DECISIONS.md 718): it would say nothing anyway.
      if (rule.onlyWith && !extracts.has(rule.onlyWith.id)) continue;
      // Nor one whose calibration gives it nothing to compare: it isn't run, so it isn't listed as checked.
      if (rule.onlyIf && !rule.onlyIf(calibration)) continue;
      if (!INSIGHT_RULES.has(rule.id)) ran.add(rule.id);
      currentRule = rule.id;
      assumed = new Set();
      currencyRead = false;
      noted = new Set();
      inRule = true;
      findings.push(...runRule(rule, ctx, pseudonyms, records, assumed, () => (mode === "none" && currencyRead ? (r) => withoutCurrency(r, noCurrency()) : null), noted));
      inRule = false;
    }
  }
  currentRule = null;
  assumed = new Set();
  const rulesRun = [...ran].sort();
  // Only what ended as an issue keeps its records, so a list never outlives its finding.
  const issues = new Set(findings.filter((f) => f.status === "issue").map((f) => f.id));
  // An automatic run's texts said for what they are on it, and its metrics' texts marked (DECISIONS.md 693):
  // last, once every rule has read the metrics as built, so no reworded text is ever put into a sentence said makes.
  if (calibration.source === "org_settings") {
    const stageRows = extracts.get(stagesExtractor.id) as { status: string; data?: Array<{ ApiName: string; MasterLabel: string }> } | undefined;
    const orgValues = orgValuesOf({ metrics, findings, stages: [...calibration.stages.map((s) => s.name), ...(stageRows?.data ?? []).flatMap((s) => [s.ApiName, s.MasterLabel])] });
    const reworded = findings.map((f) => Finding.parse(rewordFinding(guardPassedOn(f, orgValues))));
    return {
      findings: reworded.filter((f) => !INSIGHT_RULES.has(f.ruleId)),
      insights: reworded.filter((f) => INSIGHT_RULES.has(f.ruleId)),
      metrics: metrics.filter((m) => !INSIGHT_METRICS.has(m.id)).map((m) => Metric.parse(rewordMetric(m, marksOfMetric(m, orgValues)))),
      insightMetrics: metrics.filter((m) => INSIGHT_METRICS.has(m.id)).map((m) => Metric.parse(rewordMetric(m, marksOfMetric(m, orgValues)))),
      manualSteps: [...raised.values()],
      records: records.filter((r) => issues.has(r.findingId)),
      rulesRun,
    };
  }
  return { findings: findings.filter((f) => !INSIGHT_RULES.has(f.ruleId)), insights: findings.filter((f) => INSIGHT_RULES.has(f.ruleId)), metrics: metrics.filter((m) => !INSIGHT_METRICS.has(m.id)), insightMetrics: metrics.filter((m) => INSIGHT_METRICS.has(m.id)), manualSteps: [...raised.values()], records: records.filter((r) => issues.has(r.findingId)), rulesRun };
}
