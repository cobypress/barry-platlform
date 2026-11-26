import { schemaExtractor } from "../../extract/configuration.ts";
import { named, aside } from "../../engine/named.ts";
import { quotasExtractor, revenueForecastType } from "../../extract/forecast.ts";
import { nextStepsExtractor, opportunitiesExtractor, stagesExtractor } from "../../extract/opportunities.ts";
import { daysBefore } from "../../extract/types.ts";
import { usersExtractor } from "../../extract/users.ts";
import { notSized, pct, plural, segmentRate, type NotMeasurableResult, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { exposure } from "../data-quality/index.ts";
import { loadPipeline, type Deal } from "../historic-pipeline/cohort.ts";
import { potentialRevenue, SITUATIONS, type Situation } from "../historic-pipeline/potential.ts";
import { fiscalStartOf, median, pushesOf, quantile, segment } from "../historic-pipeline/metrics.ts";
import type { DateRange, Evidence, Metric } from "../../schemas/index.ts";
import type { DomainModule } from "../types.ts";
import { meanStat } from "../average.ts";
import { localDay, todayIn, zoneNote, zoneOf } from "../time-zone.ts";
import { daysSince, enteredCurrentStage, quarterBounds, stageEntries, stageRuns } from "./stages.ts";
import { deliveryOutlook, openStageMetrics, OUTLOOK_LABELS, retiredStages } from "./outlook.ts";
import { eng, joinSaid, org, said, type Said, word } from "../../engine/org-words.ts";
import { moneySaid } from "../../engine/said-forms.ts";
import { STUCK_LIMITS_BEFORE_3 } from "../../engine/loosened.ts";
import { judge, pastLine, stageModel, type StageModel, type Stay } from "./stuck-expected.ts";

// Every title, summary, recommendation and reason is written with `said`, and
// every name the org chose (a stage, a record type, a team, a person's token)
// with `org`, so the digest can say which words are the org's (DECISIONS.md 655,
// 657). A reason passed on from an extract quotes Salesforce and stays plain.

/** What a deal with no amount counts as in open pipeline: its definition says "counts as zero" (DECISIONS.md 773). */
const NO_AMOUNT = 0;
const DOMAIN = "current_pipeline";
const MIN_SEGMENT = 10;
const STUCK_PERCENTILE = 0.75;
/** The same line as a percentage, the value "three quarters" is named with (DECISIONS.md 768). */
const STUCK_PCT = STUCK_PERCENTILE * 100;
const QUIET_DAYS = 30;
const REPEAT_PUSHES = 2;
const NEXT_STEP_LIMIT = 30;
const NEXT_STEP_SEGMENT_LIMIT = 60;
const ACCOUNT_SHARE_LIMIT = 25;
const REP_SHARE_LIMIT = 40;
const MIN_REPS = 3;
const MAX_BLANK_AMOUNT_SHARE = 20;
const COVERAGE_TARGET = 3;

type Ready = Extract<ReturnType<typeof loadPipeline>, { status: "ok" }>;

function openDeals(ctx: RuleContext | Omit<RuleContext, "metrics">): { data: Ready; deals: Deal[] } | NotMeasurableResult {
  const data = loadPipeline(ctx);
  if (data.status !== "ok") return { outcome: "not_measurable", reason: data.reason, evidence: data.evidence };
  if (data.unmapped.finalStages.length > 0) return { outcome: "not_measurable", reason: said`The calibration doesn't say whether these stages are open: ${joinSaid(data.unmapped.finalStages.map((s) => said`${org(s)}`), ", ")}.`, evidence: data.evidence };
  return { data, deals: data.deals.filter((d) => d.category === "open") };
}

const DIMENSION_WORDS = ["stage", "record type", "team", "rep"] as const;

/** A dimension's name in a sentence: one of the keys below is our own word; anything else is treated as the org's. */
const dimensionWords = (dimension: string) => DIMENSION_WORDS.find((k) => k === dimension) ?? org(dimension);

const dimensions = (ctx: RuleContext): Record<(typeof DIMENSION_WORDS)[number], (d: Deal) => string> => ({
  stage: (d) => d.StageName,
  "record type": (d) => d.RecordType ?? "No record type",
  team: (d) => d.team,
  rep: (d) => ctx.userLabel(d.OwnerId),
});

/** `problem` is an affected deal's line in the action list, in words that hold no name. */
export type Scoped =
  | {
      deals: Deal[];
      isHit: (d: Deal) => boolean;
      problem?: (d: Deal) => string;
      exclusions?: string[];
      evidence?: Evidence[];
      /** A sentence for the open deals left out of the count, so its figures add up to the open pipeline. */
      leftOut?: Said;
    }
  | { reason: string; evidence?: Evidence[] };

type ShareRule = {
  id: string;
  /** Defaults to current pipeline. Forecast rules share this shape. */
  domain?: string;
  title: string;
  /** Defaults to 1. */
  version?: number;
  totalLimit: number;
  segmentLimit: number;
  severity: "high" | "medium" | "low";
  scope: (ctx: RuleContext, data: Ready, open: Deal[]) => Scoped;
  /** Our own words, for the summary and the evidence's note. */
  scopeLabel: Said;
  issueTitle: Said;
  sentence: (hits: number, n: number, rate: number) => Said;
  recommendation: Said;
  owner: string;
  /** True sizes the raw exposure. A situation sizes potential revenue at how often comparable deals still won. */
  sized: boolean | ((ctx: RuleContext, data: Ready) => Situation);
  /** What to do to each affected deal, for the action list. With the scope's `problem`, the issue lists its deals. */
  fix?: string;
};

// The shape every share-of-open-deals rule takes: scope, hit test, then the
// total and each segment against a limit, so a bad rep can't hide in a good total.
export function shareRule(options: ShareRule): Rule {
  return {
    id: options.id,
    version: options.version ?? 1,
    domain: options.domain ?? DOMAIN,
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const loaded = openDeals(ctx);
      if ("outcome" in loaded) return loaded;
      const { data } = loaded;
      const scoped = options.scope(ctx, data, loaded.deals);
      const base = { ...data.evidence[0]!, dateRange: { from: ctx.window.to, to: ctx.window.to } };
      if ("reason" in scoped) return { outcome: "not_measurable", reason: scoped.reason, evidence: [{ ...base, count: null }, ...(scoped.evidence ?? [])] };
      const evidence: Evidence[] = [{ ...base, count: scoped.deals.length, exclusions: [...data.exclusions, ...(scoped.exclusions ?? [])], note: options.scopeLabel }, ...(scoped.evidence ?? [])];
      if (scoped.deals.length === 0) return { outcome: "pass", summary: said`There are no ${options.scopeLabel} to check.`, evidence, segmentExemptReason: "There are no deals to segment." };
      const result = segmentRate(scoped.deals, scoped.isHit, dimensions(ctx), { segmentLimit: options.segmentLimit, minSegment: MIN_SEGMENT });
      const overLimit = result.rate > options.totalLimit;
      const leftOut = scoped.leftOut ? said` ${scoped.leftOut}` : said``;
      if (!overLimit && result.hotspots.length === 0) {
        return { outcome: "pass", summary: named(said`Affected: ${result.hits} of ${result.n} ${options.scopeLabel} (${result.rate}%), with no segment above ${options.segmentLimit}%.${leftOut}`, { hits: result.hits, n: result.n, rate: [result.rate, "percent"], segmentLimit: [options.segmentLimit, "percent"] }), evidence, segmentsChecked: result.segmentsChecked, facts: { hits: result.hits, n: result.n, rate: result.rate } };
      }
      const worst = result.hotspots[0];
      // A segment's key is the org's: a stage, a record type, a team or a person's token.
      const hidden = !overLimit ? named(said` The total looks acceptable, but by ${dimensionWords(worst!.dimension)}, ${org(worst!.key)} is at ${worst!.rate}% (${worst!.hits} of ${worst!.n}).`, { worstPct: worst!.rate, worstHits: worst!.hits, worstN: worst!.n }) : worst ? named(said` Worst by ${dimensionWords(worst.dimension)}: ${org(worst.key)}, at ${worst.rate}%.`, { worstPct: worst.rate }) : said``;
      return {
        outcome: "issue",
        // Made once when the rule is declared, so said again here, in the analysis, whose ledger vouches for it.
        title: said`${options.issueTitle}`,
        summary: said`${options.sentence(result.hits, result.n, result.rate)}${leftOut}${hidden}`,
        recommendation: said`${options.recommendation}`,
        recommendationType: "change",
        severity: options.severity,
        confidence: "high",
        evidence,
        value:
          typeof options.sized === "function"
            ? potentialRevenue(ctx, data, scoped.deals.filter(scoped.isHit), () => (options.sized as (c: RuleContext, d: Ready) => Situation)(ctx, data))
            : options.sized
              ? exposure(ctx, scoped.deals.filter(scoped.isHit), data.multiCurrency, named(said`${result.hits} deals affected`, { hits: result.hits }))
              : notSized(said`This affects how far the pipeline can be trusted, which has no direct figure.`),
        effort: { size: "S" },
        owner: options.owner,
        segmentsChecked: result.segmentsChecked,
        subjects: result.hotspots.slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })),
        facts: { hits: result.hits, n: result.n, rate: result.rate, ...(worst ? { worstSegmentRate: worst.rate } : {}) },
        ...(scoped.problem && options.fix ? { records: { object: "Opportunity" as const, rows: scoped.deals.filter(scoped.isHit).map((d) => ({ id: d.Id, problem: scoped.problem!(d) })), fix: options.fix } } : {}),
      };
    },
  };
}

/** How long won deals took in each stage, at the 75th percentile. A stage with too few won deals has no line. */
function stageLines(ctx: RuleContext, data: Ready): Map<string, number> {
  const byStage = new Map<string, number[]>();
  for (const deal of data.deals.filter((d) => d.category === "won")) {
    for (const run of stageRuns(deal)) {
      if (data.stages.categoryOf(run.stage) !== "open") continue;
      const days = byStage.get(run.stage);
      if (days) days.push(run.days);
      else byStage.set(run.stage, [run.days]);
    }
  }
  return new Map([...byStage].filter(([, days]) => days.length >= ctx.calibration.minSample).map(([stage, days]) => [stage, quantile(days, STUCK_PERCENTILE)]));
}

/** The open deals stuck-in-stage couldn't compare, in a sentence, or nothing when it compared them all. */
function leftOutSentence(noLine: number, noEntry: number): Said | undefined {
  const n = noLine + noEntry;
  if (n === 0) return undefined;
  const lead = named(said`${n === 1 ? "One more open deal" : said`${n} more open deals`} couldn't be compared`, { leftOut: n, noLine, noEntry });
  const line = (k: number) => said`${k === 1 ? "a stage" : "stages"} with too few won deals to set a line`;
  const entry = (k: number) => said`history that doesn't show when ${k === 1 ? "it" : "they"} entered ${k === 1 ? "its" : "their"} stage`;
  if (!noEntry) return said`${lead}, as ${n === 1 ? "it's" : "they're"} in ${line(n)}.`;
  if (!noLine) return said`${lead}, as ${n === 1 ? "it has" : "they have"} ${entry(n)}.`;
  return named(said`${lead}: ${noLine} in ${line(noLine)}, and ${noEntry} with ${entry(noEntry)}.`, { noLine, noEntry });
}

const stuckInStage = shareRule({
  id: "cur.stuck-in-stage",
  version: 2,
  title: "Open deals stuck in their stage",
  // Read by version 3's judge of what this version would have said (DECISIONS.md 777), so the two can't part.
  totalLimit: STUCK_LIMITS_BEFORE_3.total,
  segmentLimit: STUCK_LIMITS_BEFORE_3.segment,
  severity: "high",
  scopeLabel: said`open deals with a stage to compare`,
  scope(ctx, data, open) {
    const lines = stageLines(ctx, data);
    if (lines.size === 0) return { reason: named(said`No open stage has ${ctx.calibration.minSample} won deals to compare with.`, { minSample: ctx.calibration.minSample }) };
    const deals = open.filter((d) => lines.has(d.StageName) && enteredCurrentStage(d));
    const noLine = open.filter((d) => !lines.has(d.StageName)).length;
    const noEntry = open.filter((d) => lines.has(d.StageName) && !enteredCurrentStage(d)).length;
    return {
      deals,
      isHit: (d) => daysSince(enteredCurrentStage(d)!, ctx.now) > lines.get(d.StageName)!,
      problem: (d) => `In ${d.StageName} for ${daysSince(enteredCurrentStage(d)!, ctx.now)} days, where three quarters of won deals moved on within ${Math.round(lines.get(d.StageName)!)} days`,
      exclusions: [...(noLine ? [named(said`${plural(noLine, "deal")} in stages with too few won deals to compare`, { noLine })] : []), ...(noEntry ? [named(said`${plural(noEntry, "deal")} whose history doesn't show when they entered this stage`, { noEntry })] : [])],
      // Said in the summary too, where "58 of 70" sat beside 76 open deals elsewhere (night 3 reading pass, item 21).
      leftOut: leftOutSentence(noLine, noEntry),
    };
  },
  issueTitle: said`Deals are sitting in stages for longer than your wins do`,
  sentence: (hits, n, rate) => named(said`Open deals that have been in their stage longer than three quarters of your won deals took: ${hits} of ${n} (${rate}%).`, { wonWithinPct: STUCK_PCT }),
  recommendation: said`Go through these in pipeline reviews, and either move each deal on with a dated next step or close it.`,
  owner: "Head of sales",
  sized: true,
  fix: "Move it on with a dated next step, or close it",
});

const DAY_MS = 86_400_000;

/**
 * Each stay in an open stage since `from` (DECISIONS.md 777): one run of the stage in the deal's history, same-stage
 * rows (an Amount or close date edited mid-stay) merged, a re-entry a stay of its own. It ended when the next run
 * began, or when a closed deal closed; it is still running on an open deal in that stage now. The last run of an
 * open deal whose history doesn't show its current stage is left out, as its end isn't known.
 */
function stageStays(deal: Deal, data: Ready, from: Date): Array<{ stage: string; start: Date; end: Date | null }> {
  const runs = stageEntries(deal);
  const out: Array<{ stage: string; start: Date; end: Date | null }> = [];
  for (let i = 0; i < runs.length; i++) {
    const { stage, entered } = runs[i]!;
    if (data.stages.categoryOf(stage) !== "open" || entered < from) continue;
    const next = runs[i + 1];
    if (next) out.push({ stage, start: entered, end: next.entered });
    else if (deal.category === "open") {
      if (deal.StageName === stage) out.push({ stage, start: entered, end: null });
    } else if (deal.closedAt > entered) out.push({ stage, start: entered, end: deal.closedAt });
  }
  return out;
}

/**
 * Version 2's own reading of today's deals (won deals' line, its scope, its rounded shares), for the judges in
 * src/engine/loosened.ts: none when no stage had enough won deals, as version 2 would not have measured it.
 */
function wonLineReading(ctx: RuleContext, data: Ready, open: Deal[]): Record<string, number> {
  const lines = stageLines(ctx, data);
  if (lines.size === 0) return {};
  const deals = open.filter((d) => lines.has(d.StageName) && enteredCurrentStage(d));
  if (deals.length === 0) return { wonLineRate: 0 };
  const read = segmentRate(deals, (d) => daysSince(enteredCurrentStage(d)!, ctx.now) > lines.get(d.StageName)!, dimensions(ctx), { segmentLimit: -Infinity, minSegment: MIN_SEGMENT });
  return { wonLineRate: read.rate, ...(read.hotspots[0] ? { wonLineWorstSegmentRate: read.hotspots[0].rate } : {}) };
}

/** A share for a sentence, to one decimal place, as `pct` gives the observed one. The comparison reads the raw share. */
const shown = (share: number): number => Math.round(share * 10) / 10;

/** Days for a deal's line: one decimal place, a whole number without it. */
const daysText = (d: number): string => (Number.isInteger(d) ? `${d}` : d.toFixed(1));
const dayWord = (d: number): string => (d === 1 ? "day" : "days");

type LeftOut = { few: number; uncovered: number; noEntry: number; beyond: number };

/** The open deals version 3 couldn't compare, in a sentence, or nothing when it compared them all. */
function leftOutSentence3(left: LeftOut): Said | undefined {
  const { few, uncovered, noEntry, beyond } = left;
  const n = few + uncovered + noEntry + beyond;
  if (n === 0) return undefined;
  const lead = named(said`${n === 1 ? "One more open deal" : said`${n} more open deals`} couldn't be compared`, { leftOut: n, few, uncovered, noEntry, beyond });
  const one = n === 1;
  // Each reason as a list item ("2 in stages with ...") and, when it is the only one, as the reason for them all.
  const parts: Array<[number, (k: number) => Said, Said]> = [
    [few, (k) => said`${k === 1 ? "in a stage" : "in stages"} with too few finished stays to set a line`, said`as ${one ? "it's in a stage" : "they're in stages"} with too few finished stays to set a line`],
    [uncovered, (k) => said`${k === 1 ? "in a stage" : "in stages"} whose stays the history we read doesn't cover`, said`as ${one ? "it's in a stage" : "they're in stages"} whose stays the history we read doesn't cover`],
    [noEntry, (k) => said`whose history doesn't show when ${k === 1 ? "it" : "they"} entered ${k === 1 ? "its" : "their"} stage`, said`as ${one ? "its" : "their"} history doesn't show when ${one ? "it" : "they"} entered ${one ? "its" : "their"} stage`],
    [beyond, (k) => said`that ${k === 1 ? "has" : "have"} been in ${k === 1 ? "its" : "their"} stage longer than the history lets us follow stays there`, said`as ${one ? "it has" : "they have"} been in ${one ? "its" : "their"} stage longer than the history lets us follow stays there`],
  ];
  const given = parts.filter(([k]) => k > 0);
  if (given.length === 1) return said`${lead}, ${given[0]![2]}.`;
  return said`${lead}: ${joinSaid(given.map(([k, words]) => named(said`${k} ${words(k)}`, { k })), "; ")}.`;
}

/** Names in a sentence, the org's, with "and" before the last. */
function andList(names: string[]): Said {
  const all = names.map((s) => said`${org(s)}`);
  return all.length === 1 ? all[0]! : said`${joinSaid(all.slice(0, -1), ", ")} and ${all.at(-1)!}`;
}

const STUCK_SCOPE = said`open deals with a stage to compare`;
/** Version 3 passes only when it compared at least this share of the open deals (DECISIONS.md 777). */
export const MIN_COMPARED_SHARE = 0.75;
/** A stage no deal has entered for this many days, a quarter, longer than the gaps a quarterly sales calendar leaves. */
export const NO_INTAKE_DAYS = 90;
/** The fewest entries a quarter, at the read's own rate, for a stage's quarter without one to count. */
export const NO_INTAKE_USUAL = 3;
/** In such a stage, an open deal is past what its stays explain when older than this many times its longest finished stay. */
export const NO_INTAKE_FACTOR = 2;

/**
 * cur.stuck-in-stage version 3, behind HC_STUCK_EXPECTED (DECISIONS.md 777): each stage's stays, finished and still
 * running, give the line and what the same entries would show past it (src/domains/current-pipeline/stuck-expected.ts
 * says how); the total and each segment are compared against their own. Two plain cases need no estimate: a stage
 * nobody enters any more that still holds old deals, and deals in one stage since before the history we read.
 */
const stuckInStageExpected: Rule = {
  id: "cur.stuck-in-stage",
  version: 3,
  domain: DOMAIN,
  title: "Open deals stuck in their stage",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = openDeals(ctx);
    if ("outcome" in loaded) return loaded;
    const { data, deals: open } = loaded;
    const base = { ...data.evidence[0]!, dateRange: { from: ctx.window.to, to: ctx.window.to } };
    const { minSample } = ctx.calibration;
    // A day alone parses as midnight UTC, where the read starts.
    const from = new Date(ctx.window.from);
    const readFrom = ctx.window.from;
    // Every stay since the read's start, by stage, and every entry with its age today.
    const stays = new Map<string, Stay[]>();
    const newest = new Map<string, Date>();
    const all: Array<{ deal: Deal; stage: string; age: number }> = [];
    for (const deal of data.deals) {
      if (deal.category === null || deal.category === "excluded") continue;
      for (const s of stageStays(deal, data, from)) {
        const age = daysSince(s.start, ctx.now);
        if (age < 0) continue;
        const stay: Stay = s.end ? { days: (s.end.getTime() - s.start.getTime()) / DAY_MS, done: true } : { days: age, done: false };
        (stays.get(s.stage) ?? stays.set(s.stage, []).get(s.stage)!).push(stay);
        if (!newest.has(s.stage) || s.start > newest.get(s.stage)!) newest.set(s.stage, s.start);
        all.push({ deal, stage: s.stage, age });
      }
    }
    const models = new Map<string, StageModel>();
    const uncoveredStages: string[] = [];
    for (const [stage, list] of stays) {
      const m = stageModel(list, STUCK_PERCENTILE, minSample);
      if ("why" in m) {
        if (m.why === "uncovered") uncoveredStages.push(stage);
      } else models.set(stage, m);
    }
    uncoveredStages.sort();
    const entered = (d: Deal): Date | null => enteredCurrentStage(d);
    const days = (d: Deal): number => daysSince(entered(d)!, ctx.now);
    // Deals in one stage since before the history we read: older than the whole read, so no estimate is needed to call that long.
    const ancient = open.filter((d) => entered(d) !== null && entered(d)! < from);
    // Stages no deal has entered for a quarter, holding deals older than twice every stay that finished there.
    const retired = [...stays].flatMap(([stage, list]) => {
      const last = newest.get(stage)!;
      const finished = list.filter((s) => s.done);
      // Only a stage entered often enough for a quarter's silence to mean something: at the read's own rate, at least
      // NO_INTAKE_USUAL entries a quarter, so a stage almost every deal skips isn't called one nobody enters any more.
      const usual = (list.length * NO_INTAKE_DAYS) / daysSince(from, ctx.now);
      if (daysSince(last, ctx.now) <= NO_INTAKE_DAYS || finished.length === 0 || usual < NO_INTAKE_USUAL) return [];
      const longest = Math.max(...finished.map((s) => s.days));
      const old = open.filter((d) => d.StageName === stage && entered(d) !== null && entered(d)! >= from && days(d) > NO_INTAKE_FACTOR * longest);
      return old.length >= MIN_SEGMENT ? [{ stage, since: last.toISOString().slice(0, 10), deals: old }] : [];
    });
    // Compared: within what the estimate follows, its horizon. Older ones are past the line, by how much the history can't say.
    const inRead = (d: Deal): boolean => models.has(d.StageName) && entered(d) !== null && entered(d)! >= from;
    const deals = open.filter((d) => inRead(d) && days(d) <= models.get(d.StageName)!.horizon);
    const left: LeftOut = {
      few: open.filter((d) => !models.has(d.StageName) && !uncoveredStages.includes(d.StageName) && !ancient.includes(d)).length,
      uncovered: open.filter((d) => uncoveredStages.includes(d.StageName) && !ancient.includes(d)).length,
      noEntry: open.filter((d) => models.has(d.StageName) && entered(d) === null).length,
      beyond: open.filter((d) => inRead(d) && days(d) > models.get(d.StageName)!.horizon).length,
    };
    const exclusions = [
      ...(left.few ? [named(said`${plural(left.few, "deal")} in stages with too few finished stays to compare`, { few: left.few })] : []),
      ...(left.uncovered ? [named(said`${plural(left.uncovered, "deal")} in stages whose stays the history we read doesn't cover`, { uncovered: left.uncovered })] : []),
      ...(left.noEntry ? [named(said`${plural(left.noEntry, "deal")} whose history doesn't show when they entered this stage`, { noEntry: left.noEntry })] : []),
      ...(left.beyond ? [named(said`${plural(left.beyond, "deal")} in their stage longer than the history lets us follow stays there`, { beyond: left.beyond })] : []),
    ];
    const evidence: Evidence[] = [{ ...base, count: deals.length, exclusions: [...data.exclusions, ...exclusions], note: STUCK_SCOPE }];
    // What versions 1 and 2 would have said, on every reading, so a change of verdict between runs is told apart (DECISIONS.md 732, 754).
    const before = wonLineReading(ctx, data, open);
    const leftWords = leftOutSentence3(left);
    const leftOut = leftWords ? said` ${leftWords}` : said``;
    const uncoveredWords = uncoveredStages.length ? named(said`The history we read, which starts on ${eng(readFrom)}, doesn't cover how long deals stay in ${andList(uncoveredStages)}, so we can't tell which are stuck there.`, { readFrom: [readFrom, "date"] }) : null;
    const ancientWords = ancient.length ? named(said`${ancient.length === 1 ? "One open deal has" : said`${ancient.length} open deals have`} been in ${ancient.length === 1 ? "its" : "their"} stage since before ${eng(readFrom)}, when the history we read starts.`, { ancient: ancient.length, readFrom: [readFrom, "date"] }) : null;
    const retiredWords = retired.map((r) => named(said`No deal has entered ${org(r.stage)} since ${eng(r.since)}, and ${r.deals.length} are still open in it.`, { retiredSince: [r.since, "date"], retiredOpen: r.deals.length }));
    // Deals older than the read are said, never raised: a long tail leaves more than 5% that old on a steady org (on the
    // e45 probes, raising ten or more raised 52 to 61% of steady orgs with nine-month lognormal stages). They stop a pass.
    const narrow = retired.length > 0;

    const isPast = (d: Deal): boolean => pastLine(days(d), models.get(d.StageName)!);
    const judged = deals.length ? judge({ open: deals, stageOf: (d) => d.StageName, ageOf: days, entries: all.filter((e) => models.has(e.stage) && e.age <= models.get(e.stage)!.horizon), models, dimensions: dimensions(ctx), stageDimension: "stage", minSegment: MIN_SEGMENT }) : null;
    const hits = judged?.total.hits ?? 0;
    const n = judged?.total.n ?? 0;
    const rate = pct(hits, n);
    const expectedRate = shown(judged?.total.expectedShare ?? 0);
    const lead = judged ? named(said`Open deals that have been in their stage longer than about three quarters of stays in that stage last: ${hits} of ${n} (${rate}%). If deals moved through each stage at the pace your history shows, about ${expectedRate}% of those still open would be past that point.`, { stayWithinPct: STUCK_PCT, hits, n, rate: [rate, "percent"], expectedRate: [expectedRate, "percent"] }) : said``;
    const facts = { ...(judged ? { hits, n, rate, expectedRate } : {}), compared: n, open: open.length, ancient: ancient.length, retired: retired.reduce((t, r) => t + r.deals.length, 0), ...before };
    const raised = judged !== null && (judged.total.raised || judged.raised.length > 0);
    const tail = (...parts: Array<Said | null>): Said => joinSaid(parts.filter((p): p is Said => p !== null).map((p) => said` ${p}`), "");

    if (!raised && !narrow) {
      // A pass only on most of the pipeline, and with no deal older than the read: else we can't say.
      const enough = n > 0 && n >= MIN_COMPARED_SHARE * open.length;
      if (enough && ancient.length > 0) {
        // Most of the pipeline compared, but some deals are older than the read: how long they should sit, the history can't say.
        return { outcome: "not_measurable", reason: said`${ancientWords!} We can't measure stays that long from the history we read, so this doesn't show whether any deals here are stuck.`, evidence: [{ ...base, count: n || null }] };
      }
      if (!enough) {
        const share = pct(n, open.length);
        const head = judged
          ? named(said`We could compare ${n} of ${open.length} open deals (${share}%) with how long stays in their stage last, too few to tell whether any are stuck.`, { compared: n, open: open.length, comparedShare: [share, "percent"] })
          : uncoveredWords ?? named(said`No open stage has ${minSample} finished stays to compare with.`, { minSample });
        const reason = judged ? said`${head}${tail(leftWords ?? null, ancientWords)}` : said`${head}${tail(ancientWords)}`;
        return { outcome: "not_measurable", reason, evidence: [{ ...base, count: n || null }] };
      }
      return {
        outcome: "pass",
        summary: said`${lead}${leftOut} In total and in each segment, that share is within what your stage times would give, so it doesn't show whether any of these deals are stuck.`,
        evidence,
        segmentsChecked: judged!.segmentsChecked,
        facts,
      };
    }
    const worst = judged?.raised[0];
    const worstRate = worst ? pct(worst.hits, worst.n) : 0;
    const worstExpected = worst ? shown(worst.expectedShare) : 0;
    // A segment's key is the org's: a stage, a record type, a team or a person's token.
    const segmentSentence = !worst
      ? said``
      : judged!.total.raised
        ? named(said` By ${dimensionWords(worst.dimension)}, ${org(worst.key)} is furthest above what we'd expect, at ${worstRate}% against about ${worstExpected}%.`, { worstRate: [worstRate, "percent"], worstExpected: [worstExpected, "percent"] })
        : named(said` The total is within what those times could show, but by ${dimensionWords(worst.dimension)}, ${org(worst.key)} is at ${worstRate}% (${worst.hits} of ${worst.n}) against about ${worstExpected}%.`, { worstRate: [worstRate, "percent"], worstHits: worst.hits, worstN: worst.n, worstExpected: [worstExpected, "percent"] });
    const totalSentence = judged?.total.raised ? said` That is more than your stage times explain.` : said``;
    // The deals the issue is about: past the line in what was raised (only where the days printed are beyond the line
    // printed, so no line reads against itself), the retired stages' old deals, and the deals older than the read.
    const keyed = dimensions(ctx);
    const inRaised = (d: Deal): boolean => judged!.total.raised || judged!.raised.some((s) => keyed[s.dimension as keyof typeof keyed](d) === s.key);
    const shownDays = (d: Deal): number => Math.floor(days(d) * 10) / 10;
    const shownLine = (d: Deal): number => Math.ceil(models.get(d.StageName)!.line * 10) / 10;
    const listedPast = raised ? deals.filter((d) => isPast(d) && inRaised(d) && shownDays(d) > shownLine(d)) : [];
    const retiredDeals = retired.flatMap((r) => r.deals.map((d) => ({ d, since: r.since })));
    const rows = [
      ...listedPast.map((d) => ({ id: d.Id, problem: `In ${d.StageName} for ${daysText(shownDays(d))} ${dayWord(shownDays(d))}, where about three quarters of stays in that stage last ${daysText(shownLine(d))} ${dayWord(shownLine(d))} or less` })),
      ...retiredDeals.filter(({ d }) => !listedPast.includes(d)).map(({ d, since }) => ({ id: d.Id, problem: `In ${d.StageName} for ${Math.floor(days(d))} days; no deal has entered ${d.StageName} since ${since}` })),
    ];
    return {
      outcome: "issue",
      title: said`Some deals are sitting in their stage longer than your stage times explain`,
      summary: said`${judged ? said`${lead}${totalSentence}${leftOut}${segmentSentence}` : said``}${tail(...retiredWords, ancientWords)}`,
      recommendation: said`Go through these in pipeline reviews, and either move each deal on with a dated next step or close it.`,
      recommendationType: "change",
      severity: "high",
      confidence: "high",
      evidence,
      // Not the Amount on the deals past the line: a steady pipeline would have most of them past it too, and which are the extra ones can't be told (DECISIONS.md 777).
      value: notSized(said`Some deals would be past this point even if nothing were wrong, and we can't tell which of these are the extra ones, so we don't put a figure on them.`),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: judged?.segmentsChecked ?? [{ dimension: "stage", segments: new Set(open.map((d) => d.StageName)).size }],
      subjects: [...(judged?.raised ?? []).slice(0, 5).map((h) => ({ type: h.dimension, label: h.key })), ...retired.map((r) => ({ type: "stage", label: r.stage }))].slice(0, 5),
      facts: { ...facts, ...(worst ? { worstSegmentRate: worstRate, worstSegmentExpectedRate: worstExpected } : {}) },
      ...(rows.length ? { records: { object: "Opportunity" as const, rows, fix: "Move it on with a dated next step, or close it" } } : {}),
    };
  },
};

const staleDeals = shareRule({
  id: "cur.stale-deals",
  version: 2,
  title: "Open deals with no movement and no activity",
  totalLimit: 25,
  segmentLimit: 50,
  severity: "high",
  scopeLabel: said`open deals`,
  scope(ctx, _data, open) {
    const cutoff = daysBefore(ctx.now, QUIET_DAYS);
    const day = localDay(cutoff, zoneOf(ctx));
    const moved = (d: Deal): boolean => {
      const entered = enteredCurrentStage(d) ?? new Date(d.CreatedDate);
      return entered >= cutoff;
    };
    return {
      deals: open,
      isHit: (d) => !moved(d) && (d.LastActivityDate === null || d.LastActivityDate < day),
      problem: (d) => `No stage change in ${QUIET_DAYS} days, and ${d.LastActivityDate ? `last activity ${d.LastActivityDate}` : "no activity logged"}`,
    };
  },
  // Titled by its test, as two other checks find quiet deals by theirs (night 3 reading pass, item 16).
  issueTitle: named(said`Open deals with no stage change or activity in ${QUIET_DAYS} days`, { quietDays: QUIET_DAYS }),
  sentence: (hits, n, rate) => named(said`Open deals with no stage change and no logged activity in ${QUIET_DAYS} days: ${hits} of ${n} (${rate}%).`, { quietDays: QUIET_DAYS }),
  recommendation: said`Check that activity is being captured, then re-engage these deals or close them.`,
  owner: "Head of sales",
  sized: (_ctx, data) => SITUATIONS.inStageFor(QUIET_DAYS, data),
  fix: "Re-engage the customer and log it, or close the deal",
});

const repeatedPushes = shareRule({
  id: "cur.close-date-pushes",
  title: "Open deals whose close date keeps moving",
  totalLimit: 15,
  segmentLimit: 40,
  severity: "medium",
  scopeLabel: said`open deals with stage history`,
  scope(ctx, _data, open) {
    const fiscal = fiscalStartOf(ctx);
    const deals = open.filter((d) => d.history.length > 0);
    const noHistory = open.length - deals.length;
    return { deals, isHit: (d) => pushesOf(d, fiscal).length >= REPEAT_PUSHES, problem: (d) => `Close date moved later ${pushesOf(d, fiscal).length} times`, exclusions: noHistory ? [named(said`${plural(noHistory, "deal")} with no stage history`, { noHistory })] : [] };
  },
  issueTitle: said`Close dates are being pushed again and again`,
  sentence: (hits, n, rate) => named(said`Open deals whose close date has moved later ${REPEAT_PUSHES} or more times: ${hits} of ${n} (${rate}%). A date that keeps moving usually means the deal isn't really on track.`, { repeatPushes: REPEAT_PUSHES, hits, n, rate: [rate, "percent"] }),
  recommendation: said`Treat a second push as a trigger for a manager to review the deal.`,
  owner: "Head of sales",
  sized: true,
  fix: "Have a manager review it and agree a date the customer has confirmed",
});

const missingNextSteps = shareRule({
  id: "cur.missing-next-steps",
  title: "Open deals with no next step",
  totalLimit: NEXT_STEP_LIMIT,
  segmentLimit: NEXT_STEP_SEGMENT_LIMIT,
  severity: "medium",
  scopeLabel: said`open deals`,
  scope(ctx, _data, open) {
    const steps = ctx.data(nextStepsExtractor);
    if (steps.status !== "ok") return { reason: `Next steps couldn't be read: ${steps.reason}`, evidence: [ctx.ev(steps.source, { count: null })] };
    const filled = new Set(steps.data);
    return { deals: open, isHit: (d) => !filled.has(d.Id), problem: () => "Next step is empty", evidence: [ctx.ev(steps.source, { count: steps.data.length, note: "Read as filled or empty only. The text itself is never fetched" })] };
  },
  issueTitle: said`Deals are missing a next step`,
  sentence: (hits, n, rate) => named(said`Open deals with an empty next step: ${hits} of ${n} (${rate}%).`, { hits, n, rate: [rate, "percent"] }),
  recommendation: said`Make a next step part of how a deal stays in the forecast, and check it in pipeline reviews.`,
  owner: "Head of sales",
  sized: false,
  fix: "Write the next step, with a date",
});

function concentrationRule(options: { id: string; title: string; limit: number; keyOf: (d: Deal) => string | null | undefined; subject: "account" | "rep"; labelOf: (ctx: RuleContext, key: string) => string; minKeys: number; issueTitle: Said; sentence: (share: number) => Said; recommendation: Said }): Rule {
  return {
    id: options.id,
    version: 1,
    domain: DOMAIN,
    title: options.title,
    evaluate(ctx): RuleResult | RuleResult[] {
      const loaded = openDeals(ctx);
      if ("outcome" in loaded) return loaded;
      const { data, deals } = loaded;
      if (data.multiCurrency) return { outcome: "not_measurable", reason: said`The org is multi-currency, and amounts can't be added until historic rates are supplied.`, evidence: data.evidence };
      const blank = deals.filter((d) => d.Amount === null).length;
      if (pct(blank, deals.length) > MAX_BLANK_AMOUNT_SHARE) return { outcome: "not_measurable", reason: named(said`${blank} of ${deals.length} open deals have no amount, so shares of the total would mislead.`, { blank, deals: deals.length }), evidence: data.evidence };
      const byKey = new Map<string, number>();
      let unkeyed = 0;
      for (const deal of deals) {
        const key = options.keyOf(deal);
        if (!key) unkeyed++;
        else byKey.set(key, (byKey.get(key) ?? 0) + (deal.Amount ?? 0));
      }
      const total = [...byKey.values()].reduce((a, b) => a + b, 0);
      const exclusions = [...data.exclusions, ...(blank ? [named(said`${plural(blank, "deal")} with no amount`, { blank })] : []), ...(unkeyed ? [named(said`${plural(unkeyed, "deal")} with no ${options.subject}`, { unkeyed })] : [])];
      const evidence = [{ ...data.evidence[0]!, count: deals.length, dateRange: { from: ctx.window.to, to: ctx.window.to }, exclusions, note: "Open deals" }];
      if (total === 0) return { outcome: "not_measurable", reason: said`Open deals have no amounts to compare.`, evidence };
      if (byKey.size < options.minKeys) return { outcome: "not_measurable", reason: named(said`Only ${plural(byKey.size, options.subject)} ${byKey.size === 1 ? "holds" : "hold"} open pipeline, so a share means little.`, { size: byKey.size }), evidence };
      const [top, amount] = [...byKey].sort(([ka, a], [kb, b]) => b - a || ka.localeCompare(kb))[0]!;
      const share = pct(amount, total);
      const segments = [{ dimension: options.subject, segments: byKey.size }];
      // The one account or rep the finding is about: "one account holds…" counts them (DECISIONS.md 773).
      const subjects = [{ type: options.subject, label: options.labelOf(ctx, top) }];
      if (share <= options.limit) return { outcome: "pass", summary: named(said`The largest ${options.subject} holds ${share}% of open pipeline, at or under our working limit of ${options.limit}%.`, { limitPct: options.limit }), evidence, segmentsChecked: segments, facts: { share } };
      return {
        outcome: "issue",
        // Made once when the rule is declared, so said again here, in the analysis, whose ledger vouches for it.
        title: named(said`${options.issueTitle}`, { holders: subjects.length }),
        summary: named(options.sentence(share), { holders: subjects.length }),
        recommendation: said`${options.recommendation}`,
        recommendationType: "investigate",
        severity: "medium",
        confidence: "high",
        evidence,
        value: { kind: "revenue", amount: Math.round(amount), unit: ctx.calibration.currency, period: "open pipeline today", assumptions: [said`The Amount on open deals for the largest ${options.subject}: exposure, not expected loss`, named(said`${options.limit}% is our working limit, not a Salesforce figure`, { limit: [options.limit, "percent"] })] },
        effort: { size: "M" },
        owner: "Head of sales",
        segmentsChecked: segments,
        subjects,
        facts: { share, limit: options.limit, keys: byKey.size },
      };
    },
  };
}

const accountConcentration = concentrationRule({
  id: "cur.account-concentration",
  // The check's name, which a pass carries: "one" names no count (DECISIONS.md 773).
  title: aside("Open pipeline resting on one account", "one"),
  limit: ACCOUNT_SHARE_LIMIT,
  keyOf: (d) => d.AccountId,
  subject: "account",
  // The account's name isn't read, so the token restores to its id for the consultant to look up.
  labelOf: (ctx, key) => ctx.pseudonyms.label("account", key, key),
  minKeys: 2,
  issueTitle: said`Too much of your pipeline rests on one account`,
  sentence: (share) => named(said`One account holds ${share}% of open pipeline. If that relationship stalls, the number goes with it.`, { share: [share, "percent"] }),
  recommendation: said`Check how much of this is real, then build pipeline elsewhere so ${word("one")} account can't decide the quarter.`,
});

const repConcentration = concentrationRule({
  id: "cur.rep-concentration",
  title: aside("Open pipeline resting on one rep", "one"),
  limit: REP_SHARE_LIMIT,
  keyOf: (d) => d.OwnerId,
  subject: "rep",
  labelOf: (ctx, key) => ctx.userLabel(key),
  minKeys: MIN_REPS,
  issueTitle: said`Too much of your pipeline rests on one rep`,
  sentence: (share) => named(said`One rep holds ${share}% of open pipeline, so an absence or a departure would leave a large gap.`, { share: [share, "percent"] }),
  recommendation: said`Check the territory split, and make sure someone else knows these deals.`,
});

type Coverage = { coverage: number; quota: number; won: number; remaining: number; open: number; quarter: DateRange; reps: Array<{ owner: string; quota: number; won: number; open: number }> };

/** Open pipeline closing this fiscal quarter against what's left of quota, for the people who own deals. */
export function coverageOf(ctx: RuleContext | Omit<RuleContext, "metrics">, deals: Deal[]): Coverage | { reason: string; evidence: Evidence[] } {
  const { source } = ctx.calibration.targets;
  const { quotas, types } = ctx.data(quotasExtractor);
  const noTarget = [ctx.ev(quotas.source, { count: null })];
  // "None" is the console's preselected answer, so it may be a default nobody
  // chose: we ask for targets rather than report the org has none (DECISIONS.md 627).
  if (source === "none") return { reason: said`No targets were given in the calibration. Tell us your targets to see pipeline coverage.`, evidence: noTarget };
  if (source !== "forecasting_quota") return { reason: said`The calibration keeps targets in ${source === "custom_object" ? "a custom object" : "a spreadsheet"}, and we don't read that yet.`, evidence: noTarget };
  if (types.status !== "ok") return { reason: `Forecast types couldn't be read: ${types.reason}`, evidence: [ctx.ev(types.source, { count: null })] };
  if (quotas.status !== "ok") return { reason: `Quotas couldn't be read: ${quotas.reason}`, evidence: noTarget };
  const type = revenueForecastType(types.data);
  // Quotas belong to a revenue forecast type, so with none active the calibration's answer can't be right as given.
  if ("switchedOff" in type) return { reason: said`The calibration keeps targets in Forecasting quotas, but no revenue forecast type is active, so there is no quota to read. Tell us where targets are kept.`, evidence: noTarget };
  if ("reason" in type) return { reason: type.reason, evidence: noTarget };
  const fiscal = fiscalStartOf(ctx);
  if (!fiscal) return { reason: said`The org's fiscal year couldn't be read, and the calibration doesn't set one.`, evidence: noTarget };

  const today = todayIn(ctx);
  const quarter = quarterBounds(new Date(`${today}T00:00:00Z`), fiscal);
  const data = loadPipeline(ctx);
  const everyone = data.status === "ok" ? data.deals : deals;
  const owners = new Set(everyone.filter((d) => d.category === "open" || d.category === "won").map((d) => d.OwnerId));
  const reps = new Map<string, number>();
  // Only people who own deals: a manager's quota is the sum of the team's, and
  // adding it in would double the target.
  for (const quota of quotas.data) {
    if (quota.ForecastingTypeId !== type.id || quota.QuotaAmount === null || quota.StartDate < quarter.from || quota.StartDate > quarter.to || !owners.has(quota.QuotaOwnerId)) continue;
    reps.set(quota.QuotaOwnerId, (reps.get(quota.QuotaOwnerId) ?? 0) + quota.QuotaAmount);
  }
  if (reps.size === 0) return { reason: said`No deal owner has a revenue quota for the quarter starting ${eng(quarter.from)}.`, evidence: noTarget };
  const inQuarter = (day: string): boolean => day >= quarter.from && day <= quarter.to;
  const rows = [...reps].map(([owner, quota]) => ({
    owner,
    quota,
    won: everyone.filter((d) => d.OwnerId === owner && d.category === "won" && inQuarter(d.closedDay)).reduce((sum, d) => sum + (d.Amount ?? 0), 0),
    open: deals.filter((d) => d.OwnerId === owner && d.CloseDate >= today && inQuarter(d.CloseDate)).reduce((sum, d) => sum + (d.Amount ?? 0), 0),
  }));
  const quota = rows.reduce((sum, r) => sum + r.quota, 0);
  const won = rows.reduce((sum, r) => sum + r.won, 0);
  const open = rows.reduce((sum, r) => sum + r.open, 0);
  const remaining = quota - won;
  return { coverage: remaining > 0 ? Math.round((open / remaining) * 100) / 100 : Infinity, quota, won, remaining, open, quarter, reps: rows };
}

const coverage: Rule = {
  id: "cur.coverage",
  version: 1,
  domain: DOMAIN,
  title: "Pipeline coverage against quota",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = openDeals(ctx);
    if ("outcome" in loaded) return loaded;
    if (loaded.data.multiCurrency) return { outcome: "not_measurable", reason: said`The org is multi-currency, and amounts can't be added until historic rates are supplied.`, evidence: loaded.data.evidence };
    const result = coverageOf(ctx, loaded.deals);
    if ("reason" in result) return { outcome: "not_measurable", reason: result.reason, evidence: result.evidence };
    const { quotas } = ctx.data(quotasExtractor);
    const evidence = [ctx.ev(quotas.source, { count: result.reps.length, dateRange: result.quarter, exclusions: ["Quotas held by people who own no deals, because a manager's quota repeats the team's", "Open deals closing outside this quarter or already past their close date"] }), { ...loaded.data.evidence[0]!, count: loaded.deals.length }];
    const segments = [{ dimension: "rep", segments: result.reps.length }];
    const facts = { quota: result.quota, won: result.won, remaining: result.remaining, openThisQuarter: result.open };
    if (result.remaining <= 0) return { outcome: "pass", summary: named(said`Won deals have already covered this quarter's quota of ${result.quota}.`, { quota: result.quota }), evidence, segmentsChecked: segments, facts };
    const low = result.reps.filter((r) => r.quota > r.won && r.open / (r.quota - r.won) < COVERAGE_TARGET).sort((a, b) => a.open / (a.quota - a.won) - b.open / (b.quota - b.won) || a.owner.localeCompare(b.owner));
    if (result.coverage >= COVERAGE_TARGET && low.length === 0) {
      return { outcome: "pass", summary: named(said`Coverage is ${result.coverage} times what's left of quota this quarter, at or above our working target of ${COVERAGE_TARGET}.`, { coverage: [result.coverage, "ratio"], coverageTarget: [COVERAGE_TARGET, "ratio"] }), evidence, segmentsChecked: segments, facts: { ...facts, coverage: result.coverage } };
    }
    const shortfall = Math.max(0, Math.round(result.remaining * COVERAGE_TARGET - result.open));
    return {
      outcome: "issue",
      title: said`There isn't enough pipeline to cover this quarter's quota`,
      summary: named(said`Open pipeline closing this quarter is ${result.coverage} times what's left of quota, against our working target of ${COVERAGE_TARGET}. Quota ${moneySaid(result.quota, ctx.calibration.currency)}, won ${moneySaid(result.won, ctx.calibration.currency)}, open ${moneySaid(result.open, ctx.calibration.currency)}.`, { coverage: [result.coverage, "ratio"], coverageTarget: [COVERAGE_TARGET, "ratio"], quota: [result.quota, "money"], won: [result.won, "money"], open: [result.open, "money"] }),
      recommendation: said`Decide this month whether to build more pipeline or reset the number. Coverage can't be fixed in the last weeks of ${word("a quarter")}.`,
      recommendationType: "investigate",
      severity: result.coverage < COVERAGE_TARGET / 2 ? "high" : "medium",
      confidence: "medium",
      evidence,
      value: { kind: "revenue", amount: shortfall, unit: ctx.calibration.currency, period: said`more pipeline needed for the quarter starting ${eng(result.quarter.from)}`, assumptions: [named(said`A target of ${COVERAGE_TARGET} times the remaining quota, which is our working figure`, { coverageTarget: [COVERAGE_TARGET, "ratio"] }), said`Quota comes from Collaborative Forecasts, for people who own deals`, said`Open pipeline in the quarter is taken at full Amount, with no stage weighting`] },
      // The decision is the small part, and it's the urgent part. Building pipeline afterwards is the dependency.
      effort: { size: "S" },
      owner: "Head of sales",
      dependencies: ["Building pipeline, if that's the decision, is a larger piece of work"],
      segmentsChecked: segments,
      subjects: low.slice(0, 5).map((r) => ({ type: "rep", label: ctx.userLabel(r.owner) })),
      facts: { ...facts, coverage: result.coverage, target: COVERAGE_TARGET, shortfall },
    };
  },
};

type Ctx = Omit<RuleContext, "metrics">;

function currentMetrics(ctx: Ctx): Metric[] {
  const loaded = openDeals(ctx);
  const definitions = {
    count: { id: "current.open_deals", domain: DOMAIN, label: "Open deals", unit: "count" as const, definition: "Deals in a stage the calibration calls open, today." },
    value: { id: "current.open_pipeline", domain: DOMAIN, label: "Open pipeline", unit: "currency" as const, definition: named("Total Amount of open deals today. A deal with no amount counts as zero.", { countedAs: NO_AMOUNT }) },
    age: { id: "current.median_days_in_stage", domain: DOMAIN, label: "Days in current stage", unit: "days" as const, definition: "Median days open deals have been in their current stage." },
  };
  const shared = { exclusions: [] as string[], assumptions: [zoneNote(ctx)] };
  if ("outcome" in loaded) return [...Object.values(definitions), ...OUTLOOK_LABELS.map((m) => ({ ...m, domain: DOMAIN }))].map((d) => ({ ...d, ...shared, evidence: loaded.evidence, status: "not_measurable", reason: loaded.reason }));
  const { data, deals } = loaded;
  const today = { from: ctx.window.to, to: ctx.window.to };
  const evidence = [{ ...data.evidence[0]!, count: deals.length, dateRange: today }];
  const common = { ...shared, exclusions: data.exclusions, evidence };
  const { minSample } = ctx.calibration;
  const dims: Record<string, (d: Deal) => string> = { stage: (d) => d.StageName, "record type": (d) => d.RecordType ?? "No record type", team: (d) => d.team, rep: (d) => ctx.userLabel(d.OwnerId) };
  const rest = { dateRange: today, series: [], stats: {}, segmentExemptReason: deals.length === 0 ? "There are no open deals." : null };
  const metrics: Metric[] = [{ ...definitions.count, ...common, status: "measured", value: deals.length, n: deals.length, segments: [...segment(deals, dims, 1, (group) => group.length), ...retiredStages(ctx, deals)], ...rest }];
  if (data.multiCurrency) metrics.push({ ...definitions.value, ...common, status: "not_measurable", reason: "The org is multi-currency, and amounts can't be added until historic rates are supplied." });
  else {
    const total = (group: Deal[]): number => Math.round(group.reduce((sum, d) => sum + (d.Amount ?? NO_AMOUNT), 0));
    metrics.push({ ...definitions.value, ...common, status: "measured", value: total(deals), n: deals.length, segments: segment(deals, dims, 1, total), ...rest });
  }
  const placed = deals.filter((d) => enteredCurrentStage(d));
  const days = (d: Deal): number => daysSince(enteredCurrentStage(d)!, ctx.now);
  const round = (n: number): number => Math.round(n * 10) / 10;
  if (placed.length < minSample) metrics.push({ ...definitions.age, ...common, status: "not_measurable", reason: named(said`${placed.length} open deals with stage history is below the calibrated minimum of ${minSample}.`, { placed: placed.length, minSample }) });
  else metrics.push({ ...definitions.age, ...common, status: "measured", value: round(median(placed.map(days))), n: placed.length, segments: segment(placed, dims, minSample, (group) => round(median(group.map(days)))), ...rest, stats: meanStat(placed.map(days)) });
  metrics.push(...openStageMetrics(ctx, data, deals, evidence), ...deliveryOutlook(ctx, data, deals, evidence));
  return metrics;
}

export const currentPipelineModule: DomainModule = {
  id: DOMAIN,
  cloud: "sales",
  label: "Current pipeline",
  extractors: [schemaExtractor, usersExtractor, stagesExtractor, opportunitiesExtractor, nextStepsExtractor, quotasExtractor],
  metrics: [currentMetrics],
  rules: [stuckInStage, staleDeals, repeatedPushes, missingNextSteps, accountConcentration, repConcentration, coverage],
};

/**
 * Current pipeline with HC_STUCK_EXPECTED on (DECISIONS.md 777): cur.stuck-in-stage at version 3, in version 2's
 * place, and nothing else changed.
 */
export const currentPipelineExpectedModule: DomainModule = {
  ...currentPipelineModule,
  rules: currentPipelineModule.rules.map((r) => (r === stuckInStage ? stuckInStageExpected : r)),
};

/** The modules a run analyses with the switch on: current pipeline swapped for its version 3's, and nothing else touched. */
export const withStuckExpected = (modules: DomainModule[]): DomainModule[] => modules.map((m) => (m === currentPipelineModule ? currentPipelineExpectedModule : m));

/**
 * cur.stuck-in-stage version 4, behind HC_STUCK_UNSCORED (DECISIONS.md 778): version 2's line, count and deal list,
 * shown and never scored. A snapshot of open deals over-samples long stays (777), so the share past the line says
 * nothing on its own about a problem; the list is for pipeline reviews. Served as an observation: an issue of
 * severity info, as the code checker's groups are shown and not scored (673), so the deal list and the figure reach
 * the customer, and a version the score leaves out (UNSCORED_VERSIONS in scoring.ts) never moves an area, the
 * overall score or the headline, and is never counted in an area's coverage. No deal past the line: a pass.
 */
const stuckInStageUnscored: Rule = {
  id: "cur.stuck-in-stage",
  version: 4,
  domain: DOMAIN,
  title: "Open deals that have waited longest in their stage",
  evaluate(ctx): RuleResult | RuleResult[] {
    const loaded = openDeals(ctx);
    if ("outcome" in loaded) return loaded;
    const { data, deals: open } = loaded;
    const base = { ...data.evidence[0]!, dateRange: { from: ctx.window.to, to: ctx.window.to } };
    const lines = stageLines(ctx, data);
    if (lines.size === 0) return { outcome: "not_measurable", reason: named(said`No open stage has ${ctx.calibration.minSample} won deals to compare with.`, { minSample: ctx.calibration.minSample }), evidence: [{ ...base, count: null }] };
    const deals = open.filter((d) => lines.has(d.StageName) && enteredCurrentStage(d));
    const noLine = open.filter((d) => !lines.has(d.StageName)).length;
    const noEntry = open.filter((d) => lines.has(d.StageName) && !enteredCurrentStage(d)).length;
    const exclusions = [...(noLine ? [named(said`${plural(noLine, "deal")} in stages with too few won deals to compare`, { noLine })] : []), ...(noEntry ? [named(said`${plural(noEntry, "deal")} whose history doesn't show when they entered this stage`, { noEntry })] : [])];
    const evidence: Evidence[] = [{ ...base, count: deals.length, exclusions: [...data.exclusions, ...exclusions], note: said`open deals with a stage to compare` }];
    const left = leftOutSentence(noLine, noEntry);
    const leftOut = left ? said` ${left}` : said``;
    if (deals.length === 0) return { outcome: "pass", summary: said`There are no open deals with a stage to compare to check.`, evidence, segmentExemptReason: "There are no deals to segment." };
    const days = (d: Deal): number => daysSince(enteredCurrentStage(d)!, ctx.now);
    const isPast = (d: Deal): boolean => days(d) > lines.get(d.StageName)!;
    // Version 2's own reading, every segment of ten or more, for the judges between runs (loosened.ts, stuckBefore3).
    const read = segmentRate(deals, isPast, dimensions(ctx), { segmentLimit: -Infinity, minSegment: MIN_SEGMENT });
    const facts = { hits: read.hits, n: read.n, rate: read.rate, wonLineRate: read.rate, ...(read.hotspots[0] ? { wonLineWorstSegmentRate: read.hotspots[0].rate } : {}) };
    const lead = named(said`Open deals that have been in their stage longer than three quarters of your won deals took: ${read.hits} of ${read.n} (${read.rate}%).`, { wonWithinPct: STUCK_PCT, hits: read.hits, n: read.n, rate: [read.rate, "percent"] });
    if (read.hits === 0) return { outcome: "pass", summary: said`${lead}${leftOut}`, evidence, segmentsChecked: read.segmentsChecked, facts };
    // Each deal's days against its line on one basis: days down, the line up, to one decimal, and only where they read apart.
    const shownDays = (d: Deal): number => Math.floor(days(d) * 10) / 10;
    const shownLine = (d: Deal): number => Math.ceil(lines.get(d.StageName)! * 10) / 10;
    const text = (x: number): string => (Number.isInteger(x) ? `${x}` : x.toFixed(1));
    const unit = (x: number): string => (x === 1 ? "day" : "days");
    const listed = deals.filter((d) => isPast(d) && shownDays(d) > shownLine(d));
    return {
      outcome: "issue",
      title: said`Open deals that have waited longest in their stage`,
      summary: said`${lead}${leftOut} Some open deals are always past this point, because the deals still open at any moment are the slower ones, so this share on its own doesn't show a problem. The list is for your pipeline reviews.`,
      recommendation: said`In pipeline reviews, go through the deals that have waited longest in each stage, and agree a next step or a decision for each.`,
      recommendationType: "investigate",
      severity: "info",
      confidence: "high",
      evidence,
      value: notSized(said`Some open deals are past this point in any pipeline, so the Amount on them doesn't show money at risk, and we don't put a figure on it.`),
      effort: { size: "S" },
      owner: "Head of sales",
      segmentsChecked: read.segmentsChecked,
      subjects: [],
      facts,
      ...(listed.length ? { records: { object: "Opportunity" as const, rows: listed.map((d) => ({ id: d.Id, problem: `In ${d.StageName} for ${text(shownDays(d))} ${unit(shownDays(d))}, where three quarters of won deals moved on within ${text(shownLine(d))} ${unit(shownLine(d))}` })), fix: "Agree a next step or a decision for it" } } : {}),
    };
  },
};

/** Current pipeline with HC_STUCK_UNSCORED on (DECISIONS.md 778): cur.stuck-in-stage at version 4, in version 2's place. */
export const currentPipelineUnscoredModule: DomainModule = {
  ...currentPipelineModule,
  rules: currentPipelineModule.rules.map((r) => (r === stuckInStage ? stuckInStageUnscored : r)),
};

/** The modules a run analyses with HC_STUCK_UNSCORED on and HC_STUCK_EXPECTED off: current pipeline swapped for version 4's. */
export const withStuckUnscored = (modules: DomainModule[]): DomainModule[] => modules.map((m) => (m === currentPipelineModule ? currentPipelineUnscoredModule : m));
