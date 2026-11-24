import { opportunitiesExtractor, stagesExtractor, type HistoryRow, type OpportunityRow, type StageRow } from "../../extract/opportunities.ts";
import { usersExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import { localDay, zoneOf } from "../time-zone.ts";
import type { Calibration, Evidence, StageCategory } from "../../schemas/index.ts";
import { joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { andList } from "../../engine/said-forms.ts";
import { orderFromOrg } from "../../connect/auto-calibration.ts";

export type Deal = OpportunityRow & {
  category: StageCategory | null;
  history: HistoryRow[];
  team: string;
  /** When the deal entered its final closed run of stages. Falls back to CloseDate. */
  closedAt: Date;
  /** The calendar day of that close in the org's time zone. CloseDate is already a day, so it isn't converted. */
  closedDay: string;
  closedAtSource: "history" | "close_date";
  ownerKind: string | null;
};

export type StageMap = {
  categoryOf(stage: string): StageCategory | null;
  /** Open stages in sales order. Only the calibration gives an order, so an inferred open stage isn't here. */
  openStages: string[];
  orderOf(stage: string): number | null;
  /** True for a stage the calibration doesn't name whose category came from Salesforce's flags (DECISIONS.md 598). */
  isInferred(stage: string): boolean;
};

/**
 * What each stage means. The calibration's word always wins. A stage it
 * doesn't name takes Salesforce's own flags, when they could be read and hold
 * it: not closed is open, closed and won is won, closed and not won is lost.
 * "No decision" and a place in the sales order have no flag, so neither is
 * ever inferred (DECISIONS.md 598).
 */
export function stageMap(calibration: Calibration, flags: StageRow[] | null = null): StageMap {
  const byName = new Map(calibration.stages.map((s) => [s.name, s]));
  const open = calibration.stages.filter((s) => s.category === "open").sort((a, b) => a.order! - b.order!);
  const position = new Map(open.map((s, i) => [s.name, i]));
  const inferred = new Map<string, StageCategory>();
  for (const f of flags ?? []) if (!byName.has(f.ApiName)) inferred.set(f.ApiName, !f.IsClosed ? "open" : f.IsWon ? "won" : "lost");
  return {
    categoryOf: (stage) => byName.get(stage)?.category ?? inferred.get(stage) ?? null,
    openStages: open.map((s) => s.name),
    orderOf: (stage) => position.get(stage) ?? null,
    isInferred: (stage) => !byName.has(stage) && inferred.has(stage),
  };
}

/**
 * Where the run's documents keep the meaning each stage the calibration doesn't
 * name took from Salesforce's flags, so a chart drawn after the raw extracts are
 * purged still knows a won stage from a lost one (DECISIONS.md 600). Written
 * only when there's one; names and categories, nothing about a record.
 */
export const STAGE_MEANINGS_FILE = "stage-meanings.json";

/** Each stage's inferred category, for every flagged stage the calibration doesn't name. */
export function inferredMeanings(calibration: Calibration, flags: StageRow[] | null): Record<string, "open" | "won" | "lost"> {
  const map = stageMap(calibration, flags);
  const out: Record<string, "open" | "won" | "lost"> = {};
  for (const f of [...(flags ?? [])].sort((a, b) => (a.ApiName < b.ApiName ? -1 : 1))) {
    const category = map.categoryOf(f.ApiName);
    if (map.isInferred(f.ApiName) && (category === "open" || category === "won" || category === "lost")) out[f.ApiName] = category;
  }
  return out;
}

/**
 * The sentence every finding and metric that leaned on an inferred stage
 * carries (DECISIONS.md 598), written with said, each stage as the org's words
 * (657), so a summary it is added to stays marked. The category is one of ours,
 * set by stageMap from Salesforce's flags, and printed as it is named.
 */
export function inferredStagesNote(inferred: Array<{ stage: string; category: StageCategory }>, fromOrgSettings = false): Said {
  const items = inferred.map((s) => said`${org(s.stage)} as ${s.category}`);
  // An automatic run's calibration was agreed with nobody (DECISIONS.md 693): no consultant is to come back to it.
  if (fromOrgSettings) return said`You haven't told us yet what ${inferred.length === 1 ? "one stage means" : "some stages mean"}, so we used Salesforce's own setting for ${inferred.length === 1 ? "it" : "each"}: ${andList(items)}.`;
  return said`We haven't yet agreed with you what ${inferred.length === 1 ? "one stage means" : "some stages mean"}, so we used Salesforce's own setting for ${inferred.length === 1 ? "it" : "each"}: ${andList(items)}.`;
}

/** Stage names, as the org named them, joined with commas. */
const stageList = (stages: string[]): Said => joinSaid(stages.map((s) => said`${org(s)}`), ", ");

/**
 * Why a figure that splits lost from no decision can't be had from inferred
 * lost stages (DECISIONS.md 600), ending with what to give us and where, as a
 * reason that needs something from the customer does (627).
 */
export const unsplitReason = (stages: string[], fromOrgSettings = false): Said =>
  // On an automatic run no consultant will set the calibration: the customer's answer is what unlocks it (DECISIONS.md 693).
  fromOrgSettings
    ? said`Salesforce's setting can't tell lost from no decision for: ${stageList(stages)}. If you tell us which each is, your next health check can measure this.`
    : said`Salesforce's setting can't tell lost from no decision for: ${stageList(stages)}. The calibration can say which each is.`;

/** Why a check that needs the sales order can't place an inferred open stage. */
export const unorderedReason = (stages: string[]): Said => said`The calibration doesn't say where these open stages come in the sales order: ${stageList(stages)}.`;

const TERMINAL = new Set<StageCategory>(["won", "lost", "no_decision"]);

/** The start of the unbroken closed run at the end of a deal's history. A reopened deal only counts its last close. */
function closedAtFromHistory(history: HistoryRow[], stages: StageMap): Date | null {
  let closed: Date | null = null;
  for (let i = history.length - 1; i >= 0; i--) {
    const category = stages.categoryOf(history[i]!.StageName);
    if (!category || !TERMINAL.has(category)) break;
    closed = new Date(history[i]!.CreatedDate);
  }
  return closed;
}

// Plain comparison: Salesforce ids are case-sensitive, and localeCompare would
// reverse two rows written in the same second.
function byTimeThenId(a: HistoryRow, b: HistoryRow): number {
  const [x, y] = [`${a.CreatedDate}|${a.Id}`, `${b.CreatedDate}|${b.Id}`];
  return x < y ? -1 : x > y ? 1 : 0;
}

export type PipelineData =
  | { status: "unavailable"; reason: string; evidence: Evidence[] }
  | {
      status: "ok";
      deals: Deal[];
      stages: StageMap;
      evidence: Evidence[];
      multiCurrency: boolean;
      /** Exclusions applied to every metric, already written for the report. */
      exclusions: string[];
      historyStart: string | null;
      /**
       * Stages an in-scope deal ends in (final) or passed through (history)
       * that have no category, from the calibration or Salesforce's flags. A
       * stage no in-scope deal used is in neither (DECISIONS.md 598).
       * `unordered` are inferred open stages those deals used: they have no
       * place in the sales order, so checks that need one can't use them.
       */
      unmapped: { finalStages: string[]; historyStages: string[]; unordered: string[] };
      /** Stages those deals used whose category came from Salesforce's flags, with it. */
      inferred: Array<{ stage: string; category: StageCategory }>;
      /**
       * Inferred lost stages that an in-scope deal closed in the window ends
       * in. Salesforce's flags can't tell lost from no decision, so anything
       * that separates the two isn't measured while there are any (DECISIONS.md 600).
       */
      unsplitLost: string[];
      /** How many record types the in-scope deals use. Unnamed deals don't count; absent reads as none. */
      recordTypes?: number;
      /** The calibration was read from the org's settings, for an automatic run (DECISIONS.md 693): set only then. */
      fromOrgSettings?: true;
    };

/**
 * Deals built once for each analysis, by the analysis's own data accessor:
 * every input here is fixed for the length of one. Twenty-odd rules and
 * metrics read them, and each rebuilt a big org's 200,000 deals before
 * (DECISIONS.md 343). Rules only read what they're given.
 */
const built = new WeakMap<RuleContext["data"], PipelineData>();

export function loadPipeline(ctx: Omit<RuleContext, "metrics">): PipelineData {
  // Asked for on every call, cached or not: reading the step is what puts its
  // notes, such as deals left out, on whatever reads the deals (DECISIONS.md 702).
  ctx.data(opportunitiesExtractor);
  let data = built.get(ctx.data);
  if (!data) {
    data = buildPipeline(ctx);
    built.set(ctx.data, data);
  }
  // Whatever reads the deals leans on what their stages mean, so the engine
  // puts the note on its findings and metrics, and caps its confidence.
  if (data.status === "ok" && data.inferred.length > 0) ctx.assume?.(inferredStagesNote(data.inferred, ctx.calibration.source === "org_settings"));
  // A calibration read from the org's settings (DECISIONS.md 692): nobody chose
  // the record types, so blended sales processes are said, and the open stages'
  // order is Salesforce's sort order, said by whatever reads it.
  if (data.status !== "ok" || ctx.calibration.source !== "org_settings") return data;
  const recordTypes = data.recordTypes ?? 0;
  if (recordTypes > 1) ctx.assume?.(blendedProcessesNote(recordTypes));
  return orderFromOrg(ctx.calibration) ? { ...data, stages: notingOrder(data.stages, () => ctx.assume?.(unconfirmedOrderNote())) } : data;
}

/** Made in each analysis, as every said is. What every check that reads the sales order says when the order came from Salesforce's sort order, not a person (DECISIONS.md 692). */
export const unconfirmedOrderNote = (): Said => said`We took the order of your open stages from Salesforce's own stage settings, and haven't confirmed it with you.`;
/** The same sentence, for a chart drawn in that order (DECISIONS.md 693). */
export const ORDER_UNCONFIRMED = "We took the order of your open stages from Salesforce's own stage settings, and haven't confirmed it with you.";

/** What every figure that read the deals says when they hold several record types nobody has told apart (DECISIONS.md 692). */
export const blendedProcessesNote = (n: number): Said =>
  said`Your deals use ${n} record types, which may be separate sales processes. We've read them together, so these figures blend them.`;

/** The same stages, saying `note` whenever the order of the open stages is read. */
function notingOrder(stages: StageMap, note: () => void): StageMap {
  return {
    categoryOf: stages.categoryOf,
    isInferred: stages.isInferred,
    get openStages() {
      note();
      return stages.openStages;
    },
    orderOf: (stage) => {
      note();
      return stages.orderOf(stage);
    },
  };
}

function buildPipeline(ctx: Omit<RuleContext, "metrics">): PipelineData {
  const extract = ctx.data(opportunitiesExtractor);
  const { opportunities, history } = extract;
  const evidence = [
    ctx.ev(opportunities.source, { count: opportunities.status === "ok" ? opportunities.data.length : null, dateRange: ctx.window }),
    ctx.ev(history.source, { count: history.status === "ok" ? history.data.length : null, dateRange: ctx.window }),
  ];
  if (opportunities.status !== "ok") return { status: "unavailable", reason: `Opportunities couldn't be read: ${opportunities.reason}`, evidence };
  if (history.status !== "ok") return { status: "unavailable", reason: `OpportunityHistory couldn't be read: ${history.reason}`, evidence };

  const stagesData = ctx.data(stagesExtractor);
  const stages = stageMap(ctx.calibration, stagesData.status === "ok" ? stagesData.data : null);
  const zone = zoneOf(ctx);
  const users = ctx.data(usersExtractor);
  const roles = new Map(users.status === "ok" ? users.data.map((u) => [u.Id, u.UserRole?.Name ?? "No role"]) : []);
  const kinds = new Map(ctx.calibration.users.map((u) => [u.userId.slice(0, 15), u.kind as string]));

  const byOpportunity = new Map<string, HistoryRow[]>();
  for (const row of history.data) {
    const rows = byOpportunity.get(row.OpportunityId) ?? [];
    rows.push(row);
    byOpportunity.set(row.OpportunityId, rows);
  }

  const { recordTypes } = ctx.calibration.scope;
  const inScope = (o: OpportunityRow): boolean => recordTypes === "all" || (o.RecordType !== null && recordTypes.includes(o.RecordType));
  const deals: Deal[] = [];
  let outOfScope = 0;
  let testOwner = 0;
  let excludedStage = 0;
  for (const opportunity of opportunities.data) {
    if (!inScope(opportunity)) {
      outOfScope++;
      continue;
    }
    const ownerKind = kinds.get(opportunity.OwnerId.slice(0, 15)) ?? null;
    if (ownerKind === "test") {
      testOwner++;
      continue;
    }
    const category = stages.categoryOf(opportunity.StageName);
    if (category === "excluded") {
      excludedStage++;
      continue;
    }
    const rows = (byOpportunity.get(opportunity.Id) ?? []).sort(byTimeThenId);
    const fromHistory = closedAtFromHistory(rows, stages);
    deals.push({
      ...opportunity,
      category,
      history: rows,
      team: roles.get(opportunity.OwnerId) ?? "No role",
      closedAt: fromHistory ?? new Date(`${opportunity.CloseDate}T00:00:00Z`),
      closedDay: fromHistory ? localDay(fromHistory, zone) : opportunity.CloseDate,
      closedAtSource: fromHistory ? "history" : "close_date",
      ownerKind,
    });
  }

  const exclusions = [
    ...(outOfScope ? [`${outOfScope} opportunities on record types outside the calibrated scope`] : []),
    ...(testOwner ? [`${testOwner} opportunities owned by test users`] : []),
    ...(excludedStage ? [`${excludedStage} opportunities in stages the calibration excludes`] : []),
  ];

  // Only what an in-scope deal used: a stage that only out-of-scope deals, or
  // no deal, sit in can't change a figure (DECISIONS.md 598).
  const finalUsed = new Set(deals.map((d) => d.StageName));
  const historyUsed = new Set(deals.flatMap((d) => d.history.map((h) => h.StageName)));
  const used = new Set([...finalUsed, ...historyUsed]);
  const historyStart = history.data.reduce<string | null>((min, h) => (min === null || h.CreatedDate < min ? h.CreatedDate : min), null);
  const inferred = [...used].filter((s) => stages.isInferred(s)).sort().map((stage) => ({ stage, category: stages.categoryOf(stage)! }));

  return {
    status: "ok",
    deals,
    stages,
    evidence,
    multiCurrency: extract.multiCurrency,
    exclusions,
    historyStart: historyStart ? localDay(historyStart, zone) : null,
    unmapped: {
      finalStages: [...finalUsed].filter((s) => stages.categoryOf(s) === null).sort(),
      historyStages: [...historyUsed].filter((s) => stages.categoryOf(s) === null).sort(),
      unordered: inferred.filter((s) => s.category === "open").map((s) => s.stage),
    },
    inferred,
    recordTypes: new Set(deals.flatMap((d) => (d.RecordType === null ? [] : [d.RecordType]))).size,
    unsplitLost: [...new Set(deals.filter((d) => stages.isInferred(d.StageName) && d.category === "lost" && d.closedDay >= ctx.window.from && d.closedDay <= ctx.window.to).map((d) => d.StageName))].sort(),
    ...(ctx.calibration.source === "org_settings" ? { fromOrgSettings: true as const } : {}),
  };
}

/** The reason a split of lost from no decision isn't measured, or null when it can be (DECISIONS.md 600). */
export function unsplitLostReason(data: { unsplitLost: string[]; fromOrgSettings?: boolean }): Said | null {
  return data.unsplitLost.length > 0 ? unsplitReason(data.unsplitLost, data.fromOrgSettings === true) : null;
}
