import { opportunitiesExtractor, stagesExtractor, type HistoryRow, type OpportunityRow, type StageRow } from "../../extract/opportunities.ts";
import { usersExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import { localDay, zoneOf } from "../time-zone.ts";
import type { Calibration, Evidence, StageCategory } from "../../schemas/index.ts";

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

const CATEGORY_WORDS: Partial<Record<StageCategory, string>> = { open: "open", won: "won", lost: "lost" };

/** The sentence every finding and metric that leaned on an inferred stage carries (DECISIONS.md 598). */
export function inferredStagesNote(inferred: Array<{ stage: string; category: StageCategory }>): string {
  const items = inferred.map((s) => `${s.stage} as ${CATEGORY_WORDS[s.category] ?? s.category}`);
  const list = items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`;
  return `We haven't yet agreed with you what ${inferred.length === 1 ? "one stage means" : "some stages mean"}, so we used Salesforce's own setting for ${inferred.length === 1 ? "it" : "each"}: ${list}.`;
}

/** Why a figure that splits lost from no decision can't be had from inferred lost stages (DECISIONS.md 600). */
export const unsplitReason = (stages: string[]): string => `Salesforce's setting can't tell lost from no decision for: ${stages.join(", ")}.`;

/** Why a check that needs the sales order can't place an inferred open stage. */
export const unorderedReason = (stages: string[]): string => `The calibration doesn't say where these open stages come in the sales order: ${stages.join(", ")}.`;

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
    };

/**
 * Deals built once for each analysis, by the analysis's own data accessor:
 * every input here is fixed for the length of one. Twenty-odd rules and
 * metrics read them, and each rebuilt a big org's 200,000 deals before
 * (DECISIONS.md 343). Rules only read what they're given.
 */
const built = new WeakMap<RuleContext["data"], PipelineData>();

export function loadPipeline(ctx: Omit<RuleContext, "metrics">): PipelineData {
  let data = built.get(ctx.data);
  if (!data) {
    data = buildPipeline(ctx);
    built.set(ctx.data, data);
  }
  // Whatever reads the deals leans on what their stages mean, so the engine
  // puts the note on its findings and metrics, and caps its confidence.
  if (data.status === "ok" && data.inferred.length > 0) ctx.assume?.(inferredStagesNote(data.inferred));
  return data;
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
    unsplitLost: [...new Set(deals.filter((d) => stages.isInferred(d.StageName) && d.category === "lost" && d.closedDay >= ctx.window.from && d.closedDay <= ctx.window.to).map((d) => d.StageName))].sort(),
  };
}

/** The reason a split of lost from no decision isn't measured, or null when it can be (DECISIONS.md 600). */
export function unsplitLostReason(data: { unsplitLost: string[] }): string | null {
  return data.unsplitLost.length > 0 ? unsplitReason(data.unsplitLost) : null;
}
