import { opportunitiesExtractor, stagesExtractor, type HistoryRow, type OpportunityRow } from "../../extract/opportunities.ts";
import { usersExtractor } from "../../extract/users.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Calibration, Evidence, StageCategory } from "../../schemas/index.ts";

export type Deal = OpportunityRow & {
  category: StageCategory | null;
  history: HistoryRow[];
  team: string;
  /** When the deal entered its final closed run of stages. Falls back to CloseDate. */
  closedAt: Date;
  closedAtSource: "history" | "close_date";
  ownerKind: string | null;
};

export type StageMap = {
  categoryOf(stage: string): StageCategory | null;
  /** Open stages in sales order. */
  openStages: string[];
  orderOf(stage: string): number | null;
};

export function stageMap(calibration: Calibration): StageMap {
  const byName = new Map(calibration.stages.map((s) => [s.name, s]));
  const open = calibration.stages.filter((s) => s.category === "open").sort((a, b) => a.order! - b.order!);
  const position = new Map(open.map((s, i) => [s.name, i]));
  return {
    categoryOf: (stage) => byName.get(stage)?.category ?? null,
    openStages: open.map((s) => s.name),
    orderOf: (stage) => position.get(stage) ?? null,
  };
}

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
      unmapped: { finalStages: string[]; historyStages: string[] };
    };

export function loadPipeline(ctx: Omit<RuleContext, "metrics">): PipelineData {
  const extract = ctx.data(opportunitiesExtractor);
  const { opportunities, history } = extract;
  const evidence = [
    ctx.ev(opportunities.source, { count: opportunities.status === "ok" ? opportunities.data.length : null, dateRange: ctx.window }),
    ctx.ev(history.source, { count: history.status === "ok" ? history.data.length : null, dateRange: ctx.window }),
  ];
  if (opportunities.status !== "ok") return { status: "unavailable", reason: `Opportunities couldn't be read: ${opportunities.reason}`, evidence };
  if (history.status !== "ok") return { status: "unavailable", reason: `OpportunityHistory couldn't be read: ${history.reason}`, evidence };

  const stages = stageMap(ctx.calibration);
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
    const rows = (byOpportunity.get(opportunity.Id) ?? []).sort((a, b) => a.CreatedDate.localeCompare(b.CreatedDate) || a.Id.localeCompare(b.Id));
    const fromHistory = closedAtFromHistory(rows, stages);
    deals.push({
      ...opportunity,
      category,
      history: rows,
      team: roles.get(opportunity.OwnerId) ?? "No role",
      closedAt: fromHistory ?? new Date(`${opportunity.CloseDate}T00:00:00Z`),
      closedAtSource: fromHistory ? "history" : "close_date",
      ownerKind,
    });
  }

  const exclusions = [
    ...(outOfScope ? [`${outOfScope} opportunities on record types outside the calibrated scope`] : []),
    ...(testOwner ? [`${testOwner} opportunities owned by test users`] : []),
    ...(excludedStage ? [`${excludedStage} opportunities in stages the calibration excludes`] : []),
  ];

  const seenHistoryStages = new Set(history.data.map((h) => h.StageName));
  const stagesData = ctx.data(stagesExtractor);
  if (stagesData.status === "ok") for (const s of stagesData.data) if (s.IsActive) seenHistoryStages.add(s.ApiName);
  const historyStart = history.data.reduce<string | null>((min, h) => (min === null || h.CreatedDate < min ? h.CreatedDate : min), null);

  return {
    status: "ok",
    deals,
    stages,
    evidence,
    multiCurrency: extract.multiCurrency,
    exclusions,
    historyStart: historyStart ? historyStart.slice(0, 10) : null,
    unmapped: {
      finalStages: [...new Set(deals.filter((d) => d.category === null).map((d) => d.StageName))].sort(),
      historyStages: [...seenHistoryStages].filter((s) => stages.categoryOf(s) === null).sort(),
    },
  };
}
