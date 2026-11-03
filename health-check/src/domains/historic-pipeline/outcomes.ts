import { pct, type RuleContext } from "../../engine/rule.ts";
import { stageRuns } from "../current-pipeline/stages.ts";
import { localDay } from "../time-zone.ts";
import type { Deal, PipelineData, StageMap } from "./cohort.ts";
import { closedInWindow } from "./metrics.ts";

// What happened to deals that were once where today's open deals are. Every
// rate that weights open pipeline comes from here, so a stage's win rate is the
// same figure in the weighted pipeline, the delivery outlook and a finding.

type Ctx = Pick<RuleContext, "calibration" | "window">;
type Ready = Extract<PipelineData, { status: "ok" }>;

const DAY = 86_400_000;
const CLOSED = new Set(["won", "lost", "no_decision"]);

const isOpenStage = (stages: StageMap, stage: string): boolean => stages.categoryOf(stage) === "open";

/**
 * Deals closed in the window that were open at some point. A deal created
 * already closed has no open history, so it can't say how open deals end.
 */
export function closedFromOpen(ctx: Ctx, data: Ready): Deal[] {
  return data.deals.filter((d) => d.category !== null && CLOSED.has(d.category) && closedInWindow(d, ctx.window) && d.history.some((h) => isOpenStage(data.stages, h.StageName)));
}

/** Won out of every closed outcome. No decision counts as not won, because a deal that fizzles out didn't win. */
export type Cohort = { n: number; won: number; rate: number | null };

export function cohortOf(deals: Deal[], minSample: number): Cohort {
  const won = deals.filter((d) => d.category === "won").length;
  return { n: deals.length, won, rate: deals.length >= minSample ? pct(won, deals.length) : null };
}

type Span = { stage: string; from: Date; to: Date; closeDate: string };

/** Each stretch between one history row and the next while the deal was open. A stretch with no end yet is left out. */
export function openSpans(deal: Deal, stages: StageMap): Span[] {
  const spans: Span[] = [];
  for (let i = 0; i + 1 < deal.history.length; i++) {
    const row = deal.history[i]!;
    if (!isOpenStage(stages, row.StageName)) continue;
    spans.push({ stage: row.StageName, from: new Date(row.CreatedDate), to: new Date(deal.history[i + 1]!.CreatedDate), closeDate: row.CloseDate });
  }
  return spans;
}

/** Open on a day after the close date it had then. CloseDate is a plain day, so only the instant is converted. */
export const wasPastClose = (deal: Deal, stages: StageMap, zone: string | null): boolean => openSpans(deal, stages).some((s) => localDay(s.to, zone) > s.closeDate);

/** The longest time in one open stage. Close date and amount edits don't restart the clock. */
export const longestInStage = (deal: Deal, stages: StageMap): number => Math.max(0, ...stageRuns(deal).filter((r) => isOpenStage(stages, r.stage)).map((r) => r.days));

/** The longest open stretch with no history row of any kind: no change to stage, amount or close date. */
export const longestUnchanged = (deal: Deal, stages: StageMap): number => Math.max(0, ...openSpans(deal, stages).map((s) => (s.to.getTime() - s.from.getTime()) / DAY));

export const wasInStage = (deal: Deal, stage: string): boolean => deal.history.some((h) => h.StageName === stage);

/** For each open stage in sales order: of closed deals that were in it at some point, how many won. */
export function stageWinRates(ctx: Ctx, data: Ready): Map<string, Cohort> {
  const closed = closedFromOpen(ctx, data);
  return new Map(data.stages.openStages.map((stage) => [stage, cohortOf(closed.filter((d) => wasInStage(d, stage)), ctx.calibration.minSample)]));
}

/** The close date a deal was first given, from its earliest history row, or its CloseDate if it has none. */
export const firstForecast = (deal: Deal): string => deal.history[0]?.CloseDate ?? deal.CloseDate;

/** Days from the first forecast close date to the actual close. Negative is early. */
export const daysLate = (deal: Deal): number => (new Date(`${deal.closedDay}T00:00:00Z`).getTime() - new Date(`${firstForecast(deal)}T00:00:00Z`).getTime()) / DAY;

/** Won deals the reliability measures read: closed in the window, with history from while they were open. */
export const wonFromOpen = (ctx: Ctx, data: Ready): Deal[] => closedFromOpen(ctx, data).filter((d) => d.category === "won");
