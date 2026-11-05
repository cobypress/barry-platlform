import type { RuleContext } from "../../engine/rule.ts";
import type { ClosedCaseRow, OpenCaseRow, QueueRow, ServiceSetup } from "../../extract/service.ts";
import type { Calibration } from "../../schemas/index.ts";

// What both Service Cloud areas share: the calibrated limits, and how a case's
// owner, age and priority are read.

export { median, quantile, round1 } from "../historic-pipeline/metrics.ts";

/** The calibrated limits' defaults, as agreed (DECISIONS.md 154). */
export const SERVICE_DEFAULTS = { backlogAgeDays: 30, originBlankShare: 10, stalenessMonths: 12 } as const;

export type ServiceLimits = { backlogAgeDays: number; originBlankShare: number; stalenessMonths: number; firstResponse: string[]; resolution: string[] };

export function limitsOf(calibration: Calibration): ServiceLimits {
  const s = calibration.service ?? {};
  return {
    backlogAgeDays: s.backlogAgeDays ?? SERVICE_DEFAULTS.backlogAgeDays,
    originBlankShare: s.originBlankShare ?? SERVICE_DEFAULTS.originBlankShare,
    stalenessMonths: s.stalenessMonths ?? SERVICE_DEFAULTS.stalenessMonths,
    firstResponse: s.firstResponseMilestones ?? [],
    resolution: s.resolutionMilestones ?? [],
  };
}

/** Segments smaller than this are listed but never trip a rule alone. */
export const MIN_SEGMENT = 10;
export const HOUR = 3_600_000;
export const DAY = 24 * HOUR;

/** A queue owns it when the owner id is a group's (00G). */
export const queueOwned = (ownerId: string): boolean => ownerId.startsWith("00G");
export const ownerType = (c: { OwnerId: string }): string => (queueOwned(c.OwnerId) ? "Queue" : "Person");
export const priorityOf = (c: { Priority: string | null }): string => c.Priority ?? "No priority";
export const originOf = (c: { Origin: string | null }): string => c.Origin ?? "No origin";
export const ageDays = (created: string, now: Date): number => (now.getTime() - Date.parse(created)) / DAY;

/** Case queues: queues that take cases. */
export function caseQueues(setup: ServiceSetup): QueueRow[] | null {
  if (setup.queues.status !== "ok" || setup.queueObjects.status !== "ok") return null;
  const forCases = new Set(setup.queueObjects.data.map((q) => q.QueueId));
  return setup.queues.data.filter((q) => forCases.has(q.Id));
}

/** Cases created in the year, open or closed, once each. */
export function yearsCases(open: OpenCaseRow[], closed: ClosedCaseRow[], since: Date): Array<OpenCaseRow | ClosedCaseRow> {
  const byId = new Map<string, OpenCaseRow | ClosedCaseRow>();
  for (const c of [...open, ...closed]) if (Date.parse(c.CreatedDate) >= since.getTime()) byId.set(c.Id, c);
  return [...byId.values()];
}

/** The date range a year's read covers, for evidence. */
export function yearRange(ctx: Pick<RuleContext, "now">, months: number): { from: string; to: string } {
  const from = new Date(ctx.now);
  from.setUTCMonth(from.getUTCMonth() - months);
  return { from: from.toISOString().slice(0, 10), to: ctx.now.toISOString().slice(0, 10) };
}
