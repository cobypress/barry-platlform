import { said, type Said } from "../../engine/org-words.ts";
import type { RuleContext } from "../../engine/rule.ts";
import { localDay, zoneOf } from "../time-zone.ts";
import type { Deal, PipelineData } from "./cohort.ts";
import { hasOpenStage, quantile } from "./metrics.ts";
import { daysToClose, openPathOf, type PathDeal, type Paths } from "./paths/variants.ts";

// Deals chosen by when they reached a stage (DECISIONS.md 706) or were made
// (the path insights, 712), never by when they closed. Chosen by when they
// closed, a sample over-counts whichever outcome closes faster whenever deals
// arrive faster or slower than before: the stage probabilities named a true
// setting in 40 of 100 simulated shrinking orgs, and the path insights, where
// only the won deals that skipped a stage closed sooner, raised a gap that
// wasn't there in up to 66% of orgs in a cell.
//
// A stage's period runs from the start of the window to the run's date less
// the stage's maturity lag, the time within which LAG_QUANTILE of the closed
// deals that reached it had closed, counted from the day each entered the
// period, so nearly every deal in it has closed; a figure is read from a period
// only while at most OPEN_SHARE of its deals are still open, and the few that
// are are counted both ways by the reader.

type Ctx = Omit<RuleContext, "metrics">;
type Ready = Extract<PipelineData, { status: "ok" }>;

const DAY = 86_400_000;
/** The share of closed deals that reached a stage whose time to close sets the stage's maturity lag. */
export const LAG_QUANTILE = 0.9;
/** A stage's period may hold at most this share of deals still open; more, and it isn't read. */
export const OPEN_SHARE = 0.05;

/** A deal as a reached-stage cohort reads it: open or closed, with when it first entered each open stage. */
export type CohortDeal = PathDeal & { open: boolean; reachedAt: Map<string, number>; closedAtMs: number | null };

/** A deal reached a stage when its own stage history shows it there. Nothing before the first recorded stage is inferred. */
export const reached = (d: Pick<PathDeal, "path">, stage: string): boolean => d.path.includes(stage);

/** When the deal first entered each open stage, from its stage history. */
function reachedTimes(deal: Deal, open: (stage: string) => boolean): Map<string, number> {
  const at = new Map<string, number>();
  for (const row of deal.history) if (open(row.StageName) && !at.has(row.StageName)) at.set(row.StageName, new Date(row.CreatedDate).getTime());
  return at;
}

/** Deals created before the org's stage history starts can't show every stage they were in, so they aren't compared. */
export function comparable<T extends PathDeal>(ctx: Ctx, p: { closed: T[] } | Pick<Paths, "closed">, historyStart: string | null): T[] {
  const deals = p.closed as T[];
  if (!historyStart) return deals;
  const zone = zoneOf(ctx);
  return deals.filter((d) => localDay(new Date(d.deal.CreatedDate), zone) >= historyStart);
}

/** Every deal with a recorded open stage, open or closed, whenever it closed, and how many were left out as made before the history starts. */
export function cohortDeals(ctx: Ctx, data: Ready): { deals: CohortDeal[]; beforeHistory: number } {
  const open = (stage: string): boolean => data.stages.categoryOf(stage) === "open";
  const all: CohortDeal[] = data.deals.filter((d) => hasOpenStage(d, data)).map((deal) => {
    const isOpen = deal.category === "open";
    return { id: deal.Id, deal, path: openPathOf(deal, data.stages), won: deal.category === "won", days: isOpen ? 0 : daysToClose(deal), open: isOpen, reachedAt: reachedTimes(deal, open), closedAtMs: isOpen ? null : deal.closedAt.getTime() };
  });
  const deals = comparable(ctx, { closed: all }, data.historyStart);
  return { deals, beforeHistory: all.length - deals.length };
}

/** The calendar day `days` after `day`. */
export const dayPlus = (day: string, days: number): string => new Date(Date.parse(`${day}T00:00:00Z`) + days * DAY).toISOString().slice(0, 10);

/** The deals that reached a stage in its period: the lag, the period's two ends as local days, and who is in it. */
export type Period = {
  stage: string;
  /** Days within which LAG_QUANTILE of the closed deals that reached it had closed; null with none closed. */
  lagDays: number | null;
  /** The period: reached the stage from `from` to `to`. Null when no day fits. */
  from: string | null;
  to: string | null;
  /** Deals that reached it in the period, open or closed, and the closed ones. */
  cohort: CohortDeal[];
  closed: CohortDeal[];
  open: number;
  /** More than OPEN_SHARE of the period's deals still open. */
  tooOpen: boolean;
  /** The local day a deal reached the stage. */
  dayOf: (d: CohortDeal) => string;
};

/**
 * A stage's period and the deals that reached it in it. `by`: when a deal enters
 * the period, on the day it reached the stage (706) or on the day it was created,
 * for a comparison whose sides could reach the stage at different speeds.
 */
export function periodOf(ctx: Ctx, deals: CohortDeal[], stage: string, by: "reached" | "created" = "reached"): Period {
  const zone = zoneOf(ctx);
  const there = deals.filter((d) => reached(d, stage));
  const startOf = (d: CohortDeal): number => (by === "reached" ? d.reachedAt.get(stage)! : new Date(d.deal.CreatedDate).getTime());
  const durations = there.filter((d) => d.closedAtMs !== null).map((d) => Math.max(0, (d.closedAtMs! - startOf(d)) / DAY));
  const lagDays = durations.length ? Math.ceil(quantile(durations, LAG_QUANTILE)) : null;
  const to = lagDays === null ? null : localDay(new Date(ctx.now.getTime() - lagDays * DAY), zone);
  const from = to !== null && to >= ctx.window.from ? ctx.window.from : null;
  const dayOf = (d: CohortDeal): string => localDay(new Date(startOf(d)), zone);
  const cohort = from && to ? there.filter((d) => dayOf(d) >= from && dayOf(d) <= to) : [];
  const closed = cohort.filter((d) => !d.open);
  const open = cohort.length - closed.length;
  return { stage, lagDays, from, to, cohort, closed, open, tooOpen: cohort.length > 0 && open > OPEN_SHARE * cohort.length, dayOf };
}

const MONTHS: ReadonlyArray<() => Said> = [
  () => said`January`, () => said`February`, () => said`March`, () => said`April`, () => said`May`, () => said`June`,
  () => said`July`, () => said`August`, () => said`September`, () => said`October`, () => said`November`, () => said`December`,
];
const monthOf = (day: string): Said => MONTHS[Number(day.slice(5, 7)) - 1]!();

/** A calendar day in full, as "21 September 2026", so the period is one the customer can check. */
export const daySaid = (day: string): Said => said`${Number(day.slice(8, 10))} ${monthOf(day)} ${Number(day.slice(0, 4))}`;
