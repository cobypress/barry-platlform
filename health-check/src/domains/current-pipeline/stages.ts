import type { Deal } from "../historic-pipeline/cohort.ts";

const DAY = 86_400_000;

/** When the deal entered its current stage, or null if its history doesn't end in that stage. */
export function enteredCurrentStage(deal: Deal): Date | null {
  const rows = deal.history;
  if (rows.at(-1)?.StageName !== deal.StageName) return null;
  let start = rows.length - 1;
  while (start > 0 && rows[start - 1]!.StageName === deal.StageName) start--;
  return new Date(rows[start]!.CreatedDate);
}

/** Days spent in each run of a stage. The last run has no end, so it's left out. */
export function stageRuns(deal: Deal): Array<{ stage: string; days: number }> {
  const runs: Array<{ stage: string; days: number }> = [];
  let start = 0;
  for (let i = 1; i <= deal.history.length; i++) {
    const row = deal.history[i];
    if (row && row.StageName === deal.history[start]!.StageName) continue;
    if (row) runs.push({ stage: deal.history[start]!.StageName, days: (new Date(row.CreatedDate).getTime() - new Date(deal.history[start]!.CreatedDate).getTime()) / DAY });
    start = i;
  }
  return runs;
}

export const daysSince = (from: Date, now: Date): number => (now.getTime() - from.getTime()) / DAY;

/** The fiscal quarter holding `date`, as calendar dates. */
export function quarterBounds(date: Date, fiscalStart: number): { from: string; to: string } {
  const monthsIn = (date.getUTCMonth() + 12 - (fiscalStart - 1)) % 12;
  const first = date.getUTCMonth() - (monthsIn % 3);
  const from = new Date(Date.UTC(date.getUTCFullYear(), first, 1));
  const to = new Date(Date.UTC(from.getUTCFullYear(), from.getUTCMonth() + 3, 0));
  return { from: from.toISOString().slice(0, 10), to: to.toISOString().slice(0, 10) };
}
