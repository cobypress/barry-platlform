import { z } from "zod";
import { STAGES_SOQL } from "../extract/opportunities.ts";
import { BudgetStopError } from "../salesforce/budget.ts";
import { SalesforceError, type SalesforceClient } from "../salesforce/client.ts";

// After an admin connects, the worker reads the org's opportunity stages once,
// for the calibration screen's list (DECISIONS.md 611). The console can't read
// the org, so without this a stage nobody types is invisible to the
// calibration (602). It is data beside the connect: it never fails or delays
// one, and the stage names, which the org chose, go to the console's table and
// nowhere else, so every line this file logs names only the kind of failure.

/** One stage as the console's HealthCheckOrgStages holds it. */
export type OrgStage = { apiName: string; label: string; isActive: boolean; isClosed: boolean; isWon: boolean; sortOrder: number | null };

/** The read and the write together, in milliseconds. */
export const STAGES_TIMEOUT_MS = 10_000;
/** One page is one call; an org with more stages than this is no ordinary org, and its list is cut there. */
const MAX_STAGES = 2_000;

const StageRecord = z.object({
  ApiName: z.string(),
  MasterLabel: z.string(),
  IsActive: z.boolean(),
  IsClosed: z.boolean(),
  IsWon: z.boolean(),
  SortOrder: z.number().nullable(),
});

/** A reply we can't read as stages. Its message is ours, never the reply's. */
class StageShapeError extends Error {}

/** The records in sort order: a stage with none goes last, and ties go by API name. */
export function orgStages(records: unknown[]): OrgStage[] {
  const parsed = z.array(StageRecord).safeParse(records);
  if (!parsed.success) throw new StageShapeError("the reply wasn't in the shape we expect");
  return parsed.data
    .map((r) => ({ apiName: r.ApiName, label: r.MasterLabel, isActive: r.IsActive, isClosed: r.IsClosed, isWon: r.IsWon, sortOrder: r.SortOrder }))
    .sort((a, b) => (a.sortOrder ?? Infinity) - (b.sortOrder ?? Infinity) || (a.apiName < b.apiName ? -1 : a.apiName > b.apiName ? 1 : 0));
}

/** Why the stages weren't saved, from the error's kind alone: Salesforce's code or the store's own words, never a message that could hold a name. */
function failureKind(err: unknown): string {
  if (err instanceof BudgetStopError) return "the API budget stopped the read";
  if (err instanceof SalesforceError) return `Salesforce refused the read (${err.code.replace(/[^A-Z0-9_]/g, "").slice(0, 60)})`;
  if (err instanceof StageShapeError) return err.message;
  if (err instanceof StoreError) return err.message;
  return `an unexpected error (${err instanceof Error ? err.name.replace(/[^A-Za-z]/g, "").slice(0, 40) : "not an error"})`;
}

/** A store's failure to save the stages, worded by the store without the row's data. */
export class StoreError extends Error {}

export type StagesWriter = (stages: OrgStage[], readAt: string) => Promise<boolean>;

/**
 * Reads the stages and hands them to `write`, which returns false when the
 * console has no table for them. Never throws, and never takes longer than
 * `timeoutMs`: a read that comes back after that writes nothing.
 */
export async function readOrgStages(
  client: SalesforceClient,
  write: StagesWriter,
  options: { log: (line: string) => void; now: () => string; timeoutMs?: number },
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? STAGES_TIMEOUT_MS;
  let late = false;
  const work = (async (): Promise<number | null> => {
    const result = await client.query<unknown>(STAGES_SOQL, { maxRecords: MAX_STAGES });
    const stages = orgStages(result.records);
    if (late) return null;
    return (await write(stages, options.now())) ? stages.length : null;
  })();
  // Whatever it does after the timeout, nothing waits for it and nothing hears of it.
  work.catch(() => {});
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => {
      late = true;
      resolve("timeout");
    }, timeoutMs);
  });
  try {
    const saved = await Promise.race([work, timeout]);
    if (saved === "timeout") options.log(`The org's opportunity stages weren't saved for the calibration: the read didn't finish in time.`);
    else if (saved !== null) options.log(`Saved the org's ${saved} opportunity stages for the calibration.`);
  } catch (err) {
    options.log(`The org's opportunity stages weren't saved for the calibration: ${failureKind(err)}.`);
  } finally {
    clearTimeout(timer);
  }
}
