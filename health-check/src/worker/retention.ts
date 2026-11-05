import { existsSync, lstatSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { deleteRunFolder, isPurged, purgeRun } from "../retention/purge.ts";
import { JobBusyError, withJobLock } from "./lock.ts";
import type { ReleasedRun, WorkerStore } from "./types.ts";

// Roadmap 2.3 and decision 16: a released run's raw extracts go 30 days after
// release, and the whole run folder 12 months after. A monthly pulse is never
// released, so its clock starts when it finished (DECISIONS.md 136): its raw
// extracts go after 30 days too, because the next pulse compares only against
// what it found, and its folder after 12 months. Benchmark rows live in the
// console's database and are never touched here.
//
// Only runs the console says are released, with a release date we can read, are
// ever touched, and only inside the runs folder, by the id the database gives.
// Anything else is kept: a deletion can't be undone, and keeping costs nothing.

export const EXTRACT_RETENTION_DAYS = 30;
export const FOLDER_RETENTION_MONTHS = 12;
export const SWEEP_INTERVAL_MS = 86_400_000;

const DAY = 86_400_000;
// The engine's own rule for a run id, so the id can only name a folder directly inside the runs folder.
const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/;

export type RetentionAction = "extracts_deleted" | "folder_deleted" | "kept" | "already_purged" | "no_folder" | "no_release_date" | "busy" | "refused";

export type RetentionOutcome = { runId: string; action: RetentionAction; files: number; dryRun: boolean; reason?: string };

export type RetentionDeps = {
  store: Pick<WorkerStore, "listReleasedRuns" | "listPulseRuns">;
  runsDir: string;
  /** The jobs' lock folder, so a documents job and the sweep never work on one run at once. */
  locksDir?: string;
  now?: Date;
  /** HC_CLEANUP_DRY_RUN: say what would go, and delete nothing. */
  dryRun: boolean;
  log: (line: string) => void;
};

/** The same day of the month, months later, in UTC. A day the later month doesn't have becomes its last. */
export function addMonths(date: Date, months: number): Date {
  const target = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + months, 1, date.getUTCHours(), date.getUTCMinutes(), date.getUTCSeconds(), date.getUTCMilliseconds()));
  const last = new Date(Date.UTC(target.getUTCFullYear(), target.getUTCMonth() + 1, 0)).getUTCDate();
  target.setUTCDate(Math.min(date.getUTCDate(), last));
  return target;
}

/** What one released run is due for now, before anything is checked on disk. */
export function dueFor(releasedAt: Date, now: Date): "folder" | "extracts" | "nothing" {
  if (now.getTime() >= addMonths(releasedAt, FOLDER_RETENTION_MONTHS).getTime()) return "folder";
  if (now.getTime() >= releasedAt.getTime() + EXTRACT_RETENTION_DAYS * DAY) return "extracts";
  return "nothing";
}

function sweepOne(run: ReleasedRun, root: string, now: Date, dryRun: boolean): RetentionOutcome {
  const outcome = (action: RetentionAction, files = 0, reason?: string): RetentionOutcome => ({ runId: run.runId, action, files, dryRun, ...(reason ? { reason } : {}) });
  if (!RUN_ID.test(run.runId)) return outcome("refused", 0, "the id isn't a run id");
  const released = run.releasedAt ? new Date(run.releasedAt) : null;
  if (!released || Number.isNaN(released.getTime())) return outcome("no_release_date");
  const folder = join(root, run.runId);
  // Belt and braces: the id rule already keeps it here.
  if (dirname(resolve(folder)) !== root) return outcome("refused", 0, "the folder isn't directly inside the runs folder");
  if (!existsSync(folder)) return outcome("no_folder");
  if (lstatSync(folder).isSymbolicLink()) return outcome("refused", 0, "the run folder is a link");
  const due = dueFor(released, now);
  if (due === "nothing") return outcome("kept");
  if (due === "folder") {
    if (dryRun) return outcome("folder_deleted");
    return outcome("folder_deleted", deleteRunFolder(folder, run.runId).filesDeleted);
  }
  if (isPurged(folder)) return outcome("already_purged");
  if (dryRun) return outcome("extracts_deleted");
  return outcome("extracts_deleted", purgeRun(folder, now).filesDeleted);
}

const WHY: Partial<Record<RetentionAction, string>> = {
  extracts_deleted: `raw extracts, ${EXTRACT_RETENTION_DAYS} days after release, or after a pulse finished`,
  folder_deleted: `the whole run folder, ${FOLDER_RETENTION_MONTHS} months after release, or after a pulse finished`,
};

/** One pass over every released run. Logs counts and run ids only, never a path or a name. */
export async function retentionSweep(deps: RetentionDeps): Promise<RetentionOutcome[]> {
  const now = deps.now ?? new Date();
  const root = resolve(deps.runsDir);
  // A pulse's date is when it finished. One the console lists both ways is swept once, by its release.
  const released = await deps.store.listReleasedRuns();
  const pulses = (await deps.store.listPulseRuns()).filter((p) => !released.some((r) => r.runId === p.runId));
  const runs: ReleasedRun[] = [...released, ...pulses.map((p) => ({ runId: p.runId, releasedAt: p.finishedAt }))];
  const outcomes: RetentionOutcome[] = [];
  for (const run of runs) {
    let result: RetentionOutcome;
    try {
      const work = async (): Promise<RetentionOutcome> => sweepOne(run, root, now, deps.dryRun);
      result = deps.locksDir && RUN_ID.test(run.runId) ? await withJobLock(deps.locksDir, `run-${run.runId}`, work) : await work();
    } catch (err) {
      // A job holds the run: the next sweep will get it.
      if (err instanceof JobBusyError) result = { runId: run.runId, action: "busy", files: 0, dryRun: deps.dryRun };
      // The purge refuses anything unexpected, such as a run.json for another run. Its message holds a path, so it isn't logged.
      else result = { runId: run.runId, action: "refused", files: 0, dryRun: deps.dryRun, reason: "the folder wasn't as expected" };
    }
    outcomes.push(result);
    const why = WHY[result.action];
    if (why) deps.log(`Retention: run ${run.runId}: ${deps.dryRun ? "would delete" : "deleted"} ${why}${deps.dryRun ? "" : ` (${result.files} files)`}.`);
    if (result.action === "refused") deps.log(`Retention: run ${run.runId}: refused, ${result.reason}. Nothing was deleted.`);
  }
  const count = (action: RetentionAction): number => outcomes.filter((o) => o.action === action).length;
  deps.log(
    `Retention sweep${deps.dryRun ? " (dry run, nothing deleted)" : ""}: ${released.length} released runs and ${pulses.length} pulses. Raw extracts ${deps.dryRun ? "due" : "deleted"}: ${count("extracts_deleted")}. Run folders ${deps.dryRun ? "due" : "deleted"}: ${count("folder_deleted")}. Kept: ${count("kept") + count("already_purged") + count("no_folder")}. No release date we can read: ${count("no_release_date")}. Busy: ${count("busy")}. Refused: ${count("refused")}.`,
  );
  return outcomes;
}
