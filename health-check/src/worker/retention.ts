import { existsSync, lstatSync, readdirSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { APEX_SOURCE_FILE } from "../extract/apex-source.ts";
import { SUMMARIES_PARTIAL_FILE } from "../handbook/summaries.ts";
import { deleteRunFolder, isPurged, purgeRun } from "../retention/purge.ts";
import { JobBusyError, withJobLock } from "./lock.ts";
import type { FolderSharing } from "./drive.ts";
import { mayHaveAccess, syncShares } from "./share-job.ts";
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

export type RetentionAction = "extracts_deleted" | "folder_deleted" | "kept" | "already_purged" | "no_folder" | "no_release_date" | "busy" | "refused" | "access_kept";

export type RetentionOutcome = { runId: string; action: RetentionAction; files: number; dryRun: boolean; reason?: string };

export type RetentionDeps = {
  store: Pick<WorkerStore, "listReleasedRuns" | "listPulseRuns" | "loadShareJob" | "saveShare">;
  /** Takes the customer's Drive access away at 12 months, before the run folder goes (DECISIONS.md 254). */
  sharing?: FolderSharing | null;
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

/**
 * Takes away everyone's access to a run's Drive folder, and says how many
 * still have it. A dry run takes nothing away and counts who would lose it.
 * The run folder is kept while anyone does, so the record of who has it isn't
 * lost before they've lost it.
 */
async function accessLeft(runId: string, deps: RetentionDeps, now: Date): Promise<number> {
  const input = await deps.store.loadShareJob(runId);
  const active = input?.shares.filter(mayHaveAccess).length ?? 0;
  if (active === 0 || deps.dryRun) {
    if (active) deps.log(`Retention: run ${runId}: would take Drive access away from ${active} ${active === 1 ? "person" : "people"}.`);
    return 0;
  }
  const result = await syncShares(runId, { store: deps.store, sharing: deps.sharing ?? null, now: () => now, log: deps.log }, { revokeAll: "retention" });
  return result.failed;
}

/** How long the Apex code kept for Claude's summaries, and summaries made from it, may sit in a run folder. */
export const KEPT_CODE_DAYS = 7;
/** The only files the code sweep deletes, by name. */
export const KEPT_CODE_FILES: readonly string[] = [APEX_SOURCE_FILE, SUMMARIES_PARTIAL_FILE];

/**
 * Deletes the Apex code a run kept for Claude's summaries, and the partial
 * summaries beside it, once either is more than seven days old, whatever the
 * run's state in the database (DECISIONS.md 491): a run stopped on the API
 * budget and never resumed mustn't keep code. A dry run doesn't hold it back:
 * the dry run is for deletions a person might want to check first, and kept
 * code is never one. Only those two names, only directly inside a run's
 * folder, never through a link, and never outside the runs folder. Returns
 * how many it deleted.
 */
export function sweepKeptCode(runsDir: string, now: Date): number {
  const root = resolve(runsDir);
  if (!existsSync(root)) return 0;
  let deleted = 0;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    // A link to a folder is neither a directory nor followed.
    if (!entry.isDirectory() || !RUN_ID.test(entry.name)) continue;
    for (const name of KEPT_CODE_FILES) {
      const path = join(root, entry.name, name);
      if (dirname(dirname(resolve(path))) !== root) continue;
      let stat;
      try {
        stat = lstatSync(path);
      } catch {
        continue;
      }
      if (!stat.isFile() || now.getTime() - stat.mtimeMs <= KEPT_CODE_DAYS * DAY) continue;
      rmSync(path);
      deleted++;
    }
  }
  return deleted;
}

/** One pass over every released run. Logs counts and run ids only, never a path or a name. */
export async function retentionSweep(deps: RetentionDeps): Promise<RetentionOutcome[]> {
  const now = deps.now ?? new Date();
  const root = resolve(deps.runsDir);
  // First, and on its own: it looks at every run folder, released or not.
  let kept = 0;
  try {
    kept = sweepKeptCode(root, now);
    if (kept) deps.log(`Retention: deleted ${kept} ${kept === 1 ? "file" : "files"} of Apex code or summaries kept for Claude for more than ${KEPT_CODE_DAYS} days${deps.dryRun ? ", even in a dry run: kept code is never held back" : ""}.`);
  } catch {
    deps.log("Retention: the sweep of Apex code kept for Claude's summaries failed, and will run again in a day.");
  }
  // A pulse's date is when it finished. One the console lists both ways is swept once, by its release.
  const released = await deps.store.listReleasedRuns();
  const pulses = (await deps.store.listPulseRuns()).filter((p) => !released.some((r) => r.runId === p.runId));
  const runs: ReleasedRun[] = [...released, ...pulses.map((p) => ({ runId: p.runId, releasedAt: p.finishedAt }))];
  const outcomes: RetentionOutcome[] = [];
  for (const run of runs) {
    let result: RetentionOutcome;
    try {
      const work = async (): Promise<RetentionOutcome> => {
        // At 12 months, everyone's access to the Drive folder goes first, whether or not the run folder is still here.
        const released = run.releasedAt ? new Date(run.releasedAt) : null;
        if (released && !Number.isNaN(released.getTime()) && RUN_ID.test(run.runId) && dueFor(released, now) === "folder") {
          const left = await accessLeft(run.runId, deps, now);
          if (left > 0) return { runId: run.runId, action: "access_kept", files: 0, dryRun: deps.dryRun, reason: `${left} ${left === 1 ? "person still has" : "people still have"} the Drive folder` };
        }
        return sweepOne(run, root, now, deps.dryRun);
      };
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
    if (result.action === "access_kept") deps.log(`Retention: run ${run.runId}: ${result.reason}, so its folder is kept until the next sweep takes their access away.`);
  }
  const count = (action: RetentionAction): number => outcomes.filter((o) => o.action === action).length;
  deps.log(
    `Retention sweep${deps.dryRun ? ` (dry run, nothing deleted${kept ? " but the Apex code kept for Claude" : ""})` : ""}: ${released.length} released runs and ${pulses.length} pulses. Raw extracts ${deps.dryRun ? "due" : "deleted"}: ${count("extracts_deleted")}. Run folders ${deps.dryRun ? "due" : "deleted"}: ${count("folder_deleted")}. Kept: ${count("kept") + count("already_purged") + count("no_folder")}. No release date we can read: ${count("no_release_date")}. Busy: ${count("busy")}. Refused: ${count("refused")}.`,
  );
  return outcomes;
}
