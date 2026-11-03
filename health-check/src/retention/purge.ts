import { existsSync, lstatSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { z } from "zod";
import { Run } from "../schemas/index.ts";

// Raw extracts are deleted 30 days after delivery. They're the run's
// checkpoints/ folder, which holds CRM rows. Everything else stays: findings,
// metrics, scores, calibration and reports are aggregates, and the pseudonym map
// is needed to render the reports again with names in them.
//
// Nothing outside checkpoints/ is ever deleted, and only in a folder that has a
// valid run.json, so a mistyped path can't empty the wrong folder.

export const PURGE_FILE = "purged.json";
export const defaultRunsDir = (): string => join(homedir(), ".barometer", "runs");

const DAY = 86_400_000;

export const PurgeRecord = z.strictObject({
  purgedAt: z.iso.datetime(),
  deleted: z.literal("checkpoints/"),
  filesDeleted: z.number().int().nonnegative(),
});
export type PurgeRecord = z.infer<typeof PurgeRecord>;

export type PurgeResult = { runId: string; purged: boolean; filesDeleted: number; note: string | null };

/** The run in a folder, or an error saying why the folder isn't one. */
function readRun(folder: string): Run {
  if (!existsSync(folder) || !statSync(folder).isDirectory()) throw new Error(`${folder} isn't a folder.`);
  const path = join(folder, "run.json");
  if (!existsSync(path)) throw new Error(`${folder} isn't a run folder: it has no run.json.`);
  const parsed = Run.safeParse((() => {
    try {
      return JSON.parse(readFileSync(path, "utf8"));
    } catch {
      return null;
    }
  })());
  if (!parsed.success) throw new Error(`${folder} isn't a run folder: its run.json isn't a valid run.`);
  return parsed.data;
}

export function isPurged(folder: string): boolean {
  return existsSync(join(folder, PURGE_FILE));
}

/** Deletes one run's raw extracts and records when. A run already purged is left alone. */
export function purgeRun(dir: string, now: Date = new Date()): PurgeResult {
  const folder = resolve(dir);
  const run = readRun(folder);
  if (isPurged(folder)) return { runId: run.id, purged: false, filesDeleted: 0, note: "already purged" };

  const checkpoints = join(folder, "checkpoints");
  let filesDeleted = 0;
  if (existsSync(checkpoints)) {
    const stat = lstatSync(checkpoints);
    // rm would remove a link rather than follow it, but a link here means
    // something unexpected, and a person should look before anything goes.
    if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`${checkpoints} isn't a plain folder, so nothing was deleted.`);
    filesDeleted = readdirSync(checkpoints, { recursive: true, withFileTypes: true }).filter((e) => !e.isDirectory()).length;
    rmSync(checkpoints, { recursive: true });
  }
  // Recorded only after the delete, so a failure part way leaves the run unmarked and a retry finishes it.
  const record = PurgeRecord.parse({ purgedAt: now.toISOString(), deleted: "checkpoints/", filesDeleted });
  writeFileSync(join(folder, PURGE_FILE), `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
  return { runId: run.id, purged: true, filesDeleted, note: filesDeleted === 0 ? "no raw extracts were left" : null };
}

export type SweepResult = { purged: PurgeResult[]; skipped: Array<{ folder: string; reason: string }>; kept: number };

/**
 * Purges every run in a folder whose run date is more than `olderThanDays` ago.
 * The delivery date isn't recorded yet, so the run date (`asOf`) stands in for it,
 * which purges no later than delivery would.
 */
export function sweepRuns(runsDir: string, olderThanDays: number, now: Date = new Date()): SweepResult {
  if (!Number.isInteger(olderThanDays) || olderThanDays < 1) throw new Error("--older-than must be a whole number of days, 1 or more.");
  const root = resolve(runsDir);
  if (!existsSync(root) || !statSync(root).isDirectory()) throw new Error(`${root} isn't a folder.`);
  const result: SweepResult = { purged: [], skipped: [], kept: 0 };
  const cutoff = now.getTime() - olderThanDays * DAY;
  // A Dirent for a symbolic link isn't a directory, so links are never followed.
  for (const entry of readdirSync(root, { withFileTypes: true }).filter((e) => e.isDirectory()).sort((a, b) => (a.name < b.name ? -1 : 1))) {
    const folder = join(root, entry.name);
    let run: Run;
    try {
      run = readRun(folder);
    } catch (err) {
      result.skipped.push({ folder: entry.name, reason: err instanceof Error ? err.message : String(err) });
      continue;
    }
    if (isPurged(folder) || new Date(run.asOf).getTime() >= cutoff) {
      result.kept++;
      continue;
    }
    result.purged.push(purgeRun(folder, now));
  }
  return result;
}
