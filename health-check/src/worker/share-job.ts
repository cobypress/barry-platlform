import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import type { FolderSharing } from "./drive.ts";
import { withJobLock } from "./lock.ts";
import { dueFor } from "./retention.ts";
import type { DriveShare, DriveShareRemoved, WorkerStore } from "./types.ts";

// Sharing a released run's Drive folder with the customer (roadmap decisions 9
// and 26, DECISIONS.md 251 to 255). The people are the run's microsite readers,
// so the console's one list decides who sees the report and who gets the
// files. Each is given read access to the run's "Health check n" folder, never
// the client's folder above it and never Black Cloud's own, and each share is
// recorded where the console reads it. Someone taken off the list loses
// access; at 12 months the retention sweep takes everyone's away.
//
// Never before release (rule 10), never "anyone with the link", never more
// than reader: the Drive client can't make any other kind (READER_PERMISSION).

export type ShareDeps = {
  store: Pick<WorkerStore, "loadShareJob" | "saveShare">;
  sharing: FolderSharing | null;
  now: () => Date;
  log: (line: string) => void;
};

export type ShareSummary = { shared: number; removed: number; failed: number; detail: string };

const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/;

/**
 * Brings the run's shares in line with its readers. Removals come first, one
 * at a time: Drive doesn't take changes to one folder's permissions at once.
 * `revokeAll` takes everyone's access away, whatever the list says.
 */
export async function syncShares(runId: string, deps: ShareDeps, options: { revokeAll?: DriveShareRemoved } = {}): Promise<ShareSummary> {
  const input = await deps.store.loadShareJob(runId);
  if (!input) return { shared: 0, removed: 0, failed: 0, detail: "There's no such run" };
  if (!input.recordable) return { shared: 0, removed: 0, failed: 0, detail: "The console can't record shares yet, so nothing was shared" };
  const at = () => deps.now().toISOString();
  const active = input.shares.filter((s) => s.state === "shared");
  // Twelve months after release nobody has the folder (DECISIONS.md 254), so a reader added or a documents make after the sweep can't hand it out again.
  const released = input.releasedAt ? new Date(input.releasedAt) : null;
  const pastRetention = released !== null && !Number.isNaN(released.getTime()) && dueFor(released, deps.now()) === "folder";
  const revokeAll = options.revokeAll ?? (pastRetention ? "retention" : undefined);
  // Rule 10: before release there's nothing anyone may have. With no folder there's nothing to share.
  const wanted = new Set(revokeAll || !input.released || !input.folderId ? [] : input.readers.map((r) => r.trim().toLowerCase()));
  const summary = { shared: 0, removed: 0, failed: 0 };
  const needsDrive = active.some((s) => !wanted.has(s.email) || s.folderId !== input.folderId) || [...wanted].some((e) => !active.some((s) => s.email === e && s.folderId === input.folderId));
  if (needsDrive && !deps.sharing) return { ...summary, failed: 1, detail: "Google Drive isn't set up here, so the folder's sharing can't change" };

  for (const share of active) {
    if (wanted.has(share.email) && share.folderId === input.folderId) continue;
    const why: DriveShareRemoved = revokeAll ?? (share.folderId !== input.folderId ? "folder_changed" : "reader_removed");
    try {
      if (share.permissionId) await deps.sharing!.unshare(share.folderId, share.permissionId);
      await deps.store.saveShare({ ...share, state: "removed", removedAt: at(), removedWhy: why, error: null, updatedAt: at() });
      summary.removed++;
    } catch (err) {
      // Still shared: say why it couldn't be taken away, and try again next time.
      await deps.store.saveShare({ ...share, error: `Couldn't take access away: ${safeReason(err)}`, updatedAt: at() });
      summary.failed++;
    }
  }

  for (const email of wanted) {
    if (active.some((s) => s.email === email && s.folderId === input.folderId)) continue;
    const before = input.shares.find((s) => s.email === email && s.folderId === input.folderId);
    const base: DriveShare = before ?? { runId, email, folderId: input.folderId!, permissionId: null, state: "failed", sharedAt: null, removedAt: null, removedWhy: null, error: null, updatedAt: at() };
    try {
      const permissionId = await deps.sharing!.share(input.folderId!, email);
      await deps.store.saveShare({ ...base, permissionId, state: "shared", sharedAt: at(), removedAt: null, removedWhy: null, error: null, updatedAt: at() });
      summary.shared++;
    } catch (err) {
      await deps.store.saveShare({ ...base, permissionId: null, state: "failed", error: safeReason(err), updatedAt: at() });
      summary.failed++;
    }
  }
  // Counts only: an address is a person's, so it isn't logged.
  const detail = `Drive sharing for ${runId}: ${summary.shared} shared, ${summary.removed} removed, ${summary.failed} failed`;
  deps.log(detail);
  return { ...summary, detail };
}

/** A share job from the console: its readers changed on a released run. */
export async function processShareJob(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  if (!RUN_ID.test(runId)) return { outcome: "failed", detail: "The run id isn't one the worker can use" };
  const log = deps.log ?? (() => {});
  // The run's own lock: a documents job for it may be moving its folder.
  const summary = await withJobLock(deps.locksDir, `run-${runId}`, () => syncShares(runId, { store: deps.store, sharing: deps.sharing ?? null, now: deps.now ?? (() => new Date()), log }));
  // A failed share is recorded on its row for the console. Another delivery may get past a passing Drive error.
  if (summary.failed && !attempt.final) throw new Error(summary.detail);
  return { outcome: summary.failed ? "failed" : "done", detail: summary.detail };
}
