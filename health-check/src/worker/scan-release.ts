import { statSync } from "node:fs";
import { join } from "node:path";
import { OAuthError, revokeToken } from "../salesforce/oauth.ts";
import type { StoredConnection } from "../salesforce/token-store.ts";
import { fetchOAuthTransport } from "../salesforce/transport.ts";
import type { WorkerDeps } from "./deps.ts";
import { JobBusyError, withJobLock } from "./lock.ts";

// A free scan gives the org's access back however its run ends (DECISIONS.md
// 652). The scan's pages and the privacy notice promise it, so there is one way
// to let go, used by every end of a scan's run job, by a job BullMQ gave up on,
// and by the sweep for whatever slips past both. Health checks, re-checks and
// pulses keep their connections on purpose and never come here: the run job
// asks only for a run the console says is a scan, and a connection is let go
// only when it reads as that scan's: the connect job's mark, or for one stored
// before marks, its link's run read as product scan.

/** Tries at revoking, the first included. */
export const REVOKE_TRIES = 3;
/** The waits before the second and third tries. */
export const REVOKE_WAITS_MS: readonly number[] = [2_000, 8_000];

export type LetGo = "nothing_held" | "revoked" | "not_confirmed" | "unreadable" | "not_ours";

type ReleaseDeps = Pick<WorkerDeps, "tokens" | "store" | "oauthTransport" | "now" | "log" | "sleep">;

/**
 * Worth trying again: no answer (fetch's TypeError), a timeout, or Salesforce's
 * own fault (a 5xx, or OAuth's server_error or temporarily_unavailable). A 4xx is its answer, and our own refusal to send (a host that
 * isn't Salesforce's, say) is ours: neither changes by waiting.
 */
const passing = (err: unknown): boolean =>
  err instanceof OAuthError ? /^HTTP_5\d\d$|^server_error$|^temporarily_unavailable$/.test(err.code) : err instanceof TypeError || (err instanceof Error && (err.name === "TimeoutError" || err.name === "AbortError"));

/** What a log line may say of a failure: an OAuth code, or an error's class. Never its message, which could quote a value. */
const errorClass = (err: unknown): string => (err instanceof OAuthError ? err.code : err instanceof Error ? err.name : "error");

type Read = { kind: "none" } | { kind: "unreadable" } | { kind: "held"; stored: StoredConnection };

/** The stored connection, if there is one and it can be read. Never throws. */
async function read(name: string, deps: Pick<WorkerDeps, "tokens">): Promise<Read> {
  try {
    if (!deps.tokens.has(name)) return { kind: "none" };
    return { kind: "held", stored: await deps.tokens.load(name) };
  } catch {
    return { kind: "unreadable" };
  }
}

/**
 * The scan a stored connection belongs to, or null. Its mark says, or, for a
 * connection stored before marks existed, its link's run read as product
 * scan. Anything else, an error included, is null: only a positive read makes
 * a connection a scan's, so a customer's can never be one.
 */
async function scanRunOf(stored: StoredConnection, deps: Pick<WorkerDeps, "store">): Promise<string | null> {
  if (stored.scan) return stored.scan.runId;
  if (!stored.fromLink) return null;
  try {
    return await deps.store.scanRunOfLink(stored.fromLink.linkId);
  } catch {
    return null;
  }
}

/**
 * Whether a connection a scan's run names is that scan's. Marked, or made
 * through a link: only if that says this scan, read positively. Made outside
 * any link (`hc connect`), there's nothing to contradict the scan's run row,
 * which is the console's record that it is the scan's.
 */
async function belongsTo(stored: StoredConnection, runId: string, deps: Pick<WorkerDeps, "store">): Promise<boolean> {
  if (stored.scan || stored.fromLink) return (await scanRunOf(stored, deps)) === runId;
  return true;
}

/**
 * Lets go of a scan's connection: revoked at Salesforce (the refresh token,
 * which ends its access tokens too), then our copy deleted. Only when the
 * connection reads as this scan's (`belongsTo`): otherwise it's left, and
 * said. One that can't be read is kept for the sweep to try again. If
 * Salesforce doesn't confirm, our copy goes anyway, the run is marked "revoke
 * not confirmed", one line is logged, and the revoke is tried again from
 * memory, a bounded number of times. The token is never written anywhere to
 * try later. Never throws.
 */
export async function letGoOfScan(runId: string, name: string, deps: ReleaseDeps): Promise<LetGo> {
  const log = deps.log ?? (() => {});
  const held = await read(name, deps);
  if (held.kind === "none") return "nothing_held";
  if (held.kind === "unreadable") {
    log(`Scan ${runId}: the stored connection "${name}" couldn't be read, so it is kept for the scan sweep to try again.`);
    return "unreadable";
  }
  if (!(await belongsTo(held.stored, runId, deps))) {
    log(`Scan ${runId}: the stored connection "${name}" isn't marked as this scan's, so it was left alone.`);
    return "not_ours";
  }
  return revokeHeld(runId, name, held.stored, deps);
}

async function revokeHeld(runId: string, name: string, held: StoredConnection, deps: ReleaseDeps): Promise<LetGo> {
  const log = deps.log ?? (() => {});
  const note = (confirmed: boolean) => noteRevoke(runId, confirmed, deps);
  const forget = (): void => {
    try {
      deps.tokens.delete(name);
    } catch (err) {
      log(`Scan ${runId}'s stored connection couldn't be deleted (${errorClass(err)}). The scan sweep tries again.`);
    }
  };
  const transport = deps.oauthTransport ?? fetchOAuthTransport;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const revoke = (): Promise<void> => revokeToken({ transport, instanceUrl: held.instanceUrl, token: held.refreshToken ?? held.accessToken });

  try {
    await revoke();
    forget();
    log(`Scan ${runId}'s connection is revoked.`);
    return "revoked";
  } catch (err) {
    forget();
    log(`Salesforce didn't confirm revoking scan ${runId}'s connection, so the worker deleted its copy: ${errorClass(err)}.`);
    await note(false);
    if (!passing(err)) return "not_confirmed";
  }
  for (let attempt = 2; attempt <= REVOKE_TRIES; attempt++) {
    await sleep(REVOKE_WAITS_MS[attempt - 2] ?? 0);
    try {
      await revoke();
      await note(true);
      log(`Scan ${runId}'s connection is revoked, on try ${attempt} of ${REVOKE_TRIES}.`);
      return "revoked";
    } catch (err) {
      if (!passing(err)) break;
    }
  }
  return "not_confirmed";
}

async function noteRevoke(runId: string, confirmed: boolean, deps: ReleaseDeps): Promise<void> {
  try {
    await deps.store.noteScanRevoke(runId, confirmed, (deps.now ?? (() => new Date()))().toISOString());
  } catch (err) {
    (deps.log ?? (() => {}))(`Scan ${runId}: couldn't record whether Salesforce confirmed the revoke (${errorClass(err)}).`);
  }
}

/** What a scan's run job knew of its run, for letting go once the job has an outcome. */
export type SeenRun = { product?: string; connectionName?: string | null };

/**
 * After a run job, inside its lock: a scan's run lets go of the connection
 * its run names, if that reads as this scan's. With none named, or a run
 * whose product the job never read (gone, or the store down), it lets go of
 * the connections marked for it, which are only ever a scan's. Any other
 * product is left alone. Never throws.
 */
export async function letGoAfterRun(runId: string, seen: SeenRun, deps: ReleaseDeps): Promise<void> {
  try {
    if (seen.product === "scan" && seen.connectionName) {
      await letGoOfScan(runId, seen.connectionName, deps);
      return;
    }
    if (seen.product !== undefined && seen.product !== "scan") return;
    for (const name of deps.tokens.list()) {
      const held = await read(name, deps);
      if (held.kind === "held" && held.stored.scan?.runId === runId) await revokeHeld(runId, name, held.stored, deps);
    }
  } catch (err) {
    (deps.log ?? (() => {}))(`Scan ${runId}: letting go of its connection stopped (${errorClass(err)}). The scan sweep tries again.`);
  }
}

/**
 * A run job BullMQ gave up on without running it (DECISIONS.md 396): a scan's
 * lets go as any other end does. Reads the run to know its product.
 */
export async function letGoOfLostRun(runId: string, deps: ReleaseDeps): Promise<void> {
  let seen: SeenRun = {};
  try {
    const input = await deps.store.loadRunJob(runId);
    if (input) seen = { product: input.run.product, connectionName: input.connectionName };
  } catch {
    // Unread: only connections marked for this run are let go.
  }
  await letGoAfterRun(runId, seen, deps);
}

const HOUR = 3_600_000;
/** How old a scan's connection may get before the sweep lets it go, once its run has ended or is gone: a scan's longest run, with a margin. */
export const SCAN_CONNECTION_MAX_MS = 6 * HOUR;
/** How old it may get whatever its run's status says: a scan queued or running this long has stalled. */
export const SCAN_CONNECTION_HARD_MAX_MS = 24 * HOUR;
/** How long a scan's connection that can't be read is kept for the sweep to try again before it is deleted. */
export const SCAN_UNREADABLE_MAX_MS = 7 * 24 * HOUR;
/** How often the sweep runs, beside the start. */
export const SCAN_SWEEP_INTERVAL_MS = HOUR;
/** The most connections one sweep lets go of. The rest wait for the next. */
export const SCAN_SWEEP_MAX = 20;
/** The most connections no run names that one sweep decrypts. The next sweep carries on from where this one stopped. */
export const SCAN_SWEEP_READS = 100;
/** A scan whose run is in one of these is queued or running, so the sweep leaves it until the hard limit. */
const LIVE = new Set(["extracting", "analysing", "awaiting_manual"]);

export type ScanSweepOutcome = { checked: number; customers: number; letGo: number; busy: number; waiting: number; unreadable: number; left: number };
/** Where the next sweep starts among the connections no run names. Kept by the worker between sweeps. */
export type ScanSweepState = { cursor: number };

/**
 * The safety net (DECISIONS.md 652). The store first says which runs name
 * each stored connection: one named only by runs that aren't scans is a
 * customer's, and isn't even decrypted. One a scan run names is read; so are
 * those no run names, up to SCAN_SWEEP_READS a sweep in rotation. A connection
 * is a scan's only by `scanRunOf`. It is let go once more than 6 hours old if
 * its run has ended or is gone, or more than 24 hours old whatever its run
 * says, and only while no job holds the run's lock, which the sweep then
 * holds. A queued or running scan, or one whose status can't be read, is left
 * until the 24 hours. A scan's connection that can't be read (named by a scan
 * run and by no other) is kept to try again, and deleted, marked not
 * confirmed, once the file is more than 7 days old. Logs counts, run ids and
 * connection names only.
 */
export async function sweepScanConnections(deps: ReleaseDeps & Pick<WorkerDeps, "locksDir">, state: ScanSweepState = { cursor: 0 }): Promise<ScanSweepOutcome> {
  const log = deps.log ?? (() => {});
  const now = (deps.now ?? (() => new Date()))().getTime();
  const names = deps.tokens.list();
  const outcome: ScanSweepOutcome = { checked: 0, customers: 0, letGo: 0, busy: 0, waiting: 0, unreadable: 0, left: 0 };
  let named: Map<string, Array<{ runId: string; product: string }>> | null;
  try {
    named = await deps.store.connectionProducts(names);
  } catch {
    named = null;
  }
  const scanOnly = (name: string): string | null => {
    const rows = named?.get(name);
    return rows && rows.length > 0 && rows.every((r) => r.product === "scan") ? rows[0]!.runId : null;
  };
  const priority: string[] = [];
  const unnamed: string[] = [];
  for (const name of names) {
    const rows = named?.get(name);
    if (rows?.every((r) => r.product !== "scan")) outcome.customers++;
    else if (rows) priority.push(name);
    else unnamed.push(name);
  }
  const start = unnamed.length ? state.cursor % unnamed.length : 0;
  const turn = [...unnamed.slice(start), ...unnamed.slice(0, start)].slice(0, SCAN_SWEEP_READS);
  state.cursor = unnamed.length ? (start + turn.length) % unnamed.length : 0;

  const due: Array<{ name: string; runId: string; unreadable?: boolean }> = [];
  for (const name of [...priority, ...turn]) {
    outcome.checked++;
    const held = await read(name, deps);
    if (held.kind === "none") continue;
    if (held.kind === "unreadable") {
      const runId = scanOnly(name);
      if (!runId) continue;
      outcome.unreadable++;
      if (fileAge(deps.tokens.dir, name, now) > SCAN_UNREADABLE_MAX_MS) due.push({ name, runId, unreadable: true });
      else log(`Scan sweep: scan ${runId}'s stored connection "${name}" couldn't be read, and is kept to try again.`);
      continue;
    }
    const runId = await scanRunOf(held.stored, deps);
    if (!runId) continue;
    const made = Date.parse(held.stored.connectedAt);
    // A date we can't read is treated as past both limits: a scan's connection mustn't outlive the sweep for want of one.
    const age = Number.isNaN(made) ? Infinity : now - made;
    if (age <= SCAN_CONNECTION_MAX_MS) continue;
    if (age <= SCAN_CONNECTION_HARD_MAX_MS) {
      let run: { status: string } | null | "unknown";
      try {
        run = await deps.store.runState(runId);
      } catch {
        run = "unknown";
      }
      if (run === "unknown" || (run && LIVE.has(run.status))) {
        outcome.waiting++;
        continue;
      }
    }
    due.push({ name, runId });
  }
  outcome.left = Math.max(0, due.length - SCAN_SWEEP_MAX);
  for (const { name, runId, unreadable } of due.slice(0, SCAN_SWEEP_MAX)) {
    try {
      await withJobLock(deps.locksDir, `run-${runId}`, async () => {
        if (unreadable) {
          if ((await read(name, deps)).kind !== "unreadable") return;
          deps.tokens.delete(name);
          await noteRevoke(runId, false, deps);
          log(`Scan sweep: scan ${runId}'s stored connection "${name}" couldn't be read for more than 7 days, so it is deleted without a revoke.`);
          outcome.letGo++;
          return;
        }
        const result = await letGoOfScan(runId, name, deps);
        if (result !== "revoked" && result !== "not_confirmed") return;
        outcome.letGo++;
        log(`Scan sweep: let go of scan ${runId}'s connection.`);
      });
    } catch (err) {
      if (err instanceof JobBusyError) outcome.busy++;
      else log(`Scan sweep: scan ${runId}'s connection couldn't be let go (${errorClass(err)}), and is tried again next time.`);
    }
  }
  if (outcome.letGo || outcome.busy || outcome.left || outcome.waiting) {
    log(`Scan sweep: read ${outcome.checked}, let go of ${outcome.letGo}, ${outcome.waiting} with a scan still queued or running, ${outcome.busy} in a running job, ${outcome.left} left for the next sweep.`);
  }
  return outcome;
}

/** How long ago the connection's file last changed, or Infinity when it can't be told. */
function fileAge(dir: string, name: string, now: number): number {
  try {
    return now - statSync(join(dir, `${name}.json`)).mtimeMs;
  } catch {
    return Infinity;
  }
}
