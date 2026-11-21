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
// asks only for a run the console says is a scan, and the sweep matches only a
// connection the connect job marked as a scan's.

/** Tries at revoking, the first included. */
export const REVOKE_TRIES = 3;
/** The waits before the second and third tries. */
export const REVOKE_WAITS_MS: readonly number[] = [2_000, 8_000];

export type LetGo = "nothing_held" | "revoked" | "not_confirmed";

type ReleaseDeps = Pick<WorkerDeps, "tokens" | "store" | "oauthTransport" | "now" | "log" | "sleep">;

/** Worth trying again: no answer, a timeout, or Salesforce's own fault. A 4xx is its answer, and won't change. */
const passing = (err: unknown): boolean => !(err instanceof OAuthError) || /^HTTP_5\d\d$/.test(err.code);

/** What a log line may say of a failure: an OAuth code, or an error's class. Never its message, which could quote a value. */
const errorClass = (err: unknown): string => (err instanceof OAuthError ? err.code : err instanceof Error ? err.name : "error");

/**
 * Lets go of a scan's connection: revoked at Salesforce (the refresh token,
 * which ends its access tokens too), then our copy deleted. If Salesforce
 * doesn't confirm, our copy goes anyway, the run is marked "revoke not
 * confirmed", one line is logged, and the revoke is tried again from memory,
 * a bounded number of times. The token is never written anywhere to try later.
 * Never throws.
 */
export async function letGoOfScan(runId: string, name: string, deps: ReleaseDeps): Promise<LetGo> {
  const log = deps.log ?? (() => {});
  const at = (): string => (deps.now ?? (() => new Date()))().toISOString();
  const note = async (confirmed: boolean): Promise<void> => {
    try {
      await deps.store.noteScanRevoke(runId, confirmed, at());
    } catch (err) {
      log(`Scan ${runId}: couldn't record whether Salesforce confirmed the revoke (${errorClass(err)}).`);
    }
  };
  const forget = (): boolean => {
    try {
      deps.tokens.delete(name);
      return true;
    } catch (err) {
      log(`Scan ${runId}'s stored connection couldn't be deleted (${errorClass(err)}). The scan sweep tries again.`);
      return false;
    }
  };

  let held: StoredConnection;
  try {
    if (!deps.tokens.has(name)) return "nothing_held";
    held = await deps.tokens.load(name);
  } catch (err) {
    // Unreadable, so nothing to revoke with: it still mustn't stay.
    forget();
    log(`Scan ${runId}'s stored connection couldn't be read (${errorClass(err)}), so the worker deleted it without revoking it at Salesforce.`);
    await note(false);
    return "not_confirmed";
  }
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

/** What a scan's run job knew of its run, for letting go once the job has an outcome. */
export type SeenRun = { product?: string; connectionName?: string | null };

/**
 * After a run job, inside its lock: a scan's run lets go of its connection.
 * A run whose product the job never read (gone, or the store down) lets go
 * of the connections marked for it, which are only ever a scan's. Any other
 * product is left alone. Never throws.
 */
export async function letGoAfterRun(runId: string, seen: SeenRun, deps: ReleaseDeps): Promise<void> {
  try {
    if (seen.product === "scan") {
      if (seen.connectionName) await letGoOfScan(runId, seen.connectionName, deps);
      return;
    }
    if (seen.product !== undefined) return;
    for (const { name } of await markedFor(deps, (stored) => stored.scan.runId === runId)) await letGoOfScan(runId, name, deps);
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

type Marked = StoredConnection & { scan: NonNullable<StoredConnection["scan"]> };

/** Stored connections marked as a scan's that pass `match`. One that can't be read isn't known to be a scan's, so isn't listed. */
async function markedFor(deps: Pick<WorkerDeps, "tokens">, match: (stored: Marked) => boolean): Promise<Array<{ name: string; runId: string }>> {
  const found: Array<{ name: string; runId: string }> = [];
  for (const name of deps.tokens.list()) {
    const stored = await deps.tokens.load(name).catch(() => undefined);
    if (stored?.scan && match(stored as Marked)) found.push({ name, runId: stored.scan.runId });
  }
  return found;
}

/** How old a scan's connection may get before the sweep lets it go: a scan's longest run, with a margin. */
export const SCAN_CONNECTION_MAX_MS = 6 * 3_600_000;
/** How often the sweep runs, beside the start. */
export const SCAN_SWEEP_INTERVAL_MS = 3_600_000;
/** The most connections one sweep lets go of. The rest wait for the next. */
export const SCAN_SWEEP_MAX = 20;

export type ScanSweepOutcome = { checked: number; letGo: number; busy: number; left: number };

/**
 * The safety net: lets go of every connection marked as a scan's, made more
 * than SCAN_CONNECTION_MAX_MS ago, whose run no job is working on. The mark
 * is the only way in: a connection without one, a customer's, is never even
 * a candidate. The run's lock is held while letting go, so a delivery can't
 * start reading through a connection being revoked. Logs counts and run ids.
 */
export async function sweepScanConnections(deps: ReleaseDeps & Pick<WorkerDeps, "locksDir">): Promise<ScanSweepOutcome> {
  const log = deps.log ?? (() => {});
  const now = (deps.now ?? (() => new Date()))().getTime();
  const old = (stored: StoredConnection): boolean => {
    const made = Date.parse(stored.connectedAt);
    // A date we can't read is treated as old: a scan's connection mustn't outlive the sweep for want of one.
    return Number.isNaN(made) || now - made > SCAN_CONNECTION_MAX_MS;
  };
  const candidates = await markedFor(deps, old);
  const outcome: ScanSweepOutcome = { checked: deps.tokens.list().length, letGo: 0, busy: 0, left: Math.max(0, candidates.length - SCAN_SWEEP_MAX) };
  for (const { name, runId } of candidates.slice(0, SCAN_SWEEP_MAX)) {
    try {
      await withJobLock(deps.locksDir, `run-${runId}`, async () => {
        // Read again under the lock: a delivery may have let it go meanwhile.
        if (!deps.tokens.has(name)) return;
        await letGoOfScan(runId, name, deps);
        outcome.letGo++;
        log(`Scan sweep: let go of scan ${runId}'s connection, made more than ${SCAN_CONNECTION_MAX_MS / 3_600_000} hours ago.`);
      });
    } catch (err) {
      if (err instanceof JobBusyError) outcome.busy++;
      else log(`Scan sweep: scan ${runId}'s connection couldn't be let go (${errorClass(err)}), and is tried again next time.`);
    }
  }
  if (outcome.letGo || outcome.busy || outcome.left) log(`Scan sweep: let go of ${outcome.letGo}, ${outcome.busy} in a running job, ${outcome.left} left for the next sweep.`);
  return outcome;
}
