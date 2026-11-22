import { basename, dirname, join } from "node:path";
import { MODELS } from "../config.ts";
import { anthropicClientWithKey, federationClient, type NarrativeClient } from "../narrative/client.ts";
import { checkClaude } from "../narrative/guard.ts";
import { ClaudeWatch, federationCachePath, identityTokenFacts, prepareSignInCache, SIGN_IN_RETRY_MINUTES } from "../narrative/sign-in.ts";
import { tokenKeySource, TokenStore } from "../salesforce/token-store.ts";
import { HEALTH_CHECK_QUEUE, HEAVY_QUEUE, lostRecorder, RECYCLE_EXIT, RECYCLE_WATCHDOG_MS, recycleWatch, redisConnection, runQueuer, startQueueWorker, type BullModule, type StoreFill } from "../worker/bullmq.ts";
import { GoogleDrive } from "../worker/drive.ts";
import { readWorkerEnv, WORKER_ENV } from "../worker/env.ts";
import { PostgresWorkerStore, type SqlPool } from "../worker/postgres-store.ts";
import { retentionSweep, SWEEP_INTERVAL_MS } from "../worker/retention.ts";
import { createJobHandler } from "../worker/worker.ts";
import type { WorkerDeps } from "../worker/deps.ts";
import { JobBusyError, withJobLock } from "../worker/lock.ts";
import { letGoOfLostRun, SCAN_SWEEP_INTERVAL_MS, sweepScanConnections } from "../worker/scan-release.ts";

// `hc worker`: takes run, connect, documents and share jobs from the console's queue until told
// to stop. It refuses to start without every variable in WORKER_ENV. BullMQ
// and pg aren't engine dependencies (see DECISIONS), so they're loaded here,
// and only once the environment is complete.

export const WORKER_USAGE = `
  hc worker                                          Take jobs from the console's health-check-jobs queue

The worker needs:
${Object.entries(WORKER_ENV.required).map(([name, why]) => `  ${name.padEnd(24)} ${why}`).join("\n")}
and may take:
${Object.entries(WORKER_ENV.optional).map(([name, why]) => `  ${name.padEnd(24)} ${why}`).join("\n")}
It also needs bullmq and pg installed beside the engine. With HC_WORKER_RECYCLE_AT set, it
stops itself once its store of org-worded sentences holds that many, finishing the jobs in
hand, and exits ${RECYCLE_EXIT} for the host to start a fresh one (DECISIONS.md 666).`;

export type PgModule = { Pool: new (options: { connectionString: string; max?: number }) => SqlPool & { end(): Promise<void> } };
export type WorkerModules = { bull: BullModule; pg: PgModule };

export async function loadWorkerModules(): Promise<WorkerModules> {
  // Held in variables so the type checker doesn't look for packages the engine doesn't install.
  const [bullName, pgName] = ["bullmq", "pg"];
  try {
    const bull = (await import(bullName!)) as BullModule;
    const pg = (await import(pgName!)) as PgModule & { default?: PgModule };
    return { bull, pg: pg.default ?? pg };
  } catch {
    throw new Error("hc worker needs bullmq and pg: npm install bullmq@^5.70 pg@^8.19, the majors blackcloud-next uses");
  }
}

function untilSignalled(): Promise<void> {
  return new Promise((resolve) => {
    process.once("SIGINT", () => resolve());
    process.once("SIGTERM", () => resolve());
  });
}

export async function workerCommand(
  env: NodeJS.ProcessEnv = process.env,
  options: {
    load?: () => Promise<WorkerModules>;
    until?: Promise<void>;
    log?: (line: string) => void;
    /** Tests only: stands in for the startup call to Claude. */
    claudeCheck?: (client: NarrativeClient) => Promise<{ ok: boolean; detail: string; signIn?: boolean }>;
    /** Tests only: the fetch the federation client uses, for a stand-in Anthropic. */
    claudeFetch?: typeof fetch;
    /** Tests only: the waits between the startup check's tries after a failure, in place of SIGN_IN_RETRY_MINUTES. */
    signInRetryMs?: readonly number[];
    /** Tests only: stands in for the retention sweep. */
    sweep?: typeof retentionSweep;
    /** Tests only: stands in for the sweep of free scans' connections. */
    scanSweep?: typeof sweepScanConnections;
    /** Tests only: stands in for the org-word store's fill (DECISIONS.md 666). */
    storeFill?: () => StoreFill;
    /** Tests only: stands in for process.exit, which the recycle's watchdog calls. */
    exit?: (code: number) => void;
    /** Tests only: stands in for RECYCLE_WATCHDOG_MS. */
    watchdogMs?: number;
  } = {},
): Promise<number> {
  const log = options.log ?? ((line: string) => console.log(`${new Date().toISOString()} ${line}`));
  const read = readWorkerEnv(env);
  if (!read.ok) {
    console.error(`hc worker won't start:\n${read.problems.map((p) => `  ${p}`).join("\n")}`);
    return 2;
  }
  const { config } = read;
  const { bull, pg } = await (options.load ?? loadWorkerModules)();

  // With federation the SDK exchanges Render's identity token itself and refreshes its own access token, kept on the disk so a
  // process that starts again in this instance doesn't exchange an identity token the last one already used (DECISIONS.md 667).
  const signInFacts = config.claude.kind === "federation" ? () => identityTokenFacts(env) : () => "";
  const watch = new ClaudeWatch(log, signInFacts);
  const signInDir = join(dirname(config.runsDir), "claude-sign-in");
  const narrative =
    config.claude.kind === "key"
      ? anthropicClientWithKey(config.claude.apiKey, watch.observe)
      : config.claude.kind === "federation"
        ? federationClient(env, prepareSignInCache(signInDir, federationCachePath(env, signInDir), log), { observe: watch.observe, ...(options.claudeFetch ? { fetch: options.claudeFetch } : {}) })
        : undefined;
  // One more for the heavy slot (DECISIONS.md 371).
  const pool = new pg.Pool({ connectionString: config.databaseUrl, max: config.concurrency + 3 });
  const store = new PostgresWorkerStore(pool, { log });
  const locksDir = join(dirname(config.runsDir), `${basename(config.runsDir)}-locks`);
  // Only for a free scan, queued the moment its prospect connects.
  const queue = new bull.Queue(HEALTH_CHECK_QUEUE, { connection: redisConnection(config.redisUrl) });
  // One client for the documents and for sharing their folder (DECISIONS.md 251).
  const google = config.drive ? new GoogleDrive(config.drive) : null;
  const deps: WorkerDeps = {
    store,
    tokens: new TokenStore({ dir: config.connectionsDir, key: tokenKeySource({ env: { HC_TOKEN_KEY: env.HC_TOKEN_KEY } as NodeJS.ProcessEnv, platform: "linux" }) }),
    runsDir: config.runsDir,
    // Beside the runs folder, not in it, so `hc purge --older-than` doesn't trip over it.
    locksDir,
    oauthClient: config.oauthClient,
    signinClient: config.signinClient,
    connectKey: config.connectKey,
    stopAtShare: config.stopAtShare,
    apiVersion: config.apiVersion,
    drive: google,
    sharing: google,
    micrositeKey: config.micrositeKey,
    apexSummaries: config.apexSummaries,
    queueRun: runQueuer(queue),
    narrative,
    log,
  };
  const handler = createJobHandler(deps);
  // A scan's run BullMQ gave up on lets go of its connection, under the run's lock, unless a delivery holds it and will (DECISIONS.md 652).
  const letGoLost = (runId: string): Promise<void> =>
    withJobLock(locksDir, `run-${runId}`, () => letGoOfLostRun(runId, deps)).catch((err: unknown) => {
      if (!(err instanceof JobBusyError)) log(`Run ${runId}'s connection couldn't be checked after BullMQ gave it up; the scan sweep will.`);
    });
  // Only with HC_WORKER_RECYCLE_AT set: resolves when a job ends with the org-word store at it, or full (DECISIONS.md 666).
  const recycle = config.recycleAt === null ? null : recycleWatch(config.recycleAt, options.storeFill);
  const worker = startQueueWorker(bull, handler, { redisUrl: config.redisUrl, concurrency: config.concurrency, log, onLost: lostRecorder(store, undefined, letGoLost), afterJob: recycle?.check });
  log(`Taking jobs from health-check-jobs, ${config.concurrency} at a time, with runs and documents one at a time in ${HEAVY_QUEUE}.`);
  if (!config.drive) log("Google Drive isn't set up, so released runs' documents can't be made.");
  if (!config.micrositeKey) log("HC_MICROSITE_KEY isn't set, so released runs get documents but no customer microsite.");
  log(
    config.recycleAt === null
      ? "Recycling is off: HC_WORKER_RECYCLE_AT isn't set, so once the org-word store is full, marks are unknown until a restart."
      : `Recycling is on: at ${config.recycleAt} org-worded sentences, the worker finishes the jobs in hand and exits ${RECYCLE_EXIT} for the host to restart it.`,
  );
  // Said at every start, so a deploy that turns it on or off shows in the log.
  log(
    !config.apexSummaries
      ? "Claude's summaries of Apex code are off: the switch isn't on."
      : narrative
        ? "Claude's summaries of Apex code are on: each health check keeps its Apex code until Claude has summarised it."
        : "Claude's summaries of Apex code are switched on, but Claude isn't set up, so no code is kept and nothing is asked.",
  );
  log(
    config.claude.kind === "key"
      ? "Claude drafts each run's report sections, signed in with ANTHROPIC_API_KEY."
      : config.claude.kind === "federation"
        ? "Claude drafts each run's report sections, signed in through OIDC federation."
        : `${config.claude.reason}, so report sections are the engine's own text.`,
  );
  if (config.claude.kind === "federation") log(`Claude by OIDC federation: ${signInFacts()}`);
  // One small call, so the log says now whether Claude answers. It never holds up the queue. A failure is tried
  // again for about an hour, so a sign-in that comes back shows in the log; every run asks Claude meanwhile anyway.
  let stopped = false;
  let signInRetry: ReturnType<typeof setTimeout> | undefined;
  if (narrative) {
    const check = options.claudeCheck ?? ((client: NarrativeClient) => checkClaude(client, MODELS.synthesis));
    const waits = options.signInRetryMs ?? SIGN_IN_RETRY_MINUTES.map((m) => m * 60_000);
    // A failure that isn't about sign-in (a busy API, a network error) is said as what it was, and recovers as an answer, not a sign-in.
    let refusedSignIn = false;
    const step = async (attempt: number): Promise<void> => {
      try {
        await tryOnce(attempt);
      } finally {
        // The startup line has been said (or the check threw), so the watch says what changes from now on.
        if (attempt === 0) watch.begin();
      }
    };
    const tryOnce = async (attempt: number): Promise<void> => {
      const result = await check(narrative);
      if (stopped) return;
      const signIn = !result.ok && result.signIn !== false;
      if (attempt === 0) {
        log(
          result.ok
            ? `Claude sign-in works: ${result.detail}.`
            : signIn
              ? `Claude sign-in failed at start, so runs use the engine's own text until it works. The worker tries again for about an hour and says when it does. ${result.detail} ${signInFacts()}`.trim()
              : `Claude didn't answer the startup check, and it wasn't refused sign-in: ${result.detail}. Runs still ask Claude; the worker tries again for about an hour and says when it answers.`,
        );
      }
      if (result.ok) {
        watch.note(true, result.detail);
        // After a refused sign-in, the watch has said "works again".
        if (attempt > 0 && !refusedSignIn) log(`Claude answered the startup check on a later try: ${result.detail}.`);
        return;
      }
      if (signIn) {
        refusedSignIn = true;
        watch.note(false, result.detail);
      }
      if (watch.up) return;
      if (attempt >= waits.length) {
        log("Claude's startup check still fails after an hour of tries. Every run still asks Claude, and the log says when sign-in works again.");
        return;
      }
      signInRetry = setTimeout(() => tryClaude(attempt + 1), waits[attempt]);
      signInRetry.unref();
    };
    // A throwing check or log line never becomes an unhandled rejection.
    const tryClaude = (attempt: number): void => void step(attempt).catch(() => {});
    tryClaude(0);
  }
  // Retention (decision 16): once now, then daily. A failed sweep is logged and tried again tomorrow.
  const sweep = (): void =>
    void (options.sweep ?? retentionSweep)({ store, runsDir: config.runsDir, locksDir, dryRun: config.cleanupDryRun, log, sharing: google }).catch((err: unknown) =>
      log(`Retention sweep failed, and will run again in a day: ${err instanceof Error ? err.message.slice(0, 200) : "unknown error"}`),
    );
  sweep();
  const sweeping = setInterval(sweep, SWEEP_INTERVAL_MS);
  sweeping.unref();
  // Free scans' connections that slipped past their run job (DECISIONS.md 652): once now, then hourly.
  const scanSweepState = { cursor: 0 };
  const scanSweep = (): void =>
    void (options.scanSweep ?? sweepScanConnections)(deps, scanSweepState).catch((err: unknown) =>
      log(`Scan sweep failed, and will run again in an hour: ${err instanceof Error ? err.name : "unknown error"}`),
    );
  scanSweep();
  const scanSweeping = setInterval(scanSweep, SCAN_SWEEP_INTERVAL_MS);
  scanSweeping.unref();
  const signalled = (options.until ?? untilSignalled()).then(() => null);
  const stop = await (recycle ? Promise.race([signalled, recycle.due]) : signalled);
  stopped = true;
  clearTimeout(signInRetry);
  clearInterval(sweeping);
  clearInterval(scanSweeping);
  if (!stop) {
    log("Stopping: finishing the jobs in hand.");
    await worker.close();
    await queue.close();
    await pool.end();
    return 0;
  }
  // Counts only, never org text. The heavy worker closes first, taking no new run, while connect and share jobs carry on.
  log(`Recycling: the org-word store is at ${stop.size} of ${stop.limit}. Finishing the jobs in hand; the host starts a fresh worker.`);
  const began = Date.now();
  const watchdogMs = options.watchdogMs ?? RECYCLE_WATCHDOG_MS;
  // close() waits without limit for the jobs in hand, so a stuck one would hold a worker that takes no runs. Unref'd: it never keeps a finished process alive.
  const watchdog = setTimeout(() => {
    log(`Recycling: the jobs in hand hadn't finished after ${Math.round(watchdogMs / 60_000)} minutes, so the worker exits now. A run cut short resumes from its checkpoints.`);
    (options.exit ?? process.exit)(RECYCLE_EXIT);
  }, watchdogMs);
  watchdog.unref();
  await worker.closeHeavyFirst();
  await queue.close();
  await pool.end();
  clearTimeout(watchdog);
  log(`Recycled: the jobs in hand finished and the worker closed in ${Math.round((Date.now() - began) / 1000)} seconds.`);
  return RECYCLE_EXIT;
}

/**
 * `hc worker` itself. A worker that recycled itself exits at once, so nothing
 * left open can keep alive a process that takes no jobs; the host starts a fresh
 * one (DECISIONS.md 666). Any other code is left for the CLI to set.
 */
export async function runWorker(env: NodeJS.ProcessEnv = process.env, options: Parameters<typeof workerCommand>[1] = {}): Promise<number> {
  const code = await workerCommand(env, options);
  if (code === RECYCLE_EXIT) (options.exit ?? process.exit)(code);
  return code;
}
