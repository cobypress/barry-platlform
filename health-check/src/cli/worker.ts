import { basename, dirname, join } from "node:path";
import { MODELS } from "../config.ts";
import { anthropicClient, anthropicClientWithKey, type NarrativeClient } from "../narrative/client.ts";
import { checkClaude } from "../narrative/guard.ts";
import { tokenKeySource, TokenStore } from "../salesforce/token-store.ts";
import { HEALTH_CHECK_QUEUE, HEAVY_QUEUE, lostRecorder, redisConnection, runQueuer, startQueueWorker, type BullModule } from "../worker/bullmq.ts";
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
It also needs bullmq and pg installed beside the engine.`;

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
    claudeCheck?: (client: NarrativeClient) => Promise<{ ok: boolean; detail: string }>;
    /** Tests only: stands in for the retention sweep. */
    sweep?: typeof retentionSweep;
    /** Tests only: stands in for the sweep of free scans' connections. */
    scanSweep?: typeof sweepScanConnections;
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

  // With no key, the SDK finds the federation variables and the identity token itself, and refreshes its own access token.
  const narrative = config.claude.kind === "key" ? anthropicClientWithKey(config.claude.apiKey) : config.claude.kind === "federation" ? anthropicClient() : undefined;
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
  const worker = startQueueWorker(bull, handler, { redisUrl: config.redisUrl, concurrency: config.concurrency, log, onLost: lostRecorder(store, undefined, letGoLost) });
  log(`Taking jobs from health-check-jobs, ${config.concurrency} at a time, with runs and documents one at a time in ${HEAVY_QUEUE}.`);
  if (!config.drive) log("Google Drive isn't set up, so released runs' documents can't be made.");
  if (!config.micrositeKey) log("HC_MICROSITE_KEY isn't set, so released runs get documents but no customer microsite.");
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
  // One small call, so the log says now whether Claude answers. It never holds up the queue.
  if (narrative) {
    void (options.claudeCheck ?? ((client) => checkClaude(client, MODELS.synthesis)))(narrative).then((check) =>
      log(check.ok ? `Claude sign-in works: ${check.detail}.` : `Claude sign-in failed, so runs will use the engine's own text until it's fixed. ${check.detail}`),
    );
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
  await (options.until ?? untilSignalled());
  clearInterval(sweeping);
  clearInterval(scanSweeping);
  log("Stopping: finishing the jobs in hand.");
  await worker.close();
  await queue.close();
  await pool.end();
  return 0;
}
