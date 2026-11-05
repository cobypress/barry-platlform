import { basename, dirname, join } from "node:path";
import { MODELS } from "../config.ts";
import { anthropicClient, anthropicClientWithKey, type NarrativeClient } from "../narrative/client.ts";
import { checkClaude } from "../narrative/guard.ts";
import { tokenKeySource, TokenStore } from "../salesforce/token-store.ts";
import { startQueueWorker, type BullModule } from "../worker/bullmq.ts";
import { GoogleDrive } from "../worker/drive.ts";
import { readWorkerEnv, WORKER_ENV } from "../worker/env.ts";
import { PostgresWorkerStore, type SqlPool } from "../worker/postgres-store.ts";
import { createJobHandler } from "../worker/worker.ts";

// `hc worker`: takes run, connect and documents jobs from the console's queue until told
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
  const pool = new pg.Pool({ connectionString: config.databaseUrl, max: config.concurrency + 2 });
  const handler = createJobHandler({
    store: new PostgresWorkerStore(pool),
    tokens: new TokenStore({ dir: config.connectionsDir, key: tokenKeySource({ env: { HC_TOKEN_KEY: env.HC_TOKEN_KEY } as NodeJS.ProcessEnv, platform: "linux" }) }),
    runsDir: config.runsDir,
    // Beside the runs folder, not in it, so `hc purge --older-than` doesn't trip over it.
    locksDir: join(dirname(config.runsDir), `${basename(config.runsDir)}-locks`),
    oauthClient: config.oauthClient,
    connectKey: config.connectKey,
    stopAtShare: config.stopAtShare,
    apiVersion: config.apiVersion,
    drive: config.drive ? new GoogleDrive(config.drive) : null,
    narrative,
    log,
  });
  const worker = startQueueWorker(bull, handler, { redisUrl: config.redisUrl, concurrency: config.concurrency, log });
  log(`Taking jobs from health-check-jobs, ${config.concurrency} at a time.`);
  if (!config.drive) log("Google Drive isn't set up, so released runs' documents can't be made.");
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
  await (options.until ?? untilSignalled());
  log("Stopping: finishing the jobs in hand.");
  await worker.close();
  await pool.end();
  return 0;
}
