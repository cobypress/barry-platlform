import type { JobHandler } from "./worker.ts";
import { BadJobError } from "./types.ts";

// A thin adapter between BullMQ and the job handler, for the console's
// "health-check-jobs" queue (blackcloud-next lib/health-check/jobs.ts). BullMQ
// isn't a dependency of the engine: it's typed here by the little this file
// uses, and `hc worker` loads the real module at start. Tests use a fake.

export const HEALTH_CHECK_QUEUE = "health-check-jobs";

/** The parts of a BullMQ Job this adapter reads. */
export type QueueJob = { id?: string; name: string; data: unknown; attemptsMade: number; opts: { attempts?: number } };

export type RedisConnection = {
  host: string;
  port: number;
  username?: string;
  password?: string;
  db?: number;
  tls?: Record<string, never>;
  /** BullMQ requires null for a worker's connection, so a blocking wait isn't cut short. */
  maxRetriesPerRequest: null;
};

export interface QueueWorker {
  on(event: "failed", listener: (job: QueueJob | undefined, err: Error) => void): unknown;
  on(event: "error", listener: (err: Error) => void): unknown;
  close(): Promise<void>;
}

export type BullModule = {
  Worker: new (name: string, processor: (job: QueueJob) => Promise<unknown>, options: { connection: RedisConnection; concurrency: number }) => QueueWorker;
  /** Thrown to tell BullMQ not to retry. */
  UnrecoverableError: new (message?: string) => Error;
};

/** redis:// or rediss:// as ioredis options. Never echoes the URL, which holds the password. */
export function redisConnection(url: string): RedisConnection {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error("REDIS_URL isn't a valid URL");
  }
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") throw new Error("REDIS_URL must start redis:// or rediss://");
  const db = parsed.pathname.replace(/^\//, "");
  if (db && !/^\d+$/.test(db)) throw new Error("REDIS_URL's database must be a number");
  return {
    host: parsed.hostname,
    port: parsed.port ? Number(parsed.port) : 6379,
    ...(parsed.username ? { username: decodeURIComponent(parsed.username) } : {}),
    ...(parsed.password ? { password: decodeURIComponent(parsed.password) } : {}),
    ...(db ? { db: Number(db) } : {}),
    ...(parsed.protocol === "rediss:" ? { tls: {} } : {}),
    maxRetriesPerRequest: null,
  };
}

/**
 * BullMQ counts attemptsMade as attempts already finished, so while attempt n
 * runs it's n - 1. The last is the one with no retry after it. Only the
 * outcome goes back to Redis: details can name an org, and the queue holds ids only.
 */
export function processorFor(handler: JobHandler, bull: Pick<BullModule, "UnrecoverableError">, log: (line: string) => void): (job: QueueJob) => Promise<unknown> {
  return async (job) => {
    const final = job.attemptsMade + 1 >= (job.opts.attempts ?? 1);
    try {
      const result = await handler(job.data, { final });
      log(`${job.id ?? "?"} ${result.outcome}: ${result.detail}`);
      return { outcome: result.outcome };
    } catch (err) {
      if (err instanceof BadJobError) throw new bull.UnrecoverableError(err.message);
      throw err;
    }
  };
}

export function startQueueWorker(bull: BullModule, handler: JobHandler, options: { redisUrl: string; concurrency: number; log: (line: string) => void }): QueueWorker {
  const worker = new bull.Worker(HEALTH_CHECK_QUEUE, processorFor(handler, bull, options.log), { connection: redisConnection(options.redisUrl), concurrency: options.concurrency });
  worker.on("failed", (job, err) => options.log(`${job?.id ?? "?"} failed (attempt ${(job?.attemptsMade ?? 0)} of ${job?.opts.attempts ?? 1}): ${err.message}`));
  worker.on("error", (err) => options.log(`Queue error: ${err.message}`));
  return worker;
}
