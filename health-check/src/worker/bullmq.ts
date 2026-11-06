import type { JobHandler } from "./worker.ts";
import { BadJobError } from "./types.ts";

// A thin adapter between BullMQ and the job handler, for the console's
// "health-check-jobs" queue (blackcloud-next lib/health-check/jobs.ts). BullMQ
// isn't a dependency of the engine: it's typed here by the little this file
// uses, and `hc worker` loads the real module at start. Tests use a fake.

export const HEALTH_CHECK_QUEUE = "health-check-jobs";

/** The parts of a BullMQ Job this adapter reads. */
export type QueueJob = { id?: string; name: string; data: unknown; attemptsMade: number; opts: { attempts?: number; backoff?: unknown; priority?: number; removeOnComplete?: unknown; removeOnFail?: unknown } };

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

export type QueueSender = { add(name: string, data: unknown, opts: Record<string, unknown>): Promise<unknown>; close(): Promise<void> };

export type BullModule = {
  Queue: new (name: string, options: { connection: RedisConnection }) => QueueSender;
  Worker: new (name: string, processor: (job: QueueJob) => Promise<unknown>, options: { connection: RedisConnection; concurrency: number; lockDuration?: number }) => QueueWorker;
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

// The heavy gate (DECISIONS.md 371). A run or a documents make holds a big
// org's records: about 1.5 GB at 500 users. Two at once wouldn't fit a 2 GB host,
// and a connect job mustn't wait behind one, since its code lasts minutes. So
// the console's queue is worked as before, but a heavy job is handed on to a
// queue of its own, which this process works one job at a time. The console
// enqueues as it always has. A heavy job waiting its turn sits in Redis, not in
// a slot, so it holds no lock, can't time out, and isn't counted as an attempt.
// It's one process, so there's one heap, not a second one for heavy jobs.

export const HEAVY_QUEUE = "health-check-heavy";
/** The jobs that read or print a whole org. Connect and share jobs are small, and keep the console's queue moving. */
export const HEAVY_KINDS: ReadonlySet<string> = new Set(["run", "documents"]);
/**
 * A heavy job's lock. Analysis runs for tens of seconds without yielding, so
 * BullMQ's 30-second default could find a healthy job stalled and hand it to
 * another worker. A job whose process died is found once this has passed.
 */
export const HEAVY_LOCK_MS = 10 * 60_000;

const isHeavy = (data: unknown): boolean => typeof data === "object" && data !== null && HEAVY_KINDS.has(String((data as { kind?: unknown }).kind));

/**
 * The console's queue: a connect or share job runs here, and a heavy job is
 * handed to the heavy queue with the console's own options. Its id goes with
 * it, so a second delivery of the same job adds nothing: BullMQ keeps the one
 * it has.
 */
export function gatedProcessor(handler: JobHandler, bull: Pick<BullModule, "UnrecoverableError">, heavy: Pick<QueueSender, "add">, log: (line: string) => void): (job: QueueJob) => Promise<unknown> {
  const direct = processorFor(handler, bull, log);
  return async (job) => {
    if (!isHeavy(job.data)) return direct(job);
    const { attempts, backoff, priority, removeOnComplete, removeOnFail } = job.opts;
    const opts = Object.fromEntries(Object.entries({ attempts, backoff, priority, removeOnComplete, removeOnFail, jobId: job.id }).filter(([, v]) => v !== undefined));
    await heavy.add(job.name, job.data, opts);
    log(`${job.id ?? "?"} is waiting for the heavy slot`);
    return { outcome: "queued_heavy" };
  };
}

/** Both workers, closed together: the console's first, so nothing more is handed on, then the heavy one, which finishes the job in hand. */
export type GatedWorkers = { light: QueueWorker; heavy: QueueWorker; close(): Promise<void> };

export function startQueueWorker(bull: BullModule, handler: JobHandler, options: { redisUrl: string; concurrency: number; log: (line: string) => void }): GatedWorkers {
  const connection = redisConnection(options.redisUrl);
  const heavyQueue = new bull.Queue(HEAVY_QUEUE, { connection });
  const light = new bull.Worker(HEALTH_CHECK_QUEUE, gatedProcessor(handler, bull, heavyQueue, options.log), { connection, concurrency: options.concurrency });
  const heavy = new bull.Worker(HEAVY_QUEUE, processorFor(handler, bull, options.log), { connection, concurrency: 1, lockDuration: HEAVY_LOCK_MS });
  for (const worker of [light, heavy]) {
    worker.on("failed", (job, err) => options.log(`${job?.id ?? "?"} failed (attempt ${(job?.attemptsMade ?? 0)} of ${job?.opts.attempts ?? 1}): ${err.message}`));
    worker.on("error", (err) => options.log(`Queue error: ${err.message}`));
  }
  return {
    light,
    heavy,
    async close() {
      await light.close();
      await heavy.close();
      await heavyQueue.close();
    },
  };
}

/**
 * A run job on the queue, with the console's own name and options (blackcloud-next
 * lib/health-check/jobs.ts), so the worker treats it like any run the console sends.
 */
export function runQueuer(queue: Pick<QueueSender, "add">, now: () => number = Date.now): (runId: string) => Promise<void> {
  return async (runId) => {
    await queue.add("health-check-run", { kind: "run", runId }, { attempts: 3, backoff: { type: "exponential", delay: 30_000 }, removeOnComplete: true, removeOnFail: 200, priority: 10, jobId: `run-${runId}-${now()}` });
  };
}
