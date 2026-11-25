import type { JobHandler } from "./worker.ts";
import { BadJobError, type WorkerStore } from "./types.ts";

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
  Worker: new (name: string, processor: (job: QueueJob) => Promise<unknown>, options: { connection: RedisConnection; concurrency: number; lockDuration?: number; maxStalledCount?: number }) => QueueWorker;
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
export function processorFor(
  handler: JobHandler,
  bull: Pick<BullModule, "UnrecoverableError">,
  log: (line: string) => void,
): (job: QueueJob) => Promise<unknown> {
  return async (job) => {
    const final = job.attemptsMade + 1 >= (job.opts.attempts ?? 1);
    try {
      // The job's own id goes with it: "Run again" is known by its id (DECISIONS.md 698). The heavy queue keeps the console's id.
      const result = await handler(job.data, { final, ...(job.id ? { jobId: job.id } : {}) });
      // The org-word store is each run's own now (DECISIONS.md 743, 749), freed when the run ends, so the process's says nothing here.
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
/**
 * How often a heavy job may be put back after its process died (a deploy, a
 * crash, running out of memory) before BullMQ gives up on it. A run resumes
 * from its checkpoints, so trying again is cheap: two deploys during one long
 * run shouldn't lose it (DECISIONS.md 396). BullMQ's default is 1.
 */
export const HEAVY_MAX_STALLS = 2;
/** How long a finished heavy job, and so its id, is kept: a day, in seconds, as BullMQ's KeepJobs takes it. */
export const HEAVY_KEEP_DONE = { age: 86_400 };
/** BullMQ 5's reason for a job put back more than maxStalledCount times. It fails the job without running its handler. */
const STALLED_TOO_OFTEN = /stalled more than allowable limit/;
/** What the console shows for such a job. */
export const LOST_REASON = "The worker stopped part-way more than once, by a restart or by running out of memory, so this was given up. Queue it again: a run picks up from where it got to.";

/** What the console shows for a job whose last try threw before the handler could record it (DECISIONS.md 702). */
export const LAST_TRY_LOST_REASON = "The last try at this run stopped with an error before its result could be recorded. Run it again.";

/**
 * Whether a job that failed has no try left and its failure is recorded
 * nowhere (DECISIONS.md 702). BullMQ 5 raises "failed" after moveToFailed has
 * added the try just made to attemptsMade, so on the last try it equals the
 * job's attempts (an unset or 0 attempts is one try). A handler that ends on
 * its last try records the run itself and returns; one that throws there
 * (the store unreachable before the claim, a claim error, the job gone) has
 * recorded nothing, and the run would show in progress for ever. Not for an
 * UnrecoverableError, which is a job we couldn't read (BadJobError) or one
 * that stalled too often (recorded above), nor for JobBusyError: another job
 * holds the run and its own end is recorded, by its handler or here.
 */
export function lostForGood(job: Pick<QueueJob, "attemptsMade" | "opts">, err: Error): boolean {
  if (err.name === "UnrecoverableError" || err instanceof BadJobError || err.name === "JobBusyError") return false;
  return job.attemptsMade >= Math.max(job.opts.attempts ?? 1, 1);
}

const isHeavy = (data: unknown): boolean => typeof data === "object" && data !== null && HEAVY_KINDS.has(String((data as { kind?: unknown }).kind));
const isPortal = (data: unknown): boolean => isHeavy(data) && (data as { kind?: unknown; deliver?: unknown }).kind === "documents" && (data as { deliver?: unknown }).deliver === "portal";

/**
 * The one id the console adds a portal documents job under (DECISIONS.md 698):
 * BullMQ ignores an add whose id a job in the queue already holds, so a second
 * request while one is waiting or being made adds nothing. Handed on to the
 * heavy queue under the same id, and removed from it the moment it ends, done
 * or failed, so the next request makes them again and replaces the rows.
 */
export const portalDocumentsJobId = (runId: string): string => `documents-portal-${runId}`;

/**
 * The console's queue: a connect or share job runs here, and a heavy job is
 * handed to the heavy queue with the console's own options. Its id goes with
 * it, so a second delivery of the same job adds nothing: BullMQ keeps the one
 * it has.
 */
export function gatedProcessor(
  handler: JobHandler,
  bull: Pick<BullModule, "UnrecoverableError">,
  heavy: Pick<QueueSender, "add">,
  log: (line: string) => void,
): (job: QueueJob) => Promise<unknown> {
  const direct = processorFor(handler, bull, log);
  return async (job) => {
    if (!isHeavy(job.data)) return direct(job);
    const { attempts, backoff, priority, removeOnFail } = job.opts;
    // A finished heavy job is kept for a day, not removed at once as the console asks of its own jobs, so a
    // second delivery of the console's job after a crash in the hand-off finds it and adds nothing (DECISIONS.md 398).
    // A failed one is kept as the console asks, the last 200, which holds its id too.
    // A portal make keeps nothing once it ends (DECISIONS.md 698): a hand-off delivered twice makes them twice, which replaces the same rows.
    const portal = isPortal(job.data);
    const opts = Object.fromEntries(Object.entries({ attempts, backoff, priority, removeOnComplete: portal ? true : HEAVY_KEEP_DONE, removeOnFail: portal ? true : removeOnFail, jobId: job.id }).filter(([, v]) => v !== undefined));
    await heavy.add(job.name, job.data, opts);
    log(`${job.id ?? "?"} is waiting for the heavy slot`);
    return { outcome: "queued_heavy" };
  };
}

/**
 * Records a job BullMQ gave up on without running its handler (DECISIONS.md 396):
 * a run as failed, documents as not made, so the console says so and offers to
 * queue it again. Connect and share jobs have their own retries and the daily sweep.
 */
export function lostRecorder(
  store: Pick<WorkerStore, "recordRunFailure" | "failDocuments">,
  now: () => Date = () => new Date(),
  /** A free scan's run lets go of its connection, as every other end of one does (DECISIONS.md 652). */
  letGo?: (runId: string) => Promise<void>,
): (data: unknown, reason: string) => Promise<void> {
  return async (data, reason) => {
    const job = data as { kind?: unknown; runId?: unknown; deliver?: unknown };
    if (typeof job?.runId !== "string") return;
    if (job.kind === "run") {
      const runId = job.runId;
      try {
        await store.recordRunFailure(runId, reason, now().toISOString());
      } finally {
        if (letGo) await letGo(runId);
      }
    } else if (job.kind === "documents" && job.deliver !== "portal") {
      // A portal make has no documents row to fail, and the Drive make's row isn't its to touch (DECISIONS.md 698).
      await store.failDocuments(job.runId, reason, now().toISOString());
    }
  };
}

/**
 * Both workers. `close`, on SIGTERM: the console's first, so nothing more is
 * handed on, then the heavy one, which finishes the job in hand. (The recycle's
 * heavy-first close went with the recycle, DECISIONS.md 749.)
 */
export type GatedWorkers = { light: QueueWorker; heavy: QueueWorker; close(): Promise<void> };

export function startQueueWorker(
  bull: BullModule,
  handler: JobHandler,
  options: {
    redisUrl: string;
    concurrency: number;
    log: (line: string) => void;
    /** Records a heavy job BullMQ gave up on without running its handler, so it isn't left in progress for ever (DECISIONS.md 396). */
    onLost?: (data: unknown, reason: string) => Promise<void>;
  },
): GatedWorkers {
  const connection = redisConnection(options.redisUrl);
  const heavyQueue = new bull.Queue(HEAVY_QUEUE, { connection });
  const light = new bull.Worker(HEALTH_CHECK_QUEUE, gatedProcessor(handler, bull, heavyQueue, options.log), { connection, concurrency: options.concurrency });
  const heavy = new bull.Worker(HEAVY_QUEUE, processorFor(handler, bull, options.log), { connection, concurrency: 1, lockDuration: HEAVY_LOCK_MS, maxStalledCount: HEAVY_MAX_STALLS });
  for (const worker of [light, heavy]) {
    worker.on("failed", (job, err) => {
      options.log(`${job?.id ?? "?"} failed (attempt ${(job?.attemptsMade ?? 0)} of ${job?.opts.attempts ?? 1}): ${err.message}`);
      if (!job || !options.onLost) return;
      // One BullMQ raised without running the handler: nobody has recorded it.
      const reason = STALLED_TOO_OFTEN.test(err.message) ? LOST_REASON : lostForGood(job, err) ? LAST_TRY_LOST_REASON : null;
      if (reason) void options.onLost(job.data, reason).catch((e: unknown) => options.log(`${job.id ?? "?"} couldn't be recorded as failed: ${e instanceof Error ? e.message : "unknown error"}`));
    });
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
export function runQueuer(queue: Pick<QueueSender, "add">, now: () => number = Date.now): (runId: string, options?: { automatic?: boolean }) => Promise<void> {
  return async (runId, options = {}) => {
    // An automatic run's job has one fixed id (DECISIONS.md 693): BullMQ ignores an add whose id a job in the
    // queue already holds (waiting, active, delayed, or failed and kept), so a second message is never a second run.
    // A scan's id is as it was.
    const jobId = options.automatic ? automaticRunJobId(runId) : `run-${runId}-${now()}`;
    await queue.add("health-check-run", { kind: "run", runId }, { attempts: 3, backoff: { type: "exponential", delay: 30_000 }, removeOnComplete: true, removeOnFail: 200, priority: 10, jobId });
  };
}

/** The one queue id an automatic run's job is added under by the worker (DECISIONS.md 693). */
export const automaticRunJobId = (runId: string): string => `run-${runId}`;
