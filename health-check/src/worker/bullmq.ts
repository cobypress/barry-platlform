import type { JobHandler } from "./worker.ts";
import { BadJobError, type WorkerStore } from "./types.ts";
import { orgWordsStore } from "../engine/org-words.ts";

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
  /** Called after every job, done or failed, so the worker can recycle itself when the store fills (DECISIONS.md 666). It must not wait for anything. */
  afterJob?: () => void,
): (job: QueueJob) => Promise<unknown> {
  return async (job) => {
    const final = job.attemptsMade + 1 >= (job.opts.attempts ?? 1);
    try {
      const result = await handler(job.data, { final });
      // With how full the org-word store is, so a worker whose marks have degraded to unknown is seen (DECISIONS.md 663).
      const store = orgWordsStore();
      log(`${job.id ?? "?"} ${result.outcome}: ${result.detail} (org-word store ${store.size} of ${store.limit}${store.full ? ", full: marks are unknown until restart" : ""})`);
      return { outcome: result.outcome };
    } catch (err) {
      if (err instanceof BadJobError) throw new bull.UnrecoverableError(err.message);
      throw err;
    } finally {
      afterJob?.();
    }
  };
}

// Recycling the worker (DECISIONS.md 666, carrying out 663's recommendation). The
// org-word store only grows, and once full every mark falls back to unknown until
// the process restarts. In-process clearing isn't sound (663), so, when
// HC_WORKER_RECYCLE_AT is set, the worker stops itself once the store reaches it
// and the host starts a fresh process with an empty store. Off unless set: nobody
// has yet seen Render restart a worker that exits by itself, and a worker that
// stays down is worse than degraded marks. It can't loop: a fresh process starts
// at 0, and one run of the small synthetic org adds about 160 sentences (a repeat
// of the same org none), so half the bound is some 300 different orgs' runs away,
// fewer for big orgs.

/**
 * The exit code of a worker that recycled itself. Not 0: Render's docs say a
 * background worker should never exit unless sent SIGTERM, and call an exit 0
 * "exited successfully but early"; the one restart they show follows an exit
 * 137. They say nothing of what follows 0 or any other code, which is why
 * recycling is off until it has been watched on the service. 75 is EX_TEMPFAIL
 * (sysexits.h): try again. Distinct from the CLI's 2 and 3, and from Render's own
 * 2 for a failed start command.
 */
export const RECYCLE_EXIT = 75;
/**
 * How long a recycle may wait for the jobs in hand before the worker exits
 * anyway: an hour, six heavy locks. close() waits without limit, and while it
 * waits the heavy slot takes nothing. A run cut short is found stalled once its
 * lock lapses and resumes from its checkpoints in the fresh worker; it may stall
 * twice (HEAVY_MAX_STALLS) before BullMQ gives up on it.
 */
export const RECYCLE_WATCHDOG_MS = 6 * 10 * 60_000;

export type StoreFill = { size: number; limit: number; full: boolean };
/** Whether a store this full is time to recycle: at `at`, or full however it got there. */
export const shouldRecycle = (fill: StoreFill, at: number): boolean => fill.full || fill.size >= at;

/**
 * A promise that resolves, once, with the store's fill when a job ends with the
 * store at `at` or full. `check` only resolves the promise: it never waits on the
 * worker, so a check from inside a job's own processor returns at once, and the
 * job ends before anyone closes the worker.
 */
export function recycleWatch(at: number, fill: () => StoreFill = orgWordsStore): { check: () => void; due: Promise<StoreFill> } {
  let resolve!: (fill: StoreFill) => void;
  const due = new Promise<StoreFill>((r) => (resolve = r));
  let done = false;
  return {
    due,
    check() {
      if (done) return;
      const now = fill();
      if (!shouldRecycle(now, at)) return;
      done = true;
      resolve({ ...now });
    },
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
  afterJob?: () => void,
): (job: QueueJob) => Promise<unknown> {
  const direct = processorFor(handler, bull, log, afterJob);
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
 * handed on, then the heavy one, which finishes the job in hand. `closeHeavyFirst`,
 * on a recycle (DECISIONS.md 666): the heavy one first, so it takes no new run and
 * finishes the one in hand while the console's keeps serving connect and share
 * jobs (decision 371) and hands heavy jobs on to Redis for the fresh worker; then
 * the console's. SIGTERM keeps its order: Render's default grace is 30 seconds,
 * so the process is killed long before a run ends either way, and the console's
 * jobs are the ones that can finish.
 */
export type GatedWorkers = { light: QueueWorker; heavy: QueueWorker; close(): Promise<void>; closeHeavyFirst(): Promise<void> };

export function startQueueWorker(
  bull: BullModule,
  handler: JobHandler,
  options: {
    redisUrl: string;
    concurrency: number;
    log: (line: string) => void;
    /** Records a heavy job BullMQ gave up on without running its handler, so it isn't left in progress for ever (DECISIONS.md 396). */
    onLost?: (data: unknown, reason: string) => Promise<void>;
    /** Called after every job either worker runs (DECISIONS.md 666). */
    afterJob?: () => void;
  },
): GatedWorkers {
  const connection = redisConnection(options.redisUrl);
  const heavyQueue = new bull.Queue(HEAVY_QUEUE, { connection });
  const light = new bull.Worker(HEALTH_CHECK_QUEUE, gatedProcessor(handler, bull, heavyQueue, options.log, options.afterJob), { connection, concurrency: options.concurrency });
  const heavy = new bull.Worker(HEAVY_QUEUE, processorFor(handler, bull, options.log, options.afterJob), { connection, concurrency: 1, lockDuration: HEAVY_LOCK_MS, maxStalledCount: HEAVY_MAX_STALLS });
  for (const worker of [light, heavy]) {
    worker.on("failed", (job, err) => {
      options.log(`${job?.id ?? "?"} failed (attempt ${(job?.attemptsMade ?? 0)} of ${job?.opts.attempts ?? 1}): ${err.message}`);
      // A failure the handler raised, it has already recorded. One BullMQ raised without running it, nobody has.
      if (job && STALLED_TOO_OFTEN.test(err.message) && options.onLost) {
        void options.onLost(job.data, LOST_REASON).catch((e: unknown) => options.log(`${job.id ?? "?"} couldn't be recorded as failed: ${e instanceof Error ? e.message : "unknown error"}`));
      }
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
    async closeHeavyFirst() {
      await heavy.close();
      await light.close();
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
