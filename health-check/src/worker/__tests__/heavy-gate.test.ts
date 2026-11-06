import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { HEALTH_CHECK_QUEUE, HEAVY_LOCK_MS, HEAVY_QUEUE, startQueueWorker, type BullModule, type QueueJob, type QueueWorker } from "../bullmq.ts";
import type { JobHandler } from "../worker.ts";

// The heavy gate (DECISIONS.md 371): runs and documents one at a time, in a
// queue of their own, while connect and share jobs keep the console's queue
// moving. A small in-memory broker stands in for BullMQ and Redis: each
// worker takes the waiting job with the lowest priority number, up to its
// concurrency, and a job that throws goes back with one more attempt made.

type Held = QueueJob & { queue: string };

class Broker {
  readonly waiting = new Map<string, Held[]>();
  readonly known = new Set<string>();
  readonly workers: Array<{ queue: string; processor: (job: QueueJob) => Promise<unknown>; concurrency: number; lockDuration?: number; active: number; closed: boolean }> = [];
  readonly failed: string[] = [];
  #next = 0;

  add(queue: string, name: string, data: unknown, opts: Record<string, unknown>): void {
    const id = (opts.jobId as string | undefined) ?? `auto-${++this.#next}`;
    // As BullMQ: a job id it already has adds nothing.
    if (this.known.has(`${queue}/${id}`)) return;
    this.known.add(`${queue}/${id}`);
    const list = this.waiting.get(queue) ?? [];
    list.push({ queue, id, name, data, attemptsMade: 0, opts: opts as QueueJob["opts"] });
    this.waiting.set(queue, list);
    this.pump();
  }

  pump(): void {
    for (const worker of this.workers) {
      const list = this.waiting.get(worker.queue) ?? [];
      while (!worker.closed && worker.active < worker.concurrency && list.length) {
        list.sort((a, b) => (a.opts.priority ?? 0) - (b.opts.priority ?? 0));
        const job = list.shift()!;
        worker.active++;
        void worker.processor(job).then(
          () => {
            worker.active--;
            this.pump();
          },
          () => {
            worker.active--;
            if (job.attemptsMade + 1 < (job.opts.attempts ?? 1)) list.push({ ...job, attemptsMade: job.attemptsMade + 1 });
            else this.failed.push(job.id!);
            this.pump();
          },
        );
      }
    }
  }
}

function fakeBull(broker: Broker): BullModule {
  class Queue {
    readonly name: string;
    constructor(name: string) {
      this.name = name;
    }
    async add(name: string, data: unknown, opts: Record<string, unknown>) {
      broker.add(this.name, name, data, opts);
    }
    async close() {}
  }
  class Worker implements QueueWorker {
    readonly entry: Broker["workers"][number];
    constructor(name: string, processor: (job: QueueJob) => Promise<unknown>, options: { concurrency: number; lockDuration?: number }) {
      this.entry = { queue: name, processor, concurrency: options.concurrency, lockDuration: options.lockDuration, active: 0, closed: false };
      broker.workers.push(this.entry);
      broker.pump();
    }
    on(): this {
      return this;
    }
    async close() {
      this.entry.closed = true;
    }
  }
  return { Queue: Queue as unknown as BullModule["Queue"], Worker: Worker as unknown as BullModule["Worker"], UnrecoverableError: Error };
}

/** A handler whose runs hold until released, and whose connect and share jobs finish at once. */
function handler(events: string[]) {
  const release = new Map<string, () => void>();
  const attempts = new Map<string, number>();
  const failFirst = new Set<string>();
  const run: JobHandler = async (data, attempt) => {
    const job = data as { kind: string; runId?: string; linkId?: string };
    const id = job.runId ?? job.linkId!;
    if (job.kind === "connect" || job.kind === "share") {
      events.push(`${job.kind} ${id} done`);
      return { outcome: "done", detail: "" };
    }
    attempts.set(id, (attempts.get(id) ?? 0) + 1);
    events.push(`${job.kind} ${id} start`);
    if (failFirst.delete(id)) {
      assert.equal(attempt.final, false);
      throw new Error("Salesforce timed out");
    }
    await new Promise<void>((resolve) => release.set(id, resolve));
    events.push(`${job.kind} ${id} done`);
    return { outcome: "done", detail: "" };
  };
  return { run, release, attempts, failFirst };
}

const tick = () => new Promise((r) => setImmediate(r));
const settle = async () => {
  for (let i = 0; i < 20; i++) await tick();
};
/** As the console enqueues them (blackcloud-next lib/health-check/jobs.ts). */
const CONSOLE = {
  run: (runId: string) => ["health-check-run", { kind: "run", runId }, { attempts: 3, backoff: { type: "exponential", delay: 30_000 }, removeOnComplete: true, removeOnFail: 200, priority: 10, jobId: `run-${runId}-1` }] as const,
  documents: (runId: string) => ["health-check-documents", { kind: "documents", runId }, { attempts: 2, priority: 5, jobId: `documents-${runId}-1` }] as const,
  connect: (linkId: string) => ["health-check-connect", { kind: "connect", linkId }, { attempts: 1, priority: 1, jobId: `connect-${linkId}-1` }] as const,
  share: (runId: string) => ["health-check-share", { kind: "share", runId }, { attempts: 3, priority: 3, jobId: `share-${runId}-1` }] as const,
};

function setup(concurrency = 2) {
  const broker = new Broker();
  const events: string[] = [];
  const h = handler(events);
  const workers = startQueueWorker(fakeBull(broker), h.run, { redisUrl: "redis://localhost:6379", concurrency, log: () => {} });
  const send = (job: readonly [string, unknown, Record<string, unknown>]) => broker.add(HEALTH_CHECK_QUEUE, job[0], job[1], job[2]);
  return { broker, events, h, workers, send };
}

describe("the heavy gate", () => {
  test("two runs and a connect queued together: the connect finishes first, and the second run starts only after the first", async () => {
    const { events, h, send } = setup();
    send(CONSOLE.run("run-a"));
    send(CONSOLE.run("run-b"));
    send(CONSOLE.connect("link-c"));
    await settle();
    assert.deepEqual(events, ["run run-a start", "connect link-c done"], "the connect didn't wait, and run-b hasn't started");
    h.release.get("run-a")!();
    await settle();
    h.release.get("run-b")!();
    await settle();
    assert.deepEqual(events, ["run run-a start", "connect link-c done", "run run-a done", "run run-b start", "run run-b done"]);
  });

  test("a heavy job waiting its turn holds no slot, so connect and share jobs keep flowing, however many runs are queued", async () => {
    const { events, h, send, broker } = setup(1);
    for (const id of ["r1", "r2", "r3"]) send(CONSOLE.run(id));
    send(CONSOLE.documents("r0"));
    for (const id of ["l1", "l2"]) send(CONSOLE.connect(id));
    send(CONSOLE.share("r0"));
    await settle();
    assert.deepEqual(events.filter((e) => !e.startsWith("run")).sort(), ["connect l1 done", "connect l2 done", "share r0 done"]);
    // One heavy job at a time, and the rest waiting in its queue, not in a slot.
    assert.equal(events.filter((e) => e.endsWith("start")).length, 1);
    assert.equal(broker.waiting.get(HEAVY_QUEUE)!.length, 3);
    // The first run handed over took the slot. Of those waiting, the documents go next, by the console's own priority: a consultant is waiting for them.
    assert.equal(events.find((e) => e.endsWith("start")), "run r1 start");
    h.release.get("r1")!();
    await settle();
    assert.equal(events.filter((e) => e.endsWith("start"))[1], "documents r0 start");
    for (const id of ["r0", "r2", "r3"]) {
      h.release.get(id)!();
      await settle();
    }
    assert.equal(events.filter((e) => e.endsWith("done")).length, 7);
  });

  test("waiting isn't an attempt: a run held behind another starts on its first attempt, and nothing fails", async () => {
    const { h, send, broker } = setup();
    send(CONSOLE.run("first"));
    send(CONSOLE.run("second"));
    await settle();
    h.release.get("first")!();
    await settle();
    h.release.get("second")!();
    await settle();
    assert.deepEqual([h.attempts.get("first"), h.attempts.get("second"), broker.failed], [1, 1, []]);
  });

  test("a heavy job keeps the console's options, so its retries and its last attempt work as before", async () => {
    const { h, send, broker } = setup();
    h.failFirst.add("flaky");
    send(CONSOLE.run("flaky"));
    await settle();
    const retried = broker.waiting.get(HEAVY_QUEUE)!;
    assert.equal(h.attempts.get("flaky"), 2, "retried in the heavy queue");
    assert.equal(retried.length, 0);
    h.release.get("flaky")!();
    await settle();
    assert.deepEqual(broker.failed, []);
  });

  test("a second delivery of the same heavy job adds nothing: the heavy queue keeps the one it has", async () => {
    const { h, send, events } = setup();
    send(CONSOLE.run("once"));
    await settle();
    // As if the console's queue handed the job over again after a crash, before marking it done.
    send(["health-check-run", { kind: "run", runId: "once" }, { jobId: "run-once-1", attempts: 3, priority: 10 }]);
    await settle();
    h.release.get("once")!();
    await settle();
    assert.deepEqual(events, ["run once start", "run once done"]);
  });

  test("the heavy slot's lock outlasts a long analysis, so a healthy run isn't taken for a dead one, and closing stops both workers", async () => {
    const { broker, workers } = setup();
    const heavy = broker.workers.find((w) => w.queue === HEAVY_QUEUE)!;
    const light = broker.workers.find((w) => w.queue === HEALTH_CHECK_QUEUE)!;
    assert.deepEqual([heavy.concurrency, light.concurrency], [1, 2]);
    assert.ok(heavy.lockDuration! >= 5 * 60_000 && heavy.lockDuration === HEAVY_LOCK_MS);
    assert.equal(light.lockDuration, undefined, "the console's queue keeps BullMQ's default");
    await workers.close();
    assert.ok(heavy.closed && light.closed);
  });
});
