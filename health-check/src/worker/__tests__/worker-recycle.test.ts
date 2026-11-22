import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import { HEAVY_QUEUE, processorFor, RECYCLE_AT, RECYCLE_EXIT, recycleWatch, shouldRecycle, type BullModule, type QueueJob, type QueueWorker, type StoreFill } from "../bullmq.ts";
import { org, orgWordsStore, said } from "../../engine/org-words.ts";
import { temp } from "./helpers.ts";

// The worker recycles itself once the org-word store is half full (DECISIONS.md 666):
// it stops by SIGTERM's graceful path and exits RECYCLE_EXIT, so the host starts a
// fresh process with an empty store. Its own file, since the last test fills this
// process's store, which stays filled.

class Unrecoverable extends Error {}

/** BullMQ's Worker as far as the command can tell, with BullMQ's close: it waits for the jobs in hand. */
class FakeWorker implements QueueWorker {
  static made: FakeWorker[] = [];
  static events: string[] = [];
  readonly name: string;
  readonly processor: (job: QueueJob) => Promise<unknown>;
  readonly active = new Set<Promise<unknown>>();
  closed = false;
  constructor(name: string, processor: (job: QueueJob) => Promise<unknown>) {
    this.name = name;
    this.processor = processor;
    FakeWorker.made.push(this);
  }
  on(): this {
    return this;
  }
  /** As BullMQ would: the processor's promise is a job in hand until it settles. */
  run(job: QueueJob): Promise<unknown> {
    const running = this.processor(job).catch((err: unknown) => err);
    this.active.add(running);
    void running.finally(() => this.active.delete(running));
    return running;
  }
  /** Held in hand until `finish` is called, as a long run would be. */
  hold(id: string): { finish: () => void } {
    let finish!: () => void;
    const held = new Promise<void>((r) => (finish = r)).then(() => void FakeWorker.events.push(`${id} finished`));
    this.active.add(held);
    void held.finally(() => this.active.delete(held));
    return { finish };
  }
  async close(): Promise<void> {
    await Promise.all([...this.active]);
    FakeWorker.events.push(`${this.name} closed`);
    this.closed = true;
  }
}
class FakeQueue {
  async add() {}
  async close() {}
}
const bull: BullModule = { Worker: FakeWorker as unknown as BullModule["Worker"], Queue: FakeQueue as unknown as BullModule["Queue"], UnrecoverableError: Unrecoverable };

class FakePool {
  static ended = 0;
  async query(): Promise<never> {
    throw new Error("no database in tests");
  }
  async connect(): Promise<never> {
    throw new Error("no database in tests");
  }
  async end(): Promise<void> {
    FakePool.ended++;
  }
}
const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };

const env = (): NodeJS.ProcessEnv => ({
  REDIS_URL: "redis://localhost:6379",
  HC_WORKER_DATABASE_URL: "postgres://worker:pw@db.example.com/console",
  HC_CONNECT_KEY: randomBytes(32).toString("base64"),
  HC_TOKEN_KEY: randomBytes(32).toString("base64"),
  HC_CONNECTIONS_DIR: `${temp()}/connections`,
  HC_RUNS_DIR: `${temp()}/runs`,
  HC_SF_CLIENT_ID: "3MVG9fake.client.id",
  HC_SF_CLIENT_SECRET: "fake-client-secret",
  HC_SF_REDIRECT_URI: "https://console.example.com/connect/callback",
});

/** A heavy job whose payload the handler refuses at once: enough to end a job and run the check after it. */
const job = (id: string): QueueJob => ({ id, name: "health-check-run", data: { kind: "nonsense" }, attemptsMade: 0, opts: { attempts: 3 } });

/** Starts the command with the store's fill stood in for, and hands back what a test drives. */
function start(fill: { current: StoreFill }) {
  FakeWorker.made = [];
  FakeWorker.events = [];
  FakePool.ended = 0;
  const lines: string[] = [];
  let signal!: () => void;
  const until = new Promise<void>((r) => (signal = r));
  let returned: number | undefined;
  const running = workerCommand(env(), { load: async () => modules, until, log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never, storeFill: () => fill.current }).then((code) => (returned = code));
  const heavy = async (): Promise<FakeWorker> => {
    while (!FakeWorker.made.some((w) => w.name === HEAVY_QUEUE)) await new Promise((r) => setImmediate(r));
    return FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)!;
  };
  const settle = async () => {
    for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r));
  };
  return { lines, signal, running, heavy, settle, returned: () => returned };
}

const LIMIT = 100_000;

describe("when the worker recycles itself", () => {
  test("the threshold is half the store's bound, and the exit code is neither 0 nor the CLI's own", () => {
    assert.equal(orgWordsStore().limit, LIMIT);
    assert.equal(RECYCLE_AT, LIMIT / 2);
    assert.ok(![0, 1, 2, 3].includes(RECYCLE_EXIT));
    assert.equal(shouldRecycle({ size: RECYCLE_AT - 1, limit: LIMIT, full: false }), false);
    assert.equal(shouldRecycle({ size: RECYCLE_AT, limit: LIMIT, full: false }), true);
    assert.equal(shouldRecycle({ size: 10, limit: LIMIT, full: true }), true, "a full store recycles however it got there");
  });

  test("a job ending with the store at the threshold stops the worker by the graceful path, and the command returns", async () => {
    const fill = { current: { size: 10, limit: LIMIT, full: false } };
    const w = start(fill);
    const heavy = await w.heavy();
    await heavy.run(job("j1"));
    fill.current = { size: RECYCLE_AT, limit: LIMIT, full: false };
    await heavy.run(job("j2"));
    assert.equal(await w.running, RECYCLE_EXIT);
    assert.deepEqual(FakeWorker.made.map((m) => m.closed), [true, true]);
    assert.equal(FakePool.ended, 1);
    const recycling = w.lines.filter((l) => l.startsWith("Recycling"));
    assert.deepEqual(recycling, [`Recycling: the org-word store is at ${RECYCLE_AT} of ${LIMIT}. Finishing the jobs in hand; the host starts a fresh worker.`]);
    assert.ok(!w.lines.includes("Stopping: finishing the jobs in hand."));
  });

  test("a store marked full recycles too, below the threshold", async () => {
    const fill = { current: { size: 7, limit: LIMIT, full: true } };
    const w = start(fill);
    await (await w.heavy()).run(job("j1"));
    assert.equal(await w.running, RECYCLE_EXIT);
  });

  test("below the threshold the worker keeps running, and SIGTERM still stops it with 0", async () => {
    const fill = { current: { size: RECYCLE_AT - 1, limit: LIMIT, full: false } };
    const w = start(fill);
    const heavy = await w.heavy();
    for (const id of ["j1", "j2", "j3"]) await heavy.run(job(id));
    await w.settle();
    assert.equal(w.returned(), undefined, "still running");
    assert.ok(!heavy.closed);
    w.signal();
    assert.equal(await w.running, 0);
    assert.ok(w.lines.includes("Stopping: finishing the jobs in hand."));
    assert.ok(!w.lines.some((l) => l.startsWith("Recycling")));
  });

  test("a job in hand finishes before the worker closes, and the command waits for it", async () => {
    const fill = { current: { size: 0, limit: LIMIT, full: false } };
    const w = start(fill);
    const heavy = await w.heavy();
    const light = FakeWorker.made.find((m) => m.name !== HEAVY_QUEUE)!;
    const long = light.hold("long job");
    fill.current = { size: RECYCLE_AT + 3, limit: LIMIT, full: false };
    await heavy.run(job("j1"));
    await w.settle();
    assert.equal(w.returned(), undefined, "waiting for the job in hand");
    assert.ok(!light.closed);
    long.finish();
    assert.equal(await w.running, RECYCLE_EXIT);
    assert.deepEqual(FakeWorker.events, ["long job finished", "health-check-jobs closed", "health-check-heavy closed"]);
  });
});

describe("the check after a job", () => {
  test("returns at once: a close started from it, waiting for that same job, doesn't deadlock", async () => {
    const watch = recycleWatch(1, () => ({ size: 1, limit: LIMIT, full: false }));
    let processing!: Promise<unknown>;
    let closed = false;
    // As workerCommand does on the promise: close, which waits for the job in hand.
    void watch.due.then(async () => {
      await processing;
      closed = true;
    });
    const process = processorFor(async () => ({ outcome: "done", detail: "" }), bull, () => {}, watch.check);
    processing = process(job("j1"));
    await processing;
    await new Promise((r) => setImmediate(r));
    assert.ok(closed);
  });

  test("resolves once, with counts only, and after a failed job as well", async () => {
    const fill = { current: { size: 0, limit: LIMIT, full: false } };
    const watch = recycleWatch(5, () => fill.current);
    let resolved = 0;
    void watch.due.then(() => resolved++);
    const failing = processorFor(async () => Promise.reject(new Error("Salesforce timed out")), bull, () => {}, watch.check);
    await assert.rejects(failing(job("j1")));
    await new Promise((r) => setImmediate(r));
    assert.equal(resolved, 0);
    fill.current = { size: 5, limit: LIMIT, full: false };
    await assert.rejects(failing(job("j2")));
    await assert.rejects(failing(job("j3")));
    await new Promise((r) => setImmediate(r));
    assert.equal(resolved, 1);
    assert.deepEqual(Object.keys(await watch.due).sort(), ["full", "limit", "size"]);
  });
});

describe("with the real store", () => {
  test("a fresh process starts under the threshold and doesn't recycle; one filled to it does, and the log holds no org text", async (t) => {
    t.mock.method(console, "warn", () => {});
    // A fresh process: whatever this file's tests added is far below the threshold, so a restart can't loop.
    assert.ok(orgWordsStore().size < RECYCLE_AT);
    const fresh = recycleWatch();
    let due = false;
    void fresh.due.then(() => (due = true));
    fresh.check();
    await new Promise((r) => setImmediate(r));
    assert.equal(due, false);

    const lines: string[] = [];
    FakeWorker.made = [];
    let signal!: () => void;
    const running = workerCommand(env(), { load: async () => modules, until: new Promise<void>((r) => (signal = r)), log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never });
    while (!FakeWorker.made.some((w) => w.name === HEAVY_QUEUE)) await new Promise((r) => setImmediate(r));
    const heavy = FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)!;
    await heavy.run(job("j1"));
    // Filled as the store's own test fills it, to the threshold and no further.
    for (let i = orgWordsStore().size; i < RECYCLE_AT; i++) said`Stage ${org(`Secret stage ${i}`)} is held`;
    assert.equal(orgWordsStore().size, RECYCLE_AT);
    await heavy.run(job("j2"));
    const timer = setTimeout(signal, 5_000);
    assert.equal(await running, RECYCLE_EXIT);
    clearTimeout(timer);
    const log = lines.join("\n");
    assert.match(log, new RegExp(`Recycling: the org-word store is at ${RECYCLE_AT} of ${LIMIT}\\.`));
    assert.ok(!log.includes("Secret"));
  });
});
