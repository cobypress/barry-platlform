import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import { HEALTH_CHECK_QUEUE, HEAVY_QUEUE, type BullModule, type QueueJob, type QueueWorker } from "../bullmq.ts";
import { readWorkerEnv } from "../env.ts";
import { expectRunStores, org, said, withRunOrgWords } from "../../engine/org-words.ts";
import { temp } from "./helpers.ts";

// The worker no longer recycles itself (DECISIONS.md 749, replacing 666): each
// health check keeps its own store of org-worded sentences, freed when it ends
// (743), so the store a recycle watched never fills. HC_WORKER_RECYCLE_AT, set on
// the service, is accepted whatever its value and ignored, with a line at start
// saying so; and a sentence with the org's words made outside any check's store,
// which would mean the worker is sharing one again, is said once.

const T = { timeout: 10_000 };

class Unrecoverable extends Error {}

/** BullMQ's Worker as far as the command can tell, with BullMQ's close: it takes nothing more at once, then waits for the jobs in hand. */
class FakeWorker implements QueueWorker {
  static made: FakeWorker[] = [];
  static events: string[] = [];
  readonly name: string;
  readonly processor: (job: QueueJob) => Promise<unknown>;
  readonly active = new Set<Promise<unknown>>();
  closing = false;
  closed = false;
  constructor(name: string, processor: (job: QueueJob) => Promise<unknown>) {
    this.name = name;
    this.processor = processor;
    FakeWorker.made.push(this);
  }
  on(): this {
    return this;
  }
  /** As BullMQ would hand it a job from Redis: null once it is closing, since a closing worker fetches nothing. */
  take(job: QueueJob): Promise<unknown> | null {
    if (this.closing) return null;
    const running = this.processor(job).catch((err: unknown) => err);
    this.active.add(running);
    void running.finally(() => this.active.delete(running));
    return running;
  }
  /** A job held in hand until `finish` is called, as a long run would be. */
  hold(id: string): { finish: () => void } {
    let finish!: () => void;
    const held = new Promise<void>((r) => (finish = r)).then(() => void FakeWorker.events.push(`${id} finished`));
    this.active.add(held);
    void held.finally(() => this.active.delete(held));
    return { finish };
  }
  async close(): Promise<void> {
    this.closing = true;
    FakeWorker.events.push(`${this.name} closing`);
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

/** `undefined` leaves HC_WORKER_RECYCLE_AT unset. */
const env = (recycleAt: string | undefined): NodeJS.ProcessEnv => ({
  REDIS_URL: "redis://localhost:6379",
  HC_WORKER_DATABASE_URL: "postgres://worker:pw@db.example.com/console",
  HC_CONNECT_KEY: randomBytes(32).toString("base64"),
  HC_TOKEN_KEY: randomBytes(32).toString("base64"),
  HC_CONNECTIONS_DIR: `${temp()}/connections`,
  HC_RUNS_DIR: `${temp()}/runs`,
  HC_SF_CLIENT_ID: "3MVG9fake.client.id",
  HC_SF_CLIENT_SECRET: "fake-client-secret",
  HC_SF_REDIRECT_URI: "https://console.example.com/connect/callback",
  ...(recycleAt === undefined ? {} : { HC_WORKER_RECYCLE_AT: recycleAt }),
});

/** A job whose payload the handler refuses at once: enough to end a job and run the check after it. */
const job = (id: string): QueueJob => ({ id, name: "health-check-step", data: { kind: "nonsense" }, attemptsMade: 0, opts: { attempts: 3 } });

const tick = async (n = 20) => {
  for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r));
};

/** Starts the command, and hands back what a test drives. */
function start(recycleAt: string | undefined) {
  FakeWorker.made = [];
  const lines: string[] = [];
  let signal!: () => void;
  const until = new Promise<void>((r) => (signal = r));
  const running = workerCommand(env(recycleAt), { load: async () => modules, until, log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never });
  const workers = async (): Promise<{ light: FakeWorker; heavy: FakeWorker }> => {
    while (!FakeWorker.made.some((w) => w.name === HEAVY_QUEUE)) await new Promise((r) => setImmediate(r));
    return { light: FakeWorker.made.find((w) => w.name === HEALTH_CHECK_QUEUE)!, heavy: FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)! };
  };
  return { lines, signal, running, workers };
}

describe("HC_WORKER_RECYCLE_AT, no longer used (DECISIONS.md 749)", () => {
  test("is accepted whatever its value, so a service that sets it still starts", T, () => {
    for (const value of [undefined, "", "300", "50000", "abc", "1e3", "-1"]) {
      const read = readWorkerEnv(env(value));
      assert.ok(read.ok, `${value}: ${read.ok ? "" : read.problems.join("; ")}`);
      assert.equal(read.ok && read.config.recycleAtIgnored, Boolean(value), String(value));
    }
  });

  test("set, the start says it is ignored, nothing recycles after a job, and SIGTERM stops it with 0", T, async () => {
    const w = start("300");
    const { heavy } = await w.workers();
    await heavy.take(job("j1"));
    w.signal();
    assert.equal(await w.running, 0);
    assert.ok(w.lines.includes("HC_WORKER_RECYCLE_AT is set but no longer used: each health check keeps its own org-word store, freed when it ends, so the worker never recycles itself. It can be removed from the service."), w.lines.join("\n"));
    assert.ok(!w.lines.some((l) => /Recycl|org-word store \d/.test(l)), w.lines.join("\n"));
  });

  test("unset, nothing is said of it", T, async () => {
    const w = start(undefined);
    await w.workers();
    w.signal();
    assert.equal(await w.running, 0);
    assert.ok(!w.lines.some((l) => /RECYCLE|Recycl/.test(l)), w.lines.join("\n"));
  });
});

describe("a sentence with the org's words made outside a check's own store, in the worker (DECISIONS.md 749)", () => {
  test("is said once, with no org text and no id; inside a check's store nothing is said", T, () => {
    const lines: string[] = [];
    expectRunStores((l) => lines.push(l));
    withRunOrgWords(() => said`Stage ${org("Secret Stage Name")} is held`);
    assert.deepEqual(lines, []);
    said`Stage ${org("Secret Stage Name")} is held`;
    said`Stage ${org("Another Secret")} is held`;
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /outside any health check's own org-word store/);
    assert.ok(!/Secret/.test(lines[0]!));
  });
});
