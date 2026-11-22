import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { runWorker, workerCommand, type WorkerModules } from "../../cli/worker.ts";
import { HEALTH_CHECK_QUEUE, HEAVY_QUEUE, processorFor, RECYCLE_EXIT, RECYCLE_WATCHDOG_MS, recycleWatch, shouldRecycle, type BullModule, type QueueJob, type QueueWorker, type StoreFill } from "../bullmq.ts";
import { readWorkerEnv, RECYCLE_AT_LIMIT } from "../env.ts";
import { org, orgWordsStore, said } from "../../engine/org-words.ts";
import { temp } from "./helpers.ts";

// The worker recycles itself, when HC_WORKER_RECYCLE_AT is set, once the org-word
// store reaches it (DECISIONS.md 666): the heavy worker closes first while the
// console's keeps serving, then the rest, and it exits RECYCLE_EXIT so the host
// starts a fresh process with an empty store. Its own file, since the last test
// fills this process's store, which stays filled. Every test has a timeout: a
// broken recycle hangs rather than fails.

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

const LIMIT = 100_000;
const AT = 50_000;
const tick = async (n = 20) => {
  for (let i = 0; i < n; i++) await new Promise((r) => setImmediate(r));
};

/** Starts the command with the store's fill stood in for, and hands back what a test drives. */
function start(fill: { current: StoreFill }, opts: { recycleAt?: string | undefined; watchdogMs?: number; exit?: (code: number) => void; run?: typeof workerCommand } = {}) {
  FakeWorker.made = [];
  FakeWorker.events = [];
  FakePool.ended = 0;
  const lines: string[] = [];
  let signal!: () => void;
  const until = new Promise<void>((r) => (signal = r));
  let returned: number | undefined;
  const run = opts.run ?? workerCommand;
  const running = run(env("recycleAt" in opts ? opts.recycleAt : String(AT)), {
    load: async () => modules,
    until,
    log: (l) => lines.push(l),
    sweep: async () => [],
    scanSweep: (async () => {}) as never,
    storeFill: () => fill.current,
    exit: opts.exit ?? (() => assert.fail("exit wasn't expected")),
    watchdogMs: opts.watchdogMs,
  }).then((code) => (returned = code));
  const workers = async (): Promise<{ light: FakeWorker; heavy: FakeWorker }> => {
    while (!FakeWorker.made.some((w) => w.name === HEAVY_QUEUE)) await new Promise((r) => setImmediate(r));
    return { light: FakeWorker.made.find((w) => w.name === HEALTH_CHECK_QUEUE)!, heavy: FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)! };
  };
  return { lines, signal, running, workers, returned: () => returned };
}

describe("HC_WORKER_RECYCLE_AT", () => {
  test("unset is off; a whole number from 1 to half the store's bound is on; anything else refuses to start", T, () => {
    assert.equal(RECYCLE_AT_LIMIT, LIMIT / 2);
    const read = (value: string | undefined) => readWorkerEnv(env(value));
    const off = read(undefined);
    assert.ok(off.ok && off.config.recycleAt === null);
    const blank = read("  ");
    assert.ok(blank.ok && blank.config.recycleAt === null, "blank is unset, as for every other variable");
    for (const [value, at] of [["1", 1], ["300", 300], [" 50000 ", 50_000]] as const) {
      const on = read(value);
      assert.ok(on.ok && on.config.recycleAt === at, value);
    }
    for (const value of ["0", "50001", "1.5", "-3", "1e3", "0x10", "abc", "on"]) {
      const bad = read(value);
      assert.ok(!bad.ok, value);
      assert.deepEqual(!bad.ok && bad.problems, ["HC_WORKER_RECYCLE_AT must be a whole number from 1 to 50000, or unset"], value);
    }
  });

  test("off: a full store after a job doesn't recycle, the start says it is off, and SIGTERM stops it with 0", T, async () => {
    const w = start({ current: { size: LIMIT, limit: LIMIT, full: true } }, { recycleAt: undefined });
    const { light, heavy } = await w.workers();
    await heavy.take(job("j1"));
    await light.take(job("j2"));
    await tick();
    assert.equal(w.returned(), undefined, "still running");
    assert.ok(w.lines.some((l) => /^Recycling is off: HC_WORKER_RECYCLE_AT isn't set/.test(l)), w.lines.join("\n"));
    w.signal();
    assert.equal(await w.running, 0);
    assert.ok(!w.lines.some((l) => l.startsWith("Recycling:")));
  });

  test("on: the start says at what number", T, async () => {
    const w = start({ current: { size: 0, limit: LIMIT, full: false } }, { recycleAt: "300" });
    await w.workers();
    assert.ok(w.lines.includes(`Recycling is on: at 300 org-worded sentences, the worker finishes the jobs in hand and exits ${RECYCLE_EXIT} for the host to restart it.`), w.lines.join("\n"));
    w.signal();
    assert.equal(await w.running, 0);
  });

  test("an invalid value refuses to start, with exit code 2", T, async (t) => {
    const errors = t.mock.method(console, "error", () => {});
    assert.equal(await workerCommand(env("lots"), { load: async () => assert.fail("loaded BullMQ") }), 2);
    assert.match(String(errors.mock.calls[0]!.arguments[0]), /HC_WORKER_RECYCLE_AT must be a whole number from 1 to 50000, or unset/);
  });
});

describe("when the worker recycles itself", () => {
  test("the exit code is neither 0 nor the CLI's own, and a full store counts as reached", T, () => {
    assert.ok(![0, 1, 2, 3].includes(RECYCLE_EXIT));
    assert.equal(shouldRecycle({ size: AT - 1, limit: LIMIT, full: false }, AT), false);
    assert.equal(shouldRecycle({ size: AT, limit: LIMIT, full: false }, AT), true);
    assert.equal(shouldRecycle({ size: 10, limit: LIMIT, full: true }, AT), true);
  });

  test("a heavy job ending at the threshold stops the worker, heavy first, and the command returns with counts-only lines", T, async () => {
    const fill = { current: { size: 10, limit: LIMIT, full: false } };
    const w = start(fill);
    const { heavy } = await w.workers();
    await heavy.take(job("j1"));
    fill.current = { size: AT, limit: LIMIT, full: false };
    await heavy.take(job("j2"));
    assert.equal(await w.running, RECYCLE_EXIT);
    assert.deepEqual(FakeWorker.events, ["health-check-heavy closing", "health-check-heavy closed", "health-check-jobs closing", "health-check-jobs closed"]);
    assert.equal(FakePool.ended, 1);
    const recycling = w.lines.filter((l) => /^Recycl(ing|ed):/.test(l));
    assert.equal(recycling.length, 2, recycling.join("\n"));
    assert.equal(recycling[0], `Recycling: the org-word store is at ${AT} of ${LIMIT}. Finishing the jobs in hand; the host starts a fresh worker.`);
    assert.match(recycling[1]!, /^Recycled: the jobs in hand finished and the worker closed in \d+ seconds\.$/);
    assert.ok(!w.lines.includes("Stopping: finishing the jobs in hand."));
  });

  test("a light job, a connect or share job, ending at the threshold recycles too", T, async () => {
    const w = start({ current: { size: AT + 1, limit: LIMIT, full: false } });
    const { light } = await w.workers();
    await light.take(job("connect-1"));
    assert.equal(await w.running, RECYCLE_EXIT);
  });

  test("a store marked full recycles, below the number set", T, async () => {
    const w = start({ current: { size: 7, limit: LIMIT, full: true } });
    await (await w.workers()).heavy.take(job("j1"));
    assert.equal(await w.running, RECYCLE_EXIT);
  });

  test("below the threshold the worker keeps running, and SIGTERM still stops it with 0, the console's worker first", T, async () => {
    const w = start({ current: { size: AT - 1, limit: LIMIT, full: false } });
    const { light, heavy } = await w.workers();
    for (const id of ["j1", "j2", "j3"]) await heavy.take(job(id));
    await light.take(job("j4"));
    await tick();
    assert.equal(w.returned(), undefined, "still running");
    assert.ok(!heavy.closing && !light.closing);
    w.signal();
    assert.equal(await w.running, 0);
    assert.ok(w.lines.includes("Stopping: finishing the jobs in hand."));
    assert.ok(!w.lines.some((l) => l.startsWith("Recycling:")));
    assert.deepEqual(FakeWorker.events, ["health-check-jobs closing", "health-check-jobs closed", "health-check-heavy closing", "health-check-heavy closed"]);
  });

  test("with a run in hand, connect jobs still run, a new run isn't taken, and the command waits for the run (decision 371)", T, async () => {
    const fill = { current: { size: 0, limit: LIMIT, full: false } };
    const w = start(fill);
    const { light, heavy } = await w.workers();
    const run = heavy.hold("run in hand");
    fill.current = { size: AT + 3, limit: LIMIT, full: false };
    await light.take(job("connect-1"));
    await tick();
    assert.ok(heavy.closing, "the recycle has begun");
    assert.equal(heavy.take(job("run-2")), null, "a run added after the recycle began is left in Redis for the fresh worker");
    const connect = light.take(job("connect-2"));
    assert.ok(connect, "the console's worker still takes connect jobs");
    await connect;
    assert.equal(w.returned(), undefined, "waiting for the run in hand");
    assert.ok(!light.closing);
    run.finish();
    assert.equal(await w.running, RECYCLE_EXIT);
    assert.deepEqual(FakeWorker.events, ["health-check-heavy closing", "run in hand finished", "health-check-heavy closed", "health-check-jobs closing", "health-check-jobs closed"]);
  });
});

describe("the recycle's watchdog", () => {
  test("is an hour by default", T, () => {
    assert.equal(RECYCLE_WATCHDOG_MS, 3_600_000);
  });

  test("never arms on SIGTERM, nor with recycling off", T, async () => {
    const exits: number[] = [];
    for (const recycleAt of ["50000", undefined]) {
      const w = start({ current: { size: AT, limit: LIMIT, full: true } }, { recycleAt, watchdogMs: 10, exit: (code) => void exits.push(code) });
      await w.workers();
      w.signal();
      assert.equal(await w.running, 0);
    }
    await new Promise((r) => setTimeout(r, 60));
    assert.deepEqual(exits, []);
  });

  test("fires when a recycle's close hangs: exit 75, one counts-only line", T, async () => {
    const exits: number[] = [];
    let exited!: () => void;
    const done = new Promise<void>((r) => (exited = r));
    const keep = setTimeout(() => {}, 5_000);
    const w = start({ current: { size: AT, limit: LIMIT, full: false } }, { watchdogMs: 30, exit: (code) => (exits.push(code), exited()) });
    const { heavy } = await w.workers();
    heavy.hold("stuck run");
    await heavy.take(job("j1"));
    await done;
    clearTimeout(keep);
    assert.deepEqual(exits, [RECYCLE_EXIT]);
    assert.equal(w.returned(), undefined, "close is still waiting on the stuck run");
    const line = w.lines.filter((l) => /hadn't finished/.test(l));
    assert.deepEqual(line, ["Recycling: the jobs in hand hadn't finished after 0 minutes, so the worker exits now. A run cut short resumes from its checkpoints."]);
  });

  test("is cleared once the close completes", T, async () => {
    const exits: number[] = [];
    const w = start({ current: { size: AT, limit: LIMIT, full: false } }, { watchdogMs: 50, exit: (code) => void exits.push(code) });
    await (await w.workers()).heavy.take(job("j1"));
    assert.equal(await w.running, RECYCLE_EXIT);
    await new Promise((r) => setTimeout(r, 120));
    assert.deepEqual(exits, []);
  });
});

describe("hc worker's exit", () => {
  test("after a recycle, runWorker exits the process with 75", T, async () => {
    const exits: number[] = [];
    const w = start({ current: { size: AT, limit: LIMIT, full: false } }, { run: runWorker, exit: (code) => void exits.push(code) });
    await (await w.workers()).heavy.take(job("j1"));
    assert.equal(await w.running, RECYCLE_EXIT);
    assert.deepEqual(exits, [RECYCLE_EXIT]);
  });

  test("after SIGTERM or a refused start, it leaves the code to the CLI", T, async (t) => {
    const exits: number[] = [];
    const w = start({ current: { size: 0, limit: LIMIT, full: false } }, { run: runWorker, exit: (code) => void exits.push(code) });
    await w.workers();
    w.signal();
    assert.equal(await w.running, 0);
    t.mock.method(console, "error", () => {});
    assert.equal(await runWorker({}, { exit: (code) => void exits.push(code) }), 2);
    assert.deepEqual(exits, []);
  });
  test("the CLI's worker command goes through runWorker", T, () => {
    // A wiring check on the text, since running hc.ts needs BullMQ and Redis: `hc worker` must not call workerCommand directly.
    const hc = readFileSync(new URL("../../cli/hc.ts", import.meta.url), "utf8");
    assert.match(hc, /if \(positionals\[0\] === "worker" && !values\.help\) return runWorker\(\);/);
    assert.ok(!/workerCommand\(/.test(hc));
  });
});

describe("the check after a job", () => {
  test("returns at once: a close started from it, waiting for that same job, doesn't deadlock", T, async () => {
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
    await tick(2);
    assert.ok(closed);
  });

  test("resolves once, with counts only, and after a failed job as well", T, async () => {
    const fill = { current: { size: 0, limit: LIMIT, full: false } };
    const watch = recycleWatch(5, () => fill.current);
    let resolved = 0;
    void watch.due.then(() => resolved++);
    const failing = processorFor(async () => Promise.reject(new Error("Salesforce timed out")), bull, () => {}, watch.check);
    await assert.rejects(failing(job("j1")));
    await tick(2);
    assert.equal(resolved, 0);
    fill.current = { size: 5, limit: LIMIT, full: false };
    await assert.rejects(failing(job("j2")));
    await assert.rejects(failing(job("j3")));
    await tick(2);
    assert.equal(resolved, 1);
    assert.deepEqual(Object.keys(await watch.due).sort(), ["full", "limit", "size"]);
  });
});

describe("with the real store", () => {
  test("a fresh process starts under the threshold and doesn't recycle; one filled to it does, and the log holds no org text", T, async (t) => {
    t.mock.method(console, "warn", () => {});
    // A fresh process: whatever this file's tests added is far below the threshold, so a restart can't loop.
    assert.ok(orgWordsStore().size < AT);
    const fresh = recycleWatch(AT);
    let due = false;
    void fresh.due.then(() => (due = true));
    fresh.check();
    await tick(2);
    assert.equal(due, false);

    const lines: string[] = [];
    FakeWorker.made = [];
    let signal!: () => void;
    const running = workerCommand(env(String(AT)), { load: async () => modules, until: new Promise<void>((r) => (signal = r)), log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never });
    while (!FakeWorker.made.some((w) => w.name === HEAVY_QUEUE)) await new Promise((r) => setImmediate(r));
    const heavy = FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)!;
    await heavy.take(job("j1"));
    // Filled as the store's own test fills it, to the threshold and no further.
    for (let i = orgWordsStore().size; i < AT; i++) said`Stage ${org(`Secret stage ${i}`)} is held`;
    assert.equal(orgWordsStore().size, AT);
    await heavy.take(job("j2"));
    const timer = setTimeout(signal, 5_000);
    assert.equal(await running, RECYCLE_EXIT);
    clearTimeout(timer);
    const log = lines.join("\n");
    assert.match(log, new RegExp(`Recycling: the org-word store is at ${AT} of ${LIMIT}\\.`));
    assert.ok(!log.includes("Secret"));
  });
});
