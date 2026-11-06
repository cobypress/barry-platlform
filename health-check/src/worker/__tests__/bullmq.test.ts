import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { HEALTH_CHECK_QUEUE, HEAVY_QUEUE, lostRecorder, LOST_REASON, processorFor, redisConnection, startQueueWorker, type BullModule, type QueueJob, type QueueWorker } from "../bullmq.ts";
import { readWorkerEnv, WORKER_ENV } from "../env.ts";
import type { JobHandler } from "../worker.ts";
import { BadJobError } from "../types.ts";
import { temp } from "./helpers.ts";

class Unrecoverable extends Error {}

/** BullMQ's Worker, as far as the adapter can tell. Never connects to anything. */
class FakeWorker implements QueueWorker {
  static made: FakeWorker[] = [];
  readonly listeners = new Map<string, (...args: never[]) => void>();
  closed = false;
  readonly name: string;
  readonly processor: (job: QueueJob) => Promise<unknown>;
  readonly options: { connection: unknown; concurrency: number };
  constructor(name: string, processor: (job: QueueJob) => Promise<unknown>, options: { connection: unknown; concurrency: number }) {
    this.name = name;
    this.processor = processor;
    this.options = options;
    FakeWorker.made.push(this);
  }
  on(event: string, listener: (...args: never[]) => void): this {
    this.listeners.set(event, listener);
    return this;
  }
  async close(): Promise<void> {
    this.closed = true;
  }
}

/** Jobs the worker put on the queue itself. */
const added: Array<{ name: string; data: unknown; opts: Record<string, unknown> }> = [];
class FakeQueue {
  readonly name: string;
  constructor(name: string) {
    this.name = name;
  }
  async add(name: string, data: unknown, opts: Record<string, unknown>) {
    added.push({ name, data, opts });
  }
  async close() {}
}
const bull: BullModule = { Worker: FakeWorker as unknown as BullModule["Worker"], Queue: FakeQueue as unknown as BullModule["Queue"], UnrecoverableError: Unrecoverable };

/** What the console's lib/health-check/jobs.ts sends: three attempts. */
const job = (data: unknown, attemptsMade = 0): QueueJob => ({ id: "j1", name: "health-check-step", data, attemptsMade, opts: { attempts: 3 } });

describe("the BullMQ adapter", () => {
  test("listens on the console's queue", () => {
    assert.equal(HEALTH_CHECK_QUEUE, "health-check-jobs");
    const lines: string[] = [];
    const workers = startQueueWorker(bull, async () => ({ outcome: "done", detail: "" }), { redisUrl: "redis://localhost:6379", concurrency: 2, log: (l) => lines.push(l) });
    const worker = workers.light as unknown as FakeWorker;
    assert.equal(worker.name, "health-check-jobs");
    assert.equal(worker.options.concurrency, 2);
    // Runs and documents are handed to a queue of their own, worked one at a time (DECISIONS.md 371).
    const heavy = workers.heavy as unknown as FakeWorker;
    assert.deepEqual([heavy.name, heavy.options.concurrency], ["health-check-heavy", 1]);
    for (const w of [worker, heavy]) assert.ok(w.listeners.has("failed") && w.listeners.has("error"));
  });

  test("tells the handler which delivery is the last", async () => {
    const seen: boolean[] = [];
    const handler: JobHandler = async (_data, attempt) => {
      seen.push(attempt.final);
      return { outcome: "done", detail: "" };
    };
    const process = processorFor(handler, bull, () => {});
    for (const made of [0, 1, 2]) await process(job({ kind: "run", runId: "r1" }, made));
    assert.deepEqual(seen, [false, false, true]);
    await processorFor(handler, bull, () => {})({ ...job({}), opts: {} });
    assert.equal(seen.at(-1), true, "a job with no retries is always the last");
  });

  test("hands BullMQ only the outcome, and logs the detail", async () => {
    const lines: string[] = [];
    const result = await processorFor(async () => ({ outcome: "connected", detail: "Connected to org 00D000000000001AAA" }), bull, (l) => lines.push(l))(job({}));
    assert.deepEqual(result, { outcome: "connected" });
    assert.match(lines[0]!, /j1 connected: Connected to org/);
  });

  test("a payload that isn't ids only is never retried; other errors are", async () => {
    const bad = processorFor(async () => Promise.reject(new BadJobError("not ids")), bull, () => {});
    await assert.rejects(bad(job({})), Unrecoverable);
    const busy = processorFor(async () => Promise.reject(new Error("busy")), bull, () => {});
    await assert.rejects(busy(job({})), (err: Error) => !(err instanceof Unrecoverable) && err.message === "busy");
  });
});

describe("redisConnection", () => {
  test("parses the console's REDIS_URL into ioredis options", () => {
    assert.deepEqual(redisConnection("rediss://default:p%40ss@cache.example.com:6380/2"), {
      host: "cache.example.com",
      port: 6380,
      username: "default",
      password: "p@ss",
      db: 2,
      tls: {},
      maxRetriesPerRequest: null,
    });
    assert.deepEqual(redisConnection("redis://localhost"), { host: "localhost", port: 6379, maxRetriesPerRequest: null });
  });

  test("refuses anything else without echoing it", () => {
    assert.throws(() => redisConnection("http://user:secretpw@x"), (err: Error) => /redis:\/\//.test(err.message) && !err.message.includes("secretpw"));
    assert.throws(() => redisConnection("not a url"), /isn't a valid URL/);
    assert.throws(() => redisConnection("redis://x/abc"), /must be a number/);
  });
});

const goodEnv = (): NodeJS.ProcessEnv => ({
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

describe("the worker's environment", () => {
  test("a complete one is read", () => {
    const read = readWorkerEnv(goodEnv());
    assert.ok(read.ok);
    assert.equal(read.ok && read.config.concurrency, 2);
    assert.equal(read.ok && read.config.connectKey.length, 32);
  });

  test("the retention sweep deletes unless HC_CLEANUP_DRY_RUN is 1, and refuses anything else", () => {
    const dry = (value?: string) => {
      const read = readWorkerEnv({ ...goodEnv(), ...(value === undefined ? {} : { HC_CLEANUP_DRY_RUN: value }) });
      return read.ok ? read.config.cleanupDryRun : read.problems.join("; ");
    };
    assert.equal(dry(), false);
    assert.equal(dry("0"), false);
    assert.equal(dry("1"), true);
    assert.match(String(dry("yes")), /HC_CLEANUP_DRY_RUN must be 1, 0 or unset/);
  });

  test("Claude is optional: a key, or OIDC federation with an identity token", () => {
    const FEDERATION = { ANTHROPIC_FEDERATION_RULE_ID: "fdrl_synthetic", ANTHROPIC_ORGANIZATION_ID: "00000000-0000-4000-8000-000000000001", ANTHROPIC_SERVICE_ACCOUNT_ID: "svac_synthetic" };
    const claude = (extra: Record<string, string>) => {
      const read = readWorkerEnv({ ...goodEnv(), ...extra });
      assert.ok(read.ok, !read.ok ? read.problems.join("; ") : "");
      return read.config.claude;
    };
    assert.equal(claude({}).kind, "none");
    assert.deepEqual(claude({ ANTHROPIC_API_KEY: " sk-ant-fake-key-for-tests \n" }), { kind: "key", apiKey: "sk-ant-fake-key-for-tests" });
    assert.deepEqual(claude({ ...FEDERATION, ANTHROPIC_IDENTITY_TOKEN_FILE: "/etc/render/oidc/anthropic" }), { kind: "federation" });
    // Render writes the token file once OIDC is set up. Without it, drafting is off and the worker says why.
    const noToken = claude(FEDERATION);
    assert.ok(noToken.kind === "none" && /ANTHROPIC_IDENTITY_TOKEN_FILE isn't set/.test(noToken.reason));
    // A key wins, as it does in the SDK.
    assert.equal(claude({ ...FEDERATION, ANTHROPIC_IDENTITY_TOKEN_FILE: "/x", ANTHROPIC_API_KEY: "sk-ant-fake" }).kind, "key");
    const half = readWorkerEnv({ ...goodEnv(), ANTHROPIC_FEDERATION_RULE_ID: "fdrl_synthetic" });
    assert.ok(!half.ok && half.problems.some((p) => /ANTHROPIC_ORGANIZATION_ID, ANTHROPIC_SERVICE_ACCOUNT_ID missing/.test(p)));
    for (const name of ["ANTHROPIC_API_KEY", "ANTHROPIC_FEDERATION_RULE_ID", "ANTHROPIC_ORGANIZATION_ID", "ANTHROPIC_SERVICE_ACCOUNT_ID", "ANTHROPIC_WORKSPACE_ID"]) assert.ok(name in WORKER_ENV.optional, name);
  });

  test("names every missing variable", () => {
    const read = readWorkerEnv({});
    assert.ok(!read.ok);
    assert.deepEqual(!read.ok && read.problems, Object.keys(WORKER_ENV.required).map((n) => `${n} is not set`));
  });

  test("names each wrong value, never repeating one", () => {
    const env = { ...goodEnv(), REDIS_URL: "http://secret-host", HC_CONNECT_KEY: "c2hvcnQ=", HC_RUNS_DIR: "runs", HC_WORKER_CONCURRENCY: "50", HC_SF_REDIRECT_URI: "http://console.example.com/cb" };
    const read = readWorkerEnv(env);
    assert.ok(!read.ok);
    const text = !read.ok ? read.problems.join("\n") : "";
    for (const name of ["REDIS_URL", "HC_CONNECT_KEY", "HC_RUNS_DIR", "HC_WORKER_CONCURRENCY", "HC_SF_REDIRECT_URI"]) assert.match(text, new RegExp(name));
    assert.ok(!text.includes("secret-host") && !text.includes("c2hvcnQ="));
  });

  test("refuses one key for both jobs", () => {
    const env = goodEnv();
    env.HC_TOKEN_KEY = env.HC_CONNECT_KEY;
    const read = readWorkerEnv(env);
    assert.ok(!read.ok && read.problems.includes("HC_CONNECT_KEY and HC_TOKEN_KEY must be different keys"));
  });
});

describe("the startup check on Claude", () => {
  test("says which model answered, or why the call failed, never a key", async () => {
    const { checkClaude } = await import("../../narrative/guard.ts");
    const ok = await checkClaude({ complete: async (r) => ({ stopReason: "end_turn", text: '{"ok":true}', model: r.model }) }, "claude-opus-5-5");
    assert.deepEqual(ok, { ok: true, detail: "claude-opus-5-5 answered" });
    const failing = { complete: async () => { throw Object.assign(new Error("401 invalid key sk-ant-api03-abcdef123456"), { name: "AuthenticationError", status: 401 }); } };
    const bad = await checkClaude(failing, "claude-opus-5-5");
    assert.equal(bad.ok, false);
    assert.match(bad.detail, /^AuthenticationError \(401\): 401 invalid key \[removed\]$/);
  });
});

describe("hc worker", () => {
  test("refuses to start with nothing set, before loading BullMQ or Postgres", () => {
    const hc = fileURLToPath(new URL("../../cli/hc.ts", import.meta.url));
    const result = spawnSync(process.execPath, [hc, "worker"], { encoding: "utf8", env: { PATH: process.env.PATH, HOME: temp() } });
    assert.equal(result.status, 2);
    for (const name of Object.keys(WORKER_ENV.required)) assert.match(result.stderr, new RegExp(`${name} is not set`));
  });

  test("with a complete environment, starts one queue worker and one pool, then stops cleanly", async () => {
    FakeWorker.made = [];
    const pools: Array<{ options: unknown; ended: boolean }> = [];
    class FakePool {
      state: { options: unknown; ended: boolean };
      constructor(options: unknown) {
        this.state = { options, ended: false };
        pools.push(this.state);
      }
      async query(): Promise<never> {
        throw new Error("no database in tests");
      }
      async connect(): Promise<never> {
        throw new Error("no database in tests");
      }
      async end(): Promise<void> {
        this.state.ended = true;
      }
    }
    const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };
    const env = goodEnv();
    const lines: string[] = [];
    const code = await workerCommand(env, { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l) });
    assert.equal(code, 0);
    // The console's queue and the heavy one, both closed on the way out.
    assert.deepEqual(FakeWorker.made.map((w) => [w.name, w.closed]), [["health-check-jobs", true], ["health-check-heavy", true]]);
    // A connection for each of the console's slots and the heavy one, and two spare.
    assert.deepEqual(pools[0]!.options, { connectionString: env.HC_WORKER_DATABASE_URL, max: 5 });
    assert.equal(pools[0]!.ended, true);
    assert.ok(!lines.join("\n").includes(env.HC_CONNECT_KEY!) && !lines.join("\n").includes("pw@"));
    assert.ok(lines.some((l) => /Neither ANTHROPIC_API_KEY nor the ANTHROPIC_ federation variables are set, so report sections are the engine's own text/.test(l)));
  });

  test("runs the retention sweep at start, in the runs folder, with the dry-run setting", async () => {
    class FakePool {
      async query(): Promise<never> {
        throw new Error("no database in tests");
      }
      async connect(): Promise<never> {
        throw new Error("no database in tests");
      }
      async end(): Promise<void> {}
    }
    const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };
    const env: NodeJS.ProcessEnv = { ...goodEnv(), HC_CLEANUP_DRY_RUN: "1" };
    const calls: Array<{ runsDir: string; dryRun: boolean; locksDir?: string }> = [];
    const lines: string[] = [];
    const sweep = async (deps: { runsDir: string; dryRun: boolean; locksDir?: string }) => {
      calls.push({ runsDir: deps.runsDir, dryRun: deps.dryRun, locksDir: deps.locksDir });
      throw new Error("the database is down");
    };
    assert.equal(await workerCommand(env, { load: async () => modules, until: new Promise((r) => setTimeout(r, 10)), log: (l) => lines.push(l), sweep: sweep as never }), 0);
    assert.deepEqual(calls, [{ runsDir: env.HC_RUNS_DIR!, dryRun: true, locksDir: `${env.HC_RUNS_DIR}-locks` }]);
    // A failed sweep is logged, and the worker carries on.
    assert.ok(lines.some((l) => /Retention sweep failed, and will run again in a day: the database is down/.test(l)), lines.join("\n"));
  });

  test("with ANTHROPIC_API_KEY, says Claude drafts the sections, and never prints the key", async () => {
    FakeWorker.made = [];
    class FakePool {
      async query(): Promise<never> {
        throw new Error("no database in tests");
      }
      async connect(): Promise<never> {
        throw new Error("no database in tests");
      }
      async end(): Promise<void> {}
    }
    const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };
    const env = { ...goodEnv(), ANTHROPIC_API_KEY: "sk-ant-fake-key-for-tests" };
    const lines: string[] = [];
    const claudeCheck = async () => ({ ok: false, detail: "AuthenticationError (401): invalid x-api-key" });
    assert.equal(await workerCommand(env, { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l), claudeCheck }), 0);
    await new Promise((resolve) => setImmediate(resolve));
    assert.ok(lines.some((l) => /Claude sign-in failed, so runs will use the engine's own text until it's fixed\. AuthenticationError \(401\)/.test(l)), lines.join("\n"));
    assert.ok(lines.some((l) => /Claude drafts each run's report sections, signed in with ANTHROPIC_API_KEY/.test(l)));
    assert.ok(!lines.join("\n").includes("sk-ant-fake-key-for-tests"));
  });

  test("with OIDC federation, says Claude drafts the sections through it", async () => {
    FakeWorker.made = [];
    class FakePool {
      async query(): Promise<never> {
        throw new Error("no database in tests");
      }
      async connect(): Promise<never> {
        throw new Error("no database in tests");
      }
      async end(): Promise<void> {}
    }
    const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };
    const env = { ...goodEnv(), ANTHROPIC_FEDERATION_RULE_ID: "fdrl_synthetic", ANTHROPIC_ORGANIZATION_ID: "00000000-0000-4000-8000-000000000001", ANTHROPIC_SERVICE_ACCOUNT_ID: "svac_synthetic", ANTHROPIC_IDENTITY_TOKEN_FILE: "/nonexistent/token" };
    const lines: string[] = [];
    const claudeCheck = async () => ({ ok: true, detail: "claude-opus-5-5 answered" });
    assert.equal(await workerCommand(env, { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l), claudeCheck }), 0);
    await new Promise((resolve) => setImmediate(resolve));
    assert.ok(lines.some((l) => /signed in through OIDC federation/.test(l)), lines.join("\n"));
    assert.ok(lines.some((l) => /Claude sign-in works: claude-opus-5-5 answered\./.test(l)), lines.join("\n"));
  });
});

describe("a scan the worker queues itself", async () => {
  const { runQueuer } = await import("../bullmq.ts");
  test("goes on as a run job with the console's own name and options", async () => {
    added.length = 0;
    await runQueuer(new FakeQueue("health-check-jobs"), () => 1_700_000_000_000)("hc-2026-11-06-00000001");
    assert.deepEqual(added, [
      {
        name: "health-check-run",
        data: { kind: "run", runId: "hc-2026-11-06-00000001" },
        opts: { attempts: 3, backoff: { type: "exponential", delay: 30_000 }, removeOnComplete: true, removeOnFail: 200, priority: 10, jobId: "run-hc-2026-11-06-00000001-1700000000000" },
      },
    ]);
  });
});

describe("a heavy job BullMQ gives up on (DECISIONS.md 396)", () => {
  // BullMQ 5: a job whose process died is put back once its lock lapses. Past maxStalledCount it's
  // failed with "job stalled more than allowable limit" without the handler running, so only the
  // "failed" event knows. Before, that was logged and the run sat at "extracting" for ever.
  const STALLED = "job stalled more than allowable limit";
  const start = () => {
    FakeWorker.made = [];
    const lost: Array<{ data: unknown; reason: string }> = [];
    startQueueWorker(bull, async () => ({ outcome: "done", detail: "" }), { redisUrl: "redis://localhost:6379", concurrency: 2, log: () => {}, onLost: async (data, reason) => void lost.push({ data, reason }) });
    const heavy = FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)!;
    const fail = heavy.listeners.get("failed") as unknown as (job: QueueJob | undefined, err: Error) => void;
    return { heavy, fail, lost };
  };

  test("a run that stalled too often is recorded as failed, with a reason the console can show", async () => {
    const { fail, lost } = start();
    fail({ id: "run-r1-1", name: "health-check-run", data: { kind: "run", runId: "r1" }, attemptsMade: 0, opts: { attempts: 3 } }, new Error(STALLED));
    await new Promise((r) => setImmediate(r));
    assert.equal(lost.length, 1);
    assert.deepEqual(lost[0]!.data, { kind: "run", runId: "r1" });
    assert.match(lost[0]!.reason, /stopped part-way more than once/);
  });

  test("a failure the handler raised itself isn't recorded twice: the handler already did", async () => {
    const { fail, lost } = start();
    fail({ id: "run-r1-1", name: "health-check-run", data: { kind: "run", runId: "r1" }, attemptsMade: 3, opts: { attempts: 3 } }, new Error("Salesforce timed out"));
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(lost, []);
  });

  test("a heavy job may stall twice, since a run resumes from its checkpoints: two deploys during a long run don't lose it", () => {
    const { heavy } = start();
    assert.equal((heavy.options as { maxStalledCount?: number }).maxStalledCount, 2);
  });

  test("the worker records a lost run as failed and lost documents as not made, and leaves the rest", async () => {
    const store = new MemoryWorkerStore();
    const calls: string[] = [];
    store.recordRunFailure = async (id, reason) => void calls.push(`run ${id}: ${reason}`);
    store.failDocuments = async (id, reason) => void calls.push(`documents ${id}: ${reason}`);
    const record = lostRecorder(store, () => new Date("2026-11-07T01:00:00.000Z"));
    await record({ kind: "run", runId: "r1" }, LOST_REASON);
    await record({ kind: "documents", runId: "r2" }, LOST_REASON);
    await record({ kind: "connect", linkId: "l1" }, LOST_REASON);
    await record({ kind: "share", runId: "r3" }, LOST_REASON);
    assert.deepEqual(calls, [`run r1: ${LOST_REASON}`, `documents r2: ${LOST_REASON}`]);
  });
});
