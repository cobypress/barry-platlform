import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import { HEALTH_CHECK_QUEUE, processorFor, redisConnection, startQueueWorker, type BullModule, type QueueJob, type QueueWorker } from "../bullmq.ts";
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

const bull: BullModule = { Worker: FakeWorker as unknown as BullModule["Worker"], UnrecoverableError: Unrecoverable };

/** What the console's lib/health-check/jobs.ts sends: three attempts. */
const job = (data: unknown, attemptsMade = 0): QueueJob => ({ id: "j1", name: "health-check-step", data, attemptsMade, opts: { attempts: 3 } });

describe("the BullMQ adapter", () => {
  test("listens on the console's queue", () => {
    assert.equal(HEALTH_CHECK_QUEUE, "health-check-jobs");
    const lines: string[] = [];
    const worker = startQueueWorker(bull, async () => ({ outcome: "done", detail: "" }), { redisUrl: "redis://localhost:6379", concurrency: 2, log: (l) => lines.push(l) }) as unknown as FakeWorker;
    assert.equal(worker.name, "health-check-jobs");
    assert.equal(worker.options.concurrency, 2);
    assert.ok(worker.listeners.has("failed") && worker.listeners.has("error"));
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
    assert.equal(FakeWorker.made.length, 1);
    assert.equal(FakeWorker.made[0]!.closed, true);
    assert.deepEqual(pools[0]!.options, { connectionString: env.HC_WORKER_DATABASE_URL, max: 4 });
    assert.equal(pools[0]!.ended, true);
    assert.ok(!lines.join("\n").includes(env.HC_CONNECT_KEY!) && !lines.join("\n").includes("pw@"));
    assert.ok(lines.some((l) => /Neither ANTHROPIC_API_KEY nor the ANTHROPIC_ federation variables are set, so report sections are the engine's own text/.test(l)));
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
