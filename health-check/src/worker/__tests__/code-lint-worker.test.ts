import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, utimesSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CodeLintData } from "../../extract/code-lint.ts";
import { LINT_TEMP_PREFIX, PMD_NOT_INSTALLED, PMD_TIME_LIMIT_MS } from "../../extract/lint/pmd.ts";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import type { BullModule } from "../bullmq.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { readWorkerEnv } from "../env.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, RUN_ID, runInput, syntheticConnection, temp } from "./helpers.ts";

// Decision 670: the worker finds PMD through HC_PMD_HOME and HC_JAVA_HOME, and
// every health check it runs has the code-lint step, which says so when PMD isn't there.

const env = (extra: Record<string, string> = {}): NodeJS.ProcessEnv => ({
  REDIS_URL: "redis://localhost:6379",
  HC_WORKER_DATABASE_URL: "postgres://worker:pw@db.example.com/console",
  HC_CONNECT_KEY: randomBytes(32).toString("base64"),
  HC_TOKEN_KEY: randomBytes(32).toString("base64"),
  HC_CONNECTIONS_DIR: `${temp()}/connections`,
  HC_RUNS_DIR: `${temp()}/runs`,
  HC_SF_CLIENT_ID: "3MVG9fake.client.id",
  HC_SF_CLIENT_SECRET: "fake-client-secret",
  HC_SF_REDIRECT_URI: "https://console.example.com/connect/callback",
  ...extra,
});

describe("the worker and PMD", () => {
  test("unset is no PMD; absolute paths are kept; a relative one refuses to start", () => {
    const off = readWorkerEnv(env());
    assert.ok(off.ok && off.config.pmdHome === null && off.config.javaHome === null);
    const on = readWorkerEnv(env({ HC_PMD_HOME: "/opt/pmd-bin-7.29.0", HC_JAVA_HOME: "/opt/jre" }));
    assert.ok(on.ok && on.config.pmdHome === "/opt/pmd-bin-7.29.0" && on.config.javaHome === "/opt/jre");
    const bad = readWorkerEnv(env({ HC_PMD_HOME: "tools/pmd" }));
    assert.ok(!bad.ok && bad.problems.includes("HC_PMD_HOME must be an absolute path, or unset"));
  });

  test("a health check has the step, and without PMD it says PMD isn't installed and spends no call", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false });
    const run = deps.store.runs.get(RUN_ID)!;
    const step = run.run.steps.find((s) => s.id === "code-lint");
    assert.deepEqual([step?.status, step?.calls], ["done", 0]);
    const data = JSON.parse(readFileSync(join(run.runDir!, "checkpoints", "code-lint.json"), "utf8")) as CodeLintData;
    assert.deepEqual(data.pmd, { status: "not_run", tool: "pmd", reason: PMD_NOT_INSTALLED });
  });
});

/** Just enough of BullMQ and pg for the worker to start and stop: no Redis, no database. */
class StillWorker {
  on(): this {
    return this;
  }
  async close(): Promise<void> {}
}
class StillQueue {
  async add() {}
  async close() {}
}
class NoPool {
  async query(): Promise<never> {
    throw new Error("no database in tests");
  }
  async connect(): Promise<never> {
    throw new Error("no database in tests");
  }
  async end(): Promise<void> {}
}
const modules: WorkerModules = {
  bull: { Worker: StillWorker as unknown as BullModule["Worker"], Queue: StillQueue as unknown as BullModule["Queue"], UnrecoverableError: class extends Error {} },
  pg: { Pool: NoPool as unknown as WorkerModules["pg"]["Pool"] },
};

describe("the worker's start", () => {
  test("deletes the code a PMD run cut short left, older than twice its limit, with or without PMD here; a recent folder stays", { timeout: 10_000 }, async () => {
    const base = temp();
    const old = `${base}/${LINT_TEMP_PREFIX}crashed`;
    const recent = `${base}/${LINT_TEMP_PREFIX}recent`;
    mkdirSync(old);
    writeFileSync(`${old}/0.cls`, "public class Left {}");
    mkdirSync(recent);
    const then = new Date(Date.now() - 2 * PMD_TIME_LIMIT_MS - 60_000);
    utimesSync(old, then, then);
    const lines: string[] = [];
    const code = await workerCommand(env(), { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never, lintTempBase: base });
    assert.equal(code, 0);
    assert.ok(!existsSync(old));
    assert.ok(existsSync(recent));
    assert.ok(lines.includes("Deleted 1 temporary folder of Apex code that a PMD run cut short had left."));
    assert.ok(lines.includes(`${PMD_NOT_INSTALLED} Each health check says PMD didn't run.`));
  });
});
