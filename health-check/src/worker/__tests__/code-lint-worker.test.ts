import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { CodeLintData } from "../../extract/code-lint.ts";
import { PMD_NOT_INSTALLED } from "../../extract/lint/pmd.ts";
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
