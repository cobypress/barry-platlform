import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COMPLEXITY_FILE } from "../../complexity/read.ts";
import { COMPONENTS, scoreComplexity } from "../../complexity/formula.ts";
import { Complexity } from "../../schemas/complexity.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { complexityOf, COMPLEXITY_MAX_BYTES } from "../run-job.ts";
import { progressOf } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// The org's complexity score, from the run folder to the console's
// HealthCheckComplexity row (DECISIONS.md 614), held to the inventory's shape
// through a pool that records every statement.

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;

class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [(text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "extracting" }], rowCount: 1 } : undefined)];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}

// Made up: every count at nought.
const COMPLEXITY: Complexity = scoreComplexity(Object.fromEntries(COMPONENTS.map((c) => [c.id, { counts: Object.fromEntries(c.counts.map((x) => [x.id, 0])) }])), new Date("2026-10-31T12:00:00.000Z"));
const EMPTY = { findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null };
const INSERT = `INSERT INTO "HealthCheckComplexity" ("runId", "madeAt", "complexity") VALUES ($1, $2::timestamp(3), $3::jsonb)`;
const DELETE = `DELETE FROM "HealthCheckComplexity" WHERE "runId" = $1`;

const save = (pool: FakePool, complexity: Complexity | undefined, log?: (line: string) => void) => {
  const run = consoleRun();
  return new PostgresWorkerStore(pool, { log }).saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...EMPTY, ...(complexity ? { complexity } : {}) });
};

describe("the Postgres store's complexity row", () => {
  test("is replaced whole inside its own savepoint, in the results' transaction, before the commit", async () => {
    const pool = new FakePool();
    await save(pool, COMPLEXITY);
    const texts = pool.texts();
    const at = (t: string) => texts.indexOf(t);
    assert.deepEqual(texts.slice(at("SAVEPOINT run_complexity"), at("COMMIT") + 1), ["SAVEPOINT run_complexity", DELETE, INSERT, "RELEASE SAVEPOINT run_complexity", "COMMIT"]);
    assert.deepEqual(pool.statements[at(INSERT)]!.values, [consoleRun().id, COMPLEXITY.madeAt, JSON.stringify(COMPLEXITY)]);
    assert.equal(texts[0], "BEGIN");
  });

  test("a run with no complexity never names the table", async () => {
    const pool = new FakePool();
    await save(pool, undefined);
    assert.ok(!pool.texts().some((t) => t.includes("HealthCheckComplexity")));
    assert.equal(pool.texts().at(-1), "COMMIT");
  });

  test("a console without the table still gets the run's results, and says so once", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => {
      if (text.includes(`"HealthCheckComplexity"`)) throw Object.assign(new Error(`relation "HealthCheckComplexity" does not exist`), { code: "42P01" });
      return undefined;
    });
    const logs: string[] = [];
    const store = new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) });
    const run = consoleRun();
    for (let i = 0; i < 2; i++) await store.saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...EMPTY, complexity: COMPLEXITY });
    const texts = pool.texts();
    assert.ok(texts.indexOf("ROLLBACK TO SAVEPOINT run_complexity") > texts.indexOf("SAVEPOINT run_complexity"));
    assert.equal(texts.at(-1), "COMMIT");
    assert.ok(!texts.includes("ROLLBACK"));
    assert.equal(logs.filter((l) => /no HealthCheckComplexity table/.test(l)).length, 1);
  });

  test("any other error rolls the whole save back", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => {
      if (text.startsWith(`INSERT INTO "HealthCheckComplexity"`)) throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
      return undefined;
    });
    await assert.rejects(save(pool, COMPLEXITY), /deadlock/);
    assert.equal(pool.texts().at(-1), "ROLLBACK");
    assert.ok(!pool.texts().includes("COMMIT"));
  });
});

describe("the complexity a run job sends", () => {
  const folder = (text: string): string => {
    const dir = mkdtempSync(join(tmpdir(), "hc-complexity-"));
    writeFileSync(join(dir, COMPLEXITY_FILE), text);
    return dir;
  };

  test("is the file as it parses", () => {
    const logs: string[] = [];
    assert.deepEqual(complexityOf(folder(JSON.stringify(COMPLEXITY)), (l) => void logs.push(l)), COMPLEXITY);
    assert.deepEqual(logs, []);
  });

  test("is none without a file, or with one that doesn't parse, doesn't fit, holds a NUL or is over the cap, and the log never quotes it", () => {
    const logs: string[] = [];
    const log = (l: string) => void logs.push(l);
    assert.equal(complexityOf(mkdtempSync(join(tmpdir(), "hc-complexity-")), log), null);
    assert.equal(complexityOf(folder("{ not json"), log), null);
    assert.equal(complexityOf(folder(JSON.stringify({ ...COMPLEXITY, version: 2 })), log), null);
    assert.equal(complexityOf(folder(JSON.stringify({ ...COMPLEXITY, components: [{ status: "not_read", id: "code", label: "Code", weight: 20, reason: "Planted\u0000" }] })), log), null);
    assert.equal(complexityOf(folder(JSON.stringify({ ...COMPLEXITY, leftOut: ["x".repeat(COMPLEXITY_MAX_BYTES)] })), log), null);
    assert.equal(logs.length, 4);
    assert.ok(logs.every((l) => !l.includes("Planted") && !l.includes("xxx")));
  });

  test("a worker's run works out the complexity and hands it over with the results", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false });
    const output = (deps.store as MemoryWorkerStore).runs.get(RUN_ID)!.output as { complexity?: Complexity };
    assert.ok(output.complexity);
    assert.ok(Complexity.safeParse(output.complexity).success);
    assert.equal(output.complexity.status, "measured");
  });
});
