import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { INVENTORY_FILE } from "../../extract/inventory.ts";
import { Inventory } from "../../schemas/inventory.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { inventoryOf, INVENTORY_MAX_BYTES } from "../run-job.ts";
import { progressOf } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// The org's inventory, from the run folder to the console's HealthCheckInventory
// row (DECISIONS.md 605). The store has never met a database: these hold its SQL
// to the facts' shape through a pool that records every statement.

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

// Made up, and as small as the schema allows.
const INVENTORY: Inventory = {
  version: 1,
  madeAt: "2026-10-31T12:00:00.000Z",
  scope: "complete",
  fillRates: { rows: [{ object: "Account", records: 10, fields: [{ field: "Name", filled: 10, share: 1 }], skipped: [] }], notes: [] },
  packages: { rows: [], notes: [] },
  connectedApps: { unavailable: "No access" },
  riskyPermissions: { rows: [], notes: [], checked: ["Author Apex"], notInOrg: [] },
  limits: { rows: [{ name: "DailyApiRequests", max: 100, used: 1 }], notes: [] },
};
const EMPTY = { findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null };
const INSERT = `INSERT INTO "HealthCheckInventory" ("runId", "madeAt", "inventory") VALUES ($1, $2::timestamp(3), $3::jsonb)`;
const DELETE = `DELETE FROM "HealthCheckInventory" WHERE "runId" = $1`;

const save = (pool: FakePool, inventory: Inventory | undefined, log?: (line: string) => void) => {
  const run = consoleRun();
  return new PostgresWorkerStore(pool, { log }).saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...EMPTY, ...(inventory ? { inventory } : {}) });
};

describe("the Postgres store's inventory row", () => {
  test("is replaced whole inside its own savepoint, in the results' transaction, before the commit", async () => {
    const pool = new FakePool();
    await save(pool, INVENTORY);
    const texts = pool.texts();
    const at = (t: string) => texts.indexOf(t);
    assert.deepEqual(texts.slice(at("SAVEPOINT run_inventory"), at("COMMIT") + 1), ["SAVEPOINT run_inventory", DELETE, INSERT, "RELEASE SAVEPOINT run_inventory", "COMMIT"]);
    assert.deepEqual(pool.statements[at(INSERT)]!.values, [consoleRun().id, INVENTORY.madeAt, JSON.stringify(INVENTORY)]);
    assert.equal(texts[0], "BEGIN");
  });

  test("a run with no inventory never names the table", async () => {
    const pool = new FakePool();
    await save(pool, undefined);
    assert.ok(!pool.texts().some((t) => t.includes("HealthCheckInventory")));
    assert.equal(pool.texts().at(-1), "COMMIT");
  });

  test("a console without the table still gets the run's results, and says so once", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => {
      if (text.includes(`"HealthCheckInventory"`)) throw Object.assign(new Error(`relation "HealthCheckInventory" does not exist`), { code: "42P01" });
      return undefined;
    });
    const logs: string[] = [];
    const store = new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) });
    const run = consoleRun();
    for (let i = 0; i < 2; i++) await store.saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...EMPTY, inventory: INVENTORY });
    const texts = pool.texts();
    assert.ok(texts.indexOf("ROLLBACK TO SAVEPOINT run_inventory") > texts.indexOf("SAVEPOINT run_inventory"));
    assert.equal(texts.at(-1), "COMMIT");
    assert.ok(!texts.includes("ROLLBACK"));
    assert.equal(logs.filter((l) => /no HealthCheckInventory table/.test(l)).length, 1);
  });

  test("any other error rolls the whole save back, as the facts' does", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => {
      if (text.startsWith(`INSERT INTO "HealthCheckInventory"`)) throw Object.assign(new Error("deadlock detected"), { code: "40P01" });
      return undefined;
    });
    await assert.rejects(save(pool, INVENTORY), /deadlock/);
    assert.equal(pool.texts().at(-1), "ROLLBACK");
    assert.ok(!pool.texts().includes("COMMIT"));
  });
});

describe("the inventory a run job sends", () => {
  const folder = (text: string): string => {
    const dir = mkdtempSync(join(tmpdir(), "hc-inventory-"));
    writeFileSync(join(dir, INVENTORY_FILE), text);
    return dir;
  };

  test("is the file as it parses", () => {
    const logs: string[] = [];
    assert.deepEqual(inventoryOf(folder(JSON.stringify(INVENTORY)), (l) => void logs.push(l)), INVENTORY);
    assert.deepEqual(logs, []);
  });

  test("is none without a file, or with one that doesn't parse, doesn't fit, holds a NUL or is over the cap, and the log never quotes it", () => {
    const logs: string[] = [];
    const log = (l: string) => void logs.push(l);
    assert.equal(inventoryOf(mkdtempSync(join(tmpdir(), "hc-inventory-")), log), null);
    assert.equal(inventoryOf(folder("{ not json"), log), null);
    assert.equal(inventoryOf(folder(JSON.stringify({ ...INVENTORY, version: 2 })), log), null);
    assert.equal(inventoryOf(folder(JSON.stringify({ ...INVENTORY, limits: { rows: [{ name: "Daily\u0000", max: 1, used: 0 }], notes: [] } })), log), null);
    assert.equal(inventoryOf(folder(JSON.stringify({ ...INVENTORY, packages: { rows: [], notes: ["x".repeat(INVENTORY_MAX_BYTES)] } })), log), null);
    assert.equal(logs.length, 4);
    assert.ok(logs.every((l) => !l.includes("Daily") && !l.includes("xxx")));
  });

  test("a worker's run reads the inventory and hands it over with the results", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false });
    const output = (deps.store as MemoryWorkerStore).runs.get(RUN_ID)!.output as { inventory?: Inventory };
    assert.ok(output.inventory);
    assert.ok(Inventory.safeParse(output.inventory).success);
    assert.ok("rows" in output.inventory.riskyPermissions && output.inventory.riskyPermissions.rows.some((r) => r.apiName === "Sales_Console_User"));
  });
});
