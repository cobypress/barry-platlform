import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { COMPONENTS, scoreComplexity } from "../../complexity/formula.ts";
import type { FactsReport } from "../../facts/summary.ts";
import type { Inventory } from "../../schemas/inventory.ts";
import { PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { progressOf, type RunOutputRecord } from "../types.ts";
import { consoleRun } from "./helpers.ts";

// The three side tables beside a run's results (DECISIONS.md 615): a console
// table that's missing or doesn't match what the worker writes (class 42), or
// refuses the row (class 23), costs that row only. A lost connection or a
// deadlock still rolls the whole save back, so the delivery is tried again.

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

// Made up, and as small as each schema allows.
const FACTS: FactsReport = { version: 1, checkedAt: "2026-10-31T09:30:00.000Z", counts: { confirmed: 1, contradicted: 0, open: 0, notChecked: 0 }, differed: [] };
const INVENTORY: Inventory = {
  version: 1,
  madeAt: "2026-10-31T12:00:00.000Z",
  scope: "complete",
  fillRates: { unavailable: "No access" },
  packages: { rows: [], notes: [] },
  connectedApps: { unavailable: "No access" },
  riskyPermissions: { rows: [], notes: [], checked: [], notInOrg: [] },
  limits: { rows: [], notes: [] },
};
const COMPLEXITY = scoreComplexity(Object.fromEntries(COMPONENTS.map((c) => [c.id, { counts: Object.fromEntries(c.counts.map((x) => [x.id, 0])) }])), new Date("2026-10-31T12:00:00.000Z"));
const OUTPUT: Omit<RunOutputRecord, "progress" | "runDir"> = { findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: FACTS, factsFile: null, inventory: INVENTORY, complexity: COMPLEXITY };

/** A value the error's message quotes, which must never reach the log. */
const PLANTED = "planted-value-0001";
const TABLES = [
  { table: "HealthCheckRunFacts", savepoint: "run_facts" },
  { table: "HealthCheckInventory", savepoint: "run_inventory" },
  { table: "HealthCheckComplexity", savepoint: "run_complexity" },
] as const;

const failing = (table: string, code: string): Handler => (text) => {
  if (text.startsWith(`INSERT INTO "${table}"`)) throw Object.assign(new Error(`${PLANTED} broke on ${table}`), { code });
  return undefined;
};

async function saveTwice(pool: FakePool, logs: string[]): Promise<void> {
  const store = new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) });
  const run = consoleRun();
  for (let i = 0; i < 2; i++) await store.saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...OUTPUT });
}

for (const { table, savepoint } of TABLES) {
  describe(`the ${table} row`, () => {
    for (const code of ["42703", "23502", "42804"]) {
      test(`a ${code} from a table that doesn't match leaves the run's results saved, and says the run, the table and the code for every save it happens to (DECISIONS.md 618)`, async () => {
        const pool = new FakePool();
        pool.handlers.unshift(failing(table, code));
        const logs: string[] = [];
        await saveTwice(pool, logs);
        const texts = pool.texts();
        assert.equal(texts.at(-1), "COMMIT");
        assert.ok(!texts.includes("ROLLBACK"));
        assert.ok(texts.lastIndexOf(`ROLLBACK TO SAVEPOINT ${savepoint}`) > texts.lastIndexOf(`SAVEPOINT ${savepoint}`));
        assert.equal(texts.filter((t) => t === "COMMIT").length, 2);
        // The other side tables are still written.
        for (const other of TABLES.filter((t) => t.table !== table)) assert.ok(texts.includes(`RELEASE SAVEPOINT ${other.savepoint}`), other.table);
        const said = logs.filter((l) => l.includes(table));
        assert.equal(said.length, 2);
        for (const line of said) assert.match(line, new RegExp(`^Run ${consoleRun().id}: the console's ${table} table refused the run's row with SQLSTATE ${code},`));
        assert.ok(logs.every((l) => !l.includes(PLANTED)));
      });
    }

    test("a lost connection (class 08) still rolls the whole save back", async () => {
      const pool = new FakePool();
      pool.handlers.unshift(failing(table, "08006"));
      const run = consoleRun();
      await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...OUTPUT }), new RegExp(PLANTED));
      assert.equal(pool.texts().at(-1), "ROLLBACK");
      assert.ok(!pool.texts().includes("COMMIT"));
      assert.ok(!pool.texts().includes(`ROLLBACK TO SAVEPOINT ${savepoint}`));
    });

    test("an error with no SQLSTATE still rolls the whole save back", async () => {
      const pool = new FakePool();
      pool.handlers.unshift((text) => {
        if (text.startsWith(`INSERT INTO "${table}"`)) throw new Error("socket hang up");
        return undefined;
      });
      const run = consoleRun();
      await assert.rejects(new PostgresWorkerStore(pool).saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...OUTPUT }), /socket hang up/);
      assert.equal(pool.texts().at(-1), "ROLLBACK");
    });
  });
}

describe("the facts' retry without the file column", () => {
  test("a 23502 on the first write isn't retried without the file: the row is left out, the results saved", async () => {
    const pool = new FakePool();
    pool.handlers.unshift(failing("HealthCheckRunFacts", "23502"));
    const logs: string[] = [];
    await saveTwice(pool, logs);
    const inserts = pool.texts().filter((t) => t.startsWith(`INSERT INTO "HealthCheckRunFacts"`));
    assert.ok(inserts.every((t) => t.includes(`"file"`)));
    assert.equal(pool.texts().at(-1), "COMMIT");
  });

  test("a retry that fails with a class 42 or 23 error is left out too, and a class 40 one rolls the save back", async () => {
    const retried = new FakePool();
    retried.handlers.unshift((text) => {
      if (text.startsWith(`INSERT INTO "HealthCheckRunFacts"`)) throw Object.assign(new Error(PLANTED), { code: text.includes(`"file"`) ? "42703" : "23502" });
      return undefined;
    });
    const logs: string[] = [];
    await saveTwice(retried, logs);
    assert.equal(retried.texts().at(-1), "COMMIT");
    assert.deepEqual(logs.filter((l) => l.includes("HealthCheckRunFacts")).map((l) => /SQLSTATE (\w+)/.exec(l)?.[1]), ["23502", "23502"]);

    const deadlock = new FakePool();
    deadlock.handlers.unshift((text) => {
      if (text.startsWith(`INSERT INTO "HealthCheckRunFacts"`)) throw Object.assign(new Error("deadlock detected"), { code: text.includes(`"file"`) ? "42703" : "40P01" });
      return undefined;
    });
    const run = consoleRun();
    await assert.rejects(new PostgresWorkerStore(deadlock).saveRunOutput(run.id, { progress: progressOf(run), runDir: "/runs/x", ...OUTPUT }), /deadlock/);
    assert.equal(deadlock.texts().at(-1), "ROLLBACK");
  });
});
