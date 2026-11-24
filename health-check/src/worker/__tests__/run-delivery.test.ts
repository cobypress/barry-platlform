import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { processRunJob } from "../run-job.ts";
import { createJobHandler } from "../worker.ts";
import { PostgresWorkerStore, RUN_DELIVERY_SQL, type SqlPool, type SqlResult } from "../postgres-store.ts";
import type { RunDelivery } from "../types.ts";
import { baseDeps, consoleRun, FlakyTransport, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { Simulator } from "../../testing/simulator.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";

// The console's run times (DECISIONS.md 722): each delivery of a run job that
// took the run leaves one row, when it began and ended and each step it read,
// with its milliseconds and calls. A courtesy: the job's outcome, and every
// other write, are as they were without it.

async function consultantRun() {
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
  deps.store.addRun(runInput({ run: consoleRun() }));
  await deps.tokens.save(syntheticConnection());
  return deps;
}

describe("a delivery's row (DECISIONS.md 722)", () => {
  test("a run read to the end: one row, done, with every step it read, its calls the run's", async () => {
    const deps = await consultantRun();
    const outcome = await processRunJob(RUN_ID, deps, { final: false });
    assert.equal(outcome.outcome, "done");
    assert.equal(deps.store.deliveries.length, 1);
    const d = deps.store.deliveries[0]!;
    assert.equal(d.runId, RUN_ID);
    assert.equal(d.outcome, "done");
    assert.equal(d.final, false);
    assert.equal(d.again, false);
    // The test's clock stands still (the run's dates come from it): the start and end are both its time.
    assert.ok(Date.parse(d.endedAt) >= Date.parse(d.startedAt));
    const stored = deps.store.runs.get(RUN_ID)!.run;
    // Every step the run read, in its order, each once.
    assert.deepEqual(
      d.steps.map((s) => s.id),
      stored.steps.filter((s) => s.status === "done").map((s) => s.id),
    );
    for (const s of d.steps) {
      assert.ok(Number.isInteger(s.ms) && s.ms >= 0, s.id);
      assert.equal(s.outcome, "done");
      assert.equal(s.calls, stored.steps.find((x) => x.id === s.id)!.calls, s.id);
    }
    assert.equal(
      d.steps.reduce((n, s) => n + s.calls, 0),
      stored.steps.reduce((n, s) => n + s.calls, 0),
    );
  });

  test("a run already in review: skipped before its claim, so no row", async () => {
    const deps = await consultantRun();
    deps.store.runs.get(RUN_ID)!.run.status = "in_review";
    assert.equal((await processRunJob(RUN_ID, deps, { final: false })).outcome, "skipped");
    assert.equal(deps.store.deliveries.length, 0);
  });

  test("a delivery that drops part way: its row says error, with the steps it finished and the one that failed", async () => {
    const deps = await consultantRun();
    deps.dataTransport = new FlakyTransport(new ReplayTransport(loadRecording()), 12);
    await assert.rejects(processRunJob(RUN_ID, deps, { final: false }), /ECONNRESET/);
    assert.equal(deps.store.deliveries.length, 1);
    const first = deps.store.deliveries[0]!;
    assert.equal(first.outcome, "error");
    assert.equal(first.steps.at(-1)!.outcome, "failed");
    assert.ok(first.steps.slice(0, -1).every((s) => s.outcome === "done"));
    // The next delivery resumes: its row has only what it read, never a step reused from its checkpoint.
    deps.dataTransport = new ReplayTransport(loadRecording());
    assert.equal((await processRunJob(RUN_ID, deps, { final: false })).outcome, "done");
    const second = deps.store.deliveries[1]!;
    assert.equal(second.outcome, "done");
    const doneFirst = new Set(first.steps.filter((s) => s.outcome === "done").map((s) => s.id));
    assert.ok(second.steps.every((s) => !doneFirst.has(s.id)));
    assert.ok(second.steps.some((s) => s.id === first.steps.at(-1)!.id));
  });

  test("the same drop on the last delivery: failed, and final", async () => {
    const deps = await consultantRun();
    deps.dataTransport = new FlakyTransport(new ReplayTransport(loadRecording()), 12);
    assert.equal((await processRunJob(RUN_ID, deps, { final: true })).outcome, "failed");
    assert.deepEqual(
      deps.store.deliveries.map((d) => [d.outcome, d.final]),
      [["failed", true]],
    );
  });

  test("an automatic run stopped at its budget: stopped_budget, its last step too", async () => {
    const deps = baseDeps({ dataTransport: new Simulator({ dailyLimit: 1_500 }), automatic: { on: true, orgs: new Set([ORG_ID.slice(0, 15)]) } });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false });
    assert.equal(outcome.outcome, "stopped_budget");
    const d = deps.store.deliveries.at(-1)!;
    assert.equal(d.outcome, "stopped_budget");
    assert.equal(d.steps.at(-1)!.outcome, "stopped_budget");
  });

  test("a person's Run again is known by its job id", async () => {
    const deps = await consultantRun();
    await processRunJob(RUN_ID, deps, { final: false, jobId: `run-${RUN_ID}-again-1764000000000` });
    assert.equal(deps.store.deliveries.at(-1)!.again, true);
  });

  test("a store that can't keep the row: the job's outcome and writes are as without it, and it's said", async () => {
    const plain = await consultantRun();
    const expected = await processRunJob(RUN_ID, plain, { final: false });

    const failing = await consultantRun();
    failing.store.recordDelivery = async () => {
      throw new Error("connection reset");
    };
    assert.deepEqual(await processRunJob(RUN_ID, failing, { final: false }), expected);
    assert.deepEqual(failing.store.writes, plain.store.writes);
    assert.ok(failing.lines.some((l) => l.startsWith(`Run ${RUN_ID}'s delivery times couldn't be recorded`)));

    const without = await consultantRun();
    (without.store as { recordDelivery?: unknown }).recordDelivery = undefined;
    assert.deepEqual(await processRunJob(RUN_ID, without, { final: false }), expected);
    assert.deepEqual(without.store.writes, plain.store.writes);
  });
});

describe("the Postgres store's delivery row", () => {
  const ROW: RunDelivery = {
    runId: RUN_ID,
    startedAt: "2026-11-25T09:00:00.000Z",
    endedAt: "2026-11-25T09:04:00.000Z",
    outcome: "done",
    final: false,
    again: false,
    steps: [{ id: "org", ms: 812, calls: 1, outcome: "done" }],
  };

  class Pool implements SqlPool {
    readonly statements: Array<{ text: string; values: unknown[] }> = [];
    readonly fail: string | undefined;
    constructor(fail?: string) {
      this.fail = fail;
    }
    async query(text: string, values: unknown[] = []): Promise<SqlResult> {
      this.statements.push({ text, values });
      if (this.fail) throw Object.assign(new Error("refused"), { code: this.fail });
      return { rows: [], rowCount: 1 };
    }
    async connect(): Promise<never> {
      throw new Error("not used");
    }
  }

  test("one INSERT, the steps as JSON", async () => {
    const pool = new Pool();
    assert.equal(await new PostgresWorkerStore(pool).recordDelivery(ROW), true);
    assert.deepEqual(pool.statements, [{ text: RUN_DELIVERY_SQL, values: [RUN_ID, ROW.startedAt, ROW.endedAt, "done", false, false, JSON.stringify(ROW.steps)] }]);
  });

  test("no table yet: false, said once; a run gone: false, said never; anything else throws", async () => {
    const lines: string[] = [];
    const store = new PostgresWorkerStore(new Pool("42P01"), { log: (l) => lines.push(l) });
    assert.equal(await store.recordDelivery(ROW), false);
    assert.equal(await store.recordDelivery(ROW), false);
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /20261218_health_check_run_delivery\.sql/);
    assert.equal(await new PostgresWorkerStore(new Pool("23503"), { log: (l) => lines.push(l) }).recordDelivery(ROW), false);
    assert.equal(lines.length, 1);
    await assert.rejects(new PostgresWorkerStore(new Pool("57014")).recordDelivery(ROW), /refused/);
  });
});
