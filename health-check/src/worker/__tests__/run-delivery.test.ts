import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { DELIVERY_WRITE_MS, processRunJob } from "../run-job.ts";
import { createJobHandler } from "../worker.ts";
import { capSteps, MAX_DELIVERY_STEPS, OTHER_STEPS, PostgresWorkerStore, RUN_DELIVERY_SQL, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { runFromRecording } from "../../testing/harness.ts";
import type { StepTiming } from "../../engine/run.ts";
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

describe("the write never holds the job (DECISIONS.md 722)", () => {
  // Its own limit: a write the job waited on for ever fails here, rather than hang the suite.
  test("a write that never answers: the job's outcome as without it, after the limit, and it's said", { timeout: 20_000 }, async () => {
    assert.equal(DELIVERY_WRITE_MS, 5_000);
    const plain = await consultantRun();
    const expected = await processRunJob(RUN_ID, plain, { final: false });
    const hung = await consultantRun();
    hung.deliveryWriteMs = 50;
    hung.store.recordDelivery = () => new Promise<boolean>(() => {});
    const began = Date.now();
    assert.deepEqual(await processRunJob(RUN_ID, hung, { final: false }), expected);
    assert.ok(Date.now() - began < 30_000);
    assert.ok(hung.lines.includes(`Run ${RUN_ID}'s delivery times weren't recorded within 0.05 seconds, so the job went on without them.`));
  });

  test("a step whose save throws after it was timed is timed once, never done and failed both", async () => {
    const timings: StepTiming[] = [];
    let saves = 0;
    await assert.rejects(
      runFromRecording({
        onStepTimed: (t) => timings.push(t),
        onProgress: () => {
          if (++saves === 3) throw new Error("the save failed");
        },
      }),
      /the save failed/,
    );
    const ids = timings.map((t) => t.id);
    assert.equal(new Set(ids).size, ids.length, ids.join(", "));
    assert.ok(timings.every((t) => t.outcome === "done"));
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

  test("no rights (42501) or a row its CHECKs refuse (23514): false, each said once per worker process", async () => {
    const lines: string[] = [];
    for (const code of ["42501", "23514"]) {
      const store = new PostgresWorkerStore(new Pool(code), { log: (l) => lines.push(l) });
      for (let i = 0; i < 3; i++) assert.equal(await store.recordDelivery(ROW), false);
    }
    assert.equal(lines.length, 2);
    assert.match(lines[0]!, /SQLSTATE 42501/);
    assert.match(lines[1]!, /SQLSTATE 23514/);
  });

  test("very many steps: the slowest kept in their order, the rest added up in one row with their count", async () => {
    const many: StepTiming[] = Array.from({ length: 150 }, (_, i) => ({ id: `step-${i}`, ms: (i * 37) % 151, calls: 1, outcome: "done" }));
    const capped = capSteps(many);
    assert.equal(capped.length, MAX_DELIVERY_STEPS);
    const other = capped.at(-1)! as StepTiming & { count: number };
    assert.equal(other.id, OTHER_STEPS);
    assert.equal(other.count, 150 - (MAX_DELIVERY_STEPS - 1));
    assert.equal(capped.reduce((n, s) => n + s.ms, 0), many.reduce((n, s) => n + s.ms, 0));
    assert.equal(capped.reduce((n, s) => n + s.calls, 0), 150);
    const kept = capped.slice(0, -1);
    const slowestLeftOut = Math.max(...many.filter((s) => !kept.some((k) => k.id === s.id)).map((s) => s.ms));
    assert.ok(kept.every((s) => s.ms >= slowestLeftOut));
    assert.deepEqual(kept.map((s) => Number(s.id.slice(5))), [...kept.map((s) => Number(s.id.slice(5)))].sort((a, b) => a - b));
    // Under the cap, as it was; and the row sent is the capped one, inside the migration's 32 KB.
    assert.deepEqual(capSteps(many.slice(0, 10)), many.slice(0, 10));
    const pool = new Pool();
    await new PostgresWorkerStore(pool).recordDelivery({ ...ROW, steps: many });
    const sent = String(pool.statements[0]!.values[6]);
    assert.deepEqual(JSON.parse(sent), capped);
    assert.ok(Buffer.byteLength(sent) < 32_768);
  });
});
