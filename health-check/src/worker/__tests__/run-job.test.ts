import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { JobBusyError } from "../lock.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { SECTIONS_FILE } from "../run-job.ts";
import { RunClosedError, type RunProgress } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { ACCESS, baseDeps, CONNECTION, consoleRun, FlakyTransport, REFRESH, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

const job = { kind: "run", runId: RUN_ID };
const once = { final: false };
const last = { final: true };

async function setup(overrides: Parameters<typeof baseDeps>[0] = {}, input = runInput()) {
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), ...overrides });
  deps.store.addRun(input);
  await deps.tokens.save(syntheticConnection());
  return { deps, handle: createJobHandler(deps) };
}

const stored = (deps: { store: MemoryWorkerStore }) => deps.store.runs.get(RUN_ID)!;

describe("a run job", () => {
  test("runs the engine through the stored connection and hands the console its results", async () => {
    const { deps, handle } = await setup();
    const outcome = await handle(job, once);
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    const run = stored(deps);
    assert.equal(run.run.status, "awaiting_manual");
    assert.ok(run.run.steps.length > 5 && run.run.steps.every((s) => s.status === "done"), "the engine's steps replace the placeholder");
    assert.notEqual(run.run.calibrationHash, "unconfirmed");
    assert.ok(run.output!.findings.length > 0 && run.output!.metrics.length > 0 && run.output!.scores.length > 0);
    assert.equal(run.output!.manualSteps.length, 2);
    assert.ok(run.output!.sections.length > 10 && run.output!.versions.length >= run.output!.sections.length);
    assert.ok(run.output!.sections.every((s) => s.runId === RUN_ID));
    assert.equal(run.runDir, join(deps.runsDir, RUN_ID));
    for (const file of ["run.json", "findings.json", "intake.json", SECTIONS_FILE]) assert.ok(existsSync(join(run.runDir!, file)), file);
  });

  test("never writes the console's own fields", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const run = stored(deps).run;
    assert.equal(run.customerRef, "SYN-0001");
    assert.equal(run.createdBy, "consultant@example.com");
    assert.equal(run.benchmarkOptIn, true);
    assert.equal(run.startedAt, "2026-10-31T09:00:00.000Z");
  });

  test("reports progress as each step finishes, before the results", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const methods = deps.store.writes.map((w) => w.method);
    assert.ok(methods.filter((m) => m === "saveRunProgress").length >= 5);
    assert.equal(methods.at(-1), "saveRunOutput");
    assert.equal(deps.store.writes.find((w) => w.method === "saveRunProgress")!.detail, "extracting");
  });

  test("goes to review when the console's manual answers cover every step", async () => {
    const { deps, handle } = await setup({}, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    assert.equal(stored(deps).run.status, "in_review");
    assert.ok(stored(deps).output!.manualSteps.every((s) => s.status !== "pending"));
  });

  test("puts no token in anything the console sees", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const seen = JSON.stringify([...deps.store.runs.values(), deps.store.writes]) + deps.lines.join("\n");
    assert.ok(!seen.includes(ACCESS) && !seen.includes(REFRESH));
  });
});

describe("a run job delivered again", () => {
  test("in review or released, it does nothing", async () => {
    for (const status of ["in_review", "released"] as const) {
      const { deps, handle } = await setup({}, runInput({ run: consoleRun({ status }) }));
      const outcome = await handle(job, once);
      assert.equal(outcome.outcome, "skipped");
      assert.equal(deps.store.writes.length, 0);
    }
  });

  test("after it finished, it reuses every checkpoint and writes the same results", async () => {
    const transport = new ReplayTransport(loadRecording());
    const { deps, handle } = await setup({ dataTransport: transport });
    await handle(job, once);
    const first = structuredClone(stored(deps).output);
    const before = transport.requests.length;
    await handle(job, once);
    const again = transport.requests.slice(before);
    assert.ok(again.every((r) => !new URL(r.url).searchParams.has("q") && !r.url.includes("/composite/")), "no data was read a second time");
    assert.deepEqual(stored(deps).output, first);
  });

  test("after a crash part-way, it resumes without repeating finished steps", async () => {
    const recording = loadRecording();
    const { deps, handle } = await setup({ dataTransport: new FlakyTransport(new ReplayTransport(recording), 12) });
    await assert.rejects(handle(job, once), /ECONNRESET/);
    const done = stored(deps).run.steps.filter((s) => s.status === "done").map((s) => s.id);
    assert.ok(done.length > 0, "some steps finished before the crash");
    assert.equal(stored(deps).output, null);

    const second = new ReplayTransport(recording);
    const resumed = createJobHandler({ ...deps, dataTransport: second });
    assert.equal((await resumed(job, once)).outcome, "done");
    assert.ok(stored(deps).run.steps.every((s) => s.status === "done"));
    const straight = await setup();
    await straight.handle(job, once);
    assert.deepEqual(stored(deps).output!.findings, stored(straight.deps).output!.findings, "the same answer as a run that never stopped");
  });

  test("a crash on the last delivery is recorded as a failure the console can show", async () => {
    const { deps, handle } = await setup({ dataTransport: new FlakyTransport(new ReplayTransport(loadRecording()), 12) });
    const outcome = await handle(job, last);
    assert.equal(outcome.outcome, "failed");
    const run = stored(deps).run;
    assert.equal(run.status, "failed");
    assert.match(run.steps.find((s) => s.id === "worker")!.error!, /The run stopped with an error: .*ECONNRESET/);
  });

  test("two deliveries at once: the second waits for a retry", async () => {
    const { deps, handle } = await setup();
    const results = await Promise.allSettled([handle(job, once), handle(job, once)]);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
    assert.ok(rejected.reason instanceof JobBusyError);
    assert.equal(deps.store.writes.filter((w) => w.method === "saveRunOutput").length, 1);
  });

  test("if the console moves the run on meanwhile, the results aren't written over it", async () => {
    class MovingStore extends MemoryWorkerStore {
      override async saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
        await super.saveRunProgress(runId, progress, runDir);
        this.runs.get(runId)!.run.status = "in_review";
      }
    }
    const store = new MovingStore();
    const { handle } = await setup({ store });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "skipped");
    assert.match(outcome.detail, /in review/);
    assert.equal(store.runs.get(RUN_ID)!.output, null);
    assert.ok(new RunClosedError(RUN_ID, "in_review").message.includes("leaves it alone"));
  });
});

describe("a run job that can't start", () => {
  const cases: Array<[string, Parameters<typeof runInput>[0], RegExp]> = [
    ["no calibration", { calibration: null }, /calibration hasn't been confirmed/],
    ["a CLI login", { run: consoleRun({ connectionKind: "sf-cli" }) }, /only reads through a connection/],
    ["no connection named", { connectionName: null }, /Send the customer a connect link/],
    ["a connection that doesn't exist", { connectionName: "someone-else" }, /couldn't be loaded: No stored connection/],
    ["a module the worker doesn't have", { run: consoleRun({ modules: ["telepathy"] }) }, /doesn't know these modules: telepathy/],
    ["a connection to another org", { run: consoleRun({ orgId: "00D000000000099AAA" }) }, /different org/],
  ];
  for (const [name, input, reason] of cases) {
    test(`${name}: fails with the reason, having read nothing`, async () => {
      const transport = new ReplayTransport(loadRecording());
      const { deps, handle } = await setup({ dataTransport: transport }, runInput(input));
      const outcome = await handle(job, once);
      assert.equal(outcome.outcome, "failed");
      assert.match(outcome.detail, reason);
      assert.equal(stored(deps).run.status, "failed");
      assert.match(stored(deps).run.steps.find((s) => s.id === "worker")!.error!, reason);
      assert.equal(transport.requests.length, 0);
    });
  }

  test("an unknown run is skipped, and an id that isn't a folder name is refused", async () => {
    const { handle } = await setup();
    assert.equal((await handle({ kind: "run", runId: "hc-2026-10-31-99999999" }, once)).outcome, "skipped");
    assert.equal((await handle({ kind: "run", runId: "../etc" }, once)).outcome, "failed");
  });

  test("stops at the API budget, with its place kept for the next job", async () => {
    const { deps, handle } = await setup({ dataTransport: new Simulator({ used: 60_000 }) });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "stopped_budget");
    assert.equal(stored(deps).run.status, "stopped_budget");
    assert.equal(stored(deps).output, null);
  });

  test("a stored connection is still checked against the run's org on every delivery", async () => {
    const { deps, handle } = await setup();
    await deps.tokens.save(syntheticConnection({ orgId: "00D000000000099AAA" }));
    assert.match((await handle(job, once)).detail, /different org/);
    assert.equal(CONNECTION, stored(deps).connectionName);
    assert.ok(!readFileSync(join(deps.tokens.dir, `${CONNECTION}.json`), "utf8").includes(ACCESS), "tokens are encrypted at rest");
  });
});

describe("progress and the finished status", () => {
  test("progress never claims the run is finished: only the results write does", async () => {
    const { deps, handle } = await setup({}, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    const progress = deps.store.writes.filter((w) => w.method === "saveRunProgress").map((w) => w.detail);
    assert.ok(!progress.includes("in_review") && !progress.includes("awaiting_manual"), String(progress));
    assert.equal(progress.at(-1), "analysing");
    assert.equal(deps.store.writes.at(-1)!.detail, "in_review");
  });
});

describe("the sandbox flag", () => {
  test("progress keeps the console's value; the results carry what the org says", async () => {
    const { deps, handle } = await setup({}, runInput({ run: consoleRun({ isSandbox: true }) }));
    let duringRun: boolean[] = [];
    const save = deps.store.saveRunProgress.bind(deps.store);
    deps.store.saveRunProgress = async (runId, progress, runDir) => {
      duringRun.push(progress.isSandbox);
      return save(runId, progress, runDir);
    };
    await handle(job, once);
    assert.ok(duringRun.length > 0 && duringRun.every(Boolean));
    assert.equal(stored(deps).run.isSandbox, false, "the synthetic org is production");
    duringRun = [];
  });
});
