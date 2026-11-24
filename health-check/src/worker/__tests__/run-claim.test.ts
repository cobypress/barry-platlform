import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { APEX_SOURCE_FILE } from "../../extract/apex-source.ts";
import { SUMMARIES_PARTIAL_FILE } from "../../handbook/summaries.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { approvedReport, type ApprovedRun } from "../../render/render-released.ts";
import { BASELINE_READ_FILE, SURVEY_FILE } from "../../render/render-run.ts";
import type { RunStatus } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { plantEarlier } from "../../testing/recheck.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { MemoryWorkerStore, type StoredRun } from "../memory-store.ts";
import { processRunJob } from "../run-job.ts";
import { RunClosedError, type RunJobInput, type RunOutputRecord, type RunProgress } from "../types.ts";
import { baseDeps, consoleRun, FlakyTransport, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// A delivery claims its run in the store before it writes anything, to the run
// folder or the store (DECISIONS.md 537, 538, 540). Each case goes through the
// real run job, with the synthetic org's recording and the memory store.

const STATUSES = ["extracting", "stopped_budget", "analysing", "awaiting_manual", "in_review", "released", "failed"] as const satisfies readonly RunStatus[];
const CLOSED_SENTENCE = `Run ${RUN_ID} is in review, so the worker leaves it alone`;

/** The memory store, saying the run's status after each write, with a place to act just before or after the claim and before the save. */
class WatchedStore extends MemoryWorkerStore {
  readonly seen: string[] = [];
  beforeClaim: () => void = () => {};
  afterClaim: () => void = () => {};
  beforeSave: () => void = () => {};
  #say(method: string, runId: string): void {
    this.seen.push(`${method}:${this.runs.get(runId)!.run.status}`);
  }
  override async claimRun(runId: string): Promise<void> {
    this.beforeClaim();
    await super.claimRun(runId);
    this.#say("claimRun", runId);
    this.afterClaim();
  }
  override async saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
    await super.saveRunProgress(runId, progress, runDir);
    this.#say("saveRunProgress", runId);
  }
  override async saveRunOutput(runId: string, output: RunOutputRecord): Promise<void> {
    this.beforeSave();
    await super.saveRunOutput(runId, output);
    this.#say("saveRunOutput", runId);
  }
  override async recordRunFailure(runId: string, reason: string, at: string): Promise<void> {
    await super.recordRunFailure(runId, reason, at);
    this.#say("recordRunFailure", runId);
  }
}

/**
 * The console's move to review after decision 539: only from awaiting manual
 * steps, as its `updateMany` with the status in the condition. Says whether a run moved.
 */
function consoleMoveToReview(store: MemoryWorkerStore, runId: string): boolean {
  const run = store.runs.get(runId)!;
  if (run.run.status !== "awaiting_manual") return false;
  run.run.status = "in_review";
  return true;
}

async function setup(input: RunJobInput = runInput()) {
  const store = new WatchedStore();
  const deps = baseDeps({ store, dataTransport: new ReplayTransport(loadRecording()) });
  await deps.tokens.save(syntheticConnection());
  store.addRun(input);
  const dir = join(deps.runsDir, RUN_ID);
  const stored = () => store.runs.get(RUN_ID)!;
  /** A delivery, with a fresh replay of the recording. */
  const deliver = (final = true) => {
    deps.dataTransport = new ReplayTransport(loadRecording());
    return processRunJob(RUN_ID, deps, { final });
  };
  return { store, deps, dir, stored, deliver };
}

/** Every file under the folder, by its path, with its bytes' hash. */
function snapshot(dir: string): Map<string, string> {
  const out = new Map<string, string>();
  const walk = (d: string): void => {
    for (const name of readdirSync(d)) {
      const path = join(d, name);
      if (statSync(path).isDirectory()) walk(path);
      else out.set(relative(dir, path), createHash("sha256").update(readFileSync(path)).digest("hex"));
    }
  };
  walk(dir);
  return out;
}

const approve = (output: StoredRun["output"]): ApprovedRun => ({
  intake: null,
  sections: output!.sections.map((s) => ({ ...s, status: "approved" as const, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } })),
  versions: output!.versions,
  overrides: [],
  preparedBy: null,
});

/** The Apex code kept for Claude's summaries, and a partial file of them, as a delivery that stopped part way leaves them. */
function plantApexFiles(dir: string): void {
  writeFileSync(join(dir, APEX_SOURCE_FILE), '{"planted":true}\n', { mode: 0o600 });
  writeFileSync(join(dir, SUMMARIES_PARTIAL_FILE), '{"planted":true}\n', { mode: 0o600 });
}

describe("a delivery the console moved to review after the run was read (DECISIONS.md 537)", () => {
  test("survey answers that arrived after the first delivery: skipped at the claim, the folder byte for byte as the first left it, and the approved report has no survey page", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).outcome, "done");
    assert.equal(w.stored().run.status, "awaiting_manual");
    assert.ok(!existsSync(join(w.dir, SURVEY_FILE)));
    const approved = approve(w.stored().output);
    const before = snapshot(w.dir);
    const seen = w.store.seen.length;
    w.stored().survey = syntheticSurvey(12);
    // The console's move lands after the read and before the claim.
    w.store.beforeClaim = () => assert.ok(consoleMoveToReview(w.store, RUN_ID));
    const outcome = await w.deliver();
    assert.deepEqual(outcome, { outcome: "skipped", detail: CLOSED_SENTENCE });
    assert.deepEqual(snapshot(w.dir), before);
    assert.deepEqual(w.store.seen.slice(seen), []);
    const { report, pseudonyms } = approvedReport(w.dir, approved);
    assert.equal(report.survey, null);
    assert.ok(!/team survey/i.test(fullReportHtml(report, pseudonyms)));
  });

  test("a re-check whose earlier health check was unreadable at the first delivery and readable at the second: skipped at the claim, the record still false, and the approved report has no progress page", async () => {
    const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });
    const earlierId = `${RUN_ID}-earlier`;
    const w = await setup(runInput({ run: consoleRun({ baselineRunId: earlierId }) }));
    const earlier = join(w.deps.runsDir, earlierId);
    plantEarlier(recorded, earlier);
    chmodSync(join(earlier, "metrics.json"), 0);
    assert.equal((await w.deliver()).outcome, "done");
    chmodSync(join(earlier, "metrics.json"), 0o600);
    assert.equal(readFileSync(join(w.dir, BASELINE_READ_FILE), "utf8"), '{"read":false}');
    const approved = approve(w.stored().output);
    const before = snapshot(w.dir);
    w.store.beforeClaim = () => assert.ok(consoleMoveToReview(w.store, RUN_ID));
    assert.deepEqual(await w.deliver(), { outcome: "skipped", detail: CLOSED_SENTENCE });
    assert.deepEqual(snapshot(w.dir), before);
    assert.equal(readFileSync(join(w.dir, BASELINE_READ_FILE), "utf8"), '{"read":false}');
    const { report, pseudonyms } = approvedReport(w.dir, approved);
    assert.equal(report.input.baseline, null);
    assert.ok(!fullReportHtml(report, pseudonyms).includes("<h2>What has changed</h2>"));
  });

  for (const [what, input] of [
    ["a run that would go on", runInput()],
    ["a run that would fail (a module the worker doesn't have)", runInput({ run: consoleRun({ modules: ["telepathy"] }) })],
  ] as const) {
    test(`${what}, moved between the load and the claim: skipped with the refused save's sentence, the Apex files kept, nothing written to the store`, async () => {
      const w = await setup(input);
      w.stored().run.status = "awaiting_manual";
      mkdirSync(w.dir, { recursive: true });
      plantApexFiles(w.dir);
      const before = snapshot(w.dir);
      w.store.beforeClaim = () => assert.ok(consoleMoveToReview(w.store, RUN_ID));
      assert.deepEqual(await w.deliver(), { outcome: "skipped", detail: CLOSED_SENTENCE });
      assert.deepEqual(snapshot(w.dir), before);
      assert.ok(existsSync(join(w.dir, APEX_SOURCE_FILE)) && existsSync(join(w.dir, SUMMARIES_PARTIAL_FILE)));
      assert.deepEqual(w.store.writes, []);
      assert.equal(w.stored().run.status, "in_review");
    });
  }
});

describe("the claim, for each status a run can have at the job's start", () => {
  for (const status of STATUSES) {
    test(`${status}: the store's claim`, async () => {
      const store = new MemoryWorkerStore();
      store.addRun(runInput({ run: consoleRun({ status }) }));
      const claim = store.claimRun(RUN_ID);
      if (status === "in_review" || status === "released") {
        await assert.rejects(claim, (err: unknown) => err instanceof RunClosedError && err.message === `Run ${RUN_ID} is ${status.replace("_", " ")}, so the worker leaves it alone`);
        assert.deepEqual(store.writes, []);
      } else await claim;
      assert.equal(store.runs.get(RUN_ID)!.run.status, status === "awaiting_manual" ? "analysing" : status);
    });

    test(`${status}: the job`, async () => {
      const w = await setup();
      // A folder from an earlier delivery, so a skip can be seen to leave it as it was.
      assert.equal((await w.deliver()).outcome, "done");
      w.stored().run.status = status;
      const before = snapshot(w.dir);
      const seen = w.store.seen.length;
      const outcome = await w.deliver();
      if (status === "in_review" || status === "released") {
        assert.deepEqual(outcome, { outcome: "skipped", detail: `Run ${RUN_ID} is ${status.replace("_", " ")}, so there's nothing for the worker to do` });
        assert.deepEqual(snapshot(w.dir), before);
        assert.deepEqual(w.store.seen.slice(seen), []);
        return;
      }
      assert.equal(outcome.outcome, "done", outcome.detail);
      // Straight after the claim: only awaiting manual steps moved.
      assert.equal(w.store.seen[seen], `claimRun:${status === "awaiting_manual" ? "analysing" : status}`);
      assert.equal(w.store.seen.at(-1), "saveRunOutput:awaiting_manual");
    });
  }
});

describe("between the claim and the save", () => {
  test("the console's conditional move finds nothing to move, and the delivery's save goes through", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).outcome, "done");
    assert.equal(w.stored().run.status, "awaiting_manual");
    const moved: boolean[] = [];
    w.store.afterClaim = () => void moved.push(consoleMoveToReview(w.store, RUN_ID));
    w.store.beforeSave = () => void moved.push(consoleMoveToReview(w.store, RUN_ID));
    const saves = w.store.writes.filter((x) => x.method === "saveRunOutput").length;
    const outcome = await w.deliver();
    assert.deepEqual(moved, [false, false]);
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.equal(w.store.writes.filter((x) => x.method === "saveRunOutput").length, saves + 1);
  });

  test("an unconditional move (the console before decision 539) is still refused at the save, as before", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).outcome, "done");
    w.store.beforeSave = () => void (w.stored().run.status = "in_review");
    const output = structuredClone(w.stored().output);
    assert.deepEqual(await w.deliver(), { outcome: "skipped", detail: CLOSED_SENTENCE });
    assert.deepEqual(w.stored().output, output);
  });
});

describe("a claim that fails for another reason", () => {
  test("the job throws for the queue to try again, and nothing is written to the folder or the store", async () => {
    const w = await setup();
    assert.equal((await w.deliver()).outcome, "done");
    plantApexFiles(w.dir);
    const before = snapshot(w.dir);
    const writes = w.store.writes.length;
    const status = w.stored().run.status;
    w.store.beforeClaim = () => {
      throw new Error("Connection terminated unexpectedly");
    };
    await assert.rejects(w.deliver(false), /Connection terminated unexpectedly/);
    await assert.rejects(w.deliver(true), /Connection terminated unexpectedly/);
    assert.deepEqual(snapshot(w.dir), before);
    assert.equal(w.store.writes.length, writes);
    assert.equal(w.stored().run.status, status);
  });

  test("on a fresh run, nothing is made in the folder", async () => {
    const w = await setup();
    w.store.beforeClaim = () => {
      throw new Error("Connection terminated unexpectedly");
    };
    await assert.rejects(w.deliver(), /Connection terminated unexpectedly/);
    assert.ok(!existsSync(w.dir));
    assert.deepEqual(w.store.writes, []);
  });
});

describe("a failure after the claim (DECISIONS.md 540)", () => {
  const cases: Array<[string, RunJobInput, RegExp]> = [
    ["a module the worker doesn't have", runInput({ run: consoleRun({ modules: ["telepathy"] }) }), /^The worker doesn't know these modules: telepathy\.$/],
    ["a connection that doesn't exist", runInput({ connectionName: "someone-else" }), /^The connection "someone-else" couldn't be loaded: No stored connection called "someone-else"$/],
    ["a connection to another org", runInput({ run: consoleRun({ orgId: "00D000000000099AAA" }) }), /^The connection "synthetic-co" is for a different org from this run, so nothing was read\.$/],
  ];
  for (const [what, input, reason] of cases) {
    test(`${what}, on a run awaiting manual steps: analysing at the claim, then failed with the reason`, async () => {
      const w = await setup(input);
      w.stored().run.status = "awaiting_manual";
      const outcome = await w.deliver();
      assert.equal(outcome.outcome, "failed");
      assert.match(outcome.detail, reason);
      assert.deepEqual(w.store.seen, ["claimRun:analysing", "recordRunFailure:failed"]);
    });

    for (const status of ["in_review", "released"] as const) {
      test(`${what}, on a run ${status.replace("_", " ")} at the load: skipped before the claim, the folder and the store untouched`, async () => {
        const w = await setup(input);
        w.stored().run.status = status;
          mkdirSync(w.dir, { recursive: true });
        plantApexFiles(w.dir);
        const before = snapshot(w.dir);
        assert.equal((await w.deliver()).outcome, "skipped");
        assert.deepEqual(snapshot(w.dir), before);
        assert.deepEqual(w.store.writes, []);
        assert.equal(w.stored().run.status, status);
      });
    }
  }
});

describe("the statuses the store sees, as before the claim", () => {
  // Recorded on 5da8dd7, before the claim, through the same jobs: the store's status after each write.
  // Five more extracting writes since the inventory's five steps (DECISIONS.md 605), and one for PMD and ESLint's (670).
  const progress = (extracting: number): string[] => [...Array<string>(extracting).fill("saveRunProgress:extracting"), "saveRunProgress:analysing", "saveRunProgress:analysing", "saveRunOutput:awaiting_manual"];

  test("a fresh run: the claim leaves it extracting, then every status as before", async () => {
    const w = await setup();
    assert.deepEqual(await w.deliver(false), { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.deepEqual(w.store.seen, ["claimRun:extracting", ...progress(52)]);
  });

  test("a resume after a crash part way, reusing its checkpoints: runs to its save as before", async () => {
    const recording = loadRecording();
    const w = await setup();
    w.deps.dataTransport = new FlakyTransport(new ReplayTransport(recording), 12);
    await assert.rejects(processRunJob(RUN_ID, w.deps, { final: false }), /ECONNRESET/);
    assert.ok(existsSync(join(w.dir, "run.json")));
    // A delivery the queue will make again isn't the run failing, so its last progress write says extracting
    // (DECISIONS.md 702): shown failed, an automatic run would never be delivered again.
    assert.equal(w.stored().run.status, "extracting");
    w.store.seen.length = 0;
    w.deps.dataTransport = new ReplayTransport(recording);
    assert.deepEqual(await processRunJob(RUN_ID, w.deps, { final: false }), { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.deepEqual(w.store.seen, ["claimRun:extracting", ...progress(49)]);
  });

  test("a job retried after its save committed (awaiting manual steps, the folder complete): analysing at the claim, then to its save as before", async () => {
    const w = await setup();
    await w.deliver(false);
    w.store.seen.length = 0;
    assert.deepEqual(await w.deliver(false), { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    assert.deepEqual(w.store.seen, ["claimRun:analysing", ...progress(0)]);
  });
});
