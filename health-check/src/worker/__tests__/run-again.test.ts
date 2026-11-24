import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Release } from "../../schemas/index.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { AGAIN_FILE, AGAIN_NOT_RESET, AGAIN_SUPERSEDED, runAgainToken } from "../run-job.ts";
import { gatedProcessor, processorFor } from "../bullmq.ts";
import { BadJobError, parseJob } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";

// A person's "Run again" of an automatic run reads the org afresh (DECISIONS.md
// 698, for 693's "Run again"): the console sets the run back to extracting,
// clears its hold and queues `{ kind: "run", runId }` under the job id
// `run-<runId>-again-<ms>`, known by that id alone. The folder is cleared once per press;
// everything else resumes exactly as before.

const ORG15 = ORG_ID.slice(0, 15);
const PREVIOUS = "hc-2026-09-30-00000001";
const TOKEN = "1764000000000";
const AGAIN_ID = `run-${RUN_ID}-again-${TOKEN}`;

/** An automatic run held for its score, as a person finds it before pressing "Run again". */
async function heldRun(reviewMode: "automatic" | "consultant" = "automatic") {
  const transport = new AutomaticReplay();
  const deps = baseDeps({ dataTransport: transport, automatic: { on: true, orgs: new Set([ORG15]) } });
  deps.store.addRun(runInput({ run: consoleRun({ id: PREVIOUS, status: "released", startedAt: "2026-09-30T09:00:00.000Z" }) }));
  Object.assign(deps.store.runs.get(PREVIOUS)!, { overallScore: 5, overallGrade: "C" });
  deps.store.releases.set(PREVIOUS, [Release.parse({ id: "rel-0001", runId: PREVIOUS, output: "executive_summary", sectionIds: ["s-0001"], releasedBy: "consultant@example.com", releasedAt: "2026-10-01T09:00:00.000Z" })]);
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  const handle = createJobHandler(deps);
  const first = await handle({ kind: "run", runId: RUN_ID }, { final: false });
  const dir = join(deps.runsDir, RUN_ID);
  // Something only the first reading left, so a fresh start is seen to have cleared it.
  writeFileSync(join(dir, "left-from-before.txt"), "x");
  return { deps, transport, handle, first, dir };
}

/** What the console does before queueing "Run again": back to extracting, the hold cleared. */
function reset(deps: Awaited<ReturnType<typeof heldRun>>["deps"]) {
  deps.store.runs.get(RUN_ID)!.run.status = "extracting";
  deps.store.held.delete(RUN_ID);
}

describe("a person's Run again of an automatic run (DECISIONS.md 698)", () => {
  test("known by its job id alone: run-<runId>-again-<ms>, and nothing in its data", () => {
    assert.equal(runAgainToken(RUN_ID, AGAIN_ID), TOKEN);
    for (const id of [undefined, `run-${RUN_ID}`, `run-${RUN_ID}-1764000000000`, `run-${RUN_ID}-again-`, `run-${RUN_ID}-again-0123`, `run-${RUN_ID}-again-12345678901234567`, `run-${RUN_ID}-again-12a`, `run-hc-2026-10-31-00000002-again-${TOKEN}`, `documents-portal-${RUN_ID}`]) assert.equal(runAgainToken(RUN_ID, id), null, String(id));
    // The data stays ids only: no field is added for it.
    assert.throws(() => parseJob({ kind: "run", runId: RUN_ID, again: TOKEN }), BadJobError);
  });

  test("the queue hands the handler the job's own id, the heavy queue's being the console's", async () => {
    const seen: Array<string | undefined> = [];
    const process = processorFor(async (_data, attempt) => (seen.push(attempt.jobId), { outcome: "done", detail: "" }), { UnrecoverableError: Error }, () => {});
    await process({ id: AGAIN_ID, name: "health-check-run", data: { kind: "run", runId: RUN_ID }, attemptsMade: 0, opts: { attempts: 3 } });
    assert.deepEqual(seen, [AGAIN_ID]);
    const added: Array<Record<string, unknown>> = [];
    const gated = gatedProcessor(async () => ({ outcome: "done", detail: "" }), { UnrecoverableError: Error }, { add: async (_n, _d, opts) => void added.push(opts) }, () => {});
    await gated({ id: AGAIN_ID, name: "health-check-run", data: { kind: "run", runId: RUN_ID }, attemptsMade: 0, opts: { attempts: 3 } });
    assert.equal(added[0]!.jobId, AGAIN_ID);
  });

  test("set back to extracting with its hold cleared, it starts from a clean folder and reads the org again", async () => {
    const { deps, transport, handle, first, dir } = await heldRun();
    assert.deepEqual(first, { outcome: "done", detail: `Run ${RUN_ID} is held: score_moved` });
    reset(deps);
    const readBefore = transport.requests.length;
    const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.ok(!existsSync(join(dir, "left-from-before.txt")));
    assert.deepEqual(JSON.parse(readFileSync(join(dir, AGAIN_FILE), "utf8")), { again: TOKEN });
    assert.ok(transport.requests.length > readBefore, "the org was read again");
    assert.ok(deps.lines.includes(`Run ${RUN_ID} is run again from the start: its folder was cleared and the org is read afresh.`));
  });

  describe("two presses, in each order: a newer press owns the run, and an older one never clears its work", () => {
    const OLDER = `run-${RUN_ID}-again-1764000000000`;
    const NEWER = `run-${RUN_ID}-again-1764000000500`;
    const mark = (dir: string) => JSON.parse(readFileSync(join(dir, AGAIN_FILE), "utf8")) as { again: string };

    test("older then newer: the newer press starts afresh over the older one's folder", async () => {
      const { deps, handle, dir } = await heldRun();
      reset(deps);
      await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: OLDER });
      writeFileSync(join(dir, "left-from-before.txt"), "older");
      reset(deps);
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: NEWER });
      assert.notEqual(outcome.outcome, "skipped", outcome.detail);
      assert.ok(!existsSync(join(dir, "left-from-before.txt")));
      assert.deepEqual(mark(dir), { again: "1764000000500" });
    });

    test("the older press's retry after the newer one: skipped, nothing read, the newer press's folder as it was", async () => {
      const { deps, transport, handle, dir } = await heldRun();
      reset(deps);
      await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: OLDER });
      reset(deps);
      await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: NEWER });
      writeFileSync(join(dir, "left-from-before.txt"), "newer");
      reset(deps);
      const read = transport.requests.length;
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: OLDER });
      assert.deepEqual(outcome, { outcome: "skipped", detail: `Run ${RUN_ID}: ${AGAIN_SUPERSEDED}` });
      assert.equal(transport.requests.length, read);
      assert.ok(existsSync(join(dir, "left-from-before.txt")));
      assert.deepEqual(mark(dir), { again: "1764000000500" });
      assert.ok(deps.lines.includes(`Run ${RUN_ID}: a newer "Run again" owns this run, so this older one read nothing and left its folder as it was.`));
    });

    test("newer then an older press arriving late: the older is skipped", async () => {
      const { deps, transport, handle, dir } = await heldRun();
      reset(deps);
      await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: NEWER });
      writeFileSync(join(dir, "left-from-before.txt"), "newer");
      reset(deps);
      const read = transport.requests.length;
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: OLDER });
      assert.equal(outcome.detail, `Run ${RUN_ID}: ${AGAIN_SUPERSEDED}`);
      assert.equal(transport.requests.length, read);
      assert.ok(existsSync(join(dir, "left-from-before.txt")));
    });
  });

  test("moved on between its claim and the clean start (another delivery, a hold): skipped, the folder as it was", async () => {
    for (const change of [(i: { run: { status: string }; heldReason?: string | null }) => (i.run.status = "analysing"), (i: { run: { status: string }; heldReason?: string | null }) => (i.heldReason = "score_moved")]) {
      const { deps, transport, handle, dir } = await heldRun();
      reset(deps);
      // The second reading, after the claim, sees the change; the first saw a run set back and unheld.
      const load = deps.store.loadRunJob.bind(deps.store);
      let reads = 0;
      deps.store.loadRunJob = async (id) => {
        const input = await load(id);
        if (input && ++reads === 2) change(input as never);
        return input;
      };
      const read = transport.requests.length;
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
      assert.equal(outcome.outcome, "skipped", outcome.detail);
      assert.equal(transport.requests.length, read);
      assert.ok(existsSync(join(dir, "left-from-before.txt")));
      assert.ok(!existsSync(join(dir, AGAIN_FILE)));
    }
  });

  test("a retry of the same Run again resumes from where it got to; only a new token starts afresh", async () => {
    const { deps, handle, dir } = await heldRun();
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    writeFileSync(join(dir, "left-from-before.txt"), "y");
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    assert.ok(existsSync(join(dir, "left-from-before.txt")), "the same token resumed");
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: `run-${RUN_ID}-again-1764000000001` });
    assert.ok(!existsSync(join(dir, "left-from-before.txt")), "a new token started afresh");
  });

  test("not set back to extracting: skipped, nothing read, the folder as it was", async () => {
    const { deps, transport, handle, dir } = await heldRun();
    deps.store.held.delete(RUN_ID);
    for (const status of ["analysing", "in_review", "awaiting_manual", "stopped_budget"] as const) {
      deps.store.runs.get(RUN_ID)!.run.status = status;
      const readBefore = transport.requests.length;
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
      assert.equal(outcome.outcome, "skipped", status);
      if (status === "analysing" || status === "awaiting_manual" || status === "stopped_budget") assert.equal(outcome.detail, `Run ${RUN_ID}: ${AGAIN_NOT_RESET}`);
      assert.equal(transport.requests.length, readBefore, status);
      assert.ok(existsSync(join(dir, "left-from-before.txt")), status);
    }
  });

  test("still held, or still failed: skipped as 693 says, nothing read, the folder as it was", async () => {
    const { deps, transport, handle, dir } = await heldRun();
    deps.store.runs.get(RUN_ID)!.run.status = "extracting";
    const readBefore = transport.requests.length;
    const held = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    assert.match(held.detail, /is held \(score_moved\)/);
    deps.store.held.delete(RUN_ID);
    deps.store.runs.get(RUN_ID)!.run.status = "failed";
    const failed = await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    assert.match(failed.detail, /failed, so it waits for a person/);
    assert.equal(transport.requests.length, readBefore);
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
  });

  test("the switch off still refuses it before anything is cleared or read", async () => {
    const { deps, transport, handle, dir } = await heldRun();
    reset(deps);
    deps.automatic = { on: false, orgs: new Set() };
    const readBefore = transport.requests.length;
    const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: true, jobId: AGAIN_ID });
    assert.equal(outcome.outcome, "failed");
    assert.equal(transport.requests.length, readBefore);
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
  });

  test("without the token, the run resumes exactly as before", async () => {
    const { deps, handle, dir } = await heldRun();
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
    assert.ok(!existsSync(join(dir, AGAIN_FILE)));
  });

  test("a consultant-led run given the token resumes exactly as before: nothing cleared", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    deps.store.addRun(runInput({ answers: loadAnswers() }));
    await deps.tokens.save(syntheticConnection());
    const handle = createJobHandler(deps);
    await handle({ kind: "run", runId: RUN_ID }, { final: false });
    const dir = join(deps.runsDir, RUN_ID);
    writeFileSync(join(dir, "left-from-before.txt"), "x");
    deps.store.runs.get(RUN_ID)!.run.status = "extracting";
    await handle({ kind: "run", runId: RUN_ID }, { final: false, jobId: AGAIN_ID });
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
    assert.ok(!existsSync(join(dir, AGAIN_FILE)));
    assert.ok(deps.lines.includes(`Run ${RUN_ID} isn't automatic, so "Run again" resumes it as any delivery does.`));
  });
});
