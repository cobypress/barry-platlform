import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Release } from "../../schemas/index.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { AGAIN_FILE, AGAIN_NOT_RESET } from "../run-job.ts";
import { BadJobError, parseJob } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";

// A person's "Run again" of an automatic run reads the org afresh (DECISIONS.md
// 698, for 693's "Run again"): the console sets the run back to extracting,
// clears its hold and queues `{ kind: "run", runId, again: "<token>" }` under
// the job id `run-<runId>-again-<token>`. The folder is cleared once per token;
// everything else resumes exactly as before.

const ORG15 = ORG_ID.slice(0, 15);
const PREVIOUS = "hc-2026-09-30-00000001";
const TOKEN = "1764000000000";

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
  test("the job carries its token, and nothing else", () => {
    assert.deepEqual(parseJob({ kind: "run", runId: RUN_ID, again: TOKEN }), { kind: "run", runId: RUN_ID, again: TOKEN });
    assert.deepEqual(parseJob({ kind: "run", runId: RUN_ID }), { kind: "run", runId: RUN_ID });
    for (const again of ["", "a b", "x".repeat(65), 5]) assert.throws(() => parseJob({ kind: "run", runId: RUN_ID, again }), BadJobError);
  });

  test("set back to extracting with its hold cleared, it starts from a clean folder and reads the org again", async () => {
    const { deps, transport, handle, first, dir } = await heldRun();
    assert.deepEqual(first, { outcome: "done", detail: `Run ${RUN_ID} is held: score_moved` });
    reset(deps);
    const readBefore = transport.requests.length;
    const outcome = await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.ok(!existsSync(join(dir, "left-from-before.txt")));
    assert.deepEqual(JSON.parse(readFileSync(join(dir, AGAIN_FILE), "utf8")), { again: TOKEN });
    assert.ok(transport.requests.length > readBefore, "the org was read again");
    assert.ok(deps.lines.includes(`Run ${RUN_ID} is run again from the start: its folder was cleared and the org is read afresh.`));
  });

  test("a retry of the same Run again resumes from where it got to; only a new token starts afresh", async () => {
    const { deps, handle, dir } = await heldRun();
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    writeFileSync(join(dir, "left-from-before.txt"), "y");
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    assert.ok(existsSync(join(dir, "left-from-before.txt")), "the same token resumed");
    reset(deps);
    await handle({ kind: "run", runId: RUN_ID, again: "1764000000001" }, { final: false });
    assert.ok(!existsSync(join(dir, "left-from-before.txt")), "a new token started afresh");
  });

  test("not set back to extracting: skipped, nothing read, the folder as it was", async () => {
    const { deps, transport, handle, dir } = await heldRun();
    deps.store.held.delete(RUN_ID);
    for (const status of ["analysing", "in_review", "awaiting_manual", "stopped_budget"] as const) {
      deps.store.runs.get(RUN_ID)!.run.status = status;
      const readBefore = transport.requests.length;
      const outcome = await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
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
    const held = await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    assert.match(held.detail, /is held \(score_moved\)/);
    deps.store.held.delete(RUN_ID);
    deps.store.runs.get(RUN_ID)!.run.status = "failed";
    const failed = await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    assert.match(failed.detail, /failed, so it waits for a person/);
    assert.equal(transport.requests.length, readBefore);
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
  });

  test("the switch off still refuses it before anything is cleared or read", async () => {
    const { deps, transport, handle, dir } = await heldRun();
    reset(deps);
    deps.automatic = { on: false, orgs: new Set() };
    const readBefore = transport.requests.length;
    const outcome = await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: true });
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
    await handle({ kind: "run", runId: RUN_ID, again: TOKEN }, { final: false });
    assert.ok(existsSync(join(dir, "left-from-before.txt")));
    assert.ok(!existsSync(join(dir, AGAIN_FILE)));
    assert.ok(deps.lines.includes(`Run ${RUN_ID} isn't automatic, so "Run again" resumes it as any delivery does.`));
  });
});
