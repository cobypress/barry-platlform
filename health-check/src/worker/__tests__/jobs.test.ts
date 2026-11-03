import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { JobBusyError, withJobLock } from "../lock.ts";
import { BadJobError, parseJob } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, temp } from "./helpers.ts";

const LINK = "00000000-0000-4000-8000-000000000001";

describe("parseJob", () => {
  test("takes the contract's two payloads, ids only", () => {
    assert.deepEqual(parseJob({ kind: "run", runId: "hc-2026-11-03-00000001" }), { kind: "run", runId: "hc-2026-11-03-00000001" });
    assert.deepEqual(parseJob({ kind: "connect", linkId: LINK }), { kind: "connect", linkId: LINK });
    assert.deepEqual(parseJob({ kind: "run", runId: "r1", correlation_id: "health-check-run-r1" }), { kind: "run", runId: "r1" });
  });

  test("reads the console's current step payload as a run job for that run", () => {
    assert.deepEqual(parseJob({ runId: "r1", stepId: "extract", correlation_id: "health-check-step-r1-extract" }), { kind: "run", runId: "r1" });
  });

  test("refuses anything carrying more than ids, or the wrong ids", () => {
    const bad = [
      null,
      "r1",
      { kind: "run" },
      { kind: "run", runId: "r1", orgId: "00D000000000001AAA" },
      { kind: "connect", linkId: "not-a-uuid" },
      { kind: "connect", linkId: LINK, code: "aPrx000" },
      { kind: "delete", runId: "r1" },
      { runId: "r1", stepId: "extract", correlation_id: "x", customer: "Synthetic Co" },
    ];
    for (const data of bad) assert.throws(() => parseJob(data), BadJobError, JSON.stringify(data));
  });

  test("the handler refuses a bad payload before touching anything", async () => {
    const deps = baseDeps();
    await assert.rejects(createJobHandler(deps)({ kind: "run", runId: "r1", extra: 1 }, { final: false }), BadJobError);
    assert.equal(deps.store.writes.length, 0);
  });

  test("the handler refuses folders inside a git work tree (rule 8)", () => {
    assert.throws(() => createJobHandler(baseDeps({ runsDir: join(process.cwd(), "runs") })), /inside the git repository/);
  });
});

describe("withJobLock", () => {
  test("runs the work, and lets go afterwards even if it throws", async () => {
    const dir = temp();
    assert.equal(await withJobLock(dir, "run-a", async () => 1), 1);
    await assert.rejects(withJobLock(dir, "run-a", async () => Promise.reject(new Error("boom"))), /boom/);
    assert.equal(existsSync(join(dir, "run-a.lock")), false);
  });

  test("a held lock makes the second caller wait for a retry", async () => {
    const dir = temp();
    let release!: () => void;
    const first = withJobLock(dir, "run-a", () => new Promise<void>((r) => (release = r)));
    await assert.rejects(withJobLock(dir, "run-a", async () => {}), JobBusyError);
    await assert.rejects(withJobLock(dir, "run-a", async () => {}, { pid: 999_999, isAlive: () => true }), JobBusyError);
    release();
    await first;
  });

  test("a lock left by a process that's gone is taken over", async () => {
    const dir = temp();
    writeFileSync(join(dir, "run-a.lock"), JSON.stringify({ pid: 999_999, at: "2026-11-03T00:00:00.000Z" }));
    assert.equal(await withJobLock(dir, "run-a", async () => "took over", { isAlive: () => false }), "took over");
  });

  test("an unreadable lock is treated as held", async () => {
    const dir = temp();
    writeFileSync(join(dir, "run-a.lock"), "{half");
    await assert.rejects(withJobLock(dir, "run-a", async () => {}, { isAlive: () => false }), JobBusyError);
  });

  test("keys are plain names", async () => {
    await assert.rejects(withJobLock(temp(), "../x", async () => {}), /plain|letters/);
  });
});
