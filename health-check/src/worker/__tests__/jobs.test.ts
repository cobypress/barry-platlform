import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, utimesSync, writeFileSync } from "node:fs";
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

describe("withJobLock after review", () => {
  test("a lock left by an earlier process with this pid (a restarted container) is stale, not held", async () => {
    const dir = temp();
    writeFileSync(join(dir, "run-a.lock"), JSON.stringify({ pid: process.pid, token: "an-earlier-process", at: "2026-11-03T00:00:00.000Z" }));
    assert.equal(await withJobLock(dir, "run-a", async () => "took over"), "took over");
    assert.equal(existsSync(join(dir, "run-a.lock")), false);
  });

  test("two processes that both find a lock stale can't both take it", async () => {
    const dir = temp();
    writeFileSync(join(dir, "run-a.lock"), JSON.stringify({ pid: 999_999, token: "dead", at: "2026-11-03T00:00:00.000Z" }));
    const isAlive = (pid: number) => pid !== 999_999;
    let releaseB!: () => void;
    let b: Promise<void> | undefined;
    const a = withJobLock(dir, "run-a", async () => "a", {
      pid: 111,
      token: "process-a",
      isAlive,
      // B finds the same stale lock while A is deciding, and takes it first.
      onStale: () => {
        b = withJobLock(dir, "run-a", () => new Promise<void>((r) => (releaseB = r)), { pid: 222, token: "process-b", isAlive });
      },
    });
    await assert.rejects(a, JobBusyError);
    assert.match(readFileSync(join(dir, "run-a.lock"), "utf8"), /process-b/);
    releaseB();
    await b;
  });

  test("a breaker left by a crash is cleared after a minute, and a fresh one means wait", async () => {
    const dir = temp();
    const stale = () => writeFileSync(join(dir, "run-a.lock"), JSON.stringify({ pid: 999_999, token: "dead" }));
    stale();
    writeFileSync(join(dir, "run-a.lock.breaker"), "{}");
    await assert.rejects(withJobLock(dir, "run-a", async () => {}, { isAlive: () => false }), JobBusyError);
    const old = new Date(Date.now() - 120_000);
    utimesSync(join(dir, "run-a.lock.breaker"), old, old);
    assert.equal(await withJobLock(dir, "run-a", async () => "ok", { isAlive: () => false }), "ok");
  });

  test("finishing never removes a lock someone else now holds", async () => {
    const dir = temp();
    await withJobLock(dir, "run-a", async () => {
      writeFileSync(join(dir, "run-a.lock"), JSON.stringify({ pid: 1, token: "someone-else" }));
    });
    assert.ok(existsSync(join(dir, "run-a.lock")));
  });
});
