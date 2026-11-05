import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { PURGE_FILE } from "../../retention/purge.ts";
import type { BenchmarkRow } from "../../schemas/index.ts";
import { withJobLock } from "../lock.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { addMonths, dueFor, retentionSweep, type RetentionOutcome } from "../retention.ts";
import { consoleRun, runInput, temp } from "./helpers.ts";

const DAY = 86_400_000;
const RELEASED = "2026-01-31T10:00:00.000Z";
const at = (days: number): Date => new Date(new Date(RELEASED).getTime() + days * DAY);

/** A run folder as the worker leaves it: run.json, raw extracts, findings and the pseudonym map. */
function runFolder(root: string, id: string, runJsonId = id): string {
  const dir = join(root, id);
  mkdirSync(join(dir, "checkpoints"), { recursive: true });
  writeFileSync(join(dir, "run.json"), JSON.stringify(consoleRun({ id: runJsonId, status: "released" })));
  for (const file of ["opportunities.json", "users.json", "licences.json"]) writeFileSync(join(dir, "checkpoints", file), "[]");
  writeFileSync(join(dir, "findings.json"), "[]");
  writeFileSync(join(dir, "pseudonym-map.json"), "[]");
  return dir;
}

function setup() {
  const root = temp("hc-retention-");
  const runs = join(root, "runs");
  mkdirSync(runs);
  const store = new MemoryWorkerStore();
  const lines: string[] = [];
  const release = (id: string, releasedAt: string | null = RELEASED, status: "released" | "in_review" = "released"): string => {
    store.addRun(runInput({ run: consoleRun({ id, status }) }));
    if (releasedAt) store.releasedAt.set(id, releasedAt);
    return runFolder(runs, id);
  };
  const sweep = (now: Date, dryRun = false, locksDir?: string): Promise<RetentionOutcome[]> => retentionSweep({ store, runsDir: runs, now, dryRun, log: (l) => lines.push(l), ...(locksDir ? { locksDir } : {}) });
  return { root, runs, store, lines, release, sweep };
}

const action = (outcomes: RetentionOutcome[], id: string) => outcomes.find((o) => o.runId === id)?.action;

describe("the retention sweep, on a fake clock", () => {
  test("a released run keeps everything for 30 days, then loses only its raw extracts", async () => {
    const { release, sweep } = setup();
    const dir = release("hc-2026-01-01-00000001");
    assert.equal(action(await sweep(at(29.9)), "hc-2026-01-01-00000001"), "kept");
    assert.ok(existsSync(join(dir, "checkpoints", "opportunities.json")));
    const day30 = await sweep(at(30));
    assert.equal(action(day30, "hc-2026-01-01-00000001"), "extracts_deleted");
    assert.equal(day30[0]!.files, 3);
    assert.ok(!existsSync(join(dir, "checkpoints")));
    for (const kept of ["run.json", "findings.json", "pseudonym-map.json", PURGE_FILE]) assert.ok(existsSync(join(dir, kept)), kept);
    assert.equal(action(await sweep(at(31)), "hc-2026-01-01-00000001"), "already_purged");
  });

  test("and the whole folder at 12 months, and not a day before", async () => {
    const { release, sweep } = setup();
    const dir = release("hc-2026-01-01-00000002");
    // Released 31 January: 12 months on is 31 January next year.
    assert.notEqual(action(await sweep(new Date("2027-01-31T09:59:59.999Z")), "hc-2026-01-01-00000002"), "folder_deleted");
    assert.ok(existsSync(dir));
    const due = await sweep(new Date("2027-01-31T10:00:00.000Z"));
    assert.equal(action(due, "hc-2026-01-01-00000002"), "folder_deleted");
    assert.ok(!existsSync(dir));
    assert.equal(action(await sweep(new Date("2027-02-01T00:00:00.000Z")), "hc-2026-01-01-00000002"), "no_folder");
  });

  test("a run that isn't released, or whose release date can't be read, is never touched", async () => {
    const { release, sweep } = setup();
    const reviewing = release("hc-2026-01-01-00000003", RELEASED, "in_review");
    const undated = release("hc-2026-01-01-00000004", null);
    const garbled = release("hc-2026-01-01-00000005", "not a date");
    const outcomes = await sweep(at(800));
    assert.equal(action(outcomes, "hc-2026-01-01-00000003"), undefined, "an unreleased run isn't even listed");
    assert.equal(action(outcomes, "hc-2026-01-01-00000004"), "no_release_date");
    assert.equal(action(outcomes, "hc-2026-01-01-00000005"), "no_release_date");
    for (const dir of [reviewing, undated, garbled]) assert.ok(existsSync(join(dir, "checkpoints", "opportunities.json")), dir);
  });

  test("refuses anything outside the runs folder, a linked folder, or a folder holding another run", async () => {
    const { root, runs, store, release, sweep } = setup();
    // An id that would climb out of the runs folder.
    store.addRun(runInput({ run: consoleRun({ id: "../outside", status: "released" }) }));
    store.releasedAt.set("../outside", RELEASED);
    const outside = runFolder(root, "outside", "../outside");
    // A run folder that's a link to somewhere else.
    const target = runFolder(join(root, "elsewhere"), "hc-2026-01-01-00000006");
    store.addRun(runInput({ run: consoleRun({ id: "hc-2026-01-01-00000006", status: "released" }) }));
    store.releasedAt.set("hc-2026-01-01-00000006", RELEASED);
    symlinkSync(target, join(runs, "hc-2026-01-01-00000006"));
    // A folder whose run.json names a different run.
    store.addRun(runInput({ run: consoleRun({ id: "hc-2026-01-01-00000007", status: "released" }) }));
    store.releasedAt.set("hc-2026-01-01-00000007", RELEASED);
    const other = runFolder(runs, "hc-2026-01-01-00000007", "hc-2026-01-01-00000099");
    const kept = release("hc-2026-01-01-00000008");

    const outcomes = await sweep(at(800));
    assert.equal(action(outcomes, "../outside"), "refused");
    assert.equal(action(outcomes, "hc-2026-01-01-00000006"), "refused");
    assert.equal(action(outcomes, "hc-2026-01-01-00000007"), "refused");
    assert.equal(action(outcomes, "hc-2026-01-01-00000008"), "folder_deleted");
    for (const dir of [outside, target, other]) assert.ok(existsSync(join(dir, "checkpoints", "opportunities.json")), dir);
    assert.ok(!existsSync(kept));
  });

  test("a dry run deletes nothing, and says what it would", async () => {
    const { release, sweep, lines } = setup();
    const young = release("hc-2026-01-01-00000009", at(-40).toISOString());
    const old = release("hc-2026-01-01-00000010", at(-400).toISOString());
    const outcomes = await sweep(at(0), true);
    assert.equal(action(outcomes, "hc-2026-01-01-00000009"), "extracts_deleted");
    assert.equal(action(outcomes, "hc-2026-01-01-00000010"), "folder_deleted");
    assert.ok(outcomes.every((o) => o.dryRun && o.files === 0));
    for (const dir of [young, old]) assert.deepEqual(readdirSync(join(dir, "checkpoints")).length, 3, dir);
    assert.ok(lines.some((l) => /run hc-2026-01-01-00000009: would delete raw extracts, 30 days after release/.test(l)));
    assert.ok(lines.some((l) => /run hc-2026-01-01-00000010: would delete the whole run folder, 12 months after release/.test(l)));
    assert.ok(lines.some((l) => /dry run, nothing deleted/.test(l)));
  });

  test("logs counts and run ids only: no path, and nothing from inside a run", async () => {
    const { runs, release, sweep, lines } = setup();
    release("hc-2026-01-01-00000011");
    release("hc-2026-01-01-00000012", null);
    await sweep(at(45));
    const log = lines.join("\n");
    assert.ok(!log.includes(runs) && !log.includes("/"), log);
    assert.match(log, /Retention sweep: 2 released runs\. Raw extracts deleted: 1\. Run folders deleted: 0\. Kept: 0\. No release date we can read: 1\. Busy: 0\. Refused: 0\./);
  });

  test("leaves a run a job is working on, for the next sweep", async () => {
    const { root, release, sweep } = setup();
    const dir = release("hc-2026-01-01-00000013");
    const locks = join(root, "locks");
    let finish!: () => void;
    const holding = withJobLock(locks, "run-hc-2026-01-01-00000013", () => new Promise<void>((r) => (finish = r)));
    const outcomes = await sweep(at(400), false, locks);
    assert.equal(action(outcomes, "hc-2026-01-01-00000013"), "busy");
    assert.ok(existsSync(dir));
    finish();
    await holding;
    assert.equal(action(await sweep(at(400), false, locks), "hc-2026-01-01-00000013"), "folder_deleted");
  });

  test("never touches benchmark rows: it can't reach them", async () => {
    const { store, release, sweep } = setup();
    const row = { metricKey: "historic.win_rate", ruleSetVersion: "rs", value: 20, unit: "percent", industry: "technology", headcountBand: "51-200", userBand: "1-50", dealSizeBand: "5k-25k", period: "2026-Q1" } as BenchmarkRow;
    await store.addBenchmarkRows([row]);
    release("hc-2026-01-01-00000014");
    await sweep(at(800));
    assert.deepEqual(await store.loadBenchmarkRows(), [row]);
  });
});

describe("dates", () => {
  test("12 months is the same day next year, or the month's last day", () => {
    assert.equal(addMonths(new Date("2026-01-31T10:00:00.000Z"), 12).toISOString(), "2027-01-31T10:00:00.000Z");
    assert.equal(addMonths(new Date("2028-02-29T00:00:00.000Z"), 12).toISOString(), "2029-02-28T00:00:00.000Z");
    assert.equal(addMonths(new Date("2026-03-31T00:00:00.000Z"), 1).toISOString(), "2026-04-30T00:00:00.000Z");
  });

  test("what's due", () => {
    const released = new Date(RELEASED);
    assert.equal(dueFor(released, at(29)), "nothing");
    assert.equal(dueFor(released, at(30)), "extracts");
    assert.equal(dueFor(released, at(364)), "extracts");
    assert.equal(dueFor(released, at(365)), "folder");
  });
});

describe("released runs from the console's database", () => {
  test("only released runs, with their last output's release date, or null", async () => {
    const statements: string[] = [];
    const pool = {
      async query(text: string): Promise<SqlResult> {
        statements.push(text);
        return { rows: [{ runId: "hc-a", releasedAt: "2026-01-31T10:00:00.000Z" }, { runId: "hc-b", releasedAt: null }], rowCount: 2 };
      },
    } as unknown as SqlPool;
    assert.deepEqual(await new PostgresWorkerStore(pool).listReleasedRuns(), [
      { runId: "hc-a", releasedAt: "2026-01-31T10:00:00.000Z" },
      { runId: "hc-b", releasedAt: null },
    ]);
    assert.match(statements[0]!, /WHERE r\."status" = 'released'/);
    assert.match(statements[0]!, /MAX\(rel\."releasedAt"\)/);
    assert.match(statements[0]!, /LEFT JOIN "HealthCheckRelease"/);
  });
});
