import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun, OUTPUT_FILES, renderRun } from "../../render/render-run.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { PURGE_FILE, PurgeRecord, purgeRun, sweepRuns } from "../purge.ts";

const { dir: source } = await runFromRecording({ answers: loadAnswers() });

const NOW = new Date("2026-12-31T12:00:00.000Z");
const DAY = 86_400_000;

/** A copy of the synthetic run, optionally dated `ageDays` before NOW. */
function freshRun(parent: string = tempDir(), name = "run", ageDays?: number): string {
  const dir = join(parent, name);
  cpSync(source, dir, { recursive: true });
  const path = join(dir, "run.json");
  const run = JSON.parse(readFileSync(path, "utf8")) as Record<string, unknown>;
  const asOf = ageDays === undefined ? run.asOf : new Date(NOW.getTime() - ageDays * DAY).toISOString();
  writeFileSync(path, JSON.stringify({ ...run, id: name, asOf }));
  return dir;
}

/** Every file under a folder with a hash of its contents. */
function snapshot(dir: string): Map<string, string> {
  const files = readdirSync(dir, { recursive: true, withFileTypes: true }).filter((e) => e.isFile());
  return new Map(files.map((e) => {
    const path = join(e.parentPath, e.name);
    return [relative(dir, path), createHash("sha256").update(readFileSync(path)).digest("hex")];
  }));
}

describe("hc purge --run", () => {
  test("deletes checkpoints and nothing else, then records when", () => {
    const run = freshRun();
    const before = snapshot(run);
    const raw = [...before.keys()].filter((p) => p.startsWith("checkpoints/"));
    assert.ok(raw.length > 10);

    const result = purgeRun(run, NOW);
    assert.equal(result.purged, true);
    assert.equal(result.filesDeleted, raw.length);
    assert.ok(!existsSync(join(run, "checkpoints")));

    const after = snapshot(run);
    const expected = new Map([...before].filter(([p]) => !p.startsWith("checkpoints/")));
    const { [PURGE_FILE]: record, ...kept } = Object.fromEntries(after);
    assert.deepEqual(new Map(Object.entries(kept)), expected, "every other file is unchanged");
    assert.ok(record);
    for (const file of ["findings.json", "metrics.json", "scores.json", "calibration.json", "pseudonym-map.json", "raw-notes.md", "run.json"]) assert.ok(after.has(file), file);

    const parsed = PurgeRecord.parse(JSON.parse(readFileSync(join(run, PURGE_FILE), "utf8")));
    assert.deepEqual(parsed, { purgedAt: NOW.toISOString(), deleted: "checkpoints/", filesDeleted: raw.length });
    assert.equal(statSync(join(run, PURGE_FILE)).mode & 0o777, 0o600);
  });

  test("leaves a purged run alone the second time", () => {
    const run = freshRun();
    purgeRun(run, NOW);
    const record = readFileSync(join(run, PURGE_FILE), "utf8");
    const again = purgeRun(run, new Date(NOW.getTime() + DAY));
    assert.equal(again.purged, false);
    assert.equal(again.note, "already purged");
    assert.equal(readFileSync(join(run, PURGE_FILE), "utf8"), record);
  });

  test("refuses a folder with no run.json, or a broken one, and deletes nothing", () => {
    const plain = tempDir();
    mkdirSync(join(plain, "checkpoints"));
    writeFileSync(join(plain, "checkpoints", "keep.json"), "{}");
    assert.throws(() => purgeRun(plain), /isn't a run folder: it has no run\.json/);
    writeFileSync(join(plain, "run.json"), JSON.stringify({ id: "x" }));
    assert.throws(() => purgeRun(plain), /isn't a valid run/);
    writeFileSync(join(plain, "run.json"), "not json");
    assert.throws(() => purgeRun(plain), /isn't a valid run/);
    assert.ok(existsSync(join(plain, "checkpoints", "keep.json")));
    assert.ok(!existsSync(join(plain, PURGE_FILE)));
    assert.throws(() => purgeRun(join(plain, "missing")), /isn't a folder/);
  });

  test("refuses checkpoints that are a link, and leaves what it points at", () => {
    const run = freshRun();
    const elsewhere = join(tempDir(), "elsewhere");
    cpSync(join(run, "checkpoints"), elsewhere, { recursive: true });
    const count = readdirSync(elsewhere).length;
    rmSync(join(run, "checkpoints"), { recursive: true });
    symlinkSync(elsewhere, join(run, "checkpoints"));
    assert.throws(() => purgeRun(run), /isn't a plain folder/);
    assert.equal(readdirSync(elsewhere).length, count);
    assert.ok(!existsSync(join(run, PURGE_FILE)));
  });

  test("records a run whose checkpoints are already gone", () => {
    const run = freshRun();
    rmSync(join(run, "checkpoints"), { recursive: true });
    const result = purgeRun(run, NOW);
    assert.equal(result.purged, true);
    assert.equal(result.filesDeleted, 0);
    assert.ok(existsSync(join(run, PURGE_FILE)));
  });

  test("a purged run still loads and renders, because rendering never reads checkpoints", async () => {
    const run = freshRun();
    purgeRun(run, NOW);
    const { input } = loadRun(run);
    assert.ok(buildReport(input));
    const result = await renderRun(run);
    assert.equal(result.passed, true, JSON.stringify(result.layout));
    for (const file of Object.values(OUTPUT_FILES)) assert.ok(statSync(join(run, file)).size > 0, file);
  });
});

describe("hc purge --older-than", () => {
  test("purges only runs older than the limit that aren't purged yet", () => {
    const runs = tempDir();
    const old = freshRun(runs, "hc-old", 45);
    const young = freshRun(runs, "hc-young", 10);
    const edge = freshRun(runs, "hc-edge", 30);
    const done = freshRun(runs, "hc-done", 90);
    purgeRun(done, new Date(NOW.getTime() - 50 * DAY));
    const doneRecord = readFileSync(join(done, PURGE_FILE), "utf8");

    const result = sweepRuns(runs, 30, NOW);
    assert.deepEqual(result.purged.map((p) => p.runId), ["hc-old"]);
    assert.equal(result.kept, 3);
    assert.ok(!existsSync(join(old, "checkpoints")));
    assert.ok(existsSync(join(young, "checkpoints")), "a run inside the limit keeps its extracts");
    assert.ok(existsSync(join(edge, "checkpoints")), "exactly 30 days isn't older than 30 days");
    assert.equal(readFileSync(join(done, PURGE_FILE), "utf8"), doneRecord);

    assert.deepEqual(sweepRuns(runs, 5, NOW).purged.map((p) => p.runId), ["hc-edge", "hc-young"]);
  });

  test("never touches a folder that isn't a run, or a link to one", () => {
    const runs = tempDir();
    const stray = join(runs, "not-a-run");
    mkdirSync(join(stray, "checkpoints"), { recursive: true });
    writeFileSync(join(stray, "checkpoints", "keep.json"), "{}");
    writeFileSync(join(runs, "loose-file.json"), "{}");
    const outside = freshRun(tempDir(), "hc-outside", 400);
    symlinkSync(outside, join(runs, "hc-linked"));

    const result = sweepRuns(runs, 30, NOW);
    assert.deepEqual(result.purged, []);
    assert.deepEqual(result.skipped.map((s) => s.folder), ["not-a-run"]);
    assert.ok(existsSync(join(stray, "checkpoints", "keep.json")));
    assert.ok(existsSync(join(outside, "checkpoints")));
  });

  test("needs a whole number of days, and a folder that exists", () => {
    const runs = tempDir();
    for (const days of [0, -1, 1.5, Number.NaN]) assert.throws(() => sweepRuns(runs, days, NOW), /whole number of days/);
    assert.throws(() => sweepRuns(join(runs, "missing"), 30, NOW), /isn't a folder/);
  });
});

describe("the command line", () => {
  const service = fileURLToPath(new URL("../../..", import.meta.url));
  const hc = (args: string[]) => spawnSync(process.execPath, ["src/cli/hc.ts", ...args], { cwd: service, encoding: "utf8", env: { PATH: process.env.PATH, HOME: tempDir() } });

  test("hc purge --older-than prints the run ids it purged", () => {
    const runs = tempDir();
    // Dated from today, since the command line uses the real clock.
    const now = Date.now();
    for (const [name, age] of [["hc-a", 40], ["hc-b", 2]] as const) {
      const dir = join(runs, name);
      cpSync(source, dir, { recursive: true });
      const run = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Record<string, unknown>;
      writeFileSync(join(dir, "run.json"), JSON.stringify({ ...run, id: name, asOf: new Date(now - age * DAY).toISOString() }));
    }
    const result = hc(["purge", "--older-than", "30", "--runs", runs]);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /^Purged hc-a: \d+ raw extract files deleted\.$/m);
    assert.doesNotMatch(result.stdout, /hc-b:/);
    assert.match(result.stdout, /1 purged\. 1 left alone/);
    assert.ok(existsSync(join(runs, "hc-b", "checkpoints")));
  });

  test("hc purge --run refuses a folder that isn't a run", () => {
    const result = hc(["purge", "--run", tempDir()]);
    assert.equal(result.status, 2);
    assert.match(result.stderr, /isn't a run folder/);
  });

  test("hc purge with nothing to do prints usage", () => {
    assert.equal(hc(["purge"]).status, 2);
  });
});
