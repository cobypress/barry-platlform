import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { apiLimitsExtractor } from "../../extract/integrations.ts";
import { LIMITS_AT_START_FILE, parseLimitsReading } from "../../extract/types.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { benchmarkFigures } from "../../benchmark/export.ts";
import type { Finding } from "../../schemas/index.ts";
import { Inventory } from "../../schemas/inventory.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// Org limits (KAN-45, DECISIONS.md 726): every limit /limits lists is kept in
// the inventory; issues are raised only for storage, a level, and for a daily
// allowance at or near exhaustion when this check first connected. Each
// threshold is written out here as a number, not imported, so a change to one
// fails a boundary below.

type Limit = { Max: number; Remaining: number };
const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);

/** The synthetic run's findings with /limits as given: only these entries. */
const withLimits = (limits: Record<string, Limit>): Finding[] => {
  const changed = clone(extracts);
  (changed.get(apiLimitsExtractor.id) as { data: Record<string, Limit> }).data = limits;
  return analyseExtracts(changed).findings;
};
const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const FINE = { DailyApiRequests: { Max: 100_000, Remaining: 99_000 }, DailyAsyncApexExecutions: { Max: 250_000, Remaining: 200_000 } };
const ROOMY_STORAGE = { DataStorageMB: { Max: 10_000, Remaining: 9_000 }, FileStorageMB: { Max: 10_000, Remaining: 9_000 } };
const storage = (usedData: number): Finding => one(withLimits({ ...FINE, ...ROOMY_STORAGE, DataStorageMB: { Max: 10_000, Remaining: 10_000 - usedData } }), "tech.storage-use");
const api = (used: number, max = 100_000): Finding => one(withLimits({ ...ROOMY_STORAGE, ...FINE, DailyApiRequests: { Max: max, Remaining: max - used } }), "int.api-usage");
const outcome = (f: Finding): string => (f.status === "issue" ? f.severity : f.status);

describe("storage, a level: medium from 80% used, high from 95%", () => {
  test("each boundary", () => {
    assert.equal(outcome(storage(7_990)), "passed", "79.9%");
    assert.equal(outcome(storage(8_000)), "medium", "80%");
    assert.equal(outcome(storage(9_490)), "medium", "94.9%");
    assert.equal(outcome(storage(9_500)), "high", "95%");
    assert.equal(outcome(storage(10_000)), "high", "100%");
    const past = storage(10_500);
    assert.equal(outcome(past), "high", "past the allowance");
    assert.equal(past.title, "Storage is past the allowance");
  });

  test("a kind whose maximum is 0 or absent is left out: neither an issue nor a pass", () => {
    assert.equal(one(withLimits({ ...FINE, DataStorageMB: { Max: 0, Remaining: 0 }, FileStorageMB: { Max: 0, Remaining: 0 } }), "tech.storage-use").status, "not_measurable");
    assert.equal(one(withLimits({ ...FINE }), "tech.storage-use").status, "not_measurable");
    const fileOnly = one(withLimits({ ...FINE, DataStorageMB: { Max: 0, Remaining: 0 }, FileStorageMB: { Max: 10_000, Remaining: 1_000 } }), "tech.storage-use");
    assert.equal(fileOnly.status, "issue");
    assert.deepEqual(fileOnly.subjects.map((s) => s.label), ["file storage"]);
    assert.equal(fileOnly.facts.DataStorageMBShare, undefined);
  });

  test("storage clean-up is never a quick fix (decision 714)", () => {
    const f = one(output!.findings, "tech.storage-use");
    assert.equal(f.status, "issue");
    assert.notEqual(f.difficulty, "quick_fix");
  });
});

describe("a daily allowance: one reading, an issue only at or near exhaustion", () => {
  test("60% and 94.9% pass; 95% and 99.9% are medium; all used is high and used up; past it says so", () => {
    assert.equal(outcome(api(60_000)), "passed", "60%");
    assert.equal(outcome(api(94_900)), "passed", "94.9%");
    const near = api(95_000);
    assert.equal(outcome(near), "medium", "95%");
    assert.equal(outcome(api(99_900)), "medium", "99.9%");
    assert.equal(near.title, "A daily allowance is nearly used up");
    assert.match(near.summary, /^When this check first connected, the org had used 95% of its daily API requests \(95,000 of 100,000\), as read at that moment\. /);
    const all = api(100_000);
    assert.equal(outcome(all), "high");
    assert.equal(all.title, "A daily allowance is used up");
    const over = api(100_500);
    assert.equal(over.title, "A daily allowance is used up");
    assert.match(over.summary, /100\.5% of its daily API requests \(100,500, over the 100,000 allowance\)/);
  });

  test("async Apex in the synthetic org, 96% used, is the trap: medium, and API requests are left out of it", () => {
    const f = one(output!.findings, "int.api-usage");
    assert.equal(outcome(f), "medium");
    assert.deepEqual(f.subjects.map((s) => s.label), ["asynchronous Apex runs"]);
    assert.match(f.summary, /96% of its daily asynchronous Apex runs \(240,000 of 250,000\)/);
    assert.notEqual(f.difficulty, "quick_fix");
  });

  test("an allowance whose maximum is 0 is not applicable: both at 0 is not measurable, never a pass", () => {
    const zero = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 0, Remaining: 0 }, DailyAsyncApexExecutions: { Max: 0, Remaining: 0 } }), "int.api-usage");
    assert.equal(zero.status, "not_measurable");
    const oneZero = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 100_000, Remaining: 1_000 }, DailyAsyncApexExecutions: { Max: 0, Remaining: 0 } }), "int.api-usage");
    assert.equal(outcome(oneZero), "medium");
    assert.deepEqual(oneZero.subjects.map((s) => s.label), ["API requests"]);
  });
});

describe("our own calls never make the finding", () => {
  const simulated = (transport: Simulator, outDir: string, overrides = {}) =>
    runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport, answers: loadAnswers(), ...overrides });

  test("the first sitting keeps its reading in the run folder; a damaged file falls back to the connection's", async () => {
    const out = tempDir();
    const run = await simulated(new Simulator(), out);
    const kept = JSON.parse(readFileSync(join(run.dir, LIMITS_AT_START_FILE), "utf8"));
    assert.deepEqual(kept.reported.DailyApiRequests, { Max: 100_000, Remaining: 99_000 });
    assert.equal(kept.unread, null);
    assert.equal(parseLimitsReading({ all: {}, reported: { X: { Max: "1" } }, unread: null }), null);
    assert.equal(parseLimitsReading(null), null);
  });

  test("an org at 94.9% when the check starts, pushed to 95% by the check's own calls, still passes after a resume", async () => {
    const out = tempDir();
    // 949,490 of 1,000,000 is 94.9%; the run stops at 94.95%, ten calls in, before the limit steps run.
    const org = new Simulator({ used: 949_490, dailyLimit: 1_000_000, asyncApex: { Max: 250_000, Remaining: 200_000 } });
    const stopped = await simulated(org, out, { stopAtShare: 0.9495 });
    assert.equal(stopped.run.status, "stopped_budget");
    assert.notEqual(stopped.run.steps.find((s) => s.id === apiLimitsExtractor.id)?.status, "done", "the limit step runs only after the resume");
    const resumed = await simulated(org, out, { resume: stopped.run.id, stopAtShare: 1 });
    assert.ok(resumed.output);
    // The resume's own connection reads 949,500 or more, 95%, with our first sitting's calls in it.
    assert.ok(stopped.run.budget.callsMade >= 10, `${stopped.run.budget.callsMade} calls before the stop`);
    const f = one(resumed.output.findings, "int.api-usage");
    assert.equal(f.status, "passed");
    assert.equal(f.facts.DailyApiRequestsUsed, 949_490);
  });
});

describe("every limit is kept, by name, whatever its name", () => {
  test("an unknown limit, one with a maximum of 0 and an entry with no figures: kept, kept, left out; no rule reads them", async () => {
    const extra = { SomeLimitNotInTheRepo: { Max: 500, Remaining: 100 }, ZeroLimitForTest: { Max: 0, Remaining: 0 }, NotALimit: { Note: "no figures" } };
    const { dir: out, output: result } = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ extraLimits: extra }), answers: loadAnswers(), inventory: true });
    const inventory = Inventory.parse(JSON.parse(readFileSync(join(out, "inventory.json"), "utf8")));
    assert.ok("rows" in inventory.limits);
    assert.deepEqual(inventory.limits.rows.find((r) => r.name === "SomeLimitNotInTheRepo"), { name: "SomeLimitNotInTheRepo", max: 500, used: 400 });
    assert.deepEqual(inventory.limits.rows.find((r) => r.name === "ZeroLimitForTest"), { name: "ZeroLimitForTest", max: 0, used: 0 });
    assert.ok(!inventory.limits.rows.some((r) => r.name === "NotALimit"));
    // No rule raises or names a limit it doesn't know.
    for (const f of result!.findings) assert.ok(!JSON.stringify(f).includes("SomeLimitNotInTheRepo"), f.ruleId);
    assert.deepEqual(
      result!.findings.filter((f) => f.status === "issue" && ["int.api-usage", "tech.storage-use"].includes(f.ruleId)).map((f) => f.ruleId).sort(),
      ["int.api-usage", "tech.storage-use"],
    );
  });

  test("an inventory written before this change still parses", () => {
    const old = { version: 1, madeAt: "2026-11-01T00:00:00.000Z", scope: "complete", fillRates: { unavailable: "x" }, packages: { unavailable: "x" }, connectedApps: { unavailable: "x" }, riskyPermissions: { unavailable: "x" }, limits: { rows: [{ name: "DataStorageMB", max: 10_240, used: 8_940 }], notes: [] } };
    assert.ok(Inventory.safeParse(old).success);
  });
});

describe("storage use, kept for the benchmark", () => {
  test("data and file storage's used share, as whole percents, from the storage check's own facts", () => {
    const { figures } = benchmarkFigures({ metrics: output!.metrics, scores: [], findings: output!.findings });
    assert.deepEqual(figures.filter((f) => f.metricKey.startsWith("tech.")), [
      { metricKey: "tech.data_storage_used_share", unit: "percent", value: 87 },
      { metricKey: "tech.file_storage_used_share", unit: "percent", value: 22 },
    ]);
  });
});
