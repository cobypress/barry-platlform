import { test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { daysBefore } from "../types.ts";

// The login log covers the whole dormant period (DECISIONS.md 361 and 393): a
// user the log shows logging in inside it isn't dormant, so the log must reach
// back as far as the period does, whatever the calibration sets.

async function loginsFrom(dormantDays: number): Promise<string> {
  const { dir } = await runHealthCheck({ provider: syntheticProvider, calibration: { ...loadCalibration(), dormantDays }, answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator() });
  return (readCheckpoint(join(dir, "checkpoints", "logins.json")) as { data: { from: string } }).data.from;
}

test("the login log reaches back over a dormant period longer than 90 days, and never less than 90", async () => {
  assert.equal(await loginsFrom(180), daysBefore(new Date(AS_OF), 180).toISOString().slice(0, 10));
  assert.equal(await loginsFrom(90), daysBefore(new Date(AS_OF), 90).toISOString().slice(0, 10));
  assert.equal(await loginsFrom(60), daysBefore(new Date(AS_OF), 90).toISOString().slice(0, 10), "90 days still, for the other checks that read it");
});
