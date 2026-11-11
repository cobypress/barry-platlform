import { test } from "node:test";
import assert from "node:assert/strict";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { fingerprint } from "../fingerprint.ts";
import { runFromRecording, tempDir } from "../harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../scale-org.ts";
import { Simulator } from "../simulator.ts";
import { AS_OF } from "../synthetic-org.ts";

// What the engine concludes, held still while how it gets there changes
// (DECISIONS.md 341). A change made for memory or speed must leave every
// file here byte for byte the same. A change that means to move a finding,
// a metric or the raw notes updates these, and says why in its commit.

const GOLDEN: Record<string, Record<string, string>> = {
  recorded: { "findings.json": "3a2acbed932a0929", "metrics.json": "7daf36044db95bb4", "scores.json": "17cdb1708449d000", "manual-steps.json": "c4daf1ca86584a33", "action-lists.json": "7018f033c3c80e0a", "raw-notes.md": "0b0759b6348af7a4", "handbook.json": "64dcafcf493eef42" },
  generated: { "findings.json": "a2e0f6f7b138e6b8", "metrics.json": "7010a01f7d925850", "scores.json": "616e42570f940402", "manual-steps.json": "15ae87c4fd76d7b6", "action-lists.json": "8eeb97766d119778", "raw-notes.md": "45ad376dc8d6ea20", "handbook.json": "23c075e2337e23a8" },
};

const recorded = async () => fingerprint((await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir);
const generated = async () =>
  fingerprint((await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir);

test("the synthetic org's findings, metrics, scores, action lists, raw notes and handbook are as they were", async () => {
  assert.deepEqual(await recorded(), GOLDEN.recorded);
});

test("a generated org's are too, read through Salesforce's paging", async () => {
  assert.deepEqual(await generated(), GOLDEN.generated);
});
