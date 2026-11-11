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
  recorded: { "findings.json": "d8f7c6edee850bab", "metrics.json": "5b17208f28ac59c3", "scores.json": "1ea33b649d72937a", "manual-steps.json": "c4daf1ca86584a33", "action-lists.json": "7018f033c3c80e0a", "raw-notes.md": "98344bf3b98435cf", "handbook.json": "86f7eb0fb3ceb95a" },
  generated: { "findings.json": "d6b3bef8e962d5a5", "metrics.json": "259f96e994bafc7d", "scores.json": "763c408c24334b05", "manual-steps.json": "15ae87c4fd76d7b6", "action-lists.json": "8eeb97766d119778", "raw-notes.md": "0cad43aba331f194", "handbook.json": "b9790d4d0b304876" },
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
