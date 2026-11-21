import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { fingerprint } from "../fingerprint.ts";
import { runFromRecording, tempDir } from "../harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../scale-org.ts";
import { Simulator } from "../simulator.ts";
import { AS_OF, CONNECTED_USER } from "../synthetic-org.ts";

// What the engine concludes, held still while how it gets there changes
// (DECISIONS.md 341). A change made for memory or speed must leave every
// file here byte for byte the same. A change that means to move a finding,
// a metric or the raw notes updates these, and says why in its commit.

const GOLDEN: Record<string, Record<string, string>> = {
  recorded: { "findings.json": "e6b6d9e007d13ba4", "metrics.json": "10d5f5733555b541", "scores.json": "17cdb1708449d000", "manual-steps.json": "c4daf1ca86584a33", "action-lists.json": "7018f033c3c80e0a", "raw-notes.md": "5e5e3581e490bb64", "handbook.json": "64dcafcf493eef42" },
  generated: { "findings.json": "b21c1fe086631fc2", "metrics.json": "2233e1f04464f5ae", "scores.json": "616e42570f940402", "manual-steps.json": "15ae87c4fd76d7b6", "action-lists.json": "8eeb97766d119778", "raw-notes.md": "9aa44b2002f1740f", "handbook.json": "23c075e2337e23a8" },
};

// findings.json before decisions 628 and 636 added each issue's hours to fix, time band, fix complexity and fix prompt.
// Those two keys are all that moved it: with them taken out it hashes as it did.
const BEFORE_628: Record<string, string> = { recorded: "cab19e73c30f30a2", generated: "c71a5352db967288" };

// findings.json before decision 655 marked the org's words in each finding's sentences: the key it added is all that moved it.
const BEFORE_655: Record<string, string> = { recorded: "7f675e15a778749f", generated: "02b51c667053a565" };

const recordedDir = async () => (await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir;
const generatedDir = async () =>
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir;
const recorded = async () => fingerprint(await recordedDir());
const generated = async () => fingerprint(await generatedDir());

/** The run's findings.json with every finding's hours and prompt taken out, and the later marks (655), written as the run writes it. */
function withoutFixFields(dir: string): void {
  const path = join(dir, "findings.json");
  const findings = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown>>).map(({ effortHours: _h, timeBand: _b, fixComplexity: _c, fixPrompt: _p, orgWords: _w, ...rest }) => rest);
  writeFileSync(path, `${JSON.stringify(findings, null, 2)}\n`);
}

/** The run's findings.json with only decision 655's marks taken out. */
function withoutOrgWords(dir: string): void {
  const path = join(dir, "findings.json");
  const findings = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown>>).map(({ orgWords: _w, ...rest }) => rest);
  writeFileSync(path, `${JSON.stringify(findings, null, 2)}\n`);
}

test("the synthetic org's findings, metrics, scores, action lists, raw notes and handbook are as they were", async () => {
  assert.deepEqual(await recorded(), GOLDEN.recorded);
});

test("a generated org's are too, read through Salesforce's paging", async () => {
  assert.deepEqual(await generated(), GOLDEN.generated);
});

test("decision 628 moved findings.json by the hours and prompts alone: without them it is as it was, and nothing else moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const issues = (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ status: string; effortHours?: unknown; fixPrompt?: unknown }>).filter((f) => f.status === "issue");
    assert.ok(issues.length > 0 && issues.every((f) => f.effortHours && f.fixPrompt), name);
    withoutFixFields(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], "findings.json": BEFORE_628[name] }, name);
  }
});

test("decision 655 moved findings.json by the org-word marks alone: without them it is as it was, and nothing else moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ orgWords?: unknown }>;
    assert.ok(findings.some((f) => f.orgWords), name);
    withoutOrgWords(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], "findings.json": BEFORE_655[name] }, name);
  }
});
