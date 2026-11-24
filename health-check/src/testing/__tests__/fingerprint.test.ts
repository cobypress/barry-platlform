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
  recorded: { "findings.json": "8cb2e60845aa6e51", "metrics.json": "71b019b301c5e723", "scores.json": "17cdb1708449d000", "manual-steps.json": "c4daf1ca86584a33", "action-lists.json": "7018f033c3c80e0a", "raw-notes.md": "0023df82620d5055", "handbook.json": "64dcafcf493eef42" },
  generated: { "findings.json": "eb16d7cb820f1731", "metrics.json": "7d1bebd0ffb263c4", "scores.json": "616e42570f940402", "manual-steps.json": "15ae87c4fd76d7b6", "action-lists.json": "8eeb97766d119778", "raw-notes.md": "26c359e68cf0986f", "handbook.json": "23c075e2337e23a8" },
};

// Before decision 703 added the path insights' findings and the variant table's metric: with those taken out,
// findings.json and metrics.json hash as they did. raw-notes.md gains their sections and the two counts they add to.
const BEFORE_703: Record<string, { "findings.json": string; "metrics.json": string }> = {
  recorded: { "findings.json": "299bb781fb00d50f", "metrics.json": "10d5f5733555b541" },
  generated: { "findings.json": "2614371e4eb185ea", "metrics.json": "2233e1f04464f5ae" },
};

// findings.json before decisions 628 and 636 added each issue's hours to fix, time band, fix complexity and fix prompt.
// Those two keys are all that moved it: with them taken out it hashes as it did.
const BEFORE_628: Record<string, string> = { recorded: "cab19e73c30f30a2", generated: "c71a5352db967288" };

// findings.json before decision 655 marked the org's words in each finding's sentences: the key it added is all that moved it.
const BEFORE_655: Record<string, string> = { recorded: "7f675e15a778749f", generated: "02b51c667053a565" };

// findings.json before decision 663 marked the value's reason and assumptions and the hours' basis: those three keys of the marks are all that moved it.
const BEFORE_663: Record<string, string> = { recorded: "e8cb5cc01d67f9f7", generated: "bd808a0082daa8c2" };

// findings.json before decision 672 marked the org's words in each issue's fix prompt: that key of the marks is all that moved it.
const BEFORE_672: Record<string, string> = { recorded: "09e6cd7d83776ef5", generated: "d6914065746b54e2" };

const recordedDir = async () => (await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir;
const generatedDir = async () =>
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir;
const recorded = async () => fingerprint(await recordedDir());
const generated = async () => fingerprint(await generatedDir());

/** The run's findings.json and metrics.json with decision 703's findings and metric taken out, written as the run writes them. */
function withoutPaths(dir: string): void {
  for (const [file, ours] of [["findings.json", (x: Record<string, unknown>) => x.ruleId === "pipe.path-insights"], ["metrics.json", (x: Record<string, unknown>) => x.id === "historic.path_variants"]] as const) {
    const path = join(dir, file);
    const rows = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown>>).filter((x) => !ours(x));
    writeFileSync(path, `${JSON.stringify(rows, null, 2)}\n`);
  }
}

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

/** The run's findings.json with only decision 672's marks taken out. */
function withoutPromptMarks(dir: string): void {
  const path = join(dir, "findings.json");
  const findings = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown> & { orgWords?: Record<string, unknown> }>).map(({ orgWords, ...rest }) => {
    if (!orgWords) return rest;
    const { fixPrompt: _p, ...earlier } = orgWords;
    return Object.keys(earlier).length > 0 ? { ...rest, orgWords: earlier } : rest;
  });
  writeFileSync(path, `${JSON.stringify(findings, null, 2)}\n`);
}

/** The run's findings.json with decision 663's marks, and the later 672's, taken out. */
function withoutLaterMarks(dir: string): void {
  const path = join(dir, "findings.json");
  const findings = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown> & { orgWords?: Record<string, unknown> }>).map(({ orgWords, ...rest }) => {
    if (!orgWords) return rest;
    const { valueReason: _r, valueAssumptions: _a, hoursBasis: _h, fixPrompt: _p, ...earlier } = orgWords;
    return Object.keys(earlier).length > 0 ? { ...rest, orgWords: earlier } : rest;
  });
  writeFileSync(path, `${JSON.stringify(findings, null, 2)}\n`);
}

test("the synthetic org's findings, metrics, scores, action lists, raw notes and handbook are as they were", async () => {
  assert.deepEqual(await recorded(), GOLDEN.recorded);
});

// Before any generated run in this process: a sentence another run quoted with org words reads as unknown in a later one
// (the safe side of decision 655's store), which moves an earlier mark, so the earlier marks are compared on a first run.
test("decisions 663 and 672 moved findings.json by the value, hours and fix prompt marks alone: without them it is as it was, and nothing else moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ orgWords?: { valueAssumptions?: unknown; hoursBasis?: unknown } }>;
    assert.ok(findings.some((f) => f.orgWords?.valueAssumptions) && findings.some((f) => f.orgWords?.hoursBasis), name);
    // Decision 672 moved it by the fix prompts' marks alone, on every issue: without them it is as it was before 672.
    const prompts = (findings as Array<{ fixPrompt?: string; orgWords?: { fixPrompt?: unknown } }>).filter((f) => f.fixPrompt);
    assert.ok(prompts.length > 0 && prompts.every((f) => f.orgWords?.fixPrompt), `${name}: every prompt marked`);
    withoutPaths(dir);
    withoutPromptMarks(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_672[name] }, `${name} before 672`);
    withoutLaterMarks(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_663[name] }, name);
  }
});

test("a generated org's are too, read through Salesforce's paging", async () => {
  assert.deepEqual(await generated(), GOLDEN.generated);
});

test("decision 628 moved findings.json by the hours and prompts alone: without them it is as it was, and nothing else moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const issues = (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ status: string; effortHours?: unknown; fixPrompt?: unknown }>).filter((f) => f.status === "issue");
    assert.ok(issues.length > 0 && issues.every((f) => f.effortHours && f.fixPrompt), name);
    withoutPaths(dir);
    withoutFixFields(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_628[name] }, name);
  }
});

test("decision 655 moved findings.json by the org-word marks alone: without them it is as it was, and nothing else moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ orgWords?: unknown }>;
    assert.ok(findings.some((f) => f.orgWords), name);
    withoutPaths(dir);
    withoutOrgWords(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_655[name] }, name);
  }
});

// Last, so no earlier test reads a run after it: with its findings and measure taken out, and every finding's marks
// (which a run after a generated one reads as unknown, the safe side of decision 655's store), it is as before 655.
test("decision 703 added the path insights and the variant table alone: without them findings.json and metrics.json are as they were, and nothing else but the raw notes moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const findings = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ ruleId: string }>;
    assert.ok(findings.some((f) => f.ruleId === "pipe.path-insights"), name);
    withoutPaths(dir);
    withoutOrgWords(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_655[name] }, name);
  }
});
