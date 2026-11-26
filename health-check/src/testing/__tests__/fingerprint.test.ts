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
import { ENGINE_STAGES_SOQL, STAGES_SOQL } from "../../extract/opportunities.ts";
import { DURATION_MEDIANS } from "../../domains/average.ts";

// What the engine concludes, held still while how it gets there changes
// (DECISIONS.md 341). A change made for memory or speed must leave every
// file here byte for byte the same. A change that means to move a finding,
// a metric or the raw notes updates these, and says why in its commit.

// Decision 712 moved the recorded run's findings.json, metrics.json and raw-notes.md: 706's sentence names its period from
// the day the stage history starts (5 November 2024, not 31 October), and 703's variant table counts no closing-out
// moves (none of the synthetic org's 45 came within a day of its close). Both rows are stored, not served. The
// generated org's files didn't move. The earlier hashes below are each decision's own, taken from this run.
// Decision 726 moved both runs' findings.json and raw-notes.md: int.api-usage, version 3, raises a daily allowance only
// from 95% used, and the async Apex trap moved from 88% to 96% so it still fires, medium as before. metrics.json and
// scores.json didn't move. Each earlier decision's findings.json hash below was taken again from this run.
// Decision 775 moved both runs' metrics.json and raw-notes.md, and only those: three metric texts no longer end in a
// pointer to our own notes ("(QUESTIONS.md 3)", "(QUESTIONS.md)"). Each earlier decision's metrics.json hash below was
// taken again from this run; no findings.json, scores.json or other hash moved.
const GOLDEN: Record<string, Record<string, string>> = {
  recorded: { "findings.json": "5030e28f0b262677", "metrics.json": "5f0c52583efbd22e", "scores.json": "17cdb1708449d000", "manual-steps.json": "c4daf1ca86584a33", "action-lists.json": "7018f033c3c80e0a", "raw-notes.md": "88ee0f6e233712b2", "handbook.json": "64dcafcf493eef42" },
  generated: { "findings.json": "269e760d438adb2e", "metrics.json": "716b945b04143af4", "scores.json": "616e42570f940402", "manual-steps.json": "15ae87c4fd76d7b6", "action-lists.json": "8eeb97766d119778", "raw-notes.md": "bdca47c97dc7de42", "handbook.json": "23c075e2337e23a8" },
};

// findings.json before decision 714 put a difficulty on each issue: that key is all that moved it, and nothing else moved.
const BEFORE_714: Record<string, string> = { recorded: "27b379b80e5b3a77", generated: "998e50194fd092df" };

// Before decision 709 kept each duration's average beside its median: with the stat taken out, metrics.json hashes as
// it did. findings.json and scores.json never moved; raw-notes.md gains the stat in each measure's detail line.
const BEFORE_709: Record<string, { "metrics.json": string }> = {
  recorded: { "metrics.json": "d0225ed74639798c" },
  generated: { "metrics.json": "5f118e9255247669" },
};

// Before decision 706 added the stage probabilities' finding and metric, and its stage read named the probability field:
// with those taken out, and the stage read's evidence naming the query as it was, findings.json and metrics.json hash
// as they did. raw-notes.md gains their sections and the counts they add to. Nothing else moved.
const BEFORE_706: Record<string, { "findings.json": string; "metrics.json": string }> = {
  recorded: { "findings.json": "8f182601bd09585d", "metrics.json": "cef6544ddee4cd3a" },
  generated: { "findings.json": "7219673a53b25b6f", "metrics.json": "919787c4457d1fb5" },
};

// Before decision 703 added the path insights' findings and the variant table's metric: with those taken out,
// findings.json and metrics.json hash as they did. raw-notes.md gains their sections and the two counts they add to.
const BEFORE_703: Record<string, { "findings.json": string; "metrics.json": string }> = {
  recorded: { "findings.json": "8bda59fad6800f42", "metrics.json": "efde7f43d8f03003" },
  generated: { "findings.json": "459bd0053c635176", "metrics.json": "8ae17fb3a2a8b452" },
};

// findings.json before decisions 628 and 636 added each issue's hours to fix, time band, fix complexity and fix prompt.
// Those two keys are all that moved it: with them taken out it hashes as it did.
const BEFORE_628: Record<string, string> = { recorded: "5a946242d665630e", generated: "d97796974efa6886" };

// findings.json before decision 655 marked the org's words in each finding's sentences: the key it added is all that moved it.
const BEFORE_655: Record<string, string> = { recorded: "77daed943bd0f4b8", generated: "0c13fef3ebab5768" };

// findings.json before decision 663 marked the value's reason and assumptions and the hours' basis: those three keys of the marks are all that moved it.
const BEFORE_663: Record<string, string> = { recorded: "2d43460bf6b58861", generated: "dee99efc933c915a" };

// findings.json before decision 672 marked the org's words in each issue's fix prompt: that key of the marks is all that moved it.
const BEFORE_672: Record<string, string> = { recorded: "3a27b9476912cb8a", generated: "a25da837793b67c1" };

const recordedDir = async () => (await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES })).dir;
const generatedDir = async () =>
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir;
const recorded = async () => fingerprint(await recordedDir());
const generated = async () => fingerprint(await generatedDir());

/** The run's metrics.json with decision 709's average taken out of each duration's stats. */
function withoutAverages(dir: string): void {
  const path = join(dir, "metrics.json");
  const metrics = (JSON.parse(readFileSync(path, "utf8")) as Array<{ id: string; stats?: Record<string, number> }>).map((m) => {
    if (!m.stats || !DURATION_MEDIANS.has(m.id)) return m;
    const { mean: _m, ...stats } = m.stats;
    return { ...m, stats };
  });
  writeFileSync(path, `${JSON.stringify(metrics, null, 2)}\n`);
}

/** The run's findings.json with decision 714's difficulty taken out of every finding. */
function withoutDifficulty(dir: string): void {
  const path = join(dir, "findings.json");
  const findings = (JSON.parse(readFileSync(path, "utf8")) as Array<Record<string, unknown>>).map(({ difficulty: _d, ...rest }) => rest);
  writeFileSync(path, `${JSON.stringify(findings, null, 2)}\n`);
}

/** The run's findings.json and metrics.json with decision 706's finding and metric taken out, its stage read named as before it, and 709's averages and 714's difficulty before them. */
function withoutProbabilities(dir: string): void {
  withoutDifficulty(dir);
  withoutAverages(dir);
  for (const [file, ours] of [["findings.json", (x: Record<string, unknown>) => x.ruleId === "pipe.probability-vs-reality"], ["metrics.json", (x: Record<string, unknown>) => x.id === "historic.stage_probability_vs_won"]] as const) {
    const path = join(dir, file);
    const rows = (JSON.parse(readFileSync(path, "utf8").replaceAll(ENGINE_STAGES_SOQL, STAGES_SOQL)) as Array<Record<string, unknown>>).filter((x) => !ours(x));
    writeFileSync(path, `${JSON.stringify(rows, null, 2)}\n`);
  }
}

/** The run's findings.json and metrics.json with decision 703's findings and metric taken out, and 706's before them, written as the run writes them. */
function withoutPaths(dir: string): void {
  withoutProbabilities(dir);
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

// On the recorded run alone, before any generated run in this process (decision 655's store, as above); the generated
// org's findings.json is held without the difficulty by the 628 and 655 tests below.
test("decision 714 moved findings.json by the difficulty alone: without it it is as it was, and nothing else moved", async () => {
  const dir = await recordedDir();
  const issues = (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Array<{ status: string; difficulty?: unknown }>).filter((f) => f.status === "issue");
  assert.ok(issues.length > 0 && issues.every((f) => f.difficulty));
  withoutDifficulty(dir);
  assert.deepEqual(fingerprint(dir), { ...GOLDEN.recorded, "findings.json": BEFORE_714.recorded });
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
    // Decision 706's finding and measure taken out alone, on a first run: findings.json and metrics.json are as before it.
    assert.ok(findings.length > 0 && JSON.stringify(findings).includes("pipe.probability-vs-reality"), name);
    withoutProbabilities(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_706[name] }, `${name} before 706`);
    // Decision 703's findings and measure taken out too: findings.json and metrics.json are as before it.
    withoutPaths(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name] }, `${name} before 703`);
    withoutPromptMarks(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_672[name] }, `${name} before 672`);
    withoutLaterMarks(dir);
    assert.deepEqual(fingerprint(dir), { ...GOLDEN[name], ...BEFORE_703[name], "findings.json": BEFORE_663[name] }, name);
  }
});

test("decision 709 moved metrics.json by the averages alone: without them it is as it was, and scores never moved", async () => {
  for (const [name, dir] of [["recorded", await recordedDir()], ["generated", await generatedDir()]] as const) {
    const metrics = JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Array<{ id: string; stats?: Record<string, number> }>;
    assert.ok(metrics.some((m) => typeof m.stats?.mean === "number"), name);
    withoutAverages(dir);
    // findings.json is held by the golden tests either side, each on a first run: a run after a generated one reads
    // some org-word marks as unknown (decision 655's store), so it isn't compared here.
    const { "findings.json": _f, ...rest } = fingerprint(dir);
    const { "findings.json": _g, ...expected }: Record<string, string> = { ...GOLDEN[name], ...BEFORE_709[name] };
    assert.deepEqual(rest, expected, name);
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
