import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildDigest } from "../build.ts";
import { checkedDigest } from "../checked.ts";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { RULES_FILE, RulesRun } from "../../engine/rules-run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { RunDigest } from "../../schemas/digest.ts";
import type { Finding } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";

// Which rules a run checked, and which the engine knew (DECISIONS.md 718): the
// run folder's rules.json and the digest's rulesRun and rulesKnown, true on
// every product and path. Each list is held against one worked out here by
// hand from the modules, never by the engine's own functions.

/** The areas a set-up review covers, written out here rather than read from scope.ts. */
const SETUP_AREAS = new Set(["configuration", "automation", "technical_health", "security", "integrations", "best_practice", "utilisation", "adoption", "service_setup", "cpq_setup", "account_engagement"]);
/** The insights, written out: nothing serves them, so no list names them. */
const INSIGHTS = ["pipe.path-insights", "pipe.probability-vs-reality"];
/** The code checker's six, written out. */
const CHECKER = ["tech.lint-complexity", "tech.lint-data-access", "tech.lint-injection", "tech.lint-limits", "tech.lint-swallowed-errors", "tech.lint-tests"];

/** The rules a run of these modules and scope evaluates, by hand: the checker's six only with its step. */
function expectedRun(modules: DomainModule[], scope: "complete" | "essentials", checkerStep: boolean): string[] {
  const ids: string[] = [];
  for (const m of modules) {
    if (scope === "essentials" && !SETUP_AREAS.has(m.id)) continue;
    for (const r of m.rules) {
      if (scope === "essentials" && r.records) continue;
      if (INSIGHTS.includes(r.id)) continue;
      if (CHECKER.includes(r.id) && !checkerStep) continue;
      ids.push(r.id);
    }
  }
  return ids.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}
const everyKnown = (modules: DomainModule[] = ALL_MODULES): string[] => [...new Set(modules.flatMap((m) => m.rules.map((r) => r.id)).filter((id) => !INSIGHTS.includes(id)))].sort();

/** Strictly ascending by code unit: sorted, and each once. */
function assertSortedOnce(list: string[], what: string): void {
  for (let i = 1; i < list.length; i++) assert.ok(list[i - 1]! < list[i]!, `${what} out of order or repeated at ${list[i]}`);
}

const MADE_AT = "2026-11-25T09:00:00.000Z";
const run = (sim: Simulator, overrides: Partial<RunOptions> = {}) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: sim, connectedUser: CONNECTED_USER, log: () => {}, ...overrides });

function digestOf(dir: string): RunDigest {
  const loaded = loadRun(dir);
  return buildDigest({ report: buildReport(loaded.input), pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
}
const fileOf = (dir: string): RulesRun => RulesRun.parse(JSON.parse(readFileSync(join(dir, RULES_FILE), "utf8")));
const findingsOf = (dir: string): Finding[] => JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];

/** The digest's lists are the folder's, sorted and once each; every finding's rule is listed; rulesKnown holds rulesRun. */
function assertConsistent(dir: string, digest: RunDigest, noRow: string[] = []): void {
  const file = fileOf(dir);
  assert.deepEqual(digest.rulesRun, file.rulesRun);
  assert.deepEqual(digest.rulesKnown, file.rulesKnown);
  assertSortedOnce(file.rulesRun, "rulesRun");
  assertSortedOnce(file.rulesKnown, "rulesKnown");
  for (const id of file.rulesRun) assert.ok(file.rulesKnown.includes(id), `${id} run but not known`);
  for (const id of INSIGHTS) assert.ok(!file.rulesRun.includes(id) && !file.rulesKnown.includes(id), `${id} is an insight`);
  const withRows = new Set(findingsOf(dir).map((f) => f.ruleId).filter((id) => !INSIGHTS.includes(id)));
  for (const id of withRows) assert.ok(file.rulesRun.includes(id), `${id} has a finding but isn't listed`);
  assert.deepEqual(file.rulesRun.filter((id) => !withRows.has(id)), noRow, "the rules listed with no finding row");
}

const automatic = await automaticSyntheticCalibration();
const consultant = await run(new Simulator());
const setup = await run(new Simulator(), { scope: "essentials" });
const pulse = await run(new Simulator(), { product: "pulse" });
const auto = await run(new Simulator(), { calibration: automatic, answers: [], reviewMode: "automatic" });
const everything = await run(new Simulator(), { modules: ALL_MODULES });

describe("the rules a run checked (DECISIONS.md 718)", () => {
  test("a consultant-led health check lists exactly the rules its modules evaluate, and the engine's every served rule as known", () => {
    assert.deepEqual(fileOf(consultant.dir).rulesRun, expectedRun(SALES_CLOUD_MODULES, "complete", false));
    assert.deepEqual(fileOf(consultant.dir).rulesKnown, everyKnown());
    assertConsistent(consultant.dir, digestOf(consultant.dir));
  });

  test("every module the engine knows, run together: Service Cloud, CPQ and Account Engagement too", () => {
    assert.deepEqual(fileOf(everything.dir).rulesRun, expectedRun(ALL_MODULES, "complete", false));
    assert.deepEqual(fileOf(everything.dir).rulesRun, fileOf(everything.dir).rulesKnown.filter((id) => !CHECKER.includes(id)));
    assertConsistent(everything.dir, digestOf(everything.dir));
  });

  test("a set-up-only review lists only its areas' set-up rules, never one that reads records", () => {
    const listed = fileOf(setup.dir).rulesRun;
    assert.deepEqual(listed, expectedRun(SALES_CLOUD_MODULES, "essentials", false));
    assert.ok(listed.length < fileOf(consultant.dir).rulesRun.length);
    const recordRules = SALES_CLOUD_MODULES.flatMap((m) => m.rules.filter((r) => r.records).map((r) => r.id));
    assert.ok(recordRules.length > 0 && recordRules.every((id) => !listed.includes(id)));
    assert.deepEqual(fileOf(setup.dir).rulesKnown, everyKnown());
    assertConsistent(setup.dir, digestOf(setup.dir));
  });

  test("a monthly pulse lists what it ran, its digest too, and never the code checker's six", () => {
    assert.deepEqual(fileOf(pulse.dir).rulesRun, expectedRun(SALES_CLOUD_MODULES, "complete", false));
    const digest = digestOf(pulse.dir);
    assert.equal(digest.product, "pulse");
    for (const id of CHECKER) assert.ok(!digest.rulesRun!.includes(id) && digest.rulesKnown!.includes(id), id);
    assertConsistent(pulse.dir, digest);
  });

  test("an automatic run lists the stage check that has nothing to say on it, with no finding row; its checked digest still traces and is released", () => {
    assert.deepEqual(fileOf(auto.dir).rulesRun, expectedRun(SALES_CLOUD_MODULES, "complete", false));
    const loaded = loadRun(auto.dir);
    const checked = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT });
    assert.equal(checked.held, null);
    assert.equal(checked.unmatched, 0);
    assert.equal(checked.unmarked, 0);
    assert.deepEqual(checked.digest!.rulesRun, fileOf(auto.dir).rulesRun);
    assertConsistent(auto.dir, checked.digest!, ["pipe.calibration-contradicts-org"]);
  });

  test("the code checker's six are listed when its step ran, even failed, and not without it", async () => {
    const throwing = { pmd: null } as { pmd: null; pmdMissing?: string };
    Object.defineProperty(throwing, "pmdMissing", { get: () => { throw new Error("unexpected"); } });
    const withStep = await run(new Simulator(), { codeLint: throwing });
    assert.deepEqual(fileOf(withStep.dir).rulesRun, expectedRun(SALES_CLOUD_MODULES, "complete", true));
    for (const id of CHECKER) assert.equal(findingsOf(withStep.dir).find((f) => f.ruleId === id)!.status, "not_measurable");
    assertConsistent(withStep.dir, digestOf(withStep.dir));
    for (const id of CHECKER) assert.ok(!fileOf(consultant.dir).rulesRun.includes(id), id);
  });

  test("a refused read: the rules it cost are not measurable and listed; the list is the unrefused run's", async () => {
    const refusing: SimulatorOptions = { failQuery: (soql) => (/FROM Opportunity\b/.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "Not allowed." } : null) };
    const refused = await run(new Simulator(refusing));
    assert.ok((refused.run.readsRefused ?? []).length > 0, "the read was refused");
    const unmeasured = findingsOf(refused.dir).filter((f) => f.status === "not_measurable" && !INSIGHTS.includes(f.ruleId)).map((f) => f.ruleId);
    const before = new Set(findingsOf(consultant.dir).filter((f) => f.status === "not_measurable").map((f) => f.ruleId));
    const lost = unmeasured.filter((id) => !before.has(id));
    assert.ok(lost.length > 5, "the refusal cost checks");
    for (const id of unmeasured) assert.ok(fileOf(refused.dir).rulesRun.includes(id), id);
    assert.deepEqual(fileOf(refused.dir).rulesRun, fileOf(consultant.dir).rulesRun);
    assertConsistent(refused.dir, digestOf(refused.dir));
  });

  test("Professional Edition: the checks the edition blocked ran, not measurable, and are listed", async () => {
    const pe = await run(new Simulator({ organizationType: "Professional Edition" }), { calibration: automatic, answers: [], reviewMode: "automatic", modules: ALL_MODULES });
    assert.equal(pe.run.orgProfile?.edition, "Professional Edition");
    assert.deepEqual(fileOf(pe.dir).rulesRun, expectedRun(ALL_MODULES, "complete", false));
    assert.ok(findingsOf(pe.dir).some((f) => f.status === "not_measurable" && /Professional Edition/.test(f.notMeasurableReason ?? "")));
    assertConsistent(pe.dir, digestOf(pe.dir), ["pipe.calibration-contradicts-org"]);
  });

  test("a run stopped at the budget and resumed lists what the same run straight through lists", async () => {
    const probe = new Simulator();
    const straight = await run(probe);
    const billable = probe.requests.filter((r) => !r.url.endsWith("/limits") && !r.url.endsWith("/services/data/")).length;
    const first = await run(new Simulator({ used: 50_000 - Math.floor(billable / 2) }));
    assert.equal(first.run.status, "stopped_budget");
    assert.ok(!existsSync(join(first.dir, RULES_FILE)), "nothing is listed before the analysis");
    const resumed = await run(new Simulator(), { outDir: join(first.dir, ".."), resume: first.run.id });
    assert.equal(resumed.run.status, straight.run.status);
    assert.deepEqual(fileOf(resumed.dir), fileOf(straight.dir));
    assert.deepEqual(digestOf(resumed.dir).rulesRun, digestOf(straight.dir).rulesRun);
  });

  test("a retired rule: an earlier run's id is missing from a later engine's rulesKnown, so the console can say it is no longer checked", async () => {
    const gone = SALES_CLOUD_MODULES[0]!.rules[0]!.id;
    const without = (ms: DomainModule[]) => ms.map((m) => ({ ...m, rules: m.rules.filter((r) => r.id !== gone) }));
    const earlier = digestOf(consultant.dir);
    const later = await run(new Simulator(), { modules: without(SALES_CLOUD_MODULES), knownModules: without(ALL_MODULES) });
    const now = digestOf(later.dir);
    assert.ok(earlier.rulesRun!.includes(gone) && earlier.rulesKnown!.includes(gone));
    assert.ok(!now.rulesRun!.includes(gone) && !now.rulesKnown!.includes(gone));
    assert.deepEqual(now.rulesKnown, earlier.rulesKnown!.filter((id) => id !== gone));
  });

  test("a digest without the lists, as every one before them, still parses; a folder without rules.json makes one without them and is otherwise the same", () => {
    const digest = digestOf(consultant.dir);
    const { rulesRun: _r, rulesKnown: _k, ...old } = digest;
    assert.deepEqual(RunDigest.parse(old), old);
    rmSync(join(consultant.dir, RULES_FILE));
    try {
      const made = digestOf(consultant.dir);
      assert.ok(!("rulesRun" in made) && !("rulesKnown" in made));
      assert.deepEqual(made, old);
    } finally {
      writeFileSync(join(consultant.dir, RULES_FILE), `${JSON.stringify({ rulesRun: digest.rulesRun, rulesKnown: digest.rulesKnown }, null, 2)}\n`);
    }
  });

  test("the lists are ids only and sit outside every text the digest serves", async () => {
    const { digestTexts } = await import("../checked.ts");
    const digest = digestOf(consultant.dir);
    const texts = new Set(digestTexts(digest));
    for (const id of [...digest.rulesRun!, ...digest.rulesKnown!]) assert.ok(!texts.has(id), id);
  });
});
