import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { BENCHMARK_MIN_GROUP, BenchmarkRow } from "../../schemas/index.ts";
import { bound, notRun, rulesSeen, tallyOf, type LintFinding, type ToolRun } from "../../extract/lint/findings.ts";
import { PMD_NOT_INSTALLED, PMD_RULES, PMD_VERSION } from "../../extract/lint/pmd.ts";
import { NO_JAVASCRIPT } from "../../extract/lint/eslint.ts";
import { LINT_TOTALS_FILE, lintTotals } from "../../extract/lint/totals.ts";
import { appendRows, BENCHMARK_FILE, BENCHMARK_METRICS, exportBenchmark, LINT_KIND_UNREAD, LINT_NOT_MEASURED, lintKey } from "../export.ts";
import { summariseBenchmarks } from "../summary.ts";

// PMD's counts in the benchmark (DECISIONS.md 673): numbers only, through the
// same consent, strict rows and minimum group as every other figure, and a run
// whose PMD didn't run gives no row, never a zero.

const { dir: source } = await runFromRecording({ answers: loadAnswers() });

/** Class and trigger names planted in the run's PMD results: none may reach the store. */
const PLANTED = ["ZephyrBillingPlanted", "QuokkaExportPlanted", "MarlinQueuePlanted"];
const CODE_KEYS = BENCHMARK_METRICS.filter((m) => m.key.startsWith("code.pmd.")).map((m) => m.key);

function pmdRan(): ToolRun {
  const at = (rule: string, component: string, line: number, extra: Partial<LintFinding> = {}): LintFinding => ({ tool: "pmd", toolVersion: PMD_VERSION, rule, category: "Security", severity: 3, kind: "class", component, line, column: 2, ...extra });
  const all = [at("ApexCRUDViolation", PLANTED[0]!, 3), at("ApexCRUDViolation", PLANTED[0]!, 9), at("ApexCRUDViolation", PLANTED[1]!, 4, { kind: "trigger" }), at("EmptyCatchBlock", PLANTED[2]!, 5, { suppressed: true })];
  const kept = bound(all);
  return { status: "ran", tool: "pmd", toolVersion: PMD_VERSION, checked: 12, skipped: { tooLarge: 0, unparsable: 0, unreadable: 0, failed: 0, timeLimit: 0 }, found: kept.found, stored: kept.stored, components: kept.components, findings: kept.findings, rules: rulesSeen(kept.findings, PMD_RULES), tally: tallyOf(all), read: { classes: 10, triggers: 2, lines: 1_234 }, outsideRules: 0, malformed: 0, limits: [] };
}

/** A private copy of the synthetic run, with PMD's step as given: its checkpoint (names and all) and its kept totals. */
function runWith(pmd: ToolRun | null): string {
  const dir = join(tempDir(), "run");
  cpSync(source, dir, { recursive: true });
  if (pmd) {
    const data = { pmd, eslint: notRun("eslint", NO_JAVASCRIPT) };
    writeFileSync(join(dir, "checkpoints", "code-lint.json"), JSON.stringify(data));
    writeFileSync(join(dir, LINT_TOTALS_FILE), JSON.stringify(lintTotals(data)));
  }
  return dir;
}
const exportFrom = (run: string, store: string) =>
  exportBenchmark({ runDir: run, storeDir: store, industry: "technology", headcount: "51-200", consent: "Synthetic Person, agreed by email", now: new Date("2026-11-23T10:00:00.000Z") });

describe("PMD's counts in the benchmark (decision 673)", () => {
  test("every rule's found, suppressed and classes, and what PMD read: 114 keys, in a unit of their own", () => {
    assert.equal(CODE_KEYS.length, 37 * 3 + 3);
    assert.equal(new Set(BENCHMARK_METRICS.map((m) => m.key)).size, BENCHMARK_METRICS.length);
    for (const m of BENCHMARK_METRICS.filter((x) => x.key.startsWith("code.pmd."))) assert.equal(m.unit, "count");
    assert.equal(lintKey("ApexCRUDViolation"), "apex_crud_violation");
    assert.equal(lintKey("ApexXSSFromURLParam"), "apex_xss_from_url_param");
    assert.equal(lintKey("NcssCount"), "ncss_count");
    for (const key of CODE_KEYS) assert.match(key, /^[a-z][a-z0-9_.]*$/);
  });

  test("a run where PMD ran adds its counts as strict rows, and no class or trigger name reaches the store", () => {
    const run = runWith(pmdRan());
    const store = tempDir();
    const result = exportFrom(run, store);
    const byKey = new Map(result.rows.map((r) => [r.metricKey, r]));
    for (const key of CODE_KEYS) assert.ok(byKey.has(key), key);
    assert.deepEqual(["found", "suppressed", "components"].map((k) => byKey.get(`code.pmd.apex_crud_violation.${k}`)!.value), [3, 0, 2]);
    assert.deepEqual(["found", "suppressed", "components"].map((k) => byKey.get(`code.pmd.empty_catch_block.${k}`)!.value), [0, 1, 0]);
    assert.equal(byKey.get("code.pmd.apex_bad_crypto.found")!.value, 0);
    // What PMD read, rounded so a small exact figure can't single an org out: classes and triggers to the nearest 10, lines to the nearest 1,000.
    assert.deepEqual([byKey.get("code.pmd.classes")!.value, byKey.get("code.pmd.triggers")!.value, byKey.get("code.pmd.lines")!.value], [10, 0, 1_000]);
    const text = readFileSync(join(store, BENCHMARK_FILE), "utf8");
    for (const line of text.trim().split("\n")) BenchmarkRow.parse(JSON.parse(line));
    for (const name of PLANTED) {
      assert.ok(!text.includes(name), `${name} reached the benchmark store`);
      assert.ok(!text.toLowerCase().includes(name.toLowerCase()), `${name} reached the benchmark store`);
    }
    // The run folder's kept totals hold no name either; only the checkpoint did.
    const kept = readFileSync(join(run, LINT_TOTALS_FILE), "utf8");
    for (const name of PLANTED) assert.ok(!kept.includes(name), name);
    assert.ok(readFileSync(join(run, "checkpoints", "code-lint.json"), "utf8").includes(PLANTED[0]!), "the planted name was in the run's checkpoint");
  });

  test("a run where PMD didn't run gives no row for any of them, each skipped as not measured, never zero", () => {
    const store = tempDir();
    const result = exportFrom(runWith(notRun("pmd", PMD_NOT_INSTALLED)), store);
    assert.ok(!result.rows.some((r) => r.metricKey.startsWith("code.pmd.")));
    const skipped = result.skipped.filter((s) => s.metricKey.startsWith("code.pmd."));
    assert.equal(skipped.length, CODE_KEYS.length);
    for (const s of skipped) assert.equal(s.reason, LINT_NOT_MEASURED);
    assert.ok(!readFileSync(join(store, BENCHMARK_FILE), "utf8").includes("code.pmd."));
  });

  test("PMD read no code at all (every file failed to parse): not measured, never zeros", () => {
    const empty = pmdRan();
    if (empty.status === "ran") empty.read = { classes: 0, triggers: 0, lines: 0 };
    const result = exportFrom(runWith(empty), tempDir());
    assert.ok(!result.rows.some((r) => r.metricKey.startsWith("code.pmd.")));
    assert.equal(result.skipped.filter((s) => s.reason === LINT_NOT_MEASURED).length, CODE_KEYS.length);
  });

  test("classes unread: rules that read only classes, and the class count, are not measured; the rest go in", () => {
    const partial = pmdRan();
    if (partial.status === "ran") partial.unread = ["class"];
    const result = exportFrom(runWith(partial), tempDir());
    const keys = new Set(result.rows.map((r) => r.metricKey));
    for (const key of ["code.pmd.classes", "code.pmd.apex_unit_test_class_should_have_asserts.found", "code.pmd.apex_sharing_violations.components"]) {
      assert.ok(!keys.has(key), key);
      assert.equal(result.skipped.find((s) => s.metricKey === key)?.reason, LINT_KIND_UNREAD);
    }
    for (const key of ["code.pmd.triggers", "code.pmd.lines", "code.pmd.apex_crud_violation.found", "code.pmd.avoid_logic_in_trigger.found"]) assert.ok(keys.has(key), key);
  });

  test("a run with no step at all (a scan, a pulse, an older run) is the same: not measured", () => {
    const result = exportFrom(runWith(null), tempDir());
    assert.ok(!result.rows.some((r) => r.metricKey.startsWith("code.pmd.")));
    assert.equal(result.skipped.filter((s) => s.reason === LINT_NOT_MEASURED).length, CODE_KEYS.length);
    // Every other figure goes in as before.
    assert.ok(result.rows.some((r) => r.metricKey === "historic.win_rate"));
  });

  test("the minimum group holds for them as for every figure", () => {
    const store = tempDir();
    const row = { metricKey: "code.pmd.apex_crud_violation.found", ruleSetVersion: "rs-1", value: 4, unit: "count", industry: "technology", headcountBand: "51-200", userBand: "1-50", dealSizeBand: "5k-25k", period: "2026-Q4" };
    appendRows(join(store, BENCHMARK_FILE), Array.from({ length: BENCHMARK_MIN_GROUP - 1 }, () => row));
    assert.equal(summariseBenchmarks(store).groups[0]!.enough, false);
    appendRows(join(store, BENCHMARK_FILE), [row]);
    assert.equal(summariseBenchmarks(store).groups[0]!.enough, true);
  });
});
