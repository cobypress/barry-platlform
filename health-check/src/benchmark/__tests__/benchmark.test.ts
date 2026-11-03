import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, existsSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { BENCHMARK_MIN_GROUP, BenchmarkRow, Finding, Metric, Score } from "../../schemas/index.ts";
import type { PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import {
  appendRows,
  BENCHMARK_FILE,
  BENCHMARK_METRICS,
  benchmarkFigures,
  CONSENT_FILE,
  ConsentRecord,
  dealSizeBandOf,
  exportBenchmark,
  quarterOf,
  userBandOf,
} from "../export.ts";
import { formatSummary, summariseBenchmarks } from "../summary.ts";

const { dir: source } = await runFromRecording({ answers: loadAnswers() });

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const write = (dir: string, file: string, value: unknown): void => writeFileSync(join(dir, file), JSON.stringify(value));

/** A private copy of the synthetic run, so each test can change or export it. */
function freshRun(): string {
  const dir = join(tempDir(), "run");
  cpSync(source, dir, { recursive: true });
  return dir;
}

const CONSENT = "Synthetic Person, Head of Sales Operations, agreed by email";
const options = (runDir: string, storeDir: string, extra: Partial<Parameters<typeof exportBenchmark>[0]> = {}) => ({
  runDir,
  storeDir,
  industry: "technology",
  headcount: "50-249",
  consent: CONSENT,
  now: new Date("2026-11-03T10:00:00.000Z"),
  ...extra,
});

const ROW_FIELDS = Object.keys(BenchmarkRow.shape).sort();

describe("hc benchmark", () => {
  test("adds the run's allowlisted figures as strict rows, in whole units", () => {
    const run = freshRun();
    const store = tempDir();
    const result = exportBenchmark(options(run, store));
    const lines = readFileSync(join(store, BENCHMARK_FILE), "utf8").trim().split("\n");
    assert.equal(lines.length, result.rows.length);
    assert.ok(result.rows.length >= 30, `${result.rows.length} rows`);
    const allowed = new Set(BENCHMARK_METRICS.map((m) => m.key));
    for (const line of lines) {
      const raw = JSON.parse(line) as Record<string, unknown>;
      assert.deepEqual(Object.keys(raw).sort(), ROW_FIELDS);
      const row = BenchmarkRow.parse(raw);
      assert.ok(allowed.has(row.metricKey), row.metricKey);
      assert.ok(Number.isInteger(row.value));
      assert.equal(row.industry, "technology");
      assert.equal(row.headcountBand, "50-249");
      // The synthetic org has 14 active people and a median won deal of tens of thousands.
      assert.equal(row.userBand, "1-50");
      assert.equal(row.dealSizeBand, "25k-100k");
      assert.equal(row.period, "2026-Q4");
    }
    const winRate = read<Metric[]>(run, "metrics.json").find((m) => m.id === "historic.win_rate");
    assert.equal(winRate?.status, "measured");
    assert.equal(result.rows.find((r) => r.metricKey === "historic.win_rate")?.value, Math.round(winRate.status === "measured" ? winRate.value : NaN));
  });

  test("the benchmark store holds nothing that leads back to the run, the org or a person", () => {
    const run = freshRun();
    const store = tempDir();
    const runJson = read<Record<string, unknown>>(run, "run.json");
    write(run, "run.json", { ...runJson, customerRef: "Synthetic Customer Ltd" });
    const calibration = read<{ confirmedBy: string }>(run, "calibration.json");
    exportBenchmark(options(run, store));

    const text = readFileSync(join(store, BENCHMARK_FILE), "utf8");
    const id = runJson.id as string;
    const orgId = runJson.orgId as string;
    const forbidden = [
      id,
      id.replace(/^hc-/, ""),
      orgId,
      orgId.slice(0, 15),
      "Synthetic Customer Ltd",
      calibration.confirmedBy,
      runJson.calibrationHash as string,
      (runJson.asOf as string).slice(0, 10),
      (runJson.startedAt as string).slice(0, 10),
      run,
      CONSENT,
      "Synthetic Person",
    ];
    for (const entry of read<PseudonymEntry[]>(run, "pseudonym-map.json")) forbidden.push(entry.id, entry.id.slice(0, 15), entry.name, entry.token, ...entry.aliases);
    for (const value of forbidden.filter(Boolean)) assert.ok(!text.includes(value), `${value} reached the benchmark store`);
    assert.doesNotMatch(text, /\d{4}-\d{2}-\d{2}/, "no calendar date in the store");
    assert.doesNotMatch(text, /00D0/, "no org id prefix in the store");
  });

  test("records the consent in the run folder, privately, and never in the store", () => {
    const run = freshRun();
    const store = tempDir();
    const result = exportBenchmark(options(run, store));
    const record = ConsentRecord.parse(read(run, CONSENT_FILE));
    assert.equal(record.consent, CONSENT);
    assert.equal(record.recordedAt, "2026-11-03T10:00:00.000Z");
    assert.equal(record.rowsWritten, result.rows.length);
    assert.deepEqual(record.metricKeys, result.rows.map((r) => r.metricKey));
    assert.equal(statSync(join(run, CONSENT_FILE)).mode & 0o777, 0o600);
    assert.equal(statSync(join(store, BENCHMARK_FILE)).mode & 0o777, 0o600);
    assert.equal(statSync(store).mode & 0o777, 0o700);
    assert.ok(!existsSync(join(store, CONSENT_FILE)));
  });

  test("refuses without consent, and writes nothing", () => {
    const run = freshRun();
    const store = tempDir();
    for (const consent of [undefined, "", "   "]) {
      assert.throws(() => exportBenchmark(options(run, store, { consent })), /Refusing to export without --consent/);
    }
    assert.ok(!existsSync(join(store, BENCHMARK_FILE)));
    assert.ok(!existsSync(join(run, CONSENT_FILE)));
  });

  test("refuses a second export of the same run, and leaves the store as it was", () => {
    const run = freshRun();
    const store = tempDir();
    exportBenchmark(options(run, store));
    const before = readFileSync(join(store, BENCHMARK_FILE), "utf8");
    assert.throws(() => exportBenchmark(options(run, store, { consent: "Asked again" })), /already been exported/);
    assert.throws(() => exportBenchmark(options(run, tempDir())), /already been exported/);
    assert.equal(readFileSync(join(store, BENCHMARK_FILE), "utf8"), before);
    assert.equal(ConsentRecord.parse(read(run, CONSENT_FILE)).consent, CONSENT);
  });

  test("an org goes in once in any four quarters, so a repeat check can't count it twice", () => {
    const runs = tempDir();
    const first = join(runs, "first");
    const second = join(runs, "second");
    cpSync(source, first, { recursive: true });
    cpSync(source, second, { recursive: true });
    const store = tempDir();
    exportBenchmark(options(first, store));
    const before = readFileSync(join(store, BENCHMARK_FILE), "utf8");
    assert.throws(() => exportBenchmark(options(second, store)), /already in the benchmark set/);
    assert.equal(readFileSync(join(store, BENCHMARK_FILE), "utf8"), before);
    assert.ok(!existsSync(join(second, CONSENT_FILE)));

    // A year on, the repeat check can go in.
    const runJson = read<{ analysisWindow: { from: string; to: string } }>(second, "run.json");
    write(second, "run.json", { ...runJson, analysisWindow: { from: "2025-11-03", to: "2027-11-03" } });
    exportBenchmark(options(second, store));
  });

  test("takes industry and headcount only from the fixed lists", () => {
    const run = freshRun();
    const store = tempDir();
    assert.throws(() => exportBenchmark(options(run, store, { industry: "Insurance brokers in Leeds" })), /--industry must be one of/);
    assert.throws(() => exportBenchmark(options(run, store, { industry: undefined })), /--industry must be one of/);
    assert.throws(() => exportBenchmark(options(run, store, { headcount: "about 120" })), /--headcount must be one of/);
    assert.ok(!existsSync(join(store, BENCHMARK_FILE)));
    assert.ok(!existsSync(join(run, CONSENT_FILE)));
  });

  test("refuses a folder that isn't a run", () => {
    assert.throws(() => exportBenchmark(options(tempDir(), tempDir())), /isn't a run folder/);
  });

  test("refuses a store inside a git work tree", () => {
    const repo = fileURLToPath(new URL("../../..", import.meta.url));
    const run = freshRun();
    assert.throws(() => exportBenchmark(options(run, join(repo, "benchmarks"))), /Refusing to write/);
    assert.ok(!existsSync(join(run, CONSENT_FILE)));
  });
});

describe("the row format", () => {
  const good = { metricKey: "historic.win_rate", ruleSetVersion: "2026.11-m3", value: 35, unit: "percent", industry: "technology", headcountBand: "50-249", userBand: "1-50", dealSizeBand: "5k-25k", period: "2026-Q4" };

  test("rejects a row carrying anything extra, and writes none of the batch", () => {
    const path = join(tempDir(), "store", BENCHMARK_FILE);
    for (const extra of [{ runId: "hc-2026-10-31-00000000" }, { orgId: "00D000000000001AAA" }, { name: "Synthetic Person" }]) {
      assert.throws(() => appendRows(path, [good, { ...good, ...extra }]), /unrecognized key/i);
    }
    assert.ok(!existsSync(path));
  });

  test("rejects exact figures, dates and free-text bands", () => {
    const path = join(tempDir(), BENCHMARK_FILE);
    for (const bad of [{ value: 35.4 }, { period: "2026-10-31" }, { userBand: "14" }, { dealSizeBand: "42000" }, { metricKey: "Win Rate" }, { unit: "currency" }]) {
      assert.throws(() => appendRows(path, [{ ...good, ...bad }]), JSON.stringify(bad));
    }
    assert.ok(!existsSync(path));
    assert.equal(appendRows(path, [good]).length, 1);
  });
});

describe("benchmark figures", () => {
  const load = (dir: string) => ({
    metrics: z.array(Metric).parse(read(dir, "metrics.json")),
    scores: z.array(Score).parse(read(dir, "scores.json")),
    findings: z.array(Finding).parse(read(dir, "findings.json")),
  });

  test("skip a metric that isn't measurable, with its reason, rather than estimate it", () => {
    const input = load(source);
    input.metrics = input.metrics.map((m) => (m.id === "historic.win_rate" ? Metric.parse({ status: "not_measurable", id: m.id, domain: m.domain, label: m.label, definition: m.definition, unit: m.unit, evidence: m.evidence, exclusions: [], assumptions: [], reason: "Too few closed deals." }) : m));
    input.scores = input.scores.map((s) => (s.domain === "forecast" ? Score.parse({ status: "not_measurable", domain: s.domain, ruleSetVersion: s.ruleSetVersion, reason: "Too few checks." }) : s));
    input.findings = input.findings.filter((f) => f.ruleId !== "sec.health-check-score");
    const { figures, skipped } = benchmarkFigures(input);
    for (const key of ["historic.win_rate", "score.forecast", "sec.health_check_score"]) assert.ok(!figures.some((f) => f.metricKey === key), key);
    assert.equal(skipped.find((s) => s.metricKey === "historic.win_rate")?.reason, "Too few closed deals.");
    assert.equal(skipped.find((s) => s.metricKey === "score.forecast")?.reason, "Too few checks.");
    assert.ok(skipped.some((s) => s.metricKey === "sec.health_check_score"));
  });

  test("use only the org-wide value, never segments, series, stats or a worst segment", () => {
    const input = load(source);
    const plain = benchmarkFigures(input).figures;
    for (const m of input.metrics) {
      if (m.status !== "measured") continue;
      m.segments = m.segments.map((s) => ({ ...s, value: 9_999 }));
      m.series = m.series.map((p) => ({ ...p, value: 9_999 }));
      m.stats = Object.fromEntries(Object.keys(m.stats).map((k) => [k, 9_999]));
    }
    for (const f of input.findings) if (f.facts.worstSegmentRate !== undefined) f.facts.worstSegmentRate = 9_999;
    assert.deepEqual(benchmarkFigures(input).figures, plain);
    assert.ok(!plain.some((f) => f.value === 9_999));
  });

  test("skip a check that gives more than one result, since it isn't org-wide", () => {
    const input = load(source);
    const role = input.findings.find((f) => f.ruleId === "bp.users-without-role")!;
    input.findings.push({ ...role, id: "f-000000000000" });
    const { figures, skipped } = benchmarkFigures(input);
    assert.ok(!figures.some((f) => f.metricKey === "bp.users_without_role_rate"));
    assert.match(skipped.find((s) => s.metricKey === "bp.users_without_role_rate")!.reason, /2 results/);
  });

  test("skip a metric whose unit has changed", () => {
    const input = load(source);
    input.metrics = input.metrics.map((m) => (m.id === "historic.sales_cycle_won" ? { ...m, unit: "count" } : m));
    assert.ok(!benchmarkFigures(input).figures.some((f) => f.metricKey === "historic.sales_cycle_won"));
  });

  test("every allowlisted key fits the row format", () => {
    for (const m of BENCHMARK_METRICS) assert.ok(BenchmarkRow.shape.metricKey.safeParse(m.key).success, m.key);
    assert.equal(new Set(BENCHMARK_METRICS.map((m) => m.key)).size, BENCHMARK_METRICS.length);
  });
});

describe("bands", () => {
  test("users", () => {
    assert.equal(userBandOf(0), null);
    assert.equal(userBandOf(1), "1-50");
    assert.equal(userBandOf(50), "1-50");
    assert.equal(userBandOf(51), "51-200");
    assert.equal(userBandOf(200), "51-200");
    assert.equal(userBandOf(201), "201+");
  });

  test("deal size", () => {
    assert.equal(dealSizeBandOf(4_999.99), "under-5k");
    assert.equal(dealSizeBandOf(5_000), "5k-25k");
    assert.equal(dealSizeBandOf(24_999), "5k-25k");
    assert.equal(dealSizeBandOf(25_000), "25k-100k");
    assert.equal(dealSizeBandOf(100_000), "100k+");
  });

  test("a calendar quarter from the run's date", () => {
    assert.equal(quarterOf("2026-01-01"), "2026-Q1");
    assert.equal(quarterOf("2026-03-31"), "2026-Q1");
    assert.equal(quarterOf("2026-04-01"), "2026-Q2");
    assert.equal(quarterOf("2026-10-31"), "2026-Q4");
  });

  test("aren't guessed when the raw extracts have been purged", () => {
    const run = freshRun();
    rmSync(join(run, "checkpoints"), { recursive: true });
    assert.throws(() => exportBenchmark(options(run, tempDir())), /purged/);
    assert.ok(!existsSync(join(run, CONSENT_FILE)));
  });

  test("aren't guessed for a multi-currency org", () => {
    const run = freshRun();
    const path = join(run, "checkpoints", "opportunities.json");
    writeFileSync(path, JSON.stringify({ ...JSON.parse(readFileSync(path, "utf8")), multiCurrency: true }));
    assert.throws(() => exportBenchmark(options(run, tempDir())), /multi-currency/);
  });

  test("aren't guessed from too few won deals", () => {
    const run = freshRun();
    write(run, "calibration.json", { ...read<object>(run, "calibration.json"), minSample: 999 });
    assert.throws(() => exportBenchmark(options(run, tempDir())), /below the calibrated minimum of 999/);
  });
});

describe("hc benchmark --summary", () => {
  const row = (metricKey: string, industry: string) => ({ metricKey, ruleSetVersion: "2026.11-m3", value: 35, unit: "percent", industry, headcountBand: "50-249", userBand: "1-50", dealSizeBand: "5k-25k", period: "2026-Q4" });

  test("counts rows by metric and group, says which reach the minimum, and shows no values", () => {
    const store = tempDir();
    const path = join(store, BENCHMARK_FILE);
    appendRows(path, [...Array.from({ length: BENCHMARK_MIN_GROUP }, () => row("historic.win_rate", "technology")), row("historic.win_rate", "retail"), row("historic.slippage_rate", "retail")]);
    writeFileSync(path, `${readFileSync(path, "utf8")}${JSON.stringify({ ...row("historic.win_rate", "retail"), runId: "hc-x" })}\nnot json\n`);
    const summary = summariseBenchmarks(store);
    assert.equal(summary.rows, BENCHMARK_MIN_GROUP + 2);
    assert.equal(summary.invalidLines, 2);
    const groups = summary.groups.map((g) => [g.metricKey, g.industry, g.rows, g.enough]);
    assert.deepEqual(groups, [
      ["historic.slippage_rate", "retail", 1, false],
      ["historic.win_rate", "retail", 1, false],
      ["historic.win_rate", "technology", BENCHMARK_MIN_GROUP, true],
    ]);
    const text = formatSummary(summary).join("\n");
    assert.match(text, /historic\.win_rate: 11 rows/);
    assert.match(text, /technology, headcount 50-249, users 1-50, deal size 5k-25k: 10 rows, enough to compare/);
    assert.match(text, /retail, .*: 1 row, below 10/);
    assert.match(text, /2 lines don't match the row format/);
    assert.doesNotMatch(text, /\b35\b/, "no value is printed");
  });

  test("counts only the latest four quarters, so a repeat check doesn't add a second org", () => {
    const store = tempDir();
    const path = join(store, BENCHMARK_FILE);
    appendRows(path, [{ ...row("historic.win_rate", "technology"), period: "2025-Q4" }, { ...row("historic.win_rate", "technology"), period: "2026-Q1" }, row("historic.win_rate", "technology")]);
    const summary = summariseBenchmarks(store);
    assert.equal(summary.rows, 2);
    assert.equal(summary.olderRows, 1);
    assert.match(formatSummary(summary).join("\n"), /1 older rows aren't counted/);
  });

  test("says so when there are no rows yet", () => {
    assert.match(formatSummary(summariseBenchmarks(tempDir())).join("\n"), /No benchmark rows yet/);
  });
});

describe("the command line", () => {
  const service = fileURLToPath(new URL("../../..", import.meta.url));
  const hc = (args: string[]) => spawnSync(process.execPath, ["src/cli/hc.ts", ...args], { cwd: service, encoding: "utf8", env: { PATH: process.env.PATH, HOME: tempDir() } });

  test("hc benchmark refuses without --consent", () => {
    const run = freshRun();
    const store = tempDir();
    const result = hc(["benchmark", "--run", run, "--industry", "technology", "--headcount", "50-249", "--out", store]);
    assert.equal(result.status, 2);
    assert.match(result.stderr, /Refusing to export without --consent/);
    assert.ok(!existsSync(join(store, BENCHMARK_FILE)));
  });

  test("hc benchmark exports, then the summary counts it", () => {
    const run = freshRun();
    const store = tempDir();
    const result = hc(["benchmark", "--run", run, "--industry", "retail", "--headcount", "1-49", "--consent", CONSENT, "--out", store]);
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /Added \d+ rows to .*rows\.jsonl: users 1-50, deal size 25k-100k, 2026-Q4\./);
    const summary = hc(["benchmark", "--summary", "--out", store]);
    assert.equal(summary.status, 0);
    assert.match(summary.stdout, /score\.security: 1 row\n {2}retail, headcount 1-49, users 1-50, deal size 25k-100k: 1 row, below 10/);
  });

  test("the help lists both new commands", () => {
    const result = hc(["--help"]);
    assert.match(result.stdout, /hc benchmark --run <run folder> --industry <industry> --headcount <band> --consent/);
    assert.match(result.stdout, /hc purge --older-than <days>/);
  });
});
