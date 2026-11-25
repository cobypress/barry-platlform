import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { apiLimitsExtractor } from "../../extract/integrations.ts";
import { keptLimitsReadAt, LIMITS_AT_START_FILE, parseLimitsReading, type ExtractContext } from "../../extract/types.ts";
import { limitsExtractor } from "../../extract/inventory.ts";
import { compareWithBaseline, limitsReadAt, progressFigures, storageSentences, type ProgressSide } from "../../progress/compare.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { progressPage } from "../../render/progress.ts";
import { renderContext } from "../../render/html.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { EARLIER_AS_OF, plantRecheck, stampLimitsReading } from "../../testing/recheck.ts";
import { LIMITS_NO_PERMISSION } from "../../salesforce/refusal-words.ts";
import type { Transport } from "../../salesforce/transport.ts";
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
    assert.match(near.summary, /^When this check first connected, the org had used 95% of its daily API requests \(95,000 of 100,000\)\. Past /);
    const all = api(100_000);
    assert.equal(outcome(all), "high");
    assert.equal(all.title, "A daily allowance is used up");
    const over = api(100_500);
    assert.equal(over.title, "A daily allowance is used up");
    assert.match(over.summary, /100\.5% of its daily API requests \(100,500, over the 100,000 allowance\)/);
  });

  test("background jobs in the synthetic org, 96% used, is the trap: medium, and API requests are left out of it", () => {
    const f = one(output!.findings, "int.api-usage");
    assert.equal(outcome(f), "medium");
    assert.equal(f.title, "A daily allowance is nearly used up");
    assert.deepEqual(f.subjects.map((s) => s.label), ["background jobs"]);
    assert.equal(f.summary, "When this check first connected, the org had used 96% of its daily background (asynchronous Apex) jobs (240,000 of 250,000). Past an allowance, Salesforce can refuse further calls or jobs until use falls back, and the integrations that depend on them stop.");
    assert.notEqual(f.difficulty, "quick_fix");
  });

  test("with some left, the share never prints as 100%: 99.96% reads 99.9%, under nearly used up", () => {
    const f = api(99_960);
    assert.equal(f.title, "A daily allowance is nearly used up");
    assert.equal(outcome(f), "medium");
    assert.match(f.summary, /99\.9% of its daily API requests \(99,960 of 100,000\)/);
    assert.equal(f.facts.DailyApiRequestsShare, 99.9);
  });

  test("both allowances: the title is plural", () => {
    const both = (api: number, jobs: number): Finding => one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 100_000, Remaining: 100_000 - api }, DailyAsyncApexExecutions: { Max: 250_000, Remaining: 250_000 - jobs } }), "int.api-usage");
    assert.equal(both(96_000, 240_000).title, "Daily allowances are nearly used up");
    assert.equal(both(100_000, 240_000).title, "Daily allowances are used up or nearly used up");
    assert.equal(both(100_000, 250_000).title, "Daily allowances are used up");
    assert.equal(both(100_000, 250_000).severity, "high");
  });

  test("a reported maximum of 0 is no allowance: both at 0 doesn't apply (not in use), never a pass; one at 0 is said truly beside the other", () => {
    const zero = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 0, Remaining: 0 }, DailyAsyncApexExecutions: { Max: 0, Remaining: 0 } }), "int.api-usage");
    assert.equal(zero.status, "not_measurable");
    assert.equal(zero.notMeasurableReason, "This org has no daily allowance for API requests and background jobs, so there's nothing to use up.");
    assert.equal(zero.notMeasurableKind, "not_in_use");
    const jobsZero = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 100_000, Remaining: 99_000 }, DailyAsyncApexExecutions: { Max: 0, Remaining: 0 } }), "int.api-usage");
    assert.equal(jobsZero.status, "passed");
    assert.equal(jobsZero.summary, "When this check first connected, the org had used 1% of its daily API requests (1,000 of 100,000). This org has no daily allowance for background jobs, so there's nothing to use up.");
    const near = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 100_000, Remaining: 1_000 }, DailyAsyncApexExecutions: { Max: 0, Remaining: 0 } }), "int.api-usage");
    assert.equal(outcome(near), "medium");
    assert.deepEqual(near.subjects.map((s) => s.label), ["API requests"]);
    // Absent is still its own case: not reported, so not measurable and not "not in use".
    const absent = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 100_000, Remaining: 99_000 } }), "int.api-usage");
    assert.equal(absent.notMeasurableReason, "The org didn't report its daily allowance for background jobs, so we can't say it's within it.");
    assert.notEqual(absent.notMeasurableKind, "not_in_use");
  });

  test("more remaining than the maximum can't be read: not measurable with a true reason, never 0% and a pass", () => {
    const odd = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 15_000, Remaining: 15_200 }, DailyAsyncApexExecutions: { Max: 250_000, Remaining: 200_000 } }), "int.api-usage");
    assert.equal(odd.status, "not_measurable");
    assert.equal(odd.notMeasurableReason, "Salesforce reported more remaining than the maximum for API requests, so we can't say how much was used.");
    assert.doesNotMatch(JSON.stringify(odd), /0% of its daily API requests/);
    const beside = one(withLimits({ ...ROOMY_STORAGE, DailyApiRequests: { Max: 15_000, Remaining: 15_200 }, DailyAsyncApexExecutions: { Max: 250_000, Remaining: 1_000 } }), "int.api-usage");
    assert.equal(beside.status, "issue");
    assert.match(beside.summary, /Salesforce reported more remaining than the maximum for API requests, so that isn't checked\.$/);
  });

  test("the 99.9% case passes the number check in every document section", async () => {
    const run = await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ asyncApex: { Max: 250_000, Remaining: 10 } }), answers: loadAnswers() });
    assert.match(one(run.output!.findings, "int.api-usage").summary, /99\.9% of its daily background \(asynchronous Apex\) jobs \(249,990 of 250,000\)/);
    const report = buildReport(loadRun(run.dir).input);
    for (const built of Object.values(report.outputs)) {
      for (const s of built.sections) {
        const v = built.versions.find((x) => x.id === s.currentVersionId)!;
        assert.deepEqual(v.numberCheck.unmatched, [], `${built.sections[0]?.output} ${s.key}`);
      }
    }
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
    // Its own time, which a re-check's storage comparison is dated by.
    assert.ok(Number.isFinite(Date.parse(kept.readAt)), "the reading's time is kept");
    assert.equal(loadRun(run.dir).input.limitsReadAt, kept.readAt);
    assert.equal(parseLimitsReading({ all: {}, reported: { X: { Max: "1" } }, unread: null }), null);
    assert.equal(parseLimitsReading(null), null);
  });

  test("an org at 94.9% when the check starts, pushed to 95% by the check's own calls, still passes after a resume", async () => {
    const out = tempDir();
    // 949,490 of 1,000,000 is 94.9%; the run stops at 94.95%, ten calls in, before the limit steps run.
    const org = new Simulator({ used: 949_490, dailyLimit: 1_000_000, asyncApex: { Max: 250_000, Remaining: 200_000 } });
    const stopped = await simulated(org, out, { stopAtShare: 0.9495, inventory: true });
    assert.equal(stopped.run.status, "stopped_budget");
    for (const id of [apiLimitsExtractor.id, limitsExtractor.id]) assert.notEqual(stopped.run.steps.find((s) => s.id === id)?.status, "done", `${id} runs only after the resume`);
    const resumed = await simulated(org, out, { resume: stopped.run.id, stopAtShare: 1, inventory: true });
    assert.ok(resumed.output);
    // The resume's own connection reads 949,500 or more, 95%, with our first sitting's calls in it.
    assert.ok(stopped.run.budget.callsMade >= 10, `${stopped.run.budget.callsMade} calls before the stop`);
    const f = one(resumed.output.findings, "int.api-usage");
    assert.equal(f.status, "passed");
    assert.equal(f.facts.DailyApiRequestsUsed, 949_490);
    // The inventory's limits step uses the same kept reading (decision 726).
    const inventory = Inventory.parse(JSON.parse(readFileSync(join(resumed.dir, "inventory.json"), "utf8")));
    assert.ok("rows" in inventory.limits);
    assert.deepEqual(inventory.limits.rows.find((r) => r.name === "DailyApiRequests"), { name: "DailyApiRequests", max: 1_000_000, used: 949_490 });
  });

  test("both limit steps take the kept reading over the connection's, its reason when it couldn't be read included", async () => {
    const client = { apiVersion: "66.0", limitsAtConnect: { DailyApiRequests: { Max: 100, Remaining: 1 } }, allLimitsAtConnect: { DailyApiRequests: { Max: 100, Remaining: 1 } }, limitsUnread: null };
    const ctx = (limitsAtStart: ExtractContext["limitsAtStart"]) => ({ client, limitsAtStart }) as unknown as ExtractContext;
    const kept = { all: { DailyApiRequests: { Max: 100, Remaining: 90 } }, reported: { DailyApiRequests: { Max: 100, Remaining: 90 } }, unread: null };
    assert.deepEqual(await apiLimitsExtractor.run(ctx(kept)), { status: "ok", data: kept.reported, source: { kind: "rest", path: "/services/data/v66.0/limits" }, truncated: false });
    assert.deepEqual(await limitsExtractor.run(ctx(kept)), { status: "ok", data: kept.all, source: { kind: "rest", path: "/services/data/v66.0/limits" }, truncated: false });
    const refused = { all: {}, reported: {}, unread: LIMITS_NO_PERMISSION };
    assert.equal(parseLimitsReading(JSON.parse(JSON.stringify(refused)))?.unread, LIMITS_NO_PERMISSION, "the reason survives the file");
    for (const step of [apiLimitsExtractor, limitsExtractor]) assert.deepEqual(await step.run(ctx(refused)), { status: "unavailable", reason: LIMITS_NO_PERMISSION, source: { kind: "rest", path: "/services/data/v66.0/limits" } });
  });

  test("a first sitting that couldn't read the limits keeps why, and the resume says so though its own connection could read them", async () => {
    const out = tempDir();
    const org = new Simulator({ used: 949_490, dailyLimit: 1_000_000 });
    let refuse = true;
    const transport: Transport = {
      send: (request) =>
        refuse && new URL(request.url).pathname.endsWith("/limits")
          ? Promise.resolve({ status: 403, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "Not for this test's user." }]) })
          : org.send(request),
    };
    const stopped = await simulated(org, out, { transport, stopAtShare: 0.9495 });
    assert.equal(stopped.run.status, "stopped_budget");
    assert.notEqual(stopped.run.steps.find((s) => s.id === apiLimitsExtractor.id)?.status, "done");
    assert.equal(JSON.parse(readFileSync(join(stopped.dir, LIMITS_AT_START_FILE), "utf8")).unread, LIMITS_NO_PERMISSION);
    refuse = false;
    const resumed = await simulated(org, out, { transport, resume: stopped.run.id, stopAtShare: 1 });
    const f = one(resumed.output!.findings, "int.api-usage");
    assert.equal(f.status, "not_measurable");
    assert.equal(f.notMeasurableReason, `The org's limits couldn't be read: ${LIMITS_NO_PERMISSION}`);
  });
});

describe("a re-check's storage comparison is dated by when the kept reading was taken (decision 726)", () => {
  test("the file's own time; the run's start for a file without one; nothing for a damaged file", () => {
    const reading = { all: {}, reported: {}, unread: null };
    assert.equal(keptLimitsReadAt({ ...reading, readAt: "2026-11-01T10:00:00.000Z" }, "2026-10-01T00:00:00.000Z"), "2026-11-01T10:00:00.000Z");
    assert.equal(keptLimitsReadAt(reading, "2026-10-01T00:00:00.000Z"), "2026-10-01T00:00:00.000Z");
    assert.equal(keptLimitsReadAt({ readAt: "2026-11-01T10:00:00.000Z" }, "2026-10-01T00:00:00.000Z"), null);
    assert.equal(limitsReadAt(loadRun(dir).input.run, null), loadRun(dir).input.run.steps.find((s) => s.id === apiLimitsExtractor.id)!.finishedAt);
  });

  test("a resumed re-check whose limits step finished 108 days after its reading counts the days from the reading", () => {
    const root = mkdtempSync(join(tmpdir(), "hc-limits-at-"));
    const recheck = join(root, "hc-2026-11-09-recheck");
    plantRecheck(dir, recheck);
    // The step finished on the re-check's date; the kept reading was taken 10 days after the earlier health check's.
    stampLimitsReading(recheck, new Date(Date.parse(EARLIER_AS_OF) + 10 * 86_400_000).toISOString());
    const { input } = loadRun(recheck);
    assert.match(storageSentences(buildReport(input).progress!).join(" "), /in 10 days/);
  });
});

describe("a pass only the loosened rule allows isn't counted as fixed (decisions 726 and 732)", () => {
  const { input } = loadRun(dir);
  const report = buildReport(input);
  const side = (findings: Finding[]): ProgressSide => ({ runId: "r", asOf: input.run.asOf, findings, scores: input.scores ?? [], overall: report.overall, value: report.value, limitsReadAt: null });
  const usage = report.teamFindings.find((f) => f.ruleId === "int.api-usage")!;
  const passing = (share: number, version = 3): Finding => ({ ...usage, status: "passed", ruleVersion: version, facts: { DailyApiRequestsShare: 1, DailyAsyncApexExecutionsShare: share } });
  const earlier = (version: number, share: number): Finding => ({ ...usage, ruleVersion: version, facts: { DailyApiRequestsShare: 1, DailyAsyncApexExecutionsShare: share } });
  const others = report.teamFindings.filter((f) => f.ruleId !== "int.api-usage");
  const progress = (then: Finding, now = passing(88)) => compareWithBaseline(side([...others, then]), side([...others, now]));
  const where = (p: ReturnType<typeof progress>): string[] =>
    (["fixed", "remaining", "added", "unknown", "ruleChanged"] as const).filter((k) => (p[k] ?? []).some((c) => c.ruleId === "int.api-usage"));

  test("version 2 at 88%, a pass at 88% under version 3: passing since we changed the check, never fixed, open or can't tell", () => {
    const p = progress(earlier(2, 88));
    assert.deepEqual(where(p), ["ruleChanged"]);
    assert.equal(buildPulseResult({ ...report, progress: p }).counts.fixed, p.fixed.length);
    assert.ok(!buildPulseResult({ ...report, progress: p }).fixed.some((x) => x.title === usage.title), "the results email lists no fix for it");
  });

  test("version 1 or 2 at 96%, a pass at 88%: the old line would still raise it, so not fixed (726 called it fixed)", () => {
    assert.deepEqual(where(progress(earlier(2, 96))), ["ruleChanged"]);
    assert.deepEqual(where(progress(earlier(1, 96))), ["ruleChanged"]);
  });

  test("a pass the old line would pass too (40%) is fixed, whatever the earlier share; so is a version 3 issue passing at 88%", () => {
    assert.deepEqual(where(progress(earlier(2, 88), passing(40))), ["fixed"]);
    assert.deepEqual(where(progress(earlier(2, 96), passing(40))), ["fixed"]);
    assert.deepEqual(where(progress(earlier(3, 96))), ["fixed"]);
  });

  test("an ordinary version bump is still a fix: storage's version 2 issue passing under version 3", () => {
    const storage = report.teamFindings.find((f) => f.ruleId === "tech.storage-use")!;
    const p = compareWithBaseline(side([...others.filter((f) => f.ruleId !== storage.ruleId), { ...storage, status: "issue", ruleVersion: 2 }, earlier(3, 96)]), side([...others.filter((f) => f.ruleId !== storage.ruleId), { ...storage, status: "passed", ruleVersion: 3 }, passing(40)]));
    assert.ok(p.fixed.some((c) => c.ruleId === storage.ruleId));
    assert.equal(p.ruleChanged, undefined, "nothing to say: the key is absent, so a comparison is as before");
  });

  test("the re-check says it once, out of the counts, and the raw notes name it", () => {
    const p = progress(earlier(2, 88));
    const built = { ...report, progress: p };
    const html = progressPage(renderContext(new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]), built.input.calibration.currency, false), built);
    assert.match(html, /We changed how we check 1 more since then, and they pass now\. We can&#39;t say anything in your org changed, so they aren&#39;t counted as fixed\./);
    assert.ok(progressFigures(p).includes(1));
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
