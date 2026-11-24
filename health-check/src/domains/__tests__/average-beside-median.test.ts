import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { opportunitiesExtractor, stagesExtractor, type OpportunityRow } from "../../extract/opportunities.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { usersExtractor } from "../../extract/users.ts";
import { analyse } from "../../engine/evaluate.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { Calibration, type Metric } from "../../schemas/index.ts";
import { historicPipelineModule } from "../historic-pipeline/index.ts";
import { median } from "../historic-pipeline/metrics.ts";
import { averageApart, averageBeside, DURATION_MEDIANS, meanStat, MIN_AVERAGE_VALUES } from "../average.ts";
import { buildThemes } from "../../interviews/readings.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { buildReport } from "../../narrative/sections.ts";
import { fullReportHtml, fullReportHtmlAt } from "../../render/full-report.ts";
import { loadRun } from "../../render/render-run.ts";
import { plainWords, ukDates } from "../../render/format.ts";
import { pageDataset } from "../../render/page-dataset.ts";
import { checkNumbers } from "../../numbercheck/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { serviceCasesExtractor, type ServiceCases } from "../../extract/service.ts";
import { ALL_MODULES } from "../index.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";

// The average beside a duration's median (DECISIONS.md 709): kept as the stat
// `mean` from five values, shown in brackets after the median only when more
// than 10% away, and read by no rule.

type Measured = Extract<Metric, { status: "measured" }>;
const measured = (m: Metric | undefined): Measured => {
  assert.equal(m?.status, "measured", m?.status === "not_measurable" ? m.reason : "missing");
  return m as Measured;
};

describe("the average kept beside a median", () => {
  test("is the mean, to one decimal place, on odd and even counts", () => {
    assert.deepEqual(meanStat([10, 20, 30, 40, 50]), { mean: 30 });
    assert.deepEqual(meanStat([10, 20, 30, 40, 50, 61]), { mean: 35.2 });
    assert.equal(median([10, 20, 30, 40, 50, 61]), 35, "the median of an even count is the middle two's midpoint");
  });
  test("counts ties as the values they are", () => {
    assert.deepEqual(meanStat([20, 20, 20, 20, 100]), { mean: 36 });
    assert.equal(median([20, 20, 20, 20, 100]), 20);
  });
  test("moves with one extreme outlier where the median doesn't", () => {
    const even = [10, 20, 30, 40, 50];
    const far = [10, 20, 30, 40, 5000];
    assert.equal(median(even), median(far));
    assert.deepEqual([meanStat(even).mean, meanStat(far).mean], [30, 1020]);
  });
  test("is withheld below five values, whatever the calibrated minimum", () => {
    assert.equal(MIN_AVERAGE_VALUES, 5);
    assert.deepEqual(meanStat([1, 2, 3, 400]), {});
    assert.deepEqual(meanStat([]), {});
    assert.deepEqual(meanStat([1, 2, 3, 4, 400]), { mean: 82 });
  });
});

describe("the average shown beside a median", () => {
  const days = (id: string, value: number, stats: Record<string, number>, n = 40): Metric =>
    ({ id, status: "measured", value, n, unit: "days", stats, domain: "historic_pipeline" }) as unknown as Metric;
  test("only when it's more than 10% from the median", () => {
    assert.equal(averageApart(50, 55), false, "exactly 10% is one figure");
    assert.equal(averageApart(50, 55.1), true);
    assert.equal(averageApart(50, 44.9), true, "below as well as above");
    assert.equal(averageApart(0, 0.1), true, "a median of 0 says nothing about an average that isn't");
    assert.equal(averageBeside(days("historic.sales_cycle_won", 50, { mean: 54 })), null);
    assert.equal(averageBeside(days("historic.sales_cycle_won", 50, { mean: 73 })), 73);
    assert.equal(averageBeside(days("historic.close_days_late", -10, { mean: -20 })), -20, "early deals are days too");
  });
  test("never from fewer than five values, a metric that isn't a duration's median, or one with no average", () => {
    assert.equal(averageBeside(days("historic.sales_cycle_won", 50, { mean: 73 }, 4)), null);
    assert.equal(averageBeside(days("historic.quarterly_median_cycle", 50, { mean: 73 })), null);
    assert.equal(averageBeside(days("historic.sales_cycle_won", 50, {})), null);
    assert.equal(averageBeside({ id: "historic.sales_cycle_won", status: "not_measurable" } as unknown as Metric), null);
  });
});

// The won deals' cycles, by hand: each created on a day and closed some whole
// days later at the start of its close day, so every cycle is that many days less
// 9 hours. Each closes inside the window, the latest in May 2026.
const NOW = new Date("2026-06-30T12:00:00.000Z");
const WINDOW = { from: "2024-06-30", to: "2026-06-30" };
const calibration = Calibration.parse({
  version: 1,
  confirmedBy: "Test",
  confirmedAt: "2026-06-01T00:00:00.000Z",
  stages: [
    { name: "A", category: "open", order: 1 },
    { name: "Won", category: "won" },
    { name: "Lost", category: "lost" },
  ],
  scope: { recordTypes: "all" },
  targets: { source: "none" },
  minSample: 2,
});
const won = (i: number, cycle: number): OpportunityRow => {
  const created = new Date(Date.UTC(2025, 0, 1 + i));
  const closed = new Date(created.getTime() + cycle * 86_400_000);
  return {
    Id: `W${String(i).padStart(4, "0")}`,
    OwnerId: "005000000000001AAA",
    StageName: "Won",
    Amount: 100,
    CloseDate: closed.toISOString().slice(0, 10),
    CreatedDate: `${created.toISOString().slice(0, 10)}T09:00:00.000+0000`,
    IsClosed: true,
    IsWon: true,
    LastActivityDate: null,
    Type: "New",
    LeadSource: "Web",
    RecordType: "Standard",
    CurrencyIsoCode: null,
    LossReason: null,
  };
};
function cycleOf(cycles: number[]): Measured {
  const source = { kind: "soql", api: "rest", query: "SELECT Id FROM Opportunity" } as const;
  const extracts = new Map<string, unknown>([
    [orgExtractor.id, { status: "ok", source, truncated: false, data: [{ FiscalYearStartMonth: 1 }] }],
    [opportunitiesExtractor.id, { opportunities: { status: "ok", data: cycles.map((c, i) => won(i, c)), source, truncated: false }, history: { status: "ok", data: [], source, truncated: false }, multiCurrency: false, lossReasonField: null, windowFrom: WINDOW.from }],
    [usersExtractor.id, { status: "ok", source, truncated: false, data: [{ Id: "005000000000001AAA", Name: "Test Rep", Username: "t@example.com", Email: "t@example.com", IsActive: true, UserRole: { Name: "UK" } }] }],
    [stagesExtractor.id, { status: "ok", source, truncated: false, data: [] }],
  ]);
  const out = analyse({ modules: [{ ...historicPipelineModule, rules: [] }], extracts, calibration, now: NOW, window: WINDOW, answers: [], pseudonyms: new Pseudonymiser(), userDirectory: new Map() });
  return measured(out.metrics.find((m) => m.id === "historic.sales_cycle_won"));
}
// Every cycle is 9 hours short of its whole days: 0.375 of a day.
const less = (d: number): number => Math.round((d - 0.375) * 10) / 10;

describe("a sales cycle's average, from the deals", () => {
  test("odd count: the median leads and the average is kept beside it, the count it rests on with it", () => {
    const m = cycleOf([10, 20, 30, 40, 50]);
    assert.deepEqual([m.value, m.stats.mean, m.n], [less(30), less(30), 5]);
    assert.equal(averageBeside(m), null, "the same figure twice is one figure");
  });
  test("even count and ties", () => {
    assert.deepEqual([cycleOf([10, 20, 30, 40, 50, 60]).value, cycleOf([10, 20, 30, 40, 50, 60]).stats.mean], [less(35), less(35)]);
    const ties = cycleOf([20, 20, 20, 20, 100]);
    assert.deepEqual([ties.value, ties.stats.mean], [less(20), less(36)]);
    assert.equal(averageBeside(ties), less(36));
  });
  test("one extreme outlier moves the average and not the median", () => {
    const m = cycleOf([10, 20, 30, 40, 500]);
    assert.deepEqual([m.value, m.stats.mean], [less(30), less(120)]);
    assert.equal(averageBeside(m), less(120));
  });
  test("below five deals, no average is kept, though the calibrated minimum allows the median", () => {
    const m = cycleOf([10, 20, 30, 500]);
    assert.equal(m.value, less(25));
    assert.equal("mean" in m.stats, false);
  });
});

describe("what people told us: a sales cycle", () => {
  const interview = { id: "ia-0000000001", role: "sales_leader" as const, heldOn: "2026-10-27", notes: "", answers: { sales_cycle: 60 }, version: 1 };
  const cycle = (mean: number): Metric => ({ id: "historic.sales_cycle_won", status: "measured", value: 41, n: 50, unit: "days", stats: { mean }, domain: "historic_pipeline" }) as unknown as Metric;
  test("gives the average beside the median when it's more than 10% away, and reads against the median alone", () => {
    const far = buildThemes([interview], [], [cycle(73)])[0]!;
    assert.equal(far.found, "We measured a median of 41 days from creation to close for won deals (average 73).");
    assert.equal(far.reading, "disagrees", "60 is judged against 41, not 73");
    assert.equal(buildThemes([interview], [], [cycle(44)])[0]!.found, "We measured a median of 41 days from creation to close for won deals.");
  });
});

// The synthetic org, end to end: the figures it carries and the lines that show them.
const { dir } = await runFromRecording({ answers: loadAnswers() });
const loaded = loadRun(dir);
const report = buildReport(loaded.input);
const byId = new Map(report.metrics.map((m) => [m.id, m]));

describe("the synthetic org", () => {
  test("every duration's median carries its average from five values, and the value is still the median", () => {
    const metrics = JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Metric[];
    const durations = metrics.filter((m) => DURATION_MEDIANS.has(m.id) && m.status === "measured") as Measured[];
    assert.ok(durations.length >= 7, `${durations.length} durations measured`);
    for (const m of durations) assert.equal(typeof m.stats.mean, "number", m.id);
    assert.deepEqual(
      Object.fromEntries(durations.map((m) => [m.id, [m.value, m.stats.mean]])),
      {
        "historic.sales_cycle_won": [70, 79.9],
        "historic.sales_cycle_lost": [52.5, 58.5],
        "historic.slippage_days": [42.5, 43.8],
        "historic.close_days_late": [24, 26.5],
        "historic.stage_velocity_won": [69.5, 79.5],
        "historic.stage_velocity_lost": [49, 55.6],
        "current.median_days_in_stage": [89.5, 123.6],
        "current.open_median_age": [101.5, 131.5],
      },
    );
  });
  test("the full report gives the average beside the median, and only one figure when they're within 10%", () => {
    const html = fullReportHtml(report, loaded.pseudonyms);
    assert.match(html, /<td class="num">70 days \(average 79\.9\)<\/td>/);
    assert.match(html, /<td class="num">52\.5 days \(average 58\.5\)<\/td>/);
    assert.match(html, /the median won deal closed 24 days after its first close date \(average 26\.5\)\./);
    assert.equal(averageBeside(byId.get("historic.slippage_days")), null, "42.5 and 43.8 are within 10%");
    assert.match(html, /<td class="num">42\.5 days<\/td>/);
    assert.doesNotMatch(html, /43\.8/);
    // The roomiest deck gives close dates a panel of their own.
    assert.match(fullReportHtmlAt(report, loaded.pseudonyms, 0), /<p class="note">For the median won deal, against an average of 26\.5 days\. The typical range is 7 to 44 days\.<\/p>/);
  });
  test("each new line reads as plain English as written, and its every figure traces to the run's data", () => {
    const set = pageDataset(report.dataset, loaded.pseudonyms);
    const lines = [
      "70 days (average 79.9)",
      "66.5% of the 164 won deals we can follow through their stages closed in the quarter first forecast, and the median won deal closed 24 days after its first close date (average 26.5).",
      "For the median won deal, against an average of 26.5 days. The typical range is 7 to 44 days.",
      "We measured a median of 70 days from creation to close for won deals (average 79.9).",
    ];
    for (const line of lines) {
      assert.equal(plainWords(ukDates(line)), line, line);
      assert.deepEqual(checkNumbers(line, set).unmatched.map((u) => u.raw), [], line);
    }
  });
  test("an automatic run's digest carries the averages it shows, and is still released", async () => {
    const auto = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });
    const run = loadRun(auto.dir);
    const checked = checkedDigest({ input: run.input, pseudonyms: run.pseudonyms, inventory: null, madeAt: "2026-10-31T12:00:00.000Z" });
    assert.equal(checked.held, null);
    const shown = Object.fromEntries(checked.digest!.metrics.flatMap((m) => (m.status === "measured" && m.average !== undefined ? [[m.id, [m.value, m.average]]] : [])));
    assert.deepEqual(shown, {
      "historic.sales_cycle_won": [70, 79.9],
      "historic.close_days_late": [24, 26.5],
      "historic.stage_velocity_won": [69.5, 79.5],
      "historic.stage_velocity_lost": [49, 55.6],
      "current.median_days_in_stage": [89.5, 123.6],
      "current.open_median_age": [101.5, 131.5],
    });
  });
});

describe("no rule reads an average", () => {
  test("high-priority cases are judged on their median hours, so one very slow case doesn't turn a pass", async () => {
    const { dir: full } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
    const extracts = clone(loadExtracts(full));
    const cases = extracts.get(serviceCasesExtractor.id) as ServiceCases;
    assert.equal(cases.closedCases.status, "ok");
    const rows = cases.closedCases.status === "ok" ? cases.closedCases.data : [];
    const iso = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
    // High priority closes in 5 hours and the rest in 10, save one high-priority case left open 2,000 hours:
    // the high median stays 5 while its average passes 10.
    const high = rows.filter((c) => c.Priority === "High");
    assert.ok(high.length >= 5, `${high.length} high-priority cases`);
    rows.forEach((c) => Object.assign(c, { ClosedDate: iso(Date.parse(c.CreatedDate) + (c.Priority === "High" ? 5 : 10) * 3_600_000) }));
    Object.assign(high[0]!, { ClosedDate: iso(Date.parse(high[0]!.CreatedDate) + 2000 * 3_600_000) });
    const hours = high.map((c) => (Date.parse(c.ClosedDate) - Date.parse(c.CreatedDate)) / 3_600_000);
    assert.ok(hours.reduce((a, b) => a + b, 0) / hours.length > 10, "the high-priority average is past the rest's");
    const finding = analyseExtracts(extracts, { modules: ALL_MODULES }).findings.find((f) => f.ruleId === "svc.priority-resolution")!;
    assert.equal(finding.status, "passed", finding.summary);
    assert.deepEqual([finding.facts.highMedianHours, finding.facts.otherMedianHours], [5, 10]);
  });
});
