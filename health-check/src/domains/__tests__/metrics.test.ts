import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { fiscalQuarter, median, monthEnds, quantile } from "../historic-pipeline/metrics.ts";
import { opportunitiesExtractor, stagesExtractor, type HistoryRow, type OpportunityRow } from "../../extract/opportunities.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { usersExtractor } from "../../extract/users.ts";
import { analyse } from "../../engine/evaluate.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { Calibration, type Metric } from "../../schemas/index.ts";
import { historicPipelineModule } from "../historic-pipeline/index.ts";

// Every expected value below was worked out by hand from this table.
//
//   D1 won    Jan 1 to Mar 2 (60 days), A B C. Close date pushed Mar 1 to Apr 15.
//   D2 won    Feb 1 to Mar 3 (30 days), A then straight to won.
//   D3 lost   Jan 15 to Feb 15 (31 days), A B.
//   D4 lost   Mar 1 to Mar 11 (10 days), A.
//   D5 no decision, Jan 20 to Apr 1, A.
//   D6 in an excluded stage, never counted.
//   D7 won    Jan 1 to Mar 1 (59 days). Lost on Feb 1, reopened at B on Feb 10.

const NOW = new Date("2026-06-30T12:00:00.000Z");
const WINDOW = { from: "2024-06-30", to: "2026-06-30" };

const calibration = (overrides: Record<string, unknown> = {}): Calibration =>
  Calibration.parse({
    version: 1,
    confirmedBy: "Test",
    confirmedAt: "2026-06-01T00:00:00.000Z",
    stages: [
      { name: "A", category: "open", order: 1 },
      { name: "B", category: "open", order: 2 },
      { name: "C", category: "open", order: 3 },
      { name: "Won", category: "won" },
      { name: "Lost", category: "lost" },
      { name: "NoDec", category: "no_decision" },
      { name: "Junk", category: "excluded" },
    ],
    scope: { recordTypes: "all" },
    targets: { source: "none" },
    minSample: 2,
    salesCycleAssumptionDays: 45,
    ...overrides,
  });

let rowId = 0;
const row = (OpportunityId: string, StageName: string, at: string, CloseDate: string, Amount: number | null = 100): HistoryRow => ({
  Id: `h${String(++rowId).padStart(4, "0")}`,
  OpportunityId,
  StageName,
  Amount,
  CloseDate,
  CreatedDate: `${at}T09:00:00.000+0000`,
});

const deal = (Id: string, StageName: string, created: string, CloseDate: string, Amount: number | null, IsWon = false): OpportunityRow => ({
  Id,
  OwnerId: "005000000000001AAA",
  StageName,
  Amount,
  CloseDate,
  CreatedDate: `${created}T09:00:00.000+0000`,
  IsClosed: StageName !== "A" && StageName !== "B",
  IsWon,
  LastActivityDate: null,
  Type: "New",
  LeadSource: "Web",
  RecordType: "Standard",
  CurrencyIsoCode: null,
  LossReason: null,
});

const DEALS: OpportunityRow[] = [
  deal("D1", "Won", "2026-01-01", "2026-04-15", 100, true),
  deal("D2", "Won", "2026-02-01", "2026-03-31", 300, true),
  deal("D3", "Lost", "2026-01-15", "2026-05-01", 100),
  deal("D4", "Lost", "2026-03-01", "2026-05-01", 100),
  deal("D5", "NoDec", "2026-01-20", "2026-05-01", 50),
  deal("D6", "Junk", "2026-01-20", "2026-05-01", 999),
  deal("D7", "Won", "2026-01-01", "2026-03-01", 200, true),
];

const HISTORY: HistoryRow[] = [
  row("D1", "A", "2026-01-01", "2026-03-01"),
  row("D1", "B", "2026-01-10", "2026-03-01"),
  row("D1", "C", "2026-02-01", "2026-04-15"),
  row("D1", "Won", "2026-03-02", "2026-04-15"),
  row("D2", "A", "2026-02-01", "2026-03-31", 300),
  row("D2", "Won", "2026-03-03", "2026-03-31", 300),
  row("D3", "A", "2026-01-15", "2026-05-01"),
  row("D3", "B", "2026-02-01", "2026-05-01"),
  row("D3", "Lost", "2026-02-15", "2026-05-01"),
  row("D4", "A", "2026-03-01", "2026-05-01"),
  row("D4", "Lost", "2026-03-11", "2026-05-01"),
  row("D5", "A", "2026-01-20", "2026-05-01", 50),
  row("D5", "NoDec", "2026-04-01", "2026-05-01", 50),
  row("D6", "Junk", "2026-01-20", "2026-05-01", 999),
  row("D7", "A", "2026-01-01", "2026-03-01", 200),
  row("D7", "Lost", "2026-02-01", "2026-03-01", 200),
  row("D7", "B", "2026-02-10", "2026-03-01", 200),
  row("D7", "Won", "2026-03-01", "2026-03-01", 200),
];

function metrics(options: { calibration?: Calibration; deals?: OpportunityRow[]; history?: HistoryRow[]; multiCurrency?: boolean; orgFiscal?: number | null } = {}): Map<string, Metric> {
  const source = { kind: "soql", api: "rest", query: "SELECT Id FROM Opportunity" } as const;
  const orgFiscal = options.orgFiscal === undefined ? 1 : options.orgFiscal;
  const extracts = new Map<string, unknown>([
    [orgExtractor.id, orgFiscal === null ? { status: "unavailable", reason: "no", source } : { status: "ok", source, truncated: false, data: [{ FiscalYearStartMonth: orgFiscal }] }],
    [opportunitiesExtractor.id, {
      opportunities: { status: "ok", data: options.deals ?? DEALS, source, truncated: false },
      history: { status: "ok", data: options.history ?? HISTORY, source, truncated: false },
      multiCurrency: options.multiCurrency ?? false,
      lossReasonField: null,
      windowFrom: WINDOW.from,
    }],
    [usersExtractor.id, { status: "ok", source, truncated: false, data: [{ Id: "005000000000001AAA", Name: "Test Rep", Username: "t@example.com", Email: "t@example.com", IsActive: true, UserRole: { Name: "UK" } }] }],
    [stagesExtractor.id, { status: "ok", source, truncated: false, data: [] }],
  ]);
  const out = analyse({
    modules: [{ ...historicPipelineModule, rules: [] }],
    extracts,
    calibration: options.calibration ?? calibration(),
    now: NOW,
    window: WINDOW,
    answers: [],
    pseudonyms: new Pseudonymiser(),
    userDirectory: new Map([["005000000000001AAA", { name: "Test Rep", aliases: ["t@example.com"] }]]),
  });
  return new Map(out.metrics.map((m) => [m.id, m]));
}

function value(all: Map<string, Metric>, id: string): number {
  const metric = all.get(id)!;
  assert.equal(metric.status, "measured", `${id}: ${metric.status === "not_measurable" ? metric.reason : ""}`);
  return metric.status === "measured" ? metric.value : NaN;
}

const measured = (m: Metric | undefined): Extract<Metric, { status: "measured" }> => {
  assert.equal(m?.status, "measured");
  return m as Extract<Metric, { status: "measured" }>;
};

describe("win rate", () => {
  const all = metrics();
  test("won over won plus lost: 3 of 5", () => assert.equal(value(all, "historic.win_rate"), 60));
  test("excluded stages and no decision are out of the sample", () => assert.equal(measured(all.get("historic.win_rate")).n, 5));
  test("by value: 600 of 800", () => assert.equal(value(all, "historic.win_rate_by_value"), 75));
  test("counting no decision as a loss: 3 of 6", () => assert.equal(value(all, "historic.win_rate_including_no_decision"), 50));
  test("no decision rate: 1 of 6", () => assert.equal(value(all, "historic.no_decision_rate"), 16.7));
  test("the exclusion is declared", () => assert.ok(measured(all.get("historic.win_rate")).exclusions.some((e) => e.includes("1 opportunities in stages the calibration excludes"))));

  test("calibration decides what a stage means, not the stage's name", () => {
    const flipped = calibration({
      stages: [
        { name: "A", category: "open", order: 1 },
        { name: "B", category: "open", order: 2 },
        { name: "C", category: "open", order: 3 },
        { name: "Won", category: "lost" },
        { name: "Lost", category: "won" },
        { name: "NoDec", category: "won" },
        { name: "Junk", category: "excluded" },
      ],
    });
    // Lost and NoDec now win (3), Won loses (3).
    assert.equal(value(metrics({ calibration: flipped }), "historic.win_rate"), 50);
  });
});

describe("sales cycle", () => {
  const all = metrics();
  test("median of 30, 59, 60", () => assert.equal(value(all, "historic.sales_cycle_won"), 59));
  test("median of 10 and 31", () => assert.equal(value(all, "historic.sales_cycle_lost"), 20.5));
  test("quartiles and the stated assumption sit alongside", () => {
    assert.deepEqual(measured(all.get("historic.sales_cycle_won")).stats, { p25: 44.5, p75: 59.5, assumedDays: 45 });
  });
  test("a reopened deal is timed to its final close, not its first", () => {
    const only = metrics({ calibration: calibration({ minSample: 1 }), deals: DEALS.filter((d) => d.Id === "D7"), history: HISTORY.filter((h) => h.OpportunityId === "D7") });
    assert.equal(value(only, "historic.sales_cycle_won"), 59);
  });
  test("with no closing history, CloseDate is used and the assumption says so", () => {
    const deals = [deal("X1", "Won", "2026-01-01", "2026-01-31", 10, true), deal("X2", "Won", "2026-01-01", "2026-03-02", 10, true)];
    const metric = measured(metrics({ deals, history: [] }).get("historic.sales_cycle_won"));
    assert.ok(metric.assumptions.some((a) => a.includes("2 deals have no closing history row")));
  });
});

describe("stage conversion", () => {
  const metric = measured(metrics().get("historic.stage_conversion"));
  const step = (key: string) => metric.segments.find((s) => s.dimension === "step" && s.key === key)!;
  const skipped = (key: string) => metric.segments.find((s) => s.dimension === "skipped stage" && s.key === key)!;

  test("overall: 3 won of 6 that started", () => {
    assert.equal(metric.value, 50);
    assert.equal(metric.n, 6);
  });
  test("A to B: 4 of 6", () => assert.equal(step("A to B").value, 66.7));
  test("B to C: 3 of 4", () => assert.equal(step("B to C").value, 75));
  test("C to won: 3 of 3", () => assert.equal(step("C to won").value, 100));
  test("a deal that skipped a stage is counted through it, and the skip is visible", () => {
    assert.equal(skipped("B").value, 25);
    assert.equal(skipped("C").value, 66.7);
  });
  test("deals with no stage history are excluded and declared", () => {
    const deals = [...DEALS, deal("X", "Won", "2026-01-01", "2026-02-01", 10, true)];
    const m = measured(metrics({ deals }).get("historic.stage_conversion"));
    assert.equal(m.n, 6);
    assert.ok(m.exclusions.some((e) => e.includes("1 deals with no recorded open stage")));
  });
});

describe("a retired stage nobody used in the window", () => {
  test("isn't shown as a step every deal passed through", () => {
    // As seen on a real org: retired stages early in the order read as 100% steps.
    const stages = [{ name: "Old", category: "open", order: 0 }, ...calibration().stages];
    const m = measured(metrics({ calibration: calibration({ stages }) }).get("historic.stage_conversion"));
    assert.ok(!m.segments.some((s) => s.key.startsWith("Old")), "no step from the retired stage");
    assert.equal(m.segments.find((s) => s.dimension === "step" && s.key === "A to B")?.value, 66.7);
    assert.ok(m.exclusions.some((e) => e === "Stages no closed deal in the window was in: Old"));
  });
});

describe("slippage", () => {
  const all = metrics();
  test("1 of 6 closed deals moved later", () => assert.equal(value(all, "historic.slippage_rate"), 16.7));
  test("the same deal crossed from Q1 into Q2", () => assert.equal(value(all, "historic.slippage_quarter_cross_rate"), 16.7));
  test("days pushed needs its own minimum sample", () => {
    const metric = all.get("historic.slippage_days");
    assert.equal(metric?.status, "not_measurable");
  });
  test("45 days is the median when two deals slip by 45", () => {
    const history = [...HISTORY, row("D2", "A", "2026-02-02", "2026-05-15", 300)].sort((a, b) => a.CreatedDate.localeCompare(b.CreatedDate));
    assert.equal(value(metrics({ history }), "historic.slippage_days"), 45);
  });
  test("moving a date earlier isn't slippage", () => {
    const deals = DEALS.filter((d) => ["D3", "D4"].includes(d.Id));
    const history = [row("D3", "A", "2026-01-15", "2026-05-01"), row("D3", "Lost", "2026-02-15", "2026-02-15"), row("D4", "A", "2026-03-01", "2026-05-01"), row("D4", "Lost", "2026-03-11", "2026-05-01")];
    assert.equal(value(metrics({ deals, history }), "historic.slippage_rate"), 0);
  });
});

describe("point-in-time pipeline", () => {
  const counts = measured(metrics().get("historic.pipeline_point_in_time_count"));
  const values = measured(metrics().get("historic.pipeline_point_in_time_value"));
  const at = (metric: typeof counts, date: string) => metric.series.find((p) => p.date === date)!;

  test("end of January: D1, D3, D5, D7", () => {
    assert.equal(at(counts, "2026-01-31").value, 4);
    assert.equal(at(values, "2026-01-31").value, 100 + 100 + 50 + 200);
  });
  test("end of February: D3 has lost, D2 has opened, D7 has reopened", () => {
    assert.equal(at(counts, "2026-02-28").value, 4);
    assert.equal(at(values, "2026-02-28").value, 100 + 300 + 50 + 200);
  });
  test("end of March: only D5", () => {
    assert.equal(at(counts, "2026-03-31").value, 1);
    assert.equal(at(values, "2026-03-31").value, 50);
  });
  test("end of April: nothing open, and that's a real zero", () => {
    assert.equal(at(counts, "2026-04-30").value, 0);
    assert.equal(at(counts, "2026-04-30").notMeasurableReason, null);
  });
  test("months before any history are not measurable, never zero", () => {
    const point = at(counts, "2025-06-30");
    assert.equal(point.value, null);
    assert.match(point.notMeasurableReason!, /earliest stage history \(2026-01-01\)/);
  });
  test("uses the amount recorded at the time, not today's", () => {
    const deals = [deal("Y", "Won", "2026-01-01", "2026-05-01", 900, true)];
    const history = [row("Y", "A", "2026-01-05", "2026-05-01", 100), row("Y", "B", "2026-02-10", "2026-05-01", 500), row("Y", "Won", "2026-04-05", "2026-05-01", 900)];
    const m = measured(metrics({ deals, history, calibration: calibration({ minSample: 1 }) }).get("historic.pipeline_point_in_time_value"));
    assert.equal(at(m, "2026-01-31").value, 100);
    assert.equal(at(m, "2026-02-28").value, 500);
  });
});

describe("not measurable, never estimated", () => {
  test("a stage the calibration doesn't know stops everything that depends on it", () => {
    const all = metrics({ deals: [...DEALS, deal("Z", "Mystery", "2026-01-01", "2026-02-01", 10)], history: [...HISTORY, row("Z", "Mystery", "2026-01-01", "2026-02-01")] });
    for (const id of ["historic.win_rate", "historic.stage_conversion", "historic.sales_cycle_won", "historic.slippage_rate", "historic.pipeline_point_in_time_count"]) {
      const metric = all.get(id)!;
      assert.equal(metric.status, "not_measurable", id);
      assert.match(metric.status === "not_measurable" ? metric.reason : "", /Mystery/);
    }
  });

  test("a retired stage in history stops conversion but not win rate", () => {
    const history = [row("D3", "OldStage", "2026-01-16", "2026-05-01"), ...HISTORY];
    const all = metrics({ history });
    assert.equal(all.get("historic.stage_conversion")?.status, "not_measurable");
    assert.equal(all.get("historic.win_rate")?.status, "measured");
  });

  test("too few deals", () => {
    const metric = metrics({ calibration: calibration({ minSample: 50 }) }).get("historic.win_rate");
    assert.equal(metric?.status, "not_measurable");
    assert.match(metric?.status === "not_measurable" ? metric.reason : "", /below the calibrated minimum of 50/);
  });

  test("a thin segment stays listed with its reason, and no value", () => {
    const metric = measured(metrics({ calibration: calibration({ minSample: 4 }) }).get("historic.win_rate"));
    const quarter = metric.segments.find((s) => s.dimension === "closed quarter" && s.n < 4);
    if (quarter) {
      assert.equal(quarter.value, null);
      assert.match(quarter.notMeasurableReason!, /Sample below the calibrated minimum/);
    }
  });

  test("multi-currency stops value metrics, not counts", () => {
    const all = metrics({ multiCurrency: true });
    assert.equal(all.get("historic.win_rate_by_value")?.status, "not_measurable");
    assert.equal(all.get("historic.pipeline_point_in_time_value")?.status, "not_measurable");
    assert.equal(all.get("historic.win_rate")?.status, "measured");
    assert.equal(all.get("historic.pipeline_point_in_time_count")?.status, "measured");
  });

  test("too many blank amounts stops the value rate", () => {
    const deals = DEALS.map((d) => (d.StageName === "Lost" || d.Id === "D1" ? { ...d, Amount: null } : d));
    const metric = metrics({ deals }).get("historic.win_rate_by_value");
    assert.equal(metric?.status, "not_measurable");
  });

  test("an unreadable history makes every metric not measurable", () => {
    const source = { kind: "soql", api: "rest", query: "SELECT Id FROM OpportunityHistory" } as const;
    const extracts = new Map<string, unknown>([
      [orgExtractor.id, { status: "ok", source, truncated: false, data: [{ FiscalYearStartMonth: 1 }] }],
      [opportunitiesExtractor.id, { opportunities: { status: "ok", data: DEALS, source, truncated: false }, history: { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source }, multiCurrency: false, lossReasonField: null, windowFrom: WINDOW.from }],
      [usersExtractor.id, { status: "ok", source, truncated: false, data: [] }],
      [stagesExtractor.id, { status: "ok", source, truncated: false, data: [] }],
    ]);
    const out = analyse({ modules: [{ ...historicPipelineModule, rules: [] }], extracts, calibration: calibration(), now: NOW, window: WINDOW, answers: [], pseudonyms: new Pseudonymiser(), userDirectory: new Map() });
    assert.ok(out.metrics.length > 0);
    assert.ok(out.metrics.every((m) => m.status === "not_measurable"));
  });
});

describe("deals created already closed", () => {
  const createdWon = [deal("W1", "Won", "2026-02-01", "2026-02-01", 100, true), deal("W2", "Won", "2026-02-01", "2026-02-01", 100, true)];
  const createdLost = [deal("L1", "Lost", "2026-02-01", "2026-02-01", 100)];
  const loaded = [row("W1", "Won", "2026-02-01", "2026-02-01"), row("W2", "Won", "2026-02-01", "2026-02-01"), row("L1", "Lost", "2026-02-01", "2026-02-01")];
  const all = metrics({ deals: [...DEALS, ...createdWon, ...createdLost], history: [...HISTORY, ...loaded] });

  test("don't inflate conversion: still 3 of 6", () => {
    const m = measured(all.get("historic.stage_conversion"));
    assert.equal(m.value, 50);
    assert.equal(m.n, 6);
    assert.ok(m.exclusions.some((e) => e.includes("3 deals with no recorded open stage")));
  });
  test("don't pull the sales cycle down with 0-day deals", () => {
    const m = measured(all.get("historic.sales_cycle_won"));
    assert.equal(m.value, 59);
    assert.ok(m.exclusions.some((e) => e.includes("2 deals created already closed")));
  });
  test("aren't in the slippage denominator", () => assert.equal(measured(all.get("historic.slippage_rate")).n, 6));
  test("still count as real outcomes in the win rate", () => assert.equal(value(all, "historic.win_rate"), 62.5));
});

describe("a deal closed before it was created", () => {
  test("is excluded from the sales cycle, not clamped to zero", () => {
    const deals = [...DEALS, deal("B", "Won", "2026-02-20", "2026-01-01", 10, true)];
    const m = measured(metrics({ deals }).get("historic.sales_cycle_won"));
    assert.equal(m.value, 59);
  });
});

describe("fiscal year comes from the org unless the calibration says otherwise", () => {
  const quarters = (m: Metric | undefined) => measured(m).segments.filter((s) => s.dimension === "closed quarter").map((s) => s.key);
  test("an April-start org gets April quarters without being told", () => {
    assert.deepEqual(quarters(metrics({ orgFiscal: 4 }).get("historic.win_rate")), ["FY2026-Q4"]);
  });
  test("the calibration wins if it's set", () => {
    assert.deepEqual(quarters(metrics({ orgFiscal: 4, calibration: calibration({ fiscalYearStartMonth: 1 }) }).get("historic.win_rate")), ["FY2026-Q1"]);
  });
  test("if neither is known, quarters aren't guessed", () => {
    const all = metrics({ orgFiscal: null });
    assert.deepEqual(quarters(all.get("historic.win_rate")), []);
    assert.equal(all.get("historic.slippage_quarter_cross_rate")?.status, "not_measurable");
    assert.equal(all.get("historic.slippage_rate")?.status, "measured");
  });
});

describe("gaps in history, month by month", () => {
  // Three deals that existed in January have no history until March.
  const migrated = ["M1", "M2", "M3"].map((id) => deal(id, "Lost", "2026-01-02", "2026-04-20", 100));
  const late = ["M1", "M2", "M3"].flatMap((id) => [row(id, "A", "2026-03-15", "2026-04-20"), row(id, "Lost", "2026-04-10", "2026-04-20")]);
  const one = metrics({ deals: [DEALS[1]!, ...migrated], history: [...HISTORY.filter((h) => h.OpportunityId === "D2"), ...late], calibration: calibration({ minSample: 1 }) });
  const series = measured(one.get("historic.pipeline_point_in_time_count")).series;

  test("a month where most deals have no history yet is not measurable", () => {
    const feb = series.find((p) => p.date === "2026-02-28")!;
    assert.equal(feb.value, null);
    assert.match(feb.notMeasurableReason!, /3 of 4 deals that existed then have no stage history yet/);
  });
  test("once history exists, the month is measured", () => assert.equal(series.find((p) => p.date === "2026-03-31")!.value, 3));
});

describe("blank amounts at a month end", () => {
  const deals = ["P1", "P2", "P3"].map((id) => deal(id, "A", "2026-06-01", "2026-09-01", null));
  const history = ["P1", "P2", "P3"].map((id) => row(id, "A", "2026-06-01", "2026-09-01", id === "P1" ? 100 : null));
  const all = metrics({ deals, history, calibration: calibration({ minSample: 1 }) });

  test("the value isn't reported when too many are blank", () => {
    const metric = all.get("historic.pipeline_point_in_time_value");
    assert.equal(metric?.status, "not_measurable");
    assert.match(metric?.status === "not_measurable" ? metric.reason : "", /2 of 3 open deals had no amount/);
  });
  test("the count is still measured", () => assert.equal(value(all, "historic.pipeline_point_in_time_count"), 3));
});

describe("thin segments", () => {
  test("a win rate by value segment with mostly blank amounts has no value", () => {
    // One blank overall (20%, allowed), but half of this segment.
    const deals = DEALS.map((d) => (d.Id === "D3" ? { ...d, Type: "Blank", Amount: null } : d.Id === "D4" ? { ...d, Type: "Blank" } : d));
    const m = measured(metrics({ deals, calibration: calibration({ minSample: 2 }) }).get("historic.win_rate_by_value"));
    assert.equal(m.segments.find((s) => s.dimension === "type" && s.key === "Blank")?.value, null);
  });
  test("skipped-stage figures respect the minimum sample", () => {
    const m = measured(metrics({ calibration: calibration({ minSample: 5 }) }).get("historic.stage_conversion"));
    const c = m.segments.find((s) => s.dimension === "skipped stage" && s.key === "C")!;
    assert.equal(c.value, null);
    assert.match(c.notMeasurableReason!, /Sample below the calibrated minimum/);
  });
});

describe("rows written in the same second", () => {
  test("stay in id order, so a push isn't flipped", () => {
    const at = "2026-02-02T09:00:00.000+0000";
    const history: HistoryRow[] = [
      { Id: "h000Z", OpportunityId: "D", StageName: "A", Amount: 1, CloseDate: "2026-03-01", CreatedDate: at },
      { Id: "h000a", OpportunityId: "D", StageName: "A", Amount: 1, CloseDate: "2026-04-01", CreatedDate: at },
      { Id: "h000b", OpportunityId: "D", StageName: "Lost", Amount: 1, CloseDate: "2026-04-01", CreatedDate: "2026-03-20T09:00:00.000+0000" },
    ];
    const all = metrics({ deals: [deal("D", "Lost", "2026-02-01", "2026-04-01", 1)], history, calibration: calibration({ minSample: 1 }) });
    assert.equal(value(all, "historic.slippage_rate"), 100);
  });
});

describe("helpers", () => {
  test("median and quantile", () => {
    assert.equal(median([3, 1, 2]), 2);
    assert.equal(median([1, 2, 3, 4]), 2.5);
    assert.equal(quantile([10, 20, 30, 40], 0.25), 17.5);
  });
  test("fiscal quarters with a January year", () => {
    assert.equal(fiscalQuarter(new Date("2026-03-31T00:00:00Z"), 1), "FY2026-Q1");
    assert.equal(fiscalQuarter(new Date("2026-04-01T00:00:00Z"), 1), "FY2026-Q2");
  });
  test("fiscal quarters with an April year", () => {
    assert.equal(fiscalQuarter(new Date("2026-04-01T00:00:00Z"), 4), "FY2027-Q1");
    assert.equal(fiscalQuarter(new Date("2026-03-31T00:00:00Z"), 4), "FY2026-Q4");
  });
  test("month ends stay inside the window", () => {
    assert.deepEqual(monthEnds({ from: "2026-01-15", to: "2026-03-30" }), ["2026-01-31", "2026-02-28"]);
    assert.deepEqual(monthEnds({ from: "2026-01-31", to: "2026-03-31" }), ["2026-01-31", "2026-02-28", "2026-03-31"]);
  });
});
