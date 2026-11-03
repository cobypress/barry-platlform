import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { expectedClose, monthsFrom } from "../current-pipeline/outlook.ts";
import { stageMap, type Deal } from "../historic-pipeline/cohort.ts";
import { recentQuarters, repeatFlags, stageLeftFrom } from "../historic-pipeline/deep-dive.ts";
import { cohortOf, daysLate, longestInStage, longestUnchanged, stageWinRates, wasPastClose } from "../historic-pipeline/outcomes.ts";
import { potentialRevenue, SITUATIONS } from "../historic-pipeline/potential.ts";
import { priorWinsExtractor } from "../../extract/prior-wins.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { Calibration, type Metric, type Value } from "../../schemas/index.ts";
import type { HistoryRow } from "../../extract/opportunities.ts";

// Hand-built deals for the helpers, worked out by hand, then the synthetic run
// for every metric and each trap planted for it.

const WINDOW = { from: "2025-01-01", to: "2026-06-30" };
const calibration = (minSample = 2): Calibration =>
  Calibration.parse({
    version: 1,
    confirmedBy: "Test",
    confirmedAt: "2026-06-01T00:00:00.000Z",
    stages: [
      { name: "A", category: "open", order: 1 },
      { name: "B", category: "open", order: 2 },
      { name: "Won", category: "won" },
      { name: "Lost", category: "lost" },
      { name: "NoDec", category: "no_decision" },
    ],
    scope: { recordTypes: "all" },
    targets: { source: "none" },
    minSample,
  });

let rowId = 0;
const row = (StageName: string, at: string, CloseDate: string): HistoryRow => ({ Id: `h${String(++rowId).padStart(5, "0")}`, OpportunityId: "x", StageName, Amount: 100, CloseDate, CreatedDate: `${at}T09:00:00.000+0000` });

function deal(Id: string, StageName: string, Amount: number | null, history: HistoryRow[], extra: Partial<Deal> = {}): Deal {
  const categories: Record<string, Deal["category"]> = { A: "open", B: "open", Won: "won", Lost: "lost", NoDec: "no_decision" };
  const category = categories[StageName] ?? null;
  const last = history.at(-1)!;
  return {
    Id, OwnerId: "005000000000001AAA", StageName, Amount, CloseDate: last.CloseDate, CreatedDate: history[0]!.CreatedDate, IsClosed: category !== "open", IsWon: category === "won",
    LastActivityDate: null, Type: null, LeadSource: null, RecordType: null, CurrencyIsoCode: null, LossReason: null, AccountId: null,
    category, history: history.map((h) => ({ ...h, OpportunityId: Id })), team: "UK", closedAt: new Date(last.CreatedDate), closedDay: last.CreatedDate.slice(0, 10), closedAtSource: "history", ownerKind: null,
    ...extra,
  };
}

// C1 won: open past its close date (due 1 Feb, open until 10 Feb), 40 days in A.
// C2 lost: closed before its date, 10 days in A then 5 in B.
// C3 won: 60 days in A with no change at all.
// C4 no decision: 5 days in A, closed on its date.
const CLOSED = [
  deal("C1", "Won", 1000, [row("A", "2025-01-01", "2025-02-01"), row("Won", "2025-02-10", "2025-02-10")]),
  deal("C2", "Lost", 1000, [row("A", "2025-03-01", "2025-04-01"), row("B", "2025-03-11", "2025-04-01"), row("Lost", "2025-03-16", "2025-04-01")]),
  deal("C3", "Won", 1000, [row("A", "2025-05-01", "2025-09-01"), row("Won", "2025-06-30", "2025-09-01")]),
  deal("C4", "NoDec", 1000, [row("A", "2025-07-01", "2025-07-06"), row("NoDec", "2025-07-06", "2025-07-06")]),
];

type Ready = Parameters<typeof potentialRevenue>[1];
const ready = (deals: Deal[], options: { multiCurrency?: boolean } = {}, c = calibration()): Ready => ({ status: "ok", deals, stages: stageMap(c), evidence: [], multiCurrency: options.multiCurrency ?? false, exclusions: [], historyStart: null, unmapped: { finalStages: [], historyStages: [] } });
const winRate = (value: number, n: number): Metric[] => [{ id: "historic.win_rate", status: "measured", value, n } as Metric];
const ctx = (metrics: Metric[] = [], c = calibration()) => ({ calibration: c, window: WINDOW, metrics, data: (() => ({ status: "unavailable", reason: "no", source: { kind: "rest", path: "x" } })) as never });
const open = (Id: string, Amount: number | null, stage = "A"): Deal => deal(Id, stage, Amount, [row(stage, "2026-01-01", "2026-02-01")]);
const revenue = (v: Value): Exclude<Value, { kind: "not_sized" }> => (v.kind === "revenue" ? v : assert.fail(`not sized: ${v.kind === "not_sized" ? v.reason : v.kind}`));

describe("what happened to deals while they were open", () => {
  const data = ready(CLOSED);
  test("open past the close date it had then", () => assert.deepEqual(CLOSED.map((d) => wasPastClose(d, data.stages, null)), [true, false, false, false]));
  test("longest time in one open stage", () => assert.deepEqual(CLOSED.map((d) => longestInStage(d, data.stages)), [40, 10, 60, 5]));
  test("longest open stretch with no change of any kind", () => assert.deepEqual(CLOSED.map((d) => longestUnchanged(d, data.stages)), [40, 10, 60, 5]));
  test("no decision counts as not won", () => assert.deepEqual(cohortOf(CLOSED, 2), { n: 4, won: 2, rate: 50 }));
  test("a cohort below the minimum has no rate", () => assert.equal(cohortOf(CLOSED, 5).rate, null));
  test("each stage counts the deals that were in it", () => {
    const rates = stageWinRates({ calibration: calibration(), window: WINDOW }, data);
    assert.deepEqual(rates.get("A"), { n: 4, won: 2, rate: 50 });
    assert.deepEqual(rates.get("B"), { n: 1, won: 0, rate: null });
  });
  test("days late runs from the first close date", () => assert.deepEqual(CLOSED.map(daysLate), [9, -16, -63, 0]));
  test("the stage a lost deal left from", () => assert.equal(stageLeftFrom(CLOSED[1]!, data), "B"));
});

describe("potential revenue", () => {
  const data = ready(CLOSED);
  test("comparable deals' own win rate, with the raw exposure and the period stated", () => {
    const v = revenue(potentialRevenue(ctx(), data, [open("O1", 4000), open("O2", 2000)], () => SITUATIONS.inStageFor(10, data)));
    // C1, C2 and C3 were in one stage for 10 days or more, and two of them won.
    assert.equal(v.amount, 4002);
    assert.equal(v.period, "potential revenue in deals open today");
    assert.ok(v.assumptions.includes("Raw exposure: £6,000, the total Amount on 2 affected deals with an amount"));
    assert.ok(v.assumptions.includes("2 deals with £6,000 weighted at 66.7%: of 3 deals closed 2025-01-01 to 2026-06-30 that were in one stage for 10 days or more at some point, 2 went on to win"), v.assumptions.join(" | "));
  });

  test("deals with no amount are counted, never imputed", () => {
    const v = revenue(potentialRevenue(ctx(), data, [open("O1", 4000), open("O2", null), open("O3", 0)], () => SITUATIONS.inStageFor(10, data)));
    assert.ok(v.assumptions.includes("2 of the 3 affected deals have no amount, so they add nothing here. We don't impute one"));
    assert.equal(v.amount, Math.round(4000 * 0.667));
  });

  test("none with an amount is not sized", () => {
    const v = potentialRevenue(ctx(), data, [open("O1", null)], () => SITUATIONS.pastClose(ctx(), data));
    assert.equal(v.kind, "not_sized");
    assert.match(v.kind === "not_sized" ? v.reason : "", /None of these 1 deal has an amount/);
  });

  test("a thin cohort falls back to the overall win rate, called a ceiling", () => {
    // Only C1 was past its close date: one deal, under a minimum of 2.
    const v = revenue(potentialRevenue(ctx(winRate(40, 50)), data, [open("O1", 1000)], () => SITUATIONS.pastClose(ctx(), data)));
    assert.equal(v.amount, 400);
    assert.ok(v.assumptions.some((a) => /overall win rate of 40% on 50 closed deals, because only 1 deals closed .* under the calibrated minimum of 2\. .*ceiling/.test(a)), v.assumptions.join(" | "));
  });

  test("a thin cohort with no overall win rate is not sized, never given a rate", () => {
    const v = potentialRevenue(ctx(), data, [open("O1", 1000)], () => SITUATIONS.pastClose(ctx(), data));
    assert.equal(v.kind, "not_sized");
    assert.match(v.kind === "not_sized" ? v.reason : "", /overall win rate isn't measurable/);
  });

  test("a thin stage is never given the overall rate, because a late stage can beat it", () => {
    const deals = [open("O1", 1000, "A"), open("O2", 3000, "B")];
    const v = revenue(potentialRevenue(ctx(winRate(40, 50)), data, deals, (d) => SITUATIONS.inStage(d.StageName)));
    assert.equal(v.amount, 500);
    assert.ok(v.assumptions.some((a) => a.startsWith("1 deal with £3,000 aren't counted, because only 1 deals closed")), v.assumptions.join(" | "));
    const none = potentialRevenue(ctx(winRate(40, 50)), data, [open("O2", 3000, "B")], (d) => SITUATIONS.inStage(d.StageName));
    assert.equal(none.kind, "not_sized");
  });

  test("multi-currency is never added up", () => assert.equal(potentialRevenue(ctx(), ready(CLOSED, { multiCurrency: true }), [open("O1", 1000)], () => SITUATIONS.inStageFor(10, data)).kind, "not_sized"));
});

describe("calendar helpers", () => {
  test("8 fiscal quarters end with the current one", () => {
    const q = recentQuarters("2026-10-31", 4, 8);
    assert.equal(q.length, 8);
    assert.deepEqual(q.at(-1), { from: "2026-10-01", to: "2026-12-31" });
    assert.deepEqual(q[0], { from: "2025-01-01", to: "2025-03-31" });
  });
  test("months run from the current one", () => assert.deepEqual(monthsFrom("2026-11-15", 3), ["2026-11-01", "2026-12-01", "2027-01-01"]));
  test("an open deal lands no earlier than its date, or its first date moved by the median lateness", () => {
    const pushed = deal("P", "A", 1, [row("A", "2026-01-01", "2026-03-01"), row("A", "2026-02-01", "2026-05-01")]);
    assert.equal(expectedClose(pushed, 30), "2026-05-01");
    assert.equal(expectedClose(pushed, 90), "2026-05-30");
  });
  test("a repeat is a win on an account that had won before, in the window or earlier", () => {
    const won = [deal("W1", "Won", 1, [row("Won", "2026-02-01", "2026-02-01")], { AccountId: "a1" }), deal("W2", "Won", 1, [row("Won", "2026-03-01", "2026-03-01")], { AccountId: "a1" }), deal("W3", "Won", 1, [row("Won", "2026-03-01", "2026-03-01")], { AccountId: "a2" })];
    assert.deepEqual([...repeatFlags(won, [])], [["W1", false], ["W2", true], ["W3", false]]);
    assert.deepEqual([...repeatFlags(won, [{ Id: "P1", AccountId: "a2", day: "2023-01-01" }])], [["W1", false], ["W2", true], ["W3", true]]);
  });
});

describe("on the synthetic org", () => {
  let extracts: Map<string, unknown>;
  before(async () => {
    extracts = loadExtracts((await runFromRecording()).dir);
  });
  const metric = (id: string, changed = extracts, options: Parameters<typeof analyseExtracts>[1] = {}): Metric => analyseExtracts(changed, options).metrics.find((m) => m.id === id)!;
  const measured = (m: Metric): Extract<Metric, { status: "measured" }> => (m.status === "measured" ? m : assert.fail(`${m.id}: ${m.reason}`));
  const NEW = [
    "historic.stage_win_rate", "historic.quarterly_created", "historic.quarterly_won", "historic.quarterly_won_value", "historic.quarterly_win_rate", "historic.quarterly_median_won_size", "historic.quarterly_median_cycle",
    "historic.won_value_by_customer", "historic.repeat_win_share", "historic.won_value_by_type", "historic.won_value_by_source", "historic.top_account_share", "historic.lost_by_stage", "historic.lost_value_by_stage",
    "historic.close_date_reliability", "historic.close_days_late", "historic.stage_velocity_won", "historic.stage_velocity_lost",
    "current.open_no_amount", "current.open_past_close", "current.open_median_age", "current.weighted_pipeline", "current.delivery_outlook_value", "current.delivery_outlook_deals",
  ];

  test("every new metric is measured, with evidence, a date range and segments or a reason", () => {
    const all = analyseExtracts(extracts).metrics;
    for (const id of NEW) {
      const m = measured(all.find((x) => x.id === id)!);
      assert.ok(m.evidence.length > 0 && m.dateRange, id);
      assert.ok(m.segments.length > 0 || m.segmentExemptReason, id);
    }
  });

  test("trap: the retired stage holds open deals, too few closed deals to weight, and isn't weighted", () => {
    assert.ok(measured(metric("current.open_deals")).segments.some((s) => s.dimension === "retired stage" && s.key === "Qualified" && s.value === 6));
    const weighted = measured(metric("current.weighted_pipeline"));
    assert.equal(weighted.segments.find((s) => s.key === "Qualified")!.value, null);
    assert.ok(weighted.exclusions.some((e) => e.startsWith("6 open deals in Qualified")));
    assert.equal(measured(metric("historic.stage_win_rate")).segments.find((s) => s.key === "Qualified")!.value, null);
  });

  test("trap: open deals with no amount are counted, by stage", () => {
    const m = measured(metric("current.open_no_amount"));
    assert.equal(m.value, 7);
    assert.equal(m.segments.find((s) => s.dimension === "stage" && s.key === "Qualified")!.value, 2);
  });

  test("trap: the ownerless deals in the retired stage aren't sized, and the rest are", () => {
    const f = analyseExtracts(extracts).findings.find((x) => x.ruleId === "dq.opps-inactive-owner")!;
    const v = revenue(f.value);
    assert.ok(v.assumptions.some((a) => /aren't counted, because only \d+ deals closed .* in the Qualified stage/.test(a)), v.assumptions.join(" | "));
    assert.ok(v.assumptions.some((a) => /affected deals have no amount/.test(a)));
  });

  test("all five open-pipeline findings are sized as potential revenue, under their raw exposure", () => {
    const findings = analyseExtracts(extracts).findings;
    for (const id of ["dq.open-opps-past-close", "dq.opps-inactive-owner", "dq.open-opps-no-activity", "cur.stale-deals", "rev.stalled-deals"]) {
      const v = revenue(findings.find((f) => f.ruleId === id && f.status === "issue")!.value);
      const raw = Number(/Raw exposure: £([\d,]+)/.exec(v.assumptions.join(" "))![1]!.replace(/,/g, ""));
      assert.ok(v.amount > 0 && v.amount < raw, id);
      assert.equal(v.period, "potential revenue in deals open today", id);
    }
  });

  test("quarters run back from the current one, which is never the headline", () => {
    const m = measured(metric("historic.quarterly_won"));
    assert.equal(m.series.length, 8);
    assert.equal(m.series.at(-1)!.date, "2026-10-01");
    assert.equal(m.value, m.series.at(-2)!.value);
  });

  test("a thin quarter says so rather than showing a median", () => {
    const point = measured(metric("historic.quarterly_median_won_size")).series.at(-1)!;
    assert.equal(point.value, null);
    assert.match(point.notMeasurableReason!, /below the calibrated minimum/);
  });

  test("no fiscal year means no quarters, never a January guess", () => {
    const changed = clone(extracts);
    changed.set("org", { status: "unavailable", reason: "no", source: { kind: "soql", api: "rest", query: "x" } });
    const c = loadCalibration();
    delete c.fiscalYearStartMonth;
    assert.equal(metric("historic.quarterly_won", changed, { calibration: c }).status, "not_measurable");
    assert.equal(metric("historic.close_date_reliability", changed, { calibration: c }).status, "not_measurable");
  });

  test("repeat customers count wins from before the window", () => {
    const withPrior = measured(metric("historic.won_value_by_customer"));
    const changed = clone(extracts);
    (changed.get(priorWinsExtractor.id) as { data: unknown[] }).data = [];
    const without = measured(metric("historic.won_value_by_customer", changed));
    assert.ok(withPrior.stats.repeatWins! > without.stats.repeatWins!);
  });

  test("unreadable prior wins make new and repeat not measurable, rather than calling every first win new", () => {
    const changed = clone(extracts);
    changed.set(priorWinsExtractor.id, { status: "unavailable", reason: "INVALID_FIELD: no", source: { kind: "soql", api: "rest", query: "x" } });
    assert.equal(metric("historic.repeat_win_share", changed).status, "not_measurable");
    assert.equal(metric("historic.top_account_share", changed).status, "measured");
  });

  test("top accounts are tokens, never names or ids", () => {
    for (const s of measured(metric("historic.top_account_share")).segments.filter((x) => x.dimension === "account")) assert.match(s.key, /^\[\[account:A\d{4}\]\]$/);
  });

  test("lost deals split by how they closed, where there's more than one way", () => {
    const m = measured(metric("historic.lost_by_stage"));
    assert.deepEqual(new Set(m.segments.filter((s) => s.dimension === "closed as").map((s) => s.key)), new Set(["Closed Lost", "Closed Lost - No Budget", "Closed No Decision"]));
  });

  test("multi-currency keeps counts and drops values", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { multiCurrency: boolean }).multiCurrency = true;
    const all = analyseExtracts(changed).metrics;
    for (const id of ["current.weighted_pipeline", "current.delivery_outlook_value", "historic.quarterly_won_value", "historic.lost_value_by_stage", "historic.won_value_by_customer"]) assert.equal(all.find((m) => m.id === id)!.status, "not_measurable", id);
    for (const id of ["current.delivery_outlook_deals", "historic.lost_by_stage", "historic.quarterly_won"]) assert.equal(all.find((m) => m.id === id)!.status, "measured", id);
  });

  test("too few won deals to measure lateness means no outlook", () => {
    const c = loadCalibration();
    c.minSample = 10_000;
    assert.equal(metric("current.delivery_outlook_value", extracts, { calibration: c }).status, "not_measurable");
  });

  test("an unmapped stage stops the history-based figures", () => {
    const c = loadCalibration();
    c.stages = c.stages.filter((s) => s.name !== "Closed Lost - No Budget");
    for (const id of ["historic.stage_win_rate", "historic.lost_by_stage", "historic.close_days_late", "current.delivery_outlook_value"]) assert.equal(metric(id, extracts, { calibration: c }).status, "not_measurable", id);
  });
});
