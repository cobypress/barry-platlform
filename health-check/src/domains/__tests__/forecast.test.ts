import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { forecastModule } from "../forecast/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { sfId, userId } from "../../testing/synthetic-org.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;
const tokenOf = (n: number): string => `[[user:U${String(n).padStart(4, "0")}]]`;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const run = (changed: Map<string, unknown>, ruleId: string): Finding => one(analyseExtracts(changed).findings, ruleId);
const gone = (code = "INVALID_TYPE") => ({ status: "unavailable", reason: `${code}: sObject type 'ForecastingItem' is not supported.`, source: { kind: "soql", api: "rest", query: "x" } });
type Forecast = { items: any; facts: any; periods: any };
const forecast = (edit: (f: Forecast) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get("forecast") as Forecast);
  return changed;
};

describe("where Forecasting isn't in use", () => {
  const offStates: Array<[string, () => Map<string, unknown>]> = [
    ["the objects aren't available", () => forecast((f) => {
      f.items = gone();
      f.facts = gone();
    })],
    ["there are no forecast rows", () => forecast((f) => (f.items.data = []))],
    ["no revenue type is active", () => {
      const changed = clone(extracts);
      for (const t of (changed.get("forecast-quotas") as { types: { data: Array<{ IsActive: boolean }> } }).types.data) t.IsActive = false;
      return changed;
    }],
    ["forecast types can't be read", () => {
      const changed = clone(extracts);
      (changed.get("forecast-quotas") as { types: unknown }).types = gone();
      return changed;
    }],
  ];

  for (const [name, build] of offStates) {
    test(`${name}: every rule and metric is not measurable, and nothing is scored`, () => {
      const { findings, metrics } = analyseExtracts(build());
      for (const rule of forecastModule.rules) {
        for (const f of findings.filter((x) => x.ruleId === rule.id)) {
          assert.equal(f.status, "not_measurable", rule.id);
          assert.match(f.notMeasurableReason!, /Forecasting isn't in use/, rule.id);
          assert.deepEqual(f.facts, {}, rule.id);
        }
      }
      const metric = metrics.find((m) => m.id === "forecast.commit_against_quota")!;
      assert.equal(metric.status, "not_measurable");
      assert.ok(!("value" in metric));
    });
  }
});

describe("forecast rules", () => {
  test("in use, with the rows that show it", () => {
    const f = run(extracts, "fct.forecasting-in-use");
    assert.equal(f.status, "passed");
    assert.equal(f.facts.rows, 5);
  });

  test("a quantity forecast isn't read as revenue", () => {
    const f = run(extracts, "fct.forecasting-in-use");
    assert.ok(f.segmentsChecked.some((s) => s.dimension === "forecast category" && s.segments === 2));
  });

  test("accuracy is never worked out from today's rows", () => {
    const f = run(extracts, "fct.forecast-accuracy");
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /snapshot/);
  });

  test("a hand adjustment with no deal behind it is found", () => {
    const f = run(extracts, "fct.manager-adjustments");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.share, 50);
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(3)]);
  });

  test("unadjusted forecasts stay quiet", () => {
    const changed = forecast((f) => {
      for (const row of f.items.data) {
        row.ForecastAmount = row.AmountWithoutAdjustments;
        row.HasAdjustment = false;
      }
    });
    assert.equal(run(changed, "fct.manager-adjustments").status, "passed");
  });

  test("a downward adjustment counts too", () => {
    const changed = forecast((f) => {
      const row = f.items.data.find((r: { OwnerId: string; ForecastCategoryName: string }) => r.OwnerId === userId(3) && r.ForecastCategoryName === "Commit");
      row.AmountWithoutAdjustments = 400_000;
      row.ForecastAmount = 100_000;
    });
    assert.equal(run(changed, "fct.manager-adjustments").status, "issue");
  });

  test("a deal owner with no quota is found, and a manager's quota doesn't hide it", () => {
    const f = run(extracts, "fct.quota-gaps");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(14)]);
  });

  test("a quota for another forecast type doesn't count", () => {
    const changed = clone(extracts);
    (changed.get("forecast-quotas") as { quotas: { data: unknown[] } }).quotas.data.push({ QuotaOwnerId: userId(14), QuotaAmount: 50, StartDate: "2026-10-01", ForecastingTypeId: sfId("0Db", 2) });
    assert.equal(run(changed, "fct.quota-gaps").status, "issue");
  });

  test("everyone with a quota stays quiet", () => {
    const changed = clone(extracts);
    (changed.get("forecast-quotas") as { quotas: { data: unknown[] } }).quotas.data.push({ QuotaOwnerId: userId(14), QuotaAmount: 400_000, StartDate: "2026-10-01", ForecastingTypeId: sfId("0Db", 1) });
    assert.equal(run(changed, "fct.quota-gaps").status, "passed");
  });

  test("committed deals with warning signs are sized as exposure", () => {
    const f = run(extracts, "fct.commit-at-risk");
    assert.equal(f.status, "issue");
    assert.equal(f.value.kind, "revenue");
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.some((a) => a.includes("exposure")));
  });

  test("committed deals in early stages are found", () => assert.equal(run(extracts, "fct.early-stage-commits").status, "issue"));

  test("clean Commit deals stay quiet", () => {
    const changed = clone(extracts);
    const committed = new Set((changed.get("forecast") as Forecast).facts.data.filter((f: { ForecastCategoryName: string }) => f.ForecastCategoryName === "Commit").map((f: { OpportunityId: string }) => f.OpportunityId));
    const { opportunities, history } = changed.get("opportunities") as { opportunities: { data: any[] }; history: { data: any[] } };
    for (const o of opportunities.data) {
      if (!committed.has(o.Id)) continue;
      o.LastActivityDate = "2026-10-25";
      o.CloseDate = "2026-12-15";
      o.StageName = "Negotiation";
    }
    history.data = history.data.filter((h) => !committed.has(h.OpportunityId));
    assert.equal(run(changed, "fct.commit-at-risk").status, "passed");
    assert.equal(run(changed, "fct.early-stage-commits").status, "passed");
  });

  test("a renamed Commit category is not measurable, not a clean pass", () => {
    const changed = forecast((f) => {
      for (const row of f.facts.data) if (row.ForecastCategoryName === "Commit") row.ForecastCategoryName = "Banked";
    });
    assert.equal(run(changed, "fct.commit-at-risk").status, "not_measurable");
    assert.equal(run(changed, "fct.early-stage-commits").status, "not_measurable");
  });

  test("a forecast read cut off at its row limit gives no figure and no clean pass, though it still shows forecasting is in use", () => {
    const cleanCommit = clone(extracts);
    const committed = new Set((cleanCommit.get("forecast") as Forecast).facts.data.filter((f: { ForecastCategoryName: string }) => f.ForecastCategoryName === "Commit").map((f: { OpportunityId: string }) => f.OpportunityId));
    const { opportunities, history } = cleanCommit.get("opportunities") as { opportunities: { data: any[] }; history: { data: any[] } };
    for (const o of opportunities.data.filter((o) => committed.has(o.Id))) Object.assign(o, { LastActivityDate: "2026-10-25", CloseDate: "2026-12-15", StageName: "Negotiation" });
    history.data = history.data.filter((h) => !committed.has(h.OpportunityId));
    for (const row of (cleanCommit.get("forecast") as Forecast).items.data) Object.assign(row, { ForecastAmount: row.AmountWithoutAdjustments, HasAdjustment: false });
    const cut = clone(cleanCommit);
    Object.assign((cut.get("forecast") as Forecast).items, { truncated: true });
    Object.assign((cut.get("forecast") as Forecast).facts, { truncated: true });
    for (const id of ["fct.commit-at-risk", "fct.early-stage-commits", "fct.manager-adjustments"]) {
      assert.equal(run(cleanCommit, id).status, "passed", id);
      const f = run(cut, id);
      assert.equal(f.status, "not_measurable", id);
      assert.match(f.notMeasurableReason!, /stopped at its row limit/, id);
    }
    assert.equal(analyseExtracts(cut).metrics.find((m) => m.id === "forecast.commit_against_quota")!.status, "not_measurable");
    const inUse = run(cut, "fct.forecasting-in-use");
    assert.equal(inUse.status, "passed");
    assert.match(inUse.summary!, /in use: at least \d+ forecast rows for at least .* The read stopped at its row limit\./);
  });

  test("unreadable facts are not measurable", () => {
    const changed = forecast((f) => (f.facts = gone("INSUFFICIENT_ACCESS")));
    assert.equal(run(changed, "fct.commit-at-risk").status, "not_measurable");
  });
});

describe("commit against quota", () => {
  test("is worked out from deals, for people with a quota", () => {
    const m = analyseExtracts(extracts).metrics.find((x) => x.id === "forecast.commit_against_quota")!;
    assert.ok(m.status === "measured");
    assert.equal(m.stats.quota, 1_200_000);
    assert.equal(m.n, 3);
    assert.equal(m.value, Math.round(((m.stats.won! + m.stats.commit!) / m.stats.quota!) * 1000) / 10);
  });

  test("multi-currency is not measurable", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { multiCurrency: boolean }).multiCurrency = true;
    assert.equal(analyseExtracts(changed).metrics.find((m) => m.id === "forecast.commit_against_quota")!.status, "not_measurable");
  });
});
