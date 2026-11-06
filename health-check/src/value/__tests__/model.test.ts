import { test, describe } from "node:test";
import assert from "node:assert/strict";
import type { Finding, Run } from "../../schemas/index.ts";
import { buildValueModel, hourlyCost, UK_HOURLY_BENCHMARK } from "../model.ts";

let counter = 0;
const issue = (ruleId: string, value: Finding["value"], title = ruleId): Finding => ({ id: `f-${++counter}`, ruleId, title, status: "issue", severity: "medium", value }) as Finding;
const gbp = (amount: number, period: string): Finding["value"] => ({ kind: "revenue", amount, unit: "GBP", period, assumptions: ["Synthetic."] });
const run = { analysisWindow: { from: "2024-10-31", to: "2026-10-31" } } as Run;

describe("the value model", () => {
  test("figures that describe the same money count once, at the largest", () => {
    const model = buildValueModel([issue("dq.open-opps-past-close", gbp(400_000, "potential revenue in deals open today")), issue("cur.stale-deals", gbp(300_000, "potential revenue in deals open today"))], run, "GBP");
    const group = model.groups.find((g) => g.id === "stalled-deals")!;
    assert.equal(group.lines.length, 2);
    assert.equal(group.counted.annual, 400_000);
    assert.deepEqual(model.total, { low: 100_000, expected: 200_000, high: 400_000 }, "a quarter, a half and all of the largest");
  });

  test("exposure is shown beside the total, never in it", () => {
    const model = buildValueModel([issue("cur.coverage", gbp(2_000_000, "more pipeline needed for the quarter starting 2026-10-01")), issue("utl.unassigned-licence-seats", { kind: "licence_cost", amount: 10_000, unit: "GBP", period: "per year", assumptions: ["Synthetic."] })], run, "GBP");
    assert.equal(model.byKind.exposure.expected, 2_000_000);
    assert.deepEqual(model.total, { low: 5_000, expected: 7_500, high: 10_000 });
  });

  test("a gap measured over the analysis window is put over one year", () => {
    const model = buildValueModel([issue("pipe.segment-divergence", gbp(730_000, "in wins the gap may have cost, 2024-10-31 to 2026-10-31"))], run, "GBP");
    const line = model.groups[0]!.counted;
    assert.ok(Math.abs(line.annual - 730_000 / (730 / 365)) < 1);
    assert.equal(line.kind, "conversion_uplift");
  });

  test("hours are costed at the customer's rate when they gave one, else the cited UK benchmark", () => {
    const hours: Finding["value"] = { kind: "hours", amount: 100, unit: "hours", period: "per year", assumptions: ["Synthetic."] };
    assert.equal(buildValueModel([issue("x.manual", hours)], run, "GBP", 40).groups[0]!.counted.annual, 4_000);
    const benchmark = hourlyCost(null);
    const b = UK_HOURLY_BENCHMARK;
    const loaded = b.salary + 0.15 * (b.salary - 5_000) + 0.03 * (b.salary - 6_240);
    assert.ok(Math.abs(benchmark.rate - loaded / 1_950) < 0.01, String(benchmark.rate));
    assert.equal(benchmark.source, "benchmark");
  });

  test("a figure it can't put over 12 months is left out, with why, rather than guessed", () => {
    const model = buildValueModel([issue("x.odd", gbp(5_000, "every leap year"))], run, "GBP");
    assert.equal(model.total.expected, 0);
    assert.match(model.left[0]!.reason, /over 12 months/);
  });

  test("an integration account's full seat and the same seat as an unused one count once (night 3 review, DECISIONS.md 303)", () => {
    // An API-only account that's never logged in is both a seat to release and a seat to move to an integration licence.
    const seat = (ruleId: string) => issue(ruleId, { kind: "licence_cost", amount: 1_200, unit: "GBP", period: "per year", assumptions: ["Synthetic."] });
    const model = buildValueModel([seat("utl.never-logged-in"), seat("int.integration-user-licences")], run, "GBP");
    assert.equal(model.byKind.licence_waste.high, 1_200, "the same seat isn't released and moved at once");
  });

  test("passes and unsized issues add nothing", () => {
    const passed = { ...issue("dq.open-opps-past-close", gbp(1, "potential revenue in deals open today")), status: "passed" } as Finding;
    const unsized = issue("sec.modify-all-data", { kind: "not_sized", reason: "A risk." });
    assert.equal(buildValueModel([passed, unsized], run, "GBP").groups.length, 0);
  });
});
