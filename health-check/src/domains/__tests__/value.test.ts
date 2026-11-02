import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { atOwnLeadReturn, calibratedHours } from "../value.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId && f.status === "issue");
  assert.ok(found, ruleId);
  return found;
};

describe("every issue states its value", () => {
  test("sized with assumptions, or not sized with a reason", () => {
    for (const f of analyseExtracts(extracts, { answers: loadAnswers() }).findings.filter((x) => x.status === "issue")) {
      if (f.value.kind === "not_sized") assert.ok(f.value.reason.length > 10, f.ruleId);
      else {
        assert.ok(f.value.amount >= 0 && f.value.unit && f.value.period, f.ruleId);
        assert.ok(f.value.assumptions.length > 0, f.ruleId);
      }
    }
  });

  test("all three kinds are in use, and no finding is waiting on work that's done", () => {
    const findings = analyseExtracts(extracts, { answers: loadAnswers() }).findings;
    const kinds = new Set(findings.map((f) => f.value.kind));
    for (const kind of ["revenue", "licence_cost", "hours", "not_sized"]) assert.ok(kinds.has(kind as never), kind);
    for (const f of findings) {
      if (f.value.kind === "not_sized") assert.doesNotMatch(f.value.reason, /isn't built|is built|value model|lead funnel domain/, f.ruleId);
    }
  });

  test("a figure in a value never comes from outside the org's data or the calibration", () => {
    // Every sized revenue figure must name where its rate or amount came from.
    for (const f of analyseExtracts(extracts).findings.filter((x) => x.status === "issue" && x.value.kind === "revenue")) {
      assert.ok(f.value.kind === "revenue");
      assert.ok(f.value.assumptions.some((a) => /Amount|your own|Quota|Actual Cost|deals affected/i.test(a)), `${f.ruleId}: ${f.value.assumptions.join("; ")}`);
    }
  });
});

describe("segment gaps", () => {
  const collapsed = (changed = extracts): Finding => {
    const found = analyseExtracts(changed).findings.find((f) => f.ruleId === "pipe.segment-divergence" && f.subjects[0]?.label === "Sales EU" && f.title.startsWith("Win rate"));
    assert.ok(found);
    return found;
  };

  test("a win rate gap is sized as the amount in the segment times the gap", () => {
    const f = collapsed();
    assert.ok(f.value.kind === "revenue" && f.value.amount > 0);
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.some((a) => a.includes("ceiling")));
  });

  test("slippage isn't called lost revenue", () => {
    const slipped = analyseExtracts(extracts).findings.filter((f) => f.ruleId === "pipe.segment-divergence" && f.title.startsWith("Slippage"));
    for (const f of slipped) assert.equal(f.value.kind, "not_sized");
  });

  test("multi-currency isn't sized", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { multiCurrency: boolean }).multiCurrency = true;
    assert.equal(collapsed(changed).value.kind, "not_sized");
  });
});

describe("leads", () => {
  test("stale leads are priced at the org's own return per lead", () => {
    const f = one(analyseExtracts(extracts).findings, "dq.stale-leads");
    assert.ok(f.value.kind === "revenue");
    assert.ok(f.value.assumptions.some((a) => a.includes("your own average")));
  });

  test("unanswered leads are priced the same way", () => assert.equal(one(analyseExtracts(extracts).findings, "lead.no-first-response").value.kind, "revenue"));

  test("no return per lead means not sized, with the reason", () => {
    const value = atOwnLeadReturn({ metrics: [], calibration: loadCalibration() }, 10, { period: "once", caveat: "c" });
    assert.ok(value.kind === "not_sized" && value.reason.includes("isn't measurable"));
  });

  test("a return of zero isn't sized as zero revenue", () => {
    const metrics = [{ id: "lead.won_per_lead", status: "measured", value: 0, n: 100 }] as never;
    assert.equal(atOwnLeadReturn({ metrics, calibration: loadCalibration() }, 10, { period: "once", caveat: "c" }).kind, "not_sized");
  });

  test("multi-currency reaches the leads figure as not sized", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { multiCurrency: boolean }).multiCurrency = true;
    assert.equal(one(analyseExtracts(changed).findings, "dq.stale-leads").value.kind, "not_sized");
  });
});

describe("hours", () => {
  test("hours need a calibrated time, and the assumption says so", () => {
    const f = one(analyseExtracts(extracts).findings, "utl.unused-reports");
    assert.ok(f.value.kind === "hours");
    assert.equal(f.value.amount, Math.round((f.facts.unused! * 10) / 6) / 10);
    assert.ok(f.value.assumptions[0]!.includes("agreed in the calibration"));
  });

  test("no calibrated time means not sized, never a typical figure", () => {
    const calibration = loadCalibration();
    calibration.effortMinutes = {};
    const f = one(analyseExtracts(extracts, { calibration }).findings, "utl.unused-reports");
    assert.ok(f.value.kind === "not_sized" && f.value.reason.includes("won't guess"));
  });

  test("the arithmetic", () => {
    const calibration = { ...loadCalibration(), effortMinutes: { x: 90 } };
    const value = calibratedHours({ calibration }, "x", 3, "thing");
    assert.ok(value.kind === "hours" && value.amount === 4.5 && value.unit === "hours");
  });
});
