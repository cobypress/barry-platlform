import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { revenueOpportunitiesModule } from "../revenue-opportunities/index.ts";
import { atOwnWinRate } from "../value.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;

before(async () => {
  extracts = loadExtracts((await runFromRecording()).dir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const run = (changed: Map<string, unknown>, ruleId: string): Finding => one(analyseExtracts(changed).findings, ruleId);
const opportunities = (edit: (e: { multiCurrency: boolean; opportunities: { data: any[] }; history: { data: any[] } }) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get("opportunities") as never);
  return changed;
};
const ids = revenueOpportunitiesModule.rules.map((r) => r.id);

describe("every revenue figure carries its assumptions", () => {
  test("all three rules fire, with a sized figure, and each assumption is stated", () => {
    for (const id of ids) {
      const f = run(extracts, id);
      assert.equal(f.status, "issue", id);
      assert.ok(f.value.kind === "revenue", `${id} is sized`);
      assert.ok(f.value.assumptions.length >= 3, id);
      assert.ok(f.value.assumptions.some((a) => /ceiling/.test(a)), `${id} says it's a ceiling`);
      assert.ok(f.value.assumptions.some((a) => /win rate of [\d.]+%, measured on \d+ closed deals/.test(a)), `${id} names the rate and its sample`);
      assert.equal(f.value.amount, f.facts.sized, id);
      assert.equal(f.confidence, "medium", id);
    }
  });

  test("no win rate means not sized, never a typical rate", () => {
    const calibration = loadCalibration();
    calibration.minSample = 10_000;
    for (const id of ids) {
      const f = analyseExtracts(extracts, { calibration }).findings.find((x) => x.ruleId === id)!;
      if (f.status !== "issue") continue;
      assert.equal(f.value.kind, "not_sized", id);
      assert.equal(f.facts.sized, undefined, id);
    }
  });

  test("multi-currency is never added up, and the count still stands", () => {
    const changed = opportunities((e) => (e.multiCurrency = true));
    for (const id of ids) {
      const f = run(changed, id);
      assert.equal(f.status, "issue", id);
      assert.equal(f.value.kind, "not_sized", id);
    }
  });

  test("the sum is the amount times the org's own rate, rounded", () => {
    const metrics = [{ id: "historic.win_rate", status: "measured", value: 25, n: 40 }] as never;
    const value = atOwnWinRate({ metrics, calibration: loadCalibration() }, 1_000, { multiCurrency: false, period: "once", basis: "b", caveat: "c" });
    assert.ok(value.kind === "revenue" && value.amount === 250 && value.unit === "GBP");
  });

  test("nothing to size gives a reason, not a zero", () => {
    const metrics = [{ id: "historic.win_rate", status: "measured", value: 25, n: 40 }] as never;
    assert.equal(atOwnWinRate({ metrics, calibration: loadCalibration() }, 0, { multiCurrency: false, period: "once", basis: "b", caveat: "c" }).kind, "not_sized");
  });
});

describe("stalled deals", () => {
  test("recent stage changes keep a deal off the list", () => {
    const changed = opportunities((e) => {
      for (const h of e.history.data) h.CreatedDate = "2026-10-30T09:00:00.000+0000";
    });
    assert.equal(run(changed, "rev.stalled-deals").status, "passed");
  });

  test("recent activity keeps a deal off the list", () => {
    const changed = opportunities((e) => {
      for (const o of e.opportunities.data) o.LastActivityDate = "2026-10-20";
    });
    assert.equal(run(changed, "rev.stalled-deals").status, "passed");
  });
});

describe("win-backs", () => {
  test("an account with an open deal isn't a win-back", () => {
    const f = run(extracts, "rev.win-backs");
    const changed = opportunities((e) => {
      for (const o of e.opportunities.data) if (!o.IsClosed) o.AccountId = "001000000000500AAA";
      for (const o of e.opportunities.data) if (o.IsClosed) o.AccountId = "001000000000500AAA";
    });
    assert.ok(f.facts.candidates! >= 5);
    assert.equal(run(changed, "rev.win-backs").status, "passed");
  });

  test("a deal lost last month isn't a win-back yet, and one from years ago isn't either", () => {
    const changed = opportunities((e) => {
      for (const h of e.history.data) h.CreatedDate = "2026-09-30T09:00:00.000+0000";
    });
    assert.equal(run(changed, "rev.win-backs").status, "passed");
    const old = opportunities((e) => {
      for (const h of e.history.data) h.CreatedDate = "2023-09-30T09:00:00.000+0000";
    });
    assert.equal(run(old, "rev.win-backs").status, "passed");
  });

  test("no decision deals are called out", () => assert.ok(run(extracts, "rev.win-backs").facts.noDecision! > 0));
});

describe("dormant accounts", () => {
  test("found, with the ones nobody owns counted", () => {
    const f = run(extracts, "rev.dormant-accounts");
    assert.ok(f.facts.dormant! >= 5);
    assert.ok(f.facts.stranded! > 0);
    assert.match(f.summary, /inactive users/);
  });

  test("activity keeps an account off the list", () => {
    const changed = clone(extracts);
    for (const a of (changed.get("won-accounts") as { data: any[] }).data) a.LastActivityDate = "2026-10-20";
    assert.equal(run(changed, "rev.dormant-accounts").status, "passed");
  });

  test("an open deal keeps an account off the list", () => {
    const changed = opportunities((e) => {
      const accounts = (extracts.get("won-accounts") as { data: Array<{ Id: string }> }).data;
      e.opportunities.data.filter((o) => !o.IsClosed).forEach((o, i) => (o.AccountId = accounts[i % accounts.length]!.Id));
    });
    assert.ok(run(changed, "rev.dormant-accounts").facts.dormant! < run(extracts, "rev.dormant-accounts").facts.dormant!);
  });

  test("the calibrated dormant period decides what's dormant", () => {
    const calibration = loadCalibration();
    calibration.dormantDays = 10;
    const wide = analyseExtracts(extracts, { calibration }).findings.find((f) => f.ruleId === "rev.dormant-accounts")!;
    assert.ok(wide.facts.dormant! > run(extracts, "rev.dormant-accounts").facts.dormant!);
  });

  test("unreadable accounts are not measurable", () => {
    const changed = clone(extracts);
    changed.set("won-accounts", { status: "unavailable", reason: "no", source: { kind: "soql", api: "rest", query: "x" } });
    assert.equal(run(changed, "rev.dormant-accounts").status, "not_measurable");
  });

  test("the account's name isn't read", () => {
    const { source } = extracts.get("won-accounts") as { source: { query: string } };
    assert.doesNotMatch(source.query, /\bName\b/);
  });
});
