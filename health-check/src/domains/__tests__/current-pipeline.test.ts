import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { enteredCurrentStage, quarterBounds, stageRuns } from "../current-pipeline/stages.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import type { Deal } from "../historic-pipeline/cohort.ts";
import type { HistoryRow, OpportunityExtract } from "../../extract/opportunities.ts";
import type { Finding } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;
let runDir: string;
const tokenOf = (n: number): string => `[[user:U${String(n).padStart(4, "0")}]]`;

before(async () => {
  runDir = (await runFromRecording()).dir;
  extracts = loadExtracts(runDir);
});

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.find((f) => f.ruleId === ruleId);
  assert.ok(found, ruleId);
  return found;
};
const run = (changed: Map<string, unknown>, ruleId: string): Finding => one(analyseExtracts(changed).findings, ruleId);
const opportunities = (edit: (e: OpportunityExtract & { opportunities: { data: any[] }; history: { data: any[] } }) => void): Map<string, unknown> => {
  const changed = clone(extracts);
  edit(changed.get("opportunities") as never);
  return changed;
};
const gone = { status: "unavailable", reason: "INVALID_TYPE: no", source: { kind: "soql", api: "rest", query: "x" } };

describe("stage timing helpers", () => {
  const rows = (...stages: Array<[string, string]>): HistoryRow[] => stages.map(([StageName, at], i) => ({ Id: `h${i}`, OpportunityId: "o", StageName, Amount: 1, CloseDate: "2026-01-01", CreatedDate: `${at}T00:00:00.000+0000` }));
  const deal = (StageName: string, history: HistoryRow[]): Deal => ({ StageName, history }) as unknown as Deal;

  test("a close-date edit in the same stage doesn't restart the clock", () => {
    assert.equal(enteredCurrentStage(deal("B", rows(["A", "2026-01-01"], ["B", "2026-02-01"], ["B", "2026-03-01"])))?.toISOString().slice(0, 10), "2026-02-01");
  });
  test("going back to a stage restarts the clock", () => {
    assert.equal(enteredCurrentStage(deal("A", rows(["A", "2026-01-01"], ["B", "2026-02-01"], ["A", "2026-03-01"])))?.toISOString().slice(0, 10), "2026-03-01");
  });
  test("history that doesn't end in the current stage gives no date", () => assert.equal(enteredCurrentStage(deal("C", rows(["A", "2026-01-01"]))), null));
  test("no history gives no date", () => assert.equal(enteredCurrentStage(deal("C", [])), null));
  test("runs measure each completed stage, and leave out the last", () => {
    assert.deepEqual(stageRuns(deal("C", rows(["A", "2026-01-01"], ["A", "2026-01-05"], ["B", "2026-01-11"], ["C", "2026-01-21"]))), [{ stage: "A", days: 10 }, { stage: "B", days: 10 }]);
  });
  test("quarters follow the fiscal year", () => {
    assert.deepEqual(quarterBounds(new Date("2026-10-31T12:00:00Z"), 1), { from: "2026-10-01", to: "2026-12-31" });
    assert.deepEqual(quarterBounds(new Date("2026-10-31T12:00:00Z"), 4), { from: "2026-10-01", to: "2026-12-31" });
    assert.deepEqual(quarterBounds(new Date("2026-10-31T12:00:00Z"), 2), { from: "2026-08-01", to: "2026-10-31" });
    assert.deepEqual(quarterBounds(new Date("2026-01-15T12:00:00Z"), 2), { from: "2025-11-01", to: "2026-01-31" });
  });
});

describe("share rules", () => {
  test("stuck deals are measured against the org's own won deals", () => {
    const f = run(extracts, "cur.stuck-in-stage");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.rate! > 40);
    assert.equal(f.value.kind, "revenue");
  });

  test("open deals that couldn't be compared are counted in the summary, so its figures add up to the open pipeline (night 3 reading pass, item 21)", () => {
    // Pinned from the reading pass: "58 of 70" beside 76 open deals elsewhere, with nothing to say where 6 went.
    const f = run(extracts, "cur.stuck-in-stage");
    const open = (extracts.get("opportunities") as { opportunities: { data: Array<{ IsClosed: boolean }> } }).opportunities.data.filter((o) => !o.IsClosed).length;
    const left = open - (f.facts.n as number);
    assert.ok(left > 0, `${open} open, ${f.facts.n} compared`);
    assert.match(f.summary, new RegExp(` ${left} more open deals couldn't be compared, as they're in stages with too few won deals to set a line\\. `));
  });

  test("with too few won deals there's no line, so it's not measurable", () => {
    const calibration = loadCalibration();
    calibration.minSample = 10_000;
    assert.equal(analyseExtracts(extracts, { calibration }).findings.find((f) => f.ruleId === "cur.stuck-in-stage")!.status, "not_measurable");
  });

  test("deals that moved recently stay quiet", () => {
    const changed = opportunities((e) => {
      for (const h of e.history.data) h.CreatedDate = "2026-10-30T09:00:00.000+0000";
    });
    assert.notEqual(run(changed, "cur.stuck-in-stage").facts.rate, run(extracts, "cur.stuck-in-stage").facts.rate);
    assert.equal(run(changed, "cur.stale-deals").facts.hits, 0);
  });

  test("a rep gone quiet is found under an acceptable total", () => {
    const f = run(extracts, "cur.stale-deals");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.rate! <= 25, `total ${f.facts.rate}`);
    assert.ok(f.subjects.some((s) => s.label === tokenOf(3)));
    assert.match(f.summary, /The total looks acceptable/);
  });

  test("recent activity keeps a deal off the stale list", () => {
    const changed = opportunities((e) => {
      for (const o of e.opportunities.data) o.LastActivityDate = "2026-10-20";
    });
    assert.equal(run(changed, "cur.stale-deals").status, "passed");
  });

  test("repeated pushes count later moves, and a single push doesn't", () => {
    const f = run(extracts, "cur.close-date-pushes");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.hits, 14);
    const changed = opportunities((e) => {
      for (const h of e.history.data) h.CloseDate = "2026-12-01";
    });
    assert.equal(run(changed, "cur.close-date-pushes").status, "passed");
  });

  test("a rep leaving next steps empty is found under an acceptable total", () => {
    const f = run(extracts, "cur.missing-next-steps");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.rate! <= 30);
    assert.deepEqual(f.subjects.map((s) => s.label), [tokenOf(14)]);
  });

  test("all next steps filled stays quiet", () => {
    const changed = clone(extracts);
    const open = (changed.get("opportunities") as { opportunities: { data: Array<{ Id: string }> } }).opportunities.data.map((o) => o.Id);
    (changed.get("next-steps") as { data: string[] }).data = open;
    assert.equal(run(changed, "cur.missing-next-steps").status, "passed");
  });

  describe("deals open at the run's date and closed since, whose next step then can't be read (DECISIONS.md 465)", () => {
    const rowsOf = (changed: Map<string, unknown>) => analyseExtracts(changed).records.filter((r) => r.ruleId === "cur.missing-next-steps").flatMap((r) => r.rows.map((row) => row.id));
    const withClosed = (closedSince: string[] | undefined) => opportunities((e) => (closedSince === undefined ? delete e.closedSince : (e.closedSince = closedSince)));

    test("none, in a checkpoint from before they were kept or an empty list, leaves the finding as it was", () => {
      assert.deepEqual((extracts.get("opportunities") as OpportunityExtract).closedSince, []);
      const today = run(extracts, "cur.missing-next-steps");
      assert.deepEqual(run(withClosed(undefined), "cur.missing-next-steps"), today);
      assert.doesNotMatch(today.summary, /closed since/);
    });

    test("are left out of the deals tested and the action list, and said, with the count worked out", () => {
      const today = run(extracts, "cur.missing-next-steps");
      const hit = rowsOf(extracts)[0]!;
      const filled = (extracts.get("next-steps") as { data: string[] }).data[0]!;
      const one = withClosed([hit]);
      const f = run(one, "cur.missing-next-steps");
      assert.deepEqual({ n: f.facts.n, hits: f.facts.hits }, { n: today.facts.n! - 1, hits: today.facts.hits! - 1 });
      assert.ok(rowsOf(extracts).includes(hit) && !rowsOf(one).includes(hit));
      assert.equal(f.evidence[0]!.count, today.evidence[0]!.count! - 1);
      assert.deepEqual(f.evidence[0]!.exclusions, [...today.evidence[0]!.exclusions, "1 deal closed since this run's date, whose next step then can't be read"]);
      assert.match(f.summary, /\. One deal open at this run's date and closed since is left out, as we can't read the next step it had then\./);

      const two = run(withClosed([hit, filled]), "cur.missing-next-steps");
      assert.deepEqual({ n: two.facts.n, hits: two.facts.hits }, { n: today.facts.n! - 2, hits: today.facts.hits! - 1 });
      assert.match(two.summary, /\. 2 deals open at this run's date and closed since are left out, as we can't read the next step they had then\./);
    });

    test("count only those among the open deals", () => {
      const closed = (extracts.get("opportunities") as OpportunityExtract & { opportunities: { data: Array<{ Id: string; IsClosed: boolean }> } }).opportunities.data.find((o) => o.IsClosed)!.Id;
      assert.deepEqual(run(withClosed([closed]), "cur.missing-next-steps"), run(extracts, "cur.missing-next-steps"));
    });
  });

  test("unreadable next steps are not measurable, not all empty", () => {
    const changed = clone(extracts);
    changed.set("next-steps", gone);
    assert.equal(run(changed, "cur.missing-next-steps").status, "not_measurable");
  });

  test("next step text never reaches a checkpoint or the output", () => {
    const folder = join(runDir, "checkpoints");
    for (const file of readdirSync(folder)) assert.doesNotMatch(readFileSync(join(folder, file), "utf8"), /Call the buyer/, file);
    for (const file of ["findings.json", "metrics.json", "raw-notes.md"]) assert.doesNotMatch(readFileSync(join(runDir, file), "utf8"), /Call the buyer/, file);
  });

  test("an unmapped stage stops them, because we can't say what's open", () => {
    const calibration = loadCalibration();
    calibration.stages = calibration.stages.filter((s) => s.name !== "Proposal");
    assert.equal(analyseExtracts(extracts, { calibration }).findings.find((f) => f.ruleId === "cur.stale-deals")!.status, "not_measurable");
  });
});

describe("concentration", () => {
  test("one account is found, by token, with the exposure and its assumptions", () => {
    const f = run(extracts, "cur.account-concentration");
    assert.equal(f.status, "issue");
    assert.ok(f.facts.share! > 25);
    assert.match(f.subjects[0]!.label, /^\[\[account:A\d{4}\]\]$/);
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.length > 0);
  });

  test("an even spread stays quiet", () => {
    const changed = opportunities((e) => e.opportunities.data.forEach((o, i) => (o.AccountId = `001${String(i).padStart(12, "0")}AAA`)));
    assert.equal(run(changed, "cur.account-concentration").status, "passed");
  });

  test("the top rep is checked too", () => assert.equal(run(extracts, "cur.rep-concentration").status, "passed"));

  test("multi-currency is not measurable", () => {
    const changed = opportunities((e) => (e.multiCurrency = true));
    assert.equal(run(changed, "cur.account-concentration").status, "not_measurable");
    assert.equal(run(changed, "cur.coverage").status, "not_measurable");
  });

  test("mostly blank amounts are not measurable", () => {
    const changed = opportunities((e) => {
      for (const o of e.opportunities.data) o.Amount = null;
    });
    assert.equal(run(changed, "cur.account-concentration").status, "not_measurable");
  });
});

describe("coverage", () => {
  test("uses rep quotas only, so a manager's quota doesn't double the target", () => {
    const f = run(extracts, "cur.coverage");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.quota, 1_200_000);
    assert.equal(f.confidence, "medium");
    assert.ok(f.value.kind === "revenue" && f.value.assumptions.some((a) => a.includes("working figure")));
  });

  test("a quantity quota isn't counted as money", () => assert.equal(run(extracts, "cur.coverage").facts.quota! % 400_000, 0));

  test("healthy coverage stays quiet", () => {
    const changed = clone(extracts);
    const { quotas } = changed.get("forecast-quotas") as { quotas: { data: Array<{ QuotaAmount: number }> } };
    for (const q of quotas.data) q.QuotaAmount = 10_000;
    const f = run(changed, "cur.coverage");
    assert.equal(f.status, "passed");
    assert.ok(f.facts.coverage! >= 3);
  });

  for (const [source, match] of [["none", /no targets/], ["spreadsheet", /spreadsheet/], ["custom_object", /custom object/]] as const) {
    test(`targets in ${source} are not measurable, never estimated`, () => {
      const calibration = loadCalibration();
      calibration.targets = { source };
      const f = analyseExtracts(extracts, { calibration }).findings.find((x) => x.ruleId === "cur.coverage")!;
      assert.equal(f.status, "not_measurable");
      assert.match(f.notMeasurableReason!, match);
    });
  }

  test("Forecasting being off is not measurable", () => {
    const changed = clone(extracts);
    changed.set("forecast-quotas", { types: gone, quotas: gone });
    assert.equal(run(changed, "cur.coverage").status, "not_measurable");
  });

  test("two unnamed revenue types aren't added together", () => {
    const changed = clone(extracts);
    const { types } = changed.get("forecast-quotas") as { types: { data: Array<{ DeveloperName: string; IsAmount: boolean }> } };
    for (const t of types.data) {
      t.DeveloperName = "Custom";
      t.IsAmount = true;
    }
    assert.equal(run(changed, "cur.coverage").status, "not_measurable");
  });

  test("no fiscal year means no quarter, rather than a January guess", () => {
    const changed = clone(extracts);
    changed.set("org", gone);
    const calibration = loadCalibration();
    delete calibration.fiscalYearStartMonth;
    const f = analyseExtracts(changed, { calibration }).findings.find((x) => x.ruleId === "cur.coverage")!;
    assert.equal(f.status, "not_measurable");
  });
});

describe("metrics", () => {
  test("open deals and pipeline are measured and segmented", () => {
    const metrics = analyseExtracts(extracts).metrics;
    const count = metrics.find((m) => m.id === "current.open_deals")!;
    assert.ok(count.status === "measured" && count.value === 76 && count.segments.length > 0);
    const age = metrics.find((m) => m.id === "current.median_days_in_stage")!;
    assert.ok(age.status === "measured" && age.value > 0);
  });

  test("multi-currency pipeline has no value", () => {
    const changed = opportunities((e) => (e.multiCurrency = true));
    assert.equal(analyseExtracts(changed).metrics.find((m) => m.id === "current.open_pipeline")!.status, "not_measurable");
  });
});
