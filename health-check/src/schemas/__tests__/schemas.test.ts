import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { BenchmarkRow, Calibration, canRelease, CustomerBaseline, Evidence, Finding, Guard, ManualAnswer, ManualStep, Metric, NumberOverride, ReportSection, Run, Score, SectionVersion, WordingProposal } from "../index.ts";

const at = "2026-10-31T12:00:00.000Z";

const evidence = { kind: "soql", api: "rest", query: "SELECT Id FROM Lead", count: 1, dateRange: null, exclusions: [], capturedAt: at } as const;
const guard = (status: "passed" | "failed" | "unknown") => ({ guardId: "owns-no-records", subject: "[[user:U0001]]", status, detail: "Detail", evidence: [evidence] });

const finding = (overrides: Record<string, unknown> = {}) => ({
  id: "f-1",
  ruleId: "x.rule",
  ruleVersion: 1,
  domain: "utilisation",
  status: "issue",
  title: "Title",
  summary: "Summary",
  recommendation: "Do it",
  recommendationType: "change",
  severity: "medium",
  confidence: "high",
  evidence: [evidence],
  value: { kind: "not_sized", reason: "Because" },
  effort: { size: "S" },
  owner: "Admin",
  dependencies: [],
  guards: [],
  segmentsChecked: [{ dimension: "object", segments: 2 }],
  segmentExemptReason: null,
  subjects: [],
  facts: {},
  notMeasurableReason: null,
  manualStepIds: [],
  ...overrides,
});

describe("Finding", () => {
  test("a complete finding parses", () => assert.ok(Finding.parse(finding())));

  test("every required field of rule 2 is required", () => {
    for (const key of ["evidence", "severity", "confidence", "value", "effort", "owner", "dependencies"]) {
      const broken = finding();
      delete (broken as Record<string, unknown>)[key];
      assert.ok(!Finding.safeParse(broken).success, key);
    }
  });

  test("no evidence, no finding", () => assert.ok(!Finding.safeParse(finding({ evidence: [] })).success));
  test("value needs assumptions, or a reason it isn't sized", () => {
    assert.ok(!Finding.safeParse(finding({ value: { kind: "revenue", amount: 10, unit: "GBP", period: "per year", assumptions: [] } })).success);
    assert.ok(!Finding.safeParse(finding({ value: { kind: "not_sized", reason: "" } })).success);
  });

  for (const type of ["remove", "deactivate", "merge"]) {
    test(`${type} can't exist without guards`, () => assert.ok(!Finding.safeParse(finding({ recommendationType: type, guards: [] })).success));
    test(`${type} can't exist with a failed guard`, () => assert.ok(!Finding.safeParse(finding({ recommendationType: type, guards: [guard("passed"), guard("failed")] })).success));
    test(`${type} can't exist with an unknown guard`, () => assert.ok(!Finding.safeParse(finding({ recommendationType: type, guards: [guard("unknown")] })).success));
    test(`${type} parses when every guard passed`, () => assert.ok(Finding.parse(finding({ recommendationType: type, guards: [guard("passed")] }))));
  }

  test("only an issue can be destructive", () => assert.ok(!Finding.safeParse(finding({ status: "passed", recommendationType: "remove", guards: [guard("passed")] })).success));
  test("not measurable needs its reason", () => assert.ok(!Finding.safeParse(finding({ status: "not_measurable" })).success));
  test("a pass must show its segments or say why not", () => {
    assert.ok(!Finding.safeParse(finding({ status: "passed", segmentsChecked: [] })).success);
    assert.ok(Finding.parse(finding({ status: "passed", segmentsChecked: [], segmentExemptReason: "Nothing to split" })));
  });
});

describe("Evidence", () => {
  test("each source type parses", () => {
    const base = { count: null, dateRange: null, exclusions: [], capturedAt: at };
    for (const e of [
      evidence,
      { kind: "rest", path: "limits", ...base },
      { kind: "metadata", path: "flows/A.flow", ...base },
      { kind: "manual", manualStepId: "ms-1", status: "pending", answeredBy: null, answeredAt: null, ...base },
    ]) assert.ok(Evidence.parse(e));
  });
  test("exclusions and date range must be stated", () => {
    const { exclusions: _e, ...noExclusions } = evidence;
    const { dateRange: _d, ...noRange } = evidence;
    assert.ok(!Evidence.safeParse(noExclusions).success);
    assert.ok(!Evidence.safeParse(noRange).success);
  });
  test("a manual answer must say who and when", () => {
    const base = { kind: "manual", manualStepId: "ms-1", status: "answered", count: null, dateRange: null, exclusions: [], capturedAt: at };
    assert.ok(!Evidence.safeParse({ ...base, answeredBy: null, answeredAt: null }).success);
    assert.ok(Evidence.parse({ ...base, answeredBy: "A Person", answeredAt: at }));
  });
});

describe("Guard", () => {
  test("needs evidence", () => assert.ok(!Guard.safeParse({ ...guard("passed"), evidence: [] }).success));
});

describe("Metric", () => {
  const base = { id: "m", domain: "historic_pipeline", label: "L", definition: "D", unit: "percent", evidence: [evidence], exclusions: [], assumptions: [] };
  const measured = { ...base, status: "measured", value: 1, n: 1, dateRange: null, segments: [], segmentExemptReason: null, series: [], stats: {} };
  test("measured without segments or a reason is refused", () => assert.ok(!Metric.safeParse(measured).success));
  test("a segment exemption is accepted", () => assert.ok(Metric.parse({ ...measured, segmentExemptReason: "One group" })));
  test("not measurable needs its reason", () => {
    assert.ok(!Metric.safeParse({ ...base, status: "not_measurable" }).success);
    assert.ok(Metric.parse({ ...base, status: "not_measurable", reason: "Too few" }));
  });
  test("not measurable can't carry a value", () => {
    const parsed = Metric.parse({ ...base, status: "not_measurable", reason: "Too few", value: 5 });
    assert.equal((parsed as Record<string, unknown>).value, undefined);
  });
});

describe("Calibration", () => {
  const base = { version: 1, confirmedBy: "A Person", confirmedAt: at, stages: [{ name: "Open", category: "open", order: 1 }, { name: "Won", category: "won" }], scope: { recordTypes: "all" }, targets: { source: "none" } };
  test("parses and fills defaults", () => {
    const c = Calibration.parse(base);
    assert.equal(c.minSample, 20);
    assert.equal(c.analysisWindowMonths, 24);
  });
  test("must be confirmed by a person", () => {
    const { confirmedBy: _c, ...unconfirmed } = base;
    assert.ok(!Calibration.safeParse(unconfirmed).success);
  });
  test("every open stage needs an order, and each order is unique", () => {
    assert.ok(!Calibration.safeParse({ ...base, stages: [{ name: "Open", category: "open" }] }).success);
    assert.ok(!Calibration.safeParse({ ...base, stages: [{ name: "A", category: "open", order: 1 }, { name: "B", category: "open", order: 1 }] }).success);
  });
  test("a stage can't be listed twice", () => assert.ok(!Calibration.safeParse({ ...base, stages: [{ name: "Won", category: "won" }, { name: "Won", category: "lost" }] }).success));
});

describe("ManualStep and ManualAnswer", () => {
  const step = { id: "ms-1", key: "k", domain: "utilisation", raisedBy: "engine", ruleIds: [], title: "T", whyItMatters: "W", whereToGo: "Setup > X", whatToCapture: "C", fields: [{ key: "a", label: "A", type: "boolean", required: true }], answerMethods: ["fields"], status: "pending", ifSkipped: "S" };
  const answer = { stepKey: "k", outcome: "answered", method: "fields", values: { a: true }, answeredBy: "A Person", answeredAt: at };
  test("Claude can raise a step", () => assert.ok(ManualStep.parse({ ...step, raisedBy: "claude" })));
  test("a step needs fields to answer", () => assert.ok(!ManualStep.safeParse({ ...step, fields: [] }).success));
  test("an answer records who and when", () => {
    const { answeredBy: _a, ...anonymous } = answer;
    assert.ok(!ManualAnswer.safeParse(anonymous).success);
  });
  test("a skip needs a reason", () => assert.ok(!ManualAnswer.safeParse({ ...answer, outcome: "skipped", method: undefined }).success));
  test("Claude's reading of a screenshot isn't an answer until it's confirmed", () => {
    const prefill = { values: { a: true }, model: "claude-sonnet-5-5", screenshotRef: "s3://x", createdAt: at, confirmedBy: null, confirmedAt: null };
    assert.ok(!ManualAnswer.safeParse({ ...answer, method: "screenshot", prefill }).success);
    assert.ok(ManualAnswer.parse({ ...answer, method: "screenshot", prefill: { ...prefill, confirmedBy: "A Person", confirmedAt: at } }));
  });
});

describe("review console", () => {
  const section = { id: "s1", runId: "r1", output: "executive_summary", key: "verdict", title: "Verdict", order: 1, status: "draft", currentVersionId: null, findingIds: [], metricIds: [], manualStepIds: [], approval: null };
  const approved = { ...section, status: "approved", approval: { approvedBy: "A Person", approvedAt: at } };

  test("approved needs a name and a time", () => {
    assert.ok(!ReportSection.safeParse({ ...section, status: "approved" }).success);
    assert.ok(ReportSection.parse(approved));
  });
  test("an approval can't sit on an unapproved section", () => assert.ok(!ReportSection.safeParse({ ...section, approval: approved.approval }).success));
  test("every status from the brief exists", () => {
    for (const status of ["draft", "needs_input", "in_review"]) assert.ok(ReportSection.parse({ ...section, status }));
  });

  test("release needs every section approved", () => {
    const parsedApproved = ReportSection.parse(approved);
    const draft = ReportSection.parse({ ...section, key: "scorecard" });
    assert.deepEqual(canRelease([parsedApproved]), { ok: true });
    assert.deepEqual(canRelease([parsedApproved, draft]), { ok: false, blockedBy: ["scorecard"] });
    assert.equal(canRelease([]).ok, false);
  });

  const check = { passed: true, checkedAt: at, unmatched: [] };
  test("each version records who changed what, and its number check", () => {
    assert.ok(SectionVersion.parse({ id: "v1", sectionId: "s1", version: 1, body: "x", source: "claude_proposal", changedBy: "A Person", changedAt: at, changeSummary: "Tone", numberCheck: check }));
    assert.ok(!SectionVersion.safeParse({ id: "v1", sectionId: "s1", version: 1, body: "x", source: "claude_proposal", changedAt: at, changeSummary: "Tone", numberCheck: check }).success);
  });

  const proposal = { id: "p1", sectionId: "s1", baseVersionId: "v1", before: "a", after: "b", rationale: "r", model: "claude-opus-5-5", status: "proposed", decidedBy: null, decidedAt: null, numberCheck: check };
  test("a wording change that fails the number check can't be accepted", () => {
    const failing = { passed: false, checkedAt: at, unmatched: ["42%"] };
    assert.ok(!WordingProposal.safeParse({ ...proposal, status: "accepted", decidedBy: "A Person", decidedAt: at, numberCheck: failing }).success);
    assert.ok(WordingProposal.parse({ ...proposal, numberCheck: failing }));
  });
  test("a decision records who and when", () => assert.ok(!WordingProposal.safeParse({ ...proposal, status: "rejected" }).success));

  test("a number override needs a reason", () => {
    const override = { id: "o1", runId: "r1", target: { type: "metric", id: "m", field: "value" }, previousValue: 1, newValue: 2, reason: "Customer corrected it", overriddenBy: "A Person", overriddenAt: at };
    assert.ok(NumberOverride.parse(override));
    assert.ok(!NumberOverride.safeParse({ ...override, reason: "" }).success);
  });

  test("Score distinguishes not measurable from zero", () => {
    assert.ok(Score.parse({ status: "not_measurable", domain: "forecast", ruleSetVersion: "1", reason: "Forecasting isn't in use" }));
    assert.ok(!Score.safeParse({ status: "measured", domain: "forecast", ruleSetVersion: "1", score: 120, weights: {}, findingIds: [] }).success);
  });

  test("a customer baseline points at a run and has a deletion date", () => {
    assert.ok(CustomerBaseline.parse({ customerRef: "c1", runId: "r1", ruleSetVersion: "1", capturedAt: at, deleteAfter: at }));
  });
});

describe("BenchmarkRow (rule 11)", () => {
  const row = { metricKey: "historic.win_rate", ruleSetVersion: "2026.11", value: 35, unit: "percent", industry: "Manufacturing", headcountBand: "51-200", userBand: "51-200", dealSizeBand: "5k-25k", period: "2026-Q4" };
  test("a clean row parses", () => assert.ok(BenchmarkRow.parse(row)));
  test("no extra field can link back to a customer", () => {
    for (const extra of [{ orgId: "00D000000000001" }, { customerName: "Acme" }, { runId: "r1" }, { consultant: "A Person" }]) {
      assert.ok(!BenchmarkRow.safeParse({ ...row, ...extra }).success, Object.keys(extra)[0]);
    }
  });
  test("values must be rounded", () => assert.ok(!BenchmarkRow.safeParse({ ...row, value: 35.27 }).success));
  test("bands only, not exact sizes", () => assert.ok(!BenchmarkRow.safeParse({ ...row, headcountBand: "137" }).success));
  test("a quarter, not a date", () => assert.ok(!BenchmarkRow.safeParse({ ...row, period: "2026-10-31" }).success));
});

describe("Run", () => {
  test("records steps and the budget, so it can resume", () => {
    const run = {
      id: "r1", product: "barometer", customerRef: null, orgId: "00D000000000001AAA", isSandbox: false, connectionKind: "sf-cli", status: "extracting", modules: ["automation"], ruleSetVersion: "1", calibrationHash: "abc", baselineRunId: null, asOf: at,
      analysisWindow: { from: "2024-10-31", to: "2026-10-31" }, apiVersion: "66.0",
      budget: { stopAtShare: 0.5, dailyLimit: 100, usedAtStart: 1, usedAtLastCheck: 2, callsMade: 1, stoppedReason: null },
      steps: [{ id: "users", version: 1, status: "pending", calls: 0, finishedAt: null, error: null }],
      benchmarkOptIn: false, createdBy: "cli", startedAt: at, finishedAt: null,
    };
    assert.ok(Run.parse(run));
    assert.ok(Run.parse({ ...run, baselineRunId: "r0" }), "a repeat check points at its baseline");
  });
});
