import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { applyGuards, analyse, findingId } from "../evaluate.ts";
import { collectExtractors } from "../run.ts";
import type { IssueResult, Rule, RuleResult } from "../rule.ts";
import { segmentRate } from "../rule.ts";
import type { DomainModule } from "../../domains/types.ts";
import type { Extractor } from "../../extract/types.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import type { Evidence } from "../../schemas/index.ts";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { modulesFor } from "../../domains/scope.ts";
import { loadCalibration } from "../../testing/replay.ts";

const at = "2026-10-31T12:00:00.000Z";
const evidence: Evidence = { kind: "rest", path: "x", count: 1, dateRange: null, exclusions: [], capturedAt: at };
const guard = (status: "passed" | "failed" | "unknown") => ({ guardId: "g", subject: "s", status, detail: "d", evidence: [evidence] });

const issue = (overrides: Partial<IssueResult> = {}): IssueResult => ({
  outcome: "issue",
  title: "Remove this",
  summary: "Summary",
  recommendation: "Delete it",
  blockedRecommendation: "Check it first",
  recommendationType: "remove",
  severity: "medium",
  confidence: "high",
  evidence: [evidence],
  value: { kind: "licence_cost", amount: 100, unit: "GBP", period: "per year", assumptions: ["x"] },
  effort: { size: "S" },
  owner: "Admin",
  segmentsChecked: [{ dimension: "d", segments: 1 }],
  ...overrides,
});

function runRule(evaluate: Rule["evaluate"]) {
  const rule: Rule = { id: "t.rule", version: 1, domain: "test", title: "Test rule", evaluate };
  const module: DomainModule = { id: "test", cloud: "sales", label: "Test", extractors: [], rules: [rule] };
  return analyse({ modules: [module], extracts: new Map(), calibration: loadCalibration(), now: new Date(at), window: { from: "2024-10-31", to: "2026-10-31" }, answers: [], pseudonyms: new Pseudonymiser(), userDirectory: new Map() }).findings;
}

describe("guards are enforced by the engine, whatever a rule does (rule 3)", () => {
  test("a destructive finding with a failed guard is downgraded", () => {
    const out = applyGuards(issue({ guards: [guard("passed"), guard("failed")] }));
    assert.equal(out.recommendationType, "investigate");
    assert.equal(out.recommendation, "Check it first");
    assert.equal(out.value.kind, "not_sized");
  });
  test("an unknown guard blocks exactly as a failure does", () => assert.equal(applyGuards(issue({ guards: [guard("unknown")] })).recommendationType, "investigate"));
  test("no guards at all blocks too", () => assert.equal(applyGuards(issue({ guards: [] })).recommendationType, "investigate"));
  test("every guard passing leaves it alone", () => {
    const out = applyGuards(issue({ guards: [guard("passed")] }));
    assert.equal(out.recommendationType, "remove");
    assert.equal(out.value.kind, "licence_cost");
  });
  test("non-destructive findings aren't touched", () => assert.equal(applyGuards(issue({ recommendationType: "change" })).recommendationType, "change"));
  test("a rule that tries to skip its guards can't get through", () => {
    const [finding] = runRule(() => issue({ recommendationType: "deactivate", guards: [guard("failed")] }));
    assert.equal(finding?.recommendationType, "investigate");
    assert.equal(finding?.value.kind, "not_sized");
  });
});

describe("the rule contract", () => {
  test("a pass from a total alone becomes not measurable (rule 4)", () => {
    const [finding] = runRule(() => ({ outcome: "pass", summary: "Totals look fine", evidence: [evidence] }));
    assert.equal(finding?.status, "not_measurable");
    assert.match(finding?.notMeasurableReason ?? "", /without segmenting/);
  });
  test("a pass with segments stands", () => {
    const [finding] = runRule(() => ({ outcome: "pass", summary: "Fine", evidence: [evidence], segmentsChecked: [{ dimension: "team", segments: 3 }] }));
    assert.equal(finding?.status, "passed");
  });
  test("a stated exemption stands, and is kept", () => {
    const [finding] = runRule(() => ({ outcome: "pass", summary: "Fine", evidence: [evidence], segmentExemptReason: "One setting" }));
    assert.equal(finding?.status, "passed");
    assert.equal(finding?.segmentExemptReason, "One setting");
  });
  test("a crash is recorded as not measurable, not lost", () => {
    const [finding] = runRule(() => {
      throw new Error("boom");
    });
    assert.equal(finding?.status, "not_measurable");
    assert.match(finding?.notMeasurableReason ?? "", /boom/);
    assert.equal(finding?.evidence[0]?.kind, "engine");
  });
  test("a finding with no evidence is rejected, and recorded", () => {
    const [finding] = runRule(() => issue({ recommendationType: "change", evidence: [] }) as RuleResult);
    assert.equal(finding?.status, "not_measurable");
  });
  test("a rule reading an extractor that didn't run says so", () => {
    const [finding] = runRule((ctx) => {
      ctx.data({ id: "missing", version: 1, run: async () => null });
      return issue();
    });
    assert.match(finding?.notMeasurableReason ?? "", /didn't run/);
  });
  test("a real name slipped into a finding is scrubbed", () => {
    const pseudonyms = new Pseudonymiser();
    pseudonyms.label("user", "005000000000001AAA", "Alex Morgan", ["alex.morgan@example.com"]);
    const rule: Rule = { id: "t.rule", version: 1, domain: "test", title: "T", evaluate: () => issue({ recommendationType: "change", summary: "Alex Morgan (alex.morgan@example.com) owns this" }) };
    const module: DomainModule = { id: "test", cloud: "sales", label: "Test", extractors: [], rules: [rule] };
    const [finding] = analyse({ modules: [module], extracts: new Map(), calibration: loadCalibration(), now: new Date(at), window: { from: "2024-10-31", to: "2026-10-31" }, answers: [], pseudonyms, userDirectory: new Map() }).findings;
    assert.equal(finding?.summary, "[[user:U0001]] ([[user:U0001]]) owns this");
  });
});

describe("segmentRate", () => {
  const rows = [
    ...Array.from({ length: 90 }, (_, i) => ({ team: "A", bad: i < 2 })),
    ...Array.from({ length: 10 }, (_, i) => ({ team: "B", bad: i < 6 })),
  ];
  const result = segmentRate(rows, (r) => r.bad, { team: (r) => r.team }, { segmentLimit: 50, minSegment: 10 });

  test("the total looks fine", () => assert.equal(result.rate, 8));
  test("the segment underneath it doesn't", () => assert.deepEqual(result.hotspots, [{ dimension: "team", key: "B", n: 10, hits: 6, rate: 60 }]));
  test("records what it checked", () => assert.deepEqual(result.segmentsChecked, [{ dimension: "team", segments: 2 }]));
  test("ignores a segment too small to mean anything", () => {
    assert.deepEqual(segmentRate(rows, (r) => r.bad, { team: (r) => r.team }, { segmentLimit: 50, minSegment: 11 }).hotspots, []);
  });
});

describe("domains are plug-ins", () => {
  const serviceExtractor: Extractor<{ open: number }> = { id: "service-cases", version: 1, run: async () => ({ open: 7 }) };
  const serviceRule: Rule = {
    id: "svc.open-cases",
    version: 1,
    domain: "service_backlog",
    title: "Open case backlog",
    evaluate: (ctx) => ({ outcome: "pass", summary: `Open cases: ${ctx.data(serviceExtractor).open}`, evidence: [ctx.ev({ kind: "rest", path: "x" }, { count: 7 })], segmentsChecked: [{ dimension: "queue", segments: 1 }] }),
  };
  const serviceModule: DomainModule = { id: "service_backlog", cloud: "service", label: "Service backlog", extractors: [serviceExtractor], rules: [serviceRule] };

  test("a Service Cloud module runs without changing the engine", () => {
    const [finding] = analyse({ modules: [serviceModule], extracts: new Map([[serviceExtractor.id, { open: 7 }]]), calibration: loadCalibration(), now: new Date(at), window: { from: "2024-10-31", to: "2026-10-31" }, answers: [], pseudonyms: new Pseudonymiser(), userDirectory: new Map() }).findings;
    assert.equal(finding?.domain, "service_backlog");
    assert.equal(finding?.summary, "Open cases: 7");
  });

  test("its extractors join the run's steps, without repeats", () => {
    const ids = collectExtractors([...SALES_CLOUD_MODULES, serviceModule]).map((e) => e.id);
    assert.ok(ids.includes("service-cases"));
    assert.equal(new Set(ids).size, ids.length);
  });

  test("each extractor's dependencies come before it", () => {
    const ids = collectExtractors(SALES_CLOUD_MODULES).map((e) => e.id);
    assert.ok(ids.indexOf("schema") < ids.indexOf("field-usage"));
    assert.ok(ids.indexOf("schema") < ids.indexOf("opportunities"));
    assert.ok(ids.indexOf("record-counts") < ids.indexOf("ownership"));
  });

  test("the metadata step has the record counts before it, whichever modules a run picks, in either scope (DECISIONS.md 460)", () => {
    for (const scope of ["complete", "essentials"] as const) {
      for (const module of modulesFor(scope, ALL_MODULES)) {
        const ids = collectExtractors([module]).map((e) => e.id);
        if (!ids.includes("metadata")) continue;
        assert.ok(ids.includes("record-counts") && ids.indexOf("record-counts") < ids.indexOf("metadata"), `${scope} ${module.id}: ${ids.join(", ")}`);
      }
    }
  });
});

describe("finding ids", () => {
  test("are stable and differ by key", () => {
    assert.equal(findingId("a", "b"), findingId("a", "b"));
    assert.notEqual(findingId("a", "b"), findingId("a", "c"));
  });
});
