import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { automationModule } from "../automation/index.ts";
import { APEX_CODE_LIMIT, technicalHealthModule } from "../technical-health/index.ts";
import { findingId } from "../../engine/evaluate.ts";
import type { ApexData } from "../../extract/automation.ts";
import type { ClassicData } from "../../extract/technical.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

const { output, dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const gone = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql", api: "rest", query: "x" } };

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const withApex = (edit: (apex: ApexData) => void): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get("apex") as ApexData);
  return analyseExtracts(changed).findings;
};
const withClassic = (edit: (classic: ClassicData) => void): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get("classic") as ClassicData);
  return analyseExtracts(changed).findings;
};
type Row = { Name: string; Status: string; isTest: boolean; LengthWithoutComments?: number; scan: Record<string, number | boolean> | null };
const rows = (apex: ApexData): Row[] => [...(apex.classes as { data: Row[] }).data, ...(apex.triggers as { data: Row[] }).data];

describe("the checks that moved from automation", () => {
  const MOVED = ["aut.apex-coverage", "aut.old-api-versions", "aut.multiple-triggers"];

  test("sit in technical health, keep their ids, and leave automation", () => {
    for (const id of MOVED) {
      assert.ok(technicalHealthModule.rules.some((r) => r.id === id), id);
      assert.ok(!automationModule.rules.some((r) => r.id === id), id);
      const f = one(output!.findings, id);
      assert.equal(f.domain, "technical_health");
      // The id hangs on the rule alone, so a re-check of an older baseline lines it up.
      assert.equal(f.id, findingId(id, ""));
    }
  });
});

describe("Apex tests that check nothing", () => {
  test("fires on the test with no assertion and the one that reads org data, and leaves the data factory alone", () => {
    const f = one(output!.findings, "tech.test-quality");
    assert.equal(f.status, "issue");
    assert.equal(f.confidence, "medium");
    assert.deepEqual(f.subjects.map((s) => s.label), ["OpportunityHandlerTest", "LeadRouterTest"]);
    assert.deepEqual(f.facts, { testClasses: 2, withoutAssertions: 1, seeAllData: 1 });
    assert.equal(f.severity, "medium");
  });

  test("passes when every test asserts and none reads org data", () => {
    const findings = withApex((apex) => rows(apex).forEach((r) => r.scan && ((r.scan.asserts = 1), (r.scan.seeAllData = false))));
    assert.equal(one(findings, "tech.test-quality").status, "passed");
  });

  test("not measurable when there are no test methods to read", () => {
    const findings = withApex((apex) => rows(apex).forEach((r) => r.scan && (r.scan.testMethods = 0)));
    assert.equal(one(findings, "tech.test-quality").status, "not_measurable");
  });

  test("not measurable when classes can't be read", () => {
    assert.equal(one(withApex((apex) => void (apex.classes = gone as never)), "tech.test-quality").status, "not_measurable");
  });
});

describe("queries and saves inside loops", () => {
  test("fires on the class that queries and saves in a loop, not the SOQL for loop", () => {
    const f = one(output!.findings, "tech.governor-limit-risk");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["LeadRouter"]);
    assert.equal(f.severity, "medium");
    assert.match(f.summary, /100 queries and 150 saves in one transaction/);
  });

  test("high when a trigger does it, since a trigger runs for many records at once", () => {
    const findings = withApex((apex) => (apex.triggers as unknown as { data: Row[] }).data[0]!.scan!.dmlInLoops = 2);
    const f = one(findings, "tech.governor-limit-risk");
    assert.equal(f.severity, "high");
    assert.equal(f.facts.triggers, 1);
  });

  test("passes on clean code, and test classes never count", () => {
    const findings = withApex((apex) => rows(apex).forEach((r) => r.scan && !r.isTest && ((r.scan.queriesInLoops = 0), (r.scan.dmlInLoops = 0))));
    assert.equal(one(findings, "tech.governor-limit-risk").status, "passed");
    // A test class that queries and saves in a loop, on otherwise clean code, still passes (night 3 review: none did before).
    const inTests = withApex((apex) => {
      rows(apex).forEach((r) => r.scan && !r.isTest && ((r.scan.queriesInLoops = 0), (r.scan.dmlInLoops = 0)));
      const test = rows(apex).find((r) => r.isTest && r.scan)!;
      test.scan!.queriesInLoops = 3;
      test.scan!.dmlInLoops = 2;
    });
    assert.equal(one(inTests, "tech.governor-limit-risk").status, "passed");
  });

  test("not measurable when triggers can't be read", () => {
    assert.equal(one(withApex((apex) => void (apex.triggers = gone as never)), "tech.governor-limit-risk").status, "not_measurable");
  });
});

describe("Apex against the size limit", () => {
  test("passes on a small org, with test classes left out of the count", () => {
    const f = one(output!.findings, "tech.apex-code-size");
    assert.equal(f.status, "passed");
    // Classes 1200 + 800 + 400 + 350, triggers 180 + 220 + 150 + 260 + 140. The three @isTest classes aren't counted.
    assert.equal(f.facts.used, 3700);
    assert.equal(f.facts.limit, APEX_CODE_LIMIT);
  });

  test("medium past three quarters of the limit, high past nine tenths", () => {
    const at = (size: number): Finding => one(withApex((apex) => ((apex.classes as unknown as { data: Row[] }).data[0]!.LengthWithoutComments = size)), "tech.apex-code-size");
    assert.equal(at(4_600_000).severity, "medium");
    assert.equal(at(5_500_000).severity, "high");
    assert.match(at(5_500_000).summary, /no new code can be saved/);
  });

  test("not measurable when a trigger's size wasn't read", () => {
    const findings = withApex((apex) => delete (apex.triggers as unknown as { data: Row[] }).data[0]!.LengthWithoutComments);
    assert.equal(one(findings, "tech.apex-code-size").status, "not_measurable");
  });
});

describe("set-up that only works in Classic", () => {
  test("fires on the JavaScript button and the page not marked for Lightning, and not on the package's own button", () => {
    const f = one(output!.findings, "tech.classic-leftovers");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "medium");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Opportunity: Send_To_ERP", "InvoicePrint"]);
  });

  test("low when only Visualforce pages are left", () => {
    const findings = withClassic((c) => void ((c.links as { data: Array<{ LinkType: string }> }).data = []));
    assert.equal(one(findings, "tech.classic-leftovers").severity, "low");
  });

  test("passes when nothing is left, and isn't a pass when s-controls couldn't be read", () => {
    const clean = (c: ClassicData): void => {
      (c.links as { data: unknown[] }).data = [];
      for (const p of (c.pages as { data: Array<{ IsAvailableInTouch: boolean | null }> }).data) p.IsAvailableInTouch = true;
    };
    assert.equal(one(withClassic(clean), "tech.classic-leftovers").status, "passed");
    assert.equal(one(withClassic((c) => (clean(c), (c.scontrols = gone as never))), "tech.classic-leftovers").status, "not_measurable");
  });
});
