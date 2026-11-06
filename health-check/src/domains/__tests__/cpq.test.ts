import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { CPQ_MODULE, CPQ_MODULES, knownModuleIds, modulesForRun, SALES_CLOUD_MODULES } from "../index.ts";
import { ESSENTIALS_DOMAINS, modulesFor } from "../scope.ts";
import { AREA_WEIGHTS } from "../scoring.ts";
import { DEEP_DISCOUNT, DISCOUNT_GAP } from "../cpq/quoting.ts";
import { ACTION_LISTS_FILE, type ActionLists } from "../../actions/lists.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { CpqQuotes, CpqSetup } from "../../extract/cpq.ts";
import { buildReport } from "../../narrative/sections.ts";
import { recordAreas } from "../../narrative/select.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, userId } from "../../testing/synthetic-org.ts";

// Salesforce CPQ (roadmap Stage 6, DECISIONS.md 216 to 225).

const modules = [...SALES_CLOUD_MODULES, ...CPQ_MODULES];
const { output, dir, run } = await runFromRecording({ answers: loadAnswers(), modules });
const extracts = loadExtracts(dir);
const lists = JSON.parse(readFileSync(join(dir, ACTION_LISTS_FILE), "utf8")) as ActionLists;

const one = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};
const edited = <T>(id: string, edit: (data: T) => void): Finding[] => {
  const changed = clone(extracts);
  edit(changed.get(id) as T);
  return analyseExtracts(changed, { modules }).findings;
};
const CPQ_RULES = CPQ_MODULES.flatMap((m) => m.rules.map((r) => r.id));

describe("CPQ as a module", () => {
  test("switched on by \"cpq\", which brings both areas, and off unless named", () => {
    assert.equal(CPQ_MODULE, "cpq");
    assert.deepEqual(modulesForRun(["configuration", "cpq"]).map((m) => m.id), ["configuration", "cpq_setup", "cpq_quoting"]);
    assert.ok(!modulesForRun(["configuration", "service"]).some((m) => m.cloud === "cpq"));
    assert.ok(knownModuleIds().has("cpq"));
    assert.ok(!knownModuleIds(SALES_CLOUD_MODULES).has("cpq"));
  });

  test("its set-up is in a set-up review, and its quoting is measured from records", () => {
    assert.ok(ESSENTIALS_DOMAINS.includes("cpq_setup") && !ESSENTIALS_DOMAINS.includes("cpq_quoting"));
    assert.ok(modulesFor("essentials", CPQ_MODULES).every((m) => m.id === "cpq_setup"));
    assert.deepEqual(recordAreas({ scope: "essentials", modules: ["configuration", "cpq_setup"] }).filter((d) => d.startsWith("cpq")), ["cpq_quoting"]);
  });

  test("both areas are scored: quoting at 1.25, set-up at 0.75", () => {
    assert.equal(AREA_WEIGHTS.cpq_quoting, 1.25);
    assert.equal(AREA_WEIGHTS.cpq_setup, 0.75);
    assert.deepEqual(run.modules.filter((m) => m.startsWith("cpq")).sort(), ["cpq_quoting", "cpq_setup"]);
  });
});

describe("an org without CPQ, with the module switched on", async () => {
  const transport = new Simulator({ cpq: false });
  const { output: without } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), transport });

  test("every CPQ check is not measurable, saying CPQ isn't installed", () => {
    for (const id of CPQ_RULES) {
      const f = one(without!.findings, id);
      assert.equal(f.status, "not_measurable", id);
      assert.match(f.notMeasurableReason!, /CPQ \(the SBQQ package\) isn't installed/);
    }
  });

  test("nothing of CPQ is queried: one describe finds it missing", () => {
    const urls = transport.requests.map((r) => decodeURIComponent(r.url));
    assert.deepEqual(urls.filter((u) => /SBQQ/.test(u)).map((u) => u.replace(/^https:\/\/[^/]+/, "").replace(/v\d+\.\d/, "vXX")), ["/services/data/vXX/sobjects/SBQQ__Quote__c/describe"]);
  });
});

describe("CPQ set-up", () => {
  test("price rules that set the same field at the same event and order, and not one in a later order", () => {
    const f = one(output!.findings, "cpq.price-rule-conflicts");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Partner Discount and Volume Discount"]);
    assert.equal(f.confidence, "medium");
  });

  test("no conflict once the pair are in different orders", () => {
    const findings = edited<CpqSetup>("cpq-setup", (s) => (s.priceRules as { data: Array<{ Name: string; SBQQ__EvaluationOrder__c?: number | null }> }).data.forEach((r) => r.Name === "Partner Discount" && (r.SBQQ__EvaluationOrder__c = 15)));
    assert.equal(one(findings, "cpq.price-rule-conflicts").status, "passed");
  });

  test("a twin field whose type differs, and not one that matches", () => {
    const f = one(output!.findings, "cpq.twin-field-mismatch");
    assert.equal(f.status, "issue");
    assert.deepEqual(f.subjects.map((s) => s.label), ["Warranty Term"]);
  });

  test("inactive rules: four of eleven", () => {
    const f = one(output!.findings, "cpq.inactive-rules");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.inactive, 4);
    assert.equal(f.facts.total, 11);
  });

  test("inactive rules: quiet when every rule is active (night 3 review: the clean twin)", () => {
    const findings = edited<CpqSetup>("cpq-setup", (s) => {
      for (const list of [s.priceRules, s.productRules] as Array<{ data?: Array<{ SBQQ__Active__c: boolean }> }>) list.data?.forEach((r) => (r.SBQQ__Active__c = true));
    });
    assert.equal(one(findings, "cpq.inactive-rules").status, "passed");
  });
});

describe("discount leakage", () => {
  const f = one(output!.findings, "cpq.discount-leakage");

  test("names the rep who discounts deeper than their team-mate without winning more, measured from the org's own quotes", () => {
    assert.equal(f.status, "issue");
    assert.equal(f.subjects.length, 1);
    assert.equal(f.subjects[0]!.type, "rep");
    assert.ok(f.facts.segment1Median! >= f.facts.segment1PeerMedian! + DISCOUNT_GAP);
    assert.ok(f.facts.segment1WinRate! <= f.facts.segment1PeerWinRate!);
    assert.match(f.summary, /against .* for their team-mates/);
    // One rep named, so the title is singular (night 3 reading pass).
    assert.equal(f.title, "One rep discounts deeper than peers without winning more");
  });

  test("its figure is the discount past the peers' median on won deals, potential, and yearly in the value model", () => {
    assert.equal(f.value.kind, "revenue");
    assert.match(f.value.kind === "revenue" ? f.value.period : "", /^in discount beyond what peers gave/);
    const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps });
    const line = report.value.groups.flatMap((g) => g.lines).find((l) => l.ruleId === "cpq.discount-leakage");
    assert.ok(line, "the value model counts it");
    assert.match(line.basis, /divided by/);
  });

  test("its figure is money to recover, in its own group with a range, so it reaches the headline (DECISIONS.md 322)", () => {
    const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps });
    const group = report.value.groups.find((g) => g.lines.some((l) => l.ruleId === "cpq.discount-leakage"));
    assert.ok(group, "the value model groups it");
    assert.equal(group.id, "discount-leakage");
    assert.equal(group.kind, "revenue_recoverable");
    assert.match(group.label, /^Potential/);
    assert.deepEqual(group.lines.map((l) => l.ruleId), ["cpq.discount-leakage"], "nothing else shares its group");
    assert.ok(group.counted.low < group.counted.expected && group.counted.expected < group.counted.high);
    assert.ok(report.value.byKind.revenue_recoverable.expected >= group.counted.expected, "it's in the headline's recoverable revenue");
  });

  test("not measurable on too few won quotes, and quiet when the rep discounts like their team-mate", () => {
    const few = edited<CpqQuotes>("cpq-quotes", (q) => void ((q.quotes as { data: unknown[] }).data = (q.quotes as { data: unknown[] }).data.slice(0, 10)));
    assert.equal(one(few, "cpq.discount-leakage").status, "not_measurable");
    const even = edited<CpqQuotes>("cpq-quotes", (q) => (q.quotes as { data: Array<{ OwnerId: string; SBQQ__ListAmount__c: number | null; SBQQ__NetAmount__c: number | null }> }).data.forEach((x) => x.OwnerId === userId(1) && x.SBQQ__NetAmount__c !== null && (x.SBQQ__ListAmount__c = Math.round(x.SBQQ__NetAmount__c / 0.91))));
    assert.equal(one(even, "cpq.discount-leakage").status, "passed");
  });
});

describe("approvals, orders and renewals", () => {
  test("deep discounts with no approval, listed by quote", () => {
    const f = one(output!.findings, "cpq.discount-approvals");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.threshold, DEEP_DISCOUNT);
    const list = lists.lists.find((l) => l.ruleId === "cpq.discount-approvals")!;
    assert.equal(list.object, "SBQQ__Quote__c");
    assert.equal(list.rows.length, f.facts.without);
    assert.ok(list.rows.every((r) => /^Q-\d{5}$/.test(r.name ?? "")));
  });

  test("a cut-off approvals read can't call a quote unapproved: the missing approval may be in the rows not read (DECISIONS.md 325)", () => {
    const base = one(output!.findings, "cpq.discount-approvals");
    assert.equal(base.status, "issue");
    const cut = edited<CpqQuotes>("cpq-quotes", (q) => void ((q.approvals as { truncated?: boolean }).truncated = true));
    const f = one(cut, "cpq.discount-approvals");
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason ?? "", /approvals/i);
  });

  test("a draft quote on a deal still open isn't an unapproved discount: its approval may simply not have happened yet (night 3 review)", () => {
    const changed = clone(extracts);
    const cpq = changed.get("cpq-quotes") as CpqQuotes;
    const quotes = (cpq.quotes as { data: Array<Record<string, unknown>> }).data;
    const approvals = (cpq.approvals as { data: Array<{ Id: string; TargetObjectId: string; Status: string }> }).data;
    // Every quote there is went through approval, so the rule passes ...
    quotes.forEach((q, i) => approvals.push({ Id: `04g${String(800000 + i).padStart(12, "0")}`, TargetObjectId: String(q.Id), Status: "Approved" }));
    // ... and then a deep draft on a deal nobody has closed yet.
    const deals = (changed.get("opportunities") as { opportunities: { data: Array<{ Id: string; IsClosed: boolean }> } }).opportunities.data;
    const open = deals.find((d) => !d.IsClosed)!;
    quotes.push({ ...quotes[0]!, Id: "a0Q000000000999AAA", Name: "Q-99999", SBQQ__Opportunity2__c: open.Id, SBQQ__ListAmount__c: 10_000, SBQQ__NetAmount__c: 7_000 });
    const f = one(analyseExtracts(changed, { modules }).findings, "cpq.discount-approvals");
    assert.equal(f.status, "passed", f.summary);
  });

  test("a quote or approval read cut off at its row limit gives no pass and no unapproved discount", () => {
    // The clean twins of all three, each passing on the whole read.
    const clean = (cpq: CpqQuotes): void => {
      const quotes = (cpq.quotes as { data: Array<Record<string, unknown>> }).data;
      const approvals = (cpq.approvals as { data: Array<{ Id: string; TargetObjectId: string; Status: string }> }).data;
      quotes.forEach((q, i) => {
        approvals.push({ Id: `04g${String(800000 + i).padStart(12, "0")}`, TargetObjectId: String(q.Id), Status: "Approved" });
        q.SBQQ__Ordered__c = true;
        if (q.OwnerId === userId(1) && q.SBQQ__NetAmount__c !== null) q.SBQQ__ListAmount__c = Math.round(Number(q.SBQQ__NetAmount__c) / 0.91);
      });
    };
    const whole = edited<CpqQuotes>("cpq-quotes", clean);
    const cutQuotes = edited<CpqQuotes>("cpq-quotes", (cpq) => (clean(cpq), Object.assign(cpq.quotes, { truncated: true })));
    for (const id of ["cpq.discount-leakage", "cpq.discount-approvals", "cpq.quote-to-order"]) {
      assert.equal(one(whole, id).status, "passed", id);
      assert.equal(one(cutQuotes, id).status, "not_measurable", id);
      assert.match(one(cutQuotes, id).notMeasurableReason!, /read of quotes stopped at its row limit/, id);
    }
    // An approval past the cut-off would make its quote look unapproved: the issue on the real org becomes not measurable.
    const cutApprovals = edited<CpqQuotes>("cpq-quotes", (cpq) => void Object.assign(cpq.approvals, { truncated: true }));
    assert.equal(one(output!.findings, "cpq.discount-approvals").status, "issue");
    assert.match(one(cutApprovals, "cpq.discount-approvals").notMeasurableReason!, /read of approvals stopped at its row limit/);
  });

  test("not measurable with Advanced Approvals, whose records we don't read", () => {
    const findings = edited<CpqSetup>("cpq-setup", (s) => void (s.presence = { status: "installed", advancedApprovals: true }));
    assert.match(one(findings, "cpq.discount-approvals").notMeasurableReason!, /Advanced Approvals/);
  });

  test("won quotes never ordered: quiet once every won quote is ordered (night 3 review: the clean twin)", () => {
    const all = edited<CpqQuotes>("cpq-quotes", (q) => (q.quotes as { data: Array<{ SBQQ__Ordered__c?: boolean | null }> }).data.forEach((x) => (x.SBQQ__Ordered__c = true)));
    assert.equal(one(all, "cpq.quote-to-order").status, "passed");
  });

  test("won quotes never ordered, listed by opportunity, and not measurable when nothing is ever ordered", () => {
    const f = one(output!.findings, "cpq.quote-to-order");
    assert.equal(f.status, "issue");
    assert.equal(lists.lists.find((l) => l.ruleId === "cpq.quote-to-order")!.rows.length, f.facts.unordered);
    const never = edited<CpqQuotes>("cpq-quotes", (q) => (q.quotes as { data: Array<{ SBQQ__Ordered__c?: boolean | null }> }).data.forEach((x) => (x.SBQQ__Ordered__c = false)));
    assert.equal(one(never, "cpq.quote-to-order").status, "not_measurable");
  });

  test("contracts ending soon with no renewal, listed by contract number, leaving out the one ending too late", () => {
    const f = one(output!.findings, "cpq.renewals-not-started");
    assert.equal(f.status, "issue");
    assert.deepEqual([f.facts.contracts, f.facts.pending], [3, 2]);
    const list = lists.lists.find((l) => l.ruleId === "cpq.renewals-not-started")!;
    assert.deepEqual(list.rows.map((r) => r.name), ["00000101", "00000103"]);
  });
});
