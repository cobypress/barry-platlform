import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { DomainModule } from "../../domains/types.ts";
import { Finding } from "../../schemas/index.ts";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import type { RuleResult } from "../rule.ts";
import { eng, joinSaid, markSentence, org, orgWordsOf, said, spansFit, spansIfCurrent, withOrgWords, type Said } from "../org-words.ts";
import { UNNAMED } from "../../domains/automation/shared.ts";
import type { Labels } from "../../extract/labels.ts";
import { clone } from "../../testing/harness.ts";

// The marks themselves (DECISIONS.md 655): made where a sentence is put
// together, looked up by the engine, and dropped whenever the text moves.

const text = (spans: Array<{ start: number; end: number }>, s: string) => spans.map((x) => s.slice(x.start, x.end));

describe("said and org", () => {
  test("org words are marked where they're interpolated, numbers and literals are the engine's, and the string is plain", () => {
    withOrgWords(() => {
      const label = "Invoice";
      const n: number = 3;
      const s = said`${org(label)} runs ${n} ${n === 1 ? "tool" : "tools"}: ${org("Big Flow")}.`;
      assert.equal(typeof s, "string");
      assert.equal(s, "Invoice runs 3 tools: Big Flow.");
      assert.deepEqual(orgWordsOf(s), [{ start: 0, end: 7 }, { start: 22, end: 30 }]);
      assert.deepEqual(text(orgWordsOf(s)!, s), ["Invoice", "Big Flow"]);
    });
  });

  test("a sentence made from sentences keeps their spans, shifted; joinSaid keeps them across the separators", () => {
    withOrgWords(() => {
      const parts = ["Account", "Opportunity"].map((o) => said`${org(o)} has 2`);
      const joined = joinSaid(parts, "; ");
      const whole = said`Objects: ${joined}.`;
      assert.equal(whole, "Objects: Account has 2; Opportunity has 2.");
      assert.deepEqual(text(orgWordsOf(whole)!, whole), ["Account", "Opportunity"]);
    });
  });

  test("a sentence with no org words is vouched for with no spans; eng vouches for an engine string the types can't see", () => {
    withOrgWords(() => {
      assert.deepEqual(orgWordsOf(said`Flow failures aren't handled`), []);
      const s = said`${eng("2 flows")} fail`;
      assert.deepEqual(orgWordsOf(s), []);
    });
  });

  test("a string said didn't make, or one made twice with different spans, is unknown", () => {
    withOrgWords(() => {
      const label = "Invoice";
      assert.equal(orgWordsOf(`${label} runs`), null);
      said`${org("Invoice")} runs`;
      said`Invoice runs`;
      assert.equal(orgWordsOf("Invoice runs"), null);
    });
  });

  test("unknown goes up: a sentence quoting one held as unknown is unknown, through said and joinSaid, at any depth", () => {
    withOrgWords(() => {
      // The reviewer's case C: two sentences with the same text and different spans.
      const x = said`${org("Evil on Do it")} on ${org("Z")}`;
      const y = said`${org("Evil")} on ${org("Do it on Z")}`;
      assert.equal(x, y);
      assert.equal(orgWordsOf(x), null);
      const outer = said`Fields: ${joinSaid([x, y], "; ")}.`;
      assert.equal(orgWordsOf(outer), null);
      assert.equal(orgWordsOf(said`Deeper: ${outer}`), null);
      // The live case: an object labelled exactly as we name one we couldn't.
      const named = said`${org(UNNAMED)} has 2`;
      const unnamed = said`${UNNAMED} has 2`;
      assert.equal(orgWordsOf(named), null);
      assert.equal(orgWordsOf(said`${joinSaid([unnamed], "; ")}. Salesforce doesn't guarantee the order they run in.`), null);
    });
  });

  test("a part joinSaid didn't see said make is unknown, and so is a sentence with org words made outside the analysis", () => {
    const outside = said`${org("Outside Flow")} runs`;
    withOrgWords(() => {
      assert.equal(orgWordsOf(joinSaid(["2 flows" as Said, said`fine`], "; ")), null);
      assert.equal(orgWordsOf(said`Quoted: ${outside}`), null);
      // eng records nothing: what it vouches for is read, interpolated, as a literal is.
      const vouched = eng("Apex trigger, Workflow Rule");
      assert.equal(orgWordsOf(vouched), null);
      assert.deepEqual(orgWordsOf(said`${org("Invoice")} runs ${vouched}`), [{ start: 0, end: 7 }]);
    });
  });

  test("outside an analysis nothing is recorded, and an analysis's ledger goes with it", () => {
    const s = said`${org("Invoice")} runs`;
    assert.equal(orgWordsOf(s), null);
    withOrgWords(() => said`${org("Lead")} runs`);
    assert.equal(orgWordsOf("Lead runs"), null);
  });

  test("the types refuse a string read at run time: it has to be org words or vouched for", () => {
    const label: string = "Invoice";
    // @ts-expect-error a value read from the org can't pass as the engine's words
    assert.throws(() => withOrgWords(() => said`${{ label }} runs`));
    // @ts-expect-error nor can a plain string
    said`${label} runs`;
  });

  test("a kept marking is used only while its sentence still hashes to it, and only if every span fits", () => {
    const marked = markSentence("Invoice runs", [{ start: 0, end: 7 }])!;
    assert.deepEqual(spansIfCurrent("Invoice runs", marked), [{ start: 0, end: 7 }]);
    assert.equal(spansIfCurrent("Invoice runs 4", marked), null);
    assert.equal(spansIfCurrent("Invoice runs", undefined), null);
    assert.equal(spansIfCurrent("Invoice runs", { ...marked, spans: [{ start: 0, end: 40 }] }), null);
    assert.equal(spansFit("abcdef", [{ start: 2, end: 4 }, { start: 3, end: 5 }]), false);
    assert.equal(spansFit("abcdef", [{ start: 2, end: 2 }]), false);
    assert.equal(spansFit("abcdef", [{ start: 0, end: 2 }, { start: 4, end: 6 }]), true);
  });
});

describe("the engine's marks on a finding", async () => {
  const { dir } = await runFromRecording();
  const extracts = loadExtracts(dir);
  const ev = { kind: "metadata" as const, path: "flows/*.flow" };
  const stub = (result: (ctx: Parameters<DomainModule["rules"][number]["evaluate"]>[0]) => RuleResult): DomainModule => ({
    id: "automation",
    cloud: "sales",
    label: "Stub",
    extractors: [],
    rules: [{ id: "aut.stub", version: 1, domain: "automation", title: "A stub rule", evaluate: result }],
  });

  test("a sentence the name scrub changed loses its marks, so no span can sit on a person's token", () => {
    // Alex Morgan is a user of the synthetic org.
    const out = analyseExtracts(extracts, { modules: [stub((ctx) => ({ outcome: "pass", summary: said`Flow ${org("Alex Morgan Export")} runs.`, evidence: [ctx.ev(ev)], segmentExemptReason: "Stub." }))] });
    const f = Finding.parse(out.findings[0]);
    assert.notEqual(f.summary, "Flow Alex Morgan Export runs.");
    assert.equal(f.orgWords?.summary, undefined);
    // The rule's constant title is the engine's own.
    assert.deepEqual(f.orgWords?.title?.spans, []);
  });

  test("live: an object labelled as we name one we couldn't leaves the triggers rule's summary unknown, never marked as ours", () => {
    const changed = clone(extracts);
    const labels = changed.get("labels") as Labels;
    if (labels.objects.status !== "ok") throw new Error("labels");
    labels.objects.data.Opportunity = UNNAMED;
    const apex = changed.get("apex") as { triggers: { status: string; data: Array<{ Name: string; TableEnumOrId: string }> } };
    for (const t of apex.triggers.data) if (t.Name.startsWith("Invoice")) t.TableEnumOrId = "01I000000000099AAA";
    const out = analyseExtracts(changed);
    const f = out.findings.find((x) => x.ruleId === "aut.multiple-triggers")!;
    assert.equal(f.summary.split(`${UNNAMED} has 2`).length - 1, 2, f.summary);
    assert.equal(f.orgWords?.summary, undefined);
    assert.deepEqual(f.orgWords?.title?.spans, []);
  });

  test("a held-back destructive recommendation in the engine's words is marked with no spans, and the org's words in the title still are", () => {
    const out = analyseExtracts(extracts, {
      modules: [
        stub((ctx) => ({
          outcome: "issue",
          title: said`${org("Old Flow")} is unused`,
          summary: said`It hasn't run.`,
          recommendation: said`Remove it.`,
          recommendationType: "remove",
          guards: [{ guardId: "g", subject: "Old Flow", status: "unknown", detail: "Not read.", evidence: [ctx.ev(ev)] }],
          severity: "low",
          confidence: "high",
          evidence: [ctx.ev(ev)],
          value: { kind: "not_sized", reason: "Stub." },
          effort: { size: "XS" },
          owner: "Salesforce admin",
          segmentExemptReason: "Stub.",
        })),
      ],
    });
    const f = out.findings[0]!;
    assert.equal(f.recommendationType, "investigate");
    assert.deepEqual(f.orgWords?.recommendation?.spans, []);
    assert.deepEqual(text(f.orgWords!.title!.spans, f.title), ["Old Flow"]);
  });
});
