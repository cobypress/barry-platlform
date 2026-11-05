import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { collectExtractors, runHealthCheck } from "../../engine/run.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Extractor } from "../../extract/types.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { Run } from "../../schemas/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";
import { ESSENTIALS_DOMAINS, modulesFor } from "../scope.ts";

// Objects that hold CRM records. A set-up review reads none of them, not even as a count.
const CRM_OBJECTS = new Set(["Account", "Contact", "Lead", "Opportunity", "OpportunityHistory", "OpportunityFieldHistory", "OpportunityLineItem", "LeadHistory", "Task", "Event", "EmailMessage", "Case", "Campaign", "CampaignMember", "ForecastingItem", "ForecastingQuota", "ForecastingAdjustment"]);
const isCrmObject = (object: string): boolean => CRM_OBJECTS.has(object) || /__c$/.test(object) || (/History$/.test(object) && object !== "LoginHistory");

const essentials = modulesFor("essentials", SALES_CLOUD_MODULES);
const recordExtractors = new Set(collectExtractors(SALES_CLOUD_MODULES).filter((e) => e.records).map((e) => e.id));

describe("which modules, extracts and rules a scope runs", () => {
  test("complete runs everything, unchanged", () => {
    assert.equal(modulesFor("complete", SALES_CLOUD_MODULES), SALES_CLOUD_MODULES);
  });

  test("essentials runs the set-up areas, with no record extract and no rule that needs one", () => {
    assert.deepEqual(essentials.map((m) => m.id).sort(), [...ESSENTIALS_DOMAINS].sort());
    for (const m of essentials) {
      assert.deepEqual(m.extractors.filter((e) => e.records).map((e) => e.id), [], m.id);
      assert.deepEqual(m.rules.filter((r) => r.records).map((r) => r.id), [], m.id);
    }
    // The record extracts the set-up areas list. The trace below catches any new one left unflagged.
    for (const id of ["field-usage", "opportunities", "ownership"]) assert.ok(recordExtractors.has(id), id);
  });

  test("no essentials rule or metric reads a record extract, on the full synthetic data", async () => {
    // Every extract a complete run makes is there, so a rule that reaches for a
    // record extract would get it, and is caught here rather than in a customer's run.
    const { dir, run } = await runFromRecording({ answers: loadAnswers() });
    const extracts = new Map(readdirSync(join(dir, "checkpoints")).filter((f) => f.endsWith(".json")).map((f) => [f.slice(0, -5), JSON.parse(readFileSync(join(dir, "checkpoints", f), "utf8")) as unknown]));
    const touched = new Set<string>();
    const now = new Date(run.asOf);
    const ctx: Omit<RuleContext, "metrics"> = {
      scope: "essentials",
      calibration: loadCalibration(),
      now,
      window: run.analysisWindow,
      pseudonyms: new Pseudonymiser(),
      data: <T>(extractor: Extractor<T>): T => {
        touched.add(extractor.id);
        return extracts.get(extractor.id) as T;
      },
      manual: (key) => ({ step: { id: `ms-${key}` }, status: "pending", answer: null, evidence: { kind: "manual", manualStepId: `ms-${key}`, status: "pending", answeredBy: null, answeredAt: null, count: null, dateRange: null, exclusions: [], capturedAt: now.toISOString() } }) as never,
      ev: (source) => ({ ...source, count: null, dateRange: null, exclusions: [], capturedAt: now.toISOString() }) as never,
      userLabel: () => "a user",
    };
    const metrics = essentials.flatMap((m) =>
      (m.metrics ?? []).flatMap((build) => {
        touched.clear();
        const built = build(ctx);
        assert.deepEqual([...touched].filter((id) => recordExtractors.has(id)), [], `${m.id} metrics`);
        return built;
      }),
    );
    for (const m of essentials) {
      for (const rule of m.rules) {
        touched.clear();
        rule.evaluate({ ...ctx, metrics });
        assert.deepEqual([...touched].filter((id) => recordExtractors.has(id)), [], rule.id);
      }
    }
  });
});

describe("an essentials run", async () => {
  const objects = new Set<string>();
  const transport = new Simulator({
    failQuery: (soql) => {
      const object = /\bFROM\s+(\w+)/i.exec(soql)?.[1] ?? "";
      objects.add(object);
      // Fails loudly if it ever happens, so the run can't quietly succeed without it.
      return isCrmObject(object) ? { errorCode: "INSUFFICIENT_ACCESS", message: `A set-up review read ${object}` } : null;
    },
  });
  const { run, output } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport });

  test("never queries an object that holds CRM records", () => {
    assert.ok(objects.size > 10);
    assert.deepEqual([...objects].filter(isCrmObject), []);
  });

  test("records its scope and its areas", () => {
    assert.equal(Run.parse(run).scope, "essentials");
    assert.deepEqual([...run.modules].sort(), [...ESSENTIALS_DOMAINS].sort());
  });

  test("every rule ran: none is not measurable because an extract is missing", () => {
    assert.ok(output!.findings.length > 20);
    assert.deepEqual(output!.findings.filter((f) => /failed to run|didn't run/.test(f.notMeasurableReason ?? "")).map((f) => f.ruleId), []);
    assert.ok(output!.findings.every((f) => ESSENTIALS_DOMAINS.includes(f.domain)));
  });

  test("its metrics are measured without Opportunity, which it never reads", () => {
    assert.equal(output!.metrics.find((m) => m.id === "configuration.object_inventory")?.status, "measured");
    assert.equal(output!.metrics.find((m) => m.id === "investment.seats_paid")?.status, "measured");
  });

  test("a run from before scopes existed reads as complete", () => {
    const { scope: _, ...old } = run;
    assert.equal(Run.parse(old).scope, "complete");
  });
});
