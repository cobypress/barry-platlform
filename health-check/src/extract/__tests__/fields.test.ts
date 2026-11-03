import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { fieldDescribeExtractor, planFields, FIELD_SPECS } from "../fields.ts";
import type { ExtractContext, Sourced } from "../types.ts";

const run = (sim: Simulator) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: sim, answers: loadAnswers() });

const queries = (sim: Simulator, object: string): string[] =>
  sim.requests.map((r) => new URL(r.url).searchParams.get("q") ?? "").filter((q) => new RegExp(`\\bFROM ${object}\\b`).test(q));

const checkpoint = <T>(dir: string, id: string): T => JSON.parse(readFileSync(join(dir, "checkpoints", `${id}.json`), "utf8")) as T;

const finding = (findings: Array<{ ruleId: string; status: string; notMeasurableReason?: string | null }>, ruleId: string) => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.ok(found.length > 0, `no finding for ${ruleId}`);
  return found;
};

describe("a field the org doesn't have", () => {
  test("an absent optional field narrows the query, and only the rule that needs it is not measurable", async () => {
    const sim = new Simulator({ dropFields: { LoginHistory: ["ApiType"], ForecastingItem: ["AmountWithoutAdjustments"], Campaign: ["ActualCost"] } });
    const result = await run(sim);
    assert.ok(result.output, "the run finished");

    const [login] = queries(sim, "LoginHistory");
    assert.ok(login, "LoginHistory was still read");
    assert.doesNotMatch(login, /ApiType/);
    assert.match(login, /Application/);
    assert.doesNotMatch(queries(sim, "ForecastingItem")[0]!, /AmountWithoutAdjustments/);
    assert.doesNotMatch(queries(sim, "Campaign")[0]!, /ActualCost/);

    const logins = checkpoint<Sourced<unknown>>(result.dir, "logins");
    assert.equal(logins.status, "ok");
    assert.deepEqual(logins.status === "ok" && logins.missingFields, ["LoginHistory.ApiType"]);

    const findings = result.output.findings;
    for (const [rule, field] of [
      ["sec.personal-integration-accounts", "LoginHistory.ApiType"],
      ["fct.manager-adjustments", "ForecastingItem.AmountWithoutAdjustments"],
      ["lead.campaign-return", "Campaign.ActualCost"],
    ] as const) {
      for (const f of finding(findings, rule)) {
        assert.equal(f.status, "not_measurable", rule);
        assert.ok(f.notMeasurableReason?.includes(field), `${rule} names ${field}: ${f.notMeasurableReason}`);
      }
    }
    // Everything else reads the same sources and comes out exactly as on a complete org.
    const gated = new Set(["sec.personal-integration-accounts", "fct.manager-adjustments", "lead.campaign-return"]);
    const healthy = await run(new Simulator());
    const others = (out: typeof findings) => out.filter((f) => !gated.has(f.ruleId)).map((f) => `${f.ruleId} ${f.status}`).sort();
    assert.deepEqual(others(findings), others(healthy.output!.findings));
    assert.ok(healthy.output!.findings.some((f) => f.ruleId === "fct.forecasting-in-use" && f.status !== "not_measurable"), "forecast rows were still usable");
  });

  test("an org with no delegated approvers still has its users read", async () => {
    // As seen on a real Professional Edition org: the whole user read failed on this field.
    const sim = new Simulator({ dropFields: { User: ["DelegatedApproverId"] } });
    const result = await run(sim);
    const [users] = queries(sim, "User");
    assert.ok(users, "User was read");
    assert.doesNotMatch(users, /DelegatedApproverId/);
    assert.match(users, /Profile\.UserLicense\.Name/);
    const checkpointed = checkpoint<Sourced<unknown>>(result.dir, "users");
    assert.equal(checkpointed.status, "ok");
    assert.deepEqual(checkpointed.status === "ok" && checkpointed.missingFields, ["User.DelegatedApproverId"]);
    assert.ok(!result.output!.findings.some((f) => (f.notMeasurableReason ?? "").startsWith("Users couldn't be read")));
  });

  test("an absent required field makes the source unavailable, names the field, and spends no query", async () => {
    const sim = new Simulator({ dropFields: { LoginHistory: ["LoginTime"], ForecastingItem: ["ForecastCategoryName"] } });
    const result = await run(sim);
    assert.ok(result.output, "the run finished");
    assert.equal(queries(sim, "LoginHistory").length, 0);
    assert.equal(queries(sim, "ForecastingItem").length, 0);

    const logins = checkpoint<Sourced<unknown>>(result.dir, "logins");
    assert.equal(logins.status, "unavailable");
    assert.match(logins.status === "unavailable" ? logins.reason : "", /LoginHistory\.LoginTime isn't in this org/);
    const forecast = checkpoint<{ items: Sourced<unknown> }>(result.dir, "forecast");
    assert.match(forecast.items.status === "unavailable" ? forecast.items.reason : "", /ForecastingItem\.ForecastCategoryName/);

    for (const f of finding(result.output.findings, "sec.personal-integration-accounts")) {
      assert.equal(f.status, "not_measurable");
      assert.match(f.notMeasurableReason ?? "", /LoginHistory\.LoginTime/);
    }
  });

  test("an object the org doesn't have is unavailable without a query", () => {
    const describes = { Campaign: { status: "absent", reason: "NOT_FOUND: The requested resource does not exist" } };
    const ctx = { get: () => describes } as unknown as ExtractContext;
    const plan = planFields(ctx, FIELD_SPECS.campaign);
    assert.equal(plan.ok, false);
    assert.match(!plan.ok ? plan.reason : "", /Campaign isn't available in this org/);
  });

  test("a failed describe falls back to the full query, as before describes", () => {
    const ctx = { get: () => ({ LoginHistory: { status: "unknown", reason: "INSUFFICIENT_ACCESS: no" } }) } as unknown as ExtractContext;
    const plan = planFields(ctx, FIELD_SPECS.loginHistory);
    assert.deepEqual(plan, { ok: true, select: "UserId, LoginTime, Status, ApiType, Application", missing: [] });
  });
});

describe("the describe step", () => {
  test("resumes without describing an object twice", async () => {
    const sim = new Simulator();
    const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: sim });
    const rows = new Map<string, unknown[]>();
    const partial = {
      read: <R>(key: string) => [...((rows.get(key) ?? []) as R[])],
      append: <R>(key: string, more: R[]) => rows.set(key, [...(rows.get(key) ?? []), ...more]),
      clear: (key: string) => rows.delete(key),
    };
    // As if the run had stopped at the budget after describing Campaign.
    partial.append("field-describe", [{ Id: "Campaign", result: { status: "ok", fields: ["id", "type"], relationships: [] } }]);
    const out = await fieldDescribeExtractor.run({ client, partial } as unknown as ExtractContext);

    const described = sim.requests.map((r) => new URL(r.url).pathname.match(/sobjects\/(\w+)\/describe/)?.[1]).filter(Boolean);
    assert.deepEqual(described.sort(), ["ForecastingItem", "LoginHistory", "User"]);
    assert.deepEqual(out.Campaign, { status: "ok", fields: ["id", "type"], relationships: [] });
    assert.equal(out.LoginHistory?.status, "ok");
  });

  test("a budget stop isn't saved as a fact about the org", async () => {
    const sim = new Simulator({ used: 60_000 });
    const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: sim });
    const appended: unknown[] = [];
    const partial = { read: () => [], append: (_k: string, r: unknown[]) => appended.push(...r), clear: () => {} };
    await assert.rejects(fieldDescribeExtractor.run({ client, partial } as unknown as ExtractContext), /budget|share/i);
    assert.deepEqual(appended, []);
  });
});
