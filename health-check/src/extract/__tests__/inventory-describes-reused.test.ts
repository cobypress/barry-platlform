import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import type { TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { Inventory } from "../../schemas/inventory.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { CORE_OBJECTS } from "../configuration.ts";
import { fillRatesExtractor, INVENTORY_FILE } from "../inventory.ts";

// KAN-77: the fill-rate step takes the core four's describes from the schema
// step of the same run, rather than reading each a second time. Fewer calls,
// the same answers: anything the schema step couldn't read is described as before.

const BARE: DomainModule = { id: "configuration", cloud: "sales", label: "Nothing else", extractors: [], rules: [] };
const readInventory = (dir: string): Inventory => Inventory.parse(JSON.parse(readFileSync(join(dir, INVENTORY_FILE), "utf8")));
const fillRows = (dir: string): unknown[] => {
  const section = readInventory(dir).fillRates;
  assert.ok("rows" in section, JSON.stringify(section));
  return section.rows;
};
const run = (transport: Simulator, extra: Partial<RunOptions> = {}) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER, inventory: true, ...extra });
const describesOf = (sim: Simulator, object: string): number => sim.requests.filter((r) => new URL(r.url).pathname.endsWith(`/sobjects/${object}/describe`)).length;
const fillCalls = (result: Awaited<ReturnType<typeof run>>): number => result.run.steps.find((s) => s.id === fillRatesExtractor.id)!.calls;

/** The simulator, refusing the first describe of one object, as Salesforce refuses a read the user can't make. */
class RefuseFirstDescribe extends Simulator {
  #object: string;
  #refused = false;
  constructor(object: string) {
    super();
    this.#object = object;
  }
  override async send(request: TransportRequest): Promise<TransportResponse> {
    if (!this.#refused && new URL(request.url).pathname.endsWith(`/sobjects/${this.#object}/describe`)) {
      this.#refused = true;
      this.requests.push(request);
      return { status: 403, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "No access" }]) };
    }
    return super.send(request);
  }
}

describe("the fill-rate step reuses the run's own describes of the core objects", () => {
  test("it describes none of the core four again: the run makes no more of their describes than it does without the inventory", async () => {
    const without = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: without, connectedUser: CONNECTED_USER });
    const withIt = new Simulator();
    const result = await run(withIt);
    for (const object of CORE_OBJECTS) assert.equal(describesOf(withIt, object), describesOf(without, object), object);
    // The three custom objects with records are still described: Invoice__c, Project__c, and Project_Line__c, whose describe fails.
    assert.equal(describesOf(withIt, "Invoice__c"), 1);
    assert.equal(fillCalls(result), 12);
  });

  test("every object's row is what the step gives when it describes each object itself", async () => {
    const own = await run(new Simulator(), { modules: [BARE] });
    const reused = await run(new Simulator());
    assert.deepEqual(fillRows(reused.dir), fillRows(own.dir));
  });

  test("an object the schema step couldn't describe is described by the step itself, and counted as before", async () => {
    const sim = new RefuseFirstDescribe("Account");
    const result = await run(sim);
    const schema = readCheckpoint<Record<string, { status: string }>>(join(result.dir, "checkpoints", "schema.json"));
    assert.equal(schema.Account!.status, "unavailable");
    assert.equal(describesOf(sim, "Account"), 2);
    const own = await run(new Simulator(), { modules: [BARE] });
    assert.deepEqual(fillRows(result.dir), fillRows(own.dir));
    assert.equal(fillCalls(result), 13);
  });

  test("a run stopped at the budget part way through the step resumes where it stopped: nothing counted twice, the same rows, the same calls in all", async () => {
    const wholeSim = new Simulator();
    const whole = await run(wholeSim);
    const before = whole.run.steps.slice(0, whole.run.steps.findIndex((s) => s.id === fillRatesExtractor.id)).reduce((n, s) => n + s.calls, 0);
    // An automatic run stops at a tenth of the daily limit, by its own count: here six calls into the step.
    const outDir = tempDir();
    const firstSim = new Simulator({ dailyLimit: (before + 6) * 10, used: 0 });
    const first = await run(firstSim, { outDir, reviewMode: "automatic" });
    const step = first.run.steps.find((s) => s.id === fillRatesExtractor.id)!;
    assert.equal(step.status, "pending");
    assert.ok(step.calls > 0 && step.calls < 12, String(step.calls));
    const counted = (sim: Simulator): string[] => sim.requests.filter((r) => r.url.endsWith("/composite/batch")).flatMap((r) => (JSON.parse(r.body ?? "{}") as { batchRequests: Array<{ url: string }> }).batchRequests.map((b) => b.url)).filter((u) => u.includes("COUNT(Id)%20n"));
    const again = new Simulator();
    const resumed = await run(again, { outDir, reviewMode: "automatic", resume: first.run.id });
    // One describe is read again: Invoice__c's, made before the stop while its counts waited for a batch, as before KAN-77.
    assert.equal(fillCalls(resumed), 13);
    assert.equal(describesOf(again, "Invoice__c"), 1);
    assert.equal(counted(firstSim).length + counted(again).length, 9);
    assert.ok(!counted(again).some((u) => counted(firstSim).includes(u)), "a count already answered was asked again");
    // The schema step's checkpoint still answers for the core four on the second delivery: only the schema step's own describe is missing.
    for (const object of CORE_OBJECTS) assert.equal(describesOf(again, object), describesOf(wholeSim, object) - 1, object);
    assert.deepEqual(fillRows(resumed.dir), fillRows(whole.dir));
  });
});

describe("a reused describe is read as the step's own would be", () => {
  test("what Salesforce can't count, from the schema step's describe, is skipped with why and never put in a query", async () => {
    const kept = new Map<string, unknown[]>();
    const sent: string[] = [];
    const budget = new ApiBudget({ stopAtShare: 1 });
    budget.observe(0, 100_000);
    const field = (name: string, type: string, extra: Partial<{ calculated: boolean; aggregatable: boolean }> = {}) => ({ name, type, calculated: false, aggregatable: true, activePicklistValues: [], ...extra });
    const accountSchema = { status: "ok", data: { name: "Account", recordTypes: [], fields: [field("Id", "id"), field("Name", "string"), field("Description", "textarea", { aggregatable: false }), field("Score__c", "double", { calculated: true })] } };
    const client = {
      budget,
      recordCounts: async () => [{ name: "Account", count: 10 }],
      describe: async () => {
        throw new Error("described again");
      },
      queryBatch: async (qs: string[]) => {
        sent.push(...qs);
        return qs.map(() => ({ ok: true, result: { records: [{ n: 10, f0: 7 }], totalSize: 1, truncated: false } }));
      },
    };
    const ctx = {
      client,
      get: (e: { id: string }) => {
        if (e.id === "record-type-use") return { access: { status: "ok", viewAllData: true, viewAllRecords: [] } };
        if (e.id === "schema") return { Account: accountSchema, Contact: accountSchema, Lead: accountSchema, Opportunity: accountSchema };
        throw new Error("not run");
      },
      connectedUser: CONNECTED_USER,
      partial: { read: (k: string) => kept.get(k) ?? [], append: (k: string, r: unknown[]) => void kept.set(k, [...(kept.get(k) ?? []), ...r]), clear: () => {} },
    };
    const out = await fillRatesExtractor.run(ctx as never);
    assert.ok(out.status === "ok");
    assert.ok(sent.length > 0 && sent.every((q) => q === "SELECT COUNT(Id) n, COUNT(Name) f0 FROM Account" || / FROM (Contact|Lead|Opportunity)$/.test(q)), sent.join("\n"));
    const account = out.data.objects.find((o) => o.object === "Account")!;
    assert.ok("skipped" in account);
    assert.deepEqual(account.skipped.map((s) => s.field), ["Id", "Description", "Score__c"]);
  });
});
