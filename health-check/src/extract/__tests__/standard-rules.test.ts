import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { automationModule } from "../../domains/automation/index.ts";
import { FilePartialStore, runHealthCheck } from "../../engine/run.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { ApiBudget, BudgetStopError } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { FURTHER_STANDARD_OBJECTS, metadataExtractor, RETRIEVED_STANDARD_OBJECTS } from "../automation.ts";
import { recordCountsExtractor } from "../configuration.ts";
import type { MetadataBundle } from "../metadata-parse.ts";
import type { ExtractContext, Extractor, Sourced } from "../types.ts";

// The retrieve also reads the validation rules on more standard objects, named
// only where the org's record counts list them, with one more retrieve of the
// four alone if that fails (DECISIONS.md 460).

type Counts = Sourced<Array<{ name: string; count: number }>>;
const COUNTS_SOURCE = { kind: "rest", path: "limits/recordCount" } as const;
const listing = (...names: string[]): Counts => ({ status: "ok", data: names.map((name) => ({ name, count: 3 })), source: COUNTS_SOURCE, truncated: false });
const UNREAD: Counts = { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: COUNTS_SOURCE };

async function extract(counts: Counts, options: SimulatorOptions = {}, sim = new Simulator(options), partial: ExtractContext["partial"] = { read: () => [], append: () => {}, clear: () => {} }): Promise<{ out: Sourced<MetadataBundle>; retrieves: string[][] }> {
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: sim, sleep: async () => {} });
  const get = <T>(extractor: Extractor<T>): T => {
    if (extractor.id !== recordCountsExtractor.id) throw new Error(`Extractor "${extractor.id}" must run first`);
    return counts as T;
  };
  const out = await metadataExtractor.run({ client, partial, get } as unknown as ExtractContext);
  return { out, retrieves: retrieved(sim) };
}

/** The CustomObject members a retrieve's body names. Assignment rules name Case too, so the whole body won't do. */
function customObjects(body: string): string[] {
  const objects = [...body.matchAll(/<met:types>(.*?)<met:name>([^<]+)<\/met:name><\/met:types>/g)].find((t) => t[2] === "CustomObject")![1]!;
  return [...objects.matchAll(/<met:members>([^<]+)<\/met:members>/g)].map((m) => m[1]!);
}

/** The CustomObject members each retrieve named, in the order sent. */
function retrieved(sim: Simulator): string[][] {
  return sim.requests
    .map((r) => r.body ?? "")
    .filter((body) => body.includes("<met:retrieve>"))
    .map(customObjects);
}

const ok = (out: Sourced<MetadataBundle>): MetadataBundle => (out.status === "ok" ? out.data : assert.fail(`unavailable: ${out.status === "unavailable" ? out.reason : ""}`));
const FOUR = ["*", ...RETRIEVED_STANDARD_OBJECTS];

describe("which standard objects the retrieve names", () => {
  test("a further object the record counts list is named, and one they don't list isn't", async () => {
    const { out, retrieves } = await extract(listing("Account", "Invoice__c", "Case"));
    assert.deepEqual(retrieves, [[...FOUR, "Case"]]);
    assert.ok(!retrieves[0]!.includes("Quote"));
    const bundle = ok(out);
    assert.deepEqual(bundle.handbook!.validationRulesCover, { customObjects: "all", standardObjects: [...RETRIEVED_STANDARD_OBJECTS, "Case"] });
    assert.deepEqual(bundle.validationRules.filter((v) => v.object === "Case").map((v) => v.name), ["Reason_When_Escalated"]);
    assert.equal(bundle.retrieveFallback, undefined);
  });

  test("with no record counts, only the four", async () => {
    const { out, retrieves } = await extract(UNREAD);
    assert.deepEqual(retrieves, [FOUR]);
    const bundle = ok(out);
    assert.deepEqual(bundle.handbook!.validationRulesCover.standardObjects, [...RETRIEVED_STANDARD_OBJECTS]);
    // Not named, so not sent: nothing on Case is read.
    assert.ok(!bundle.validationRules.some((v) => v.object === "Case"));
  });

  test("further objects go after the four in the list's order, whatever order the counts give, and only by exact name", async () => {
    const { out, retrieves } = await extract(listing("User", "Quote", "case", "Case", "Order__c", "Opportunity"));
    assert.deepEqual(retrieves, [[...FOUR, "Case", "Quote", "User"]]);
    assert.deepEqual(ok(out).handbook!.validationRulesCover.standardObjects, [...RETRIEVED_STANDARD_OBJECTS, "Case", "Quote", "User"]);
  });

  test("every further object can be named, and the four are never named twice", async () => {
    const { retrieves } = await extract(listing(...RETRIEVED_STANDARD_OBJECTS, ...FURTHER_STANDARD_OBJECTS));
    assert.deepEqual(retrieves, [[...FOUR, ...FURTHER_STANDARD_OBJECTS]]);
    assert.equal(FURTHER_STANDARD_OBJECTS.length, 16);
  });
});

describe("when a retrieve naming further objects fails", () => {
  const naming = (object: string) => (body: string) => customObjects(body).includes(object);

  test("one more names only the four, the bundle is theirs, and why the first failed is kept", async () => {
    const { out, retrieves } = await extract(listing("Case"), { failRetrieve: (body) => (naming("Case")(body) ? { faultCode: "INVALID_CROSS_REFERENCE_KEY", message: "No such object" } : null) });
    assert.deepEqual(retrieves, [[...FOUR, "Case"], FOUR]);
    const bundle = ok(out);
    assert.deepEqual(bundle.handbook!.validationRulesCover, { customObjects: "all", standardObjects: [...RETRIEVED_STANDARD_OBJECTS] });
    assert.ok(!bundle.validationRules.some((v) => v.object === "Case"));
    assert.ok(bundle.flows.length > 0);
    assert.deepEqual(bundle.retrieveFallback, { named: ["Case"], reason: "INVALID_CROSS_REFERENCE_KEY: INVALID_CROSS_REFERENCE_KEY: No such object" });
  });

  test("if that fails too, the step is unavailable as before, after two retrieves", async () => {
    const failRetrieve = () => ({ faultCode: "UNKNOWN_EXCEPTION", message: "Something went wrong" });
    const { out, retrieves } = await extract(listing("Case"), { failRetrieve });
    assert.deepEqual(retrieves, [[...FOUR, "Case"], FOUR]);
    // The same as a retrieve of the four alone failing.
    const alone = await extract(UNREAD, { failRetrieve });
    assert.deepEqual(alone.retrieves, [FOUR]);
    assert.equal(out.status, "unavailable");
    assert.deepEqual(out, alone.out);
  });

  test("a retrieve of the four alone that fails isn't tried again", async () => {
    const { retrieves } = await extract(listing("Account"), { failRetrieve: () => ({ faultCode: "UNKNOWN_EXCEPTION", message: "x" }) });
    assert.deepEqual(retrieves, [FOUR]);
  });

  test("a connection refused the Metadata API goes to Tooling without a second retrieve", async () => {
    const { out, retrieves } = await extract(listing("Case"), { readOnly: true });
    assert.equal(retrieves.length, 1);
    assert.equal(ok(out).route, "tooling");
  });

  test("a run stopped at the budget during the retrieve of the four resumes with that retrieve, never the one that failed", async () => {
    const outDir = tempDir();
    const options = { provider: syntheticProvider, calibration: loadCalibration(), modules: [automationModule], outDir, now: new Date(AS_OF), log: () => {} };
    const wider = (body: string) => body.includes("<met:name>CustomObject</met:name>") && customObjects(body).includes("Case");
    const four = (body: string) => body.includes("<met:name>CustomObject</met:name>") && !customObjects(body).includes("Case");
    // The wider retrieve fails, and the budget runs out during the one that follows.
    const first = new Simulator({ failRetrieve: (body) => (wider(body) ? { faultCode: "UNKNOWN_EXCEPTION", message: "Too many objects" } : four(body) ? { faultCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." } : null) });
    const stopped = await runHealthCheck({ ...options, transport: first });
    assert.equal(stopped.run.status, "stopped_budget");
    assert.deepEqual(retrieved(first), [[...FOUR, "Case"], FOUR]);
    // What the stopped run keeps: the objects named and Salesforce's error, nothing else.
    const kept = readFileSync(join(stopped.dir, "checkpoints", "metadata-retrieve-fallback.partial.jsonl"), "utf8");
    assert.deepEqual(kept.trim().split("\n").map((line) => JSON.parse(line)), [{ named: ["Case"], reason: "UNKNOWN_EXCEPTION: UNKNOWN_EXCEPTION: Too many objects" }]);

    // Resumed, the wider retrieve would fail again if it were made.
    const second = new Simulator({ failRetrieve: (body) => (wider(body) ? { faultCode: "UNKNOWN_EXCEPTION", message: "Too many objects" } : null) });
    const resumed = await runHealthCheck({ ...options, transport: second, resume: stopped.run.id });
    assert.ok(resumed.output, "the run finished");
    assert.deepEqual(retrieved(second), [FOUR]);
    const out = JSON.parse(readFileSync(join(resumed.dir, "checkpoints", "metadata.json"), "utf8")) as Sourced<MetadataBundle>;
    const bundle = ok(out);
    assert.deepEqual(bundle.handbook!.validationRulesCover.standardObjects, [...RETRIEVED_STANDARD_OBJECTS]);
    assert.deepEqual(bundle.retrieveFallback, { named: ["Case"], reason: "UNKNOWN_EXCEPTION: UNKNOWN_EXCEPTION: Too many objects" });

    // Saved with the step, the record is cleared, so the step run again names the further objects again.
    const checkpoints = join(resumed.dir, "checkpoints");
    assert.ok(!existsSync(join(checkpoints, "metadata-retrieve-fallback.partial.jsonl")));
    const again = await extract(listing("Case"), {}, undefined, new FilePartialStore(checkpoints));
    assert.deepEqual(again.retrieves, [[...FOUR, "Case"]]);
  });

  test("a stop at the API budget isn't a failed retrieve: nothing more is asked", async () => {
    const sim = new Simulator({ failRetrieve: () => ({ faultCode: "REQUEST_LIMIT_EXCEEDED", message: "TotalRequests Limit exceeded." }) });
    await assert.rejects(extract(listing("Case"), {}, sim), BudgetStopError);
    assert.equal(retrieved(sim).length, 1);
  });
});
