import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, modulesForRun } from "../../domains/index.ts";
import { modulesFor } from "../../domains/scope.ts";
import type { DomainModule } from "../../domains/types.ts";
import { recordCountsExtractor } from "../../extract/configuration.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { Run, type RunScope } from "../../schemas/index.ts";
import { loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";

// An extractor that reads another's output needs that one listed before it in
// every module that lists it: a run of one module alone has nothing else to
// bring it in, and ctx.get throws "must run first".

const RUN_ID = "hc-alone";

/** The run as its run.json has it, whether or not the run got to the end. */
async function runAlone(modules: DomainModule[], options: { scope?: RunScope; transport?: Transport } = {}): Promise<{ run: Run; dir: string; error: Error | null }> {
  const outDir = tempDir();
  let error: Error | null = null;
  try {
    await runFromRecording({ modules, answers: loadAnswers(), outDir, runId: RUN_ID, ...(options.scope ? { scope: options.scope } : {}), ...(options.transport ? { transport: options.transport } : {}) });
  } catch (err) {
    error = err as Error;
  }
  const dir = join(outDir, RUN_ID);
  return { run: Run.parse(JSON.parse(readFileSync(join(dir, "run.json"), "utf8"))), dir, error };
}

const unfinished = (run: Run): string => JSON.stringify(run.steps.filter((s) => s.status !== "done"));

describe("a run of one module alone", () => {
  test("integrations alone runs, though its documentation read needs the record counts", async () => {
    const modules = modulesForRun(["integrations"]);
    assert.deepEqual(modules.map((m) => m.id), ["integrations"]);
    const { run, error } = await runAlone(modules);
    assert.equal(error, null);
    assert.ok(run.steps.every((s) => s.status === "done"), unfinished(run));
  });

  test("integrations alone runs when the record counts can't be read, and the documentation falls back to the custom objects", async () => {
    // The fallback reads other objects than the recording holds, so the simulator answers.
    const simulator = new Simulator();
    const transport: Transport = {
      send: (request) =>
        new URL(request.url).pathname.endsWith("/limits/recordCount")
          ? Promise.resolve({ status: 500, text: JSON.stringify([{ errorCode: "UNKNOWN_EXCEPTION", message: "Not today" }]), header: () => null })
          : simulator.send(request),
    };
    const { run, dir, error } = await runAlone(modulesForRun(["integrations"]), { transport });
    assert.equal(error, null);
    assert.ok(run.steps.every((s) => s.status === "done"), unfinished(run));
    const counts = loadExtracts(dir).get(recordCountsExtractor.id) as { status: string };
    assert.equal(counts.status, "unavailable", "the documentation read had no record counts to go on");
  });

  for (const module of ALL_MODULES) {
    test(`${module.id} alone reads no step it hasn't listed first`, async () => {
      const { run, error } = await runAlone([module]);
      assert.doesNotMatch(error?.message ?? "", /must run first/);
      assert.ok(run.steps.every((s) => s.status === "done"), `${module.id}: ${unfinished(run)}`);
    });

    // Utilisation and adoption gate their metrics on the opportunities read, so they list it (DECISIONS.md 426).
    test(`${module.id} alone runs to the end`, async () => {
      const { run, error } = await runAlone([module]);
      assert.equal(error, null);
      assert.notEqual(run.status, "failed");
    });
  }

  // A set-up review takes the record reads out, so a step that reads one of them would stop there.
  for (const module of modulesFor("essentials", ALL_MODULES)) {
    test(`${module.id} alone, in a set-up review, runs to the end`, async () => {
      const { run, error } = await runAlone(ALL_MODULES.filter((m) => m.id === module.id), { scope: "essentials" });
      assert.equal(error, null);
      assert.ok(run.steps.every((s) => s.status === "done"), `${module.id}: ${unfinished(run)}`);
    });
  }
});
