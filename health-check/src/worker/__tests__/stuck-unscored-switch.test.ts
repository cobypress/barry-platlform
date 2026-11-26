import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { baseDeps, consoleRun, runInput, syntheticConnection } from "./helpers.ts";
import { createJobHandler } from "../worker.ts";
import { loadRun } from "../../render/render-run.ts";
import { Simulator } from "../../testing/simulator.ts";

// HC_STUCK_UNSCORED through the worker's own path (DECISIONS.md 778): version 4 alone, and version 3 winning when
// HC_STUCK_EXPECTED is on too, as run-job passes both to the engine (not the test harness's copy).

const RULE = "cur.stuck-in-stage";
async function versionOf(switches: { stuckUnscored?: boolean; stuckExpected?: boolean }): Promise<number> {
  const deps = baseDeps({ dataTransport: new Simulator() });
  await deps.tokens.save(syntheticConnection());
  Object.assign(deps, switches);
  const id = "hc-2026-10-31-00000009";
  deps.store.addRun(runInput({ run: consoleRun({ id }) }));
  const outcome = await createJobHandler(deps)({ kind: "run", runId: id }, { final: true });
  assert.equal(outcome.outcome, "done", JSON.stringify(outcome));
  return loadRun(join(deps.runsDir, id)).input.findings.find((f) => f.ruleId === RULE)!.ruleVersion;
}

describe("the switches through the worker (decision 778)", () => {
  test("HC_STUCK_UNSCORED alone runs version 4; with HC_STUCK_EXPECTED too, version 3 wins; neither, version 2", async () => {
    assert.equal(await versionOf({ stuckUnscored: true }), 4);
    assert.equal(await versionOf({ stuckUnscored: true, stuckExpected: true }), 3);
    assert.equal(await versionOf({}), 2);
  });
});
