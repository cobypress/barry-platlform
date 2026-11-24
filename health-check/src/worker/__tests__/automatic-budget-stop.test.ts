import { test } from "node:test";
import assert from "node:assert/strict";
import { createJobHandler } from "../worker.ts";
import { BUDGET_STOPPED, BUDGET_STOPPED_STEP } from "../run-job.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { Simulator } from "../../testing/simulator.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";

// An automatic run spends at most a tenth of the org's daily limit, and one
// that stops there is carried on by nothing: staff see it in its steps, in
// words true of it, until the console has a flag for it (DECISIONS.md 710).

const run = async (reviewMode: "automatic" | "consultant") => {
  const deps = baseDeps({ dataTransport: new Simulator({ dailyLimit: 1_500 }), automatic: { on: true, orgs: new Set([ORG_ID.slice(0, 15)]) } });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode }), calibration: await automaticSyntheticCalibration(), ...(reviewMode === "automatic" ? { intake: null, answers: [] } : {}) }));
  await deps.tokens.save(syntheticConnection());
  const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: false } as never);
  return { outcome, stored: deps.store.runs.get(RUN_ID)! };
};

test("an automatic run on a small org stops at a tenth of its limit, and says so in its steps", async () => {
  const { outcome, stored } = await run("automatic");
  assert.equal(outcome.outcome, "stopped_budget");
  assert.equal(stored.run.status, "stopped_budget");
  const step = stored.run.steps.find((s) => s.id === BUDGET_STOPPED_STEP);
  assert.equal(step?.error, BUDGET_STOPPED);
  assert.ok(stored.run.budget.callsMade <= 150);
});

test("a consultant-led run on the same org isn't held to it, and gets no such step", async () => {
  const { stored } = await run("consultant");
  assert.notEqual(stored.run.status, "stopped_budget");
  assert.ok(!stored.run.steps.some((s) => s.id === BUDGET_STOPPED_STEP));
});
