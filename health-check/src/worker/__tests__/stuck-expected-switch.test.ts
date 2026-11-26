import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { baseDeps, consoleRun, runInput, syntheticConnection } from "./helpers.ts";
import { createJobHandler } from "../worker.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords } from "../../testing/simulator.ts";
import { steadyPipeline } from "../../testing/steady-pipeline.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildReport } from "../../narrative/sections.ts";

// HC_STUCK_EXPECTED turned on and off between runs, through the worker (DECISIONS.md 777, the review's chain.ts): on a
// steady long-tailed org version 2 raises cur.stuck-in-stage and version 3 passes, with nothing in the org changed,
// so no link may call it fixed or new. A re-check against a check, then a pulse against the re-check.

const RULE = "cur.stuck-in-stage";
type Row = Record<string, unknown>;
// The steady pipeline as the simulator serves Opportunity rows (record type and loss reason as Salesforce names them).
const pipeline = (): OrgRecords["pipeline"] => {
  const p = steadyPipeline("bimodal 80/20", 300, 7, new Date(AS_OF));
  return {
    opportunities: (p.opportunities as Row[]).map(({ RecordType, LossReason, CurrencyIsoCode, ...o }) => ({ ...o, RecordType: { DeveloperName: RecordType }, Loss_Reason__c: LossReason, AccountId: "001000000000002AAA", NextStep: null })),
    history: p.history,
  } as unknown as OrgRecords["pipeline"];
};

async function chain(schedule: boolean[]) {
  const records = { ...SYNTHETIC_RECORDS, pipeline: pipeline() };
  const deps = baseDeps({ dataTransport: new Simulator({ records }) });
  await deps.tokens.save(syntheticConnection());
  const ids = ["hc-2026-08-31-00000001", "hc-2026-09-30-00000002", "hc-2026-10-31-00000003"];
  const story: Array<{ stuck: string[]; where: string[]; newCount: number | null }> = [];
  for (let i = 0; i < 3; i++) {
    deps.stuckExpected = schedule[i];
    deps.dataTransport = new Simulator({ records });
    const run = consoleRun({ id: ids[i]!, product: i === 2 ? "pulse" : "barometer", baselineRunId: i === 0 ? null : ids[i - 1]! });
    deps.store.addRun(runInput({ run }));
    const outcome = await createJobHandler(deps)({ kind: "run", runId: ids[i]! }, { final: true });
    assert.equal(outcome.outcome, "done", JSON.stringify(outcome));
    const { input } = loadRun(join(deps.runsDir, ids[i]!));
    const progress = buildReport(input).progress;
    const where = progress ? Object.entries(progress).filter(([, v]) => Array.isArray(v) && v.some((c: { ruleId?: string }) => c.ruleId === RULE)).map(([k]) => k) : [];
    const pulse = deps.store.pulseResults.get(ids[i]!)?.result;
    story.push({
      stuck: input.findings.filter((f) => f.ruleId === RULE).map((f) => `${f.status}@${f.ruleVersion}`),
      where,
      newCount: pulse ? pulse.newIssues.filter((x: { title?: string }) => /stage/.test(x.title ?? "")).length : null,
    });
  }
  return story;
}

describe("the switch on and off between runs, through the worker (decision 777)", () => {
  test("off, on, off: version 2 raises, version 3 passes, version 2 raises again; never fixed, never new", async () => {
    const s = await chain([false, true, false]);
    assert.deepEqual(s.map((x) => x.stuck), [["issue@2"], ["passed@3"], ["issue@2"]]);
    assert.deepEqual(s[1]!.where, ["ruleChanged"], "a pass only because the check changed");
    assert.deepEqual(s[2]!.where, ["remaining"], "still open, never new");
    assert.equal(s[2]!.newCount, 0, "the pulse lists no new issue for it");
  });

  test("on, off, on: version 3 passes, version 2 raises, version 3 passes; never new, never fixed", async () => {
    const s = await chain([true, false, true]);
    assert.deepEqual(s.map((x) => x.stuck), [["passed@3"], ["issue@2"], ["passed@3"]]);
    assert.deepEqual(s[1]!.where, ["changedBack"], "an issue only because the check changed back: open, never new, nothing said");
    assert.deepEqual(s[2]!.where, ["ruleChanged"], "and its pass under 3 again is no fix");
    assert.equal(s[2]!.newCount, 0);
  });
});
