import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { Finding } from "../../schemas/index.ts";
import { RULE_CHANGES_FILE } from "../../progress/compare.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { Simulator } from "../../testing/simulator.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, runInput, syntheticConnection } from "./helpers.ts";

// The worker keeps what a comparison set apart in the run folder (DECISIONS.md 732), on
// the pulse path and the check path alike, so the next run's comparison goes on from it:
// an issue raised under version 2 of int.api-usage that passes at 88% under version 3
// stands apart and is in no count; raised again at 96% on the next run, it is still open,
// never new. Without the file the second run would call it new.

const RULE = "int.api-usage";
const BASELINE = "hc-2026-09-30-00000001";
const FIRST = "hc-2026-10-31-00000002";
const SECOND = "hc-2026-11-30-00000003";
const last = { final: true };
/** 88% of the background-job allowance: an issue under 80% (versions 1 and 2), a pass under 95% (version 3). */
const AT_88 = { Max: 250_000, Remaining: 30_000 };

/** The baseline's int.api-usage issue as version 2 raised it. */
function raisedUnderVersion2(dir: string): void {
  const file = join(dir, "findings.json");
  const findings = JSON.parse(readFileSync(file, "utf8")) as Finding[];
  assert.ok(findings.some((f) => f.ruleId === RULE && f.status === "issue"), "the synthetic org raises it (96%)");
  writeFileSync(file, JSON.stringify(findings.map((f) => (f.ruleId === RULE ? { ...f, ruleVersion: 2 } : f))));
}

async function chain(product: "pulse" | "barometer") {
  const deps = baseDeps({ dataTransport: new Simulator() });
  await deps.tokens.save(syntheticConnection());
  const handle = createJobHandler(deps);
  deps.store.addRun(runInput({ run: consoleRun({ id: BASELINE }) }));
  assert.equal((await handle({ kind: "run", runId: BASELINE }, last)).outcome, "done");
  raisedUnderVersion2(join(deps.runsDir, BASELINE));
  // A month later the org reads 88%: a pass only the loosened rule allows.
  deps.dataTransport = new Simulator({ asyncApex: AT_88 });
  deps.store.addRun(runInput({ run: consoleRun({ id: FIRST, product, baselineRunId: BASELINE }) }));
  await handle({ kind: "run", runId: FIRST }, last);
  // And a month after that, 96% again.
  deps.dataTransport = new Simulator();
  deps.store.addRun(runInput({ run: consoleRun({ id: SECOND, product, baselineRunId: FIRST }) }));
  await handle({ kind: "run", runId: SECOND }, last);
  return deps;
}

describe("the worker keeps an issue apart for the next comparison (decision 732)", async () => {
  test("pulse path: the file is written, the issue is apart and in no count, and re-raised next month it is still open, never new", async () => {
    const deps = await chain("pulse");
    const file = join(deps.runsDir, FIRST, RULE_CHANGES_FILE);
    assert.ok(existsSync(file), "the first pulse keeps the file");
    assert.deepEqual(JSON.parse(readFileSync(file, "utf8")), { carried: { [RULE]: 2 } });
    const first = deps.store.pulseResults.get(FIRST)!.result;
    const firstProgress = buildReport(loadRun(join(deps.runsDir, FIRST)).input).progress!;
    assert.deepEqual(firstProgress.ruleChanged?.map((c) => c.ruleId), [RULE]);
    assert.equal(first.counts.fixed, 0, "not fixed");
    assert.equal(first.counts.cantTell, 0);
    assert.ok(!first.fixed.some((f) => f.title.includes("allowance")));
    const second = deps.store.pulseResults.get(SECOND)!.result;
    assert.equal(second.counts.new, 0, "raised again: never new");
    assert.equal(second.counts.stillOpen, first.counts.stillOpen! + 1, "still open, with the rest");
    assert.ok(!existsSync(join(deps.runsDir, SECOND, RULE_CHANGES_FILE)), "nothing left apart");
  });

  test("check path (a re-check): the same, through the run's own report", async () => {
    const deps = await chain("barometer");
    const file = join(deps.runsDir, FIRST, RULE_CHANGES_FILE);
    assert.ok(existsSync(file), "the re-check keeps the file");
    assert.deepEqual(JSON.parse(readFileSync(file, "utf8")), { carried: { [RULE]: 2 } });
    const first = buildReport(loadRun(join(deps.runsDir, FIRST)).input).progress!;
    assert.deepEqual(first.ruleChanged?.map((c) => c.ruleId), [RULE]);
    assert.ok(![...first.fixed, ...first.remaining, ...first.added, ...first.unknown].some((c) => c.ruleId === RULE));
    const second = buildReport(loadRun(join(deps.runsDir, SECOND)).input).progress!;
    assert.ok(second.remaining.some((c) => c.ruleId === RULE), "still open");
    assert.ok(!second.added.some((c) => c.ruleId === RULE), "never new");
    assert.equal(second.added.length, 0);
  });
});
