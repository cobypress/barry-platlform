import { test } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";

// Three issues about open deals that have gone quiet read almost alike, so each
// is titled by its own test (night 3 reading pass, item 16).

const { dir } = await runFromRecording();
const findings = analyseExtracts(loadExtracts(dir)).findings.filter((f) => f.status === "issue");
const titleOf = (ruleId: string): string => findings.find((f) => f.ruleId === ruleId)!.title;

test("each quiet-deal issue is titled by what it tests, with its own period", () => {
  assert.equal(titleOf("dq.open-opps-no-activity"), "Open deals with no activity in 90 days");
  assert.equal(titleOf("cur.stale-deals"), "Open deals with no stage change or activity in 30 days");
  assert.equal(titleOf("rev.stalled-deals"), "Open deals with no stage change or activity in 60 days");
});
