import { test } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { sfId, userId } from "../../testing/synthetic-org.ts";
import { SALES_CLOUD_MODULES } from "../index.ts";

// The scale test (docs/overnight/night-3/scale.md) found checks that grouped
// records by copying each group's list on every row, so their time grew with
// the square of a group's size. A big org's reads, still under their caps,
// took minutes. These run the checks on reads that size, with a time limit.

const recorded = loadExtracts((await runFromRecording({ answers: loadAnswers() })).dir);
const ROWS = 150_000;

/** The recorded run, with its events and leads made up to ROWS each, spread over the synthetic org's reps. */
function bigOrg(): Map<string, unknown> {
  const out = clone(recorded);
  const reps = [1, 2, 3, 14].map(userId);
  const at = (i: number): string => new Date(Date.parse("2025-11-03T09:00:00Z") + (i % 360) * 86_400_000).toISOString().replace("Z", "+0000");
  const activity = out.get("activity") as { events: { data: unknown[] } };
  activity.events.data = Array.from({ length: ROWS }, (_, i) => ({ Id: sfId("00U", 2_000_000 + i), OwnerId: reps[i % 4], WhoId: null, WhatId: i % 3 ? sfId("006", 1 + (i % 200)) : null, StartDateTime: at(i) }));
  const leads = out.get("leads") as { leads: { data: unknown[] } };
  leads.leads.data = Array.from({ length: ROWS }, (_, i) => ({ Id: sfId("00Q", 2_000_000 + i), OwnerId: reps[i % 4], LeadSource: ["Web", "Event", "Referral", "Partner"][i % 4], CreatedDate: at(i), IsConverted: i % 4 === 0, ConvertedDate: i % 4 === 0 ? at(i).slice(0, 10) : null, ConvertedOpportunityId: null, ConvertedContactId: null }));
  return out;
}

test("the activity and lead checks group a big org's records in one pass, within seconds", { timeout: 30_000 }, () => {
  const started = performance.now();
  const findings = analyseExtracts(bigOrg(), { modules: SALES_CLOUD_MODULES }).findings;
  const seconds = (performance.now() - started) / 1000;
  assert.ok(seconds < 20, `${seconds.toFixed(1)}s`);
  // They ran on the records, rather than stopping for want of them.
  for (const id of ["act.event-link-collapse", "lead.conversion-by-source"]) assert.ok(findings.some((f) => f.ruleId === id), id);
});
