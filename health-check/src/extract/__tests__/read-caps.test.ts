import { test } from "node:test";
import assert from "node:assert/strict";
import { buildScaleOrg, FIVE_HUNDRED_USERS } from "../../testing/scale-org.ts";
import { FORECAST_MAX_ROWS } from "../forecast.ts";
import { RECORD_READ_CAP } from "../opportunities.ts";
import { PARDOT_LEAD_ROWS } from "../pardot.ts";
import { CASE_ROW_CAP } from "../service.ts";

// A 500-user customer is read in full (DECISIONS.md 381 to 385): every capped
// read holds the scale test's 500-user org. At or over its cap a read is a
// part, so each list must be under it.

const org = buildScaleOrg(FIVE_HUNDRED_USERS);
const cases = org.supportCases.cases;

const lists: Array<[string, number, number]> = [
  ["tasks", org.activity.tasks.length, RECORD_READ_CAP],
  ["events", org.activity.events.length, RECORD_READ_CAP],
  ["emails", org.activity.emails.length, RECORD_READ_CAP],
  ["leads", org.leadFunnel.leads.length, RECORD_READ_CAP],
  ["lead status changes", org.leadFunnel.statusChanges.length, RECORD_READ_CAP],
  ["campaign members", org.leadFunnel.members.length, RECORD_READ_CAP],
  ["forecast facts", org.forecastFacts.length, FORECAST_MAX_ROWS],
  ["forecast rows", org.forecastItems.length, FORECAST_MAX_ROWS],
  ["open cases", cases.filter((c) => !c.IsClosed).length, CASE_ROW_CAP],
  ["closed cases", cases.filter((c) => c.IsClosed).length, CASE_ROW_CAP],
  ["case status changes", org.supportCases.history.length, CASE_ROW_CAP],
  ["case milestones", org.supportCases.milestones.length, CASE_ROW_CAP],
  ["case replies", org.supportCases.replies.length, CASE_ROW_CAP],
  ["routed work", org.supportCases.work.length, CASE_ROW_CAP],
  ["open synced leads", org.pardotOpenLeads.length, PARDOT_LEAD_ROWS],
];

for (const [what, rows, cap] of lists) {
  test(`a 500-user org's ${what} are read in full: ${rows.toLocaleString("en-GB")} under a cap of ${cap.toLocaleString("en-GB")}`, () => {
    assert.ok(rows < cap, `${what}: ${rows} against ${cap}`);
  });
}

test("a run resumed across the cap raise re-reads its capped steps: each extractor's version moved with its cap (DECISIONS.md 399)", async () => {
  // A step is reused only when its checkpoint's version matches (engine/run.ts). These carried the
  // versions below before 381 and 383 to 385 raised their caps, so a run resumed across that deploy
  // reused reads capped at the old limits. A cap raised again needs its extractor's version moved again.
  const { activityExtractor } = await import("../activity.ts");
  const { leadsExtractor } = await import("../leads.ts");
  const { forecastExtractor } = await import("../forecast.ts");
  const { serviceCasesExtractor, serviceActivityExtractor } = await import("../service.ts");
  const { pardotLeadsExtractor } = await import("../pardot.ts");
  const { wonAccountsExtractor } = await import("../opportunities.ts");
  const before: Array<[{ id: string; version: number }, number]> = [
    [activityExtractor, 1],
    [leadsExtractor, 2],
    [wonAccountsExtractor, 1],
    [forecastExtractor, 2],
    [serviceCasesExtractor, 1],
    [serviceActivityExtractor, 1],
    [pardotLeadsExtractor, 1],
  ];
  for (const [extractor, version] of before) assert.ok(extractor.version > version, `${extractor.id} is still version ${extractor.version}`);
});
