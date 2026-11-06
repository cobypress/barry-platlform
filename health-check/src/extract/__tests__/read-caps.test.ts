import { test } from "node:test";
import assert from "node:assert/strict";
import { FIVE_HUNDRED_USERS } from "../../testing/scale-org.ts";
import { RECORD_READ_CAP } from "../opportunities.ts";

// A 500-user customer's activity and leads are read in full (DECISIONS.md 381):
// the capped reads hold the scale test's 500-user org. At or over the cap a
// read is a part, so each must be under it.

test("the capped reads hold a 500-user org's year of tasks and events, and its leads, in full", () => {
  for (const [what, rows] of [["tasks", FIVE_HUNDRED_USERS.tasks], ["events", FIVE_HUNDRED_USERS.events], ["leads", FIVE_HUNDRED_USERS.leads]] as const) {
    assert.ok(rows < RECORD_READ_CAP, `${what}: ${rows} against a cap of ${RECORD_READ_CAP}`);
  }
});
