import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { FIX_TABLE } from "../../src/fix/table.ts";
import { EFFORT_HOURS_FILE, REMAKE, STATUS, effortHoursFile, effortHoursText, type EffortHoursFile } from "../effort-hours.ts";

// The hours table for the owner to review (DECISIONS.md 628): made from
// src/fix/table.ts by `npm run effort-hours` and committed. Read here as parsed
// from disk, as he reads it.

const text = readFileSync(EFFORT_HOURS_FILE, "utf8");
const committed = JSON.parse(text) as EffortHoursFile;
const STALE = `fixtures/effort-hours.json is out of date. Make it again from health-check/ with \`${REMAKE}\``;

describe("the hours table exported for the owner's review (decision 628)", () => {
  test("made again from the table, it is the same bytes", () => {
    const moved = Object.keys(FIX_TABLE).filter((id) => JSON.stringify(committed.rules[id]) !== JSON.stringify(effortHoursFile().rules[id]));
    assert.ok(effortHoursText() === text, `${STALE}. What moved: ${moved.slice(0, 20).join(", ") || "the order, spacing or status"}`);
  });

  test("says plainly that the figures are estimates awaiting the owner's review, and holds every rule in the table once", () => {
    assert.equal(committed.status, STATUS);
    assert.match(committed.status, /^Estimates awaiting the owner's review\./);
    assert.deepEqual(Object.keys(committed.rules).sort(), Object.keys(FIX_TABLE).sort());
  });

  test("every figure is a whole number of hours or minutes, a range runs low to high, and every unit rate has a cap and its reason", () => {
    for (const [id, entry] of Object.entries(committed.rules)) {
      if ("basis" in entry) {
        assert.ok(Number.isInteger(entry.low) && entry.low >= 1 && Number.isInteger(entry.high) && entry.high >= entry.low, id);
        assert.ok(entry.basis.trim().length > 10, id);
      } else {
        assert.ok(Number.isInteger(entry.minutesLow) && entry.minutesLow >= 1 && Number.isInteger(entry.minutesHigh) && entry.minutesHigh >= entry.minutesLow, id);
        assert.ok(Number.isInteger(entry.capHours) && entry.capHours >= 1, id);
        assert.ok(entry.pastTheCap.trim().length > 10 && entry.facts.length > 0 && entry.each.length > 0, id);
      }
    }
  });

  test("a changed figure makes the file stale", () => {
    const [first, ...rest] = Object.entries(FIX_TABLE);
    const h = first![1].hours;
    const changed = Object.fromEntries([[first![0], { ...first![1], hours: h.kind === "fixed" ? { ...h, high: h.high + 1 } : { ...h, cap: h.cap + 1 } }], ...rest]);
    assert.notEqual(effortHoursText(changed), text);
  });
});
