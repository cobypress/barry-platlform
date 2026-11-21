import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
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

  test("each rule's fix complexity is exported as the table declares it (decision 636)", () => {
    for (const [id, entry] of Object.entries(committed.rules)) {
      assert.equal(entry.fixComplexity, FIX_TABLE[id]!.complexity, id);
      assert.ok(["simple", "moderate", "complex"].includes(entry.fixComplexity), id);
    }
  });

  test("the owner's trim (decision 636): every high end, rate and cap is about 88% of the recorded one, rounded as stated, or named as too small to round down", () => {
    type Old = { low: number; high: number } | { minutesLow: number; minutesHigh: number; capHours: number };
    const before = (JSON.parse(readFileSync(fileURLToPath(new URL("../../fixtures/effort-hours-pretrim.json", import.meta.url)), "utf8")) as { rules: Record<string, Old> }).rules;
    assert.deepEqual(Object.keys(before).sort(), Object.keys(committed.rules).sort());
    const trimmed = (old: number, floor: number): number => Math.max(floor, Math.round(old * 0.88));
    const trimmedMinutes = (old: number, floor: number): number => Math.max(floor, 5 * Math.round((old * 0.88) / 5));
    const unchanged: string[] = [];
    for (const [id, now] of Object.entries(committed.rules)) {
      const old = before[id]!;
      if ("basis" in now) {
        assert.ok("high" in old && now.low === old.low, id);
        assert.ok(now.high <= old.high, id);
        assert.equal(now.high, trimmed(old.high, now.low), id);
        if (now.high === old.high) unchanged.push(id);
      } else {
        assert.ok("minutesHigh" in old && now.minutesLow === old.minutesLow, id);
        assert.ok(now.minutesHigh <= old.minutesHigh && now.capHours <= old.capHours, id);
        assert.equal(now.minutesHigh, trimmedMinutes(old.minutesHigh, now.minutesLow), id);
        assert.equal(now.capHours, trimmed(old.capHours, 1), id);
        if (now.minutesHigh === old.minutesHigh && now.capHours === old.capHours) unchanged.push(id);
      }
    }
    // Too small to round down: a high of 2, 3 or 4 hours stays where it was.
    assert.deepEqual(unchanged.sort(), UNCHANGED_BY_ROUNDING);
    assert.equal(Object.keys(committed.rules).length - unchanged.length, 80);
  });

  test("a changed figure makes the file stale", () => {
    const [first, ...rest] = Object.entries(FIX_TABLE);
    const h = first![1].hours;
    const changed = Object.fromEntries([[first![0], { ...first![1], hours: h.kind === "fixed" ? { ...h, high: h.high + 1 } : { ...h, cap: h.cap + 1 } }], ...rest]);
    assert.notEqual(effortHoursText(changed), text);
  });
});

/** The 44 rules whose figures 88% rounds back to themselves (decision 636). */
const UNCHANGED_BY_ROUNDING = [
  "act.rep-activity-spread", "act.unlinked-tasks", "act.won-vs-lost-activity", "cpq.quote-to-order", "cur.account-concentration", "cur.close-date-pushes", "cur.coverage", "cur.missing-next-steps",
  "cur.rep-concentration", "cur.stale-deals", "cur.stuck-in-stage", "dq.loss-reason-missing", "dq.open-opps-missing-amount", "dq.open-opps-no-activity", "dq.open-opps-past-close", "dq.opps-inactive-owner",
  "dq.opps-without-account", "dq.record-type-use", "fct.commit-at-risk", "fct.early-stage-commits", "fct.manager-adjustments", "lead.campaign-return", "lead.conversion-by-source", "lead.converted-no-opportunity",
  "lead.no-first-response", "lead.source-return", "mkt.campaign-influence", "mkt.unowned-prospects", "mkt.unworked-handovers", "pipe.calibration-contradicts-org", "pipe.dead-end-stage", "pipe.segment-divergence",
  "pipe.unmapped-stages", "rev.dormant-accounts", "rev.stalled-deals", "rev.win-backs", "sec.insecure-remote-sites", "svc.backlog-age", "svc.case-classification", "svc.origin-missing",
  "svc.queue-sprawl", "utl.eac-advanced-sync", "utl.package-seats", "utl.unassigned-licence-seats",
];
