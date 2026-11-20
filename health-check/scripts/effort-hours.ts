// Writes fixtures/effort-hours.json: every rule's hours to fix, from the table in
// src/fix/table.ts (DECISIONS.md 628). These are the builder's first estimates
// and await the owner's review: he reads and changes every figure here in one
// place, then the change is made in the table and this file made again. No
// document prints them yet. Made here, never by hand: a test fails when the file
// and the table differ.
//
//   npm run effort-hours        (from health-check/)

import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { FIX_TABLE } from "../src/fix/table.ts";
import type { FixRule } from "../src/fix/types.ts";

export const EFFORT_HOURS_FILE = fileURLToPath(new URL("../fixtures/effort-hours.json", import.meta.url));
/** How the file is made again, from the health-check folder. */
export const REMAKE = "npm run effort-hours";

export const STATUS =
  "Estimates awaiting the owner's review. Hours for a fix led by a consultant who knows Salesforce, as a range in whole hours. A fixed range covers the work whatever the size of the org. A range for each unit is multiplied by the count the finding holds (the sum of the facts named, or of all its facts), rounded up to whole hours. Past the cap, a rule whose work stops growing says why and holds the range at the cap; every other rule's range covers only the first units the cap allows, and says the rest is scoped separately. Printed in no document until approved.";

type PerUnit = { facts: string[] | "all"; each: string; minutesLow: number; minutesHigh: number; capHours: number; pastTheCap: string };

export type EffortHoursFile = {
  status: string;
  rules: Record<string, { low: number; high: number; basis: string } | PerUnit>;
};

/** Each rule once, by id in code-point order, so the file is the same bytes wherever it's made. */
export function effortHoursFile(table: Readonly<Record<string, FixRule>> = FIX_TABLE): EffortHoursFile {
  const rules: EffortHoursFile["rules"] = {};
  for (const id of Object.keys(table).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))) {
    const h = table[id]!.hours;
    rules[id] = h.kind === "fixed" ? { low: h.low, high: h.high, basis: h.basis } : { facts: h.facts === "all" ? "all" : [...h.facts], each: h.one, minutesLow: h.minutes[0], minutesHigh: h.minutes[1], capHours: h.cap, pastTheCap: h.bulk ? `held at the cap: ${h.bulk}` : "covers the first units the cap allows; the rest is scoped separately" };
  }
  return { status: STATUS, rules };
}

export const effortHoursText = (table?: Readonly<Record<string, FixRule>>): string => `${JSON.stringify(effortHoursFile(table), null, 2)}\n`;

if (import.meta.main) {
  writeFileSync(EFFORT_HOURS_FILE, effortHoursText());
  console.log(`Wrote ${Object.keys(effortHoursFile().rules).length} rules to fixtures/effort-hours.json. They are estimates awaiting the owner's review.`);
}
