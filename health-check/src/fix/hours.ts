import type { EffortHours } from "../schemas/index.ts";
import type { HoursRule } from "./types.ts";

// Hours to fix one finding, from its rule's declaration and a count the finding
// already holds (DECISIONS.md 628). Never from a figure the finding doesn't hold:
// with no such count there's no range.

/** "15 to 30 minutes", "an hour", "1 to 2 hours", "2 to 4 hours". */
export function durationText([low, high]: readonly [number, number]): string {
  const hours = low % 60 === 0 && high % 60 === 0;
  if (!hours) return low === high ? `${low} minutes` : `${low} to ${high} minutes`;
  const [a, b] = [low / 60, high / 60];
  if (a === b) return a === 1 ? "an hour" : `${a} hours`;
  return `${a} to ${b} hours`;
}

/** The count a per-unit declaration multiplies: the sum of the facts it names that the finding holds, or null. */
export function countOf(rule: Extract<HoursRule, { kind: "each" }>, facts: Readonly<Record<string, number>>): number | null {
  const keys = rule.facts === "all" ? Object.keys(facts) : rule.facts.filter((k) => Object.hasOwn(facts, k));
  if (keys.length === 0) return null;
  const values = keys.map((k) => facts[k]!);
  if (values.some((v) => !Number.isInteger(v) || v < 0)) return null;
  const count = values.reduce((a, b) => a + b, 0);
  return count >= 1 ? count : null;
}

const hoursFor = (units: number, minutes: number): number => Math.ceil((units * minutes) / 60);

export function effortHours(rule: HoursRule, facts: Readonly<Record<string, number>>): EffortHours | null {
  if (rule.kind === "fixed") return { low: rule.low, high: rule.high, basis: `${rule.basis}.` };
  const count = countOf(rule, facts);
  if (count === null) return null;
  const each = `About ${durationText(rule.minutes)} for each ${rule.one}, for ${count} ${count === 1 ? rule.one : rule.many}`;
  const low = Math.max(1, hoursFor(count, rule.minutes[0]));
  const high = Math.max(low, hoursFor(count, rule.minutes[1]));
  if (high <= rule.cap) return { low, high, basis: `${each}.` };
  if (rule.bulk) return { low: Math.min(low, rule.cap), high: rule.cap, basis: `${each}, held at ${rule.cap} hours at most: ${rule.bulk}.` };
  // The work grows with every unit, so the range says how many it covers and claims nothing for the rest.
  const covered = Math.max(1, Math.floor((rule.cap * 60) / rule.minutes[1]));
  const coveredLow = Math.max(1, hoursFor(covered, rule.minutes[0]));
  return {
    low: coveredLow,
    high: Math.max(coveredLow, hoursFor(covered, rule.minutes[1])),
    basis: `${each}. This range covers the first ${covered} ${covered === 1 ? rule.one : rule.many}; the other ${count - covered} are scoped separately.`,
  };
}
