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

export function effortHours(rule: HoursRule, facts: Readonly<Record<string, number>>): EffortHours | null {
  if (rule.kind === "fixed") return { low: rule.low, high: rule.high, basis: `${rule.basis}.` };
  const count = facts[rule.fact];
  if (count === undefined || !Number.isInteger(count) || count < 1) return null;
  const low = Math.max(1, Math.ceil((count * rule.minutes[0]) / 60));
  const high = Math.max(low, Math.ceil((count * rule.minutes[1]) / 60));
  const capped = high > rule.cap;
  const unit = count === 1 ? rule.one : rule.many;
  const basis = `About ${durationText(rule.minutes)} for each ${rule.one}, for ${count} ${unit}${capped ? `, held at ${rule.cap} hours at most: ${rule.capReason}` : ""}.`;
  return { low: Math.min(low, rule.cap), high: capped ? rule.cap : high, basis };
}
