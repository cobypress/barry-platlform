import type { Evidence } from "../../../schemas/index.ts";
import type { NotMeasurableResult, RuleResult } from "../../../engine/rule.ts";
import { thinResult } from "./unmarked-helper.ts";

// Negative fixtures for the producers guard (DECISIONS.md 743): each writes a rule's reason with a plain template in a
// way the guard once missed. Never imported by the engine; the guard's own test reads them as a listed folder's.

export function laterAssignment(name: string, evidence: Evidence[]): NotMeasurableResult {
  const r: NotMeasurableResult = { outcome: "not_measurable", reason: "", evidence };
  r.reason = `${name} couldn't be read.`;
  return r;
}

export const computedKey = (name: string, evidence: Evidence[]): RuleResult => ({ outcome: "not_measurable", ["reason"]: `${name} couldn't be counted.`, evidence });

export const stringKey = (name: string, evidence: Evidence[]): RuleResult => ({ outcome: "not_measurable", "reason": `${name} couldn't be listed.`, evidence });

export const throughHelper = (n: number, evidence: Evidence[]): RuleResult => thinResult(n, evidence);
