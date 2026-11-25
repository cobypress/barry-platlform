import type { Evidence } from "../../../schemas/index.ts";
import type { NotMeasurableResult } from "../../../engine/rule.ts";

// A negative fixture for the producers guard (DECISIONS.md 743): a helper outside the listed folders that builds a rule's
// result with a plain template. Never imported by the engine.

export const thinResult = (n: number, evidence: Evidence[]): NotMeasurableResult => ({ outcome: "not_measurable", reason: `${n} things is too few.`, evidence });

/** A result's parts built here, outside the listed folders, and spread into a result there. */
export const partsFor = (n: number): { reason: string } => ({ reason: `G: ${n} parts couldn't be read.` });

/** A result given its reason by Object.assign, here. */
export function assigned(n: number, evidence: Evidence[]): NotMeasurableResult {
  const r: NotMeasurableResult = { outcome: "not_measurable", reason: "", evidence };
  Object.assign(r, { reason: `H: ${n} parts couldn't be read.` });
  return r;
}
