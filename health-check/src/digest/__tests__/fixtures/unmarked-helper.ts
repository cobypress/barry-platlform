import type { Evidence } from "../../../schemas/index.ts";
import type { NotMeasurableResult } from "../../../engine/rule.ts";

// A negative fixture for the producers guard (DECISIONS.md 743): a helper outside the listed folders that builds a rule's
// result with a plain template. Never imported by the engine.

export const thinResult = (n: number, evidence: Evidence[]): NotMeasurableResult => ({ outcome: "not_measurable", reason: `${n} things is too few.`, evidence });
