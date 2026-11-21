import type { EffortHours, Finding, FixComplexity, TimeBand } from "../schemas/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { effortHours, timeBandOf } from "./hours.ts";
import { fixPrompt } from "./prompt.ts";
import { FIX_TABLE } from "./table.ts";

export { FIX_TABLE, NEVER_RAISES } from "./table.ts";

/**
 * What an issue gains from its rule's fix declaration (DECISIONS.md 628): hours
 * to fix and a fix prompt, each only where the rule declares it and the finding
 * holds what it needs. Data only, for the portal and the connector.
 */
export function fixFields(
  ruleId: string,
  f: Pick<Finding, "title" | "summary" | "recommendation" | "recommendationType" | "subjects" | "facts">,
  pseudonyms: Pseudonymiser,
): { effortHours?: EffortHours; timeBand?: TimeBand; fixComplexity?: FixComplexity; fixPrompt?: string } {
  const rule = FIX_TABLE[ruleId];
  if (!rule) return {};
  const hours = effortHours(rule.hours, f.facts);
  const prompt = fixPrompt(f, rule.prompt, pseudonyms);
  // The band follows the hours, so it's present exactly when they are (DECISIONS.md 636).
  return { ...(hours ? { effortHours: hours, timeBand: timeBandOf(hours) } : {}), fixComplexity: rule.complexity, ...(prompt ? { fixPrompt: prompt } : {}) };
}
