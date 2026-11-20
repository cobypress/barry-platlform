import type { EffortHours, Finding } from "../schemas/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { effortHours } from "./hours.ts";
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
  f: Pick<Finding, "title" | "summary" | "recommendation" | "subjects" | "facts">,
  pseudonyms: Pseudonymiser,
  options: { blocked?: boolean } = {},
): { effortHours?: EffortHours; fixPrompt?: string } {
  const rule = FIX_TABLE[ruleId];
  if (!rule) return {};
  const hours = effortHours(rule.hours, f.facts);
  const prompt = fixPrompt(f, rule.prompt, pseudonyms, options);
  return { ...(hours ? { effortHours: hours } : {}), ...(prompt ? { fixPrompt: prompt } : {}) };
}
