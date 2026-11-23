import type { EffortHours, Finding, FixComplexity, TimeBand } from "../schemas/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { effortHours, timeBandOf } from "./hours.ts";
import { markedFixPrompt } from "./prompt.ts";
import { textSha256, type MarkedSentence } from "../engine/org-words.ts";
import { FIX_TABLE } from "./table.ts";

export { FIX_TABLE, NEVER_RAISES } from "./table.ts";

/**
 * What an issue gains from its rule's fix declaration (DECISIONS.md 628): hours
 * to fix and a fix prompt, each only where the rule declares it and the finding
 * holds what it needs. Data only, for the portal and the connector.
 */
export function fixFields(
  ruleId: string,
  f: Pick<Finding, "title" | "summary" | "recommendation" | "recommendationType" | "subjects" | "facts"> & Partial<Pick<Finding, "orgWords">>,
  pseudonyms: Pseudonymiser,
): { fields: { effortHours?: EffortHours; timeBand?: TimeBand; fixComplexity?: FixComplexity; fixPrompt?: string }; promptWords?: MarkedSentence } {
  const rule = FIX_TABLE[ruleId];
  if (!rule) return { fields: {} };
  const hours = effortHours(rule.hours, f.facts);
  const prompt = markedFixPrompt(f, rule.prompt, pseudonyms);
  // The band follows the hours, so it's present exactly when they are (DECISIONS.md 636).
  const fields = { ...(hours ? { effortHours: hours, timeBand: timeBandOf(hours) } : {}), fixComplexity: rule.complexity, ...(prompt ? { fixPrompt: prompt.text } : {}) };
  // The prompt's org words, kept with the hash of the text they were made for (DECISIONS.md 672). None when unsure.
  return prompt?.spans ? { fields, promptWords: { spans: prompt.spans, textSha256: textSha256(prompt.text) } } : { fields };
}
