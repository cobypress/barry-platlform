import { z } from "zod";
import { INSIGHT_RULES } from "../domains/scoring.ts";
import type { DomainModule } from "../domains/types.ts";

// Which rules a run checked, and which the engine knew when it did (DECISIONS.md
// 718), kept in the run folder beside its findings and served in its digest, so
// a reader comparing runs tells a check that passed or found nothing to report
// from one the run never made, and a check since retired from one only this run
// left out. Rule ids only: no text, no figure.

/** The run folder's file: written with each analysis, read with the findings. */
export const RULES_FILE = "rules.json";

const RuleId = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,199}$/);
export const RulesRun = z.object({
  /** Every rule the analysis ran to a result, sorted, each once. */
  rulesRun: z.array(RuleId),
  /** Every served rule the engine knew at that analysis, sorted, each once. */
  rulesKnown: z.array(RuleId),
});
export type RulesRun = z.infer<typeof RulesRun>;

/** Every rule the engine serves among these modules, sorted, each once: never an insight's, which nothing serves (DECISIONS.md 703). */
export const servedRuleIds = (modules: DomainModule[]): string[] => [...new Set(modules.flatMap((m) => m.rules.map((r) => r.id)).filter((id) => !INSIGHT_RULES.has(id)))].sort();

/**
 * What a run folder keeps: the rules this analysis ran, and those the engine
 * knows, which always include them, so a run's own list is never outside its
 * engine's even when a caller gave modules of its own.
 */
export function rulesOf(rulesRun: string[], known: DomainModule[]): RulesRun {
  return RulesRun.parse({ rulesRun, rulesKnown: [...new Set([...servedRuleIds(known), ...rulesRun])].sort() });
}
