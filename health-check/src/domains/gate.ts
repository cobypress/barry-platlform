import { opportunitiesExtractor } from "../extract/opportunities.ts";
import type { RuleContext } from "../engine/rule.ts";

/**
 * DECISIONS 37: no metric is published when Opportunity can't be read. A set-up
 * review never reads it, by design, so the gate is only for complete runs.
 * Returns the reason a metric isn't published, or null.
 */
export function opportunitiesGate(ctx: Pick<RuleContext, "data" | "scope">): string | null {
  if (ctx.scope === "essentials") return null;
  const { opportunities } = ctx.data(opportunitiesExtractor);
  return opportunities.status === "ok" ? null : `Opportunities couldn't be read, and no metric is published without them: ${opportunities.reason}`;
}
