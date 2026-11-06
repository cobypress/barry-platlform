import type { NotMeasurableResult, RuleContext } from "../../engine/rule.ts";
import { cpqSetupExtractor, type CpqPresence } from "../../extract/cpq.ts";

// What both CPQ areas share (roadmap Stage 6, DECISIONS.md 216 to 225).

/** The console's module for both CPQ areas, as "service" is for Service Cloud. */
export const CPQ_MODULE = "cpq";

/** Not measurable, with the reason, when CPQ isn't installed or we can't tell. Null when it is. */
export function presenceGap(ctx: RuleContext): NotMeasurableResult | null {
  const setup = ctx.data(cpqSetupExtractor);
  const presence: CpqPresence = setup.presence;
  if (presence.status === "installed") return null;
  return { outcome: "not_measurable", reason: presence.reason, evidence: [ctx.ev(setup.priceRules.source, { count: null })] };
}

/** A quote's discount off list price, in points, to one decimal. Null without a list price. */
export function discountOf(q: { SBQQ__ListAmount__c: number | null; SBQQ__NetAmount__c: number | null }): number | null {
  const list = q.SBQQ__ListAmount__c ?? 0;
  if (list <= 0 || q.SBQQ__NetAmount__c === null) return null;
  return Math.round(((list - q.SBQQ__NetAmount__c) / list) * 1000) / 10;
}

export const listed = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);
