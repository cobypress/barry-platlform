import { notSized, plural, type RuleContext } from "../../engine/rule.ts";
import { money } from "../../render/format.ts";
import type { Value } from "../../schemas/index.ts";
import { zoneOf } from "../time-zone.ts";
import { measuredRate } from "../value.ts";
import type { Deal, PipelineData } from "./cohort.ts";
import { closedFromOpen, cohortOf, longestInStage, longestUnchanged, wasInStage, wasPastClose } from "./outcomes.ts";

// Open deals that have gone stale are still deals, and some of them still win.
// This sizes them at how often deals in the same situation have won in this
// org's own history, rebuilt from stage history. It never borrows a rate: if
// too few comparable deals have closed, the fallback is the org's own win rate
// called a ceiling, or nothing.

type Ready = Extract<PipelineData, { status: "ok" }>;

export type Situation = {
  /** Groups affected deals that share a cohort. */
  key: string;
  /** Reads after "deals that were": "open past their close date". */
  phrase: string;
  /** Whether this situation happened to a closed deal while it was open. */
  matches: (closed: Deal) => boolean;
  /**
   * Whether the overall win rate is an upper bound for deals in this situation.
   * True for stale situations, which only lower a deal's chances. False for a
   * stage, where a late stage can win more often than the average deal.
   */
  ceiling: boolean;
};

export const SITUATIONS = {
  pastClose: (ctx: Pick<RuleContext, "data">, data: Ready): Situation => {
    const zone = zoneOf(ctx);
    return { key: "past-close", phrase: "open past their close date", matches: (d) => wasPastClose(d, data.stages, zone), ceiling: true };
  },
  inStageFor: (days: number, data: Ready): Situation => ({ key: `in-stage-${days}`, phrase: `in one stage for ${days} days or more`, matches: (d) => longestInStage(d, data.stages) >= days, ceiling: true }),
  unchangedFor: (days: number, data: Ready): Situation => ({ key: `unchanged-${days}`, phrase: `open for ${days} days or more with no change to stage, amount or close date`, matches: (d) => longestUnchanged(d, data.stages) >= days, ceiling: true }),
  inStage: (stage: string): Situation => ({ key: `stage-${stage}`, phrase: `in the ${stage} stage`, matches: (d) => wasInStage(d, stage), ceiling: false }),
};

const sum = (deals: Deal[]): number => deals.reduce((total, d) => total + (d.Amount ?? 0), 0);

/**
 * Potential revenue in `affected` open deals: each deal's amount at the share of
 * comparable closed deals that still won. The raw exposure and the deals with no
 * amount are stated, and no amount is ever imputed.
 */
export function potentialRevenue(ctx: Pick<RuleContext, "calibration" | "window" | "metrics" | "data">, data: Ready, affected: Deal[], situationOf: (d: Deal) => Situation): Value {
  if (data.multiCurrency) return notSized("The org is multi-currency, so these amounts can't be added.");
  if (affected.length === 0) return notSized("There are no deals to size.");
  const priced = affected.filter((d) => (d.Amount ?? 0) > 0);
  const unpriced = affected.length - priced.length;
  if (priced.length === 0) return notSized(`None of these ${plural(affected.length, "deal")} has an amount, and we don't impute one.`);

  const { currency, minSample } = ctx.calibration;
  const period = `${ctx.window.from} to ${ctx.window.to}`;
  const closed = closedFromOpen(ctx, data);
  const groups = new Map<string, { situation: Situation; deals: Deal[] }>();
  for (const deal of priced) {
    const situation = situationOf(deal);
    const group = groups.get(situation.key) ?? { situation, deals: [] };
    group.deals.push(deal);
    groups.set(situation.key, group);
  }

  let amount = 0;
  let sized = 0;
  const lines: string[] = [];
  const fallback: Array<{ situation: Situation; deals: Deal[]; n: number }> = [];
  for (const { situation, deals } of [...groups.values()].sort((a, b) => a.situation.key.localeCompare(b.situation.key))) {
    const cohort = cohortOf(closed.filter(situation.matches), minSample);
    if (cohort.rate === null) {
      fallback.push({ situation, deals, n: cohort.n });
      continue;
    }
    amount += (sum(deals) * cohort.rate) / 100;
    sized += deals.length;
    lines.push(`${plural(deals.length, "deal")} with ${money(sum(deals), currency)} weighted at ${cohort.rate}%: of ${cohort.n} deals closed ${period} that were ${situation.phrase} at some point, ${cohort.won} went on to win`);
  }

  const overall = measuredRate(ctx, "historic.win_rate");
  const unsizedNotes: string[] = [];
  for (const { situation, deals, n } of fallback) {
    const thin = `only ${n} deals closed ${period} that were ${situation.phrase}, under the calibrated minimum of ${minSample}`;
    if (situation.ceiling && !("reason" in overall)) {
      amount += (sum(deals) * overall.value) / 100;
      sized += deals.length;
      lines.push(`${plural(deals.length, "deal")} with ${money(sum(deals), currency)} weighted at your overall win rate of ${overall.value}% on ${overall.n} closed deals, because ${thin}. Deals in this situation usually win less often, so treat this part as a ceiling`);
    } else {
      unsizedNotes.push(`${plural(deals.length, "deal")} with ${money(sum(deals), currency)} aren't counted, because ${thin}${situation.ceiling ? ", and your overall win rate isn't measurable" : ""}`);
    }
  }
  if (sized === 0) return notSized(`Too few comparable deals have closed to size this: ${unsizedNotes.join("; ")}.`);

  return {
    kind: "revenue",
    amount: Math.round(amount),
    unit: currency,
    period: "potential revenue in deals open today",
    assumptions: [
      `Raw exposure: ${money(sum(priced), currency)}, the total Amount on ${plural(priced.length, "affected deal")} with an amount`,
      ...(unpriced ? [`${unpriced} of the ${affected.length} affected deals have no amount, so they add nothing here. We don't impute one`] : []),
      ...lines,
      ...unsizedNotes,
      "Lost and no decision deals both count as not won",
    ],
  };
}
