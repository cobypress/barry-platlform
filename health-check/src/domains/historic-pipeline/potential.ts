import { notSized, plural, type RuleContext } from "../../engine/rule.ts";
import { named } from "../../engine/named.ts";
import { eng, joinSaid, org, said, type Said, word } from "../../engine/org-words.ts";
import { moneySaid } from "../../engine/said-forms.ts";
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
  /** Reads after "deals that were": "open past their close date". A stage's name in it is the org's (DECISIONS.md 663). */
  phrase: Said;
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
    return { key: "past-close", phrase: said`open past their close date`, matches: (d) => wasPastClose(d, data.stages, zone), ceiling: true };
  },
  inStageFor: (days: number, data: Ready): Situation => ({ key: `in-stage-${days}`, phrase: named(said`in ${word("one")} stage for ${days} days or more`, { inStageDays: days }), matches: (d) => longestInStage(d, data.stages) >= days, ceiling: true }),
  unchangedFor: (days: number, data: Ready): Situation => ({ key: `unchanged-${days}`, phrase: named(said`open for ${days} days or more with no change to stage, amount or close date`, { unchangedDays: days }), matches: (d) => longestUnchanged(d, data.stages) >= days, ceiling: true }),
  inStage: (stage: string): Situation => ({ key: `stage-${stage}`, phrase: said`in the ${org(stage)} stage`, matches: (d) => wasInStage(d, stage), ceiling: false }),
};

const sum = (deals: Deal[]): number => deals.reduce((total, d) => total + (d.Amount ?? 0), 0);

/**
 * Potential revenue in `affected` open deals: each deal's amount at the share of
 * comparable closed deals that still won. The raw exposure and the deals with no
 * amount are stated, and no amount is ever imputed.
 */
export function potentialRevenue(ctx: Pick<RuleContext, "calibration" | "window" | "metrics" | "data">, data: Ready, affected: Deal[], situationOf: (d: Deal) => Situation): Value {
  if (data.multiCurrency) return notSized(said`The org is multi-currency, so these amounts can't be added.`);
  if (affected.length === 0) return notSized(said`There are no deals to size.`);
  const priced = affected.filter((d) => (d.Amount ?? 0) > 0);
  const unpriced = affected.length - priced.length;
  if (priced.length === 0) return notSized(named(said`None of these ${plural(affected.length, "deal")} has an amount, and we don't impute ${word("one")}.`, { affected: affected.length }));

  const { currency, minSample } = ctx.calibration;
  // The analysis window's two dates, which the engine works out from the run's date: digits and dashes only.
  const period = eng(`${ctx.window.from} to ${ctx.window.to}`);
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
  const lines: Said[] = [];
  const fallback: Array<{ situation: Situation; deals: Deal[]; n: number }> = [];
  for (const { situation, deals } of [...groups.values()].sort((a, b) => a.situation.key.localeCompare(b.situation.key))) {
    const cohort = cohortOf(closed.filter(situation.matches), minSample);
    if (cohort.rate === null) {
      fallback.push({ situation, deals, n: cohort.n });
      continue;
    }
    const dealsAmount = sum(deals);
    amount += (dealsAmount * cohort.rate) / 100;
    sized += deals.length;
    lines.push(named(said`${plural(deals.length, "deal")} with ${moneySaid(dealsAmount, currency)} weighted at ${cohort.rate}%: of ${cohort.n} deals closed ${period} that were ${situation.phrase} at some point, ${cohort.won} went on to win`, { deals: deals.length, dealsMoney: dealsAmount, winPct: cohort.rate, closed: cohort.n, won: cohort.won }));
  }

  // Read, and leaned on, only where a group falls back to it (DECISIONS.md 756): a figure that never uses the overall
  // win rate doesn't carry what that rate leaned on, such as closed stages read from their names.
  let read: ReturnType<typeof measuredRate> | undefined;
  const overallRate = () => (read ??= measuredRate(ctx, "historic.win_rate"));
  const unsizedNotes: Said[] = [];
  for (const { situation, deals, n } of fallback) {
    const dealsAmount = sum(deals);
    const thin = named(said`only ${n} deals closed ${period} that were ${situation.phrase}, under the calibrated minimum of ${minSample}`, { closed: n, minSample });
    const overall = situation.ceiling ? overallRate() : null;
    if (overall && !("reason" in overall)) {
      amount += (dealsAmount * overall.value) / 100;
      sized += deals.length;
      lines.push(named(said`${plural(deals.length, "deal")} with ${moneySaid(dealsAmount, currency)} weighted at your overall win rate of ${overall.value}% on ${overall.n} closed deals, because ${thin}. Deals in this situation usually win less often, so treat this part as a ceiling`, { deals: deals.length, dealsMoney: dealsAmount, winPct: overall.value, closedOverall: overall.n }));
    } else {
      unsizedNotes.push(named(said`${plural(deals.length, "deal")} with ${moneySaid(dealsAmount, currency)} aren't counted, because ${thin}${situation.ceiling ? ", and your overall win rate isn't measurable" : ""}`, { deals: deals.length, dealsMoney: dealsAmount }));
    }
  }
  if (sized === 0) return notSized(said`Too few comparable deals have closed to size this: ${joinSaid(unsizedNotes, "; ")}.`);

  const exposure = sum(priced);
  return {
    kind: "revenue",
    amount: Math.round(amount),
    unit: currency,
    period: "potential revenue in deals open today",
    assumptions: [
      named(said`Raw exposure: ${moneySaid(exposure, currency)}, the total Amount on ${plural(priced.length, "affected deal")} with an amount`, { exposureMoney: exposure, priced: priced.length }),
      ...(unpriced ? [named(said`${unpriced} of the ${affected.length} affected deals have no amount, so they add nothing here. We don't impute ${word("one")}`, { unpriced, affected: affected.length })] : []),
      ...lines,
      ...unsizedNotes,
      said`Lost and no decision deals both count as not won`,
    ],
  };
}
