import { orgWordsOf, said, type Said } from "../engine/org-words.ts";
import { moneySaid } from "../engine/said-forms.ts";
import { notSized, type RuleContext } from "../engine/rule.ts";
import type { Metric, Value } from "../schemas/index.ts";

// Every sized figure is built here, so each one carries what it rests on. The
// only multipliers are the org's own measured rates and the calibration: nothing
// here is a benchmark or a typical figure, because Claude may not introduce one
// and neither may we.

export const measuredRate = (ctx: Pick<RuleContext, "metrics" | "leanOn">, id: string): Extract<Metric, { status: "measured" }> | { reason: string } => {
  const metric = ctx.metrics.find((m) => m.id === id);
  if (!metric) return { reason: `${id} wasn't calculated.` };
  if (metric.status !== "measured") return { reason: metric.reason };
  // A figure sized from this rate rests on whatever the rate rested on (DECISIONS.md 598).
  ctx.leanOn?.(id);
  return metric;
};

/** A sum of deal amounts, weighted by the org's own win rate. */
export function atOwnWinRate(ctx: Pick<RuleContext, "metrics" | "calibration" | "leanOn">, amount: number, options: { multiCurrency: boolean; period: string; basis: Said; caveat: Said }): Value {
  if (options.multiCurrency) return notSized(said`The org is multi-currency, so these amounts can't be added.`);
  if (amount <= 0) return notSized(said`These records have no amounts to size.`);
  const rate = measuredRate(ctx, "historic.win_rate");
  // Said only when the reason itself was, so its marks come with it; a reason said didn't make stays unknown (DECISIONS.md 693).
  if ("reason" in rate) return notSized(orgWordsOf(rate.reason) ? said`We won't size this without your own win rate, which isn't measurable: ${rate.reason as Said}` : `We won't size this without your own win rate, which isn't measurable: ${rate.reason}`);
  return {
    kind: "revenue",
    amount: Math.round((amount * rate.value) / 100),
    unit: ctx.calibration.currency,
    period: options.period,
    // Each made by said, so the value's assumptions are marked (DECISIONS.md 663).
    assumptions: [options.basis, said`Weighted by your own win rate of ${rate.value}%, measured on ${rate.n} closed deals`, options.caveat],
  };
}

/** Leads priced at what the org's own leads have returned on average. */
export function atOwnLeadReturn(ctx: Pick<RuleContext, "metrics" | "calibration" | "leanOn">, leads: number, options: { period: string; caveat: Said }): Value {
  const rate = measuredRate(ctx, "lead.won_per_lead");
  if ("reason" in rate) return notSized(`We won't size this without your own return per lead, which isn't measurable: ${rate.reason}`);
  if (rate.value <= 0 || leads <= 0) return notSized(said`Your leads haven't produced won revenue to size this from.`);
  return {
    kind: "revenue",
    amount: Math.round(leads * rate.value),
    unit: ctx.calibration.currency,
    period: options.period,
    assumptions: [said`${leads} leads at your own average of ${moneySaid(rate.value, ctx.calibration.currency)} won per lead, measured on ${rate.n} leads`, options.caveat],
  };
}

/** Hours, only where the calibration says how long one item takes. */
/** The calibration keys a time per item is read from: our own names for the work, never the org's. */
type UpkeepKey = "report-upkeep" | "custom-field-upkeep";

export function calibratedHours(ctx: Pick<RuleContext, "calibration">, key: UpkeepKey, count: number, noun: "unused report" | "unused custom field"): Value {
  const minutes = ctx.calibration.effortMinutes[key];
  if (minutes === undefined) return notSized(said`The calibration has no time per ${noun} (${key}), and we won't guess one.`);
  return {
    kind: "hours",
    amount: Math.round((count * minutes) / 6) / 10,
    unit: "hours",
    period: "per year",
    assumptions: [said`${count} of them at ${minutes} minutes a year each, agreed in the calibration`],
  };
}
