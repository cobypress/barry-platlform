import { money } from "../render/format.ts";
import { notSized, type RuleContext } from "../engine/rule.ts";
import type { Metric, Value } from "../schemas/index.ts";

// Every sized figure is built here, so each one carries what it rests on. The
// only multipliers are the org's own measured rates and the calibration: nothing
// here is a benchmark or a typical figure, because Claude may not introduce one
// and neither may we.

export const measuredRate = (ctx: Pick<RuleContext, "metrics">, id: string): Extract<Metric, { status: "measured" }> | { reason: string } => {
  const metric = ctx.metrics.find((m) => m.id === id);
  if (!metric) return { reason: `${id} wasn't calculated.` };
  return metric.status === "measured" ? metric : { reason: metric.reason };
};

/** A sum of deal amounts, weighted by the org's own win rate. */
export function atOwnWinRate(ctx: Pick<RuleContext, "metrics" | "calibration">, amount: number, options: { multiCurrency: boolean; period: string; basis: string; caveat: string }): Value {
  if (options.multiCurrency) return notSized("The org is multi-currency, so these amounts can't be added.");
  if (amount <= 0) return notSized("These records have no amounts to size.");
  const rate = measuredRate(ctx, "historic.win_rate");
  if ("reason" in rate) return notSized(`We won't size this without your own win rate, which isn't measurable: ${rate.reason}`);
  return {
    kind: "revenue",
    amount: Math.round((amount * rate.value) / 100),
    unit: ctx.calibration.currency,
    period: options.period,
    assumptions: [options.basis, `Weighted by your own win rate of ${rate.value}%, measured on ${rate.n} closed deals`, options.caveat],
  };
}

/** Leads priced at what the org's own leads have returned on average. */
export function atOwnLeadReturn(ctx: Pick<RuleContext, "metrics" | "calibration">, leads: number, options: { period: string; caveat: string }): Value {
  const rate = measuredRate(ctx, "lead.won_per_lead");
  if ("reason" in rate) return notSized(`We won't size this without your own return per lead, which isn't measurable: ${rate.reason}`);
  if (rate.value <= 0 || leads <= 0) return notSized("Your leads haven't produced won revenue to size this from.");
  return {
    kind: "revenue",
    amount: Math.round(leads * rate.value),
    unit: ctx.calibration.currency,
    period: options.period,
    assumptions: [`${leads} leads at your own average of ${money(rate.value, ctx.calibration.currency)} won per lead, measured on ${rate.n} leads`, options.caveat],
  };
}

/** Hours, only where the calibration says how long one item takes. */
export function calibratedHours(ctx: Pick<RuleContext, "calibration">, key: string, count: number, noun: string): Value {
  const minutes = ctx.calibration.effortMinutes[key];
  if (minutes === undefined) return notSized(`The calibration has no time per ${noun} (${key}), and we won't guess one.`);
  return {
    kind: "hours",
    amount: Math.round((count * minutes) / 6) / 10,
    unit: "hours",
    period: "per year",
    assumptions: [`${count} of them at ${minutes} minutes a year each, agreed in the calibration`],
  };
}
