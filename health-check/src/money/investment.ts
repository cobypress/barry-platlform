import { INVESTMENT, INVESTMENT_METRIC_IDS } from "../domains/utilisation/investment.ts";
import type { Intake } from "../narrative/intake.ts";
import type { Metric } from "../schemas/index.ts";
import { money } from "../render/format.ts";

// What the customer pays a year, as a report states it. Their own total wins
// (roadmap decision 4), then seats priced from the calibration, then seats at
// list price. The figure always says which it is, and list price is shown beside
// their own figure for context, never in place of it.

export { INVESTMENT, INVESTMENT_METRIC_IDS };

export type CostBasis = "customer" | "calibration" | "list_price" | "calibration_and_list_price";

export type AnnualCost = { status: "measured"; value: number; basis: CostBasis; label: string } | { status: "not_measurable"; reason: string };

export type Investment = {
  annualCost: AnnualCost;
  /** Seats at list price, when a figure of the customer's own sets the annual cost. */
  listPriceContext: { status: "measured"; value: number } | { status: "not_measurable"; reason: string } | null;
  currency: string;
};

type Measured = Extract<Metric, { status: "measured" }>;
const find = (metrics: Metric[], id: string): Metric | undefined => metrics.find((m) => m.id === id);
const measured = (m: Metric | undefined): Measured | null => (m?.status === "measured" ? m : null);

const LABELS: Record<CostBasis, string> = {
  customer: "What you told us you spend on Salesforce a year",
  calibration: "Your seats at the cost per seat agreed with you",
  list_price: "Your seats at Salesforce's list price, which isn't what you pay",
  calibration_and_list_price: "Your seats at the cost per seat agreed with you, and at Salesforce's list price where we have no figure of yours",
};

function seatBasis(m: Measured): CostBasis {
  const own = m.stats.calibrationPriced ?? 0;
  const list = m.stats.listPriced ?? 0;
  return own && list ? "calibration_and_list_price" : list ? "list_price" : "calibration";
}

/** The annual cost by precedence, and list price for context, from the run's metrics and the intake as it stands. */
export function buildInvestment(metrics: Metric[], intake: Intake | null | undefined, currency: string): Investment {
  const seats = find(metrics, INVESTMENT.seatCost);
  const list = find(metrics, INVESTMENT.listPriceCost);
  const spend = intake?.salesforceSpendAnnual;
  if (spend !== undefined) {
    const listed = measured(list);
    return {
      annualCost: { status: "measured", value: spend, basis: "customer", label: LABELS.customer },
      listPriceContext: listed ? { status: "measured", value: listed.value } : { status: "not_measurable", reason: list?.status === "not_measurable" ? list.reason : "The run has no list price figure." },
      currency,
    };
  }
  const priced = measured(seats);
  if (priced) {
    const basis = seatBasis(priced);
    const listed = measured(list);
    // Beside the calibration's figure, list price is context. Where list price is the figure, it would only repeat it.
    const context = basis === "calibration" ? (listed ? { status: "measured" as const, value: listed.value } : { status: "not_measurable" as const, reason: list?.status === "not_measurable" ? list.reason : "The run has no list price figure." }) : null;
    return { annualCost: { status: "measured", value: priced.value, basis, label: LABELS[basis] }, listPriceContext: context, currency };
  }
  const why = seats?.status === "not_measurable" ? seats.reason : "The run has no seat cost figure.";
  return { annualCost: { status: "not_measurable", reason: `You haven't told us what you spend, and ${why.charAt(0).toLowerCase()}${why.slice(1)}` }, listPriceContext: null, currency };
}

/** The working, in sentences: the raw notes print it, and the number check reads it. */
export function investmentText(investment: Investment, metrics: Metric[]): string[] {
  const { annualCost, listPriceContext, currency } = investment;
  const lines: string[] = [];
  const paid = measured(find(metrics, INVESTMENT.seatsPaid));
  const used = measured(find(metrics, INVESTMENT.seatsUsed));
  const unused = measured(find(metrics, INVESTMENT.seatsUnused));
  const active = measured(find(metrics, INVESTMENT.activeUserShare));
  const unusedCost = measured(find(metrics, INVESTMENT.unusedSeatCost));
  if (paid && used && unused) lines.push(`You pay for ${paid.value} Salesforce ${paid.value === 1 ? "seat" : "seats"}. ${used.value} are in use and ${unused.value} aren't.`);
  else lines.push("We couldn't read your licences, so we can't say how many seats you pay for.");
  if (active) lines.push(`${active.value}% of the seats in use belong to someone who logged in within ${active.stats.dormantDays} days.`);
  if (annualCost.status === "measured") lines.push(`Annual cost: ${money(annualCost.value, currency)}. ${annualCost.label}.`);
  else lines.push(`Annual cost: not measurable. ${annualCost.reason}`);
  if (listPriceContext?.status === "measured") lines.push(`At Salesforce's list price, your seats would cost ${money(listPriceContext.value, currency)} a year. That's for context, not what you pay.`);
  else if (listPriceContext) lines.push(`List price, for context: not measurable. ${listPriceContext.reason}`);
  if (unusedCost) lines.push(`Unused seats cost ${money(unusedCost.value, currency)} a year, priced as in the seat cost below.`);
  return lines;
}
