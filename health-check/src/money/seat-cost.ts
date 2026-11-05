import type { Calibration } from "../schemas/index.ts";
import { EDITION_PLANS, LIST_PRICES, listPrice, type ListPrice } from "./list-prices.ts";

// What one seat of a licence costs a year: the customer's own figure from the
// calibration first, then Salesforce's list price. Each answer says which it is,
// because a report must never pass a list price off as what they pay.

export type SeatPrice = { licence: string; annual: number; currency: string; basis: "calibration" | "list_price"; note: string };
export type Unpriced = { licence: string; reason: string };

export type SeatPricing = {
  calibration: Pick<Calibration, "licenceCosts" | "currency">;
  /** Organization.OrganizationType, or null when it couldn't be read. */
  edition: string | null;
  /** For tests. */
  prices?: ListPrice[];
  plans?: Record<string, string>;
};

/** The annual cost of one seat of this licence, or why we can't say. */
export function costPerSeat(licence: string, pricing: SeatPricing): SeatPrice | Unpriced {
  const { licenceCosts, currency } = pricing.calibration;
  const own = licenceCosts[licence];
  if (own !== undefined) return { licence, annual: own, currency, basis: "calibration", note: `${own} ${currency} a year, from the calibration` };
  const list = listPrice(licence, pricing.edition, currency, pricing.prices ?? LIST_PRICES, pricing.plans ?? EDITION_PLANS);
  if (list.status === "priced") {
    const p = list.price;
    return { licence, annual: list.annual, currency, basis: "list_price", note: `${list.annual} ${currency} a year at Salesforce's list price for ${p.plan} (${p.monthly} ${p.currency} a user a month, ${p.billing.toLowerCase()}; ${p.url}, read on ${p.checked})` };
  }
  return { licence, reason: `The calibration has no cost for it, and there's no list price: ${list.reason}` };
}

export const isPriced = (p: SeatPrice | Unpriced): p is SeatPrice => "annual" in p;
