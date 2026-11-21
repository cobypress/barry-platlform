import { eng, org, said, type Said } from "../engine/org-words.ts";
import type { Calibration } from "../schemas/index.ts";
import { EDITION_PLANS, LIST_PRICES, listPrice, type ListPrice } from "./list-prices.ts";

// What one seat of a licence costs a year: the customer's own figure from the
// calibration first, then Salesforce's list price. Each answer says which it is,
// because a report must never pass a list price off as what they pay.

export type SeatPrice = { licence: string; annual: number; currency: string; basis: "calibration" | "list_price"; note: Said };
export type Unpriced = { licence: string; reason: Said };

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
  if (own !== undefined) return { licence, annual: own, currency, basis: "calibration", note: said`${own} ${org(currency)} a year, from the calibration` };
  const list = listPrice(licence, pricing.edition, currency, pricing.prices ?? LIST_PRICES, pricing.plans ?? EDITION_PLANS);
  if (list.status === "priced") {
    const p = list.price;
    // The plan, billing, link and date are our list price table's own words (list-prices.ts), never read from the org; the currency is the calibration's.
    const source = eng(`${p.plan} (${p.monthly} ${p.currency} a user a month, ${p.billing.toLowerCase()}; ${p.url}, read on ${p.checked})`);
    return { licence, annual: list.annual, currency, basis: "list_price", note: said`${list.annual} ${org(currency)} a year at Salesforce's list price for ${source}` };
  }
  return { licence, reason: said`The calibration has no cost for it, and there's no list price: ${list.reason}` };
}

export const isPriced = (p: SeatPrice | Unpriced): p is SeatPrice => "annual" in p;
