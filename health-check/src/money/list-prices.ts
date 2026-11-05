// Salesforce's published list prices, the last resort for what a seat costs
// (roadmap decision 4). Every price is as Salesforce's own page states it, with
// the address and the date we read it. A price we couldn't read there isn't here.
//
// Salesforce replaced its editions on 3 September 2026. The plans below are what
// it sells now. An org reports its edition as Organization.OrganizationType, and
// we haven't confirmed which value an org on each new plan reports, or what the
// legacy editions cost now ("existing customer pricing on legacy editions remains
// unchanged", and isn't published). So no edition maps to a plan yet, and list
// price prices nothing until one is confirmed (QUESTIONS.md). A guessed price
// would be worse than none.

export type ListPrice = {
  plan: string;
  /** The UserLicense.Name the price is for. */
  licence: string;
  currency: string;
  /** As published: per user per month. */
  monthly: number;
  billing: string;
  url: string;
  /** When we read it, YYYY-MM-DD. */
  checked: string;
};

const SALES_PRICING = "https://www.salesforce.com/uk/sales/pricing/";
const CHECKED = "2026-11-05";

// The UK Service Cloud page gives the same prices for the same plans, so a full
// Salesforce licence costs the same whichever cloud it's used for.
export const LIST_PRICES: ListPrice[] = [
  { plan: "Starter Suite", licence: "Salesforce", currency: "GBP", monthly: 20, billing: "Billed monthly or annually", url: SALES_PRICING, checked: CHECKED },
  { plan: "Pro Suite", licence: "Salesforce", currency: "GBP", monthly: 80, billing: "Billed annually", url: SALES_PRICING, checked: CHECKED },
  { plan: "Core", licence: "Salesforce", currency: "GBP", monthly: 156, billing: "Billed annually", url: SALES_PRICING, checked: CHECKED },
  { plan: "Advanced", licence: "Salesforce", currency: "GBP", monthly: 316, billing: "Billed annually", url: SALES_PRICING, checked: CHECKED },
  { plan: "Max", licence: "Salesforce", currency: "GBP", monthly: 440, billing: "Billed annually", url: SALES_PRICING, checked: CHECKED },
];

/** OrganizationType to the plan it's priced as. Empty until a mapping is confirmed: see the note above. */
export const EDITION_PLANS: Record<string, string> = {};

export type ListPriceResult = { status: "priced"; annual: number; price: ListPrice } | { status: "unpriced"; reason: string };

/** The annual list price of one seat of a licence in an edition, or why there isn't one. */
export function listPrice(licence: string, edition: string | null, currency: string, prices: ListPrice[] = LIST_PRICES, plans: Record<string, string> = EDITION_PLANS): ListPriceResult {
  if (!edition) return { status: "unpriced", reason: "The org's edition couldn't be read." };
  const plan = plans[edition];
  if (!plan) return { status: "unpriced", reason: `We haven't confirmed which of Salesforce's current plans ${edition} is priced as.` };
  const price = prices.find((p) => p.plan === plan && p.licence === licence && p.currency === currency);
  if (!price) return { status: "unpriced", reason: `Salesforce doesn't publish a ${currency} list price for a ${licence} seat on ${plan} that we've read.` };
  return { status: "priced", annual: price.monthly * 12, price };
}
