import { org, said, type Said } from "../engine/org-words.ts";
// Salesforce's published list prices, the last resort for what a seat costs
// (roadmap decision 4). Every price is as Salesforce's own page states it, with
// the address and the date we read it. A price we couldn't read there isn't here.
//
// Salesforce replaced its editions on 3 September 2026. The plans below are what
// it sells now. An org reports its edition as Organization.OrganizationType, and
// we haven't confirmed which value an org on each new plan reports, or what the
// legacy editions cost now ("existing customer pricing on legacy editions remains
// unchanged", and isn't published). So no edition maps to a plan yet, and list
// price prices nothing until one is confirmed (QUESTIONS.md 47 and 48, DECISIONS.md
// 679). A guessed price would be worse than none. Nothing here is fetched at run
// time: the table is read by a person and dated.

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

/** A page we read the prices from, what it showed and when. */
export type ListPriceSource = { url: string; read: string; shows: string; vat: string; earlier: string };

const SALES_PRICING = "https://www.salesforce.com/uk/sales/pricing/";
const CHECKED = "2026-11-23";

export const LIST_PRICE_SOURCES: ListPriceSource[] = [
  {
    url: SALES_PRICING,
    read: CHECKED,
    shows: "Starter Suite £20 (billed monthly or annually), Pro Suite £80, Core £156, Advanced £316 and Max £440 a user a month (billed annually)",
    vat: "The page doesn't say whether VAT is included",
    earlier: "Read on 2026-11-05 with the same prices",
  },
];

/** A list price read longer ago than this says so on the figure (DECISIONS.md 679). */
export const LIST_PRICE_MAX_AGE_DAYS = 90;

/** Whether a price read on `checked` (YYYY-MM-DD) is older than the limit on `now`, the check's own date. A date we can't read counts as old. */
export function listPriceIsStale(checked: string, now: Date): boolean {
  const read = /^\d{4}-\d{2}-\d{2}$/.test(checked) ? Date.parse(`${checked}T00:00:00Z`) : Number.NaN;
  return Number.isNaN(read) || now.getTime() - read > LIST_PRICE_MAX_AGE_DAYS * 86_400_000;
}

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

/**
 * Editions we looked for a price for and left unpriced on purpose, and why, as
 * read on 2026-11-23 (DECISIONS.md 679). For us, not the report: the finding
 * still says it has no figure in the words below listPrice uses for any edition
 * not in EDITION_PLANS. An edition here must never also be in EDITION_PLANS.
 */
export const UNMAPPED_EDITIONS: Record<string, string> = {
  "Enterprise Edition": "A legacy edition: Salesforce says existing customers keep their pricing and doesn't publish it. Core is compared with it by value only, and is a different product at a different price.",
  "Unlimited Edition": "A legacy edition: Salesforce says existing customers keep their pricing and doesn't publish it. Advanced is compared with it by value only, and is a different product at a different price.",
  "Professional Edition": "Not on Salesforce's UK pricing page, and no page says Pro Suite is the same product or what an org on Pro Suite reports.",
  "Developer Edition": "Free, and not sold, so a seat has no list price.",
};

export type ListPriceResult = { status: "priced"; annual: number; price: ListPrice } | { status: "unpriced"; reason: Said };

/** The currency of every price we hold: Salesforce's UK pages, in pounds. */
export const LIST_PRICE_CURRENCY = "GBP";

/** The annual list price of one seat of a licence in an edition, or why there isn't one. */
export function listPrice(licence: string, edition: string | null, currency: string, prices: ListPrice[] = LIST_PRICES, plans: Record<string, string> = EDITION_PLANS): ListPriceResult {
  if (!edition) return { status: "unpriced", reason: said`The org's edition couldn't be read.` };
  // Own keys only, so an edition such as "constructor" can't reach the object's prototype and pass as a plan.
  const plan = Object.hasOwn(plans, edition) ? plans[edition] : undefined;
  if (!plan) return { status: "unpriced", reason: said`We haven't confirmed which of Salesforce's current plans ${org(edition)} is priced as.` };
  // Converting a pound price into another currency would be an estimate, so a check in another currency gets no list price.
  // A currency not known by name (DECISIONS.md 700) is never named.
  if (currency === "XXX") return { status: "unpriced", reason: said`We hold only Salesforce's UK list prices, in pounds, and we don't know your currency yet, so we don't use them.` };
  if (currency !== LIST_PRICE_CURRENCY) return { status: "unpriced", reason: said`We hold only Salesforce's UK list prices, in pounds, and this check reports in ${org(currency)}, so we don't convert them.` };
  const price = prices.find((p) => p.plan === plan && p.licence === licence && p.currency === currency);
  // The plan is from our own map, marked with the org's words: the safe side (DECISIONS.md 663).
  if (!price) return { status: "unpriced", reason: said`Salesforce doesn't publish a ${org(currency)} list price for a ${org(licence)} seat on ${org(plan)} that we've read.` };
  return { status: "priced", annual: price.monthly * 12, price };
}
