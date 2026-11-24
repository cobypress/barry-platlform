import type { Calibration } from "../schemas/index.ts";
import { said, type Said } from "./org-words.ts";

// What an automatic run knows of the currency its amounts are in (DECISIONS.md
// 700), and what it says about it. A person's calibration, and an automatic one
// that read the org's own default currency (an org with several), are as
// before. A single-currency org's amounts are all in its one currency, whose
// name no documented query gives: they are shown and added up as plain
// numbers, with no code or symbol, and each place that shows one says so. A run
// that can't tell measures no money figure at all, and says why.

/** ISO 4217's code for "no currency": what the console's column holds when the name isn't known. Never printed. */
export const NO_CURRENCY = "XXX";

/**
 * Inside an analysis that can't tell the currency: ISO 4217's code for testing,
 * never a real currency, so whatever a rule writes with it is found and taken
 * out before anything leaves the analysis.
 */
export const UNREAD_MARK = "XTS";

/**
 * confirmed: a person's calibration. read: the org's own default currency
 * (several currencies on). org: one currency, not known by name. none: can't
 * tell. `unread` (a failed read at connect) is `org` when the run saw that the
 * org has one currency, else `none`: only an analysis can say, from the
 * opportunities' describe.
 */
export type CurrencyStanding = "confirmed" | "read" | "org" | "unread";

export function currencyStanding(c: Pick<Calibration, "source" | "currency" | "currencyFrom">): CurrencyStanding {
  if (c.source !== "org_settings") return "confirmed";
  if (c.currencyFrom === "org") return "org";
  if (c.currencyFrom === "unread" || c.currency === NO_CURRENCY) return "unread";
  return "read";
}

/** Whether amounts are printed with no code or symbol: the currency isn't known by name. A person's calibration always names one. */
export const noCurrencyCode = (c: Pick<Calibration, "source" | "currency">): boolean => c.source === "org_settings" && c.currency === NO_CURRENCY;

/** The line beside every amount whose currency isn't known by name (DECISIONS.md 700). Nothing is guessed, so nothing is less sure for it. */
export const CURRENCY_NOT_NAMED = "Amounts are in your Salesforce org's currency. Tell us which it is and we'll show it.";
/** Made in each analysis, as every said is. */
export const currencyNotNamedNote = (): Said => said`Amounts are in your Salesforce org's currency. Tell us which it is and we'll show it.`;

/** The plain words for a money figure when the run can't tell its currency: an "answer to unlock" on the portal (needs_input). */
export const CURRENCY_UNKNOWN = "We couldn't read which currency your Salesforce amounts are in. Tell us and your next health check can show these.";
/** Made in each analysis, as every said is: the reason above, marked as all ours. */
export const currencyUnknownReason = (): Said => said`We couldn't read which currency your Salesforce amounts are in. Tell us and your next health check can show these.`;

/** Where a money total has nothing in it because the run couldn't tell the currency (DECISIONS.md 700): said in place of "they matter for the risk they carry". */
export const NO_MONEY_FIGURE = "We couldn't read which currency your Salesforce amounts are in, so we haven't put a money figure on these. Tell us and your next health check can show it.";

/** The words that name a currency in a draft (DECISIONS.md 700): sent back on a run whose currency isn't known by name, number or not. */
export const CURRENCY_WORDS = /\b(?:pounds?|dollars?|euros?|sterling|yen|rupees?|quid)\b/i;

/** Every ISO 4217 code a page could print, and the symbols: none may appear in what the customer of a run with no code reads. */
export const CURRENCY_SIGNS = /[£$€¥₹¤]|\b(?:XXX|XTS|GBP|USD|EUR|AUD|CAD|NZD|CHF|JPY|CNY|INR|ZAR|SEK|NOK|DKK|PLN|SGD|HKD|AED|BRL|MXN)\b/;
