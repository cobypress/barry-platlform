import type { Calibration } from "../schemas/index.ts";
import { org, said, type Said } from "./org-words.ts";

// What an automatic run knows of the currency its amounts are in (DECISIONS.md
// 700), and what it says about it. A person's calibration, and an automatic one
// that read the org's own default currency, are as before. One whose currency
// was taken from the org's default locale has it as an assumption, said on
// every money figure, as the unconfirmed stage order is (692). One with no
// currency measures no money figure at all, and says why.

/** ISO 4217's code for "no currency", kept where the console's column must hold one and nothing is known. */
export const NO_CURRENCY = "XXX";

/**
 * confirmed: a person's calibration. read: the org's own default currency.
 * locale: taken from the org's default locale, unconfirmed. none: unknown.
 */
export type CurrencyStanding = "confirmed" | "read" | "locale" | "none";

export function currencyStanding(c: Pick<Calibration, "source" | "currency" | "currencyFrom">): CurrencyStanding {
  if (c.source !== "org_settings") return "confirmed";
  if (c.currencyFrom === "none" || c.currency === NO_CURRENCY) return "none";
  return c.currencyFrom === "locale" ? "locale" : "read";
}

/** The plain words for a money figure no currency was read for: an "answer to unlock" on the portal (needs_input). */
export const CURRENCY_UNKNOWN = "We couldn't read which currency your Salesforce amounts are in. Tell us and your next health check can show these.";

/** Made in each analysis, as every said is: the reason above, marked as all ours. */
export const currencyUnknownReason = (): Said => said`We couldn't read which currency your Salesforce amounts are in. Tell us and your next health check can show these.`;

/** What every money figure says when its currency came from the org's locale (DECISIONS.md 700). The code is marked as the org's, as every currency code is (693). */
export const currencyFromLocaleNote = (code: string): Said => said`We took your currency (${org(code)}) from your Salesforce locale. Tell us if your amounts are in another currency.`;

/** The same sentence, outside an analysis, for a page or a total. */
export const currencyFromLocaleText = (code: string): string => `We took your currency (${code}) from your Salesforce locale. Tell us if your amounts are in another currency.`;
