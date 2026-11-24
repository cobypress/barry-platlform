import { SalesforceError, type SalesforceClient } from "../salesforce/client.ts";
import { currencyForLocale } from "./locale-currency.ts";

// An automatic run's currency, read at connect beside the stages (DECISIONS.md
// 693, 700). No person confirms an automatic calibration, so the currency is
// what the org says, or an assumption said as one, or none: never the schema's
// pounds. It never holds a run (700): with none, every money figure is not
// measurable and everything else is measured.
//
// a) Organization.DefaultCurrencyIsoCode, which exists only where multiple
//    currencies are on. A single-currency org answers INVALID_FIELD, its normal
//    answer, not a failure.
// b) Then, only after that answer, the org's default locale, mapped to its
//    country's currency (locale-currency.ts). Salesforce documents no field a
//    query can read for a single-currency org's own currency (docs/QUESTIONS.md),
//    so this one is marked as taken from the locale and never confirmed.
// Both go through the client's query route, which the allowlist already lets
// through: no new route.

export const ORG_CURRENCY_SOQL = "SELECT DefaultCurrencyIsoCode FROM Organization";
export const ORG_LOCALE_SOQL = "SELECT DefaultLocaleSidKey FROM Organization";

/** The time each read may take, in milliseconds. */
export const CURRENCY_TIMEOUT_MS = 10_000;

const ISO_CODE = /^[A-Z]{3}$/;

/**
 * Where an automatic run's currency came from (DECISIONS.md 700): the org's own
 * default currency, its default locale (an assumption), or nowhere.
 */
export type OrgCurrency = { code: string; from: "org" } | { code: string; from: "locale" } | { code: null; from: "none" };

const NONE: OrgCurrency = { code: null, from: "none" };

type Read<T> = { ok: true; value: T } | { ok: false; kind: string };

/** One field of the one Organization row, in time, or the kind of failure. Never throws. */
async function readField(client: SalesforceClient, soql: string, field: string, timeoutMs: number): Promise<Read<unknown>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const work = client.query<Record<string, unknown>>(soql, { maxRecords: 1 }).then((r) => r.records[0]?.[field]);
  work.catch(() => {});
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), timeoutMs);
  });
  try {
    const read = await Promise.race([work, timeout]);
    return read === "timeout" ? { ok: false, kind: "timeout" } : { ok: true, value: read };
  } catch (err) {
    if (err instanceof SalesforceError) return { ok: false, kind: err.code.replace(/[^A-Z_0-9]/g, "").slice(0, 40) };
    return { ok: false, kind: err instanceof Error ? err.name.replace(/[^A-Za-z]/g, "").slice(0, 40) : "not an error" };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * The org's currency and where it came from. Never throws, and logs only the
 * kind of each answer: never a value the org chose beyond the code we keep.
 */
export async function readOrgCurrency(client: SalesforceClient, options: { log: (line: string) => void; timeoutMs?: number }): Promise<OrgCurrency> {
  const timeoutMs = options.timeoutMs ?? CURRENCY_TIMEOUT_MS;
  const own = await readField(client, ORG_CURRENCY_SOQL, "DefaultCurrencyIsoCode", timeoutMs);
  if (own.ok) {
    if (typeof own.value === "string" && ISO_CODE.test(own.value)) return { code: own.value, from: "org" };
    options.log("The org's currency wasn't read: the reply held no currency code, so its amounts are left without one.");
    return NONE;
  }
  if (own.kind !== "INVALID_FIELD") {
    options.log(`The org's currency wasn't read (${own.kind === "timeout" ? "the read didn't finish in time" : own.kind}), so its amounts are left without one.`);
    return NONE;
  }
  // The normal answer of an org with one currency, not a failure (DECISIONS.md 700).
  options.log("The org has one currency: Salesforce has no DefaultCurrencyIsoCode on its Organization, as on every org without multiple currencies. Reading its default locale instead.");
  const locale = await readField(client, ORG_LOCALE_SOQL, "DefaultLocaleSidKey", timeoutMs);
  if (!locale.ok) {
    options.log(`The org's default locale wasn't read (${locale.kind === "timeout" ? "the read didn't finish in time" : locale.kind}), so its amounts are left without a currency.`);
    return NONE;
  }
  const code = typeof locale.value === "string" ? currencyForLocale(locale.value) : null;
  if (!code) {
    options.log("The org's default locale names no country whose currency we can be sure of, so its amounts are left without a currency.");
    return NONE;
  }
  options.log(`The org's currency is taken as ${code} from its default locale, and isn't confirmed.`);
  return { code, from: "locale" };
}
