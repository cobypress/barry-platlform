import { SalesforceError, type SalesforceClient } from "../salesforce/client.ts";
import { currencyForLocale } from "./locale-currency.ts";

// An automatic run's currency, read at connect beside the stages (DECISIONS.md
// 693, 700). No person confirms an automatic calibration, so the currency is
// what the org says, or none: never the schema's pounds, and never a guess. It
// never holds a run (700).
//
// a) Organization.DefaultCurrencyIsoCode, which exists only where multiple
//    currencies are on: a code there is the org's own.
// b) A single-currency org answers INVALID_FIELD, its normal answer, not a
//    failure. Its amounts are all in its one currency, whose name no documented
//    query gives (docs/QUESTIONS.md): they are shown as plain numbers. Its
//    default locale is read only for a suggestion the portal can offer when it
//    asks the customer which currency it is (locale-currency.ts).
// c) Anything else is unread: the run decides from the opportunities' describe.
// Both go through the client's query route, which the allowlist already lets
// through: no new route.

export const ORG_CURRENCY_SOQL = "SELECT DefaultCurrencyIsoCode FROM Organization";
export const ORG_LOCALE_SOQL = "SELECT DefaultLocaleSidKey FROM Organization";

/** The time each read may take, in milliseconds. */
export const CURRENCY_TIMEOUT_MS = 10_000;

const ISO_CODE = /^[A-Z]{3}$/;

/**
 * What connect learnt of the currency (DECISIONS.md 700): the org's own code
 * (several currencies on); one currency, not known by name, with the locale's
 * suggestion or none; or nothing, for the run to decide.
 */
export type OrgCurrency = { kind: "code"; code: string } | { kind: "single"; suggested: string | null } | { kind: "unread" };

const UNREAD: OrgCurrency = { kind: "unread" };

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
 * What the org says of its currency. Never throws, and logs only the kind of
 * each answer: never a value the org chose beyond a code.
 */
export async function readOrgCurrency(client: SalesforceClient, options: { log: (line: string) => void; timeoutMs?: number }): Promise<OrgCurrency> {
  const timeoutMs = options.timeoutMs ?? CURRENCY_TIMEOUT_MS;
  const own = await readField(client, ORG_CURRENCY_SOQL, "DefaultCurrencyIsoCode", timeoutMs);
  if (own.ok) {
    if (typeof own.value === "string" && ISO_CODE.test(own.value)) return { kind: "code", code: own.value };
    options.log("The org's currency wasn't read: the reply held no currency code, so the run decides from its opportunities.");
    return UNREAD;
  }
  if (own.kind !== "INVALID_FIELD") {
    options.log(`The org's currency wasn't read (${own.kind === "timeout" ? "the read didn't finish in time" : own.kind}), so the run decides from its opportunities.`);
    return UNREAD;
  }
  // The normal answer of an org with one currency, not a failure (DECISIONS.md 700).
  options.log("The org has one currency: Salesforce has no DefaultCurrencyIsoCode on its Organization, as on every org without multiple currencies. Its amounts are shown with no currency named; its default locale is read for a suggestion only.");
  const locale = await readField(client, ORG_LOCALE_SOQL, "DefaultLocaleSidKey", timeoutMs);
  const suggested = locale.ok && typeof locale.value === "string" ? currencyForLocale(locale.value) : null;
  if (!locale.ok) options.log(`The org's default locale wasn't read (${locale.kind === "timeout" ? "the read didn't finish in time" : locale.kind}), so there's no currency to suggest.`);
  else if (!suggested) options.log("The org's default locale names no country whose currency we can be sure of, so there's no currency to suggest.");
  return { kind: "single", suggested };
}
