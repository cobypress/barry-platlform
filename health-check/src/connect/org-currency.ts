import type { SalesforceClient } from "../salesforce/client.ts";

// An automatic run's currency, read at connect beside the stages (DECISIONS.md
// 693). No person confirms an automatic calibration, and the schema's default
// is pounds, so without this every amount on an org in another currency would
// be labelled in pounds. The org's own default currency is the one its amounts
// are held in when it has one currency; an org with several is told apart by
// the run itself (the opportunities read sees CurrencyIsoCode), and its amounts
// are then never added, as on any run. One read through the client's query
// route, which the allowlist already lets through: no new route.

export const ORG_CURRENCY_SOQL = "SELECT DefaultCurrencyIsoCode FROM Organization";

/** The time the read may take, in milliseconds. */
export const CURRENCY_TIMEOUT_MS = 10_000;

const ISO_CODE = /^[A-Z]{3}$/;

/**
 * The org's currency as a three-letter code, or null when it couldn't be read
 * in time or wasn't one. Never throws, and logs only the kind of failure.
 */
export async function readOrgCurrency(client: SalesforceClient, options: { log: (line: string) => void; timeoutMs?: number }): Promise<string | null> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const work = (async (): Promise<string | null> => {
    const result = await client.query<{ DefaultCurrencyIsoCode?: unknown }>(ORG_CURRENCY_SOQL, { maxRecords: 1 });
    const code = result.records[0]?.DefaultCurrencyIsoCode;
    return typeof code === "string" && ISO_CODE.test(code) ? code : null;
  })();
  work.catch(() => {});
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), options.timeoutMs ?? CURRENCY_TIMEOUT_MS);
  });
  try {
    const read = await Promise.race([work, timeout]);
    if (read === "timeout") {
      options.log("The org's currency wasn't read: the read didn't finish in time.");
      return null;
    }
    if (read === null) options.log("The org's currency wasn't read: the reply held no currency code.");
    return read;
  } catch (err) {
    options.log(`The org's currency wasn't read: ${err instanceof Error ? err.name.replace(/[^A-Za-z]/g, "").slice(0, 40) : "not an error"}.`);
    return null;
  } finally {
    clearTimeout(timer);
  }
}
