import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ORG_SETTINGS_CALIBRATOR } from "../connect/auto-calibration.ts";
import { StoreError } from "../connect/org-stages.ts";
import { CURRENCY_TIMEOUT_MS, readOrgCurrency, type OrgCurrency } from "../connect/org-currency.ts";
import { NO_CURRENCY } from "../engine/currency.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import type { OAuthConnectionProvider } from "../salesforce/oauth-provider.ts";
import type { StoredConnection } from "../salesforce/token-store.ts";
import { Calibration, isCurrencyCode } from "../schemas/calibration.ts";
import { answeredCurrency, logCurrency } from "./connect-job.ts";
import { safeReason, type WorkerDeps } from "./deps.ts";
import type { CurrencyWas, RunJobInput } from "./types.ts";

// An automatic calibration saved before decision 740, or after a settings read
// that couldn't say, names no currency from Salesforce: "XXX" from "org", or
// the customer's portal answer (731). Nothing built it again, so the customer
// and every pulse copied from it stayed unnamed (DECISIONS.md 745). When such a
// run starts afresh through its own org's connection, the org's settings are
// read once through the same path as at connect, and only an answer that path
// accepts names the saved calibration's currency, before the run's checks use
// it. Everything else costs only the read: the run goes on with the
// calibration it had.

/** The run folder's mark that this start has had its one settings read, so a redelivery of it doesn't read again. */
export const CURRENCY_READ_FILE = "currency-read.json";

/**
 * The saved calibration's currency where it is ours to name from the org's
 * settings, or null: an automatic run's, read from the org's settings by no
 * person, unnamed or named only by the customer. Never a person's (a consultant
 * who confirmed a calibration chose its currency), never one Salesforce named
 * (its own code, or the settings'), never one whose read failed ("unread": we
 * couldn't tell one currency from several).
 */
export function unnamedBySalesforce(input: Pick<RunJobInput, "run" | "calibration">): CurrencyWas | null {
  const c = input.calibration;
  if (input.run.reviewMode !== "automatic" || !c) return null;
  if (c.source !== "org_settings" || c.confirmedBy !== ORG_SETTINGS_CALIBRATOR) return null;
  if (c.currencyFrom === "org" && c.currency === NO_CURRENCY) return { currency: c.currency, currencyFrom: "org" };
  if (c.currencyFrom === "customer" && isCurrencyCode(c.currency)) return { currency: c.currency, currencyFrom: "customer" };
  return null;
}

/** Work in time, or "timeout". The work goes on unheard if it's late: it writes nothing of ours. */
async function inTime<T>(work: Promise<T>, ms: number): Promise<T | "timeout"> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  work.catch(() => {});
  try {
    return await Promise.race([work, new Promise<"timeout">((resolve) => (timer = setTimeout(() => resolve("timeout"), ms)))]);
  } finally {
    clearTimeout(timer);
  }
}

/** Why a currency read that wasn't the settings' leaves the calibration as it was, for the log. */
function notNamed(runId: string, currency: OrgCurrency): string | null {
  if (currency.kind === "code") return `Automatic run ${runId}'s org now gives its own currency code, so it may have several currencies: the org's settings weren't read and the calibration's currency is left as it was.`;
  if (currency.kind === "unread") return `Automatic run ${runId}'s org couldn't say whether it has one currency, so the org's settings weren't read and the calibration's currency is left as it was.`;
  // "single": readOrgCurrency has said why the org's settings couldn't name it, in one line.
  return null;
}

/**
 * The calibration the run goes on with: the saved one with its currency named
 * from the org's settings, or the saved one as it was. The caller has checked
 * the run (automatic, the switch on, an OAuth connection to the run's own org)
 * and that this delivery starts it afresh. Never throws.
 */
export async function nameCurrencyBeforeRun(o: { runId: string; input: RunJobInput; stored: StoredConnection; provider: OAuthConnectionProvider; dir: string; deps: WorkerDeps; log: (line: string) => void; readThisStart?: boolean }): Promise<Calibration> {
  const { runId, input, stored, deps, log } = o;
  const c = input.calibration!;
  const was = unnamedBySalesforce(input);
  if (!was) return c;
  // Nothing is read through a connection to another org: the run's own check, and the calibration's.
  if (stored.orgId.slice(0, 15) !== input.run.orgId.slice(0, 15) || (c.orgId && c.orgId.slice(0, 15) !== stored.orgId.slice(0, 15))) return c;
  const timeoutMs = deps.stagesTimeoutMs ?? CURRENCY_TIMEOUT_MS;
  try {
    const mark = join(o.dir, CURRENCY_READ_FILE);
    if (existsSync(mark)) {
      log(`Automatic run ${runId}'s org's settings were read for its currency on an earlier delivery of this start, so they weren't read again.`);
      return c;
    }
    // Marked before the read: a delivery that dies part way costs this start its read, never a second one.
    mkdirSync(o.dir, { recursive: true, mode: 0o700 });
    writeFileSync(mark, `${JSON.stringify({ at: (deps.now ?? (() => new Date()))().toISOString() })}\n`, { mode: 0o600 });
    // Built by this delivery's Run again from a read just made (700, 740): marked, so its retries don't read again.
    if (o.readThisStart) return c;
  } catch (err) {
    log(`Automatic run ${runId}'s org's settings weren't read for its currency (its folder couldn't be marked: ${safeReason(err)}), so the calibration's currency is left as it was.`);
    return c;
  }
  let currency: OrgCurrency;
  try {
    // Its own small budget, as a connect's reads have: one query and one settings call, retried only by the client's policy.
    const opened = await inTime(SalesforceClient.connect(o.provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: deps.dataTransport, sleep: deps.dataTransport ? async () => {} : undefined }), timeoutMs);
    if (opened === "timeout") {
      log(`Automatic run ${runId}'s org's settings weren't read for its currency (the connection didn't open in time), so the calibration's currency is left as it was.`);
      return c;
    }
    currency = await readOrgCurrency(opened, { log, timeoutMs, suggest: false });
  } catch (err) {
    log(`Automatic run ${runId}'s org's settings weren't read for its currency (${safeReason(err, [stored.accessToken, stored.refreshToken])}), so the calibration's currency is left as it was.`);
    return c;
  }
  if (currency.kind !== "settings") {
    const line = notNamed(runId, currency);
    if (line) log(line);
    return c;
  }
  const { currencySuggested: _suggested, ...rest } = c;
  const named = Calibration.safeParse({ ...rest, currency: currency.code, currencyFrom: "connect_api" });
  if (!named.success) {
    log(`Automatic run ${runId}'s calibration refused the currency the org's settings name, so it is left as it was.`);
    return c;
  }
  // The customer's answer, only to say if it differs: Salesforce's stands (740). The newest one, or the one the calibration holds.
  const answered = (await answeredCurrency(currency, stored.orgId, deps, log)) ?? (was.currencyFrom === "customer" ? was.currency : null);
  let saved: "updated" | "unchanged";
  try {
    saved = await deps.store.nameCalibrationCurrency(runId, was, currency.code);
  } catch (err) {
    const reason = err instanceof StoreError ? err.message : "an unexpected error";
    log(`Automatic run ${runId}'s currency from the org's settings (${currency.code}) couldn't be saved (${reason}), so the run goes on with the calibration it had.`);
    return c;
  }
  if (saved === "unchanged") {
    // Another delivery named it, or a person confirmed one, since this run read it: the store's is the one kept.
    const now = await deps.store.loadRunJob(runId).catch(() => null);
    log(`Automatic run ${runId}'s calibration changed while the org's settings were read, so the run goes on with the one saved now.`);
    return now?.calibration ?? c;
  }
  logCurrency(runId, named.data, answered, log);
  log(`Automatic run ${runId}'s saved calibration named no currency from Salesforce, so the org's settings were read before it ran: its currency is now ${currency.code}, and nothing else in it changed.`);
  return named.data;
}
