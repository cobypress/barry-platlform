import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ORG_SETTINGS_CALIBRATOR } from "../connect/auto-calibration.ts";
import { StoreError } from "../connect/org-stages.ts";
import { CURRENCY_TIMEOUT_MS, readOrgCurrency, type OrgCurrency } from "../connect/org-currency.ts";
import { NO_CURRENCY } from "../engine/currency.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import type { ConnectionProvider, OrgConnection } from "../salesforce/connection.ts";
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

/** A query's failure in our words: the real cause, so a budget stop or a lost token reads as itself. */
function causeOf(kind: string): string {
  if (kind === "timeout") return "the read didn't finish in time";
  if (kind === "BudgetStopError") return "the API budget stopped it";
  if (kind === "ReconnectionNeeded") return "Salesforce refused the connection's token";
  if (kind === "no code") return "the reply held no currency code";
  return kind;
}

/**
 * The naming step's connection, its token work tracked (DECISIONS.md 745, fix
 * round): a refresh a time-out left running must finish, and store any token
 * it rotated, before the run's own connection loads the stored one, or the run
 * would present a refresh token Salesforce has already rotated out.
 */
class SettlingProvider implements ConnectionProvider {
  readonly kind: ConnectionProvider["kind"];
  readonly #inner: ConnectionProvider;
  readonly #pending = new Set<Promise<unknown>>();
  constructor(inner: ConnectionProvider) {
    this.#inner = inner;
    this.kind = inner.kind;
  }
  connect(): Promise<OrgConnection> {
    return this.#track(this.#inner.connect());
  }
  refresh(): Promise<OrgConnection> {
    return this.#track(this.#inner.refresh());
  }
  accepted(): void {
    this.#inner.accepted?.();
  }
  #track<T>(work: Promise<T>): Promise<T> {
    this.#pending.add(work);
    const done = () => void this.#pending.delete(work);
    work.then(done, done);
    return work;
  }
  /** Every connect and refresh begun has finished, its token stored. Each is bounded by the transport's own time limit. */
  async settled(): Promise<void> {
    while (this.#pending.size > 0) await Promise.allSettled([...this.#pending]);
  }
}

/** A delivery that can't tell which calibration is stored, before its run has started: thrown so the queue delivers it again, with nothing pinned. */
export class CalibrationUnsettled extends Error {}

/**
 * The calibration the run goes on with: the saved one with its currency named
 * from the org's settings, or the saved one as it was. The caller has checked
 * the run (automatic, the switch on, an OAuth connection to the run's own org)
 * and that this delivery starts it afresh. Throws only CalibrationUnsettled,
 * and only on a delivery that isn't the last (fix round): after a write that
 * threw or matched nothing, the run goes on with the row as stored, never one
 * that may differ from it, or a resumed delivery would find its calibration
 * changed. On the last delivery an unreadable row costs nothing more than
 * before: the run goes on with the calibration it had, and nothing resumes it.
 */
export async function nameCurrencyBeforeRun(o: { runId: string; input: RunJobInput; stored: StoredConnection; provider: ConnectionProvider; dir: string; deps: WorkerDeps; log: (line: string) => void; readThisStart?: boolean; final?: boolean; settleMs?: number }): Promise<Calibration> {
  const provider = new SettlingProvider(o.provider);
  try {
    return await naming({ ...o, provider });
  } finally {
    // Bounded: a refresh still running after this is left, and the run starts as it would have before 745.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = await Promise.race([provider.settled().then(() => false), new Promise<boolean>((resolve) => (timer = setTimeout(() => resolve(true), o.settleMs ?? SETTLE_MS)))]);
    clearTimeout(timer);
    if (late) o.log(`Automatic run ${o.runId}'s connection for the org's settings was still refreshing its token after ${Math.round((o.settleMs ?? SETTLE_MS) / 1000)} seconds, so the run starts without waiting for it.`);
  }
}

/** The longest the run waits for the naming step's token work to finish (745, second fix round). */
export const SETTLE_MS = 15_000;

async function naming(o: { runId: string; input: RunJobInput; stored: StoredConnection; provider: ConnectionProvider; dir: string; deps: WorkerDeps; log: (line: string) => void; readThisStart?: boolean; final?: boolean }): Promise<Calibration> {
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
  let unread = "the org couldn't say whether it has one currency";
  try {
    // Its own small budget, as a connect's reads have (a fixed half of the daily limit, as connect-job's: neither takes HC_API_BUDGET_SHARE).
    const opened = await inTime(SalesforceClient.connect(o.provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: deps.dataTransport, sleep: deps.dataTransport ? async () => {} : undefined }), timeoutMs);
    if (opened === "timeout") {
      log(`Automatic run ${runId}'s org's settings weren't read for its currency (the connection didn't open in time), so the calibration's currency is left as it was.`);
      return c;
    }
    currency = await readOrgCurrency(opened, { log, timeoutMs, suggest: false, unread: (kind) => (unread = causeOf(kind)) });
  } catch (err) {
    const kind = err instanceof Error ? err.name : "";
    log(`Automatic run ${runId}'s org's settings weren't read for its currency (${kind === "BudgetStopError" || kind === "ReconnectionNeeded" ? causeOf(kind) : safeReason(err, [stored.accessToken, stored.refreshToken])}), so the calibration's currency is left as it was.`);
    return c;
  }
  if (currency.kind === "code") {
    log(`Automatic run ${runId}'s org now gives its own currency code, so it may have several currencies: the org's settings weren't read and the calibration's currency is left as it was.`);
    return c;
  }
  if (currency.kind === "unread") {
    log(`Automatic run ${runId}'s org's settings weren't read for its currency (the currency query failed: ${unread}), so the calibration's currency is left as it was.`);
    return c;
  }
  // "single": readOrgCurrency has said why the org's settings couldn't name it, in one line.
  if (currency.kind !== "settings") return c;
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
    // The write may have been committed before the error reached us (a connection dropped after COMMIT): the row decides.
    const reason = err instanceof StoreError ? err.message : "an unexpected error";
    log(`Automatic run ${runId}'s currency from the org's settings (${currency.code}) may not have been saved (${reason}), so the run goes on with the calibration as stored.`);
    return asStored(o, c, currency.code);
  }
  if (saved === "unchanged") {
    // Another delivery named it, or a person confirmed one, since this run read it: the store's is the one kept.
    log(`Automatic run ${runId}'s calibration changed while the org's settings were read, so the run goes on with the one saved now.`);
    return asStored(o, c);
  }
  logCurrency(runId, named.data, answered, log);
  log(`Automatic run ${runId}'s saved calibration named no currency from Salesforce, so the org's settings were read before it ran: its currency is now ${currency.code}, and nothing else in it changed.`);
  return named.data;
}

/**
 * The calibration as the store holds it now. Unreadable, a delivery the queue
 * will make again stops here, before the run's folder records any calibration;
 * the last goes on with the one it had, as before 745.
 */
async function asStored(o: { runId: string; deps: WorkerDeps; log: (line: string) => void; final?: boolean }, c: Calibration, maySay?: string): Promise<Calibration> {
  let now: Awaited<ReturnType<WorkerDeps["store"]["loadRunJob"]>>;
  try {
    now = await o.deps.store.loadRunJob(o.runId);
  } catch {
    now = null;
  }
  if (now?.calibration) return now.calibration;
  if (!o.final) throw new CalibrationUnsettled(`Automatic run ${o.runId}'s calibration couldn't be read again after the currency from the org's settings was saved, so this delivery stops before the run starts and the queue delivers it again.`);
  o.log(`Automatic run ${o.runId}'s calibration couldn't be read again after the currency from the org's settings was saved, and this is the last delivery, so the run goes on with the calibration it had.`);
  // The write may have been committed: then the calibration page names the code while this run's results don't.
  if (maySay) o.log(`Automatic run ${o.runId}'s stored calibration may name ${maySay} from the org's settings while this run used the one it had (${c.currency === "XXX" ? "amounts in plain numbers" : c.currency}): a calibration page that disagrees with the run's results is this.`);
  return c;
}
