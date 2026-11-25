import { SalesforceError, type SalesforceClient } from "../salesforce/client.ts";
import { isCurrencyCode } from "../schemas/calibration.ts";
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
//    Since 740, that org's settings are read once through the Connect REST
//    API's Organization resource, the one read the owner approved for this on
//    25 November 2026: a code it gives that passes every check below names the
//    currency, and the locale isn't read. Anything else costs only that read.
// c) Anything else is unread: the run decides from the opportunities' describe.
// The two queries go through the client's query route; the settings through
// its own route (allowlist.ts, connect.organization), and only in case (b).

export const ORG_CURRENCY_SOQL = "SELECT DefaultCurrencyIsoCode FROM Organization";
export const ORG_LOCALE_SOQL = "SELECT DefaultLocaleSidKey FROM Organization";

/** The time each read may take, in milliseconds. */
export const CURRENCY_TIMEOUT_MS = 10_000;

const ISO_CODE = /^[A-Z]{3}$/;

/**
 * What connect learnt of the currency (DECISIONS.md 700): the org's own code
 * (several currencies on); one currency, not known by name, with the locale's
 * suggestion or none; one currency, named by the org's settings (740); or
 * nothing, for the run to decide.
 */
export type OrgCurrency = { kind: "code"; code: string } | { kind: "single"; suggested: string | null } | { kind: "settings"; code: string } | { kind: "unread" };

const UNREAD: OrgCurrency = { kind: "unread" };

type Read<T> = { ok: true; value: T } | { ok: false; kind: string; status?: number };

/** One field of the one Organization row, in time, or the kind of failure. Never throws. */
function readField(client: SalesforceClient, soql: string, field: string, timeoutMs: number): Promise<Read<unknown>> {
  return inTime(client.query<Record<string, unknown>>(soql, { maxRecords: 1 }).then((r) => r.records[0]?.[field]), timeoutMs);
}

/** A read's answer in time, or the kind of failure, in our words or Salesforce's code alone. Never throws. */
async function inTime(work: Promise<unknown>, timeoutMs: number): Promise<Read<unknown>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  work.catch(() => {});
  const timeout = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), timeoutMs);
  });
  try {
    const read = await Promise.race([work, timeout]);
    return read === "timeout" ? { ok: false, kind: "timeout" } : { ok: true, value: read };
  } catch (err) {
    if (err instanceof SalesforceError) return { ok: false, kind: err.code.replace(/[^A-Z_0-9]/g, "").slice(0, 40), status: err.status };
    return { ok: false, kind: err instanceof Error ? err.name.replace(/[^A-Za-z]/g, "").slice(0, 40) : "not an error" };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * What the org says of its currency. Never throws, and logs only the kind of
 * each answer: never a value the org chose beyond a code.
 */
export async function readOrgCurrency(client: SalesforceClient, options: { log: (line: string) => void; timeoutMs?: number; suggest?: boolean; unread?: (kind: string) => void }): Promise<OrgCurrency> {
  const timeoutMs = options.timeoutMs ?? CURRENCY_TIMEOUT_MS;
  // False for a calibration already saved (745): its currency is only ever named, never suggested again, so
  // the locale isn't read and a settings answer we can't use leaves the calibration exactly as it was.
  const suggest = options.suggest !== false;
  const own = await readField(client, ORG_CURRENCY_SOQL, "DefaultCurrencyIsoCode", timeoutMs);
  if (own.ok) {
    if (typeof own.value === "string" && ISO_CODE.test(own.value)) return { kind: "code", code: own.value };
    options.log("The org's currency wasn't read: the reply held no currency code, so the run decides from its opportunities.");
    options.unread?.("no code");
    return UNREAD;
  }
  if (own.kind !== "INVALID_FIELD") {
    options.log(`The org's currency wasn't read (${own.kind === "timeout" ? "the read didn't finish in time" : own.kind}), so the run decides from its opportunities.`);
    options.unread?.(own.kind);
    return UNREAD;
  }
  // The normal answer of an org with one currency, not a failure (DECISIONS.md 700).
  options.log(
    suggest
      ? "The org has one currency: Salesforce has no DefaultCurrencyIsoCode on its Organization, as on every org without multiple currencies. Its settings are read for its name; if they can't say, its amounts are shown with no currency named and its default locale is read for a suggestion only."
      : "The org has one currency: Salesforce has no DefaultCurrencyIsoCode on its Organization, as on every org without multiple currencies. Its settings are read for its name; if they can't say, the calibration's currency is left as it was.",
  );
  // Only here, after the query's INVALID_FIELD: never on an org with several currencies or a failed read (740).
  const settings = await inTime(client.connectOrganization(), timeoutMs);
  const named: SettingsCurrency = settings.ok ? currencyFromSettings(settings.value) : { ok: false, why: failedWhy(settings), keys: null };
  if (named.ok) {
    options.log(`The org's settings name its one currency: ${named.code} (Connect REST API, ${named.where === "top" ? "at the top level" : "inside features"}).`);
    return { kind: "settings", code: named.code };
  }
  options.log(settingsUnusable(named.why, named.keys, suggest));
  if (!suggest) return { kind: "single", suggested: null };
  const locale = await readField(client, ORG_LOCALE_SOQL, "DefaultLocaleSidKey", timeoutMs);
  const suggested = locale.ok && typeof locale.value === "string" ? currencyForLocale(locale.value) : null;
  if (!locale.ok) options.log(`The org's default locale wasn't read (${locale.kind === "timeout" ? "the read didn't finish in time" : locale.kind}), so there's no currency to suggest.`);
  else if (!suggested) options.log("The org's default locale names no country whose currency we can be sure of, so there's no currency to suggest.");
  return { kind: "single", suggested };
}

// ─── The org's settings (DECISIONS.md 740) ───────────────────────────────────
//
// NOT VERIFIED (docs/QUESTIONS.md): Salesforce's documentation sample of the
// Connect REST API's Organization resource shows `defaultCurrencyIsoCode` and
// `multiCurrency`, but not for certain where they sit (the top level or a
// `features` object), what the code holds on an org with one currency, the
// version that brought them, the permission or edition they need, or whether
// Chatter must be on. So every doubt costs the read and never names a wrong
// currency: the code is taken only from a field of exactly that name, at the top
// level or inside a top-level `features`, on our fixed list, beside a
// `multiCurrency` of exactly false that agrees with the query's INVALID_FIELD.
// The real shape is to be read off the first real run's log.

export const SETTINGS_CODE_FIELD = "defaultCurrencyIsoCode";
export const SETTINGS_MULTI_FIELD = "multiCurrency";

/** The key names of an answer we couldn't use, so the first real run shows its shape: Salesforce's own field names, never a value. */
export type SettingsKeys = { top: string[]; features: string[] | null };
export type SettingsCurrency = { ok: true; code: string; where: "top" | "features" } | { ok: false; why: string; keys: SettingsKeys | null };

const plainObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const has = (o: Record<string, unknown>, k: string): boolean => Object.prototype.hasOwnProperty.call(o, k);
// A field name's shape only, at most 60 of them: a key that isn't one is counted, never written.
const KEY_NAME = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const MAX_KEYS = 60;
function keyNames(o: Record<string, unknown>): string[] {
  const all = Object.keys(o);
  const named = all.filter((k) => KEY_NAME.test(k)).slice(0, MAX_KEYS);
  const left = all.length - named.length;
  return left > 0 ? [...named, `(${left} more)`] : named;
}

/**
 * The one currency the Connect API's Organization answer names, or why it
 * can't be used. Accepted only when every check holds: an object; the code field
 * at the top level or inside `features` (both: they must agree); a code on
 * CURRENCY_CODES; and `multiCurrency` exactly false where the code is, and not
 * true anywhere. Never throws.
 */
export function currencyFromSettings(body: unknown): SettingsCurrency {
  if (!plainObject(body)) return { ok: false, why: "the answer wasn't an object", keys: null };
  const features = plainObject(body.features) ? body.features : null;
  const keys: SettingsKeys = { top: keyNames(body), features: features ? keyNames(features) : null };
  const no = (why: string): SettingsCurrency => ({ ok: false, why, keys });
  const places = ([["top", body], ["features", features]] as const).filter((p): p is readonly ["top" | "features", Record<string, unknown>] => p[1] !== null);
  if (places.some(([, o]) => has(o, SETTINGS_MULTI_FIELD) && o[SETTINGS_MULTI_FIELD] !== false)) return no("it doesn't say multiCurrency is false, though the query said the org has one currency");
  const coded = places.filter(([, o]) => has(o, SETTINGS_CODE_FIELD));
  if (coded.length === 0) return no("it has no defaultCurrencyIsoCode field");
  for (const [, o] of coded) {
    if (o[SETTINGS_MULTI_FIELD] !== false) return no("it doesn't say multiCurrency is false beside its currency");
    if (!isCurrencyCode(o[SETTINGS_CODE_FIELD])) return no("its currency isn't a code on our list");
  }
  const codes = new Set(coded.map(([, o]) => o[SETTINGS_CODE_FIELD]));
  if (codes.size !== 1) return no("it names two different currencies, at the top level and inside features");
  return { ok: true, code: coded[0]![1][SETTINGS_CODE_FIELD] as string, where: coded[0]![0] };
}

/** Why a settings read that didn't answer couldn't be used: Salesforce's code and status alone, never its message. */
function failedWhy(read: { kind: string; status?: number }): string {
  if (read.kind === "timeout") return "the read didn't finish in time";
  if (read.kind === "SyntaxError") return "the answer wasn't JSON";
  return read.status ? `Salesforce answered ${read.kind}, HTTP ${read.status}` : `the read stopped: ${read.kind}`;
}

/** The one line an unusable settings read gives: our words, and an answer's key names only. */
function settingsUnusable(why: string, keys: SettingsKeys | null, suggest: boolean): string {
  const shape = keys ? ` Its answer's fields: ${keys.top.join(", ") || "none"}${keys.features ? `; inside features: ${keys.features.join(", ") || "none"}` : "; no features object"}.` : "";
  return `The org's settings didn't name its currency (${why}), so ${suggest ? "its amounts are shown with no currency named, as before" : "the calibration's currency is left as it was"}.${shape}`;
}
