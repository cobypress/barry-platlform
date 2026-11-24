import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { authorise } from "../../salesforce/allowlist.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { COUNTRY_CURRENCY, currencyForLocale, localeCountry } from "../locale-currency.ts";
import { ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL, readOrgCurrency } from "../org-currency.ts";

// An automatic run's currency at connect (DECISIONS.md 700): the org's own
// default currency where it has several, else one taken from its default
// locale and said as an assumption, else none. Never a hold.

type Answer = { field?: unknown } | { error: string } | "hang";

/** An org answering the two Organization reads as told, through the real client and allowlist. */
class Org implements Transport {
  readonly asked: string[] = [];
  readonly currency: Answer;
  readonly locale: Answer;
  constructor(currency: Answer, locale: Answer = { error: "NOT_ASKED" }) {
    this.currency = currency;
    this.locale = locale;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    const url = new URL(request.url);
    const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
    if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
    if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    const q = url.searchParams.get("q") ?? "";
    this.asked.push(q);
    const answer = q === ORG_CURRENCY_SOQL ? this.currency : q === ORG_LOCALE_SOQL ? this.locale : { error: "UNEXPECTED" };
    if (answer === "hang") return new Promise(() => {});
    if ("error" in answer) return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: answer.error, message: "No such column 'DefaultCurrencyIsoCode' on entity 'Organization'" }]) };
    const field = q === ORG_CURRENCY_SOQL ? "DefaultCurrencyIsoCode" : "DefaultLocaleSidKey";
    return ok({ records: [{ attributes: { type: "Organization" }, [field]: answer.field }], totalSize: 1, done: true });
  }
}

async function read(org: Org, timeoutMs?: number) {
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: org, sleep: async () => {} });
  const lines: string[] = [];
  const currency = await readOrgCurrency(client, { log: (l) => void lines.push(l), timeoutMs });
  return { currency, lines };
}

describe("the currency read at connect", () => {
  test("both queries pass the real allowlist as it stands, through the query route", () => {
    for (const soql of [ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL]) {
      assert.equal(authorise({ method: "GET", path: `/services/data/v68.0/query?q=${encodeURIComponent(soql)}` }), "rest.query");
    }
  });

  test("a multi-currency org: its own default currency, read and kept, and the locale never asked", async () => {
    const org = new Org({ field: "EUR" });
    const { currency } = await read(org);
    assert.deepEqual(currency, { code: "EUR", from: "org" });
    assert.deepEqual(org.asked, [ORG_CURRENCY_SOQL]);
  });

  test("a single-currency org answers INVALID_FIELD, its normal answer: the locale is read and mapped, and said as taken from it", async () => {
    const org = new Org({ error: "INVALID_FIELD" }, { field: "en_GB" });
    const { currency, lines } = await read(org);
    assert.deepEqual(currency, { code: "GBP", from: "locale" });
    assert.deepEqual(org.asked, [ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL]);
    // Said as the normal answer it is, never as a failure; the locale itself is never logged.
    assert.ok(lines.some((l) => /The org has one currency: Salesforce has no DefaultCurrencyIsoCode on its Organization/.test(l)), lines.join("\n"));
    assert.ok(!lines.some((l) => /wasn't read/.test(l)));
    assert.ok(!lines.some((l) => l.includes("en_GB")));
  });

  test("a locale with no country, or one we can't map with certainty: no currency, and nothing guessed", async () => {
    for (const locale of ["en", "bg_BG", "es_VE", "xx_ZZ", "", 42]) {
      const { currency, lines } = await read(new Org({ error: "INVALID_FIELD" }, { field: locale }));
      assert.deepEqual(currency, { code: null, from: "none" }, String(locale));
      assert.ok(lines.some((l) => /names no country whose currency we can be sure of/.test(l)));
    }
  });

  test("a failed read: no currency, and any other error than INVALID_FIELD never reads the locale", async () => {
    const refused = new Org({ error: "INSUFFICIENT_ACCESS" }, { field: "en_GB" });
    assert.deepEqual((await read(refused)).currency, { code: null, from: "none" });
    assert.deepEqual(refused.asked, [ORG_CURRENCY_SOQL]);
    const noLocale = new Org({ error: "INVALID_FIELD" }, { error: "INSUFFICIENT_ACCESS" });
    const { currency, lines } = await read(noLocale);
    assert.deepEqual(currency, { code: null, from: "none" });
    assert.ok(lines.some((l) => /default locale wasn't read \(INSUFFICIENT_ACCESS\)/.test(l)));
    assert.deepEqual((await read(new Org({ field: "pounds" }))).currency, { code: null, from: "none" });
    assert.deepEqual((await read(new Org("hang"), 20)).currency, { code: null, from: "none" });
  });
});

describe("the locale table", () => {
  test("reads the country from Salesforce's locale keys, with a script or a variant", () => {
    assert.equal(localeCountry("en_GB"), "GB");
    assert.equal(localeCountry("de_DE_EURO"), "DE");
    assert.equal(localeCountry("sr_Latn_RS"), "RS");
    assert.equal(localeCountry("en"), null);
    assert.equal(localeCountry("EN_GB"), null);
  });

  test("maps the countries it names, the euro's included, and nothing else", () => {
    assert.equal(currencyForLocale("en_GB"), "GBP");
    assert.equal(currencyForLocale("en_US"), "USD");
    assert.equal(currencyForLocale("fr_FR"), "EUR");
    assert.equal(currencyForLocale("de_AT_EURO"), "EUR");
    assert.equal(currencyForLocale("hr_HR"), "EUR");
    assert.equal(currencyForLocale("sh_ME"), "EUR");
    assert.equal(currencyForLocale("ja_JP"), "JPY");
    assert.equal(currencyForLocale("de_CH"), "CHF");
    for (const unsure of ["bg_BG", "es_PA", "km_KH", "ar_LB", "es_VE", "en_ZW", "es_CU"]) assert.equal(currencyForLocale(unsure), null, unsure);
    for (const [country, code] of Object.entries(COUNTRY_CURRENCY)) {
      assert.match(country, /^[A-Z]{2}$/);
      assert.match(code, /^[A-Z]{3}$/);
      assert.notEqual(code, "XXX");
    }
  });
});
