import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { SF_DEFAULTS } from "../../config.ts";
import { authorise } from "../../salesforce/allowlist.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { ORG_SETTINGS_VARIANTS, Simulator, type OrgSettingsVariant } from "../../testing/simulator.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { currencyFromSettings, ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL, readOrgCurrency } from "../org-currency.ts";

// The org's settings through the Connect REST API's Organization resource
// (DECISIONS.md 740), the one read the owner approved on 25 November 2026 for
// the currency of an org with one currency. Its real shape is NOT VERIFIED
// (docs/QUESTIONS.md): every variant here is a shape we might meet, and only the
// ones that pass every check name a currency. Anything else costs only that read:
// the org reads exactly as before 740, one currency with none named.

const SETTINGS_PATH = /\/services\/data\/v\d+\.\d\/connect\/organization$/;

/** A single-currency synthetic org: its Organization has no DefaultCurrencyIsoCode, so the query answers INVALID_FIELD. */
async function readSingle(variant: OrgSettingsVariant, options: { currency?: string; timeoutMs?: number } = {}) {
  const sim = new Simulator({ dropFields: { Organization: ["DefaultCurrencyIsoCode"] }, orgSettings: variant, orgSettingsCurrency: options.currency });
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: sim, sleep: async () => {} });
  const before = client.budget.callsMade;
  const lines: string[] = [];
  const currency = await readOrgCurrency(client, { log: (l) => void lines.push(l), timeoutMs: options.timeoutMs ?? 200 });
  const paths = sim.requests.map((r) => new URL(r.url));
  return {
    currency,
    lines,
    calls: client.budget.callsMade - before,
    settingsCalls: paths.filter((u) => SETTINGS_PATH.test(u.pathname)).length,
    queries: paths.map((u) => u.searchParams.get("q")).filter((q): q is string => q !== null),
  };
}

/** What the synthetic org's single-currency read gave before 740: one currency, nothing suggested (its Organization has no locale). */
const AS_BEFORE = { kind: "single", suggested: null };
/** Values in the simulated answer that are the org's: none may reach the log. */
const ORG_VALUES = ["Synthetic Co", ORG_ID, "maintenance", "Chatter is disabled", "JPY"];

const settingsLines = (lines: string[]) => lines.filter((l) => /org's settings/.test(l));

describe("the settings read (simulator, every variant)", () => {
  test("the route passes the real allowlist, GET only", () => {
    assert.equal(authorise({ method: "GET", path: "/services/data/v66.0/connect/organization" }), "connect.organization");
  });

  const accepted: Array<[OrgSettingsVariant, "top" | "features"]> = [
    ["top", "top"],
    ["features", "features"],
    ["both", "top"],
  ];
  for (const [variant, where] of accepted) {
    test(`${variant}: the code is the org's one currency, the locale isn't read, and one call is spent`, async () => {
      const r = await readSingle(variant, { currency: "EUR" });
      assert.deepEqual(r.currency, { kind: "settings", code: "EUR" });
      assert.deepEqual(r.queries, [ORG_CURRENCY_SOQL]);
      assert.equal(r.settingsCalls, 1);
      assert.equal(r.calls, 2, "the query and the settings: one call each against the budget");
      assert.deepEqual(settingsLines(r.lines), [`The org's settings name its one currency: EUR (Connect REST API, ${where === "top" ? "at the top level" : "inside features"}).`]);
      for (const v of ORG_VALUES) assert.ok(!r.lines.join("\n").includes(v), v);
    });
  }

  const refused: Array<[OrgSettingsVariant, RegExp, boolean]> = [
    ["both_disagree", /names two different currencies/, true],
    ["multi_true", /doesn't say multiCurrency is false, though the query said the org has one currency/, true],
    ["multi_absent", /doesn't say multiCurrency is false beside its currency/, true],
    ["off_list", /isn't a code on our list/, true],
    ["404", /Salesforce answered NOT_FOUND, HTTP 404/, false],
    ["403", /Salesforce answered FUNCTIONALITY_NOT_ENABLED, HTTP 403/, false],
    ["500", /Salesforce answered UNKNOWN_EXCEPTION, HTTP 500/, false],
    ["not_json", /the answer wasn't JSON/, false],
    ["hang", /the read didn't finish in time/, false],
  ];
  for (const [variant, why, shape] of refused) {
    test(`${variant}: not usable, one line saying why, and the org reads exactly as before`, async () => {
      const r = await readSingle(variant, { timeoutMs: variant === "hang" ? 20 : 200 });
      assert.deepEqual(r.currency, AS_BEFORE);
      assert.deepEqual(r.queries, [ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL], "then the locale, as before 740");
      const said = settingsLines(r.lines);
      assert.equal(said.length, 1, said.join("\n"));
      assert.match(said[0]!, why);
      assert.match(said[0]!, /so its amounts are shown with no currency named, as before\./);
      // An answer of the wrong shape says its key names, so the first real run shows the shape; never a value.
      if (shape) assert.match(said[0]!, /Its answer's fields: name, orgId, accessTimeout, .*features, userSettings; inside features: chatterEnabled, communitiesEnabled/);
      else assert.doesNotMatch(said[0]!, /Its answer's fields/);
      for (const v of ORG_VALUES) assert.ok(!r.lines.join("\n").includes(v), `${v} in ${said[0]}`);
      // Retried only as every read is: a 500 is tried again by the client's own policy, then given up.
      assert.equal(r.settingsCalls, variant === "500" ? 1 + SF_DEFAULTS.maxRetries : 1);
    });
  }

  test("every variant the simulator has is tested above", () => {
    assert.deepEqual([...accepted.map(([v]) => v), ...refused.map(([v]) => v)].sort(), [...ORG_SETTINGS_VARIANTS].sort());
  });
});

describe("the acceptance rule, shape by shape", () => {
  const ok = (body: unknown) => currencyFromSettings(body);
  test("at the top level or inside features, with multiCurrency exactly false beside it", () => {
    assert.deepEqual(ok({ defaultCurrencyIsoCode: "GBP", multiCurrency: false }), { ok: true, code: "GBP", where: "top" });
    assert.deepEqual(ok({ features: { defaultCurrencyIsoCode: "JPY", multiCurrency: false } }), { ok: true, code: "JPY", where: "features" });
    assert.deepEqual(ok({ defaultCurrencyIsoCode: "GBP", multiCurrency: false, features: { defaultCurrencyIsoCode: "GBP", multiCurrency: false } }), { ok: true, code: "GBP", where: "top" });
  });

  test("refused: multiCurrency absent, true, a string or anywhere not false; a code off the list, not upper case, or not a string", () => {
    for (const body of [
      { defaultCurrencyIsoCode: "GBP" },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: true },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: "false" },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: 0 },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: null },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: false, features: { multiCurrency: true } },
      { multiCurrency: false, features: { defaultCurrencyIsoCode: "GBP" } },
      { defaultCurrencyIsoCode: "XXX", multiCurrency: false },
      { defaultCurrencyIsoCode: "XTS", multiCurrency: false },
      { defaultCurrencyIsoCode: "HRK", multiCurrency: false },
      { defaultCurrencyIsoCode: "gbp", multiCurrency: false },
      { defaultCurrencyIsoCode: " GBP", multiCurrency: false },
      { defaultCurrencyIsoCode: ["GBP"], multiCurrency: false },
      { defaultCurrencyIsoCode: null, multiCurrency: false },
      { DefaultCurrencyIsoCode: "GBP", multiCurrency: false },
      { currencyIsoCode: "GBP", multiCurrency: false },
      { userSettings: { userDefaultCurrencyIsoCode: "GBP" }, multiCurrency: false },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: false, features: { defaultCurrencyIsoCode: "EUR", multiCurrency: false } },
      { defaultCurrencyIsoCode: "GBP", multiCurrency: false, features: { defaultCurrencyIsoCode: "QQQ", multiCurrency: false } },
      { features: [{ defaultCurrencyIsoCode: "GBP", multiCurrency: false }] },
      { organization: { defaultCurrencyIsoCode: "GBP", multiCurrency: false } },
      [{ defaultCurrencyIsoCode: "GBP", multiCurrency: false }],
      "GBP",
      null,
      42,
    ]) {
      assert.equal(ok(body).ok, false, JSON.stringify(body));
    }
  });

  test("key names only, of identifier shape, at most 60: a key that isn't one is counted, never written", () => {
    const r = currencyFromSettings({ "Mrs Example's org": 1, okName: 2, ...Object.fromEntries(Array.from({ length: 70 }, (_, i) => [`k${i}`, i])) });
    assert.ok(!r.ok);
    assert.ok(r.keys);
    assert.equal(r.keys.top.length, 61);
    assert.equal(r.keys.top.at(-1), "(12 more)");
    assert.ok(!r.keys.top.some((k) => k.includes("Example")));
  });
});
