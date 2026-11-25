import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { orgStages } from "../../connect/org-stages.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { CURRENCY_NOT_NAMED, currencyStanding, NO_CURRENCY } from "../../engine/currency.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { Calibration, CURRENCY_CODES, isCurrencyCode } from "../../schemas/calibration.ts";
import { AutomaticReplay, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { HOURS_IN_POUNDS, valueCurrency } from "../../value/model.ts";
import { calibrationFromRow, calibrationRow, CUSTOMER_CURRENCY_SQL, PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { MemoryWorkerStore } from "../memory-store.ts";

// The currency a customer names in the portal (KAN-41): an "answer to unlock".
// It names an automatic run's amounts only where connect read that the org
// has one currency; never where Salesforce gave the org's own code, never
// where the read failed, and never from the locale's suggestion. A run made
// with it is exactly a run whose currency was read, and carries no line.

const ORG = "00D000000000001AAA";
const AT = "2026-11-25T09:00:00.000Z";
const STAGES = orgStages([
  { ApiName: "Prospecting", MasterLabel: "Prospecting", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 1 },
  { ApiName: "Closed Won", MasterLabel: "Closed Won", IsActive: true, IsClosed: true, IsWon: true, SortOrder: 2 },
  { ApiName: "Closed Lost", MasterLabel: "Closed Lost", IsActive: true, IsClosed: true, IsWon: false, SortOrder: 3 },
] as never);

const built = (currency: Parameters<typeof automaticCalibration>[1]["currency"], answered?: string | null): Calibration => {
  const b = automaticCalibration(STAGES, { orgId: ORG, at: AT, currency, answered });
  assert.ok(b.ok);
  return b.calibration;
};

describe("the customer's currency at calibration", () => {
  test("a single-currency org with an answer: the customer's code, marked as theirs, nothing suggested beside it", () => {
    const c = built({ kind: "single", suggested: "GBP" }, "EUR");
    assert.equal(c.currency, "EUR");
    assert.equal(c.currencyFrom, "customer");
    assert.equal(c.currencySuggested, undefined);
    assert.equal(currencyStanding(c), "customer");
  });

  test("never on a multi-currency org: Salesforce's own code stands, unmarked", () => {
    const c = built({ kind: "code", code: "USD" }, "EUR");
    assert.equal(c.currency, "USD");
    assert.equal(c.currencyFrom, undefined);
    assert.equal(built("USD", "EUR").currency, "USD");
  });

  test("never where the read failed: the org may hold several currencies", () => {
    const c = built({ kind: "unread" }, "EUR");
    assert.equal(c.currency, NO_CURRENCY);
    assert.equal(c.currencyFrom, "unread");
    assert.equal(built(null, "EUR").currencyFrom, "unread");
  });

  test("no answer: as before, the locale's suggestion kept apart and never the currency", () => {
    const c = built({ kind: "single", suggested: "GBP" });
    assert.equal(c.currency, NO_CURRENCY);
    assert.equal(c.currencyFrom, "org");
    assert.equal(c.currencySuggested, "GBP");
    assert.deepEqual(built({ kind: "single", suggested: "GBP" }, null), c);
  });

  test("an answer that isn't a currency's code is no answer", () => {
    for (const bad of ["XXX", "XTS", "eur", "EU", "EURO", "ZZZ", "", " EUR", 7 as unknown as string]) {
      const c = built({ kind: "single", suggested: null }, bad);
      assert.equal(c.currency, NO_CURRENCY, String(bad));
      assert.equal(c.currencyFrom, "org", String(bad));
    }
  });

  test("the schema: a customer's currency is a real code, on a calibration read from the org's settings only", () => {
    const base = built({ kind: "single", suggested: null }, "EUR");
    assert.ok(Calibration.safeParse(base).success);
    assert.ok(!Calibration.safeParse({ ...base, source: "confirmed" }).success, "never a person's calibration");
    assert.ok(!Calibration.safeParse({ ...base, currency: "XXX" }).success, "never no currency");
    assert.ok(!Calibration.safeParse({ ...base, currency: "ZZZ" }).success, "never a code Intl doesn't know");
  });

  test("the codes come from the runtime's Intl, without no currency or the testing code", () => {
    assert.ok(CURRENCY_CODES.size > 100);
    for (const c of ["GBP", "EUR", "USD", "JPY", "INR"]) assert.ok(isCurrencyCode(c), c);
    for (const c of ["XXX", "XTS", "gbp"]) assert.ok(!isCurrencyCode(c), c);
    for (const c of CURRENCY_CODES) assert.doesNotThrow(() => new Intl.NumberFormat("en-GB", { style: "currency", currency: c }).format(1), c);
  });

  test("the value model treats it as a currency read from the org: hours stay hours beside a currency that isn't pounds, and no line", () => {
    const c = built({ kind: "single", suggested: null }, "EUR");
    assert.deepEqual(valueCurrency(c), { unpricedHours: HOURS_IN_POUNDS("EUR") });
    assert.deepEqual(valueCurrency(built({ kind: "code", code: "EUR" })), valueCurrency(c));
    assert.deepEqual(valueCurrency(built({ kind: "single", suggested: null }, "GBP")), {});
  });
});

describe("the stores", () => {
  test("the row: the mark rides in the fields column and is read back, with nothing suggested", () => {
    const c = built({ kind: "single", suggested: "GBP" }, "EUR");
    const row = calibrationRow("run-x", c) as Record<string, unknown>;
    assert.equal(row.currency, "EUR");
    assert.deepEqual(row.fields, { currencyFrom: "customer" });
    const back = calibrationFromRow({ ...row, source: "org_settings", fields: row.fields });
    assert.equal(back.currencyFrom, "customer");
    assert.equal(back.currency, "EUR");
    assert.equal(back.currencySuggested, undefined);
  });

  test("memory: by the org's 15 characters, a code only, none without the table", async () => {
    const store = new MemoryWorkerStore();
    assert.equal(await store.customerCurrency(ORG.slice(0, 15)), null);
    store.customerCurrencies.set(ORG.slice(0, 15), "EUR");
    assert.equal(await store.customerCurrency(ORG), "EUR");
    store.customerCurrencies.set(ORG.slice(0, 15), "ZZZ");
    assert.equal(await store.customerCurrency(ORG), null);
    store.customerCurrencies.set(ORG.slice(0, 15), "EUR");
    store.customerAnswerTable = false;
    assert.equal(await store.customerCurrency(ORG), null);
  });

  class Pool implements SqlPool {
    readonly seen: Array<{ text: string; values: unknown[] }> = [];
    readonly answer: (text: string) => SqlResult;
    constructor(answer: (text: string) => SqlResult) {
      this.answer = answer;
    }
    async query(text: string, values: unknown[] = []): Promise<SqlResult> {
      this.seen.push({ text, values });
      return this.answer(text);
    }
    async connect(): Promise<SqlClient & { release(): void }> {
      return { query: (t, v) => this.query(t, v), release: () => {} };
    }
  }

  test("Postgres: one read-only statement for the active customer of the org, the newest answer, a code only", async () => {
    assert.match(CUSTOMER_CURRENCY_SQL, /^SELECT /);
    assert.doesNotMatch(CUSTOMER_CURRENCY_SQL, /\b(INSERT|UPDATE|DELETE)\b/);
    assert.match(CUSTOMER_CURRENCY_SQL, /c\."orgId15" = \$1 AND c\."status" = 'active' AND a\."question" = 'currency' ORDER BY a\."answeredAt" DESC/);
    const pool = new Pool(() => ({ rows: [{ value: "EUR" }], rowCount: 1 }));
    assert.equal(await new PostgresWorkerStore(pool).customerCurrency(ORG), "EUR");
    assert.deepEqual(pool.seen, [{ text: CUSTOMER_CURRENCY_SQL, values: [ORG.slice(0, 15)] }]);
    assert.equal(await new PostgresWorkerStore(new Pool(() => ({ rows: [{ value: "XXX" }], rowCount: 1 }))).customerCurrency(ORG), null);
    assert.equal(await new PostgresWorkerStore(new Pool(() => ({ rows: [], rowCount: 0 }))).customerCurrency(ORG), null);
  });

  test("Postgres: no table yet is no answer, said once; any other fault is thrown by its SQLSTATE alone", async () => {
    const lines: string[] = [];
    const missing = new PostgresWorkerStore(new Pool(() => { throw Object.assign(new Error("relation does not exist"), { code: "42P01" }); }), { log: (l) => lines.push(l) });
    assert.equal(await missing.customerCurrency(ORG), null);
    assert.equal(await missing.customerCurrency(ORG), null);
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /20261219_health_check_portal_answers\.sql/);
    const broken = new PostgresWorkerStore(new Pool(() => { throw Object.assign(new Error("EUR planted"), { code: "57014" }); }));
    await assert.rejects(broken.customerCurrency(ORG), (err: Error) => /SQLSTATE 57014/.test(err.message) && !err.message.includes("planted"));
  });
});

describe("a run made with the customer's currency (synthetic org)", async () => {
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const calibrate = (currency: Parameters<typeof automaticCalibration>[1]["currency"], answered?: string): Calibration => {
    const b = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency, answered });
    assert.ok(b.ok);
    return b.calibration;
  };
  const run = async (calibration: Calibration) => loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  const answered = await run(calibrate({ kind: "single", suggested: "GBP" }, "EUR"));
  const read = await run(calibrate("EUR"));
  const MADE = "2026-11-25T09:00:00.000Z";

  test("its findings and metrics are exactly those of a run that read EUR from the org: amounts named, no flag, no line", () => {
    assert.deepEqual(answered.input.findings, read.input.findings);
    assert.deepEqual(answered.input.metrics, read.input.metrics);
    assert.ok(answered.input.findings.some((f) => f.status === "issue" && f.value.kind !== "not_sized" && f.value.unit === "EUR"));
    assert.ok(answered.input.findings.every((f) => f.amountsUnnamed === undefined));
    assert.ok(!JSON.stringify(answered.input.metrics).includes(CURRENCY_NOT_NAMED));
  });

  test("its digest names the currency and carries no note and no suggestion", () => {
    const d = checkedDigest({ input: answered.input, pseudonyms: answered.pseudonyms, inventory: null, madeAt: MADE });
    assert.equal(d.held, null);
    assert.equal(d.digest!.value.currency, "EUR");
    assert.equal(d.digest!.value.currencyNote, undefined);
    assert.equal(d.digest!.value.currencySuggested, undefined);
    assert.deepEqual(d.digest!.value, checkedDigest({ input: read.input, pseudonyms: read.pseudonyms, inventory: null, madeAt: MADE }).digest!.value);
    const html = fullReportHtml(buildReport(answered.input), answered.pseudonyms);
    assert.ok(!html.includes("which you haven't told us yet"));
    assert.ok(!html.includes(CURRENCY_NOT_NAMED.replace("'", "&#39;")) && !html.includes(CURRENCY_NOT_NAMED));
  });
});
