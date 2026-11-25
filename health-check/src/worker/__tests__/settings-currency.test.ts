import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { orgStages } from "../../connect/org-stages.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { CURRENCY_NOT_NAMED, currencyStanding, NO_CURRENCY } from "../../engine/currency.ts";
import { buildReport } from "../../narrative/sections.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { loadRun } from "../../render/render-run.ts";
import { Calibration } from "../../schemas/calibration.ts";
import { AutomaticReplay, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { HOURS_IN_POUNDS, valueCurrency } from "../../value/model.ts";
import { calibrationFromRow, calibrationRow } from "../postgres-store.ts";

// A single-currency org's currency as its settings name it through the Connect
// REST API (DECISIONS.md 740): "connect_api". It prints exactly as a currency
// read from the org: amounts named, no line, no flag. It wins over a customer's
// answer, and never applies to an org with several currencies or a failed read.

const ORG = "00D000000000001AAA";
const AT = "2026-11-25T09:00:00.000Z";
const STAGES = orgStages([
  { ApiName: "Prospecting", MasterLabel: "Prospecting", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 1 },
  { ApiName: "Closed Won", MasterLabel: "Closed Won", IsActive: true, IsClosed: true, IsWon: true, SortOrder: 2 },
] as never);

const built = (currency: Parameters<typeof automaticCalibration>[1]["currency"], answered?: string | null): Calibration => {
  const b = automaticCalibration(STAGES, { orgId: ORG, at: AT, currency, answered });
  assert.ok(b.ok);
  return b.calibration;
};

describe("the settings' currency at calibration", () => {
  test("the settings' code, marked as theirs, nothing suggested, standing connect_api", () => {
    const c = built({ kind: "settings", code: "EUR" });
    assert.deepEqual([c.currency, c.currencyFrom, c.currencySuggested], ["EUR", "connect_api", undefined]);
    assert.equal(currencyStanding(c), "connect_api");
  });

  test("Salesforce's answer wins over the customer's, whether they differ or agree", () => {
    for (const answered of ["GBP", "EUR", null]) {
      const c = built({ kind: "settings", code: "EUR" }, answered);
      assert.deepEqual([c.currency, c.currencyFrom], ["EUR", "connect_api"], String(answered));
    }
  });

  test("everything else is as before 740: the org's own code, one currency unnamed, the customer's answer, unread", () => {
    assert.deepEqual(built({ kind: "code", code: "USD" }, "EUR"), built("USD"));
    assert.equal(built({ kind: "code", code: "USD" }).currencyFrom, undefined);
    assert.deepEqual([built({ kind: "single", suggested: "GBP" }).currency, built({ kind: "single", suggested: "GBP" }).currencyFrom], [NO_CURRENCY, "org"]);
    assert.equal(built({ kind: "single", suggested: "GBP" }, "EUR").currencyFrom, "customer");
    assert.equal(built({ kind: "unread" }, "EUR").currencyFrom, "unread");
  });

  test("a settings code off the list is one currency unnamed, never a named one", () => {
    const c = built({ kind: "settings", code: "QQQ" });
    assert.deepEqual([c.currency, c.currencyFrom], [NO_CURRENCY, "org"]);
  });

  test("the schema: connect_api only with source org_settings and a code's shape", () => {
    const base = built({ kind: "settings", code: "EUR" });
    assert.equal(Calibration.safeParse({ ...base, source: "confirmed" }).success, false);
    assert.equal(Calibration.safeParse({ ...base, currency: NO_CURRENCY }).success, false);
    assert.equal(Calibration.safeParse({ ...base, currency: "XTS" }).success, false);
    // A stored code off the list still parses, as a customer's does, so a stored row never breaks a run.
    assert.equal(Calibration.safeParse({ ...base, currency: "QQQ" }).success, true);
  });

  test("the value model treats it as a currency read from the org", () => {
    assert.deepEqual(valueCurrency(built({ kind: "settings", code: "EUR" })), valueCurrency(built({ kind: "code", code: "EUR" })));
    assert.deepEqual(valueCurrency(built({ kind: "settings", code: "EUR" })), { unpricedHours: HOURS_IN_POUNDS("EUR") });
    assert.deepEqual(valueCurrency(built({ kind: "settings", code: "GBP" })), {});
  });

  test("the row: the mark rides in the fields column and is read back", () => {
    const c = built({ kind: "settings", code: "EUR" });
    const row = calibrationRow("run-x", c) as Record<string, unknown>;
    assert.deepEqual(row.fields, { currencyFrom: "connect_api" });
    const back = calibrationFromRow({ ...row, source: "org_settings" });
    assert.deepEqual([back.currency, back.currencyFrom, back.currencySuggested], ["EUR", "connect_api", undefined]);
  });
});

describe("a run made with the settings' currency (synthetic org)", async () => {
  const first = await runFromRecording();
  const stages = stagesOf(first.dir);
  const calibrate = (currency: Parameters<typeof automaticCalibration>[1]["currency"]): Calibration => {
    const b = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency });
    assert.ok(b.ok);
    return b.calibration;
  };
  const run = async (calibration: Calibration) => loadRun((await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir);
  const settings = await run(calibrate({ kind: "settings", code: "EUR" }));
  const read = await run(calibrate("EUR"));
  const plain = await run(calibrate({ kind: "single", suggested: null }));
  const MADE = "2026-11-25T09:00:00.000Z";
  const digest = (r: typeof read) => checkedDigest({ input: r.input, pseudonyms: r.pseudonyms, inventory: null, madeAt: MADE });

  test("byte for byte the run of an org whose own code was read: findings, metrics, digest, report", () => {
    assert.deepEqual(settings.input.findings, read.input.findings);
    assert.deepEqual(settings.input.metrics, read.input.metrics);
    assert.deepEqual(digest(settings).digest!.value, digest(read).digest!.value);
    assert.equal(fullReportHtml(buildReport(settings.input), settings.pseudonyms), fullReportHtml(buildReport(read.input), read.pseudonyms));
  });

  test("what changes from the same org with its currency unnamed: amounts named, the line and the flag gone", () => {
    const now = digest(settings);
    const before = digest(plain);
    assert.equal(now.held, null);
    assert.deepEqual([before.digest!.value.currency, now.digest!.value.currency], [NO_CURRENCY, "EUR"]);
    assert.ok(before.digest!.value.currencyNote);
    assert.equal(now.digest!.value.currencyNote, undefined);
    assert.ok(plain.input.findings.some((f) => f.amountsUnnamed === true));
    assert.ok(settings.input.findings.every((f) => f.amountsUnnamed === undefined));
    assert.ok(settings.input.findings.some((f) => f.status === "issue" && f.value.kind !== "not_sized" && f.value.unit === "EUR"));
    assert.ok(JSON.stringify(plain.input.metrics).includes(CURRENCY_NOT_NAMED));
    assert.ok(!JSON.stringify(settings.input.metrics).includes(CURRENCY_NOT_NAMED));
    const html = fullReportHtml(buildReport(settings.input), settings.pseudonyms);
    const untold = /which you haven(?:'|&#39;)t told us yet/;
    assert.doesNotMatch(html, untold);
    assert.match(fullReportHtml(buildReport(plain.input), plain.pseudonyms), untold);
  });
});
