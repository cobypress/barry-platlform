import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { orgProfile } from "../../benchmark/export.ts";
import { automaticCalibration } from "../../connect/auto-calibration.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { CURRENCY_UNKNOWN, currencyFromLocaleText, NO_CURRENCY } from "../../engine/currency.ts";
import { buildReport } from "../../narrative/sections.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";
import { AutomaticReplay, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { buildValueModel, HOURS_IN_POUNDS, valueCurrency } from "../../value/model.ts";

// An automatic run's currency over the synthetic org (DECISIONS.md 700): read
// from the org, taken from its locale (an assumption every money figure says),
// or none (every money figure not measurable, the rest measured as before).

const first = await runFromRecording();
const stages = stagesOf(first.dir);
const calibrate = (currency: Parameters<typeof automaticCalibration>[1]["currency"]): Calibration => {
  const built = automaticCalibration(stages, { orgId: first.run.orgId, at: "2026-10-31T09:00:00.000Z", currency });
  assert.ok(built.ok);
  return built.calibration;
};
const run = async (calibration: Calibration) => {
  const { dir } = await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() });
  return { ...loadRun(dir), dir };
};

const read = await run(calibrate("GBP"));
const locale = await run(calibrate({ code: "GBP", from: "locale" }));
const none = await run(calibrate(null));
const NOTE = currencyFromLocaleText("GBP");

/** Every string a run serves: findings, metrics, sections, digest, and the full report and one-pager as printed. */
function served(loaded: Pick<ReturnType<typeof loadRun>, "input" | "pseudonyms">): string[] {
  const report = buildReport(loaded.input);
  const digest = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" }).digest;
  const strings = (v: unknown): string[] => (typeof v === "string" ? [v] : Array.isArray(v) ? v.flatMap(strings) : v && typeof v === "object" ? Object.values(v).flatMap(strings) : []);
  // The raw notes are ours, never the customer's (as automatic-wording.test.ts reads them).
  const bodies = Object.values(report.outputs).filter((o) => o.output !== "raw_notes").flatMap((o) => o.versions.map((v) => v.body));
  // The digest's value model keeps its currency code, ISO 4217's "XXX" with none, a code the portal reads and never prints as an amount's.
  const { currency: _code, ...value } = digest!.value;
  return [...strings(loaded.input.findings), ...strings(loaded.input.metrics), ...bodies, ...strings({ ...digest, value }), fullReportHtml(report, loaded.pseudonyms), onePagerHtml(report, loaded.pseudonyms)];
}

const byId = <T extends { id: string }>(xs: T[]) => new Map(xs.map((x) => [x.id, x]));
const money = (f: Finding) => f.status === "issue" && f.value.kind !== "not_sized" && /^[A-Z]{3}$/.test(f.value.unit);
const saysAmount = (x: unknown) => JSON.stringify(x).includes("£");

describe("an automatic run whose currency was taken from the org's locale", () => {
  test("every money figure says so, and no finding that leans on it is surer than medium", () => {
    const withMoney = locale.input.findings.filter((f) => money(f) || saysAmount(f));
    assert.ok(withMoney.length >= 12, `${withMoney.length}`);
    for (const f of withMoney) {
      assert.ok(f.summary.includes(NOTE), `${f.ruleId}: ${f.summary.slice(-160)}`);
      assert.notEqual(f.confidence, "high", f.ruleId);
    }
    const metrics = locale.input.metrics.filter((m): m is Extract<Metric, { status: "measured" }> => m.status === "measured" && (m.unit === "currency" || saysAmount(m)));
    assert.ok(metrics.length >= 10);
    for (const m of metrics) assert.ok(m.assumptions.includes(NOTE), m.id);
  });

  test("nothing else changes: the same findings and metrics, measured the same, as a run that read the org's own currency", () => {
    const r = byId(read.input.findings);
    assert.equal(locale.input.findings.length, read.input.findings.length);
    for (const f of locale.input.findings) {
      const was = r.get(f.id)!;
      assert.equal(f.status, was.status, f.ruleId);
      if (!f.summary.includes(NOTE)) assert.deepEqual(f, was, f.ruleId);
    }
    const rm = byId(read.input.metrics);
    for (const m of locale.input.metrics) {
      const was = rm.get(m.id)!;
      assert.equal(m.status, was.status, m.id);
      if (m.status === "measured" && was.status === "measured") assert.equal(m.value, was.value, m.id);
    }
  });

  test("nothing is held: its digest is released, the total carries the sentence marked, and the pages print it", () => {
    const checked = checkedDigest({ input: locale.input, pseudonyms: locale.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" });
    assert.equal(checked.held, null);
    assert.equal(checked.digest!.value.currencyNote, NOTE);
    assert.deepEqual(checked.digest!.value.orgWords, { currencyNote: { orgWordsMarked: true, orgWords: [{ start: NOTE.indexOf("GBP"), end: NOTE.indexOf("GBP") + 3 }] } });
    const report = buildReport(locale.input);
    const html = fullReportHtml(report, locale.pseudonyms);
    assert.ok(html.includes(NOTE.replace(/'/g, "&#39;")));
    assert.ok(html.includes("GBP, taken from your Salesforce locale and not confirmed"));
    // A run that read the org's own currency says none of it.
    assert.ok(!served(read).some((t) => t.includes("Salesforce locale")));
  });
});

describe("an automatic run with no currency", () => {
  test("is never held for it, and its digest is released", () => {
    assert.equal(checkedDigest({ input: none.input, pseudonyms: none.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" }).held, null);
  });

  test("serves no amount and no code in a currency anywhere, and nothing that names a calibration or a consultant", () => {
    for (const t of served(none)) {
      assert.ok(!t.includes(NO_CURRENCY), t.slice(Math.max(0, t.indexOf(NO_CURRENCY) - 120), t.indexOf(NO_CURRENCY) + 40));
      assert.ok(!t.includes("£"), t.slice(Math.max(0, t.indexOf("£") - 120), t.indexOf("£") + 40));
    }
    for (const t of [...served(none), ...served(locale)].filter((x) => x.length < 5_000)) assert.ok(!/consultant|calibrat/i.test(t), t.slice(0, 200));
    assert.ok(!none.input.findings.some(money));
    assert.deepEqual(buildReport(none.input).value.total, { low: 0, expected: 0, high: 0 });
  });

  test("exactly these become not measurable, or lose their figure, for the plain reason; the rest are measured as before", () => {
    const r = byId(read.input.findings);
    const nowUnmeasured = none.input.findings.filter((f) => f.status === "not_measurable" && r.get(f.id)!.status !== "not_measurable");
    assert.deepEqual(nowUnmeasured.map((f) => f.ruleId).sort(), ["lead.campaign-return", "lead.source-return"]);
    for (const f of nowUnmeasured) {
      assert.equal(f.notMeasurableReason, CURRENCY_UNKNOWN);
      assert.equal(f.notMeasurableKind, "needs_input");
    }
    const unsized = none.input.findings.filter((f) => f.status === "issue" && f.value.kind === "not_sized" && r.get(f.id)!.value.kind !== "not_sized");
    assert.deepEqual(unsized.map((f) => f.ruleId).sort(), [
      "cur.account-concentration", "cur.close-date-pushes", "cur.stale-deals", "cur.stuck-in-stage", "dq.open-opps-no-activity", "dq.open-opps-past-close",
      "dq.opps-inactive-owner", "dq.stale-leads", "fct.commit-at-risk", "lead.no-first-response", "rev.stalled-deals",
    ]);
    for (const f of unsized) assert.ok(f.value.kind === "not_sized" && f.value.reason.endsWith(CURRENCY_UNKNOWN), f.ruleId);
    // Everything else, counts, rates and texts, exactly as a run that read the currency.
    const changed = new Set([...nowUnmeasured, ...unsized].map((f) => f.id));
    for (const f of none.input.findings.filter((x) => !changed.has(x.id))) assert.deepEqual(f, r.get(f.id), f.ruleId);

    const rm = byId(read.input.metrics);
    const metricsNow = none.input.metrics.filter((m) => m.status === "not_measurable" && rm.get(m.id)!.status === "measured");
    assert.deepEqual(metricsNow.map((m) => m.id).sort(), [
      "current.delivery_outlook_value", "current.open_pipeline", "current.weighted_pipeline", "historic.pipeline_point_in_time_value", "historic.quarterly_median_won_size",
      "historic.quarterly_won_value", "historic.won_value_by_customer", "historic.won_value_by_source", "historic.won_value_by_type", "lead.won_per_lead",
    ]);
    for (const m of metricsNow) {
      assert.equal(m.unit, "currency");
      assert.ok(m.status === "not_measurable" && m.reason === CURRENCY_UNKNOWN && m.notMeasurableKind === "needs_input", m.id);
    }
    const moved = new Set(metricsNow.map((m) => m.id));
    for (const m of none.input.metrics.filter((x) => !moved.has(x.id))) assert.deepEqual(m, rm.get(m.id), m.id);
  });

  test("the schema's pounds never apply: the calibration keeps ISO 4217's code for none", () => {
    assert.equal(none.input.calibration.currency, NO_CURRENCY);
    assert.equal(none.input.calibration.currencyFrom, "none");
  });
});

describe("money beyond the findings", () => {
  test("hours are priced in pounds only where the currency is pounds, or the customer gave their own hourly cost", () => {
    const person = read.input.calibration;
    assert.deepEqual(valueCurrency({ ...person, source: "confirmed" }), {});
    assert.deepEqual(valueCurrency(calibrate("GBP")), {});
    assert.deepEqual(valueCurrency(calibrate("EUR")), { unpricedHours: HOURS_IN_POUNDS("EUR") });
    assert.deepEqual(valueCurrency(calibrate("EUR"), 40), {});
    assert.deepEqual(valueCurrency(calibrate({ code: "GBP", from: "locale" })), { note: NOTE });
    assert.deepEqual(valueCurrency(calibrate({ code: "USD", from: "locale" })), { unpricedHours: HOURS_IN_POUNDS("USD"), note: currencyFromLocaleText("USD") });
    assert.deepEqual(valueCurrency(calibrate(null), 40), { unpricedHours: CURRENCY_UNKNOWN });
    const hours = { ...read.input.findings.find((f) => f.status === "issue")!, value: { kind: "hours" as const, amount: 10, unit: "hours", period: "per year", assumptions: ["a"] } };
    const model = buildValueModel([hours], read.input.run, NO_CURRENCY, null, null, valueCurrency(calibrate(null)));
    assert.deepEqual(model.left, [{ findingId: hours.id, reason: CURRENCY_UNKNOWN }]);
    assert.equal(model.total.expected, 0);
    assert.equal(buildValueModel([hours], read.input.run, "GBP", null, null, valueCurrency(calibrate("GBP"))).total.expected > 0, true);
  });

  test("the benchmark keeps out the deal size band of a run whose currency is unconfirmed or unknown", () => {
    for (const loaded of [locale, none]) assert.throws(() => orgProfile(loaded.dir, loaded.input.run, loaded.input.calibration), /currency/);
  });
});
