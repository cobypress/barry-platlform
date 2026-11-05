import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { INVESTMENT, investmentMetrics } from "../../domains/utilisation/investment.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Extractor } from "../../extract/types.ts";
import type { LicenceData, PackageLicenseRow, UserLicenseRow } from "../../extract/users.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, currentBody, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections } from "../../render/references.ts";
import { Metric, type Calibration } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { buildInvestment } from "../investment.ts";
import { isPaidLicence } from "../licences.ts";
import { LIST_PRICES, listPrice } from "../list-prices.ts";
import { costPerSeat } from "../seat-cost.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const calibration = loadCalibration();
const metric = (metrics: Metric[], id: string) => metrics.find((m) => m.id === id)!;
const value = (metrics: Metric[], id: string): number => {
  const m = metric(metrics, id);
  assert.equal(m.status, "measured", m.status === "not_measurable" ? `${id}: ${m.reason}` : id);
  return (m as Extract<Metric, { status: "measured" }>).value;
};
const reason = (metrics: Metric[], id: string): string => {
  const m = metric(metrics, id);
  assert.equal(m.status, "not_measurable", id);
  return (m as Extract<Metric, { status: "not_measurable" }>).reason;
};

/** The synthetic run's own extracts, with some replaced. */
function ctxWith(overrides: Record<string, unknown> = {}, cal: Calibration = calibration): Omit<RuleContext, "metrics"> {
  const now = new Date(run.asOf);
  return {
    calibration: cal,
    now,
    window: run.analysisWindow,
    pseudonyms,
    data: <T>(extractor: Extractor<T>): T => (overrides[extractor.id] ?? JSON.parse(readFileSync(join(dir, "checkpoints", `${extractor.id}.json`), "utf8"))) as T,
    ev: (source, input = {}) => ({ ...source, count: input.count ?? null, dateRange: input.dateRange ?? null, exclusions: input.exclusions ?? [], note: input.note, capturedAt: now.toISOString() }) as never,
    manual: () => {
      throw new Error("no manual steps here");
    },
    userLabel: () => "a user",
  };
}

const licences = (): LicenceData => JSON.parse(readFileSync(join(dir, "checkpoints", "licences.json"), "utf8")) as LicenceData;
const ok = <T>(data: T, query = "SELECT") => ({ status: "ok" as const, data, source: { kind: "soql" as const, api: "rest" as const, query } });
const withLicences = (userLicenses: UserLicenseRow[], packageLicenses: PackageLicenseRow[] = []) => ({ licences: { ...licences(), userLicenses: ok(userLicenses), packageLicenses: ok(packageLicenses) } });

describe("investment metrics, on the synthetic org's traps", () => {
  const metrics = output!.metrics;

  test("every one is in the run, valid, under the investment group", () => {
    for (const id of Object.values(INVESTMENT)) assert.equal(Metric.parse(metric(metrics, id)).domain, "investment", id);
  });

  test("seats count paid licence types only: Chatter Free's 5,000 aren't seats anyone pays for", () => {
    assert.equal(value(metrics, INVESTMENT.seatsPaid), 40);
    assert.equal(value(metrics, INVESTMENT.seatsUsed), 25);
    assert.equal(value(metrics, INVESTMENT.seatsUnused), 15);
    const unused = metric(metrics, INVESTMENT.seatsUnused) as Extract<Metric, { status: "measured" }>;
    assert.deepEqual(unused.segments.map((s) => [s.key, s.value]), [["Salesforce", 15], ["Salesforce Platform", 0]]);
  });

  test("a licence with no price leaves the total unpriced, rather than understated", () => {
    assert.match(reason(metrics, INVESTMENT.seatCost), /Salesforce Platform has no cost per seat/);
    // Only Salesforce seats are unused, and the calibration prices them.
    assert.equal(value(metrics, INVESTMENT.unusedSeatCost), 15 * 1200);
    assert.match(reason(metrics, INVESTMENT.listPriceCost), /haven't confirmed which of Salesforce's current plans Enterprise Edition is priced as/);
  });

  test("package seats: only active packages sold by the seat", () => {
    assert.equal(value(metrics, INVESTMENT.packageSeatsPaid), 20);
    assert.equal(value(metrics, INVESTMENT.packageSeatsUnused), 8);
    const paid = metric(metrics, INVESTMENT.packageSeatsPaid) as Extract<Metric, { status: "measured" }>;
    assert.deepEqual(paid.segments.map((s) => s.key), ["acmecpq"]);
  });

  test("active-user share: recent logins over seats in use", () => {
    const share = metric(metrics, INVESTMENT.activeUserShare) as Extract<Metric, { status: "measured" }>;
    assert.equal(share.status, "measured");
    assert.ok(share.stats.activeUsers! > 0 && share.stats.activeUsers! < 25);
    assert.equal(share.value, Math.round((share.stats.activeUsers! / 25) * 1000) / 10);
  });
});

describe("investment metrics, on clean data", () => {
  const clean = withLicences([
    { Name: "Salesforce", TotalLicenses: 20, UsedLicenses: 20, Status: "Active" },
    { Name: "Chatter Free", TotalLicenses: 5000, UsedLicenses: 2, Status: "Active" },
  ]);
  const metrics = investmentMetrics(ctxWith(clean));

  test("nothing unused, nothing to pay for", () => {
    assert.equal(value(metrics, INVESTMENT.seatsUnused), 0);
    assert.equal(value(metrics, INVESTMENT.unusedSeatCost), 0);
    assert.equal(value(metrics, INVESTMENT.seatCost), 20 * 1200);
    assert.equal(value(metrics, INVESTMENT.packageSeatsPaid), 0);
  });

  test("a priced free licence counts: the calibration says they pay for it", () => {
    const priced = investmentMetrics(ctxWith(withLicences([{ Name: "Salesforce Integration", TotalLicenses: 5, UsedLicenses: 1, Status: "Active" }]), { ...calibration, licenceCosts: { "Salesforce Integration": 120 } }));
    assert.equal(value(priced, INVESTMENT.seatsPaid), 5);
    assert.equal(value(priced, INVESTMENT.unusedSeatCost), 4 * 120);
    assert.equal(isPaidLicence({ Name: "Salesforce Integration", TotalLicenses: 5, Status: "Active" }, {}), false);
    assert.equal(isPaidLicence({ Name: "Salesforce", TotalLicenses: 5, Status: "Disabled" }, {}), false);
  });
});

describe("not measurable", () => {
  const unavailable = { status: "unavailable" as const, reason: "INSUFFICIENT_ACCESS: no", source: { kind: "soql" as const, api: "rest" as const, query: "SELECT" } };

  test("when licences can't be read, every seat and cost figure says so, and packages still count", () => {
    const metrics = investmentMetrics(ctxWith({ licences: { ...licences(), userLicenses: unavailable } }));
    for (const id of [INVESTMENT.seatsPaid, INVESTMENT.seatsUsed, INVESTMENT.seatsUnused, INVESTMENT.seatCost, INVESTMENT.unusedSeatCost, INVESTMENT.listPriceCost, INVESTMENT.activeUserShare]) {
      assert.match(reason(metrics, id), /Licences couldn't be read: INSUFFICIENT_ACCESS/, id);
    }
    assert.equal(value(metrics, INVESTMENT.packageSeatsPaid), 20);
  });

  test("when packages can't be read, only the package figures say so", () => {
    const metrics = investmentMetrics(ctxWith({ licences: { ...licences(), packageLicenses: unavailable } }));
    assert.match(reason(metrics, INVESTMENT.packageSeatsPaid), /Installed packages' licences couldn't be read/);
    assert.equal(value(metrics, INVESTMENT.seatsPaid), 40);
  });

  test("when users can't be read, or no seat is in use, there's no active share", () => {
    assert.match(reason(investmentMetrics(ctxWith({ users: unavailable })), INVESTMENT.activeUserShare), /Users couldn't be read/);
    assert.match(reason(investmentMetrics(ctxWith(withLicences([{ Name: "Salesforce", TotalLicenses: 5, UsedLicenses: 0, Status: "Active" }]))), INVESTMENT.activeUserShare), /No paid seat is in use/);
  });

  test("when Opportunity can't be read, nothing is published (DECISIONS 37)", () => {
    const opportunities = JSON.parse(readFileSync(join(dir, "checkpoints", "opportunities.json"), "utf8"));
    const metrics = investmentMetrics(ctxWith({ opportunities: { ...opportunities, opportunities: unavailable } }));
    assert.ok(metrics.every((m) => m.status === "not_measurable"));
    assert.equal(metrics.length, Object.keys(INVESTMENT).length);
  });
});

describe("cost per seat", () => {
  const pricing = { calibration: { licenceCosts: { Salesforce: 1200 }, currency: "GBP" }, edition: "Enterprise Edition" };
  const plans = { "Enterprise Edition": "Core" };

  test("the calibration first, then list price, each saying which it is", () => {
    assert.deepEqual(costPerSeat("Salesforce", pricing), { licence: "Salesforce", annual: 1200, currency: "GBP", basis: "calibration", note: "1200 GBP a year, from the calibration" });
    const list = costPerSeat("Salesforce", { ...pricing, calibration: { licenceCosts: {}, currency: "GBP" }, plans });
    assert.ok("annual" in list && list.annual === 156 * 12 && list.basis === "list_price");
    assert.match("note" in list ? list.note : "", /list price for Core \(156 GBP a user a month, billed annually; https:\/\/www\.salesforce\.com\/uk\/sales\/pricing\/, read on 2026-11-05\)/);
  });

  test("no price is ever guessed", () => {
    for (const p of [
      costPerSeat("Salesforce Platform", { ...pricing, plans }),
      costPerSeat("Salesforce", { ...pricing, calibration: { licenceCosts: {}, currency: "GBP" } }),
      costPerSeat("Salesforce", { ...pricing, calibration: { licenceCosts: {}, currency: "USD" }, plans }),
      costPerSeat("Salesforce", { ...pricing, calibration: { licenceCosts: {}, currency: "GBP" }, edition: null, plans }),
    ]) assert.ok("reason" in p && !("annual" in p));
  });

  test("every list price has a source on salesforce.com and the date it was read", () => {
    for (const p of LIST_PRICES) {
      assert.match(p.url, /^https:\/\/www\.salesforce\.com\//);
      assert.match(p.checked, /^\d{4}-\d{2}-\d{2}$/);
      assert.ok(p.monthly > 0);
    }
    assert.equal(listPrice("Salesforce", "Enterprise Edition", "GBP").status, "unpriced");
  });
});

describe("annual cost, by precedence", () => {
  const priced = investmentMetrics(ctxWith(withLicences([{ Name: "Salesforce", TotalLicenses: 20, UsedLicenses: 15, Status: "Active" }])));
  const intake = (spend?: number) => Intake.parse({ summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", ...(spend ? { salesforceSpendAnnual: spend } : {}) });

  test("the customer's own total wins, labelled, with list price beside it for context", () => {
    const investment = buildInvestment(priced, intake(50_000), "GBP");
    assert.deepEqual(investment.annualCost, { status: "measured", value: 50_000, basis: "customer", label: "What you told us you spend on Salesforce a year" });
    assert.equal(investment.listPriceContext?.status, "not_measurable");
  });

  test("then seats at the calibration's cost per seat", () => {
    const investment = buildInvestment(priced, intake(), "GBP");
    assert.equal(investment.annualCost.status === "measured" && investment.annualCost.value, 20 * 1200);
    assert.equal(investment.annualCost.status === "measured" && investment.annualCost.basis, "calibration");
  });

  test("then list price, labelled as not what they pay", () => {
    const metrics = priced.map((m) => (m.id === INVESTMENT.seatCost && m.status === "measured" ? { ...m, stats: { calibrationPriced: 0, listPriced: 1 } } : m));
    const investment = buildInvestment(metrics, null, "GBP");
    assert.equal(investment.annualCost.status === "measured" && investment.annualCost.basis, "list_price");
    assert.match(investment.annualCost.status === "measured" ? investment.annualCost.label : "", /list price, which isn't what you pay/);
    assert.equal(investment.listPriceContext, null);
  });

  test("not measurable when there's neither their figure nor a price for every seat", () => {
    const investment = buildInvestment(output!.metrics, intake(), "GBP");
    assert.equal(investment.annualCost.status, "not_measurable");
    assert.match(investment.annualCost.status === "not_measurable" ? investment.annualCost.reason : "", /^You haven't told us what you spend, and the seats can't be priced/);
  });
});

describe("in the raw notes", () => {
  const input: ReportInput = {
    run,
    calibration,
    findings: output!.findings,
    metrics: output!.metrics,
    manualSteps: output!.manualSteps,
    intake: Intake.parse({ summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", salesforceSpendAnnual: 48_500 }),
  };
  const report = buildReport(input);

  test("an Investment section after the Barometer score, numbering every investment metric", () => {
    const sections = noteSections(report);
    const at = sections.findIndex((s) => s.key === "investment");
    assert.equal(sections[at - 1]!.key, "barometer_score");
    assert.deepEqual(sections[at]!.items.map((i) => i.id), Object.values(INVESTMENT));
    assert.ok(!sections.find((s) => s.key === "pipeline_metrics")!.items.some((i) => i.id.startsWith("investment.")));
  });

  test("states the annual cost and where it came from, and passes the number check", () => {
    const body = currentBody(report.outputs.raw_notes, "investment");
    assert.match(body, /Annual cost: £48,500\. What you told us you spend on Salesforce a year\./);
    assert.match(body, /You pay for 40 Salesforce seats\. 25 are in use and 15 aren't\./);
    const section = report.outputs.raw_notes.sections.find((s) => s.key === "investment")!;
    const version = report.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!;
    assert.deepEqual(version.numberCheck.unmatched, []);
    assert.equal(report.investment.annualCost.status, "measured");
  });

  test("the Word notes print each investment metric under its reference", async () => {
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    const section = noteSections(report).find((s) => s.key === "investment")!;
    assert.match(text, new RegExp(`${section.ref} +Investment`));
    assert.match(text, new RegExp(`${section.items[0]!.ref} +Seats paid for: +40`));
  });
});
