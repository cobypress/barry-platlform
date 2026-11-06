import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { executiveHtml } from "../executive.ts";
import { returnOnSpend } from "../money.ts";
import { compactMoney } from "../format.ts";
import { Intake } from "../../narrative/intake.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const intake = (extra: Record<string, unknown> = {}) => Intake.parse({ customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", ...extra });

describe("the executive summary, rebuilt around money", () => {
  const html = executiveHtml(buildReport(base), pseudonyms);
  const order = ["What fixing this is worth", "What you pay, and what you use", "How each area stands", "Your pipeline today", "What could hurt you", "What we'd do first", "What happens next", "How we worked it out"];

  test("reads in the order a CEO asks: worth, investment, standing, pipeline, risk, plan, next, working", () => {
    const at = order.map((h) => html.indexOf(h));
    for (const [i, h] of order.entries()) assert.ok(at[i]! > 0, h);
    assert.deepEqual([...at].sort((a, b) => a - b), at);
  });

  test("the risk page holds only serious issues money can't measure", () => {
    const report = buildReport(base);
    const risk = html.slice(html.indexOf("What could hurt you"), html.indexOf("What we'd do first"));
    const serious = report.teamFindings.filter((f) => f.status === "issue" && f.value.kind === "not_sized" && f.severity === "high");
    assert.ok(serious.some((f) => risk.includes(f.title.replace(/'/g, "&#39;"))));
    for (const f of report.teamFindings.filter((x) => x.value.kind !== "not_sized")) assert.ok(!risk.includes(`<h3>${f.title}</h3>`), f.title);
  });

  test("with the customer's own spend, the investment page states it, and what fixing it is worth against it", () => {
    const report = buildReport({ ...base, intake: intake({ salesforceSpendAnnual: 250_000 }) });
    const page = executiveHtml(report, pseudonyms);
    assert.match(page, /£250k/);
    assert.match(page, /What you told us you spend on Salesforce a year/);
    const ratio = returnOnSpend(report)!;
    assert.ok(ratio > 0);
    assert.match(page, new RegExp(`is ${ratio} times what you pay for Salesforce a year`));
  });

  test("without a cost, the investment page says it shows seats, and never why in pricing jargon", () => {
    assert.match(html, /so this page shows your seats rather than what they all cost/);
    assert.doesNotMatch(html, /current plans .* is priced as/);
  });

  test("the money table lists every line behind the total, says how each was counted, and ends with the total (DECISIONS.md 326)", () => {
    const report = buildReport(base);
    const m = (n: number) => compactMoney(n, report.value.currency);
    const money = html.slice(html.indexOf("What fixing this is worth"), html.indexOf("What you pay, and what you use"));
    const lines = report.value.groups.filter((g) => g.kind !== "exposure");
    assert.ok(lines.length > 5, "more lines than the table used to show");
    for (const g of lines) assert.ok(money.includes(`<td class="num">${m(g.counted.expected)}</td>`), g.label);
    assert.match(money, new RegExp(`Total, each counted once</td><td class="num">${m(report.value.total.expected)}</td>`));
    assert.match(money, /We count 50% \(25% to 100%\)/);
    assert.doesNotMatch(money, /the expected case is the conservative one/);
  });

  test("the investment page's unassigned seats are the money page's, by the same name, with the share counted", () => {
    const report = buildReport(base);
    const seats = report.value.groups.find((g) => g.id === "unassigned-seats")!;
    const page = html.slice(html.indexOf("What you pay, and what you use"), html.indexOf("How each area stands"));
    assert.match(page, /Seats nobody is assigned to/);
    assert.ok(page.includes(`We count a share of it, ${compactMoney(seats.counted.expected, report.value.currency)}, in what fixing this is worth.`));
    assert.doesNotMatch(page, /Seats nobody uses/);
  });
});
