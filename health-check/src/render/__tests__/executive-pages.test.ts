import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { EXECUTIVE_MAX_PAGES, executiveHtml, executiveMaxPages, RECHECK_EXECUTIVE_MAX_PAGES } from "../executive.ts";
import { fullReportHtml } from "../full-report.ts";
import { topIssues } from "../../narrative/select.ts";
import { returnOnSpend } from "../money.ts";
import { compactMoney } from "../format.ts";
import { Intake } from "../../narrative/intake.ts";
import type { Finding } from "../../schemas/index.ts";

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

  test("the checks behind the Not measurable tile are named, and the pipeline page's overlapping lines say so beside them", () => {
    const report = buildReport(base);
    const open = report.teamFindings.filter((f) => f.status === "not_measurable");
    assert.ok(open.length > 0 && open.length <= 3);
    for (const f of open) assert.ok(html.includes(f.title.replace(/'/g, "&#39;")), f.title);
    assert.match(html, /Not measurable: .*The full report&#39;s appendix says why for each\.|Not measurable: .*The full report's appendix says why for each\./);
    assert.match(html, /These lines overlap, so they aren(&#39;|')t added together\./);
  });

  test("pipeline needed for coverage is said as a target, not money, and doesn't lead as money (DECISIONS.md 351)", () => {
    const report = buildReport(base);
    const coverage = report.teamFindings.find((f) => f.ruleId === "cur.coverage" && f.status === "issue")!;
    assert.ok(coverage, "the synthetic run has a coverage gap");
    const deck = fullReportHtml(report, pseudonyms);
    assert.match(deck, /To reach 3x coverage of your £1\.2m quota this quarter, you(&#39;|')d need £2\.79m more pipeline/);
    assert.doesNotMatch(deck + html, /£2,785,000|more pipeline needed for the quarter/);
    const money = html.slice(html.indexOf("What fixing this is worth"), html.indexOf("What you pay, and what you use"));
    assert.doesNotMatch(money, /£2\.79m/);
    const top = topIssues(report.teamFindings, 5);
    const sized = top.filter((f) => f.value.kind !== "not_sized" && f.ruleId !== "cur.coverage" && f.severity === coverage.severity);
    for (const f of sized) assert.ok(top.indexOf(f) < (top.indexOf(coverage) === -1 ? Infinity : top.indexOf(coverage)), f.title);
  });

  test("a re-check's summary has 10% headroom over the first check's ceiling, as the decks do (DECISIONS.md 353)", () => {
    const first = buildReport(base);
    const fixed = new Set(first.teamFindings.filter((f) => f.status === "issue").map((f) => f.ruleId).slice(0, 3));
    const later = base.findings.map((f): Finding => (fixed.has(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
    const recheck = buildReport({ ...base, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, baseline: base });
    assert.ok(recheck.progress);
    assert.equal(executiveMaxPages(first), EXECUTIVE_MAX_PAGES);
    assert.equal(executiveMaxPages(recheck), RECHECK_EXECUTIVE_MAX_PAGES);
    assert.equal(RECHECK_EXECUTIVE_MAX_PAGES, Math.floor(EXECUTIVE_MAX_PAGES * 1.1));
  });

  test("what matters most ranks by what each issue adds to the total, after overlaps (DECISIONS.md 362)", () => {
    const report = buildReport(base);
    const top = topIssues(report.teamFindings, 50, report.added);
    const order = ["critical", "high", "medium", "low", "info"];
    for (let i = 1; i < top.length; i++) {
      const [a, b] = [top[i - 1]!, top[i]!];
      if (a.severity !== b.severity) continue;
      const [x, y] = [report.added.get(a.id) ?? -1, report.added.get(b.id) ?? -1];
      assert.ok(x >= y, `${a.title} (${x}) before ${b.title} (${y})`);
      // Equal adds, such as nothing, follow the issue's own figure.
      if (x === y && a.value.kind !== "not_sized" && b.value.kind !== "not_sized" && a.ruleId !== "cur.coverage" && b.ruleId !== "cur.coverage") assert.ok(a.value.amount >= b.value.amount, `${a.title} before ${b.title}`);
    }
    // An issue whose money sits inside a larger overlapping one adds nothing, so ranks below one that adds money.
    const held = report.value.groups.flatMap((g) => g.lines.filter((l) => l.findingId !== g.counted.findingId)).map((l) => l.findingId);
    assert.ok(held.length > 0);
    for (const id of held) assert.equal(report.added.get(id), 0);
    // The total is what the counted lines add.
    const sum = [...report.added.values()].reduce((s, n) => s + n, 0);
    assert.ok(Math.abs(sum - report.value.total.expected) < 1, `${sum} against ${report.value.total.expected}`);
    // And the summary's top issues follow it.
    const counted = (f: (typeof top)[number]) => order.indexOf(f.severity) * 1e12 - (report.added.get(f.id) ?? -1);
    const shown = topIssues(report.teamFindings, 5, report.added);
    assert.deepEqual(shown.map((f) => f.id), [...shown].sort((a, b) => counted(a) - counted(b) || 0).map((f) => f.id));
  });

  test("the investment page's unassigned seats are the money page's, by the same name, with the share counted", () => {
    const report = buildReport(base);
    const seats = report.value.groups.find((g) => g.id === "unassigned-seats")!;
    const page = html.slice(html.indexOf("What you pay, and what you use"), html.indexOf("How each area stands"));
    assert.match(page, /Seats nobody is assigned to/);
    assert.match(page, /Seats you pay for, all licence types/);
    assert.ok(page.includes(`We count a share of it, ${compactMoney(seats.counted.expected, report.value.currency)}, in what fixing this is worth.`));
    assert.doesNotMatch(page, /Seats nobody uses/);
  });
});
