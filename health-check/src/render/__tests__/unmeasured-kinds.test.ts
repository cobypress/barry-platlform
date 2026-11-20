import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { ALL_MODULES } from "../../domains/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { areasOf, isThinData, thinDataLine } from "../../narrative/select.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { checkNumbers, scopeDataset, SOURCE } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import type { Finding, NotMeasurableKind } from "../../schemas/index.ts";
import { buildScanResult } from "../../scan/result.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { executiveHtml, executiveMaxPages } from "../executive.ts";
import { deckRangeFor, fullReportHtml } from "../full-report.ts";
import { launchBrowser, layoutOf } from "../pdf.ts";

// DECISIONS.md 627: wherever a document lists or counts what couldn't be
// measured, it groups by kind, in plain words, and a run without kinds (or
// with nothing but checks we couldn't read) prints as it did before.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

const FORECAST = "No revenue forecast type is active, so Forecasting isn't in use for revenue, and we don't report a forecast figure.";
const LOSS = "The calibration doesn't name a loss reason field that exists on Opportunity, so we can't tell a blank reason from a missing field.";
const AMOUNTS = "207 of 398 open deals have no amount, so shares of the total would mislead.";
const unmeasured = (f: Finding, reason: string, kind: NotMeasurableKind): Finding => ({ ...f, status: "not_measurable", severity: "info", notMeasurableReason: reason, notMeasurableKind: kind, summary: reason, facts: {}, subjects: [], guards: [], recommendation: "", recommendationType: "none" });

// Every forecast check doesn't apply, one data quality check needs the loss
// reason field named, one current pipeline check has too little data, and the
// synthetic org's dashboard check, which we couldn't read, stays as it is: all four kinds.
function fourKinds(): Finding[] {
  let dq = 0;
  let cp = 0;
  return output!.findings.map((f) => {
    if (f.domain === "forecast") return unmeasured(f, FORECAST, "not_in_use");
    if (f.domain === "data_quality" && f.status === "issue" && dq++ === 0) return unmeasured(f, LOSS, "needs_input");
    if (f.domain === "current_pipeline" && f.status === "issue" && cp++ === 0) return unmeasured(f, AMOUNTS, "too_little_data");
    return f;
  });
}
const findings = fourKinds();
const scores = scoreDomains(areasOf(run, findings), findings);
const input: ReportInput = { ...base, findings, scores };
const strip = (list: Finding[]): Finding[] => list.map((f) => {
  const { notMeasurableKind: _, ...rest } = f;
  return rest as Finding;
});

const decode = (html: string): string => html.replace(/<style>[\s\S]*?<\/style>/g, "").replace(/<[^>]+>/g, " ").replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
const sectionBody = (report: BuiltReport, output: "raw_notes" | "full_health_check", key: string): string => {
  const built = report.outputs[output];
  const section = built.sections.find((s) => s.key === key)!;
  return built.versions.find((v) => v.id === section.currentVersionId)!.body;
};
const open = (report: BuiltReport) => report.teamFindings.filter((f) => f.status === "not_measurable");

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("a run with all four kinds", () => {
  const report = buildReport(input);
  const n = open(report);
  const tally = (kind: NotMeasurableKind) => n.filter((f) => f.notMeasurableKind === kind).length;
  const counted = `${tally("not_in_use")} don't apply to your org, 1 needs something from you, 1 has too little data to judge fairly and 1 we couldn't read`;
  const exec = executiveHtml(report, pseudonyms);
  const full = fullReportHtml(report, pseudonyms);

  test("has each kind, and isn't a thin report", () => {
    assert.ok(tally("not_in_use") > 1);
    assert.deepEqual([tally("needs_input"), tally("too_little_data"), tally("unreadable")], [1, 1, 1]);
    assert.ok(!isThinData(report.teamFindings));
  });

  test("the appendix counts them by kind", () => {
    assert.equal(sectionBody(report, "full_health_check", "appendix").split("\n\n")[1], `We couldn't measure ${n.length} checks: ${counted}. We report these rather than estimate them.`);
  });

  test("the exclusions and limits list each under its kind's heading, in order", () => {
    const body = sectionBody(report, "raw_notes", "exclusions_limits");
    const at = ["What we couldn't measure:", "Doesn't apply to your org:", "Needs something from you:", "Not enough data to judge fairly:", "We couldn't read this:"].map((h) => body.indexOf(`${h}\n`));
    assert.ok(at.every((i) => i >= 0), body);
    assert.deepEqual([...at].sort((a, b) => a - b), at);
    assert.ok(body.includes(`\n${LOSS}`) || body.includes(`: ${LOSS}`));
  });

  test("the raw notes group the Not measurable section and count by kind in the summary", () => {
    const md = renderRawNotes({ run, calibration: loadCalibration(), findings, metrics: output!.metrics, manualSteps: output!.manualSteps, pseudonyms, scores });
    assert.ok(md.includes(`${n.length} not measurable: ${counted}`), "summary");
    const section = md.slice(md.indexOf("## Not measurable"), md.indexOf("## Checks passed"));
    const at = ["**Doesn't apply to your org**", "**Needs something from you**", "**Not enough data to judge fairly**", "**We couldn't read this**"].map((h) => section.indexOf(h));
    assert.ok(at.every((i) => i >= 0));
    assert.deepEqual([...at].sort((a, b) => a - b), at);
  });

  test("the executive summary counts them by kind past three", () => {
    assert.ok(decode(exec).includes(`Of the ${n.length} checks we couldn't measure, ${counted}. The full report's appendix names each, and why.`));
  });

  test("the full report's appendix lists each kind under its heading", () => {
    const text = decode(full);
    for (const heading of ["Doesn't apply to your org", "Needs something from you", "Not enough data to judge fairly", "We couldn't read this"]) assert.ok(text.includes(heading), heading);
    assert.ok(!text.includes("What we couldn't measure Forecast"), "not one undivided list");
  });

  test("the area with nothing in use says so, not that we couldn't measure it", () => {
    const score = scores.find((s) => s.domain === "forecast")!;
    assert.equal(score.status === "not_measurable" && score.reason, "You don't use forecasting, so there is nothing to score.");
    assert.ok(decode(full).includes("You don't use forecasting, so there is nothing to score."));
    // The scan carries the area's kind, so the console can say the same.
    const area = buildScanResult(report).areas.find((a) => a.domain === "forecast");
    assert.equal(area?.notMeasurableKind, "not_in_use");
  });

  test("nothing new fails the number check", async () => {
    const summary = scopeDataset(report.dataset, [SOURCE.summary]);
    assert.deepEqual(checkNumbers(`We couldn't measure ${n.length} checks: ${counted}.`, summary).unmatched, []);
    assert.equal(checkNumbers(`We couldn't measure ${n.length} checks: ${n.length + 7} don't apply to your org.`, summary).passed, false, "a count the data doesn't hold is refused");
    const execLayout = await layoutOf(browser, exec, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
    assert.deepEqual(execLayout.problems, [], JSON.stringify(execLayout.problems.slice(0, 8)));
    const range = deckRangeFor(run);
    const fullLayout = await layoutOf(browser, full, { minPages: range.min, maxPages: range.max, dataset: report.dataset });
    assert.deepEqual(fullLayout.problems, [], JSON.stringify(fullLayout.problems.slice(0, 8)));
  });

  test("a thin report's front line says of what kind", () => {
    const thin = report.teamFindings.map((f) => (f.status === "issue" && ["automation", "security", "integrations", "configuration", "technical_health", "utilisation"].includes(f.domain) ? { ...f, status: "not_measurable" as const, notMeasurableReason: "Users couldn't be read: INSUFFICIENT_ACCESS", notMeasurableKind: "unreadable" as const } : f));
    assert.ok(isThinData(thin));
    assert.match(thinDataLine(thin, "The appendix"), /so this report says less than a full report would\. Of those, \d+ don't apply to your org, 1 needs something from you, 1 has too little data to judge fairly and \d+ we couldn't read\. The appendix names each/);
  });
});

describe("a run without kinds prints as before", () => {
  test("the same findings with no kinds give the old wording", () => {
    const report = buildReport({ ...input, findings: strip(findings) });
    const n = open(report).length;
    assert.equal(sectionBody(report, "full_health_check", "appendix").split("\n\n")[1], `We couldn't measure ${n} checks. We report these rather than estimate them.`);
    const body = sectionBody(report, "raw_notes", "exclusions_limits");
    assert.ok(body.includes("What we couldn't measure:\n") && !body.includes("Doesn't apply to your org"), body);
    assert.ok(decode(executiveHtml(report, pseudonyms)).includes("The full report's appendix names each check we couldn't measure, and why."));
  });

  test("the synthetic org, whose two are both unreadable, is byte for byte the same with or without kinds", () => {
    const withKinds = buildReport(base);
    const without = buildReport({ ...base, findings: strip(base.findings) });
    assert.ok(open(withKinds).every((f) => f.notMeasurableKind === "unreadable"));
    assert.equal(executiveHtml(withKinds, pseudonyms), executiveHtml(without, pseudonyms));
    assert.equal(fullReportHtml(withKinds, pseudonyms), fullReportHtml(without, pseudonyms));
    for (const key of ["exclusions_limits"]) assert.equal(sectionBody(withKinds, "raw_notes", key), sectionBody(without, "raw_notes", key));
    const md = (list: Finding[]) => renderRawNotes({ run, calibration: loadCalibration(), findings: list, metrics: output!.metrics, manualSteps: output!.manualSteps, pseudonyms });
    assert.equal(md(base.findings), md(strip(base.findings)));
  });
});
