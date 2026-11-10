import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { domainLabel, isThinData, THIN_DATA_SHARE } from "../../narrative/select.ts";
import { checkNumbers, scopeDataset, SOURCE } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import type { Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { executiveHtml, executiveMaxPages } from "../executive.ts";
import { deckRangeFor, fullReportHtml } from "../full-report.ts";
import { launchBrowser, layoutOf } from "../pdf.ts";

// DECISIONS.md 450: an org with too little data hears it once, at the front, not
// area by area. The base sample has 2 of 102 checks not measurable; the thin copy
// turns whole areas, and part of others, not measurable to pass a third.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const issue = output!.findings.find((f) => f.status === "issue")!;
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast can't be trusted.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [issue.id], assessment: "The data supports this." }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake };

const REASON = "Fewer than 30 records in the window, so the share can't be read";
const unmeasured = (f: Finding): Finding => ({ ...f, status: "not_measurable", notMeasurableReason: REASON });

// Three areas with nothing measured, forecast with only its pass left, and most
// of data quality, so each kind of area page is exercised.
const WHOLE = new Set(["automation", "security", "integrations", "forecast"]);
const PART = "data_quality";
const PART_COUNT = 8;
function thinInput(): ReportInput {
  let part = 0;
  const findings = output!.findings.map((f) => (f.status === "issue" && (WHOLE.has(f.domain) || (f.domain === PART && part++ < PART_COUNT)) ? unmeasured(f) : f));
  return { ...base, findings };
}

const decode = (html: string): string => html.replace(/<style>[\s\S]*?<\/style>/g, "").replace(/<[^>]+>/g, " ").replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&").replace(/\s+/g, " ");
const pagesOf = (html: string): string[] => html.split(/<section class="page/).slice(1).map((p) => p.split("</section>")[0]!);
const pageWith = (html: string, marker: string): string => {
  const page = pagesOf(html).find((p) => p.includes(marker));
  assert.ok(page, `a page with ${marker}`);
  return page;
};
const counts = (report: BuiltReport) => ({ checks: report.teamFindings.length, open: report.teamFindings.filter((f) => f.status === "not_measurable").length });
const front = (n: number, of: number, appendix: string): string => `We couldn't measure ${n} of the ${of} checks: this org holds too little data for them. ${appendix} names each, with the reason. The pages that follow cover the ones we could.`;

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("a thin org", () => {
  const report = buildReport(thinInput());
  const { checks, open } = counts(report);
  const exec = executiveHtml(report, pseudonyms);
  const full = fullReportHtml(report, pseudonyms);

  test("is past the threshold, and the base sample isn't", () => {
    assert.equal(checks, 102);
    assert.equal(open, 35);
    assert.ok(open / checks > THIN_DATA_SHARE);
    assert.ok(isThinData(report.teamFindings));
    assert.ok(!isThinData(buildReport(base).teamFindings));
  });

  test("the Verdict page says it once, with the counts", () => {
    const verdict = decode(pageWith(exec, '<div class="eyebrow">Verdict</div>'));
    assert.ok(verdict.includes(front(open, checks, "The full report's appendix")), verdict);
    assert.ok(!verdict.includes("names each check we couldn't measure, and why"));
  });

  test("the Overview says it too", () => {
    const overview = decode(pageWith(full, '<div class="eyebrow">Overview</div>'));
    assert.ok(overview.includes(front(open, checks, "The appendix")), overview);
  });

  test("its counts trace to the summary's own figures, not to a figure that happens to match", () => {
    // The whole dataset holds many small numbers, so the layout check alone would pass a wrong count.
    const summary = scopeDataset(report.dataset, [SOURCE.summary]);
    assert.deepEqual(checkNumbers(front(open, checks, "The appendix"), summary).unmatched, []);
    // How many we could measure isn't a summary figure, which is why the sentence doesn't give it.
    assert.equal(checkNumbers(`The pages that follow cover the ${checks - open} we could.`, summary).passed, false);
  });

  test("no area page says it again", () => {
    // The chapters are there, so the note has somewhere it could have gone.
    assert.ok(pagesOf(full).some((p) => p.includes(`<div class="eyebrow">${domainLabel(PART)}</div>`) && p.includes("At a glance")));
    // So is the forecast, with its one pass, among what's working well.
    const working = decode(pageWith(full, "<h2>What&#39;s working well"));
    assert.ok(working.includes(`${domainLabel("forecast")} No issues in the one check we could make here.`), working);
    const text = decode(full);
    assert.doesNotMatch(text, /couldn't be measured here/);
    assert.doesNotMatch(text, /couldn't be measured, and the appendix/);
    assert.ok(!decode(exec).includes("couldn't be measured here"));
  });

  test("where we can't give a view holds only the areas with nothing measured", () => {
    const slide = pageWith(full, "<h2>Where we can&#39;t give a view yet");
    const listed = [...slide.matchAll(/<strong>([^<]+)<\/strong>/g)].map((m) => decode(m[1]!).trim());
    const byLabel = new Map(report.teamFindings.map((f) => [domainLabel(f.domain), f.domain]));
    for (const domain of ["automation", "security", "integrations"]) assert.ok(listed.includes(domainLabel(domain)), domain);
    for (const label of listed) {
      const domain = byLabel.get(label);
      if (domain) assert.ok(report.teamFindings.filter((f) => f.domain === domain).every((f) => f.status === "not_measurable"), label);
    }
  });

  test("the appendix still names every check we couldn't measure", () => {
    const appendix = decode(pagesOf(full).filter((p) => p.includes("<h2>What we couldn&#39;t measure")).join(" "));
    for (const f of report.teamFindings.filter((x) => x.status === "not_measurable")) assert.ok(appendix.includes(f.title), f.title);
  });

  test("the verdict's brief tells Claude to say it once, from counts its sources hold", () => {
    const brief = report.briefs.get(`${run.id}:executive_summary:verdict`)!;
    assert.ok(brief.purpose.includes(`${open} of the ${checks} checks couldn't be measured, because this org holds too little data for them`), brief.purpose);
    assert.match(brief.purpose, /Say so once, briefly/);
    const summary = brief.sources.find((s) => s.kind === "summary")!;
    assert.equal(summary.checks, checks);
    assert.equal(summary.notMeasurable, open);
  });

  test("both documents pass every layout and number check", async () => {
    const execLayout = await layoutOf(browser, exec, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
    assert.deepEqual(execLayout.problems, [], JSON.stringify(execLayout.problems.slice(0, 8)));
    const range = deckRangeFor(run);
    const fullLayout = await layoutOf(browser, full, { minPages: range.min, maxPages: range.max, dataset: report.dataset });
    assert.deepEqual(fullLayout.problems, [], JSON.stringify(fullLayout.problems.slice(0, 8)));
  });
});

describe("the base sample, below the threshold", () => {
  const report = buildReport(base);
  const exec = executiveHtml(report, pseudonyms);
  const full = fullReportHtml(report, pseudonyms);

  test("keeps the Verdict page's line naming each check", () => {
    assert.deepEqual(counts(report), { checks: 102, open: 2 });
    const verdict = decode(pageWith(exec, '<div class="eyebrow">Verdict</div>'));
    assert.ok(verdict.includes("Not measurable: Forecast accuracy; Dashboard use. The full report's appendix says why for each."), verdict);
    assert.ok(!verdict.includes("this org holds too little data"));
  });

  test("keeps each area's note, and says nothing on the Overview", () => {
    assert.equal([...decode(full).matchAll(/1 more check couldn't be measured here\. The appendix says why\./g)].length, 2);
    assert.ok(!decode(full).includes("this org holds too little data"));
  });

  test("the verdict's brief says nothing about thin data", () => {
    // Not the exact purpose: other briefs work (decision 440) adds to it, and this is only about 450.
    const { purpose } = report.briefs.get(`${run.id}:executive_summary:verdict`)!;
    assert.match(purpose, /^Tell an executive reader, in a few sentences, what state this Salesforce org is in and what matters most\./);
    assert.doesNotMatch(purpose, /too little data|couldn't be measured/);
  });
});
