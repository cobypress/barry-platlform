import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { strFromU8, unzipSync } from "fflate";
import { scoreDomains } from "../../domains/scoring.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, currentBody, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { SURVEY } from "../../survey/summary.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { SYNTHETIC_COMMENT, syntheticSurvey } from "../../testing/survey.ts";
import { buildValueModel, TEAM_TIME_GROUP, TEAM_TIME_LINE, WORKING_WEEKS } from "../../value/model.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { executiveHtml } from "../executive.ts";
import { DECK_RANGE, fullReportHtml } from "../full-report.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";
import { renderRawNotesDocx } from "../raw-notes-docx.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings), intake };
const withSurvey = (n: number): ReportInput => ({ ...base, survey: syntheticSurvey(n) });

const decode = (value: string): string => value.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&apos;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&");
async function notesText(report: BuiltReport): Promise<string> {
  const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
  return [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => decode(m[1]!)).join(" ");
}
const sectionOf = (report: BuiltReport, output: keyof BuiltReport["outputs"]) => report.outputs[output].sections.find((s) => s.key === "team_survey");

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("a run without a survey", () => {
  test("has no survey section, slide or value line", () => {
    const report = buildReport(base);
    assert.equal(report.survey, null);
    for (const output of ["executive_summary", "full_health_check", "raw_notes"] as const) assert.equal(sectionOf(report, output), undefined, output);
    assert.ok(!report.value.groups.some((g) => g.id === TEAM_TIME_GROUP));
    assert.ok(!fullReportHtml(report, pseudonyms).includes("What your team says"));
  });
});

describe("a survey that was sent and nobody answered", () => {
  test("says fewer than 5 answered, in every output, and nothing more", async () => {
    const report = buildReport(withSurvey(0));
    assert.equal(report.survey?.shown, false);
    const line = "Fewer than 5 people answered the team survey, so we don't show its results.";
    for (const output of ["executive_summary", "full_health_check", "raw_notes"] as const) assert.ok(currentBody(report.outputs[output], "team_survey").startsWith(line), output);
    // The pages escape the apostrophe, so they're read up to it.
    const start = "Fewer than 5 people answered the team survey, so we don";
    assert.ok(executiveHtml(report, pseudonyms).includes(start));
    assert.ok(!fullReportHtml(report, pseudonyms).includes("What your team says, against"));
    assert.ok((await notesText(report)).includes(line));
    assert.ok(buildMicrositePages(report, pseudonyms).pages[0]!.html.includes(start));
    assert.ok(!report.value.groups.some((g) => g.id === TEAM_TIME_GROUP));
  });
});

describe("a survey with fewer than 5 answers", () => {
  const report = buildReport(withSurvey(4));

  test("shows only the line saying so: no count, no figure, no slide", async () => {
    assert.equal(report.survey?.shown, false);
    const line = "Fewer than 5 people answered the team survey, so we don't show its results.";
    assert.equal(currentBody(report.outputs.executive_summary, "team_survey"), line);
    const exec = executiveHtml(report, pseudonyms);
    assert.ok(exec.includes("Fewer than 5 people answered the team survey"));
    assert.ok(!/\b4 people\b/.test(exec));
    assert.ok(!fullReportHtml(report, pseudonyms).includes("What your team says, against"));
    assert.ok(!report.value.groups.some((g) => g.id === TEAM_TIME_GROUP));
    assert.ok(!report.dataset.figures.some((f) => f.source === "survey" && f.kind === "duration" && f.unit === "hour" && f.value !== 1));
    const notes = await notesText(report);
    assert.ok(notes.includes(line));
    assert.ok(!/left a comment/.test(notes));
  });
});

describe("a survey with enough answers", () => {
  const input = withSurvey(15);
  const report = buildReport(input);

  test("has a section in every output, each passing the number check", () => {
    for (const output of ["executive_summary", "full_health_check", "raw_notes"] as const) {
      const section = sectionOf(report, output)!;
      assert.ok(section, output);
      const version = report.outputs[output].versions.find((v) => v.id === section.currentVersionId)!;
      assert.deepEqual(version.numberCheck.unmatched, [], output);
      assert.equal(section.status, "draft", output);
    }
  });

  test("the executive summary passes the layout and number checks", async () => {
    const { layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: EXECUTIVE_PAGES.max, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 6)));
    assert.ok(executiveHtml(report, pseudonyms).includes("15 people in your team answered our survey"));
  });

  test("the full report passes the layout and number checks, with the survey after adoption", async () => {
    const html = fullReportHtml(report, pseudonyms);
    const { layout } = await renderPdf(browser, html, { minPages: DECK_RANGE.min, maxPages: DECK_RANGE.max, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 6)));
    const says = html.indexOf("<h2>What your team says</h2>");
    const against = html.indexOf("<h2>What your team says, against what we found</h2>");
    const adoption = html.indexOf("<h2>Who uses Salesforce, and how</h2>");
    assert.ok(adoption > 0 && says > adoption && against > says, "the survey follows the adoption chapter");
    assert.match(html, /<li>What your team says<span class="pg" data-ref>\d+<\/span><\/li>/, "and the contents list it, with its page");
  });

  test("the microsite's overview carries it, and every page passes the number check", () => {
    const { pages } = buildMicrositePages(report, pseudonyms);
    const overview = pages.find((p) => p.slug === "overview")!.html;
    assert.ok(overview.includes("<h2>What your team says</h2>"));
    assert.ok(overview.includes("Your team says"));
  });

  test("never prints a comment, anywhere", async () => {
    assert.ok(JSON.stringify(input.survey).includes(SYNTHETIC_COMMENT), "the test survey has comments");
    const printed = [executiveHtml(report, pseudonyms), fullReportHtml(report, pseudonyms), await notesText(report), ...buildMicrositePages(report, pseudonyms).pages.map((p) => p.html), JSON.stringify(report.outputs), JSON.stringify(report.metrics), JSON.stringify(report.dataset), JSON.stringify(report.survey)];
    for (const text of printed) assert.ok(!text.includes(SYNTHETIC_COMMENT));
    // How many commented is all the raw notes say.
    assert.match(await notesText(report), /5 people left a comment\. We don't put comments in any document/);
  });

  test("costs the hours the team says it loses, in one group with the time the findings cost", () => {
    const group = report.value.groups.find((g) => g.id === TEAM_TIME_GROUP)!;
    const line = group.lines.find((l) => l.findingId === TEAM_TIME_LINE)!;
    const lost = report.metrics.find((m) => m.id === SURVEY.lost)!;
    const users = report.metrics.find((m) => m.id === "adoption.monthly_active_users")!;
    assert.ok(lost.status === "measured" && users.status === "measured");
    assert.equal(line.annual, Math.round(lost.value * users.value * report.value.hourly.rate * WORKING_WEEKS * 100) / 100);
    assert.deepEqual([line.low, line.expected, line.high].map((n) => Math.round((n / line.annual) * 100)), [50, 75, 100]);
    // Each finding's time cost is in the same group now, and only the largest counts.
    const alone = buildReport(base).value;
    const timeLines = alone.groups.filter((g) => g.kind === "time_cost").flatMap((g) => g.lines);
    assert.ok(timeLines.length > 0, "the synthetic org has a time cost of its own");
    for (const l of timeLines) assert.ok(group.lines.some((x) => x.findingId === l.findingId), l.title);
    assert.equal(report.value.groups.filter((g) => g.kind === "time_cost").length, 1);
    assert.equal(group.counted.expected, Math.max(...group.lines.map((l) => l.expected)));
    assert.equal(report.value.byKind.time_cost.expected, group.counted.expected);
    // Nothing else moves.
    for (const kind of ["revenue_recoverable", "conversion_uplift", "licence_waste", "exposure"] as const) assert.deepEqual(report.value.byKind[kind], alone.byKind[kind], kind);
  });

  test("counts a finding's time instead, when it's the larger", () => {
    const tiny = buildValueModel(base.findings, run, base.calibration.currency, null, { hoursPerWeek: 0.001, users: 1, usersFrom: "synthetic" });
    const group = tiny.groups.find((g) => g.id === TEAM_TIME_GROUP)!;
    assert.notEqual(group.counted.findingId, TEAM_TIME_LINE);
    assert.equal(tiny.byKind.time_cost.expected, group.counted.expected);
  });
});
