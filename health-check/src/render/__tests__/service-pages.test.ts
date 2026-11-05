import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { strFromU8, unzipSync } from "fflate";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { practiceFor } from "../../practice/index.ts";
import { Score } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { executiveHtml } from "../executive.ts";
import { DECK_RANGE, fullReportHtml } from "../full-report.ts";
import { onePagerHtml, ONE_PAGER_PAGES } from "../one-pager.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";
import { renderRawNotesDocx } from "../raw-notes-docx.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";

// A health check with Service Cloud: its two areas go through every document
// as any area does, with the same layout and number checks.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const scores = (JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as unknown[]).map((s) => Score.parse(s));
const report: BuiltReport = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores });

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("a health check with Service Cloud", () => {
  test("every section passes the number check", () => {
    for (const built of Object.values(report.outputs)) {
      for (const s of built.sections) {
        const v = built.versions.find((x) => x.id === s.currentVersionId)!;
        assert.deepEqual(v.numberCheck.unmatched, [], `${built.sections[0]?.output} ${s.key}`);
      }
    }
  });

  test("both areas count towards the Barometer score", () => {
    assert.equal(report.overall.status, "measured");
    const areas = report.overall.status === "measured" ? report.overall.areas.map((a) => [a.domain, a.weight]) : [];
    assert.deepEqual(areas.filter(([d]) => String(d).startsWith("service")), [["service_operations", 1.25], ["service_setup", 0.75]]);
  });

  test("the full report passes the layout and number checks, with a chapter for each area", async () => {
    const html = fullReportHtml(report, pseudonyms);
    const { layout } = await renderPdf(browser, html, { minPages: DECK_RANGE.min, maxPages: DECK_RANGE.max, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 6)));
    for (const label of ["Service operations", "Service set-up"]) assert.ok(html.includes(`<h1>${label}</h1>`), label);
    for (const p of [...practiceFor("service_operations").slice(0, 3), ...practiceFor("service_setup").slice(0, 3)]) assert.ok(html.includes(p.text.replace(/'/g, "&#39;")), p.text);
  });

  test("the executive summary and one-pager pass their checks", async () => {
    for (const [name, html, range] of [
      ["executive", executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: EXECUTIVE_PAGES.max }],
      ["one-pager", onePagerHtml(report, pseudonyms), { minPages: ONE_PAGER_PAGES.min, maxPages: ONE_PAGER_PAGES.max }],
    ] as const) {
      const { layout } = await renderPdf(browser, html, { ...range, dataset: report.dataset });
      assert.deepEqual(layout.problems, [], `${name}: ${JSON.stringify(layout.problems.slice(0, 6))}`);
    }
  });

  test("the raw notes give the case figures their own section, and every finding", async () => {
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    for (const heading of ["Cases", "Findings: Service operations", "Findings: Service set-up", "Time to resolve a case", "Cases by channel"]) assert.ok(text.includes(heading), heading);
    const pipeline = report.outputs.raw_notes.sections.find((s) => s.key === "pipeline_metrics")!;
    assert.ok(!pipeline.metricIds.some((id) => id.startsWith("service.")), "case figures aren't pipeline figures");
  });

  test("the microsite shows both areas, with what good looks like, and its figures trace", () => {
    const { pages } = buildMicrositePages(report, pseudonyms);
    const issues = pages.find((p) => p.slug === "issues")!.html;
    for (const label of ["Service operations", "Service set-up"]) assert.ok(issues.includes(label), label);
    assert.ok(issues.includes(practiceFor("service_setup")[0]!.text));
  });
});
