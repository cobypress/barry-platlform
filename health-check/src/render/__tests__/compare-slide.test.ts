import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { z } from "zod";
import { COMPARED_METRICS, compareRun } from "../../benchmark/compare.ts";
import type { PublishedFigure } from "../../benchmark/published.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { BenchmarkRow, Score } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { quarterOf } from "../../benchmark/export.ts";
import { COMPARE_CSS, compareSlide } from "../compare.ts";
import { fullReportHtml } from "../full-report.ts";
import { renderContext } from "../html.ts";
import { layoutOf, launchBrowser } from "../pdf.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const scores = z.array(Score).parse(JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")));

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

/** The full report with the comparison slide added last, as full-report.ts will place it. */
function deckWith(slideHtml: string, classes = "tight"): string {
  const intake = Intake.parse({ industry: "professional-services", headcountBand: "1001-5000", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
  const html = fullReportHtml(buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake }), pseudonyms);
  const page = `<section class="page ${classes}" data-page="999">${slideHtml}<div class="footer" data-chrome><span>Black Cloud Barometer</span><span>999</span></div></section>`;
  return html.replace("</style>", `${COMPARE_CSS}</style>`).replace("</body>", `${page}</body>`);
}

describe("the comparison slide", () => {
  test("fits one slide at its longest: the widest group label and two published figures on every row", async () => {
    const period = quarterOf(run.analysisWindow.to);
    const rows = COMPARED_METRICS.flatMap((m) =>
      [5, 9, 12, 18, 22, 25, 31, 36, 44, 52, 60, 75].map((value) =>
        BenchmarkRow.parse({ metricKey: m.key, ruleSetVersion: "rs-1", value, unit: m.unit, industry: "professional-services", headcountBand: "1001-5000", userBand: "201+", dealSizeBand: "100k+", period }),
      ),
    );
    const long = (m: (typeof COMPARED_METRICS)[number], n: number): PublishedFigure => ({
      id: `long-${m.key}-${n}`,
      metricKey: m.key,
      value: 123,
      unit: m.unit,
      statistic: "median",
      population: "synthetic",
      measures: "synthetic",
      differences: [],
      basis: "survey",
      source: { publisher: "A Very Long Research Publisher Name", title: "Synthetic", year: 2026, url: "https://example.com", checked: "2026-11-05" },
    });
    const comparisons = compareRun({
      metrics: output!.metrics,
      scores,
      findings: output!.findings,
      industry: "professional-services",
      headcountBand: "1001-5000",
      period,
      benchmark: { rows, includesThisRun: false },
      published: COMPARED_METRICS.flatMap((m) => [long(m, 1), long(m, 2)]),
    });
    const slide = compareSlide(renderContext(pseudonyms, "GBP", false), comparisons);
    // Layout alone: the number check on the real report's slide is in the benchmark tests.
    const layout = await layoutOf(browser, deckWith(slide.html, slide.classes), { minPages: 1, maxPages: 999 });
    const last = layout.pages;
    assert.deepEqual(layout.problems.filter((p) => p.page === last), []);
  });

  test("with the published research we hold, every figure on it traces through the number check in print", async () => {
    const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores });
    const slide = compareSlide(renderContext(pseudonyms, "GBP", false), report.comparisons);
    const layout = await layoutOf(browser, deckWith(slide.html, slide.classes), { minPages: 1, maxPages: 999, dataset: report.dataset });
    assert.deepEqual(layout.problems.filter((p) => p.page === layout.pages), []);
    for (const c of report.comparisons) {
      const first = c.published[0];
      if (first) assert.ok(slide.html.includes(`${first.figure.source.publisher.replace(/&/g, "&amp;")}, ${first.figure.source.year}`), c.metric.key);
    }
  });
});
