import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { practiceFor } from "../../practice/index.ts";
import { Score } from "../../schemas/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { deckRangeFor, fullReportHtml } from "../full-report.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";

// A health check with Salesforce CPQ (DECISIONS.md 216 to 225): its two areas go
// through the full report as any area does, under the same checks and ceiling.

async function reportFor(modules: DomainModule[]): Promise<{ report: BuiltReport; pseudonyms: Pseudonymiser }> {
  const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const scores = (JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")) as unknown[]).map((s) => Score.parse(s));
  return { report: buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores }), pseudonyms };
}

const withCpq = await reportFor([...SALES_CLOUD_MODULES, ...CPQ_MODULES]);
const everything = await reportFor(ALL_MODULES);

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("a health check with CPQ", () => {
  test("every section passes the number check", () => {
    for (const built of Object.values(withCpq.report.outputs)) {
      for (const s of built.sections) {
        const v = built.versions.find((x) => x.id === s.currentVersionId)!;
        assert.deepEqual(v.numberCheck.unmatched, [], `${s.output} ${s.key}`);
      }
    }
  });

  test("both areas count towards the Barometer score", () => {
    const areas = withCpq.report.overall.status === "measured" ? withCpq.report.overall.areas.map((a) => [a.domain, a.weight]) : [];
    assert.deepEqual(areas.filter(([d]) => String(d).startsWith("cpq")).sort(), [["cpq_quoting", 1.25], ["cpq_setup", 0.75]]);
  });

  test("the full report fits its ceiling with a chapter for each area, and passes the layout check", async () => {
    const html = fullReportHtml(withCpq.report, withCpq.pseudonyms);
    const range = deckRangeFor(withCpq.report.input.run);
    assert.equal(range.max, 120, "Sales Cloud's 110, and 10 for CPQ");
    const { layout } = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: withCpq.report.dataset });
    assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 6)));
    for (const label of ["Quoting and discounts", "CPQ set-up"]) assert.ok(html.includes(`<h1>${label}</h1>`), label);
    for (const p of practiceFor("cpq_setup").slice(0, 3)) assert.ok(html.includes(p.text.replace(/'/g, "&#39;")), p.text);
  });
});

describe("with Service Cloud, CPQ and Account Engagement all on, and every trap firing", () => {
  // Each module raises the ceiling, so the worst case keeps its chapters (DECISIONS.md 281).
  test("the full report fits its ceiling of 140 with a chapter for every area, contents page included", async () => {
    const html = fullReportHtml(everything.report, everything.pseudonyms);
    const range = deckRangeFor(everything.report.input.run);
    assert.equal(range.max, 140);
    const { layout } = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: everything.report.dataset });
    assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 6)));
    assert.match(html, /<ol class="contents long">/);
    assert.doesNotMatch(html, /Each area at a glance/);
    for (const label of ["Service operations", "Quoting and discounts", "CPQ set-up", "Marketing handover"]) assert.ok(html.includes(`<h1>${label}</h1>`), label);
  });
});
