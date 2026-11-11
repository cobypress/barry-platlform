import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { STORAGE_LIMITS } from "../../testing/synthetic-org.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { executiveHtml, executiveMaxPages } from "../executive.ts";
import { deckRangeFor, fullReportHtml } from "../full-report.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import type { Finding } from "../../schemas/index.ts";

// An org past its data storage allowance, on a single health check with no
// earlier one (DECISIONS.md 502). Salesforce is assumed to report a negative
// Remaining there (QUESTIONS.md, 3.2 item f), so used is above the allowance.

const RULE = "tech.storage-use";
const OVER = "Data storage is 102.5% used (10,500 MB, over the 10,240 MB allowance) and file storage is 21.9% used (4,480 of 20,480 MB).";
const WITHIN = "Data storage is 87.3% used (8,940 of 10,240 MB) and file storage is 21.9% used (4,480 of 20,480 MB).";

async function reportFor(storage: typeof STORAGE_LIMITS): Promise<{ report: BuiltReport; finding: Finding; pseudonyms: Pseudonymiser }> {
  const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), transport: new Simulator({ storage }) });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) });
  return { report, finding: output!.findings.find((f) => f.ruleId === RULE)!, pseudonyms };
}

const over = await reportFor({ ...STORAGE_LIMITS, DataStorageMB: { Max: 10_240, Remaining: -260 } });

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("data storage past its allowance on a single health check (DECISIONS.md 502)", () => {
  test("the rule's sentence says it is over the allowance, with the facts and severity as before", () => {
    const { finding } = over;
    assert.equal(finding.status, "issue");
    assert.equal(finding.ruleVersion, 3);
    assert.ok(finding.summary.startsWith(`${OVER} `), finding.summary);
    assert.doesNotMatch(finding.summary, /10,500 of 10,240/);
    assert.equal(finding.facts.DataStorageMBUsed, 10_500);
    assert.equal(finding.facts.DataStorageMBMax, 10_240);
    assert.equal(finding.facts.DataStorageMBShare, 102.5);
    assert.equal(finding.facts.FileStorageMBUsed, 4_480);
    assert.equal(finding.severity, "high");
  });

  test("within the allowance the sentence is as it was, character for character", async () => {
    const { finding } = await reportFor(STORAGE_LIMITS);
    assert.ok(finding.summary.startsWith(`${WITHIN} `), finding.summary);
    assert.equal(finding.severity, "medium");
  });

  test("the microsite is made, and every raw notes section that prints the finding passes its number check", () => {
    const { report, pseudonyms, finding } = over;
    assert.doesNotThrow(() => buildMicrositePages(report, pseudonyms));
    const built = report.outputs.raw_notes;
    const printing = built.sections.filter((s) => s.findingIds.includes(finding.id));
    assert.ok(printing.length > 0, "a section lists the finding");
    for (const section of printing) {
      assert.deepEqual(built.versions.find((v) => v.id === section.currentVersionId)!.numberCheck.unmatched, [], section.key);
      // The finding's own sentence, checked in the section's scope as the raw notes print it under the section.
      assert.deepEqual(checkNumbers(finding.summary, scopeDataset(report.dataset, report.scopes.get(section.id)!)).unmatched, [], section.key);
    }
    // As the rule wrote it before, the same check refuses it.
    assert.equal(checkNumbers(finding.summary.replace("10,500 MB, over the 10,240 MB allowance", "10,500 of 10,240 MB"), report.dataset).passed, false);
  });

  test("neither deck's layout check has a number check problem, rendered as printDocuments does", async () => {
    const { report, pseudonyms } = over;
    const full = fullReportHtml(report, pseudonyms);
    assert.match(full, /10,500 MB, over the 10,240 MB allowance/);
    const range = deckRangeFor(report.input.run);
    const decks = [
      ["executive", await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset, carryOver: true })],
      ["full report", await renderPdf(browser, full, { minPages: range.min, maxPages: range.max, dataset: report.dataset, carryOver: true })],
    ] as const;
    for (const [name, { layout }] of decks) assert.deepEqual(layout.problems.filter((p) => p.check === "number check"), [], name);
  });
});
