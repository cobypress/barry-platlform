import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { buildReport, currentBody, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { STORAGE_LIMITS } from "../../testing/synthetic-org.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { storageSentences } from "../../progress/compare.ts";
import { executiveHtml, executiveMaxPages } from "../executive.ts";
import { deckRangeFor, fullReportHtml } from "../full-report.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import type { Finding, Run } from "../../schemas/index.ts";

// An org past its data storage allowance, on a single health check with no
// earlier one (DECISIONS.md 502 and 504). Salesforce is assumed to report a negative
// Remaining there (QUESTIONS.md, 3.2 item f), so used is above the allowance.

const RULE = "tech.storage-use";
const OVER = "Data storage is 102.5% used (10,500 MB, over the 10,240 MB allowance) and file storage is 21.9% used (4,480 of 20,480 MB).";
const WITHIN = "Data storage is 87.3% used (8,940 of 10,240 MB) and file storage is 21.9% used (4,480 of 20,480 MB).";

// What the finding prints within the allowance, and past it (DECISIONS.md 504).
const CLOSE_TITLE = "Storage is close to the allowance";
const CLOSE_ADVICE = "Archive or delete what you no longer need, starting with the largest objects and old files, and plan any extra storage before you reach the limit.";
const PAST_TITLE = "Storage is past the allowance";
const PAST_ADVICE = "Archive or delete what you no longer need, starting with the largest objects and old files, and arrange extra storage now, because you are past the allowance.";
const WARNING = "Past the allowance, Salesforce can stop new records or files being saved until space is freed or more is bought.";
const VALUE = "We hold no price for extra storage, so we don't put a figure on it.";
const DEPENDENCY = "Agree with the business how long each kind of record must be kept before archiving any";

async function reportFor(storage: typeof STORAGE_LIMITS): Promise<{ report: BuiltReport; finding: Finding; pseudonyms: Pseudonymiser; dir: string }> {
  const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), transport: new Simulator({ storage }) });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const report = buildReport({ run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) });
  return { report, finding: output!.findings.find((f) => f.ruleId === RULE)!, pseudonyms, dir };
}

const over = await reportFor({ ...STORAGE_LIMITS, DataStorageMB: { Max: 10_240, Remaining: -260 } });
const within = await reportFor(STORAGE_LIMITS);

/** The storage finding with the org's limits set to these, the rest of the extracts as recorded. */
const extracts = loadExtracts(within.dir);
const storageAt = (limits: Record<string, { Max: number; Remaining: number }>): Finding => {
  const changed = clone(extracts);
  Object.assign((changed.get("api-limits") as { data: Record<string, unknown> }).data, limits);
  const found = analyseExtracts(changed, { calibration: loadCalibration() }).findings.filter((f) => f.ruleId === RULE);
  assert.equal(found.length, 1);
  return found[0]!;
};

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

  test("within the allowance the sentence is as it was, character for character", () => {
    const { finding } = within;
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

describe("the storage finding's title and advice past the allowance (DECISIONS.md 504)", () => {
  /** Every sentence the finding prints, each true of an org past its allowance. */
  const assertPast = (f: Finding, opening: string, label: string): void => {
    assert.equal(f.status, "issue", label);
    assert.equal(f.title, PAST_TITLE, label);
    assert.ok(f.summary.startsWith(`${opening} ${WARNING}`), `${label}: ${f.summary}`);
    assert.equal(f.recommendation, PAST_ADVICE, label);
    assert.deepEqual(f.value, { kind: "not_sized", reason: VALUE }, label);
    assert.deepEqual(f.dependencies, [DEPENDENCY], label);
    assert.equal(f.severity, "high", label);
  };

  test("data past and file within, file past and data within, and both past: titled and advised as past it", () => {
    assertPast(over.finding, OVER, "data past, from the simulator");
    assertPast(storageAt({ DataStorageMB: { Max: 10_240, Remaining: -260 } }), OVER, "data past");
    assertPast(storageAt({ FileStorageMB: { Max: 20_480, Remaining: -500 } }), "Data storage is 87.3% used (8,940 of 10,240 MB) and file storage is 102.4% used (20,980 MB, over the 20,480 MB allowance).", "file past");
    assertPast(storageAt({ DataStorageMB: { Max: 10_240, Remaining: -260 }, FileStorageMB: { Max: 20_480, Remaining: -500 } }), "Data storage is 102.5% used (10,500 MB, over the 10,240 MB allowance) and file storage is 102.4% used (20,980 MB, over the 20,480 MB allowance).", "both past");
  });

  test("exactly at the allowance, and within it at 80% and 95%, the title and advice are as they were", () => {
    for (const [remaining, opening, severity] of [
      [0, "Data storage is 100% used (10,240 of 10,240 MB)", "high"],
      [2_048, "Data storage is 80% used (8,192 of 10,240 MB)", "medium"],
      [512, "Data storage is 95% used (9,728 of 10,240 MB)", "high"],
    ] as const) {
      const f = storageAt({ DataStorageMB: { Max: 10_240, Remaining: remaining } });
      assert.equal(f.status, "issue", opening);
      assert.ok(f.summary.startsWith(`${opening} and file storage is 21.9% used (4,480 of 20,480 MB). ${WARNING}`), f.summary);
      assert.equal(f.title, CLOSE_TITLE, opening);
      assert.equal(f.recommendation, CLOSE_ADVICE, opening);
      assert.equal(f.severity, severity, opening);
    }
    assert.equal(within.finding.title, CLOSE_TITLE);
    assert.equal(within.finding.recommendation, CLOSE_ADVICE);
  });

  test("the title and advice pass the number check wherever the finding prints", async () => {
    const { report, pseudonyms, finding } = over;
    const built = report.outputs.raw_notes;
    const printing = built.sections.filter((s) => s.findingIds.includes(finding.id));
    assert.ok(printing.length > 0, "a section lists the finding");
    for (const section of printing) {
      const scope = scopeDataset(report.dataset, report.scopes.get(section.id)!);
      for (const text of [finding.title, finding.recommendation]) assert.deepEqual(checkNumbers(text, scope).unmatched, [], `${section.key}: ${text}`);
    }
    const site = buildMicrositePages(report, pseudonyms).pages.map((p) => p.html).join("\n");
    const full = fullReportHtml(report, pseudonyms);
    for (const [name, html] of [["the microsite", site], ["the full report", full]] as const) {
      assert.ok(html.includes(PAST_TITLE), `${name} prints the title`);
      assert.ok(html.includes(PAST_ADVICE), `${name} prints the advice`);
      assert.ok(!html.includes(CLOSE_TITLE) && !html.includes("before you reach the limit"), `${name} says nothing of being close`);
    }
    const range = deckRangeFor(report.input.run);
    for (const [name, html, pages] of [
      ["executive", executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report) }],
      ["full report", full, { minPages: range.min, maxPages: range.max }],
    ] as const) {
      const { layout } = await renderPdf(browser, html, { ...pages, dataset: report.dataset, carryOver: true });
      assert.deepEqual(layout.problems.filter((p) => p.check === "number check"), [], name);
    }
  });

  test("a re-check from close to the allowance to past it lists the rule once, under Still open, with the title it has now", () => {
    const stamped = (r: Run, finishedAt: string, extra: Partial<Run> = {}): Run => ({ ...r, ...extra, steps: r.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt } : s)) });
    const earlier: ReportInput = { ...within.report.input, run: stamped(within.report.input.run, "2026-07-14T12:00:00.000Z") };
    const now = over.report.input;
    const report = buildReport({ ...now, run: stamped(now.run, "2026-11-09T12:00:00.000Z", { id: `${now.run.id}-recheck`, baselineRunId: earlier.run.id }), baseline: earlier });
    const p = report.progress!;
    assert.equal(within.finding.facts.DataStorageMBShare, 87.3);
    assert.equal(over.finding.facts.DataStorageMBShare, 102.5);
    for (const [name, list] of [["fixed", p.fixed], ["new", p.added], ["can't tell", p.unknown]] as const) assert.ok(!list.some((c) => c.ruleId === RULE), name);
    assert.deepEqual(p.remaining.filter((c) => c.ruleId === RULE).map((c) => c.title), [PAST_TITLE]);
    const section = report.outputs.raw_notes.sections.find((s) => s.key === "progress")!;
    assert.equal(section.status, "draft");
    assert.deepEqual(report.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!.numberCheck.unmatched, []);
    const text = currentBody(report.outputs.raw_notes, "progress");
    assert.equal(text.split(PAST_TITLE).length - 1, 1, text);
    assert.ok(!text.includes(CLOSE_TITLE), text);
    assert.ok(text.split("Still open")[1]!.split("\n\n")[0]!.includes(PAST_TITLE), text);
    const [data] = storageSentences(p);
    assert.equal(data, "Data storage: 8,940 of 10,240 MB used then, 10,500 MB used now, over the 10,240 MB allowance, up 1,560 MB in 118 days (an average of 397 MB every 30 days).");
    assert.ok(text.includes(data!), text);
  });
});
