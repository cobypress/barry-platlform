import { test, describe, after } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml, deckRangeFor } from "../../render/full-report.ts";
import { launchBrowser, renderPdf } from "../../render/pdf.ts";
import { loadRun } from "../../render/render-run.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import type { Finding } from "../../schemas/index.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const first: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };

// Ninety days on: two rules fixed, one rule newly raised.
const issueRules = [...new Set(first.findings.filter((f) => f.status === "issue").map((f) => f.ruleId))];
const fixedRules = issueRules.slice(0, 2);
// A rule that only passed the first time, so raising it now is new.
const passedRule = first.findings.find((f) => f.status === "passed" && !issueRules.includes(f.ruleId))!.ruleId;
const later = first.findings.map((f): Finding => (fixedRules.includes(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f.ruleId === passedRule ? { ...f, status: "issue", severity: "medium" } : f));
const second: ReportInput = { ...first, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: first };
const report = buildReport(second);

describe("a re-check", () => {
  test("lines rules up: fixed, still open, and new since then", () => {
    const p = report.progress!;
    assert.deepEqual(p.fixed.map((c) => c.ruleId).sort(), [...fixedRules].sort());
    assert.ok(p.added.some((c) => c.ruleId === passedRule));
    assert.equal(p.remaining.length, issueRules.length - fixedRules.length);
    assert.equal(p.baselineRunId, run.id);
  });

  test("a rule that couldn't be measured this time is 'can't tell', never 'fixed'", () => {
    const rule = issueRules[2]!;
    const unmeasured = first.findings.map((f): Finding => (f.ruleId === rule ? { ...f, status: "not_measurable", notMeasurableReason: "Not readable this time." } : f));
    const p = buildReport({ ...second, findings: unmeasured, baseline: first }).progress!;
    assert.ok(p.unknown.some((c) => c.ruleId === rule));
    assert.ok(!p.fixed.some((c) => c.ruleId === rule));
  });

  test("a first health check has no progress page", () => {
    const plain = buildReport(first);
    assert.equal(plain.progress, null);
    assert.doesNotMatch(executiveHtml(plain, pseudonyms), /What has changed/);
  });

  test("both decks and the microsite show it, and every figure passes the checks", async () => {
    const exec = executiveHtml(report, pseudonyms);
    const full = fullReportHtml(report, pseudonyms);
    for (const html of [exec, full]) assert.match(html, /What has changed/);
    assert.ok(buildMicrositePages(report, pseudonyms).pages.some((p) => p.slug === "progress"));
    const browser = await launchBrowser();
    after(() => browser.close());
    const e = await renderPdf(browser, exec, { minPages: 5, maxPages: 14, dataset: report.dataset });
    assert.deepEqual(e.layout.problems, [], JSON.stringify(e.layout.problems.slice(0, 5)));
    const range = deckRangeFor(report.input.run);
    const f = await renderPdf(browser, full, { minPages: range.min, maxPages: range.max, dataset: report.dataset });
    assert.deepEqual(f.layout.problems, [], JSON.stringify(f.layout.problems.slice(0, 5)));
  });
});

describe("loading a re-check's folder", () => {
  test("finds the first check beside it, and ignores one from another org", () => {
    const root = mkdtempSync(join(tmpdir(), "hc-recheck-"));
    cpSync(dir, join(root, run.id), { recursive: true });
    const recheck = join(root, `${run.id}-recheck`);
    cpSync(dir, recheck, { recursive: true });
    const runJson = JSON.parse(readFileSync(join(recheck, "run.json"), "utf8"));
    writeFileSync(join(recheck, "run.json"), JSON.stringify({ ...runJson, id: `${run.id}-recheck`, baselineRunId: run.id }));
    assert.equal(loadRun(recheck).input.baseline?.run.id, run.id);
    const firstJson = JSON.parse(readFileSync(join(root, run.id, "run.json"), "utf8"));
    writeFileSync(join(root, run.id, "run.json"), JSON.stringify({ ...firstJson, orgId: "00D000000000999AAA" }));
    assert.equal(loadRun(recheck).input.baseline, null);
  });
});
