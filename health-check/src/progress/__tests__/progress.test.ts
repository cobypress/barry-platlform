import { test, describe, after } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildReport, PROGRESS, type ReportInput } from "../../narrative/sections.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { executiveHtml, executiveMaxPages } from "../../render/executive.ts";
import { fullReportHtml, deckRangeFor } from "../../render/full-report.ts";
import { launchBrowser, renderPdf } from "../../render/pdf.ts";
import { EXECUTIVE_PAGES, loadRun } from "../../render/render-run.ts";
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

describe("the titles a re-check lists trace in its number check (DECISIONS.md 503)", () => {
  // A rule from each list whose title holds a figure. The stalled-deals rule was
  // calibrated at 83 days the first time, so that title is only in the earlier run.
  const [FIXED, STILL_OPEN, NEW, CANT_TELL] = ["rev.stalled-deals", "dq.record-type-use", "cur.stale-deals", "dq.open-opps-no-activity"] as const;
  const EARLIER_ONLY = "Open deals with no stage change or activity in 83 days";
  const passed = (f: Finding): Finding => ({ ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } });
  const earlierFindings = first.findings.map((f): Finding => (f.ruleId === FIXED && f.status === "issue" ? { ...f, title: EARLIER_ONLY } : f.ruleId === NEW ? passed(f) : f));
  const nowFindings = first.findings.map((f): Finding =>
    f.ruleId === FIXED && f.status === "issue" ? passed(f) : f.ruleId === CANT_TELL && f.status === "issue" ? { ...f, status: "not_measurable", notMeasurableReason: "Not readable this time.", value: { kind: "not_sized", reason: "Not measured." } } : f,
  );
  const earlier: ReportInput = { ...first, findings: earlierFindings, scores: scoreDomains(run.modules, earlierFindings) };
  const titled = buildReport({ ...second, findings: nowFindings, scores: scoreDomains(run.modules, nowFindings), baseline: earlier });
  const section = titled.outputs.raw_notes.sections.find((s) => s.key === "progress")!;
  const body = titled.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!;

  test("each list has its rule, each title with a figure, and one title is only the earlier run's", () => {
    const p = titled.progress!;
    const title = (list: typeof p.fixed, ruleId: string) => list.find((c) => c.ruleId === ruleId)?.title ?? "";
    assert.equal(title(p.fixed, FIXED), EARLIER_ONLY);
    assert.match(title(p.remaining, STILL_OPEN), /12 months/);
    assert.match(title(p.added, NEW), /30 days/);
    assert.match(title(p.unknown, CANT_TELL), /90 days/);
    assert.ok(!nowFindings.some((f) => f.title === EARLIER_ONLY), "this run has no finding with that title");
    // Leave the progress source out and nothing else in this run's data backs "83 days".
    const others = [...new Set(titled.dataset.figures.map((f) => f.source))].filter((s) => s !== PROGRESS);
    assert.equal(checkNumbers(EARLIER_ONLY, scopeDataset(titled.dataset, others)).passed, false);
  });

  test("the raw notes' progress section passes its number check, so it's a draft", () => {
    for (const t of [EARLIER_ONLY, "12 months", "30 days", "90 days"]) assert.ok(body.body.includes(t), t);
    assert.deepEqual(body.numberCheck.unmatched, []);
    assert.equal(section.status, "draft");
  });

  test("a figure in no title and no progress figure still fails the section's check", () => {
    const scope = scopeDataset(titled.dataset, titled.scopes.get(section.id)!);
    assert.equal(checkNumbers(body.body, scope).passed, true);
    const typed = checkNumbers(`${body.body}\nWe also found 977 deals with no next step.`, scope);
    assert.deepEqual(typed.unmatched.map((u) => u.raw), ["977"]);
  });

  test("the microsite is made, and neither deck's layout check has a number check problem", async () => {
    const pages = buildMicrositePages(titled, pseudonyms).pages;
    assert.ok(pages.find((p) => p.slug === "progress")!.html.includes(EARLIER_ONLY));
    const browser = await launchBrowser();
    after(() => browser.close());
    const range = deckRangeFor(titled.input.run);
    const decks = [
      ["executive", await renderPdf(browser, executiveHtml(titled, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(titled), dataset: titled.dataset, carryOver: true })],
      ["full report", await renderPdf(browser, fullReportHtml(titled, pseudonyms), { minPages: range.min, maxPages: range.max, dataset: titled.dataset, carryOver: true })],
    ] as const;
    for (const [name, { layout }] of decks) assert.deepEqual(layout.problems.filter((p) => p.check === "number check"), [], name);
  });

  test("no section Claude drafts has the progress source in its scope, so the titles never reach a brief", () => {
    assert.ok(titled.briefs.size > 0);
    for (const [id, brief] of titled.briefs) {
      assert.ok(!brief.sources.some((s) => s.id === PROGRESS), `${id}'s brief cites the progress source`);
      assert.ok(!titled.scopes.get(id)!.includes(PROGRESS), `${id}'s scope holds the progress source`);
    }
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
