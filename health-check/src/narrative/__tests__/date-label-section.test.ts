import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { buildReport, type ReportInput } from "../sections.ts";
import { extractFigures, scopeDataset } from "../../numbercheck/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { crmHealthExtractor } from "../../extract/opportunities.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";

// The exclusions section is the engine's own text, so a failed check there holds
// it for a person. Its line "Leads created since <date>" sits in a scope that
// holds the month keys of monthly active users, "2026-08" among them, which the
// reader once claimed out of the date and read the day as a minus figure
// (DECISIONS.md 551, 553).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const first: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
const later = first.findings.map((f) => (f.status === "issue" && f.ruleId === first.findings.find((x) => x.status === "issue")!.ruleId ? { ...f, status: "passed" as const, value: { kind: "not_sized" as const, reason: "Passed." } } : f));
const REPORTS: Record<string, ReportInput> = {
  "full health check": first,
  "full health check with intake": { ...first, intake: { customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" } },
  scan: { ...first, run: { ...run, product: "scan" } },
  pulse: { ...first, run: { ...run, id: `${run.id}-pulse`, product: "pulse", baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: first },
  "re-check": { ...first, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: first },
};

function exclusions(input: ReportInput) {
  const report = buildReport(input);
  const section = report.outputs.raw_notes.sections.find((s) => s.key === "exclusions_limits")!;
  const version = report.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!;
  const scope = scopeDataset(report.dataset, report.scopes.get(section.id)!);
  const figures = extractFigures(version.body, { labels: scope.labels.map((l) => l.value) }).figures;
  return { section, version, scope, figures };
}

describe("the exclusions section beside a month key", () => {
  test("in each synthetic report, reads the date its line holds, not a minus figure", () => {
    for (const [name, input] of Object.entries(REPORTS)) {
      const { section, version, scope, figures } = exclusions(input);
      assert.ok(version.body.includes("Leads created since 2026-08-02"), name);
      assert.ok(scope.labels.some((l) => l.value === "2026-08"), name);
      assert.ok(figures.some((f) => f.kind === "date" && f.raw === "2026-08-02"), name);
      assert.ok(!figures.some((f) => f.raw === "-02" || f.value === -2), name);
      assert.equal(section.status, "draft", name);
      assert.deepEqual([version.numberCheck.passed, version.numberCheck.unmatched], [true, []], name);
    }
  });

  test("with the day moved to the 18th, as on the run it held, nothing else backs it and the section stays a draft", () => {
    // The data-quality rule run again on this test's own copy of the extracts, with the cut-off it reports moved.
    const extracts = loadExtracts(dir);
    const health = clone(extracts.get(crmHealthExtractor.id)) as { staleBefore: string };
    assert.equal(health.staleBefore, "2026-08-02");
    extracts.set(crmHealthExtractor.id, { ...health, staleBefore: "2026-08-18" });
    const moved = analyseExtracts(extracts, { answers: loadAnswers() });
    const { section, version, scope, figures } = exclusions({ ...first, findings: moved.findings, metrics: moved.metrics, manualSteps: moved.manualSteps });
    assert.ok(version.body.includes("Leads created since 2026-08-18"));
    assert.ok(scope.labels.some((l) => l.value === "2026-08"));
    // Nothing in scope could back the day read as a figure.
    assert.ok(!scope.figures.some((f) => f.value === -18));
    assert.deepEqual([section.status, version.numberCheck.passed, version.numberCheck.unmatched], ["draft", true, []]);
    assert.ok(figures.some((f) => f.kind === "date" && f.raw === "2026-08-18"));
  });
});
