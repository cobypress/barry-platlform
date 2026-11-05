import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Intake } from "../../narrative/intake.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import type { Finding } from "../../schemas/index.ts";
import { caseStudyDraft, renderCaseStudyDocx } from "../case-study.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const intake = (consent: boolean) =>
  Intake.parse({ customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [] }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", caseStudyConsent: consent });
const first: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
const fixedRules = [...new Set(first.findings.filter((f) => f.status === "issue").map((f) => f.ruleId))].slice(0, 3);
const later = first.findings.map((f): Finding => (fixedRules.includes(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
const recheck = (consent: boolean): ReportInput => ({ ...first, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), intake: intake(consent), baseline: first });

describe("the case study draft", () => {
  test("is only drafted for a re-check whose customer agreed to be a case study", () => {
    assert.equal(caseStudyDraft(buildReport({ ...first, intake: intake(true) })), null, "a first check has nothing to show yet");
    assert.equal(caseStudyDraft(buildReport(recheck(false))), null, "no consent, no draft");
    assert.ok(caseStudyDraft(buildReport(recheck(true))));
  });

  test("tells the change from the re-check's own figures, and leaves the quote to the customer", () => {
    const report = buildReport(recheck(true));
    const study = caseStudyDraft(report)!;
    assert.equal(study.customer, "Synthetic Co");
    assert.deepEqual(study.challenge, ["We don't trust the forecast."]);
    assert.equal(study.fixed.length, Math.min(6, report.progress!.fixed.length));
    assert.ok(study.changed.some((l) => /kinds of issue fixed/.test(l)));
    assert.ok(study.beforePublishing.some((l) => /written approval/.test(l)));
    assert.ok(study.beforePublishing.some((l) => /Never write one for them/.test(l)));
  });

  test("the Word draft is marked as a draft, holds the quote placeholder, and no name token", async () => {
    const bytes = (await renderCaseStudyDocx(buildReport(recheck(true)), pseudonyms))!;
    const xml = strFromU8(unzipSync(new Uint8Array(bytes))["word/document.xml"]!);
    assert.match(xml, /Draft\. Black Cloud only until the customer has approved it\./);
    assert.match(xml, /Their quote, in their words/);
    assert.doesNotMatch(xml, /\[\[[a-z]+:/);
    assert.equal(await renderCaseStudyDocx(buildReport(recheck(false)), pseudonyms), null);
  });
});
