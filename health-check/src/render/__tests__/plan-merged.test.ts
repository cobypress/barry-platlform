import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildPlan } from "../../narrative/select.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import type { Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { fullReportHtml } from "../full-report.ts";
import { noteSections, referenceMap } from "../references.ts";

// Issues the plan merges into one action, from the review of night 3's reading passes
// (docs/overnight/night-3/review-reading.md).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

describe("issues the plan merges into one action", () => {
  /** A low, large action from one rule twice, on different records, behind a phase 2 already full, so both land after 90 days. */
  function merged(): { input: ReportInput; lead: Finding; also: Finding } {
    const template = output!.findings.find((f) => f.status === "issue" && f.recommendation)!;
    const fill = Array.from({ length: 12 }, (_, i): Finding => ({ ...template, id: `fill-${String(i).padStart(10, "0")}`, ruleId: `test.fill-${i}`, recommendation: `Do fill task ${i}.`, severity: "medium", effort: { size: "L" } }));
    const lead: Finding = { ...template, id: `zz-lead-00000001`, ruleId: "test.merged", title: "Merged lead", recommendation: "Fix both lists.", severity: "low", effort: { size: "L" } };
    const also: Finding = { ...lead, id: `zz-also-00000002`, title: "Merged follower" };
    return { input: { ...base, findings: [...output!.findings, ...fill, lead, also] }, lead, also };
  }

  test("an issue merged into an action after 90 days keeps its reference in the full report", () => {
    const { input, lead: a, also: b } = merged();
    const report = buildReport(input);
    const plan = buildPlan(report.teamFindings, report.added);
    // Whichever ranks first leads the action, and the other joins it.
    const lead = plan.later.find((f) => f.id === a.id || f.id === b.id)!;
    assert.ok(lead, "the action is after 90 days");
    const also = lead.id === a.id ? b : a;
    assert.deepEqual(plan.alsoFor[lead.id]?.map((f) => f.id), [also.id]);
    const refs = referenceMap(noteSections(report));
    const html = fullReportHtml(report, pseudonyms);
    const after = html.slice(html.indexOf("After 90 days"));
    assert.ok(refs.get(lead.id) && after.includes(refs.get(lead.id)!), "the lead's reference");
    assert.ok(refs.get(also.id) && after.includes(refs.get(also.id)!), "the merged issue's reference");
  });

  test("every issue merged into an action is among the roadmap's findings, as its action is", () => {
    const { input } = merged();
    const report = buildReport(input);
    const plan = buildPlan(report.teamFindings, report.added);
    const merged_ = Object.values(plan.alsoFor).flat();
    assert.ok(merged_.length >= 2, "the recorded org's own merge, and the test's");
    const roadmap = report.outputs.full_health_check.sections.find((s) => s.key === "roadmap")!;
    for (const f of merged_) assert.ok(roadmap.findingIds.includes(f.id), f.id);
  });
});
