import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { danglingCitations, processRunJob } from "../run-job.ts";
import type { RunJobInput } from "../types.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// The worker's write path logs a section that cites an id its own run doesn't
// hold, and stores the run's results all the same (DECISIONS.md 643).

const { dir } = await runFromRecording({ answers: loadAnswers() });
const loaded = loadRun(dir);
const report = buildReport(loaded.input);
const sections = Object.values(report.outputs).flatMap((o) => o.sections);
const output = { findings: loaded.input.findings, metrics: loaded.input.metrics, manualSteps: loaded.input.manualSteps };

async function deliver(input: RunJobInput) {
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
  await deps.tokens.save(syntheticConnection());
  deps.store.addRun(input);
  const outcome = await processRunJob(RUN_ID, deps, { final: true });
  return { deps, outcome };
}

describe("the worker's citation log (decision 643)", () => {
  test("a run's own sections cite nothing it doesn't hold, so nothing is logged", () => {
    assert.deepEqual(danglingCitations(RUN_ID, sections, output), []);
  });

  test("a planted dangling id is logged with the run, the section and the count, and the metric's id alone", () => {
    const victim = sections.find((s) => s.output === "raw_notes" && s.key === "pipeline_metrics")!;
    const planted = sections.map((s) => (s.id === victim.id ? { ...s, findingIds: [...s.findingIds, "f-000000000001"], metricIds: [...s.metricIds, "historic.planted_metric"], manualStepIds: [...s.manualStepIds, "ms-0001"] } : s));
    assert.deepEqual(danglingCitations(RUN_ID, planted, output), [`Run ${RUN_ID}: section raw_notes:pipeline_metrics cites 3 ids not in the run, kept as they are: 1 finding, 1 metric (historic.planted_metric), 1 manual step.`]);
    const line = danglingCitations(RUN_ID, planted, output)[0]!;
    assert.ok(!line.includes("f-000000000001") && !line.includes("ms-0001") && !line.includes(victim.title), "no finding or step id, and not the section's title");
  });

  test("through the run job: a set-up only run with interviews, the fault's own shape, logs nothing and stores no historic metric id", async () => {
    const { deps, outcome } = await deliver(runInput({ run: consoleRun({ scope: "essentials" }), interviews: syntheticInterviews() }));
    assert.equal(outcome.outcome, "done");
    assert.deepEqual(deps.lines.filter((l) => l.includes("not in the run")), []);
    const saved = deps.store.runs.get(RUN_ID)!.output!;
    assert.ok(saved.sections.some((s) => s.key === "interviews"));
    assert.ok(!saved.metrics.some((m) => m.id.startsWith("historic.")));
    assert.deepEqual(saved.sections.flatMap((s) => s.metricIds.filter((id) => id.startsWith("historic."))), []);
  });

  test("through the run job: a run with interviews logs nothing, and with a survey, whose figures the run now holds (decision 645), nothing either", async () => {
    const plain = await deliver(runInput({ interviews: syntheticInterviews() }));
    assert.equal(plain.outcome.outcome, "done");
    assert.deepEqual(plain.deps.lines.filter((l) => l.includes("not in the run")), []);

    const surveyed = await deliver(runInput({ survey: syntheticSurvey(12) }));
    assert.equal(surveyed.outcome.outcome, "done");
    assert.deepEqual(surveyed.deps.lines.filter((l) => l.includes("not in the run")), []);
    const saved = surveyed.deps.store.runs.get(RUN_ID)!.output!;
    assert.ok(saved.sections.some((s) => s.key === "team_survey"), "the sections were stored");
    assert.ok(saved.sections.some((s) => s.metricIds.some((id) => id.startsWith("survey."))), "and they cite the survey's metrics");
  });
});
