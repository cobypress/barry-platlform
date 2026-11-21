import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { applyOverrides, refusalLines, refusalReason, SURVEY_FIXED } from "../overrides.ts";
import { buildReport } from "../sections.ts";
import { loadRun, OverridesRefused, renderRun, SURVEY_FILE } from "../../render/render-run.ts";
import type { Metric, NumberOverride } from "../../schemas/index.ts";
import { SURVEY } from "../../survey/summary.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";

// DECISIONS.md 649: a team survey's figure is what people answered, so no
// correction of one is ever carried. It is refused through the one refusal a
// correction already has, unless it is put back, and nothing else changes.

const AT = "2026-10-31T13:00:00.000Z";
const EVIDENCE = { kind: "engine", source: "HealthCheckSurveyResponse", count: 12, dateRange: null, exclusions: [], capturedAt: AT } as unknown as Metric["evidence"][number];

type Measured = Extract<Metric, { status: "measured" }>;

function measure(id: string, domain: string, value: number, edit: Partial<Measured> = {}): Measured {
  return { status: "measured", id, domain, label: "A planted measure", definition: "The median answer out of 5.", unit: "count", evidence: [EVIDENCE], exclusions: [], assumptions: [], value, n: 12, dateRange: null, segments: [], segmentExemptReason: "One figure.", series: [], stats: {}, ...edit };
}

const row = (id: string, field: string, previousValue: number, newValue: number, n = 1): NumberOverride => ({ id: `ov-${n}`, runId: "run-0001", target: { type: "metric", id, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: AT });

const ease = measure(SURVEY.ease, "survey", 3.5, { stats: { p75: 4.5 }, segments: [{ dimension: "role", key: "Sales rep", n: 7, value: 3, notMeasurableReason: null }], segmentExemptReason: null });
const winRate = measure("hist.win_rate", "historic_pipeline", 72.5, { unit: "percent", definition: "The share of closed deals that were won." });

describe("a correction of a team survey figure (decision 649)", () => {
  test("its value, n, a stat or a segment's figure is refused with the fixed reason, and the measure is as read", () => {
    for (const [field, asRead] of [["value", 3.5], ["n", 12], ["stats.p75", 4.5], ["segments.role.Sales rep", 3]] as const) {
      const { metrics, refused } = applyOverrides([], [ease], [row(ease.id, field, asRead, asRead + 1)]);
      assert.deepEqual(metrics, [ease], field);
      assert.deepEqual(refused.map((r) => [r.metricId, r.field, r.asRead, r.last, r.why.kind]), [[ease.id, field, asRead, asRead + 1, "survey"]], field);
      assert.equal(refusalLines(refused)[1], `"A planted measure", ${field === "segments.role.Sales rep" ? "a figure" : field === "stats.p75" ? "p75" : field}: ${asRead} as read, corrected to ${asRead + 1}, but a team survey figure is what people answered, so it can't be corrected.`, field);
    }
    assert.equal(SURVEY_FIXED, "A team survey figure is what people answered, so it can't be corrected.");
  });

  test("a segment's key is never printed in a refusal's line", () => {
    const { refused } = applyOverrides([], [ease], [row(ease.id, "segments.role.Sales rep", 3, 4)]);
    assert.ok(!refusalReason(refused).includes("Sales rep"));
  });

  test("one that isn't measured is refused too, judged from its first row", () => {
    const thin: Metric = { status: "not_measurable", id: SURVEY.responses, domain: "survey", label: "People who answered", definition: "How many people answered.", unit: "count", evidence: [EVIDENCE], exclusions: [], assumptions: [], reason: "Fewer than 5 people answered the team survey, so we don't show its results." };
    const { metrics, refused } = applyOverrides([], [thin], [row(thin.id, "value", 4, 5)]);
    assert.deepEqual(metrics, [thin]);
    assert.deepEqual(refused.map((r) => [r.field, r.asRead, r.last, r.why.kind]), [["value", 4, 5, "survey"]]);
  });

  test("a put-back is never refused, even over a first row made from another value, and changes nothing", () => {
    for (const rows of [[row(ease.id, "value", 3.5, 4, 1), row(ease.id, "value", 4, 3.5, 2)], [row(ease.id, "value", 3, 4, 1), row(ease.id, "value", 4, 3.5, 2)], [row(ease.id, "segments.x", 3, 4, 1), row(ease.id, "segments.x", 4, 3, 2)]]) {
      assert.deepEqual(applyOverrides([], [ease], rows), { findings: [], metrics: [ease], refused: [] });
    }
  });

  test("an org measure corrected in the same run is still carried, and a measure in another domain is judged as before", () => {
    const { metrics, refused } = applyOverrides([], [ease, winRate], [row(ease.id, "value", 3.5, 4, 1), row(winRate.id, "value", 72.5, 70, 2)]);
    assert.deepEqual(metrics, [ease, { ...winRate, value: 70 }]);
    assert.deepEqual(refused.map((r) => [r.metricId, r.why.kind]), [[ease.id, "survey"]]);
    // A survey id in another domain is no survey measure: the domain decides.
    const named = { ...winRate, id: SURVEY.trust };
    assert.deepEqual(applyOverrides([], [named], [row(named.id, "value", 72.5, 70)]).metrics, [{ ...named, value: 70 }]);
  });
});

describe("a surveyed run with a survey correction (decision 649)", async () => {
  const { dir } = await runFromRecording({ answers: loadAnswers() });
  const folder = join(tempDir(), "surveyed");
  cpSync(dir, folder, { recursive: true });
  writeFileSync(join(folder, SURVEY_FILE), JSON.stringify(syntheticSurvey(12)));
  const plain = buildReport(loadRun(folder).input);
  const asAnswered = plain.metrics.find((m) => m.id === SURVEY.ease)!;
  assert.equal(asAnswered.status, "measured");
  const value = asAnswered.status === "measured" ? asAnswered.value : 0;
  const correction = row(SURVEY.ease, "value", value, value + 1);

  test("the report keeps the figure as answered, its number check data holds no corrected one, and the refusal is listed", () => {
    const report = buildReport({ ...loadRun(folder).input, overrides: [correction] });
    assert.deepEqual(report.metrics.find((m) => m.id === SURVEY.ease), asAnswered);
    assert.deepEqual(report.refused.map((r) => [r.metricId, r.why.kind]), [[SURVEY.ease, "survey"]]);
    assert.ok(!report.dataset.figures.some((f) => f.source === SURVEY.ease && f.value === value + 1), "no figure of the measure is the corrected one");
  });

  test("holds the documents as any refused correction does, and writes nothing", async () => {
    writeFileSync(join(folder, "overrides.json"), JSON.stringify([correction]));
    const before = readdirSync(folder).sort();
    await assert.rejects(() => renderRun(folder), (err: unknown) => err instanceof OverridesRefused && /but a team survey figure is what people answered, so it can't be corrected\./.test(err.message));
    assert.deepEqual(readdirSync(folder).sort(), before);
  });
});
