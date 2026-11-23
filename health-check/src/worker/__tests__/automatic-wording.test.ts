import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { benchmarkFigures, UNCONFIRMED_SKIP } from "../../benchmark/export.ts";
import { ORDER_UNCONFIRMED } from "../../domains/historic-pipeline/cohort.ts";
import { AUTOMATIC_NO_STATED_PROBLEMS, buildReport } from "../../narrative/sections.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { loadRun } from "../../render/render-run.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

// What an automatic run's customer reads says nothing of a consultant who
// isn't there, and says what it assumed in place of what a person would have
// told us (DECISIONS.md 693, closing lane E2's review).

const automatic = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });
const consultant = await runFromRecording({ answers: loadAnswers() });
const auto = loadRun(automatic.dir);
const cons = loadRun(consultant.dir);
const NO_KINDS = "Nobody has told us which of your users are integrations, shared or system accounts";

/** Every string a run's findings and metrics hold, and its sections' text and its digest's. */
function customerText(loaded: ReturnType<typeof loadRun>): string[] {
  const report = buildReport(loaded.input);
  const digest = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" }).digest;
  const strings = (v: unknown): string[] => (typeof v === "string" ? [v] : Array.isArray(v) ? v.flatMap(strings) : v && typeof v === "object" ? Object.values(v).flatMap(strings) : []);
  const bodies = Object.values(report.outputs).filter((o) => o.output !== "raw_notes").flatMap((o) => o.versions.map((v) => v.body));
  return [...strings(loaded.input.findings.map(({ owner: _o, ...f }) => f)), ...strings(loaded.input.metrics), ...bodies, ...strings(digest)];
}

describe("an automatic run's words", () => {
  test("nothing it serves the customer addresses a consultant, or speaks of what we agreed with them", () => {
    const text = customerText(auto);
    for (const t of text) assert.ok(!/consultant|agreed with you|The calibration can say/i.test(t), t.slice(0, 200));
    // Owners included: no finding is given to a consultant to do.
    assert.ok(!auto.input.findings.some((f) => /consultant/i.test(f.owner)));
  });

  test("the stages the calibration lacks are not measured, with what the customer can tell us, never a high-severity issue for a consultant", () => {
    const f = auto.input.findings.find((x) => x.ruleId === "pipe.unmapped-stages")!;
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /^The calibration doesn't say what these stages mean, as it was read from your org's own stage settings: /);
    assert.match(f.notMeasurableReason!, /If you tell us what each means, your next health check can use it\.$/);
    assert.equal(f.notMeasurableKind, "needs_input");
  });

  test("the stages it took from the flags say so as the customer's to tell us; lost or no decision likewise", () => {
    const text = customerText(auto).join("\n");
    assert.match(text, /You haven't told us yet what (?:one stage means|some stages mean), so we used Salesforce's own setting/);
    assert.match(text, /If you tell us which each is, your next health check can measure this\./);
  });

  test("the stated problems it has none of are the customer's to tell, not a consultant's to link", () => {
    const report = buildReport(auto.input);
    const bodies = Object.values(report.outputs).flatMap((o) => o.versions.map((v) => v.body));
    assert.ok(bodies.includes(AUTOMATIC_NO_STATED_PROBLEMS));
  });

  test("figures that count users as people say nobody told us who isn't one; a consultant's calibration says nothing of it", () => {
    const said = (loaded: ReturnType<typeof loadRun>) => [...loaded.input.findings.map((f) => f.summary), ...loaded.input.metrics.flatMap((m) => m.assumptions)].filter((t) => t.includes(NO_KINDS)).length;
    assert.ok(said(auto) > 0);
    assert.equal(said(cons), 0);
    // A finding leaning on it is at most medium confidence, as with every assumption.
    for (const f of auto.input.findings.filter((x) => x.status === "issue" && x.summary.includes(NO_KINDS))) assert.notEqual(f.confidence, "high");
  });

  test("charts drawn in the stages' order carry the unconfirmed order's note; a consultant-led run's don't", () => {
    // As the page escapes it.
    const note = `<p class="note">${ORDER_UNCONFIRMED.replace(/'/g, "&#39;")}</p>`;
    const pages = fullReportHtml(buildReport(auto.input), auto.pseudonyms);
    assert.ok(pages.split(note).length - 1 >= 2, "on the pipeline and the process charts at least");
    assert.ok(!fullReportHtml(buildReport(cons.input), cons.pseudonyms).includes(note));
  });
});

describe("the benchmark set and an automatic run", () => {
  const read = (loaded: ReturnType<typeof loadRun>, orgSettings: boolean) => benchmarkFigures({ metrics: loaded.input.metrics, scores: loaded.input.scores ?? [], findings: loaded.input.findings, orgSettings });

  test("figures resting on the unconfirmed order, blended record types or a stage's meaning from the flags stay out; the rest go in", () => {
    const { figures, skipped } = read(auto, true);
    const out = skipped.filter((s) => s.reason === UNCONFIRMED_SKIP).map((s) => s.metricKey);
    assert.ok(out.length > 0);
    assert.ok(!figures.some((f) => f.metricKey === "historic.stage_conversion"));
    assert.ok(figures.some((f) => f.metricKey.startsWith("score.")));
    // The same figures read as a consultant's calibration would be kept: the skip is the source's, not the figures'.
    const asConfirmed = read(auto, false);
    assert.ok(asConfirmed.figures.length > figures.length);
  });

  test("a consultant-led run's figures are as they were", () => {
    assert.deepEqual(read(cons, false), benchmarkFigures({ metrics: cons.input.metrics, scores: cons.input.scores ?? [], findings: cons.input.findings }));
  });
});
