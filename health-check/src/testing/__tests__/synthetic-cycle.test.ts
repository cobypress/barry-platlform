import { test } from "node:test";
import assert from "node:assert/strict";
import { runFromRecording } from "../harness.ts";
import { loadAnswers } from "../replay.ts";

// The synthetic org's deals close at a usual pace in every quarter (DECISIONS.md 390).
// Its services block once gave every deal a creation date over a year ago, so the
// deck showed a median cycle of 355 days in the quarter from July 2026, against about
// 70 in the rest: an artefact no real org could show, read as a finding.

const { output } = await runFromRecording({ answers: loadAnswers() });

test("no quarter's median cycle is more than twice the window's, on won deals", () => {
  const quarterly = output!.metrics.find((m) => m.id === "historic.quarterly_median_cycle")!;
  const overall = output!.metrics.find((m) => m.id === "historic.sales_cycle_won")!;
  assert.ok(quarterly.status === "measured" && overall.status === "measured");
  const points = quarterly.series.filter((p) => p.value !== null);
  assert.ok(points.length >= 6, "enough quarters to compare");
  for (const p of points) assert.ok(p.value! <= 2 * overall.value!, `${p.date}: ${p.value} days against ${overall.value} for the window`);
});
