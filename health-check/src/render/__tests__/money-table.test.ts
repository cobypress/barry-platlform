import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { compactMoney } from "../format.ts";
import { renderContext } from "../html.ts";
import { linesToName, moneyPage, moneySlides } from "../money.ts";

// The money table, from the review of night 3's reading passes (docs/overnight/night-3/review-reading.md).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

describe("the money table", () => {
  test("on the full report's slides every line is named, in order, and the rows add up to the total beneath them (DECISIONS.md 439)", () => {
    // The slide used to name six and add up the rest. The full report has no ceiling, so it carries on instead.
    const report = buildReport(base);
    const money = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    assert.ok(money.length >= 8, `${money.length} lines: more than one slide held before`);
    const slides = moneySlides(renderContext(pseudonyms, "GBP", false), report);
    const m = (n: number) => compactMoney(n, "GBP");
    const rows = slides.flatMap((s) => [...s.matchAll(/<tr( class="total")?><td>([\s\S]*?)<\/td><td class="num">([^<]*)<\/td><td class="num">([^<]*)<\/td><\/tr>/g)]);
    assert.doesNotMatch(slides.join(""), /smaller line/);
    const lines = rows.filter((x) => !x[1]);
    assert.deepEqual(lines.map((x) => x[3]), money.map((g) => m(g.counted.expected)));
    lines.forEach((x, i) => assert.ok(x[2]!.startsWith(money[i]!.label.replace(/'/g, "&#39;")), `${x[2]}: ${money[i]!.label}`));
    // The total once, on the last slide.
    assert.deepEqual(slides.map((s) => (s.match(/<tr class="total">/g) ?? []).length), [...slides.slice(1).map(() => 0), 1]);
    const { total } = report.value;
    for (const k of ["low", "expected", "high"] as const) assert.ok(Math.abs(money.reduce((sum, g) => sum + g.counted[k], 0) - total[k]) < 0.01 * money.length, k);
  });

  test("the page's last row never stands for one line whose label fits its row (DECISIONS.md 438)", () => {
    const fits = () => true;
    const long = () => false;
    // The page holds seven of eight: the eighth's label fits the row, but its basis doesn't fit the page, so the row stands for two.
    assert.equal(linesToName(8, 8, (n) => n <= 7, fits), 6);
    // Where its label is longer than the row's own words, the row stands for it alone: it takes less room than the line.
    assert.equal(linesToName(8, 8, (n) => n <= 7, long), 7);
    // Where the page holds every line, each is named.
    assert.equal(linesToName(8, 8, () => true, fits), 8);
    // Past the most it has room for, the last row stands for the rest, never fewer than two.
    assert.equal(linesToName(9, 8, () => true, fits), 7);
    assert.equal(linesToName(16, 8, (n) => n <= 5, fits), 5);
    // One line is always named, whatever the room.
    assert.equal(linesToName(2, 8, () => false, fits), 1);
    assert.equal(linesToName(3, 8, () => false, fits), 1);
  });

  test("each line's figure before the share is said in words that fit its kind (DECISIONS.md 395)", () => {
    const report = buildReport(base);
    const html = moneyPage(renderContext(pseudonyms, "GBP", false), report);
    const money = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    const bases = [...html.matchAll(/<span class="basis">([^<]*)<\/span>/g)].map((x) => x[1]!);
    const shown = money.slice(0, bases.length);
    const words = { revenue_recoverable: "if all came back", conversion_uplift: "if the gap closed in full", licence_waste: "if every seat were released", time_cost: "if all those hours were saved" } as const;
    assert.ok(new Set(shown.map((g) => g.kind)).size >= 3, shown.map((g) => g.kind).join(", "));
    shown.forEach((g, i) => assert.ok(bases[i]!.startsWith(`${compactMoney(g.counted.annual, "GBP")} ${words[g.kind as keyof typeof words]}`), `${g.id}: ${bases[i]}`));
  });
});
