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
import { moneyPage } from "../money.ts";

// The money table, from the review of night 3's reading passes (docs/overnight/night-3/review-reading.md).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

describe("the money table", () => {
  test("on a slide with more lines than it holds, the rest are given with their amount, so the rows add up to the total", () => {
    const report = buildReport(base);
    const money = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    assert.ok(money.length >= 8, `${money.length} lines: the slide holds 7`);
    const html = moneyPage(renderContext(pseudonyms, "GBP", false), report, "slide");
    const rest = money.slice(6);
    const restRow = /<tr><td colspan[^>]*>[\s\S]*?<\/tr>|<tr><td>And \d+ smaller[\s\S]*?<\/tr>/.exec(html)?.[0] ?? "";
    assert.match(restRow, /And 2 smaller lines/);
    const m = (n: number) => compactMoney(n, "GBP");
    const sum = (k: "expected" | "low" | "high") => rest.reduce((s, g) => s + g.counted[k], 0);
    assert.ok(restRow.includes(m(sum("expected"))), `${restRow} should carry ${m(sum("expected"))}`);
    assert.ok(restRow.includes(`${m(sum("low"))} to ${m(sum("high"))}`), restRow);
  });

  test("each line's figure before the share is said in words that fit its kind (DECISIONS.md 395)", () => {
    const report = buildReport(base);
    const html = moneyPage(renderContext(pseudonyms, "GBP", false), report, "page");
    const money = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    const bases = [...html.matchAll(/<span class="basis">([^<]*)<\/span>/g)].map((x) => x[1]!);
    const shown = money.slice(0, bases.length);
    const words = { revenue_recoverable: "if all came back", conversion_uplift: "if the gap closed in full", licence_waste: "if every seat were released", time_cost: "if all those hours were saved" } as const;
    assert.ok(new Set(shown.map((g) => g.kind)).size >= 3, shown.map((g) => g.kind).join(", "));
    shown.forEach((g, i) => assert.ok(bases[i]!.startsWith(`${compactMoney(g.counted.annual, "GBP")} ${words[g.kind as keyof typeof words]}`), `${g.id}: ${bases[i]}`));
  });
});
