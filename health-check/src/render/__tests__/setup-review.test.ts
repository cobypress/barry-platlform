import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport } from "../../narrative/sections.ts";
import { domainLabel, recordAreasLine } from "../../narrative/select.ts";
import { tempDir, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { executiveHtml } from "../executive.ts";
import { DECK_RANGE, deckRangeFor, fullReportHtml, SETUP_REVIEW_DECK_RANGE } from "../full-report.ts";
import { plainWords } from "../format.ts";
import { loadRun, renderRun } from "../render-run.ts";

// A set-up review's documents: the same engine and renderers, with nothing that
// says "not measured" only because a set-up review doesn't read records.

const INTAKE = { customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" };

const { dir } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), answers: loadAnswers(), scope: "essentials", transport: new Simulator() });
writeFileSync(join(dir, "intake.json"), JSON.stringify(INTAKE));
const { input, pseudonyms } = loadRun(dir);
const report = buildReport(input);
const executive = executiveHtml(report, pseudonyms);
const full = fullReportHtml(report, pseudonyms);
const RECORD_AREAS = ["Current pipeline", "Historic pipeline", "Forecast", "Data quality", "Activity", "Lead funnel"];
const text = (html: string): string => html.replace(/<[^>]+>/g, " ").replace(/&#39;/g, "'").replace(/&amp;/g, "&");

describe("a set-up review's documents", () => {
  test("say what they are on both covers", () => {
    for (const html of [executive, full]) {
      assert.match(html, /<h1>Set-up review<\/h1>/);
      assert.doesNotMatch(html, /Salesforce health check<\/h1>/);
    }
  });

  test("have no pipeline pages, no comparison, and no pipeline sections", () => {
    for (const html of [executive, full]) {
      assert.doesNotMatch(html, /Your pipeline today|Open pipeline by stage|Win rate by segment|How you compare|What your data can.t tell us yet|Deep dive/);
    }
    const keys = Object.values(report.outputs).flatMap((o) => o.sections.map((s) => s.key));
    for (const key of ["pipeline", "pipeline_deep_dive", "comparisons", "domain_historic_pipeline", "domain_current_pipeline"]) assert.ok(!keys.includes(key), key);
    assert.equal(report.outputs.raw_notes.sections.find((s) => s.key === "pipeline_metrics")?.title, "Set-up metrics");
  });

  test("never list a record area as a gap, and say once, as a fact, where they're covered", () => {
    const line = recordAreasLine(input.run)!;
    assert.match(line, /measured from your CRM records, which a set-up review doesn't read\. They're part of the full Barometer check\.$/);
    assert.equal(text(full).split(plainWords(line)).length, 2, "the line appears once");
    const gaps = text(full).split("Where we can't give a view yet")[1] ?? "";
    for (const area of RECORD_AREAS) assert.ok(!new RegExp(`\\b${area}\\b`).test(gaps.split("Areas measured from records")[0]!), area);
    // The scorecard rows are the set-up areas only.
    assert.ok(!/Not assessed in this run/.test(full + executive));
    assert.equal(domainLabel("historic_pipeline"), "Historic pipeline");
  });

  test("keep a Barometer score from the set-up areas", () => {
    assert.equal(report.overall.status, "measured");
    assert.ok(report.overall.status === "measured" && report.overall.areas.every((a) => input.run.modules.includes(a.domain)));
  });

  test("print within their ranges and pass the layout and number checks", async () => {
    const result = await renderRun(dir);
    assert.deepEqual([...result.layout.executive.problems, ...result.layout.full.problems], []);
    assert.ok(result.passed);
    assert.ok(result.layout.full.pages >= SETUP_REVIEW_DECK_RANGE.min && result.layout.full.pages <= SETUP_REVIEW_DECK_RANGE.max, `${result.layout.full.pages} slides`);
  });
});

describe("a complete run's documents", () => {
  test("are unchanged: the health check cover, its pipeline part and its own range", async () => {
    const { dir: complete } = await runFromRecording({ answers: loadAnswers() });
    writeFileSync(join(complete, "intake.json"), JSON.stringify(INTAKE));
    const loaded = loadRun(complete);
    assert.equal(loaded.input.run.scope, "complete");
    const built = buildReport(loaded.input);
    const html = fullReportHtml(built, loaded.pseudonyms);
    assert.match(html, /<h1>Salesforce health check<\/h1>/);
    // Its pipeline pages, whether behind their own opener or, in the densest deck, introduced on the areas page.
    assert.match(html, /Open pipeline by stage/);
    assert.equal(recordAreasLine(loaded.input.run), null);
    assert.ok(built.outputs.full_health_check.sections.some((s) => s.key === "pipeline_deep_dive"));
    assert.equal(DECK_RANGE.min, 20);
  });
});

describe("the deck's ceiling (DECISIONS.md 281)", () => {
  const max = (scope: "complete" | "essentials", modules: string[]): number => deckRangeFor({ scope, modules: ["configuration", ...modules] }).max;

  test("is 120 for Sales Cloud, and 10 more for each optional module, up to 150", () => {
    assert.deepEqual(deckRangeFor({ scope: "complete", modules: ["configuration"] }), { min: 20, max: 120 });
    assert.equal(max("complete", ["service_operations", "service_setup"]), 130);
    assert.equal(max("complete", ["cpq_quoting", "cpq_setup", "account_engagement"]), 140);
    assert.equal(max("complete", ["service_operations", "service_setup", "cpq_quoting", "cpq_setup", "account_engagement"]), 150);
  });

  test("is 66 for a set-up review, and 6 more a module, up to 81, counting a module whichever of its areas runs", () => {
    assert.deepEqual(deckRangeFor({ scope: "essentials", modules: ["configuration"] }), { min: 12, max: 66 });
    assert.equal(max("essentials", ["cpq_setup"]), 72);
    assert.equal(max("essentials", ["cpq_setup", "account_engagement"]), 78);
    assert.equal(max("essentials", ["service_setup", "cpq_setup", "account_engagement"]), 81);
  });
});
