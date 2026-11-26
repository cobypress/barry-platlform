import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { areasOf, scorecard } from "../../narrative/select.ts";
import { refusalSentence } from "../../salesforce/refusal-words.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { withWholeNotes } from "../area-notes.ts";
import { scorecardChart, scorecardOptions } from "../charts.ts";
import { fullReportHtml } from "../full-report.ts";
import { loadRun } from "../render-run.ts";
import { renderReleased, type ApprovedRun } from "../render-released.ts";
import type { ReportSection, SectionVersion } from "../../schemas/index.ts";

// An unscored area's reason in the documents of a run analysed with
// HC_AREA_REASONS on (DECISIONS.md 769): the scorecard chart prints a whole
// sentence or nothing, never one cut; the full report lists every area the score
// leaves out with its reason whole, paginated as that list already is; the
// documents' own number and fit checks pass. Any other run's documents are as before.

const recorded = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES, log: () => {} });
const extracts = loadExtracts(recorded.dir);
const LONG = refusalSentence("Account", "no_permission");

/** The recorded run, copied, analysed from these extracts, with the switch on or off. */
function folder(changed: Map<string, unknown>, on: boolean, minSample?: number): string {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-area-")), "hc-2026-10-31-0000000a");
  cpSync(recorded.dir, dir, { recursive: true });
  const calibration = { ...loadCalibration(), ...(minSample ? { minSample } : {}) };
  const a = analyseExtracts(changed, { calibration, modules: ALL_MODULES });
  const run = JSON.parse(readFileSync(join(dir, "run.json"), "utf8"));
  if (on) run.areaReasons = true;
  writeFileSync(join(dir, "run.json"), JSON.stringify(run));
  writeFileSync(join(dir, "calibration.json"), JSON.stringify(calibration));
  writeFileSync(join(dir, "findings.json"), JSON.stringify(a.findings));
  writeFileSync(join(dir, "metrics.json"), JSON.stringify(a.metrics));
  writeFileSync(join(dir, "scores.json"), JSON.stringify(scoreDomains(run.modules, a.findings, run.ruleSetVersion, on ? { areaReasons: true } : {})));
  return dir;
}
const unreadableAll = new Map([...extracts].map(([id, v]) => {
  const x = v as Record<string, unknown>;
  if ("status" in x) return [id, { status: "unavailable", reason: LONG, source: x.source ?? { kind: "soql", api: "rest", query: "x" } }];
  const out = clone(x);
  for (const [k, part] of Object.entries(out)) if (part && typeof part === "object" && "status" in part) out[k] = { status: "unavailable", reason: LONG, source: (part as { source?: unknown }).source };
  return [id, out];
}));

const chartNotes = (report: BuiltReport): string[] => {
  const rows = withWholeNotes(report, scorecard(report.teamFindings, report.input.scores ?? [], areasOf(report.input.run, report.teamFindings)));
  const chart = scorecardChart(rows, scorecardOptions(report));
  assert.equal(chart.kind, "chart");
  const texts = chart.kind === "chart" ? [...chart.svg.matchAll(/<text[^>]*>([^<]*)<\/text>/g)].map((m) => m[1]!.replace(/&#39;|&apos;/g, "'")) : [];
  return rows.filter((r) => r.score === null).map((r) => texts.find((t) => (r.wholeNotes ?? [r.note]).some((n) => n === t || t.endsWith("…"))) ?? "");
};

describe("the scorecard chart with the areas' own reasons (DECISIONS.md 769)", () => {
  const on = loadRun(folder(unreadableAll, true));
  const off = loadRun(folder(unreadableAll, false));
  const reportOn = buildReport(on.input);
  const reportOff = buildReport(off.input);

  test("on: every unscored area's note is a whole sentence it was given, none cut", () => {
    const rows = withWholeNotes(reportOn, scorecard(reportOn.teamFindings, reportOn.input.scores ?? [], areasOf(reportOn.input.run, reportOn.teamFindings)));
    const chart = scorecardChart(rows, scorecardOptions(reportOn));
    const svg = chart.kind === "chart" ? chart.svg : "";
    assert.ok(!svg.includes("…"), "nothing cut");
    const unscored = rows.filter((r) => r.score === null && r.wholeNotes);
    assert.ok(unscored.length >= 15, `${unscored.length}`);
    for (const row of unscored) assert.ok(row.wholeNotes!.some((n) => svg.includes(`>${n.replace(/'/g, "&#39;")}<`) || svg.includes(`>${n}<`)), `${row.domain}: ${row.wholeNotes!.join(" / ")}`);
  });

  test("off: the rows and the chart are as they were", () => {
    const rows = scorecard(reportOff.teamFindings, reportOff.input.scores ?? [], areasOf(reportOff.input.run, reportOff.teamFindings));
    assert.equal(withWholeNotes(reportOff, rows), rows);
    assert.ok(chartNotes(reportOff).length > 0);
  });

  test("on: the full report lists every area the score leaves out, with its reason whole", () => {
    const html = fullReportHtml(reportOn, on.pseudonyms).replace(/&#39;/g, "'");
    const at = html.indexOf("Where we can't give a view yet");
    assert.ok(at > 0);
    for (const s of reportOn.input.scores ?? []) if (s.status === "not_measurable") assert.ok(html.slice(at).includes(s.reason.slice(0, 60)) || html.slice(at).includes(s.reason.replace(/\bcalibrated /g, "").slice(0, 40)), `${s.domain}: ${s.reason}`);
  });
});

/** Every section approved, as a consultant-led run is released. */
function approved(dir: string): ApprovedRun {
  const report = buildReport(loadRun(dir).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      sections.push({ ...s, status: "approved", approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } });
      versions.push(built.versions.find((x) => x.id === s.currentVersionId)!);
    }
  }
  return { intake: null, sections, versions, overrides: [] };
}

describe("the documents' own checks with the areas' own reasons (DECISIONS.md 769)", () => {
  test("every area unscored with a long reason, on and off: the list paginates, nothing overflows, every figure traces", async () => {
    for (const on of [true, false]) {
      const dir = folder(unreadableAll, on);
      const made = await renderReleased(dir, approved(dir), join(mkdtempSync(join(tmpdir(), "hc-area-out-")), "out"));
      assert.ok(!("refused" in made), "made");
      if ("refused" in made) return;
      assert.deepEqual(made.held, [], `on ${on}`);
      assert.deepEqual(made.warnings, [], `on ${on}`);
      if (on) {
        const unscored = (loadRun(dir).input.scores ?? []).filter((s) => s.status === "not_measurable").length;
        assert.ok(unscored >= 15, `${unscored} unscored areas`);
      }
    }
  });

  test("every sample thin, on: figures in an area's reason trace, and nothing is held", async () => {
    const dir = folder(extracts, true, 100_000);
    const made = await renderReleased(dir, approved(dir), join(mkdtempSync(join(tmpdir(), "hc-area-out-")), "out"));
    assert.ok(!("refused" in made));
    if ("refused" in made) return;
    assert.deepEqual(made.held, []);
    assert.deepEqual(made.warnings, []);
    assert.ok((loadRun(dir).input.scores ?? []).some((s) => s.status === "not_measurable" && /\d/.test(s.reason)), "a reason with a figure in it");
  });
});
