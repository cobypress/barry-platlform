import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { checkNumbers } from "../../numbercheck/index.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { compareSlide } from "../../render/compare.ts";
import { renderContext } from "../../render/html.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { noteSections, referenceMap } from "../../render/references.ts";
import { z } from "zod";
import { BENCHMARK_MIN_GROUP, BenchmarkRow, Score, type Metric } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { COMPARED_METRICS, comparisonId, comparisonNotes, compareRun, OWN_RANGE_MIN, positionOf, type CompareInput, type Comparison } from "../compare.ts";
import { BENCHMARK_METRICS, quarterOf } from "../export.ts";
import { PUBLISHED_FIGURES, type PublishedFigure } from "../published.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const SCORES = z.array(Score).parse(JSON.parse(readFileSync(join(dir, "scores.json"), "utf8")));
const PERIOD = quarterOf(run.analysisWindow.to);
const WIN = "historic.win_rate";

/** The synthetic run's metrics, with one figure set to a known value. */
function metricsWith(id: string, value: number): Metric[] {
  return output!.metrics.map((m) => (m.id === id && m.status === "measured" ? { ...m, value } : m));
}

type RowSpec = Partial<Pick<BenchmarkRow, "industry" | "headcountBand" | "period" | "metricKey">>;
const row = (value: number, spec: RowSpec = {}): BenchmarkRow =>
  BenchmarkRow.parse({ metricKey: WIN, ruleSetVersion: "rs-1", value, unit: "percent", industry: "technology", headcountBand: "51-200", userBand: "51-200", dealSizeBand: "5k-25k", period: PERIOD, ...spec });
const rows = (values: number[], spec: RowSpec = {}): BenchmarkRow[] => values.map((v) => row(v, spec));
const count = (n: number, value = 20, spec: RowSpec = {}): BenchmarkRow[] => rows(Array.from({ length: n }, () => value), spec);

const PUBLISHED: PublishedFigure = {
  id: "test-win-rate",
  metricKey: WIN,
  value: 21,
  unit: "percent",
  statistic: "on average",
  population: "synthetic companies in a test",
  measures: "Won deals as a share of closed deals.",
  differences: ["It counts no decision deals as lost."],
  basis: "records",
  source: { publisher: "Example Research", title: "Synthetic Benchmarks", year: 2025, url: "https://example.com/report", checked: "2026-11-05" },
};

function compare(extra: Partial<CompareInput> & { value?: number } = {}): Comparison {
  const { value = 30, ...rest } = extra;
  const input: CompareInput = {
    metrics: metricsWith(WIN, value),
    scores: SCORES,
    findings: output!.findings,
    industry: "technology",
    headcountBand: "51-200",
    period: PERIOD,
    benchmark: { rows: [], includesThisRun: false },
    published: [],
    ...rest,
  };
  return compareRun(input).find((c) => c.metric.key === WIN)!;
}

const own = (c: Comparison) => {
  assert.equal(c.own.status, "measured", c.own.status === "not_measurable" ? c.own.reason : "");
  return c.own as Extract<Comparison["own"], { status: "measured" }>;
};

describe("our own customers: widening the group", () => {
  const benchmark = (r: BenchmarkRow[]) => ({ rows: r, includesThisRun: false });

  test("industry and size first, from 10 orgs", () => {
    const c = compare({ benchmark: benchmark([...count(BENCHMARK_MIN_GROUP), ...count(20, 90, { industry: "retail" })]) });
    assert.equal(own(c).group.kind, "industry-and-size");
    assert.equal(own(c).group.orgs, BENCHMARK_MIN_GROUP);
    assert.equal(own(c).group.label, "Black Cloud customers in technology with 51 to 200 staff");
  });

  test("9 orgs in industry and size widens to industry", () => {
    const c = compare({ benchmark: benchmark([...count(9), ...count(1, 20, { headcountBand: "201-500" })]) });
    assert.equal(own(c).group.kind, "industry");
    assert.deepEqual(c.own.tried.map((g) => [g.kind, g.orgs]), [["industry-and-size", 9], ["industry", 10]]);
  });

  test("then size, then every customer", () => {
    const bySize = compare({ benchmark: benchmark([...count(4), ...count(6, 20, { industry: "retail" })]) });
    assert.equal(own(bySize).group.kind, "size");
    const everyone = compare({ benchmark: benchmark([...count(2), ...count(3, 20, { industry: "retail", headcountBand: "11-50" })]) });
    assert.equal(own(everyone).group.kind, "all");
    assert.equal(own(everyone).group.label, "Black Cloud customers");
    assert.deepEqual(everyone.own.tried.map((g) => g.orgs), [2, 2, 2, 5]);
  });

  test("every customer is shown from 5 orgs, and never below", () => {
    assert.equal(own(compare({ benchmark: benchmark(count(OWN_RANGE_MIN)) })).group.orgs, OWN_RANGE_MIN);
    const four = compare({ benchmark: benchmark(count(OWN_RANGE_MIN - 1)) });
    assert.equal(four.own.status, "not_measurable");
    assert.match(four.own.status === "not_measurable" ? four.own.reason : "", /from 4 Black Cloud customers\. We show a range from 5/);
  });

  test("industry and size groups need 10, not 5", () => {
    const c = compare({ benchmark: benchmark(count(9)) });
    assert.equal(own(c).group.kind, "all");
    assert.equal(own(c).group.floor, OWN_RANGE_MIN);
    assert.ok(c.own.tried.slice(0, 3).every((g) => g.floor === BENCHMARK_MIN_GROUP));
  });

  test("an unrecorded or 'other' industry isn't a group, and says why", () => {
    for (const industry of [null, "other" as const]) {
      const c = compare({ industry, benchmark: benchmark([...count(10, 20, { industry: "other" })]) });
      assert.equal(own(c).group.kind, "size");
      assert.deepEqual(c.own.tried.map((g) => g.orgs), [null, null, 10]);
      assert.ok(c.own.tried[0]!.why);
    }
    const noSize = compare({ headcountBand: null, benchmark: benchmark(count(12)) });
    assert.equal(own(noSize).group.kind, "industry");
    assert.equal(noSize.own.tried[0]!.orgs, null);
  });

  test("only other figures, other units and the latest four quarters count", () => {
    const old = count(10, 20, { period: "2024-Q1" });
    const other = count(10, 20, { metricKey: "historic.win_rate_by_value" });
    const c = compare({ benchmark: benchmark([...old, ...other, ...count(3)]) });
    assert.equal(c.own.status, "not_measurable");
    assert.deepEqual(c.own.tried.map((g) => g.orgs), [3, 3, 3, 3]);
  });

  test("a run whose own rows are in the set isn't compared with itself", () => {
    const set = [...rows([10, 15, 25, 35]), row(30)];
    assert.equal(own(compare({ value: 30, benchmark: { rows: set, includesThisRun: false } })).group.orgs, 5);
    const without = compare({ value: 30, benchmark: { rows: set, includesThisRun: true } });
    assert.equal(without.own.status, "not_measurable");
    assert.deepEqual(without.own.tried.at(-1)!.orgs, 4);
    // Its row can't be found, so it can't be left out, and nothing is shown.
    const missing = compare({ value: 31, benchmark: { rows: count(12), includesThisRun: true } });
    assert.equal(missing.own.status, "not_measurable");
  });
});

describe("positions", () => {
  test("quartiles between neighbours, in whole units", () => {
    // 10, 20, 30, 40, 50, 60: p25 at 22.5, median 35, p75 at 47.5.
    const c = compare({ value: 50, benchmark: { rows: rows([60, 10, 40, 20, 50, 30]), includesThisRun: false } });
    assert.deepEqual([own(c).p25, own(c).median, own(c).p75], [23, 35, 48]);
    assert.equal(own(c).position, "top quarter");
  });

  test("higher is better", () => {
    assert.equal(positionOf(10, 20, 40, "higher"), "bottom quarter");
    assert.equal(positionOf(20, 20, 40, "higher"), "middle half");
    assert.equal(positionOf(40, 20, 40, "higher"), "middle half");
    assert.equal(positionOf(41, 20, 40, "higher"), "top quarter");
  });

  test("lower is better: the top quarter is the shortest or lowest", () => {
    assert.equal(positionOf(10, 20, 40, "lower"), "top quarter");
    assert.equal(positionOf(30, 20, 40, "lower"), "middle half");
    assert.equal(positionOf(41, 20, 40, "lower"), "bottom quarter");
    assert.equal(COMPARED_METRICS.find((m) => m.key === "historic.sales_cycle_won")!.better, "lower");
  });

  test("against the figures as shown, so a tie with a quartile is the middle half", () => {
    // 10, 20, 25, 29, 30, 40: p75 is 29.75, shown as 30. Against 29.75, 30 would be the top quarter.
    const tie = compare({ value: 30, benchmark: { rows: rows([10, 20, 25, 29, 30, 40]), includesThisRun: false } });
    assert.equal(own(tie).p75, 30);
    assert.equal(own(tie).position, "middle half");
  });
});

describe("published research", () => {
  test("is compared beside our own, never combined with it", () => {
    const c = compare({ value: 25, published: [PUBLISHED], benchmark: { rows: [], includesThisRun: false } });
    assert.equal(c.published.length, 1);
    assert.equal(c.published[0]!.difference, 4);
    assert.equal(c.published[0]!.side, "better");
    // No rows: the published figure doesn't stand in for our own range.
    assert.equal(c.own.status, "not_measurable");
  });

  test("lower is better reads the other way, and a tie is level", () => {
    const cycle: PublishedFigure = { ...PUBLISHED, metricKey: "historic.sales_cycle_won", unit: "days", value: 90 };
    const metrics = output!.metrics.map((m) => (m.id === "historic.sales_cycle_won" && m.status === "measured" ? { ...m, value: 80 } : m));
    const c = compareRun({ metrics, scores: SCORES, findings: output!.findings, industry: null, headcountBand: null, period: PERIOD, benchmark: null, published: [cycle] }).find((x) => x.metric.key === "historic.sales_cycle_won")!;
    assert.equal(c.published[0]!.difference, -10);
    assert.equal(c.published[0]!.side, "better");
    assert.equal(compare({ value: 21, published: [PUBLISHED] }).published[0]!.side, "level");
  });

  test("a figure in another unit is never compared", () => {
    assert.equal(compare({ published: [{ ...PUBLISHED, unit: "days" }] }).published.length, 0);
  });

  test("is never shown without its source", () => {
    const c = compare({ value: 25, published: [PUBLISHED] });
    const notes = comparisonNotes(c);
    const at = notes.findIndex((l) => l.label === "Published research");
    assert.equal(notes[at]!.text, "Example Research, Synthetic Benchmarks (2025): 21%, on average.");
    assert.ok(notes.some((l) => l.label === "Who it describes" && l.text.startsWith("synthetic companies")));
    assert.ok(notes.some((l) => l.label === "Source" && l.text.includes("https://example.com/report") && l.text.includes("2026-11-05")));
    assert.ok(notes.some((l) => l.label === "How it differs from ours" && l.text.includes("no decision")));
    const slide = compareSlide(renderContext(pseudonyms, "GBP", false), [c]).html;
    const cell = /<td>21% on average[^]*?<\/td>/.exec(slide)?.[0] ?? "";
    assert.match(cell, /Example Research, 2025/);
  });

  test("every published figure has a full source, a population, what it measures, and a metric we compare", () => {
    const ids = new Set<string>();
    for (const f of PUBLISHED_FIGURES) {
      assert.ok(!ids.has(f.id), f.id);
      ids.add(f.id);
      assert.ok(COMPARED_METRICS.some((m) => m.key === f.metricKey && m.unit === f.unit), f.id);
      assert.ok(f.source.publisher && f.source.title && f.statistic && f.population && f.measures, f.id);
      assert.match(f.source.url, /^https:\/\//, f.id);
      assert.match(f.source.checked, /^\d{4}-\d{2}-\d{2}$/, f.id);
      assert.ok(f.source.year >= 2022 && f.source.year <= Number(f.source.checked.slice(0, 4)), f.id);
      assert.ok(Number.isFinite(f.value), f.id);
    }
  });
});

describe("not measurable", () => {
  test("a figure the run didn't measure, with the run's reason", () => {
    const metrics = output!.metrics.map((m) => (m.id === WIN ? ({ status: "not_measurable", id: m.id, domain: m.domain, label: m.label, definition: m.definition, unit: m.unit, evidence: m.evidence, exclusions: [], assumptions: [], reason: "Too few closed deals." } as Metric) : m));
    const c = compare({ metrics, published: [PUBLISHED], benchmark: { rows: count(20), includesThisRun: false } });
    assert.equal(c.value, null);
    assert.equal(c.reason, "Too few closed deals.");
    assert.equal(c.published.length, 0);
    assert.ok(c.publishedReason);
    assert.equal(c.own.status, "not_measurable");
  });

  test("without our own set, or before anyone has contributed", () => {
    const none = compare({ benchmark: null });
    assert.match(none.own.status === "not_measurable" ? none.own.reason : "", /didn't have the Black Cloud customer figures/);
    const empty = compare({ benchmark: { rows: [], includesThisRun: false } });
    assert.match(empty.own.status === "not_measurable" ? empty.own.reason : "", /No Black Cloud customer has contributed/);
  });

  test("no published research that measures it our way", () => {
    assert.match(compare({ published: [] }).publishedReason ?? "", /haven't found published research/);
  });
});

describe("in the report", () => {
  const intake = Intake.parse({ industry: "technology", headcountBand: "51-200", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
  // Every compared figure, from 12 synthetic orgs, so each row has a range and a position.
  const set = COMPARED_METRICS.flatMap((m) => [5, 9, 12, 18, 22, 25, 31, 36, 44, 52, 60, 75].map((v) => BenchmarkRow.parse({ ...row(v), metricKey: m.key, unit: m.unit })));
  const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake, benchmark: { rows: set, includesThisRun: false } };
  const report = buildReport(input);

  test("covers every compared figure, with a position wherever the run measured it", () => {
    assert.deepEqual(report.comparisons.map((c) => c.metric.key), COMPARED_METRICS.map((m) => m.key));
    for (const c of report.comparisons) assert.equal(c.own.status === "measured", c.value !== null, c.metric.key);
    assert.ok(report.comparisons.some((c) => c.own.status === "measured"));
  });

  test("the raw notes have a numbered section with every comparison's working", async () => {
    const section = noteSections(report).find((s) => s.key === "comparisons")!;
    assert.equal(section.title, "How you compare");
    assert.deepEqual(section.items.map((i) => i.id), COMPARED_METRICS.map((m) => comparisonId(m.key)));
    assert.equal(noteSections(report).at(-1)!.key, "manual_steps");
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    for (const item of section.items) assert.ok(text.includes(item.ref), item.ref);
    assert.match(text, /Black Cloud customers in technology with 51 to 200 staff, from 12 orgs: +the middle half runs from/);
    assert.match(text, /Groups tried, narrowest first/);
  });

  test("every figure on the slide traces through the number check", () => {
    const r = renderContext(pseudonyms, "GBP", false, referenceMap(noteSections(report)));
    const html = compareSlide(r, report.comparisons).html;
    // As the layout check reads it: references are places, not figures.
    const text = html.replace(/<span class="ref" data-ref>[^<]*<\/span>/g, " ").replace(/<[^>]+>/g, " ").replace(/&amp;/g, "&").replace(/&#39;|&apos;/g, "'");
    const result = checkNumbers(text, report.dataset);
    assert.deepEqual(result.unmatched, []);
    assert.ok(result.checked > 10, `${result.checked} figures checked`);
    for (const c of report.comparisons) assert.ok(html.includes(r.ref(comparisonId(c.metric.key))), c.metric.key);
  });

  test("and so does the working in the notes, against its own section's scope", () => {
    const section = report.outputs.raw_notes.sections.find((s) => s.key === "comparisons")!;
    assert.ok(report.scopes.get(section.id)!.includes(comparisonId(WIN)));
    const versions = report.outputs.raw_notes.versions.find((v) => v.id === section.currentVersionId)!;
    assert.equal(versions.numberCheck.passed, true);
  });

  test("benchmark rows that only exist in the comparison hold nothing beyond the strict row", () => {
    for (const r of set) assert.deepEqual(Object.keys(r).sort(), Object.keys(BenchmarkRow.shape).sort());
    assert.ok(COMPARED_METRICS.every((m) => BENCHMARK_METRICS.some((b) => b.key === m.key)));
  });
});
