import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { colour } from "../../design/tokens.ts";
import { scorecard, type ScorecardRow } from "../../narrative/select.ts";
import { buildDataset, checkNumbers } from "../../numbercheck/index.ts";
import type { Metric } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { pipelineOverTime, scorecardChart, stageFunnel, winRateBySegment, type Chart } from "../charts.ts";
import { compactMoney, money } from "../format.ts";

const { output } = await runFromRecording({ answers: loadAnswers() });
const metric = (id: string): Metric => output!.metrics.find((m) => m.id === id)!;
const dataset = buildDataset({ findings: output!.findings, metrics: output!.metrics, currency: "GBP" });

const rows = scorecard(output!.findings, []);
const charts = (): Chart[] => [winRateBySegment(metric("historic.win_rate")), stageFunnel(metric("historic.stage_conversion")), pipelineOverTime(metric("historic.pipeline_point_in_time_value"), "GBP"), scorecardChart(rows)];
const svg = (chart: Chart): string => (chart.kind === "chart" ? chart.svg : assert.fail(`${chart.id} is a gap`));
const texts = (markup: string): string[] => [...markup.matchAll(/<text [^>]*?(class="tick")?[^>]*>([^<]*)<\/text>/g)].filter((m) => !m[0].includes('class="tick"')).map((m) => m[2]!.replace(/&#39;/g, "'").replace(/&amp;/g, "&"));

describe("every chart", () => {
  test("renders as well-formed SVG with no browser", () => {
    for (const chart of charts()) {
      assert.equal(chart.kind, "chart");
      const markup = svg(chart);
      assert.match(markup, /^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg" viewBox="0 0 800 \d+"/);
      assert.match(markup, /role="img" aria-label="[^"]+"/);
      assert.ok(markup.endsWith("</svg>"));
      assert.equal((markup.match(/<text/g) ?? []).length, (markup.match(/<\/text>/g) ?? []).length);
    }
  });

  test("keeps to the design rules: flat fills, brand colours, no emoji", () => {
    const allowed = new Set(Object.values(colour).map((c) => c.toLowerCase()));
    for (const chart of charts()) {
      const markup = svg(chart);
      assert.doesNotMatch(markup, /gradient|filter=|<image|<script|\p{Extended_Pictographic}/iu);
      for (const [hex] of markup.matchAll(/#[0-9a-fA-F]{6}\b/g)) assert.ok(allowed.has(hex.toLowerCase()), hex);
    }
  });

  test("uses red for nothing but an alert", () => {
    assert.ok(!svg(winRateBySegment(metric("historic.win_rate"))).includes(colour.alert));
    assert.ok(!svg(stageFunnel(metric("historic.stage_conversion"))).includes(colour.alert));
    assert.ok(!svg(pipelineOverTime(metric("historic.pipeline_point_in_time_value"), "GBP")).includes(colour.alert));
    assert.ok(!svg(scorecardChart(rows.map((r) => ({ ...r, high: 0 })))).includes(colour.alert));
    assert.ok(svg(scorecardChart(rows)).includes(colour.alert));
  });

  test("every printed figure passes the number check", () => {
    for (const chart of charts()) {
      const printed = texts(svg(chart)).join(". ");
      const outcome = checkNumbers(printed, dataset);
      assert.equal(outcome.passed, true, `${chart.id}: ${JSON.stringify(outcome.unmatched)}`);
    }
  });

  test("the same input gives the same bytes", () => {
    assert.deepEqual(charts(), charts());
  });
});

describe("win rate by segment", () => {
  const chart = (): string => svg(winRateBySegment(metric("historic.win_rate")));

  test("shows team and record type, and never a person", () => {
    assert.match(chart(), /Sales EU/);
    assert.match(chart(), /11\.4%/);
    assert.doesNotMatch(chart(), /\[\[user|U00/);
  });

  test("a thin segment says so instead of drawing nothing", () => {
    const m = structuredClone(metric("historic.win_rate")) as Extract<Metric, { status: "measured" }>;
    m.segments = [{ dimension: "team", key: "Sales UK", n: 3, value: null, notMeasurableReason: "Sample below the calibrated minimum (3 of 20)" }];
    assert.match(svg(winRateBySegment(m)), /Too few deals to measure \(3\)/);
  });

  test("a label can't break the markup", () => {
    const m = structuredClone(metric("historic.win_rate")) as Extract<Metric, { status: "measured" }>;
    m.segments = [{ dimension: "team", key: "<script>&\"'", n: 30, value: 20, notMeasurableReason: null }];
    const markup = svg(winRateBySegment(m));
    assert.doesNotMatch(markup, /<script/);
    assert.match(markup, /&lt;script&gt;/);
  });

  test("a long label is shortened, not left to overflow", () => {
    const m = structuredClone(metric("historic.win_rate")) as Extract<Metric, { status: "measured" }>;
    m.segments = [{ dimension: "team", key: "A very long team name that would run straight into the bars", n: 30, value: 20, notMeasurableReason: null }];
    assert.match(svg(winRateBySegment(m)), /…/);
  });
});

describe("the overall line", () => {
  test("doesn't run through a value label", () => {
    const m = structuredClone(metric("historic.win_rate")) as Extract<Metric, { status: "measured" }>;
    const markup = svg(winRateBySegment(m));
    const line = Number(/<line x1="([\d.]+)"/.exec(markup)![1]);
    for (const hit of markup.matchAll(/<text x="([\d.]+)"[^>]*>(\d+(?:\.\d+)?%)<\/text>/g)) {
      const start = Number(hit[1]);
      assert.ok(start > line || start + 45 < line, `${hit[2]} sits on the line at ${line}`);
    }
  });
});

describe("a chart with nothing to draw gives its reason", () => {
  const gapped: Metric = { status: "not_measurable", id: "m", domain: "historic_pipeline", label: "Win rate", definition: "D.", unit: "percent", evidence: metric("historic.win_rate").evidence, exclusions: [], assumptions: [], reason: "No closed deals in the window." };

  test("not measurable", () => {
    for (const chart of [winRateBySegment(gapped), stageFunnel(gapped), pipelineOverTime(gapped, "GBP")]) {
      assert.deepEqual([chart.kind, chart.kind === "gap" && chart.reason], ["gap", "No closed deals in the window."]);
    }
  });

  test("missing altogether", () => {
    assert.equal(winRateBySegment(undefined).kind, "gap");
    assert.equal(stageFunnel(undefined).kind, "gap");
    assert.equal(pipelineOverTime(undefined, "GBP").kind, "gap");
    assert.equal(scorecardChart([]).kind, "gap");
  });

  test("no segments, and too few points", () => {
    const m = structuredClone(metric("historic.win_rate")) as Extract<Metric, { status: "measured" }>;
    m.segments = [];
    assert.equal(winRateBySegment(m).kind, "gap");
    assert.equal(stageFunnel(m).kind, "gap");
    m.series = [{ date: "2026-01-31", value: 5, n: 2, notMeasurableReason: null }];
    assert.equal(pipelineOverTime(m, "GBP").kind, "gap");
  });
});

describe("pipeline over time", () => {
  test("a month with no value breaks the line, rather than dropping to zero", () => {
    const m = structuredClone(metric("historic.pipeline_point_in_time_value")) as Extract<Metric, { status: "measured" }>;
    m.series = [{ date: "2026-01-31", value: 100, n: 2, notMeasurableReason: null }, { date: "2026-02-28", value: null, n: 0, notMeasurableReason: "Gap" }, { date: "2026-03-31", value: 120, n: 2, notMeasurableReason: null }, { date: "2026-04-30", value: 130, n: 2, notMeasurableReason: null }];
    const markup = svg(pipelineOverTime(m, "GBP"));
    const d = /<path d="(M[^"]+)" fill="none"/.exec(markup)![1]!;
    assert.equal((d.match(/M/g) ?? []).length, 2);
    assert.equal((d.match(/L/g) ?? []).length, 1);
    assert.match(markup, /Gaps are months we couldn't measure|couldn&#39;t measure/);
  });
});

describe("the scorecard", () => {
  const row = (overrides: Partial<ScorecardRow> = {}): ScorecardRow => ({ domain: "forecast", label: "Forecast", score: null, note: "Forecasting isn't in use.", assessed: false, issues: 0, high: 0, ...overrides });

  test("shows the reason for a missing score, and draws no bar", () => {
    const markup = svg(scorecardChart([row()]));
    assert.match(markup, /Forecasting isn/);
    assert.ok(!markup.includes(`fill="${colour.chartreuse}"`));
  });

  test("a domain nobody assessed doesn't claim zero issues", () => {
    assert.doesNotMatch(svg(scorecardChart([row()])), /0 issues/);
    assert.match(svg(scorecardChart([row({ assessed: true })])), /0 issues/);
  });

  test("a real score of zero is drawn as a score", () => {
    assert.match(svg(scorecardChart([row({ score: 0, note: "" })])), />0<\/text>/);
  });
});

describe("number formats", () => {
  test("write money for a UK reader", () => {
    assert.equal(money(18000, "GBP"), "£18,000");
    assert.equal(money(18000, "AUD"), "AUD 18,000");
    assert.equal(compactMoney(2892000, "GBP"), "£2.89m");
    assert.equal(compactMoney(652000, "GBP"), "£652k");
    assert.equal(compactMoney(950, "GBP"), "£950");
  });
});
