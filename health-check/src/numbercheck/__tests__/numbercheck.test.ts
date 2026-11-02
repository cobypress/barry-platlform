import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { agrees, buildDataset, checkNumbers, extractFigures, normalise, scopeDataset, SOURCE, toNumberCheckResult } from "../index.ts";
import { NumberCheckResult, type Finding, type Metric, type NumberOverride } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

const NOW = "2026-10-31T12:00:00.000Z";
const evidence = { kind: "soql", api: "rest", query: "SELECT Id FROM Opportunity LIMIT 2000", count: 70, dateRange: { from: "2024-10-31", to: "2026-10-31" }, exclusions: [], capturedAt: NOW } as Finding["evidence"][number];

function finding(overrides: Partial<Finding> = {}): Finding {
  return {
    id: "f-000000000001",
    ruleId: "dq.past_close",
    ruleVersion: 1,
    domain: "data_quality",
    status: "issue",
    title: "Your pipeline includes deals that should already have closed",
    summary: "Open deals with a close date in the past: 15 of 70 (21.4%). The worst segment is rep [[user:U0014]] at 33.3%.",
    recommendation: "Add a weekly check to pipeline reviews.",
    recommendationType: "change",
    severity: "high",
    confidence: "high",
    evidence: [evidence],
    value: { kind: "revenue", amount: 652000, unit: "GBP", period: "open pipeline today", assumptions: ["15 deals affected"] },
    effort: { size: "S" },
    owner: "Sales operations",
    dependencies: [],
    guards: [],
    segmentsChecked: [{ dimension: "rep", segments: 7 }],
    segmentExemptReason: null,
    subjects: [{ type: "rep", label: "[[user:U0014]]" }],
    facts: { hits: 15, n: 70, rate: 21.4, worstSegmentRate: 33.3 },
    notMeasurableReason: null,
    manualStepIds: [],
    ...overrides,
  };
}

function metric(overrides: Partial<Extract<Metric, { status: "measured" }>> = {}): Metric {
  return {
    status: "measured",
    id: "historic.win_rate",
    domain: "historic_pipeline",
    label: "Win rate",
    definition: "Won deals as a share of won and lost deals.",
    unit: "percent",
    evidence: [evidence],
    exclusions: [],
    assumptions: [],
    value: 35,
    n: 177,
    dateRange: { from: "2024-10-31", to: "2026-10-31" },
    segments: [{ dimension: "team", key: "Sales EU", n: 88, value: 11.4, notMeasurableReason: null }, { dimension: "team", key: "Team 2", n: 12, value: null, notMeasurableReason: "Sample below the calibrated minimum (12 of 20)" }],
    segmentExemptReason: null,
    series: [],
    stats: { won: 62, lost: 115 },
    ...overrides,
  };
}

const pipeline: Metric = metric({ id: "historic.pipeline_point_in_time_value", label: "Open pipeline", unit: "currency", value: 2892000, n: 70, segments: [{ dimension: "stage", key: "Discovery", n: 22, value: 777000, notMeasurableReason: null }], stats: {} });
const cycle: Metric = metric({ id: "historic.sales_cycle_won", label: "Sales cycle", unit: "days", value: 63.5, n: 62, segments: [{ dimension: "team", key: "Sales UK", n: 52, value: 63.5, notMeasurableReason: null }], stats: { p25: 53.3, p75: 71.8, assumedDays: 60 } });
const half: Finding = finding({ id: "f-000000000002", summary: "Half-size example: 35 of 70 (50%).", title: "Half", facts: { hits: 35, n: 70, rate: 50 }, value: { kind: "not_sized", reason: "Not sized." }, subjects: [] });

const data = buildDataset({ findings: [finding(), half], metrics: [metric(), pipeline, cycle], currency: "GBP" });
const blocks = (text: string): void => assert.equal(checkNumbers(text, data).passed, false, `should block: ${text}`);
const passes = (text: string): void => {
  const outcome = checkNumbers(text, data);
  assert.equal(outcome.passed, true, `should pass: ${text} ${JSON.stringify(outcome.unmatched)}`);
};

describe("figures that trace pass", () => {
  for (const text of [
    "Your win rate is 35%.",
    "Of your open deals, 15 of 70 are past their close date.",
    "That's 21.4% of open deals, or 15 out of 70.",
    "Sales EU wins 11.4% of its deals.",
    "We sized this at £652,000.",
    "We sized this at GBP 652,000.",
    "We sized this at 652,000 GBP.",
    "Open pipeline is £2,892,000.",
    "Open pipeline is about £2.9m.",
    "Open pipeline is about £2.89 million.",
    "Open pipeline is about 2.9 million pounds.",
    "About £652k is exposed.",
    "The median cycle is 63.5 days.",
    "The cycle runs at around 64 days.",
    "Thirty-five per cent of deals are won.",
    "Fifteen of seventy deals are affected.",
    "Half of these deals have slipped.",
    "A third of the pipeline is affected.",
    "Rep [[user:U0014]] is at 33.3%.",
    "The analysis ran from 2024-10-31 to 31 October 2026.",
    "Between October 2024 and October 2026 you closed 177 deals.",
    "We looked at deals from 2024.",
    "**35%** of your deals are won.",
    "Your win rate is 35 percent, with `62` won.",
    "No figures here at all.",
    "We'd do this first, and bring in a second person, with no one left out.",
    "Double-check this with a third party before any one-off change.",
    "Team 2 has too few deals to measure.",
    "",
  ]) test(text || "(empty)", () => passes(text));
});

describe("invented figures are blocked", () => {
  for (const text of [
    "Your win rate is 36%.",
    "Your win rate is 34%.",
    "You have 71 open deals.",
    "15 of 71 deals are affected.",
    "We sized this at £652,001.",
    "We sized this at £700,000.",
    "We sized this at $652,000.",
    "We sized this at €652,000.",
    "We sized this at 652,000 euros.",
    "Open pipeline is about £2.8m.",
    "Open pipeline is about £1m.",
    "Open pipeline is £2,900,000.",
    "The sales cycle is 70 days.",
    "Forty per cent of deals are won.",
    "Twenty-one deals are affected.",
    "Three hundred deals are affected.",
    "Two thousand reps are affected.",
    "About a fifth of leads are stale.",
    "Three quarters of deals slipped.",
    "One in ten deals is lost.",
    "You could save half a million pounds.",
    "This is a 1.5 million pound problem.",
    "This could add 12% to revenue.",
    "It takes 6 weeks.",
    "It takes 3 months.",
    "Hundreds of deals are stale.",
    "Thousands of records are affected.",
    "Tens of thousands of pounds are at stake.",
  ]) test(text, () => blocks(text));
});

describe("a true value in the wrong unit is blocked", () => {
  for (const text of [
    "Your win rate is 177%.",
    "The win rate is 70%.",
    "The cycle is 177 days.",
    "That's 62 days.",
    "That's £70.",
    "That's £21.4.",
    "That's 21.4 deals.",
    "A gap of 21.4 points.",
    "Costs 15%.",
    "Pipeline of 2,892,000 deals.",
    "The cycle is 63.5%.",
  ]) test(text, () => blocks(text));
});

describe("rounding", () => {
  test("a figure rounded to its written precision agrees", () => {
    assert.ok(agrees(75, 1, 74.7));
    assert.ok(agrees(74.7, 0.1, 74.7));
    assert.ok(agrees(64, 1, 63.5));
    assert.ok(agrees(2.9e6, 1e5, 2892000));
  });

  test("a figure rounded the wrong way doesn't", () => {
    assert.ok(!agrees(74, 1, 74.7));
    assert.ok(!agrees(63, 1, 63.5));
    assert.ok(!agrees(21.43, 0.01, 21.4), "more precise than the data knows");
  });

  test("heavy rounding is refused even when it's arithmetically right", () => {
    assert.ok(!agrees(1e6, 1e6, 1.4e6));
    assert.ok(!agrees(1, 1, 0.6));
    assert.ok(!agrees(0, 1, 0.4), "nothing rounds to zero");
  });

  test("a sign can't flip", () => {
    assert.ok(!agrees(-5, 1, 5));
    assert.ok(!agrees(5, 1, -5));
  });

  test("a decimal doesn't falsely match through float error", () => {
    assert.ok(agrees(33.3, 0.1, 33.3));
    assert.ok(agrees(0.3, 0.1, 0.1 + 0.2));
  });
});

describe("words", () => {
  const value = (text: string): number | undefined => extractFigures(text).figures[0]?.value;

  test("reads compound numbers", () => {
    assert.equal(value("twenty-five"), 25);
    assert.equal(value("thirty five"), 35);
    assert.equal(value("two hundred and fifty"), 250);
    assert.equal(value("three thousand four hundred"), 3400);
    assert.equal(value("a hundred"), 100);
    assert.equal(value("a dozen"), 12);
    assert.equal(value("a couple of"), 2);
    assert.equal(value("Fourty"), 40);
  });

  test("reads large numbers in fraction form", () => {
    assert.equal(value("half a million"), 500000);
    assert.equal(value("a quarter of a million"), 250000);
  });

  test("tells a fraction from a period of time", () => {
    assert.equal(extractFigures("in a quarter").figures.length, 0);
    assert.equal(extractFigures("each quarter, per quarter, last quarter").figures.length, 0);
    assert.equal(extractFigures("a quarter of reps").figures[0]?.kind, "fraction");
    assert.equal(extractFigures("on behalf of the team").figures.length, 0);
    assert.equal(extractFigures("a half-year review").figures.length, 0);
  });

  test("a word fraction must sit on a real percentage", () => {
    passes("Half of them.");
    blocks("A quarter of them.");
    const near = buildDataset({ findings: [finding({ summary: "Affected: 44.3%.", facts: {}, value: { kind: "not_sized", reason: "No." }, subjects: [], title: "T" })], metrics: [] });
    assert.equal(checkNumbers("Nearly half of them.", near).passed, false, "44.3% isn't half, hedged or not");
  });

  test("multiples are ratios, and a ratio has to be in the data", () => {
    blocks("Twice as many deals slip.");
    blocks("Slippage has doubled.");
    blocks("Tripled in a year.");
    blocks("4x the pipeline.");
    blocks("Three times the volume.");
    blocks("Slippage has halved.");
    blocks("A 3:1 coverage ratio.");
  });

  test("ordinary words stay quiet", () => {
    for (const text of ["no one", "one-off", "one another", "third party", "third-party apps", "double-check", "first", "second", "next", "last"]) {
      assert.equal(extractFigures(text).figures.length, 0, text);
    }
  });

  test("a higher ordinal is a figure", () => {
    assert.equal(value("the seventh stage"), 7);
    assert.equal(value("the 7th stage"), 7);
  });
});

describe("dates", () => {
  test("the same date in any common form matches", () => {
    for (const text of ["2024-10-31", "31 October 2024", "31st October 2024", "October 31, 2024", "31/10/2024", "31 Oct 2024", "October 2024", "Oct 2024", "Q4 2024", "31 October"]) passes(`It started on ${text}.`);
  });

  test("dates outside the data are blocked", () => {
    for (const text of ["2024-11-01", "1 November 2024", "30 October 2024", "November 2023", "Q1 2024", "in 2023", "FY2030"]) blocks(`It started on ${text}.`);
  });

  test("a US date that can't be a UK date is blocked rather than guessed", () => {
    assert.equal(extractFigures("10/31/2024").figures[0]?.kind, "malformed");
  });

  test("a bare year only counts as a year after a word that introduces one", () => {
    assert.equal(extractFigures("in 2025").figures[0]?.kind, "date");
    assert.equal(extractFigures("2025 deals").figures[0]?.kind, "number");
    blocks("2025 deals are affected.");
    blocks("£2025 is at stake.");
    blocks("2024% growth.");
  });
});

describe("ids", () => {
  test("a Salesforce id that isn't in the data is blocked", () => {
    blocks("See record 006000000000123AAA.");
    blocks("See record 00600000000012.");
  });

  test("an unknown name token is blocked", () => {
    blocks("Rep [[user:U9999]] is the outlier.");
    blocks("Rep [[user:U0014 is the outlier.");
    passes("Rep [[user:U0014]] is the outlier.");
  });

  test("an invented finding or run id is blocked, and a token's digits aren't read as figures", () => {
    blocks("See f-ffffffffffff.");
    blocks("See hc-2026-10-31-deadbeef.");
    assert.deepEqual(extractFigures("[[user:U0014]]").figures.map((f) => f.kind), ["token"]);
  });

  test("a known id passes", () => {
    const set = buildDataset({ findings: [finding({ summary: "Record 006000000000001AAA is stuck." })], metrics: [] });
    assert.equal(checkNumbers("Record 006000000000001AAA is stuck.", set).passed, true);
  });

  test("codes with digits in them must already appear in the data", () => {
    blocks("Move to API v47.");
    blocks("Their Win-Rate-2 report.");
    blocks("A B2B motion.");
    blocks("See https://example.com/report/7.");
    blocks("Contact rep1@example.com.");
  });

  test("a label with digits in it isn't read as a claim, but only exactly", () => {
    passes("Team 2 has too few deals.");
    blocks("Team 97 has too few deals.");
    blocks("Team 23 has too few deals.");
  });
});

describe("attempts to hide a figure", () => {
  const hidden = (text: string, expected: number): void => {
    const hit = extractFigures(text).figures.find((f) => f.value === expected);
    assert.ok(hit, `${JSON.stringify(text)} should expose ${expected}`);
  };

  test("full-width digits", () => hidden("４５％", 45));
  test("a zero-width space inside a number", () => hidden("4​5%", 45));
  test("a soft hyphen inside a number", () => hidden("4­5%", 45));
  test("a word joiner", () => hidden("4⁠5%", 45));
  test("right-to-left marks", () => hidden("‮45%", 45));
  test("bold around the digits", () => hidden("**4**5%", 45));
  test("a non-breaking space before the unit", () => assert.equal(extractFigures("45 %").figures[0]?.kind, "percent"));
  test("a thin space in a thousands group", () => hidden("12 500", 12500));
  test("a Unicode minus", () => assert.equal(extractFigures("−5%").figures[0]?.value, -5));
  test("a vulgar fraction", () => assert.equal(extractFigures("¾ of deals").figures[0]?.value, 75));
  test("superscript digits", () => hidden("4⁵", 45));
  test("capitals", () => assert.equal(extractFigures("FORTY-FIVE").figures[0]?.value, 45));

  test("Arabic-Indic digits are blocked outright", () => blocks("Win rate is ٣٥%."));
  test("an HTML entity is blocked outright", () => blocks("Win rate is &#51;5%."));
  test("a European decimal comma is blocked outright", () => blocks("Win rate is 11,4%."));
  test("a European thousands dot is blocked outright", () => blocks("Pipeline is 2.892.000."));
  test("a badly grouped number is blocked outright", () => blocks("Pipeline is 28,92,000."));
  test("basis points are blocked outright", () => blocks("Up 50 basis points."));

  test("a right value in a range doesn't carry a wrong one", () => {
    blocks("Between 35% and 40%.");
    blocks("35-40%.");
    blocks("£652,000 to £700,000.");
    passes("Between 11.4% and 35%.");
  });

  test("a range shares its unit and scale", () => {
    const [a, b] = extractFigures("£1-2m").figures;
    assert.deepEqual([a?.kind, a?.value, a?.currency], ["currency", 1e6, "GBP"]);
    assert.deepEqual([b?.kind, b?.value, b?.currency], ["currency", 2e6, "GBP"]);
    assert.equal(extractFigures("10-20%").figures[0]?.kind, "percent");
    assert.equal(extractFigures("10-20%").figures[1]?.value, 20);
    assert.equal(extractFigures("10 to 20 days").figures[0]?.unit, "day");
    assert.equal(extractFigures("between 5 and 10%").figures[0]?.kind, "percent");
  });

  test("a pair has to come from one finding, and read the right way round", () => {
    blocks("15 of 177 deals are affected.");
    blocks("70 of 15 deals are affected.");
    blocks("15 of 62 deals.");
    passes("62 of 177 deals.");
  });

  test("a share written as a pair can trace through its percentage", () => {
    passes("One in two open deals.");
    passes("1 in 2 of them.");
    blocks("One in four open deals.");
    blocks("Nine in ten open deals.");
  });

  test("every digit is accounted for, whatever surrounds it", () => {
    const noise = ["4", "4.", ".4", "4,", "(4)", "[4]", "#4", "~4", "~~4", "<4", ">4", "=4", "4/", "/4", "4-", "-4-", "4_", "_4_", "4'", "'4'", "a4", "4a", "4a4", "4.4.4", "4,4", "4:4:4", "4/4/4/4", "£", "£4£", "$$4", "4%%", "%4", "4..", "4 4 4", "4e6", "0x4", "1e3%"];
    for (const text of noise) {
      const { normalised, figures } = extractFigures(text);
      let masked = normalised;
      for (const figure of [...figures].reverse()) masked = masked.slice(0, figure.start) + " ".repeat(figure.end - figure.start) + masked.slice(figure.end);
      assert.doesNotMatch(masked, /\d/, `${JSON.stringify(text)} left a digit behind`);
    }
  });

  test("random text never leaves a digit unaccounted for", () => {
    let seed = 7;
    const next = (n: number): number => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed % n;
    };
    const parts = ["7", "12", "1,000", ".", ",", "-", "%", "£", "$", " ", "of", "m", "k", "half", "one", "/", ":", "_", "a", "Q3", "2025", "[[", "]]", "(", ")", "​", "٣", "st"];
    for (let i = 0; i < 2000; i++) {
      const text = Array.from({ length: 1 + next(8) }, () => parts[next(parts.length)]!).join("");
      const { normalised, figures } = extractFigures(text);
      let masked = normalised;
      for (const figure of [...figures].reverse()) masked = masked.slice(0, figure.start) + " ".repeat(figure.end - figure.start) + masked.slice(figure.end);
      assert.doesNotMatch(masked, /\d/, JSON.stringify(text));
    }
  });
});

describe("normalise", () => {
  test("keeps ordinary text as it is", () => assert.equal(normalise("Win rate is 35%."), "Win rate is 35%."));
});

describe("the dataset", () => {
  test("is the only source of permission: an empty one blocks every figure", () => {
    const empty = buildDataset({ findings: [], metrics: [] });
    assert.equal(checkNumbers("Nothing numeric here.", empty).passed, true);
    assert.equal(checkNumbers("15 deals.", empty).passed, false);
  });

  test("a fact the summary never shows still counts, as a plain number or a named rate", () => {
    const set = buildDataset({ findings: [finding({ summary: "Stale deals.", title: "T", subjects: [], facts: { stale: 9, staleRate: 12.5 }, value: { kind: "not_sized", reason: "No." } })], metrics: [] });
    assert.equal(checkNumbers("9 stale deals, or 12.5%.", set).passed, true);
    assert.equal(checkNumbers("9%.", set).passed, false);
    assert.equal(checkNumbers("12.5 deals.", set).passed, false);
  });

  test("a not sized finding gives no money", () => {
    const set = buildDataset({ findings: [finding({ value: { kind: "not_sized", reason: "We can't size this." } })], metrics: [], currency: "GBP" });
    assert.equal(checkNumbers("£652,000.", set).passed, false);
  });

  test("a segment with no value gives no value", () => {
    assert.equal(checkNumbers("Team 2 wins 12%.", data).passed, false);
  });

  test("a not measurable metric gives no value", () => {
    const m: Metric = { status: "not_measurable", id: "m", domain: "forecast", label: "Forecast accuracy", definition: "How close the forecast was.", unit: "percent", evidence: [evidence], exclusions: [], assumptions: [], reason: "Forecasting isn't in use." };
    const set = buildDataset({ findings: [], metrics: [m] });
    assert.equal(checkNumbers("Accuracy is 80%.", set).passed, false);
    assert.equal(checkNumbers("Forecasting isn't in use.", set).passed, true);
  });

  test("numbers inside a SOQL query aren't figures from the data", () => {
    assert.equal(checkNumbers("Limited to 2000 rows.", data).passed, false);
  });

  test("a section can be scoped to its own sources", () => {
    const scoped = scopeDataset(data, ["historic.win_rate"]);
    assert.equal(checkNumbers("Win rate is 35%.", scoped).passed, true);
    assert.equal(checkNumbers("21.4% of open deals.", scoped).passed, false);
    assert.equal(checkNumbers("Rep [[user:U0014]].", scoped).passed, false);
  });

  test("an override replaces the old figure, and the old one stops passing", () => {
    const override: NumberOverride = { id: "o1", runId: "r", target: { type: "finding", id: "f-000000000001", field: "facts.rate" }, previousValue: 21.4, newValue: 19.5, reason: "Two deals were test records.", overriddenBy: "Synthetic Consultant", overriddenAt: NOW };
    const set = buildDataset({ findings: [finding()], metrics: [], overrides: [override] });
    assert.equal(checkNumbers("That's 19.5%.", set).passed, true);
    assert.equal(checkNumbers("That's 21.4%.", set).passed, false);
  });

  test("structural figures are explicit and carry their unit", () => {
    const set = buildDataset({ findings: [], metrics: [], structural: [{ kind: "duration", value: 90, unit: "day" }, { kind: "number", value: 5 }] });
    assert.equal(checkNumbers("A 90-day plan with five issues.", set).passed, true);
    assert.equal(checkNumbers("A 90-week plan.", set).passed, false);
    assert.equal(scopeDataset(set, [SOURCE.structure]).figures.length, 2);
    assert.equal(checkNumbers("A 90-day plan.", scopeDataset(set, [SOURCE.summary])).passed, false, "a narrative scope doesn't include them");
  });

  test("the summary counts are available when asked for", () => {
    const issues = buildDataset({ findings: [finding(), half], metrics: [] });
    assert.equal(checkNumbers("We found 2 issues.", scopeDataset(issues, [SOURCE.summary])).passed, true);
    assert.equal(checkNumbers("We found 3 issues.", scopeDataset(issues, [SOURCE.summary])).passed, false);
  });
});

describe("the result", () => {
  test("names each figure it blocked, and why", () => {
    const outcome = checkNumbers("Win rate is 40%, and 35 percent, on £9.", data);
    assert.equal(outcome.passed, false);
    assert.deepEqual(outcome.unmatched.map((u) => u.raw), ["40%", "£9"]);
    assert.equal(outcome.checked, 3);
  });

  test("fits the schema a section version stores", () => {
    const stored = toNumberCheckResult(checkNumbers("Win rate is 40%.", data), new Date(NOW));
    assert.ok(NumberCheckResult.parse(stored));
    assert.match(stored.unmatched[0]!, /^40%: /);
  });

  test("a broken dataset blocks rather than passing", () => {
    const outcome = checkNumbers("15 deals.", { figures: null } as never);
    assert.equal(outcome.passed, false);
    assert.equal(checkNumbers("No figures.", { figures: null } as never).passed, false);
  });
});

describe("against a full synthetic run", () => {
  test("every finding's and metric's own text passes against the dataset", async () => {
    const { output, run } = await runFromRecording({ answers: loadAnswers() });
    const { findings, metrics, manualSteps } = output!;
    const set = buildDataset({ findings, metrics, manualSteps, run, currency: "GBP" });
    for (const f of findings) {
      for (const text of [f.title, f.summary, f.recommendation]) {
        const outcome = checkNumbers(text, set);
        assert.equal(outcome.passed, true, `${f.ruleId}: ${text} ${JSON.stringify(outcome.unmatched)}`);
      }
    }
    for (const m of metrics) assert.equal(checkNumbers(`${m.label}. ${m.definition}`, set).passed, true, m.id);
  });

  test("each finding's text passes against that finding alone", async () => {
    const { output } = await runFromRecording({ answers: loadAnswers() });
    const set = buildDataset({ findings: output!.findings, metrics: output!.metrics, currency: "GBP" });
    for (const f of output!.findings) {
      const outcome = checkNumbers(f.summary, scopeDataset(set, [f.id]));
      assert.equal(outcome.passed, true, `${f.ruleId}: ${JSON.stringify(outcome.unmatched)}`);
    }
  });

  test("changing any fact in a summary by one blocks it", async () => {
    const { output } = await runFromRecording({ answers: loadAnswers() });
    const set = buildDataset({ findings: output!.findings, metrics: [], currency: "GBP" });
    let mutated = 0;
    for (const f of output!.findings.filter((x) => x.status === "issue")) {
      for (const value of Object.values(f.facts)) {
        const written = String(value);
        if (!f.summary.includes(written)) continue;
        const scoped = scopeDataset(set, [f.id]);
        const text = f.summary.replace(new RegExp(`(?<![\\d.])${written.replace(".", "\\.")}(?![\\d.])`), String(value + 997));
        if (text === f.summary) continue;
        mutated++;
        assert.equal(checkNumbers(text, scoped).passed, false, `${f.ruleId}: ${text}`);
      }
    }
    assert.ok(mutated > 30, `only ${mutated} mutations ran`);
  });
});
