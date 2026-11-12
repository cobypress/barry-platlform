import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { applyOverrides, refusalReason, refusalSummary, type OverrideRefusal } from "../overrides.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { printDocuments, OUTPUT_FILES, OverridesRefused, renderRun } from "../../render/render-run.ts";
import { heldFigures } from "../../render/render-released.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildTickets, loadActionLists } from "../../actions/index.ts";
import type { Finding, NumberOverride } from "../../schemas/index.ts";
import { docxParagraphs } from "../../testing/docx-text.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";

// DECISIONS.md 511 and 512: a consultant's override is carried into a finding's
// wording only where that is certain, worked out from the finding as read, and
// one that can't be carried holds the documents with a reason of its own.

const AT = "2026-10-31T13:00:00.000Z";
const evidence = { kind: "soql", api: "rest", query: "SELECT Id FROM Opportunity LIMIT 2000", count: 70, dateRange: { from: "2024-10-31", to: "2026-10-31" }, exclusions: [], capturedAt: AT } as Finding["evidence"][number];
const ID = "f-000000000001";

/** A planted finding whose only figures are the ones a test puts in it. */
function finding(summary: string, facts: Record<string, number>, edit: Partial<Finding> = {}): Finding {
  return {
    id: ID,
    ruleId: "tech.storage-use",
    ruleVersion: 1,
    domain: "technical_health",
    status: "issue",
    title: "A planted finding",
    summary,
    recommendation: "Look into it.",
    recommendationType: "change",
    severity: "medium",
    confidence: "high",
    evidence: [evidence],
    value: { kind: "not_sized", reason: "Not sized." },
    effort: { size: "S" },
    owner: "Salesforce admin",
    dependencies: [],
    guards: [],
    segmentsChecked: [],
    segmentExemptReason: "One org-wide figure.",
    subjects: [],
    facts,
    notMeasurableReason: null,
    manualStepIds: [],
    ...edit,
  };
}

const override = (field: string, previousValue: number, newValue: number, id = "o1", target = ID): NumberOverride => ({ id, runId: "r", target: { type: "finding", id: target, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: AT });

/** The finding as reports show it, and what was refused. */
function applied(f: Finding, ...overrides: NumberOverride[]): { f: Finding; refused: OverrideRefusal[] } {
  const result = applyOverrides([f], [], overrides);
  return { f: result.findings[0]!, refused: result.refused };
}
const shown = (summary: string, facts: Record<string, number>, ...overrides: NumberOverride[]): Finding => applied(finding(summary, facts), ...overrides).f;
const whyOf = (f: Finding, ...overrides: NumberOverride[]): OverrideRefusal["why"][] => applied(f, ...overrides).refused.map((r) => r.why);

/** Every text a document prints of a finding, as overrides.ts lists them. */
const textsOf = (f: Finding): string[] => [f.title, f.summary, f.recommendation, ...(f.value.kind === "not_sized" ? [f.value.reason] : [...f.value.assumptions, f.value.period]), f.effort.note ?? "", f.owner, ...f.dependencies, f.notMeasurableReason ?? "", f.segmentExemptReason ?? "", ...f.guards.map((g) => g.detail)];

describe("the forms a figure is found and written in (decisions 506, 511)", () => {
  test("a grouped figure takes the new value grouped", () => {
    const f = shown("Data storage is 102.5% used (10,500 of 10,240 MB).", { used: 10500 }, override("facts.used", 10500, 9800));
    assert.equal(f.summary, "Data storage is 102.5% used (9,800 of 10,240 MB).");
    assert.equal(f.facts.used, 9800);
  });

  test("a new value under 1,000 is written as it is", () => {
    assert.equal(shown("10,500 of 10,240 MB.", { used: 10500 }, override("facts.used", 10500, 950)).summary, "950 of 10,240 MB.");
  });

  test("a new value with decimals keeps them, grouped", () => {
    assert.equal(shown("10,500 of 10,240 MB.", { used: 10500 }, override("facts.used", 10500, 1234.5)).summary, "1,234.5 of 10,240 MB.");
    assert.equal(shown("10,500 of 10,240 MB.", { used: 10500 }, override("facts.used", 10500, 9876.54321)).summary, "9,876.54321 of 10,240 MB.");
  });

  test("an old figure printed with every decimal is found, and the new one keeps every decimal", () => {
    assert.equal(shown("Used 12,345.6789 MB.", { used: 12345.6789 }, override("facts.used", 12345.6789, 23456.789)).summary, "Used 23,456.789 MB.");
  });

  test("a plain figure takes the new value plain", () => {
    assert.equal(shown("Tasks linked to no record: 32 of 10500.", { n: 10500 }, override("facts.n", 10500, 9800)).summary, "Tasks linked to no record: 32 of 9800.");
  });

  test("a figure under 1,000 reads the same in both forms, so its new value is written grouped, as the rules print it", () => {
    assert.equal(shown("Open deals past their close date: 21 of 76 (27.6%).", { n: 76 }, override("facts.n", 76, 1200)).summary, "Open deals past their close date: 21 of 1,200 (27.6%).");
    assert.equal(shown("Open deals past their close date: 21 of 76 (27.6%).", { rate: 27.6 }, override("facts.rate", 27.6, 19.5)).summary, "Open deals past their close date: 21 of 76 (19.5%).");
  });

  test("an old value never matches inside a longer number", () => {
    // Nowhere in its own form, and nothing near it: the fact changes and the words don't.
    const small = "Invoice has 10,500 records, and 1,500.5 more.";
    const f = applied(finding(small, { n: 500 }), override("facts.n", 500, 400));
    assert.deepEqual([f.f.summary, f.f.facts.n, f.refused], [small, 400, []]);
    // 10,500.25 is near 10,500, so it may be the figure printed another way.
    const large = "Not 110,500, nor 10,500.25, nor 10,500,000, nor 110500.";
    const g = applied(finding(large, { n: 10500 }), override("facts.n", 10500, 9800));
    assert.deepEqual([g.f.summary, g.f.facts.n, g.refused.map((r) => r.why)], [large, 10500, [{ kind: "rounded", found: "10,500.25" }]]);
  });

  test("the boundaries still let a figure end a sentence, sit before a comma or in brackets", () => {
    assert.equal(shown("It was 10,500.", { n: 10500 }, override("facts.n", 10500, 9800)).summary, "It was 9,800.");
    assert.equal(shown("Then 10,500, and more.", { n: 10500 }, override("facts.n", 10500, 9800)).summary, "Then 9,800, and more.");
    assert.equal(shown("Used (10,500).", { n: 10500 }, override("facts.n", 10500, 9800)).summary, "Used (9,800).");
  });

  test("a negative figure is found and written grouped, with en-GB's hyphen-minus", () => {
    // en-GB writes the sign as U+002D, not the minus sign U+2212.
    assert.equal((-1470).toLocaleString("en-GB"), "-1,470");
    assert.equal(shown("Growth was -1,470 MB.", { growth: -1470 }, override("facts.growth", -1470, -2310)).summary, "Growth was -2,310 MB.");
    assert.equal(shown("Growth was -1,470 MB.", { growth: -1470 }, override("facts.growth", -1470, 1200)).summary, "Growth was 1,200 MB.");
  });

  test("a figure printed in both forms is found twice, and refused", () => {
    const f = finding("10,500 records, read as 10500.", { n: 10500 });
    assert.deepEqual(whyOf(f, override("facts.n", 10500, 9800)), [{ kind: "repeated", places: 2 }]);
  });

  test("a value String writes with an exponent is looked for, and written, with its digits in full", () => {
    // String(1e21) is "1e+21", which no rule prints.
    const big = shown("Read 1,000,000,000,000,000,000,000 bytes.", { bytes: 1e21 }, override("facts.bytes", 1e21, 2e21));
    assert.equal(big.summary, "Read 2,000,000,000,000,000,000,000 bytes.");
    const small = shown("A share of 0.0000001.", { share: 1e-7 }, override("facts.share", 1e-7, 2e-7));
    assert.equal(small.summary, "A share of 0.0000002.");
    // Found plain, a new value String would write with an exponent is written plain, in full.
    assert.equal(shown("Read 1500 bytes.", { bytes: 1500 }, override("facts.bytes", 1500, 1e21)).summary, "Read 1000000000000000000000 bytes.");
  });

  test("two overrides of one figure in turn end at the last", () => {
    const f = shown("10,500 of 10,240 MB.", { used: 10500 }, override("facts.used", 10500, 9800, "o1"), override("facts.used", 9800, 9000, "o2"));
    assert.equal(f.summary, "9,000 of 10,240 MB.");
    assert.equal(f.facts.used, 9000);
  });
});

describe("where a figure is looked for (decision 511, item 3)", () => {
  test("the money working: a fact printed once, only in an assumption, is rewritten there", () => {
    const f = finding("Seats nobody holds.", { price: 1200 }, { value: { kind: "licence_cost", amount: 18000, unit: "GBP", period: "per year", assumptions: ["15 seats at 1200 GBP a year, from the calibration"] } });
    const { f: shownF, refused } = applied(f, override("facts.price", 1200, 1300));
    assert.deepEqual(refused, []);
    assert.deepEqual(shownF.value.kind !== "not_sized" && shownF.value.assumptions, ["15 seats at 1300 GBP a year, from the calibration"]);
    assert.equal(shownF.facts.price, 1300);
    assert.deepEqual({ ...shownF, value: f.value, facts: f.facts }, f, "nothing else moved");
  });

  test("the recommendation: a fact printed once, only there, is rewritten there", () => {
    const f = finding("Dormant users hold licences.", { users: 3 }, { recommendation: "Deactivate 3 users and release the licences." });
    assert.equal(applied(f, override("facts.users", 3, 4)).f.recommendation, "Deactivate 4 users and release the licences.");
  });

  test("a guard's detail: a fact printed once, only there, is rewritten there", () => {
    const f = finding("A user who still owns records.", { records: 310 }, { guards: [{ guardId: "owns-records", subject: "user", status: "failed", detail: "Owns 310 records: 310 Account.", evidence: [evidence] }] });
    assert.deepEqual(whyOf(f, override("facts.records", 310, 320)), [{ kind: "repeated", places: 2 }], "twice in one detail");
    const once = finding("A user who still owns records.", { records: 310 }, { guards: [{ guardId: "owns-records", subject: "user", status: "failed", detail: "Owns 310 Account records.", evidence: [evidence] }] });
    assert.equal(applied(once, override("facts.records", 310, 320)).f.guards[0]!.detail, "Owns 320 Account records.");
  });

  test("each other text a document prints is searched too", () => {
    const at = (edit: Partial<Finding>): OverrideRefusal["why"][] => whyOf(finding("It was 37 once.", { n: 37 }, edit), override("facts.n", 37, 40));
    const repeated = [{ kind: "repeated", places: 2 }];
    assert.deepEqual(at({ value: { kind: "not_sized", reason: "Not sized: 37 are unknown." } }), repeated);
    assert.deepEqual(at({ value: { kind: "hours", amount: 5, unit: "hours", period: "37 weeks", assumptions: ["An hour each."] } }), repeated);
    assert.deepEqual(at({ effort: { size: "S", note: "37 at a time." } }), repeated);
    assert.deepEqual(at({ owner: "Admin for 37 users" }), repeated);
    assert.deepEqual(at({ dependencies: ["None", "The 37 owners agree"] }), repeated);
    assert.deepEqual(at({ notMeasurableReason: "37 weren't read." }), repeated);
    assert.deepEqual(at({ segmentExemptReason: "37 is org-wide." }), repeated);
    assert.deepEqual(at({ title: "37 of them" }), repeated);
    assert.deepEqual(at({}), [], "once, in the summary");
  });

  test("a figure in no text and nothing near it: the fact changes, every text is as read, character for character", () => {
    const f = finding("Open deals past their close date: 21 of 76 (27.6%).", { weeks: 9 }, { recommendation: "Close them within 2 weeks." });
    const { f: shownF, refused } = applied(f, override("facts.weeks", 9, 12));
    assert.deepEqual(refused, []);
    assert.equal(shownF.facts.weeks, 12);
    assert.deepEqual(textsOf(shownF), textsOf(f));
  });
});

describe("printed rounded: the 5% test (decision 511, item 3)", () => {
  const rounded = (summary: string, value: number, found: string): void => {
    const f = finding(summary, { x: value });
    const { f: shownF, refused } = applied(f, override("facts.x", value, value * 2 + 1));
    assert.deepEqual(refused.map((r) => r.why), [{ kind: "rounded", found }], summary);
    assert.deepEqual(shownF, f);
  };

  test("a fact of 87.34 printed 87.3%, 0.873 printed 87.3%, and 1,234,567 printed £1.2m are refused, naming what was found", () => {
    rounded("Data storage is 87.3% used.", 87.34, "87.3%");
    rounded("Data storage is 87.3% used.", 0.873, "87.3%");
    rounded("Open pipeline of £1.2m.", 1234567, "£1.2m");
  });

  test("beside £ and k, in thousands", () => {
    rounded("Open £815k this quarter.", 815000, "£815k");
    rounded("About 12k records.", 12000, "12k");
  });

  test("a negative value is set against what's printed by its size, whatever the sign", () => {
    rounded("Growth was down 1,470 MB.", -1470, "1,470");
    rounded("Growth was −1,470 MB.", -1470, "1,470");
  });

  test("nothing but 0 is near 0", () => {
    const f = finding("1 of 2 deals, 0.5 a week, and 100% of them.", { x: 0 });
    const { f: shownF, refused } = applied(f, override("facts.x", 0, 3));
    assert.deepEqual([refused, shownF.summary, shownF.facts.x], [[], f.summary, 3]);
    rounded("A rate of 0.0%.", 0, "0.0%");
  });

  test("a number past 5% of every scale isn't near", () => {
    const f = finding("Data storage is 82% used, 1 of 2.", { x: 87.34 });
    assert.deepEqual(applied(f, override("facts.x", 87.34, 80)).refused, []);
  });
});

describe("pseudonym tokens (decision 511, item 3)", () => {
  test("a fact of 4 beside [[user:U0004]] isn't found in the token, and the token is unchanged by a rewrite beside it", () => {
    const f = shown("[[user:U0004]] owns 4 of 9 deals.", { owned: 4 }, override("facts.owned", 4, 5));
    assert.equal(f.summary, "[[user:U0004]] owns 5 of 9 deals.");
  });

  test("a token's digits are never read as a figure printed rounded", () => {
    const f = finding("Owned by [[user:U0004]].", { owned: 4 });
    const { f: shownF, refused } = applied(f, override("facts.owned", 4, 5));
    assert.deepEqual([refused, shownF.summary, shownF.facts.owned], [[], "Owned by [[user:U0004]].", 5]);
  });

  test("a token whose digits read as the figure is left alone", () => {
    const f = shown("[[user:U1234]] owns 1,234 records.", { owned: 1234 }, override("facts.owned", 1234, 1300));
    assert.equal(f.summary, "[[user:U1234]] owns 1,300 records.");
  });
});

describe("only the value as read and the last value count (decision 511, items 1, 2 and 4)", () => {
  test("a figure overridden and put back changes nothing, whether it would carry or not", () => {
    for (const f of [finding("10,500 of 10,240 MB.", { used: 10500 }), finding("10,500 of 10,240 MB, and 10,500 again.", { used: 10500 })]) {
      const result = applyOverrides([f], [], [override("facts.used", 10500, 9800, "o1"), override("facts.used", 9800, 10500, "o2")]);
      assert.deepEqual(result, { findings: [f], metrics: [], refused: [] });
    }
  });

  test("an override to a value already printed, then a third, is the same as one row from the value as read to the last", () => {
    const f = finding("Data storage is 87.3% used (8,940 of 10,240 MB).", { used: 8940, max: 10240 });
    const chain = applied(f, override("facts.used", 8940, 10240, "o1"), override("facts.used", 10240, 9000, "o2"));
    const single = applied(f, override("facts.used", 8940, 9000, "o1"));
    assert.deepEqual(chain, single);
    assert.equal(chain.f.summary, "Data storage is 87.3% used (9,000 of 10,240 MB).");
  });

  test("delivered again: a first row made from a value the run no longer reads is refused, and a row back to the value as read clears it", () => {
    const f = finding("10,500 of 10,240 MB.", { used: 10500 });
    const stale = applied(f, override("facts.used", 11000, 9800, "o1"));
    assert.deepEqual(stale.refused.map((r) => r.why), [{ kind: "stale", from: 11000 }]);
    assert.deepEqual(stale.f, f);
    assert.match(refusalReason(stale.refused), /, but the correction was made from 11,000, which this run no longer reads\.$/m);
    const back = applied(f, override("facts.used", 11000, 9800, "o1"), override("facts.used", 9800, 10500, "o2"));
    assert.deepEqual(back, { f, refused: [] });
  });

  test("two figures carried are both rewritten, in one pass: a new value equal to the other's as read isn't rewritten again", () => {
    const f = finding("Data storage is 87.3% used (8,940 of 10,240 MB).", { used: 8940, max: 10240 });
    const both = applied(f, override("facts.used", 8940, 10240, "o1"), override("facts.max", 10240, 12000, "o2"));
    assert.deepEqual(both.refused, []);
    assert.equal(both.f.summary, "Data storage is 87.3% used (10,240 of 12,000 MB).");
    assert.deepEqual(both.f.facts, { used: 10240, max: 12000 });
  });

  test("one figure carried and one refused: the whole finding stays as read", () => {
    const f = finding("Data storage is 87.3% used (8,940 of 10,240 MB), and 10,240 is the most.", { used: 8940, max: 10240 });
    const result = applied(f, override("facts.used", 8940, 9000, "o1"), override("facts.max", 10240, 12000, "o2"));
    assert.deepEqual(result.refused.map((r) => [r.field, r.why]), [["facts.max", { kind: "repeated", places: 2 }]]);
    assert.deepEqual(result.f, f);
  });
});

describe("a row the run doesn't hold (decision 511, item 5)", () => {
  // The words hold the row's previous value, as if an earlier swap had rewritten them: they're left alone.
  const f = finding("Data storage is 9,800 of 10,240 MB.", { used: 10500 });
  const unchanged = (o: NumberOverride): void => assert.deepEqual(applyOverrides([f], [], [o]), { findings: [f], metrics: [], refused: [] });

  test("a finding id the run doesn't hold", () => unchanged(override("facts.used", 9800, 9000, "o1", "f-000000000999")));
  test("a fact key the finding doesn't have, or one only its prototype has", () => {
    unchanged(override("facts.usedMB", 9800, 9000));
    unchanged(override("facts.toString", 9800, 9000));
    unchanged(override("facts", 9800, 9000));
  });
  test("the sized amount of a finding that isn't sized", () => unchanged(override("value.amount", 9800, 9000)));
  // Decision 517 judges a metric's figure against its parts and texts, so the planted metric has them all; none holds 47.
  test("a metric's override whose value as read no part or text of the metric holds still applies", () => {
    const metric: Parameters<typeof applyOverrides>[1][number] = { id: "historic.win_rate", domain: "historic", status: "measured", label: "Win rate", definition: "Won deals over closed deals.", unit: "percent", evidence: [evidence], exclusions: [], assumptions: [], value: 47, n: 100, dateRange: null, segments: [], segmentExemptReason: "One org-wide figure.", series: [], stats: {} };
    const result = applyOverrides([f], [metric], [{ ...override("value", 47, 45), target: { type: "metric", id: "historic.win_rate", field: "value" } }]);
    assert.equal((result.metrics[0] as { value: number }).value, 45);
    assert.deepEqual(result.refused, []);
  });
});

describe("the admitted limits of decision 511, as the customer then reads them", () => {
  test("known limit: a constant equal to an unprinted fact, found once, is taken for the fact and rewritten", () => {
    const f = finding("Commit deals still in the first 2 open stages: 1 of 9 (11.1%).", { n: 9, stages: 2 });
    assert.equal(applied(f, override("facts.stages", 2, 5)).f.summary, "Commit deals still in the first 5 open stages: 1 of 9 (11.1%).");
  });

  test("known limit: a count is carried and its percentage beside it stays", () => {
    const f = finding("Fields with neither help text nor a description: 6 of 8 (75%).", { bare: 6, fields: 8, rate: 75 });
    assert.equal(applied(f, override("facts.bare", 6, 7)).f.summary, "Fields with neither help text nor a description: 7 of 8 (75%).");
  });

  test("known limit: a figure printed in a form the 5% test doesn't see, days as weeks, stays", () => {
    const f = finding("Leads wait about 3 weeks.", { days: 21 });
    const result = applied(f, override("facts.days", 21, 35));
    assert.deepEqual([result.refused, result.f.summary, result.f.facts.days], [[], "Leads wait about 3 weeks.", 35]);
  });
});

// The reason the documents are held, in the shape the console's reader lists
// (decision 512; console lib/health-check/documents-error.ts).

// Decision 517: the first and last lines name the documents and a measure's chart or table, for every refusal.
const CLOSE = "Override each one back to its figure as read, then make the documents again. A finding's wording and a measure's chart or table can't be edited, so a figure we can't carry changes only with a new health check.";
const refusal = (title: string, why: OverrideRefusal["why"] = { kind: "repeated", places: 2 }): OverrideRefusal => ({ findingId: ID, title, field: "facts.hits", asRead: 1200, last: 1100.5, why });

describe("the reason a refusal holds the documents (decision 512)", () => {
  test("one refusal, whole", () => {
    assert.equal(
      refusalReason([refusal("Deals that should have closed")]),
      [
        "Held back: 1 corrected figure can't be carried into the documents.",
        '"Deals that should have closed", hits: 1,200 as read, corrected to 1,100.5, but the wording has 1,200 in 2 places, and we can\'t tell which of them are this figure.',
        CLOSE,
      ].join("\n"),
    );
    assert.equal(refusalSummary([refusal("x")]), "1 corrected figure can't be carried. See the run's Release tab.");
  });

  test("each reason, and the sized amount by name", () => {
    const lines = refusalReason([
      refusal("A", { kind: "shared", other: "quota" }),
      { ...refusal("B", { kind: "shared", other: "the sized amount" }), field: "facts.annualSaving" },
      { ...refusal("C", { kind: "rounded", found: "£1.2m" }), field: "value.amount" },
      refusal("D", { kind: "stale", from: 1250 }),
      refusal("E", { kind: "unsure", found: "1200" }),
    ]).split("\n");
    assert.deepEqual(lines.slice(0, 6), [
      "Held back: 5 corrected figures can't be carried into the documents.",
      '"A", hits: 1,200 as read, corrected to 1,100.5, but another figure of this finding (quota) is also 1,200, so we can\'t tell which one the wording prints.',
      '"B", annualSaving: 1,200 as read, corrected to 1,100.5, but another figure of this finding (the sized amount) is also 1,200, so we can\'t tell which one the wording prints.',
      '"C", the sized amount: 1,200 as read, corrected to 1,100.5, but the wording doesn\'t print 1,200 itself and has £1.2m, which may be this figure rounded.',
      '"D", hits: 1,200 as read, corrected to 1,100.5, but the correction was made from 1,250, which this run no longer reads.',
      // DECISIONS.md 516 and 520: a form that may or may not be the figure.
      '"E", hits: 1,200 as read, corrected to 1,100.5, but the wording has 1,200 only as 1200, which we can\'t be sure is this figure and can\'t rewrite.',
    ]);
  });

  test("ten listed at most, then how many more", () => {
    const twelve = Array.from({ length: 12 }, (_, i) => refusal(`Finding ${i + 1}`));
    const lines = refusalReason(twelve).split("\n");
    assert.equal(lines.length, 13);
    assert.equal(lines[0], "Held back: 12 corrected figures can't be carried into the documents.");
    assert.ok(lines[10]!.startsWith('"Finding 10", '));
    assert.equal(lines[11], "And 2 more.");
    assert.equal(lines[12], CLOSE);
    assert.equal(refusalSummary(twelve), "12 corrected figures can't be carried. See the run's Release tab.");
  });

  test("the console lists it: three lines or more, the first starting Held back:, the last the close, none empty, whatever the reasons", () => {
    // Every reason in turn, the one decision 516 added first, so a single refusal is that one; then
    // decision 520's figure in words, so two refusals hold both unsure lines.
    const whys: OverrideRefusal["why"][] = [{ kind: "unsure", found: "3:9" }, { kind: "unsure", found: "a dozen" }, { kind: "repeated", places: 2 }, { kind: "shared", other: "quota" }, { kind: "rounded", found: "£1.2m" }, { kind: "stale", from: 1250 }];
    for (const n of [1, 2, 10, 11, 12]) {
      const reason = refusalReason(Array.from({ length: n }, (_, i) => refusal(`Finding\n\n${i}\r\n  title\n`, whys[i % whys.length])));
      assert.ok(reason.includes("only as 3:9, which we can't be sure is this figure and can't rewrite."));
      if (n > 1) assert.ok(reason.split("\n")[2]!.endsWith("but the wording has 1,200 only as a dozen, which we can't be sure is this figure and can't rewrite."), reason);
      // As documentsErrorView reads it: split on newlines, trimmed, blanks dropped.
      const read = reason.split("\n").map((l) => l.trim()).filter(Boolean);
      assert.deepEqual(read, reason.split("\n"), "no line empty or padded");
      assert.ok(read.length >= 3, `${n}`);
      assert.ok(read[0]!.startsWith("Held back: "));
      assert.equal(read.at(-1), CLOSE);
      assert.equal(read.length, 2 + Math.min(n, 10) + (n > 10 ? 1 : 0));
      assert.ok(read.slice(1, Math.min(n, 10) + 1).every((l) => l.startsWith('"Finding ')), "a title's newline can't start a line");
    }
  });

  test("a newline anywhere in a line, such as in a fact's key, can't start a line of its own", () => {
    const reason = refusalReason([{ ...refusal("A"), field: "facts.odd\nkey", why: { kind: "shared", other: "other\r\n  key" } }]);
    assert.equal(reason.split("\n").length, 3);
    assert.ok(reason.split("\n")[1]!.startsWith('"A", odd key: 1,200 as read, corrected to 1,100.5, but another figure of this finding (other key) is also 1,200'), reason);
  });

  test("a title's person token prints as a named person, never the token", () => {
    const line = refusalReason([refusal("Worst by rep: [[user:U0009]] and [[user:U10000]]")]).split("\n")[1]!;
    assert.ok(line.startsWith('"Worst by rep: a named person and a named person", '), line);
    assert.doesNotMatch(line, /\[\[|U0009|U10000/);
  });

  test("a title is cut at 120 characters, after its token is replaced", () => {
    const title = `${"x".repeat(100)} [[user:U0001]] ${"y".repeat(5)}`;
    const shownTitle = `${"x".repeat(100)} a named person ${"y".repeat(5)}`.slice(0, 120);
    assert.ok(refusalReason([refusal(title)]).split("\n")[1]!.startsWith(`"${shownTitle}", hits:`));
    const long = "z".repeat(121);
    assert.ok(refusalReason([refusal(long)]).split("\n")[1]!.startsWith(`"${"z".repeat(120)}", hits:`));
    assert.ok(refusalReason([refusal("z".repeat(120))]).split("\n")[1]!.startsWith(`"${"z".repeat(120)}", hits:`));
  });

  test("a quote mark in a title is printed as it is: the console reads lines, not quotes", () => {
    assert.ok(refusalReason([refusal('The "Prospecting" stage')]).split("\n")[1]!.startsWith('"The "Prospecting" stage", hits:'));
  });
});

// The synthetic run, as `hc render` and the documents job build it.
const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const lists = loadActionLists(dir);
const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const issue = (ruleId: string, n = 0): Finding => output!.findings.filter((f) => f.ruleId === ruleId && f.status === "issue")[n]!;
const onRun = (f: Finding, field: string, newValue: number, id = "o1", previousValue?: number): NumberOverride => ({
  id,
  runId: run.id,
  target: { type: "finding", id: f.id, field },
  previousValue: previousValue ?? (field === "value.amount" ? (f.value.kind === "not_sized" ? 0 : f.value.amount) : f.facts[field.slice(6)]!),
  newValue,
  reason: "Checked with the customer.",
  overriddenBy: "Synthetic Consultant",
  overriddenAt: AT,
});
const built = (...overrides: NumberOverride[]): BuiltReport => buildReport({ ...input, overrides });
const teamOf = (report: BuiltReport, f: Finding): Finding => report.teamFindings.find((x) => x.id === f.id)!;
const lineOf = (report: BuiltReport): string => refusalReason(report.refused).split("\n")[1]!;
const storage = issue("tech.storage-use");
const coverage = issue("cur.coverage");
const asBuilt = built();

describe("on the synthetic run (decision 511)", () => {
  test("the run prints the figures these tests use as the measurement found", () => {
    // If a rule's wording moves, the tests below test something else.
    assert.ok(storage.summary.includes("(8,940 of 10,240 MB)"));
    assert.equal(coverage.facts.remaining, coverage.facts.quota);
    assert.ok(issue("dq.open-opps-past-close").value.kind !== "not_sized");
  });

  test("two facts sharing a value: refused, naming the other, and the finding is as read", () => {
    const report = built(onRun(coverage, "facts.remaining", 1100000));
    assert.equal(lineOf(report), `"${coverage.title}", remaining: 1,200,000 as read, corrected to 1,100,000, but another figure of this finding (quota) is also 1,200,000, so we can't tell which one the wording prints.`);
    assert.deepEqual(teamOf(report, coverage), teamOf(asBuilt, coverage));
  });

  test("a constant equal to the fact, or the fact printed more than once: refused with how many places", () => {
    const skew = issue("bp.ownership-skew");
    const seats = issue("utl.unassigned-licence-seats");
    const report = built(onRun(skew, "facts.limit", 9000, "o1"), onRun(seats, "facts.seats", 14, "o2"));
    const why = new Map(report.refused.map((r) => [r.findingId, r.why]));
    assert.deepEqual([report.refused.length, why.get(skew.id), why.get(seats.id)], [2, { kind: "repeated", places: 2 }, { kind: "repeated", places: 3 }]);
    assert.ok(refusalReason(report.refused).includes(`"${skew.title}", limit: 10,000 as read, corrected to 9,000, but the wording has 10,000 in 2 places, and we can't tell which of them are this figure.`));
    assert.ok(refusalReason(report.refused).includes(`"${seats.title}", seats: 15 as read, corrected to 14, but the wording has 15 in 3 places, and we can't tell which of them are this figure.`));
    for (const f of [skew, seats]) assert.deepEqual(teamOf(report, f), teamOf(asBuilt, f));
  });

  test("a fact quoted in the money working: refused, counted across the summary and the working", () => {
    const past = issue("dq.open-opps-past-close");
    const report = built(onRun(past, "facts.hits", 19));
    assert.deepEqual(report.refused.map((r) => r.why), [{ kind: "repeated", places: 2 }]);
    assert.deepEqual(teamOf(report, past), teamOf(asBuilt, past));
  });

  test("override then back, on a figure that would be refused and one that would be carried: as if never overridden", () => {
    const skew = issue("bp.ownership-skew");
    const report = built(onRun(skew, "facts.limit", 9000, "o1"), onRun(skew, "facts.limit", 10000, "o2", 9000), onRun(storage, "facts.DataStorageMBUsed", 8200, "o3"), onRun(storage, "facts.DataStorageMBUsed", 8940, "o4", 8200));
    assert.deepEqual(report.refused, []);
    assert.deepEqual(report.teamFindings, asBuilt.teamFindings);
    assert.deepEqual(report.value, asBuilt.value);
  });

  test("the sized amount, unique and unprinted, changes and the value model follows", () => {
    const past = issue("dq.open-opps-past-close");
    const amount = past.value.kind === "not_sized" ? 0 : past.value.amount;
    const report = built(onRun(past, "value.amount", amount + 100000));
    assert.deepEqual(report.refused, []);
    const value = teamOf(report, past).value;
    assert.equal(value.kind !== "not_sized" && value.amount, amount + 100000);
    assert.deepEqual(textsOf(teamOf(report, past)), textsOf(teamOf(asBuilt, past)));
    assert.ok(report.value.total.expected > asBuilt.value.total.expected, "the total follows");
  });

  test("the sized amount equal to a fact: refused, naming it", () => {
    const dormant = issue("utl.dormant-licensed-users");
    const report = built(onRun(dormant, "value.amount", 2400));
    assert.equal(lineOf(report), `"${dormant.title}", the sized amount: 1,200 as read, corrected to 2,400, but another figure of this finding (annualSaving) is also 1,200, so we can't tell which one the wording prints.`);
    assert.deepEqual(teamOf(report, dormant), teamOf(asBuilt, dormant));
  });
});

// Each figure alone, as the console offers them: every numeric fact and every
// sized amount. The counts are pinned, so a rule or a wording change that moves
// them is seen and the number Coby was given is checked again.
const occurs = (text: string, value: number): boolean => {
  const masked = text.replace(/\[\[[a-z]+:[A-Z]\d{4,}\]\]/g, "#");
  return [String(value), value.toLocaleString("en-GB", { maximumFractionDigits: 100 })].some((form) => new RegExp(`(?<![\\d.,])${form.replace(/[.]/g, "\\.")}(?![\\d]|[.,]\\d)`).test(masked));
};

describe("every overridable figure of the synthetic run, each alone (decision 511)", () => {
  const figures = output!.findings.flatMap((f) => [...Object.entries(f.facts).map(([key, value]) => ({ f, field: `facts.${key}`, value })), ...(f.value.kind === "not_sized" ? [] : [{ f, field: "value.amount", value: f.value.amount }])]);

  for (const [label, next] of [["plus 7", (v: number) => v + 7], ["times 2 plus 0.5", (v: number) => v * 2 + 0.5]] as const) {
    test(`${label}: carried leaves no trace of the value as read, refused leaves the finding as read, and the counts are pinned`, () => {
      const counts: Record<string, number> = {};
      for (const { f, field, value } of figures) {
        const last = next(value);
        const { f: after, refused } = applied(f, onRun(f, field, last));
        let outcome: string;
        if (refused.length > 0) {
          assert.deepEqual(after, f, `${f.ruleId} ${field}`);
          outcome = `refused, ${refused[0]!.why.kind}`;
        } else {
          for (const text of textsOf(after)) assert.ok(!occurs(text, value) || occurs(text, last), `${f.ruleId} ${field}: ${text}`);
          assert.equal(field === "value.amount" ? after.value.kind !== "not_sized" && after.value.amount : after.facts[field.slice(6)], last);
          outcome = JSON.stringify(textsOf(after)) === JSON.stringify(textsOf(f)) ? "changed with no text" : "rewritten in one place";
        }
        counts[outcome] = (counts[outcome] ?? 0) + 1;
      }
      assert.equal(figures.length, 343, "319 facts and 24 sized amounts");
      // Decision 514 masks dates: bp.users-without-role's n of 11 is no longer also read in "2026-11-02" (repeated to rewritten);
      // sec.personal-integration-accounts' peopleShare of 8.3 isn't near "08" in "2026-08-02", nor
      // dq.open-opps-no-activity's sized amount near "2024" in "2024-10-31" (rounded to changed with no text).
      // Decision 516 moves none: no text on the run holds a colon form, and the one bare year its reader finds,
      // adp.falling-adoption's "2026" of "in 2026-02", is part of a year and month. A refusal for a figure
      // printed only as one would be counted as "refused, unsure".
      // Decision 520 makes a figure in words an unsure form. A fact of 1 beside "one" in its wording: rewritten to repeated for
      // dq.duplicate-accounts' share ("at least one other"), sec.modify-all-data's unexpected ("one compromised or careless login"),
      // bp.ownership-skew's pairs and act.rep-activity-spread's low; changed with no text to unsure for int.api-usage's
      // DailyApiRequestsShare ("which one reading can't size"), and adp.low-engagement-group's groups of 2 ("count them twice").
      // int.api-usage's DailyApiRequestsUsed of 1,000 is near "one" in thousands, so changed with no text to rounded.
      assert.deepEqual(counts, { "rewritten in one place": 169, "changed with no text": 71, "refused, repeated": 43, "refused, shared": 51, "refused, rounded": 7, "refused, unsure": 2 });
    });
  }
});

// Printed as the documents job prints them: the decks through the same
// renderers, into a folder outside the repo, then the tickets and the
// microsite. Two figures the rules print grouped, each once: the storage
// finding's used MB (a count) and the coverage finding's open pipeline (money).
const overrides = [onRun(storage, "facts.DataStorageMBUsed", 8200, "o1"), onRun(coverage, "facts.openThisQuarter", 790000, "o2")];
const report = built(...overrides);
const out = mkdtempSync(join(tmpdir(), "hc-override-"));
const paths = { rawNotes: join(out, OUTPUT_FILES.rawNotes), executive: join(out, OUTPUT_FILES.executive), full: join(out, OUTPUT_FILES.full), ceo: join(out, OUTPUT_FILES.ceo), kit: join(out, OUTPUT_FILES.kit), layout: join(out, OUTPUT_FILES.layout) };
const layout = await printDocuments(report, pseudonyms, paths);
const ENTITIES: Record<string, string> = { "&#39;": "'", "&quot;": '"', "&amp;": "&", "&lt;": "<", "&gt;": ">", "&pound;": "£" };
/** What a reader sees of a page: its words, without the markup, styles or a chart's coordinates. */
const decoded = (html: string): string => html.replace(/<(style|script)\b[\s\S]*?<\/\1>/g, " ").replace(/<[^>]*>/g, " ").replace(/&#39;|&quot;|&amp;|&lt;|&gt;|&pound;/g, (e) => ENTITIES[e]!);
const printed = {
  executive: decoded(executiveHtml(report, pseudonyms)),
  full: decoded(fullReportHtml(report, pseudonyms)),
  tickets: buildTickets(report, pseudonyms, lists),
  microsite: decoded(buildMicrositePages(report, pseudonyms).pages.map((p) => p.html).join("\n")),
};

describe("a figure printed once, grouped, on the synthetic run (decision 511)", () => {
  test("is rewritten in the form found, and the old figure is in no text of the finding; nothing refused", () => {
    assert.deepEqual(report.refused, []);
    const s = teamOf(report, storage);
    assert.ok(s.summary.includes("(8,200 of 10,240 MB)"), s.summary);
    assert.equal(s.facts.DataStorageMBUsed, 8200);
    for (const text of textsOf(s)) assert.ok(!occurs(text, 8940), text);
    const c = teamOf(report, coverage);
    assert.ok(c.summary.includes("open £790,000"), c.summary);
    for (const text of textsOf(c)) assert.ok(!occurs(text, 815000), text);
  });

  test("the executive summary, the full report, the tickets and the microsite print the new figures and never the old", () => {
    assert.ok(printed.full.includes("8,200 of 10,240 MB"), "the full report prints the storage finding");
    assert.ok(printed.tickets.includes("8,200 of 10,240 MB") && printed.tickets.includes("open £790,000"));
    assert.ok([printed.executive, printed.full, printed.microsite].some((text) => text.includes("£790,000")), "the coverage finding is printed somewhere");
    for (const [name, text] of Object.entries(printed)) {
      for (const old of ["8,940", "8940", "815,000", "815000"]) assert.ok(!text.includes(old), `${name} prints ${old}`);
    }
  });

  test("every section that cites either finding passes the number check", () => {
    const cited = Object.values(report.outputs).flatMap((o) => o.sections.filter((s) => s.findingIds.includes(storage.id) || s.findingIds.includes(coverage.id)).map((s) => ({ s, v: o.versions.find((v) => v.id === s.currentVersionId)! })));
    assert.ok(cited.length > 0);
    for (const { s, v } of cited) assert.deepEqual(v.numberCheck.unmatched, [], s.key);
  });

  test("the full report and the executive summary print with nothing held", () => {
    assert.ok(layout.full.pages >= 20 && layout.executive.pages >= 5, "both were printed");
    assert.deepEqual(heldFigures(layout), []);
    assert.equal(layout.full.passed, true, JSON.stringify(layout.full.problems));
    assert.equal(layout.executive.passed, true, JSON.stringify(layout.executive.problems));
  });

  test("the raw notes print the findings as read, and the overrides with both values", () => {
    const paragraphs = docxParagraphs(readFileSync(paths.rawNotes)).map((p) => p.text);
    assert.ok(paragraphs.includes(storage.summary), "the storage finding's summary as read");
    assert.ok(paragraphs.some((p) => p.includes("open £815,000")));
    assert.ok(!paragraphs.some((p) => p.includes("8,200 of") || p.includes("£790,000")), "no rewritten summary in the raw notes");
    for (const cell of ["facts.DataStorageMBUsed", "8940", "8200", "facts.openThisQuarter", "815000", "790000"]) assert.ok(paragraphs.includes(cell), cell);
  });
});

describe("the command line's render (decision 512)", () => {
  test("a folder whose overrides.json holds a refused override fails with the reason's lines, and writes nothing", async () => {
    const folder = join(mkdtempSync(join(tmpdir(), "hc-refused-")), "run");
    cpSync(dir, folder, { recursive: true });
    const skew = issue("bp.ownership-skew");
    writeFileSync(join(folder, "overrides.json"), JSON.stringify([onRun(skew, "facts.limit", 9000)]));
    const before = readdirSync(folder).sort();
    await assert.rejects(
      () => renderRun(folder),
      (err: unknown) => {
        assert.ok(err instanceof OverridesRefused);
        assert.equal(err.message, refusalReason(built(onRun(skew, "facts.limit", 9000)).refused));
        assert.match(err.message, /^Held back: 1 corrected figure can't be carried into the documents\.\n".+", limit: 10,000 as read, corrected to 9,000, but the wording has 10,000 in 2 places/);
        return true;
      },
    );
    assert.deepEqual(readdirSync(folder).sort(), before, "no document, sections or layout file written");
    // `hc render` prints the same lines, and nothing else, and fails.
    const cli = spawnSync(process.execPath, [fileURLToPath(new URL("../../cli/hc.ts", import.meta.url)), "render", "--run", folder], { encoding: "utf8" });
    assert.equal(cli.status, 2, cli.stderr);
    assert.equal(cli.stdout, `${refusalReason(built(onRun(skew, "facts.limit", 9000)).refused)}\n`);
    assert.deepEqual(readdirSync(folder).sort(), before);
  });
});
