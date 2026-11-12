import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { applyOverrides, refusalReason, type OverrideRefusal } from "../overrides.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { loadRun, printDocuments, OUTPUT_FILES } from "../../render/render-run.ts";
import { heldFigures, renderReleased, type ApprovedRun } from "../../render/render-released.ts";
import { buildDataset, checkNumbers, extractFigures, scopeDataset, type FigureSet } from "../../numbercheck/index.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { executiveHtml } from "../../render/executive.ts";
import type { Finding, Metric, NumberOverride, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";

// DECISIONS.md 514: the number check is told what was carried, not the rows,
// so a figure put back clears; and a date, an identifier and a clock time are
// not figures.
// DECISIONS.md 515: digits joined by a colon are never rewritten and never
// passed over; a metric's row runs from its value as read.

const AT = "2026-10-31T13:00:00.000Z";
const evidence = { kind: "soql", api: "rest", query: "SELECT Id FROM User LIMIT 2000", count: 11, dateRange: { from: "2024-10-31", to: "2026-10-31" }, exclusions: [], capturedAt: AT } as Finding["evidence"][number];
const ID = "f-000000000001";

/** A planted finding whose only figures are the ones a test puts in it. */
function finding(summary: string, facts: Record<string, number>, edit: Partial<Finding> = {}): Finding {
  return {
    id: ID,
    ruleId: "int.personal-api-logins",
    ruleVersion: 1,
    domain: "integrations",
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

const override = (field: string, previousValue: number, newValue: number, id = "o1"): NumberOverride => ({ id, runId: "r", target: { type: "finding", id: ID, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: AT });

/** Every text a document prints of a finding, as overrides.ts lists them. */
const textsOf = (f: Finding): string[] => [f.title, f.summary, f.recommendation, ...(f.value.kind === "not_sized" ? [f.value.reason] : [...f.value.assumptions, f.value.period]), f.effort.note ?? "", f.owner, ...f.dependencies, f.notMeasurableReason ?? "", f.segmentExemptReason ?? "", ...f.guards.map((g) => g.detail)];

/** An unprinted fact equal to digits in a shape the number check's reader doesn't take as a figure: carried, every text as read. */
function untouched(summary: string, value: number, last = value + 1): void {
  const f = finding(summary, { x: value });
  const result = applyOverrides([f], [], [override("facts.x", value, last)]);
  assert.deepEqual(result.refused, [], summary);
  assert.deepEqual(textsOf(result.findings[0]!), textsOf(f), summary);
  assert.equal(result.findings[0]!.facts.x, last, summary);
}

/** A fact printed once as a figure beside a masked shape: rewritten at the figure. */
const carried = (summary: string, facts: Record<string, number>, field: string, last: number): string => {
  const result = applyOverrides([finding(summary, facts)], [], [override(`facts.${field}`, facts[field]!, last)]);
  assert.deepEqual(result.refused, [], summary);
  return result.findings[0]!.summary;
};

describe("a date, an identifier and a clock time are not figures (decision 514, item 2)", () => {
  // The real wording of src/domains/integrations/index.ts:85, with a fact it doesn't print.
  const SINCE = "2 personal accounts made 57 API logins since 2026-10-04, through Data Loader, Workbench. If that person leaves and the account is deactivated, the integration stops.";

  test("the integrations wording with an unprinted fact of 10 corrected to 11 and to 12.5: the date as read", () => {
    for (const last of [11, 12.5]) {
      const result = applyOverrides([finding(SINCE, { accounts: 2, apiLogins: 57, peopleShare: 10 })], [], [override("facts.peopleShare", 10, last)]);
      assert.deepEqual(result.refused, [], `${last}`);
      assert.equal(result.findings[0]!.summary, SINCE, `${last}`);
      assert.equal(result.findings[0]!.facts.peopleShare, last);
    }
  });

  test("the month of an ISO date", () => untouched("Checked 2026-10-04.", 10));
  test("the day of an ISO date", () => untouched("Checked 2026-10-04.", 4));
  test("the year of an ISO date", () => untouched("Checked 2026-10-04.", 2026));
  test("the month of a year and month, 2026-02, which the number check's reader doesn't take as a date", () => {
    untouched("Open deals with no owner fell from 8 in 2026-02 to 5 in 2026-09.", 2);
    untouched("Open deals with no owner fell from 8 in 2026-02 to 5 in 2026-09.", 9);
  });
  test("the year of in 2026-02, which the number check's reader takes as a bare year: part of the year and month, never tested (decision 516, item 2)", () => {
    // adp.falling-adoption's wording, the one text on the synthetic run with a bare year the reader calls a date.
    untouched("Monthly active users fell from 8 in 2026-02 to 5 in 2026-09.", 2026);
    untouched("Monthly active users fell from 8 in 2026-02 to 5 in 2026-09.", 2030);
  });
  // Decision 518: a date read by a word is a guess, so these are refused as unsure, below, where round 4 masked them.
  test("the quarter of Q3 and of Q3 2026", () => {
    untouched("Closing in Q3.", 3);
    untouched("Closing in Q3 2026.", 3);
  });
  test("the digits of FY26", () => untouched("Set in the FY26 plan.", 26));
  test("the day of 04/10/2026, which was read as a figure printed rounded", () => untouched("Checked on 04/10/2026.", 4));
  test("58 in v58", () => untouched("Called through API v58.", 58));
  test("5 in R5.100, which was read as a figure printed rounded", () => untouched("Rule R5.100 applies.", 5));
  // "09:30" on its own is a colon form, refused when a part is the fact (decision 515, below).
  test("the hour of 2026-10-04T09:30", () => untouched("Last ran 2026-10-04T09:30.", 9));

  test("a figure and a date with the same digits in one sentence: rewritten at the figure, the date untouched", () => {
    assert.equal(carried("10 people logged in since 2026-10-04.", { n: 10 }, "n", 12), "12 people logged in since 2026-10-04.");
  });
});

describe("a figure beside a masked shape stays a figure (decision 514, item 2)", () => {
  const SINCE = "since 2026-10-04";
  test("2 of 11 (18.2%): each is found and rewritten", () => {
    const summary = `People with no role ${SINCE}: 2 of 11 (18.2%).`;
    const facts = { hits: 2, n: 11, rate: 18.2 };
    assert.equal(carried(summary, facts, "hits", 3), `People with no role ${SINCE}: 3 of 11 (18.2%).`);
    assert.equal(carried(summary, facts, "n", 12), `People with no role ${SINCE}: 2 of 12 (18.2%).`);
    assert.equal(carried(summary, facts, "rate", 19.5), `People with no role ${SINCE}: 2 of 11 (19.5%).`);
  });

  test("5-10 days: each end of the range is a figure of its own, and only the end that is the fact changes", () => {
    const summary = `Leads wait 5-10 days ${SINCE}.`;
    assert.equal(carried(summary, { least: 5, most: 10 }, "least", 6), `Leads wait 6-10 days ${SINCE}.`);
    assert.equal(carried(summary, { least: 5, most: 10 }, "most", 12), `Leads wait 5-12 days ${SINCE}.`);
  });

  test("£1,200,000 and 47%", () => {
    assert.equal(carried(`Quota of £1,200,000 ${SINCE}.`, { quota: 1200000 }, "quota", 1100000), `Quota of £1,100,000 ${SINCE}.`);
    assert.equal(carried(`A win rate of 47% ${SINCE}.`, { rate: 47 }, "rate", 45), `A win rate of 45% ${SINCE}.`);
  });

  test("a figure near the value beside a date is still read as the figure printed rounded", () => {
    const f = finding(`Data storage is 87.3% used ${SINCE}.`, { x: 87.34 });
    assert.deepEqual(applyOverrides([f], [], [override("facts.x", 87.34, 90)]).refused.map((r) => r.why), [{ kind: "rounded", found: "87.3%" }]);
  });
});

describe("a text the number check's reader normalises to another length (decision 514, item 2)", () => {
  test("a vulgar fraction before a date: the date is masked where it stands in the text as given", () => {
    untouched("½ of them since 2026-10-04.", 10);
    assert.equal(carried("½ of 10 people since 2026-10-04.", { n: 10 }, "n", 12), "½ of 12 people since 2026-10-04.");
  });

  test("characters that normalise into one (ㄱㅏ is 가) are masked whole, and the rest stays in place", () => {
    // Cut at the reader's indices, one place early, the day's 4 would be left unmasked.
    untouched("ㄱㅏ 2026-10-04.", 4);
    assert.equal(carried("ㄱㅏ 10 since 2026-10-04.", { n: 10 }, "n", 12), "ㄱㅏ 12 since 2026-10-04.");
  });
});

describe("digits joined by a colon are never rewritten and never passed over (decisions 515, item 1, and 516, item 2)", () => {
  /**
   * An unprinted fact refused as the colon form `found`, the finding as read, and the reason's line naming the form whole:
   * "only as" where a part equals the fact, which decision 516 counts as a place that can't be rewritten, and "rounded" where one is only near it.
   */
  function refusedAs(summary: string, value: number, found: string, kind: "unsure" | "rounded", last = value + 1): void {
    const f = finding(summary, { x: value });
    const result = applyOverrides([f], [], [override("facts.x", value, last)]);
    assert.deepEqual(result.refused.map((r) => r.why), [{ kind, found }], `${summary} ${value}`);
    assert.deepEqual(result.findings[0], f, `${summary} ${value}`);
    const [shown, to] = [value.toLocaleString("en-GB"), last.toLocaleString("en-GB")];
    const why = kind === "unsure" ? `the wording has ${shown} only as ${found}, which may be this figure or a date, a time or a ratio, and we can't tell` : `the wording doesn't print ${shown} itself and has ${found}, which may be this figure rounded`;
    assert.equal(refusalReason(result.refused).split("\n")[1], `"A planted finding", x: ${shown} as read, corrected to ${to}, but ${why}.`);
  }

  test("a fact of 10, unprinted, beside at 10:00, which the number check's reader calls malformed: refused, the line naming 10:00, as only that where 10 is a part and as rounded where 10.2 is near one", () => {
    refusedAs("The job runs at 10:00 each day.", 10, "10:00", "unsure");
    refusedAs("The job runs at 10:00 each day.", 10.2, "10:00", "rounded");
  });

  test("09:30 with a fact of 9 and of 30: refused naming 09:30. A part with a leading zero is compared as its value, 09 as 9, and by the 5% test, so 9.2 is refused too, as rounded", () => {
    refusedAs("The job runs at 09:30 each day.", 9, "09:30", "unsure");
    refusedAs("The job runs at 09:30 each day.", 30, "09:30", "unsure");
    refusedAs("The job runs at 09:30 each day.", 9.2, "09:30", "rounded");
  });

  test("09:30 and 10:00 with a fact of 7, near neither part: carried, the text as read", () => {
    untouched("The job runs at 09:30 each day.", 7);
    untouched("The job runs at 10:00 each day.", 7);
  });

  test("a fact of 3, or of 1, printed only as 1:3: refused, the wording has it only as 1:3 (round 3 had rounded)", () => {
    refusedAs("Admins to users are 1:3.", 3, "1:3", "unsure");
    refusedAs("Admins to users are 1:3.", 1, "1:3", "unsure");
  });

  test("a fact of 3 printed once as 3 accounts beside 1:3: refused, in 2 places, since the ratio's 3 may be the same figure (round 3 rewrote it at the figure and left the ratio)", () => {
    const f = finding("3 accounts, admins to users 1:3.", { n: 3 });
    const result = applyOverrides([f], [], [override("facts.n", 3, 4)]);
    assert.deepEqual(result.findings[0], f);
    assert.equal(refusalReason(result.refused).split("\n")[1], `"A planted finding", n: 3 as read, corrected to 4, but the wording has 3 in 2 places, and we can't tell which of them are this figure.`);
  });

  test("2026-10-04T09:30 with a fact of 9 or of 30: carried, the text as read, since the time is the date's", () => {
    untouched("Last ran 2026-10-04T09:30.", 9);
    untouched("Last ran 2026-10-04T09:30.", 30);
  });

  test("10:30:15 with a fact of 15, or of 10: refused naming the whole form", () => {
    refusedAs("Logged at 10:30:15.", 15, "10:30:15", "unsure");
    refusedAs("Logged at 10:30:15.", 10, "10:30:15", "unsure");
  });

  test("a part with decimals or groups is the form's, so no digit of it is left to be found as a figure", () => {
    refusedAs("Admins to users are 1.5:1.", 1, "1.5:1", "unsure");
    refusedAs("Admins to users are 1.5:1.", 1.5, "1.5:1", "unsure");
    refusedAs("Admins to users are 1,000:1.", 1000, "1,000:1", "unsure");
    refusedAs("Admins to users are 1,000:1.", 1, "1,000:1", "unsure");
  });
});

describe("a form that may or may not be the figure is a place that can't be rewritten (decision 516, item 2)", () => {
  /** The reason's line for a planted finding's one refusal. */
  const lineOf = (summary: string, facts: Record<string, number>, field: string, last: number): string => {
    const f = finding(summary, facts);
    const result = applyOverrides([f], [], [override(`facts.${field}`, facts[field]!, last)]);
    assert.deepEqual(result.findings[0], f, `${summary}: the finding as read`);
    assert.equal(result.refused.length, 1, summary);
    return refusalReason(result.refused).split("\n")[1]!;
  };
  const onlyAs = (value: string, last: string, form: string, field = "x"): string =>
    `"A planted finding", ${field}: ${value} as read, corrected to ${last}, but the wording has ${value} only as ${form}, which may be this figure or a date, a time or a ratio, and we can't tell.`;
  const inPlaces = (value: string, last: string, places: number, field = "x"): string => `"A planted finding", ${field}: ${value} as read, corrected to ${last}, but the wording has ${value} in ${places} places, and we can't tell which of them are this figure.`;
  const rounded = (value: string, last: string, found: string): string => `"A planted finding", x: ${value} as read, corrected to ${last}, but the wording doesn't print ${value} itself and has ${found}, which may be this figure rounded.`;

  test("12 of 2000 active users, which the number check's reader takes as a year: refused, the wording has 2,000 only as 2000", () => {
    assert.equal(lineOf("12 of 2000 active users (0.6%) logged in this month.", { active: 2000 }, "active", 2100), onlyAs("2,000", "2,100", "2000", "active"));
    assert.equal(lineOf("12 open deals of 2010 have no amount.", { x: 2010 }, "x", 2100), onlyAs("2,010", "2,100", "2010"));
  });

  test("in 2025 with an unprinted fact of 2025: refused the same way; with a fact of 7: carried, the text as read", () => {
    assert.equal(lineOf("Set up in 2025, the rule still runs.", { x: 2025 }, "x", 2030), onlyAs("2,025", "2,030", "2025"));
    untouched("Set up in 2025, the rule still runs.", 7);
  });

  test("in 2025 with an unprinted fact near it: the 5% test takes 1,930 (95 off, within 96.5), 1,990 (35 off, within 99.5) and 2,120 (95 off, within 106) as 2025 rounded", () => {
    for (const value of [1930, 1990, 2120]) {
      assert.equal(lineOf("Set up in 2025, the rule still runs.", { x: value }, "x", value + 7), rounded(value.toLocaleString("en-GB"), (value + 7).toLocaleString("en-GB"), "2025"));
    }
  });

  test("12 of 2,000, with the comma, is no year: rewritten to 12 of 2,100 as round 1 did", () => {
    assert.equal(carried("12 of 2,000 active users logged in this month.", { active: 2000 }, "active", 2100), "12 of 2,100 active users logged in this month.");
  });

  test("a figure printed once plainly and once as a bare year: refused, in 2 places", () => {
    assert.equal(lineOf("2000 users, 12 of 2000 active.", { x: 2000 }, "x", 2100), inPlaces("2,000", "2,100", 2));
  });

  test("3 admins and 9 users (3:9): a correction of either is refused, in 2 places, where round 3 carried it and left the ratio", () => {
    const summary = "3 admins and 9 users (3:9).";
    assert.equal(lineOf(summary, { admins: 3, users: 9 }, "admins", 4), inPlaces("3", "4", 2, "admins"));
    assert.equal(lineOf(summary, { admins: 3, users: 9 }, "users", 10), inPlaces("9", "10", 2, "users"));
  });

  test("a part equal to the figure counts once a part: 3:3 with a fact of 3 is 2 places", () => {
    assert.equal(lineOf("Admins to users are 3:3.", { x: 3 }, "x", 4), inPlaces("3", "4", 2));
  });

  test("a fact of 3 beside 10:30, which has no part equal or near: carried and rewritten at its one place", () => {
    assert.equal(carried("3 jobs run at 10:30 each day.", { n: 3 }, "n", 4), "4 jobs run at 10:30 each day.");
  });

  test("a figure sharing its value with another, printed only as a ratio or a bare year: refused as shared, naming the other", () => {
    const shared = (summary: string, value: number): void => {
      const result = applyOverrides([finding(summary, { x: value, other: value })], [], [override("facts.x", value, value + 1)]);
      assert.deepEqual(result.refused.map((r) => r.why), [{ kind: "shared", other: "other" }], summary);
    };
    shared("Admins to users are 1:3.", 3);
    shared("12 of 2000 active users logged in this month.", 2000);
  });
});

// The synthetic run, as the documents job builds and prints it.
const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const built = (...overrides: NumberOverride[]): BuiltReport => buildReport({ ...input, overrides });
let rows = 0;
const onRun = (f: Finding, field: string, previousValue: number, newValue: number): NumberOverride => ({
  id: `o${++rows}`,
  runId: run.id,
  target: { type: "finding", id: f.id, field },
  previousValue,
  newValue,
  reason: "Checked with the customer.",
  overriddenBy: "Synthetic Consultant",
  overriddenAt: AT,
});

/** The executive summary, the full report and the one-pager, printed as the documents job prints them, into a folder outside the repo. */
async function printed(report: BuiltReport): Promise<ReturnType<typeof heldFigures>> {
  const out = mkdtempSync(join(tmpdir(), "hc-carried-"));
  const paths = { rawNotes: join(out, OUTPUT_FILES.rawNotes), executive: join(out, OUTPUT_FILES.executive), full: join(out, OUTPUT_FILES.full), ceo: join(out, OUTPUT_FILES.ceo), kit: join(out, OUTPUT_FILES.kit), layout: join(out, OUTPUT_FILES.layout) };
  return heldFigures(await printDocuments(report, pseudonyms, paths));
}

const divergence = output!.findings.find((f) => f.ruleId === "pipe.segment-divergence" && f.facts.deals === 43 && f.facts.gap === 33)!;

describe("a figure put back clears the documents (decision 514, item 1)", () => {
  test("pipe.segment-divergence, deals 43 to 33 then 33 to 43, then gap 33 to 34 to 33 on top: nothing refused, nothing held", async () => {
    const back = [onRun(divergence, "facts.deals", 43, 33), onRun(divergence, "facts.deals", 33, 43)];
    for (const overrides of [back, [...back, onRun(divergence, "facts.gap", 33, 34), onRun(divergence, "facts.gap", 34, 33)]]) {
      const report = built(...overrides);
      assert.deepEqual(report.refused, []);
      assert.deepEqual(await printed(report), [], `${overrides.length} rows`);
    }
  });
});

const asBuilt = built();
/** A dataset without the raw notes' overrides section, the one source rows may change. */
const sansOverrides = (set: FigureSet): FigureSet => ({ figures: set.figures.filter((f) => f.source !== "overrides"), dates: set.dates.filter((f) => f.source !== "overrides"), identifiers: set.identifiers.filter((f) => f.source !== "overrides"), tokens: set.tokens.filter((f) => f.source !== "overrides"), labels: set.labels.filter((f) => f.source !== "overrides") });
/** The raw notes' overrides section, a line a row. */
const rawNotesRows = (report: BuiltReport): string[] => {
  const notes = report.outputs.raw_notes;
  const section = notes.sections.find((s) => s.key === "overrides")!;
  return notes.versions.find((v) => v.id === section.currentVersionId)!.body.split("\n");
};
const rowLine = (o: NumberOverride): string => `${o.target.type} ${o.target.id}, ${o.target.field}: changed from ${o.previousValue} to ${o.newValue} by ${o.overriddenBy} on ${o.overriddenAt.slice(0, 10)}. Reason: ${o.reason}`;
// The number check's data is given no rows (decision 516, item 1): what rows
// put back leave, and what a refused figure leaves, is read from the data.
describe("the number check's data after rows put back or refused (decisions 514 and 516, item 1)", () => {
  test("a figure put back: the raw notes still list every row, in order", () => {
    const rows = [onRun(divergence, "facts.deals", 43, 33), onRun(divergence, "facts.deals", 33, 43), onRun(divergence, "facts.gap", 33, 34), onRun(divergence, "facts.gap", 34, 33)];
    assert.deepEqual(rawNotesRows(built(...rows)), rows.map(rowLine));
  });

  test("a refused figure beside a carried one in another finding: the carried one's last value and not its value as read or a value between, and the refused finding's figures as read", () => {
    const storage = output!.findings.find((f) => f.ruleId === "tech.storage-use" && f.status === "issue")!;
    const coverage = output!.findings.find((f) => f.ruleId === "cur.coverage" && f.status === "issue")!;
    const remaining = coverage.facts.remaining!;
    const rows = [onRun(coverage, "facts.remaining", remaining, remaining - 100000), onRun(storage, "facts.DataStorageMBUsed", 8940, 8500), onRun(storage, "facts.DataStorageMBUsed", 8500, 8200)];
    const report = built(...rows);
    assert.deepEqual(report.refused.map((r) => r.findingId), [coverage.id]);
    const of = (set: FigureSet, id: string): number[] => set.figures.filter((f) => f.source === id).map((f) => f.value).sort((a, b) => a - b);
    assert.deepEqual(of(report.dataset, coverage.id), of(asBuilt.dataset, coverage.id), "the refused finding's figures as read");
    assert.ok(of(report.dataset, storage.id).includes(8200) && !of(report.dataset, storage.id).includes(8940) && !of(report.dataset, storage.id).includes(8500));
  });

  test("a stale row, then a row back to the value as read: the data as with no override, and the documents are made", async () => {
    const storage = output!.findings.find((f) => f.ruleId === "tech.storage-use" && f.status === "issue")!;
    const rows = [onRun(storage, "facts.DataStorageMBUsed", 11000, 9800), onRun(storage, "facts.DataStorageMBUsed", 9800, 8940)];
    const report = built(...rows);
    assert.deepEqual(report.refused, []);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(await printed(report), []);
  });
});

describe("a metric's rows: the data holds the metric as the rows leave it (decisions 514, 515 and 516, item 1)", () => {
  // The win rate of 47 is a percentage; the number 47 is another figure of the same metric, the
  // 47 deals of its FY2025-Q3 segment, and so is the number 45. A row dropped every figure of the old
  // value, the segment's 47 with it, and added the new value as a number too (decision 516, item 1).
  const onMetric = (field: string, previousValue: number, newValue: number): NumberOverride => ({ ...onRun(divergence, field, previousValue, newValue), target: { type: "metric", id: "historic.win_rate", field } });
  const of = (set: FigureSet): string[] => set.figures.filter((f) => f.source === "historic.win_rate").map((f) => `${f.kind} ${f.value}`).sort();

  test("the run prints the figures these tests use", () => {
    assert.ok(of(asBuilt.dataset).includes("number 45") && of(asBuilt.dataset).includes("number 47") && of(asBuilt.dataset).includes("percent 47"));
    const metric = input.metrics.find((m) => m.id === "historic.win_rate")!;
    assert.ok(metric.status === "measured" && metric.value === 47 && metric.segments.some((s) => s.key === "FY2025-Q3" && s.n === 47));
  });

  test("A to B to A: the report is the report of no override. The rows given in turn dropped the metric's own 45", () => {
    const rows = [onMetric("value", 47, 45), onMetric("value", 45, 47)];
    const report = built(...rows);
    assert.deepEqual(report.metrics, asBuilt.metrics);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(rawNotesRows(report), rows.map(rowLine));
  });

  // Decision 517: the win rate's 47 is also its FY2025-Q3 segment's n, so a correction that doesn't end at 47 is
  // refused and the metric is as read everywhere, where round 4 carried it beside the segment's 47.
  test("A to B to C: refused, since a segment is also 47, and the report's metrics and data are the run's with no override", () => {
    const rows = [onMetric("value", 47, 45), onMetric("value", 45, 52)];
    const report = built(...rows);
    assert.deepEqual(report.refused.map((r) => [r.metricId, r.field, r.asRead, r.last, r.why]), [["historic.win_rate", "value", 47, 52, { kind: "part", where: "a segment" }]]);
    assert.deepEqual(report.metrics, asBuilt.metrics);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(rawNotesRows(report), rows.map(rowLine));
  });

  test("A as read, then a stale B to C and C to B: refused as stale, where round 4 applied the rows in order and ended at B", () => {
    const rows = [onMetric("value", 45, 52), onMetric("value", 52, 45)];
    const report = built(...rows);
    assert.deepEqual(report.refused.map((r) => r.why), [{ kind: "stale", from: 45 }]);
    assert.match(refusalReason(report.refused), /^"Win rate", value: 47 as read, corrected to 45, but the correction was made from 45, which this run no longer reads\.$/m);
    assert.deepEqual(report.metrics, asBuilt.metrics);
  });

  test("a stale row, then a row back to A: the report is the report of no override. The first row's old value to the last's new gave a row that dropped the metric's own 45", () => {
    const rows = [onMetric("value", 45, 52), onMetric("value", 52, 47)];
    const report = built(...rows);
    assert.deepEqual(report.metrics, asBuilt.metrics);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(rawNotesRows(report), rows.map(rowLine));
  });

  test("a metric the run doesn't hold, or a field it doesn't have, changes nothing", () => {
    for (const row of [{ ...onMetric("value", 47, 45), target: { type: "metric" as const, id: "historic.nothing", field: "value" } }, onMetric("label", 47, 45)]) {
      const report = built(row);
      assert.deepEqual(report.metrics, asBuilt.metrics);
      assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    }
  });
});

describe("bp.users-without-role on the synthetic run (decision 514, item 2)", () => {
  test("n of 11, checked 2026-11-02: carried, 2 of 12, the date as read", () => {
    const f = output!.findings.find((x) => x.ruleId === "bp.users-without-role" && x.status === "issue")!;
    assert.ok(f.summary.includes("2 of 11 (18.2%)") && f.summary.includes("checked 2026-11-02"), f.summary);
    const report = built(onRun(f, "facts.n", 11, 12));
    assert.deepEqual(report.refused, []);
    const shown = report.teamFindings.find((x) => x.id === f.id)!;
    assert.equal(shown.summary, f.summary.replace("2 of 11 (18.2%)", "2 of 12 (18.2%)"));
    assert.equal(shown.facts.n, 12);
  });
});

// Every overridable figure, put back. A figure's other values are every other
// number its finding prints (an override to one of them is the case the rows
// given in turn got wrong) and its value plus 7. Rows for different findings,
// and for different figures of one finding, are judged apart, so each round
// puts back one value a figure, across the run: every pair is in one round.
describe("every overridable figure of the synthetic run, put back (decision 514, item 1)", () => {
  const values = (f: Finding): number[] => [...new Set(textsOf(f).flatMap((t) => extractFigures(t).figures.flatMap((x) => (typeof x.value === "number" ? [x.value] : []))))];
  const figures = output!.findings.flatMap((f) => {
    const printedValues = values(f);
    return [...Object.entries(f.facts).map(([key, value]) => ({ f, field: `facts.${key}`, value })), ...(f.value.kind === "not_sized" ? [] : [{ f, field: "value.amount", value: f.value.amount }])].map((figure) => ({
      ...figure,
      others: [...printedValues.filter((x) => x !== figure.value), figure.value + 7],
    }));
  });

  test("A to X to A, for every number X its finding prints, and A to A+7 to A: the dataset, findings, metrics and value model are the run's with no override", () => {
    const rounds = Math.max(...figures.map((x) => x.others.length));
    let pairs = 0;
    for (let round = 0; round < rounds; round++) {
      const rows = figures.flatMap(({ f, field, value, others }) => (round < others.length ? [onRun(f, field, value, others[round]!), onRun(f, field, others[round]!, value)] : []));
      pairs += rows.length / 2;
      const report = built(...rows);
      assert.deepEqual(report.refused, [], `round ${round}`);
      assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset), `round ${round}`);
      assert.deepEqual(report.teamFindings, asBuilt.teamFindings, `round ${round}`);
      assert.deepEqual(report.metrics, asBuilt.metrics, `round ${round}`);
      assert.deepEqual(report.value, asBuilt.value, `round ${round}`);
    }
    assert.equal(figures.length, 343, "319 facts and 24 sized amounts");
    assert.equal(pairs, 1414 + 343, "each figure against every other number its finding prints, and plus 7");
  });
});

// Measured on the synthetic run (session 22, lane A, round 4): of every pair of
// figures of one finding, A to B's value as read and B to B plus 1, the pairs
// both carried, and the figures the number check then refuses, each an
// impossible "X of Y" such as "7 of 6".
const CARRIED_PAIRS = 634;
const IMPOSSIBLE_PAIRS = 49;

const byId = (id: string): Finding => output!.findings.find((f) => f.id === id)!;

// Ten of the figures put back that held the printed documents when the rows
// were given in turn, one a rule: the first ten a scan of every pair above
// found held, run on round 1's code and printed (session 22, lane A, round 2).
const HELD_WHEN_GIVEN_IN_TURN: Array<[string, string, number]> = [
  ["f-adfc6c7e6577", "facts.changes", 90],
  ["f-ab135b76f2a7", "facts.used", 0.1],
  ["f-d3e294671df9", "facts.DataStorageMBUsed", 87.3],
  ["f-2652164f9e06", "facts.unused", 74.7],
  ["f-10a29aeb78f5", "facts.months", 37.5],
  ["f-9f32364f9405", "facts.hits", 807000],
  ["f-a12d5a82a197", "facts.hits", 103000],
  ["f-2ec6b13fd974", "facts.hits", 9.2],
  ["f-dd5bb22bc57f", "facts.hits", 23.7],
  ["f-21634974044e", "facts.hits", 6.6],
];

describe("figures put back that the rows given in turn held, printed (decision 514, item 1)", () => {
  for (const [id, field, x] of HELD_WHEN_GIVEN_IN_TURN) {
    test(`${byId(id).ruleId} ${field} to ${x} and back: the rows given in turn drop a figure; given no rows, nothing is held`, async () => {
      const f = byId(id);
      const value = f.facts[field.slice("facts.".length)]!;
      const rows = [onRun(f, field, value, x), onRun(f, field, x, value)];
      // The old path, on this finding alone: the number check's data given the rows themselves.
      const figuresOf = (set: FigureSet): string[] => set.figures.map((g) => `${g.kind} ${g.value} ${g.currency ?? ""}`).sort();
      assert.notDeepEqual(figuresOf(buildDataset({ findings: [f], metrics: [], overrides: rows })), figuresOf(buildDataset({ findings: [f], metrics: [] })));
      const report = built(...rows);
      assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
      assert.deepEqual(await printed(report), []);
    });
  }
});

// Measured on the synthetic run (session 22, lane A, round 2): every figure
// carried alone, as read plus 7, times 2 plus 0.5 and times 0.9, 730 printed.
// These are all that held a page the run with no override doesn't: each makes
// "X of Y" with X past Y, a sentence the rule never writes and the number check
// is right to refuse (DECISIONS.md 514, item 3). The row back clears each one.
const HELD_WHEN_CARRIED: Array<[string, string, number, string]> = [
  ["f-50346da1a6a4", "facts.bare", 13, "13 of 8"],
  ["f-10b47c476984", "facts.affected", 8, "8 of 3"],
  ["f-af6e09f0342c", "facts.broken", 8, "8 of 4"],
  ["f-004401da42bd", "facts.jobs", 8, "8 of 3"],
  ["f-bb3a02927e55", "facts.hits", 8, "8 of 4"],
  ["f-50346da1a6a4", "facts.bare", 12.5, "12.5 of 8"],
  ["f-ca58d782d405", "facts.missing", 24.5, "24.5 of 20"],
  ["f-d3e294671df9", "facts.DataStorageMBUsed", 17880.5, "17,880.5 of 10,240"],
  ["f-49f3f62d89cb", "facts.DailyAsyncApexExecutionsUsed", 440000.5, "440,000.5 of 250,000"],
];

describe("a carried figure the number check is right to hold (decision 514, item 3)", () => {
  for (const [id, field, last, sentence] of HELD_WHEN_CARRIED) {
    test(`${byId(id).ruleId} ${field} to ${last} prints "${sentence}", held as impossible, and the row back clears it`, async () => {
      const f = byId(id);
      const value = f.facts[field.slice("facts.".length)]!;
      const report = built(onRun(f, field, value, last));
      assert.deepEqual(report.refused, []);
      assert.ok(report.teamFindings.find((x) => x.id === id)!.summary.includes(sentence));
      const [figure] = sentence.split(" of ");
      assert.deepEqual(
        (await printed(report)).map((h) => [h.figure, h.reason]),
        [[figure, `can't be ${sentence}`]],
      );
      assert.deepEqual(await printed(built(onRun(f, field, value, last), onRun(f, field, last, value))), []);
    });
  }
});

/** What a reader sees of a page: its words, without the markup, styles or a chart's coordinates. */
const decoded = (html: string): string => html.replace(/<(style|script)\b[\s\S]*?<\/\1>/g, " ").replace(/<[^>]*>/g, " ").replace(/&#39;/g, "'").replace(/&amp;/g, "&");
const ruleOf = (ruleId: string): Finding => output!.findings.find((f) => f.ruleId === ruleId && f.status === "issue")!;
/** The figures of a finding an override can name, as the console offers them. */
const figuresOf = (f: Finding): Array<{ field: string; value: number }> => [...Object.entries(f.facts).map(([key, value]) => ({ field: `facts.${key}`, value })), ...(f.value.kind === "not_sized" ? [] : [{ field: "value.amount", value: f.value.amount }])];
const valuesFrom = (set: FigureSet, source: string): number[] => set.figures.filter((f) => f.source === source).map((f) => f.value);

describe("the number check's data is the findings as corrected, with no rows (decision 516, item 1)", () => {
  const missing = ruleOf("dq.open-opps-missing-amount");

  test("dq.open-opps-missing-amount, rate 9.2 to 45.5 with worstSegmentRate 45.5 to 46.5, in either order: nothing refused, nothing held, and the page reads both corrected figures", async () => {
    assert.ok(missing.summary.includes("7 of 76 (9.2%)") && missing.summary.includes("Renewal is at 45.5% (5 of 11)"), missing.summary);
    const rows = [onRun(missing, "facts.rate", 9.2, 45.5), onRun(missing, "facts.worstSegmentRate", 45.5, 46.5)];
    for (const order of [rows, rows.slice().reverse()]) {
      const report = built(...order);
      assert.deepEqual(report.refused, []);
      const shown = report.teamFindings.find((x) => x.id === missing.id)!;
      assert.ok(shown.summary.includes("7 of 76 (45.5%)") && shown.summary.includes("Renewal is at 46.5% (5 of 11)"), shown.summary);
      assert.deepEqual(await printed(report), [], order.map((o) => o.target.field).join(" then "));
      const page = decoded(fullReportHtml(report, pseudonyms));
      assert.ok(page.includes("7 of 76 (45.5%)") && page.includes("Renewal is at 46.5% (5 of 11)"), "the full report prints both corrected figures");
    }
  });

  test("a carried figure's value as read leaves the data from its finding and stays from the raw notes' override line; a figure of the finding sharing it, unprinted, stays", () => {
    const storage = ruleOf("tech.storage-use");
    const report = built(onRun(storage, "facts.DataStorageMBUsed", 8940, 8200));
    assert.ok(!valuesFrom(report.dataset, storage.id).includes(8940) && valuesFrom(report.dataset, storage.id).includes(8200));
    // The limit of 509: the raw notes' line about the override still vouches for the old count.
    assert.ok(valuesFrom(asBuilt.dataset, "overrides").length === 0 && valuesFrom(report.dataset, "overrides").includes(8940));
    // The annual saving and the sized amount are both 18,000 and neither is printed: the saving is carried, and the amount keeps 18,000 as money.
    const seats = ruleOf("utl.unassigned-licence-seats");
    assert.ok(seats.value.kind !== "not_sized" && seats.value.amount === 18000 && seats.facts.annualSaving === 18000);
    const saving = built(onRun(seats, "facts.annualSaving", 18000, 18007));
    assert.deepEqual(saving.refused, []);
    assert.ok(saving.dataset.figures.some((f) => f.source === seats.id && f.kind === "currency" && f.value === 18000), "the sized amount's 18,000");
    assert.ok(valuesFrom(saving.dataset, seats.id).includes(18007));
  });

  test("two figures of one finding exchanged, tech.storage-use's data and file storage used: both carried, nothing held", async () => {
    const storage = ruleOf("tech.storage-use");
    const report = built(onRun(storage, "facts.DataStorageMBUsed", 8940, 4480), onRun(storage, "facts.FileStorageMBUsed", 4480, 8940));
    assert.deepEqual(report.refused, []);
    const shown = report.teamFindings.find((x) => x.id === storage.id)!;
    assert.ok(shown.summary.includes("(4,480 of 10,240 MB)") && shown.summary.includes("(8,940 of 20,480 MB)"), shown.summary);
    assert.deepEqual(await printed(report), []);
  });

  // Every pair of figures of one finding, A to B's value as read and B to B
  // plus 1, both carried: the case where the rows met. Rows of different
  // findings are judged apart, so each round takes one pair a finding. Each
  // finding's texts are checked against the report's data without the other
  // findings changed in its round, so no other row can back a figure for it,
  // and against the run's own data scoped the same way for what was there
  // before: the number check reads "-02" of "since 2026-08-02" as a figure,
  // once a metric's segment key "2026-08" is masked, and only another source's
  // 2 backs it.
  test("every pair of one finding, A to B's value and B to B plus 1, both carried: every printed text passes the number check but an impossible X of Y", () => {
    const pairs = new Map(output!.findings.map((f) => [f.id, figuresOf(f).flatMap((a) => figuresOf(f).filter((b) => b.field !== a.field && b.value !== a.value).map((b) => [a, b] as const))]));
    const sources = (set: FigureSet): string[] => [...new Set([...set.figures, ...set.dates, ...set.identifiers, ...set.tokens, ...set.labels].map((x) => x.source))];
    const unmatched = (f: Finding, set: FigureSet, others: Set<string>) => {
      const data = scopeDataset(set, sources(set).filter((s) => s === f.id || !others.has(s)));
      return textsOf(f).flatMap((t) => checkNumbers(t, data).unmatched);
    };
    const impossible: string[] = [];
    let carriedPairs = 0;
    for (let round = 0; round < Math.max(...[...pairs.values()].map((p) => p.length)); round++) {
      const changed = output!.findings.filter((f) => round < pairs.get(f.id)!.length);
      const rows = changed.flatMap((f) => {
        const [a, b] = pairs.get(f.id)![round]!;
        return [onRun(f, a.field, a.value, b.value), onRun(f, b.field, b.value, b.value + 1)];
      });
      const report = built(...rows);
      const refused = new Set(report.refused.map((r) => r.findingId));
      const others = new Set(changed.map((f) => f.id));
      for (const f of changed.filter((x) => !refused.has(x.id))) {
        carriedPairs++;
        const before = unmatched(asBuilt.teamFindings.find((x) => x.id === f.id)!, asBuilt.dataset, others);
        for (const u of unmatched(report.teamFindings.find((x) => x.id === f.id)!, report.dataset, others).filter((u) => !before.some((b) => b.raw === u.raw && b.reason === u.reason))) {
          const [a, b] = pairs.get(f.id)![round]!;
          assert.match(u.reason, /^can't be [\d,.]+ of [\d,.]+$/, `${f.ruleId} ${a.field} to ${b.value}, ${b.field} to ${b.value + 1}: ${u.raw} ${u.reason}`);
          impossible.push(`${f.ruleId} ${u.raw} ${u.reason}`);
        }
      }
    }
    assert.equal(carriedPairs, CARRIED_PAIRS);
    assert.equal(impossible.length, IMPOSSIBLE_PAIRS, impossible.join("\n"));
  });

  for (const [ruleId, [a, aTo], [b, bTo]] of [
    ["tech.storage-use", ["facts.DataStorageMBShare", 21.9], ["facts.FileStorageMBShare", 22.9]],
    ["act.event-link-collapse", ["facts.priorShare", 5.6], ["facts.recentShare", 6.6]],
    ["pipe.dead-end-stage", ["facts.share", 47.8], ["facts.wait", 48.8]],
  ] as const) {
    test(`${ruleId}, ${a} to ${aTo} with ${b} to ${bTo}, which the rows held: printed, nothing held`, async () => {
      const f = ruleOf(ruleId);
      const report = built(onRun(f, a, f.facts[a.slice(6)]!, aTo), onRun(f, b, f.facts[b.slice(6)]!, bTo));
      assert.deepEqual(report.refused, []);
      assert.notDeepEqual(report.teamFindings.find((x) => x.id === f.id)!.summary, f.summary);
      assert.deepEqual(await printed(report), []);
    });
  }
});

// Decision 518: a date the number check's reader finds by a word is a guess, and its digits standing by themselves are unsure.
describe("a date read by a word is a guess, and its own digits are unsure (decision 518)", () => {
  const onlyAs = (value: string, last: string, form: string): string =>
    `"A planted finding", x: ${value} as read, corrected to ${last}, but the wording has ${value} only as ${form}, which may be this figure or a date, a time or a ratio, and we can't tell.`;
  const lineOf = (summary: string, value: number, last: number): string => {
    const f = finding(summary, { x: value });
    const result = applyOverrides([f], [], [override("facts.x", value, last)]);
    assert.deepEqual(result.findings[0], f, summary);
    return refusalReason(result.refused).split("\n")[1]!;
  };

  test("the third review's four sentences, each with the count corrected: refused, naming the date as written, where round 4 rewrote nothing and left the count printed", () => {
    assert.equal(lineOf("3 may lose access when the licences lapse.", 3, 4), onlyAs("3", "4", "3 may"));
    assert.equal(lineOf("12 of March's deals closed late.", 12, 13), onlyAs("12", "13", "12 of March"));
    assert.equal(lineOf("In March, 2000 deals were opened.", 2000, 2100), onlyAs("2,000", "2,100", "March, 2000"));
    assert.equal(lineOf("In Q3 12 deals slipped.", 12, 13), onlyAs("12", "13", "Q3 12"));
    assert.equal(lineOf("FY 2000 deals were opened.", 2000, 2100), onlyAs("2,000", "2,100", "FY 2000"));
  });
});

/** Every section approved as the engine drafted it, as the console holds a released run, with the overrides given. */
function approvedWith(overrides: NumberOverride[]): ApprovedRun {
  const report = buildReport(loadRun(dir).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const b of Object.values(report.outputs)) {
    for (const s of b.sections) {
      sections.push({ ...s, status: "approved", approval: { approvedBy: "consultant@example.com", approvedAt: AT } });
      versions.push(b.versions.find((v) => v.id === s.currentVersionId)!);
    }
  }
  return { intake: null, sections, versions, overrides };
}

const onMetricOf = (id: string, field: string, previousValue: number, newValue: number): NumberOverride => ({ ...onRun(divergence, field, previousValue, newValue), target: { type: "metric", id, field } });
const CLOSE = "Override each one back to its figure as read, then make the documents again. A finding's wording and a measure's chart or table can't be edited, so a figure we can't carry changes only with a new health check.";

// Decision 517: the third review's metrics, each printed corrected beside a chart, table or series as read with nothing held on round 4.
describe("a metric's figure another of its figures also holds is refused (decision 517)", () => {
  for (const [id, asRead, last, where] of [
    ["historic.top_account_share", 32.1, 39.1, "a segment"],
    ["adoption.mobile_login_share", 2.1, 2.8, "a segment"],
    ["historic.quarterly_won_value", 803000, 850000, "a point in its series"],
  ] as const) {
    test(`${id}, ${asRead} to ${last}: refused naming ${where}, and the documents job's make stops before a file is written`, async () => {
      const report = built(onMetricOf(id, "value", asRead, last));
      assert.deepEqual(report.refused.map((r) => [r.metricId, r.why]), [[id, { kind: "part", where }]]);
      assert.deepEqual(report.metrics, asBuilt.metrics);
      const out = join(mkdtempSync(join(tmpdir(), "hc-metric-")), "documents");
      const result = await renderReleased(dir, approvedWith([onMetricOf(id, "value", asRead, last)]), out);
      assert.ok("refused" in result);
      assert.equal(existsSync(out) && readdirSync(out).length > 0, false, "no file written");
      const label = input.metrics.find((m) => m.id === id)!.label;
      const g = (n: number): string => n.toLocaleString("en-GB", { maximumFractionDigits: 100 });
      assert.equal(
        refusalReason(result.refused),
        [
          "Held back: 1 corrected figure can't be carried into the documents.",
          `"${label}", value: ${g(asRead)} as read, corrected to ${g(last)}, but another figure of this measure (${where}) is also ${g(asRead)}, and a chart, table or sentence prints it as read.`,
          CLOSE,
        ].join("\n"),
      );
    });
  }

  test("historic.top_account_share's reason, whole, as the row has it", () => {
    assert.equal(
      refusalReason(built(onMetricOf("historic.top_account_share", "value", 32.1, 39.1)).refused).split("\n")[1],
      `"Won value from the top 10 accounts", value: 32.1 as read, corrected to 39.1, but another figure of this measure (a segment) is also 32.1, and a chart, table or sentence prints it as read.`,
    );
  });
});

// Decision 519: the executive summary's Pipeline section is written from the metrics as corrected, as every other section, tile and chart is.
describe("the executive Pipeline section reads the metrics as corrected (decision 519)", () => {
  /** The executive Pipeline page as a reader sees it: from its heading to the next page's. */
  const pipelinePage = (report: BuiltReport): string => {
    const html = executiveHtml(report, pseudonyms);
    const from = html.indexOf("<h2>Your pipeline today</h2>");
    return decoded(html.slice(from, html.indexOf('class="eyebrow"', from))).replace(/\s+/g, " ");
  };
  const briefOf = (report: BuiltReport) => report.briefs.get(`${run.id}:executive_summary:pipeline`)!;
  const valueIn = (report: BuiltReport, id: string): unknown => (briefOf(report).sources.find((s) => s.id === id) as { value?: number } | undefined)?.value;

  test("current.open_pipeline 3,047,000 to 2,742,300 and current.delivery_outlook_deals 31 to 28: the sentence and the tile read £2.74m, across about 28 deals, the brief's sources hold both, nothing held", async () => {
    const asRead = pipelinePage(asBuilt);
    assert.ok(asRead.includes("worth £3.05m") && asRead.includes("£3.05m Open pipeline") && asRead.includes("across about 31 deals"), asRead);
    const report = built(onMetricOf("current.open_pipeline", "value", 3047000, 2742300), onMetricOf("current.delivery_outlook_deals", "value", 31, 28));
    assert.deepEqual(report.refused, []);
    const page = pipelinePage(report);
    assert.ok(page.includes("You have 76 open deals worth £2.74m.") && page.includes("£2.74m Open pipeline"), page);
    assert.ok(page.includes("across about 28 deals"), page);
    assert.ok(!page.includes("£3.05m") && !page.includes("31 deals"), page);
    assert.equal(valueIn(report, "current.open_pipeline"), 2742300);
    assert.equal(valueIn(report, "current.delivery_outlook_deals"), 28);
    assert.deepEqual(await printed(report), []);
  });

  test("current.weighted_pipeline and current.delivery_outlook_value, each times 0.9: carried, nothing held, where the section from the metrics as read had them held", async () => {
    for (const id of ["current.weighted_pipeline", "current.delivery_outlook_value"]) {
      const metric = input.metrics.find((m) => m.id === id)!;
      assert.ok(metric.status === "measured");
      const report = built(onMetricOf(id, "value", metric.value, metric.value * 0.9));
      assert.deepEqual(report.refused, [], id);
      assert.equal(valueIn(report, id), metric.value * 0.9, id);
      assert.deepEqual(await printed(report), [], id);
    }
  });

  // The admitted limit of 519: two metrics hold the same quantity, and a correction to one leaves the other's chart as read.
  test("current.open_pipeline corrected: the pipeline-over-time chart's last label still reads £3.05m, nothing held; historic.pipeline_point_in_time_value corrected too is refused naming a point in its series", async () => {
    const chart = (report: BuiltReport): string => decoded(fullReportHtml(report, pseudonyms)).replace(/\s+/g, " ");
    assert.ok(chart(asBuilt).includes("£3.05m Nov 2024 Oct 2026"));
    const open = onMetricOf("current.open_pipeline", "value", 3047000, 2742300);
    const report = built(open);
    assert.deepEqual(report.refused, []);
    assert.ok(chart(report).includes("£3.05m Nov 2024 Oct 2026"), "the chart's last label as read");
    assert.deepEqual(await printed(report), []);
    const both = built(open, onMetricOf("historic.pipeline_point_in_time_value", "value", 3047000, 2742300));
    assert.deepEqual(both.refused.map((r) => r.metricId), ["historic.pipeline_point_in_time_value"]);
    assert.equal(
      refusalReason(both.refused).split("\n")[1],
      `"Open pipeline at each month end", value: 3,047,000 as read, corrected to 2,742,300, but another figure of this measure (a point in its series) is also 3,047,000, and a chart, table or sentence prints it as read.`,
    );
  });

  test("current.open_deals 76 to 80, whose n is also 76: refused, the Pipeline section as read, and the documents job's make held with 517's reason", async () => {
    const row = onMetricOf("current.open_deals", "value", 76, 80);
    const report = built(row);
    assert.deepEqual(report.refused.map((r) => [r.metricId, r.why]), [["current.open_deals", { kind: "part", where: "n" }]]);
    assert.equal(pipelinePage(report), pipelinePage(asBuilt));
    assert.equal(valueIn(report, "current.open_deals"), 76);
    const out = join(mkdtempSync(join(tmpdir(), "hc-metric-")), "documents");
    const result = await renderReleased(dir, approvedWith([row]), out);
    assert.ok("refused" in result);
    assert.equal(existsSync(out) && readdirSync(out).length > 0, false, "no file written");
    assert.equal(
      refusalReason(result.refused),
      [
        "Held back: 1 corrected figure can't be carried into the documents.",
        `"Open deals", value: 76 as read, corrected to 80, but another figure of this measure (n) is also 76, and a chart, table or sentence prints it as read.`,
        CLOSE,
      ].join("\n"),
    );
  });
});

// Decision 517 on planted measures: each part and text of a measure, alone, against its value as read.
type Measured = Extract<Metric, { status: "measured" }>;
const MID = "historic.planted";
/** A planted measure whose only figures are the ones a test puts in it: the evidence's count is 900. */
function measure(edit: Partial<Measured> = {}): Measured {
  return { id: MID, domain: "historic_pipeline", status: "measured", label: "A planted measure", definition: "Won deals over closed deals.", unit: "percent", evidence: [{ ...evidence, count: 900 }], exclusions: [], assumptions: [], value: 37, n: 100, dateRange: null, segments: [], segmentExemptReason: "One org-wide figure.", series: [], stats: {}, ...edit };
}
const onMeasure = (field: string, previousValue: number, newValue: number, id = "m1"): NumberOverride => ({ ...override(field, previousValue, newValue, id), target: { type: "metric", id: MID, field } });
const segment = (edit: Partial<Measured["segments"][number]> = {}): Measured["segments"][number] => ({ dimension: "stage", key: "Negotiation", n: 10, value: 12, notMeasurableReason: null, ...edit });
const point = (edit: Partial<Measured["series"][number]> = {}): Measured["series"][number] => ({ date: "2026-07-01", n: 10, value: 12, notMeasurableReason: null, ...edit });
/** The measure's one refusal, and the measure as read beside it. */
function refusedOf(m: Measured, ...rows: NumberOverride[]): OverrideRefusal {
  const result = applyOverrides([], [m], rows);
  assert.deepEqual(result.metrics, [m], "the measure as read");
  assert.equal(result.refused.length, 1);
  return result.refused[0]!;
}

describe("a measure's figure another of its figures holds, each where in the row's order (decision 517, item 3)", () => {
  test("value equal to n; to a stat; to a segment's n; to a series point's n; to an evidence count only: each named", () => {
    const where = (m: Measured): unknown => refusedOf(m, onMeasure("value", 47, 52)).why;
    assert.deepEqual(where(measure({ value: 47, n: 47 })), { kind: "part", where: "n" });
    assert.deepEqual(where(measure({ value: 47, stats: { moves: 47 } })), { kind: "part", where: "the stat moves" });
    assert.deepEqual(where(measure({ value: 47, segments: [segment({ n: 47 })] })), { kind: "part", where: "a segment" });
    assert.deepEqual(where(measure({ value: 47, series: [point({ n: 47 })] })), { kind: "part", where: "a point in its series" });
    assert.deepEqual(where(measure({ value: 47, evidence: [{ ...evidence, count: 47 }] })), { kind: "part", where: "an evidence count" });
    // A segment's or a point's value counts as well as its n.
    assert.deepEqual(where(measure({ value: 47, segments: [segment({ value: 47 })] })), { kind: "part", where: "a segment" });
    assert.deepEqual(where(measure({ value: 47, series: [point({ value: 47 })] })), { kind: "part", where: "a point in its series" });
  });

  test("an override of n equal to the value names value; an override of a stat equal to another stat names that stat", () => {
    assert.deepEqual(refusedOf(measure({ value: 100, n: 100 }), onMeasure("n", 100, 90)).why, { kind: "part", where: "value" });
    const stat = refusedOf(measure({ stats: { won: 12, lost: 12 } }), onMeasure("stats.won", 12, 13));
    assert.deepEqual(stat.why, { kind: "part", where: "the stat lost" });
    assert.equal(refusalReason([stat]).split("\n")[1], `"A planted measure", won: 12 as read, corrected to 13, but another figure of this measure (the stat lost) is also 12, and a chart, table or sentence prints it as read.`);
  });

  test("when several hold it, the first in the row's order is named: n, a stat, a segment, a point, an evidence count", () => {
    const all: Partial<Measured> = { value: 47, n: 47, stats: { moves: 47 }, segments: [segment({ n: 47 })], series: [point({ n: 47 })], evidence: [{ ...evidence, count: 47 }] };
    const order: Array<[Partial<Measured>, string]> = [
      [{}, "n"],
      [{ n: 100 }, "the stat moves"],
      [{ n: 100, stats: {} }, "a segment"],
      [{ n: 100, stats: {}, segments: [] }, "a point in its series"],
      [{ n: 100, stats: {}, segments: [], series: [] }, "an evidence count"],
    ];
    for (const [less, where] of order) assert.deepEqual(refusedOf(measure({ ...all, ...less }), onMeasure("value", 47, 52)).why, { kind: "part", where }, where);
  });

  test("a segment's key, a person's token or the customer's own stage name, is in the reason nowhere", () => {
    for (const key of ["[[user:U0047]]", "Negotiation/Review"]) {
      const reason = refusalReason([refusedOf(measure({ value: 47, segments: [segment({ dimension: "owner", key, n: 47 })] }), onMeasure("value", 47, 52))]);
      assert.ok(!reason.includes(key) && !reason.includes("U0047") && !reason.includes("Negotiation"), reason);
      assert.ok(reason.includes("another figure of this measure (a segment) is also 47"), reason);
    }
  });
});

describe("a measure's own wording that prints its value (decision 517, item 4)", () => {
  const worded = `"A planted measure", value: 5 as read, corrected to 6, but the measure's own wording has 5, which a correction doesn't rewrite.`;
  const lineOf = (m: Measured, last = 6): string => refusalReason([refusedOf(m, onMeasure("value", m.value, last))]).split("\n")[1]!;

  test("the value standing once in the label, the definition, an assumption, an exclusion, the reason it isn't segmented, a segment's reason or a point's: refused, the texts as read", () => {
    const places: Array<[string, Partial<Measured>]> = [
      ["label", { label: "Deals over 5 stages" }],
      ["definition", { definition: "Deals that moved through 5 stages." }],
      ["an assumption", { assumptions: ["Fewer than 5 people logged in."] }],
      ["an exclusion", { exclusions: ["Deals with 5 owners or more."] }],
      ["the segment exempt reason", { segmentExemptReason: "Fewer than 5 people, so one org-wide figure." }],
      ["a segment's reason", { segments: [segment({ value: null, notMeasurableReason: "Fewer than 5 people." })] }],
      ["a point's reason", { series: [point({ value: null, notMeasurableReason: "Fewer than 5 people." })] }],
    ];
    for (const [where, edit] of places) {
      const m = measure({ value: 5, ...edit });
      const line = lineOf(m);
      assert.equal(line.replace(/^"[^"]*"/, '"A planted measure"'), worded, where);
      assert.deepEqual(refusedOf(m, onMeasure("value", 5, 6)).why, { kind: "worded" }, where);
    }
  });

  test("the value only as 3 may in an assumption: refused the same way", () => {
    assert.deepEqual(refusedOf(measure({ value: 3, assumptions: ["Users who left 3 may still hold a licence."] }), onMeasure("value", 3, 4)).why, { kind: "worded" });
  });

  test("a number near it and not it: refused with the row's rounded wording", () => {
    const line = lineOf(measure({ value: 37, definition: "About 36 deals a quarter." }), 40);
    assert.equal(line, `"A planted measure", value: 37 as read, corrected to 40, but the measure's own wording doesn't print 37 itself and has 36, which may be this figure rounded.`);
  });

  test("the value only inside an ISO date or a person's token in its texts: carried, the texts as read", () => {
    for (const [value, edit] of [
      [12, { definition: "Read on 2026-03-12." }],
      [3, { assumptions: ["Read on 2026-03-12."] }],
      [12, { label: "Deals owned by [[user:U0012]]" }],
    ] as Array<[number, Partial<Measured>]>) {
      const m = measure({ value, ...edit });
      const result = applyOverrides([], [m], [onMeasure("value", value, value + 7)]);
      assert.deepEqual(result.refused, [], JSON.stringify(edit));
      assert.deepEqual(result.metrics, [{ ...m, value: value + 7 }], JSON.stringify(edit));
    }
  });
});

describe("one measure, several figures, and rows that change nothing (decision 517)", () => {
  test("two figures of one measure, one refused and one carriable: the whole measure as read, one refusal listed", () => {
    const m = measure({ n: 100, segments: [segment({ n: 100 })] });
    const result = applyOverrides([], [m], [onMeasure("value", 37, 40, "m1"), onMeasure("n", 100, 110, "m2")]);
    assert.deepEqual(result.metrics, [m]);
    assert.deepEqual(result.refused.map((r) => [r.metricId, r.field, r.why]), [[MID, "n", { kind: "part", where: "a segment" }]]);
    assert.equal(refusalReason(result.refused).split("\n").length, 3);
  });

  test("a row for a measure the run doesn't hold, one not measured, or a field it doesn't have: nothing changes, nothing refused", () => {
    const notMeasured: Metric = { id: MID, domain: "historic_pipeline", status: "not_measurable", label: "A planted measure", definition: "Won deals over closed deals.", unit: "percent", evidence: [evidence], exclusions: [], assumptions: [], reason: "Fewer than 5 deals closed." };
    for (const [m, row] of [
      [measure(), { ...onMeasure("value", 37, 40), target: { type: "metric" as const, id: "historic.nothing", field: "value" } }],
      [notMeasured, onMeasure("value", 37, 40)],
      [measure(), onMeasure("stats.nothing", 37, 40)],
      [measure(), onMeasure("label", 37, 40)],
    ] as Array<[Metric, NumberOverride]>) {
      const result = applyOverrides([], [m], [row]);
      assert.deepEqual(result, { findings: [], metrics: [m], refused: [] }, row.target.field);
    }
  });
});

describe("a measure's refusal beside a finding's, as the console lists them (decisions 512 and 517)", () => {
  const ofFinding = (i: number): OverrideRefusal => ({ findingId: `f-${i}`, title: `Finding ${i}`, field: "facts.hits", asRead: 1200, last: 1100, why: { kind: "repeated", places: 2 } });
  const ofMeasure = (i: number, why: OverrideRefusal["why"] = { kind: "part", where: "a segment" }, title = `Measure ${i}`): OverrideRefusal => ({ metricId: `m-${i}`, title, field: "value", asRead: 47, last: 52, why });

  test("a finding's refusal and a measure's together: the finding's line first, whichever came first, and through applyOverrides", () => {
    const lines = refusalReason([ofMeasure(1), ofFinding(1)]).split("\n");
    assert.deepEqual([lines[0], lines[1]!.slice(0, 11), lines[2]!.slice(0, 11), lines[3]], ["Held back: 2 corrected figures can't be carried into the documents.", '"Finding 1"', '"Measure 1"', CLOSE]);
    const f = finding("3 admins and 3 users.", { admins: 3 });
    const result = applyOverrides([f], [measure({ n: 37 })], [onMeasure("value", 37, 40, "m1"), override("facts.admins", 3, 4, "o1")]);
    assert.deepEqual(result.refused.map((r) => r.findingId ?? r.metricId), [ID, MID]);
  });

  test("eleven refusals across both: ten lines, the findings' first, then And 1 more.", () => {
    const lines = refusalReason([...Array.from({ length: 5 }, (_, i) => ofMeasure(i)), ...Array.from({ length: 6 }, (_, i) => ofFinding(i))]).split("\n");
    assert.equal(lines.length, 13);
    assert.equal(lines[0], "Held back: 11 corrected figures can't be carried into the documents.");
    assert.deepEqual(lines.slice(1, 11).map((l) => l.split('"')[1]), [...Array.from({ length: 6 }, (_, i) => `Finding ${i}`), ...Array.from({ length: 4 }, (_, i) => `Measure ${i}`)]);
    assert.deepEqual(lines.slice(11), ["And 1 more.", CLOSE]);
  });

  test("a measure's label holding a person's token prints a named person", () => {
    const line = refusalReason([ofMeasure(1, { kind: "worded" }, "Deals owned by [[user:U0047]]")]).split("\n")[1]!;
    assert.equal(line, `"Deals owned by a named person", value: 47 as read, corrected to 52, but the measure's own wording has 47, which a correction doesn't rewrite.`);
  });

  test("the console's reader over a measure-only reason, a mixed one and each of a measure's whys: Held back: first, three lines or more, none empty, the close last", () => {
    const whys: OverrideRefusal["why"][] = [{ kind: "part", where: "n" }, { kind: "part", where: "an evidence count" }, { kind: "worded" }, { kind: "rounded", found: "36" }, { kind: "unsure", found: "3 may" }, { kind: "stale", from: 45 }];
    const reasons = [refusalReason([ofMeasure(1)]), refusalReason([ofMeasure(1), ofFinding(1)]), ...whys.map((why) => refusalReason([ofMeasure(1, why)]))];
    for (const reason of reasons) {
      const read = reason.split("\n").map((l) => l.trim()).filter(Boolean);
      assert.deepEqual(read, reason.split("\n"), "no line empty or padded");
      assert.ok(read[0]!.startsWith("Held back: ") && read.length >= 3 && read.at(-1) === CLOSE, reason);
    }
    assert.equal(reasons[5]!.split("\n")[1], `"Measure 1", value: 47 as read, corrected to 52, but the measure's own wording doesn't print 47 itself and has 36, which may be this figure rounded.`);
  });
});

// Decision 518, the rest: what stays a figure beside a guessed date, and the full dates round 4 masked whole.
describe("a guessed date's own digits, and digits joined to a letter (decision 518)", () => {
  const lineOf = (summary: string, value: number, last: number): string => {
    const f = finding(summary, { x: value });
    const result = applyOverrides([f], [], [override("facts.x", value, last)]);
    assert.deepEqual(result.findings[0], f, summary);
    return refusalReason(result.refused).split("\n")[1]!;
  };
  const g = (n: number): string => n.toLocaleString("en-GB");
  const onlyAs = (value: number, last: number, form: string): string => `"A planted finding", x: ${g(value)} as read, corrected to ${g(last)}, but the wording has ${g(value)} only as ${form}, which may be this figure or a date, a time or a ratio, and we can't tell.`;
  const rounded = (value: number, last: number, found: string): string => `"A planted finding", x: ${g(value)} as read, corrected to ${g(last)}, but the wording doesn't print ${g(value)} itself and has ${found}, which may be this figure rounded.`;
  const inTwo = (value: number, last: number): string => `"A planted finding", x: ${g(value)} as read, corrected to ${g(last)}, but the wording has ${g(value)} in 2 places, and we can't tell which of them are this figure.`;

  test("the third review's four sentences and FY 2000, each with the figure also printed plainly: refused, in 2 places, where round 4 rewrote the plain one and left the other", () => {
    assert.equal(lineOf("3 may lose access; 3 accounts in all.", 3, 4), inTwo(3, 4));
    assert.equal(lineOf("12 of March's deals closed late, 12 in all.", 12, 13), inTwo(12, 13));
    assert.equal(lineOf("In March, 2000 deals were opened: 2,000 in all.", 2000, 2100), inTwo(2000, 2100));
    assert.equal(lineOf("In Q3 12 deals slipped, 12 of 40.", 12, 13), inTwo(12, 13));
    assert.equal(lineOf("FY 2000 deals were opened: 2,000 in all.", 2000, 2100), inTwo(2000, 2100));
  });

  test("In Q3, 3 deals slipped, with a fact of 3 to 4: rewritten at its one place, since Q3's 3 is joined to a letter", () => {
    assert.equal(carried("In Q3, 3 deals slipped.", { x: 3 }, "x", 4), "In Q3, 4 deals slipped.");
  });

  test("FY26, March 3rd and the 3rd of March with an unprinted fact of 26 or 3: carried, the text as read", () => {
    untouched("Set in the FY26 plan.", 26);
    untouched("Agreed on March 3rd.", 3);
    untouched("Agreed on the 3rd of March.", 3);
  });

  // Round 4 masked these whole (its "the day and the year of 31 December 2025" and "the year of December 2025").
  test("31 December 2025 with an unprinted fact of 31 or 2025, and December 2025 with 2025: refused as unsure, naming the date as written; with 7: carried", () => {
    assert.equal(lineOf("Closed by 31 December 2025.", 31, 38), onlyAs(31, 38, "31 December 2025"));
    assert.equal(lineOf("Closed by 31 December 2025.", 2025, 2032), onlyAs(2025, 2032, "31 December 2025"));
    assert.equal(lineOf("Closed in December 2025.", 2025, 2030), onlyAs(2025, 2030, "December 2025"));
    untouched("Closed by 31 December 2025.", 7);
  });

  test("31 December 2025 and the 5% test: 30 is near 31, 20 near 2025 as a percentage (2,000), and 2,000 near 2025, so each is refused as rounded, naming the date", () => {
    for (const value of [30, 20, 2000]) assert.equal(lineOf("Closed by 31 December 2025.", value, value + 7), rounded(value, value + 7, "31 December 2025"));
  });

  test("12/03/2026 and 2026-03-12T10:30:00Z, digits and separators only, with an unprinted fact of 12, 3 or 10: carried, the text as read", () => {
    for (const value of [12, 3, 10]) {
      untouched("Checked on 12/03/2026.", value);
      untouched("Checked at 2026-03-12T10:30:00Z.", value);
    }
  });
});

describe("the two automation findings that print 31 December 2025, on the synthetic run (decision 518)", () => {
  // Measured before and after 518 (bdf574b and this branch): every figure of both, plus 7, times 2 plus 0.5 and
  // times 0.9, has the same verdict, since none is 31, 2025 or near either. What moved is an unprinted figure of 31 or 2025.
  const rules = ["aut.legacy-workflow-rules", "aut.legacy-process-builder"].map(ruleOf);

  test("each figure's verdict, unchanged by 518", () => {
    const verdicts = rules.flatMap((f) => figuresOf(f).map(({ field, value }) => {
      const result = applyOverrides([f], [], [onRun(f, field, value, value + 7)]);
      return [f.ruleId, field, value, result.refused[0]?.why ?? (result.findings[0]!.summary === f.summary ? "changed with no text" : "rewritten")];
    }));
    assert.deepEqual(verdicts, [
      ["aut.legacy-workflow-rules", "facts.activeRules", 3, { kind: "shared", other: "objects" }],
      ["aut.legacy-workflow-rules", "facts.objects", 3, { kind: "shared", other: "activeRules" }],
      ["aut.legacy-process-builder", "facts.activeProcesses", 1, "rewritten"],
    ]);
  });

  test("their wording with an unprinted figure of 31 or 2025: refused naming 31 December 2025, where round 4 carried it", () => {
    for (const f of rules) {
      assert.ok(f.summary.includes("on 31 December 2025."), f.summary);
      for (const value of [31, 2025]) {
        const planted = { ...f, facts: { ...f.facts, planted: value } };
        const result = applyOverrides([planted], [], [onRun(planted, "facts.planted", value, value + 7)]);
        assert.deepEqual(result.refused.map((r) => r.why), [{ kind: "unsure", found: "31 December 2025" }], `${f.ruleId} ${value}`);
      }
    }
  });
});
