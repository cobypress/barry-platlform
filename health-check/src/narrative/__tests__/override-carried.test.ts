import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { applyOverrides, refusalReason } from "../overrides.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { printDocuments, OUTPUT_FILES } from "../../render/render-run.ts";
import { heldFigures } from "../../render/render-released.ts";
import { buildDataset, extractFigures, type FigureSet } from "../../numbercheck/index.ts";
import type { Finding, NumberOverride } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";

// DECISIONS.md 514: the number check is told what was carried, not the rows,
// so a figure put back clears; and a date, an identifier and a clock time are
// not figures.
// DECISIONS.md 515: digits joined by a colon are never rewritten and never
// passed over.

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
  test("the day and the year of 31 December 2025", () => {
    untouched("Closed by 31 December 2025.", 31);
    untouched("Closed by 31 December 2025.", 2025);
  });
  test("the year of December 2025", () => untouched("Closed in December 2025.", 2025));
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

describe("digits joined by a colon are never rewritten and never passed over (decision 515, item 1)", () => {
  /** An unprinted fact refused as the colon form `found`, the finding as read, and the reason's line naming the form whole. */
  function refusedAs(summary: string, value: number, found: string, last = value + 1): void {
    const f = finding(summary, { x: value });
    const result = applyOverrides([f], [], [override("facts.x", value, last)]);
    assert.deepEqual(result.refused.map((r) => r.why), [{ kind: "rounded", found }], `${summary} ${value}`);
    assert.deepEqual(result.findings[0], f, `${summary} ${value}`);
    assert.equal(refusalReason(result.refused).split("\n")[1], `"A planted finding", x: ${value.toLocaleString("en-GB")} as read, corrected to ${last.toLocaleString("en-GB")}, but the wording doesn't print ${value.toLocaleString("en-GB")} itself and has ${found}, which may be this figure rounded.`);
  }

  test("a fact of 10, unprinted, beside at 10:00, which the number check's reader calls malformed: refused, the line naming 10:00", () => {
    refusedAs("The job runs at 10:00 each day.", 10, "10:00");
  });

  test("09:30 with a fact of 9 and of 30: refused naming 09:30. A part with a leading zero is compared as its value, 09 as 9, and by the 5% test, so 9.2 is refused too", () => {
    refusedAs("The job runs at 09:30 each day.", 9, "09:30");
    refusedAs("The job runs at 09:30 each day.", 30, "09:30");
    refusedAs("The job runs at 09:30 each day.", 9.2, "09:30");
  });

  test("09:30 and 10:00 with a fact of 7, near neither part: carried, the text as read", () => {
    untouched("The job runs at 09:30 each day.", 7);
    untouched("The job runs at 10:00 each day.", 7);
  });

  test("a fact of 3, or of 1, printed only as 1:3: refused naming 1:3", () => {
    refusedAs("Admins to users are 1:3.", 3, "1:3");
    refusedAs("Admins to users are 1:3.", 1, "1:3");
  });

  test("511's limit, a second figure worked out from the first: a fact of 3 printed once as 3 accounts beside 1:3 is rewritten at the figure, never in the ratio, and the ratio stays as read", () => {
    // Found once as a figure, so carried by 511 (3) as any figure beside a number worked out from it is; the colon part is never a place.
    assert.equal(carried("3 accounts, admins to users 1:3.", { n: 3 }, "n", 4), "4 accounts, admins to users 1:3.");
  });

  test("2026-10-04T09:30 with a fact of 9 or of 30: carried, the text as read, since the time is the date's", () => {
    untouched("Last ran 2026-10-04T09:30.", 9);
    untouched("Last ran 2026-10-04T09:30.", 30);
  });

  test("10:30:15 with a fact of 15, or of 10: refused naming the whole form", () => {
    refusedAs("Logged at 10:30:15.", 15, "10:30:15");
    refusedAs("Logged at 10:30:15.", 10, "10:30:15");
  });

  test("a part with decimals or groups is the form's, so no digit of it is left to be found as a figure", () => {
    refusedAs("Admins to users are 1.5:1.", 1, "1.5:1");
    refusedAs("Admins to users are 1.5:1.", 1.5, "1.5:1");
    refusedAs("Admins to users are 1,000:1.", 1000, "1,000:1");
    refusedAs("Admins to users are 1,000:1.", 1, "1,000:1");
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
/** What the number check's data was given. */
const given = (...overrides: NumberOverride[]): NumberOverride[] => applyOverrides(input.findings, input.metrics, overrides).carried;

describe("the rows the number check's data is given (decision 514, item 1)", () => {
  test("a figure put back: the raw notes still list every row, in order", () => {
    const rows = [onRun(divergence, "facts.deals", 43, 33), onRun(divergence, "facts.deals", 33, 43), onRun(divergence, "facts.gap", 33, 34), onRun(divergence, "facts.gap", 34, 33)];
    assert.deepEqual(given(...rows), []);
    assert.deepEqual(rawNotesRows(built(...rows)), rows.map(rowLine));
  });

  test("a refused figure beside a carried one in another finding: a row for the carried one only, from the value as read", () => {
    const storage = output!.findings.find((f) => f.ruleId === "tech.storage-use" && f.status === "issue")!;
    const coverage = output!.findings.find((f) => f.ruleId === "cur.coverage" && f.status === "issue")!;
    const remaining = coverage.facts.remaining!;
    const rows = [onRun(coverage, "facts.remaining", remaining, remaining - 100000), onRun(storage, "facts.DataStorageMBUsed", 8940, 8500), onRun(storage, "facts.DataStorageMBUsed", 8500, 8200)];
    assert.deepEqual(given(...rows), [{ ...rows[2]!, previousValue: 8940 }]);
    const report = built(...rows);
    assert.deepEqual(report.refused.map((r) => r.findingId), [coverage.id]);
    const of = (set: FigureSet, id: string): number[] => set.figures.filter((f) => f.source === id).map((f) => f.value).sort((a, b) => a - b);
    assert.deepEqual(of(report.dataset, coverage.id), of(asBuilt.dataset, coverage.id), "the refused finding's figures as read");
    assert.ok(of(report.dataset, storage.id).includes(8200) && !of(report.dataset, storage.id).includes(8940) && !of(report.dataset, storage.id).includes(8500));
  });

  test("a stale row, then a row back to the value as read: no row given, and the documents are made", async () => {
    const storage = output!.findings.find((f) => f.ruleId === "tech.storage-use" && f.status === "issue")!;
    const rows = [onRun(storage, "facts.DataStorageMBUsed", 11000, 9800), onRun(storage, "facts.DataStorageMBUsed", 9800, 8940)];
    assert.deepEqual(given(...rows), []);
    const report = built(...rows);
    assert.deepEqual(report.refused, []);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(await printed(report), []);
  });
});

describe("a metric's rows, collapsed field by field (decision 514, item 1)", () => {
  // The win rate of 47 is printed as a number and a percentage; 45 is another figure of the same metric.
  const onMetric = (field: string, previousValue: number, newValue: number): NumberOverride => ({ ...onRun(divergence, field, previousValue, newValue), target: { type: "metric", id: "historic.win_rate", field } });
  const of = (set: FigureSet): string[] => set.figures.filter((f) => f.source === "historic.win_rate").map((f) => `${f.kind} ${f.value}`).sort();

  test("the run prints the figures these tests use", () => {
    assert.ok(of(asBuilt.dataset).includes("number 45") && of(asBuilt.dataset).includes("number 47") && of(asBuilt.dataset).includes("percent 47"));
  });

  test("A to B to A: no row, and the report is the report of no override. The rows given in turn dropped the metric's own 45", () => {
    const rows = [onMetric("value", 47, 45), onMetric("value", 45, 47)];
    assert.deepEqual(given(...rows), []);
    const report = built(...rows);
    assert.deepEqual(report.metrics, asBuilt.metrics);
    assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset));
    assert.deepEqual(rawNotesRows(report), rows.map(rowLine));
  });

  test("A to B to C: one row, A to C, with the last row's other fields. The rows given in turn dropped the metric's own 45 too", () => {
    const rows = [onMetric("value", 47, 45), onMetric("value", 45, 52)];
    assert.deepEqual(given(...rows), [{ ...rows[1]!, previousValue: 47 }]);
    const report = built(...rows);
    assert.equal((report.metrics.find((m) => m.id === "historic.win_rate") as { value: number }).value, 52);
    assert.deepEqual(of(report.dataset), of(asBuilt.dataset).map((f) => f.replace(/ 47$/, " 52")).sort());
    assert.deepEqual(rawNotesRows(report), rows.map(rowLine));
  });

  test("a metric the run doesn't hold, or a field it doesn't have, gives no row", () => {
    assert.deepEqual(given({ ...onMetric("value", 47, 45), target: { type: "metric", id: "historic.nothing", field: "value" } }), []);
    assert.deepEqual(given(onMetric("label", 47, 45)), []);
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
      assert.deepEqual(given(...rows), [], `round ${round}`);
      assert.deepEqual(sansOverrides(report.dataset), sansOverrides(asBuilt.dataset), `round ${round}`);
      assert.deepEqual(report.teamFindings, asBuilt.teamFindings, `round ${round}`);
      assert.deepEqual(report.metrics, asBuilt.metrics, `round ${round}`);
      assert.deepEqual(report.value, asBuilt.value, `round ${round}`);
    }
    assert.equal(figures.length, 343, "319 facts and 24 sized amounts");
    assert.equal(pairs, 1414 + 343, "each figure against every other number its finding prints, and plus 7");
  });
});

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
    test(`${byId(id).ruleId} ${field} to ${x} and back: the rows given in turn drop a figure; given what was carried, nothing is held`, async () => {
      const f = byId(id);
      const value = f.facts[field.slice("facts.".length)]!;
      const rows = [onRun(f, field, value, x), onRun(f, field, x, value)];
      // The old path, on this finding alone: the number check's data given the rows themselves.
      const figuresOf = (set: FigureSet): string[] => set.figures.map((g) => `${g.kind} ${g.value} ${g.currency ?? ""}`).sort();
      assert.notDeepEqual(figuresOf(buildDataset({ findings: [f], metrics: [], overrides: rows })), figuresOf(buildDataset({ findings: [f], metrics: [] })));
      const report = built(...rows);
      assert.deepEqual(given(...rows), []);
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
