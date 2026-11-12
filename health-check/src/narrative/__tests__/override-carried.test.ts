import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { applyOverrides } from "../overrides.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { printDocuments, OUTPUT_FILES } from "../../render/render-run.ts";
import { heldFigures } from "../../render/render-released.ts";
import type { Finding, NumberOverride } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";

// DECISIONS.md 514: the number check is told what was carried, not the rows,
// so a figure put back clears; and a date, an identifier and a clock time are
// not figures.

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
  test("the hour of 09:30, and of 2026-10-04T09:30", () => {
    untouched("Runs at 09:30 each day.", 9);
    untouched("Last ran 2026-10-04T09:30.", 9);
  });

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
