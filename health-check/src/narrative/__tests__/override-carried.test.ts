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

describe("a date is not a figure (decision 514, item 2)", () => {
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
