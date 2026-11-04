import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import type { NumberOverride } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { plainWords, ukDates } from "../format.ts";
import { renderRawNotesDocx } from "../raw-notes-docx.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

const decode = (value: string): string => value.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&apos;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, "&");

async function render(input: ReportInput = base): Promise<{ bytes: Buffer; xml: string; text: string }> {
  const bytes = await renderRawNotesDocx(buildReport(input), pseudonyms);
  const xml = strFromU8(unzipSync(new Uint8Array(bytes))["word/document.xml"]!);
  const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => decode(m[1]!)).join(" ");
  return { bytes, xml, text };
}

const notes = await render();

describe("the Word raw notes", () => {
  test("are a real Word file", () => {
    assert.equal(notes.bytes.subarray(0, 2).toString(), "PK");
    const files = Object.keys(unzipSync(new Uint8Array(notes.bytes)));
    assert.ok(files.includes("word/document.xml"));
    assert.ok(files.includes("[Content_Types].xml"));
  });

  test("hold each part the brief asks for", () => {
    for (const heading of ["Intake summary", "Assumptions", "Manual steps", "Pipeline metrics", "Queries", "Exclusions and limits", "Overrides"]) assert.ok(notes.text.includes(heading), heading);
  });

  test("hold every finding, in full", () => {
    for (const f of base.findings) {
      assert.ok(notes.text.includes(f.id), f.id);
      assert.ok(notes.text.includes(plainWords(ukDates(pseudonyms.restore(f.title)))), f.title);
      assert.ok(notes.text.includes(plainWords(ukDates(pseudonyms.restore(f.summary)))), f.summary);
    }
  });

  test("hold the evidence: queries, counts, periods and exclusions", () => {
    assert.match(notes.text, /Query: SELECT/);
    assert.match(notes.text, /Count: \d+/);
    assert.match(notes.text, /Period: \d{1,2} [A-Z][a-z]{2} \d{4} to/);
    assert.match(notes.text, /Excludes: /);
    const queries = base.findings.flatMap((f) => f.evidence).flatMap((e) => (e.kind === "soql" ? [e.query] : []));
    for (const query of queries) assert.ok(notes.text.includes(query), query);
  });

  test("hold guards and metric tables", () => {
    assert.match(notes.text, /owns-no-records/);
    assert.match(notes.text, /Win rate: 47%/);
    assert.match(notes.text, /Month end/);
    for (const m of base.metrics) assert.ok(notes.text.includes(m.label), m.id);
  });

  test("hold the object inventory, every object in its own row", () => {
    assert.match(notes.text, /In short: +Object inventory: 7 objects counted/);
    for (const object of ["Opportunity", "Invoice__c", "Lead", "Account", "Project__c", "Contact", "Project_Line__c"]) assert.ok(notes.text.includes(object), object);
    assert.match(notes.text, /Not read: +Opportunity, records: Salesforce keeps no count for this object/);
  });

  test("say what couldn't be measured and why", () => {
    const gap = base.findings.find((f) => f.status === "not_measurable")!;
    assert.ok(notes.text.includes(ukDates(gap.notMeasurableReason!)));
  });

  test("a reader gets UK dates, while queries and the run id stay exact", () => {
    const withoutQueries = notes.text.replace(/Query: .*?(?=Count: |$)/g, "").replace(/Tooling query: .*?(?=Count: |$)/g, "");
    const rest = withoutQueries.replace(/SELECT [^|]*/g, "").replaceAll(run.id, "");
    assert.doesNotMatch(rest.replace(/Query: \S*/g, ""), /(?<![\w:-])\d{4}-\d{2}-\d{2}(?![\w-])/);
    assert.ok(notes.text.includes(run.id));
    assert.match(notes.text, /CreatedDate >= 20\d{2}-\d{2}-\d{2}/);
  });

  test("names come back here, and no token survives", () => {
    assert.match(notes.text, /Sam Reed/);
    assert.doesNotMatch(notes.xml, /\[\[user:/);
  });

  test("an override appears with its reason", async () => {
    const f = base.findings.find((x) => typeof x.facts.rate === "number")!;
    const override: NumberOverride = { id: "o1", runId: run.id, target: { type: "finding", id: f.id, field: "facts.rate" }, previousValue: f.facts.rate!, newValue: 12.5, reason: "Two deals were test records.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-10-31T13:00:00.000Z" };
    const { text } = await render({ ...base, overrides: [override] });
    assert.match(text, /Two deals were test records\./);
    assert.match(text, /12\.5/);
    assert.match(text, /31 Oct 2026/);
  });

  test("say no figures were overridden when none were", () => {
    assert.match(notes.text, /No figures were overridden/);
  });

  test("an unapproved document says it can't be released", () => {
    assert.match(notes.text, /Draft: not every section has been approved/);
  });

  test("an intake appears when there is one", async () => {
    const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast is unreliable.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
    assert.match((await render({ ...base, intake })).text, /forecast is unreliable/);
    assert.match(notes.text, /No intake has been recorded/);
  });

  test("use the brand font and colours", () => {
    assert.match(notes.xml, /w:ascii="Poppins"/);
    assert.match(notes.xml, /w:fill="21221C"/);
  });
});
