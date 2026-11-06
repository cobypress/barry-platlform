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

describe("references between the decks and the raw notes", async () => {
  const { executiveHtml } = await import("../executive.ts");
  const { fullReportHtml } = await import("../full-report.ts");
  const { noteSections, referenceMap } = await import("../references.ts");
  const report = buildReport(base);
  const sections = noteSections(report);
  const refs = referenceMap(sections);
  const tags = (html: string): string[] => [...html.matchAll(/<span class="ref" data-ref>(R\d+\.\d+)<\/span>/g)].map((m) => m[1]!);

  test("number every section and item of the notes, in the order they're printed", () => {
    sections.forEach((s) => assert.ok(notes.text.includes(`${s.ref} ${plainWords(s.title)}`), s.ref));
    assert.equal(sections[0]!.ref, "R1");
    for (const f of base.findings) assert.ok(notes.text.includes(`${refs.get(f.id)} ${plainWords(ukDates(pseudonyms.restore(f.title)))}`), f.id);
    for (const m of base.metrics) assert.ok(notes.text.includes(`${refs.get(m.id)} ${m.label}`), m.id);
  });

  test("every reference a deck prints leads to an item in the notes", () => {
    const printed = [...tags(executiveHtml(report, pseudonyms)), ...tags(fullReportHtml(report, pseudonyms))];
    assert.ok(printed.length > 20);
    const known = new Set(refs.values());
    for (const ref of printed) assert.ok(known.has(ref), ref);
  });

  test("the executive summary lists the references it used, and only those", () => {
    const html = executiveHtml(report, pseudonyms);
    const page = html.slice(html.indexOf("Where to find the detail"));
    const listed = [...page.matchAll(/<b data-ref>(R\d+\.\d+)<\/b>/g)].map((m) => m[1]!);
    assert.deepEqual(new Set(listed), new Set(tags(html.slice(0, html.indexOf("Where to find the detail")))));
  });
});

describe("the Barometer score in the documents", async () => {
  const { scoreDomains } = await import("../../domains/scoring.ts");
  const { executiveHtml } = await import("../executive.ts");
  const { fullReportHtml } = await import("../full-report.ts");
  const scored = buildReport({ ...base, scores: scoreDomains(run.modules, base.findings) });

  test("the covers, verdict and scorecard show it, and the raw notes show its working", async () => {
    assert.ok(scored.overall.status === "measured");
    const { score, grade } = scored.overall;
    for (const html of [executiveHtml(scored, pseudonyms), fullReportHtml(scored, pseudonyms)]) {
      assert.match(html, new RegExp(`<div class="cover-score">[\\s\\S]*?<b>${score}</b>`));
      assert.ok(html.includes(`Barometer score of ${score} out of 100, grade ${grade}`) || html.includes(`Barometer score is ${score} out of 100, grade ${grade}`));
    }
    const bytes = await renderRawNotesDocx(scored, pseudonyms);
    const xml = strFromU8(unzipSync(new Uint8Array(bytes))["word/document.xml"]!);
    assert.match(xml, new RegExp(`Your Barometer score is ${score} out of 100`));
    assert.match(xml, /weighted 1\.5/);
  });

  test("an org with no scores gets no score anywhere, rather than a zero", () => {
    const report = buildReport(base);
    assert.equal(report.overall.status, "not_measurable");
    assert.doesNotMatch(fullReportHtml(report, pseudonyms), /<div class="cover-score">|Barometer score is/);
  });
});

describe("how you compare, in the full report", async () => {
  const { fullReportHtml } = await import("../full-report.ts");

  test("has its own slide and a place in the contents when a figure has something to stand against", () => {
    const report = buildReport(base);
    const comparable = report.comparisons.some((c) => c.value !== null && c.published.length > 0);
    assert.ok(comparable, "the synthetic org has a figure with published research");
    const html = fullReportHtml(report, pseudonyms);
    assert.match(html, /<h2>How you compare<\/h2>/);
    assert.match(html, /<li>How you compare<span class="pg" data-ref>\d+<\/span><\/li>/);
  });
});
