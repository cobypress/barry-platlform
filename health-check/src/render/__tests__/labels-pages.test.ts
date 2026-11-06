import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { fullReportHtml } from "../full-report.ts";

// What a customer reads names objects, fields and record types by their labels,
// on the documented org's pages and the object inventory too (DECISIONS.md 411, 412).

const { output, run, dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const report = buildReport(input);
const text = (html: string): string => html.replace(/<style[\s\S]*?<\/style>/g, " ").replace(/data:[^"')]+/g, " ").replace(/<[^>]+>/g, " ");
const API_NAME = /\b[A-Za-z][A-Za-z0-9]*__[A-Za-z0-9_]+\b|\b(?:Lead|Opportunity|Account|Contact|Case|User)\.[A-Z][A-Za-z0-9_]+\b/g;
// Their labels need reads of their own, which come next.
const LATER = /\bSBQQ__Discount__c\b/;

const pages = { full: text(fullReportHtml(report, pseudonyms)), microsite: text(JSON.stringify(buildMicrositePages(report, pseudonyms)).replace(/\\"/g, '"')) };

test("no page a customer reads names an object or field by its API name", () => {
  for (const [name, t] of Object.entries(pages)) {
    const found = [...new Set([...t.matchAll(API_NAME)].map((m) => m[0]).filter((m) => !LATER.test(m)))];
    assert.deepEqual(found, [], name);
  }
});

test("no page names an installed app by its namespace (DECISIONS.md 413)", () => {
  for (const [name, t] of Object.entries(pages)) assert.doesNotMatch(t, /\bacmecpq\b/, name);
  assert.match(pages.microsite, /CPQ User \(Acme CPQ\)/);
});

test("objects, relationship fields and record types read as the org labels them", () => {
  assert.match(pages.microsite, /\bInvoice\b[^]*?\bAccount\b[^]*?\bLookup\b/);
  assert.match(pages.microsite, /Credit Note/);
  assert.doesNotMatch(pages.microsite, /Credit_Note/);
  assert.match(pages.full, /Project Line/);
});
