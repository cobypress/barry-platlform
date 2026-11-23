import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildReport } from "../../narrative/sections.ts";
import { checkNumbers, scopeDataset } from "../index.ts";
import { loadRun } from "../../render/render-run.ts";
import { runFromRecording } from "../../testing/harness.ts";
import type { Finding } from "../../schemas/index.ts";

// Decision 682, the hotfix for 23 November: a page layout named with a
// Salesforce-id-shaped word ("… Id 04a000000000001") was printed, its word read
// as an identifier, and the dataset held only the label whole, so the documents
// failed. Every letters-and-digits word in an org-chosen label is now in the
// data where the dataset is built, so a run already made is covered when its
// documents are made again from its stored findings.

const LABELS = ["Old Opportunity Layout: Sales Id 04a000000000001", "Partner Lead Layout v2", "Web2Lead Layout"];

const { dir: recorded } = await runFromRecording();

/** The recorded run's stored findings, as a run made before this change left them, with the layouts renamed. */
function storedRun(): string {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-labels-")), "hc-2026-10-31-0000000b");
  cpSync(recorded, dir, { recursive: true });
  const path = join(dir, "findings.json");
  const findings = JSON.parse(readFileSync(path, "utf8")) as Finding[];
  const layouts = findings.find((f) => f.ruleId === "cfg.unassigned-layouts" && f.status === "issue");
  assert.ok(layouts && layouts.subjects.length >= 2, "the synthetic org has unassigned layouts");
  layouts.subjects = layouts.subjects.map((s, i) => ({ ...s, label: LABELS[i] ?? s.label })).concat(layouts.subjects.length < 3 ? [{ type: "layout", label: LABELS[2]! }] : []);
  writeFileSync(path, JSON.stringify(findings));
  return dir;
}

describe("letters-and-digits words in an org-chosen label are in the data", () => {
  const dir = storedRun();
  const report = buildReport(loadRun(dir).input);

  test("an id-shaped word, a version and a mixed word from layout names trace wherever a page prints them, whole or in part", () => {
    const page = "Layouts nobody sees | Old Opportunity Layout: Sales | Id 04a000000000001 | Partner Lead Layout | v2 | Web2Lead Layout";
    assert.deepEqual(checkNumbers(page, report.dataset).unmatched, []);
    // Whole, as the label masks it.
    assert.deepEqual(checkNumbers(LABELS.join(", "), report.dataset).unmatched, []);
  });

  test("nothing else is loosened: another id-shaped word, a number, a date, an email, a link and a name token are still held", () => {
    const planted = "Id 04a000000000009 and Web3Lead on 4,321 deals on 3 March 2019, from x@example.com, https://example.org/a and [[user:U0999]].";
    assert.deepEqual(checkNumbers(planted, report.dataset).unmatched.map((u) => u.raw), ["04a000000000009", "Web3Lead", "4,321", "3 March 2019", "x@example.com", "https://example.org/a", "[[user:U0999]]"]);
  });

  test("a section's draft is still checked against its own sources: the layout's id word is held in a section the finding isn't in", () => {
    const pipeline = report.scopes.get(`${report.input.run.id}:full_health_check:domain_current_pipeline`)!;
    assert.deepEqual(checkNumbers("Id 04a000000000001 needs a look.", scopeDataset(report.dataset, pipeline)).unmatched.map((u) => u.raw), ["04a000000000001"]);
  });
});
