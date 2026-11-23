import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildReport } from "../../narrative/sections.ts";
import { checkNumbers, scopeDataset, withRestoredIds } from "../../numbercheck/index.ts";
import { buildMicrositePages, pageText } from "../../microsite/pages.ts";
import type { PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers } from "../../testing/replay.ts";
import type { Finding, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { fullReportHtml } from "../full-report.ts";
import { approvedReport, renderReleased, type ApprovedRun } from "../render-released.ts";
import { INTERVIEW_VERSION } from "../../interviews/questions.ts";
import { INTERVIEWS_FILE, loadRun } from "../render-run.ts";

// Decision 683, the hotfix for 23 November: the engine doesn't read an
// account's name, so the account concentration finding's token restores to the
// account's record id. Claude's draft of the current pipeline section named the
// token; its check passed, since the token is in the data, and the full report
// printed the id, which the page check then held as an id not in the data. The
// page-time set now holds each id a token of this run restores to, from the
// run's own pseudonym map, so a run already made is covered when its documents
// are made again.

const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });
const map = JSON.parse(readFileSync(join(recorded, "pseudonym-map.json"), "utf8")) as PseudonymEntry[];
const finding = (JSON.parse(readFileSync(join(recorded, "findings.json"), "utf8")) as Finding[]).find((f) => f.ruleId === "cur.account-concentration" && f.status === "issue")!;
const token = finding.subjects[0]!.label;
const account = map.find((e) => e.token === token)!;
const share = finding.facts.share!;
const DRAFT = `${token} holds ${share}% of open pipeline. If that relationship stalls, the number goes with it.`;

/**
 * A run folder as the worker left it, and every section approved, with Claude's
 * draft of the current pipeline chapter naming the top account's token. With
 * interviews, the interviews chapter names it too: the microsite prints that
 * chapter's text, and no other section's.
 */
function approvedWithDraft(interviews = false): { dir: string; approved: ApprovedRun } {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-account-id-")), "hc-2026-10-31-0000000c");
  cpSync(recorded, dir, { recursive: true });
  if (interviews) writeFileSync(join(dir, INTERVIEWS_FILE), JSON.stringify({ version: INTERVIEW_VERSION, interviews: syntheticInterviews() }));
  const drafts = new Set(["domain_current_pipeline", ...(interviews ? ["interviews"] : [])]);
  const report = buildReport(loadRun(dir).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      const v = built.versions.find((x) => x.id === s.currentVersionId)!;
      const drafted = s.output === "full_health_check" && drafts.has(s.key) ? { ...v, id: `${v.id}-claude`, version: 2, body: `${DRAFT}\n\n${v.body}`, source: "claude_draft" as const } : v;
      sections.push({ ...s, status: "approved", currentVersionId: drafted.id, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-23T09:00:00.000Z" } });
      versions.push(drafted);
    }
  }
  return { dir, approved: { intake: null, sections, versions, overrides: [] } };
}

describe("an account labelled by its id prints its id, and the documents trace it (DECISIONS.md 683)", () => {
  test("the synthetic run has the case: the top account's token restores to its own record id", () => {
    assert.match(token, /^\[\[account:A\d{4,}\]\]$/);
    assert.equal(account.name, account.id);
    assert.match(account.id, /^[A-Za-z0-9]{18}$/);
  });

  test("the draft's own check passes on the token, in the section's scope", () => {
    const report = buildReport(loadRun(recorded).input);
    const scope = report.scopes.get(`${report.input.run.id}:full_health_check:domain_current_pipeline`)!;
    assert.deepEqual(checkNumbers(DRAFT, scopeDataset(report.dataset, scope)).unmatched, []);
  });

  test("made again from the run folder: nothing held, and the full report prints the id", async () => {
    const { dir, approved } = approvedWithDraft();
    const { report, pseudonyms } = approvedReport(dir, approved);
    const html = fullReportHtml(report, pseudonyms);
    assert.ok(html.includes(`${account.id} holds ${share}% of open pipeline`), "the page prints the restored id");
    const result = await renderReleased(dir, approved, join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.equal(result.microsite?.ok, true, result.microsite && !result.microsite.ok ? result.microsite.reason : "");
  });

  test("the microsite's check passes on a page that prints the id", () => {
    const { dir, approved } = approvedWithDraft(true);
    assert.ok(approved.sections.some((s) => s.key === "interviews"), "the run has an interviews chapter");
    const { report, pseudonyms } = approvedReport(dir, approved);
    const { pages } = buildMicrositePages(report, pseudonyms);
    assert.ok(pages.some((p) => pageText(p.html).includes(account.id)), "a microsite page prints the restored id");
  });

  test("nothing else is loosened: another id-shaped word, a number, a date, an email, a link and an unknown token are still held", () => {
    const { dir, approved } = approvedWithDraft();
    const { report, pseudonyms } = approvedReport(dir, approved);
    const set = withRestoredIds(report.dataset, pseudonyms.entries());
    assert.deepEqual(checkNumbers(`${account.id} holds ${share}% of open pipeline.`, set).unmatched, []);
    // An account labelled by its id whose token the data doesn't hold, a real-looking 18-character id and a 15-character one.
    const unused = map.find((e) => e.name === e.id && !report.dataset.tokens.some((t) => t.value === e.token));
    assert.ok(unused, "the map has an account labelled by its id that no finding names");
    const planted = `${unused.id} and 001QH000026NaJLYA0 and a0B5g000001AbCd on 4,321 deals on 3 March 2019, from x@example.com, https://example.org/a and [[account:A0999]].`;
    assert.deepEqual(checkNumbers(planted, set).unmatched.map((u) => u.raw), [unused.id, "001QH000026NaJLYA0", "a0B5g000001AbCd", "4,321", "3 March 2019", "x@example.com", "https://example.org/a", "[[account:A0999]]"]);
    // Only ids labelled by themselves are added: no person's or rep's name becomes an identifier.
    const added = set.identifiers.slice(report.dataset.identifiers.length).map((i) => i.value);
    assert.deepEqual(added, map.filter((e) => e.name === e.id && report.dataset.tokens.some((t) => t.value === e.token)).map((e) => e.id.toLowerCase()));
  });

  test("a draft or an edit that writes the raw id itself is still held at draft time", () => {
    const report = buildReport(loadRun(recorded).input);
    const scope = report.scopes.get(`${report.input.run.id}:full_health_check:domain_current_pipeline`)!;
    assert.deepEqual(checkNumbers(`${account.id} holds ${share}% of open pipeline.`, scopeDataset(report.dataset, scope)).unmatched.map((u) => u.raw), [account.id]);
  });
});
