import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { buildTickets } from "../../actions/index.ts";
import { guardDigest } from "../../digest/build.ts";
import { MODELS } from "../../config.ts";
import { INTERVIEW_VERSION } from "../../interviews/questions.ts";
import { buildMicrositePages, pageText } from "../../microsite/pages.ts";
import type { NarrativeClient, NarrativeReply } from "../../narrative/client.ts";
import { draftSection, type SectionBrief } from "../../narrative/draft.ts";
import { Intake } from "../../narrative/intake.ts";
import { SYSTEM_PROMPT } from "../../narrative/prompt.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { NAMED_PEOPLE_SENTENCE } from "../../schemas/digest.ts";
import type { Finding, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { renderCaseStudyDocx } from "../case-study.ts";
import { executiveHtml } from "../executive.ts";
import { fullReportHtml } from "../full-report.ts";
import { onePagerHtml } from "../one-pager.ts";
import { renderRawNotesDocx } from "../raw-notes-docx.ts";
import { approvedReport, renderReleased, type ApprovedRun } from "../render-released.ts";
import { INTERVIEWS_FILE, loadRun } from "../render-run.ts";

// Decision 684. The engine doesn't read an account's name, so its token restores
// to the account's record id, and a customer's report read "001QH000026NaJLYA0
// holds 36.3% of open pipeline". What a customer reads now says "One account"
// there; Black Cloud's own outputs keep the id, for a consultant to look it up.

/** A Salesforce id's shape: 15 or 18 letters and digits with at least one digit, as the digest holds it. */
const SALESFORCE_ID = /\b(?=[a-zA-Z0-9]*\d)[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/g;

const { dir: recorded, output, run } = await runFromRecording({ answers: loadAnswers() });
const map = JSON.parse(readFileSync(join(recorded, "pseudonym-map.json"), "utf8")) as PseudonymEntry[];
const finding = (JSON.parse(readFileSync(join(recorded, "findings.json"), "utf8")) as Finding[]).find((f) => f.ruleId === "cur.account-concentration" && f.status === "issue")!;
const token = finding.subjects[0]!.label;
const account = map.find((e) => e.token === token)!;
const share = finding.facts.share!;
/** Another account the run labels by its id, which no finding names. */
const other = map.find((e) => e.token.startsWith("[[account:") && e.name === e.id && e.token !== token)!;

/** A run folder as the worker left it, every section approved, with Claude's current pipeline chapter (and interviews chapter) holding `draft` first. */
function approvedWith(draft: string, interviews = false): { dir: string; approved: ApprovedRun } {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-one-account-")), "hc-2026-10-31-0000000c");
  cpSync(recorded, dir, { recursive: true });
  if (interviews) writeFileSync(join(dir, INTERVIEWS_FILE), JSON.stringify({ version: INTERVIEW_VERSION, interviews: syntheticInterviews() }));
  const drafts = new Set(["domain_current_pipeline", ...(interviews ? ["interviews"] : [])]);
  const report = buildReport(loadRun(dir).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      const v = built.versions.find((x) => x.id === s.currentVersionId)!;
      const drafted = s.output === "full_health_check" && drafts.has(s.key) ? { ...v, id: `${v.id}-claude`, version: 2, body: `${draft}\n\n${v.body}`, source: "claude_draft" as const } : v;
      sections.push({ ...s, status: "approved", currentVersionId: drafted.id, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-23T09:00:00.000Z" } });
      versions.push(drafted);
    }
  }
  return { dir, approved: { intake: null, sections, versions, overrides: [] } };
}

const DRAFT = `${token} holds ${share}% of open pipeline. If that relationship stalls, the number goes with it.`;
const ids = (text: string): string[] => text.match(SALESFORCE_ID) ?? [];
const docxText = (bytes: Buffer): string => strFromU8(unzipSync(new Uint8Array(bytes))["word/document.xml"]!);

describe("an account we have no name for, as a customer reads it (DECISIONS.md 684)", () => {
  const entries: PseudonymEntry[] = [
    ...["001000000000000AAA", "001000000000000AAB", "001000000000000AAC", "001000000000000AAD"].map((id, i) => ({ token: `[[account:A000${i + 1}]]`, id, name: id, aliases: [id] })),
    { token: "[[user:U0001]]", id: "005000000000000AAA", name: "Sam Reed", aliases: ["Sam Reed"] },
  ];
  const p = new Pseudonymiser(entries);
  const say = (text: string) => p.customerWords()(text);

  test("the words take a capital where a sentence, line, bullet, heading or a line's label starts, and not mid-sentence", () => {
    assert.equal(say("[[account:A0001]] holds 36.3% of open pipeline."), "One account holds 36.3% of open pipeline.");
    assert.equal(say("Of open pipeline, [[account:A0001]] holds 36.3%."), "Of open pipeline, one account holds 36.3%.");
    assert.equal(say("Pipeline is lumpy. [[account:A0001]] holds 36.3%."), "Pipeline is lumpy. One account holds 36.3%.");
    assert.equal(say("Is it real? [[account:A0001]] holds it."), "Is it real? One account holds it.");
    assert.equal(say("First.\n[[account:A0001]] holds it."), "First.\nOne account holds it.");
    assert.equal(say("Risk: [[account:A0001]] holds 36.3%."), "Risk: One account holds 36.3%.");
    assert.equal(say("- [[account:A0001]] holds 36.3%."), "- One account holds 36.3%.");
    assert.equal(say("• [[account:A0001]] holds 36.3%."), "• One account holds 36.3%.");
    assert.equal(say("1. [[account:A0001]] holds 36.3%."), "1. One account holds 36.3%.");
    assert.equal(say("## [[account:A0001]] and the quarter"), "## One account and the quarter");
    assert.equal(say("Large deals, e.g. [[account:A0001]], move it."), "Large deals, e.g. one account, move it.");
    assert.equal(say('He said "Fine." [[account:A0001]] holds it.'), 'He said "Fine." One account holds it.');
  });

  test("two different accounts never read as one: one, another, a third, then another, the same account the same each time", () => {
    const words = p.customerWords();
    assert.equal(words("With [[account:A0001]] and [[account:A0002]], most of it is two deals."), "With one account and another account, most of it is two deals.");
    assert.equal(words("[[account:A0003]] follows, then [[account:A0004]]."), "A third account follows, then another account.");
    assert.equal(words("[[account:A0002]] is the newer, and [[account:A0001]] the older."), "Another account is the newer, and one account the older.");
    // A new document counts again.
    assert.equal(p.customerWords()("[[account:A0002]] holds it."), "One account holds it.");
  });

  test("possessives, plurals and the account's own noun read as English", () => {
    assert.equal(say("[[account:A0001]]'s share is 36.3%."), "One account's share is 36.3%.");
    assert.equal(say("The account [[account:A0001]] holds 36.3%."), "One account holds 36.3%.");
    assert.equal(say("Most of it sits with the account [[account:A0001]]."), "Most of it sits with one account.");
    assert.equal(say("Your account [[account:A0001]] holds it."), "One account holds it.");
    assert.equal(say("Accounts [[account:A0001]] and [[account:A0002]] hold it."), "One account and another account hold it.");
    assert.equal(say("It rests on [[account:A0001]]'s and [[account:A0002]]'s renewals."), "It rests on one account's and another account's renewals.");
  });

  test("a person's token restores to their name, an unknown token stays, and restore() itself is unchanged", () => {
    assert.equal(say("[[user:U0001]] owns [[account:A0001]] and [[account:A0999]]."), "Sam Reed owns one account and [[account:A0999]].");
    assert.equal(p.restore("[[account:A0001]] holds it."), "001000000000000AAA holds it.", "restore() itself still gives the id");
  });

  test("only accounts are labelled by their id today: a user with no name is 'Unknown user', and the customer by its reference", () => {
    const labelled = map.filter((e) => e.name === e.id);
    assert.ok(labelled.length > 0);
    assert.deepEqual([...new Set(labelled.map((e) => e.token.slice(2, e.token.indexOf(":"))))], ["account"]);
  });
});

describe("the customer's documents print plain words, Black Cloud's keep the id (DECISIONS.md 684)", () => {
  test("the synthetic run has the case: the top account's token restores to its own record id", () => {
    assert.match(token, /^\[\[account:A\d{4,}\]\]$/);
    assert.equal(account.name, account.id);
    assert.ok(other);
  });

  test("made again from the run folder through renderReleased: nothing held, and no customer page holds an id", async () => {
    const { dir, approved } = approvedWith(DRAFT, true);
    const result = await renderReleased(dir, approved, join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.deepEqual(result.warnings, []);
    assert.equal(result.microsite?.ok, true, result.microsite && !result.microsite.ok ? result.microsite.reason : "");
    // The pages the PDFs and the microsite are printed from, as a reader sees them.
    const { report, pseudonyms } = approvedReport(dir, approved);
    const full = pageText(fullReportHtml(report, pseudonyms));
    assert.ok(full.includes(`One account holds ${share}% of open pipeline`), "the full report says one account");
    const microsite = result.microsite?.ok ? result.microsite.pages.pages.map((pg) => pageText(pg.html)) : [];
    assert.ok(microsite.some((text) => text.includes(`One account holds ${share}%`)), "the microsite says one account");
    const pages = [full, pageText(executiveHtml(report, pseudonyms)), pageText(onePagerHtml(report, pseudonyms)), ...microsite];
    for (const text of pages) {
      assert.deepEqual(ids(text), [], "no Salesforce-id-shaped word on a customer page");
      assert.ok(!text.includes(account.id));
    }
    // Black Cloud's own: the raw notes and the tickets keep the id, for the consultant to look the account up.
    assert.ok(docxText(readFileSync(result.files.raw_notes)).includes(account.id), "the raw notes keep the id");
    assert.ok(readFileSync(result.files.tickets, "utf8").includes(account.id), "the tickets keep the id");
  });

  test("the page check passes on plain words, which hold no figure and no identifier", () => {
    const { dir, approved } = approvedWith(DRAFT);
    const { report } = approvedReport(dir, approved);
    assert.deepEqual(checkNumbers(`One account holds ${share}% of open pipeline, and another account a third account.`, report.dataset).unmatched, []);
  });

  test("two different accounts in one section read as one account and another account", () => {
    const { dir, approved } = approvedWith(`${DRAFT} Behind ${token} sits ${other.token}, and ${token} renews first.`);
    const { report, pseudonyms } = approvedReport(dir, approved);
    const full = pageText(fullReportHtml(report, pseudonyms));
    assert.ok(full.includes("Behind one account sits another account, and one account renews first."));
    assert.deepEqual(ids(full), []);
    // Black Cloud's own tickets still name the id.
    assert.ok(buildTickets(report, pseudonyms, null).includes(account.id));
  });

  test("the raw notes print the id where a customer's page prints plain words", async () => {
    const { dir, approved } = approvedWith(DRAFT);
    const { report, pseudonyms } = approvedReport(dir, approved);
    const xml = docxText(await renderRawNotesDocx(report, pseudonyms));
    assert.ok(xml.includes(account.id));
  });

  // The digest never restores a token: a sentence holding one is the fixed sentence, so it never held the id either.
  // Plain words there would change what digest-privacy.test.ts holds for an account's token, a privacy test this lane leaves alone.
  test("the digest holds neither the id nor the token: a sentence naming the account is the fixed sentence, as before", () => {
    const pseudonyms = new Pseudonymiser(map);
    const guarded = guardDigest({ summary: `${token} holds ${share}% of open pipeline.`, title: `${account.id} holds it` }, pseudonyms);
    assert.deepEqual(guarded, { summary: NAMED_PEOPLE_SENTENCE, title: NAMED_PEOPLE_SENTENCE });
  });

  test("the case study draft, the customer's to approve, prints plain words", async () => {
    const intake = Intake.parse({ customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [{ id: "p1", statement: `${token} holds most of our pipeline.`, findingIds: [] }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", caseStudyConsent: true });
    const first: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
    const fixed = [...new Set(first.findings.filter((f) => f.status === "issue").map((f) => f.ruleId))].slice(0, 3);
    const later = first.findings.map((f): Finding => (fixed.includes(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
    const report = buildReport({ ...first, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), intake, baseline: first });
    const xml = docxText((await renderCaseStudyDocx(report, new Pseudonymiser(map)))!);
    assert.ok(xml.includes("One account holds most of our pipeline."));
    assert.ok(!xml.includes(account.id));
  });
});

describe("a new draft writes one account, never the token (DECISIONS.md 684)", () => {
  const report = buildReport(loadRun(recorded).input);
  const pseudonyms = new Pseudonymiser(map);
  const brief: SectionBrief = { title: "Current pipeline", purpose: "Say what the open pipeline shows.", sources: [{ id: finding.id, kind: "finding" }], maxWords: 200 };
  const dataset = scopeDataset(report.dataset, [finding.id]);
  const reply = (text: string): NarrativeReply => ({ stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text, sources: [finding.id] }] }), model: MODELS.synthesis });
  const draft = (text: string) => draftSection({ brief, dataset, client: { async complete() { return reply(text); } } satisfies NarrativeClient, pseudonyms });

  test("the prompt says an account has no name and is written as one account", () => {
    assert.match(SYSTEM_PROMPT, /an account has no name/);
    assert.match(SYSTEM_PROMPT, /Write "one account", and "another account" for a second/);
    assert.match(SYSTEM_PROMPT, /Keep a person's token exactly as written/);
  });

  test("a draft naming an id-labelled account's token is sent back, and one saying one account is accepted", async () => {
    const named = await draft(`${token} holds ${share}% of open pipeline.`);
    assert.equal(named.status, "rejected");
    assert.ok(named.status === "rejected" && named.reasons.some((r) => r.includes('write "one account"')), JSON.stringify(named));
    const plain = await draft(`One account holds ${share}% of open pipeline.`);
    assert.equal(plain.status, "accepted", JSON.stringify(plain));
  });

  test("a person's token is handled as before: only a roles-only section refuses it", async () => {
    const user = map.find((e) => e.token.startsWith("[[user:") && e.name !== "Unknown user")!;
    const p = new Pseudonymiser([...map]);
    const run = (rolesOnly: boolean) =>
      draftSection({ brief: { ...brief, rolesOnly }, dataset: { ...dataset, tokens: [...dataset.tokens, { source: finding.id, value: user.token }] }, client: { async complete() { return reply(`${user.token} owns the pipeline.`); } } satisfies NarrativeClient, pseudonyms: p });
    const free = await run(false);
    assert.ok(free.status === "accepted" || !free.reasons.some((r) => /one account|by their role/.test(r)), JSON.stringify(free));
    const roles = await run(true);
    assert.ok(roles.status === "rejected" && roles.reasons.some((r) => /by their role/.test(r)) && !roles.reasons.some((r) => /one account/.test(r)));
  });
});
