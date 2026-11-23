import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { buildDataset, checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { ENGINE_WORDS, CODE_WORDS } from "../../numbercheck/engine-words.ts";
import { scanEngineWords } from "../../numbercheck/engine-words-scan.ts";
import type { PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { asPrinted, heldFigures, LEFT_OUT_SENTENCE, leaveOut, renderReleased, repairWarnings, type ApprovedRun } from "../render-released.ts";
import { loadRun, OUTPUT_FILES, pageDataset, printDocuments } from "../render-run.ts";
import type { Metric, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { heldBackReason } from "../../worker/documents-job.ts";

// Decision 682: a figure the number check can't trace on a printed page is
// repaired, never handed to a person without saying where, and never printed.

const ACCOUNT = "[[account:A0011]]";
/** An account named with a letter and a digit, as real ones are ("Unit4", "O2", "3M"). */
const NAMED = "Unit4 Ltd";
const SRC = join(import.meta.dirname, "..", "..");

const { dir: recorded } = await runFromRecording();

/** The recorded run, copied, with the account behind A0011 renamed to one with a digit. */
function folder(): string {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-repair-")), "hc-2026-10-31-0000000a");
  cpSync(recorded, dir, { recursive: true });
  const path = join(dir, "pseudonym-map.json");
  const map = JSON.parse(readFileSync(path, "utf8")) as PseudonymEntry[];
  const entry = map.find((e) => e.token === ACCOUNT);
  assert.ok(entry, "the synthetic org has the account the current pipeline names");
  entry.name = NAMED;
  writeFileSync(path, JSON.stringify(map));
  return dir;
}

/** Every section approved, with these bodies in place of the engine's, by output and key. */
function approved(dir: string, changes: Record<string, { body: string; source: SectionVersion["source"] }>): ApprovedRun {
  const report = buildReport(loadRun(dir).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      const v = built.versions.find((x) => x.id === s.currentVersionId)!;
      const change = changes[`${built.output}:${s.key}`];
      const current = change ? { ...v, id: `${v.id}-2`, version: 2, body: change.body, source: change.source, changedBy: change.source === "consultant_edit" ? "consultant@example.com" : "claude" } : v;
      sections.push({ ...s, status: "approved", currentVersionId: current.id, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } });
      versions.push(current);
    }
  }
  return { intake: null, sections, versions, overrides: [] };
}

const numberProblems = (dir: string): unknown[] => {
  const layout = JSON.parse(readFileSync(join(dir, "documents", OUTPUT_FILES.layout), "utf8")) as Record<string, { problems: Array<{ check: string }> }>;
  return Object.values(layout).flatMap((l) => l.problems.filter((p) => p.check === "number check"));
};

const PIPELINE = "full_health_check:domain_current_pipeline";
const draftOf = (report: BuiltReport, key: string): string[] => report.scopes.get(`${report.input.run.id}:${key}`) ?? [];

describe("the cause of 23 November: a name token passed when drafted and its name was held on the page", () => {
  test("Claude's draft names an account by its token; the token traces, the name it restores to didn't, and now does", () => {
    const { input, pseudonyms } = loadRun(folder());
    const report = buildReport(input);
    const body = `Most of the open value sits with ${ACCOUNT}.`;
    // As drafted: the token is in the section's scope, so the draft check passed.
    assert.equal(checkNumbers(body, scopeDataset(report.dataset, draftOf(report, PIPELINE))).passed, true);
    // As printed, against the set the pages were checked with until now: held, as on page 53.
    const printed = asPrinted(body, pseudonyms);
    assert.ok(printed.includes(NAMED));
    assert.deepEqual(checkNumbers(printed, report.dataset).unmatched, [{ raw: "Unit4", reason: "this id isn't in the data" }]);
    // The set the pages are checked with now holds the name its token stands for.
    assert.equal(checkNumbers(printed, pageDataset(report.dataset, pseudonyms)).passed, true);
  });

  test("a name only masks itself: an invented word or number beside it is still held, and a bare token code is an id not in the data", () => {
    const { input, pseudonyms } = loadRun(folder());
    const report = buildReport(input);
    const set = pageDataset(report.dataset, pseudonyms);
    assert.deepEqual(checkNumbers(`${NAMED} and Unit5 hold 4,321 deals.`, set).unmatched.map((u) => u.raw), ["Unit5", "4,321"]);
    assert.deepEqual(checkNumbers("Most of it sits with A0011.", set).unmatched, [{ raw: "A0011", reason: "this id isn't in the data" }]);
  });
});

describe("the documents are made, and nothing untraceable is printed", () => {
  test("Claude's drafts and a consultant's edit: what traces is printed, what doesn't falls back to our wording, and the final pages hold nothing untraceable", async () => {
    const dir = folder();
    // Invented: a letters-and-digits word, a number, a date, an email, a link and a person's token.
    const invented = "We saw Zeta9Flux on 4,321 deals on 3 March 2019, from x@example.com, https://example.org/a and [[user:U0999]].";
    const result = await renderReleased(
      dir,
      approved(dir, {
        [PIPELINE]: { body: `Most of the open value sits with ${ACCOUNT}.`, source: "claude_draft" },
        "full_health_check:appendix": { body: invented, source: "claude_draft" },
        "executive_summary:verdict": { body: invented, source: "consultant_edit" },
      }),
      join(dir, "documents"),
    );
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.deepEqual(numberProblems(dir), [], "the final pages, checked again, hold nothing");
    assert.deepEqual(result.leftOut, []);
    const fell = result.fellBack ?? [];
    // The pipeline draft names the account, which traces now: it's printed as approved.
    assert.deepEqual(fell.map((f) => `${f.output}:${f.key}:${f.source}`), ["executive_summary:verdict:consultant_edit", "full_health_check:appendix:claude_draft"]);
    const draft = fell.find((f) => f.source === "claude_draft")!;
    assert.deepEqual(draft.kinds, { identifier: 3, number: 1, date: 1, token: 1 });
    assert.equal(draft.words, undefined, "a draft's figures aren't repeated");
    const edit = fell.find((f) => f.source === "consultant_edit")!;
    assert.deepEqual(edit.words, ["Zeta9Flux", "4,321", "3 March 2019", "x@example.com", "https://example.org/a", "[[user:U0999]]"]);
    const lines = repairWarnings(fell, []);
    assert.match(lines[0]!, /^1 section uses our own wording because a figure in the draft couldn't be traced: "[^"]+"\. Ask for a new draft/);
    assert.match(lines[1]!, /^"[^"]+" uses our own wording because your edit's "Zeta9Flux", "4,321", .* couldn't be traced to the data\. Your edit is kept/);
    assert.ok(result.warnings.every((w) => !w.includes("number check")), JSON.stringify(result.warnings));
  });

  test("our own text or a table: the sentence holding an untraceable word or number is left out with the fixed sentence, and the pages pass", async () => {
    const dir = folder();
    const { input, pseudonyms } = loadRun(dir);
    const report = buildReport(input);
    const out = mkdtempSync(join(tmpdir(), "hc-repair-out-"));
    const paths = { rawNotes: join(out, "n.docx"), executive: join(out, "e.pdf"), full: join(out, "f.pdf"), ceo: join(out, "c.pdf"), kit: join(out, "k.docx"), layout: join(out, "l.json") };
    // A defect planted in the engine's own page: a table cell and a sentence of fixed text.
    const plant = (html: string): string => html.replace("</h2>", "</h2><p>Our fixed text names OAuth9Zeta here. It stays.</p><table><tr><td>We counted 4,321 of them.</td></tr></table>");
    const held = heldFigures(await printDocuments(report, pseudonyms, paths, plant));
    assert.ok(held.some((h) => h.figure === "an identifier") && held.some((h) => h.figure === "4,321"), JSON.stringify(held));
    const repaired = await printDocuments(report, pseudonyms, paths, (html) => leaveOut(plant(html), ["OAuth9Zeta", "4,321"]));
    assert.deepEqual(heldFigures(repaired), []);
  });

  test("leaving out touches only the sentence, cell or label that holds the figure, never styles, and runs of them read once", () => {
    const html = `<style>.OAuth9Zeta{}</style><p>First fine. OAuth9Zeta is here. oauth9zeta again. Last fine.</p><td>4,321 rows</td><td>14,321 rows</td><text>OAuth9Zetas</text>`;
    assert.equal(
      leaveOut(html, ["OAuth9Zeta", "4,321"]),
      `<style>.OAuth9Zeta{}</style><p>First fine. ${LEFT_OUT_SENTENCE.replaceAll("'", "&#39;")} Last fine.</p><td>${LEFT_OUT_SENTENCE.replaceAll("'", "&#39;")}</td><td>14,321 rows</td><text>OAuth9Zetas</text>`,
    );
    assert.equal(checkNumbers(LEFT_OUT_SENTENCE, buildDataset({ findings: [], metrics: [] })).checked, 0, "the fixed sentence holds no figure");
  });
});

describe("what's still held names its section", () => {
  test("never 'those sections' without naming one", () => {
    const named = heldBackReason([{ document: "The full health check", page: 53, figure: "an identifier", reason: "this id isn't in the data", masked: true, section: "Current pipeline" }]);
    assert.equal(named, 'Held back: 1 figure on 1 page can\'t be traced to the data.\nThe full health check, page 53, in "Current pipeline": an identifier: this id isn\'t in the data.\nEdit the wording in "Current pipeline", approve it again, and make the documents again.');
    const ours = heldBackReason([{ document: "The full health check", page: 53, figure: "an identifier", reason: "this id isn't in the data", masked: true }]);
    assert.doesNotMatch(ours, /those sections/);
    assert.match(ours, /\nThese are in our own text, not in a section's wording, so they're ours to fix: tell Black Cloud\.$/);
  });
});

describe("org-chosen names are traced by construction", () => {
  const metric = (segments: Array<{ dimension: string; key: string }>): Metric =>
    ({ id: "documented.relationships", label: "Relationships", definition: "Links between objects", unit: "count", status: "measured", value: 1, n: 1, stats: {}, segments: segments.map((s) => ({ ...s, value: 1, n: 1 })), series: [], assumptions: [], exclusions: [], evidence: [] }) as unknown as Metric;

  test("each letters-and-digits word in a segment's key or dimension traces wherever it's printed; its numbers don't", () => {
    const set = buildDataset({ findings: [], metrics: [metric([{ dimension: "master-detail", key: "Invoice2__c.Account2__c to Account" }, { dimension: "Project365__c|before save|described", key: "Route_Deal_B2B" }])] });
    assert.equal(checkNumbers("Invoice2__c looks up to Account2__c. Project365__c runs Route_Deal_B2B.", set).passed, true);
    assert.deepEqual(checkNumbers("Invoice3__c has 365 rows.", set).unmatched.map((u) => u.raw), ["Invoice3__c", "365"]);
  });
});

describe("the engine's own vocabulary is an explicit list read from its source", () => {
  test("every letters-and-digits word the source writes is listed, as text or as code, and nothing listed is missing from the source", () => {
    const found = scanEngineWords(SRC);
    const unlisted = [...found.keys()].filter((w) => !ENGINE_WORDS.has(w) && !CODE_WORDS.has(w));
    assert.deepEqual(unlisted.map((w) => `${w} (${found.get(w)})`), [], "review each: ENGINE_WORDS if a page may print it, CODE_WORDS if not");
    assert.deepEqual([...ENGINE_WORDS, ...CODE_WORDS].filter((w) => !found.has(w)), [], "a listed word the source no longer writes");
    assert.deepEqual([...ENGINE_WORDS].filter((w) => CODE_WORDS.has(w)), []);
  });

  test("a listed word is accepted only as a word: not as a web address, an email or a code word", () => {
    const set = buildDataset({ findings: [], metrics: [] });
    assert.equal(checkNumbers("Switch on EnableCampaignInfluence2.", set).passed, true);
    assert.deepEqual(checkNumbers("See https://enablecampaigninfluence2.example.com or enablecampaigninfluence2@example.com. Zeta9Flux, h2 and woff2.", set).unmatched.map((u) => u.raw), ["https://enablecampaigninfluence2.example.com", "enablecampaigninfluence2@example.com", "Zeta9Flux", "h2", "woff2"]);
  });
});

