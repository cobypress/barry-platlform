import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { strFromU8, unzipSync } from "fflate";
import { buildReport } from "../../narrative/sections.ts";
import { buildPlan } from "../../narrative/select.ts";
import { buildDataset, checkNumbers, extractFigures, scopeDataset } from "../../numbercheck/index.ts";
import { ENGINE_WORDS, CODE_WORDS } from "../../numbercheck/engine-words.ts";
import { scanEngineWords } from "../../numbercheck/engine-words-scan.ts";
import type { PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { asPrinted, renderReleased, repairWarnings, type ApprovedRun, type ReleasedDocuments } from "../render-released.ts";
import { heldPattern, LEFT_OUT_SENTENCE, leaveOutSentences, repairInput, ROW_LEFT_OUT } from "../repair.ts";
import { loadRun, OUTPUT_FILES, pageDataset } from "../render-run.ts";
import { micrositeMisses } from "../../microsite/pages.ts";
import type { Finding, Metric, ReportSection, SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { heldBackReason } from "../../worker/documents-job.ts";

// Decision 682: a figure the number check can't trace is repaired at its
// source, every output is made from the repaired report and checked, and
// what can't be repaired is never handed to a person without saying where.

const ACCOUNT = "[[account:A0011]]";
/** An account named with a letter and a digit, as real ones are ("Unit4", "O2"). */
const NAMED = "Unit4 Ltd";
const SRC = join(import.meta.dirname, "..", "..");
const INVENTED = "Zeta9Flux";

const { dir: recorded } = await runFromRecording();

/** The recorded run, copied, with the account behind A0011 renamed, and the stored findings changed as given. */
function folder(name = NAMED, findings: (all: Finding[], dir: string) => void = () => {}): string {
  const dir = join(mkdtempSync(join(tmpdir(), "hc-repair-")), "hc-2026-10-31-0000000a");
  cpSync(recorded, dir, { recursive: true });
  const path = join(dir, "pseudonym-map.json");
  const map = JSON.parse(readFileSync(path, "utf8")) as PseudonymEntry[];
  const entry = map.find((e) => e.token === ACCOUNT);
  assert.ok(entry, "the synthetic org has the account the current pipeline names");
  entry.name = name;
  writeFileSync(path, JSON.stringify(map));
  const stored = JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as Finding[];
  findings(stored, dir);
  writeFileSync(join(dir, "findings.json"), JSON.stringify(stored));
  return dir;
}

/** Every section approved, with these bodies in place of the engine's, by output and key. */
function approved(dir: string, changes: Record<string, { body: string; source: SectionVersion["source"] }> = {}): ApprovedRun {
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
  return { intake: null, sections, versions, overrides: [], digest: { inventory: null, madeAt: "2026-11-05T09:00:00.000Z" } };
}

/** The plan's first action (on the one-pager), or the issue the report ranks last (not on it), by id. */
function issueAt(dir: string, at: "first" | "last"): string {
  const report = buildReport(loadRun(dir).input);
  if (at === "first") return buildPlan(report.teamFindings, report.added).phases.flatMap((p) => p.findings)[0]!.id;
  return report.teamFindings.filter((f) => f.status === "issue").at(-1)!.id;
}

const docx = (path: string): string => Object.entries(unzipSync(readFileSync(path))).filter(([n]) => n.startsWith("word/")).map(([, b]) => strFromU8(b)).join(" ");
const xlsx = (path: string): string => Object.entries(unzipSync(readFileSync(path))).filter(([n]) => n.startsWith("xl/")).map(([, b]) => strFromU8(b)).join(" ");

/** Every output a customer, Drive or the connector gets, as text, with the final PDF check's number problems. */
function everyOutput(dir: string, result: ReleasedDocuments): { text: string; numberProblems: unknown[] } {
  const layout = JSON.parse(readFileSync(join(dir, "documents", OUTPUT_FILES.layout), "utf8")) as Record<string, { problems: Array<{ check: string }> }>;
  const text = [docx(result.files.raw_notes), docx(result.files.readout_kit), xlsx(result.files.action_lists), readFileSync(result.files.tickets, "utf8"), JSON.stringify(result.digest), JSON.stringify(result.microsite)].join("\n");
  return { text, numberProblems: Object.values(layout).flatMap((l) => l.problems.filter((p) => p.check === "number check")) };
}

const PIPELINE = "full_health_check:domain_current_pipeline";

describe("the cause of a name token: it passed when drafted and its name was held on the page", () => {
  test("Claude's draft names an account by its token; the name it restores to now traces as printed", () => {
    const { input, pseudonyms } = loadRun(folder());
    const report = buildReport(input);
    const body = `Most of the open value sits with ${ACCOUNT}.`;
    assert.equal(checkNumbers(body, scopeDataset(report.dataset, report.scopes.get(`${report.input.run.id}:${PIPELINE}`)!)).passed, true);
    const printed = asPrinted(body, pseudonyms);
    assert.deepEqual(checkNumbers(printed, report.dataset).unmatched, [{ raw: "Unit4", reason: "this id isn't in the data" }]);
    assert.equal(checkNumbers(printed, pageDataset(report.dataset, pseudonyms)).passed, true);
    assert.deepEqual(checkNumbers("Most of it sits with A0011.", pageDataset(report.dataset, pseudonyms)).unmatched, [{ raw: "A0011", reason: "this id isn't in the data" }]);
  });

  test("a restored name the check reads as a figure never masks one: an account called 3M, 24 Seven or Studio 54", () => {
    const set = buildDataset({ findings: [], metrics: [] });
    for (const name of ["3M", "24 Seven", "Studio 54"]) {
      const pseudonyms = new Pseudonymiser([{ token: "[[account:A0001]]", id: "001000000000001AAA", name, aliases: [] }]);
      const page = pageDataset({ ...set, tokens: [{ source: "f", value: "[[account:A0001]]" }] }, pseudonyms);
      assert.equal(page.labels.length, 0, name);
    }
    // Even a label given directly is never read after a currency sign or inside a number.
    const labelled = { ...set, labels: [{ source: "x", value: "3M" }] };
    assert.deepEqual(checkNumbers("We found 3M records worth £3M.", labelled).unmatched.map((u) => u.raw), ["£3M"]);
    assert.deepEqual(checkNumbers("Worth 1.3M, or 3M%.", labelled).unmatched.map((u) => u.raw), ["1.3M", "3M%"]);
  });
});

describe("the documents are made from a repaired report, and every output is clean", () => {
  test("engine text (an owner) in the full report, a Claude draft and a consultant's edit: repaired, and the word is in no output", async () => {
    let id = "";
    const dir = folder(NAMED, (all, d) => {
      id = issueAt(d, "last");
      const f = all.find((x) => x.id === id)!;
      f.owner = `${INVENTED} team`;
    });
    const invented = `We saw ${INVENTED} on 4,321 deals on 2019-03-03, from x@example.com, https://example.org/a and [[user:U0999]].`;
    const result = await renderReleased(
      dir,
      approved(dir, {
        [PIPELINE]: { body: `Most of the open value sits with ${ACCOUNT}.`, source: "claude_draft" },
        "full_health_check:appendix": { body: invented, source: "claude_draft" },
        "executive_summary:verdict": { body: `A fine sentence. ${invented}`, source: "consultant_edit" },
      }),
      join(dir, "documents"),
    );
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    const outputs = everyOutput(dir, result);
    assert.deepEqual(outputs.numberProblems, [], "the final pages, checked again, hold nothing");
    assert.doesNotMatch(outputs.text, new RegExp(INVENTED), "nor does any other output");
    assert.ok(result.microsite?.ok, "the microsite is made, repaired");
    assert.ok(result.digest?.ok, "the digest is made, repaired");
    assert.equal(result.repairs, "some");
    assert.ok((result.leftOut ?? []).length >= 1 && (result.leftOut ?? []).every((l) => l.field === "owner" && l.ruleId && l.kind === "identifier"), JSON.stringify(result.leftOut));
    const fell = result.fellBack ?? [];
    assert.deepEqual(fell.map((f) => `${f.output}:${f.key}:${f.source}`), ["executive_summary:verdict:consultant_edit", "full_health_check:appendix:claude_draft"]);
    assert.deepEqual(fell[1]!.kinds, { identifier: 3, number: 1, date: 1, token: 1 });
    // The consultant's sentence as typed, the ISO date included, and not the fine one.
    assert.deepEqual(fell[0]!.words, [invented]);
    assert.match(repairWarnings(fell, result.leftOut ?? [], result.repairs).join("\n"), /"[^"]+" uses our own wording because what you wrote couldn't be traced to the data: "We saw Zeta9Flux on 4,321 deals on 2019-03-03/);
  });

  test("on the one-pager: the documents are made from our own wording alone, and marked heavy", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "first"))!.owner = `${INVENTED} team`;
    });
    const result = await renderReleased(dir, approved(dir, { "executive_summary:verdict": { body: "A sentence a consultant wrote, with nothing to trace.", source: "consultant_edit" } }), join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.equal(result.repairs, "heavy");
    assert.deepEqual(everyOutput(dir, result).numberProblems, []);
    assert.doesNotMatch(everyOutput(dir, result).text, new RegExp(INVENTED));
    const verdict = (result.fellBack ?? []).find((f) => f.key === "verdict")!;
    assert.deepEqual([verdict.source, verdict.kinds, verdict.words], ["consultant_edit", {}, undefined], "fell back for the heavy repair, with nothing of theirs held");
  });
});

describe("every output is checked, not only the pages", () => {
  test("a figure taken out of the report that a record row still prints holds the make, naming the action lists", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "last"))!.owner = `${INVENTED} team`;
      // The same word as a record's name in an action list, which the report's data doesn't hold.
      const path = join(d, "action-lists.json");
      const lists = JSON.parse(readFileSync(path, "utf8")) as { lists: Array<{ object: string; rows: Array<{ name: string | null }> }> };
      const list = lists.lists.find((l) => l.object !== "User" && l.rows.length > 0)!;
      list.rows[0]!.name = `${INVENTED} Ltd`;
      writeFileSync(path, JSON.stringify(lists));
    });
    const result = await renderReleased(dir, approved(dir), join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held.map((h) => [h.document, h.figure]), [["The action lists", "an identifier"]]);
  });
});

describe("taking out is precise", () => {
  test("a held figure is matched whole, each time: never inside a larger number, a percentage, an amount or a word", () => {
    const four = heldPattern("4");
    assert.equal(leaveOutSentences("We found 4,321 rows. It is 4.5 or 4% or £4 or A4. Only 4 here.", [four]), `We found 4,321 rows. It is 4.5 or 4% or £4 or A4. ${LEFT_OUT_SENTENCE}`);
    assert.equal(leaveOutSentences("OAUTH9ZETA one. Two. oauth9zeta three.", [heldPattern("OAuth9Zeta")]), `${LEFT_OUT_SENTENCE} Two. ${LEFT_OUT_SENTENCE}`, "an identifier in any case, as the check compares it");
    assert.equal(leaveOutSentences("£1.99m once. £1.99M twice.", [heldPattern("£1.99m")]), `${LEFT_OUT_SENTENCE} £1.99M twice.`, "anything else as written");
    assert.equal(checkNumbers(`${LEFT_OUT_SENTENCE} ${ROW_LEFT_OUT}`, buildDataset({ findings: [], metrics: [] })).checked, 0, "the fixed sentences hold no figure");
  });

  test("a table row holding one is dropped whole and the table says so once; a finding's text loses only its sentence", () => {
    const { input } = loadRun(folder());
    const metric = input.metrics.find((m): m is Extract<Metric, { status: "measured" }> => m.status === "measured" && m.segments.length > 1)!;
    const key = metric.segments[0]!.key;
    const raw = "Q9Zeta";
    const planted = { ...input, metrics: input.metrics.map((m) => (m.id === metric.id ? { ...metric, segments: metric.segments.map((s, i) => (i === 0 ? { ...s, key: `${key} ${raw}` } : s)) } : m)) };
    const { input: repaired, repairs } = repairInput(planted, [raw]);
    const after = repaired.metrics.find((m) => m.id === metric.id) as typeof metric;
    assert.equal(after.segments.length, metric.segments.length - 1);
    assert.deepEqual(after.exclusions.filter((e) => e === ROW_LEFT_OUT), [ROW_LEFT_OUT]);
    assert.deepEqual(repairs, [{ raw, metricId: metric.id, field: "segments" }]);
    assert.doesNotMatch(JSON.stringify(repaired), /Q9Zeta/);
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

  test("the microsite is checked with the names its tokens restore to", () => {
    const { input, pseudonyms } = loadRun(folder());
    const report = buildReport(input);
    assert.deepEqual(micrositeMisses([{ slug: "x", title: "X", html: `<p>Most sits with ${NAMED}, and ${INVENTED}.</p>` }], report, pseudonyms).map((m) => m.raw), [INVENTED]);
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

  test("nothing on the list reads as a figure in any casing or setting", () => {
    for (const word of ENGINE_WORDS) {
      for (const form of [word, word.toUpperCase(), word.replace(/^./, (c) => c.toUpperCase()), `£${word}`, `${word} users`, `in ${word}`]) {
        const figures = extractFigures(form).figures;
        assert.ok(figures.length === 1 && figures[0]!.kind === "identifier" && figures[0]!.word && ENGINE_WORDS.has(figures[0]!.raw.toLowerCase()), `${form}: ${JSON.stringify(figures)}`);
      }
    }
    const set = buildDataset({ findings: [], metrics: [] });
    assert.deepEqual(checkNumbers("We saved £5k-25k from 25K-100K users in the under-5k band.", set).passed, false);
  });

  test("a listed word is accepted only as a word: not as a web address, an email or a code word", () => {
    const set = buildDataset({ findings: [], metrics: [] });
    assert.equal(checkNumbers("Switch on EnableCampaignInfluence2.", set).passed, true);
    assert.deepEqual(checkNumbers(`See https://enablecampaigninfluence2.example.com or enablecampaigninfluence2@example.com. ${INVENTED}, h2 and woff2.`, set).unmatched.map((u) => u.raw), ["https://enablecampaigninfluence2.example.com", "enablecampaigninfluence2@example.com", INVENTED, "h2", "woff2"]);
  });
});
