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
import { approvedReport, asPrinted, heldFigures, renderReleased, repairWarnings, toRepair, type ApprovedRun, type ReleasedDocuments } from "../render-released.ts";
import { heldPattern, MASK, maskPage, maskText, withheldLine, withheldNote } from "../repair.ts";
import { loadRun, OUTPUT_FILES, pageDataset, printDocuments } from "../render-run.ts";
import { fullReportHtml } from "../full-report.ts";
import { executiveHtml } from "../executive.ts";
import { onePagerHtml } from "../one-pager.ts";
import { micrositeMisses, pageText } from "../../microsite/pages.ts";
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

describe("the documents are made from a masked report, and every output is clean", () => {
  test("engine text (an owner), a Claude draft and a consultant's edit: the word is masked or falls back, and is in no output", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "last"))!.owner = `${INVENTED} team`;
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
    assert.match(docx(result.files.raw_notes), /\(detail withheld\) team/, "the raw notes print the mask where the owner was");
    assert.equal(result.stillIn, undefined);
    assert.ok(result.microsite?.ok, "the microsite is made");
    assert.ok(result.digest?.ok, "the digest is written");
    assert.doesNotMatch(JSON.stringify(result.digest), new RegExp(INVENTED));
    assert.notEqual(result.repairs, "none");
    assert.ok((result.leftOut ?? []).length === 1 && result.leftOut![0]!.field === "owner" && result.leftOut![0]!.ruleId, JSON.stringify(result.leftOut));
    const fell = result.fellBack ?? [];
    assert.deepEqual(fell.map((f) => `${f.output}:${f.key}:${f.source}`), ["executive_summary:verdict:consultant_edit", "full_health_check:appendix:claude_draft"]);
    assert.deepEqual(fell[1]!.kinds, { identifier: 3, number: 1, date: 1, token: 1 });
    assert.deepEqual(fell[0]!.words, [invented], "the consultant's sentence as typed, the ISO date included, and not the fine one");
    assert.match(repairWarnings(fell, result.leftOut ?? []).join("\n"), /"[^"]+" uses our own wording because what you wrote couldn't be traced to the data: "We saw Zeta9Flux on 4,321 deals on 2019-03-03/);
  });

  test("on the one-pager: the one-pager is made with its mask, and the repair is marked heavy", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "first"))!.owner = `${INVENTED} team`;
    });
    const result = await renderReleased(dir, approved(dir), join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.equal(result.repairs, "heavy");
    assert.ok(result.masks!.ceo > 0);
    assert.deepEqual(result.fellBack, [], "nothing else is replaced");
    assert.deepEqual(everyOutput(dir, result).numberProblems, []);
  });

  test("the repair loop takes what the microsite holds as it takes what the pages hold", () => {
    const layout = { executive: { pages: 6, passed: false, problems: [{ page: 4, check: "number check", detail: "31.4: not in the data" }] }, full: { pages: 40, passed: true, problems: [] }, ceo: { pages: 1, passed: false, problems: [{ page: 1, check: "number check", detail: "(whole text): The number check failed to run: boom" }] } };
    assert.deepEqual(toRepair(layout, [{ raw: INVENTED }]), [{ document: "The executive summary", page: 4, raw: "31.4" }, { document: "The microsite", page: 0, raw: INVENTED }]);
  });

  test("a masked figure a record row also prints is said in the log, never a reason to hold the make", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "last"))!.owner = "Team 4417";
      const path = join(d, "action-lists.json");
      const lists = JSON.parse(readFileSync(path, "utf8")) as { lists: Array<{ object: string; rows: Array<{ name: string | null }> }> };
      lists.lists.find((l) => l.object !== "User" && l.rows.length > 0)!.rows[0]!.name = "Step 4417";
      writeFileSync(path, JSON.stringify(lists));
    });
    const result = await renderReleased(dir, approved(dir), join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.deepEqual(result.stillIn, ["the action lists"]);
    assert.ok(result.digest?.ok, "a held bare number elsewhere never withholds the digest");
    assert.doesNotMatch(JSON.stringify(result.digest), /Team 4417/);
  });

  test("what no stored field holds is masked where it's printed: a date the page rewrote", async () => {
    const dir = folder(NAMED, (all, d) => {
      all.find((x) => x.id === issueAt(d, "last"))!.owner = "Since 2019-03-03";
    });
    const result = await renderReleased(dir, approved(dir), join(dir, "documents"));
    assert.ok(!("refused" in result));
    assert.deepEqual(result.held, []);
    assert.deepEqual(everyOutput(dir, result).numberProblems, []);
    assert.deepEqual((result.leftOut ?? []).map((l) => [l.kind, l.field]), [["date", "page"]]);
  });

  test("a name a token restores to that reads as a figure: masked where it's printed", () => {
    const html = `<p>Most sits with 3M. We found 3M records worth £3M.</p>`;
    assert.equal(maskPage(html, ["3M"]).html, `<p>Most sits with ${MASK}. We found ${MASK} records worth £3M.</p>`);
  });

  test("fixed text in a template, or a survey figure built inside the report: masked where it's printed, and the pages pass", async () => {
    const { input, pseudonyms } = loadRun(folder());
    const report = buildReport(input);
    const out = mkdtempSync(join(tmpdir(), "hc-repair-out-"));
    const paths = { rawNotes: join(out, "n.docx"), executive: join(out, "e.pdf"), full: join(out, "f.pdf"), ceo: join(out, "c.pdf"), kit: join(out, "k.docx"), layout: join(out, "l.json") };
    const plant = (html: string): string => html.replace("</h2>", "</h2><p>Our fixed text names OAuth9Zeta here, beside 4,321 of them. It stays.</p>");
    const held = heldFigures(await printDocuments(report, pseudonyms, paths, plant));
    assert.ok(held.some((h) => h.figure === "an identifier") && held.some((h) => h.figure === "4,321"), JSON.stringify(held));
    assert.deepEqual(heldFigures(await printDocuments(report, pseudonyms, paths, (html) => withheldNote(maskPage(plant(html), ["OAuth9Zeta", "4,321"]).html))), []);
  });
});

describe("masking changes the token and nothing else", () => {
  const strip = (html: string): string[] => extractFigures(pageText(html)).figures.map((f) => f.raw);
  const withPaths = (dir: string, change: (won: Extract<Metric, { status: "measured" }>) => void): void => {
    const path = join(dir, "metrics.json");
    const metrics = JSON.parse(readFileSync(path, "utf8")) as Metric[];
    const won = metrics.find((m) => m.id === "historic.path_to_won") as Extract<Metric, { status: "measured" }>;
    assert.ok(won && won.status === "measured" && won.segments.length > 1, "the synthetic org has paths to won");
    change(won);
    writeFileSync(path, JSON.stringify(metrics));
  };
  const RAW = "Q9Zeta";

  test("the first path to won masked: every figure on every page is as it was, but the masked word", () => {
    const dir = folder();
    withPaths(dir, (won) => void (won.segments[0]!.key = `${won.segments[0]!.key} ${RAW}`));
    const plain = approvedReport(dir, approved(dir));
    const masked = approvedReport(dir, approved(dir), [RAW]);
    for (const html of [fullReportHtml, executiveHtml, onePagerHtml]) {
      const before = strip(html(plain.report, plain.pseudonyms)).filter((f) => f !== RAW);
      assert.deepEqual(strip(html(masked.report, masked.pseudonyms)), before);
    }
    assert.ok(fullReportHtml(masked.report, masked.pseudonyms).includes(MASK));
    assert.deepEqual(masked.repairs, [{ raw: RAW, metricId: "historic.path_to_won", field: "segments.key" }]);
    // The figures a page is checked against are the data's as stored: a mask makes nothing else untraceable.
    assert.deepEqual(masked.report.dataset, plain.report.dataset);
  });

  test("the only path to won masked: no crash, and the row is still there", () => {
    const dir = folder();
    withPaths(dir, (won) => void (won.segments = [{ ...won.segments[0]!, key: `${won.segments[0]!.key} ${RAW}` }]));
    const masked = approvedReport(dir, approved(dir), [RAW]);
    const html = fullReportHtml(masked.report, masked.pseudonyms);
    assert.ok(html.includes(MASK) && !html.includes(RAW));
  });

  test("a held figure is matched whole, each time, in any script: never inside a larger number, a percentage, an amount or a word", () => {
    assert.equal(maskText("We found 4,321 rows. It is 4.5 or 4% or £4 or A4 or café4. Only 4 here, and 4.", [heldPattern("4")]).text, `We found 4,321 rows. It is 4.5 or 4% or £4 or A4 or café4. Only ${MASK} here, and ${MASK}.`);
    assert.equal(maskText("OAUTH9ZETA one, oauth9zeta two.", [heldPattern("OAuth9Zeta")]).text, `${MASK} one, ${MASK} two.`, "an identifier in any case, as the check compares it");
    assert.equal(maskText("£1.99m once. £1.99M twice.", [heldPattern("£1.99m")]).text, `${MASK} once. £1.99M twice.`, "anything else as written");
    assert.equal(checkNumbers(MASK, buildDataset({ findings: [], metrics: [] })).checked, 0, "the mask is never read as a figure");
    assert.equal(withheldLine(1), "A detail is withheld in 1 place because we couldn't trace it to your data.");
    assert.match(withheldNote(`<section class="page"><p>${MASK}</p><div class="footer" data-chrome><span>Black Cloud Barometer</span></div></section>`), /<span>Black Cloud Barometer \| A detail is withheld in 1 place because we couldn&#39;t trace it to your data\.<\/span>/);
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
      // Nor any piece of it: "5k-25k" is read whole as a word, but it's a range of amounts, and "2025" in an id is a year.
      for (const piece of word.split(/[-_.]+/)) assert.ok(extractFigures(piece).figures.every((f) => f.kind === "identifier"), `${word}: ${piece}`);
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
