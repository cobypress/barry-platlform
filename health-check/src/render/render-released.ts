import { mkdirSync, readFileSync } from "node:fs";
import { strFromU8, unzipSync } from "fflate";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { micrositeMisses, micrositePages, type MicrositePages } from "../microsite/pages.ts";
import { heldPattern, maskInput, maskPage, maskText, masksIn, withheldNote, type SourceRepair } from "./repair.ts";
import type { ReportInput } from "../narrative/sections.ts";
import type { Intake } from "../narrative/intake.ts";
import type { OverrideRefusal } from "../narrative/overrides.ts";
import { buildReport, currentBody, surveyMetricsOf, type BuiltReport, type ReportOutputKey } from "../narrative/sections.ts";
import { checkNumbers, extractFigures, type FigureSet } from "../numbercheck/index.ts";
import { escapeXml, plainWords, ukDates } from "./format.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Metric, type NumberOverride, type ReportSection, type SectionVersion } from "../schemas/index.ts";
import type { LayoutProblem, LayoutReport } from "./layout.ts";
import { renderCaseStudyDocx } from "./case-study.ts";
import { writeFileSync } from "node:fs";
import { loadRun, OUTPUT_FILES, pageDataset, printDocuments, type PrintedLayout } from "./render-run.ts";
import { writeActionFiles } from "../actions/index.ts";
import { buildDigest } from "../digest/build.ts";
import type { RunDigest } from "../schemas/digest.ts";
import type { Inventory } from "../schemas/inventory.ts";

// The documents for a released run, in the wording the console approved. The
// run folder supplies the findings, metrics and pseudonym map, as for `hc
// render`; the console supplies each section's approved text, the figures a
// consultant overrode, the intake as it stands now and who prepared the report
// (for the cover; the documents job never calls Claude). Nothing unapproved is
// printed: a section that isn't approved stops the whole set.

export type ApprovedRun = {
  intake: Intake | null;
  sections: ReportSection[];
  /** The current version of each section. */
  versions: SectionVersion[];
  overrides: NumberOverride[];
  /** The consultant the cover names. Left out or null, the cover names only the client. */
  preparedBy?: string | null;
  /** Other customers' benchmark rows. Without them, the run is compared with published research only. */
  benchmark?: BenchmarkInput | null;
  /**
   * Given, the make also returns the run's digest (DECISIONS.md 632), from the
   * same approved report the documents print: the inventory as the console's
   * row holds it, for the names it withholds, and when it was made.
   */
  digest?: { inventory: Inventory | null; madeAt: string };
};

/** The report's three outputs, then the one-pager and the action lists for the customer, and the kit for Black Cloud only. */
export type DocumentKey = ReportOutputKey | "ceo_one_pager" | "action_lists" | "tickets" | "readout_kit";

export type ReleasedDocuments = {
  files: Record<DocumentKey, string>;
  /** What the layout checks found. The files are still written, for a person to look over. */
  warnings: string[];
  /** Each figure the number check couldn't trace on a printed page once the make has repaired what it can (DECISIONS.md 682), with the section whose text holds it. Any at all holds the documents back (DECISIONS.md 430). */
  held: HeldFigure[];
  /** The customer microsite's pages, or why they couldn't be made. Left out by renderers that don't make them. */
  microsite?: { ok: true; pages: MicrositePages } | { ok: false; reason: string };
  /** A case study draft, only for a re-check whose customer agreed to be one. For Black Cloud's own folder. */
  caseStudy?: string;
  /** How many of the earlier health check's corrected figures a re-check shows as read, for the log (DECISIONS.md 522). Left out by renderers that don't compare. */
  baselineRefused?: number;
  /** The earlier health check the re-check's draft read can't be read now, so nothing compares with it, for the log (DECISIONS.md 534). */
  baselineGone?: boolean;
  /** The run folder gives an earlier health check and the approved sections hold no comparison with it, so the documents have none, for the log (DECISIONS.md 535). */
  baselineUnapproved?: boolean;
  /** The run's digest, or the class of the error that stopped it, only when asked for. Left out by renderers that don't make one. */
  digest?: { ok: true; digest: RunDigest } | { ok: false; error: string };
  /** The team survey's metrics as the printed report made them, for the run's stored metrics (DECISIONS.md 645). Left out by renderers that don't say. */
  surveyMetrics?: Metric[];
  /** Sections printed in the engine's own wording because a figure in their approved text couldn't be traced (DECISIONS.md 682). Left out by renderers that don't repair. */
  fellBack?: SectionFallback[];
  /** What was taken out of the engine's own data, for us (DECISIONS.md 682). Left out by renderers that don't repair. */
  leftOut?: LeftOut[];
  /**
   * How much the make repaired (DECISIONS.md 682, item 7): "none"; "some", a
   * section fell back, a detail was masked or the microsite wasn't made;
   * "heavy", more than MASK_CAP masks in a document, any on the one-pager, or
   * more than a quarter of a document's sections fell back. The documents are
   * made the same way whichever it is. Left out by renderers that don't repair.
   */
  repairs?: "none" | "some" | "heavy";
  /** How many details each PDF shows withheld (DECISIONS.md 682). */
  masks?: Record<"executive" | "full" | "ceo", number>;
  /** The microsite wasn't made because a page still held a figure after the repair. */
  micrositeHeld?: true;
  /** The other files a figure masked in the data still shows in, for the log only. */
  stillIn?: string[];
};

/**
 * What a make returns instead when a corrected figure can't be carried into its
 * finding's wording: nothing was written (DECISIONS.md 512).
 */
export type RefusedDocuments = { refused: OverrideRefusal[] };

/**
 * A figure on a printed page that doesn't trace to the run's data. `masked`
 * when what the number check flagged wasn't a plain figure, so `figure` says
 * "an identifier" in its place.
 */
export type HeldFigure = { document: string; page: number; figure: string; reason: string; masked?: true; section?: string };

/**
 * A section printed in the engine's own wording in place of the approved text,
 * because a figure in that text couldn't be traced (DECISIONS.md 682). Claude's
 * draft gives kinds and counts only. A consultant's edit also gives the words
 * that failed, which the consultant typed and may be shown; the edit itself is
 * untouched in the console, kept aside, never rewritten.
 */
export type SectionFallback = {
  output: ReportOutputKey;
  key: string;
  title: string;
  source: "claude_draft" | "claude_proposal" | "consultant_edit";
  /** How many figures of each kind couldn't be traced: "identifier: 1". Empty when it fell back only for a heavy repair. */
  kinds: Record<string, number>;
  /** A consultant's edit that failed only: the sentences they typed that couldn't be traced, as typed. */
  words?: string[];
};

/**
 * A figure no output could trace that the engine's own data held, taken out of
 * that data so no output is made with it (DECISIONS.md 682): where it was first
 * held, what kind it was, and the finding's rule or the metric and the field it
 * was in. Never the figure: it's for us.
 */
export type LeftOut = { document: string; page: number; kind: string; ruleId?: string; metricId?: string; stepId?: string; field: string };

/** How many times everything is made again with what couldn't be traced taken out. */
export const REPAIR_ROUNDS = 2;

/** Where the microsite's held figures are said to be. */
const MICROSITE = "The microsite";

const DOCUMENT_NAMES = { executive: "The executive summary", full: "The full health check", ceo: "The one page summary" } as const;

type Layouts = { executive: LayoutReport; full: LayoutReport; ceo?: LayoutReport };

/** The check `checkLayout` names a figure the number check couldn't trace. */
const NUMBER_CHECK = "number check";

const FIGURE_NUMBER = String.raw`[-+\u2212]?[£$€]?[-\u2212]?(?:[1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{0,8}|0)(?:\.\d+)?(?:k|mn|m|bn|b|x|%|st|nd|rd|th)?`;
const FIGURE_WORDS = String.raw`a|an|in|of|and|to|per|cent|percent|percentage|points?|pts?|ppts?|pp|bps|basis|times|fold|gbp|usd|eur|pounds?|sterling|dollars?|euros?|hundreds?|thousands?|millions?|billions?|trillions?|dozens|tens|scores|half|thirds?|quarters?|fifths?|tenths?|days?|weeks?|months?|years?|hours?|minutes?`;
const MONTHS = String.raw`jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?`;

/**
 * A figure as the report prints it: numbers (a currency sign, commas, a decimal
 * point, k, m, bn, %, x) joined by spaces or hyphens to a short list of unit and
 * scale words, or a date with a month in it. Anything else the number check
 * flagged (a record id, an email, a web address) might be a CRM record, and
 * rule 7 keeps those in the worker, so it isn't repeated to the console or the
 * log. A number with a leading zero, or more than nine digits without commas,
 * reads as an id too.
 */
const PLAIN_FIGURE = new RegExp(String.raw`^(?:${FIGURE_NUMBER}(?:[-\u2013]${FIGURE_NUMBER})?|${FIGURE_WORDS}|${MONTHS})(?:[ -](?:${FIGURE_NUMBER}|${FIGURE_WORDS}|${MONTHS}))*$`, "i");
const MONTH_WORD = new RegExp(String.raw`(?:^|[ -])(?:${MONTHS})(?=$|[ -])`, "i");

/** Whether a flagged value may be repeated. A month alone could be someone's name, so a date needs its number. */
export function isPlainFigure(raw: string): boolean {
  if (raw === "(whole text)") return true;
  return PLAIN_FIGURE.test(raw) && (/\d/.test(raw) || !MONTH_WORD.test(raw));
}

/** The flagged value, or "an identifier" in its place when it isn't a plain figure. */
function shownFigure(raw: string): { figure: string; masked?: true } {
  return isPlainFigure(raw) ? { figure: raw } : { figure: "an identifier", masked: true };
}

function problemsOf(layout: Layouts): Array<{ document: string; problem: LayoutProblem }> {
  return (Object.keys(DOCUMENT_NAMES) as Array<keyof typeof DOCUMENT_NAMES>).flatMap((doc) => (layout[doc]?.problems ?? []).map((problem) => ({ document: DOCUMENT_NAMES[doc], problem })));
}

const where = (document: string, page: number): string => (page === 0 ? document : `${document}, page ${page}`);

/**
 * One line per layout problem, without the check's detail: an empty-page
 * detail quotes the page's text, which has names in it by now, and these lines
 * go to the console. The full report stays in the run folder. A number check's
 * detail is kept: it's the figure and a fixed reason from our own code, and a
 * person needs both to know what to fix.
 */
export function layoutWarnings(layout: Layouts): string[] {
  return problemsOf(layout).map(({ document, problem: p }) => {
    if (p.check !== NUMBER_CHECK) return `${where(document, p.page)}: ${p.check}`;
    const { figure, reason } = splitDetail(p.detail);
    return `${where(document, p.page)}: ${p.check}: ${shownFigure(figure).figure}${reason ? `: ${reason}` : ""}`;
  });
}

/** A number check's detail is "<figure>: <reason>" (layout.ts), split at the first ": ", since a reason can hold one too. */
function splitDetail(detail: string): { figure: string; reason: string } {
  const at = detail.indexOf(": ");
  return at < 0 ? { figure: detail, reason: "" } : { figure: detail.slice(0, at), reason: detail.slice(at + 2) };
}

/**
 * The figures that hold the documents back, with any value that isn't a plain
 * figure masked. A check that failed to run counts, with "(whole text)" as its
 * figure.
 */
export function heldFigures(layout: Layouts): HeldFigure[] {
  return problemsOf(layout)
    .filter(({ problem }) => problem.check === NUMBER_CHECK)
    .map(({ document, problem }) => {
      const { figure, reason } = splitDetail(problem.detail);
      return { document, page: problem.page, ...shownFigure(figure), reason };
    });
}

/** Where each held figure is, and why, in the words the error and the log use. */
export function describeHeld(held: HeldFigure): string {
  return `${where(held.document, held.page)}${held.section ? `, in "${held.section}"` : ""}: ${held.figure}${held.reason ? `: ${held.reason}` : ""}`;
}

const quoted = (titles: string[]): string => [...new Set(titles)].map((t) => `"${t}"`).join(", ");

/**
 * What the console is told the make repaired (decision 682), one line each, in
 * the documents' warnings: the sections that fell back, by title, and how many
 * details were left out of our own text, by page. No figure from the data; a
 * consultant's own words only, which they typed.
 */
export function repairWarnings(fellBack: SectionFallback[], leftOut: LeftOut[], micrositeHeld = false): string[] {
  const lines: string[] = [];
  const drafts = fellBack.filter((f) => f.source !== "consultant_edit");
  if (drafts.length) lines.push(`${drafts.length === 1 ? "1 section uses" : `${drafts.length} sections use`} our own wording because a figure in the draft couldn't be traced: ${quoted(drafts.map((f) => f.title))}. Ask for a new draft to try again.`);
  for (const f of fellBack.filter((f) => f.source === "consultant_edit")) {
    lines.push(`"${f.title}" uses our own wording because what you wrote couldn't be traced to the data: ${quoted(f.words ?? [])}. Your edit is kept: correct it, approve it again and make the documents again.`);
  }
  if (leftOut.length) {
    const pages = [...new Set(leftOut.map((l) => where(l.document, l.page)))];
    lines.push(`We withheld ${leftOut.length === 1 ? "1 detail" : `${leftOut.length} details`} of our own text because ${leftOut.length === 1 ? "it" : "they"} couldn't be traced to the data, first seen in ${pages.join("; ")}. That's ours to fix.`);
  }
  if (micrositeHeld) lines.push("The microsite wasn't made because a figure on it couldn't be traced to the data. That's ours to fix.");
  return lines;
}

/**
 * For the worker's log when a re-check's earlier health check has corrected
 * figures today's reader can't carry (DECISIONS.md 522, item 3): the count
 * only, since a title or a figure is the customer's.
 */
export function baselineRefusedSummary(n: number): string {
  return n === 1
    ? "1 corrected figure of the earlier health check can't be carried, so the comparison shows it as read."
    : `${n} corrected figures of the earlier health check can't be carried, so the comparison shows them as read.`;
}

/** For the worker's log when a re-check is drafted, or a pulse built, without its earlier health check's corrections (DECISIONS.md 531). No path, error or figure. */
export const BASELINE_ROWS_UNREAD = "the earlier health check's corrections couldn't be read, so the comparison shows its figures as read.";

/**
 * The run's report with the console's approved sections in place of the
 * engine's drafts. A re-check compares with its earlier health check only if
 * the approved Word notes hold the progress section (DECISIONS.md 535): a draft
 * holds it exactly when it had an earlier health check, and the console can't
 * remove one, so without it no draft a person approved held a comparison,
 * whatever the folder's record says. Then the report is built with none, and
 * `baselineUnapproved` says so for the log. `baselineGone` (534) only where the
 * section is held: with none approved, nothing is lost.
 *
 * `raws` are figures no output could trace (decision 682): each is masked where
 * the run's own data writes it (maskInput), so everything built from the report
 * shows the mask, and a section approved in the engine's own words that quotes
 * one shows the mask too. The report's dataset stays the one the data as
 * stored gives, so the mask makes nothing else untraceable.
 */
export function approvedReport(dir: string, approved: ApprovedRun, raws: string[] = []): { report: BuiltReport; pseudonyms: Pseudonymiser; baselineGone: boolean; baselineUnapproved: boolean; engineText: Map<string, string>; repairs: SourceRepair[] } {
  const loaded = loadRun(dir);
  const { pseudonyms } = loaded;
  const held = approved.sections.some((s) => s.output === "raw_notes" && s.key === "progress");
  const baselineUnapproved = loaded.input.baseline != null && !held;
  const baselineGone = loaded.baselineGone && held;
  const build = (input: ReportInput): BuiltReport => buildReport({ ...input, ...(baselineUnapproved ? { baseline: null } : {}), overrides: approved.overrides, intake: approved.intake ?? input.intake, preparedBy: approved.preparedBy ?? null, benchmark: approved.benchmark ?? null });
  const { input, repairs } = maskInput(loaded.input, raws);
  const report = build(input);
  if (raws.length) report.dataset = build(loaded.input).dataset;
  // The engine's own text for each section, as built now, from the masked data (decision 682).
  const engineText = new Map<string, string>();
  for (const built of Object.values(report.outputs)) for (const section of built.sections) engineText.set(`${built.output}:${section.key}`, currentBody(built, section.key));
  const patterns: RegExp[] = raws.map((raw) => heldPattern(raw));
  for (const built of Object.values(report.outputs)) {
    const sections = approved.sections.filter((s) => s.output === built.output).sort((a, b) => a.order - b.order);
    if (!canRelease(sections).ok) throw new Error(`Not every section of the ${built.output.replace(/_/g, " ")} is approved, so no documents were made`);
    const current = new Set(sections.map((s) => s.currentVersionId));
    const versions = approved.versions.filter((v) => current.has(v.id));
    if (versions.length !== sections.length) throw new Error(`A section of the ${built.output.replace(/_/g, " ")} has no approved text, so no documents were made`);
    built.sections = sections;
    built.versions = versions.map((v) => (v.source === "engine" && raws.length ? { ...v, body: maskText(v.body, patterns).text } : v));
  }
  return { report, pseudonyms, baselineGone, baselineUnapproved, engineText, repairs };
}

/** A section's text as a page prints it, so it's checked as it will be read. */
export const asPrinted = (body: string, pseudonyms: Pseudonymiser): string => plainWords(ukDates(pseudonyms.restore(body)));

const kindOf = (raw: string, set: FigureSet): string => extractFigures(raw, { labels: set.labels.map((l) => l.value) }).figures[0]?.kind ?? "malformed";

/**
 * Decision 682, items 3 and 4. Each section whose approved text isn't the
 * engine's is checked again as the page prints it, against the set every
 * output is checked with: the same function on the same text. One that fails
 * is printed in the engine's own text for that section, as built now, and said
 * so; no other section changes. A consultant's edit is quoted by the sentences
 * they typed that fail, as typed. Changes the report in place; the console's
 * versions are untouched.
 */
export function fallBackSections(report: BuiltReport, pseudonyms: Pseudonymiser, engineText: Map<string, string>, set: FigureSet): SectionFallback[] {
  const fellBack: SectionFallback[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const section of built.sections) {
      const version = built.versions.find((v) => v.id === section.currentVersionId);
      const engine = engineText.get(`${built.output}:${section.key}`);
      if (!version || version.source === "engine" || engine === undefined) continue;
      const outcome = checkNumbers(asPrinted(version.body, pseudonyms), set);
      if (outcome.passed) continue;
      built.versions[built.versions.indexOf(version)] = { ...version, body: engine, source: "engine" };
      const kinds: Record<string, number> = {};
      for (const u of outcome.unmatched) kinds[kindOf(u.raw, set)] = (kinds[kindOf(u.raw, set)] ?? 0) + 1;
      // The consultant's own sentences, as typed, that the check holds: never a name a token restored to, never empty.
      const typed = version.body.split(/(?<=[.!?])\s+|\n+/).map((s) => s.trim()).filter((s) => s && !checkNumbers(asPrinted(s, pseudonyms), set).passed);
      fellBack.push({
        output: built.output,
        key: section.key,
        title: section.title,
        source: version.source,
        kinds,
        ...(version.source === "consultant_edit" ? { words: typed.length ? typed : [version.body.trim().slice(0, 200)] } : {}),
      });
    }
  }
  return fellBack;
}

/** The raw figure of each number check problem, unmasked, for the worker alone: it never leaves the run folder. */
function heldRaws(layout: Layouts): Array<{ document: string; page: number; raw: string }> {
  return problemsOf(layout)
    .filter(({ problem }) => problem.check === NUMBER_CHECK)
    .map(({ document, problem }) => ({ document, page: problem.page, raw: splitDetail(problem.detail).figure }));
}

/**
 * The section a figure still held is in, found with the check itself: the
 * section whose printed text, checked alone against the same set, holds that
 * figure (decision 682, item 6). Only the title goes out.
 */
function sectionHolding(report: BuiltReport, pseudonyms: Pseudonymiser, set: FigureSet, raw: string): string | undefined {
  for (const built of Object.values(report.outputs)) {
    for (const section of built.sections) {
      if (checkNumbers(asPrinted(currentBody(built, section.key), pseudonyms), set).unmatched.some((u) => u.raw === raw)) return section.title;
    }
  }
  return undefined;
}

/** Text a Word file shows, from its document part. */
function docxWords(path: string): string {
  const zip = unzipSync(readFileSync(path));
  return Object.entries(zip).filter(([name]) => /^word\/(document|header\d*|footer\d*)\.xml$/.test(name)).map(([, bytes]) => strFromU8(bytes).replace(/<[^>]+>/g, " ")).join(" ");
}

/** Text a workbook shows, from its shared strings and sheets. */
function xlsxWords(path: string): string {
  const zip = unzipSync(readFileSync(path));
  return Object.entries(zip).filter(([name]) => /^xl\/(sharedStrings|worksheets\/sheet\d+)\.xml$/.test(name)).map(([, bytes]) => strFromU8(bytes).replace(/<[^>]+>/g, " ")).join(" ");
}

const unescape = (text: string): string => text.replace(/&#39;|&apos;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");

/**
 * What a make still holds, from the PDFs' pages and the microsite's alike: the
 * repair loop masks both, so a figure only the microsite prints is repaired
 * too, never left to cost the microsite. A check that failed to run isn't a
 * figure to mask.
 */
export function toRepair(layout: Layouts, siteMisses: Array<{ raw: string }>): Array<{ document: string; page: number; raw: string }> {
  return [...heldRaws(layout), ...siteMisses.map((s) => ({ document: MICROSITE, page: 0, raw: s.raw }))].filter((h) => h.raw !== "(whole text)");
}

/** More masks than this in one document, or any on the one-pager, marks the repair heavy. */
export const MASK_CAP = 3;

type Doc = "executive" | "full" | "ceo";

export async function renderReleased(dir: string, approved: ApprovedRun, outDir: string): Promise<ReleasedDocuments | RefusedDocuments> {
  const folder = resolve(dir);
  const out = resolve(outDir);
  assertOutsideGit(folder);
  assertOutsideGit(out);
  const files = {
    raw_notes: join(out, OUTPUT_FILES.rawNotes),
    executive_summary: join(out, OUTPUT_FILES.executive),
    full_health_check: join(out, OUTPUT_FILES.full),
    ceo_one_pager: join(out, OUTPUT_FILES.ceo),
    readout_kit: join(out, OUTPUT_FILES.kit),
    action_lists: join(out, OUTPUT_FILES.actionLists),
    tickets: join(out, OUTPUT_FILES.tickets),
  };
  const paths = { rawNotes: files.raw_notes, executive: files.executive_summary, full: files.full_health_check, ceo: files.ceo_one_pager, kit: files.readout_kit, layout: join(out, OUTPUT_FILES.layout) };

  // One make: the report with every figure found untraceable so far masked in the data, its sections checked
  // and fallen back, its pages printed (with any page-level masks) and checked, and its microsite pages checked.
  const make = async (raws: string[], pageRaws: string[]) => {
    const built = approvedReport(folder, approved, raws);
    if (built.report.refused.length > 0) return { refused: built.report.refused };
    mkdirSync(out, { recursive: true, mode: 0o700 });
    const set = pageDataset(built.report.dataset, built.pseudonyms);
    const fellBack = fallBackSections(built.report, built.pseudonyms, built.engineText, set);
    const masks: Record<Doc, number> = { executive: 0, full: 0, ceo: 0 };
    const pageMasks: Record<Doc, number> = { executive: 0, full: 0, ceo: 0 };
    const layout: PrintedLayout = await printDocuments(built.report, built.pseudonyms, paths, (html, doc) => {
      const page = maskPage(html, pageRaws);
      pageMasks[doc] = page.masked;
      masks[doc] = masksIn(page.html);
      return withheldNote(page.html);
    });
    let site: { ok: true; pages: MicrositePages["pages"] } | { ok: false; reason: string };
    try {
      site = { ok: true, pages: micrositePages(built.report, built.pseudonyms).map((p) => ({ ...p, html: maskPage(p.html, pageRaws).html })) };
    } catch (err) {
      site = { ok: false, reason: err instanceof Error ? err.message : "The pages couldn't be built" };
    }
    const siteMisses = site.ok ? micrositeMisses(site.pages, built.report, built.pseudonyms) : [];
    return { ...built, set, fellBack, layout, site, siteMisses, masks, pageMasks };
  };
  type Made = Exclude<Awaited<ReturnType<typeof make>>, { refused: unknown }>;

  const raws: string[] = [];
  const pageRaws: string[] = [];
  const firstSeen: Array<{ document: string; page: number; raw: string }> = [];
  const held = (m: Made) => toRepair(m.layout, m.siteMisses);
  const note = (found: Array<{ document: string; page: number; raw: string }>): void => {
    for (const h of found) if (!firstSeen.some((x) => x.raw === h.raw)) firstSeen.push(h);
  };
  const first = await make(raws, pageRaws);
  if ("refused" in first) return { refused: first.refused! };
  let made: Made = first;
  // Masked in the run's data first, twice at most; what no stored field holds is then masked where it's printed.
  for (let round = 0; round < REPAIR_ROUNDS; round++) {
    const found = held(made).filter((h) => !raws.includes(h.raw));
    if (found.length === 0) break;
    note(found);
    raws.push(...new Set(found.map((h) => h.raw)));
    made = (await make(raws, pageRaws)) as Made;
  }
  const left = held(made).filter((h) => !pageRaws.includes(h.raw));
  if (left.length) {
    note(left);
    pageRaws.push(...new Set(left.map((h) => h.raw)));
    made = (await make(raws, pageRaws)) as Made;
  }
  const { report, pseudonyms, set, layout, site, siteMisses } = made;

  // The rest of the outputs, made from the same masked report, so they're under the same gate as the decks (rule 10).
  writeActionFiles(report, pseudonyms, folder, { workbook: files.action_lists, tickets: files.tickets });
  const study = await renderCaseStudyDocx(report, pseudonyms);
  const caseStudy = study ? join(out, "case-study-draft.docx") : undefined;
  if (study && caseStudy) writeFileSync(caseStudy, study, { mode: 0o600 });
  let digest: ReleasedDocuments["digest"];
  if (approved.digest) {
    try {
      digest = { ok: true, digest: buildDigest({ report, pseudonyms, inventory: approved.digest.inventory, madeAt: approved.digest.madeAt }) };
    } catch (err) {
      digest = { ok: false, error: err instanceof Error ? err.name : "Error" };
    }
  }

  // The final check (decision 682, item 1). The PDFs are checked whole: anything still held fails the make, named.
  const stillHeld: HeldFigure[] = heldFigures(layout).map((h, i) => {
    const section = sectionHolding(report, pseudonyms, set, heldRaws(layout)[i]!.raw);
    return section ? { ...h, section } : h;
  });
  // The Word files, the action lists and the tickets hold record names, ids, references and queries no dataset
  // holds, so they can't be checked whole. They're made from the masked report; a figure masked in the data
  // still showing in one of them is said in the log, never a reason to hold the make: a bare "4" can be a
  // record's "Step 4".
  const patterns: RegExp[] = raws.map((raw) => heldPattern(raw));
  const stillIn: string[] = [];
  const elsewhere: Array<[string, () => string]> = [
    ["the raw notes", () => docxWords(files.raw_notes)],
    ["the readout kit", () => docxWords(files.readout_kit)],
    ["the action lists", () => xlsxWords(files.action_lists)],
    ["the tickets", () => readFileSync(files.tickets, "utf8")],
    ...(caseStudy ? [["the case study draft", () => docxWords(caseStudy)] as [string, () => string]] : []),
  ];
  for (const [document, read] of elsewhere) {
    try {
      const text = unescape(read());
      if (patterns.some((p) => new RegExp(p.source, p.flags.replace("g", "")).test(text))) stillIn.push(document);
    } catch {
      stillIn.push(document);
    }
  }

  const leftOut: LeftOut[] = firstSeen.map((h) => {
    const at = made.repairs.find((r) => r.raw === h.raw);
    return { document: h.document, page: h.page, kind: kindOf(h.raw, set), ...(at?.ruleId ? { ruleId: at.ruleId } : {}), ...(at?.metricId ? { metricId: at.metricId } : {}), ...(at?.stepId ? { stepId: at.stepId } : {}), field: at ? at.field : "page" };
  });
  const fellBack = made.fellBack;
  // A microsite still failing its own check isn't made, as before this decision; the documents are, and the warning and alert say so.
  const micrositeHeld = site.ok && siteMisses.length > 0;
  const microsite: ReleasedDocuments["microsite"] = site.ok ? (micrositeHeld ? { ok: false, reason: "A microsite page has a figure the number check can't trace, so the microsite wasn't made" } : { ok: true, pages: { pages: site.pages } }) : site;
  const fallBackHeavy = Object.values(report.outputs).some((b) => b.sections.length > 0 && fellBack.filter((f) => f.output === b.output).length * 4 > b.sections.length);
  const heavy = made.masks.ceo > 0 || made.masks.executive > MASK_CAP || made.masks.full > MASK_CAP || fallBackHeavy;
  const any = fellBack.length > 0 || leftOut.length > 0 || micrositeHeld || Object.values(made.masks).some((n) => n > 0);
  return {
    files,
    warnings: layoutWarnings(layout),
    held: stillHeld,
    fellBack,
    leftOut,
    repairs: heavy ? "heavy" : any ? "some" : "none",
    masks: made.masks,
    ...(micrositeHeld ? { micrositeHeld: true as const } : {}),
    ...(stillIn.length ? { stillIn } : {}),
    microsite,
    ...(caseStudy ? { caseStudy } : {}),
    ...(digest ? { digest } : {}),
    surveyMetrics: surveyMetricsOf(report),
    baselineRefused: report.baselineRefused,
    baselineGone: made.baselineGone,
    baselineUnapproved: made.baselineUnapproved,
  };
}
