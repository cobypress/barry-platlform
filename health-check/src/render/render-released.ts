import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { buildMicrositePages, type MicrositePages } from "../microsite/pages.ts";
import type { Intake } from "../narrative/intake.ts";
import type { OverrideRefusal } from "../narrative/overrides.ts";
import { buildReport, type BuiltReport, type ReportOutputKey } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type NumberOverride, type ReportSection, type SectionVersion } from "../schemas/index.ts";
import type { LayoutProblem, LayoutReport } from "./layout.ts";
import { renderCaseStudyDocx } from "./case-study.ts";
import { writeFileSync } from "node:fs";
import { loadRun, OUTPUT_FILES, printDocuments } from "./render-run.ts";
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
  /** Each figure the number check couldn't trace on a printed page. Any at all holds the documents back (DECISIONS.md 430). */
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
export type HeldFigure = { document: string; page: number; figure: string; reason: string; masked?: true };

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
  return `${where(held.document, held.page)}: ${held.figure}${held.reason ? `: ${held.reason}` : ""}`;
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
 */
export function approvedReport(dir: string, approved: ApprovedRun): { report: BuiltReport; pseudonyms: Pseudonymiser; baselineGone: boolean; baselineUnapproved: boolean } {
  const loaded = loadRun(dir);
  const { input, pseudonyms } = loaded;
  const held = approved.sections.some((s) => s.output === "raw_notes" && s.key === "progress");
  const baselineUnapproved = input.baseline != null && !held;
  const baselineGone = loaded.baselineGone && held;
  const report = buildReport({ ...input, ...(baselineUnapproved ? { baseline: null } : {}), overrides: approved.overrides, intake: approved.intake ?? input.intake, preparedBy: approved.preparedBy ?? null, benchmark: approved.benchmark ?? null });
  for (const built of Object.values(report.outputs)) {
    const sections = approved.sections.filter((s) => s.output === built.output).sort((a, b) => a.order - b.order);
    if (!canRelease(sections).ok) throw new Error(`Not every section of the ${built.output.replace(/_/g, " ")} is approved, so no documents were made`);
    const current = new Set(sections.map((s) => s.currentVersionId));
    const versions = approved.versions.filter((v) => current.has(v.id));
    if (versions.length !== sections.length) throw new Error(`A section of the ${built.output.replace(/_/g, " ")} has no approved text, so no documents were made`);
    built.sections = sections;
    built.versions = versions;
  }
  return { report, pseudonyms, baselineGone, baselineUnapproved };
}

export async function renderReleased(dir: string, approved: ApprovedRun, outDir: string): Promise<ReleasedDocuments | RefusedDocuments> {
  const folder = resolve(dir);
  const out = resolve(outDir);
  assertOutsideGit(folder);
  assertOutsideGit(out);
  const { report, pseudonyms, baselineGone, baselineUnapproved } = approvedReport(folder, approved);
  // Before anything is written: a refused make never leaves documents in the folder beside its failed row.
  if (report.refused.length > 0) return { refused: report.refused };
  mkdirSync(out, { recursive: true, mode: 0o700 });
  const files = {
    raw_notes: join(out, OUTPUT_FILES.rawNotes),
    executive_summary: join(out, OUTPUT_FILES.executive),
    full_health_check: join(out, OUTPUT_FILES.full),
    ceo_one_pager: join(out, OUTPUT_FILES.ceo),
    readout_kit: join(out, OUTPUT_FILES.kit),
    action_lists: join(out, OUTPUT_FILES.actionLists),
    tickets: join(out, OUTPUT_FILES.tickets),
  };
  const layout = await printDocuments(report, pseudonyms, {
    rawNotes: files.raw_notes,
    executive: files.executive_summary,
    full: files.full_health_check,
    ceo: files.ceo_one_pager,
    kit: files.readout_kit,
    layout: join(out, OUTPUT_FILES.layout),
  });
  // Made from the same approved report, so they're under the same gate as the decks (rule 10).
  writeActionFiles(report, pseudonyms, folder, { workbook: files.action_lists, tickets: files.tickets });
  let microsite: ReleasedDocuments["microsite"];
  try {
    microsite = { ok: true, pages: buildMicrositePages(report, pseudonyms) };
  } catch (err) {
    microsite = { ok: false, reason: err instanceof Error ? err.message : "The pages couldn't be built" };
  }
  const study = await renderCaseStudyDocx(report, pseudonyms);
  const caseStudy = study ? join(out, "case-study-draft.docx") : undefined;
  if (study && caseStudy) writeFileSync(caseStudy, study, { mode: 0o600 });
  // From the report just printed, so its figures are the documents' own. Never a reason to fail the make.
  let digest: ReleasedDocuments["digest"];
  if (approved.digest) {
    try {
      digest = { ok: true, digest: buildDigest({ report, pseudonyms, inventory: approved.digest.inventory, madeAt: approved.digest.madeAt }) };
    } catch (err) {
      digest = { ok: false, error: err instanceof Error ? err.name : "Error" };
    }
  }
  return { files, warnings: layoutWarnings(layout), held: heldFigures(layout), microsite, ...(caseStudy ? { caseStudy } : {}), ...(digest ? { digest } : {}), baselineRefused: report.baselineRefused, baselineGone, baselineUnapproved };
}
