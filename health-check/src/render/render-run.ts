import { existsSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { FACTS_FILE } from "../extract/facts.ts";
import { STAGE_MEANINGS_FILE } from "../domains/historic-pipeline/cohort.ts";
import { RULES_FILE, RulesRun } from "../engine/rules-run.ts";
import { FactsFile, summariseFacts } from "../facts/summary.ts";
import { join, resolve } from "node:path";
import { z } from "zod";
import { assertOutsideGit } from "../engine/run.ts";
import { baselineRun } from "./baseline.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { Intake } from "../narrative/intake.ts";
import { refusalReason, type OverrideRefusal } from "../narrative/overrides.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import { pageDataset } from "./page-dataset.ts";
import { Pseudonymiser, type PseudonymEntry } from "../privacy/pseudonymiser.ts";
import { Calibration, Finding, ManualStep, Metric, NumberOverride, Run, Score } from "../schemas/index.ts";
import { EXECUTIVE_MAX_PAGES, executiveHtml, executiveMaxPages } from "./executive.ts";
import { deckRangeFor, fullReportHtml } from "./full-report.ts";
import { launchBrowser, pdfPageCount, renderPdf } from "./pdf.ts";
import type { LayoutProblem, LayoutReport } from "./layout.ts";
import { renderRawNotesDocx } from "./raw-notes-docx.ts";
import { ONE_PAGER_PAGES, onePagerHtml } from "./one-pager.ts";
import { renderReadoutKitDocx } from "./readout-kit.ts";
import { Survey } from "../survey/questions.ts";
import { SURVEY_METRIC_IDS } from "../survey/summary.ts";
import { loadActionLists, recordCounts, writeActionFiles } from "../actions/index.ts";
import { Interviews } from "../interviews/questions.ts";

// What each stage the calibration doesn't name was taken to mean (DECISIONS.md 600).
const StageMeanings = z.record(z.string(), z.enum(["open", "won", "lost"]));

/** The team survey as it stood when the sections were built, comments left out (DECISIONS.md 147). */
export const SURVEY_FILE = "survey.json";

/**
 * The folder's metrics.json with the team survey's metrics as the report made
 * them (DECISIONS.md 645): the engine's own as they are, any survey metric an
 * earlier write left dropped, then these. So the folder holds every metric its
 * sections cite, as the console's reader checks. None leaves the engine's alone.
 */
export function keepSurveyMetrics(dir: string, survey: Metric[]): void {
  const path = join(dir, "metrics.json");
  // As written, not through the schema, so the engine's own metrics keep every byte.
  const own = (JSON.parse(readFileSync(path, "utf8")) as Array<{ id: string }>).filter((m) => !SURVEY_METRIC_IDS.includes(m.id));
  writeFileSync(path, `${JSON.stringify([...own, ...survey], null, 2)}\n`, { mode: 0o600 });
}

/**
 * The stakeholder interviews as they stood when the sections were built
 * (DECISIONS.md 180). Kept with the intake, outside the raw extracts, so it goes
 * when the run folder goes, 12 months after release, as the intake does.
 */
export const INTERVIEWS_FILE = "interviews.json";

// Turns a finished run folder into its three documents. Everything is written to
// that folder, outside the repo, because this is where names exist in the clear.

export const EXECUTIVE_PAGES = { min: 5, max: EXECUTIVE_MAX_PAGES } as const;

export const OUTPUT_FILES = {
  rawNotes: "raw-notes.docx",
  executive: "executive-summary.pdf",
  full: "full-health-check.pdf",
  /** The CEO one-pager, built to be forwarded (DECISIONS.md 137). */
  ceo: "ceo-one-pager.pdf",
  /** The consultant's talking points. Black Cloud only, never the customer's (DECISIONS.md 138). */
  kit: "readout-kit.docx",
  /** The action lists: every record to fix, and a ticket an issue (DECISIONS.md 191 to 200). */
  actionLists: "action-lists.xlsx",
  tickets: "tickets.csv",
  sections: "report-sections.json",
  layout: "layout-report.json",
  rejections: "narrative-rejections.json",
} as const;

const read = <T>(path: string): T => JSON.parse(readFileSync(path, "utf8")) as T;
const readOptional = <T>(path: string): T | null => (existsSync(path) ? read<T>(path) : null);
/** A file only the worker's own user can read: the documents hold real names. */
export const write = (path: string, data: string | Buffer): void => writeFileSync(path, data, { mode: 0o600 });

/**
 * Which corrections of a re-check's earlier health check it is built with
 * (DECISIONS.md 528). "kept": the rows the re-check's sections were drafted
 * with, kept in its own folder; none when there's no file, so a re-check
 * drafted before then is built as read. "live": the earlier folder's
 * overrides.json as it stands, read only where the report is built from
 * nothing else: the run job's draft, which then keeps them, and a pulse.
 */
export type BaselineRows = "kept" | "live";

type LoadedBaseline = { baseline: ReportInput | null; rowsUnread: boolean; gone: boolean };

/**
 * A re-check's first health check, from the folder beside it, as the report
 * compares against it. Null if it's gone (retention), unreadable, or another
 * org's. Read live, corrections that can't be read count as none and say so
 * (`rowsUnread`): the draft then holds the figures as read, as the documents
 * will with no kept file (DECISIONS.md 531). Kept rows that can't be read
 * leave no baseline: they're the ones the drafts were made with, and nothing
 * else is (528). A later build follows the draft's record of whether it read
 * the earlier health check (534): not read, there's none, and the earlier
 * folder isn't opened; a record that can't be read, none either; read but
 * unreadable now, none, and `gone` says so for the log.
 */
function loadBaseline(dir: string, run: Run, rows: BaselineRows): LoadedBaseline {
  const none: LoadedBaseline = { baseline: null, rowsUnread: false, gone: false };
  let record: boolean | null = null;
  if (rows === "kept") {
    try {
      record = readBaselineRead(dir);
    } catch {
      return none;
    }
    if (record === false) return none;
  }
  // Only a draft that read it can have printed a comparison the documents now lack.
  const lost: LoadedBaseline = { ...none, gone: record === true };
  const baseline = baselineRun(dir, run);
  if (!baseline || !existsSync(join(baseline.dir, "findings.json"))) return lost;
  let overrides: NumberOverride[];
  let rowsUnread = false;
  try {
    overrides = rows === "live" ? readOverrides(baseline.dir) : readBaselineOverrides(dir);
  } catch {
    if (rows === "kept") return none;
    overrides = [];
    rowsUnread = true;
  }
  try {
    return { baseline: loadRun(baseline.dir, { baseline: false, overrides }).input, rowsUnread, gone: false };
  } catch {
    return lost;
  }
}

/**
 * Loads a run folder. Everything is validated, so a damaged file stops here.
 * `overrides` stands in for the folder's own overrides.json, which is then not
 * read; `baselineRows` says which corrections a re-check's earlier health
 * check is read with, by default the ones kept with its drafts.
 * `baselineRowsUnread` and `baselineGone` are for the log alone, and nothing
 * built reads them: the earlier folder's corrections, read live, couldn't be
 * read; the earlier health check the draft read can't be read now.
 */
export function loadRun(dir: string, options: { baseline?: boolean; baselineRows?: BaselineRows; overrides?: NumberOverride[] } = {}): { input: ReportInput; pseudonyms: Pseudonymiser; baselineRowsUnread: boolean; baselineGone: boolean } {
  const intake = readOptional<unknown>(join(dir, "intake.json"));
  const survey = readOptional<unknown>(join(dir, SURVEY_FILE));
  const interviews = readOptional<unknown>(join(dir, INTERVIEWS_FILE));
  const facts = readOptional<unknown>(join(dir, FACTS_FILE));
  const stageMeanings = readOptional<unknown>(join(dir, STAGE_MEANINGS_FILE));
  const rules = readOptional<unknown>(join(dir, RULES_FILE));
  const input: ReportInput = {
    run: Run.parse(read(join(dir, "run.json"))),
    calibration: Calibration.parse(read(join(dir, "calibration.json"))),
    findings: z.array(Finding).parse(read(join(dir, "findings.json"))),
    metrics: z.array(Metric).parse(read(join(dir, "metrics.json"))),
    manualSteps: z.array(ManualStep).parse(read(join(dir, "manual-steps.json"))),
    scores: z.array(Score).parse(readOptional(join(dir, "scores.json")) ?? []),
    overrides: options.overrides ?? readOverrides(dir),
    intake: intake ? Intake.parse(intake) : null,
    survey: survey ? Survey.parse(survey) : null,
    recordCounts: recordCounts(loadActionLists(dir)),
    interviews: interviews ? Interviews.parse(interviews).interviews : null,
    facts: facts ? summariseFacts(FactsFile.parse(facts)) : null,
    stageMeanings: stageMeanings ? StageMeanings.parse(stageMeanings) : null,
    // A folder analysed before decision 718 has none, and its digest then lists none.
    rules: rules ? RulesRun.parse(rules) : null,
  };
  let baselineRowsUnread = false;
  let baselineGone = false;
  if (options.baseline !== false) {
    const loaded = loadBaseline(dir, input.run, options.baselineRows ?? "kept");
    input.baseline = loaded.baseline;
    baselineRowsUnread = loaded.rowsUnread;
    baselineGone = loaded.gone;
  }
  return { input, pseudonyms: loadPseudonymMap(dir), baselineRowsUnread, baselineGone };
}

/**
 * The override rows the documents were last made with, which the documents job
 * keeps in the run folder (DECISIONS.md 522). None when there's no file; a
 * damaged one throws, so nothing is read from half a file.
 */
export const OVERRIDES_FILE = "overrides.json";
export function readOverrides(dir: string): NumberOverride[] {
  return z.array(NumberOverride).parse(readOptional(join(dir, OVERRIDES_FILE)) ?? []);
}

/**
 * A re-check's earlier health check's corrections as its sections were drafted
 * with them, kept in the re-check's folder by the run job (DECISIONS.md 528).
 * Absent: none.
 */
export const BASELINE_OVERRIDES_FILE = "baseline-overrides.json";
export function readBaselineOverrides(dir: string): NumberOverride[] {
  return z.array(NumberOverride).parse(readOptional(join(dir, BASELINE_OVERRIDES_FILE)) ?? []);
}

/**
 * Whether a re-check's draft read its earlier health check, which the run job
 * records in the re-check's folder at each draft (DECISIONS.md 534), so every
 * later build reads what the draft read. Null with no file: drafted before
 * then, built as before. Anything but `{"read":true}` or `{"read":false}`
 * throws, so a damaged record is never taken for either.
 */
export const BASELINE_READ_FILE = "baseline-read.json";
const BaselineRead = z.object({ read: z.boolean() }).strict();
export function readBaselineRead(dir: string): boolean | null {
  const path = join(dir, BASELINE_READ_FILE);
  return existsSync(path) ? BaselineRead.parse(read(path)).read : null;
}

/** The record, written as the kept rows are. */
export function writeBaselineRead(path: string, wasRead: boolean): void {
  writeByRename(path, JSON.stringify(BaselineRead.parse({ read: wasRead })));
}

/**
 * Override rows written to a run folder, only their own fields, private to
 * the worker, and by a rename so a reader never meets half a file: an
 * unreadable one leaves a re-check with no baseline. The temporary name is
 * fixed: both writers hold the run's lock, and one a crash left is overwritten.
 */
export function writeRows(path: string, rows: NumberOverride[]): void {
  writeByRename(path, JSON.stringify(rows.map((row) => NumberOverride.parse(row))));
}

function writeByRename(path: string, text: string): void {
  const writing = `${path}.writing`;
  try {
    writeFileSync(writing, text, { mode: 0o600 });
    renameSync(writing, path);
  } finally {
    rmSync(writing, { force: true });
  }
}

/** The run's map from token to name, for rendering only (rule 7). Throws if it's missing or damaged. */
export function loadPseudonymMap(dir: string): Pseudonymiser {
  return new Pseudonymiser(read<PseudonymEntry[]>(join(dir, "pseudonym-map.json")));
}

/**
 * A corrected figure in the folder's `overrides.json` couldn't be carried into
 * its finding's wording. The message is the reason the documents job gives,
 * line by line (DECISIONS.md 512), and holds no token.
 */
export class OverridesRefused extends Error {
  readonly refused: OverrideRefusal[];
  constructor(refused: OverrideRefusal[]) {
    super(refusalReason(refused));
    this.name = "OverridesRefused";
    this.refused = refused;
  }
}

export { pageDataset };

export type PrintedLayout = { executive: LayoutReport; full: LayoutReport; ceo: LayoutReport };
/** For the log, and `hc render`'s errors, when a re-check's draft read its earlier health check and a later build can't (DECISIONS.md 534). No path or error. */
export const BASELINE_GONE = "the earlier health check can't be read now, so the documents have no comparison with it.";
/** For the log when a re-check's folder gives an earlier health check and its approved Word notes hold no comparison with it, so the documents have none (DECISIONS.md 535). No path, id or error. */
export const BASELINE_UNAPPROVED = "the approved sections hold no comparison with the earlier health check, so the documents have none.";

/** `baselineGone`: the earlier health check its draft read can't be read now, for the caller to say (DECISIONS.md 534). */
export type RenderResult = { report: BuiltReport; layout: PrintedLayout; rejections: Rejection[]; passed: boolean; files: string[]; baselineGone: boolean };

/**
 * With a client, sections are drafted by Claude first. Without one, they're the
 * engine's own text. Throws `OverridesRefused`, having written nothing, when an
 * override can't be carried.
 */
export async function renderRun(dir: string, options: { client?: NarrativeClient } = {}): Promise<RenderResult> {
  const folder = resolve(dir);
  assertOutsideGit(folder);
  const { input, pseudonyms, baselineGone } = loadRun(folder);
  const report = buildReport(input);
  // Before Claude is asked or a file is written, as the documents job stops.
  if (report.refused.length > 0) throw new OverridesRefused(report.refused);
  const rejections = options.client ? await applyNarrative(report, options.client, pseudonyms) : [];

  const paths = {
    rawNotes: join(folder, OUTPUT_FILES.rawNotes),
    executive: join(folder, OUTPUT_FILES.executive),
    full: join(folder, OUTPUT_FILES.full),
    ceo: join(folder, OUTPUT_FILES.ceo),
    kit: join(folder, OUTPUT_FILES.kit),
    actionLists: join(folder, OUTPUT_FILES.actionLists),
    tickets: join(folder, OUTPUT_FILES.tickets),
    sections: join(folder, OUTPUT_FILES.sections),
    layout: join(folder, OUTPUT_FILES.layout),
    rejections: join(folder, OUTPUT_FILES.rejections),
  };
  const layout = await printDocuments(report, pseudonyms, paths);
  writeActionFiles(report, pseudonyms, folder, { workbook: paths.actionLists, tickets: paths.tickets });
  write(paths.sections, `${JSON.stringify(Object.values(report.outputs), null, 2)}\n`);
  write(paths.rejections, `${JSON.stringify(rejections, null, 2)}\n`);
  return { report, layout, rejections, passed: layout.executive.passed && layout.full.passed && layout.ceo.passed, files: Object.values(paths), baselineGone };
}

/** Prints the three PDFs, the Word notes and the readout kit from a built report, and writes what the layout checks found. */
export async function printDocuments(
  report: BuiltReport,
  pseudonyms: Pseudonymiser,
  paths: { rawNotes: string; executive: string; full: string; ceo: string; kit: string; layout: string },
  /** Applied to each page's HTML before it's printed and checked: the documents job's repair (DECISIONS.md 682). */
  transform: (html: string, doc: "executive" | "full" | "ceo") => string = (html) => html,
): Promise<PrintedLayout> {
  const scope = pageDataset(report.dataset, pseudonyms);
  const browser = await launchBrowser();
  try {
    const executive = await renderPdf(browser, transform(executiveHtml(report, pseudonyms), "executive"), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: scope, carryOver: true });
    const full = await renderPdf(browser, transform(fullReportHtml(report, pseudonyms), "full"), { minPages: deckRangeFor(report.input.run).min, maxPages: deckRangeFor(report.input.run).max, dataset: scope, carryOver: true });
    // Exactly one page, or it isn't a one-pager.
    const ceo = await renderPdf(browser, transform(onePagerHtml(report, pseudonyms), "ceo"), { minPages: ONE_PAGER_PAGES.min, maxPages: ONE_PAGER_PAGES.max, dataset: scope });
    const pagesMatch = (result: typeof executive, name: string): LayoutProblem[] => {
      const printed = pdfPageCount(result.pdf);
      return printed === result.layout.pages ? [] : [{ page: 0, check: "page count", detail: `${name} printed ${printed} pages from ${result.layout.pages}` }];
    };
    executive.layout.problems.push(...pagesMatch(executive, "The executive summary"));
    full.layout.problems.push(...pagesMatch(full, "The full report"));
    ceo.layout.problems.push(...pagesMatch(ceo, "The one page summary"));
    executive.layout.passed = executive.layout.problems.length === 0;
    full.layout.passed = full.layout.problems.length === 0;
    ceo.layout.passed = ceo.layout.problems.length === 0;

    write(paths.rawNotes, await renderRawNotesDocx(report, pseudonyms));
    write(paths.executive, executive.pdf);
    write(paths.full, full.pdf);
    write(paths.ceo, ceo.pdf);
    write(paths.kit, await renderReadoutKitDocx(report, pseudonyms));
    write(paths.layout, `${JSON.stringify({ executive: executive.layout, full: full.layout, ceo: ceo.layout }, null, 2)}\n`);
    return { executive: executive.layout, full: full.layout, ceo: ceo.layout };
  } finally {
    await browser.close();
  }
}
