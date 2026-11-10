import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { FACTS_FILE } from "../extract/facts.ts";
import { FactsFile, summariseFacts } from "../facts/summary.ts";
import { join, resolve } from "node:path";
import { z } from "zod";
import { assertOutsideGit } from "../engine/run.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { Intake } from "../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
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
import { loadActionLists, recordCounts, writeActionFiles } from "../actions/index.ts";
import { Interviews } from "../interviews/questions.ts";

/** The team survey as it stood when the sections were built, comments left out (DECISIONS.md 147). */
export const SURVEY_FILE = "survey.json";
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
 * A re-check's first health check, from the folder beside it, as the report
 * compares against it. Null if it's gone (retention), unreadable, or another org's.
 */
function loadBaseline(dir: string, run: Run): ReportInput | null {
  if (!run.baselineRunId || !/^[A-Za-z0-9_-]+$/.test(run.baselineRunId)) return null;
  const other = join(dir, "..", run.baselineRunId);
  if (!existsSync(join(other, "run.json")) || !existsSync(join(other, "findings.json"))) return null;
  try {
    const { input } = loadRun(other, { baseline: false });
    return input.run.orgId.slice(0, 15) === run.orgId.slice(0, 15) ? input : null;
  } catch {
    return null;
  }
}

/** Loads a run folder. Everything is validated, so a damaged file stops here. */
export function loadRun(dir: string, options: { baseline?: boolean } = {}): { input: ReportInput; pseudonyms: Pseudonymiser } {
  const intake = readOptional<unknown>(join(dir, "intake.json"));
  const survey = readOptional<unknown>(join(dir, SURVEY_FILE));
  const interviews = readOptional<unknown>(join(dir, INTERVIEWS_FILE));
  const facts = readOptional<unknown>(join(dir, FACTS_FILE));
  const input: ReportInput = {
    run: Run.parse(read(join(dir, "run.json"))),
    calibration: Calibration.parse(read(join(dir, "calibration.json"))),
    findings: z.array(Finding).parse(read(join(dir, "findings.json"))),
    metrics: z.array(Metric).parse(read(join(dir, "metrics.json"))),
    manualSteps: z.array(ManualStep).parse(read(join(dir, "manual-steps.json"))),
    scores: z.array(Score).parse(readOptional(join(dir, "scores.json")) ?? []),
    overrides: z.array(NumberOverride).parse(readOptional(join(dir, "overrides.json")) ?? []),
    intake: intake ? Intake.parse(intake) : null,
    survey: survey ? Survey.parse(survey) : null,
    recordCounts: recordCounts(loadActionLists(dir)),
    interviews: interviews ? Interviews.parse(interviews).interviews : null,
    facts: facts ? summariseFacts(FactsFile.parse(facts)) : null,
  };
  if (options.baseline !== false) input.baseline = loadBaseline(dir, input.run);
  return { input, pseudonyms: new Pseudonymiser(read<PseudonymEntry[]>(join(dir, "pseudonym-map.json"))) };
}

export type PrintedLayout = { executive: LayoutReport; full: LayoutReport; ceo: LayoutReport };
export type RenderResult = { report: BuiltReport; layout: PrintedLayout; rejections: Rejection[]; passed: boolean; files: string[] };

/** With a client, sections are drafted by Claude first. Without one, they're the engine's own text. */
export async function renderRun(dir: string, options: { client?: NarrativeClient } = {}): Promise<RenderResult> {
  const folder = resolve(dir);
  assertOutsideGit(folder);
  const { input, pseudonyms } = loadRun(folder);
  const report = buildReport(input);
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
  return { report, layout, rejections, passed: layout.executive.passed && layout.full.passed && layout.ceo.passed, files: Object.values(paths) };
}

/** Prints the three PDFs, the Word notes and the readout kit from a built report, and writes what the layout checks found. */
export async function printDocuments(
  report: BuiltReport,
  pseudonyms: Pseudonymiser,
  paths: { rawNotes: string; executive: string; full: string; ceo: string; kit: string; layout: string },
): Promise<PrintedLayout> {
  const scope = report.dataset;
  const browser = await launchBrowser();
  try {
    const executive = await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: scope, carryOver: true });
    const full = await renderPdf(browser, fullReportHtml(report, pseudonyms), { minPages: deckRangeFor(report.input.run).min, maxPages: deckRangeFor(report.input.run).max, dataset: scope, carryOver: true });
    // Exactly one page, or it isn't a one-pager.
    const ceo = await renderPdf(browser, onePagerHtml(report, pseudonyms), { minPages: ONE_PAGER_PAGES.min, maxPages: ONE_PAGER_PAGES.max, dataset: scope });
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
