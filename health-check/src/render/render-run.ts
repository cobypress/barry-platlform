import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { z } from "zod";
import { assertOutsideGit } from "../engine/run.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { Intake } from "../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../privacy/pseudonymiser.ts";
import { Calibration, Finding, ManualStep, Metric, NumberOverride, Run, Score } from "../schemas/index.ts";
import { executiveHtml } from "./executive.ts";
import { DECK_RANGE, fullReportHtml } from "./full-report.ts";
import { launchBrowser, pdfPageCount, renderPdf } from "./pdf.ts";
import type { LayoutProblem, LayoutReport } from "./layout.ts";
import { renderRawNotesDocx } from "./raw-notes-docx.ts";

// Turns a finished run folder into its three documents. Everything is written to
// that folder, outside the repo, because this is where names exist in the clear.

export const EXECUTIVE_PAGES = { min: 5, max: 8 } as const;

export const OUTPUT_FILES = {
  rawNotes: "raw-notes.docx",
  executive: "executive-summary.pdf",
  full: "full-health-check.pdf",
  sections: "report-sections.json",
  layout: "layout-report.json",
  rejections: "narrative-rejections.json",
} as const;

const read = <T>(path: string): T => JSON.parse(readFileSync(path, "utf8")) as T;
const readOptional = <T>(path: string): T | null => (existsSync(path) ? read<T>(path) : null);
const write = (path: string, data: string | Buffer): void => writeFileSync(path, data, { mode: 0o600 });

/** Loads a run folder. Everything is validated, so a damaged file stops here. */
export function loadRun(dir: string): { input: ReportInput; pseudonyms: Pseudonymiser } {
  const intake = readOptional<unknown>(join(dir, "intake.json"));
  const input: ReportInput = {
    run: Run.parse(read(join(dir, "run.json"))),
    calibration: Calibration.parse(read(join(dir, "calibration.json"))),
    findings: z.array(Finding).parse(read(join(dir, "findings.json"))),
    metrics: z.array(Metric).parse(read(join(dir, "metrics.json"))),
    manualSteps: z.array(ManualStep).parse(read(join(dir, "manual-steps.json"))),
    scores: z.array(Score).parse(readOptional(join(dir, "scores.json")) ?? []),
    overrides: z.array(NumberOverride).parse(readOptional(join(dir, "overrides.json")) ?? []),
    intake: intake ? Intake.parse(intake) : null,
  };
  return { input, pseudonyms: new Pseudonymiser(read<PseudonymEntry[]>(join(dir, "pseudonym-map.json"))) };
}

export type RenderResult = { report: BuiltReport; layout: { executive: LayoutReport; full: LayoutReport }; rejections: Rejection[]; passed: boolean; files: string[] };

/** With a client, sections are drafted by Claude first. Without one, they're the engine's own text. */
export async function renderRun(dir: string, options: { client?: NarrativeClient } = {}): Promise<RenderResult> {
  const folder = resolve(dir);
  assertOutsideGit(folder);
  const { input, pseudonyms } = loadRun(folder);
  const report = buildReport(input);
  const rejections = options.client ? await applyNarrative(report, options.client, pseudonyms) : [];
  const scope = report.dataset;

  const browser = await launchBrowser();
  try {
    const executive = await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: EXECUTIVE_PAGES.max, dataset: scope });
    const full = await renderPdf(browser, fullReportHtml(report, pseudonyms), { minPages: DECK_RANGE.min, maxPages: DECK_RANGE.max, dataset: scope });
    const pagesMatch = (result: typeof executive, name: string): LayoutProblem[] => {
      const printed = pdfPageCount(result.pdf);
      return printed === result.layout.pages ? [] : [{ page: 0, check: "page count", detail: `${name} printed ${printed} pages from ${result.layout.pages}` }];
    };
    executive.layout.problems.push(...pagesMatch(executive, "The executive summary"));
    full.layout.problems.push(...pagesMatch(full, "The full report"));
    executive.layout.passed = executive.layout.problems.length === 0;
    full.layout.passed = full.layout.problems.length === 0;

    const paths = {
      rawNotes: join(folder, OUTPUT_FILES.rawNotes),
      executive: join(folder, OUTPUT_FILES.executive),
      full: join(folder, OUTPUT_FILES.full),
      sections: join(folder, OUTPUT_FILES.sections),
      layout: join(folder, OUTPUT_FILES.layout),
      rejections: join(folder, OUTPUT_FILES.rejections),
    };
    write(paths.rawNotes, await renderRawNotesDocx(report, pseudonyms));
    write(paths.executive, executive.pdf);
    write(paths.full, full.pdf);
    write(paths.sections, `${JSON.stringify(Object.values(report.outputs), null, 2)}\n`);
    write(paths.layout, `${JSON.stringify({ executive: executive.layout, full: full.layout }, null, 2)}\n`);
    write(paths.rejections, `${JSON.stringify(rejections, null, 2)}\n`);
    return { report, layout: { executive: executive.layout, full: full.layout }, rejections, passed: executive.layout.passed && full.layout.passed, files: Object.values(paths) };
  } finally {
    await browser.close();
  }
}
