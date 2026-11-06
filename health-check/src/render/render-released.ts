import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import type { BenchmarkInput } from "../benchmark/compare.ts";
import { buildMicrositePages, type MicrositePages } from "../microsite/pages.ts";
import type { Intake } from "../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportOutputKey } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type NumberOverride, type ReportSection, type SectionVersion } from "../schemas/index.ts";
import type { LayoutReport } from "./layout.ts";
import { renderCaseStudyDocx } from "./case-study.ts";
import { writeFileSync } from "node:fs";
import { loadRun, OUTPUT_FILES, printDocuments } from "./render-run.ts";
import { writeActionFiles } from "../actions/index.ts";

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
};

/** The report's three outputs, then the one-pager and the action lists for the customer, and the kit for Black Cloud only. */
export type DocumentKey = ReportOutputKey | "ceo_one_pager" | "action_lists" | "tickets" | "readout_kit";

export type ReleasedDocuments = {
  files: Record<DocumentKey, string>;
  /** What the layout checks found. The files are still written, for a person to look over. */
  warnings: string[];
  /** The customer microsite's pages, or why they couldn't be made. Left out by renderers that don't make them. */
  microsite?: { ok: true; pages: MicrositePages } | { ok: false; reason: string };
  /** A case study draft, only for a re-check whose customer agreed to be one. For Black Cloud's own folder. */
  caseStudy?: string;
};

const DOCUMENT_NAMES = { executive: "The executive summary", full: "The full health check", ceo: "The one page summary" } as const;

/**
 * One line per layout problem, without the check's detail: an empty-page
 * detail quotes the page's text, which has names in it by now, and these lines
 * go to the console. The full report stays in the run folder.
 */
export function layoutWarnings(layout: { executive: LayoutReport; full: LayoutReport; ceo?: LayoutReport }): string[] {
  return (Object.keys(DOCUMENT_NAMES) as Array<keyof typeof DOCUMENT_NAMES>).flatMap((doc) =>
    (layout[doc]?.problems ?? []).map((p) => (p.page === 0 ? `${DOCUMENT_NAMES[doc]}: ${p.check}` : `${DOCUMENT_NAMES[doc]}, page ${p.page}: ${p.check}`)),
  );
}

/** The run's report with the console's approved sections in place of the engine's drafts. */
export function approvedReport(dir: string, approved: ApprovedRun): { report: BuiltReport; pseudonyms: Pseudonymiser } {
  const { input, pseudonyms } = loadRun(dir);
  const report = buildReport({ ...input, overrides: approved.overrides, intake: approved.intake ?? input.intake, preparedBy: approved.preparedBy ?? null, benchmark: approved.benchmark ?? null });
  for (const built of Object.values(report.outputs)) {
    const sections = approved.sections.filter((s) => s.output === built.output).sort((a, b) => a.order - b.order);
    if (!canRelease(sections).ok) throw new Error(`Not every section of the ${built.output.replace(/_/g, " ")} is approved, so no documents were made`);
    const current = new Set(sections.map((s) => s.currentVersionId));
    const versions = approved.versions.filter((v) => current.has(v.id));
    if (versions.length !== sections.length) throw new Error(`A section of the ${built.output.replace(/_/g, " ")} has no approved text, so no documents were made`);
    built.sections = sections;
    built.versions = versions;
  }
  return { report, pseudonyms };
}

export async function renderReleased(dir: string, approved: ApprovedRun, outDir: string): Promise<ReleasedDocuments> {
  const folder = resolve(dir);
  const out = resolve(outDir);
  assertOutsideGit(folder);
  assertOutsideGit(out);
  const { report, pseudonyms } = approvedReport(folder, approved);
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
  return { files, warnings: layoutWarnings(layout), microsite, ...(caseStudy ? { caseStudy } : {}) };
}
