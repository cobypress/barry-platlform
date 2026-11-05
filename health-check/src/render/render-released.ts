import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { assertOutsideGit } from "../engine/run.ts";
import type { Intake } from "../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportOutputKey } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type NumberOverride, type ReportSection, type SectionVersion } from "../schemas/index.ts";
import type { LayoutReport } from "./layout.ts";
import { loadRun, OUTPUT_FILES, printDocuments } from "./render-run.ts";

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
};

export type ReleasedDocuments = {
  files: Record<ReportOutputKey, string>;
  /** What the layout checks found. The files are still written, for a person to look over. */
  warnings: string[];
};

const DOCUMENT_NAMES = { executive: "The executive summary", full: "The full health check" } as const;

/**
 * One line per layout problem, without the check's detail: an empty-page
 * detail quotes the page's text, which has names in it by now, and these lines
 * go to the console. The full report stays in the run folder.
 */
export function layoutWarnings(layout: { executive: LayoutReport; full: LayoutReport }): string[] {
  return (Object.keys(DOCUMENT_NAMES) as Array<keyof typeof DOCUMENT_NAMES>).flatMap((doc) =>
    layout[doc].problems.map((p) => (p.page === 0 ? `${DOCUMENT_NAMES[doc]}: ${p.check}` : `${DOCUMENT_NAMES[doc]}, page ${p.page}: ${p.check}`)),
  );
}

/** The run's report with the console's approved sections in place of the engine's drafts. */
export function approvedReport(dir: string, approved: ApprovedRun): { report: BuiltReport; pseudonyms: Pseudonymiser } {
  const { input, pseudonyms } = loadRun(dir);
  const report = buildReport({ ...input, overrides: approved.overrides, intake: approved.intake ?? input.intake, preparedBy: approved.preparedBy ?? null });
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
  };
  const layout = await printDocuments(report, pseudonyms, {
    rawNotes: files.raw_notes,
    executive: files.executive_summary,
    full: files.full_health_check,
    layout: join(out, OUTPUT_FILES.layout),
  });
  return { files, warnings: layoutWarnings(layout) };
}
