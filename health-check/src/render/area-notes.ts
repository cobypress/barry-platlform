import { areaReasonFrom } from "../domains/scoring.ts";
import { areaReasonShown } from "../digest/build.ts";
import { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import type { ScorecardRow } from "../narrative/select.ts";

// An unscored area's reason in the documents of a run analysed with
// HC_AREA_REASONS on (DECISIONS.md 769). The scorecard chart has room for one
// short line, and cut mid-way a sentence can end inside a number, so it prints
// a whole sentence or nothing: the area's first sentence, else the reason it gave
// before, else that it wasn't scored. The full reason is printed whole on the
// full report's list of areas we can't give a view of. Any other run's documents
// are as before.

/** What the chart says for an area it has no score for. Our words. */
const NOT_SCORED = "Not scored in this run";

/** Whether this run's documents print the areas' own reasons. */
export const areaReasonsOn = (report: Pick<BuiltReport, "input">): boolean => report.input.run.areaReasons === true;

/** The scorecard's rows with whole sentences to print for each unscored area, or the rows as they were. */
export function withWholeNotes(report: Pick<BuiltReport, "input">, rows: ScorecardRow[], pseudonyms: Pseudonymiser | undefined): ScorecardRow[] {
  if (!areaReasonsOn(report)) return rows;
  return rows.map((row) => {
    const score = report.input.scores?.find((s) => s.domain === row.domain);
    if (row.score !== null || score?.status !== "not_measurable") return row;
    // One the name and id guard would change is the sentence the area gave before, here as in the digest.
    const shown = areaReasonShown(report, row.domain, score.reason, pseudonyms ?? new Pseudonymiser());
    if (shown !== score.reason) return { ...row, wholeNotes: [...new Set([shown, NOT_SCORED])] };
    const from = areaReasonFrom(row.domain, report.input.findings, score.reason);
    return { ...row, wholeNotes: [...new Set([...(from ? [from.parts[0]!.text, from.today] : [score.reason]), NOT_SCORED])] };
  });
}

/** An unscored area's reason as this run's full report lists it: as the digest shows it, with the switch on. */
export function listedReason(report: Pick<BuiltReport, "input">, domain: string, reason: string, pseudonyms: Pseudonymiser | undefined): string {
  return areaReasonsOn(report) ? areaReasonShown(report, domain, reason, pseudonyms ?? new Pseudonymiser()) : reason;
}

/** The areas the score leaves out that a list of areas we can't give a view of doesn't hold yet, in order; none for any other run. */
export function unscoredBeyond(report: Pick<BuiltReport, "input">, domains: string[], listed: string[]): string[] {
  if (!areaReasonsOn(report)) return [];
  return domains.filter((d) => !listed.includes(d) && report.input.scores?.find((s) => s.domain === d)?.status === "not_measurable");
}
