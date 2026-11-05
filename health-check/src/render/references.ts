import { comparisonId } from "../benchmark/compare.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { domainsInOrder, issuesOf } from "../narrative/select.ts";
import { practiceItems } from "../practice/index.ts";
import type { Finding } from "../schemas/index.ts";

// Every figure in the decks points at the raw notes, where the working is: R4 is
// the fourth section of the notes, and R4.3 the third item in it. The notes and
// the decks both number from here, so the two can't drift apart.

export type NoteItem = { ref: string; id: string };
export type NoteSection = { ref: string; key: string; title: string; items: NoteItem[] };

/** Issues first, then what couldn't be measured, then what passed: the order a finding section is read in. */
export function rawNotesOrder(findings: Finding[]): Finding[] {
  return [...issuesOf(findings), ...findings.filter((f) => f.status === "not_measurable"), ...findings.filter((f) => f.status === "passed")];
}

export function noteSections(report: BuiltReport): NoteSection[] {
  const { findings, metrics, manualSteps } = report.input;
  const sections = report.outputs.raw_notes.sections.slice().sort((a, b) => a.order - b.order);
  return sections.map((section, i) => {
    const ref = `R${i + 1}`;
    const ids = section.key.startsWith("findings_")
      ? rawNotesOrder(findings.filter((f) => section.findingIds.includes(f.id))).map((f) => f.id)
      : section.key === "pipeline_metrics" || section.key === "investment" || section.key === "adoption" || section.key === "sales_process" || section.key === "org_documented" || section.key === "team_survey" || section.key === "service_cases"
        ? section.metricIds
        : section.key === "value_model"
          ? report.value.groups.map((g) => valueItemId(g.id))
        : section.key === "practice"
          ? practiceItems(domainsInOrder(report.teamFindings))
        : section.key === "comparisons"
          ? report.comparisons.map((c) => comparisonId(c.metric.key))
          : section.key === "manual_steps"
            ? manualSteps.map((s) => s.id)
            : [];
    return { ref, key: section.key, title: section.title, items: ids.map((id, k) => ({ ref: `${ref}.${k + 1}`, id })) };
  });
}

/** A value group's id among the notes' items. */
export const valueItemId = (group: string): string => `value:${group}`;

/** Each finding, metric and manual step's reference, by its id. */
export function referenceMap(sections: NoteSection[]): Map<string, string> {
  return new Map(sections.flatMap((s) => s.items.map((item): [string, string] => [item.id, item.ref])));
}
