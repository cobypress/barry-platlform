import { checkNumbers, scopeDataset, toNumberCheckResult } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { SectionVersion } from "../schemas/index.ts";
import type { NarrativeClient } from "./client.ts";
import { draftSection } from "./draft.ts";
import type { BuiltReport } from "./sections.ts";

export type Rejection = { sectionId: string; reasons: string[] };

/**
 * Asks Claude for each section that has a brief. An accepted draft becomes a new
 * version. A rejected one leaves the engine's text and sends the section to a
 * person, with the reasons kept for the console.
 */
export async function applyNarrative(report: BuiltReport, client: NarrativeClient, pseudonyms: Pseudonymiser, now: Date = new Date()): Promise<Rejection[]> {
  const rejections: Rejection[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const section of built.sections) {
      const brief = report.briefs.get(section.id);
      if (!brief) continue;
      const result = await draftSection({ brief, dataset: report.dataset, client, pseudonyms });
      if (result.status === "rejected") {
        section.status = "needs_input";
        rejections.push({ sectionId: section.id, reasons: result.reasons });
        continue;
      }
      const version = built.versions.filter((v) => v.sectionId === section.id).length + 1;
      const scope = scopeDataset(report.dataset, brief.sources.map((s) => s.id));
      const record = SectionVersion.parse({
        id: `${section.id}:v${version}`,
        sectionId: section.id,
        version,
        body: result.body,
        source: "claude_draft",
        changedBy: result.model,
        changedAt: now.toISOString(),
        changeSummary: `Claude draft, attempt ${result.attempts}`,
        numberCheck: toNumberCheckResult(checkNumbers(result.body, scope), now),
      });
      built.versions.push(record);
      section.currentVersionId = record.id;
    }
  }
  return rejections;
}
