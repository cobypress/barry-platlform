import { editionNoteOf } from "../extract/org-profile.ts";
import { EDITION_NOTE } from "../salesforce/refusal-words.ts";
import type { BuiltReport } from "./sections.ts";
import { DOMAIN_LABELS, domainLabel } from "./select.ts";

// Two notes said once for a run (DECISIONS.md 710). The edition's, for an org
// whose edition kept the Metadata API from us. And the score's, on any run:
// an overall score from fewer areas can read better than the org is, and
// nothing a customer read said so.

/** The run's note about its edition, or null for an org whose edition kept nothing from us. */
export function editionNoteFor(report: Pick<BuiltReport, "input">): string | null {
  return editionNoteOf(report.input.run.orgProfile);
}

/** Whether a text is exactly the edition note. */
export const isEditionNote = (text: string): boolean => text === EDITION_NOTE;

const LEAVES_OUT = "Your Barometer score leaves out the areas we couldn't score: ";

const listed = (labels: string[]): string => (labels.length === 1 ? labels[0]! : `${labels.slice(0, -1).join(", ")} and ${labels.at(-1)}`);

/** Under the overall score, the areas it leaves out; null when it leaves none out, or there is no score. */
export function scoreCaveatFor(report: Pick<BuiltReport, "input" | "overall">): string | null {
  if (report.overall.status !== "measured") return null;
  const scored = new Set(report.overall.areas.map((a) => a.domain));
  const left = (report.input.scores ?? []).filter((s) => !scored.has(s.domain)).map((s) => domainLabel(s.domain));
  return left.length > 0 ? `${LEAVES_OUT}${listed(left)}.` : null;
}

const LABELS: ReadonlySet<string> = new Set(Object.values(DOMAIN_LABELS));

/** Whether a text is exactly a caveat scoreCaveatFor writes: our words, and area names from our own table. */
export function isScoreCaveat(text: string): boolean {
  if (!text.startsWith(LEAVES_OUT) || !text.endsWith(".")) return false;
  const list = text.slice(LEAVES_OUT.length, -1);
  const cut = list.lastIndexOf(" and ");
  const labels = cut === -1 ? [list] : [...list.slice(0, cut).split(", "), list.slice(cut + 5)];
  return labels.every((l) => LABELS.has(l)) && listed(labels) === list;
}
