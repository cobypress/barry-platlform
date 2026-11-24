import { editionNoteOf } from "../extract/org-profile.ts";
import { EDITION_NOTE } from "../salesforce/refusal-words.ts";
import type { BuiltReport } from "./sections.ts";
import { DOMAIN_LABELS, domainLabel } from "./select.ts";

// The run's one note about its edition (DECISIONS.md 710), for the digest and
// each page that shows a check the edition left unmeasured. An overall score
// from fewer areas can read better than the org is, and nothing a customer
// reads said so, so the note names the areas the score leaves out.

const LEAVES_OUT = " Your Barometer score leaves out the areas we couldn't score: ";

const listed = (labels: string[]): string => (labels.length === 1 ? labels[0]! : `${labels.slice(0, -1).join(", ")} and ${labels.at(-1)}`);

/** The note for a built report, or null for an org whose edition kept nothing from us. */
export function editionNoteFor(report: Pick<BuiltReport, "input" | "overall">): string | null {
  const note = editionNoteOf(report.input.run.orgProfile);
  if (!note) return null;
  const scored = report.overall.status === "measured" ? new Set(report.overall.areas.map((a) => a.domain)) : null;
  const left = scored ? (report.input.scores ?? []).filter((s) => !scored.has(s.domain)).map((s) => domainLabel(s.domain)) : [];
  return left.length > 0 ? `${note}${LEAVES_OUT}${listed(left)}.` : note;
}

const LABELS: ReadonlySet<string> = new Set(Object.values(DOMAIN_LABELS));

/** Whether a text is exactly a note editionNoteFor writes: our words, and area names from our own table. */
export function isEditionNote(text: string): boolean {
  if (text === EDITION_NOTE) return true;
  if (!text.startsWith(`${EDITION_NOTE}${LEAVES_OUT}`) || !text.endsWith(".")) return false;
  const list = text.slice(EDITION_NOTE.length + LEAVES_OUT.length, -1);
  const [head, last] = list.includes(" and ") ? [list.slice(0, list.lastIndexOf(" and ")), list.slice(list.lastIndexOf(" and ") + 5)] : ["", list];
  const labels = [...(head ? head.split(", ") : []), last];
  return labels.every((l) => LABELS.has(l)) && listed(labels) === list;
}
