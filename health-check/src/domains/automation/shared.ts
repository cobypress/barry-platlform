import { APEX_POINTER, editionIsWhy } from "../../salesforce/refusal-words.ts";
import type { Sourced } from "../../extract/types.ts";
import { metadataExtractor } from "../../extract/automation.ts";
import { unreadReason, type MetadataBundle, type UnreadKind } from "../../extract/metadata-parse.ts";
import { plural, type NotMeasurableResult, type RuleContext } from "../../engine/rule.ts";
import { eng, said, type Said } from "../../engine/org-words.ts";

// Shared by the automation area and technical health, which reads the same
// code (DECISIONS.md 169).

// An edition with no Apex says so plainly, rather than as a failed read.
export const apexGap = (what: string, reason: string): string => (reason.startsWith("Professional Edition has no Apex") || editionIsWhy(reason) ? reason : `Apex ${what} couldn't be read: ${reason}`);

/**
 * The org's own classes or triggers: as read, or none when a Professional
 * Edition org was asked and answered that it has none, which Salesforce
 * documents (DECISIONS.md 710); null when they couldn't be read.
 */
export const ownApex = <T>(part: Sourced<T[]>): T[] | null => (part.status === "ok" ? part.data : part.reason === APEX_POINTER ? [] : null);
export const apexReason = <T>(part: Sourced<T[]>): string => (part.status === "ok" ? "" : part.reason);
/** Said beside a figure that read no Apex because the edition allows none of the org's own. */
export const NO_OWN_APEX = "Apex: Professional Edition allows no Apex of the org's own";

// `needs` names the kinds a rule counts. If the Tooling route couldn't read one
// whole, the rule is not measurable rather than a pass on part of it (rule 5).
export function metadataOr(ctx: RuleContext, needs: UnreadKind[] = []): { bundle: MetadataBundle } | NotMeasurableResult {
  const metadata = ctx.data(metadataExtractor);
  if (metadata.status !== "ok") return { outcome: "not_measurable", reason: `Metadata couldn't be retrieved: ${metadata.reason}`, evidence: [ctx.ev(metadata.source, { count: null })] };
  const unread = unreadReason(metadata.data, needs);
  if (unread) return { outcome: "not_measurable", reason: unread, evidence: [ctx.ev(metadata.source, { count: null })] };
  return { bundle: metadata.data };
}

export const groupCount = (values: string[]): number => new Set(values).size;

export const UNNAMED = "a custom object we couldn't name";

/** Says which triggers sit on custom objects we couldn't name, and why, or nothing. */
export function unnamedNote(count: number, idsRead: boolean, consequence: Said): Said {
  if (count === 0) return said``;
  return said` ${eng(plural(count, "active Apex trigger"))} ${count === 1 ? "is" : "are"} on ${count === 1 ? UNNAMED : "custom objects we couldn't name"}${idsRead ? "" : ", because the list of custom objects couldn't be read"}, so ${consequence}.`;
}
