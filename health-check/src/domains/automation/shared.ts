import { metadataExtractor } from "../../extract/automation.ts";
import { unreadReason, type MetadataBundle, type UnreadKind } from "../../extract/metadata-parse.ts";
import { plural, type NotMeasurableResult, type RuleContext } from "../../engine/rule.ts";

// Shared by the automation area and technical health, which reads the same
// code (DECISIONS.md 169).

// An edition with no Apex says so plainly, rather than as a failed read.
export const apexGap = (what: string, reason: string): string => (reason.startsWith("Professional Edition has no Apex") ? reason : `Apex ${what} couldn't be read: ${reason}`);

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
export function unnamedNote(count: number, idsRead: boolean, consequence: string): string {
  if (count === 0) return "";
  return ` ${plural(count, "active Apex trigger")} ${count === 1 ? "is" : "are"} on ${count === 1 ? UNNAMED : "custom objects we couldn't name"}${idsRead ? "" : ", because the list of custom objects couldn't be read"}, so ${consequence}.`;
}
