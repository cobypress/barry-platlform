import { apexExtractor, metadataExtractor } from "../extract/automation.ts";
import type { FieldSummary } from "../extract/configuration.ts";
import type { RuleContext } from "../engine/rule.ts";
import type { Guard } from "../schemas/index.ts";

export function fieldGuards(ctx: RuleContext, object: string, field: FieldSummary): Guard[] {
  const subject = `${object}.${field.name}`;
  const metadata = ctx.data(metadataExtractor);
  const apex = ctx.data(apexExtractor);
  const guards: Guard[] = [];

  const mentions = [...(metadata.status === "ok" ? (metadata.data.references.fields[field.name] ?? []) : []), ...(apex.references.fields[field.name] ?? [])];
  const evidence = [ctx.ev(metadata.source, { count: mentions.length, note: "Searched flows, workflow rules, approval processes, assignment rules, validation rules and Apex" })];
  if (mentions.length > 0) {
    guards.push({ guardId: "not-referenced-in-automation", subject, status: "failed", detail: `Referenced in ${[...new Set(mentions)].sort().join(", ")}.`, evidence });
  } else if (metadata.status !== "ok" || apex.classes.status !== "ok" || apex.triggers.status !== "ok") {
    guards.push({ guardId: "not-referenced-in-automation", subject, status: "unknown", detail: "Metadata or Apex couldn't be read.", evidence });
  } else {
    guards.push({ guardId: "not-referenced-in-automation", subject, status: "passed", detail: "Not referenced in the metadata or Apex we searched.", evidence });
  }

  const special = field.externalId || field.unique || !field.nillable;
  guards.push({
    guardId: "not-an-integration-key",
    subject,
    status: special ? "failed" : "passed",
    detail: special ? "The field is an external id, unique or required, so something probably relies on it." : "The field isn't an external id, unique or required.",
    evidence,
  });

  // Report filters and columns aren't scanned yet, and an unscanned guard must
  // block. So field removal stays "investigate" until report scanning is built.
  guards.push({ guardId: "not-used-in-reports", subject, status: "unknown", detail: "We haven't scanned report definitions for this field.", evidence });
  return guards;
}
