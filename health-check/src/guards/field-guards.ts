import { apexExtractor, metadataExtractor } from "../extract/automation.ts";
import type { FieldSummary } from "../extract/configuration.ts";
import { reportDefinitionsExtractor } from "../extract/report-definitions.ts";
import { plural, type RuleContext } from "../engine/rule.ts";
import type { Guard, GuardStatus } from "../schemas/index.ts";

const MAX_LISTED_REPORTS = 5;

// Rule 3: a clean scan only counts if it's a whole scan. Other people's private
// reports can't be read, so any that exist leave this unknown, and unknown blocks.
function reportGuard(ctx: RuleContext, field: string, subject: string): Guard {
  const scan = ctx.data(reportDefinitionsExtractor);
  const guard = (status: GuardStatus, detail: string, count: number | null): Guard => ({ guardId: "not-used-in-reports", subject, status, detail, evidence: [ctx.ev(scan.source, { count })] });
  if (scan.status !== "ok") return guard("unknown", `Report definitions couldn't be read: ${scan.reason}`, null);
  const { expected, notRead, privateReports, privateReason, fields } = scan.data;
  const uses = fields[field] ?? [];
  if (uses.length > 0) return guard("failed", `Used in ${plural(uses.length, "report")}: ${[...uses].sort().slice(0, MAX_LISTED_REPORTS).join(", ")}.`, uses.length);
  if (notRead > 0) return guard("unknown", `${notRead} of ${expected} reports couldn't be read, so one of them could use this field.`, notRead);
  if (privateReports === null) return guard("unknown", `We couldn't tell whether other people have private reports: ${privateReason}`, null);
  if (privateReports > 0) return guard("unknown", `Not used in the ${expected} reports we read, but ${plural(privateReports, "private report")} can't be read.`, privateReports);
  return guard("passed", `Not used in any of the ${expected} reports, and there are no private reports we can't read.`, expected);
}

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

  guards.push(reportGuard(ctx, field.name, subject));
  return guards;
}
