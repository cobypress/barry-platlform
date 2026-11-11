import type { RecordTypeRow } from "../extract/record-types.ts";
import type { SourceInfo } from "../extract/types.ts";
import type { RuleContext } from "../engine/rule.ts";
import type { Guard } from "../schemas/index.ts";

// Rule 3 for a record type (DECISIONS.md 497). A profile can default to it, and
// a flow, Apex or an integration can name it, and nothing we read says which,
// so the check is always unknown for now, and unknown blocks. It becomes a real
// check only once profiles' record-type settings are read.

export const RECORD_TYPE_GUARD = "record-type-references";

export function recordTypeGuard(ctx: RuleContext, type: RecordTypeRow, source: SourceInfo): Guard {
  return {
    guardId: RECORD_TYPE_GUARD,
    subject: `${type.SobjectType}.${type.DeveloperName}`,
    status: "unknown",
    detail: "We don't read which profiles default to this record type, or which flows, Apex and integrations name it, so we can't tell whether anything still relies on it.",
    evidence: [ctx.ev(source, { count: null, note: "The record type list. Profiles' record type settings aren't read" })],
  };
}
