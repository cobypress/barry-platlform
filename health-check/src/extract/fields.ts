import { isUnavailable } from "../salesforce/client.ts";
import type { ExtractContext, Extractor, Sourced } from "./types.ts";

// A field that isn't in the org fails the whole query, which would make every
// rule on that source not measurable. So the objects below are described first.
// An optional field that's missing is dropped from the SELECT, and only the
// rules that need it go not measurable. A missing required field still makes
// the source unavailable, without spending the query.
//
// Only REST objects can be described here: Tooling describe isn't on the
// allowlist. Core objects come from `schemaExtractor` instead.

export type FieldSpec = {
  object: string;
  /** Without these the source is no use. Relationship paths such as `Profile.Name` check the first hop. */
  required: readonly string[];
  /** Feature- or edition-dependent. Dropped when absent, and named on the source. */
  optional?: readonly string[];
};

export const FIELD_SPECS = {
  loginHistory: { object: "LoginHistory", required: ["UserId", "LoginTime", "Status"], optional: ["ApiType", "Application"] },
  forecastingItem: {
    object: "ForecastingItem",
    required: ["OwnerId", "PeriodId", "ForecastingTypeId", "ForecastCategoryName", "ForecastAmount"],
    optional: ["AmountWithoutAdjustments", "HasAdjustment"],
  },
  campaign: { object: "Campaign", required: ["Id", "Type"], optional: ["ActualCost"] },
} as const satisfies Record<string, FieldSpec>;

export type DescribedObject =
  | { status: "ok"; fields: string[]; relationships: string[] }
  /** Salesforce says the object isn't there, or isn't visible to us. */
  | { status: "absent"; reason: string }
  /** The describe itself failed. The query runs with every field, as it did before describes. */
  | { status: "unknown"; reason: string };

type DescribeRow = { Id: string; result: DescribedObject };
type Describe = { fields: Array<{ name: string; relationshipName?: string | null }> };

const ABSENT_CODES = new Set(["NOT_FOUND", "INVALID_TYPE"]);
const PARTIAL_KEY = "field-describe";

/** One describe per object, checkpointed as it goes, so a run stopped at the budget picks up where it left off. */
export const fieldDescribeExtractor: Extractor<Record<string, DescribedObject>> = {
  id: "field-describe",
  version: 1,
  partialKeys: [PARTIAL_KEY],
  async run(ctx) {
    const done = new Map(ctx.partial.read<DescribeRow>(PARTIAL_KEY).map((r) => [r.Id, r.result]));
    const objects = [...new Set(Object.values(FIELD_SPECS).map((s) => s.object))].sort();
    for (const object of objects) {
      if (done.has(object)) continue;
      let result: DescribedObject;
      try {
        const d = await ctx.client.describe<Describe>(object);
        result = {
          status: "ok",
          fields: d.fields.map((f) => f.name.toLowerCase()),
          relationships: d.fields.flatMap((f) => (f.relationshipName ? [f.relationshipName.toLowerCase()] : [])),
        };
      } catch (err) {
        // A budget stop or a fault isn't a fact about the org, so it isn't saved.
        if (!isUnavailable(err)) throw err;
        result = ABSENT_CODES.has(err.code) ? { status: "absent", reason: err.message } : { status: "unknown", reason: err.message };
      }
      ctx.partial.append<DescribeRow>(PARTIAL_KEY, [{ Id: object, result }]);
      done.set(object, result);
    }
    return Object.fromEntries(objects.map((o) => [o, done.get(o)!]));
  },
};

export type FieldPlan = { ok: true; select: string; missing: string[] } | { ok: false; reason: string };

const listOf = (names: string[]): string => (names.length === 1 ? names[0]! : `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`);

function has(described: Extract<DescribedObject, { status: "ok" }>, field: string): boolean {
  const [first, ...rest] = field.toLowerCase().split(".");
  return rest.length > 0 ? described.relationships.includes(first!) : described.fields.includes(first!);
}

/** The SELECT list to send, given what the org has. Never queries anything itself. */
export function planFields(ctx: ExtractContext, spec: FieldSpec): FieldPlan {
  const optional = spec.optional ?? [];
  const everything = { ok: true as const, select: [...spec.required, ...optional].join(", "), missing: [] };
  const described = ctx.get(fieldDescribeExtractor)[spec.object];
  if (!described || described.status === "unknown") return everything;
  if (described.status === "absent") return { ok: false, reason: `${spec.object} isn't available in this org, or to the connected user: ${described.reason}` };
  const missingRequired = spec.required.filter((f) => !has(described, f));
  if (missingRequired.length > 0) {
    const names = missingRequired.map((f) => `${spec.object}.${f}`);
    return { ok: false, reason: `${listOf(names)} ${names.length === 1 ? "isn't" : "aren't"} in this org, and the read needs ${names.length === 1 ? "it" : "them"}, so it wasn't run.` };
  }
  const present = optional.filter((f) => has(described, f));
  return { ok: true, select: [...spec.required, ...present].join(", "), missing: optional.filter((f) => !present.includes(f)).map((f) => `${spec.object}.${f}`) };
}

/** Why a rule can't run, if a field it needs was dropped from its source. Null when everything is there. */
export function missingFieldReason(source: Sourced<unknown>, ...fields: string[]): string | null {
  if (source.status !== "ok") return null;
  const gone = fields.filter((f) => source.missingFields?.includes(f));
  if (gone.length === 0) return null;
  return `${listOf(gone)} ${gone.length === 1 ? "isn't" : "aren't"} in this org, so this can't be measured.`;
}
