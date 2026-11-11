import { isUnavailable, SalesforceError, type MetadataMember } from "../salesforce/client.ts";
import { redactSecrets } from "../privacy/secrets.ts";
import { scanApex, type ApexScan } from "./apex-scan.ts";
import { readTouches, type ApexTouches } from "./apex-touches.ts";
import { orgExtractor, recordCountsExtractor } from "./configuration.ts";
import { indexReferences, parseMetadataBundle, type MetadataBundle, type ReferenceIndex } from "./metadata-parse.ts";
import { readBundleThroughTooling, ToolingGapError, TOOLING_PARTIAL_KEYS } from "./metadata-tooling.ts";
import { readableReason, readSoql, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// Salesforce's wildcard for CustomObject brings custom objects only, so a standard
// object's rules are read only where the retrieve names it (DECISIONS.md 455).
// Every org has these four, so every retrieve names them.
export const RETRIEVED_STANDARD_OBJECTS = ["Account", "Contact", "Lead", "Opportunity"] as const;

// More standard objects, read for their validation rules and nothing else
// (DECISIONS.md 460). One is named only when the org's record counts list it, so
// we never ask for an object the org may not have.
export const FURTHER_STANDARD_OBJECTS = ["Case", "Campaign", "CampaignMember", "Contract", "Order", "OrderItem", "Quote", "QuoteLineItem", "OpportunityLineItem", "Product2", "Pricebook2", "PricebookEntry", "Asset", "Task", "Event", "User"] as const;

// One retrieve costs a few calls however many flows there are. The Tooling route
// is one call per flow, because a query can only return Metadata one row at a time.
export function retrieveTypes(further: readonly string[] = []): MetadataMember[] {
  return [
    { name: "Flow", members: ["*"] },
    { name: "Workflow", members: ["*"] },
    { name: "CustomObject", members: ["*", ...RETRIEVED_STANDARD_OBJECTS, ...further] },
    { name: "ApprovalProcess", members: ["*"] },
    { name: "AssignmentRules", members: ["Lead", "Case"] },
  ];
}

/** The further standard objects the record counts list by name, in the list's order. None when the counts couldn't be read. */
export function furtherObjects(counts: Sourced<Array<{ name: string; count: number }>>): string[] {
  if (counts.status !== "ok") return [];
  const listed = new Set(counts.data.map((c) => c.name));
  return FURTHER_STANDARD_OBJECTS.filter((o) => listed.has(o));
}

export const TOOLING_SOURCE: SourceInfo = { kind: "metadata", path: "Tooling API: FlowDefinition, Flow, WorkflowRule and its actions, ValidationRule, FieldDefinition, EntityParticle, RecordType" };

// A connection holding only Barometer Read Only can't use the Metadata API at
// all, so it reads the same bundle through Tooling instead (DECISIONS.md 73).
const needsWritePermission = (err: unknown): boolean => isUnavailable(err) && err.code === "INSUFFICIENT_ACCESS";

// Why a retrieve naming further objects failed, kept before the retrieve of the
// four starts, so a run stopped at the budget during that one doesn't make the
// failing retrieve again when it resumes. It holds the objects named and the
// Salesforce error, nothing read from the org.
const FALLBACK_PARTIAL = "metadata-retrieve-fallback";
type Fallback = NonNullable<MetadataBundle["retrieveFallback"]>;

export const metadataExtractor: Extractor<Sourced<MetadataBundle>> = {
  id: "metadata",
  // 8: each flow keeps every way it starts (DECISIONS.md 446).
  // 9: validation rules on the further standard objects the org has (DECISIONS.md 460).
  // 10: numeric character references decoded, as the named entities are (DECISIONS.md 471).
  version: 10,
  partialKeys: [...TOOLING_PARTIAL_KEYS, FALLBACK_PARTIAL],
  async run(ctx) {
    const further = furtherObjects(ctx.get(recordCountsExtractor));
    const source: SourceInfo = { kind: "metadata", path: retrieveTypes().map((t) => t.name).join(", ") };
    const retrieve = async (named: readonly string[]): Promise<Sourced<MetadataBundle> & { status: "ok" }> => {
      const files = await ctx.client.retrieveMetadata(retrieveTypes(named));
      return { status: "ok", data: parseMetadataBundle(files, RETRIEVED_STANDARD_OBJECTS, named), source, truncated: false };
    };
    let retrieveError: unknown;
    // A resumed run whose wider retrieve already failed goes straight to the four.
    let first: Fallback | undefined = ctx.partial.read<Fallback>(FALLBACK_PARTIAL)[0];
    if (!first) {
      try {
        return await retrieve(further);
      } catch (err) {
        retrieveError = err;
      }
    }
    // We can't tell from here what Salesforce does with a retrieve naming an
    // object it won't give us, so a failure there is tried once more with the four
    // only. A connection refused the Metadata API isn't about the objects named,
    // and goes straight to Tooling. Anything not from Salesforce is thrown as before.
    if (!first && further.length > 0 && retrieveError instanceof SalesforceError && !needsWritePermission(retrieveError)) {
      first = { named: further, reason: readableReason(retrieveError.message) };
      ctx.partial.append(FALLBACK_PARTIAL, [first]);
    }
    if (first) {
      try {
        const fallback = await retrieve([]);
        fallback.data.retrieveFallback = first;
        return fallback;
      } catch (err) {
        retrieveError = err;
      }
    }
    if (!needsWritePermission(retrieveError)) return unavailable(source, retrieveError);
    try {
      return { status: "ok", data: await readBundleThroughTooling(ctx), source: TOOLING_SOURCE, truncated: false };
    } catch (err) {
      const why = err instanceof ToolingGapError ? err.message : isUnavailable(err) ? readableReason(err.message) : null;
      if (why === null) throw err;
      const first = readableReason((retrieveError as Error).message);
      return { status: "unavailable", reason: `The Metadata API refused this connection (${first}), and reading the same through the Tooling API didn't work either: ${why}`, source: TOOLING_SOURCE };
    }
  },
};

// The body isn't kept, so what rules need from it is worked out here: whether it
// has any comment, whether it's a test, what the scan finds, and what it touches
// for the org's documents (DECISIONS.md 456). Null when the body is hidden.
type CodeFacts = {
  commented: boolean | null;
  isTest: boolean;
  scan: ApexScan | null;
  /** Undefined in a checkpoint from before it was read, which says "not read", not "touches nothing". */
  touches?: ApexTouches | null;
};
export type ApexClassRow = { Id: string; Name: string; ApiVersion: number; Status: string; LengthWithoutComments: number } & CodeFacts;
/** When a trigger runs (Object Reference, API 68.0). Undefined in a checkpoint from before they were read. */
export type TriggerUsage = { UsageBeforeInsert?: boolean; UsageBeforeUpdate?: boolean; UsageBeforeDelete?: boolean; UsageAfterInsert?: boolean; UsageAfterUpdate?: boolean; UsageAfterDelete?: boolean; UsageAfterUndelete?: boolean };
export type ApexTriggerRow = { Id: string; Name: string; TableEnumOrId: string; ApiVersion: number; Status: string; LengthWithoutComments?: number } & TriggerUsage & CodeFacts;
export type ClassCoverage = { name: string; covered: number; uncovered: number };

export type ApexData = {
  classes: Sourced<ApexClassRow[]>;
  triggers: Sourced<ApexTriggerRow[]>;
  orgCoverage: Sourced<Array<{ PercentCovered: number | null }>>;
  classCoverage: Sourced<ClassCoverage[]>;
  references: ReferenceIndex;
};

// Bodies are scanned for references here and then dropped, so code never sits
// in a checkpoint. A comment is one outside a string, so a URL isn't one (DECISIONS.md 174).

function stripBodies<R extends { Name: string; Body?: string }>(rows: Sourced<R[]>, prefix: string, references: ReferenceIndex): Sourced<Array<Omit<R, "Body"> & CodeFacts>> {
  if (rows.status !== "ok") return rows;
  const data = rows.data.map(({ Body, ...row }) => {
    if (Body) indexReferences(references, redactSecrets(Body), `${prefix}/${row.Name}`);
    const readable = Boolean(Body) && Body !== "(hidden)";
    const scan = readable ? scanApex(Body!) : null;
    return { ...row, commented: scan ? scan.commented : null, isTest: readable && /@isTest/i.test(Body!), scan, touches: readTouches(Body) };
  });
  return { ...rows, data };
}

export const apexExtractor: Extractor<ApexData> = {
  id: "apex",
  // 9: queries in loops count the WithBinds forms and cursors (DECISIONS.md 470).
  version: 9,
  async run(ctx) {
    const references: ReferenceIndex = { users: {}, fields: {} };
    // Through REST, not Tooling: a user with View Setup but not Author Apex gets
    // INVALID_TYPE from Tooling's ApexClass, and reads REST's with the same
    // fields, body included (DECISIONS.md 73).
    const classes = await readSoql<Omit<ApexClassRow, keyof CodeFacts> & { Body?: string }>(
      ctx,
      "SELECT Id, Name, ApiVersion, Status, LengthWithoutComments, Body FROM ApexClass WHERE NamespacePrefix = null",
    );
    // Through REST, not Tooling: the Tooling reference doesn't list Name,
    // TableEnumOrId or NamespacePrefix on ApexTrigger, and the Object Reference does.
    const triggers = await readSoql<Omit<ApexTriggerRow, keyof CodeFacts> & { Body?: string }>(
      ctx,
      "SELECT Id, Name, TableEnumOrId, ApiVersion, Status, LengthWithoutComments, UsageBeforeInsert, UsageBeforeUpdate, UsageBeforeDelete, UsageAfterInsert, UsageAfterUpdate, UsageAfterDelete, UsageAfterUndelete, Body FROM ApexTrigger WHERE NamespacePrefix = null",
    );
    const orgCoverage = await readSoql<{ PercentCovered: number | null }>(ctx, "SELECT PercentCovered FROM ApexOrgWideCoverage", { api: "tooling" });
    const rawCoverage = await readSoql<{ ApexClassOrTrigger: { Name: string } | null; NumLinesCovered: number; NumLinesUncovered: number }>(
      ctx,
      "SELECT ApexClassOrTrigger.Name, NumLinesCovered, NumLinesUncovered FROM ApexCodeCoverageAggregate",
      { api: "tooling" },
    );
    const classCoverage: Sourced<ClassCoverage[]> =
      rawCoverage.status === "ok"
        ? {
            ...rawCoverage,
            data: rawCoverage.data.map((r) => ({ name: r.ApexClassOrTrigger?.Name ?? "Unknown", covered: r.NumLinesCovered ?? 0, uncovered: r.NumLinesUncovered ?? 0 })),
          }
        : rawCoverage;
    // Professional Edition has no Apex, so "isn't available" is the answer, not a
    // gap. A real one read as if the check had failed.
    const org = ctx.get(orgExtractor);
    const edition = org?.status === "ok" ? org.data[0]?.OrganizationType : undefined;
    const noApex = <T>(rows: Sourced<T>): Sourced<T> =>
      rows.status === "unavailable" && edition === "Professional Edition" && /isn't available in this org/.test(rows.reason) ? { ...rows, reason: "Professional Edition has no Apex, so there's no code to check." } : rows;
    return {
      classes: noApex(stripBodies(classes, "ApexClass", references)) as Sourced<ApexClassRow[]>,
      triggers: noApex(stripBodies(triggers, "ApexTrigger", references)) as Sourced<ApexTriggerRow[]>,
      orgCoverage,
      classCoverage,
      references,
    };
  },
};

export type CronRow = { Id: string; OwnerId: string; State: string; CronJobDetail: { Name: string; JobType: string } | null };

export const scheduledJobsExtractor: Extractor<Sourced<CronRow[]>> = {
  id: "scheduled-jobs",
  version: 1,
  run: (ctx) => readSoql<CronRow>(ctx, "SELECT Id, OwnerId, State, CronJobDetail.Name, CronJobDetail.JobType FROM CronTrigger WHERE State NOT IN ('DELETED', 'COMPLETE')"),
};
