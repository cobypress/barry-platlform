import { isUnavailable } from "../salesforce/client.ts";
import { redactSecrets } from "../privacy/secrets.ts";
import { scanApex, type ApexScan } from "./apex-scan.ts";
import { orgExtractor } from "./configuration.ts";
import { indexReferences, parseMetadataBundle, type MetadataBundle, type ReferenceIndex } from "./metadata-parse.ts";
import { readBundleThroughTooling, ToolingGapError, TOOLING_PARTIAL_KEYS } from "./metadata-tooling.ts";
import { readableReason, readSoql, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// One retrieve costs a few calls however many flows there are. The Tooling route
// is one call per flow, because a query can only return Metadata one row at a time.
const RETRIEVE_TYPES = [
  { name: "Flow", members: ["*"] },
  { name: "Workflow", members: ["*"] },
  { name: "CustomObject", members: ["*", "Account", "Contact", "Lead", "Opportunity"] },
  { name: "ApprovalProcess", members: ["*"] },
  { name: "AssignmentRules", members: ["Lead", "Case"] },
];

export const TOOLING_SOURCE: SourceInfo = { kind: "metadata", path: "Tooling API: FlowDefinition, Flow, WorkflowRule and its actions, ValidationRule, FieldDefinition, EntityParticle, RecordType" };

// A connection holding only Barometer Read Only can't use the Metadata API at
// all, so it reads the same bundle through Tooling instead (DECISIONS.md 73).
const needsWritePermission = (err: unknown): boolean => isUnavailable(err) && err.code === "INSUFFICIENT_ACCESS";

export const metadataExtractor: Extractor<Sourced<MetadataBundle>> = {
  id: "metadata",
  version: 5,
  partialKeys: TOOLING_PARTIAL_KEYS,
  async run(ctx) {
    const source: SourceInfo = { kind: "metadata", path: RETRIEVE_TYPES.map((t) => t.name).join(", ") };
    let retrieveError: unknown;
    try {
      const files = await ctx.client.retrieveMetadata(RETRIEVE_TYPES);
      return { status: "ok", data: parseMetadataBundle(files), source, truncated: false };
    } catch (err) {
      if (!needsWritePermission(err)) return unavailable(source, err);
      retrieveError = err;
    }
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
// has any comment, whether it's a test, and what the scan finds. Null when the
// body is hidden.
type CodeFacts = { commented: boolean | null; isTest: boolean; scan: ApexScan | null };
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
    return { ...row, commented: scan ? scan.commented : null, isTest: readable && /@isTest/i.test(Body!), scan };
  });
  return { ...rows, data };
}

export const apexExtractor: Extractor<ApexData> = {
  id: "apex",
  version: 7,
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
