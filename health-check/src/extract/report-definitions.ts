import { isUnavailable, SalesforceError } from "../salesforce/client.ts";
import { indexReferences, type ReferenceIndex } from "./metadata-parse.ts";
import { readSoql, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

export type ReportScan = {
  /** Reports we tried to read. */
  expected: number;
  retrieved: number;
  /** Listed but not read, including those in a folder we couldn't place. A gap means a clean result isn't proof. */
  notRead: number;
  /** Other people's private reports, which can't be read. Null if the count itself couldn't be read. */
  privateReports: number | null;
  privateReason: string | null;
  /** Field name to report paths. Filter values and titles are dropped, because they can hold customer data. */
  fields: Record<string, string[]>;
  /** One retrieve, or each report's describe through the Analytics API for a connection that can't use the Metadata API. */
  route?: "retrieve" | "analytics";
};

type ReportRow = { Id: string; DeveloperName: string; OwnerId: string };
type FolderRow = { Id: string; DeveloperName: string | null; ParentId: string | null };

const RETRIEVE_CHUNK = 2_000;
const MAX_REPORTS = 20_000;
const MAX_FOLDER_DEPTH = 10;
const UNFILED = "unfiled$public";
const PRIVATE_SOQL = "SELECT COUNT() FROM Report USING SCOPE allPrivate";
const PRIVATE_PROBE = "SELECT Id FROM Report USING SCOPE allPrivate LIMIT 1";
// Our ceiling on one-at-a-time describes. Reports past it count as not read.
export const MAX_REPORT_DESCRIBES = 3_000;
export const REPORT_PARTIAL_KEY = "report-describes";

// A report's OwnerId is its folder. A wrong guess here costs nothing: the member
// isn't found, the file doesn't come back, and the report counts as not read.
function memberOf(report: ReportRow, folders: Map<string, FolderRow>): string | null {
  if (report.OwnerId.startsWith("00D")) return `${UNFILED}/${report.DeveloperName}`;
  const path: string[] = [];
  let folder = folders.get(report.OwnerId);
  for (let depth = 0; folder && depth < MAX_FOLDER_DEPTH; depth++) {
    if (!folder.DeveloperName) return null;
    path.unshift(folder.DeveloperName);
    if (!folder.ParentId) return `${path.join("/")}/${report.DeveloperName}`;
    folder = folders.get(folder.ParentId);
  }
  return null;
}

// On bc-hc-dev, a user without Manage All Private Reports and Dashboards got a
// COUNT() of every report it could see, public ones included, not an error. The
// same scope asking for rows fails for that user with INVALID_QUERY_SCOPE, so
// the rows are asked for first, and the count is only trusted after that.
async function countPrivate(ctx: ExtractContext): Promise<{ count: number | null; reason: string | null }> {
  try {
    await ctx.client.query(PRIVATE_PROBE);
    return { count: (await ctx.client.query(PRIVATE_SOQL)).totalSize, reason: null };
  } catch (err) {
    if (err instanceof SalesforceError && err.code === "INVALID_QUERY_SCOPE") {
      return { count: null, reason: "Only a user who can manage all private reports can count other people's, and the connected user can't." };
    }
    if (err instanceof SalesforceError) return { count: null, reason: err.message };
    throw err;
  }
}

type Described = { id: string; fields: string[] };
type ReportDescribe = { reportMetadata?: Record<string, unknown> };

/**
 * Each report's definition through the Analytics API, one call each and
 * checkpointed. Only `reportMetadata` is searched: `reportTypeMetadata` lists
 * every field the report type offers, used or not.
 */
async function describeReports(ctx: ExtractContext, reports: ReportRow[], path: (r: ReportRow) => string): Promise<{ retrieved: number; fields: Record<string, string[]> }> {
  const done = new Map(ctx.partial.read<Described>(REPORT_PARTIAL_KEY).map((d) => [d.id, d]));
  const references: ReferenceIndex = { users: {}, fields: {} };
  let retrieved = 0;
  for (const report of reports.slice(0, MAX_REPORT_DESCRIBES)) {
    let described = done.get(report.Id);
    if (!described) {
      try {
        const body = await ctx.client.describeReport<ReportDescribe>(report.Id);
        const scratch: ReferenceIndex = { users: {}, fields: {} };
        indexReferences(scratch, JSON.stringify(body.reportMetadata ?? {}), report.Id);
        described = { id: report.Id, fields: Object.keys(scratch.fields) };
      } catch (err) {
        // One report we can't describe, its report type hidden from the user say,
        // counts as not read. It doesn't stop the rest.
        if (err instanceof SalesforceError) continue;
        throw err;
      }
      ctx.partial.append(REPORT_PARTIAL_KEY, [described]);
    }
    retrieved++;
    for (const field of described.fields) (references.fields[field] ??= []).push(path(report));
  }
  return { retrieved, fields: references.fields };
}

/** Salesforce only allows a retrieve by report name, so reports and folders are listed first. */
export const reportDefinitionsExtractor: Extractor<Sourced<ReportScan>> = {
  id: "report-definitions",
  version: 2,
  partialKeys: [REPORT_PARTIAL_KEY],
  async run(ctx) {
    const source: SourceInfo = { kind: "metadata", path: "Report" };
    const reports = await readSoql<ReportRow>(ctx, "SELECT Id, DeveloperName, OwnerId FROM Report", { maxRecords: MAX_REPORTS });
    if (reports.status !== "ok") return { status: "unavailable", reason: `Reports couldn't be listed: ${reports.reason}`, source };
    const folders = await readSoql<FolderRow>(ctx, "SELECT Id, DeveloperName, ParentId FROM Folder WHERE Type = 'Report'");
    if (folders.status !== "ok") return { status: "unavailable", reason: `Report folders couldn't be read: ${folders.reason}`, source };
    if (reports.truncated) return { status: "unavailable", reason: `There are more than ${MAX_REPORTS} reports, which is too many to scan.`, source };

    const byId = new Map(folders.data.map((f) => [f.Id, f]));
    const members = reports.data.flatMap((r) => memberOf(r, byId) ?? []);
    const references: ReferenceIndex = { users: {}, fields: {} };
    let retrieved = 0;
    let route: ReportScan["route"] = "retrieve";
    try {
      for (let i = 0; i < members.length; i += RETRIEVE_CHUNK) {
        const files = await ctx.client.retrieveMetadata([{ name: "Report", members: members.slice(i, i + RETRIEVE_CHUNK) }]);
        for (const [path, xml] of Object.entries(files)) {
          if (!path.endsWith(".report") && !path.endsWith(".report-meta.xml")) continue;
          retrieved++;
          indexReferences(references, xml, path);
        }
      }
    } catch (err) {
      // A connection that can't use the Metadata API describes each report instead
      // (DECISIONS.md 73). Unlike the main retrieve, any other failure costs one
      // guard, not the run.
      if (!(isUnavailable(err) && err.code === "INSUFFICIENT_ACCESS")) {
        if (err instanceof SalesforceError) return { status: "unavailable", reason: err.message, source };
        throw err;
      }
      route = "analytics";
      const described = await describeReports(ctx, reports.data, (r) => `reports/${memberOf(r, byId) ?? r.DeveloperName}.report`);
      retrieved = described.retrieved;
      references.fields = described.fields;
    }
    const { count, reason } = await countPrivate(ctx);
    return {
      status: "ok",
      source: route === "analytics" ? { kind: "rest", path: "analytics/reports/<id>/describe" } : source,
      truncated: false,
      data: { expected: reports.data.length, retrieved, notRead: Math.max(0, reports.data.length - retrieved), privateReports: count, privateReason: reason, fields: references.fields, route },
    };
  },
};
