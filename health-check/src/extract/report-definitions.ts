import { SalesforceError } from "../salesforce/client.ts";
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
};

type ReportRow = { Id: string; DeveloperName: string; OwnerId: string };
type FolderRow = { Id: string; DeveloperName: string | null; ParentId: string | null };

const RETRIEVE_CHUNK = 2_000;
const MAX_REPORTS = 20_000;
const MAX_FOLDER_DEPTH = 10;
const UNFILED = "unfiled$public";
const PRIVATE_SOQL = "SELECT COUNT() FROM Report USING SCOPE allPrivate";

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

async function countPrivate(ctx: ExtractContext): Promise<{ count: number | null; reason: string | null }> {
  try {
    return { count: (await ctx.client.query(PRIVATE_SOQL)).totalSize, reason: null };
  } catch (err) {
    if (err instanceof SalesforceError) return { count: null, reason: err.message };
    throw err;
  }
}

/** Salesforce only allows a retrieve by report name, so reports and folders are listed first. */
export const reportDefinitionsExtractor: Extractor<Sourced<ReportScan>> = {
  id: "report-definitions",
  version: 1,
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
      // Unlike the main retrieve, a failure here costs one guard, not the run.
      if (err instanceof SalesforceError) return { status: "unavailable", reason: err.message, source };
      throw err;
    }
    const { count, reason } = await countPrivate(ctx);
    return {
      status: "ok",
      source,
      truncated: false,
      data: { expected: reports.data.length, retrieved, notRead: Math.max(0, reports.data.length - retrieved), privateReports: count, privateReason: reason, fields: references.fields },
    };
  },
};
