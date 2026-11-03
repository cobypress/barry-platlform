import { isUnavailable } from "../salesforce/client.ts";
import { daysBefore, monthsBefore, readSoql, soqlDateTime, unavailable, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

export const CORE_OBJECTS = ["Account", "Contact", "Lead", "Opportunity"] as const;

type DescribeField = {
  name: string;
  label: string;
  type: string;
  custom: boolean;
  calculated: boolean;
  aggregatable: boolean;
  filterable: boolean;
  nillable: boolean;
  unique: boolean;
  externalId: boolean;
  picklistValues?: Array<{ value: string; active: boolean }>;
};
type Describe = { fields: DescribeField[]; recordTypeInfos: Array<{ developerName: string; name: string; active: boolean; master: boolean }> };

export type FieldSummary = Omit<DescribeField, "picklistValues"> & { activePicklistValues: string[] };
export type ObjectSummary = { name: string; fields: FieldSummary[]; recordTypes: Array<{ developerName: string; active: boolean }> };

const NAMESPACED = /^[A-Za-z0-9]+__.+__c$/;
export const isOwnCustomField = (f: { custom: boolean; name: string }): boolean => f.custom && !NAMESPACED.test(f.name);

export type SharingDefaults = { DefaultAccountAccess: string | null; DefaultContactAccess: string | null; DefaultOpportunityAccess: string | null; DefaultLeadAccess: string | null };
export type OrgSummary = { Id: string; Name: string; OrganizationType: string; IsSandbox: boolean; TimeZoneSidKey: string; FiscalYearStartMonth: number } & SharingDefaults;

export const orgExtractor: Extractor<Sourced<OrgSummary[]>> = {
  id: "org",
  version: 3,
  // The org's Name is the customer's name. No rule uses it: it goes on the report
  // cover, and is masked like a person's name before anything reaches Claude.
  run: (ctx) =>
    readSoql<OrgSummary>(
      ctx,
      "SELECT Id, Name, OrganizationType, IsSandbox, TimeZoneSidKey, FiscalYearStartMonth, DefaultAccountAccess, DefaultContactAccess, DefaultOpportunityAccess, DefaultLeadAccess FROM Organization",
    ),
};

export const schemaExtractor: Extractor<Record<string, Sourced<ObjectSummary>>> = {
  id: "schema",
  version: 1,
  async run(ctx) {
    const out: Record<string, Sourced<ObjectSummary>> = {};
    for (const name of CORE_OBJECTS) {
      const source: SourceInfo = { kind: "rest", path: `sobjects/${name}/describe` };
      try {
        const d = await ctx.client.describe<Describe>(name);
        out[name] = {
          status: "ok",
          source,
          truncated: false,
          data: {
            name,
            fields: d.fields.map(({ picklistValues, ...f }) => ({
              ...f,
              activePicklistValues: (picklistValues ?? []).filter((v) => v.active).map((v) => v.value),
            })),
            recordTypes: d.recordTypeInfos.filter((r) => !r.master).map((r) => ({ developerName: r.developerName, active: r.active })),
          },
        };
      } catch (err) {
        out[name] = unavailable(source, err);
      }
    }
    return out;
  },
};

export type FieldUsage = { field: string; total: number; filled: number; recentTotal: number; recentFilled: number };
export type ObjectFieldUsage = { object: string; fields: FieldUsage[]; notScanned: string[]; recentFrom: string };

const USAGE_CHUNK = 50;
const usageTypes = (f: FieldSummary): boolean => f.aggregatable && !f.calculated && f.type !== "boolean";

export const fieldUsageExtractor: Extractor<Record<string, Sourced<ObjectFieldUsage>>> = {
  id: "field-usage",
  version: 1,
  async run(ctx) {
    const recentFrom = monthsBefore(ctx.now, 12);
    const schema = ctx.get(schemaExtractor);
    const out: Record<string, Sourced<ObjectFieldUsage>> = {};
    for (const object of CORE_OBJECTS) {
      const summary = schema[object]!;
      const source: SourceInfo = { kind: "soql", api: "rest", query: `SELECT COUNT(Id) total, COUNT(<field>) ... FROM ${object}` };
      if (summary.status !== "ok") {
        out[object] = { status: "unavailable", reason: summary.reason, source };
        continue;
      }
      const own = summary.data.fields.filter(isOwnCustomField);
      const scanned = own.filter(usageTypes);
      const notScanned = own.filter((f) => !usageTypes(f)).map((f) => f.name);
      const usage: FieldUsage[] = [];
      let failure: string | null = null;

      for (let i = 0; i < scanned.length && !failure; i += USAGE_CHUNK) {
        const chunk = scanned.slice(i, i + USAGE_CHUNK);
        const columns = chunk.map((f, j) => `COUNT(${f.name}) f${j}`).join(", ");
        const [all, recent] = await ctx.client.queryBatch<Record<string, number>>([
          `SELECT COUNT(Id) total, ${columns} FROM ${object}`,
          `SELECT COUNT(Id) total, ${columns} FROM ${object} WHERE CreatedDate >= ${soqlDateTime(recentFrom)}`,
        ]);
        if (!all?.ok || !recent?.ok) {
          const failed = !all?.ok ? all : recent;
          failure = failed && !failed.ok ? failed.error.message : "The field usage query failed";
          break;
        }
        const allRow = all.result.records[0]!;
        const recentRow = recent.result.records[0]!;
        chunk.forEach((f, j) => {
          usage.push({
            field: f.name,
            total: allRow.total!,
            filled: allRow[`f${j}`]!,
            recentTotal: recentRow.total!,
            recentFilled: recentRow[`f${j}`]!,
          });
        });
      }
      out[object] = failure
        ? { status: "unavailable", reason: failure, source }
        : { status: "ok", source, truncated: false, data: { object, fields: usage, notScanned, recentFrom: recentFrom.toISOString().slice(0, 10) } };
    }
    return out;
  },
};

export const recordCountsExtractor: Extractor<Sourced<Array<{ name: string; count: number }>>> = {
  id: "record-counts",
  version: 1,
  async run(ctx) {
    const source: SourceInfo = { kind: "rest", path: "limits/recordCount" };
    try {
      return { status: "ok", data: await ctx.client.recordCounts(), source, truncated: false };
    } catch (err) {
      return unavailable(source, err);
    }
  },
};

export type LayoutData = {
  layouts: Sourced<Array<{ Id: string; Name: string; EntityDefinitionId: string }>>;
  assignments: Sourced<Array<{ LayoutId: string }>>;
};

// The Tooling API reference lists EntityDefinitionId on Layout, not TableEnumOrId,
// which is ProfileLayout's field. Both hold the object's name, or its id if custom.
export const layoutsExtractor: Extractor<LayoutData> = {
  id: "layouts",
  version: 2,
  async run(ctx) {
    return {
      layouts: await readSoql(ctx, "SELECT Id, Name, EntityDefinitionId FROM Layout", { api: "tooling" }),
      assignments: await readSoql(ctx, "SELECT LayoutId FROM ProfileLayout", { api: "tooling" }),
    };
  },
};

export type CustomObjectRow = { Id: string; DeveloperName: string; NamespacePrefix: string | null };

// Layout and ApexTrigger can name a custom object by its id rather than its API
// name. Nothing else we read links the two, so the inventory reads it here.
export const customObjectsExtractor: Extractor<Sourced<CustomObjectRow[]>> = {
  id: "custom-objects",
  version: 1,
  run: (ctx) => readSoql<CustomObjectRow>(ctx, "SELECT Id, DeveloperName, NamespacePrefix FROM CustomObject", { api: "tooling" }),
};

export const duplicateRulesExtractor: Extractor<Sourced<Array<{ DeveloperName: string; SobjectType: string; IsActive: boolean }>>> = {
  id: "duplicate-rules",
  version: 1,
  run: (ctx) => readSoql(ctx, "SELECT DeveloperName, SobjectType, IsActive FROM DuplicateRule"),
};

export type SetupAuditSummary = {
  rows: number;
  earliest: string | null;
  bySection: Record<string, number>;
  configChangesByUser: Record<string, number>;
  excludedUserAdmin: number;
};

const USER_ADMIN_SECTION = "Manage Users";
const AUDIT_DAYS = 180;
const AUDIT_MAX_ROWS = 2000;

export const setupAuditExtractor: Extractor<Sourced<SetupAuditSummary>> = {
  id: "setup-audit",
  version: 1,
  async run(ctx) {
    // Section can't be filtered in SOQL, so the rows are fetched and split here.
    const soql =
      `SELECT Section, CreatedDate, CreatedById FROM SetupAuditTrail ` +
      `WHERE CreatedDate >= ${soqlDateTime(daysBefore(ctx.now, AUDIT_DAYS))} ORDER BY CreatedDate DESC`;
    const rows = await readSoql<{ Section: string | null; CreatedDate: string; CreatedById: string | null }>(ctx, soql, { maxRecords: AUDIT_MAX_ROWS });
    if (rows.status !== "ok") return rows;
    const summary: SetupAuditSummary = { rows: rows.data.length, earliest: null, bySection: {}, configChangesByUser: {}, excludedUserAdmin: 0 };
    for (const row of rows.data) {
      const section = row.Section ?? "Unknown";
      summary.bySection[section] = (summary.bySection[section] ?? 0) + 1;
      if (!summary.earliest || row.CreatedDate < summary.earliest) summary.earliest = row.CreatedDate;
      if (section === USER_ADMIN_SECTION) summary.excludedUserAdmin++;
      else if (row.CreatedById) summary.configChangesByUser[row.CreatedById] = (summary.configChangesByUser[row.CreatedById] ?? 0) + 1;
    }
    return { ...rows, data: summary };
  },
};

// Not every object has an owner: a detail object answers INVALID_FIELD, which
// is a fact about the object, not a gap in the scan.
export function isNoOwnerField(err: unknown): boolean {
  return isUnavailable(err) && err.code === "INVALID_FIELD";
}
