import { redactSecrets } from "../privacy/secrets.ts";
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
/** A record type by its developer name and its label, which customer text uses (DECISIONS.md 412). */
export type ObjectSummary = { name: string; fields: FieldSummary[]; recordTypes: Array<{ developerName: string; label: string; active: boolean }> };

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
  version: 2,
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
            recordTypes: d.recordTypeInfos.filter((r) => !r.master).map((r) => ({ developerName: r.developerName, label: r.name, active: r.active })),
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
  // Counts how many records fill each field.
  records: true,
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
//
// From Tooling's EntityDefinition, not CustomObject: a user with View Setup but
// not Customize Application gets INVALID_TYPE from CustomObject (DECISIONS.md 73).
// A custom object's DurableId is its 01I id, in 15 characters, and the same id
// ValidationRule.EntityDefinitionId holds. Custom metadata types, platform events
// and external objects have other suffixes, and custom settings are filtered out.
export const customObjectsExtractor: Extractor<Sourced<CustomObjectRow[]>> = {
  id: "custom-objects",
  version: 2,
  async run(ctx) {
    const rows = await readSoql<{ DurableId: string; QualifiedApiName: string; NamespacePrefix: string | null }>(
      ctx,
      "SELECT DurableId, QualifiedApiName, NamespacePrefix FROM EntityDefinition WHERE IsCustomizable = true AND IsCustomSetting = false",
      { api: "tooling" },
    );
    if (rows.status !== "ok") return rows;
    const data = rows.data
      .filter((r) => /__c$/.test(r.QualifiedApiName) && /^01I/.test(r.DurableId))
      .map((r) => {
        const prefix = r.NamespacePrefix ? `${r.NamespacePrefix}__` : "";
        const bare = r.QualifiedApiName.startsWith(prefix) ? r.QualifiedApiName.slice(prefix.length) : r.QualifiedApiName;
        return { Id: r.DurableId, DeveloperName: bare.replace(/__c$/, ""), NamespacePrefix: r.NamespacePrefix };
      });
    return { ...rows, data };
  },
};

export const duplicateRulesExtractor: Extractor<Sourced<Array<{ DeveloperName: string; SobjectType: string; IsActive: boolean }>>> = {
  id: "duplicate-rules",
  version: 1,
  run: (ctx) => readSoql(ctx, "SELECT DeveloperName, SobjectType, IsActive FROM DuplicateRule"),
};

/**
 * One setup change that isn't user administration, as the trail records it
 * (DECISIONS.md 461). For the customer's own documents only: no rule reads it.
 */
export type SetupAuditChange = {
  CreatedDate: string;
  Section: string | null;
  Action: string | null;
  /** Salesforce's sentence for the change, redacted as it's read, so a key isn't kept even in the checkpoint. */
  Display: string | null;
  CreatedById: string | null;
  /** CreatedBy.Name: who made it, for a user the run's user list doesn't hold. */
  CreatedByName: string | null;
  /** The username someone was logged in as when they made it. */
  DelegateUser: string | null;
};

export type SetupAuditSummary = {
  rows: number;
  earliest: string | null;
  bySection: Record<string, number>;
  configChangesByUser: Record<string, number>;
  excludedUserAdmin: number;
  /** In the order Salesforce gave them, newest first. Missing from a checkpoint made before version 2. */
  changes: SetupAuditChange[];
};

type SetupAuditRow = Omit<SetupAuditChange, "CreatedByName"> & { CreatedBy: { Name: string | null } | null };

const USER_ADMIN_SECTION = "Manage Users";
/** How far back the trail is read: Salesforce keeps it for 180 days (DECISIONS.md 421). */
export const SETUP_AUDIT_DAYS = 180;
const AUDIT_MAX_ROWS = 2000;

export const setupAuditExtractor: Extractor<Sourced<SetupAuditSummary>> = {
  id: "setup-audit",
  version: 2,
  async run(ctx) {
    // Section can't be filtered in SOQL, so the rows are fetched and split here.
    const soql =
      `SELECT Action, Section, Display, CreatedDate, CreatedById, CreatedBy.Name, DelegateUser FROM SetupAuditTrail ` +
      `WHERE CreatedDate >= ${soqlDateTime(daysBefore(ctx.now, SETUP_AUDIT_DAYS))} ORDER BY CreatedDate DESC`;
    const rows = await readSoql<SetupAuditRow>(ctx, soql, { maxRecords: AUDIT_MAX_ROWS });
    if (rows.status !== "ok") return rows;
    const summary: SetupAuditSummary = { rows: rows.data.length, earliest: null, bySection: {}, configChangesByUser: {}, excludedUserAdmin: 0, changes: [] };
    for (const row of rows.data) {
      const section = row.Section ?? "Unknown";
      summary.bySection[section] = (summary.bySection[section] ?? 0) + 1;
      if (!summary.earliest || row.CreatedDate < summary.earliest) summary.earliest = row.CreatedDate;
      // User administration is mostly about people (password resets, who was
      // given what), so it's counted and its rows go no further.
      if (section === USER_ADMIN_SECTION) {
        summary.excludedUserAdmin++;
        continue;
      }
      if (row.CreatedById) summary.configChangesByUser[row.CreatedById] = (summary.configChangesByUser[row.CreatedById] ?? 0) + 1;
      summary.changes.push({
        CreatedDate: row.CreatedDate,
        Section: row.Section ?? null,
        Action: row.Action ?? null,
        Display: row.Display == null ? null : redactSecrets(row.Display),
        CreatedById: row.CreatedById ?? null,
        CreatedByName: row.CreatedBy?.Name ?? null,
        DelegateUser: row.DelegateUser ?? null,
      });
    }
    return { ...rows, data: summary };
  },
};

// Not every object has an owner: a detail object answers INVALID_FIELD, which
// is a fact about the object, not a gap in the scan.
export function isNoOwnerField(err: unknown): boolean {
  return isUnavailable(err) && err.code === "INVALID_FIELD";
}
