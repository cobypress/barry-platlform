// Tries each candidate read-only route as the Barometer Read Only user, before
// any of them goes into the engine. Development org only.
//
//   HC_APPROVED_ORG_ALIASES=bc-hc-dev-read-only node scripts/probe-read-only.ts [--only <probe id prefix>]
//
// It loads the stored connection through TokenStore, refreshing once on a 401,
// and only ever sends GETs: REST and Tooling queries, describes, and Analytics
// report describes. `get` refuses anything else before it leaves the process.
// It prints the HTTP status, Salesforce's error code, and a count and the
// metadata names that came back. bc-hc-dev is Black Cloud's own org, so
// metadata names are fine to print. It never prints a token.
//
// This is deliberately not the engine's client: the point is to try routes the
// allowlist doesn't have yet. Anything that works goes in through the allowlist,
// with a test and a DECISIONS.md row.
import { parseArgs } from "node:util";
import { connectionStore } from "../src/cli/connect.ts";
import { OAuthConnectionProvider } from "../src/salesforce/oauth-provider.ts";
import { instanceOrigin } from "../src/salesforce/hosts.ts";
import type { OrgConnection } from "../src/salesforce/connection.ts";

const CONNECTION = "bc-hc-dev-read-only";
const DEV_ORG_ID = "00Dd300000AhjiT";
const MAX_NAMES = 25;

const { values } = parseArgs({ options: { only: { type: "string" } } });

const provider = new OAuthConnectionProvider(CONNECTION, { store: connectionStore() });
let connection: OrgConnection = await provider.connect();
if (!connection.orgId?.startsWith(DEV_ORG_ID)) throw new Error("The stored connection isn't for bc-hc-dev. Stopped before any request.");
let origin = instanceOrigin(connection.instanceUrl, true);

// GETs to the data API only, and never the routes that run code.
const ALLOWED = /^\/services\/data\/(?:v\d{2,3}\.\d\/(?:query|tooling\/query|sobjects|tooling\/sobjects|analytics\/reports)(?:[/?].*)?)?$/;
const CODE_ROUTES = /executeAnonymous|apexrest|runTestsSynchronous|runTestsAsynchronous|_HttpMethod/i;

type Reply = { status: number; json: unknown; text: string };

async function get(path: string): Promise<Reply> {
  if (!ALLOWED.test(path.split("?")[0]!) || CODE_ROUTES.test(path)) throw new Error(`The probe refuses ${path.split("?")[0]}`);
  // A report run is a GET on /analytics/reports/<id> too. Only its describe is metadata.
  if (/\/analytics\/reports\/[^/?]+(?:\?|$)/.test(path)) throw new Error("The probe never runs a report");
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(`${origin}${path}`, { method: "GET", headers: { Authorization: `Bearer ${connection.accessToken}`, Accept: "application/json" }, redirect: "error" });
    if (res.status === 401 && attempt === 0) {
      connection = await provider.refresh();
      origin = instanceOrigin(connection.instanceUrl, true);
      continue;
    }
    const text = await res.text();
    let json: unknown = null;
    try {
      json = JSON.parse(text);
    } catch {
      // Not JSON: reported by status alone.
    }
    return { status: res.status, json, text };
  }
}

const versions = (await get("/services/data/")).json as Array<{ version: string }>;
const API = `v${versions.map((v) => v.version).sort((a, b) => Number(b) - Number(a))[0]}`;
console.log(`Connected to bc-hc-dev as the read-only user, API ${API}\n`);

type Row = Record<string, unknown>;
type Outcome = { id: string; status: number; code: string; count: number | null; names: string[]; note: string };
const outcomes: Outcome[] = [];

function errorOf(reply: Reply): { code: string; message: string } {
  const first = Array.isArray(reply.json) ? (reply.json[0] as Row | undefined) : undefined;
  return { code: String(first?.errorCode ?? (reply.status >= 400 ? `HTTP ${reply.status}` : "")), message: String(first?.message ?? "").split("\n").at(-1)!.slice(0, 160) };
}

function pick(row: Row, field: string): string {
  return String(field.split(".").reduce<unknown>((value, key) => (value && typeof value === "object" ? (value as Row)[key] : undefined), row) ?? "");
}

async function query(id: string, soql: string, options: { tooling?: boolean; name?: string | string[] } = {}): Promise<Row[] | null> {
  if (values.only && !id.startsWith(values.only)) return null;
  const reply = await get(`/services/data/${API}/${options.tooling ? "tooling/" : ""}query?q=${encodeURIComponent(soql)}`);
  if (reply.status !== 200) {
    const { code, message } = errorOf(reply);
    outcomes.push({ id, status: reply.status, code, count: null, names: [], note: message });
    return null;
  }
  const body = reply.json as { totalSize: number; records: Row[]; done: boolean };
  const fields = [options.name ?? []].flat();
  const names = fields.length ? body.records.map((r) => fields.map((f) => pick(r, f)).join(" | ")) : [];
  outcomes.push({ id, status: 200, code: "", count: body.totalSize, names, note: body.done ? "" : "more pages" });
  return body.records;
}

async function rest(id: string, path: string, summarise: (json: unknown) => { count: number | null; names: string[]; note?: string }): Promise<unknown> {
  if (values.only && !id.startsWith(values.only)) return null;
  const reply = await get(`/services/data/${API}${path}`);
  if (reply.status !== 200) {
    const { code, message } = errorOf(reply);
    outcomes.push({ id, status: reply.status, code, count: null, names: [], note: message });
    return null;
  }
  const s = summarise(reply.json);
  outcomes.push({ id, status: 200, code: "", count: s.count, names: s.names, note: s.note ?? "" });
  return reply.json;
}

// The baseline's failures, for the record.
await query("baseline.tooling.ApexClass", "SELECT Id, Name FROM ApexClass WHERE NamespacePrefix = null", { tooling: true, name: "Name" });
await query("baseline.tooling.ApexOrgWideCoverage", "SELECT PercentCovered FROM ApexOrgWideCoverage", { tooling: true, name: "PercentCovered" });
await query("baseline.tooling.ApexCodeCoverageAggregate", "SELECT ApexClassOrTrigger.Name, NumLinesCovered, NumLinesUncovered FROM ApexCodeCoverageAggregate", { tooling: true, name: "ApexClassOrTrigger.Name" });
await query("baseline.tooling.CustomObject", "SELECT Id, DeveloperName, NamespacePrefix FROM CustomObject", { tooling: true, name: "DeveloperName" });
await query("baseline.rest.AuthProvider", "SELECT DeveloperName, ExecutionUserId FROM AuthProvider", { name: "DeveloperName" });
await query("baseline.rest.ReportAllPrivate", "SELECT COUNT() FROM Report USING SCOPE allPrivate");

// Flows and Process Builders.
const flowViews = await query(
  "flow.rest.FlowDefinitionView",
  "SELECT DurableId, ApiName, Label, ProcessType, TriggerType, TriggerObjectOrEventId, TriggerObjectOrEventLabel, RecordTriggerType, IsActive, ActiveVersionId, LatestVersionId, NamespacePrefix, Description FROM FlowDefinitionView WHERE IsTemplate = false",
  { name: ["ApiName", "ProcessType", "TriggerType", "IsActive", "TriggerObjectOrEventId"] },
);
await query("flow.rest.FlowVersionView", "SELECT DurableId, FlowDefinitionViewId, VersionNumber, Status, ApiVersionRuntime, ProcessType, RunInMode, Description FROM FlowVersionView WHERE Status = 'Active'", {
  name: ["FlowDefinitionViewId", "VersionNumber", "Status", "ApiVersionRuntime"],
});
await query("flow.tooling.FlowDefinition", "SELECT Id, DeveloperName, ActiveVersionId, LatestVersionId, NamespacePrefix, Description FROM FlowDefinition", { tooling: true, name: "DeveloperName" });
const flows = await query("flow.tooling.Flow", "SELECT Id, DefinitionId, MasterLabel, ProcessType, Status, ApiVersion, VersionNumber, Description FROM Flow WHERE Status = 'Active'", {
  tooling: true,
  name: ["MasterLabel", "ProcessType", "ApiVersion"],
});
const firstFlow = flows?.[0]?.Id ?? (flowViews?.find((f) => f.ActiveVersionId)?.ActiveVersionId as string | undefined);
if (firstFlow) {
  const rows = await query("flow.tooling.Flow.Metadata", `SELECT Id, FullName, Metadata FROM Flow WHERE Id = '${firstFlow}'`, { tooling: true, name: "FullName" });
  const meta = rows?.[0]?.Metadata as Row | undefined;
  if (meta) outcomes.at(-1)!.note = `Metadata keys: ${Object.keys(meta).filter((k) => meta[k] !== null && !(Array.isArray(meta[k]) && (meta[k] as unknown[]).length === 0)).join(", ").slice(0, 300)}`;
}

// Workflow rules, validation rules, fields, record types, objects.
const workflowRules = await query("wf.tooling.WorkflowRule", "SELECT Id, Name, TableEnumOrId, NamespacePrefix FROM WorkflowRule", { tooling: true, name: ["Name", "TableEnumOrId"] });
if (workflowRules?.[0]) {
  const rows = await query("wf.tooling.WorkflowRule.Metadata", `SELECT Id, FullName, Metadata FROM WorkflowRule WHERE Id = '${workflowRules[0].Id}'`, { tooling: true, name: "FullName" });
  const meta = rows?.[0]?.Metadata as Row | undefined;
  if (meta) outcomes.at(-1)!.note = `active=${meta.active} description=${meta.description ? "yes" : "no"} timeTriggers=${(meta.workflowTimeTriggers as unknown[] | undefined)?.length ?? 0} actions=${(meta.actions as unknown[] | undefined)?.length ?? 0}`;
}
const validationRules = await query("vr.tooling.ValidationRule", "SELECT Id, ValidationName, Active, Description, EntityDefinitionId, NamespacePrefix FROM ValidationRule", {
  tooling: true,
  name: ["ValidationName", "Active", "EntityDefinitionId"],
});
if (validationRules?.[0]) {
  const rows = await query("vr.tooling.ValidationRule.Metadata", `SELECT Id, FullName, Metadata FROM ValidationRule WHERE Id = '${validationRules[0].Id}'`, { tooling: true, name: "FullName" });
  const meta = rows?.[0]?.Metadata as Row | undefined;
  if (meta) outcomes.at(-1)!.note = `formula length ${String(meta.errorConditionFormula ?? "").length}`;
}
await query("field.tooling.CustomField", "SELECT Id, DeveloperName, TableEnumOrId, NamespacePrefix, Description, InlineHelpText FROM CustomField WHERE NamespacePrefix = null", {
  tooling: true,
  name: ["TableEnumOrId", "DeveloperName", "Description"],
});
await query("field.tooling.CustomField.noDescription", "SELECT Id, DeveloperName, TableEnumOrId, NamespacePrefix FROM CustomField WHERE NamespacePrefix = null", {
  tooling: true,
  name: ["TableEnumOrId", "DeveloperName"],
});
await query("field.rest.FieldDefinition", "SELECT QualifiedApiName, Description, EntityDefinitionId FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'Account'", {
  name: ["QualifiedApiName", "Description"],
});
await query("field.tooling.FieldDefinition", "SELECT QualifiedApiName, Description FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'Account'", {
  tooling: true,
  name: ["QualifiedApiName", "Description"],
});
await query("rt.rest.RecordType", "SELECT SobjectType, DeveloperName, IsActive, NamespacePrefix FROM RecordType", { name: ["SobjectType", "DeveloperName", "IsActive"] });
await query("obj.rest.EntityDefinition", "SELECT DurableId, QualifiedApiName, NamespacePrefix, KeyPrefix FROM EntityDefinition WHERE IsCustomizable = true AND IsCustomSetting = false", {
  name: ["QualifiedApiName", "DurableId"],
});
await query("obj.tooling.EntityDefinition", "SELECT DurableId, QualifiedApiName, NamespacePrefix, KeyPrefix FROM EntityDefinition WHERE IsCustomizable = true AND IsCustomSetting = false", {
  tooling: true,
  name: ["QualifiedApiName", "DurableId"],
});
await rest("obj.rest.describeGlobal", "/sobjects/", (json) => {
  const sobjects = (json as { sobjects: Array<{ name: string; custom: boolean; customSetting: boolean }> }).sobjects;
  const custom = sobjects.filter((s) => s.custom && !s.customSetting && s.name.endsWith("__c"));
  return { count: custom.length, names: custom.map((s) => s.name), note: `${sobjects.length} objects in all` };
});
await rest("field.rest.describe.Account", "/sobjects/Account/describe", (json) => {
  const fields = (json as { fields: Array<{ name: string; custom: boolean; inlineHelpText: string | null }> }).fields.filter((f) => f.custom);
  return { count: fields.length, names: fields.map((f) => `${f.name} help=${f.inlineHelpText ? "yes" : "no"}`) };
});

// Approval processes and assignment rules, which the retrieve also covered.
await query("ap.rest.ProcessDefinition", "SELECT Id, DeveloperName, TableEnumOrId, State, Type FROM ProcessDefinition", { name: ["DeveloperName", "TableEnumOrId", "State"] });
await query("ar.rest.AssignmentRule", "SELECT Id, Name, SobjectType, Active FROM AssignmentRule", { name: ["Name", "SobjectType", "Active"] });

// Apex through REST.
await query("apex.rest.ApexClass", "SELECT Id, Name, ApiVersion, Status, LengthWithoutComments, Body FROM ApexClass WHERE NamespacePrefix = null", { name: ["Name", "ApiVersion"] });
const bodies = outcomes.at(-1);
if (bodies?.id === "apex.rest.ApexClass" && bodies.status === 200) bodies.note = "Body selected";
await query("apex.rest.ApexTrigger", "SELECT Id, Name, TableEnumOrId, ApiVersion, Status FROM ApexTrigger WHERE NamespacePrefix = null", { name: ["Name", "TableEnumOrId"] });
await query("apex.rest.ApexTestResult", "SELECT Id, Outcome, ApexClass.Name, MethodName FROM ApexTestResult ORDER BY TestTimestamp DESC LIMIT 20", { name: ["ApexClass.Name", "Outcome"] });
await query("apex.rest.ApexCodeCoverageAggregate", "SELECT ApexClassOrTriggerId, NumLinesCovered, NumLinesUncovered FROM ApexCodeCoverageAggregate");
await query("apex.tooling.ApexTrigger", "SELECT Id, Name FROM ApexTrigger", { tooling: true, name: "Name" });

// Where fields are used: the dependency API, in place of scanning metadata for names.
await query(
  "dep.tooling.MetadataComponentDependency",
  "SELECT MetadataComponentName, MetadataComponentType, RefMetadataComponentName, RefMetadataComponentType FROM MetadataComponentDependency WHERE RefMetadataComponentType = 'CustomField'",
  { tooling: true, name: ["MetadataComponentType", "MetadataComponentName", "RefMetadataComponentName"] },
);

await query(
  "dep.tooling.MetadataComponentDependency.notLayouts",
  "SELECT MetadataComponentId, MetadataComponentName, MetadataComponentType, RefMetadataComponentId, RefMetadataComponentName, RefMetadataComponentType FROM MetadataComponentDependency WHERE MetadataComponentType != 'Layout'",
  { tooling: true, name: ["MetadataComponentType", "MetadataComponentName", "RefMetadataComponentType", "RefMetadataComponentName", "RefMetadataComponentId"] },
);
await query("field.rest.FieldDefinition.custom", "SELECT DurableId, QualifiedApiName, Description, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'Account' AND DeveloperName LIKE 'BT%'", {
  name: ["QualifiedApiName", "DurableId", "Description"],
});
await query("field.tooling.FieldDefinition.widget", "SELECT DurableId, QualifiedApiName, Description, InlineHelpText, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName = 'BT_Widget__c'", {
  tooling: true,
  name: ["QualifiedApiName", "DurableId", "Description", "InlineHelpText"],
});
const entities = await query("obj.tooling.EntityDefinition.custom", "SELECT DurableId, QualifiedApiName, DeveloperName, NamespacePrefix FROM EntityDefinition WHERE IsCustomizable = true AND IsCustomSetting = false", { tooling: true });
if (entities) {
  const custom = entities.filter((e) => /__c$/.test(String(e.QualifiedApiName)));
  outcomes.at(-1)!.note = `${custom.length} ending __c`;
  outcomes.at(-1)!.names = custom.map((e) => `${e.QualifiedApiName} | ${e.DurableId} | ${e.NamespacePrefix ?? ""}`);
}
await rest("obj.rest.describe.BT_Widget__c", "/sobjects/BT_Widget__c/describe", (json) => {
  const fields = (json as { fields: Array<{ name: string; custom: boolean; inlineHelpText: string | null }> }).fields.filter((f) => f.custom);
  return { count: fields.length, names: fields.map((f) => `${f.name} help=${f.inlineHelpText ? "yes" : "no"}`) };
});
await query("rt.tooling.RecordType", "SELECT Id, Name, SobjectType, IsActive, NamespacePrefix FROM RecordType", { tooling: true, name: ["SobjectType", "Name", "IsActive"] });

// Reports: the list, the Analytics API list, and one report's describe.
const reports = await query("report.rest.Report", "SELECT Id, DeveloperName, OwnerId, Format FROM Report", { name: ["DeveloperName", "Format", "OwnerId"] });
await rest("report.analytics.list", "/analytics/reports", (json) => {
  const rows = json as Array<{ name: string }>;
  return { count: rows.length, names: rows.map((r) => r.name), note: "recently viewed only, per the docs" };
});
await query("report.rest.ReportAllPrivate.owners", "SELECT Id, DeveloperName, OwnerId FROM Report USING SCOPE allPrivate", { name: ["DeveloperName", "OwnerId"] });
await query("report.rest.Folder", "SELECT Id, DeveloperName, Type, AccessType FROM Folder WHERE Type = 'Report'", { name: ["DeveloperName", "AccessType"] });
for (const report of (reports ?? []).filter((r) => String(r.DeveloperName).startsWith("BT_")).concat((reports ?? []).slice(0, 1))) {
  await rest(`report.analytics.describe.${report.DeveloperName}`, `/analytics/reports/${report.Id}/describe`, (json) => {
    const meta = (json as { reportMetadata: { detailColumns?: string[]; groupingsDown?: Array<{ name: string }>; reportFilters?: Array<{ column: string }>; aggregates?: string[] } }).reportMetadata;
    const columns = [...(meta.detailColumns ?? []), ...(meta.groupingsDown ?? []).map((g) => g.name), ...(meta.reportFilters ?? []).map((f) => `filter:${f.column}`)];
    return { count: columns.length, names: columns };
  });
}

// Other run-as sources the baseline marked incomplete.
await query("runas.tooling.PlatformEventSubscriberConfig", "SELECT DeveloperName, UserId FROM PlatformEventSubscriberConfig", { tooling: true, name: "DeveloperName" });

// Second round: what the Tooling route for the metadata bundle needs.
for (const api of ["rest", "tooling"] as const) {
  await query(`x.${api}.FieldDefinition.in`, "SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, Description, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN ('Account', 'BT_Widget__c') AND QualifiedApiName LIKE 'BT%'", {
    tooling: api === "tooling",
    name: ["EntityDefinition.QualifiedApiName", "QualifiedApiName", "Description"],
  });
  await query(`x.${api}.EntityParticle.in`, "SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, InlineHelpText FROM EntityParticle WHERE EntityDefinition.QualifiedApiName IN ('Account', 'BT_Widget__c') AND QualifiedApiName LIKE 'BT%'", {
    tooling: api === "tooling",
    name: ["EntityDefinition.QualifiedApiName", "QualifiedApiName", "InlineHelpText"],
  });
}
for (const type of ["WorkflowFieldUpdate", "WorkflowAlert", "WorkflowTask", "WorkflowOutboundMessage"]) {
  await query(`x.tooling.${type}`, `SELECT Id, Name, NamespacePrefix FROM ${type}`, { tooling: true, name: "Name" });
}
await query("x.rest.ProcessDefinition.approval", "SELECT Id FROM ProcessDefinition WHERE Type = 'Approval'");
const own = await query("x.tooling.FlowDefinition.own", "SELECT Id, DeveloperName, ActiveVersionId, LatestVersionId, NamespacePrefix FROM FlowDefinition WHERE NamespacePrefix = null", { tooling: true, name: ["DeveloperName", "ActiveVersionId"] });
const stamp = own?.find((f) => f.DeveloperName === "BT_Account_Region_Stamp");
if (stamp) {
  const rows = await query("x.tooling.Flow.Metadata.stamp", `SELECT Id, Status, Metadata FROM Flow WHERE Id = '${stamp.ActiveVersionId}'`, { tooling: true, name: "Status" });
  if (rows?.[0]) console.log(JSON.stringify(rows[0].Metadata, (k, v) => (v === null || (Array.isArray(v) && v.length === 0) ? undefined : v)).slice(0, 1800));
}
const vr = await query("x.tooling.ValidationRule.Metadata.many", "SELECT Id, Metadata FROM ValidationRule", { tooling: true });
if (vr === null) outcomes.at(-1)!.note += " (Metadata on more than one row)";

console.log("| Probe | HTTP | Code | Rows | Note |\n| --- | --- | --- | --- | --- |");
for (const o of outcomes) console.log(`| ${o.id} | ${o.status} | ${o.code || "-"} | ${o.count ?? "-"} | ${o.note.replaceAll("|", "/") || "-"} |`);
console.log("");
for (const o of outcomes.filter((x) => x.names.length)) {
  console.log(`${o.id}: ${o.names.slice(0, MAX_NAMES).join("; ")}${o.names.length > MAX_NAMES ? `; and ${o.names.length - MAX_NAMES} more` : ""}`);
}
