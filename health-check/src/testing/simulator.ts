import { XMLParser } from "fast-xml-parser";
import { strToU8, zipSync } from "fflate";
import type { Transport, TransportRequest, TransportResponse } from "../salesforce/transport.ts";
import * as org from "./synthetic-org.ts";

// Answers requests the way Salesforce would, from the synthetic org. It sits
// behind the real client and allowlist, so nothing about them is mocked. It's
// also strict: SOQL it doesn't recognise gets a 400, as a real org would give.

const CORE_TOTALS = org.RECORD_TOTALS;

export type SimulatorOptions = {
  dailyLimit?: number;
  used?: number;
  /** Fail a query whose text matches, to imitate a permission or feature gap. */
  failQuery?: (soql: string) => { errorCode: string; message: string } | null;
  /** Fields this org doesn't have, by object. Describe leaves them out, and a query that names one fails as a real org would. */
  dropFields?: Record<string, string[]>;
  /**
   * Answers as Salesforce answered a user holding only Barometer Read Only on
   * bc-hc-dev (DECISIONS.md 60 and 68): no Metadata API, no Tooling ApexClass,
   * coverage or CustomObject, no AuthProvider, a COUNT() of every visible report
   * for the allPrivate scope and INVALID_QUERY_SCOPE for its rows, and no access
   * to custom objects' records.
   */
  readOnly?: boolean;
  /** Return a Tooling item with a null Metadata, for the single-row query that matches. */
  withholdMetadata?: (soql: string) => boolean;
  /** The day's async Apex allowance, as /limits reports it. The synthetic org's own by default. */
  asyncApex?: { Max: number; Remaining: number };
  /** Data and file storage, as /limits reports them. The synthetic org's own by default. */
  storage?: typeof org.STORAGE_LIMITS;
  /** False for an org without Salesforce CPQ: no SBQQ licence, and its objects can't be described. */
  cpq?: boolean;
  /** False for an org without Account Engagement: no pi licence, and none of its fields on Lead. */
  pardot?: boolean;
};

type Json = Record<string, unknown>;

function jsonResponse(status: number, body: unknown, limitInfo: string): TransportResponse {
  return { status, header: (name) => (name.toLowerCase() === "sforce-limit-info" ? limitInfo : null), text: JSON.stringify(body) };
}

function page(records: unknown[]): Json {
  return { totalSize: records.length, done: true, records };
}

const reject = (errorCode: string, message: string) => ({ error: { errorCode, message } });
const unsupported = (object: string) => reject("INVALID_TYPE", `sObject type '${object}' is not supported.`);

/** The instant a query's `field >= ...` asks from, as the "+0000" text the synthetic org stores. Empty when it asks from nothing. */
function since(soql: string, field: string): string {
  const value = new RegExp(`\\b${field}\\s*>=\\s*(\\S+?)\\)?(?:\\s|$)`).exec(soql)?.[1];
  return value ? new Date(value).toISOString().replace("Z", "+0000") : "";
}

const LONDON = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/London", year: "numeric", month: "numeric" });
/** Year and month in the synthetic org's zone, as CALENDAR_MONTH(convertTimezone(...)) gives them. */
function londonMonth(value: string): [number, number] {
  const parts = Object.fromEntries(LONDON.formatToParts(new Date(value.replace("+0000", "Z"))).map((p) => [p.type, p.value]));
  return [Number(parts.year), Number(parts.month)];
}

// The Tooling view of the synthetic org's metadata, built from the same files the
// retrieve returns, so the two routes can be compared. Values stay strings, as
// the XML parser gives them; real Tooling sends booleans and numbers, and the
// engine reads both.
const toolingParser = new XMLParser({
  ignoreAttributes: true,
  parseTagValue: false,
  isArray: (name) => ["rules", "actions", "workflowTimeTriggers", "validationRules", "fields", "recordCreates", "recordUpdates", "recordDeletes", "actionCalls", "loops", "recordTypes"].includes(name),
});

type ToolingItem = { Id: string; FullName: string; Metadata: Json };

function buildToolingModel() {
  const items = new Map<string, ToolingItem>();
  const flowDefinitions: Json[] = [];
  const workflowRules: Json[] = [];
  const validationRules: Json[] = [];
  const fields: Json[] = [];
  const recordTypes: Json[] = [];
  const objectIds = new Map(org.CUSTOM_OBJECTS.map((o) => [`${o.DeveloperName}__c`, o.Id.slice(0, 15)]));
  let n = 0;
  for (const [path, xml] of Object.entries(org.METADATA_FILES).sort(([a], [b]) => a.localeCompare(b))) {
    const parsed = toolingParser.parse(xml) as Record<string, Json>;
    if (path.startsWith("flows/")) {
      const name = path.slice(6).replace(/\.flow$/, "");
      const flow = parsed.Flow!;
      const version = org.sfId("301", ++n);
      flowDefinitions.push({ Id: org.sfId("300", n), DeveloperName: name, ActiveVersionId: flow.status === "Active" ? version : null, LatestVersionId: version, NamespacePrefix: null });
      items.set(version, { Id: version, FullName: name, Metadata: flow });
    } else if (path.startsWith("workflows/")) {
      const object = path.slice(10).replace(/\.workflow$/, "");
      for (const rule of ((parsed.Workflow?.rules ?? []) as Json[])) {
        const id = org.sfId("01Q", ++n);
        workflowRules.push({ Id: id, Name: rule.fullName, TableEnumOrId: object, NamespacePrefix: null });
        items.set(id, { Id: id, FullName: `${object}.${String(rule.fullName)}`, Metadata: rule });
      }
    } else if (path.startsWith("objects/")) {
      const object = path.slice(8).replace(/\.object$/, "");
      const definition = parsed.CustomObject ?? {};
      for (const rule of ((definition.validationRules ?? []) as Json[])) {
        const id = org.sfId("03d", ++n);
        validationRules.push({ Id: id, ValidationName: rule.fullName, Active: rule.active === "true", Description: rule.description ?? null, EntityDefinitionId: objectIds.get(object) ?? object, NamespacePrefix: null });
        items.set(id, { Id: id, FullName: `${object}.${String(rule.fullName)}`, Metadata: rule });
      }
      for (const field of ((definition.fields ?? []) as Json[])) {
        fields.push({ EntityDefinition: { QualifiedApiName: object }, QualifiedApiName: field.fullName, Description: field.description ?? null, InlineHelpText: field.inlineHelpText ?? null, NamespacePrefix: null });
      }
      for (const type of ((definition.recordTypes ?? []) as Json[])) {
        recordTypes.push({ Id: org.sfId("012", ++n), Name: type.label, SobjectType: object, IsActive: type.active === "true", NamespacePrefix: null });
      }
    }
  }
  const entities = [
    ...["Account", "Contact", "Lead", "Opportunity", "Case", "Task", "Event"].map((name) => ({ DurableId: name, QualifiedApiName: name, NamespacePrefix: null })),
    ...org.CUSTOM_OBJECTS.map((o) => ({ DurableId: o.Id.slice(0, 15), QualifiedApiName: `${o.NamespacePrefix ? `${o.NamespacePrefix}__` : ""}${o.DeveloperName}__c`, NamespacePrefix: o.NamespacePrefix })),
    // Kinds the custom object list must leave out.
    { DurableId: org.sfId("01I", 90).slice(0, 15), QualifiedApiName: "Routing_Setting__mdt", NamespacePrefix: null },
    { DurableId: org.sfId("01I", 91).slice(0, 15), QualifiedApiName: "Order_Placed__e", NamespacePrefix: null },
  ];
  const approvals = Object.keys(org.METADATA_FILES).filter((p) => p.startsWith("approvalProcesses/")).map((_, i) => ({ Id: org.sfId("04a", i + 1) }));
  return { items, flowDefinitions, workflowRules, validationRules, fields, recordTypes, entities, approvals };
}

const TOOLING = buildToolingModel();

/** What a report's describe would say, from its retrieved definition. The report type offers fields the report doesn't use. */
function reportDescribe(id: string): Json | null {
  const report = org.REPORTS.find((r) => r.Id === id || r.Id.slice(0, 15) === id);
  const xml = report ? org.REPORT_FILES[`reports/${report.FolderName}/${report.DeveloperName}.report`] : undefined;
  if (!report || !xml) return null;
  const parsed = (toolingParser.parse(xml) as { Report: Json }).Report;
  const columns = [parsed.columns ?? []].flat() as Json[];
  const criteria = [((parsed.filter as Json | undefined)?.criteriaItems) ?? []].flat() as Json[];
  return {
    reportMetadata: { id: report.Id, name: parsed.name, detailColumns: columns.map((c) => c.field), reportFilters: criteria.map((c) => ({ column: c.column, operator: "equals", value: c.value })) },
    reportTypeMetadata: { categories: [{ label: "Opportunity", columns: { "Opportunity.Deal_Score__c": { label: "Deal Score" }, "Opportunity.Old_Region__c": { label: "Old Region" } } }] },
  };
}

/** Rows whose object is in the query's `EntityDefinition.QualifiedApiName IN (...)`. */
function inObjects(rows: Json[], soql: string): Json[] {
  const list = soql.match(/EntityDefinition\.QualifiedApiName IN \(([^)]*)\)/)?.[1] ?? "";
  const wanted = new Set([...list.matchAll(/'([^']+)'/g)].map((m) => m[1]));
  return rows.filter((r) => wanted.has(String((r.EntityDefinition as Json).QualifiedApiName)));
}

export class Simulator implements Transport {
  readonly requests: TransportRequest[] = [];
  #used: number;
  #limit: number;
  #failQuery: SimulatorOptions["failQuery"];
  #dropFields: Record<string, string[]>;
  #readOnly: boolean;
  #withholdMetadata: SimulatorOptions["withholdMetadata"];
  #asyncApex: { Max: number; Remaining: number };
  #storage: typeof org.STORAGE_LIMITS;
  #cpq: boolean;
  #pardot: boolean;
  #retrieveChecks = 0;
  // Each retrieve gets its own id, so a recording's status polls tell them apart
  // when a replay runs a different set of modules.
  #retrieves = 0;
  #retrieving: Record<string, string> = org.METADATA_FILES;

  constructor(options: SimulatorOptions = {}) {
    this.#used = options.used ?? 1000;
    this.#limit = options.dailyLimit ?? 100_000;
    this.#failQuery = options.failQuery;
    this.#dropFields = options.dropFields ?? {};
    this.#readOnly = options.readOnly ?? false;
    this.#withholdMetadata = options.withholdMetadata;
    this.#asyncApex = options.asyncApex ?? org.ASYNC_APEX_LIMIT;
    this.#storage = options.storage ?? org.STORAGE_LIMITS;
    this.#cpq = options.cpq ?? true;
    this.#pardot = options.pardot ?? true;
    if (!this.#pardot) this.#dropFields = { ...this.#dropFields, Lead: [...(this.#dropFields.Lead ?? []), "pi__url__c", "pi__score__c", "pi__grade__c"] };
  }

  get #limitInfo(): string {
    return `api-usage=${this.#used}/${this.#limit}`;
  }

  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/services/data/") return jsonResponse(200, [{ version: "60.0" }, { version: org.API_VERSION }], this.#limitInfo);
    if (path.endsWith("/limits")) return jsonResponse(200, { DailyApiRequests: { Max: this.#limit, Remaining: this.#limit - this.#used }, DailyAsyncApexExecutions: this.#asyncApex, ...this.#storage }, this.#limitInfo);

    this.#used++;
    if (path.endsWith("/limits/recordCount")) return jsonResponse(200, { sObjects: org.RECORD_COUNTS }, this.#limitInfo);
    if (path.endsWith("/composite/batch")) return this.#composite(request);
    if (path.includes("/Soap/m/")) return this.#soap(request);

    const report = path.match(/\/analytics\/reports\/([A-Za-z0-9]+)\/describe$/);
    if (report) {
      const body = reportDescribe(report[1]!);
      return body ? jsonResponse(200, body, this.#limitInfo) : jsonResponse(404, [{ errorCode: "NOT_FOUND", message: "The requested resource does not exist" }], this.#limitInfo);
    }
    const describe = path.match(/\/sobjects\/([A-Za-z0-9_]+)\/describe$/);
    if (describe) {
      const name = describe[1]!;
      const d = org.DESCRIBES[name] ?? org.FIELD_DESCRIBES[name] ?? (this.#cpq ? org.CPQ_DESCRIBES[name] : undefined);
      if (!d) return jsonResponse(404, [{ errorCode: "NOT_FOUND", message: "The requested resource does not exist" }], this.#limitInfo);
      const dropped = new Set(this.#dropFields[name] ?? []);
      return jsonResponse(200, { name, ...d, fields: d.fields.filter((f) => !dropped.has((f as { name: string }).name)) }, this.#limitInfo);
    }
    if (path.endsWith("/query") || path.endsWith("/query/")) {
      const soql = url.searchParams.get("q") ?? "";
      const result = this.#answer(soql, path.includes("/tooling/"));
      if ("error" in result) return jsonResponse(400, [result.error], this.#limitInfo);
      return jsonResponse(200, result, this.#limitInfo);
    }
    return jsonResponse(404, [{ errorCode: "NOT_FOUND", message: `No route for ${path}` }], this.#limitInfo);
  }

  #composite(request: TransportRequest): TransportResponse {
    const { batchRequests } = JSON.parse(request.body!) as { batchRequests: Array<{ url: string }> };
    const results = batchRequests.map((sub) => {
      // Imitates the platform behaviour: Tooling through composite comes back empty.
      if (sub.url.includes("/tooling/")) return { statusCode: 200, result: page([]) };
      const soql = new URLSearchParams(sub.url.split("?")[1]).get("q") ?? "";
      const result = this.#answer(soql, false);
      return "error" in result ? { statusCode: 400, result: [result.error] } : { statusCode: 200, result };
    });
    return jsonResponse(200, { hasErrors: false, results }, this.#limitInfo);
  }

  #soap(request: TransportRequest): TransportResponse {
    const body = request.body ?? "";
    const respond = (text: string): TransportResponse => ({ status: 200, header: () => null, text });
    if (this.#readOnly) {
      return {
        status: 500,
        header: () => null,
        text: `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>sf:INSUFFICIENT_ACCESS</faultcode><faultstring>INSUFFICIENT_ACCESS: use of the Metadata API requires a user with the ModifyAllData or ModifyMetadata permissions</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`,
      };
    }
    const envelope = (operation: string, inner: string): string =>
      `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><${operation}Response xmlns="http://soap.sforce.com/2006/04/metadata"><result>${inner}</result></${operation}Response></soapenv:Body></soapenv:Envelope>`;
    if (body.includes("<met:retrieve>")) {
      this.#retrieving = body.includes("<met:name>Report</met:name>") ? org.REPORT_FILES : body.includes("<met:name>EscalationRules</met:name>") ? org.ESCALATION_FILES : body.includes("<met:name>Settings</met:name>") ? org.SETTINGS_FILES : org.METADATA_FILES;
      return respond(envelope("retrieve", `<done>false</done><id>${org.sfId("09S", ++this.#retrieves)}</id><state>Queued</state>`));
    }
    // The first poll isn't ready, so the polling path is exercised.
    this.#retrieveChecks++;
    if (this.#retrieveChecks % 2 === 1) return respond(envelope("checkRetrieveStatus", `<done>false</done><status>InProgress</status>`));
    const zip = zipSync(Object.fromEntries(Object.entries(this.#retrieving).map(([path, text]) => [path, [strToU8(text), { mtime: new Date("2026-01-01T00:00:00Z") }]])) as never);
    return respond(envelope("checkRetrieveStatus", `<done>true</done><status>Succeeded</status><success>true</success><zipFile>${Buffer.from(zip).toString("base64")}</zipFile>`));
  }

  #answer(soql: string, tooling: boolean): Json | { error: { errorCode: string; message: string } } {
    const failure = this.#failQuery?.(soql);
    if (failure) return { error: failure };

    const object = soql.match(/\bFROM\s+([A-Za-z0-9_]+)/i)?.[1] ?? "";
    const dropped = this.#dropFields[object] ?? [];
    const named = dropped.find((f) => new RegExp(`\\b${f}\\b`).test(soql));
    if (named) return reject("INVALID_FIELD", `No such column '${named}' on entity '${object}'.`);
    const result = this.#answerKnown(soql, tooling, object);
    // Rows only carry fields the org has, as on a real org.
    if (dropped.length === 0 || "error" in result || !Array.isArray(result.records)) return result;
    return { ...result, records: (result.records as Json[]).map((r) => Object.fromEntries(Object.entries(r).filter(([k]) => !dropped.includes(k)))) };
  }

  #answerKnown(soql: string, tooling: boolean, object: string): Json | { error: { errorCode: string; message: string } } {
    const count = (n: number): Json => ({ totalSize: n, done: true, records: [] });
    const where = soql.match(/\bWHERE\b(.*)$/is)?.[1] ?? "";

    if (/^SELECT COUNT\(Id\) total,/i.test(soql)) return this.#fieldUsage(soql, object, where);
    if (/^SELECT OwnerId, COUNT\(Id\) c FROM/i.test(soql)) return this.#ownership(object);
    const readOnlyGap = this.#readOnly && (tooling ? ["ApexClass", "ApexTrigger", "ApexOrgWideCoverage", "ApexCodeCoverageAggregate", "CustomObject", "CustomField"] : ["ApexOrgWideCoverage", "ApexCodeCoverageAggregate", "AuthProvider"]).includes(object);
    if (readOnlyGap) return unsupported(object);
    // Tooling returns Metadata and FullName for one row per query.
    const single = /\bSELECT Id, FullName, Metadata FROM\b/i.test(soql);
    if (single) {
      const item = tooling ? this.#toolingItem(soql) : null;
      if (!item) return reject("MALFORMED_QUERY", "When retrieving results with Metadata or FullName fields, the query qualifications must specify no more than one row for retrieval.");
      return page([this.#withholdMetadata?.(soql) ? { ...item, Metadata: null } : item]);
    }

    switch (object) {
      case "Organization":
        return page([{ Id: org.ORG_ID, Name: "Synthetic Co", OrganizationType: "Enterprise Edition", IsSandbox: false, TimeZoneSidKey: "Europe/London", FiscalYearStartMonth: 1, ...org.SHARING_DEFAULTS }]);
      case "User":
        return page(/UserPermissionsSupportUser/.test(soql) ? org.supportUsers : org.usersTable);
      case "UserLicense":
        return page(org.USER_LICENSES);
      case "PermissionSetLicense":
        return page(org.PERMISSION_SET_LICENSES);
      case "PackageLicense":
        return page(org.PACKAGE_LICENSES.filter((p) => (this.#cpq || p.NamespacePrefix !== "SBQQ") && (this.#pardot || p.NamespacePrefix !== "pi")));
      case "SBQQ__PriceRule__c":
        return this.#cpq ? page(org.CPQ_PRICE_RULES) : unsupported(object);
      case "SBQQ__PriceAction__c":
        return this.#cpq ? page(org.CPQ_PRICE_ACTIONS) : unsupported(object);
      case "SBQQ__ProductRule__c":
        return this.#cpq ? page(org.CPQ_PRODUCT_RULES) : unsupported(object);
      case "SBQQ__Quote__c": {
        if (!this.#cpq) return unsupported(object);
        const after = /\bCreatedDate\s*>=\s*(\S+)/i.exec(where)?.[1];
        return this.#keyset(org.CPQ_QUOTES.filter((q) => !after || new Date(q.CreatedDate.replace("+0000", "Z")) >= new Date(after)), soql);
      }
      case "ProcessInstance": {
        const after = /\bCreatedDate\s*>=\s*(\S+)/i.exec(where)?.[1];
        return page(org.CPQ_APPROVALS.filter((a) => !after || new Date(a.CreatedDate.replace("+0000", "Z")) >= new Date(after)).map(({ CreatedDate: _c, ...a }) => a));
      }
      case "Contract": {
        const from = /\bEndDate >= (\d{4}-\d{2}-\d{2})/.exec(where)?.[1] ?? "0000";
        const to = /\bEndDate <= (\d{4}-\d{2}-\d{2})/.exec(where)?.[1] ?? "9999";
        return page(org.CPQ_CONTRACTS.filter((c) => c.EndDate >= from && c.EndDate <= to && /Status = 'Activated'/.test(where) === (c.Status === "Activated")));
      }
      case "LoginHistory": {
        if (/\b(Status|Application)\s*(=|!=|IN|LIKE)/i.test(where)) return reject("INVALID_FIELD", "field 'Status' can not be filtered in a query call");
        // LoginTime is filterable, and every read of it gives a start.
        const since = /\bLoginTime\s*>=\s*(\S+)/i.exec(where)?.[1];
        return page(since ? org.LOGIN_HISTORY.filter((r) => new Date(r.LoginTime.replace("+0000", "Z")) >= new Date(since)) : org.LOGIN_HISTORY);
      }
      case "SetupAuditTrail":
        return /\b(Section|Display|Action)\s*(=|!=|IN|LIKE)/i.test(where) ? reject("INVALID_FIELD", "field 'Section' can not be filtered in a query call") : page(org.SETUP_AUDIT);
      case "SecurityHealthCheck":
        return tooling ? page([{ Score: org.HEALTH_SCORE }]) : unsupported("SecurityHealthCheck");
      case "SecurityHealthCheckRisks":
        return tooling ? page(org.HEALTH_RISKS) : unsupported("SecurityHealthCheckRisks");
      case "PermissionSet":
        if (/IsOwnedByProfile = true/.test(where)) return page(org.PROFILE_SETS);
        if (/PermissionsApiUserOnly = true/.test(where)) return page(org.PERMISSION_SETS.filter((s) => s.PermissionsApiUserOnly));
        return page(/IsOwnedByProfile = false/.test(where) ? org.DOCUMENTED_PERMISSION_SETS : org.PERMISSION_SETS);
      case "PermissionSetGroupComponent":
        return page(org.PERMISSION_SET_GROUP_COMPONENTS);
      case "PermissionSetAssignment": {
        if (/\bPermissionSetGroupId\b/.test(soql)) return page(org.ACCESS_ASSIGNMENTS);
        if (/PermissionsApiUserOnly = true/.test(where)) {
          const sets = new Set(org.PERMISSION_SETS.filter((s) => s.PermissionsApiUserOnly && !s.IsOwnedByProfile).map((s) => s.Id));
          const inactive = new Set(org.usersTable.filter((u) => !u.IsActive).map((u) => u.Id));
          return page(org.PERMISSION_ASSIGNMENTS.filter((a) => sets.has(a.PermissionSetId) && !inactive.has(a.AssigneeId)));
        }
        return page(/IsOwnedByProfile = false/.test(where) ? org.DOCUMENTED_ASSIGNMENTS : org.PERMISSION_ASSIGNMENTS);
      }
      case "WebLink":
        return page(org.WEB_LINKS.filter((l) => !/NamespacePrefix = null/.test(where) || l.NamespacePrefix === null));
      case "Scontrol":
        return page(org.SCONTROLS.filter((c) => !/NamespacePrefix = null/.test(where) || c.NamespacePrefix === null));
      case "ApexPage":
        return page(org.APEX_PAGES.filter((p) => !/NamespacePrefix = null/.test(where) || p.NamespacePrefix === null));
      case "AsyncApexJob": {
        const since = /\bCreatedDate\s*>=\s*(\S+)/i.exec(where)?.[1];
        return page(since ? org.ASYNC_JOBS.filter((j) => new Date(j.CreatedDate.replace("+0000", "Z")) >= new Date(since)) : org.ASYNC_JOBS);
      }
      case "UserRole":
        return page(org.ROLES);
      case "ConnectedApplication":
        return page(org.CONNECTED_APPS);
      case "NamedCredential":
        return page(org.NAMED_CREDENTIALS);
      case "RemoteProxy":
        return tooling ? page(org.REMOTE_SITES) : unsupported("RemoteProxy");
      case "Layout":
        return tooling ? page(org.LAYOUTS) : unsupported("Layout");
      case "CustomObject":
        return tooling ? page(org.CUSTOM_OBJECTS) : unsupported("CustomObject");
      case "EntityDefinition": {
        if (!/SharingModel/.test(soql)) return page(TOOLING.entities);
        const wanted = new Set([...(soql.match(/QualifiedApiName IN \(([^)]*)\)/)?.[1] ?? "").matchAll(/'([^']+)'/g)].map((m) => m[1]));
        return page(Object.entries(org.SHARING_MODELS).filter(([name]) => wanted.has(name)).map(([QualifiedApiName, [InternalSharingModel, ExternalSharingModel]]) => ({ QualifiedApiName, InternalSharingModel, ExternalSharingModel })));
      }
      case "FlowDefinition":
        return tooling ? page(TOOLING.flowDefinitions) : unsupported("FlowDefinition");
      case "WorkflowRule":
        return tooling ? page(TOOLING.workflowRules) : unsupported("WorkflowRule");
      case "WorkflowFieldUpdate":
      case "WorkflowAlert":
      case "WorkflowTask":
      case "WorkflowOutboundMessage":
        return tooling ? page([]) : unsupported(object);
      case "ValidationRule":
        return tooling ? page(TOOLING.validationRules) : unsupported("ValidationRule");
      case "FieldDefinition":
        if (/\bReferenceTo\b/.test(soql)) return page(inObjects(org.RELATIONSHIP_FIELDS, soql));
        return page(inObjects(TOOLING.fields, soql).map(({ InlineHelpText, ...f }) => f));
      case "EntityParticle":
        return page(inObjects(TOOLING.fields, soql).map(({ Description, ...f }) => f));
      case "RecordType":
        return tooling ? page(TOOLING.recordTypes) : unsupported("RecordType");
      case "ProcessDefinition":
        return page(TOOLING.approvals);
      case "AssignmentRule":
        return page(/SobjectType = 'Case'/.test(soql) ? org.CASE_ASSIGNMENT_RULES : org.ASSIGNMENT_RULES);
      case "ProfileLayout":
        return tooling ? page(org.PROFILE_LAYOUTS) : unsupported("ProfileLayout");
      case "DuplicateRule":
        return page(org.DUPLICATE_RULES);
      case "ApexClass":
        return page(org.APEX_CLASSES);
      case "ApexTrigger":
        return page(org.APEX_TRIGGERS);
      case "ApexOrgWideCoverage":
        return page(org.ORG_COVERAGE);
      case "ApexCodeCoverageAggregate":
        return page(org.CLASS_COVERAGE);
      case "CronTrigger":
        return page(org.CRON_TRIGGERS);
      case "Dashboard":
        return page(org.DASHBOARDS);
      case "EmailServicesAddress":
        return page(org.EMAIL_SERVICES);
      case "AuthProvider":
        return page(org.AUTH_PROVIDERS);
      case "Site":
        return unsupported("Site");
      case "PlatformEventSubscriberConfig":
        return this.#readOnly ? reject("INVALID_FIELD", "No such column 'UserId' on entity 'PlatformEventSubscriberConfig'.") : page([]);
      case "Report":
        if (/USING SCOPE allPrivate/i.test(soql)) {
          // Without Manage All Private Reports, the scope is ignored for a count
          // and refused for rows, as bc-hc-dev answered.
          if (/^SELECT COUNT\(\)/i.test(soql)) return count(this.#readOnly ? org.REPORTS.length : org.PRIVATE_REPORTS);
          if (this.#readOnly) return reject("INVALID_QUERY_SCOPE", "Invalid query scope: allPrivate");
          return page(org.PRIVATE_REPORTS > 0 ? [{ Id: org.sfId("00O", 900) }] : []);
        }
        return page(org.REPORTS);
      case "Folder":
        return page(org.REPORT_FOLDERS);
      case "ActivityUsrConnectionStatus":
        return page(org.ACTIVITY_CONNECTIONS);
      case "OpportunityStage":
        return page(org.STAGES);
      case "Opportunity":
        if (/IsWon = true/.test(soql) && /CloseDate < /.test(soql)) {
          const before = soql.match(/CloseDate < (\d{4}-\d{2}-\d{2})/)?.[1] ?? "";
          const earlier = [...org.PRIOR_WINS, ...org.pipeline.opportunities.filter((o) => o.IsWon && o.CloseDate < before)];
          return this.#keyset(earlier.map(({ Id, AccountId, CloseDate }) => ({ Id, AccountId, CloseDate })), soql);
        }
        if (/NextStep != null/.test(soql)) return this.#keyset(org.pipeline.opportunities.filter((o) => !o.IsClosed && o.NextStep !== null).map((o) => ({ Id: o.Id })), soql);
        // Free text the query didn't ask for mustn't come back, as on a real org.
        return this.#keyset(org.pipeline.opportunities.map(({ NextStep, ...o }) => ({ ...o, Name: org.dealName(o.Id) })), soql);
      case "ForecastingType":
        return page(org.FORECAST_TYPES);
      case "ForecastingQuota":
        return page(org.QUOTAS);
      case "Task":
        return this.#keyset(org.activity.tasks, soql);
      case "Event":
        return this.#keyset(org.activity.events, soql);
      case "EmailMessage":
        if (/\bParentId\b/.test(soql)) return this.#caseEmails(soql);
        return this.#keyset(org.activity.emails, soql);
      case "Period":
        return page(org.PERIODS);
      case "ForecastingItem":
        return page(org.FORECAST_ITEMS);
      case "ForecastingFact":
        return page(org.FORECAST_FACTS);
      case "OpportunityHistory":
        return this.#keyset(org.pipeline.history, soql);
      case "Account": {
        if (/GROUP BY Name/i.test(soql)) {
          const sizes = new Map<string, number>();
          for (const a of org.DUPLICATE_ACCOUNTS) sizes.set(a.Name, (sizes.get(a.Name) ?? 0) + 1);
          return page([...sizes].filter(([, c]) => c > 1).sort(([, a], [, b]) => b - a).map(([Name, c]) => ({ Name, c })));
        }
        const named = /\bName IN \(([^)]*)\)/.exec(soql)?.[1];
        if (named) {
          const names = new Set([...named.matchAll(/'((?:[^'\\]|\\.)*)'/g)].map((m) => m[1]!.replace(/\\(.)/g, "$1")));
          return page(org.DUPLICATE_ACCOUNTS.filter((a) => names.has(a.Name)));
        }
        return this.#keyset(org.WON_ACCOUNTS, soql);
      }
      case "LeadHistory":
        return this.#keyset(org.leadFunnel.statusChanges, soql);
      case "Campaign":
        return page(org.CAMPAIGNS);
      case "CampaignMember":
        return this.#keyset(org.leadFunnel.members, soql);
      case "Lead":
        if (/\bpi__url__c\b/.test(soql)) {
          if (/^SELECT COUNT\(\)/i.test(soql)) return count(/pi__grade__c = null/.test(soql) ? org.PARDOT_COUNTS.ungraded : /pi__score__c = null/.test(soql) ? org.PARDOT_COUNTS.unscored : org.PARDOT_COUNTS.synced);
          const since = /\bCreatedDate\s*>=\s*(\S+)/i.exec(where)?.[1];
          return this.#keyset(org.PARDOT_OPEN_LEADS.filter((l) => !since || new Date(l.CreatedDate.replace("+0000", "Z")) >= new Date(since)), soql);
        }
        if (/\bConvertedOpportunityId\b/.test(soql)) return this.#keyset(org.leadFunnel.leads, soql);
        if (/GROUP BY LeadSource/i.test(soql)) return page(/LastActivityDate/i.test(soql) ? org.LEADS.staleBySource : org.LEADS.openBySource);
        if (/^SELECT COUNT\(\)/i.test(soql)) return count(/LastActivityDate/i.test(soql) ? org.LEADS.stale : org.LEADS.open);
        if (/^SELECT Id, Name\b/i.test(soql) && /LastActivityDate = null/.test(soql)) return this.#keyset(org.STALE_LEAD_ROWS, soql);
        break;
      case "Contact":
        if (/^SELECT COUNT\(\)/i.test(soql)) return count(/AccountId = null/.test(soql) ? org.CONTACTS.noAccount : /Email = null/.test(soql) ? org.CONTACTS.noEmail : org.CONTACTS.total);
        if (/^SELECT Id, Name\b/i.test(soql)) return this.#keyset(/AccountId = null/.test(soql) ? org.CONTACTS_NO_ACCOUNT : org.CONTACTS_NO_EMAIL, soql);
        break;
      case "Case":
        return this.#cases(soql);
      case "CaseHistory":
        return page(org.supportCases.history.filter((h) => h.CreatedDate >= since(soql, "CreatedDate")).map(({ Id: _id, Field: _f, ...h }) => h));
      case "CaseMilestone":
        return page(org.supportCases.milestones.filter((m) => m.StartDate >= since(soql, "StartDate")));
      case "CaseStatus":
        return page(org.CASE_STATUSES);
      case "CaseArticle": {
        const closed = new Set(org.supportCases.cases.filter((c) => c.IsClosed && c.ClosedDate! >= since(soql, "ClosedDate")).map((c) => c.Id));
        return page(org.supportCases.articles.filter((a) => closed.has(a.CaseId)).map(({ CaseId }) => ({ CaseId })));
      }
      case "Group":
        if (!/Type = 'Queue'/.test(soql)) break;
        return page([...org.CASE_QUEUES, ...org.OTHER_QUEUES]);
      case "QueueSobject":
        return page(org.QUEUE_SOBJECTS.filter((q) => !/SobjectType = 'Case'/.test(soql) || q.SobjectType === "Case"));
      case "GroupMember":
        return page(org.QUEUE_MEMBERS);
      case "BusinessHours":
        return page(org.BUSINESS_HOURS);
      case "SlaProcess":
        return page(org.SLA_PROCESSES);
      case "MilestoneType":
        return page(org.MILESTONE_TYPES);
      case "ServiceChannel":
        return page(org.SERVICE_CHANNELS);
      case "QueueRoutingConfig":
        return page(org.ROUTING_CONFIGS);
      case "AgentWork":
        return page(org.supportCases.work.filter((w) => w.RequestDateTime >= since(soql, "RequestDateTime")).map(({ Id: _id, ...w }) => w));
      case "Macro":
        return page(org.MACROS);
      case "MacroUsage":
        return page(org.MACRO_USAGE);
      case "QuickText":
        return page(org.QUICK_TEXT);
      case "QuickTextUsage":
        return page(org.QUICK_TEXT_USAGE);
      // Agents aren't enabled in the synthetic org.
      case "BotDefinition":
        return unsupported(object);
      case "KnowledgeArticleVersion":
        return /PublishStatus = 'Online'/.test(soql) ? page(org.ARTICLES) : reject("MALFORMED_QUERY", "Implementation restriction: KnowledgeArticleVersion query must filter on PublishStatus or Id");
      case "KnowledgeArticleViewStat":
        return page(org.ARTICLE_VIEWS);
      // Messaging, chat and voice aren't set up in the synthetic org.
      case "MessagingSession":
      case "LiveChatTranscript":
      case "VoiceCall":
        return unsupported(object);
    }
    return reject("MALFORMED_QUERY", `The synthetic org doesn't answer this query: ${soql.slice(0, 120)}`);
  }

  /** Case reads: monthly and origin counts, open cases, and cases closed in the year. */
  #cases(soql: string): Json | { error: { errorCode: string; message: string } } {
    const all = org.supportCases.cases;
    const month = (field: "CreatedDate" | "ClosedDate") => {
      const after = since(soql, field);
      const groups = new Map<string, { y: number; m: number; Priority: string | null; n: number }>();
      for (const c of all) {
        const value = c[field];
        if (!value || value < after || (field === "ClosedDate" && !c.IsClosed)) continue;
        const [y, m] = londonMonth(value);
        const key = `${y}-${m}-${c.Priority}`;
        const g = groups.get(key) ?? { y, m, Priority: c.Priority, n: 0 };
        g.n++;
        groups.set(key, g);
      }
      return page([...groups.values()]);
    };
    if (/CALENDAR_YEAR\(convertTimezone\(CreatedDate\)\)/.test(soql)) return month("CreatedDate");
    if (/CALENDAR_YEAR\(convertTimezone\(ClosedDate\)\)/.test(soql)) return month("ClosedDate");
    if (/GROUP BY Origin/.test(soql)) {
      const counts = new Map<string | null, number>();
      for (const c of all.filter((x) => x.CreatedDate >= since(soql, "CreatedDate"))) counts.set(c.Origin, (counts.get(c.Origin) ?? 0) + 1);
      return page([...counts].map(([Origin, n]) => ({ Origin, n })));
    }
    if (/WHERE IsClosed = false/.test(soql)) return page(all.filter((c) => !c.IsClosed).map(({ Id, Status, Priority, Origin, OwnerId, CreatedDate, ContactId, AccountId }) => ({ Id, Status, Priority, Origin, OwnerId, CreatedDate, ContactId, AccountId })));
    if (/WHERE IsClosed = true AND ClosedDate >=/.test(soql)) {
      return page(all.filter((c) => c.IsClosed && c.ClosedDate! >= since(soql, "ClosedDate")).map(({ Id, CreatedDate, ClosedDate, Priority, Origin, OwnerId, Type, Reason }) => ({ Id, CreatedDate, ClosedDate, Priority, Origin, OwnerId, Type, Reason })));
    }
    return reject("MALFORMED_QUERY", `The synthetic org doesn't answer this case query: ${soql.slice(0, 120)}`);
  }

  /** Emails on cases: the first replies, or a count of those coming in. */
  #caseEmails(soql: string): Json {
    const after = since(soql, "CreatedDate");
    const recent = new Set(org.supportCases.cases.filter((c) => c.CreatedDate >= after).map((c) => c.Id));
    const incoming = /Incoming = true/.test(soql);
    const rows = org.supportCases.replies.filter((r) => r.Incoming === incoming && recent.has(r.ParentId));
    if (/^SELECT COUNT\(\)/i.test(soql)) return { totalSize: rows.length, done: true, records: [] };
    return page(rows.map(({ ParentId, MessageDate }) => ({ ParentId, MessageDate })));
  }

  #toolingItem(soql: string): ToolingItem | null {
    const id = soql.match(/\bWHERE Id = '([A-Za-z0-9]+)'/)?.[1];
    return id ? (TOOLING.items.get(id) ?? null) : null;
  }

  #keyset(rows: Array<{ Id: string }>, soql: string): Json {
    const after = soql.match(/\bId > '([^']+)'/)?.[1];
    const limit = Number(soql.match(/\bLIMIT (\d+)/)?.[1] ?? rows.length);
    return page([...rows].sort((a, b) => a.Id.localeCompare(b.Id)).filter((r) => !after || r.Id > after).slice(0, limit));
  }

  #fieldUsage(soql: string, object: string, where: string): Json {
    const recent = /CreatedDate/.test(where);
    const totals = CORE_TOTALS[object]!;
    const row: Record<string, number> = { total: recent ? totals.recent : totals.total };
    for (const match of soql.matchAll(/COUNT\(([A-Za-z0-9_]+__c)\) (f\d+)/g)) row[match[2]!] = org.fieldFilled(object, match[1]!, recent);
    return page([row]);
  }

  #ownership(object: string): Json | { error: { errorCode: string; message: string } } {
    if (object === "Opportunity") {
      const byOwner = new Map<string, number>();
      for (const o of org.pipeline.opportunities) byOwner.set(o.OwnerId, (byOwner.get(o.OwnerId) ?? 0) + 1);
      return page([...byOwner].map(([OwnerId, c]) => ({ OwnerId, c })));
    }
    // The read-only user has no access to custom objects' records.
    if (this.#readOnly && object.endsWith("__c")) return unsupported(object);
    const rows = org.OWNERSHIP[object];
    if (rows) return page(rows.map(([n, c]) => ({ OwnerId: org.userId(n), c })));
    return { error: { errorCode: "INVALID_FIELD", message: "No such column 'OwnerId'" } };
  }
}
