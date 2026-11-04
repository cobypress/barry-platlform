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
  #retrieveChecks = 0;
  #retrieving: Record<string, string> = org.METADATA_FILES;

  constructor(options: SimulatorOptions = {}) {
    this.#used = options.used ?? 1000;
    this.#limit = options.dailyLimit ?? 100_000;
    this.#failQuery = options.failQuery;
    this.#dropFields = options.dropFields ?? {};
    this.#readOnly = options.readOnly ?? false;
    this.#withholdMetadata = options.withholdMetadata;
  }

  get #limitInfo(): string {
    return `api-usage=${this.#used}/${this.#limit}`;
  }

  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/services/data/") return jsonResponse(200, [{ version: "60.0" }, { version: org.API_VERSION }], this.#limitInfo);
    if (path.endsWith("/limits")) return jsonResponse(200, { DailyApiRequests: { Max: this.#limit, Remaining: this.#limit - this.#used } }, this.#limitInfo);

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
      const d = org.DESCRIBES[name] ?? org.FIELD_DESCRIBES[name];
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
      this.#retrieving = body.includes("<met:name>Report</met:name>") ? org.REPORT_FILES : org.METADATA_FILES;
      return respond(envelope("retrieve", `<done>false</done><id>${org.sfId("09S", 1)}</id><state>Queued</state>`));
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
        return page(org.usersTable);
      case "UserLicense":
        return page(org.USER_LICENSES);
      case "PermissionSetLicense":
        return page(org.PERMISSION_SET_LICENSES);
      case "LoginHistory":
        return /\b(Status|Application)\s*(=|!=|IN|LIKE)/i.test(where) ? reject("INVALID_FIELD", "field 'Status' can not be filtered in a query call") : page(org.LOGIN_HISTORY);
      case "SetupAuditTrail":
        return /\b(Section|Display|Action)\s*(=|!=|IN|LIKE)/i.test(where) ? reject("INVALID_FIELD", "field 'Section' can not be filtered in a query call") : page(org.SETUP_AUDIT);
      case "SecurityHealthCheck":
        return tooling ? page([{ Score: org.HEALTH_SCORE }]) : unsupported("SecurityHealthCheck");
      case "SecurityHealthCheckRisks":
        return tooling ? page(org.HEALTH_RISKS) : unsupported("SecurityHealthCheckRisks");
      case "PermissionSet":
        return page(org.PERMISSION_SETS);
      case "PermissionSetAssignment":
        return page(org.PERMISSION_ASSIGNMENTS);
      case "Layout":
        return tooling ? page(org.LAYOUTS) : unsupported("Layout");
      case "CustomObject":
        return tooling ? page(org.CUSTOM_OBJECTS) : unsupported("CustomObject");
      case "EntityDefinition":
        return page(TOOLING.entities);
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
        return page(inObjects(TOOLING.fields, soql).map(({ InlineHelpText, ...f }) => f));
      case "EntityParticle":
        return page(inObjects(TOOLING.fields, soql).map(({ Description, ...f }) => f));
      case "RecordType":
        return tooling ? page(TOOLING.recordTypes) : unsupported("RecordType");
      case "ProcessDefinition":
        return page(TOOLING.approvals);
      case "AssignmentRule":
        return page(org.ASSIGNMENT_RULES);
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
        return this.#keyset(org.pipeline.opportunities.map(({ NextStep, ...o }) => o), soql);
      case "ForecastingType":
        return page(org.FORECAST_TYPES);
      case "ForecastingQuota":
        return page(org.QUOTAS);
      case "Task":
        return this.#keyset(org.activity.tasks, soql);
      case "Event":
        return this.#keyset(org.activity.events, soql);
      case "EmailMessage":
        return this.#keyset(org.activity.emails, soql);
      case "Period":
        return page(org.PERIODS);
      case "ForecastingItem":
        return page(org.FORECAST_ITEMS);
      case "ForecastingFact":
        return page(org.FORECAST_FACTS);
      case "OpportunityHistory":
        return this.#keyset(org.pipeline.history, soql);
      case "Account":
        return this.#keyset(org.WON_ACCOUNTS, soql);
      case "LeadHistory":
        return this.#keyset(org.leadFunnel.statusChanges, soql);
      case "Campaign":
        return page(org.CAMPAIGNS);
      case "CampaignMember":
        return this.#keyset(org.leadFunnel.members, soql);
      case "Lead":
        if (/\bConvertedOpportunityId\b/.test(soql)) return this.#keyset(org.leadFunnel.leads, soql);
        if (/GROUP BY LeadSource/i.test(soql)) return page(/LastActivityDate/i.test(soql) ? org.LEADS.staleBySource : org.LEADS.openBySource);
        if (/^SELECT COUNT\(\)/i.test(soql)) return count(/LastActivityDate/i.test(soql) ? org.LEADS.stale : org.LEADS.open);
        break;
      case "Contact":
        if (/^SELECT COUNT\(\)/i.test(soql)) return count(/AccountId = null/.test(soql) ? org.CONTACTS.noAccount : /Email = null/.test(soql) ? org.CONTACTS.noEmail : org.CONTACTS.total);
        break;
    }
    return reject("MALFORMED_QUERY", `The synthetic org doesn't answer this query: ${soql.slice(0, 120)}`);
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
