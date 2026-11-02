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

export class Simulator implements Transport {
  readonly requests: TransportRequest[] = [];
  #used: number;
  #limit: number;
  #failQuery: SimulatorOptions["failQuery"];
  #retrieveChecks = 0;

  constructor(options: SimulatorOptions = {}) {
    this.#used = options.used ?? 1000;
    this.#limit = options.dailyLimit ?? 100_000;
    this.#failQuery = options.failQuery;
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

    const describe = path.match(/\/sobjects\/([A-Za-z0-9_]+)\/describe$/);
    if (describe) {
      const d = org.DESCRIBES[describe[1]!];
      return d ? jsonResponse(200, { name: describe[1], ...d }, this.#limitInfo) : jsonResponse(404, [{ errorCode: "NOT_FOUND", message: "Not found" }], this.#limitInfo);
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
    const envelope = (operation: string, inner: string): string =>
      `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><${operation}Response xmlns="http://soap.sforce.com/2006/04/metadata"><result>${inner}</result></${operation}Response></soapenv:Body></soapenv:Envelope>`;
    if (body.includes("<met:retrieve>")) return respond(envelope("retrieve", `<done>false</done><id>${org.sfId("09S", 1)}</id><state>Queued</state>`));
    // The first poll isn't ready, so the polling path is exercised.
    this.#retrieveChecks++;
    if (this.#retrieveChecks % 2 === 1) return respond(envelope("checkRetrieveStatus", `<done>false</done><status>InProgress</status>`));
    const zip = zipSync(Object.fromEntries(Object.entries(org.METADATA_FILES).map(([path, text]) => [path, [strToU8(text), { mtime: new Date("2026-01-01T00:00:00Z") }]])) as never);
    return respond(envelope("checkRetrieveStatus", `<done>true</done><status>Succeeded</status><success>true</success><zipFile>${Buffer.from(zip).toString("base64")}</zipFile>`));
  }

  #answer(soql: string, tooling: boolean): Json | { error: { errorCode: string; message: string } } {
    const failure = this.#failQuery?.(soql);
    if (failure) return { error: failure };

    const object = soql.match(/\bFROM\s+([A-Za-z0-9_]+)/i)?.[1] ?? "";
    const count = (n: number): Json => ({ totalSize: n, done: true, records: [] });
    const where = soql.match(/\bWHERE\b(.*)$/is)?.[1] ?? "";

    if (/^SELECT COUNT\(Id\) total,/i.test(soql)) return this.#fieldUsage(soql, object, where);
    if (/^SELECT OwnerId, COUNT\(Id\) c FROM/i.test(soql)) return this.#ownership(object);

    switch (object) {
      case "Organization":
        return page([{ Id: org.ORG_ID, OrganizationType: "Enterprise Edition", IsSandbox: false, TimeZoneSidKey: "Europe/London", FiscalYearStartMonth: 1, ...org.SHARING_DEFAULTS }]);
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
        return page([]);
      case "Report":
        return page(org.REPORTS);
      case "ActivityUsrConnectionStatus":
        return page(org.ACTIVITY_CONNECTIONS);
      case "OpportunityStage":
        return page(org.STAGES);
      case "Opportunity":
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
    const rows = org.OWNERSHIP[object];
    if (rows) return page(rows.map(([n, c]) => ({ OwnerId: org.userId(n), c })));
    return { error: { errorCode: "INVALID_FIELD", message: "No such column 'OwnerId'" } };
  }
}
