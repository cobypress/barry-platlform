import { XMLParser } from "fast-xml-parser";
import { unzipSync, strFromU8 } from "fflate";
import { BUDGET_DEFAULTS, SF_DEFAULTS } from "../config.ts";
import { authorise, ForbiddenOperationError, type SfRequest } from "./allowlist.ts";
import { ApiBudget, BudgetStopError } from "./budget.ts";
import type { ConnectionProvider, OrgConnection } from "./connection.ts";
import { instanceOrigin } from "./hosts.ts";
import { fetchTransport, type Transport, type TransportResponse } from "./transport.ts";

export type QueryApi = "rest" | "tooling";

export type QueryOptions = {
  api?: QueryApi;
  /** Stop paging after this many rows. `truncated` on the result says it happened. */
  maxRecords?: number;
};

export type QueryResult<T> = { records: T[]; totalSize: number; truncated: boolean };

export type MetadataMember = { name: string; members: string[] };

/** A Salesforce response that isn't a success. `code` is Salesforce's own, such as INVALID_TYPE. */
export class SalesforceError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(`${code}: ${message}`);
    this.name = "SalesforceError";
    this.status = status;
    this.code = code;
  }
}

// Org shape or permissions, not a fault: the object isn't there or we can't see
// it, so the caller reports "not measurable" rather than failing the run.
const UNAVAILABLE_CODES = new Set([
  "INVALID_TYPE",
  "INVALID_FIELD",
  "MALFORMED_QUERY",
  "INSUFFICIENT_ACCESS",
  "INSUFFICIENT_ACCESS_OR_READONLY",
  "API_DISABLED_FOR_ORG",
  "NOT_FOUND",
  "EXCEEDED_ID_LIMIT",
  "OPERATION_TOO_LARGE",
  "QUERY_TIMEOUT",
  "UNKNOWN_EXCEPTION",
]);

export function isUnavailable(err: unknown): err is SalesforceError {
  return err instanceof SalesforceError && UNAVAILABLE_CODES.has(err.code);
}

const xml = new XMLParser({ removeNSPrefix: true, ignoreAttributes: true, parseTagValue: false });

function escapeXml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&apos;");
}

function soapEnvelope(token: string, body: string): string {
  return (
    `<?xml version="1.0" encoding="UTF-8"?>` +
    `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:met="http://soap.sforce.com/2006/04/metadata">` +
    `<soapenv:Header><met:SessionHeader><met:sessionId>${escapeXml(token)}</met:sessionId></met:SessionHeader></soapenv:Header>` +
    `<soapenv:Body>${body}</soapenv:Body></soapenv:Envelope>`
  );
}

const QUERY_MORE = /\/query\/[A-Za-z0-9]{15,18}-\d+$/;

export { instanceOrigin };

type SendOptions = { soapOperation?: string; contentType?: string };

// The whole read surface. There's deliberately no generic request method, and a
// test pins this list, so adding a way to reach the org is a visible decision.
export const CLIENT_METHODS = [
  "apiVersion",
  "orgId",
  "connectionKind",
  "budget",
  "refreshLimits",
  // What refreshLimits first read, kept. It sends nothing.
  "limitsAtConnect",
  // The names /limits reported, for the facts step. It sends nothing.
  "limitNames",
  // The org's own domain, for the action lists' links. It sends nothing.
  "instanceUrl",
  "query",
  "queryAll",
  "queryBatch",
  "describe",
  "describeGlobal",
  "recordCounts",
  "describeReport",
  "retrieveMetadata",
] as const;

export type ClientOptions = {
  budget: ApiBudget;
  transport?: Transport;
  /** Pin a version. Otherwise the org's newest is used. */
  apiVersion?: string;
  sleep?: (ms: number) => Promise<void>;
  retrievePollMs?: number;
};

export type OrgLimit = { Max: number; Remaining: number };
/** The /limits entries kept for the integrations area and storage (roadmap 3.2). */
export const REPORTED_LIMITS = ["DailyApiRequests", "DailyAsyncApexExecutions", "DataStorageMB", "FileStorageMB"] as const;
const isLimit = (value: unknown): value is OrgLimit => typeof (value as OrgLimit)?.Max === "number" && typeof (value as OrgLimit)?.Remaining === "number";

export class SalesforceClient {
  readonly budget: ApiBudget;
  readonly connectionKind: ConnectionProvider["kind"];
  apiVersion: string;
  orgId: string;
  #connection: OrgConnection;
  readonly #provider: ConnectionProvider;
  readonly #transport: Transport;
  readonly #sleep: (ms: number) => Promise<void>;
  readonly #retrievePollMs: number;
  readonly #checkHost: boolean;
  #sinceLimitsRead = 0;
  #limitsAtConnect: Record<string, OrgLimit> | null = null;
  #limitNames: string[] = [];

  private constructor(provider: ConnectionProvider, connection: OrgConnection, options: ClientOptions) {
    this.budget = options.budget;
    this.connectionKind = provider.kind;
    this.apiVersion = options.apiVersion ?? "";
    this.orgId = connection.orgId ?? "";
    this.#provider = provider;
    this.#checkHost = !options.transport;
    this.#connection = { ...connection, instanceUrl: instanceOrigin(connection.instanceUrl, this.#checkHost) };
    this.#transport = options.transport ?? fetchTransport;
    this.#sleep = options.sleep ?? ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
    this.#retrievePollMs = options.retrievePollMs ?? SF_DEFAULTS.retrievePollMs;
  }

  /** Connects, settles the API version and reads the limits, so the budget is live before any spend. */
  static async connect(provider: ConnectionProvider, options: ClientOptions): Promise<SalesforceClient> {
    const connection = await provider.connect();
    const client = new SalesforceClient(provider, connection, options);
    if (!client.apiVersion) client.apiVersion = await client.#newestApiVersion();
    await client.refreshLimits();
    if (!client.orgId) {
      const org = await client.query<{ Id: string }>("SELECT Id FROM Organization");
      client.orgId = org.records[0]?.Id ?? "";
    }
    return client;
  }

  async #newestApiVersion(): Promise<string> {
    const versions = JSON.parse((await this.#send({ method: "GET", path: "/services/data/" }, { free: true })).text) as Array<{ version: string }>;
    const newest = versions.map((v) => v.version).sort((a, b) => Number(b) - Number(a))[0];
    if (!newest || Number(newest) < SF_DEFAULTS.minApiVersion) throw new Error(`The org's API version ${newest} is below ${SF_DEFAULTS.minApiVersion}.0`);
    return newest;
  }

  #data(path: string): string {
    return `/services/data/v${this.apiVersion}${path}`;
  }

  /** The org's own domain, as the connection gave it: where a link to a record goes. */
  get instanceUrl(): string {
    return this.#connection.instanceUrl;
  }

  /**
   * The org's limits as first read on connecting, before this connection spent
   * anything, so they show the org's own use. Only the ones a rule reads.
   */
  get limitsAtConnect(): Record<string, OrgLimit> {
    return { ...this.#limitsAtConnect };
  }

  /** Every limit's name in /limits as first read: names only, for the facts step. */
  get limitNames(): string[] {
    return [...this.#limitNames];
  }

  /** Doesn't count against the daily limit. Also how the budget learns the limit. */
  async refreshLimits(): Promise<void> {
    const res = await this.#send({ method: "GET", path: this.#data("/limits") }, { free: true });
    const limits = JSON.parse(res.text) as Record<string, OrgLimit | undefined>;
    if (this.#limitNames.length === 0) this.#limitNames = Object.keys(limits).sort();
    this.#limitsAtConnect ??= Object.fromEntries(REPORTED_LIMITS.flatMap((name) => (isLimit(limits[name]) ? [[name, { Max: limits[name].Max, Remaining: limits[name].Remaining }]] : [])));
    const daily = limits.DailyApiRequests;
    if (!daily) throw new Error("The org didn't report a daily API limit");
    this.budget.observe(daily.Max - daily.Remaining, daily.Max);
    this.#sinceLimitsRead = 0;
  }

  async #send(request: SfRequest, options: { free?: boolean } & SendOptions = {}): Promise<TransportResponse> {
    // The boundary. If this throws, nothing has left the process.
    authorise(request);
    if (!options.free) {
      if (this.#sinceLimitsRead >= BUDGET_DEFAULTS.refreshEveryCalls) await this.refreshLimits();
      this.budget.assertCanSpend();
    }

    let refreshed = false;
    for (let attempt = 0; ; attempt++) {
      if (!options.free) {
        this.budget.recordCall();
        this.#sinceLimitsRead++;
      }
      const res = await this.#transport.send({
        method: request.method,
        url: `${this.#connection.instanceUrl}${request.path}`,
        headers: {
          ...(options.soapOperation ? {} : { Authorization: `Bearer ${this.#connection.accessToken}` }),
          Accept: options.soapOperation ? "text/xml" : "application/json",
          ...request.headers,
        },
        body: request.body,
      });
      this.budget.observeHeader(res.header("sforce-limit-info"));

      if (res.status === 401 && !refreshed && !options.soapOperation) {
        refreshed = true;
        await this.#refreshConnection();
        continue;
      }
      if ((res.status === 429 || res.status >= 500) && !options.soapOperation && attempt < SF_DEFAULTS.maxRetries) {
        await this.#sleep(1_000 * 2 ** attempt);
        continue;
      }
      if (res.status >= 400) throw this.#toError(res);
      return res;
    }
  }

  #toError(res: TransportResponse): Error {
    let code = `HTTP_${res.status}`;
    let message = "Salesforce returned an error";
    try {
      const body = JSON.parse(res.text) as unknown;
      const first = (Array.isArray(body) ? body[0] : body) as { errorCode?: string; message?: string };
      code = first.errorCode ?? code;
      message = first.message ?? message;
    } catch {
      const fault = (xml.parse(res.text) as { Envelope?: { Body?: { Fault?: { faultcode?: string; faultstring?: string } } } }).Envelope?.Body?.Fault;
      if (fault) {
        code = String(fault.faultcode ?? code).replace(/^.*:/, "");
        message = String(fault.faultstring ?? message);
      }
    }
    if (code === "REQUEST_LIMIT_EXCEEDED") return new BudgetStopError("Salesforce reports the daily API limit is exhausted");
    return new SalesforceError(res.status, code, message);
  }

  #queryPath(soql: string, api: QueryApi): string {
    return this.#data(`${api === "tooling" ? "/tooling" : ""}/query?q=${encodeURIComponent(soql)}`);
  }

  /** One page at a time up to `maxRecords`. Aggregate queries return everything in one page. */
  async query<T>(soql: string, options: QueryOptions = {}): Promise<QueryResult<T>> {
    const api = options.api ?? "rest";
    const records: T[] = [];
    let path: string | undefined = this.#queryPath(soql, api);
    let totalSize = 0;
    while (path) {
      const res: TransportResponse = await this.#send({ method: "GET", path });
      const page = JSON.parse(res.text) as { records: T[]; totalSize: number; nextRecordsUrl?: string };
      totalSize = page.totalSize;
      records.push(...page.records);
      if (options.maxRecords !== undefined && records.length >= options.maxRecords) {
        return { records: records.slice(0, options.maxRecords), totalSize, truncated: Boolean(page.nextRecordsUrl) || records.length > options.maxRecords };
      }
      // Salesforce supplies this path, so it can only ever be a next page.
      if (page.nextRecordsUrl && !QUERY_MORE.test(page.nextRecordsUrl)) throw new ForbiddenOperationError("nextRecordsUrl isn't a query page");
      path = page.nextRecordsUrl;
    }
    return { records, totalSize, truncated: false };
  }

  async queryAll<T>(soql: string, options: QueryOptions = {}): Promise<T[]> {
    return (await this.query<T>(soql, options)).records;
  }

  /** Up to 25 REST queries per call. Tooling queries must not come through here. */
  async queryBatch<T>(soqls: string[]): Promise<Array<{ ok: true; result: QueryResult<T> } | { ok: false; error: SalesforceError }>> {
    const out: Array<{ ok: true; result: QueryResult<T> } | { ok: false; error: SalesforceError }> = [];
    for (let i = 0; i < soqls.length; i += SF_DEFAULTS.compositeBatchSize) {
      const chunk = soqls.slice(i, i + SF_DEFAULTS.compositeBatchSize);
      const res = await this.#send({
        method: "POST",
        path: this.#data("/composite/batch"),
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          haltOnError: false,
          batchRequests: chunk.map((soql) => ({ method: "GET", url: `v${this.apiVersion}/query?q=${encodeURIComponent(soql)}` })),
        }),
      });
      const { results } = JSON.parse(res.text) as { results: Array<{ statusCode: number; result: unknown }> };
      for (const item of results) {
        if (item.statusCode === 200) {
          const page = item.result as { records: T[]; totalSize: number; nextRecordsUrl?: string };
          out.push({ ok: true, result: { records: page.records, totalSize: page.totalSize, truncated: Boolean(page.nextRecordsUrl) } });
        } else {
          const first = (Array.isArray(item.result) ? item.result[0] : item.result) as { errorCode?: string; message?: string };
          out.push({ ok: false, error: new SalesforceError(item.statusCode, first?.errorCode ?? "UNKNOWN", first?.message ?? "Subrequest failed") });
        }
      }
    }
    return out;
  }

  async describe<T>(object: string): Promise<T> {
    return JSON.parse((await this.#send({ method: "GET", path: this.#data(`/sobjects/${object}/describe`) })).text) as T;
  }

  async describeGlobal<T>(): Promise<T> {
    return JSON.parse((await this.#send({ method: "GET", path: this.#data("/sobjects") })).text) as T;
  }

  async recordCounts(): Promise<Array<{ name: string; count: number }>> {
    const res = await this.#send({ method: "GET", path: this.#data("/limits/recordCount") });
    return (JSON.parse(res.text) as { sObjects: Array<{ name: string; count: number }> }).sObjects;
  }

  /** A report's definition through the Analytics API, without running it. */
  async describeReport<T>(reportId: string): Promise<T> {
    if (!/^00O[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/.test(reportId)) throw new ForbiddenOperationError("not a report id");
    return JSON.parse((await this.#send({ method: "GET", path: this.#data(`/analytics/reports/${reportId}/describe`) })).text) as T;
  }

  async #refreshConnection(): Promise<void> {
    const fresh = await this.#provider.refresh();
    this.#connection = { ...fresh, instanceUrl: instanceOrigin(fresh.instanceUrl, this.#checkHost) };
  }

  async #soap(operation: string, body: string): Promise<Record<string, unknown>> {
    // The token is in the body, so a retry has to rebuild the envelope.
    const send = (): Promise<TransportResponse> =>
      this.#send(
        {
          method: "POST",
          path: `/services/Soap/m/${this.apiVersion}`,
          headers: { "Content-Type": "text/xml; charset=UTF-8", SOAPAction: `"${operation}"` },
          body: soapEnvelope(this.#connection.accessToken, body),
        },
        { soapOperation: operation },
      );
    let res: TransportResponse;
    try {
      res = await send();
    } catch (err) {
      // A session can expire during a long retrieve, and SOAP reports that as a fault, not a 401.
      if (!(err instanceof SalesforceError) || err.code !== "INVALID_SESSION_ID") throw err;
      await this.#refreshConnection();
      res = await send();
    }
    const parsed = xml.parse(res.text) as { Envelope?: { Body?: Record<string, { result?: Record<string, unknown> }> } };
    const result = parsed.Envelope?.Body?.[`${operation}Response`]?.result;
    if (!result) throw new SalesforceError(res.status, "BAD_SOAP_RESPONSE", `No result in the ${operation} response`);
    return result;
  }

  /** Starts a retrieve, polls it and returns the unzipped text files by path. */
  async retrieveMetadata(types: MetadataMember[]): Promise<Record<string, string>> {
    const typesXml = types
      .map((t) => `<met:types>${t.members.map((m) => `<met:members>${escapeXml(m)}</met:members>`).join("")}<met:name>${escapeXml(t.name)}</met:name></met:types>`)
      .join("");
    const started = await this.#soap(
      "retrieve",
      `<met:retrieve><met:retrieveRequest><met:apiVersion>${this.apiVersion}</met:apiVersion><met:singlePackage>true</met:singlePackage>` +
        `<met:unpackaged>${typesXml}<met:version>${this.apiVersion}</met:version></met:unpackaged></met:retrieveRequest></met:retrieve>`,
    );
    const id = String(started.id ?? "");
    if (!/^[A-Za-z0-9]{15,18}$/.test(id)) throw new SalesforceError(200, "BAD_SOAP_RESPONSE", "The retrieve returned no job id");

    for (let poll = 0; poll < SF_DEFAULTS.retrieveMaxPolls; poll++) {
      const status = await this.#soap(
        "checkRetrieveStatus",
        `<met:checkRetrieveStatus><met:asyncProcessId>${id}</met:asyncProcessId><met:includeZip>true</met:includeZip></met:checkRetrieveStatus>`,
      );
      if (String(status.done) === "true") {
        if (String(status.success) !== "true" || !status.zipFile) {
          throw new SalesforceError(200, "RETRIEVE_FAILED", String(status.errorMessage ?? status.status ?? "The retrieve failed"));
        }
        const files = unzipSync(Buffer.from(String(status.zipFile), "base64"));
        return Object.fromEntries(Object.entries(files).map(([path, bytes]) => [path, strFromU8(bytes)]));
      }
      await this.#sleep(this.#retrievePollMs);
    }
    throw new SalesforceError(200, "RETRIEVE_TIMEOUT", "The retrieve didn't finish in time");
  }
}
