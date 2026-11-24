import { AsyncLocalStorage } from "node:async_hooks";
import { XMLParser } from "fast-xml-parser";
import { unzipSync, strFromU8 } from "fflate";
import { BUDGET_DEFAULTS, SF_DEFAULTS } from "../config.ts";
import { authorise, ForbiddenOperationError, type SfRequest } from "./allowlist.ts";
import { ApiBudget, BudgetStopError } from "./budget.ts";
import { LIMITS_NO_PERMISSION, LIMITS_REFUSED, NO_API_FOR_ORG, NO_API_FOR_USER } from "./refusal-words.ts";
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
  /** A Metadata API SOAP fault: Salesforce's answer to that one call, sent with HTTP 500 whatever it says. */
  readonly fault: boolean;
  /** The SOAP operation the error came from, or undefined for a REST call. */
  readonly operation: string | undefined;

  constructor(status: number, code: string, message: string, options: { fault?: boolean; operation?: string } = {}) {
    super(`${code}: ${message}`);
    this.name = "SalesforceError";
    this.status = status;
    this.code = code;
    this.fault = options.fault ?? false;
    this.operation = options.operation;
  }
}

/**
 * A connection Salesforce won't give the API to at all (DECISIONS.md 702): a
 * final refusal, said plainly, never retried. No read can work, so it is the
 * connect's answer and the run's, never one check's.
 */
export class NoApiAccessError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NoApiAccessError";
  }
}

export { LIMITS_NO_PERMISSION, LIMITS_REFUSED, NO_API_FOR_ORG, NO_API_FOR_USER } from "./refusal-words.ts";

/**
 * Which errors cost one read and which stop the run (DECISIONS.md 702). One
 * place draws the line, and every read goes by it.
 *
 * Costs one read, so its checks are not measurable and the run goes on:
 * - `not_available`: the feature isn't in this edition, or isn't switched on.
 * - `no_permission`: the connected user can't see it.
 * - `refused`: Salesforce said no to this read for another reason, or a code
 *   we haven't seen. Any 4xx with Salesforce's own error in it, and any SOAP
 *   fault but a server's, is Salesforce answering that one call, whatever its code.
 *
 * Stops the run, as before 702:
 * - `auth_lost`: the session is gone after the client's refresh. Only a person
 *   signing in again helps.
 * - `api_limit`: the org's daily API limit (a BudgetStopError, which the run
 *   saves and resumes from).
 * - `instance_unavailable`: a 5xx or 429 after the client's retries, a SOAP
 *   `Server` fault, a retrieve that timed out or came back malformed, an
 *   expired query cursor, or a response with no Salesforce error in it. It may
 *   pass, so the run stops and the worker reads it again: never saved as unread.
 * - `not_salesforce`: anything else, such as a network failure or our own bug.
 */
export type ErrorClass = "not_available" | "no_permission" | "refused" | "auth_lost" | "api_limit" | "instance_unavailable" | "not_salesforce";

/** The codes we know, by class. A code not here is still classed, by its status and where it came from. */
export const KNOWN_CODES: Readonly<Record<string, ErrorClass>> = {
  INVALID_TYPE: "not_available",
  INVALID_FIELD: "not_available",
  NOT_FOUND: "not_available",
  API_DISABLED_FOR_ORG: "not_available",
  FUNCTIONALITY_NOT_ENABLED: "not_available",
  INSUFFICIENT_ACCESS: "no_permission",
  INSUFFICIENT_ACCESS_OR_READONLY: "no_permission",
  INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY: "no_permission",
  API_CURRENTLY_DISABLED: "no_permission",
  MALFORMED_QUERY: "refused",
  INVALID_QUERY_FILTER_OPERATOR: "refused",
  INVALID_QUERY_SCOPE: "refused",
  // A query cursor expired between pages: read again, it works (DECISIONS.md 702).
  INVALID_QUERY_LOCATOR: "instance_unavailable",
  INVALID_OPERATION: "refused",
  QUERY_TOO_COMPLICATED: "refused",
  EXCEEDED_ID_LIMIT: "refused",
  OPERATION_TOO_LARGE: "refused",
  QUERY_TIMEOUT: "refused",
  UNKNOWN_EXCEPTION: "refused",
  // The client's own, for a retrieve that didn't give back its files. One that
  // failed is Salesforce's answer; one that ran out of time, or came back
  // malformed, may work on the next delivery.
  RETRIEVE_FAILED: "refused",
  RETRIEVE_TIMEOUT: "instance_unavailable",
  BAD_SOAP_RESPONSE: "instance_unavailable",
  INVALID_SESSION_ID: "auth_lost",
  INVALID_AUTH_HEADER: "auth_lost",
  REQUEST_LIMIT_EXCEEDED: "api_limit",
  SERVER_UNAVAILABLE: "instance_unavailable",
  // SOAP's own generic fault codes, with their namespace taken off: a server's
  // is an outage, never an answer about the read.
  Server: "instance_unavailable",
};

const PER_READ: ReadonlySet<ErrorClass> = new Set(["not_available", "no_permission", "refused"]);

export function classifyError(err: unknown): ErrorClass {
  if (err instanceof BudgetStopError) return "api_limit";
  if (!(err instanceof SalesforceError)) return "not_salesforce";
  const known = KNOWN_CODES[err.code];
  // A session refused after the refresh is lost, whatever code came with it.
  if (err.status === 401 || known === "auth_lost") return "auth_lost";
  if (known) return known;
  // No Salesforce error in the body: the client named it from the status alone.
  if (/^HTTP_\d+$/.test(err.code)) return "instance_unavailable";
  if (err.fault) return "refused";
  if (err.status === 429 || err.status >= 500) return "instance_unavailable";
  return err.status >= 400 ? "refused" : "instance_unavailable";
}

/**
 * The SOAP operations a server fault or a malformed answer may cost one read
 * on: a retrieve and its status checks, which are one read of the org's
 * set-up and nothing else (DECISIONS.md 702). The client sends no other SOAP
 * call today; one added later stops the run on either, as any outage does,
 * unless it is listed here with a reason.
 */
export const RETRIEVE_OPERATIONS: ReadonlySet<string> = new Set(["retrieve", "checkRetrieveStatus"]);

/**
 * The transient faults that belong to one read, not the whole instance: an
 * expired query cursor, a retrieve that timed out, and a server fault or a
 * malformed answer on a retrieve or its status check. Retried while the
 * worker can deliver again; on its last delivery, each costs only its read
 * (DECISIONS.md 702), so a fault that comes back every time can't fail every
 * run for that org. Such a read is lost, not refused: its sentence says
 * Salesforce didn't answer, and an automatic run that lost one isn't released.
 */
export function oneReadTransient(err: unknown): err is SalesforceError {
  if (!(err instanceof SalesforceError)) return false;
  if (err.code === "INVALID_QUERY_LOCATOR" || err.code === "RETRIEVE_TIMEOUT") return true;
  const onRetrieve = err.operation !== undefined && RETRIEVE_OPERATIONS.has(err.operation);
  return onRetrieve && (err.code === "BAD_SOAP_RESPONSE" || (err.fault && err.code === "Server"));
}

/** Set by the run around each step on the worker's last delivery (DECISIONS.md 702). */
export const lastDelivery = new AsyncLocalStorage<true>();

/** True when the error costs only the read it came from (DECISIONS.md 702). */
export function isUnavailable(err: unknown): err is SalesforceError {
  if (!(err instanceof SalesforceError)) return false;
  return PER_READ.has(classifyError(err)) || (lastDelivery.getStore() === true && oneReadTransient(err));
}

/**
 * Why a run stopped, in plain words a person can read in the console: never
 * Salesforce's code or message, which go to the log (DECISIONS.md 702).
 */
export function runStoppedWords(err: unknown): string | null {
  if (!(err instanceof SalesforceError)) return null;
  const kind = classifyError(err);
  if (kind === "instance_unavailable") return "Salesforce didn't answer as it should, after several tries, so the org may be busy or unavailable.";
  if (kind === "auth_lost") return "Salesforce no longer accepts the connection.";
  return "Salesforce refused a read the run can't go on without.";
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

/** Refreshes per request at most: one, and one more after tokens found in the store (DECISIONS.md 631). */
export const MAX_REFRESHES = 2;

/**
 * The connection the client keeps: the named fields only, so a flag such as
 * `fromStore`, which describes one refresh, never travels with the token.
 */
export function keptConnection(connection: OrgConnection, checkHost: boolean): OrgConnection {
  const kept: OrgConnection = { instanceUrl: instanceOrigin(connection.instanceUrl, checkHost), accessToken: connection.accessToken };
  if (connection.orgId !== undefined) kept.orgId = connection.orgId;
  if (connection.username !== undefined) kept.username = connection.username;
  return kept;
}

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
  // Every limit /limits reported as first read, for the inventory (DECISIONS.md 603). It sends nothing.
  "allLimitsAtConnect",
  // Why /limits couldn't be read at connect, or null (DECISIONS.md 702). It sends nothing.
  "limitsUnread",
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
  #allLimitsAtConnect: Record<string, OrgLimit> | null = null;
  #limitsUnread: string | null = null;

  private constructor(provider: ConnectionProvider, connection: OrgConnection, options: ClientOptions) {
    this.budget = options.budget;
    this.connectionKind = provider.kind;
    this.apiVersion = options.apiVersion ?? "";
    this.orgId = connection.orgId ?? "";
    this.#provider = provider;
    this.#checkHost = !options.transport;
    this.#connection = keptConnection(connection, this.#checkHost);
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

  /**
   * Every limit as first read on connecting, by its name: each top-level entry
   * with a maximum and what remains. The per-app entries some limits nest are left out.
   */
  get allLimitsAtConnect(): Record<string, OrgLimit> {
    return { ...this.#allLimitsAtConnect };
  }

  /**
   * Why the org's limits couldn't be read on connecting, or null when they
   * were (DECISIONS.md 702): the connected user can't view Setup, or Salesforce
   * refused the read. Our own words, never Salesforce's.
   */
  get limitsUnread(): string | null {
    return this.#limitsUnread;
  }

  /** Doesn't count against the daily limit. Also how the budget learns the limit. */
  async refreshLimits(): Promise<void> {
    // Refused at connect, it would be refused again: the budget goes by the
    // usage header on each response, within its allowance until one comes.
    if (this.#limitsUnread !== null) {
      this.#sinceLimitsRead = 0;
      return;
    }
    let res: TransportResponse;
    try {
      res = await this.#send({ method: "GET", path: this.#data("/limits") }, { free: true });
    } catch (err) {
      // Only the read at connect is decided here. A later one fails as before.
      if (this.#allLimitsAtConnect !== null) throw err;
      this.#limitsUnreadAtConnect(err);
      return;
    }
    const limits = JSON.parse(res.text) as Record<string, OrgLimit | undefined>;
    if (this.#limitNames.length === 0) this.#limitNames = Object.keys(limits).sort();
    this.#allLimitsAtConnect ??= Object.fromEntries(Object.entries(limits).flatMap(([name, limit]) => (isLimit(limit) ? [[name, { Max: limit.Max, Remaining: limit.Remaining }]] : [])));
    this.#limitsAtConnect ??= Object.fromEntries(REPORTED_LIMITS.flatMap((name) => (isLimit(limits[name]) ? [[name, { Max: limits[name].Max, Remaining: limits[name].Remaining }]] : [])));
    const daily = limits.DailyApiRequests;
    if (isLimit(daily)) this.budget.observe(daily.Max - daily.Remaining, daily.Max);
    // No daily limit reported: the budget keeps a small allowance until a response's header says (DECISIONS.md 702).
    else this.budget.limitUnread();
    this.#sinceLimitsRead = 0;
  }

  /**
   * The first /limits read refused (DECISIONS.md 702). No API at all is the
   * connect's final answer, said plainly. Any other refusal of this one read,
   * such as a user without View Setup and Configuration, lets the connect go
   * on with the limits not measured and a small, safe budget: never an
   * unlimited one. A lost session, the daily limit or an outage stops it as before.
   */
  #limitsUnreadAtConnect(err: unknown): void {
    if (err instanceof SalesforceError && err.code === "API_DISABLED_FOR_ORG") throw new NoApiAccessError(NO_API_FOR_ORG);
    if (err instanceof SalesforceError && err.code === "API_CURRENTLY_DISABLED") throw new NoApiAccessError(NO_API_FOR_USER);
    if (!isUnavailable(err)) throw err;
    this.#limitsUnread = classifyError(err) === "no_permission" ? LIMITS_NO_PERMISSION : LIMITS_REFUSED;
    this.#limitNames = [];
    this.#allLimitsAtConnect = {};
    this.#limitsAtConnect = {};
    this.budget.limitUnread();
    this.#sinceLimitsRead = 0;
  }

  async #send(request: SfRequest, options: { free?: boolean } & SendOptions = {}): Promise<TransportResponse> {
    // The boundary. If this throws, nothing has left the process.
    authorise(request);
    if (!options.free) {
      if (this.#sinceLimitsRead >= BUDGET_DEFAULTS.refreshEveryCalls) await this.refreshLimits();
      this.budget.assertCanSpend();
    }

    // One refresh per request, and a second only if the first found another
    // process's tokens rather than refreshing. Never more than two, whatever the provider says (DECISIONS.md 631).
    let refreshes = 0;
    let allowed = 1;
    let adopted = false;
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

      if (res.status === 401 && refreshes < allowed && !options.soapOperation) {
        refreshes++;
        adopted = await this.#refreshConnection();
        if (adopted && refreshes === 1) allowed = MAX_REFRESHES;
        continue;
      }
      if ((res.status === 429 || res.status >= 500) && !options.soapOperation && attempt < SF_DEFAULTS.maxRetries) {
        await this.#sleep(1_000 * 2 ** attempt);
        continue;
      }
      if (res.status >= 400) throw this.#toError(res, options.soapOperation);
      if (adopted) this.#provider.accepted?.();
      return res;
    }
  }

  #toError(res: TransportResponse, operation?: string): Error {
    let code = `HTTP_${res.status}`;
    let message = "Salesforce returned an error";
    let isFault = false;
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
        isFault = true;
      }
    }
    if (code === "REQUEST_LIMIT_EXCEEDED") return new BudgetStopError("Salesforce reports the daily API limit is exhausted");
    return new SalesforceError(res.status, code, message, { fault: isFault, ...(operation ? { operation } : {}) });
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
          const error = new SalesforceError(item.statusCode, first?.errorCode ?? "UNKNOWN", first?.message ?? "Subrequest failed");
          // A lost session or the daily limit inside one answer is the run's, not one count's (DECISIONS.md 702).
          const kind = classifyError(error);
          if (kind === "api_limit") throw new BudgetStopError("Salesforce reports the daily API limit is exhausted");
          if (kind === "auth_lost") throw error;
          out.push({ ok: false, error });
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

  /** True if the provider found another process's tokens rather than refreshing (DECISIONS.md 631). */
  async #refreshConnection(): Promise<boolean> {
    const fresh = await this.#provider.refresh();
    this.#connection = keptConnection(fresh, this.#checkHost);
    return fresh.fromStore === true;
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
      if (!(await this.#refreshConnection())) res = await send();
      else {
        // Another process's tokens: their session may have ended too, so one
        // more refresh, and no more, whatever the provider says (DECISIONS.md 631).
        try {
          res = await send();
          this.#provider.accepted?.();
        } catch (again) {
          if (!(again instanceof SalesforceError) || again.code !== "INVALID_SESSION_ID") throw again;
          await this.#refreshConnection();
          res = await send();
        }
      }
    }
    const parsed = xml.parse(res.text) as { Envelope?: { Body?: Record<string, { result?: Record<string, unknown> }> } };
    const result = parsed.Envelope?.Body?.[`${operation}Response`]?.result;
    if (!result) throw new SalesforceError(res.status, "BAD_SOAP_RESPONSE", `No result in the ${operation} response`, { operation });
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
    if (!/^[A-Za-z0-9]{15,18}$/.test(id)) throw new SalesforceError(200, "BAD_SOAP_RESPONSE", "The retrieve returned no job id", { operation: "retrieve" });

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
