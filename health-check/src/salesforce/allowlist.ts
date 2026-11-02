import { XMLParser } from "fast-xml-parser";
import { SF_DEFAULTS } from "../config.ts";
import { assertSafeSoql, UnfilterableFieldError } from "./soql.ts";

// The read-only boundary (rule 6). Every request is checked here immediately
// before it's sent, against the exact bytes that would go out.
//
// It's an allowlist of paths, not of HTTP methods, because the method tells you
// little in Salesforce: GET tooling/executeAnonymous runs Apex, a GET on an Apex
// REST class can do anything, and POST ?_HttpMethod=PATCH is an update. Metadata
// retrieve and composite batch are POSTs that only read, so each is allowed by
// exact endpoint and has its body inspected.

export type SfRequest = {
  method: string;
  /** Path and query only. The client adds the instance URL. */
  path: string;
  headers?: Record<string, string>;
  body?: string;
};

export class ForbiddenOperationError extends Error {
  constructor(reason: string) {
    super(`Blocked by the read-only allowlist: ${reason}`);
    this.name = "ForbiddenOperationError";
  }
}

const VERSION = String.raw`v\d{2,3}\.\d`;
const LOCATOR = String.raw`[A-Za-z0-9]{15,18}-\d+`;

const GET_ROUTES: ReadonlyArray<{ id: string; pattern: RegExp; requiresSoql?: boolean }> = [
  { id: "rest.versions", pattern: /^\/services\/data\/?$/ },
  { id: "rest.limits", pattern: new RegExp(`^/services/data/${VERSION}/limits/?$`) },
  { id: "rest.recordCount", pattern: new RegExp(`^/services/data/${VERSION}/limits/recordCount/?$`) },
  { id: "rest.query", pattern: new RegExp(`^/services/data/${VERSION}/query/?$`), requiresSoql: true },
  { id: "rest.queryMore", pattern: new RegExp(`^/services/data/${VERSION}/query/${LOCATOR}$`) },
  { id: "rest.describeGlobal", pattern: new RegExp(`^/services/data/${VERSION}/sobjects/?$`) },
  { id: "rest.describe", pattern: new RegExp(`^/services/data/${VERSION}/sobjects/[A-Za-z0-9_]+/describe/?$`) },
  { id: "tooling.query", pattern: new RegExp(`^/services/data/${VERSION}/tooling/query/?$`), requiresSoql: true },
  { id: "tooling.queryMore", pattern: new RegExp(`^/services/data/${VERSION}/tooling/query/${LOCATOR}$`) },
];

const COMPOSITE_BATCH = new RegExp(`^/services/data/${VERSION}/composite/batch/?$`);
const COMPOSITE_SUBREQUEST = new RegExp(`^(?:/services/data/)?${VERSION}/query/?\\?`);
const METADATA_SOAP = /^\/services\/Soap\/m\/\d{2,3}\.\d$/;

export const ALLOWED_SOAP_OPERATIONS: ReadonlySet<string> = new Set(["retrieve", "checkRetrieveStatus"]);

const soapParser = new XMLParser({ removeNSPrefix: true, ignoreAttributes: true, parseTagValue: false, processEntities: false });

function assertCleanPath(path: string): void {
  if (!path.startsWith("/")) throw new ForbiddenOperationError("path must be relative to the instance");
  if (path.startsWith("//") || path.includes("\\")) throw new ForbiddenOperationError("malformed path");
  // URL parsers drop tabs and newlines, so what we checked wouldn't be what's sent.
  // The client always percent-encodes, so raw whitespace is never legitimate.
  if (/[\s\u0000-\u001f\u007f]/.test(path)) throw new ForbiddenOperationError("raw whitespace or control characters in the path");
  const pathOnly = path.split("?")[0]!;
  // Encoded slashes and dots could reach a different route than the one matched.
  if (/%2e|%2f|%5c/i.test(pathOnly) || pathOnly.split("/").some((part) => part === ".." || part === ".")) {
    throw new ForbiddenOperationError("path traversal");
  }
}

function assertNoOverrideParam(params: URLSearchParams): void {
  for (const key of params.keys()) {
    if (key.toLowerCase() === "_httpmethod") throw new ForbiddenOperationError("method override parameter");
  }
}

/** Exactly one q and nothing else. A second q could be the one Salesforce reads. */
function soqlFrom(params: URLSearchParams): string {
  const keys = [...params.keys()];
  if (keys.length !== 1 || keys[0] !== "q") throw new ForbiddenOperationError("a query takes exactly one q parameter");
  return params.get("q") ?? "";
}

function assertNoMethodOverride(request: SfRequest, params: URLSearchParams): void {
  assertNoOverrideParam(params);
  for (const header of Object.keys(request.headers ?? {})) {
    if (header.toLowerCase() === "x-http-method-override") throw new ForbiddenOperationError("method override header");
  }
}

function assertSoql(soql: string): void {
  try {
    assertSafeSoql(soql);
  } catch (err) {
    // A bad filter is a code bug with its own message, so let it through as itself.
    if (err instanceof UnfilterableFieldError) throw err;
    throw new ForbiddenOperationError(err instanceof Error ? err.message : "unsafe query");
  }
}

function authoriseComposite(body: string | undefined): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body ?? "");
  } catch {
    throw new ForbiddenOperationError("composite body is not JSON");
  }
  const batch = parsed as { batchRequests?: unknown };
  const keys = Object.keys(batch ?? {});
  if (!Array.isArray(batch?.batchRequests) || keys.some((k) => k !== "batchRequests" && k !== "haltOnError")) {
    throw new ForbiddenOperationError("composite body has an unexpected shape");
  }
  if (batch.batchRequests.length === 0 || batch.batchRequests.length > SF_DEFAULTS.compositeBatchSize) {
    throw new ForbiddenOperationError("composite batch size out of range");
  }
  for (const sub of batch.batchRequests as Array<Record<string, unknown>>) {
    if (Object.keys(sub).some((k) => k !== "method" && k !== "url")) {
      throw new ForbiddenOperationError("composite subrequest has extra fields");
    }
    if (sub.method !== "GET" || typeof sub.url !== "string") throw new ForbiddenOperationError("composite subrequests must be GET");
    // Tooling is refused here as well as being read-only by route: some Tooling
    // queries come back empty through composite batch, which looks like "no data".
    if (!COMPOSITE_SUBREQUEST.test(sub.url)) throw new ForbiddenOperationError(`composite subrequest not allowed: ${sub.url.split("?")[0]}`);
    if (/[\s\u0000-\u001f\u007f]/.test(sub.url)) throw new ForbiddenOperationError("raw whitespace or control characters in a subrequest");
    const params = new URLSearchParams(sub.url.split("?").slice(1).join("?"));
    assertNoOverrideParam(params);
    assertSoql(soqlFrom(params));
  }
  return "rest.compositeBatch";
}

function authoriseSoap(request: SfRequest): string {
  const headers = Object.fromEntries(Object.entries(request.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]));
  let body: unknown;
  try {
    body = (soapParser.parse(request.body ?? "") as { Envelope?: { Body?: unknown } }).Envelope?.Body;
  } catch {
    throw new ForbiddenOperationError("SOAP body is not valid XML");
  }
  const operations = body && typeof body === "object" ? Object.keys(body) : [];
  if (operations.length !== 1) throw new ForbiddenOperationError("SOAP body must hold exactly one operation");
  const operation = operations[0]!;
  if (!ALLOWED_SOAP_OPERATIONS.has(operation)) throw new ForbiddenOperationError(`Metadata API operation ${operation}`);
  const action = headers["soapaction"]?.replace(/"/g, "");
  if (action !== operation) throw new ForbiddenOperationError("SOAPAction doesn't match the body");
  return `metadata.${operation}`;
}

/** Returns the operation id, or throws. Nothing is sent unless this returns. */
export function authorise(request: SfRequest): string {
  assertCleanPath(request.path);
  const [pathOnly, ...rest] = request.path.split("?");
  const params = new URLSearchParams(rest.join("?"));
  assertNoMethodOverride(request, params);

  if (request.method === "GET") {
    if (request.body) throw new ForbiddenOperationError("GET with a body");
    const route = GET_ROUTES.find((r) => r.pattern.test(pathOnly!));
    if (!route) throw new ForbiddenOperationError(`GET ${pathOnly}`);
    if (route.requiresSoql) assertSoql(soqlFrom(params));
    else if (params.size > 0) throw new ForbiddenOperationError(`${route.id} takes no parameters`);
    return route.id;
  }

  if (request.method === "POST") {
    if (params.size > 0) throw new ForbiddenOperationError("POST with a query string");
    if (COMPOSITE_BATCH.test(pathOnly!)) return authoriseComposite(request.body);
    if (METADATA_SOAP.test(pathOnly!)) return authoriseSoap(request);
    throw new ForbiddenOperationError(`POST ${pathOnly}`);
  }

  throw new ForbiddenOperationError(`${request.method} ${pathOnly}`);
}
