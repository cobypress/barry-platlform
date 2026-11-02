import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { authorise, ForbiddenOperationError, type SfRequest } from "../allowlist.ts";
import { UnfilterableFieldError } from "../soql.ts";

const V = "/services/data/v66.0";
const q = (soql: string): string => `?q=${encodeURIComponent(soql)}`;

const soapBody = (operation: string, inner = ""): string =>
  `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:met="http://soap.sforce.com/2006/04/metadata"><soapenv:Header><met:SessionHeader><met:sessionId>x</met:sessionId></met:SessionHeader></soapenv:Header><soapenv:Body><met:${operation}>${inner}</met:${operation}></soapenv:Body></soapenv:Envelope>`;

const soap = (operation: string, action = operation): SfRequest => ({
  method: "POST",
  path: "/services/Soap/m/66.0",
  headers: { "Content-Type": "text/xml", SOAPAction: `"${action}"` },
  body: soapBody(operation),
});

const composite = (requests: unknown[], extra: Record<string, unknown> = {}): SfRequest => ({
  method: "POST",
  path: `${V}/composite/batch`,
  body: JSON.stringify({ haltOnError: false, batchRequests: requests, ...extra }),
});

const blocked = (request: SfRequest): void => assert.throws(() => authorise(request), ForbiddenOperationError, `${request.method} ${request.path}`);

const ALLOWED: Array<[string, SfRequest]> = [
  ["rest.versions", { method: "GET", path: "/services/data/" }],
  ["rest.limits", { method: "GET", path: `${V}/limits` }],
  ["rest.recordCount", { method: "GET", path: `${V}/limits/recordCount` }],
  ["rest.query", { method: "GET", path: `${V}/query${q("SELECT Id FROM Account")}` }],
  ["rest.queryMore", { method: "GET", path: `${V}/query/01g000000000001AAA-2000` }],
  ["rest.describeGlobal", { method: "GET", path: `${V}/sobjects` }],
  ["rest.describe", { method: "GET", path: `${V}/sobjects/Account/describe` }],
  ["tooling.query", { method: "GET", path: `${V}/tooling/query${q("SELECT Id FROM ApexClass")}` }],
  ["tooling.queryMore", { method: "GET", path: `${V}/tooling/query/01g000000000001AAA-200` }],
  ["rest.compositeBatch", composite([{ method: "GET", url: `v66.0/query${q("SELECT Id FROM Account")}` }])],
  ["metadata.retrieve", soap("retrieve")],
  ["metadata.checkRetrieveStatus", soap("checkRetrieveStatus")],
];

describe("what the allowlist permits", () => {
  for (const [id, request] of ALLOWED) {
    test(id, () => assert.equal(authorise(request), id));
  }

  test("every allowed path refuses every other HTTP method", () => {
    for (const [, request] of ALLOWED) {
      for (const method of ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]) {
        if (method === request.method) continue;
        blocked({ ...request, method });
      }
    }
  });
});

describe("writes are refused", () => {
  const cases: Array<[string, SfRequest]> = [
    ["create a record", { method: "POST", path: `${V}/sobjects/Account`, body: "{}" }],
    ["update a record", { method: "PATCH", path: `${V}/sobjects/Account/001000000000001AAA`, body: "{}" }],
    ["delete a record", { method: "DELETE", path: `${V}/sobjects/Account/001000000000001AAA` }],
    ["upsert by external id", { method: "PATCH", path: `${V}/sobjects/Account/Ext__c/1`, body: "{}" }],
    ["sObject collections", { method: "POST", path: `${V}/composite/sobjects`, body: "{}" }],
    ["composite graph", { method: "POST", path: `${V}/composite`, body: "{}" }],
    ["composite tree", { method: "POST", path: `${V}/composite/tree/Account`, body: "{}" }],
    ["Tooling create", { method: "POST", path: `${V}/tooling/sobjects/ApexClass`, body: "{}" }],
    ["Tooling composite", { method: "POST", path: `${V}/tooling/composite/batch`, body: "{}" }],
    ["bulk ingest", { method: "POST", path: `${V}/jobs/ingest`, body: "{}" }],
    ["bulk query job", { method: "POST", path: `${V}/jobs/query`, body: "{}" }],
    ["legacy bulk", { method: "POST", path: "/services/async/66.0/job", body: "" }],
    ["partner SOAP", { method: "POST", path: "/services/Soap/u/66.0", body: "" }],
    ["Tooling SOAP", { method: "POST", path: "/services/Soap/T/66.0", body: "" }],
    ["REST deploy", { method: "POST", path: `${V}/metadata/deployRequest`, body: "" }],
    ["invocable action", { method: "POST", path: `${V}/actions/standard/emailSimple`, body: "{}" }],
    ["flow interview", { method: "POST", path: `${V}/actions/custom/flow/Do_Thing`, body: "{}" }],
  ];
  for (const [name, request] of cases) test(name, () => blocked(request));
});

describe("GET is not automatically safe", () => {
  // These are why the allowlist is by path. Each is a GET that changes the org
  // or reaches code we can't vouch for.
  const cases: Array<[string, string]> = [
    ["anonymous Apex", `${V}/tooling/executeAnonymous/?anonymousBody=delete%20[select%20id%20from%20account];`],
    ["run tests", `${V}/tooling/runTestsSynchronous/?classnames=Foo`],
    ["custom Apex REST", "/services/apexrest/barry/close-case"],
    ["a single record", `${V}/sobjects/Account/001000000000001AAA`],
    ["recent items", `${V}/recent`],
    ["search", `${V}/search?q=FIND%20%7Bx%7D`],
    ["queryAll", `${V}/queryAll${q("SELECT Id FROM Account")}`],
    ["user password", `${V}/sobjects/User/005000000000001AAA/password`],
    ["describe layouts", `${V}/sobjects/Account/describe/layouts`],
    ["OAuth endpoint", "/services/oauth2/userinfo"],
    ["a page", "/apex/SomePage"],
  ];
  for (const [name, path] of cases) test(name, () => blocked({ method: "GET", path }));
});

describe("tricks are refused", () => {
  test("_HttpMethod override on a POST", () => blocked({ ...composite([]), path: `${V}/composite/batch?_HttpMethod=PATCH` }));
  test("_HttpMethod override in any case on a GET", () => blocked({ method: "GET", path: `${V}/limits?_httpmethod=DELETE` }));
  test("X-HTTP-Method-Override header", () => blocked({ method: "GET", path: `${V}/limits`, headers: { "x-http-method-override": "PATCH" } }));
  test("path traversal into a write route", () => blocked({ method: "GET", path: `${V}/query/../sobjects/Account/001` }));
  test("encoded path traversal", () => blocked({ method: "GET", path: `${V}/query/%2e%2e/sobjects/Account` }));
  test("encoded slash", () => blocked({ method: "GET", path: `${V}/sobjects/Account%2Fdescribe` }));
  test("a full URL to another host", () => blocked({ method: "GET", path: "https://evil.example.com/services/data/v66.0/limits" }));
  test("a protocol-relative URL", () => blocked({ method: "GET", path: "//evil.example.com/services/data/v66.0/limits" }));
  test("a GET with a body", () => blocked({ method: "GET", path: `${V}/limits`, body: "{}" }));
  test("a lower-case method isn't assumed to be GET", () => blocked({ method: "get", path: `${V}/limits` }));
});

describe("SOQL through the allowed routes", () => {
  test("a query with no q parameter", () => blocked({ method: "GET", path: `${V}/query` }));
  test("FOR VIEW writes LastViewedDate", () => blocked({ method: "GET", path: `${V}/query${q("SELECT Id FROM Account LIMIT 1 FOR VIEW")}` }));
  test("FOR REFERENCE writes LastReferencedDate", () => blocked({ method: "GET", path: `${V}/query${q("SELECT Id FROM Account FOR REFERENCE")}` }));
  test("FOR UPDATE locks records", () => blocked({ method: "GET", path: `${V}/query${q("SELECT Id FROM Account FOR UPDATE")}` }));
  test("the same through Tooling", () => blocked({ method: "GET", path: `${V}/tooling/query${q("SELECT Id FROM ApexClass FOR VIEW")}` }));
  test("an unfilterable field is stopped before it costs a call", () => {
    assert.throws(() => authorise({ method: "GET", path: `${V}/query${q("SELECT Id FROM LoginHistory WHERE Status = 'Success'")}` }), UnfilterableFieldError);
  });
});

describe("composite batch", () => {
  const sub = (url: string, method = "GET") => ({ method, url });
  const select = `v66.0/query${q("SELECT Id FROM Account")}`;

  test("a write subrequest", () => blocked(composite([sub("v66.0/sobjects/Account", "POST")])));
  test("a PATCH subrequest", () => blocked(composite([sub("v66.0/sobjects/Account/001000000000001AAA", "PATCH")])));
  test("a GET to a path that isn't a query", () => blocked(composite([sub("v66.0/sobjects/Account/001000000000001AAA")])));
  test("Tooling, which can come back empty through composite", () => blocked(composite([sub(`v66.0/tooling/query${q("SELECT Id FROM ApexClass")}`)])));
  test("one bad subrequest among good ones", () => blocked(composite([sub(select), sub("v66.0/sobjects/Account", "POST")])));
  test("unsafe SOQL inside a subrequest", () => blocked(composite([sub(`v66.0/query${q("SELECT Id FROM Account FOR VIEW")}`)])));
  test("a subrequest that smuggles a body", () => blocked(composite([{ ...sub(select), richInput: { Name: "x" } }])));
  test("an override inside a subrequest", () => blocked(composite([sub(`${select}&_HttpMethod=DELETE`)])));
  test("more than 25 subrequests", () => blocked(composite(Array.from({ length: 26 }, () => sub(select)))));
  test("an empty batch", () => blocked(composite([])));
  test("an unexpected top-level key", () => blocked(composite([sub(select)], { allOrNone: true })));
  test("a body that isn't JSON", () => blocked({ method: "POST", path: `${V}/composite/batch`, body: "not json" }));
});

describe("Metadata API", () => {
  const writes = ["deploy", "deployRecentValidation", "cancelDeploy", "createMetadata", "updateMetadata", "upsertMetadata", "deleteMetadata", "renameMetadata"];
  for (const operation of writes) test(operation, () => blocked(soap(operation)));

  test("reads we don't use are refused too", () => {
    for (const operation of ["listMetadata", "readMetadata", "describeMetadata"]) blocked(soap(operation));
  });
  test("a SOAPAction that doesn't match the body", () => blocked(soap("deploy", "retrieve")));
  test("no SOAPAction", () => blocked({ ...soap("retrieve"), headers: {} }));
  test("two operations in one body", () => {
    const body = soapBody("retrieve").replace("</soapenv:Body>", "<met:deploy></met:deploy></soapenv:Body>");
    blocked({ ...soap("retrieve"), body });
  });
  test("a body that isn't SOAP", () => blocked({ ...soap("retrieve"), body: "hello" }));
  test("a POST with a query string", () => blocked({ ...soap("retrieve"), path: "/services/Soap/m/66.0?x=1" }));
});
