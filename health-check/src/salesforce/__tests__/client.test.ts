import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { strToU8, zipSync } from "fflate";
import { ApiBudget, BudgetStopError } from "../budget.ts";
import { CLIENT_METHODS, SalesforceClient, SalesforceError, instanceOrigin, isUnavailable } from "../client.ts";
import { ForbiddenOperationError } from "../allowlist.ts";
import type { Transport, TransportRequest, TransportResponse } from "../transport.ts";
import type { ConnectionProvider } from "../connection.ts";

const TOKEN = "00D000000000001!SECRETTOKENVALUE";

const provider = (token = TOKEN): ConnectionProvider => ({
  kind: "sf-cli",
  connect: async () => ({ instanceUrl: "https://x.example.com", accessToken: token, orgId: "00D000000000001AAA" }),
  refresh: async () => ({ instanceUrl: "https://x.example.com", accessToken: "refreshed-token", orgId: "00D000000000001AAA" }),
});

type Handler = (request: TransportRequest) => Partial<TransportResponse> & { json?: unknown };

function stub(handler: Handler, limit = { used: 100, max: 10_000 }): Transport & { requests: TransportRequest[] } {
  const requests: TransportRequest[] = [];
  return {
    requests,
    async send(request) {
      requests.push(request);
      const url = new URL(request.url);
      if (url.pathname === "/services/data/") return ok([{ version: "66.0" }]);
      if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: limit.max, Remaining: limit.max - limit.used } });
      const r = handler(request);
      return { status: r.status ?? 200, header: r.header ?? (() => null), text: r.text ?? JSON.stringify(r.json ?? {}) };
    },
  };
}

function ok(json: unknown): TransportResponse {
  return { status: 200, header: () => null, text: JSON.stringify(json) };
}

const connect = (transport: Transport, budget = new ApiBudget({ stopAtShare: 0.5 }), p = provider()) =>
  SalesforceClient.connect(p, { budget, transport, sleep: async () => {}, retrievePollMs: 0 });

describe("the client's surface", () => {
  test("has no method beyond the pinned read list", () => {
    const onPrototype = Object.getOwnPropertyNames(SalesforceClient.prototype).filter((n) => n !== "constructor");
    const instance = ["apiVersion", "orgId", "connectionKind", "budget"];
    assert.deepEqual([...onPrototype, ...instance].sort(), [...CLIENT_METHODS].sort());
  });

  test("there's no generic request, write or delete method", async () => {
    const client = await connect(stub(() => ({})));
    for (const name of ["request", "send", "create", "update", "upsert", "delete", "insert", "post", "patch", "put", "executeAnonymous"]) {
      assert.equal((client as unknown as Record<string, unknown>)[name], undefined, name);
    }
  });

  test("unsafe SOQL never reaches the network", async () => {
    const transport = stub(() => ({ json: { records: [], totalSize: 0 } }));
    const client = await connect(transport);
    const before = transport.requests.length;
    await assert.rejects(client.query("SELECT Id FROM Account FOR VIEW"), ForbiddenOperationError);
    assert.equal(transport.requests.length, before);
    assert.equal(client.budget.callsMade, 0);
  });

  test("an object name can't be used to reach another route", async () => {
    const transport = stub(() => ({ json: {} }));
    const client = await connect(transport);
    const before = transport.requests.length;
    await assert.rejects(client.describe("Account/001000000000001AAA"), ForbiddenOperationError);
    assert.equal(transport.requests.length, before);
  });

  test("a nextRecordsUrl from a hostile response is still checked", async () => {
    const transport = stub(() => ({ json: { records: [{ Id: "1" }], totalSize: 2, nextRecordsUrl: "/services/data/v66.0/sobjects/Account/001000000000001AAA" } }));
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), ForbiddenOperationError);
  });
});

describe("source scan", () => {
  const src = fileURLToPath(new URL("../../", import.meta.url));
  const files = (dir: string): string[] =>
    readdirSync(dir).flatMap((name) => {
      const path = join(dir, name);
      if (statSync(path).isDirectory()) return name === "__tests__" || name === "testing" ? [] : files(path);
      return path.endsWith(".ts") ? [path] : [];
    });

  test("only transport.ts touches the network", () => {
    for (const file of files(src)) {
      const text = readFileSync(file, "utf8");
      if (file.endsWith("salesforce/transport.ts")) continue;
      assert.doesNotMatch(text, /\bfetch\(/, `${file} calls fetch`);
      assert.doesNotMatch(text, /from "(node:)?https?"|from "undici"|from "jsforce"|from "axios"/, `${file} imports an HTTP library`);
    }
  });

  test("only client.ts sends through a transport, and only after authorise", () => {
    for (const file of files(src)) {
      if (file.endsWith("salesforce/client.ts") || file.endsWith("salesforce/transport.ts")) continue;
      assert.doesNotMatch(readFileSync(file, "utf8"), /transport\.send\(/, file);
    }
    const client = readFileSync(join(src, "salesforce/client.ts"), "utf8");
    assert.equal(client.match(/#transport\.send\(/g)?.length, 1, "exactly one send");
    assert.ok(client.indexOf("authorise(request)") < client.indexOf("#transport.send("), "authorise runs first");
  });

  test("nothing else builds a write", () => {
    for (const file of files(src)) {
      if (file.endsWith("allowlist.ts") || file.endsWith("client.ts") || file.endsWith("salesforce/transport.ts")) continue;
      assert.doesNotMatch(readFileSync(file, "utf8"), /method:\s*"(POST|PATCH|PUT|DELETE)"/, file);
    }
  });

  // The OAuth token and revoke endpoints are POSTs outside the data API. They're
  // the one other way out, and only behind the endpoint check (DECISIONS.md 55).
  test("transport.ts has one POST, for the OAuth endpoints only, behind the endpoint check", () => {
    const transport = readFileSync(join(src, "salesforce/transport.ts"), "utf8");
    assert.equal(transport.match(/method:\s*"(POST|PATCH|PUT|DELETE)"/g)?.length, 1);
    assert.equal(transport.match(/\bfetch\(/g)?.length, 2, "the data transport and the OAuth transport");
    const oauth = transport.slice(transport.indexOf("export const fetchOAuthTransport"));
    assert.match(oauth, /^export const fetchOAuthTransport: OAuthTransport = checkedOAuthTransport\(\{/);
    assert.match(oauth, /method:\s*"POST"/);
    const oauthModule = readFileSync(join(src, "salesforce/oauth.ts"), "utf8");
    assert.equal(oauthModule.match(/\.postForm\(/g)?.length, 2, "token and revoke");
    assert.equal(oauthModule.match(/assertOAuthEndpoint\(endpoint\)/g)?.length, 2, "each checked first");
  });
});

describe("platform behaviour: Tooling through composite batch", () => {
  test("queryBatch can't carry a Tooling query, and queries go through the REST route", async () => {
    const transport = stub((r) => ({ json: { results: (JSON.parse(r.body!) as { batchRequests: unknown[] }).batchRequests.map(() => ({ statusCode: 200, result: { records: [], totalSize: 0 } })) } }));
    const client = await connect(transport);
    await client.queryBatch(["SELECT Id FROM Account"]);
    const body = transport.requests.at(-1)!.body!;
    assert.doesNotMatch(body, /tooling/);
  });

  test("Tooling queries use the direct route", async () => {
    const transport = stub(() => ({ json: { records: [], totalSize: 0 } }));
    const client = await connect(transport);
    await client.query("SELECT Id FROM ApexClass", { api: "tooling" });
    assert.match(transport.requests.at(-1)!.url, /\/tooling\/query\?q=/);
    assert.equal(transport.requests.at(-1)!.method, "GET");
  });
});

describe("queries", () => {
  test("follows nextRecordsUrl", async () => {
    let page = 0;
    const transport = stub(() => {
      page++;
      return { json: page === 1 ? { records: [{ Id: "1" }], totalSize: 2, nextRecordsUrl: "/services/data/v66.0/query/01g000000000001AAA-1" } : { records: [{ Id: "2" }], totalSize: 2 } };
    });
    const client = await connect(transport);
    assert.deepEqual(await client.queryAll("SELECT Id FROM Account"), [{ Id: "1" }, { Id: "2" }]);
  });

  test("reports truncation at maxRecords", async () => {
    const transport = stub(() => ({ json: { records: [{ Id: "1" }, { Id: "2" }], totalSize: 9, nextRecordsUrl: "/services/data/v66.0/query/01g000000000001AAA-2" } }));
    const client = await connect(transport);
    const result = await client.query("SELECT Id FROM Account", { maxRecords: 2 });
    assert.equal(result.records.length, 2);
    assert.equal(result.truncated, true);
  });

  test("chunks composite batches at 25 and keeps order", async () => {
    const transport = stub((r) => ({ json: { results: (JSON.parse(r.body!) as { batchRequests: Array<{ url: string }> }).batchRequests.map((b) => ({ statusCode: 200, result: { records: [{ url: b.url }], totalSize: 1 } })) } }));
    const client = await connect(transport);
    const results = await client.queryBatch(Array.from({ length: 30 }, (_, i) => `SELECT Id FROM Obj${i}__c`));
    assert.equal(results.length, 30);
    assert.equal(client.budget.callsMade, 2);
    const last = results[29]!;
    assert.ok(last.ok && decodeURIComponent((last.result.records[0] as { url: string }).url).includes("Obj29__c"));
  });

  test("a failed subrequest is a result, not an exception", async () => {
    const transport = stub(() => ({ json: { results: [{ statusCode: 400, result: [{ errorCode: "INVALID_TYPE", message: "nope" }] }] } }));
    const client = await connect(transport);
    const [result] = await client.queryBatch(["SELECT Id FROM Missing__c"]);
    assert.ok(result && !result.ok && result.error.code === "INVALID_TYPE");
    assert.ok(result && !result.ok && isUnavailable(result.error));
  });
});

describe("connection failures", () => {
  test("refreshes once after a 401 and retries with the new token", async () => {
    let calls = 0;
    const transport = stub((r) => {
      calls++;
      if (calls === 1) return { status: 401, json: [{ errorCode: "INVALID_SESSION_ID", message: "expired" }] };
      assert.match(r.headers.Authorization!, /refreshed-token/);
      return { json: { records: [], totalSize: 0 } };
    });
    const client = await connect(transport);
    await client.query("SELECT Id FROM Account");
    assert.equal(calls, 2);
  });

  test("doesn't retry a second 401 forever", async () => {
    const transport = stub(() => ({ status: 401, json: [{ errorCode: "INVALID_SESSION_ID", message: "expired" }] }));
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), SalesforceError);
  });

  test("retries a 503 with backoff", async () => {
    let calls = 0;
    const transport = stub(() => (++calls < 3 ? { status: 503, text: "busy" } : { json: { records: [], totalSize: 0 } }));
    const client = await connect(transport);
    await client.query("SELECT Id FROM Account");
    assert.equal(calls, 3);
  });

  test("an error never carries the token", async () => {
    const transport = stub((r) => ({ status: 400, json: [{ errorCode: "MALFORMED_QUERY", message: `bad: ${r.url.slice(0, 20)}` }] }));
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), (err: Error) => {
      assert.doesNotMatch(`${err.message}${err.stack}`, /SECRETTOKEN/);
      return true;
    });
  });

  test("a nextRecordsUrl can only be a next page, even a read-only looking one", async () => {
    const transport = stub(() => ({ json: { records: [{ Id: "1" }], totalSize: 2, nextRecordsUrl: `/services/data/v66.0/query?q=${encodeURIComponent("SELECT Id FROM User")}` } }));
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), ForbiddenOperationError);
  });

  test("an instance URL can't smuggle another host", () => {
    assert.throws(() => instanceOrigin("https://acme.my.salesforce.com@evil.example", true), /bare host/);
    assert.throws(() => instanceOrigin("https://evil.example/acme.salesforce.com", true), /bare host/);
    assert.throws(() => instanceOrigin("https://acme.salesforce.com.evil.example", true), /isn't a Salesforce host/);
    assert.throws(() => instanceOrigin("https://evilsalesforce.com", true), /isn't a Salesforce host/);
    assert.throws(() => instanceOrigin("not a url", true), /valid URL/);
  });

  test("real Salesforce hosts are accepted, and normalised", () => {
    assert.equal(instanceOrigin("https://acme.my.salesforce.com/", true), "https://acme.my.salesforce.com");
    assert.equal(instanceOrigin("https://acme--dev.sandbox.my.salesforce.com", true), "https://acme--dev.sandbox.my.salesforce.com");
  });

  test("with the real transport, a host that isn't Salesforce is refused before any request", async () => {
    await assert.rejects(SalesforceClient.connect(provider(), { budget: new ApiBudget({ stopAtShare: 0.5 }) }), /isn't a Salesforce host/);
  });

  test("a refresh can't downgrade to http or move the token elsewhere", async () => {
    const bad: ConnectionProvider = { ...provider(), refresh: async () => ({ instanceUrl: "http://x.example.com", accessToken: "t" }) };
    const transport = stub(() => ({ status: 401, json: [{ errorCode: "INVALID_SESSION_ID", message: "expired" }] }));
    const client = await SalesforceClient.connect(bad, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport, sleep: async () => {} });
    await assert.rejects(client.query("SELECT Id FROM Account"), /must be https/);
  });

  test("refuses a plain http instance", async () => {
    const insecure: ConnectionProvider = { ...provider(), connect: async () => ({ instanceUrl: "http://x.example.com", accessToken: "t" }) };
    await assert.rejects(SalesforceClient.connect(insecure, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: stub(() => ({})) }), /https/);
  });
});

describe("API budget", () => {
  test("stops before the call that would cross the share", async () => {
    const budget = new ApiBudget({ stopAtShare: 0.5 });
    const transport = stub(() => ({ json: { records: [], totalSize: 0 } }), { used: 4_998, max: 10_000 });
    const client = await connect(transport, budget);
    await client.query("SELECT Id FROM Account");
    await client.query("SELECT Id FROM Account");
    const sent = transport.requests.length;
    await assert.rejects(client.query("SELECT Id FROM Account"), BudgetStopError);
    assert.equal(transport.requests.length, sent, "the third call never left");
  });

  test("an org already over the share gets no calls at all", async () => {
    const transport = stub(() => ({ json: { records: [], totalSize: 0 } }), { used: 6_000, max: 10_000 });
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), BudgetStopError);
  });

  test("honours the header, which sees other integrations spending", () => {
    const budget = new ApiBudget({ stopAtShare: 0.5 });
    budget.observe(100, 10_000);
    budget.observeHeader("api-usage=4999/10000");
    budget.assertCanSpend();
    budget.observeHeader("api-usage=5000/10000");
    assert.throws(() => budget.assertCanSpend(), BudgetStopError);
  });

  test("stops at the per-run cap whatever the org's limit", () => {
    const budget = new ApiBudget({ stopAtShare: 1, maxCallsPerRun: 2 });
    budget.observe(0, 1_000_000);
    budget.recordCall(2);
    assert.throws(() => budget.assertCanSpend(), /cap of 2/);
  });

  test("won't spend until it knows the limit", () => {
    assert.throws(() => new ApiBudget({ stopAtShare: 0.5 }).assertCanSpend(), BudgetStopError);
  });

  test("rejects a nonsense share", () => {
    for (const share of [0, -1, 1.5]) assert.throws(() => new ApiBudget({ stopAtShare: share }));
  });

  test("Salesforce's own limit error stops the run rather than failing it", async () => {
    const transport = stub(() => ({ status: 403, json: [{ errorCode: "REQUEST_LIMIT_EXCEEDED", message: "over" }] }));
    const client = await connect(transport);
    await assert.rejects(client.query("SELECT Id FROM Account"), BudgetStopError);
  });

  test("re-reads /limits regularly, because SOAP responses carry no usage header", async () => {
    const transport = stub(() => ({ json: { records: [], totalSize: 0 } }));
    const client = await connect(transport);
    for (let i = 0; i < 55; i++) await client.query("SELECT Id FROM Account");
    const reads = transport.requests.filter((r) => new URL(r.url).pathname.endsWith("/limits"));
    assert.equal(reads.length, 2);
  });
});

describe("metadata retrieve", () => {
  const envelope = (operation: string, inner: string): string =>
    `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><${operation}Response xmlns="http://soap.sforce.com/2006/04/metadata"><result>${inner}</result></${operation}Response></soapenv:Body></soapenv:Envelope>`;
  const zip = Buffer.from(zipSync({ "flows/A.flow": strToU8("<Flow/>") })).toString("base64");

  test("polls until done, then unzips", async () => {
    let checks = 0;
    const transport = stub((r) => {
      if (r.body!.includes("<met:retrieve>")) return { text: envelope("retrieve", "<id>09S000000000001AAA</id><done>false</done>") };
      checks++;
      return { text: envelope("checkRetrieveStatus", checks < 3 ? "<done>false</done>" : `<done>true</done><success>true</success><zipFile>${zip}</zipFile>`) };
    });
    const client = await connect(transport);
    assert.deepEqual(await client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), { "flows/A.flow": "<Flow/>" });
    assert.equal(checks, 3);
  });

  test("the token goes in the SOAP header and members are escaped", async () => {
    const transport = stub((r) => ({ text: r.body!.includes("<met:retrieve>") ? envelope("retrieve", "<id>09S000000000001AAA</id>") : envelope("checkRetrieveStatus", `<done>true</done><success>true</success><zipFile>${zip}</zipFile>`) }));
    const client = await connect(transport);
    await client.retrieveMetadata([{ name: "Flow", members: ["a<b"] }]);
    const body = transport.requests.find((r) => r.body?.includes("<met:retrieve>"))!.body!;
    assert.match(body, /<met:sessionId>00D000000000001!SECRETTOKENVALUE<\/met:sessionId>/);
    assert.match(body, /a&lt;b/);
  });

  const expired = `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>sf:INVALID_SESSION_ID</faultcode><faultstring>INVALID_SESSION_ID: Invalid Session ID found in SessionHeader</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`;

  test("an expired session mid-retrieve refreshes once, with the new token in the envelope", async () => {
    const zip = Buffer.from(zipSync({ "flows/A.flow": strToU8("<Flow/>") })).toString("base64");
    let faults = 0;
    const transport = stub((r) => {
      if (r.body!.includes("<met:retrieve>")) return { text: envelope("retrieve", "<id>09S000000000001AAA</id><done>false</done>") };
      if (faults === 0) {
        faults++;
        return { status: 500, text: expired };
      }
      return { text: envelope("checkRetrieveStatus", `<done>true</done><success>true</success><zipFile>${zip}</zipFile>`) };
    });
    const client = await connect(transport);
    assert.deepEqual(await client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), { "flows/A.flow": "<Flow/>" });
    const checks = transport.requests.filter((r) => r.body?.includes("<met:checkRetrieveStatus>"));
    assert.equal(checks.length, 2);
    assert.ok(checks[0]!.body!.includes(TOKEN));
    assert.ok(checks[1]!.body!.includes("refreshed-token"));
    assert.ok(!checks[1]!.body!.includes(TOKEN));
  });

  test("a session that stays expired fails rather than looping", async () => {
    const transport = stub(() => ({ status: 500, text: expired }));
    const client = await connect(transport);
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), /INVALID_SESSION_ID/);
    assert.equal(transport.requests.filter((r) => r.body?.includes("<met:retrieve>")).length, 2);
  });

  test("a SOAP refresh can't move the token elsewhere", async () => {
    const bad: ConnectionProvider = { ...provider(), refresh: async () => ({ instanceUrl: "http://x.example.com", accessToken: "t" }) };
    const transport = stub(() => ({ status: 500, text: expired }));
    const client = await connect(transport, undefined, bad);
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), /https/);
    assert.ok(!transport.requests.some((r) => r.body?.includes(">t<")));
  });

  test("other SOAP faults don't trigger a refresh", async () => {
    let refreshes = 0;
    const counting: ConnectionProvider = { ...provider(), refresh: async () => (refreshes++, provider().refresh()) };
    const transport = stub(() => ({ status: 500, text: expired.replaceAll("INVALID_SESSION_ID", "INVALID_OPERATION") }));
    const client = await connect(transport, undefined, counting);
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]));
    assert.equal(refreshes, 0);
  });

  test("a failed retrieve is an error", async () => {
    const transport = stub((r) => ({ text: r.body!.includes("<met:retrieve>") ? envelope("retrieve", "<id>09S000000000001AAA</id>") : envelope("checkRetrieveStatus", "<done>true</done><success>false</success><errorMessage>nope</errorMessage>") }));
    const client = await connect(transport);
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), /RETRIEVE_FAILED/);
  });

  test("a SOAP fault becomes a coded error", async () => {
    const transport = stub(() => ({ status: 500, text: `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>sf:INSUFFICIENT_ACCESS</faultcode><faultstring>no</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>` }));
    const client = await connect(transport);
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), (err: unknown) => isUnavailable(err));
  });
});
