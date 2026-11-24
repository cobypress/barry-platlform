import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import { ApiBudget, BudgetStopError } from "../../salesforce/budget.ts";
import { classifyError, isUnavailable, KNOWN_CODES, LIMITS_NO_PERMISSION, NO_API_FOR_ORG, NoApiAccessError, SalesforceClient, SalesforceError, type ErrorClass } from "../../salesforce/client.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { loadExtracts, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type SimulatorOptions } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import type { PardotLeads } from "../pardot.ts";
import type { CrmHealth, OpportunityExtract } from "../opportunities.ts";
import type { OwnershipScan } from "../users.ts";
import type { ObjectFieldUsage } from "../configuration.ts";
import type { Sourced } from "../types.ts";

// A Salesforce error costs one check, never the run (DECISIONS.md 702). Each
// test runs the whole engine against the simulator, behind the real client and
// allowlist, with one answer changed as a real org might give it.

const UNKNOWN = "SOMETHING_NEW_FROM_SALESFORCE";
const WORDING = "Salesforce's own wording that no customer should read";

const errorBody = (status: number, errorCode: string, message = WORDING): TransportResponse => ({ status, header: () => null, text: JSON.stringify([{ errorCode, message }]) });

/** The simulator, with any request `answer` names answered its way instead. */
function changed(answer: (request: TransportRequest) => TransportResponse | null, options: SimulatorOptions = {}): Transport {
  const simulator = new Simulator(options);
  return { send: async (request) => answer(request) ?? simulator.send(request) };
}

const soqlOf = (request: TransportRequest): string => new URL(request.url).searchParams.get("q") ?? "";
const isBatch = (request: TransportRequest, marker: string): boolean => new URL(request.url).pathname.endsWith("/composite/batch") && decodeURIComponent(request.body ?? "").includes(marker);

function run(transport: Transport, overrides: Partial<RunOptions> = {}) {
  const log: string[] = [];
  const done = runHealthCheck({ provider: syntheticProvider, connectedUser: org.CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(org.AS_OF), answers: loadAnswers(), transport, log: (line) => log.push(line), ...overrides });
  return { done, log };
}

const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
/** Everything a customer can be shown from a run's results. */
const customerText = (dir: string): string => ["findings.json", "metrics.json", "manual-steps.json"].map((f) => readFileSync(join(dir, f), "utf8")).join("\n");
const reasons = (dir: string): string[] => [...read<Finding[]>(dir, "findings.json"), ...read<Metric[]>(dir, "metrics.json")].flatMap((x) => ("reason" in x && typeof x.reason === "string" ? [x.reason] : []));

describe("the line between one read and the run (DECISIONS.md 702)", () => {
  const sf = (status: number, code: string, fault = false) => new SalesforceError(status, code, "m", { fault });

  test("every code we know falls in the class the decision lists", () => {
    const expected: Record<ErrorClass, string[]> = {
      not_available: ["INVALID_TYPE", "INVALID_FIELD", "NOT_FOUND", "API_DISABLED_FOR_ORG", "FUNCTIONALITY_NOT_ENABLED"],
      no_permission: ["INSUFFICIENT_ACCESS", "INSUFFICIENT_ACCESS_OR_READONLY", "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY", "API_CURRENTLY_DISABLED"],
      refused: ["MALFORMED_QUERY", "INVALID_QUERY_FILTER_OPERATOR", "INVALID_QUERY_SCOPE", "INVALID_QUERY_LOCATOR", "INVALID_OPERATION", "QUERY_TOO_COMPLICATED", "EXCEEDED_ID_LIMIT", "OPERATION_TOO_LARGE", "QUERY_TIMEOUT", "UNKNOWN_EXCEPTION", "RETRIEVE_FAILED", "RETRIEVE_TIMEOUT", "BAD_SOAP_RESPONSE"],
      auth_lost: ["INVALID_SESSION_ID", "INVALID_AUTH_HEADER"],
      api_limit: ["REQUEST_LIMIT_EXCEEDED"],
      instance_unavailable: ["SERVER_UNAVAILABLE"],
      not_salesforce: [],
    };
    for (const [kind, codes] of Object.entries(expected)) for (const code of codes) assert.equal(classifyError(sf(400, code)), kind, code);
    assert.equal(Object.keys(KNOWN_CODES).length, Object.values(expected).flat().length);
  });

  test("a code nobody has seen costs one read when Salesforce sent it, in a 4xx or a SOAP fault", () => {
    assert.equal(classifyError(sf(400, UNKNOWN)), "refused");
    assert.equal(classifyError(sf(403, UNKNOWN)), "refused");
    assert.equal(classifyError(sf(500, UNKNOWN, true)), "refused");
    assert.ok(isUnavailable(sf(400, UNKNOWN)));
  });

  test("a lost session, the daily limit, an outage and our own faults stop the run", () => {
    assert.equal(classifyError(sf(401, "INVALID_SESSION_ID")), "auth_lost");
    assert.equal(classifyError(sf(401, UNKNOWN)), "auth_lost");
    assert.equal(classifyError(sf(500, "INVALID_SESSION_ID", true)), "auth_lost");
    assert.equal(classifyError(new BudgetStopError("limit")), "api_limit");
    assert.equal(classifyError(sf(503, UNKNOWN)), "instance_unavailable");
    assert.equal(classifyError(sf(502, "HTTP_502")), "instance_unavailable");
    assert.equal(classifyError(sf(404, "HTTP_404")), "instance_unavailable");
    assert.equal(classifyError(sf(429, UNKNOWN)), "instance_unavailable");
    assert.equal(classifyError(new TypeError("ours")), "not_salesforce");
    for (const err of [sf(401, "INVALID_SESSION_ID"), sf(503, UNKNOWN), sf(502, "HTTP_502"), new BudgetStopError("limit"), new Error("ours")]) assert.equal(isUnavailable(err), false);
  });
});

describe("an error code nobody listed on an optional read (audit risk 4)", () => {
  test("costs that read's checks, in plain words, and the run finishes", async () => {
    const { done, log } = run(changed((r) => (soqlOf(r).includes("FROM ForecastingQuota") ? errorBody(400, UNKNOWN) : null)));
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    const plain = "ForecastingQuota couldn't be read, because Salesforce refused this read.";
    assert.ok(reasons(dir).some((r) => r.endsWith(plain)), "a check says what couldn't be read");
    assert.equal(notMeasurableKind(plain), "unreadable");
    // The code is ours to see, in run.json and the log, and never the customer's.
    assert.deepEqual(finished.readsRefused, [{ step: "forecast-quotas", read: "ForecastingQuota", code: UNKNOWN, status: 400 }]);
    assert.ok(log.some((line) => line.includes(UNKNOWN)));
    assert.doesNotMatch(customerText(dir), new RegExp(`${UNKNOWN}|${WORDING}`));
  });

  test("a code we know says why", async () => {
    const { done } = run(changed((r) => (soqlOf(r).includes("FROM ForecastingQuota") ? errorBody(400, "FUNCTIONALITY_NOT_ENABLED") : null)));
    const { dir } = await done;
    assert.ok(reasons(dir).some((r) => r.endsWith("ForecastingQuota couldn't be read, because it isn't available in this Salesforce edition or isn't switched on.")));
    assert.doesNotMatch(customerText(dir), /FUNCTIONALITY_NOT_ENABLED/);
  });

  test("an unlisted SOAP fault on a Metadata API retrieve costs that retrieve, not the run", async () => {
    const { done } = run(new Simulator({ failRetrieve: (body) => (body.includes("<met:name>Settings</met:name>") ? { faultCode: UNKNOWN, message: WORDING } : null) }));
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    assert.ok(finished.readsRefused?.some((r) => r.code === UNKNOWN && r.read === "The org's set-up from the Metadata API"));
    assert.doesNotMatch(customerText(dir), new RegExp(`${UNKNOWN}|${WORDING}`));
  });
});

describe("a whole composite batch refused (audit risk 5)", () => {
  test("field usage, ownership, CRM health and Account Engagement's leads each lose their counts, and the run finishes", async () => {
    const markers = ["COUNT(Id) total", "SELECT OwnerId, COUNT(Id) c", "FROM Contact WHERE Email = null", "pi__url__c"];
    const { done } = run(
      changed((r) => (markers.some((m) => isBatch(r, m)) ? errorBody(400, "INVALID_OPERATION") : null)),
      { modules: ALL_MODULES },
    );
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    const extracts = loadExtracts(dir);

    const usage = extracts.get("field-usage") as Record<string, Sourced<ObjectFieldUsage>>;
    const scanned = Object.values(usage).filter((u) => u.status === "unavailable");
    assert.ok(scanned.length > 0);
    for (const u of scanned) assert.match(u.status === "unavailable" ? u.reason : "", /^(Account|Contact|Lead|Opportunity) couldn't be read, because Salesforce refused this read\.$/);

    const ownership = extracts.get("ownership") as OwnershipScan;
    assert.deepEqual(ownership.scanned, []);
    for (const object of ["Account", "Contact", "Lead", "Opportunity", "Case"]) assert.ok(ownership.incomplete.includes(object), object);

    const crm = extracts.get("crm-health") as CrmHealth;
    for (const [key, count] of Object.entries(crm.counts)) assert.equal(count.status, "unavailable", key);
    assert.equal(crm.counts.contactsNoEmail!.status === "unavailable" && crm.counts.contactsNoEmail!.reason, "Contact couldn't be read, because Salesforce refused this read.");

    const pardot = extracts.get("pardot-leads") as PardotLeads;
    for (const [key, count] of Object.entries(pardot.counts)) assert.equal(count.status === "unavailable" && count.reason, "Lead couldn't be read, because Salesforce refused this read.", key);

    assert.doesNotMatch(customerText(dir), /INVALID_OPERATION/);
  });

  test("a whole batch lost to an outage still fails the run", async () => {
    const { done } = run(changed((r) => (isBatch(r, "SELECT OwnerId, COUNT(Id) c") ? errorBody(503, "SERVER_UNAVAILABLE") : null)));
    await assert.rejects(done, /SERVER_UNAVAILABLE/);
  });
});

describe("the limits read at connect (audit risk 2)", () => {
  const limitsRefused = (code: string) => (r: TransportRequest) => (new URL(r.url).pathname.endsWith("/limits") ? errorBody(403, code) : null);

  test("refused for want of View Setup and Configuration, the run goes on with the limits not measured", async () => {
    const { done } = run(changed(limitsRefused("INSUFFICIENT_ACCESS")));
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    const extracts = loadExtracts(dir);
    assert.deepEqual(extracts.get("api-limits"), { status: "unavailable", reason: LIMITS_NO_PERMISSION, source: { kind: "rest", path: `/services/data/v${org.API_VERSION}/limits` } });
    // The usage header on each response gave the real figures.
    assert.equal(finished.budget.dailyLimit, 100_000);
    assert.doesNotMatch(customerText(dir), new RegExp(WORDING));
  });

  test("with no limits and no usage header, a connection may spend only a small allowance, never an unlimited one", async () => {
    const transport: Transport = {
      async send(request) {
        const path = new URL(request.url).pathname;
        if (path === "/services/data/") return { status: 200, header: () => null, text: JSON.stringify([{ version: org.API_VERSION }]) };
        if (path.endsWith("/limits")) return errorBody(403, "INSUFFICIENT_ACCESS");
        return { status: 200, header: () => null, text: JSON.stringify({ totalSize: 0, done: true, records: [] }) };
      },
    };
    const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport, sleep: async () => {} });
    assert.equal(client.limitsUnread, LIMITS_NO_PERMISSION);
    assert.deepEqual(client.limitsAtConnect, {});
    let calls = 0;
    await assert.rejects(async () => {
      for (;;) {
        await client.query("SELECT Id FROM Account");
        calls++;
      }
    }, (err: unknown) => err instanceof BudgetStopError && /couldn't be read/.test(err.message));
    assert.equal(calls, 1_000);
  });

  test("with no API for the org, the run fails at once with a plain sentence", async () => {
    const { done } = run(changed(limitsRefused("API_DISABLED_FOR_ORG")));
    await assert.rejects(done, (err: unknown) => err instanceof NoApiAccessError && err.message === NO_API_FOR_ORG);
  });
});

describe("rows in a shape we don't expect (audit risk 6)", () => {
  const records = (deals: number, history: number) => {
    const pipeline = structuredClone(org.pipeline);
    // A number where a date belongs: Salesforce would never, and an export might.
    for (const o of pipeline.opportunities.slice(0, deals)) (o as Record<string, unknown>).LastActivityDate = 20260101;
    for (const h of pipeline.history.slice(0, history)) (h as Record<string, unknown>).Amount = "lots";
    return { ...SYNTHETIC_RECORDS, pipeline };
  };

  test("one bad deal and one bad history entry are left out, counted and said", async () => {
    const { done } = run(new Simulator({ records: records(1, 1) }));
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    const deals = loadExtracts(dir).get("opportunities") as OpportunityExtract;
    assert.deepEqual(deals.leftOut, { deals: 1, history: 1 });
    assert.equal(deals.opportunities.status, "ok");
    const metrics = read<Metric[]>(dir, "metrics.json");
    const winRate = metrics.find((m) => m.id === "historic.win_rate")!;
    assert.equal(winRate.status, "measured");
    assert.ok(winRate.status === "measured" && winRate.assumptions.includes("1 deal couldn't be read and is left out."));
    assert.ok(winRate.status === "measured" && winRate.assumptions.includes("1 entry in the deals' stage history couldn't be read and is left out."));
  });

  test("more than 1 in 100 bad deals and the deals aren't measured, rather than skewed", async () => {
    const { done } = run(new Simulator({ records: records(40, 0) }));
    const { dir, run: finished } = await done;
    assert.equal(finished.status, "in_review");
    const deals = loadExtracts(dir).get("opportunities") as OpportunityExtract;
    assert.equal(deals.opportunities.status, "unavailable");
    assert.match(deals.opportunities.status === "unavailable" ? deals.opportunities.reason : "", /^40 of the [\d,]+ deals Salesforce gave us weren't in a shape we could read, more than 1 in 100, so figures from them would mislead\.$/);
    const winRate = read<Metric[]>(dir, "metrics.json").find((m) => m.id === "historic.win_rate")!;
    assert.equal(winRate.status, "not_measurable");
  });

  test("a clean org says nothing was left out", async () => {
    const { done } = run(new Simulator());
    const { dir } = await done;
    assert.equal((loadExtracts(dir).get("opportunities") as OpportunityExtract).leftOut, undefined);
    assert.doesNotMatch(customerText(dir), /couldn't be read and (?:is|are) left out/);
  });
});

describe("what still stops the run", () => {
  const onQuota = (response: TransportResponse) => (r: TransportRequest) => (soqlOf(r).includes("FROM ForecastingQuota") ? response : null);

  test("authentication lost on an optional read fails the run, and isn't taken as not measurable", async () => {
    const { done } = run(changed(onQuota(errorBody(401, "INVALID_SESSION_ID", "Session expired or invalid"))));
    await assert.rejects(done, (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_SESSION_ID");
  });

  test("the daily API limit reached on an optional read stops the run where it can be resumed", async () => {
    const { done } = run(changed(onQuota(errorBody(403, "REQUEST_LIMIT_EXCEEDED", "TotalRequests Limit exceeded."))));
    const { run: stopped } = await done;
    assert.equal(stopped.status, "stopped_budget");
  });
});
