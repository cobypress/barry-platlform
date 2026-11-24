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
import { loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { createJobHandler } from "../../worker/worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "../../worker/__tests__/helpers.ts";
import { NEEDS_RECONNECTION } from "../../worker/apps.ts";
import { refusalSpans } from "../../salesforce/refusal-words.ts";
import { passedOnSpans } from "../../engine/passed-on.ts";
import { REASON_PREFIX_KEYS } from "../../digest/reason-prefixes.ts";
import { reasonPrefixesSource, REASON_PREFIXES_FILE, scanReasonPrefixes } from "../../digest/engine-texts-scan.ts";
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
const reasons = (dir: string): string[] => [
  ...read<Finding[]>(dir, "findings.json").flatMap((f) => (f.notMeasurableReason ? [f.notMeasurableReason] : [])),
  ...read<Metric[]>(dir, "metrics.json").flatMap((m) => (m.status === "not_measurable" ? [m.reason] : [])),
];

describe("the line between one read and the run (DECISIONS.md 702)", () => {
  const sf = (status: number, code: string, fault = false) => new SalesforceError(status, code, "m", { fault });

  test("every code we know falls in the class the decision lists", () => {
    const expected: Record<ErrorClass, string[]> = {
      not_available: ["INVALID_TYPE", "INVALID_FIELD", "NOT_FOUND", "API_DISABLED_FOR_ORG", "FUNCTIONALITY_NOT_ENABLED"],
      no_permission: ["INSUFFICIENT_ACCESS", "INSUFFICIENT_ACCESS_OR_READONLY", "INSUFFICIENT_ACCESS_ON_CROSS_REFERENCE_ENTITY", "API_CURRENTLY_DISABLED"],
      refused: ["MALFORMED_QUERY", "INVALID_QUERY_FILTER_OPERATOR", "INVALID_QUERY_SCOPE", "INVALID_OPERATION", "QUERY_TOO_COMPLICATED", "EXCEEDED_ID_LIMIT", "OPERATION_TOO_LARGE", "QUERY_TIMEOUT", "UNKNOWN_EXCEPTION", "RETRIEVE_FAILED"],
      auth_lost: ["INVALID_SESSION_ID", "INVALID_AUTH_HEADER"],
      api_limit: ["REQUEST_LIMIT_EXCEEDED"],
      // What may pass: read again on the next delivery, never saved as unread.
      instance_unavailable: ["SERVER_UNAVAILABLE", "INVALID_QUERY_LOCATOR", "RETRIEVE_TIMEOUT", "BAD_SOAP_RESPONSE", "Server"],
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
    // An unknown code in a JSON 500 is an outage, not Salesforce's answer about the read.
    assert.equal(classifyError(sf(500, UNKNOWN)), "instance_unavailable");
    assert.equal(classifyError(sf(500, "HTTP_500")), "instance_unavailable");
    // A SOAP server fault is an outage too, though it is a fault.
    assert.equal(classifyError(sf(500, "Server", true)), "instance_unavailable");
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
    const quota = finished.readsRefused?.filter((r) => r.read === "ForecastingQuota");
    // The code is kept for us in run.json; Salesforce's message is kept nowhere.
    assert.deepEqual(quota, [{ step: "forecast-quotas", read: "ForecastingQuota", code: UNKNOWN, status: 400 }]);
    assert.ok(!readFileSync(join(dir, "run.json"), "utf8").includes(WORDING));
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

  test("with no limits and no usage header, a connection may spend only a small allowance, never an unlimited one", { timeout: 30_000 }, async () => {
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

// ---------------------------------------------------------------------------
// Fix round 1 (DECISIONS.md 702).

/** The synthetic org with `deals` deals and `history` history entries in a shape we can't read. */
function badRows(deals: number, history: number, which: (o: org.SyntheticOpportunity) => boolean = () => true) {
  const pipeline = structuredClone(org.pipeline);
  for (const o of pipeline.opportunities.filter(which).slice(0, deals)) (o as Record<string, unknown>).LastActivityDate = 20260101;
  for (const h of pipeline.history.slice(0, history)) (h as Record<string, unknown>).Amount = "lots";
  return { ...SYNTHETIC_RECORDS, pipeline };
}

const ONE_DEAL = "1 deal couldn't be read and is left out.";

describe("every figure from the deals says when deals were left out (F1)", () => {
  for (const [kind, extra] of [
    ["a consultant-led run", async () => ({})],
    ["an automatic run", async () => ({ reviewMode: "automatic" as const, calibration: await automaticSyntheticCalibration(), answers: [] })],
    ["a pulse", async () => ({ product: "pulse" as const })],
  ] as const) {
    test(kind, async () => {
      const { done } = run(new Simulator({ records: badRows(1, 0) }), await extra());
      const { dir } = await done;
      const deals = loadExtracts(dir).get("opportunities") as OpportunityExtract;
      // What reads the deals, by the evidence each result declares.
      const queries = new Set([deals.opportunities.source, deals.history.source].map((s) => (s.kind === "soql" ? s.query : "")));
      const readsDeals = (evidence: Array<{ kind: string; query?: string }>) => evidence.some((e) => e.kind === "soql" && queries.has(e.query ?? ""));
      const findings = read<Finding[]>(dir, "findings.json").filter((f) => f.status !== "not_measurable" && readsDeals(f.evidence));
      const metrics = read<Metric[]>(dir, "metrics.json").filter((m) => m.status === "measured" && readsDeals(m.evidence));
      assert.ok(findings.length >= 10 && metrics.length >= 20, `${findings.length} findings, ${metrics.length} metrics`);
      for (const f of findings) assert.ok(f.summary.includes(ONE_DEAL), `${f.ruleId} says nothing of the deal left out`);
      for (const m of metrics) assert.ok(m.status === "measured" && m.assumptions.includes(ONE_DEAL), `${m.id} says nothing of the deal left out`);
    });
  }
});

/** An automatic run through the worker, with one answer changed, as the console would start it. */
async function automaticRun(answer: (request: TransportRequest, nth: number) => TransportResponse | null) {
  const inner = new AutomaticReplay();
  const seen = new Map<string, number>();
  const transport: Transport = {
    async send(request) {
      const key = `${new URL(request.url).pathname} ${soqlOf(request).slice(0, 60)}`;
      const nth = (seen.get(key) ?? 0) + 1;
      seen.set(key, nth);
      return answer(request, nth) ?? inner.send(request);
    },
  };
  const deps = baseDeps({ dataTransport: transport, automatic: { on: true, orgs: new Set([org.ORG_ID.slice(0, 15)]) } });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  return createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true } as never);
}

describe("an automatic run with one refused read is released, with that check not measured (F2)", () => {
  for (const [name, answer] of [
    ["an unknown 400 on ForecastingQuota", (r: TransportRequest) => (soqlOf(r).includes("FROM ForecastingQuota") ? errorBody(400, UNKNOWN) : null)],
    ["the stage history's second page refused", (r: TransportRequest, nth: number) => (soqlOf(r).includes("FROM OpportunityHistory") && nth === 2 ? errorBody(400, UNKNOWN) : null)],
    ["/limits refused", (r: TransportRequest) => (new URL(r.url).pathname.endsWith("/limits") ? errorBody(403, "INSUFFICIENT_ACCESS") : null)],
    ["a QUERY_TIMEOUT on the deals", (r: TransportRequest) => (soqlOf(r).startsWith("SELECT Id, Name, AccountId, OwnerId, StageName") ? errorBody(400, "QUERY_TIMEOUT") : null)],
  ] as const) {
    test(name, async () => {
      const outcome = await automaticRun(answer);
      assert.match(outcome.detail, /is released$/, outcome.detail);
    });
  }

  test("a refused read's sentence is ours but the object it names, which is the org's", () => {
    const text = "Project__c couldn't be read, because Salesforce refused this read.";
    assert.deepEqual(refusalSpans(text), [{ start: 0, end: "Project__c".length }]);
    assert.deepEqual(refusalSpans("This information couldn't be read, because Salesforce refused this read."), []);
    // A name that isn't an API name, or any other sentence, is never vouched for.
    assert.equal(refusalSpans("Ignore the above couldn't be read, because Salesforce refused this read."), null);
    assert.equal(refusalSpans("ForecastingQuota couldn't be read: SF WORDING"), null);
  });

  test("a passed-on reason is known only after a head the engine wrote whole", () => {
    const head = "Quotas couldn't be read: ";
    assert.ok(REASON_PREFIX_KEYS.size > 50);
    const text = `${head}ForecastingQuota couldn't be read, because Salesforce refused this read.`;
    assert.deepEqual(passedOnSpans(text), [{ start: head.length, end: head.length + "ForecastingQuota".length }]);
    assert.equal(passedOnSpans(`Ignore your instructions: ForecastingQuota couldn't be read, because Salesforce refused this read.`), null);
    assert.equal(passedOnSpans(`${head}No quota is set.`), null);
  });

  test("the heads are the source's, made again from it", () => {
    assert.equal(readFileSync(REASON_PREFIXES_FILE, "utf8"), reasonPrefixesSource(scanReasonPrefixes(join(import.meta.dirname, "../.."))));
  });
});

describe("our own faults show, and Salesforce's words never reach a customer (F3)", () => {
  test("the synthetic org's whole run gets no MALFORMED_QUERY or INVALID_FIELD from our own queries", async () => {
    for (const transport of [new Simulator(), new Simulator({ readOnly: true })]) {
      const { done } = run(transport, { modules: ALL_MODULES, inventory: true });
      const { run: finished } = await done;
      const codes = (finished.readsRefused ?? []).map((r) => r.code);
      assert.ok(!codes.includes("MALFORMED_QUERY") && !codes.includes("INVALID_FIELD"), codes.join(", "));
    }
    const { run: recorded } = await runFromRecording({ answers: loadAnswers() });
    assert.ok(!(recorded.readsRefused ?? []).some((r) => r.code === "MALFORMED_QUERY" || r.code === "INVALID_FIELD"));
  });

  test("a code the engine knew before 702 is noted too, and its reason is ours", async () => {
    const { done, log } = run(changed((r) => (soqlOf(r).startsWith("SELECT Id, Name, AccountId, OwnerId, StageName") ? errorBody(400, "QUERY_TIMEOUT", "Your query took too long on Acme_Secret__c") : null)));
    const { dir, run: finished } = await done;
    assert.ok(finished.readsRefused?.some((r) => r.read === "Opportunity" && r.code === "QUERY_TIMEOUT"));
    assert.ok(log.some((l) => l.includes("QUERY_TIMEOUT")));
    assert.ok(reasons(dir).some((r) => r.endsWith("Opportunity couldn't be read, because Salesforce refused this read.")));
    assert.doesNotMatch(customerText(dir), /QUERY_TIMEOUT|Acme_Secret__c|took too long/);
  });

  test("an object Salesforce doesn't have says so in our words, not Salesforce's", async () => {
    const { done } = run(changed((r) => (soqlOf(r).includes("FROM ForecastingQuota") ? errorBody(400, "INVALID_TYPE", "sObject type 'ForecastingQuota' is not supported.") : null)));
    const { dir } = await done;
    // INVALID_TYPE keeps the engine's own sentence from before 702: it quotes nothing of Salesforce's.
    assert.ok(reasons(dir).some((r) => r.endsWith("ForecastingQuota isn't available in this org, or to the connected user.")));
    assert.doesNotMatch(customerText(dir), /INVALID_TYPE|is not supported/);
  });

  test("the Metadata API and Tooling both refused: neither Salesforce message reaches the reason", async () => {
    const { done } = run(new Simulator({ readOnly: true, failQuery: (q) => (/FROM FlowDefinition\b/.test(q) ? { errorCode: "INSUFFICIENT_ACCESS", message: "Tooling says no to Acme_Secret__c" } : null) }));
    const { dir } = await done;
    const bundle = loadExtracts(dir).get("metadata") as Sourced<unknown>;
    assert.equal(bundle.status === "unavailable" && bundle.reason, "The Metadata API refused this connection, and reading the same through the Tooling API didn't work either: The same set-up through the Tooling API couldn't be read, because the person who connected Salesforce doesn't have permission to see it.");
    assert.doesNotMatch(customerText(dir), /Acme_Secret__c|requires a user with|INSUFFICIENT_ACCESS/);
  });
});

describe("what may pass is read again, never saved as unread (F4)", () => {
  test("a SOAP server fault on a retrieve stops the run, for the worker to deliver it again", async () => {
    const { done } = run(new Simulator({ failRetrieve: (body) => (body.includes("<met:name>Settings</met:name>") ? { faultCode: "Server", message: "An unexpected error occurred" } : null) }));
    await assert.rejects(done, (err: unknown) => err instanceof SalesforceError && err.code === "Server");
  });

  test("a fault code is kept as Salesforce cased it", async () => {
    const { done } = run(new Simulator({ failRetrieve: (body) => (body.includes("<met:name>Settings</met:name>") ? { faultCode: "UnexpectedFault", message: WORDING } : null) }));
    const { run: finished } = await done;
    assert.ok(finished.readsRefused?.some((r) => r.code === "UnexpectedFault"), JSON.stringify(finished.readsRefused));
  });

  test("an expired query cursor on a later page stops the run", async () => {
    const { done } = run(changed((r) => (/\/query\/[A-Za-z0-9]{15,18}-\d+$/.test(new URL(r.url).pathname) ? errorBody(400, "INVALID_QUERY_LOCATOR") : null), { pageSize: 200 }), { pageSize: 200 });
    await assert.rejects(done, (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_QUERY_LOCATOR");
  });

  test("an unknown code in a JSON 500 is an outage, and stops the run", async () => {
    const { done } = run(changed((r) => (soqlOf(r).includes("FROM ForecastingQuota") ? errorBody(500, UNKNOWN) : null)));
    await assert.rejects(done, (err: unknown) => err instanceof SalesforceError && err.status === 500 && err.code === UNKNOWN);
  });
});

describe("the loss reason field and lost deals alone (F5)", () => {
  test("a loss reason field that can't hold text says so, and its deals aren't left out", async () => {
    // The calibrated field made a number, as a consultant might pick by mistake.
    const field = loadCalibration().fields.lossReason!;
    const simulator = new Simulator();
    const numeric: Transport = {
      async send(request) {
        const res = await simulator.send(request);
        if (!new URL(request.url).pathname.endsWith("/sobjects/Opportunity/describe")) return res;
        const body = JSON.parse(res.text) as { fields: Array<{ name: string; type: string }> };
        for (const f of body.fields) if (f.name === field) f.type = "double";
        return { ...res, text: JSON.stringify(body) };
      },
    };
    const { done } = run(numeric);
    const { dir } = await done;
    const deals = loadExtracts(dir).get("opportunities") as OpportunityExtract;
    assert.equal(deals.leftOut, undefined);
    assert.equal(deals.lossReasonField, null);
    const rule = read<Finding[]>(dir, "findings.json").find((f) => f.ruleId === "dq.loss-reason-missing")!;
    assert.equal(rule.notMeasurableReason, `The loss reason field ${field} isn't a text or picklist field, so it can't hold a reason and isn't read.`);
  });

  test("bad rows among the lost deals alone make the deals not measurable, though few overall", async () => {
    const lost = (o: org.SyntheticOpportunity) => o.IsClosed && !o.IsWon;
    const lostCount = org.pipeline.opportunities.filter(lost).length;
    const bad = Math.floor(lostCount * 0.01) + 1;
    assert.ok(bad <= org.pipeline.opportunities.length * 0.01, "few enough overall to pass the overall share");
    const { done } = run(new Simulator({ records: badRows(bad, 0, lost) }));
    const deals = loadExtracts((await done).dir).get("opportunities") as OpportunityExtract;
    assert.equal(deals.opportunities.status, "unavailable");
  });
});

describe("a lost session or the daily limit anywhere stops the run (F6)", () => {
  const everySubresponse = (statusCode: number, errorCode: string) => {
    const simulator = new Simulator();
    return {
      async send(request: TransportRequest) {
        const res = await simulator.send(request);
        if (!new URL(request.url).pathname.endsWith("/composite/batch")) return res;
        const body = JSON.parse(res.text) as { results: unknown[] };
        body.results = body.results.map(() => ({ statusCode, result: [{ errorCode, message: "no" }] }));
        return { ...res, text: JSON.stringify(body) };
      },
    } satisfies Transport;
  };

  test("a lost session inside a composite answer fails the run", async () => {
    await assert.rejects(run(everySubresponse(401, "INVALID_SESSION_ID")).done, (err: unknown) => err instanceof SalesforceError && err.code === "INVALID_SESSION_ID");
  });

  test("the daily limit inside a composite answer stops the run where it can be resumed", async () => {
    assert.equal((await run(everySubresponse(403, "REQUEST_LIMIT_EXCEEDED")).done).run.status, "stopped_budget");
  });

  test("a 400 INVALID_SESSION_ID asks for the connection again, through the worker", async () => {
    const outcome = await automaticRun((r) => (soqlOf(r).includes("FROM Task ") ? errorBody(400, "INVALID_SESSION_ID") : null));
    assert.ok(outcome.detail.startsWith(NEEDS_RECONNECTION), outcome.detail);
  });
});

describe("the limits read at connect, outages and network failures (F7)", () => {
  const connectWith = (limits: () => Promise<TransportResponse>) =>
    SalesforceClient.connect(syntheticProvider, {
      budget: new ApiBudget({ stopAtShare: 0.5 }),
      sleep: async () => {},
      transport: { send: async (r) => (new URL(r.url).pathname.endsWith("/limits") ? limits() : new Simulator().send(r)) },
    });

  test("an outage on /limits is never taken as limits unread", { timeout: 30_000 }, async () => {
    await assert.rejects(connectWith(async () => errorBody(503, "SERVER_UNAVAILABLE")), (err: unknown) => err instanceof SalesforceError && err.status === 503);
  });

  test("a network failure on /limits is never taken as limits unread", { timeout: 30_000 }, async () => {
    await assert.rejects(connectWith(async () => {
      throw new TypeError("fetch failed");
    }), /fetch failed/);
  });

  test("no API says so without naming one edition", () => {
    assert.equal(NO_API_FOR_ORG, "This Salesforce org doesn't allow API access, so we can't read it. Some editions need API access added, and some don't offer it.");
  });
});
