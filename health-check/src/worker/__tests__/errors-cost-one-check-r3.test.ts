import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { isUnavailable, lastDelivery, SalesforceError } from "../../salesforce/client.ts";
import { refusalReason } from "../../extract/types.ts";
import { refusalSpans } from "../../salesforce/refusal-words.ts";
import { HEAVY_QUEUE, LAST_TRY_LOST_REASON, lostForGood, lostRecorder, startQueueWorker, type BullModule, type QueueJob, type QueueWorker } from "../bullmq.ts";
import { JobBusyError } from "../lock.ts";
import { READS_LOST } from "../run-job.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { Finding } from "../../schemas/index.ts";

// Review round 3 of decision 702: a run whose last delivery never reaches the
// engine is recorded, not left extracting; a whole Metadata API outage on the
// last delivery isn't released to a customer, and isn't called a refusal.

const ON = { on: true, orgs: new Set([ORG_ID.slice(0, 15)]) };
const soql = (r: TransportRequest): string => {
  try {
    return new URL(r.url).searchParams.get("q") ?? "";
  } catch {
    return "";
  }
};
const isSoap = (r: TransportRequest): boolean => Boolean(r.headers?.SOAPAction);
const body = (status: number, errorCode: string): TransportResponse => ({ status, header: () => null, text: JSON.stringify([{ errorCode, message: "SF WORDING" }]) });
const SERVER_FAULT: TransportResponse = {
  status: 500,
  header: () => null,
  text: `<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>soapenv:Server</faultcode><faultstring>An unexpected error occurred. ErrorId: 1</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`,
};
const MAINTENANCE_PAGE: TransportResponse = { status: 200, header: () => null, text: `<html><body>Down for maintenance</body></html>` };

/** BullMQ's Worker as far as the adapter sees it: the processor to call, and the listeners it set. */
class FakeWorker implements QueueWorker {
  static made: FakeWorker[] = [];
  readonly listeners = new Map<string, (...args: never[]) => void>();
  readonly name: string;
  readonly processor: (job: QueueJob) => Promise<unknown>;
  constructor(name: string, processor: (job: QueueJob) => Promise<unknown>) {
    this.name = name;
    this.processor = processor;
    FakeWorker.made.push(this);
  }
  on(event: string, listener: (...args: never[]) => void): this {
    this.listeners.set(event, listener);
    return this;
  }
  async close(): Promise<void> {}
}
class FakeQueue {
  async add() {}
  async close() {}
}
class Unrecoverable extends Error {
  constructor(message?: string) {
    super(message);
    this.name = "UnrecoverableError";
  }
}
const bull: BullModule = { Worker: FakeWorker as unknown as BullModule["Worker"], Queue: FakeQueue as unknown as BullModule["Queue"], UnrecoverableError: Unrecoverable };

type Mode = "automatic" | "consultant";
type Hook = (r: TransportRequest) => TransportResponse | null;

/**
 * A run on the heavy queue, delivered as BullMQ 5 delivers it (three tries):
 * the processor runs with attemptsMade the tries finished, and a throw raises
 * "failed" with attemptsMade counting the try just made, as moveToFailed does.
 */
async function world(hook: Hook, mode: Mode) {
  const inner: Transport = mode === "automatic" ? new AutomaticReplay() : new ReplayTransport(loadRecording());
  const dataTransport: Transport = { send: async (r) => hook(r) ?? inner.send(r) };
  const deps = baseDeps({ dataTransport, ...(mode === "automatic" ? { automatic: ON } : {}) });
  await deps.tokens.save(syntheticConnection());
  if (mode === "automatic") deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
  else deps.store.addRun(runInput({ run: consoleRun() }));
  FakeWorker.made = [];
  const lines: string[] = [];
  startQueueWorker(bull, createJobHandler(deps), { redisUrl: "redis://localhost:6379", concurrency: 2, log: (l) => lines.push(l), onLost: lostRecorder(deps.store) });
  const heavy = FakeWorker.made.find((w) => w.name === HEAVY_QUEUE)!;
  const failed = heavy.listeners.get("failed") as unknown as (job: QueueJob, err: Error) => void;
  const data = { kind: "run", runId: RUN_ID };
  /** One try, the nth of three. */
  const deliver = async (nth: number): Promise<unknown> => {
    const job: QueueJob = { id: `run-${RUN_ID}`, name: "health-check-run", data, attemptsMade: nth - 1, opts: { attempts: 3 } };
    try {
      return await heavy.processor(job);
    } catch (err) {
      failed({ ...job, attemptsMade: nth }, err as Error);
      // onLost runs unawaited, as it does under BullMQ.
      for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
      return { threw: (err as Error).message };
    }
  };
  const stored = () => deps.store.runs.get(RUN_ID)!;
  return { deps, deliver, stored, lines, flags: () => deps.store.flagsOf(RUN_ID) };
}

describe("a last delivery that never reaches the engine is recorded (round 3, finding 1)", () => {
  for (const [name, breakIt] of [
    ["the first reading of the run", (s: { loadRunJob: unknown }) => void (s.loadRunJob = async () => { throw new Error("connection terminated unexpectedly"); })],
    ["the claim", (s: { claimRun: unknown }) => void (s.claimRun = async () => { throw new Error("connection terminated unexpectedly"); })],
  ] as const) {
    test(`a retried failure, then ${name} throws on the last try: failed, with a reason and the run_failed flag`, async () => {
      const w = await world((r) => (soql(r).includes("FROM Task ") ? body(503, "SERVER_UNAVAILABLE") : null), "automatic");
      await w.deliver(1);
      // A try the queue will make again isn't the run failing.
      assert.equal(w.stored().run.status, "extracting");
      assert.deepEqual(w.flags(), []);
      breakIt(w.deps.store as never);
      await w.deliver(3);
      assert.equal(w.stored().run.status, "failed");
      assert.equal(w.stored().run.steps.find((s) => s.id === "worker")?.error, LAST_TRY_LOST_REASON);
      assert.deepEqual(w.flags(), ["run_failed"]);
    });
  }

  test("a try with more to come records nothing", async () => {
    const w = await world(() => null, "automatic");
    (w.deps.store as { loadRunJob: unknown }).loadRunJob = async () => {
      throw new Error("connection terminated unexpectedly");
    };
    await w.deliver(1);
    await w.deliver(2);
    assert.equal(w.stored().run.status, "extracting");
    assert.deepEqual(w.flags(), []);
  });

  test("a run already finished is never overwritten: released stays released, with no flag", async () => {
    const w = await world(() => null, "automatic");
    await w.deliver(3);
    assert.equal(w.stored().run.status, "released");
    // A stray last try of the same run that throws (BullMQ's view: failed for good).
    (w.deps.store as { loadRunJob: unknown }).loadRunJob = async () => {
      throw new Error("connection terminated unexpectedly");
    };
    await w.deliver(3);
    assert.equal(w.stored().run.status, "released");
    assert.deepEqual(w.flags(), []);
    assert.ok(w.lines.some((l) => /couldn't be recorded as failed/.test(l)));
  });

  test("in review stays in review", async () => {
    const w = await world(() => null, "consultant");
    w.deps.store.runs.get(RUN_ID)!.run.status = "in_review";
    const record = lostRecorder(w.deps.store);
    await assert.rejects(record({ kind: "run", runId: RUN_ID }, LAST_TRY_LOST_REASON));
    assert.equal(w.stored().run.status, "in_review");
  });

  test("which failures are lost for good: BullMQ 5 counts the try just made before it raises failed", () => {
    const at = (attemptsMade: number, attempts?: number) => ({ attemptsMade, opts: attempts === undefined ? {} : { attempts } });
    const plain = new Error("connection terminated unexpectedly");
    assert.equal(lostForGood(at(1, 3), plain), false);
    assert.equal(lostForGood(at(2, 3), plain), false);
    assert.equal(lostForGood(at(3, 3), plain), true);
    // BullMQ's default is attempts 0, which is one try.
    assert.equal(lostForGood(at(1, 0), plain), true);
    assert.equal(lostForGood(at(1), plain), true);
    // Another job holds the run, and its end is recorded; a job we couldn't read, or one that stalled, isn't this.
    assert.equal(lostForGood(at(3, 3), new JobBusyError(`run-${RUN_ID}`)), false);
    assert.equal(lostForGood(at(3, 3), new Unrecoverable("bad job")), false);
  });

  test("another job holding the run on the last try records nothing", async () => {
    const w = await world(() => null, "automatic");
    const failed = FakeWorker.made.find((x) => x.name === HEAVY_QUEUE)!.listeners.get("failed") as unknown as (job: QueueJob, err: Error) => void;
    failed({ id: `run-${RUN_ID}`, name: "health-check-run", data: { kind: "run", runId: RUN_ID }, attemptsMade: 3, opts: { attempts: 3 } }, new JobBusyError(`run-${RUN_ID}`));
    for (let i = 0; i < 5; i++) await new Promise((r) => setImmediate(r));
    assert.equal(w.stored().run.status, "extracting");
  });
});

const runJson = (dir: string): { readsRefused?: Array<{ step: string; code: string; lost?: true }> } => JSON.parse(readFileSync(join(dir, RUN_ID, "run.json"), "utf8"));

describe("a whole Metadata API outage on the last delivery (round 3, finding 2)", () => {
  for (const [name, hook] of [
    ["a SOAP server fault on every Metadata API call (B1)", (r: TransportRequest) => (isSoap(r) ? SERVER_FAULT : null)],
    ["a maintenance page on every Metadata API call (B2)", (r: TransportRequest) => (isSoap(r) ? MAINTENANCE_PAGE : null)],
  ] as const) {
    test(`${name}: an automatic run isn't released; it fails with the run_failed flag, in plain words`, async () => {
      const w = await world(hook, "automatic");
      await w.deliver(1);
      assert.equal(w.stored().run.status, "extracting");
      await w.deliver(3);
      assert.equal(w.stored().run.status, "failed");
      assert.equal(w.stored().output ?? null, null);
      assert.equal(w.stored().run.steps.find((s) => s.id === "worker")?.error, READS_LOST);
      assert.deepEqual(w.flags(), ["run_failed"]);
      const lost = (runJson(w.deps.runsDir).readsRefused ?? []).filter((r) => r.lost);
      assert.ok(lost.some((r) => r.step === "metadata"), JSON.stringify(lost));
    });

    test(`${name}: a consultant-led run goes on to review, and says Salesforce didn't answer, not that it refused`, async () => {
      const w = await world(hook, "consultant");
      await w.deliver(1);
      await w.deliver(3);
      assert.ok(["awaiting_manual", "in_review"].includes(w.stored().run.status), w.stored().run.status);
      const findings = (w.stored().output as { findings: Finding[] }).findings;
      const reasons = findings.map((f) => f.notMeasurableReason ?? "").filter((r) => /Metadata API/.test(r));
      assert.ok(reasons.length > 0);
      for (const r of reasons) {
        assert.doesNotMatch(r, /refused/, r);
        assert.match(r, /The org's set-up from the Metadata API couldn't be read, because Salesforce didn't answer as it should\.$/, r);
      }
    });
  }
});

describe("a server fault or a malformed answer costs one read only on a retrieve (round 3, finding 2a)", () => {
  const onLast = (err: SalesforceError): boolean => lastDelivery.run(true, () => isUnavailable(err));
  test("on the retrieve and its status checks, on the last delivery", () => {
    for (const operation of ["retrieve", "checkRetrieveStatus"]) {
      assert.equal(onLast(new SalesforceError(500, "Server", "x", { fault: true, operation })), true, operation);
      assert.equal(onLast(new SalesforceError(200, "BAD_SOAP_RESPONSE", "x", { operation })), true, operation);
    }
  });
  test("never on another SOAP call, or with no operation known, which stops the run as an outage does", () => {
    for (const operation of ["listMetadata", "readMetadata", undefined]) {
      assert.equal(onLast(new SalesforceError(500, "Server", "x", { fault: true, ...(operation ? { operation } : {}) })), false, String(operation));
      assert.equal(onLast(new SalesforceError(200, "BAD_SOAP_RESPONSE", "x", operation ? { operation } : {})), false, String(operation));
    }
  });
});

describe("refused only when Salesforce said no (round 3, finding 2b)", () => {
  const what = "The org's set-up from the Metadata API";
  for (const [err, ending] of [
    [new SalesforceError(200, "RETRIEVE_TIMEOUT", "x"), "Salesforce didn't answer as it should."],
    [new SalesforceError(500, "Server", "x", { fault: true, operation: "retrieve" }), "Salesforce didn't answer as it should."],
    [new SalesforceError(200, "BAD_SOAP_RESPONSE", "x", { operation: "retrieve" }), "Salesforce didn't answer as it should."],
    [new SalesforceError(400, "INVALID_QUERY_LOCATOR", "x"), "Salesforce didn't answer as it should."],
    [new SalesforceError(400, "QUERY_TIMEOUT", "x"), "Salesforce didn't answer as it should."],
    [new SalesforceError(503, "SERVER_UNAVAILABLE", "x"), "Salesforce didn't answer as it should."],
    [new SalesforceError(403, "INSUFFICIENT_ACCESS", "x"), "the person who connected Salesforce doesn't have permission to see it."],
    [new SalesforceError(400, "FUNCTIONALITY_NOT_ENABLED", "x"), "it isn't available in this Salesforce edition or isn't switched on."],
    [new SalesforceError(400, "MALFORMED_QUERY", "x"), "Salesforce refused this read."],
  ] as const) {
    test(`${err.code}: "…because ${ending}"`, () => {
      const sentence = refusalReason(err, what);
      assert.equal(sentence, `${what} couldn't be read, because ${ending}`);
      // Known as ours, so an automatic run's wording guard passes it.
      assert.deepEqual(refusalSpans(sentence), []);
    });
  }
  test("an object named in the new sentence is marked as the org's", () => {
    assert.deepEqual(refusalSpans("Project__c couldn't be read, because Salesforce didn't answer as it should."), [{ start: 0, end: "Project__c".length }]);
  });
});
