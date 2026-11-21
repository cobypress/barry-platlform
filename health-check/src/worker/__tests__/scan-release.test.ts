import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { utimesSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { beginAuthorisation, createConnectLink } from "../../connect/link.ts";
import type { TokenStore } from "../../salesforce/token-store.ts";
import type { OAuthTransport, Transport, TransportResponse } from "../../salesforce/transport.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { lostRecorder } from "../bullmq.ts";
import { sealConnectSecret } from "../connect-crypto.ts";
import { withJobLock } from "../lock.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { letGoOfLostRun, REVOKE_TRIES, REVOKE_WAITS_MS, SCAN_CONNECTION_HARD_MAX_MS, SCAN_CONNECTION_MAX_MS, SCAN_SWEEP_MAX, SCAN_SWEEP_READS, SCAN_UNREADABLE_MAX_MS, sweepScanConnections } from "../scan-release.ts";
import { DISCONNECT_STEP, ORG_AWAITING_CONNECT, SCAN_REVOKE_NOT_CONFIRMED, type RunJobInput } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, CONNECTION, consoleRun, oauthClient, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// DECISIONS.md 652: a free scan gives the org's access back however its run
// ends. Every end of a scan's run job lets go through one function: revoked at
// Salesforce while our copy is still held, then our copy deleted. A paying
// customer's connection is never let go, by a run or by the sweep.

const job = { kind: "run", runId: RUN_ID };
const once = { final: false };
const last = { final: true };

// Recognisable, so a log line, outcome or record that quoted one would show.
const PLANTED_ACCESS = "00D000000000001!PLANTEDACCESSTOKENxyzzy0001";
const PLANTED_REFRESH = "5AepPLANTEDREFRESHTOKENxyzzy0001";

type Answer = number | "network";

/** Salesforce's token and revoke endpoints: each revoke says whether our copy was still held when it came, and answers as told. Refreshes are refused. */
class Endpoint implements OAuthTransport {
  readonly revokes: Array<{ token: string; held: boolean }> = [];
  answers: Answer[] = [];
  tokens?: TokenStore;
  name = CONNECTION;
  async postForm(url: string, form: URLSearchParams): Promise<TransportResponse> {
    if (new URL(url).pathname === "/services/oauth2/revoke") {
      this.revokes.push({ token: form.get("token") ?? "", held: this.tokens?.has(this.name) ?? false });
      const answer = this.answers.shift() ?? 200;
      if (answer === "network") throw new TypeError(`fetch failed: ECONNRESET ${form.get("token")}`);
      // A 5xx as a maintenance page, not JSON, so its code is the HTTP status.
      if (answer >= 500) return { status: answer, header: () => null, text: "<html>Service unavailable</html>" };
      return { status: answer, header: () => null, text: answer === 200 ? "" : JSON.stringify({ error: "unsupported_token_type", error_description: `refused ${form.get("token")}` }) };
    }
    return { status: 400, header: () => null, text: JSON.stringify({ error: "invalid_grant", error_description: "expired access/refresh token" }) };
  }
}

/** Every data request refused as an expired session, so the client asks for a refresh, which is refused too. */
class Expired implements Transport {
  async send(): Promise<TransportResponse> {
    return { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) };
  }
}

/** Fails like a dropped network after `after` requests. */
class Dropping implements Transport {
  readonly #inner: Transport;
  #left: number;
  constructor(inner: Transport, after: number) {
    this.#inner = inner;
    this.#left = after;
  }
  async send(request: Parameters<Transport["send"]>[0]): Promise<TransportResponse> {
    if (this.#left-- <= 0) throw new Error("fetch failed: ECONNRESET");
    return this.#inner.send(request);
  }
}

const LINK_ID = "00000000-0000-4000-8000-000000000652";
const STATE_HASH = "a".repeat(43);

/** A console link whose run is `runId`, added as a run of `product` when it isn't there. */
function linkTo(s: { deps: { store: MemoryWorkerStore } }, linkId: string, runId: string, product: "scan" | "barometer" = "scan", connectionName = "linked"): void {
  if (!s.deps.store.runs.has(runId)) s.deps.store.addRun(runInput({ run: consoleRun({ id: runId, product }), connectionName }));
  const { link } = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName, createdBy: "consultant@example.com" });
  s.deps.store.addLink({ ...link, id: linkId }, "used", { pending: null, code: null }, runId);
}

const scanRun = (overrides: Parameters<typeof consoleRun>[0] = {}) => consoleRun({ scope: "essentials", product: "scan", ...overrides });

async function setup(options: { input?: RunJobInput | null; data?: Transport; connection?: Parameters<typeof syntheticConnection>[0]; deps?: Parameters<typeof baseDeps>[0] } = {}) {
  const endpoint = new Endpoint();
  const waits: number[] = [];
  const deps = baseDeps({ dataTransport: options.data ?? new ReplayTransport(loadRecording()), oauthTransport: endpoint, sleep: async (ms) => void waits.push(ms), ...options.deps });
  endpoint.tokens = deps.tokens;
  if (options.input !== null) deps.store.addRun(options.input ?? runInput({ run: scanRun() }));
  await deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, accessToken: PLANTED_ACCESS, refreshToken: PLANTED_REFRESH, ...options.connection }));
  return { deps, endpoint, waits, handle: createJobHandler(deps) };
}

type Setup = Awaited<ReturnType<typeof setup>>;

/** Let go: one revoke of the refresh token, sent while our copy was still held, and then nothing held. */
function assertLetGo(s: Setup): void {
  assert.equal(s.endpoint.revokes.length, 1, "one revoke");
  assert.deepEqual(s.endpoint.revokes[0], { token: PLANTED_REFRESH, held: true }, "the refresh token, revoked before our copy went");
  assert.equal(s.deps.tokens.has(CONNECTION), false, "our copy is gone");
  assert.deepEqual(s.deps.tokens.list(), []);
}

function assertKept(s: Setup): void {
  assert.equal(s.endpoint.revokes.length, 0, "nothing revoked");
  assert.equal(s.deps.tokens.has(CONNECTION), true, "the connection is kept");
}

/** No planted token in any log line, outcome, store write or run record. */
function assertNoToken(s: Setup, ...more: unknown[]): void {
  const seen = [s.deps.lines.join("\n"), JSON.stringify(s.deps.store.writes), JSON.stringify([...s.deps.store.runs.values()]), JSON.stringify(more)].join("\n");
  for (const token of [PLANTED_ACCESS, PLANTED_REFRESH, "PLANTED"]) assert.ok(!seen.includes(token), `a token reached what's seen: ${token}`);
}

describe("every end of a free scan's run lets go of its connection (DECISIONS.md 652)", () => {
  test("done: the teaser saved, then revoked and deleted", async () => {
    const s = await setup();
    const outcome = await s.handle(job, once);
    assert.equal(outcome.outcome, "done");
    assert.ok(s.deps.store.scanResults.has(RUN_ID));
    assertLetGo(s);
    assert.ok(s.deps.lines.includes(`Scan ${RUN_ID}'s connection is revoked.`));
    assertNoToken(s, outcome);
  });

  test("failed before reading: the run can't start (no calibration)", async () => {
    const s = await setup({ input: runInput({ run: scanRun(), calibration: null }) });
    const outcome = await s.handle(job, once);
    assert.equal(outcome.outcome, "failed");
    assertLetGo(s);
    assertNoToken(s, outcome);
  });

  test("refused: the connection is for a different org", async () => {
    const s = await setup({ input: runInput({ run: scanRun({ orgId: "00D0000000000ZZAAA" }) }) });
    const outcome = await s.handle(job, once);
    assert.match(outcome.detail, /different org/);
    assertLetGo(s);
  });

  test("refused: the app it was made with isn't set up on this worker", async () => {
    const s = await setup({ connection: { clientId: "3MVG9not.set.up" } });
    const outcome = await s.handle(job, once);
    assert.match(outcome.detail, /no longer set up for/);
    assertLetGo(s);
  });

  test("refused: Salesforce no longer accepts it (needs reconnection), on a delivery that isn't the last", async () => {
    const s = await setup({ data: new Expired() });
    const outcome = await s.handle(job, once);
    assert.match(outcome.detail, /^Needs reconnection:/);
    assertLetGo(s);
    assertNoToken(s, outcome);
  });

  test("stopped at the API budget", async () => {
    const s = await setup({ data: new Simulator({ used: 60_000 }) });
    assert.equal((await s.handle(job, once)).outcome, "stopped_budget");
    assertLetGo(s);
  });

  test("an error on the last delivery fails the run, and lets go", async () => {
    const s = await setup({ data: new Dropping(new ReplayTransport(loadRecording()), 3) });
    const outcome = await s.handle(job, last);
    assert.equal(outcome.outcome, "failed");
    assertLetGo(s);
    assertNoToken(s, outcome);
  });

  test("results that couldn't be saved on the last delivery", async () => {
    const s = await setup();
    s.deps.store.saveRunOutput = async () => Promise.reject(new Error("deadlock detected"));
    assert.equal((await s.handle(job, last)).outcome, "failed");
    assertLetGo(s);
  });

  test("the run closed by the console before the job (skipped)", async () => {
    const s = await setup({ input: runInput({ run: scanRun({ status: "in_review" }) }) });
    assert.equal((await s.handle(job, once)).outcome, "skipped");
    assertLetGo(s);
  });

  test("the run gone, cancelled or deleted by the console: the connections marked for it go, and a customer's beside it stays", async () => {
    const s = await setup({ input: null, connection: { scan: { runId: RUN_ID } } });
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "customer-co" }));
    assert.equal((await s.handle(job, once)).outcome, "skipped");
    assert.deepEqual(s.endpoint.revokes, [{ token: PLANTED_REFRESH, held: true }]);
    assert.deepEqual(s.deps.tokens.list(), ["customer-co"], "the scan's is gone, and the customer's untouched");
  });

  test("a store that throws on the last delivery's claim: the error goes back to the queue after letting go", async () => {
    const s = await setup();
    s.deps.store.claimRun = async () => Promise.reject(new Error("connection terminated"));
    await assert.rejects(s.handle(job, last), /connection terminated/);
    assertLetGo(s);
  });

  test("a scan BullMQ gave up on without running it (stalled too often) is failed and lets go", async () => {
    const s = await setup();
    await lostRecorder(s.deps.store, () => new Date(), (runId) => letGoOfLostRun(runId, s.deps))(job, "lost");
    assert.equal(s.deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assertLetGo(s);
  });
});

describe("a delivery that will be followed by another keeps the connection, and the last lets go", () => {
  test("an error on an early delivery goes back to the queue with the connection kept; the next delivery resumes through it and lets go", async () => {
    const s = await setup({ data: new Dropping(new ReplayTransport(loadRecording()), 5) });
    await assert.rejects(s.handle(job, once), /ECONNRESET/);
    assertKept(s);
    s.deps.dataTransport = new ReplayTransport(loadRecording());
    assert.equal((await s.handle(job, last)).outcome, "done");
    assert.ok(s.deps.store.scanResults.has(RUN_ID), "the redelivery worked");
    assertLetGo(s);
  });

  test("a delivery that finds another holding the run leaves the connection to it", async () => {
    const s = await setup();
    await withJobLock(s.deps.locksDir, `run-${RUN_ID}`, async () => {
      await assert.rejects(s.handle(job, last), /Another job/);
      assertKept(s);
    });
  });
});

describe("paying customers' connections are never let go by a run", () => {
  for (const product of ["barometer", "pulse"] as const) {
    test(`a ${product} run that fails on its last delivery keeps its connection`, async () => {
      const s = await setup({ input: runInput({ run: consoleRun({ product }) }), data: new Dropping(new ReplayTransport(loadRecording()), 3) });
      assert.equal((await s.handle(job, last)).outcome, "failed");
      assertKept(s);
    });
  }

  test("a health check that BullMQ gave up on keeps its connection", async () => {
    const s = await setup({ input: runInput() });
    await lostRecorder(s.deps.store, () => new Date(), (runId) => letGoOfLostRun(runId, s.deps))(job, "lost");
    assertKept(s);
  });
});

describe("when Salesforce doesn't confirm the revoke", () => {
  test("our copy still goes, the run is marked not confirmed, one line names the run and the error's class, and it's tried 3 times in all", async () => {
    const s = await setup();
    s.endpoint.answers = [503, 503, 503];
    const outcome = await s.handle(job, once);
    assert.equal(outcome.outcome, "done");
    assert.equal(s.deps.tokens.has(CONNECTION), false, "our copy is deleted");
    assert.equal(s.endpoint.revokes.length, REVOKE_TRIES);
    assert.ok(s.endpoint.revokes[0]!.held && !s.endpoint.revokes[1]!.held, "tried first while held, then from memory");
    assert.ok(s.endpoint.revokes.every((r) => r.token === PLANTED_REFRESH));
    assert.deepEqual(s.waits, REVOKE_WAITS_MS);
    const step = s.deps.store.runs.get(RUN_ID)!.run.steps.find((st) => st.id === DISCONNECT_STEP);
    assert.deepEqual({ status: step?.status, error: step?.error }, { status: "failed", error: SCAN_REVOKE_NOT_CONFIRMED });
    const lines = s.deps.lines.filter((l) => /revok/i.test(l));
    assert.deepEqual(lines, [`Salesforce didn't confirm revoking scan ${RUN_ID}'s connection, so the worker deleted its copy: HTTP_503.`]);
    assertNoToken(s, outcome);
  });

  test("a network failure is logged by its class alone, never its message, which here quotes the token", async () => {
    const s = await setup();
    s.endpoint.answers = ["network", "network", "network"];
    await s.handle(job, once);
    assert.ok(s.deps.lines.includes(`Salesforce didn't confirm revoking scan ${RUN_ID}'s connection, so the worker deleted its copy: TypeError.`), s.deps.lines.join("\n"));
    assert.equal(s.endpoint.revokes.length, REVOKE_TRIES);
    assertNoToken(s);
  });

  test("a refusal (a 4xx) is Salesforce's answer, and isn't tried again", async () => {
    const s = await setup();
    s.endpoint.answers = [400];
    await s.handle(job, once);
    assert.equal(s.endpoint.revokes.length, 1);
    assert.deepEqual(s.waits, []);
    assert.equal(s.deps.tokens.has(CONNECTION), false);
    assert.ok(s.deps.store.runs.get(RUN_ID)!.run.steps.some((st) => st.id === DISCONNECT_STEP));
    assertNoToken(s);
  });

  test("a later try that succeeds takes the mark away and says so", async () => {
    const s = await setup();
    s.endpoint.answers = [503, 200];
    await s.handle(job, once);
    assert.equal(s.endpoint.revokes.length, 2);
    assert.deepEqual(s.waits, [REVOKE_WAITS_MS[0]]);
    assert.ok(!s.deps.store.runs.get(RUN_ID)!.run.steps.some((st) => st.id === DISCONNECT_STEP));
    assert.ok(s.deps.lines.includes(`Scan ${RUN_ID}'s connection is revoked, on try 2 of ${REVOKE_TRIES}.`));
    assert.deepEqual(s.deps.store.writes.filter((w) => w.method === "noteScanRevoke").map((w) => w.detail), ["not confirmed", "confirmed"]);
  });

  test("a connection that can't be read is kept for the sweep, logged by its name and nothing of its content, and nothing revoked or marked", async () => {
    const s = await setup();
    writeFileSync(join(s.deps.tokens.dir, `${CONNECTION}.json`), "not a connection PLANTED", { mode: 0o600 });
    assert.equal((await s.handle(job, once)).outcome, "failed");
    assert.equal(s.deps.tokens.has(CONNECTION), true, "kept, to try again");
    assert.equal(s.endpoint.revokes.length, 0);
    assert.ok(!s.deps.store.runs.get(RUN_ID)!.run.steps.some((st) => st.id === DISCONNECT_STEP));
    assert.ok(s.deps.lines.includes(`Scan ${RUN_ID}: the stored connection "${CONNECTION}" couldn't be read, so it is kept for the scan sweep to try again.`));
    assertNoToken(s);
  });
});

describe("the job lets go only of a connection that reads as its own scan's", () => {
  test("the run names a connection marked for another scan: left, and said", async () => {
    const s = await setup({ connection: { scan: { runId: "hc-2026-11-21-00000077" } } });
    await s.handle(job, once);
    assertKept(s);
    assert.ok(s.deps.lines.includes(`Scan ${RUN_ID}: the stored connection "${CONNECTION}" isn't marked as this scan's, so it was left alone.`));
  });

  test("the run names a connection whose link's run is a health check: left", async () => {
    const s = await setup({ connection: { fromLink: { linkId: LINK_ID, stateHash: STATE_HASH } } });
    linkTo(s, LINK_ID, "hc-2026-11-21-00000088", "barometer");
    await s.handle(job, once);
    assertKept(s);
  });

  test("the run names a connection stored before marks, whose link's run is this scan: let go", async () => {
    const s = await setup({ connection: { fromLink: { linkId: LINK_ID, stateHash: STATE_HASH } } });
    linkTo(s, LINK_ID, RUN_ID);
    await s.handle(job, once);
    assertLetGo(s);
  });

  test("the run names no connection: the ones marked for it go", async () => {
    const s = await setup({ input: runInput({ run: scanRun(), connectionName: null }), connection: { scan: { runId: RUN_ID } } });
    assert.equal((await s.handle(job, once)).outcome, "failed");
    assertLetGo(s);
  });
});

describe("the sweep of scan connections that slipped through (DECISIONS.md 652)", () => {
  const now = new Date("2026-11-21T12:00:00.000Z");
  const ago = (ms: number) => new Date(now.getTime() - ms).toISOString();
  const OLD = ago(SCAN_CONNECTION_MAX_MS + 60_000);

  async function sweepSetup() {
    const s = await setup({ input: null, deps: { now: () => now } });
    s.deps.tokens.delete(CONNECTION);
    return s;
  }

  test("lets go of an old scan connection whose run no job holds, and never touches a customer's of the same age", async () => {
    const s = await sweepSetup();
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "scan-orphan", connectedAt: OLD, accessToken: PLANTED_ACCESS, refreshToken: PLANTED_REFRESH, scan: { runId: "hc-2026-11-21-00000009" } }));
    // A paying customer's, as old, with the scan-like name a customer reference could give it: no mark, so never a candidate.
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "scan-ltd-0a1b2c3d", connectedAt: OLD, refreshToken: "5AepCUSTOMERREFRESH0000000000001" }));
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "customer-co", connectedAt: ago(400 * 86_400_000) }));
    s.endpoint.name = "scan-orphan";
    const outcome = await sweepScanConnections(s.deps);
    assert.equal(outcome.letGo, 1);
    assert.deepEqual(s.endpoint.revokes, [{ token: PLANTED_REFRESH, held: true }], "only the scan's, revoked before our copy went");
    assert.deepEqual(s.deps.tokens.list(), ["customer-co", "scan-ltd-0a1b2c3d"]);
    assert.ok(s.deps.lines.some((l) => l.includes("hc-2026-11-21-00000009")));
    assertNoToken(s, outcome);
  });

  test("a young scan connection, or one whose run a job holds, is left for later", async () => {
    const s = await sweepSetup();
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "scan-young", connectedAt: ago(SCAN_CONNECTION_MAX_MS - 60_000), scan: { runId: "hc-2026-11-21-00000001" } }));
    await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: "scan-busy", connectedAt: OLD, scan: { runId: "hc-2026-11-21-00000002" } }));
    const outcome = await withJobLock(s.deps.locksDir, "run-hc-2026-11-21-00000002", () => sweepScanConnections(s.deps));
    assert.deepEqual({ letGo: outcome.letGo, busy: outcome.busy }, { letGo: 0, busy: 1 });
    assert.deepEqual(s.deps.tokens.list(), ["scan-busy", "scan-young"]);
    assert.equal(s.endpoint.revokes.length, 0);
  });

  const conn = (name: string, connectedAt: string, extra: Parameters<typeof syntheticConnection>[0] = {}) => syntheticConnection({ instanceUrl: FAKE.instanceUrl, name, connectedAt, ...extra });

  test("a connection stored before marks is a scan's only when its link's run reads as product scan: let go then, and left for a customer's link, a link with no run, or a store that can't say", async () => {
    const s = await sweepSetup();
    const link = (n: number) => `00000000-0000-4000-8000-00000000000${n}`;
    await s.deps.tokens.save(conn("old-scan", OLD, { fromLink: { linkId: link(1), stateHash: STATE_HASH } }));
    await s.deps.tokens.save(conn("old-customer", OLD, { fromLink: { linkId: link(2), stateHash: STATE_HASH } }));
    await s.deps.tokens.save(conn("old-nowhere", OLD, { fromLink: { linkId: link(3), stateHash: STATE_HASH } }));
    linkTo(s, link(1), "hc-2026-11-21-00000101", "scan", "unnamed-a");
    linkTo(s, link(2), "hc-2026-11-21-00000102", "barometer", "unnamed-b");
    s.deps.store.runs.get("hc-2026-11-21-00000101")!.run.status = "failed";
    s.endpoint.name = "old-scan";
    const outcome = await sweepScanConnections(s.deps);
    assert.equal(outcome.letGo, 1);
    assert.deepEqual(s.deps.tokens.list(), ["old-customer", "old-nowhere"]);
    // A store that throws on the link: nothing is a scan's by it.
    const t = await sweepSetup();
    await t.deps.tokens.save(conn("old-scan", OLD, { fromLink: { linkId: link(1), stateHash: STATE_HASH } }));
    linkTo(t, link(1), "hc-2026-11-21-00000101", "scan", "unnamed-a");
    t.deps.store.scanRunOfLink = async () => Promise.reject(new Error("the database is down"));
    assert.equal((await sweepScanConnections(t.deps)).letGo, 0);
    assert.deepEqual(t.deps.tokens.list(), ["old-scan"]);
  });

  test("a scan whose run is still queued or running is left until 24 hours; ended, gone or older, it goes; a status that can't be read leaves it", async () => {
    const s = await sweepSetup();
    const id = (n: number) => `hc-2026-11-21-0000020${n}`;
    const add = (n: number, status: "extracting" | "analysing" | "failed" | "in_review", age: number) => {
      s.deps.store.addRun(runInput({ run: scanRun({ id: id(n), status }), connectionName: `s-${n}` }));
      return s.deps.tokens.save(conn(`s-${n}`, ago(age), { scan: { runId: id(n) } }));
    };
    await add(1, "extracting", SCAN_CONNECTION_MAX_MS + 60_000);
    await add(2, "analysing", SCAN_CONNECTION_MAX_MS + 60_000);
    await add(3, "failed", SCAN_CONNECTION_MAX_MS + 60_000);
    await add(4, "in_review", SCAN_CONNECTION_MAX_MS + 60_000);
    await add(5, "extracting", SCAN_CONNECTION_HARD_MAX_MS + 60_000);
    const outcome = await sweepScanConnections(s.deps);
    assert.deepEqual({ letGo: outcome.letGo, waiting: outcome.waiting }, { letGo: 3, waiting: 2 });
    assert.deepEqual(s.deps.tokens.list(), ["s-1", "s-2"]);
    s.deps.store.runState = async () => Promise.reject(new Error("the database is down"));
    assert.equal((await sweepScanConnections(s.deps)).letGo, 0);
    assert.deepEqual(s.deps.tokens.list(), ["s-1", "s-2"]);
  });

  test("a scan's connection that can't be read is kept to try again, and deleted, marked not confirmed, once more than 7 days old; one no scan run names is never touched", async () => {
    const s = await sweepSetup();
    s.deps.store.addRun(runInput({ run: scanRun({ id: "hc-2026-11-21-00000301", status: "failed" }), connectionName: "s-young" }));
    s.deps.store.addRun(runInput({ run: scanRun({ id: "hc-2026-11-21-00000302", status: "failed" }), connectionName: "s-old" }));
    for (const name of ["s-young", "s-old", "unknown-old"]) {
      await s.deps.tokens.save(conn(name, OLD));
      writeFileSync(join(s.deps.tokens.dir, `${name}.json`), "not a connection", { mode: 0o600 });
    }
    const weekAgo = (now.getTime() - SCAN_UNREADABLE_MAX_MS - 60_000) / 1000;
    for (const name of ["s-old", "unknown-old"]) utimesSync(join(s.deps.tokens.dir, `${name}.json`), weekAgo, weekAgo);
    const outcome = await sweepScanConnections(s.deps);
    assert.deepEqual({ letGo: outcome.letGo, unreadable: outcome.unreadable }, { letGo: 1, unreadable: 2 });
    assert.deepEqual(s.deps.tokens.list(), ["s-young", "unknown-old"]);
    assert.ok(s.deps.store.runs.get("hc-2026-11-21-00000302")!.run.steps.some((st) => st.id === DISCONNECT_STEP));
    assert.ok(s.deps.lines.some((l) => l.includes(`"s-young" couldn't be read, and is kept to try again`)));
    assert.equal(s.endpoint.revokes.length, 0);
  });

  test(`a connection only customers' runs name is never decrypted; those no run names are read ${SCAN_SWEEP_READS} a sweep, in rotation`, async () => {
    const s = await sweepSetup();
    s.deps.store.addRun(runInput({ run: consoleRun({ id: "hc-2026-11-21-00000401", product: "barometer" }), connectionName: "customer-co" }));
    await s.deps.tokens.save(conn("customer-co", OLD));
    for (let i = 0; i < SCAN_SWEEP_READS + 5; i++) await s.deps.tokens.save(conn(`u-${String(i).padStart(3, "0")}`, OLD));
    const read: string[] = [];
    const load = s.deps.tokens.load.bind(s.deps.tokens);
    s.deps.tokens.load = async (name: string) => (read.push(name), load(name));
    const state = { cursor: 0 };
    const first = await sweepScanConnections(s.deps, state);
    assert.equal(first.customers, 1);
    assert.equal(first.checked, SCAN_SWEEP_READS);
    await sweepScanConnections(s.deps, state);
    assert.ok(!read.includes("customer-co"), "a customer's is never decrypted");
    assert.equal(new Set(read).size, SCAN_SWEEP_READS + 5, "every other file is reached over two sweeps");
  });

  test(`lets go of at most ${SCAN_SWEEP_MAX} a sweep, and the next takes the rest`, async () => {
    const s = await sweepSetup();
    for (let i = 0; i < SCAN_SWEEP_MAX + 3; i++) await s.deps.tokens.save(syntheticConnection({ instanceUrl: FAKE.instanceUrl, name: `scan-${String(i).padStart(3, "0")}`, connectedAt: OLD, scan: { runId: `hc-2026-11-21-${String(i).padStart(8, "0")}` } }));
    const first = await sweepScanConnections(s.deps);
    assert.deepEqual({ letGo: first.letGo, left: first.left }, { letGo: SCAN_SWEEP_MAX, left: 3 });
    assert.equal(s.deps.tokens.list().length, 3);
    assert.equal((await sweepScanConnections(s.deps)).letGo, 3);
    assert.deepEqual(s.deps.tokens.list(), []);
  });
});

describe("the connect job marks a scan's connection, and only a scan's", () => {
  async function connect(product: "scan" | "barometer") {
    const fake = new FakeTokenEndpoint();
    const deps = baseDeps({ oauthTransport: fake, dataTransport: new Expired(), now: () => new Date() });
    const { link } = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: "acme", createdBy: "consultant@example.com" });
    const begun = beginAuthorisation(link, oauthClient);
    const code = fake.issueCode(begun.authorisationUrl);
    deps.store.addRun(runInput({ run: consoleRun({ orgId: ORG_AWAITING_CONNECT, product }) }));
    deps.store.addLink(link, "pending", { pending: sealConnectSecret(JSON.stringify(begun.pending), deps.connectKey, link.id), code: sealConnectSecret(code, deps.connectKey, link.id) }, RUN_ID);
    assert.equal((await createJobHandler(deps)({ kind: "connect", linkId: link.id }, last)).outcome, "connected");
    return (await deps.tokens.load("acme")).scan;
  }

  test("a scan's link: marked with its run", async () => {
    assert.deepEqual(await connect("scan"), { runId: RUN_ID });
  });

  test("a health check's link: no mark", async () => {
    assert.equal(await connect("barometer"), undefined);
  });
});

describe("the Postgres store's mark of a revoke not confirmed", () => {
  class Pool implements SqlPool {
    readonly statements: Array<{ text: string; values: unknown[] }> = [];
    async query(text: string, values: unknown[] = []): Promise<SqlResult> {
      this.statements.push({ text, values });
      return { rows: [], rowCount: 1 };
    }
    async connect(): Promise<SqlClient & { release(): void }> {
      return { query: (t, v) => this.query(t, v), release: () => {} };
    }
  }

  test("not confirmed: the run's disconnect step, replaced, with the fixed sentence, whatever the run's status", async () => {
    const pool = new Pool();
    await new PostgresWorkerStore(pool).noteScanRevoke("r1", false, "2026-11-21T12:00:00.000Z");
    const texts = pool.statements.map((s) => s.text);
    assert.ok(!texts.some((t) => t.includes("FOR UPDATE")), "not held to an open run: a scan lets go after its results");
    assert.ok(texts.includes(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1 AND "id" = $2`));
    const insert = pool.statements.find((s) => s.text.startsWith(`INSERT INTO "HealthCheckRunStep"`))!;
    assert.deepEqual(insert.values, ["r1", DISCONNECT_STEP, 1, "failed", 0, "2026-11-21T12:00:00.000Z", SCAN_REVOKE_NOT_CONFIRMED]);
  });

  test("confirmed: the step taken away, nothing put in", async () => {
    const pool = new Pool();
    await new PostgresWorkerStore(pool).noteScanRevoke("r1", true, "2026-11-21T12:00:00.000Z");
    assert.ok(pool.statements.some((s) => s.text.startsWith(`DELETE FROM "HealthCheckRunStep"`) && s.values[1] === DISCONNECT_STEP));
    assert.ok(!pool.statements.some((s) => s.text.startsWith("INSERT")));
  });
});
