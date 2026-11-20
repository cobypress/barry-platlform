import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { beginAuthorisation, createConnectLink } from "../../connect/link.ts";
import { orgStages, StoreError, type OrgStage } from "../../connect/org-stages.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { ConnectLink } from "../../schemas/connect.ts";
import type { Run } from "../../schemas/index.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { sealConnectSecret } from "../connect-crypto.ts";
import { LAST_SCAN_SQL, PostgresWorkerStore, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { ORG_AWAITING_CONNECT } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, oauthClient, RUN_ID, runInput } from "./helpers.ts";

// Two checks at connect: one free scan per org every 30 days (DECISIONS.md 610),
// and the org's opportunity stages read for the calibration screen (611).

const NAME = "acme";
const once = { final: false };
const last = { final: true };
const NOW = new Date("2026-11-20T12:00:00.000Z");
const daysAgo = (days: number): string => new Date(NOW.getTime() - days * 86_400_000).toISOString();
const OTHER_SCAN = "hc-2026-10-01-00000002";
const ORG15 = FAKE.orgId.slice(0, 15);

const stage = (ApiName: string, SortOrder: number | null, flags: Partial<{ IsActive: boolean; IsClosed: boolean; IsWon: boolean }> = {}) => ({
  attributes: { type: "OpportunityStage" },
  ApiName,
  MasterLabel: `${ApiName} label`,
  IsActive: true,
  IsClosed: false,
  IsWon: false,
  SortOrder,
  ...flags,
});
// Names no log line could hold by chance.
const STAGES = [stage("Quokka Won", 3, { IsClosed: true, IsWon: true }), stage("Quokka Prospect", 1), stage("Quokka Retired", null, { IsActive: false }), stage("Quokka Proposal", 2)];

/** The customer's org: the read-only check's reads, and the stage read. */
class Org implements Transport {
  readonly requests: TransportRequest[] = [];
  stages: unknown[] | "refused" = STAGES;
  /** Set to hold the stage read until released. */
  hold: Promise<void> | null = null;
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const url = new URL(request.url);
    const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
    if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
    if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    const q = url.searchParams.get("q") ?? "";
    if (/FROM PermissionSet WHERE/.test(q)) return ok({ records: [{ Name: "Barometer_Read_Only" }], totalSize: 1, done: true });
    if (/FROM OpportunityStage/.test(q)) {
      if (this.hold) await this.hold;
      if (this.stages === "refused") return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "no" }]) };
      return ok({ records: this.stages, totalSize: this.stages.length, done: true });
    }
    return ok({ records: [], totalSize: 0, done: true });
  }
  stageReads(): number {
    return this.requests.filter((r) => /FROM OpportunityStage/.test(new URL(r.url).searchParams.get("q") ?? "")).length;
  }
}

/** A run behind the link (this one unless `run` says otherwise), its link pending, and a worker to deliver the job. */
async function flow(options: { product?: Run["product"]; others?: Run[]; withoutRun?: boolean } = {}) {
  const fake = new FakeTokenEndpoint();
  const org = new Org();
  const deps = baseDeps({ oauthTransport: fake, dataTransport: org, now: () => NOW });
  if (!options.withoutRun) deps.store.addRun(runInput({ run: consoleRun({ orgId: ORG_AWAITING_CONNECT, product: options.product ?? "scan", startedAt: daysAgo(0.1) }) }));
  for (const run of options.others ?? []) deps.store.addRun(runInput({ run }));
  const f = { fake, org, deps, handle: createJobHandler(deps) };
  const link = addLink(f, options.withoutRun ? undefined : RUN_ID);
  return { ...f, link, job: { kind: "connect", linkId: link.id } };
}

/** A new pending link, signed in at the fake and sealed as the console seals it. */
function addLink(f: { fake: FakeTokenEndpoint; deps: ReturnType<typeof baseDeps> }, runId: string | undefined, state: "pending" | "refused" = "pending"): ConnectLink {
  const { link } = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: NAME, createdBy: "consultant@example.com" });
  const begun = beginAuthorisation(link, oauthClient);
  const code = f.fake.issueCode(begun.authorisationUrl);
  f.deps.store.addLink(link, state, { pending: sealConnectSecret(JSON.stringify(begun.pending), f.deps.connectKey, link.id), code: sealConnectSecret(code, f.deps.connectKey, link.id) }, runId);
  return link;
}

/** Another scan, made `madeDaysAgo` for `orgId`. */
const otherScan = (orgId: string, madeDaysAgo: number, id = OTHER_SCAN): Run => consoleRun({ id, product: "scan", orgId, startedAt: daysAgo(madeDaysAgo) });

/** A link of another run that connected `orgId` some days ago. */
function connectedLink(f: Awaited<ReturnType<typeof flow>>, runId: string, orgId: string, connectedDaysAgo: number): void {
  const link = addLink(f, runId);
  const held = f.deps.store.links.get(link.id)!;
  Object.assign(held, { state: "used", sealedCode: null, sealedPending: null, connected: { at: daysAgo(connectedDaysAgo), orgId, userId: FAKE.userId, note: null } });
}

const stored = (f: Awaited<ReturnType<typeof flow>>) => f.deps.store.links.get(f.link.id)!;
const refusal = (after: string) => `This Salesforce org had a free Barometer Scan in the last 30 days. You can run another after ${after}, or talk to us about a plan that scans every month.`;

describe("one free scan per Salesforce org every 30 days (DECISIONS.md 610)", () => {
  test("a second scan of the same org inside 30 days is refused with the date, its tokens revoked and nothing read", async () => {
    const f = await flow({ others: [otherScan(FAKE.orgId, 29)] });
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    assert.equal(stored(f).reason, refusal("21 Nov 2026"));
    assert.equal(stored(f).state, "refused");
    assert.equal(f.deps.tokens.has(NAME), false, "never kept");
    assert.equal(f.fake.liveRefreshTokens(), 0, "revoked at Salesforce");
    assert.equal(f.org.requests.length, 0, "nothing read from the org");
    assert.equal(f.deps.store.runs.get(RUN_ID)!.run.orgId, ORG_AWAITING_CONNECT, "the refused scan doesn't take the org");
    assert.equal(f.deps.store.orgStages.size, 0);
  });

  test("one made 31 days ago doesn't count, and the scan connects", async () => {
    const f = await flow({ others: [otherScan(FAKE.orgId, 31)] });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(stored(f).state, "used");
  });

  test("a paid run is never refused, however recent the org's scan", async () => {
    const f = await flow({ product: "barometer", others: [otherScan(FAKE.orgId, 1)] });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.ok(f.deps.tokens.has(NAME));
  });

  test("a paid run of the org doesn't count against a scan", async () => {
    const f = await flow({ others: [consoleRun({ id: OTHER_SCAN, product: "barometer", orgId: FAKE.orgId, startedAt: daysAgo(1) })] });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
  });

  test("the 15 and 18 character forms of one org id match, whichever is stored", async () => {
    // Made for the 15 character form, recently.
    const made = await flow({ others: [otherScan(ORG15, 2)] });
    assert.equal((await made.handle(made.job, once)).outcome, "refused");
    assert.equal(stored(made).reason, refusal("18 Dec 2026"));
    // Made long ago, but connected 10 days ago, by an 18 character id in another case of the suffix.
    const connected = await flow({ others: [otherScan(ORG15, 60)] });
    connectedLink(connected, OTHER_SCAN, `${ORG15}XYZ`, 10);
    assert.equal((await connected.handle(connected.job, once)).outcome, "refused");
    assert.equal(stored(connected).reason, refusal("10 Dec 2026"), "the date counts from the connect");
    // And the store asks with the 15 character form whatever it holds.
    assert.equal(await connected.deps.store.lastScanOfOrg(ORG15, RUN_ID, daysAgo(30)), daysAgo(10));
  });

  test("another org's scan, a sandbox's own id among them, doesn't count", async () => {
    const f = await flow({ others: [otherScan("00D000000000002AAA", 1)] });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
  });

  test("the scan's own earlier failed attempt doesn't count, nor another scan's refused one", async () => {
    // Another scan whose prospect never got through keeps awaiting its org.
    const f = await flow({ others: [otherScan(ORG_AWAITING_CONNECT, 3)] });
    const earlier = addLink(f, RUN_ID, "refused");
    f.deps.store.links.get(earlier.id)!.reason = refusal("1 Dec 2026");
    addLink(f, OTHER_SCAN, "refused");
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(stored(f).state, "used");
  });

  test("the scan's own run, connected before under another link, isn't counted against it", async () => {
    const f = await flow();
    connectedLink(f, RUN_ID, FAKE.orgId, 1);
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
  });

  test("a store that can't answer refuses the scan and revokes the tokens", async () => {
    const f = await flow();
    f.deps.store.lastScanOfOrg = async () => Promise.reject(new Error("connection terminated"));
    assert.equal((await f.handle(f.job, last)).outcome, "refused");
    assert.match(stored(f).reason!, /^We couldn't check whether this Salesforce org has had a free scan recently, so we disconnected it and read nothing\. Start again from the link\.$/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0);
  });
});

describe("the org's opportunity stages read at connect (DECISIONS.md 611)", () => {
  test("one read, written as one row in sort order, with no sort order last", async () => {
    const f = await flow({ product: "barometer" });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.org.stageReads(), 1);
    const row = f.deps.store.orgStages.get(RUN_ID)!;
    assert.equal(row.readAt, NOW.toISOString());
    assert.deepEqual(row.stages, [
      { apiName: "Quokka Prospect", label: "Quokka Prospect label", isActive: true, isClosed: false, isWon: false, sortOrder: 1 },
      { apiName: "Quokka Proposal", label: "Quokka Proposal label", isActive: true, isClosed: false, isWon: false, sortOrder: 2 },
      { apiName: "Quokka Won", label: "Quokka Won label", isActive: true, isClosed: true, isWon: true, sortOrder: 3 },
      { apiName: "Quokka Retired", label: "Quokka Retired label", isActive: false, isClosed: false, isWon: false, sortOrder: null },
    ]);
    assert.ok(f.deps.lines.includes("Saved the org's 4 opportunity stages for the calibration."));
  });

  test("a free scan's connect reads them too", async () => {
    const f = await flow();
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.deps.store.orgStages.get(RUN_ID)!.stages.length, 4);
  });

  test("a reconnect replaces the row", async () => {
    const f = await flow({ product: "barometer" });
    await f.handle(f.job, once);
    f.org.stages = [stage("Quokka Only", 1)];
    const again = addLink(f, RUN_ID);
    assert.equal((await f.handle({ kind: "connect", linkId: again.id }, once)).outcome, "connected");
    assert.deepEqual(f.deps.store.orgStages.get(RUN_ID)!.stages.map((s) => s.apiName), ["Quokka Only"]);
    assert.equal(f.deps.store.orgStages.size, 1);
  });

  test("a refused read leaves the connect successful and writes nothing", async () => {
    const f = await flow({ product: "barometer" });
    f.org.stages = "refused";
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.equal(stored(f).state, "used");
    assert.equal(f.deps.store.orgStages.size, 0);
    assert.deepEqual(f.deps.lines.filter((l) => /stages/.test(l)), ["The org's opportunity stages weren't saved for the calibration: Salesforce refused the read (INSUFFICIENT_ACCESS)."]);
  });

  test("a reply of another shape writes nothing", async () => {
    const f = await flow({ product: "barometer" });
    f.org.stages = [{ ApiName: "Quokka Odd", MasterLabel: "Quokka Odd", IsActive: "yes" }];
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.deps.store.orgStages.size, 0);
    assert.ok(f.deps.lines.includes("The org's opportunity stages weren't saved for the calibration: the reply wasn't in the shape we expect."));
  });

  test("a console without the table leaves the connect successful", async () => {
    const f = await flow({ product: "barometer" });
    f.deps.store.orgStagesTable = false;
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.deps.store.orgStages.size, 0);
    assert.ok(!f.deps.lines.some((l) => /^Saved the org's/.test(l)), "not said to be saved");
  });

  test("a store that throws, quoting a stage, is logged without it and the connect stands", async () => {
    const f = await flow({ product: "barometer" });
    f.deps.store.saveOrgStages = async (_r, stages) => Promise.reject(new Error(`invalid input for ${stages[0]!.apiName}`));
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.ok(f.deps.lines.includes("The org's opportunity stages weren't saved for the calibration: an unexpected error (Error)."));
  });

  test("a read slower than its limit leaves the connect successful, and when it comes back writes nothing", async () => {
    const f = await flow({ product: "barometer" });
    let release = () => {};
    f.org.hold = new Promise<void>((resolve) => (release = resolve));
    f.deps.stagesTimeoutMs = 30;
    const started = Date.now();
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.ok(Date.now() - started < 5_000, "the connect didn't wait for the read");
    assert.ok(f.deps.lines.includes("The org's opportunity stages weren't saved for the calibration: the read didn't finish in time."));
    release();
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(f.org.stageReads(), 1);
    assert.equal(f.deps.store.orgStages.size, 0, "the late reply wrote nothing");
  });

  test("when the check couldn't open a connection on the last try, the stages aren't read and the log says so", async () => {
    const f = await flow({ product: "barometer" });
    const org = f.org;
    f.deps.dataTransport = { send: async () => Promise.reject(new Error("fetch failed: ETIMEDOUT")) };
    assert.equal((await f.handle(f.job, last)).outcome, "connected");
    assert.equal(org.stageReads(), 0);
    assert.ok(f.deps.lines.includes("The org's opportunity stages weren't saved for the calibration: the connection couldn't be opened to read them."));
  });

  test("a link with no run behind it reads nothing", async () => {
    const f = await flow({ withoutRun: true });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.org.stageReads(), 0);
  });

  test("nothing from the stage list reaches a log line, a store write's detail or the outcome", async () => {
    const seen: string[] = [];
    for (const setup of [(_f: Awaited<ReturnType<typeof flow>>) => {}, (f: Awaited<ReturnType<typeof flow>>) => void (f.org.stages = [...STAGES, { ApiName: "Quokka Odd" }]), (f: Awaited<ReturnType<typeof flow>>) => void (f.deps.store.orgStagesTable = false)]) {
      const f = await flow({ product: "barometer" });
      setup(f);
      const outcome = await f.handle(f.job, once);
      seen.push(...f.deps.lines, JSON.stringify(f.deps.store.writes), outcome.detail, JSON.stringify(stored(f)));
    }
    assert.ok(seen.length > 0);
    for (const text of seen) assert.doesNotMatch(text, /Quokka/);
  });
});

describe("the stages as written", () => {
  test("ties in sort order go by API name, and a missing field refuses the whole reply", () => {
    assert.deepEqual(orgStages([stage("B", 1), stage("A", 1)]).map((s) => s.apiName), ["A", "B"]);
    assert.throws(() => orgStages([{ ApiName: "A" }]), /shape/);
  });
});

// The Postgres side, through a pool that records every statement.
type Handler = (text: string, values: unknown[]) => SqlResult | undefined;
class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}
const ROWS: OrgStage[] = [{ apiName: "Quokka Prospect", label: "Quokka Prospect", isActive: true, isClosed: false, isWon: false, sortOrder: 1 }];
const DELETE = `DELETE FROM "HealthCheckOrgStages" WHERE "runId" = $1`;
const INSERT = `INSERT INTO "HealthCheckOrgStages" ("runId", "readAt", "stages") VALUES ($1, $2::timestamp(3), $3::jsonb)`;

describe("the Postgres store's connect checks", () => {
  test("asks for the latest other scan by the 15 character id, leaving out the scan asking, from both runs and links", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => (text === LAST_SCAN_SQL ? { rows: [{ at: "2026-11-10T09:00:00.000Z" }], rowCount: 1 } : undefined));
    const store = new PostgresWorkerStore(pool);
    assert.equal(await store.lastScanOfOrg(ORG15, RUN_ID, "2026-10-21T12:00:00.000Z"), "2026-11-10T09:00:00.000Z");
    assert.deepEqual(pool.statements[0]!.values, [ORG15, RUN_ID, "2026-10-21T12:00:00.000Z"]);
    for (const half of LAST_SCAN_SQL.split(" UNION ALL ")) {
      assert.match(half, /r\."product" = 'scan' AND r\."id" <> \$2 AND LEFT\(\w\."(orgId|connectedOrgId)", 15\) = \$1 AND \w\."(startedAt|usedAt)" > \$3::timestamp\(3\)/);
    }
    pool.handlers.unshift((text) => (text === LAST_SCAN_SQL ? { rows: [{ at: null }], rowCount: 1 } : undefined));
    assert.equal(await store.lastScanOfOrg(ORG15, RUN_ID, "2026-10-21T12:00:00.000Z"), null);
  });

  test("the run's id reaches the connect job for every product", async () => {
    const pool = new FakePool();
    const link = { id: "00000000-0000-4000-8000-000000000001", tokenHash: "A".repeat(43), customerRef: "SYN-0001", connectionName: "acme", loginUrl: "https://login.salesforce.com", createdBy: "consultant@example.com", createdAt: "2026-11-03T20:00:00.000Z", expiresAt: "2026-11-06T20:00:00.000Z", usedAt: null, revokedAt: null };
    pool.handlers.push((text) => (text.startsWith(`SELECT "orgId", "id", "product" FROM "HealthCheckRun"`) ? { rows: [{ orgId: "awaiting-connect", id: RUN_ID, product: "barometer" }], rowCount: 1 } : undefined));
    pool.handlers.push((text) => (text.includes(`FROM "HealthCheckConnectLink"`) ? { rows: [{ ...link, state: "pending", sealedPending: null, sealedCode: null }], rowCount: 1 } : undefined));
    const job = await new PostgresWorkerStore(pool).loadConnectJob(link.id);
    assert.equal(job!.runId, RUN_ID);
    assert.equal(job!.scanRunId, undefined);
  });

  test("replaces the stages row whole in a transaction of its own", async () => {
    const pool = new FakePool();
    assert.equal(await new PostgresWorkerStore(pool).saveOrgStages(RUN_ID, ROWS, NOW.toISOString()), true);
    assert.deepEqual(pool.texts(), ["BEGIN", DELETE, INSERT, "COMMIT"]);
    assert.deepEqual(pool.statements[2]!.values, [RUN_ID, NOW.toISOString(), JSON.stringify(ROWS)]);
  });

  test("a console without the table: false, rolled back, said once across two saves", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => {
      if (text.includes(`"HealthCheckOrgStages"`)) throw Object.assign(new Error(`relation "HealthCheckOrgStages" does not exist`), { code: "42P01" });
      return undefined;
    });
    const lines: string[] = [];
    const store = new PostgresWorkerStore(pool, { log: (l) => void lines.push(l) });
    for (let i = 0; i < 2; i++) assert.equal(await store.saveOrgStages(RUN_ID, ROWS, NOW.toISOString()), false);
    assert.equal(pool.texts().filter((t) => t === "ROLLBACK").length, 2);
    assert.equal(lines.filter((l) => /no HealthCheckOrgStages table/.test(l)).length, 1);
  });

  test("any other error is thrown by its SQLSTATE alone, never the driver's words", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => {
      if (text.startsWith("INSERT")) throw Object.assign(new Error(`invalid input syntax near "Quokka Prospect"`), { code: "22P02" });
      return undefined;
    });
    const err = await new PostgresWorkerStore(pool).saveOrgStages(RUN_ID, ROWS, NOW.toISOString()).catch((e: unknown) => e);
    assert.ok(err instanceof StoreError);
    assert.equal((err as Error).message, "the console's database refused it (SQLSTATE 22P02)");
    assert.equal(pool.texts().at(-1), "ROLLBACK");
  });
});
