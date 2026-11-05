import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { beginAuthorisation, createConnectLink, revokeLink } from "../../connect/link.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { ConnectLink } from "../../schemas/connect.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { sealConnectSecret } from "../connect-crypto.ts";
import { ORG_AWAITING_CONNECT, type LinkState } from "../types.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, oauthClient, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

const NAME = "acme";
const once = { final: false };
const last = { final: true };

const permissionSet = (name: string, extra: Record<string, boolean> = {}) => ({
  Name: name,
  PermissionsModifyAllData: false,
  PermissionsModifyMetadata: false,
  PermissionsCustomizeApplication: false,
  PermissionsAuthorApex: false,
  PermissionsManageUsers: false,
  PermissionsEditTask: false,
  PermissionsEditEvent: false,
  ...extra,
});

/** Salesforce's data API for the read-only check, from the customer's org. */
class PermissionsOrg implements Transport {
  readonly requests: TransportRequest[] = [];
  sets: unknown[] | "unreadable" = [permissionSet("Barometer_Read_Only")];
  objects: unknown[] = [];
  failNext = 0;
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    if (this.failNext > 0) {
      this.failNext--;
      throw new Error("fetch failed: ETIMEDOUT");
    }
    const url = new URL(request.url);
    const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
    if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
    if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    const q = url.searchParams.get("q") ?? "";
    if (/FROM PermissionSet WHERE/.test(q)) {
      if (this.sets === "unreadable") return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INSUFFICIENT_ACCESS", message: "no" }]) };
      return ok({ records: this.sets, totalSize: this.sets.length, done: true });
    }
    return ok({ records: this.objects, totalSize: this.objects.length, done: true });
  }
}

type Flow = Awaited<ReturnType<typeof flow>>;

/** The console's half: a link, the admin's trip to Salesforce, and the sealed code and pending authorisation. */
async function flow(options: { state?: LinkState; link?: (l: ConnectLink) => ConnectLink; sealFor?: string; key?: Buffer; consoleNow?: Date; runOrgId?: string; product?: "barometer" | "scan" } = {}) {
  const fake = new FakeTokenEndpoint();
  const org = new PermissionsOrg();
  const deps = baseDeps({ oauthTransport: fake, dataTransport: org, now: () => new Date() });
  const created = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: NAME, createdBy: "consultant@example.com", now: options.consoleNow });
  const link = options.link ? options.link(created.link) : created.link;
  const begun = beginAuthorisation(link, oauthClient, options.consoleNow);
  const code = fake.issueCode(begun.authorisationUrl);
  const sealKey = options.key ?? deps.connectKey;
  const sealFor = options.sealFor ?? link.id;
  // The console's run behind the link, when a test cares which org it expects.
  if (options.runOrgId) deps.store.addRun(runInput({ run: consoleRun({ orgId: options.runOrgId, ...(options.product ? { product: options.product } : {}) }) }));
  deps.store.addLink(
    link,
    options.state ?? "pending",
    {
      pending: sealConnectSecret(JSON.stringify(begun.pending), sealKey, sealFor),
      code: sealConnectSecret(code, sealKey, sealFor),
    },
    options.runOrgId ? RUN_ID : undefined,
  );
  const handle = createJobHandler(deps);
  return { fake, org, deps, link, code, verifier: begun.pending.codeVerifier, handle, job: { kind: "connect", linkId: link.id } };
}

const stored = (f: Flow) => f.deps.store.links.get(f.link.id)!;
const exchanges = (f: Flow) => f.fake.requests.filter((r) => r.form.get("grant_type") === "authorization_code").length;

/** Every secret this flow handled, and everything the console or a log could see. */
function assertNoSecrets(f: Flow, extra: string[] = []): void {
  const seen = JSON.stringify([...f.deps.store.links.values(), f.deps.store.writes]) + f.deps.lines.join("\n") + extra.join("\n");
  const secrets = [f.code, f.verifier, ...f.fake.requests.flatMap((r) => [r.form.get("refresh_token"), r.form.get("token")])].filter((s): s is string => !!s);
  for (const secret of secrets) assert.ok(!seen.includes(secret), "a secret reached the console or the log");
  assert.doesNotMatch(seen, /5Aep[0-9a-f]{8}|![0-9a-f]{32}/, "nothing that looks like a token");
}

describe("a connect job", () => {
  test("exchanges the code, stores the tokens encrypted, reads what the user can do, then marks the link used", async () => {
    const f = await flow();
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    const link = stored(f);
    assert.equal(link.state, "used");
    assert.ok(link.link.usedAt);
    assert.deepEqual({ orgId: link.connected!.orgId, userId: link.connected!.userId, note: link.connected!.note }, { orgId: FAKE.orgId, userId: FAKE.userId, note: "Connected as a user who can only read." });
    assert.equal(link.sealedCode, null, "spent secrets are cleared");
    assert.equal(link.sealedPending, null);
    const connection = await f.deps.tokens.load(NAME);
    assert.equal(connection.grant, "web-server");
    assert.equal(connection.clientId, FAKE.clientId);
    assert.ok(connection.refreshToken);
    assert.ok(f.org.requests.some((r) => /FROM PermissionSet/.test(new URL(r.url).searchParams.get("q") ?? "")), "the check read the org's permissions");
    assertNoSecrets(f);
  });

  test("a run that didn't know its org takes the one the customer connected", async () => {
    const f = await flow({ runOrgId: ORG_AWAITING_CONNECT });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.deps.store.runs.get(RUN_ID)!.run.orgId, FAKE.orgId);
  });

  test("a run that expects its org connects to it, and keeps the id as the consultant typed it", async () => {
    const f = await flow({ runOrgId: FAKE.orgId.slice(0, 15) });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(f.deps.store.runs.get(RUN_ID)!.run.orgId, FAKE.orgId.slice(0, 15));
  });

  test("signing in to a different org is refused before anything is read, and the tokens revoked", async () => {
    const f = await flow({ runOrgId: "00D000000000999" });
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    assert.match(stored(f).reason!, /different Salesforce org \(00D000000000001\) from the one this health check is for \(00D000000000999\)/);
    assert.equal(f.org.requests.length, 0, "nothing read from the org");
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0, "revoked at Salesforce");
    assert.equal(f.deps.store.runs.get(RUN_ID)!.run.orgId, "00D000000000999");
    assertNoSecrets(f);
  });

  test("a full admin is accepted, kept, and the consultant told what they can change (DECISIONS 151)", async () => {
    const f = await flow();
    f.org.sets = [permissionSet("X00e000000000001", { PermissionsModifyAllData: true, PermissionsModifyMetadata: true, PermissionsCustomizeApplication: true, PermissionsAuthorApex: true, PermissionsManageUsers: true })];
    f.org.objects = [{ SobjectType: "Account" }];
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    const link = stored(f);
    assert.equal(link.state, "used");
    assert.ok(link.link.usedAt);
    assert.equal(
      link.connected!.note,
      "Connected as a user who can change data (Author Apex; Customize Application; Manage Users, and 3 more). Barometer only reads, as its request list enforces.",
    );
    assert.match(outcome.detail, /Barometer only reads/);
    assert.ok(f.deps.tokens.has(NAME), "the connection is kept");
    assert.equal(f.fake.liveRefreshTokens(), 1, "and not revoked");
    assertNoSecrets(f);
  });

  test("a user whose permissions can't be read is kept, with a note for the consultant", async () => {
    const f = await flow();
    f.org.sets = "unreadable";
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.equal(stored(f).state, "used");
    assert.match(stored(f).connected!.note!, /^Connected\. We couldn't read what the user can change: The permission records couldn't be read \(INSUFFICIENT_ACCESS\)\. Barometer only reads/);
    assert.ok(f.deps.tokens.has(NAME));
  });

  test("a code Salesforce won't take is refused with what to do, and nothing is stored", async () => {
    const f = await flow();
    await f.fake.postForm(`${FAKE.loginUrl}/services/oauth2/token`, new URLSearchParams({ grant_type: "authorization_code", code: f.code, client_id: FAKE.clientId, client_secret: FAKE.clientSecret, redirect_uri: FAKE.redirectUri, code_verifier: "wrong" }));
    // The fake forgets a code once presented, as Salesforce does once it's used.
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    assert.match(stored(f).reason!, /didn't accept the sign-in.*Start again from the link/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assertNoSecrets(f);
  });

  test("secrets sealed with another key, or for another link, are refused without an exchange", async () => {
    for (const options of [{ key: Buffer.alloc(32, 7) }, { sealFor: "00000000-0000-4000-8000-000000000009" }]) {
      const f = await flow(options);
      const outcome = await f.handle(f.job, once);
      assert.equal(outcome.outcome, "refused");
      assert.match(stored(f).reason!, /couldn't read the saved sign-in/);
      assert.equal(exchanges(f), 0);
    }
  });

  test("a withdrawn link is refused, and a settled or open one is left alone", async () => {
    const withdrawn = await flow({ link: (l) => revokeLink(l) });
    assert.equal((await withdrawn.handle(withdrawn.job, once)).outcome, "refused");
    assert.match(stored(withdrawn).reason!, /withdrawn/);
    for (const state of ["open", "used", "refused", "expired", "withdrawn"] as const) {
      const f = await flow({ state });
      assert.equal((await f.handle(f.job, once)).outcome, "skipped", state);
      assert.equal(f.deps.store.writes.length, 0);
      assert.equal(exchanges(f), 0);
    }
  });
});

describe("a connect job delivered again", () => {
  test("after it finished, it does nothing", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    const writes = f.deps.store.writes.length;
    assert.equal((await f.handle(f.job, once)).outcome, "skipped");
    assert.equal(f.deps.store.writes.length, writes);
    assert.equal(exchanges(f), 1);
  });

  test("after a crash between storing the tokens and the check, it checks without exchanging again", async () => {
    const f = await flow();
    f.org.failNext = 1;
    let thrown = "";
    await f.handle(f.job, once).catch((err: Error) => (thrown = err.message));
    assert.match(thrown, /ETIMEDOUT/);
    assert.equal(stored(f).state, "pending");
    assert.ok(f.deps.tokens.has(NAME), "the tokens were stored before the check");
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(exchanges(f), 1, "the spent code wasn't sent again");
    assertNoSecrets(f, [thrown]);
  });

  test("if the check still can't run on the last delivery, the connection is kept with a note saying so", async () => {
    const f = await flow();
    f.org.failNext = 10;
    const outcome = await f.handle(f.job, last);
    assert.equal(outcome.outcome, "connected");
    assert.match(stored(f).connected!.note!, /^Connected\. We couldn't read what the user can change: the check failed \(.*ETIMEDOUT.*\)\. Barometer only reads, as its request list enforces\.$/);
    assert.ok(f.deps.tokens.has(NAME));
    assertNoSecrets(f);
  });

  test("a network failure at the exchange is retried, and the error carries no code", async () => {
    const f = await flow();
    const fake = f.fake;
    let failures = 1;
    f.deps.oauthTransport = {
      postForm: async (url, form) => {
        if (failures-- > 0) throw new Error(`fetch failed: ECONNRESET while sending ${form.get("code")}`);
        return fake.postForm(url, form);
      },
    };
    let thrown = "";
    await f.handle(f.job, once).catch((err: Error) => (thrown = err.message));
    assert.match(thrown, /ECONNRESET/);
    assert.ok(!thrown.includes(f.code), "the code was cut out of the error");
    assert.equal(stored(f).state, "pending");
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
  });

  test("two deliveries at once never exchange twice", async () => {
    const f = await flow();
    const results = await Promise.allSettled([f.handle(f.job, once), f.handle(f.job, once)]);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    assert.equal(exchanges(f), 1);
    assert.equal(stored(f).state, "used");
  });
});

describe("a connect job over an existing connection", () => {
  test("a read-only reconnect replaces it", async () => {
    const f = await flow();
    await f.deps.tokens.save(syntheticConnection({ name: NAME, orgId: FAKE.orgId, connectedAt: "2026-01-01T00:00:00.000Z" }));
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.notEqual((await f.deps.tokens.load(NAME)).connectedAt, "2026-01-01T00:00:00.000Z");
    assert.ok(f.deps.lines.some((l) => /Replaced the connection "acme"/.test(l)));
  });

  test("a reconnect as a user who can write replaces the old connection, with the note", async () => {
    const f = await flow();
    const old = syntheticConnection({ name: NAME, orgId: FAKE.orgId, connectedAt: "2026-01-01T00:00:00.000Z" });
    await f.deps.tokens.save(old);
    f.org.sets = [permissionSet("System Administrator", { PermissionsModifyAllData: true })];
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.notDeepEqual(await f.deps.tokens.load(NAME), old);
    assert.match(stored(f).connected!.note!, /can change data \(Modify All Data\)/);
  });

  test("a reconnect that's refused after the exchange puts the old connection back", async () => {
    const f = await flow();
    const old = syntheticConnection({ name: NAME, orgId: FAKE.orgId, connectedAt: "2026-01-01T00:00:00.000Z" });
    await f.deps.tokens.save(old);
    f.deps.store.markLinkUsed = async () => Promise.reject(new Error("connection terminated"));
    assert.equal((await f.handle(f.job, last)).outcome, "refused");
    assert.match(stored(f).reason!, /couldn't record it/);
    assert.deepEqual(await f.deps.tokens.load(NAME), old);
  });

  test("a sign-in to a different org from the one held is refused and revoked", async () => {
    const f = await flow();
    await f.deps.tokens.save(syntheticConnection({ name: NAME, orgId: "00D000000000099AAA", connectedAt: "2026-01-01T00:00:00.000Z" }));
    assert.equal((await f.handle(f.job, once)).outcome, "refused");
    assert.match(stored(f).reason!, /different Salesforce org/);
    assert.equal((await f.deps.tokens.load(NAME)).orgId, "00D000000000099AAA");
    assert.equal(f.fake.liveRefreshTokens(), 0);
  });
});

describe("review fixes: tokens never outlive a link that didn't connect", () => {
  test("the console withdraws the link during the check: the tokens are revoked and forgotten at once", async () => {
    const f = await flow();
    const store = f.deps.store;
    const mark = store.markLinkUsed.bind(store);
    store.markLinkUsed = async (id, connected) => {
      store.links.get(id)!.state = "withdrawn";
      return mark(id, connected);
    };
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "skipped");
    assert.match(outcome.detail, /became withdrawn while we were connecting/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0);
  });

  test("marking used fails with the link still pending: the tokens go, and the link is refused", async () => {
    const f = await flow();
    f.deps.store.markLinkUsed = async () => Promise.reject(new Error("connection terminated"));
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    assert.match(stored(f).reason!, /couldn't record it, so we disconnected/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0);
  });

  test("tokens an earlier delivery stored are revoked when a later one finds the link withdrawn", async () => {
    const f = await flow();
    f.org.failNext = 1;
    await f.handle(f.job, once).catch(() => {});
    assert.ok(f.deps.tokens.has(NAME));
    const link = stored(f);
    link.state = "withdrawn";
    link.link.revokedAt = new Date().toISOString();
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "skipped");
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0);
  });

  test("a used link's connection is left alone on a later delivery", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    assert.equal((await f.handle(f.job, once)).outcome, "skipped");
    assert.ok(f.deps.tokens.has(NAME));
  });
});

describe("review fixes: a delivery knows its own tokens by the link, not the clock", () => {
  test("with the console's clock five minutes ahead, a retry still checks without exchanging again", async () => {
    const f = await flow({ consoleNow: new Date(Date.now() + 5 * 60_000) });
    f.org.failNext = 1;
    await assert.rejects(f.handle(f.job, once), /ETIMEDOUT/);
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(exchanges(f), 1);
  });

  test("a later connection from another link under the same name isn't taken for this sign-in", async () => {
    const f = await flow();
    await f.deps.tokens.save(
      syntheticConnection({ name: NAME, orgId: FAKE.orgId, connectedAt: "2099-01-01T00:00:00.000Z", fromLink: { linkId: "00000000-0000-4000-8000-000000000099", stateHash: "B".repeat(43) } }),
    );
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.equal(exchanges(f), 1, "this sign-in's own code was exchanged");
    assert.equal((await f.deps.tokens.load(NAME)).fromLink?.linkId, f.link.id);
  });

  test("the stored connection names its link and sign-in", async () => {
    const f = await flow();
    await f.handle(f.job, once);
    const connection = await f.deps.tokens.load(NAME);
    assert.equal(connection.fromLink?.linkId, f.link.id);
    assert.match(connection.fromLink!.stateHash, /^[A-Za-z0-9_-]{43}$/);
  });
});

describe("review fixes: a refusal survives a retry", () => {
  // A different org is the refusal left once any user is accepted (DECISIONS 151).
  const wrongOrg = () => flow({ runOrgId: "00D000000000999" });

  test("the store fails to record a wrong-org refusal: tokens are gone, and the retry records the same reason without a second exchange", async () => {
    const f = await wrongOrg();
    const store = f.deps.store;
    const refuseLink = store.refuseLink.bind(store);
    let failures = 1;
    store.refuseLink = async (...args: Parameters<MemoryWorkerStore["refuseLink"]>) => (failures-- > 0 ? Promise.reject(new Error("connection terminated")) : refuseLink(...args));
    await assert.rejects(f.handle(f.job, once), /connection terminated/);
    assert.equal(f.deps.tokens.has(NAME), false, "revoked before the store was asked");
    assert.equal(f.fake.liveRefreshTokens(), 0);
    assert.equal(stored(f).state, "pending");

    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    assert.match(stored(f).reason!, /different Salesforce org \(00D000000000001\)/);
    assert.equal(exchanges(f), 1);
    assertNoSecrets(f);
  });

  test("on the last delivery too, the reason waits for whichever delivery can record it", async () => {
    const f = await wrongOrg();
    const store = f.deps.store;
    const refuseLink = store.refuseLink.bind(store);
    store.refuseLink = async () => Promise.reject(new Error("connection terminated"));
    await assert.rejects(f.handle(f.job, last), /connection terminated/);
    store.refuseLink = refuseLink;
    assert.equal((await f.handle(f.job, last)).outcome, "refused");
    assert.match(stored(f).reason!, /different Salesforce org/);
    assert.equal(stored(f).sealedCode, null);
    assert.equal(exchanges(f), 1);
  });

  test("once the console has settled the link, the waiting reason is dropped", async () => {
    const f = await wrongOrg();
    f.deps.store.refuseLink = async () => Promise.reject(new Error("connection terminated"));
    await assert.rejects(f.handle(f.job, once));
    stored(f).state = "expired";
    assert.equal((await f.handle(f.job, once)).outcome, "skipped");
    stored(f).state = "pending";
    f.deps.store.refuseLink = MemoryWorkerStore.prototype.refuseLink.bind(f.deps.store);
    // With the note gone, a fresh look at the same (spent) code is refused on its own terms.
    assert.match((await f.handle(f.job, once)).detail, /didn't accept the sign-in/);
  });
});

describe("a free scan's connect job", () => {
  test("queues the scan the moment the prospect has connected", async () => {
    const f = await flow({ runOrgId: ORG_AWAITING_CONNECT, product: "scan" });
    const queued: string[] = [];
    f.deps.queueRun = async (runId) => void queued.push(runId);
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.deepEqual(queued, [RUN_ID]);
    assert.deepEqual(f.deps.store.queued, [RUN_ID]);
  });

  test("an ordinary run waits for the consultant, and a queue that fails still leaves the link connected", async () => {
    const plain = await flow({ runOrgId: ORG_AWAITING_CONNECT });
    const queued: string[] = [];
    plain.deps.queueRun = async (runId) => void queued.push(runId);
    await plain.handle(plain.job, once);
    assert.deepEqual(queued, []);
    const down = await flow({ runOrgId: ORG_AWAITING_CONNECT, product: "scan" });
    down.deps.queueRun = async () => Promise.reject(new Error("redis is down"));
    assert.equal((await down.handle(down.job, once)).outcome, "connected");
    assert.ok(down.deps.lines.some((l) => /waits for "Start the scan"/.test(l)));
  });
});
