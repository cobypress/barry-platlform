import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { beginAuthorisation, createConnectLink, revokeLink } from "../../connect/link.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import type { ConnectLink } from "../../schemas/connect.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { sealConnectSecret } from "../connect-crypto.ts";
import type { LinkState } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, oauthClient, syntheticConnection } from "./helpers.ts";

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
async function flow(options: { state?: LinkState; link?: (l: ConnectLink) => ConnectLink; sealFor?: string; key?: Buffer } = {}) {
  const fake = new FakeTokenEndpoint();
  const org = new PermissionsOrg();
  const deps = baseDeps({ oauthTransport: fake, dataTransport: org, now: () => new Date() });
  const created = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: NAME, createdBy: "consultant@example.com" });
  const link = options.link ? options.link(created.link) : created.link;
  const begun = beginAuthorisation(link, oauthClient);
  const code = fake.issueCode(begun.authorisationUrl);
  const sealKey = options.key ?? deps.connectKey;
  const sealFor = options.sealFor ?? link.id;
  deps.store.addLink(link, options.state ?? "pending", {
    pending: sealConnectSecret(JSON.stringify(begun.pending), sealKey, sealFor),
    code: sealConnectSecret(code, sealKey, sealFor),
  });
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
  test("exchanges the code, stores the tokens encrypted, proves read only, then marks the link used", async () => {
    const f = await flow();
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    const link = stored(f);
    assert.equal(link.state, "used");
    assert.ok(link.link.usedAt);
    assert.deepEqual({ orgId: link.connected!.orgId, userId: link.connected!.userId, note: link.connected!.note }, { orgId: FAKE.orgId, userId: FAKE.userId, note: null });
    assert.equal(link.sealedCode, null, "spent secrets are cleared");
    assert.equal(link.sealedPending, null);
    const connection = await f.deps.tokens.load(NAME);
    assert.equal(connection.grant, "web-server");
    assert.equal(connection.clientId, FAKE.clientId);
    assert.ok(connection.refreshToken);
    assert.ok(f.org.requests.some((r) => /FROM PermissionSet/.test(new URL(r.url).searchParams.get("q") ?? "")), "the check read the org's permissions");
    assertNoSecrets(f);
  });

  test("a user who can write is refused, revoked at Salesforce and forgotten", async () => {
    const f = await flow();
    f.org.sets = [permissionSet("Barometer_Read_Only"), permissionSet("X00e000000000001", { PermissionsModifyAllData: true })];
    f.org.objects = [{ SobjectType: "Account" }];
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "refused");
    const link = stored(f);
    assert.equal(link.state, "refused");
    assert.match(link.reason!, /can change data in Salesforce \(Modify All Data; create, edit or delete on Account\)/);
    assert.match(link.reason!, /Barometer Read Only/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0, "revoked at Salesforce");
    assert.equal(link.link.usedAt, null, "a refusal leaves the link usable until it expires (DECISIONS 63)");
    assertNoSecrets(f);
  });

  test("a user whose permissions can't be read is kept, with a note for the consultant (DECISIONS 64)", async () => {
    const f = await flow();
    f.org.sets = "unreadable";
    const outcome = await f.handle(f.job, once);
    assert.equal(outcome.outcome, "connected");
    assert.equal(stored(f).state, "used");
    assert.match(stored(f).connected!.note!, /couldn't prove the user is read only: The permission records couldn't be read \(INSUFFICIENT_ACCESS\)/);
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

  test("if the check still can't run on the last delivery, the connection isn't kept", async () => {
    const f = await flow();
    f.org.failNext = 10;
    const outcome = await f.handle(f.job, last);
    assert.equal(outcome.outcome, "refused");
    assert.match(stored(f).reason!, /couldn't check the user's permissions/);
    assert.equal(f.deps.tokens.has(NAME), false);
    assert.equal(f.fake.liveRefreshTokens(), 0);
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

  test("a reconnect as a user who can write puts the old connection back", async () => {
    const f = await flow();
    const old = syntheticConnection({ name: NAME, orgId: FAKE.orgId, connectedAt: "2026-01-01T00:00:00.000Z" });
    await f.deps.tokens.save(old);
    f.org.sets = [permissionSet("System Administrator", { PermissionsModifyAllData: true })];
    assert.equal((await f.handle(f.job, once)).outcome, "refused");
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
