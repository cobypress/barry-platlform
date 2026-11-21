import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { strToU8, zipSync } from "fflate";
import { join } from "node:path";
import { ApiBudget } from "../../salesforce/budget.ts";
import { keptConnection, SalesforceClient, SalesforceError } from "../../salesforce/client.ts";
import type { ConnectionProvider, OrgConnection } from "../../salesforce/connection.ts";
import { authorisationUrl, exchangeCode, newPkce, newState, type OAuthClient } from "../../salesforce/oauth.ts";
import { OAuthConnectionProvider, ReconnectionNeeded } from "../../salesforce/oauth-provider.ts";
import { decryptJson, TokenStore, type StoredConnection } from "../../salesforce/token-store.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { CONNECTED_USER, ORG_ID } from "../../testing/synthetic-org.ts";
import { needsReconnection } from "../apps.ts";
import { approvedFromConsole } from "../run-job.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, CONNECTION, RUN_ID, runInput, syntheticConnection, temp } from "./helpers.ts";

// A refresh token another worker process rotated is not "needs reconnection"
// (DECISIONS.md 631). Two providers hold one stored connection: the first
// refreshes and stores the new tokens, the second is refused with the old
// refresh token, re-loads the store once, and carries on with what it finds.
// A token that is truly gone still fails at once, as 617 says. The apps' ids
// and secrets are planted and recognisable, and every token the fake issues is
// collected, so any output holding one can be found.

const PACKAGE_SECRET = "PLANTED-PACKAGE-SECRET-631a77e1";
const SIGNIN_SECRET = "PLANTED-SIGNIN-SECRET-631b2c90";
const PACKAGE: OAuthClient = { clientId: "3MVG9planted.package.app.631", clientSecret: PACKAGE_SECRET, redirectUri: "https://console.example.com/internal/health-check/connect/callback" };
const SIGNIN: OAuthClient = { clientId: "3MVG9planted.signin.app.631", clientSecret: SIGNIN_SECRET, redirectUri: "https://console.example.com/internal/health-check/connect/callback" };

const once = { final: false };
const ADOPTED_LINE = /^A newer token for the connection "synthetic-co" was found in the store; trying it\.$/;

/** A store that counts its loads, and can hand back something altered from the next load on. */
class CountingStore extends TokenStore {
  loads = 0;
  alter?: (stored: StoredConnection) => StoredConnection;
  override async load(name: string): Promise<StoredConnection> {
    this.loads++;
    const stored = await super.load(name);
    return this.alter ? this.alter(stored) : stored;
  }
}

/**
 * A connection made with `app` through the fake token endpoint and stored, as
 * the connect job would. `reuseRevokesFamily`: whether Salesforce ends the newer
 * tokens too when a rotated-out one is presented. Its documentation doesn't
 * say, so the adopting cases run with it off and one case runs with it on.
 */
async function connected(app: OAuthClient, reuseRevokesFamily = false) {
  const fake = new FakeTokenEndpoint();
  fake.apps.clear();
  fake.apps.set(PACKAGE.clientId, PACKAGE_SECRET);
  fake.apps.set(SIGNIN.clientId, SIGNIN_SECRET);
  fake.orgId = ORG_ID;
  fake.reuseRevokesFamily = reuseRevokesFamily;
  const { verifier, challenge } = newPkce();
  const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client: app, state: newState(), codeChallenge: challenge }), CONNECTED_USER.id);
  const tokens = await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client: app, code, codeVerifier: verifier });
  const key = randomBytes(32);
  const store = new CountingStore({ dir: join(temp("hc-rotated-"), "connections"), key: async () => key });
  const original = syntheticConnection({ clientId: app.clientId, instanceUrl: FAKE.instanceUrl, accessToken: tokens.accessToken, refreshToken: tokens.refreshToken });
  await store.save(original);
  const lines: string[] = [];
  const provider = () => new OAuthConnectionProvider(CONNECTION, { store, client: app, transport: fake, env: approvedFromConsole(CONNECTION), log: (line) => lines.push(line) });
  return { fake, store, original, lines, provider, key };
}

const refreshes = (fake: FakeTokenEndpoint) => fake.requests.filter((r) => r.form.get("grant_type") === "refresh_token").length;

/** Both apps' ids and secrets, and every token the fake issued or was sent. */
function secrets(fake: FakeTokenEndpoint, stored: StoredConnection[]): string[] {
  const sent = fake.requests.map((r) => r.form.get("refresh_token"));
  return [PACKAGE.clientId, SIGNIN.clientId, PACKAGE_SECRET, SIGNIN_SECRET, ...sent, ...stored.flatMap((s) => [s.accessToken, s.refreshToken])].filter((s): s is string => !!s);
}

function assertNothingSecret(texts: unknown[], planted: string[]): void {
  const seen = texts.map((t) => (typeof t === "string" ? t : JSON.stringify(t))).join("\n");
  assert.ok(planted.length >= 6, "the planted values were collected");
  for (const secret of planted) assert.ok(!seen.includes(secret), "a token, client id or secret reached an output");
}

const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
const expired: TransportResponse = { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) };

/** The synthetic org's recording, refusing the access tokens `refused` names, as Salesforce refuses an ended session. */
class Org implements Transport {
  readonly tokens: string[] = [];
  readonly #inner: Transport;
  readonly #before?: () => Promise<void>;
  readonly #refused: (token: string) => boolean;
  constructor(refused: (token: string) => boolean, options: { inner?: Transport; before?: () => Promise<void> } = {}) {
    this.#refused = refused;
    this.#inner = options.inner ?? new ReplayTransport(loadRecording());
    this.#before = options.before;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    // Another worker process gets in first, between this run loading the connection and its first request.
    if (this.#before && this.tokens.length === 0) await this.#before();
    const token = (request.headers?.Authorization ?? "").replace(/^Bearer /, "");
    this.tokens.push(token);
    return this.#refused(token) ? expired : this.#inner.send(request);
  }
}

/** Enough of an org for a client to connect and read one query. */
const tinyOrg: Transport = {
  async send(request: TransportRequest) {
    const path = new URL(request.url).pathname;
    if (path === "/services/data/") return ok([{ version: "68.0" }]);
    if (path.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    return ok({ records: [{ Id: "00D000000000001AAA" }], totalSize: 1, done: true });
  },
};

describe("a refresh token another worker rotated is adopted, not needs reconnection (DECISIONS.md 631)", () => {
  for (const [label, app] of [["sign-in", SIGNIN], ["package", PACKAGE]] as const) {
    test(`on the ${label} app: two providers on one connection, the second is refused, re-loads once and carries on`, async () => {
      const c = await connected(app);
      const first = c.provider();
      const second = c.provider();
      await first.connect();
      await second.connect();
      const rotated = await first.refresh();
      const loadsBefore = c.store.loads;

      const adopted = await second.refresh();

      // The second provider's adopted access token is the one the first stored: no second refresh was sent for it.
      assert.equal(adopted.accessToken, rotated.accessToken);
      assert.equal(adopted.fromStore, true);
      assert.equal(refreshes(c.fake), 2, "the first provider's refresh and the second's refused one, nothing more");
      assert.equal(c.store.loads - loadsBefore, 1, "exactly one extra read of the store");
      assert.equal(second.lostAccess, false);
      // Nothing written: the store still holds exactly what the first provider got from Salesforce.
      const stored = await c.store.load(CONNECTION);
      assert.equal(stored.accessToken, rotated.accessToken);
      assert.equal(stored.clientId, app.clientId);
      assert.equal(c.lines.length, 1);
      assert.match(c.lines[0]!, ADOPTED_LINE);
      assertNothingSecret(c.lines, secrets(c.fake, [c.original, stored]));
    });
  }

  test("the adopted access token is tried first; refused too, one refresh with the stored refresh token, and the request still succeeds", async () => {
    const c = await connected(SIGNIN);
    const second = c.provider();
    const start = c.original.accessToken;
    let adoptedRefused = false;
    let rotatedAccess = "";
    // The second provider's own session has ended, and so, once, has the adopted one.
    const org = new Org(
      (token) => token === start || (token === rotatedAccess && !adoptedRefused && (adoptedRefused = true)),
      { inner: tinyOrg, before: async () => void (rotatedAccess = (await c.provider().refresh()).accessToken) },
    );
    await SalesforceClient.connect(second, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: org, sleep: async () => {} });
    assert.equal(org.tokens[0], start);
    assert.equal(org.tokens[1], rotatedAccess, "the adopted access token was used before any second refresh");
    const stored = await c.store.load(CONNECTION);
    assert.equal(org.tokens[2], stored.accessToken, "then the token from refreshing with the stored refresh token, stored before it's used");
    assert.notEqual(stored.accessToken, rotatedAccess);
    assert.equal(refreshes(c.fake), 3, "the other process's, the refused one, and one with the adopted refresh token");
    assert.equal(second.lostAccess, false);
  });

  test("a run whose connection another job rotated after it loaded it finishes instead of needing reconnection", async () => {
    const c = await connected(SIGNIN);
    const org = new Org((token) => token === c.original.accessToken, { before: async () => void (await c.provider().refresh()) });
    const deps = baseDeps({ oauthClient: PACKAGE, signinClient: SIGNIN, oauthTransport: c.fake, dataTransport: org, tokens: c.store });
    deps.store.addRun(runInput());
    const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, once);
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.equal(refreshes(c.fake), 2, "the other job's refresh and this run's refused one");
    assert.equal(deps.lines.filter((l) => ADOPTED_LINE.test(l)).length, 1);
    assertNothingSecret([outcome, deps.lines, deps.store.runs.get(RUN_ID)], secrets(c.fake, [c.original, await c.store.load(CONNECTION)]));
  });

  test("a truly revoked token fails at once as needs reconnection: exactly one extra read, no second refresh", async () => {
    const c = await connected(PACKAGE);
    c.fake.expireAll();
    const provider = c.provider();
    await provider.connect();
    const loadsBefore = c.store.loads;
    await assert.rejects(provider.refresh(), ReconnectionNeeded);
    assert.equal(c.store.loads - loadsBefore, 1, "exactly one extra read of the store");
    assert.equal(refreshes(c.fake), 1, "no second refresh when the stored token is unchanged");
    assert.equal(provider.lostAccess, true);
    assert.deepEqual(c.lines, []);
  });

  test("a truly revoked token still fails the run at once, on the first delivery, as needs reconnection", async () => {
    const c = await connected(SIGNIN);
    c.fake.expireAll();
    const deps = baseDeps({ oauthClient: PACKAGE, signinClient: SIGNIN, oauthTransport: c.fake, dataTransport: new Org((token) => token === c.original.accessToken), tokens: c.store });
    deps.store.addRun(runInput());
    const outcome = await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, once);
    assert.deepEqual(outcome, { outcome: "failed", detail: needsReconnection(CONNECTION) });
    assert.equal(refreshes(c.fake), 1);
    assertNothingSecret([outcome, deps.lines, deps.store.runs.get(RUN_ID)], secrets(c.fake, [c.original]));
  });

  test("adopted, then the adopted refresh token refused too: needs reconnection, with no second re-load", async () => {
    // If Salesforce ends the newer tokens when the old one is presented, adopting can't help, and the run says so.
    const c = await connected(SIGNIN, true);
    const second = c.provider();
    await second.connect();
    await c.provider().refresh();
    assert.equal((await second.refresh()).fromStore, true);
    const loadsBefore = c.store.loads;
    await assert.rejects(second.refresh(), ReconnectionNeeded);
    assert.equal(c.store.loads - loadsBefore, 0, "no re-load after a refusal of adopted tokens, so two processes can't ping-pong");
    assert.equal(refreshes(c.fake), 3, "one refresh with the adopted refresh token, refused, and no more");
    assert.equal(second.lostAccess, true);
  });

  test("a stored connection for a different app, name, org, user or grant is never adopted", async () => {
    const cases: Array<[string, (s: StoredConnection) => StoredConnection]> = [
      ["app", (s) => ({ ...s, clientId: PACKAGE.clientId })],
      ["name", (s) => ({ ...s, name: "someone-else" })],
      ["org", (s) => ({ ...s, orgId: "00D000000000999AAA" })],
      ["user", (s) => ({ ...s, userId: "005000000000999AAA" })],
      ["grant", (s) => ({ ...s, grant: "jwt-bearer" })],
    ];
    for (const [label, alter] of cases) {
      const c = await connected(SIGNIN);
      const second = c.provider();
      await second.connect();
      await c.provider().refresh();
      c.store.alter = alter;
      const loadsBefore = c.store.loads;
      await assert.rejects(second.refresh(), ReconnectionNeeded, label);
      assert.equal(c.store.loads - loadsBefore, 1, label);
      assert.equal(refreshes(c.fake), 2, `${label}: nothing sent with the other connection's tokens`);
      assert.equal(second.lostAccess, true, label);
      assert.deepEqual(c.lines, [], label);
    }
  });

  test("adopted tokens that worked are the provider's own: a later, separate rotation by the other process is adopted again", async () => {
    const c = await connected(SIGNIN);
    const first = c.provider();
    const second = c.provider();
    const dead = new Set([c.original.accessToken]);
    // The first process rotates once before the second's first request.
    const org = new Org((token) => dead.has(token), { inner: tinyOrg, before: async () => void (await first.refresh()) });
    const client = await SalesforceClient.connect(second, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: org, sleep: async () => {} });
    const adopted = (await c.store.load(CONNECTION)).accessToken;
    assert.equal(org.tokens.at(-1), adopted, "the second process is working on the first's tokens");
    // Later, a separate refresh: the first process rotates again, and the adopted session ends.
    await first.refresh();
    dead.add(adopted);
    const result = await client.query<{ Id: string }>("SELECT Id FROM Organization");
    assert.equal(result.records.length, 1);
    const latest = await c.store.load(CONNECTION);
    assert.equal(org.tokens.at(-1), latest.accessToken, "the newer tokens were adopted again");
    assert.equal(second.lostAccess, false);
    assert.equal(c.lines.filter((l) => ADOPTED_LINE.test(l)).length, 2);
    assert.equal(refreshes(c.fake), 4, "the first process's two refreshes and the second's two refused ones");
  });

  test("the client never refreshes more than twice for one request, on REST or SOAP, whatever the provider returns", async () => {
    let calls = 0;
    // A provider that says every time that it found tokens in the store. It stops at five, so a missing cap fails rather than hangs.
    const always: ConnectionProvider = {
      kind: "oauth",
      connect: async () => ({ instanceUrl: "https://x.example.com", accessToken: "start-token", orgId: ORG_ID }),
      refresh: async () => {
        if (++calls > 5) throw new Error("no cap: refreshed more than five times");
        return { instanceUrl: "https://x.example.com", accessToken: `store-token-${calls}`, orgId: ORG_ID, fromStore: true };
      },
    };
    const soapExpired = `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>sf:INVALID_SESSION_ID</faultcode><faultstring>INVALID_SESSION_ID: Invalid Session ID found in SessionHeader</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`;
    const endless: Transport = {
      async send(request: TransportRequest) {
        const path = new URL(request.url).pathname;
        if (path === "/services/data/") return ok([{ version: "68.0" }]);
        if (path.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
        return path.includes("/Soap/") ? { status: 500, header: () => null, text: soapExpired } : expired;
      },
    };
    const client = await SalesforceClient.connect(always, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: endless, sleep: async () => {} });
    await assert.rejects(client.query("SELECT Id FROM Account"), (err: unknown) => err instanceof SalesforceError && err.status === 401);
    assert.equal(calls, 2, "REST: two refreshes, then the 401 stands");
    calls = 0;
    await assert.rejects(client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), /INVALID_SESSION_ID/);
    assert.equal(calls, 2, "SOAP: two refreshes, then the fault stands");
  });

  test("SOAP: a session found in the store is tried first, and refused too, one refresh more and the retrieve finishes", async () => {
    const c = await connected(SIGNIN);
    const second = c.provider();
    let rotatedAccess = "";
    let adoptedRefused = false;
    const envelope = (operation: string, inner: string) =>
      `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><${operation}Response xmlns="http://soap.sforce.com/2006/04/metadata"><result>${inner}</result></${operation}Response></soapenv:Body></soapenv:Envelope>`;
    const fault = `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>sf:INVALID_SESSION_ID</faultcode><faultstring>INVALID_SESSION_ID: Invalid Session ID found in SessionHeader</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`;
    const zip = Buffer.from(zipSync({ "flows/A.flow": strToU8("<Flow/>") })).toString("base64");
    const sessions: string[] = [];
    const soapOrg: Transport = {
      async send(request: TransportRequest) {
        const path = new URL(request.url).pathname;
        if (path === "/services/data/") return ok([{ version: "68.0" }]);
        if (path.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
        if (!path.includes("/Soap/")) return ok({ records: [{ Id: ORG_ID }], totalSize: 1, done: true });
        const session = /<met:sessionId>([^<]*)<\/met:sessionId>/.exec(request.body ?? "")![1]!;
        sessions.push(session);
        // Another process rotates just before this retrieve; the old session has ended, and the adopted one once.
        if (sessions.length === 1) rotatedAccess = (await c.provider().refresh()).accessToken;
        const refused = session === c.original.accessToken || (session === rotatedAccess && !adoptedRefused && (adoptedRefused = true));
        if (refused) return { status: 500, header: () => null, text: fault };
        const text = request.body!.includes("<met:retrieve>") ? envelope("retrieve", "<id>09S000000000001AAA</id><done>false</done>") : envelope("checkRetrieveStatus", `<done>true</done><success>true</success><zipFile>${zip}</zipFile>`);
        return { status: 200, header: () => null, text };
      },
    };
    const client = await SalesforceClient.connect(second, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: soapOrg, sleep: async () => {}, retrievePollMs: 0 });
    assert.deepEqual(await client.retrieveMetadata([{ name: "Flow", members: ["*"] }]), { "flows/A.flow": "<Flow/>" });
    const stored = await c.store.load(CONNECTION);
    assert.deepEqual(sessions.slice(0, 3), [c.original.accessToken, rotatedAccess, stored.accessToken], "old session, the adopted one, then the one from refreshing with the stored refresh token");
    assert.equal(refreshes(c.fake), 3);
    assert.equal(second.lostAccess, false);
  });

  test("fromStore never travels: not on the connection the client keeps, and not in the stored file", async () => {
    const kept = keptConnection({ instanceUrl: "https://x.example.com", accessToken: "adopted-token", orgId: ORG_ID, fromStore: true } as OrgConnection, false);
    assert.deepEqual(kept, { instanceUrl: "https://x.example.com", accessToken: "adopted-token", orgId: ORG_ID });
    assert.ok(!("fromStore" in kept));
    // Adopt, then refresh with the adopted refresh token, which saves: the file holds no such flag.
    const c = await connected(SIGNIN);
    const second = c.provider();
    await second.connect();
    await c.provider().refresh();
    assert.equal((await second.refresh()).fromStore, true);
    await second.refresh();
    const raw = decryptJson(readFileSync(join(c.store.dir, `${CONNECTION}.json`), "utf8"), c.key, `connection:${CONNECTION}`) as Record<string, unknown>;
    assert.ok(!("fromStore" in raw));
    assert.equal(raw.accessToken, (await c.store.load(CONNECTION)).accessToken);
  });
});
