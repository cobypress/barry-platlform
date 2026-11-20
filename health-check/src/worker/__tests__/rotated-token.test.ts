import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import { authorisationUrl, exchangeCode, newPkce, newState, type OAuthClient } from "../../salesforce/oauth.ts";
import { OAuthConnectionProvider, ReconnectionNeeded } from "../../salesforce/oauth-provider.ts";
import { TokenStore, type StoredConnection } from "../../salesforce/token-store.ts";
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
const ADOPTED_LINE = /^A newer token for the connection "synthetic-co" was found in the store and used\.$/;

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
  return { fake, store, original, lines, provider };
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
});
