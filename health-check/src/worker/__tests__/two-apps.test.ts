import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { beginAuthorisation, createConnectLink } from "../../connect/link.ts";
import { authorisationUrl, exchangeCode, newPkce, newState, type OAuthClient } from "../../salesforce/oauth.ts";
import { revokeConnection } from "../../salesforce/oauth-provider.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { CONNECTED_USER, ORG_ID } from "../../testing/synthetic-org.ts";
import { APP_NOT_SET_UP_AT_CONNECT, appNotSetUp, needsReconnection, NEEDS_RECONNECTION } from "../apps.ts";
import { openConnectSecret, sealConnectSecret } from "../connect-crypto.ts";
import { readWorkerEnv } from "../env.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, CONNECTION, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// Two Salesforce apps in one worker (DECISIONS.md 616) and a connection
// Salesforce has ended (617). The fake token endpoint knows both apps, and a
// code or refresh token works only with the app that issued it, as Salesforce
// does. Every secret here is planted and recognisable, so any output that
// holds one can be found.

const PACKAGE_SECRET = "PLANTED-PACKAGE-SECRET-7f3a9c2e";
const SIGNIN_SECRET = "PLANTED-SIGNIN-SECRET-b81e44d0";
const PACKAGE: OAuthClient = { clientId: "3MVG9fake.package.app", clientSecret: PACKAGE_SECRET, redirectUri: "https://console.example.com/internal/health-check/connect/callback" };
const SIGNIN: OAuthClient = { clientId: "3MVG9fake.signin.app", clientSecret: SIGNIN_SECRET, redirectUri: "https://console.example.com/internal/health-check/connect/callback" };
const SECRETS = [PACKAGE_SECRET, SIGNIN_SECRET];

const once = { final: false };

function endpoint(): FakeTokenEndpoint {
  const fake = new FakeTokenEndpoint();
  fake.apps.clear();
  fake.apps.set(PACKAGE.clientId, PACKAGE_SECRET);
  fake.apps.set(SIGNIN.clientId, SIGNIN_SECRET);
  fake.orgId = ORG_ID;
  return fake;
}

/** Which app each token request named, and whether it sent that app's own secret. */
const sentBy = (fake: FakeTokenEndpoint, grant: string) =>
  fake.requests.filter((r) => r.form.get("grant_type") === grant).map((r) => `${r.form.get("client_id")} ${r.form.get("client_secret")}`);
const own = (app: OAuthClient) => `${app.clientId} ${app.clientSecret}`;

/** Every file under a folder, as text. */
const filesUnder = (dir: string): string[] => {
  try {
    return readdirSync(dir).flatMap((name) => (statSync(join(dir, name)).isDirectory() ? filesUnder(join(dir, name)) : [readFileSync(join(dir, name), "utf8")]));
  } catch {
    return [];
  }
};

function assertNoPlantedSecret(texts: unknown[]): void {
  const seen = texts.map((t) => (typeof t === "string" ? t : JSON.stringify(t))).join("\n");
  for (const secret of SECRETS) assert.ok(!seen.includes(secret), "a client secret reached an output");
}

/** The customer's org for the read-only check at connect. */
class PermissionsOrg implements Transport {
  readonly requests: TransportRequest[] = [];
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    const url = new URL(request.url);
    const ok = (json: unknown): TransportResponse => ({ status: 200, header: () => null, text: JSON.stringify(json) });
    if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
    if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
    return ok({ records: [], totalSize: 0, done: true });
  }
}

/** The console's half of a connect, with the app it chose for the link. `pendingClientId` overrides what the pending names. */
function connectFlow(options: { app: OAuthClient; signin?: OAuthClient | null; pendingClientId?: string | null }) {
  const fake = endpoint();
  const org = new PermissionsOrg();
  const deps = baseDeps({ oauthClient: PACKAGE, signinClient: options.signin === undefined ? SIGNIN : options.signin, oauthTransport: fake, dataTransport: org, now: () => new Date() });
  const { link } = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: "acme", createdBy: "consultant@example.com" });
  const begun = beginAuthorisation(link, options.app);
  const code = fake.issueCode(begun.authorisationUrl);
  const pending: Record<string, unknown> = { ...begun.pending };
  if (options.pendingClientId === null) delete pending.clientId;
  else if (options.pendingClientId !== undefined) pending.clientId = options.pendingClientId;
  const sealedPending = sealConnectSecret(JSON.stringify(pending), deps.connectKey, link.id);
  deps.store.addLink(link, "pending", { pending: sealedPending, code: sealConnectSecret(code, deps.connectKey, link.id) });
  return { fake, org, deps, link, begun, sealedPending, handle: createJobHandler(deps), job: { kind: "connect", linkId: link.id } };
}

/** A connection made with `app` through the fake, then a run whose first data request finds the access token expired. */
async function runFlow(options: { app: OAuthClient; signin?: OAuthClient | null; expired?: boolean; refuseAfter?: number }) {
  const fake = endpoint();
  const { verifier, challenge } = newPkce();
  const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client: options.app, state: newState(), codeChallenge: challenge }), CONNECTED_USER.id);
  const tokens = await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client: options.app, code, codeVerifier: verifier });
  if (options.expired) fake.expireAll();
  const replay = new ReplayTransport(loadRecording());
  const data = new ExpiresOnce(replay, options.refuseAfter);
  const deps = baseDeps({ oauthClient: PACKAGE, signinClient: options.signin === undefined ? SIGNIN : options.signin, oauthTransport: fake, dataTransport: data });
  deps.store.addRun(runInput());
  await deps.tokens.save(syntheticConnection({ clientId: options.app.clientId, instanceUrl: FAKE.instanceUrl, accessToken: tokens.accessToken, refreshToken: tokens.refreshToken }));
  return { fake, data, deps, handle: createJobHandler(deps), job: { kind: "run", runId: RUN_ID } };
}

/** The synthetic org's recording, but the first request (or the one after `after`) is refused as an expired session, once. */
class ExpiresOnce implements Transport {
  readonly requests: TransportRequest[] = [];
  readonly #inner: Transport;
  #left: number;
  #refused = false;
  constructor(inner: Transport, after = 0) {
    this.#inner = inner;
    this.#left = after;
  }
  async send(request: TransportRequest): Promise<TransportResponse> {
    this.requests.push(request);
    if (!this.#refused && this.#left-- <= 0) {
      this.#refused = true;
      return { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired or invalid" }]) };
    }
    return this.#inner.send(request);
  }
}

describe("the worker's two apps: configuration (DECISIONS.md 616)", () => {
  const base: NodeJS.ProcessEnv = {
    REDIS_URL: "redis://localhost:6379",
    HC_WORKER_DATABASE_URL: "postgres://localhost/hc",
    HC_CONNECT_KEY: Buffer.alloc(32, 1).toString("base64"),
    HC_TOKEN_KEY: Buffer.alloc(32, 2).toString("base64"),
    HC_CONNECTIONS_DIR: "/tmp/hc-connections",
    HC_RUNS_DIR: "/tmp/hc-runs",
    HC_SF_CLIENT_ID: PACKAGE.clientId,
    HC_SF_CLIENT_SECRET: PACKAGE_SECRET,
    HC_SF_REDIRECT_URI: PACKAGE.redirectUri,
  };
  const signin = { HC_SF_SIGNIN_CLIENT_ID: SIGNIN.clientId, HC_SF_SIGNIN_CLIENT_SECRET: SIGNIN_SECRET, HC_SF_SIGNIN_REDIRECT_URI: SIGNIN.redirectUri };

  test("with only the package app set, there's no sign-in app and the package app is as before", () => {
    const read = readWorkerEnv(base);
    assert.ok(read.ok);
    assert.deepEqual(read.config.oauthClient, PACKAGE);
    assert.equal(read.config.signinClient, null);
  });

  test("with both set, each app has its own id, secret and callback", () => {
    const read = readWorkerEnv({ ...base, ...signin, HC_SF_SIGNIN_REDIRECT_URI: "http://localhost:3000/internal/health-check/connect/callback" });
    assert.ok(read.ok);
    assert.deepEqual(read.config.oauthClient, PACKAGE);
    assert.deepEqual(read.config.signinClient, { ...SIGNIN, redirectUri: "http://localhost:3000/internal/health-check/connect/callback" });
  });

  test("half a sign-in app, the package app's id again or a plain-http callback stops the worker, naming the variable and never a value", () => {
    const cases: Array<[NodeJS.ProcessEnv, RegExp]> = [
      [{ ...base, HC_SF_SIGNIN_CLIENT_ID: SIGNIN.clientId, HC_SF_SIGNIN_CLIENT_SECRET: SIGNIN_SECRET }, /HC_SF_SIGNIN_REDIRECT_URI missing/],
      [{ ...base, HC_SF_SIGNIN_CLIENT_SECRET: SIGNIN_SECRET }, /HC_SF_SIGNIN_CLIENT_ID, HC_SF_SIGNIN_REDIRECT_URI missing/],
      [{ ...base, ...signin, HC_SF_SIGNIN_CLIENT_ID: PACKAGE.clientId }, /HC_SF_SIGNIN_CLIENT_ID must be a different app from HC_SF_CLIENT_ID/],
      [{ ...base, ...signin, HC_SF_SIGNIN_REDIRECT_URI: "http://console.example.com/callback" }, /HC_SF_SIGNIN_REDIRECT_URI must be https, or http on localhost/],
    ];
    for (const [env, problem] of cases) {
      const read = readWorkerEnv(env);
      assert.ok(!read.ok);
      assert.match(read.problems.join("\n"), problem);
      assertNoPlantedSecret(read.problems);
      assert.ok(!read.problems.join("\n").includes(SIGNIN.clientId) && !read.problems.join("\n").includes(PACKAGE.clientId));
    }
  });
});

describe("a connect job uses the app the link's trip used (DECISIONS.md 616)", () => {
  for (const [label, app, other] of [["sign-in", SIGNIN, PACKAGE], ["package", PACKAGE, SIGNIN]] as const) {
    test(`a link begun with the ${label} app exchanges with it alone, and the connection keeps its id`, async () => {
      const f = connectFlow({ app });
      const outcome = await f.handle(f.job, once);
      assert.equal(outcome.outcome, "connected", outcome.detail);
      assert.deepEqual(sentBy(f.fake, "authorization_code"), [own(app)]);
      assert.ok(!f.fake.requests.some((r) => r.form.get("client_id") === other.clientId || r.form.get("client_secret") === other.clientSecret), "the other app was never tried");
      assert.equal((await f.deps.tokens.load("acme")).clientId, app.clientId);
      // The pending holds the client id, never a secret, as the console seals it.
      const pending = openConnectSecret(f.sealedPending, f.deps.connectKey, f.link.id);
      assert.equal(JSON.parse(pending).clientId, app.clientId);
      assertNoPlantedSecret([pending, f.sealedPending, outcome, [...f.deps.store.links.values()], f.deps.store.writes, f.deps.lines, filesUnder(f.deps.locksDir), JSON.stringify(await f.deps.tokens.load("acme"))]);
    });
  }

  test("a pending from an older console, naming no app, exchanges with the package app as before", async () => {
    const f = connectFlow({ app: PACKAGE, pendingClientId: null });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.deepEqual(sentBy(f.fake, "authorization_code"), [own(PACKAGE)]);
  });

  test("with only the package app set up, a link begun with it connects exactly as before", async () => {
    const f = connectFlow({ app: PACKAGE, signin: null });
    assert.equal((await f.handle(f.job, once)).outcome, "connected");
    assert.deepEqual(sentBy(f.fake, "authorization_code"), [own(PACKAGE)]);
  });

  test("a link whose app the worker isn't set up for is refused with a plain reason, and nothing is sent anywhere", async () => {
    for (const f of [connectFlow({ app: SIGNIN, signin: null }), connectFlow({ app: PACKAGE, pendingClientId: "3MVG9retired.app" })]) {
      const outcome = await f.handle(f.job, once);
      assert.deepEqual(outcome, { outcome: "refused", detail: APP_NOT_SET_UP_AT_CONNECT });
      assert.equal(f.deps.store.links.get(f.link.id)!.reason, APP_NOT_SET_UP_AT_CONNECT);
      assert.equal(f.fake.requests.length, 0, "no token request, with either app");
      assert.equal(f.org.requests.length, 0, "nothing read");
      assert.equal(f.deps.tokens.has("acme"), false);
      assertNoPlantedSecret([outcome, [...f.deps.store.links.values()], f.deps.lines]);
    }
  });
});

describe("a run refreshes with its connection's own app (DECISIONS.md 616)", () => {
  for (const [label, app, other] of [["sign-in", SIGNIN, PACKAGE], ["package", PACKAGE, SIGNIN]] as const) {
    test(`a connection made with the ${label} app refreshes and revokes with it alone`, async () => {
      const f = await runFlow({ app });
      const outcome = await f.handle(f.job, once);
      assert.equal(outcome.outcome, "done", outcome.detail);
      assert.deepEqual(sentBy(f.fake, "refresh_token"), [own(app)], "one refresh, with the connection's own app");
      assert.ok(!f.fake.requests.some((r) => r.form.get("client_id") === other.clientId || r.form.get("client_secret") === other.clientSecret));
      // A revoke carries the token alone: no app's id or secret.
      await revokeConnection({ name: CONNECTION, store: f.deps.tokens, transport: f.fake });
      const revoke = f.fake.requests.at(-1)!;
      assert.match(revoke.url, /\/services\/oauth2\/revoke$/);
      assert.deepEqual([...revoke.form.keys()], ["token"]);
      assert.equal(f.fake.liveRefreshTokens(), 0);
      assertNoPlantedSecret([outcome, f.deps.store.runs.get(RUN_ID), f.deps.lines, filesUnder(f.deps.runsDir), filesUnder(f.deps.locksDir)]);
    });
  }

  test("a connection whose app the worker no longer has fails at once with a plain reason, and nothing is sent", async () => {
    const f = await runFlow({ app: SIGNIN, signin: null });
    const outcome = await f.handle(f.job, once);
    assert.deepEqual(outcome, { outcome: "failed", detail: appNotSetUp(CONNECTION) });
    assert.equal(f.deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assert.equal(f.data.requests.length, 0, "nothing read");
    assert.equal(f.fake.requests.length, 1, "only the exchange that made the connection");
    assertNoPlantedSecret([outcome, f.deps.store.runs.get(RUN_ID), f.deps.lines]);
  });
});

describe("a refresh Salesforce refuses ends the run as needs reconnection (DECISIONS.md 617)", () => {
  for (const [label, app] of [["sign-in", SIGNIN], ["package", PACKAGE]] as const) {
    test(`on the ${label} app: failed at once, on the first delivery, with the plain reason`, async () => {
      const f = await runFlow({ app, expired: true });
      const outcome = await f.handle(f.job, once);
      assert.deepEqual(outcome, { outcome: "failed", detail: needsReconnection(CONNECTION) });
      assert.ok(outcome.detail.startsWith(NEEDS_RECONNECTION));
      const run = f.deps.store.runs.get(RUN_ID)!.run;
      assert.equal(run.status, "failed");
      assert.equal(run.steps.find((s) => s.id === "worker")!.error, needsReconnection(CONNECTION));
      // The console's Ongoing view sorts a failed pulse by this test (lib/health-check/ongoing/flow.ts).
      assert.match(outcome.detail, /connection|revoked or expired|connect again|different org/i);
      assert.deepEqual(sentBy(f.fake, "refresh_token"), [own(app)], "one refused refresh, with its own app");
      assertNoPlantedSecret([outcome, f.deps.store.runs.get(RUN_ID), f.deps.lines]);
    });
  }

  test("refused part way through the run: the unread sources aren't saved as results, and the run needs reconnection", async () => {
    // Request 121 is a query whose read turns the refusal into an unread source, so the engine returns rather than throws.
    const f = await runFlow({ app: SIGNIN, expired: true, refuseAfter: 120 });
    const outcome = await f.handle(f.job, once);
    assert.deepEqual(outcome, { outcome: "failed", detail: needsReconnection(CONNECTION) });
    assert.ok(f.data.requests.length > 120, "the run had started reading");
    assert.ok(!f.deps.store.runs.get(RUN_ID)!.output, "no results saved");
  });
});
