import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { chmodSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ApiBudget } from "../budget.ts";
import { SalesforceClient } from "../client.ts";
import { authorisationUrl, exchangeCode, newPkce, OAuthError, type OAuthClient } from "../oauth.ts";
import { describeConnection, OAuthConnectionProvider, revokeConnection } from "../oauth-provider.ts";
import { OrgNotApprovedError } from "../sf-cli-provider.ts";
import { TokenStore, type StoredConnection } from "../token-store.ts";
import type { Transport, TransportRequest } from "../transport.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";

const client: OAuthClient = { clientId: FAKE.clientId, clientSecret: FAKE.clientSecret, redirectUri: FAKE.redirectUri };
const env = { HC_APPROVED_ORG_ALIASES: "acme" } as NodeJS.ProcessEnv;
const newStore = () => new TokenStore({ dir: join(mkdtempSync(join(tmpdir(), "hc-oauth-")), "connections"), key: async () => key });
const key = randomBytes(32);

async function connectedStore(fake = new FakeTokenEndpoint()): Promise<{ fake: FakeTokenEndpoint; store: TokenStore; stored: StoredConnection }> {
  const store = newStore();
  const { verifier, challenge } = newPkce();
  const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client, state: "s", codeChallenge: challenge }));
  const t = await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client, code, codeVerifier: verifier });
  const stored: StoredConnection = {
    version: 1,
    name: "acme",
    grant: "web-server",
    loginUrl: FAKE.loginUrl,
    clientId: FAKE.clientId,
    instanceUrl: t.instanceUrl,
    orgId: t.orgId,
    userId: t.userId,
    scope: t.scope,
    accessToken: t.accessToken,
    refreshToken: t.refreshToken,
    issuedAt: t.issuedAt,
    connectedAt: "2026-11-03T12:00:00.000Z",
  };
  await store.save(stored);
  return { fake, store, stored };
}

describe("OAuthConnectionProvider", () => {
  test("refuses a connection name that isn't approved", () => {
    assert.throws(() => new OAuthConnectionProvider("acme", { store: newStore(), env: {} as NodeJS.ProcessEnv }), OrgNotApprovedError);
    assert.throws(() => new OAuthConnectionProvider("acme-2", { store: newStore(), env }), OrgNotApprovedError);
  });

  test("connects with the stored token", async () => {
    const { store, stored } = await connectedStore();
    const provider = new OAuthConnectionProvider("acme", { store, client, env });
    assert.equal(provider.kind, "oauth");
    assert.deepEqual(await provider.connect(), { instanceUrl: FAKE.instanceUrl, accessToken: stored.accessToken, orgId: FAKE.orgId });
  });

  test("a refresh rotates, and the new refresh token is stored before it's used", async () => {
    const { fake, store, stored } = await connectedStore();
    const provider = new OAuthConnectionProvider("acme", { store, client, transport: fake, env });
    await provider.connect();
    const fresh = await provider.refresh();
    const saved = await store.load("acme");
    assert.equal(saved.accessToken, fresh.accessToken);
    assert.notEqual(saved.refreshToken, stored.refreshToken);
    // A second provider, as the next run would be, carries on from the stored token.
    const next = new OAuthConnectionProvider("acme", { store, client, transport: fake, env });
    await next.connect();
    await next.refresh();
    assert.equal(fake.liveRefreshTokens(), 1);
  });

  test("a refresh for a different org or user is refused and nothing is stored", async () => {
    const { fake, store, stored } = await connectedStore();
    fake.orgId = "00D000000000002AAA";
    const provider = new OAuthConnectionProvider("acme", { store, client, transport: fake, env });
    await assert.rejects(provider.refresh(), (err: unknown) => err instanceof OAuthError && err.code === "IDENTITY_CHANGED");
    assert.equal((await store.load("acme")).accessToken, stored.accessToken);
  });

  test("a revoked connection says the admin must connect again", async () => {
    const { fake, store, stored } = await connectedStore();
    await revokeConnection({ name: "acme", store, transport: fake });
    await store.save(stored);
    const provider = new OAuthConnectionProvider("acme", { store, client, transport: fake, env });
    await assert.rejects(provider.refresh(), /connect again/);
  });

  test("a refresh needs the same OAuth client the connection was made with", async () => {
    const { fake, store } = await connectedStore();
    const provider = new OAuthConnectionProvider("acme", { store, client: { ...client, clientId: "other" }, transport: fake, env });
    await assert.rejects(provider.refresh(), /doesn't match/);
    await assert.rejects(new OAuthConnectionProvider("acme", { store, transport: fake, env }).refresh(), /doesn't match/);
  });

  test("the client refreshes through the provider after a 401", async () => {
    const { fake, store } = await connectedStore();
    const tokensSeen: string[] = [];
    let expired = true;
    const data: Transport = {
      async send(request: TransportRequest) {
        tokensSeen.push(request.headers.Authorization ?? "");
        const path = new URL(request.url).pathname;
        if (path === "/services/data/") return { status: 200, header: () => null, text: JSON.stringify([{ version: "68.0" }]) };
        if (path.endsWith("/limits")) return { status: 200, header: () => null, text: JSON.stringify({ DailyApiRequests: { Max: 1000, Remaining: 990 } }) };
        if (expired) {
          expired = false;
          return { status: 401, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_SESSION_ID", message: "Session expired" }]) };
        }
        return { status: 200, header: () => null, text: JSON.stringify({ records: [{ Id: "001000000000001AAA" }], totalSize: 1 }) };
      },
    };
    const provider = new OAuthConnectionProvider("acme", { store, client, transport: fake, env });
    const sf = await SalesforceClient.connect(provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: data, sleep: async () => {} });
    assert.equal(sf.connectionKind, "oauth");
    const result = await sf.query("SELECT Id FROM Account");
    assert.equal(result.records.length, 1);
    assert.notEqual(tokensSeen.at(-1), tokensSeen.at(-2), "the retry used the refreshed token");
    assert.equal(tokensSeen.at(-1), `Bearer ${(await store.load("acme")).accessToken}`);
  });

  test("a JWT bearer connection refreshes by signing a new assertion", async () => {
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    const dir = mkdtempSync(join(tmpdir(), "hc-jwt-"));
    const keyPath = join(dir, "jwt.key");
    writeFileSync(keyPath, privateKey.export({ type: "pkcs8", format: "pem" }), { mode: 0o600 });
    const fake = new FakeTokenEndpoint();
    fake.jwtPublicKey = publicKey.export({ type: "spki", format: "pem" }).toString();
    fake.preAuthorised.set("reader@example.com", FAKE.userId);
    const store = newStore();
    await store.save({
      version: 1,
      name: "acme",
      grant: "jwt-bearer",
      loginUrl: FAKE.instanceUrl,
      clientId: FAKE.clientId,
      instanceUrl: FAKE.instanceUrl,
      orgId: FAKE.orgId,
      userId: FAKE.userId,
      scope: ["api"],
      accessToken: "expired",
      issuedAt: 0,
      connectedAt: "2026-11-03T12:00:00.000Z",
      jwt: { username: "reader@example.com", audience: FAKE.loginUrl, privateKeyPath: keyPath },
    });
    const provider = new OAuthConnectionProvider("acme", { store, transport: fake, env });
    const fresh = await provider.refresh();
    assert.notEqual(fresh.accessToken, "expired");
    chmodSync(keyPath, 0o644);
    await assert.rejects(provider.refresh(), /readable by other users/);
  });

  test("revoking kills the tokens at Salesforce, then forgets them", async () => {
    const { fake, store, stored } = await connectedStore();
    await revokeConnection({ name: "acme", store, transport: fake });
    assert.equal(fake.isAccessTokenLive(stored.accessToken), false);
    assert.equal(store.has("acme"), false);
  });

  test("a description of a connection never shows a token", async () => {
    const { stored } = await connectedStore();
    const text = JSON.stringify(describeConnection(stored));
    assert.ok(!text.includes(stored.accessToken));
    assert.ok(!text.includes(stored.refreshToken!));
    assert.match(text, /held/);
  });
});
