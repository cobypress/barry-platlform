import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import {
  authorisationUrl,
  exchangeCode,
  identityFromUrl,
  jwtBearer,
  newPkce,
  OAUTH_SCOPES,
  OAuthError,
  pkceChallenge,
  readCallback,
  refreshTokens,
  revokeToken,
  type OAuthClient,
} from "../oauth.ts";
import { assertOAuthEndpoint, type OAuthTransport } from "../transport.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";

const client: OAuthClient = { clientId: FAKE.clientId, clientSecret: FAKE.clientSecret, redirectUri: FAKE.redirectUri };

async function connected(fake = new FakeTokenEndpoint()) {
  const { verifier, challenge } = newPkce();
  const url = authorisationUrl({ loginUrl: FAKE.loginUrl, client, state: "s", codeChallenge: challenge });
  const tokens = await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client, code: fake.issueCode(url), codeVerifier: verifier });
  return { fake, tokens };
}

describe("PKCE", () => {
  test("matches the RFC 7636 example", () => {
    assert.equal(pkceChallenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"), "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
  });

  test("a new verifier is 43 url-safe characters, and never repeats", () => {
    const a = newPkce();
    const b = newPkce();
    assert.match(a.verifier, /^[A-Za-z0-9_-]{43}$/);
    assert.notEqual(a.verifier, b.verifier);
    assert.equal(a.challenge, pkceChallenge(a.verifier));
  });
});

describe("the authorisation URL", () => {
  const url = new URL(authorisationUrl({ loginUrl: "https://login.salesforce.com/", client, state: "state-1", codeChallenge: "challenge-1" }));

  test("asks for the code flow with PKCE, our two scopes and a fresh login", () => {
    assert.equal(url.origin + url.pathname, "https://login.salesforce.com/services/oauth2/authorize");
    assert.equal(url.searchParams.get("response_type"), "code");
    assert.equal(url.searchParams.get("scope"), "api refresh_token");
    assert.deepEqual([...OAUTH_SCOPES], ["api", "refresh_token"]);
    assert.equal(url.searchParams.get("code_challenge"), "challenge-1");
    assert.equal(url.searchParams.get("code_challenge_method"), "S256");
    assert.equal(url.searchParams.get("state"), "state-1");
    assert.equal(url.searchParams.get("prompt"), "login consent");
  });

  test("never carries the secret", () => {
    assert.ok(!url.toString().includes(FAKE.clientSecret));
  });

  test("only goes to a Salesforce login host", () => {
    for (const loginUrl of ["https://login.salesforce.com.evil.example", "https://login.salesforce.com@evil.example", "http://login.salesforce.com", "https://login.salesforce.com/path"]) {
      assert.throws(() => authorisationUrl({ loginUrl, client, state: "s", codeChallenge: "c" }), Error, loginUrl);
    }
    assert.ok(authorisationUrl({ loginUrl: "https://acme.my.salesforce.com", client, state: "s", codeChallenge: "c" }));
  });

  test("a redirect must be https, or http on this machine", () => {
    assert.throws(() => authorisationUrl({ loginUrl: FAKE.loginUrl, client: { ...client, redirectUri: "http://console.example.com/cb" }, state: "s", codeChallenge: "c" }), /https/);
    assert.ok(authorisationUrl({ loginUrl: FAKE.loginUrl, client: { ...client, redirectUri: "https://console.example.com/cb" }, state: "s", codeChallenge: "c" }));
  });
});

describe("the callback", () => {
  test("reads the code and state", () => {
    assert.deepEqual(readCallback("http://localhost:8787/oauth/callback?code=aPrx1&state=s1"), { code: "aPrx1", state: "s1" });
  });

  test("an admin who clicks Deny is an OAuth error, not a missing code", () => {
    assert.throws(() => readCallback("http://localhost:8787/oauth/callback?error=access_denied&error_description=end-user+denied+authorization&state=s1"), (err: unknown) => err instanceof OAuthError && err.code === "access_denied");
  });

  test("no code is an error", () => {
    assert.throws(() => readCallback("http://localhost:8787/oauth/callback?state=s1"), /no code/);
  });
});

describe("the code exchange", () => {
  test("returns the tokens, the instance and who connected", async () => {
    const { fake, tokens } = await connected();
    assert.equal(tokens.instanceUrl, FAKE.instanceUrl);
    assert.equal(tokens.orgId, FAKE.orgId);
    assert.equal(tokens.userId, FAKE.userId);
    assert.ok(tokens.refreshToken);
    assert.deepEqual(tokens.scope, ["api", "refresh_token"]);
    const sent = fake.requests[0]!;
    assert.equal(sent.url, "https://login.salesforce.com/services/oauth2/token");
    assert.equal(sent.form.get("client_secret"), FAKE.clientSecret, "the secret goes in the body");
    assert.ok(!sent.url.includes("secret"));
  });

  test("the wrong verifier is refused, so a stolen code is no use alone", async () => {
    const fake = new FakeTokenEndpoint();
    const { challenge } = newPkce();
    const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client, state: "s", codeChallenge: challenge }));
    await assert.rejects(exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client, code, codeVerifier: newPkce().verifier }), (err: unknown) => err instanceof OAuthError && err.code === "invalid_grant");
  });

  test("a code works once", async () => {
    const fake = new FakeTokenEndpoint();
    const { verifier, challenge } = newPkce();
    const code = fake.issueCode(authorisationUrl({ loginUrl: FAKE.loginUrl, client, state: "s", codeChallenge: challenge }));
    await exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client, code, codeVerifier: verifier });
    await assert.rejects(exchangeCode({ transport: fake, loginUrl: FAKE.loginUrl, client, code, codeVerifier: verifier }), /invalid_grant/);
  });

  test("an instance URL that isn't a bare Salesforce host is refused", async () => {
    for (const instanceUrl of ["https://acme.my.salesforce.com@evil.example", "https://evil.example", "https://acme.my.salesforce.com/path", "http://acme.my.salesforce.com"]) {
      const fake = new FakeTokenEndpoint();
      fake.instanceUrl = instanceUrl;
      await assert.rejects(connected(fake), Error, instanceUrl);
    }
  });

  test("an identity URL off Salesforce, or not a user, is refused", async () => {
    const fake = new FakeTokenEndpoint();
    fake.idHost = "evil.example";
    await assert.rejects(connected(fake), /identity URL/);
    assert.throws(() => identityFromUrl("https://login.salesforce.com/id/00D000000000001AAA/00G000000000001AAA"), /identity URL/);
  });

  test("a token without the api scope is refused", async () => {
    const fake = new FakeTokenEndpoint();
    fake.scope = "refresh_token";
    await assert.rejects(connected(fake), (err: unknown) => err instanceof OAuthError && err.code === "BAD_SCOPE");
  });

  test("a body that won't parse is never quoted", async () => {
    const leaky: OAuthTransport = { postForm: async () => ({ status: 200, header: () => null, text: '{"access_token":"SECRET-TOKEN"' }) };
    await assert.rejects(exchangeCode({ transport: leaky, loginUrl: FAKE.loginUrl, client, code: "c", codeVerifier: "v".repeat(43) }), (err: Error) => !err.message.includes("SECRET"));
  });
});

describe("refresh with rotation", () => {
  test("each refresh returns a new refresh token, and the old one stops working", async () => {
    const { fake, tokens } = await connected();
    const first = await refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: tokens.refreshToken! });
    assert.ok(first.refreshToken);
    assert.notEqual(first.refreshToken, tokens.refreshToken);
    assert.equal(fake.requests.at(-1)!.url, "https://acme.my.salesforce.com/services/oauth2/token");
    const second = await refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: first.refreshToken! });
    assert.ok(second.refreshToken);
  });

  test("replaying a rotated-out token is refused and revokes the newer one too", async () => {
    const { fake, tokens } = await connected();
    const next = await refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: tokens.refreshToken! });
    await assert.rejects(refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: tokens.refreshToken! }), /invalid_grant/);
    await assert.rejects(refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: next.refreshToken! }), /invalid_grant/);
  });

  test("without rotation, no new refresh token comes back", async () => {
    const { fake, tokens } = await connected();
    fake.rotation = false;
    const next = await refreshTokens({ transport: fake, instanceUrl: tokens.instanceUrl, client, refreshToken: tokens.refreshToken! });
    assert.equal(next.refreshToken, undefined);
  });

  test("a refresh never goes to a host that isn't Salesforce", async () => {
    const fake = new FakeTokenEndpoint();
    await assert.rejects(refreshTokens({ transport: fake, instanceUrl: "https://evil.example", client, refreshToken: "r" }), /Salesforce host/);
    assert.equal(fake.requests.length, 0);
  });
});

describe("revocation", () => {
  test("revoking the refresh token kills its access tokens", async () => {
    const { fake, tokens } = await connected();
    assert.ok(fake.isAccessTokenLive(tokens.accessToken));
    await revokeToken({ transport: fake, instanceUrl: tokens.instanceUrl, token: tokens.refreshToken! });
    assert.equal(fake.isAccessTokenLive(tokens.accessToken), false);
    assert.equal(fake.requests.at(-1)!.url, "https://acme.my.salesforce.com/services/oauth2/revoke");
  });

  test("a token Salesforce doesn't know counts as already revoked", async () => {
    await revokeToken({ transport: new FakeTokenEndpoint(), instanceUrl: FAKE.instanceUrl, token: "unknown" });
  });

  test("any other failure is an error", async () => {
    const down: OAuthTransport = { postForm: async () => ({ status: 503, header: () => null, text: "unavailable" }) };
    await assert.rejects(revokeToken({ transport: down, instanceUrl: FAKE.instanceUrl, token: "t" }), /HTTP_503/);
  });
});

describe("JWT bearer", () => {
  const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const pem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();

  test("a pre-authorised user gets an access token and no refresh token", async () => {
    const fake = new FakeTokenEndpoint();
    fake.jwtPublicKey = publicKey.export({ type: "spki", format: "pem" }).toString();
    fake.preAuthorised.set("reader@example.com", FAKE.userId);
    const tokens = await jwtBearer({ transport: fake, tokenUrl: FAKE.instanceUrl, clientId: FAKE.clientId, username: "reader@example.com", audience: FAKE.loginUrl, privateKey: pem });
    assert.equal(tokens.userId, FAKE.userId);
    assert.equal(tokens.refreshToken, undefined);
    assert.equal(fake.requests[0]!.form.get("client_secret"), null, "no secret needed");
  });

  test("a user the admin hasn't pre-authorised is refused", async () => {
    const fake = new FakeTokenEndpoint();
    fake.jwtPublicKey = publicKey.export({ type: "spki", format: "pem" }).toString();
    await assert.rejects(jwtBearer({ transport: fake, tokenUrl: FAKE.instanceUrl, clientId: FAKE.clientId, username: "admin@example.com", audience: FAKE.loginUrl, privateKey: pem }), /invalid_grant/);
  });
});

describe("the OAuth endpoint check", () => {
  test("allows only the token and revoke endpoints on a Salesforce host", () => {
    assertOAuthEndpoint("https://login.salesforce.com/services/oauth2/token");
    assertOAuthEndpoint("https://acme.my.salesforce.com/services/oauth2/revoke");
    for (const url of [
      "https://login.salesforce.com/services/oauth2/authorize",
      "https://acme.my.salesforce.com/services/data/v68.0/sobjects/Account",
      "https://acme.my.salesforce.com/services/oauth2/token?grant_type=password",
      "https://acme.my.salesforce.com/services/oauth2/token/../../data",
      "http://login.salesforce.com/services/oauth2/token",
      "https://evil.example/services/oauth2/token",
      "https://login.salesforce.com@evil.example/services/oauth2/token",
    ]) {
      assert.throws(() => assertOAuthEndpoint(url), Error, url);
    }
  });
});
