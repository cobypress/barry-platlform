import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { beginAuthorisation, completeAuthorisation, ConnectLinkError, createConnectLink, findUsableLink, LINK_TTL_MS, markUsed, PENDING_TTL_MS, revokeLink, tokenFromLink } from "../link.ts";
import { exchangeCode, OAuthError, type OAuthClient } from "../../salesforce/oauth.ts";
import { ConnectLink, PendingAuthorisation } from "../../schemas/connect.ts";
import { FAKE, FakeTokenEndpoint } from "../../testing/fake-oauth.ts";

const client: OAuthClient = { clientId: FAKE.clientId, clientSecret: FAKE.clientSecret, redirectUri: "https://console.example.com/connect/callback" };
const now = new Date("2026-11-03T12:00:00.000Z");
const later = (ms: number) => new Date(now.getTime() + ms);

const make = (extra: Partial<Parameters<typeof createConnectLink>[0]> = {}) =>
  createConnectLink({ baseUrl: "https://console.example.com/", customerRef: "customer-0001", connectionName: "customer-0001", createdBy: "consultant@example.com", now, ...extra });

const problem = (p: string) => (err: unknown) => err instanceof ConnectLinkError && err.problem === p;

describe("making a connect link", () => {
  test("the URL holds the secret and the record holds only its hash", () => {
    const { url, link } = make();
    const token = tokenFromLink(url);
    assert.match(url, /^https:\/\/console\.example\.com\/connect\/[A-Za-z0-9_-]{43}$/);
    assert.ok(!JSON.stringify(link).includes(token));
    assert.ok(ConnectLink.safeParse(link).success);
  });

  test("lasts 72 hours, unused, and logs in at login.salesforce.com unless told otherwise", () => {
    const { link } = make();
    assert.equal(Date.parse(link.expiresAt) - Date.parse(link.createdAt), LINK_TTL_MS);
    assert.equal(link.usedAt, null);
    assert.equal(link.loginUrl, "https://login.salesforce.com");
    assert.equal(make({ loginUrl: "https://acme.my.salesforce.com/" }).link.loginUrl, "https://acme.my.salesforce.com");
  });

  test("two links never share a secret", () => {
    assert.notEqual(tokenFromLink(make().url), tokenFromLink(make().url));
  });

  test("a link must be https and log in at Salesforce", () => {
    assert.throws(() => make({ baseUrl: "http://console.example.com" }), /https/);
    assert.throws(() => make({ loginUrl: "https://login.salesforce.com.evil.example" }), /Salesforce host/);
  });
});

describe("opening a link", () => {
  test("finds the link a token belongs to", () => {
    const a = make();
    const b = make();
    assert.equal(findUsableLink([a.link, b.link], tokenFromLink(b.url), now).id, b.link.id);
  });

  test("an unknown or malformed token is refused", () => {
    const { link } = make();
    assert.throws(() => findUsableLink([link], tokenFromLink(make().url), now), problem("unknown"));
    assert.throws(() => findUsableLink([link], "short", now), problem("unknown"));
    assert.throws(() => tokenFromLink("https://console.example.com/connect/short"), problem("unknown"));
  });

  test("expires after 72 hours", () => {
    const { url, link } = make();
    findUsableLink([link], tokenFromLink(url), later(LINK_TTL_MS - 1));
    assert.throws(() => findUsableLink([link], tokenFromLink(url), later(LINK_TTL_MS)), problem("expired"));
  });

  test("works for one connection only", () => {
    const { url, link } = make();
    assert.throws(() => findUsableLink([markUsed(link, now)], tokenFromLink(url), now), problem("used"));
  });

  test("a withdrawn link is refused", () => {
    const { url, link } = make();
    assert.throws(() => findUsableLink([revokeLink(link, now)], tokenFromLink(url), now), problem("revoked"));
  });

  test("messages are for the customer's admin, and never echo the token", () => {
    const { url, link } = make();
    try {
      findUsableLink([markUsed(link, now)], tokenFromLink(url), now);
    } catch (err) {
      assert.match((err as Error).message, /Black Cloud consultant/);
      assert.ok(!(err as Error).message.includes(tokenFromLink(url)));
    }
  });
});

describe("the trip to Salesforce and back", () => {
  test("ends with tokens for the user who approved", async () => {
    const fake = new FakeTokenEndpoint();
    const { url, link } = make();
    const found = findUsableLink([link], tokenFromLink(url), now);
    const { authorisationUrl, state, pending } = beginAuthorisation(found, client, now);
    assert.ok(PendingAuthorisation.safeParse(pending).success);
    assert.ok(!JSON.stringify(pending).includes(state), "only the state's hash is kept");
    const code = fake.issueCode(authorisationUrl);
    const callback = `${client.redirectUri}?code=${code}&state=${state}`;
    const { codeVerifier } = completeAuthorisation(pending, link, callback, later(60_000));
    const tokens = await exchangeCode({ transport: fake, loginUrl: link.loginUrl, client, code, codeVerifier });
    assert.equal(tokens.userId, FAKE.userId);
  });

  test("a reply with someone else's state is refused", () => {
    const { link } = make();
    const { pending } = beginAuthorisation(link, client, now);
    const other = beginAuthorisation(link, client, now);
    assert.throws(() => completeAuthorisation(pending, link, `${client.redirectUri}?code=c&state=${other.state}`, now), problem("state"));
  });

  test("a reply for another link is refused", () => {
    const { link } = make();
    const { pending, state } = beginAuthorisation(link, client, now);
    assert.throws(() => completeAuthorisation(pending, make().link, `${client.redirectUri}?code=c&state=${state}`, now), problem("state"));
  });

  test("the round trip has 10 minutes", () => {
    const { link } = make();
    const { pending, state } = beginAuthorisation(link, client, now);
    assert.throws(() => completeAuthorisation(pending, link, `${client.redirectUri}?code=c&state=${state}`, later(PENDING_TTL_MS)), problem("pending_expired"));
  });

  test("a link used or withdrawn meanwhile is refused", () => {
    const { link } = make();
    const { pending, state } = beginAuthorisation(link, client, now);
    const callback = `${client.redirectUri}?code=c&state=${state}`;
    assert.throws(() => completeAuthorisation(pending, markUsed(link, now), callback, now), problem("used"));
    assert.throws(() => completeAuthorisation(pending, revokeLink(link, now), callback, now), problem("revoked"));
  });

  test("an admin who clicks Deny gets Salesforce's reason", () => {
    const { link } = make();
    const { pending, state } = beginAuthorisation(link, client, now);
    assert.throws(() => completeAuthorisation(pending, link, `${client.redirectUri}?error=access_denied&error_description=end-user+denied+authorization&state=${state}`, now), OAuthError);
  });
});
