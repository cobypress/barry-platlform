import { createHash, createVerify, randomBytes } from "node:crypto";
import type { OAuthTransport, TransportResponse } from "../salesforce/transport.ts";

// A stand-in for Salesforce's token and revoke endpoints, in process. It holds
// the protocol to account rather than echoing it: PKCE is verified, the secret
// is required, codes work once, and rotation kills the old refresh token. Using
// a rotated-out token revokes the whole family, as Salesforce documents.

export const FAKE = {
  loginUrl: "https://login.salesforce.com",
  instanceUrl: "https://acme.my.salesforce.com",
  orgId: "00D000000000001AAA",
  userId: "005000000000001AAA",
  clientId: "3MVG9fake.client.id",
  clientSecret: "fake-client-secret",
  redirectUri: "http://localhost:8787/oauth/callback",
};

type Code = { challenge: string; clientId: string; redirectUri: string; userId: string };

export class FakeTokenEndpoint implements OAuthTransport {
  readonly requests: Array<{ url: string; form: URLSearchParams }> = [];
  rotation = true;
  instanceUrl = FAKE.instanceUrl;
  idHost = "login.salesforce.com";
  orgId = FAKE.orgId;
  scope = "api refresh_token";
  /** JWT bearer: the public key that signs assertions, and who is pre-authorised. */
  jwtPublicKey?: string;
  preAuthorised = new Map<string, string>();
  readonly #codes = new Map<string, Code>();
  readonly #live = new Map<string, { family: string; userId: string }>();
  readonly #used = new Map<string, string>();
  readonly #revokedFamilies = new Set<string>();
  readonly #accessTokens = new Map<string, string>();

  /** What Salesforce does when the admin clicks Allow: a code bound to the PKCE challenge. */
  issueCode(authorisationUrl: string, userId = FAKE.userId): string {
    const params = new URL(authorisationUrl).searchParams;
    if (params.get("code_challenge_method") !== "S256") throw new Error("fake: PKCE must be S256");
    const code = `aPrx${randomBytes(12).toString("hex")}`;
    this.#codes.set(code, { challenge: params.get("code_challenge")!, clientId: params.get("client_id")!, redirectUri: params.get("redirect_uri")!, userId });
    return code;
  }

  liveRefreshTokens(): number {
    return [...this.#live.values()].filter((t) => !this.#revokedFamilies.has(t.family)).length;
  }

  isAccessTokenLive(token: string): boolean {
    const family = this.#accessTokens.get(token);
    return family !== undefined && !this.#revokedFamilies.has(family);
  }

  async postForm(url: string, form: URLSearchParams): Promise<TransportResponse> {
    this.requests.push({ url, form });
    const path = new URL(url).pathname;
    if (path === "/services/oauth2/revoke") return this.#revoke(form);
    switch (form.get("grant_type")) {
      case "authorization_code":
        return this.#exchange(form);
      case "refresh_token":
        return this.#refresh(form);
      case "urn:ietf:params:oauth:grant-type:jwt-bearer":
        return this.#jwt(form);
      default:
        return error(400, "unsupported_grant_type", "grant type not supported");
    }
  }

  #secretOk(form: URLSearchParams): boolean {
    return form.get("client_id") === FAKE.clientId && form.get("client_secret") === FAKE.clientSecret;
  }

  #exchange(form: URLSearchParams): TransportResponse {
    if (!this.#secretOk(form)) return error(400, "invalid_client", "invalid client credentials");
    const code = this.#codes.get(form.get("code") ?? "");
    this.#codes.delete(form.get("code") ?? "");
    if (!code) return error(400, "invalid_grant", "invalid authorization code");
    if (form.get("redirect_uri") !== code.redirectUri) return error(400, "invalid_grant", "redirect_uri mismatch");
    const challenge = createHash("sha256").update(form.get("code_verifier") ?? "").digest("base64url");
    if (challenge !== code.challenge) return error(400, "invalid_grant", "invalid code verifier");
    const family = randomBytes(4).toString("hex");
    return this.#tokens(code.userId, family, true);
  }

  #refresh(form: URLSearchParams): TransportResponse {
    if (!this.#secretOk(form)) return error(400, "invalid_client", "invalid client credentials");
    const token = form.get("refresh_token") ?? "";
    const reused = this.#used.get(token);
    if (reused) {
      this.#revokedFamilies.add(reused);
      return error(400, "invalid_grant", "expired access/refresh token");
    }
    const live = this.#live.get(token);
    if (!live || this.#revokedFamilies.has(live.family)) return error(400, "invalid_grant", "expired access/refresh token");
    if (this.rotation) {
      this.#live.delete(token);
      this.#used.set(token, live.family);
    }
    return this.#tokens(live.userId, live.family, this.rotation);
  }

  #jwt(form: URLSearchParams): TransportResponse {
    const [header, claims, signature] = (form.get("assertion") ?? "").split(".");
    if (!this.jwtPublicKey || !header || !claims || !signature) return error(400, "invalid_grant", "invalid assertion");
    const valid = createVerify("RSA-SHA256").update(`${header}.${claims}`).verify(this.jwtPublicKey, signature, "base64url");
    const body = JSON.parse(Buffer.from(claims, "base64url").toString()) as { iss: string; sub: string; aud: string; exp: number };
    if (!valid || body.iss !== FAKE.clientId || body.aud !== FAKE.loginUrl || body.exp * 1000 < Date.now() - 60_000) return error(400, "invalid_grant", "invalid assertion");
    const userId = this.preAuthorised.get(body.sub);
    if (!userId) return error(400, "invalid_grant", "user hasn't approved this consumer");
    return this.#tokens(userId, randomBytes(4).toString("hex"), false);
  }

  #revoke(form: URLSearchParams): TransportResponse {
    const token = form.get("token") ?? "";
    const family = this.#live.get(token)?.family ?? this.#accessTokens.get(token);
    if (!family) return error(400, "invalid_token", "invalid token");
    this.#revokedFamilies.add(family);
    return { status: 200, header: () => null, text: "" };
  }

  #tokens(userId: string, family: string, withRefresh: boolean): TransportResponse {
    const access = `${this.orgId.slice(0, 15)}!${randomBytes(16).toString("hex")}`;
    this.#accessTokens.set(access, family);
    let refresh: string | undefined;
    if (withRefresh) {
      refresh = `5Aep${randomBytes(16).toString("hex")}`;
      this.#live.set(refresh, { family, userId });
    }
    const body = {
      access_token: access,
      ...(refresh ? { refresh_token: refresh } : {}),
      instance_url: this.instanceUrl,
      id: `https://${this.idHost}/id/${this.orgId}/${userId}`,
      issued_at: String(Date.now()),
      scope: this.scope,
      token_type: "Bearer",
      signature: "ignored",
    };
    return { status: 200, header: () => null, text: JSON.stringify(body) };
  }
}

function error(status: number, code: string, description: string): TransportResponse {
  return { status, header: () => null, text: JSON.stringify({ error: code, error_description: description }) };
}
