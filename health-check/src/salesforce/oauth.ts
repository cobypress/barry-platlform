import { createHash, createSign, randomBytes, type KeyObject } from "node:crypto";
import { z } from "zod";
import { instanceOrigin, isSalesforceHost } from "./hosts.ts";
import { assertOAuthEndpoint, type OAuthTransport } from "./transport.ts";

// The OAuth 2.0 flows Barometer uses with its External Client App. Pure
// functions over an injected transport, so tests run against a fake token
// endpoint and nothing here can reach the network on its own.
//
// Only api and refresh_token are asked for. "full" would add web and visualforce
// access we don't need, and "web" would let the token open a browser session.

export const OAUTH_SCOPES = ["api", "refresh_token"] as const;

export type OAuthClient = {
  clientId: string;
  /** The app requires its secret for the web server and refresh flows (DECISIONS.md). */
  clientSecret?: string;
  redirectUri: string;
};

export type TokenSet = {
  accessToken: string;
  /** Present after a code exchange, and after a refresh when rotation is on. */
  refreshToken?: string;
  instanceUrl: string;
  orgId: string;
  userId: string;
  scope: string[];
  issuedAt: number;
};

export class OAuthError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(`${code}: ${message}`);
    this.name = "OAuthError";
    this.code = code;
  }
}

const b64url = (bytes: Buffer): string => bytes.toString("base64url");

/** RFC 7636. The verifier stays with us, and only its hash goes in the link. */
export function newPkce(): { verifier: string; challenge: string } {
  const verifier = b64url(randomBytes(32));
  return { verifier, challenge: pkceChallenge(verifier) };
}

export function pkceChallenge(verifier: string): string {
  return b64url(createHash("sha256").update(verifier).digest());
}

export function newState(): string {
  return b64url(randomBytes(32));
}

/** login.salesforce.com, test.salesforce.com or a My Domain. Never a path. */
export function loginOrigin(loginUrl: string): string {
  return instanceOrigin(loginUrl, true);
}

// Salesforce matches the callback exactly, so anything else is a mistake. Plain
// http is only for a listener on this machine.
function assertRedirectUri(redirectUri: string): void {
  let url: URL;
  try {
    url = new URL(redirectUri);
  } catch {
    throw new Error("The redirect URI isn't a valid URL");
  }
  const local = url.protocol === "http:" && (url.hostname === "localhost" || url.hostname === "127.0.0.1");
  if (url.protocol !== "https:" && !local) throw new Error("The redirect URI must be https, or http on localhost");
  if (url.hash || url.username || url.password) throw new Error("The redirect URI can't hold a fragment or credentials");
}

/**
 * `prompt=login consent` makes the admin sign in afresh and see the consent
 * screen, so a browser already signed in to Salesforce doesn't silently
 * authorise as whoever that is: the admin chooses who connects.
 */
export function authorisationUrl(options: { loginUrl: string; client: OAuthClient; state: string; codeChallenge: string }): string {
  assertRedirectUri(options.client.redirectUri);
  const url = new URL("/services/oauth2/authorize", loginOrigin(options.loginUrl));
  url.search = new URLSearchParams({
    response_type: "code",
    client_id: options.client.clientId,
    redirect_uri: options.client.redirectUri,
    scope: OAUTH_SCOPES.join(" "),
    state: options.state,
    code_challenge: options.codeChallenge,
    code_challenge_method: "S256",
    prompt: "login consent",
  }).toString();
  return url.toString();
}

/** What came back to the callback. Salesforce sends `error` instead of `code` when the admin says no. */
export function readCallback(callbackUrl: string): { code: string; state: string } {
  let params: URLSearchParams;
  try {
    params = new URL(callbackUrl).searchParams;
  } catch {
    throw new Error("The callback URL isn't a valid URL");
  }
  const error = params.get("error");
  if (error) throw new OAuthError(error, (params.get("error_description") ?? "Salesforce refused the authorisation").slice(0, 200));
  const code = params.get("code");
  const state = params.get("state");
  if (!code || !state) throw new Error("The callback URL has no code or state");
  return { code, state };
}

const TokenResponse = z.object({
  access_token: z.string().min(1),
  refresh_token: z.string().min(1).optional(),
  instance_url: z.string().min(1),
  id: z.string().min(1),
  issued_at: z.string().optional(),
  scope: z.string().optional(),
  token_type: z.string().optional(),
});

const ORG_ID = /^00D[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/;
const USER_ID = /^005[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/;

/** The identity URL is https://<login host>/id/<org id>/<user id>. */
export function identityFromUrl(idUrl: string): { orgId: string; userId: string } {
  let url: URL;
  try {
    url = new URL(idUrl);
  } catch {
    throw new OAuthError("BAD_TOKEN_RESPONSE", "The identity URL isn't a valid URL");
  }
  const [, id, orgId, userId] = url.pathname.split("/");
  if (url.protocol !== "https:" || !isSalesforceHost(url.hostname) || id !== "id" || !ORG_ID.test(orgId ?? "") || !USER_ID.test(userId ?? "")) {
    throw new OAuthError("BAD_TOKEN_RESPONSE", "The identity URL isn't a Salesforce user");
  }
  return { orgId: orgId!, userId: userId! };
}

async function tokenRequest(transport: OAuthTransport, endpoint: string, form: URLSearchParams): Promise<TokenSet> {
  // Checked here as well as in the real transport, so a test fake gets no leeway.
  assertOAuthEndpoint(endpoint);
  const res = await transport.postForm(endpoint, form);
  let body: unknown;
  try {
    body = JSON.parse(res.text);
  } catch {
    // Never quote the body: a half-parsed token response holds the token.
    throw new OAuthError(`HTTP_${res.status}`, "The token endpoint didn't return JSON");
  }
  if (res.status !== 200) {
    const err = body as { error?: unknown; error_description?: unknown };
    throw new OAuthError(String(err.error ?? `HTTP_${res.status}`), String(err.error_description ?? "The token request failed").slice(0, 200));
  }
  const parsed = TokenResponse.safeParse(body);
  if (!parsed.success) throw new OAuthError("BAD_TOKEN_RESPONSE", "The token response is missing a field");
  const token = parsed.data;
  // Checked on every response, refresh included: the client will send the token here.
  const instanceUrl = instanceOrigin(token.instance_url, true);
  const scope = (token.scope ?? "").split(/\s+/).filter(Boolean);
  if (token.scope !== undefined && !scope.includes("api")) throw new OAuthError("BAD_SCOPE", "The token wasn't granted the api scope");
  return {
    accessToken: token.access_token,
    refreshToken: token.refresh_token,
    instanceUrl,
    ...identityFromUrl(token.id),
    scope,
    issuedAt: Number(token.issued_at) || Date.now(),
  };
}

function withSecret(form: URLSearchParams, client: { clientSecret?: string }): URLSearchParams {
  if (client.clientSecret) form.set("client_secret", client.clientSecret);
  return form;
}

export async function exchangeCode(options: { transport: OAuthTransport; loginUrl: string; client: OAuthClient; code: string; codeVerifier: string }): Promise<TokenSet> {
  assertRedirectUri(options.client.redirectUri);
  const form = new URLSearchParams({
    grant_type: "authorization_code",
    code: options.code,
    client_id: options.client.clientId,
    redirect_uri: options.client.redirectUri,
    code_verifier: options.codeVerifier,
  });
  return tokenRequest(options.transport, `${loginOrigin(options.loginUrl)}/services/oauth2/token`, withSecret(form, options.client));
}

/**
 * With rotation on, the response carries a new refresh token and the old one
 * stops working at once, so the caller must store the new one before using the
 * access token. Without rotation, `refreshToken` is undefined and the old one stands.
 */
export async function refreshTokens(options: { transport: OAuthTransport; instanceUrl: string; client: Pick<OAuthClient, "clientId" | "clientSecret">; refreshToken: string }): Promise<TokenSet> {
  const form = new URLSearchParams({ grant_type: "refresh_token", refresh_token: options.refreshToken, client_id: options.client.clientId, format: "json" });
  return tokenRequest(options.transport, `${instanceOrigin(options.instanceUrl, true)}/services/oauth2/token`, withSecret(form, options.client));
}

/** Revoking a refresh token also revokes the access tokens issued from it. An unknown token counts as already revoked. */
export async function revokeToken(options: { transport: OAuthTransport; instanceUrl: string; token: string }): Promise<void> {
  const endpoint = `${instanceOrigin(options.instanceUrl, true)}/services/oauth2/revoke`;
  assertOAuthEndpoint(endpoint);
  const res = await options.transport.postForm(endpoint, new URLSearchParams({ token: options.token }));
  if (res.status === 200) return;
  let code = `HTTP_${res.status}`;
  try {
    code = String((JSON.parse(res.text) as { error?: unknown }).error ?? code);
  } catch {
    // Keep the HTTP status.
  }
  if (code === "invalid_token") return;
  throw new OAuthError(code, "Salesforce didn't revoke the token");
}

/**
 * The JWT bearer flow, for a user an admin has pre-authorised for the app. No
 * browser and no refresh token: each refresh signs a new assertion. The audience
 * is login.salesforce.com, or test.salesforce.com for a sandbox, even when the
 * token endpoint is a My Domain.
 */
export async function jwtBearer(options: {
  transport: OAuthTransport;
  tokenUrl: string;
  clientId: string;
  username: string;
  audience: string;
  privateKey: KeyObject | string;
  now?: number;
}): Promise<TokenSet> {
  const now = Math.floor((options.now ?? Date.now()) / 1000);
  const part = (value: unknown): string => b64url(Buffer.from(JSON.stringify(value)));
  const unsigned = `${part({ alg: "RS256" })}.${part({ iss: options.clientId, sub: options.username, aud: loginOrigin(options.audience), exp: now + 180 })}`;
  const signature = createSign("RSA-SHA256").update(unsigned).sign(options.privateKey, "base64url");
  const form = new URLSearchParams({ grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer", assertion: `${unsigned}.${signature}` });
  return tokenRequest(options.transport, `${loginOrigin(options.tokenUrl)}/services/oauth2/token`, form);
}
