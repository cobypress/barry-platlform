// salesforce.ts
type SalesforceTokenResponse = {
  access_token: string;
  instance_url: string;
  id?: string;
  token_type?: string;
  issued_at?: string;
  signature?: string;
};

type TokenCache = {
  accessToken: string;
  instanceUrl: string;
  expiresAt: number; // epoch ms
};

const SF_CLIENT_ID = process.env.SF_CLIENT_ID!;
const SF_CLIENT_SECRET = process.env.SF_CLIENT_SECRET!;
const SF_LOGIN_URL = process.env.SF_LOGIN_URL || "https://login.salesforce.com";
const SF_API_VERSION = process.env.SF_API_VERSION || "60.0";
const SKEW_SECONDS = Number(process.env.SF_TOKEN_SKEW_SECONDS || "60");

if (!SF_CLIENT_ID || !SF_CLIENT_SECRET) {
  throw new Error("Missing Salesforce env vars (SF_CLIENT_ID/SF_CLIENT_SECRET).");
}

let tokenCache: TokenCache | null = null;
let refreshing: Promise<TokenCache> | null = null;

function nowMs() {
  return Date.now();
}

function isValid(cache: TokenCache | null) {
  return !!cache && cache.expiresAt > nowMs();
}

// Salesforce token responses often don't include expires_in.
// Use a conservative TTL and rely on retry-once on auth failure.
function computeExpiresAtConservative(ttlSeconds = 10 * 60) {
  return nowMs() + (ttlSeconds - SKEW_SECONDS) * 1000;
}

async function refreshAccessToken(): Promise<TokenCache> {
  if (refreshing) return refreshing;

  refreshing = (async () => {
    const url = `${SF_LOGIN_URL}/services/oauth2/token`;

    // Client Credentials flow — runs as the Connected App's designated Integration
    // User, no refresh token involved. Nothing here can silently expire the way a
    // refresh token tied to a personal login can (password reset, revoked session,
    // an org security policy) — see docs/barry-salesforce-auth.md.
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: SF_CLIENT_ID,
      client_secret: SF_CLIENT_SECRET,
    });

    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });

    const text = await res.text();
    if (!res.ok) {
      throw new Error(`Salesforce token request failed (${res.status}): ${text}`);
    }

    const data = JSON.parse(text) as SalesforceTokenResponse;

    if (!data.access_token || !data.instance_url) {
      throw new Error(`Salesforce token response missing fields: ${text}`);
    }

    const next: TokenCache = {
      accessToken: data.access_token,
      instanceUrl: data.instance_url,
      expiresAt: computeExpiresAtConservative(),
    };

    tokenCache = next;
    return next;
  })();

  try {
    return await refreshing;
  } finally {
    refreshing = null;
  }
}

async function getAccessToken(): Promise<TokenCache> {
  if (isValid(tokenCache)) return tokenCache!;
  return refreshAccessToken();
}

function isAuthFailure(status: number, bodyText?: string) {
  if (status === 401) return true;
  if (status === 403 && bodyText?.includes("INVALID_SESSION_ID")) return true;
  return false;
}

type SfRequestOptions = Omit<RequestInit, "headers"> & {
  headers?: Record<string, string>;
  noRetry?: boolean; // internal only
};

export function sfRestPath(relative: string) {
  return `/services/data/v${SF_API_VERSION}${relative.startsWith("/") ? "" : "/"}${relative}`;
}

async function sfFetch(path: string, options: SfRequestOptions = {}) {
  const cache = await getAccessToken();

  const url =
    path.startsWith("http")
      ? path
      : `${cache.instanceUrl}${path.startsWith("/") ? "" : "/"}${path}`;

  // Strip wrapper-only props before passing to fetch()
  const { noRetry, headers: extraHeaders, ...init } = options;

  const headers: Record<string, string> = {
    Authorization: `Bearer ${cache.accessToken}`,
    "Content-Type": "application/json",
    ...(extraHeaders || {}),
  };

  const res = await fetch(url, { ...init, headers });

  if (!noRetry && (res.status === 401 || res.status === 403)) {
    const bodyText = await res.clone().text().catch(() => "");
    if (isAuthFailure(res.status, bodyText)) {
      await refreshAccessToken();

      const cache2 = await getAccessToken();
      const url2 =
        path.startsWith("http")
          ? path
          : `${cache2.instanceUrl}${path.startsWith("/") ? "" : "/"}${path}`;

      const headers2: Record<string, string> = {
        ...headers,
        Authorization: `Bearer ${cache2.accessToken}`,
      };

      // Retry once, but do NOT pass noRetry into fetch()
      const res2 = await fetch(url2, { ...init, headers: headers2 });
      return res2;
    }
  }

  return res;
}

async function sfJson<T>(path: string, options: SfRequestOptions = {}): Promise<T> {
  const res = await sfFetch(path, options);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`Salesforce API error (${res.status}) on ${path}: ${text}`);
  }

  if (!text) return {} as T;
  return JSON.parse(text) as T;
}

export const salesforce = {
  sfFetch,
  sfJson,
  sfRestPath,
  forceRefresh: refreshAccessToken,
};