import { SF_DEFAULTS } from "../config.ts";
import { isSalesforceHost } from "./hosts.ts";

export type TransportRequest = {
  method: string;
  url: string;
  headers: Record<string, string>;
  body?: string;
};

export type TransportResponse = {
  status: number;
  header(name: string): string | null;
  text: string;
};

export interface Transport {
  send(request: TransportRequest): Promise<TransportResponse>;
}

// The only place in the service that touches the network. A source scan test
// holds that line. Redirects are errors so the bearer token can't follow one.
export const fetchTransport: Transport = {
  async send(request) {
    const res = await fetch(request.url, {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: "error",
      signal: AbortSignal.timeout(SF_DEFAULTS.requestTimeoutMs),
    });
    return { status: res.status, header: (name) => res.headers.get(name), text: await res.text() };
  },
};

// The OAuth endpoints: getting, refreshing and revoking a token. They sit
// outside the org's data API, so they don't go through the client or its
// allowlist, and they're held to exactly these two paths on a Salesforce host.
// A test fake is still checked, so a bad URL fails in tests too.
export const OAUTH_PATHS: ReadonlySet<string> = new Set(["/services/oauth2/token", "/services/oauth2/revoke"]);

export interface OAuthTransport {
  postForm(url: string, form: URLSearchParams): Promise<TransportResponse>;
}

export function assertOAuthEndpoint(endpoint: string): void {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new Error("The OAuth endpoint isn't a valid URL");
  }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) throw new Error("The OAuth endpoint must be a bare https URL");
  if (!isSalesforceHost(url.hostname)) throw new Error("The OAuth endpoint isn't a Salesforce host");
  if (!OAUTH_PATHS.has(url.pathname)) throw new Error("Only the token and revoke endpoints are allowed");
}

export function checkedOAuthTransport(inner: OAuthTransport): OAuthTransport {
  return {
    postForm(url, form) {
      assertOAuthEndpoint(url);
      return inner.postForm(url, form);
    },
  };
}

export const fetchOAuthTransport: OAuthTransport = checkedOAuthTransport({
  async postForm(url, form) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded", Accept: "application/json" },
      body: form.toString(),
      redirect: "error",
      signal: AbortSignal.timeout(SF_DEFAULTS.requestTimeoutMs),
    });
    return { status: res.status, header: (name) => res.headers.get(name), text: await res.text() };
  },
});
