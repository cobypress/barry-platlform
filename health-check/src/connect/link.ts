import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { authorisationUrl, loginOrigin, newPkce, newState, readCallback, type OAuthClient } from "../salesforce/oauth.ts";
import { ConnectLink, PendingAuthorisation } from "../schemas/connect.ts";

// The one-time connect link. A consultant makes one for a customer and sends it
// to their Salesforce admin. The admin opens it, reads what Barometer will and
// won't do, and goes to Salesforce to sign in as the read-only integration user
// and approve the app. Nobody shares a password, and nobody at Black Cloud ever
// sees one.
//
// Everything here is pure, so the console (Milestone 5) only adds storage and
// pages. A link is good for 72 hours and for one successful connection. The
// trip to Salesforce and back must finish within 10 minutes.

export const LINK_TTL_MS = 72 * 60 * 60 * 1000;
export const PENDING_TTL_MS = 10 * 60 * 1000;

export type LinkProblem = "unknown" | "expired" | "used" | "revoked";

export class ConnectLinkError extends Error {
  readonly problem: LinkProblem | "state" | "pending_expired";

  constructor(problem: LinkProblem | "state" | "pending_expired") {
    const messages = {
      unknown: "This link isn't valid. Ask your Black Cloud consultant for a new one.",
      expired: "This link has expired. Ask your Black Cloud consultant for a new one.",
      used: "This link has already been used to connect an org. Ask your Black Cloud consultant if you need to connect again.",
      revoked: "This link was withdrawn. Ask your Black Cloud consultant for a new one.",
      state: "The reply from Salesforce doesn't match this connection. Start again from the link.",
      pending_expired: "That took longer than 10 minutes. Start again from the link.",
    };
    super(messages[problem]);
    this.name = "ConnectLinkError";
    this.problem = problem;
  }
}

const sha256 = (value: string): string => createHash("sha256").update(value).digest("base64url");

function sameHash(a: string, b: string): boolean {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && timingSafeEqual(x, y);
}

function assertBaseUrl(baseUrl: string): string {
  const url = new URL(baseUrl);
  const local = url.protocol === "http:" && (url.hostname === "localhost" || url.hostname === "127.0.0.1");
  if (url.protocol !== "https:" && !local) throw new Error("A connect link must be https");
  if (url.search || url.hash || url.username || url.password) throw new Error("The console URL must be bare");
  return url.toString().replace(/\/$/, "");
}

/** The URL holds the only copy of the secret. The record keeps its hash. */
export function createConnectLink(options: {
  baseUrl: string;
  customerRef: string;
  connectionName: string;
  loginUrl?: string;
  createdBy: string;
  now?: Date;
  ttlMs?: number;
}): { url: string; link: ConnectLink } {
  const now = options.now ?? new Date();
  const token = randomBytes(32).toString("base64url");
  const link = ConnectLink.parse({
    id: randomUUID(),
    tokenHash: sha256(token),
    customerRef: options.customerRef,
    connectionName: options.connectionName,
    loginUrl: loginOrigin(options.loginUrl ?? "https://login.salesforce.com"),
    createdBy: options.createdBy,
    createdAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + (options.ttlMs ?? LINK_TTL_MS)).toISOString(),
    usedAt: null,
    revokedAt: null,
  });
  return { url: `${assertBaseUrl(options.baseUrl)}/connect/${token}`, link };
}

/** The token from the path of a link the admin opened. */
export function tokenFromLink(url: string): string {
  const match = new URL(url).pathname.match(/\/connect\/([A-Za-z0-9_-]{43})$/);
  if (!match) throw new ConnectLinkError("unknown");
  return match[1]!;
}

/** Finds the link a presented token belongs to, and says why it can't be used if it can't. */
export function findUsableLink(links: Iterable<ConnectLink>, token: string, now: Date = new Date()): ConnectLink {
  if (!/^[A-Za-z0-9_-]{43}$/.test(token)) throw new ConnectLinkError("unknown");
  const hash = sha256(token);
  let found: ConnectLink | undefined;
  // Every record is compared, so the time taken doesn't hint at a near match.
  for (const link of links) if (sameHash(link.tokenHash, hash)) found = link;
  if (!found) throw new ConnectLinkError("unknown");
  if (found.revokedAt) throw new ConnectLinkError("revoked");
  if (found.usedAt) throw new ConnectLinkError("used");
  if (now.getTime() >= Date.parse(found.expiresAt)) throw new ConnectLinkError("expired");
  return found;
}

/** The admin clicked Continue. Returns where to send them, and what to keep until they're back. */
export function beginAuthorisation(link: ConnectLink, client: OAuthClient, now: Date = new Date()): { authorisationUrl: string; state: string; pending: PendingAuthorisation } {
  const { verifier, challenge } = newPkce();
  const state = newState();
  const pending = PendingAuthorisation.parse({
    linkId: link.id,
    stateHash: sha256(state),
    codeVerifier: verifier,
    createdAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + PENDING_TTL_MS).toISOString(),
  });
  return { authorisationUrl: authorisationUrl({ loginUrl: link.loginUrl, client, state, codeChallenge: challenge }), state, pending };
}

/**
 * Salesforce sent the admin back. Checks the state and timing, then returns the
 * code and verifier to exchange. The caller marks the link used only once the
 * exchange and the read-only check have both succeeded, so a refusal can be retried.
 */
export function completeAuthorisation(pending: PendingAuthorisation, link: ConnectLink, callbackUrl: string, now: Date = new Date()): { code: string; codeVerifier: string } {
  const { code, state } = readCallback(callbackUrl);
  if (pending.linkId !== link.id || !sameHash(sha256(state), pending.stateHash)) throw new ConnectLinkError("state");
  if (now.getTime() >= Date.parse(pending.expiresAt)) throw new ConnectLinkError("pending_expired");
  if (link.revokedAt) throw new ConnectLinkError("revoked");
  if (link.usedAt) throw new ConnectLinkError("used");
  if (now.getTime() >= Date.parse(link.expiresAt)) throw new ConnectLinkError("expired");
  return { code, codeVerifier: pending.codeVerifier };
}

export function markUsed(link: ConnectLink, now: Date = new Date()): ConnectLink {
  return { ...link, usedAt: now.toISOString() };
}

export function revokeLink(link: ConnectLink, now: Date = new Date()): ConnectLink {
  return { ...link, revokedAt: link.revokedAt ?? now.toISOString() };
}
