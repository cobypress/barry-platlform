import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { ClaudeObserver } from "./client.ts";

// How the worker signs in to Claude by OIDC federation, and how it says in the
// log when Claude stops and starts answering (DECISIONS.md 667). Nothing here
// ever logs or returns an identity token or an access token: only times, counts
// and whether the token carries a single-use id.

/**
 * Where the access token is kept between processes. One file per rule,
 * organisation, service account and workspace, so a changed variable never
 * picks up a token minted for the old one.
 */
export function federationCachePath(env: NodeJS.ProcessEnv, dir: string): string {
  const ids = [env.ANTHROPIC_FEDERATION_RULE_ID, env.ANTHROPIC_ORGANIZATION_ID, env.ANTHROPIC_SERVICE_ACCOUNT_ID, env.ANTHROPIC_WORKSPACE_ID].map((v) => v ?? "").join("\n");
  return join(dir, `federation-${createHash("sha256").update(ids).digest("hex").slice(0, 16)}.json`);
}

/**
 * What the log may say about the identity token Render wrote: how old it is,
 * how long it has left, and whether Anthropic will take it only once (a `jti`
 * claim). Read from the token's unsigned middle part; never its text, subject
 * or any id.
 */
export function identityTokenFacts(env: NodeJS.ProcessEnv, now: Date = new Date()): string {
  let jwt: string;
  try {
    jwt = env.ANTHROPIC_IDENTITY_TOKEN_FILE ? readFileSync(env.ANTHROPIC_IDENTITY_TOKEN_FILE, "utf8").trim() : (env.ANTHROPIC_IDENTITY_TOKEN ?? "").trim();
  } catch (err) {
    return `The identity token file couldn't be read (${(err as NodeJS.ErrnoException).code ?? "unknown error"}).`;
  }
  if (!jwt) return "The identity token is empty.";
  let claims: Record<string, unknown>;
  try {
    claims = JSON.parse(Buffer.from(jwt.split(".")[1] ?? "", "base64url").toString("utf8")) as Record<string, unknown>;
    if (!claims || typeof claims !== "object") throw new Error("not an object");
  } catch {
    return "The identity token isn't a JWT the worker can read.";
  }
  const seconds = now.getTime() / 1000;
  const minutes = (n: number): string => `${Math.round(n / 60)} minute${Math.round(n / 60) === 1 ? "" : "s"}`;
  const issued = typeof claims.iat === "number" ? `issued ${minutes(seconds - claims.iat)} ago` : "with no issue time";
  const expires = typeof claims.exp === "number" ? (claims.exp > seconds ? `expires in ${minutes(claims.exp - seconds)}` : `expired ${minutes(seconds - claims.exp)} ago`) : "with no expiry";
  const once = "jti" in claims ? "it carries a single-use id, so Anthropic takes it once only" : "it carries no single-use id";
  return `The identity token was ${issued} and ${expires}; ${once}.`;
}

/** A failure that means Claude won't let this worker in, rather than a busy or unreachable API. */
export function isSignInFailure(err: unknown): boolean {
  const e = err as { status?: unknown; statusCode?: unknown; name?: unknown; message?: unknown } | null;
  if (!e || typeof e !== "object") return false;
  if (e.status === 401 || e.status === 403 || e.statusCode === 401 || e.statusCode === 403) return true;
  if (e.name === "WorkloadIdentityError") return true;
  return typeof e.message === "string" && e.message.startsWith("Could not resolve authentication method");
}

function safeDetail(err: unknown): string {
  const message = err instanceof Error ? err.message.replace(/sk-ant-[A-Za-z0-9_-]+/g, "[removed]").replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_.-]+/g, "[removed]").slice(0, 300) : "an unexpected error";
  return `${err instanceof Error ? err.name : "Error"}: ${message}`;
}

/**
 * Says in the log when Claude's sign-in stops working and when it works again,
 * once each way. It changes nothing a run does: every run still asks Claude,
 * and a run Claude can't serve keeps the engine's own text, as before. The
 * first state comes from the startup check, which has its own line.
 */
export class ClaudeWatch {
  private state: "unknown" | "up" | "down" = "unknown";
  private readonly log: (line: string) => void;
  private readonly facts: () => string;

  constructor(log: (line: string) => void, facts: () => string = () => "") {
    this.log = log;
    this.facts = facts;
  }

  get up(): boolean {
    return this.state === "up";
  }

  /** Records whether Claude let the worker in. Logs only a change from a known state. */
  note(ok: boolean, detail: string): void {
    const before = this.state;
    this.state = ok ? "up" : "down";
    if (before === "down" && ok) this.log(`Claude sign-in works again: ${detail}. Runs from now on get Claude's drafts.`);
    if (before === "up" && !ok) this.log(`Claude sign-in stopped working, so runs use the engine's own text until it works again. ${detail} ${this.facts()}`.trim());
  }

  /** For the client wrapper (`anthropicClient`), which tells it how every call went. */
  readonly observe: ClaudeObserver = (outcome) => {
    if (outcome.ok) this.note(true, `${outcome.model} answered`);
    else if (isSignInFailure(outcome.error)) this.note(false, safeDetail(outcome.error));
  };
}

/** Minutes between the startup check's tries after a failure: about an hour in all, longer than Render's identity token lives. */
export const SIGN_IN_RETRY_MINUTES = [1, 2, 4, 8, 15, 15, 15] as const;
