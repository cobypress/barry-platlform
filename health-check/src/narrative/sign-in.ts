import { createHash } from "node:crypto";
import { accessSync, chmodSync, constants, lstatSync, mkdirSync, readFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { isSignInFailure, type ClaudeObserver } from "./client.ts";

// How the worker signs in to Claude by OIDC federation, and how it says in the
// log when Claude stops and starts answering (DECISIONS.md 667). Nothing here
// ever logs or returns an identity token or an access token: only times, counts
// and whether the token carries a single-use id.

/**
 * Where the access token is kept between processes. One file per rule,
 * organisation, service account and workspace, so a change to one of those
 * four ids never picks up a token minted for the old one. Nothing else is in
 * the name: a changed ANTHROPIC_BASE_URL, or a rule edited under the same id,
 * reads the kept token until it runs out.
 */
export function federationCachePath(env: NodeJS.ProcessEnv, dir: string): string {
  const ids = [env.ANTHROPIC_FEDERATION_RULE_ID, env.ANTHROPIC_ORGANIZATION_ID, env.ANTHROPIC_SERVICE_ACCOUNT_ID, env.ANTHROPIC_WORKSPACE_ID].map((v) => v ?? "").join("\n");
  return join(dir, `federation-${createHash("sha256").update(ids).digest("hex").slice(0, 16)}.json`);
}

/**
 * Makes the folder the access token is kept in safe before the SDK uses it,
 * and says once whether the token can be kept. Returns the file's path when it
 * can, or null, and the SDK then keeps the token in memory only. Never throws:
 * a cache that can't be used costs a fresh sign-in after a restart, not the
 * worker's start. The SDK refuses a group- or world-readable file on every
 * call, so one found here is deleted rather than left to stop Claude.
 */
export function prepareSignInCache(dir: string, file: string, log: (line: string) => void): string | null {
  const uid = typeof process.getuid === "function" ? process.getuid() : null;
  const notKept = (why: string): null => {
    log(`Claude's access token can't be kept on the disk (${why}), so a worker that starts again in place signs in afresh.`);
    return null;
  };
  try {
    try {
      mkdirSync(dir, { recursive: true, mode: 0o700 });
    } catch (err) {
      return notKept((err as NodeJS.ErrnoException).code ?? "the folder couldn't be made");
    }
    const folder = lstatSync(dir);
    if (folder.isSymbolicLink() || !folder.isDirectory() || (uid !== null && folder.uid !== uid)) return notKept("its folder is a link, not a folder, or another user's");
    chmodSync(dir, 0o700);
    let found: ReturnType<typeof lstatSync> | null = null;
    try {
      found = lstatSync(file);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
    // unlink removes a link itself, never what it points at.
    if (found && (found.isSymbolicLink() || !found.isFile() || (uid !== null && found.uid !== uid) || (found.mode & 0o077) !== 0)) {
      unlinkSync(file);
      log("A kept Claude access token wasn't safe to use (a link, another user's, or readable by others), so it was deleted and the worker signs in afresh.");
    }
    try {
      accessSync(dir, constants.W_OK);
    } catch {
      return notKept("its folder isn't writable");
    }
    log("Claude's access token is kept on the disk, so a worker that starts again in place uses it while it lasts instead of signing in again.");
    return file;
  } catch (err) {
    return notKept((err as NodeJS.ErrnoException).code ?? "unexpected error");
  }
}

// Times outside these, or not finite numbers, are said as unknown.
const SANE_FROM = 946_684_800; // 2000-01-01
const SANE_TO = 4_102_444_800; // 2100-01-01
const sane = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v) && v >= SANE_FROM && v <= SANE_TO;

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
  // The SDK's own limit, before anything is decoded.
  if (jwt.length > 16 * 1024) return `The identity token is ${Math.ceil(jwt.length / 1024)} KiB, over the 16 KiB Anthropic takes.`;
  let claims: Record<string, unknown>;
  try {
    claims = JSON.parse(Buffer.from(jwt.split(".")[1] ?? "", "base64url").toString("utf8")) as Record<string, unknown>;
    if (!claims || typeof claims !== "object" || Array.isArray(claims)) throw new Error("not an object");
  } catch {
    return "The identity token isn't a JWT the worker can read.";
  }
  const seconds = now.getTime() / 1000;
  const minutes = (n: number): string => `${Math.round(n / 60)} minute${Math.round(n / 60) === 1 ? "" : "s"}`;
  const issued = !sane(claims.iat) ? "issued at an unknown time" : claims.iat > seconds ? `issued ${minutes(claims.iat - seconds)} in the future` : `issued ${minutes(seconds - claims.iat)} ago`;
  const expires = !sane(claims.exp) ? "has an unknown expiry" : claims.exp > seconds ? `expires in ${minutes(claims.exp - seconds)}` : `expired ${minutes(seconds - claims.exp)} ago`;
  const once = "jti" in claims ? "it carries a single-use id, so Anthropic takes it once only" : "it carries no single-use id";
  return `The identity token was ${issued} and ${expires}; ${once}.`;
}

/** An error's class name (the SDK's classes leave `name` as "Error") and message, with no token in it. */
export function safeDetail(err: unknown): string {
  const message = err instanceof Error ? err.message.replace(/sk-ant-[A-Za-z0-9_-]+/g, "[removed]").replace(/eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_.-]+/g, "[removed]").slice(0, 300) : "an unexpected error";
  const name = err instanceof Error ? (err.constructor?.name && err.constructor.name !== "Error" ? err.constructor.name : err.name) : "Error";
  return `${name}: ${message}`;
}

/**
 * Says in the log when Claude's sign-in stops working and when it works again,
 * once each way. It changes nothing a run does: every run still asks Claude,
 * and a run Claude can't serve keeps the engine's own text, as before. It is
 * quiet until the startup check has said its own line (`begin`).
 */
export class ClaudeWatch {
  private state: "unknown" | "up" | "down" = "unknown";
  private reporting = false;
  private readonly log: (line: string) => void;
  private readonly facts: () => string;

  constructor(log: (line: string) => void, facts: () => string = () => "") {
    this.log = log;
    this.facts = facts;
  }

  get up(): boolean {
    return this.state === "up";
  }

  get down(): boolean {
    return this.state === "down";
  }

  /** From now on, a change is logged. */
  begin(): void {
    this.reporting = true;
  }

  /** Records whether Claude let the worker in. Logs a failure after anything but a failure, and a success after a failure. */
  note(ok: boolean, detail: string): void {
    const before = this.state;
    this.state = ok ? "up" : "down";
    if (!this.reporting) return;
    if (before === "down" && ok) this.log(`Claude sign-in works again: ${detail}. Runs from now on get Claude's drafts.`);
    if (before !== "down" && !ok) this.log(`Claude sign-in stopped working, so runs use the engine's own text until it works again. ${detail} ${this.facts()}`.trim());
  }

  /** For the client wrapper (`anthropicClient`), which tells it how every call went. A busy or unreachable API changes nothing. */
  readonly observe: ClaudeObserver = (outcome) => {
    if (outcome.ok) this.note(true, `${outcome.model} answered`);
    else if (isSignInFailure(outcome.error)) this.note(false, safeDetail(outcome.error));
  };
}

/** Minutes between the startup check's tries after a failure: about an hour in all, longer than Render's identity token lives. */
export const SIGN_IN_RETRY_MINUTES = [1, 2, 4, 8, 15, 15, 15] as const;
