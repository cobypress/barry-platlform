import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { checkReadOnlyUser, type ReadOnlyCheck } from "../connect/read-only-check.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import { exchangeCode, OAuthError, revokeToken, type TokenSet } from "../salesforce/oauth.ts";
import { OAuthConnectionProvider, revokeConnection } from "../salesforce/oauth-provider.ts";
import { StoredConnection } from "../salesforce/token-store.ts";
import { fetchOAuthTransport } from "../salesforce/transport.ts";
import { PendingAuthorisation, type ConnectLink } from "../schemas/connect.ts";
import { openConnectSecret } from "./connect-crypto.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import { withJobLock } from "./lock.ts";
import { approvedFromConsole } from "./run-job.ts";

// A connect job: the admin came back from Salesforce, the console checked the
// state and timing (completeAuthorisation), sealed the code and the pending
// authorisation, set the link to pending and queued this. The worker opens
// them, exchanges the code, stores the tokens encrypted, and proves the user
// can't write (DECISIONS 63 and 64). Only then is the link used.
//
// Every outcome the console shows is plain text from this file. A token, code
// or verifier never reaches the store, the log or a thrown error.

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const START_AGAIN = "Start again from the link.";

/** Turns PermissionsModifyAllData into "Modify All Data". */
const readable = (permission: string): string => permission.replace(/^Permissions/, "").replace(/([a-z])([A-Z])/g, "$1 $2");

export function canWriteReason(check: Extract<ReadOnlyCheck, { status: "can_write" }>): string {
  const what = [...check.systemPermissions.map(readable), ...check.objects.map((o) => `create, edit or delete on ${o}`)];
  return `The user you signed in as can change data in Salesforce (${what.join("; ")}), so we disconnected straight away and kept nothing. Sign in as a user who holds only Barometer Read Only.`;
}

/** What an OAuth error from the code exchange means to the admin. Only the code is used, never Salesforce's description. */
function exchangeReason(err: OAuthError): string {
  if (err.code === "invalid_grant") return `Salesforce didn't accept the sign-in. It may have taken too long, or been used already. ${START_AGAIN}`;
  if (err.code === "invalid_client" || err.code === "invalid_client_id") return "Salesforce didn't recognise Barometer's app. Ask your Black Cloud consultant to check the installation.";
  if (err.code === "BAD_SCOPE") return `Salesforce didn't grant API access. Approve both permissions Barometer asks for. ${START_AGAIN}`;
  if (err.code === "BAD_TOKEN_RESPONSE") return `Salesforce's reply wasn't one we could use. ${START_AGAIN}`;
  return `Salesforce refused the sign-in (${err.code}). ${START_AGAIN}`;
}

/** A 5xx or a network failure may pass. Anything Salesforce said no to won't. */
const transient = (err: unknown): boolean => !(err instanceof OAuthError) || /^HTTP_5\d\d$/.test(err.code);

class Refusal extends Error {
  constructor(reason: string) {
    super(reason);
    this.name = "Refusal";
  }
}

export async function processConnectJob(linkId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  if (!UUID.test(linkId)) return { outcome: "failed", detail: "The link id isn't a UUID" };
  return withJobLock(deps.locksDir, `connect-${linkId.toLowerCase()}`, () => connectLocked(linkId, deps, attempt));
}

/** A refusal decided but not yet recorded, kept on the worker's disk so a retry records the same one without a second exchange. */
const journalPath = (deps: WorkerDeps, linkId: string): string => join(deps.locksDir, `connect-${linkId.toLowerCase()}.refusal.json`);

function readJournal(path: string): string | null {
  try {
    const reason = (JSON.parse(readFileSync(path, "utf8")) as { reason?: unknown }).reason;
    return typeof reason === "string" && reason ? reason : null;
  } catch {
    return null;
  }
}

async function connectLocked(linkId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const at = () => (deps.now ?? (() => new Date()))().toISOString();
  const job = await deps.store.loadConnectJob(linkId);
  if (!job) return { outcome: "skipped", detail: `No link ${linkId}` };
  const { link } = job;
  const name = link.connectionName;
  const journal = journalPath(deps, linkId);
  const secrets: Array<string | undefined> = [];
  // Set once tokens from this sign-in are in the store, so any failure after that forgets them.
  let holding = false;
  let previous: StoredConnection | undefined;

  /** Revokes and forgets whatever the store holds from this link, whichever sign-in it came from. */
  const releaseBound = async (): Promise<boolean> => {
    if (!deps.tokens.has(name)) return false;
    const held = await deps.tokens.load(name).catch(() => undefined);
    if (held?.fromLink?.linkId !== link.id) return false;
    secrets.push(held.accessToken, held.refreshToken);
    await disconnect(name, deps, log);
    return true;
  };

  /** Forgets this sign-in's tokens, revoking them at Salesforce, and puts back any older connection. */
  const letGo = async (): Promise<boolean> => {
    if (!holding) return true;
    const revoked = await disconnect(name, deps, log);
    holding = false;
    if (previous) await deps.tokens.save(previous);
    return revoked;
  };

  /**
   * Written down first, then the tokens go, then the console is told. If the
   * console can't be told, the next delivery finds the note and tells it the
   * same thing, rather than spending the code again and losing the reason.
   */
  const refuse = async (reason: string): Promise<JobOutcome> => {
    const text = safeReason(new Error(reason), secrets);
    if (readJournal(journal) !== text) {
      mkdirSync(deps.locksDir, { recursive: true, mode: 0o700 });
      writeFileSync(journal, JSON.stringify({ reason: text, at: at() }), { mode: 0o600 });
    }
    await letGo();
    await releaseBound();
    await deps.store.refuseLink(linkId, text, at());
    rmSync(journal, { force: true });
    return { outcome: "refused", detail: text };
  };

  // Settled already, by an earlier delivery of this job or by the console. A
  // link that never connected keeps no tokens: an earlier delivery may have
  // stored some before the console withdrew it.
  if (job.state !== "pending") {
    rmSync(journal, { force: true });
    const released = job.state !== "used" && !link.usedAt && (await releaseBound());
    return { outcome: "skipped", detail: `The link is ${job.state}, so there's nothing to do${released ? ". We disconnected the tokens an earlier attempt had stored" : ""}` };
  }
  const decided = readJournal(journal);
  if (decided) return refuse(decided);

  try {
    if (link.revokedAt) throw new Refusal("This link was withdrawn. Ask your Black Cloud consultant for a new one.");
    if (link.usedAt) throw new Refusal("This link has already been used to connect an org.");
    if (!job.sealedPending || !job.sealedCode) throw new Refusal(`The sign-in wasn't saved properly. ${START_AGAIN}`);

    let pending: PendingAuthorisation;
    let code: string;
    try {
      code = openConnectSecret(job.sealedCode, deps.connectKey, link.id);
      secrets.push(code);
      pending = PendingAuthorisation.parse(JSON.parse(openConnectSecret(job.sealedPending, deps.connectKey, link.id)));
      secrets.push(pending.codeVerifier);
    } catch {
      throw new Refusal(`We couldn't read the saved sign-in. ${START_AGAIN}`);
    }
    if (pending.linkId !== link.id) throw new Refusal(`The saved sign-in belongs to a different link. ${START_AGAIN}`);

    // A connection under this name already? If it's bound to this link and this
    // sign-in, it's ours from an earlier delivery that stopped after storing it:
    // the code is spent, so carry on to the check. Bound to this link but an
    // earlier sign-in, it was abandoned, so it goes. Anything else is an older
    // connection, kept aside in case the new user turns out able to write.
    let existing = deps.tokens.has(name) ? await deps.tokens.load(name) : undefined;
    if (existing) secrets.push(existing.accessToken, existing.refreshToken);
    const ours = existing?.fromLink?.linkId === link.id && existing.fromLink.stateHash === pending.stateHash;
    if (existing && !ours && existing.fromLink?.linkId === link.id) {
      await releaseBound();
      existing = undefined;
    }
    previous = ours ? undefined : existing;
    holding = ours;

    if (!ours) {
      const tokens = await exchange(link, code, pending.codeVerifier, deps, secrets);
      if (previous && previous.orgId.slice(0, 15) !== tokens.orgId.slice(0, 15)) {
        await revokeQuietly(tokens, deps, log);
        throw new Refusal(`You signed in to a different Salesforce org from the one Barometer already holds for this customer, so we disconnected it. Ask your Black Cloud consultant.`);
      }
      if (!tokens.refreshToken) {
        await revokeQuietly(tokens, deps, log);
        throw new Refusal("Salesforce didn't let Barometer keep access. Check the app allows the refresh_token scope, then start again from the link.");
      }
      await deps.tokens.save(
        StoredConnection.parse({
          version: 1,
          name,
          grant: "web-server",
          loginUrl: link.loginUrl,
          clientId: deps.oauthClient.clientId,
          instanceUrl: tokens.instanceUrl,
          orgId: tokens.orgId,
          userId: tokens.userId,
          scope: tokens.scope,
          accessToken: tokens.accessToken,
          refreshToken: tokens.refreshToken,
          issuedAt: tokens.issuedAt,
          connectedAt: at(),
          fromLink: { linkId: link.id, stateHash: pending.stateHash },
        }),
      );
      holding = true;
    }

    const stored = await deps.tokens.load(name);
    secrets.push(stored.accessToken, stored.refreshToken);
    let check: ReadOnlyCheck;
    try {
      const provider = new OAuthConnectionProvider(name, { store: deps.tokens, client: deps.oauthClient, transport: deps.oauthTransport, env: approvedFromConsole(name) });
      const client = await SalesforceClient.connect(provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: deps.dataTransport, sleep: deps.dataTransport ? async () => {} : undefined });
      check = await checkReadOnlyUser(client, stored.userId);
    } catch (err) {
      // A retry finds the stored connection and checks again. On the last try,
      // a connection nobody could check isn't kept.
      if (!attempt.final) throw err;
      throw new Refusal(`We couldn't check the user's permissions (${safeReason(err, secrets)}), so we disconnected. ${START_AGAIN}`);
    }

    if (check.status === "can_write") {
      // Revoked here, before the refusal is written, so the reason can say whether Salesforce confirmed it.
      const revoked = await letGo();
      const reason = canWriteReason(check);
      throw new Refusal(revoked ? reason : `${reason} Salesforce didn't confirm the disconnection, so also remove Barometer's access under Connected Apps OAuth Usage in Setup.`);
    }

    const note =
      check.status === "unknown"
        ? `Connected, but we couldn't prove the user is read only: ${check.reason}. Check the user holds only Barometer Read Only before running.`
        : null;
    try {
      await deps.store.markLinkUsed(linkId, { at: at(), orgId: stored.orgId, userId: stored.userId, note });
    } catch {
      // Tokens for a link that isn't marked used must not stay usable, so they
      // go first, before anything that could fail or be retried.
      await letGo();
      const now = await deps.store.loadConnectJob(linkId).catch(() => null);
      if (now && now.state !== "pending") {
        return { outcome: "skipped", detail: `The link became ${now.state} while we were connecting, so we disconnected and kept nothing` };
      }
      throw new Refusal(`We connected, but couldn't record it, so we disconnected to be safe. ${START_AGAIN}`);
    }
    // The older connection's refresh token is left to lapse (30 days unused,
    // DECISIONS 58) rather than revoked: revoking it might reach the new grant.
    if (previous) log(`Replaced the connection "${name}". The old refresh token wasn't revoked.`);
    return { outcome: "connected", detail: note ?? `Connected "${name}" read only, to org ${stored.orgId}` };
  } catch (err) {
    if (err instanceof Refusal) return refuse(err.message);
    if (!attempt.final && transient(err)) throw new Error(safeReason(err, secrets));
    if (err instanceof OAuthError) return refuse(exchangeReason(err));
    return refuse(`Something went wrong connecting: ${safeReason(err, secrets)}. ${START_AGAIN}`);
  }
}

async function exchange(link: ConnectLink, code: string, codeVerifier: string, deps: WorkerDeps, secrets: Array<string | undefined>): Promise<TokenSet> {
  const tokens = await exchangeCode({ transport: deps.oauthTransport ?? fetchOAuthTransport, loginUrl: link.loginUrl, client: deps.oauthClient, code, codeVerifier });
  secrets.push(tokens.accessToken, tokens.refreshToken);
  return tokens;
}

async function revokeQuietly(tokens: TokenSet, deps: WorkerDeps, log: (line: string) => void): Promise<void> {
  try {
    await revokeToken({ transport: deps.oauthTransport ?? fetchOAuthTransport, instanceUrl: tokens.instanceUrl, token: tokens.refreshToken ?? tokens.accessToken });
  } catch (err) {
    log(`Salesforce didn't confirm a revocation: ${err instanceof OAuthError ? err.code : "error"}`);
  }
}

/** Revokes at Salesforce and forgets the tokens. If Salesforce doesn't answer, the tokens are still forgotten here. */
async function disconnect(name: string, deps: WorkerDeps, log: (line: string) => void): Promise<boolean> {
  try {
    await revokeConnection({ name, store: deps.tokens, transport: deps.oauthTransport });
    return true;
  } catch (err) {
    log(`Salesforce didn't confirm revoking "${name}": ${err instanceof OAuthError ? err.code : "error"}`);
    deps.tokens.delete(name);
    return false;
  }
}
