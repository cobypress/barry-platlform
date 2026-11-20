import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { myDomainHost } from "../connect/instance-host.ts";
import { readOrgStages } from "../connect/org-stages.ts";
import { checkReadOnlyUser, permissionNote, type ReadOnlyCheck } from "../connect/read-only-check.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import { exchangeCode, OAuthError, revokeToken, type OAuthClient, type TokenSet } from "../salesforce/oauth.ts";
import { OAuthConnectionProvider, revokeConnection } from "../salesforce/oauth-provider.ts";
import { StoredConnection } from "../salesforce/token-store.ts";
import { fetchOAuthTransport } from "../salesforce/transport.ts";
import { longDate } from "../render/format.ts";
import { PendingAuthorisation, type ConnectLink } from "../schemas/connect.ts";
import { APP_NOT_SET_UP_AT_CONNECT, appFor, appForPending } from "./apps.ts";
import { openConnectSecret } from "./connect-crypto.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import { withJobLock } from "./lock.ts";
import { approvedFromConsole } from "./run-job.ts";
import { ORG_AWAITING_CONNECT } from "./types.ts";

// A connect job: the admin came back from Salesforce, the console checked the
// state and timing (completeAuthorisation), sealed the code and the pending
// authorisation, set the link to pending and queued this. The worker opens
// them, exchanges the code, stores the tokens encrypted, reads what the user
// can do and marks the link used. Any user of the right org is accepted: what
// they can do is a note for the consultant, because Barometer's reads are held
// to its request list, not to the user (DECISIONS 151).
//
// Every outcome the console shows is plain text from this file. A token, code
// or verifier never reaches the store, the log or a thrown error.

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export const START_AGAIN = "Start again from the link.";

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
    // The app the authorise URL used, and only that one (DECISIONS.md 616). Not set up here: nothing is sent.
    const app = appForPending(pending, deps);
    if (!app) throw new Refusal(APP_NOT_SET_UP_AT_CONNECT);

    // A connection under this name already? If it's bound to this link and this
    // sign-in, it's ours from an earlier delivery that stopped after storing it:
    // the code is spent, so carry on to the check. Bound to this link but an
    // earlier sign-in, it was abandoned, so it goes. Anything else is an older
    // connection, kept aside in case the new sign-in is refused.
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
      const tokens = await exchange(link, app, code, pending.codeVerifier, deps, secrets);
      // One free scan per org every 30 days (DECISIONS.md 610), decided before the tokens are kept.
      if (job.scanRunId) {
        const refusal = await scanCap(tokens.orgId, job.scanRunId, deps, log);
        if (refusal) {
          await revokeQuietly(tokens, deps, log);
          throw new Refusal(refusal);
        }
      }
      // Caught here, before anything is read, rather than when the run starts.
      if (job.runOrgId !== ORG_AWAITING_CONNECT && job.runOrgId.slice(0, 15) !== tokens.orgId.slice(0, 15)) {
        await revokeQuietly(tokens, deps, log);
        throw new Refusal(`You signed in to a different Salesforce org (${tokens.orgId.slice(0, 15)}) from the one this health check is for (${job.runOrgId.slice(0, 15)}), so we disconnected it and read nothing. Sign in to the right org, or ask your Black Cloud consultant.`);
      }
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
          clientId: app.clientId,
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
    // What the user can do is read for the consultant, and never refuses them.
    let check: ReadOnlyCheck;
    // Kept for the stage read after the link is marked used: no second connect, one budget.
    let client: SalesforceClient | undefined;
    try {
      // The connection's own app: a delivery that found its tokens stored earlier made them with the app it holds.
      const provider = new OAuthConnectionProvider(name, { store: deps.tokens, client: appFor(stored.clientId, deps) ?? undefined, transport: deps.oauthTransport, env: approvedFromConsole(name) });
      client = await SalesforceClient.connect(provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: deps.dataTransport, sleep: deps.dataTransport ? async () => {} : undefined });
      check = await checkReadOnlyUser(client, stored.userId);
    } catch (err) {
      // A retry finds the stored connection and reads again. On the last try,
      // the connection is kept and the note says what couldn't be read.
      if (!attempt.final) throw err;
      check = { status: "unknown", reason: `the check failed (${safeReason(err, secrets)})` };
    }
    const note = permissionNote(check);
    // The org's My Domain, for the portal's links into Setup (DECISIONS.md 626),
    // or null. It can't throw, and a database without its column marks the link
    // used as before, so it adds no way for a connect to fail. Never logged.
    const instanceHost = myDomainHost(stored.instanceUrl);
    try {
      await deps.store.markLinkUsed(linkId, { at: at(), orgId: stored.orgId, userId: stored.userId, note }, instanceHost);
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
    // A free scan needs no consultant from here: it runs as soon as the prospect has connected.
    if (job.scanRunId && deps.queueRun) {
      try {
        await deps.queueRun(job.scanRunId);
        await deps.store.recordRunQueued(job.scanRunId, at());
        log(`Scan ${job.scanRunId} is queued.`);
      } catch (err) {
        log(`Scan ${job.scanRunId} couldn't be queued, so it waits for "Start the scan": ${safeReason(err)}`);
      }
    }
    // The org's stages for the calibration screen (DECISIONS.md 611). It can't throw, and has its own time limit.
    if (job.runId) {
      const runId = job.runId;
      if (client) await readOrgStages(client, (stages, readAt) => deps.store.saveOrgStages(runId, stages, readAt), { log, now: at, timeoutMs: deps.stagesTimeoutMs });
      else log("The org's opportunity stages weren't saved for the calibration: the connection couldn't be opened to read them.");
    }
    return { outcome: "connected", detail: `Connected "${name}" to org ${stored.orgId}. ${note}` };
  } catch (err) {
    if (err instanceof Refusal) return refuse(err.message);
    if (!attempt.final && transient(err)) throw new Error(safeReason(err, secrets));
    if (err instanceof OAuthError) return refuse(exchangeReason(err));
    return refuse(`Something went wrong connecting: ${safeReason(err, secrets)}. ${START_AGAIN}`);
  }
}

export const SCAN_EVERY_DAYS = 30;
const DAY_MS = 86_400_000;

/**
 * The customer's refusal when another free scan was made for or connected to
 * this org in the last 30 days, or null. The scan asking is left out, so its
 * own earlier attempts never count. If the store can't say, the scan is refused
 * too: the code is spent, so the tokens can't wait for a retry.
 */
async function scanCap(orgId: string, scanRunId: string, deps: WorkerDeps, log: (line: string) => void): Promise<string | null> {
  const now = (deps.now ?? (() => new Date()))();
  let last: string | null;
  try {
    last = await deps.store.lastScanOfOrg(orgId.slice(0, 15), scanRunId, new Date(now.getTime() - SCAN_EVERY_DAYS * DAY_MS).toISOString());
  } catch (err) {
    // Said, or a renamed console column would refuse every scan in silence. The SQLSTATE only: a driver's message can quote a value.
    const code = (err as { code?: unknown } | null)?.code;
    log(`The free-scan cap couldn't be checked: SQLSTATE ${typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : "no code"}`);
    return `We couldn't check whether this Salesforce org has had a free scan recently, so we disconnected it and read nothing. ${START_AGAIN}`;
  }
  if (!last) return null;
  const after = longDate(new Date(Date.parse(last) + SCAN_EVERY_DAYS * DAY_MS).toISOString());
  return `This Salesforce org had a free Barometer Scan in the last 30 days. You can run another after ${after}, or talk to us about a plan that scans every month.`;
}

async function exchange(link: ConnectLink, app: OAuthClient, code: string, codeVerifier: string, deps: WorkerDeps, secrets: Array<string | undefined>): Promise<TokenSet> {
  const tokens = await exchangeCode({ transport: deps.oauthTransport ?? fetchOAuthTransport, loginUrl: link.loginUrl, client: app, code, codeVerifier });
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
