import { readFileSync, statSync } from "node:fs";
import type { ConnectionProvider, OrgConnection } from "./connection.ts";
import { jwtBearer, OAuthError, refreshTokens, revokeToken, type OAuthClient, type TokenSet } from "./oauth.ts";
import { approvedAliases, OrgNotApprovedError } from "./sf-cli-provider.ts";
import type { StoredConnection, TokenStore } from "./token-store.ts";
import { fetchOAuthTransport, type OAuthTransport } from "./transport.ts";

// A connection made through Barometer's External Client App: the customer's
// admin authorised it, so nobody shared a login. The token is the user's, who
// may be able to write: every request is held to the allowlist (DECISIONS 151).
// Tokens live encrypted in a TokenStore.
//
// The name must still be in HC_APPROVED_ORG_ALIASES, as for the CLI provider:
// this machine can hold connections to customer orgs, and a typo mustn't reach one.

/**
 * Salesforce refused the refresh token: it expired (30 days unused in a
 * production org) or was revoked. It never works again, with any app, so the
 * worker ends the run as needs reconnection rather than retrying (DECISIONS.md 617).
 */
export class ReconnectionNeeded extends OAuthError {
  constructor(name: string) {
    super("invalid_grant", `The connection "${name}" was revoked or expired. The customer's admin needs to connect again`);
    this.name = "ReconnectionNeeded";
  }
}

export class OAuthConnectionProvider implements ConnectionProvider {
  readonly kind = "oauth";
  readonly #name: string;
  readonly #store: TokenStore;
  readonly #client?: Pick<OAuthClient, "clientId" | "clientSecret">;
  readonly #transport: OAuthTransport;
  readonly #now: () => number;
  readonly #log: (line: string) => void;
  #current?: StoredConnection;
  #lostAccess = false;
  /**
   * The tokens held came from the store after a refusal, and Salesforce hasn't
   * yet accepted their access token. While set, a refusal doesn't re-load
   * again: that is the same refresh attempt, and looking again could hand the
   * connection back and forth between two processes.
   */
  #adopted = false;

  constructor(
    name: string,
    options: { store: TokenStore; client?: Pick<OAuthClient, "clientId" | "clientSecret">; transport?: OAuthTransport; env?: NodeJS.ProcessEnv; now?: () => number; log?: (line: string) => void },
  ) {
    if (!approvedAliases(options.env ?? process.env).includes(name)) throw new OrgNotApprovedError(name);
    this.#name = name;
    this.#store = options.store;
    this.#client = options.client;
    this.#transport = options.transport ?? fetchOAuthTransport;
    this.#now = options.now ?? Date.now;
    this.#log = options.log ?? (() => {});
  }

  /**
   * True once Salesforce has refused this connection's refresh token. A read
   * turns the error into an unread source, so the run asks here as well as
   * looking at what was thrown.
   */
  get lostAccess(): boolean {
    return this.#lostAccess;
  }

  async connect(): Promise<OrgConnection> {
    this.#current = await this.#store.load(this.#name);
    return toConnection(this.#current);
  }

  /**
   * The client says a request succeeded with the token last returned. Adopted
   * tokens that worked are this provider's own from now on, so a later,
   * separate rotation by another process may be looked for again (DECISIONS.md 631).
   */
  accepted(): void {
    this.#adopted = false;
  }

  /** Called by the client after a 401, or an expired session in SOAP. */
  async refresh(): Promise<OrgConnection> {
    const stored = this.#current ?? (await this.#store.load(this.#name));
    let fresh: TokenSet;
    if (stored.grant === "jwt-bearer") fresh = await this.#reassert(stored);
    else {
      try {
        fresh = await this.#rotate(stored);
      } catch (err) {
        if (!(err instanceof OAuthError && err.code === "invalid_grant")) throw err;
        // Another worker process may have refreshed first: with rotation, its
        // refresh ended the token this one holds, and the newer one is in the
        // store. Look once per refresh attempt: not again while adopted tokens
        // are still unproven, so two processes can't hand the connection back and forth (DECISIONS.md 631).
        const newer = this.#adopted ? undefined : await this.#newerInStore(stored);
        if (!newer) {
          this.#lostAccess = true;
          throw new ReconnectionNeeded(this.#name);
        }
        this.#current = newer;
        this.#adopted = true;
        this.#log(`A newer token for the connection "${this.#name}" was found in the store; trying it.`);
        // Its access token is tried first. Refused too, the client asks again, and that refresh uses its refresh token.
        return { ...toConnection(newer), fromStore: true };
      }
    }
    // A refresh must come back for the same org and user. Anything else is
    // refused before the new token is stored or used.
    if (fresh.orgId !== stored.orgId || fresh.userId !== stored.userId) {
      throw new OAuthError("IDENTITY_CHANGED", "The refreshed token is for a different org or user");
    }
    const next: StoredConnection = {
      ...stored,
      accessToken: fresh.accessToken,
      refreshToken: fresh.refreshToken ?? stored.refreshToken,
      instanceUrl: fresh.instanceUrl,
      scope: fresh.scope.length > 0 ? fresh.scope : stored.scope,
      issuedAt: fresh.issuedAt,
    };
    // Stored before it's used: with rotation, the old refresh token is already dead.
    await this.#store.save(next);
    this.#current = next;
    this.#adopted = false;
    return toConnection(next);
  }

  /**
   * The stored connection, if it holds a different refresh token from the one
   * Salesforce just refused, for the same name, app, org and user. Anything
   * else, or a store that can't be read, is not used: the refusal stands.
   */
  async #newerInStore(refused: StoredConnection): Promise<StoredConnection | undefined> {
    let latest: StoredConnection;
    try {
      latest = await this.#store.load(this.#name);
    } catch {
      return undefined;
    }
    const same = latest.name === this.#name && latest.grant === "web-server" && latest.clientId === refused.clientId && latest.orgId === refused.orgId && latest.userId === refused.userId;
    return same && latest.refreshToken !== undefined && latest.refreshToken !== refused.refreshToken ? latest : undefined;
  }

  #rotate(stored: StoredConnection): Promise<TokenSet> {
    if (!stored.refreshToken) throw new Error(`The connection "${this.#name}" has no refresh token. Connect it again`);
    if (!this.#client || this.#client.clientId !== stored.clientId) throw new Error("The OAuth client doesn't match the one this connection was made with");
    return refreshTokens({ transport: this.#transport, instanceUrl: stored.instanceUrl, client: this.#client, refreshToken: stored.refreshToken });
  }

  #reassert(stored: StoredConnection): Promise<TokenSet> {
    if (!stored.jwt) throw new Error(`The connection "${this.#name}" has no JWT settings`);
    if ((statSync(stored.jwt.privateKeyPath).mode & 0o077) !== 0) throw new Error(`${stored.jwt.privateKeyPath} is readable by other users. Make it 600 first`);
    return jwtBearer({
      transport: this.#transport,
      tokenUrl: stored.loginUrl,
      clientId: stored.clientId,
      username: stored.jwt.username,
      audience: stored.jwt.audience,
      privateKey: readFileSync(stored.jwt.privateKeyPath, "utf8"),
      now: this.#now(),
    });
  }
}

function toConnection(stored: StoredConnection): OrgConnection {
  return { instanceUrl: stored.instanceUrl, accessToken: stored.accessToken, orgId: stored.orgId };
}

/** Revokes at Salesforce first, then forgets the tokens. A JWT connection has nothing long-lived to revoke. */
export async function revokeConnection(options: { name: string; store: TokenStore; transport?: OAuthTransport }): Promise<void> {
  const stored = await options.store.load(options.name);
  const transport = options.transport ?? fetchOAuthTransport;
  await revokeToken({ transport, instanceUrl: stored.instanceUrl, token: stored.refreshToken ?? stored.accessToken });
  options.store.delete(options.name);
}

/** What a person may see about a connection. Never a token. */
export function describeConnection(stored: StoredConnection): Record<string, string> {
  return {
    name: stored.name,
    grant: stored.grant,
    org: stored.orgId,
    user: stored.userId,
    instance: new URL(stored.instanceUrl).hostname,
    scope: stored.scope.join(" "),
    connected: stored.connectedAt,
    refreshToken: stored.refreshToken ? "held" : "none",
  };
}
