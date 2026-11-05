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

export class OAuthConnectionProvider implements ConnectionProvider {
  readonly kind = "oauth";
  readonly #name: string;
  readonly #store: TokenStore;
  readonly #client?: Pick<OAuthClient, "clientId" | "clientSecret">;
  readonly #transport: OAuthTransport;
  readonly #now: () => number;
  #current?: StoredConnection;

  constructor(
    name: string,
    options: { store: TokenStore; client?: Pick<OAuthClient, "clientId" | "clientSecret">; transport?: OAuthTransport; env?: NodeJS.ProcessEnv; now?: () => number },
  ) {
    if (!approvedAliases(options.env ?? process.env).includes(name)) throw new OrgNotApprovedError(name);
    this.#name = name;
    this.#store = options.store;
    this.#client = options.client;
    this.#transport = options.transport ?? fetchOAuthTransport;
    this.#now = options.now ?? Date.now;
  }

  async connect(): Promise<OrgConnection> {
    this.#current = await this.#store.load(this.#name);
    return toConnection(this.#current);
  }

  /** Called by the client after a 401, or an expired session in SOAP. */
  async refresh(): Promise<OrgConnection> {
    const stored = this.#current ?? (await this.#store.load(this.#name));
    const fresh = stored.grant === "jwt-bearer" ? await this.#reassert(stored) : await this.#rotate(stored);
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
    return toConnection(next);
  }

  #rotate(stored: StoredConnection): Promise<TokenSet> {
    if (!stored.refreshToken) throw new Error(`The connection "${this.#name}" has no refresh token. Connect it again`);
    if (!this.#client || this.#client.clientId !== stored.clientId) throw new Error("The OAuth client doesn't match the one this connection was made with");
    return refreshTokens({ transport: this.#transport, instanceUrl: stored.instanceUrl, client: this.#client, refreshToken: stored.refreshToken }).catch((err: unknown) => {
      if (err instanceof OAuthError && err.code === "invalid_grant") {
        throw new OAuthError("invalid_grant", `The connection "${this.#name}" was revoked or expired. The customer's admin needs to connect again`);
      }
      throw err;
    });
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
