import { readFileSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { z } from "zod";
import { beginAuthorisation, completeAuthorisation, createConnectLink } from "../connect/link.ts";
import { checkReadOnlyUser, type ReadOnlyCheck } from "../connect/read-only-check.ts";
import { assertOutsideGit } from "../engine/run.ts";
import { ApiBudget } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import { exchangeCode, jwtBearer, loginOrigin, type OAuthClient } from "../salesforce/oauth.ts";
import { describeConnection, OAuthConnectionProvider, revokeConnection } from "../salesforce/oauth-provider.ts";
import { approvedAliases, OrgNotApprovedError } from "../salesforce/sf-cli-provider.ts";
import { ConnectionName, createKeychainKey, StoredConnection, tokenKeySource, TokenStore } from "../salesforce/token-store.ts";
import { fetchOAuthTransport } from "../salesforce/transport.ts";
import { ConnectLink, PendingAuthorisation } from "../schemas/connect.ts";

// `hc connect`: makes and manages OAuth connections on this machine. The console
// does the same for customers in Milestone 5, through the connect link. Nothing
// here prints a token, a secret or a consumer key.

export const CONNECT_USAGE = `
  hc connect key                                     Create the token key in the macOS keychain
  hc connect start --connection <name> [--login-url <url>]
  hc connect finish --connection <name> --callback-url <url>
  hc connect jwt --connection <name> --config <file> Development only: JWT bearer for a pre-authorised user
  hc connect check --connection <name>               Prove the connected user can't write
  hc connect show --connection <name>
  hc connect revoke --connection <name>

The connection name must be listed in HC_APPROVED_ORG_ALIASES. The OAuth client
comes from HC_SF_CLIENT_ID, HC_SF_CLIENT_SECRET and HC_SF_REDIRECT_URI, or from
~/.barometer/oauth-client.json (mode 600).`;

const ClientFile = z.object({ clientId: z.string().min(1), clientSecret: z.string().min(1).optional(), redirectUri: z.string().url() });
const DevJwtFile = z.object({ clientId: z.string().min(1), username: z.string().min(1), instanceUrl: z.string().url(), audience: z.string().url(), privateKeyPath: z.string().min(1) });
const PendingFile = z.object({ link: ConnectLink, pending: PendingAuthorisation });

function ownerOnly(path: string): void {
  if ((statSync(path).mode & 0o077) !== 0) throw new Error(`${path} is readable by other users. Make it 600 first`);
}

export function oauthClient(env: NodeJS.ProcessEnv = process.env, file = join(homedir(), ".barometer", "oauth-client.json")): OAuthClient {
  if (env.HC_SF_CLIENT_ID) {
    if (!env.HC_SF_REDIRECT_URI) throw new Error("HC_SF_REDIRECT_URI is needed with HC_SF_CLIENT_ID");
    return { clientId: env.HC_SF_CLIENT_ID, clientSecret: env.HC_SF_CLIENT_SECRET || undefined, redirectUri: env.HC_SF_REDIRECT_URI };
  }
  try {
    ownerOnly(file);
    return ClientFile.parse(JSON.parse(readFileSync(file, "utf8")));
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") throw new Error(`No OAuth client: set HC_SF_CLIENT_ID or create ${file}`);
    throw err instanceof z.ZodError ? new Error(`${file} needs clientId, redirectUri and usually clientSecret`) : err;
  }
}

export function connectionStore(env: NodeJS.ProcessEnv = process.env): TokenStore {
  const store = new TokenStore({ dir: env.HC_CONNECTIONS_DIR || undefined, key: tokenKeySource({ env }) });
  // Tokens are as sensitive as run data, so the same rule applies (rule 8).
  assertOutsideGit(store.dir);
  return store;
}

async function readOnlyCheck(name: string, store: TokenStore, env: NodeJS.ProcessEnv): Promise<ReadOnlyCheck> {
  const stored = await store.load(name);
  const client = await SalesforceClient.connect(new OAuthConnectionProvider(name, { store, client: tryClient(env), env }), { budget: new ApiBudget({ stopAtShare: 0.5 }) });
  return checkReadOnlyUser(client, stored.userId);
}

function tryClient(env: NodeJS.ProcessEnv): OAuthClient | undefined {
  try {
    return oauthClient(env);
  } catch {
    return undefined;
  }
}

function report(check: ReadOnlyCheck): string {
  if (check.status === "read_only") return `Read only: the user holds ${check.permissionSets.join(", ")}, with no create, edit or delete on any object and no write system permission.`;
  if (check.status === "unknown") return `Not proven read only: ${check.reason}.`;
  return `CAN WRITE: ${[...check.systemPermissions, ...check.objects.map((o) => `edit ${o}`)].join(", ")} (from ${check.permissionSets.join(", ")}).`;
}

export async function connectCommand(sub: string | undefined, values: Record<string, string | boolean | undefined>, env: NodeJS.ProcessEnv = process.env): Promise<number> {
  if (sub === "key") {
    console.log((await createKeychainKey()) === "created" ? "Created the token key in the login keychain." : "The keychain already has a token key.");
    return 0;
  }
  const name = typeof values.connection === "string" ? ConnectionName.parse(values.connection) : undefined;
  if (!sub || !name) {
    console.log(CONNECT_USAGE);
    return 2;
  }
  if (!approvedAliases(env).includes(name)) throw new OrgNotApprovedError(name);
  const store = connectionStore(env);

  if (sub === "start") {
    const client = oauthClient(env);
    // The consultant's own run of the customer journey: a link made and opened at once.
    const { link } = createConnectLink({
      baseUrl: "http://localhost",
      customerRef: name,
      connectionName: name,
      loginUrl: typeof values["login-url"] === "string" ? values["login-url"] : undefined,
      createdBy: "hc connect",
    });
    const { authorisationUrl, pending } = beginAuthorisation(link, client);
    await store.savePending(name, { link, pending });
    console.log(`Open this in a browser within 10 minutes, sign in as the read-only user and click Allow:\n\n${authorisationUrl}\n`);
    console.log(`Salesforce then sends the browser to ${client.redirectUri}. Nothing listens there, so copy the whole address from the browser's address bar and run:\n`);
    console.log(`  hc connect finish --connection ${name} --callback-url '<the address>'`);
    return 0;
  }

  if (sub === "finish") {
    if (typeof values["callback-url"] !== "string") throw new Error("--callback-url is needed");
    const saved = await store.takePending(name);
    if (saved === undefined) throw new Error(`No authorisation in progress for "${name}". Run hc connect start first`);
    const { link, pending } = PendingFile.parse(saved);
    const { code, codeVerifier } = completeAuthorisation(pending, link, values["callback-url"]);
    const client = oauthClient(env);
    const tokens = await exchangeCode({ transport: fetchOAuthTransport, loginUrl: link.loginUrl, client, code, codeVerifier });
    if (!tokens.refreshToken) throw new Error("Salesforce didn't return a refresh token. Check the app has the refresh_token scope");
    await store.save(
      StoredConnection.parse({
        version: 1,
        name,
        grant: "web-server",
        loginUrl: link.loginUrl,
        clientId: client.clientId,
        instanceUrl: tokens.instanceUrl,
        orgId: tokens.orgId,
        userId: tokens.userId,
        scope: tokens.scope,
        accessToken: tokens.accessToken,
        refreshToken: tokens.refreshToken,
        issuedAt: tokens.issuedAt,
        connectedAt: new Date().toISOString(),
      }),
    );
    console.log(`Connected "${name}" to org ${tokens.orgId} as user ${tokens.userId}. Stored encrypted in ${store.dir}`);
    return finishWithCheck(name, store, env);
  }

  if (sub === "jwt") {
    if (env.NODE_ENV === "production") throw new Error("The JWT bearer command is for development only");
    if (typeof values.config !== "string") throw new Error("--config is needed");
    ownerOnly(values.config);
    const config = DevJwtFile.parse(JSON.parse(readFileSync(values.config, "utf8")));
    ownerOnly(config.privateKeyPath);
    const tokens = await jwtBearer({
      transport: fetchOAuthTransport,
      tokenUrl: config.instanceUrl,
      clientId: config.clientId,
      username: config.username,
      audience: config.audience,
      privateKey: readFileSync(config.privateKeyPath, "utf8"),
    });
    await store.save(
      StoredConnection.parse({
        version: 1,
        name,
        grant: "jwt-bearer",
        loginUrl: loginOrigin(config.instanceUrl),
        clientId: config.clientId,
        instanceUrl: tokens.instanceUrl,
        orgId: tokens.orgId,
        userId: tokens.userId,
        scope: tokens.scope,
        accessToken: tokens.accessToken,
        issuedAt: tokens.issuedAt,
        connectedAt: new Date().toISOString(),
        jwt: { username: config.username, audience: config.audience, privateKeyPath: config.privateKeyPath },
      }),
    );
    console.log(`Connected "${name}" by JWT bearer to org ${tokens.orgId} as user ${tokens.userId}. Stored encrypted in ${store.dir}`);
    return finishWithCheck(name, store, env);
  }

  if (sub === "check") {
    // Says whether the check had to refresh, which is how rotation is tested live.
    const before = (await store.load(name)).issuedAt;
    const code = await finishWithCheck(name, store, env);
    const after = await store.load(name).then((c) => c.issuedAt, () => before);
    console.log(after === before ? "The stored access token was still valid, so nothing was refreshed." : "The access token had expired: it was refreshed and the new tokens stored.");
    return code;
  }

  if (sub === "show") {
    for (const [key, value] of Object.entries(describeConnection(await store.load(name)))) console.log(`${key.padEnd(13)} ${value}`);
    return 0;
  }

  if (sub === "revoke") {
    await revokeConnection({ name, store });
    console.log(`Revoked "${name}" at Salesforce and deleted its stored tokens.`);
    return 0;
  }

  console.log(CONNECT_USAGE);
  return 2;
}

/** A connection that can write is revoked and forgotten. One we couldn't check is kept, with a warning. */
async function finishWithCheck(name: string, store: TokenStore, env: NodeJS.ProcessEnv): Promise<number> {
  const check = await readOnlyCheck(name, store, env);
  console.log(report(check));
  if (check.status !== "can_write") return check.status === "read_only" ? 0 : 2;
  await revokeConnection({ name, store });
  console.log(`Revoked "${name}" and deleted its tokens. Connect again as a user who holds only Barometer Read Only.`);
  return 2;
}
