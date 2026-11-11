import { isAbsolute } from "node:path";
import { BUDGET_DEFAULTS } from "../config.ts";
import type { OAuthClient } from "../salesforce/oauth.ts";
import type { DriveConfig } from "./drive.ts";
import { MicrositeKeyError, micrositeKey } from "../microsite/seal.ts";

// What `hc worker` needs before it starts. Every variable is required except
// the optional ones, and the worker refuses to start with any missing or wrong,
// naming each one and never its value.

export const WORKER_ENV = {
  required: {
    REDIS_URL: "The console's Redis, where health-check-jobs is queued",
    HC_WORKER_DATABASE_URL: "Postgres holding the console's health-check tables. Separate from DATABASE_URL on purpose",
    HC_CONNECT_KEY: "32 bytes, base64: opens the link secrets the console seals. The console holds the same key",
    HC_TOKEN_KEY: "32 bytes, base64: encrypts stored OAuth tokens. Only the worker holds it",
    HC_CONNECTIONS_DIR: "Absolute folder for the encrypted tokens",
    HC_RUNS_DIR: "Absolute folder for run folders, which hold CRM records",
    HC_SF_CLIENT_ID: "Barometer's External Client App consumer key",
    HC_SF_CLIENT_SECRET: "Its secret, needed for the code exchange and refresh",
    HC_SF_REDIRECT_URI: "The console's OAuth callback, exactly as registered on the app",
  },
  optional: {
    HC_WORKER_CONCURRENCY: "Jobs at once from the console's queue, 1 to 8. Default 2. Runs and documents are handed to a queue of their own and run one at a time beside them, so a connect job never waits behind one",
    HC_API_BUDGET_SHARE: `Share of an org's daily API limit a run may use. Default ${BUDGET_DEFAULTS.stopAtShare}`,
    HC_SF_API_VERSION: "Pin a Salesforce API version",
    HC_DRIVE_SA_EMAIL: "Released documents: the Google service account's email. Set all three HC_DRIVE_ variables or none",
    HC_DRIVE_SA_KEY: "Its private key, PEM, with \\n for line breaks",
    HC_DRIVE_FOLDER_ID: "The top Health checks folder in Google Drive, shared with the service account",
    HC_DRIVE_IMPERSONATE: "A user the service account acts as, if the folder isn't in a shared drive (domain-wide delegation)",
    ANTHROPIC_API_KEY: "Claude drafts each report section. Without it or the federation variables below, sections are the engine's own text",
    ANTHROPIC_FEDERATION_RULE_ID: "Claude through OIDC instead of a key: the federation rule (fdrl_...). Set it, ANTHROPIC_ORGANIZATION_ID and ANTHROPIC_SERVICE_ACCOUNT_ID together, or none",
    ANTHROPIC_ORGANIZATION_ID: "The Anthropic organization's id",
    ANTHROPIC_SERVICE_ACCOUNT_ID: "The service account the rule acts as (svac_...)",
    ANTHROPIC_WORKSPACE_ID: "The workspace Claude's usage is billed to (wrkspc_...)",
    HC_CLEANUP_DRY_RUN: "1 to log what the daily retention sweep would delete, and delete nothing of released runs and pulses. It doesn't cover Apex code kept for Claude's summaries, which is deleted after seven days whatever this says. 0 or unset: it deletes",
    HC_HANDBOOK_SUMMARIES: "on, exactly, and Claude summarises each health check's Apex classes and triggers for the org handbook, from code the run keeps until then. Anything else, or unset, is off. Leave it unset until the privacy notice says Apex code is sent to Claude",
    HC_MICROSITE_KEY: "The customer microsite: 32 bytes, base64 (openssl rand -base64 32), the same value as the console's, and different from HC_CONNECT_KEY and HC_TOKEN_KEY. Without it, released runs get documents but no microsite",
  },
} as const;

const FEDERATION_VARS = ["ANTHROPIC_FEDERATION_RULE_ID", "ANTHROPIC_ORGANIZATION_ID", "ANTHROPIC_SERVICE_ACCOUNT_ID"] as const;

/**
 * How the worker signs in to Claude. A key wins if both are set, as it does in
 * the SDK. Federation needs an identity token too: Render writes one and points
 * ANTHROPIC_IDENTITY_TOKEN_FILE at it once its OIDC is set up for the service.
 */
export type ClaudeAccess = { kind: "key"; apiKey: string } | { kind: "federation" } | { kind: "none"; reason: string };

const DRIVE_VARS = ["HC_DRIVE_SA_EMAIL", "HC_DRIVE_SA_KEY", "HC_DRIVE_FOLDER_ID"] as const;

export type WorkerConfig = {
  redisUrl: string;
  databaseUrl: string;
  connectKey: Buffer;
  connectionsDir: string;
  runsDir: string;
  oauthClient: OAuthClient;
  concurrency: number;
  stopAtShare: number;
  apiVersion?: string;
  /** Null when Drive isn't set up. The worker still starts: only documents jobs need it. */
  drive: DriveConfig | null;
  /** "none" when neither is set up. The worker still starts, and sections stay the engine's own text. */
  claude: ClaudeAccess;
  /** HC_CLEANUP_DRY_RUN=1: the retention sweep logs what it would delete, and deletes nothing. */
  cleanupDryRun: boolean;
  /** Seals the customer microsite's pages. Null when it isn't set up. */
  micrositeKey: Buffer | null;
  /** HC_HANDBOOK_SUMMARIES=on, and only that: Claude summarises the Apex code (DECISIONS.md 490). */
  apexSummaries: boolean;
};

const key32 = (raw: string): Buffer | null => {
  const key = Buffer.from(raw.trim(), "base64");
  return key.length === 32 ? key : null;
};

export function readWorkerEnv(env: NodeJS.ProcessEnv): { ok: true; config: WorkerConfig } | { ok: false; problems: string[] } {
  const problems: string[] = [];
  const get = (name: string): string => env[name]?.trim() ?? "";
  for (const name of Object.keys(WORKER_ENV.required)) if (!get(name)) problems.push(`${name} is not set`);
  if (problems.length) return { ok: false, problems };

  if (!/^rediss?:\/\//.test(get("REDIS_URL"))) problems.push("REDIS_URL must start redis:// or rediss://");
  if (!/^postgres(ql)?:\/\//.test(get("HC_WORKER_DATABASE_URL"))) problems.push("HC_WORKER_DATABASE_URL must start postgres:// or postgresql://");
  const connectKey = key32(get("HC_CONNECT_KEY"));
  if (!connectKey) problems.push("HC_CONNECT_KEY must be 32 bytes, base64-encoded");
  const tokenKey = key32(get("HC_TOKEN_KEY"));
  if (!tokenKey) problems.push("HC_TOKEN_KEY must be 32 bytes, base64-encoded");
  // The console holds one; only the worker holds the other. One key for both would hand the console the tokens.
  if (connectKey && tokenKey && connectKey.equals(tokenKey)) problems.push("HC_CONNECT_KEY and HC_TOKEN_KEY must be different keys");
  for (const name of ["HC_CONNECTIONS_DIR", "HC_RUNS_DIR"]) if (!isAbsolute(get(name))) problems.push(`${name} must be an absolute path`);
  if (!/^https:\/\//.test(get("HC_SF_REDIRECT_URI")) && !/^http:\/\/(localhost|127\.0\.0\.1)[:/]/.test(get("HC_SF_REDIRECT_URI"))) {
    problems.push("HC_SF_REDIRECT_URI must be https, or http on localhost");
  }
  const concurrency = get("HC_WORKER_CONCURRENCY") ? Number(get("HC_WORKER_CONCURRENCY")) : 2;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 8) problems.push("HC_WORKER_CONCURRENCY must be a whole number from 1 to 8");
  const stopAtShare = get("HC_API_BUDGET_SHARE") ? Number(get("HC_API_BUDGET_SHARE")) : BUDGET_DEFAULTS.stopAtShare;
  if (!(stopAtShare > 0 && stopAtShare <= 1)) problems.push("HC_API_BUDGET_SHARE must be more than 0 and at most 1");
  const driveSet = DRIVE_VARS.filter((name) => get(name));
  if (driveSet.length > 0 && driveSet.length < DRIVE_VARS.length) problems.push(`Set all of ${DRIVE_VARS.join(", ")} or none: ${DRIVE_VARS.filter((n) => !get(n)).join(", ")} missing`);
  if (get("HC_DRIVE_SA_KEY") && !get("HC_DRIVE_SA_KEY").includes("PRIVATE KEY")) problems.push("HC_DRIVE_SA_KEY must be the service account's PEM private key");
  if (get("HC_DRIVE_FOLDER_ID") && !/^[A-Za-z0-9_-]{10,200}$/.test(get("HC_DRIVE_FOLDER_ID"))) problems.push("HC_DRIVE_FOLDER_ID must be a Drive folder id, the last part of the folder's address");
  const federationSet = FEDERATION_VARS.filter((name) => get(name));
  if (federationSet.length > 0 && federationSet.length < FEDERATION_VARS.length) problems.push(`Set all of ${FEDERATION_VARS.join(", ")} or none: ${FEDERATION_VARS.filter((n) => !get(n)).join(", ")} missing`);
  if (!["", "0", "1"].includes(get("HC_CLEANUP_DRY_RUN"))) problems.push("HC_CLEANUP_DRY_RUN must be 1, 0 or unset");
  let siteKey: Buffer | null = null;
  try {
    siteKey = micrositeKey({ HC_MICROSITE_KEY: get("HC_MICROSITE_KEY"), HC_CONNECT_KEY: get("HC_CONNECT_KEY"), HC_TOKEN_KEY: get("HC_TOKEN_KEY") });
  } catch (err) {
    if (err instanceof MicrositeKeyError) problems.push(err.message);
    else throw err;
  }
  if (problems.length) return { ok: false, problems };
  const identityToken = Boolean(get("ANTHROPIC_IDENTITY_TOKEN_FILE") || get("ANTHROPIC_IDENTITY_TOKEN"));
  const claude: ClaudeAccess = get("ANTHROPIC_API_KEY")
    ? { kind: "key", apiKey: get("ANTHROPIC_API_KEY") }
    : federationSet.length === 0
      ? { kind: "none", reason: "Neither ANTHROPIC_API_KEY nor the ANTHROPIC_ federation variables are set" }
      : identityToken
        ? { kind: "federation" }
        : // The worker still runs: the checks matter more than the prose.
          { kind: "none", reason: "The federation variables are set, but there's no identity token: ANTHROPIC_IDENTITY_TOKEN_FILE isn't set. On Render, it appears once the service's OIDC is set up and it's redeployed" };
  const drive: DriveConfig | null =
    driveSet.length === DRIVE_VARS.length
      ? {
          clientEmail: get("HC_DRIVE_SA_EMAIL"),
          // Hosts usually hold the key on one line with literal "\n"s.
          privateKey: get("HC_DRIVE_SA_KEY").replace(/\\n/g, "\n"),
          rootFolderId: get("HC_DRIVE_FOLDER_ID"),
          subject: get("HC_DRIVE_IMPERSONATE") || null,
        }
      : null;

  return {
    ok: true,
    config: {
      redisUrl: get("REDIS_URL"),
      databaseUrl: get("HC_WORKER_DATABASE_URL"),
      connectKey: connectKey!,
      connectionsDir: get("HC_CONNECTIONS_DIR"),
      runsDir: get("HC_RUNS_DIR"),
      oauthClient: { clientId: get("HC_SF_CLIENT_ID"), clientSecret: get("HC_SF_CLIENT_SECRET"), redirectUri: get("HC_SF_REDIRECT_URI") },
      concurrency,
      stopAtShare,
      apiVersion: get("HC_SF_API_VERSION") || undefined,
      drive,
      claude,
      cleanupDryRun: get("HC_CLEANUP_DRY_RUN") === "1",
      micrositeKey: siteKey,
      // Exactly "on", untrimmed: a switch that sends code to Claude doesn't guess.
      apexSummaries: env.HC_HANDBOOK_SUMMARIES === "on",
    },
  };
}
