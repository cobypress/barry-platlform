import { isAbsolute } from "node:path";
import { BUDGET_DEFAULTS } from "../config.ts";
import type { OAuthClient } from "../salesforce/oauth.ts";

// What `hc worker` needs before it starts. Every variable is required except
// the last three, and the worker refuses to start with any missing or wrong,
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
    HC_WORKER_CONCURRENCY: "Jobs at once, 1 to 8. Default 1",
    HC_API_BUDGET_SHARE: `Share of an org's daily API limit a run may use. Default ${BUDGET_DEFAULTS.stopAtShare}`,
    HC_SF_API_VERSION: "Pin a Salesforce API version",
  },
} as const;

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
  const concurrency = get("HC_WORKER_CONCURRENCY") ? Number(get("HC_WORKER_CONCURRENCY")) : 1;
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 8) problems.push("HC_WORKER_CONCURRENCY must be a whole number from 1 to 8");
  const stopAtShare = get("HC_API_BUDGET_SHARE") ? Number(get("HC_API_BUDGET_SHARE")) : BUDGET_DEFAULTS.stopAtShare;
  if (!(stopAtShare > 0 && stopAtShare <= 1)) problems.push("HC_API_BUDGET_SHARE must be more than 0 and at most 1");
  if (problems.length) return { ok: false, problems };

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
    },
  };
}
