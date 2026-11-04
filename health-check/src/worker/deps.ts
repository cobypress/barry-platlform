import type { DomainModule } from "../domains/types.ts";
import type { OAuthClient } from "../salesforce/oauth.ts";
import type { TokenStore } from "../salesforce/token-store.ts";
import type { OAuthTransport, Transport } from "../salesforce/transport.ts";
import type { renderReleased } from "../render/render-released.ts";
import type { DocumentDrive } from "./drive.ts";
import type { WorkerStore } from "./types.ts";

export type WorkerDeps = {
  store: WorkerStore;
  /** Encrypted OAuth tokens. Only the worker holds these; the console never sees a token. */
  tokens: TokenStore;
  /** Parent of every run folder. Outside any git work tree (rule 8). */
  runsDir: string;
  locksDir: string;
  oauthClient: OAuthClient;
  /** HC_CONNECT_KEY, for the console's sealed link secrets. */
  connectKey: Buffer;
  modules?: DomainModule[];
  stopAtShare?: number;
  apiVersion?: string;
  /** Where released documents go. Null when Drive isn't set up: a documents job then fails with what to set. */
  drive?: DocumentDrive | null;
  /** Tests only: prints without a browser. */
  renderReleased?: typeof renderReleased;
  /** Tests only: fakes for Salesforce's data API and its token endpoint. */
  dataTransport?: Transport;
  oauthTransport?: OAuthTransport;
  now?: () => Date;
  log?: (line: string) => void;
};

/** Whether the queue will deliver this job again if it throws. */
export type Attempt = { final: boolean };

export type JobOutcome =
  | { outcome: "done"; detail: string }
  | { outcome: "stopped_budget"; detail: string }
  | { outcome: "skipped"; detail: string }
  | { outcome: "failed"; detail: string }
  | { outcome: "connected"; detail: string }
  | { outcome: "refused"; detail: string };

/**
 * An error as text for the console. Our own errors hold codes and messages,
 * never tokens (see hc.ts), but the worker handles codes, verifiers and tokens
 * in the same breath, so any it knows about are cut out as well.
 */
export function safeReason(err: unknown, secrets: Array<string | undefined> = []): string {
  let text = err instanceof Error ? err.message : "Something went wrong that we didn't expect";
  for (const secret of secrets) if (secret && secret.length >= 8) text = text.split(secret).join("[removed]");
  return text.slice(0, 1000);
}
