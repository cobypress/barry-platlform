import type { CodeLintTools } from "../extract/code-lint.ts";
import type { DomainModule } from "../domains/types.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import type { OAuthClient } from "../salesforce/oauth.ts";
import type { TokenStore } from "../salesforce/token-store.ts";
import type { OAuthTransport, Transport } from "../salesforce/transport.ts";
import type { renderReleased } from "../render/render-released.ts";
import type { renderHandbookDocx } from "../handbook/docx.ts";
import type { DocumentDrive, FolderSharing } from "./drive.ts";
import type { WorkerStore } from "./types.ts";
import type { AutomaticSwitch } from "./automatic.ts";
import type { PortalDocumentsSwitch } from "./portal-documents.ts";

export type WorkerDeps = {
  store: WorkerStore;
  /** How long a delivery's run-times row may take before the job goes on without it (DECISIONS.md 722). DELIVERY_WRITE_MS unless a test says. */
  deliveryWriteMs?: number;
  /** Encrypted OAuth tokens. Only the worker holds these; the console never sees a token. */
  tokens: TokenStore;
  /** Parent of every run folder. Outside any git work tree (rule 8). */
  runsDir: string;
  locksDir: string;
  /** The package app, and the app of any link or connection that names none. */
  oauthClient: OAuthClient;
  /** The sign-in app, when it's set up (DECISIONS.md 616). A connection only ever uses the app it was made with. */
  signinClient?: OAuthClient | null;
  /** HC_CONNECT_KEY, for the console's sealed link secrets. */
  connectKey: Buffer;
  modules?: DomainModule[];
  stopAtShare?: number;
  apiVersion?: string;
  /** Claude, for drafting report sections. Absent when ANTHROPIC_API_KEY isn't set: sections are then the engine's own text. */
  narrative?: NarrativeClient;
  /** Where released documents go. Null when Drive isn't set up: a documents job then fails with what to set. */
  drive?: DocumentDrive | null;
  /** Shares a released run's folder with its readers (DECISIONS.md 251). The same Google Drive client, when Drive is set up. */
  sharing?: FolderSharing | null;
  /** Puts a run job on the queue, for a free scan the moment its prospect connects. Without it, the console's "Start the scan" does. */
  queueRun?: (runId: string, options?: { automatic?: boolean }) => Promise<void>;
  /** Seals the customer microsite. Without it, released runs get documents and no microsite. */
  micrositeKey?: Buffer | null;
  /**
   * The worker's switch for Claude's summaries of Apex code, read in env.ts:
   * a health check's Apex step keeps the redacted code, and Claude summarises
   * it after the analysis (DECISIONS.md 490 to 492). Off when absent.
   */
  apexSummaries?: boolean;
  /** HC_DOCUMENTS_ON_DEMAND and HC_DOCUMENTS_KEY, read once at start in env.ts (DECISIONS.md 698). Absent: off, and every portal make is refused. */
  portalDocuments?: PortalDocumentsSwitch;
  /**
   * HC_STAGE_NAMES=on, exactly, read once at start in env.ts (DECISIONS.md 756): an automatic calibration reads a
   * closed, not-won stage the customer hasn't answered for as lost or no decision from its name, by a fixed rule,
   * said as an assumption. Off when absent: no name is read, and one read before is dropped at the run's next fresh start.
   */
  stageNames?: boolean;
  /**
   * HC_STAGE_NAMES_MODEL=on, exactly, read once at start in env.ts (DECISIONS.md 766): with stageNames also on, a model
   * reads the closed, not-won stages the name rule leaves undecided, from their label and API name only, said as an
   * assumption. Off when absent: no model is asked, and a reading made before is dropped at the run's next fresh start.
   */
  stageNamesModel?: boolean;
  /**
   * HC_STAGE_NAMES_MODEL_CONSOLE=ready, exactly (DECISIONS.md 766): the console's copy of the calibration carries a
   * model's readings. Until it does, its strict schema throws on one, so stageNamesModel is ignored without it.
   */
  stageNamesModelConsole?: boolean;
  /** The worker's switch for automatic runs, read once at start in env.ts (DECISIONS.md 688). Absent: off. */
  automatic?: AutomaticSwitch;
  /**
   * PMD as the worker found it at start, for each health check's code-lint
   * step (DECISIONS.md 670). Absent: the step says PMD isn't installed.
   */
  codeLint?: CodeLintTools;
  /**
   * One line to the consultants' channel, when a documents make repaired what
   * it couldn't trace (DECISIONS.md 682). Counts and a run id only. Absent, the
   * line is only logged: the engine has no Slack of its own yet.
   */
  alert?: (line: string) => Promise<void>;
  /** Tests only: prints without a browser. */
  renderReleased?: typeof renderReleased;
  /** Tests only: stands in for the org handbook's renderer, so a failing render can be seen. */
  renderHandbook?: typeof renderHandbookDocx;
  /** Tests only: fakes for Salesforce's data API and its token endpoint. */
  dataTransport?: Transport;
  oauthTransport?: OAuthTransport;
  now?: () => Date;
  /** Tests only: stands in for the waits between tries at revoking a scan's connection (DECISIONS.md 652). */
  sleep?: (ms: number) => Promise<void>;
  /** Tests only: the connect job's time limit on reading the org's stages (DECISIONS.md 611). */
  stagesTimeoutMs?: number;
  log?: (line: string) => void;
};

/** Whether the queue will deliver this job again if it throws. */
/**
 * Whether the queue will deliver this job again if it throws, and the job's
 * own queue id where the queue gives one: a person's "Run again" is known by
 * its id alone, `run-<runId>-again-<ms>` (DECISIONS.md 698), since the job's
 * data holds ids only and its schema is strict.
 */
export type Attempt = { final: boolean; jobId?: string };

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
