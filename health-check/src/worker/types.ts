import type { ScanResult } from "../scan/result.ts";
import { z } from "zod";
import type { Intake } from "../narrative/intake.ts";
import type { BenchmarkRow, Calibration, ConnectLink, Finding, ManualAnswer, ManualStep, Metric, NumberOverride, ReportSection, Run, Score, SectionVersion } from "../schemas/index.ts";

// What the worker takes from the queue, and what it needs from the console's
// database. Jobs carry ids only (the contract with the console, lane C2), so
// nothing about a customer ever sits in Redis.

const RunJobPayload = z.object({ kind: z.literal("run"), runId: z.string().min(1).max(100), correlation_id: z.string().max(300).optional() }).strict();
const ConnectJobPayload = z.object({ kind: z.literal("connect"), linkId: z.string().uuid(), correlation_id: z.string().max(300).optional() }).strict();
const DocumentsJobPayload = z.object({ kind: z.literal("documents"), runId: z.string().min(1).max(100), correlation_id: z.string().max(300).optional() }).strict();
/**
 * What lib/health-check/jobs.ts in the console sends today, before C2 moves it
 * to the contract. One step per job isn't needed: the engine's steps resume
 * themselves, so a step job is read as a run job for its run.
 */
const LegacyStepPayload = z.object({ runId: z.string().min(1).max(100), stepId: z.string().min(1).max(100), correlation_id: z.string().max(300) }).strict();

export type WorkerJob = { kind: "run"; runId: string } | { kind: "connect"; linkId: string } | { kind: "documents"; runId: string };

export class BadJobError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BadJobError";
  }
}

/** Anything but ids is refused, so a payload can't smuggle customer data through the queue. */
export function parseJob(data: unknown): WorkerJob {
  const run = RunJobPayload.safeParse(data);
  if (run.success) return { kind: "run", runId: run.data.runId };
  const connect = ConnectJobPayload.safeParse(data);
  if (connect.success) return { kind: "connect", linkId: connect.data.linkId };
  const documents = DocumentsJobPayload.safeParse(data);
  if (documents.success) return { kind: "documents", runId: documents.data.runId };
  const legacy = LegacyStepPayload.safeParse(data);
  if (legacy.success) return { kind: "run", runId: legacy.data.runId };
  throw new BadJobError("The job isn't a run, connect or documents job with ids only");
}

/** The console's link states (contract with C2). */
export const LinkState = z.enum(["open", "pending", "used", "refused", "expired", "withdrawn"]);
export type LinkState = z.infer<typeof LinkState>;

export type RunJobInput = {
  /** The console's run record. */
  run: Run;
  /** The engine's intake shape, mapped from the console's intake by the store. */
  intake: Intake | null;
  calibration: Calibration | null;
  /** The stored OAuth connection this run reads through. */
  connectionName: string | null;
  answers: ManualAnswer[];
};

/**
 * The run fields the worker owns. Everything else on the run (customer
 * reference, who made it, benchmark consent, modules, org) is the console's,
 * and the worker never writes it.
 */
export type RunProgress = Pick<Run, "status" | "ruleSetVersion" | "calibrationHash" | "asOf" | "analysisWindow" | "apiVersion" | "budget" | "steps" | "isSandbox" | "finishedAt">;

export function progressOf(run: Run): RunProgress {
  const { status, ruleSetVersion, calibrationHash, asOf, analysisWindow, apiVersion, budget, steps, isSandbox, finishedAt } = structuredClone(run);
  return { status, ruleSetVersion, calibrationHash, asOf, analysisWindow, apiVersion, budget, steps, isSandbox, finishedAt };
}

export type RunOutputRecord = {
  progress: RunProgress;
  /** The run folder on the worker's disk. Holds CRM records, so it never leaves the worker. */
  runDir: string;
  findings: Finding[];
  metrics: Metric[];
  scores: Score[];
  manualSteps: ManualStep[];
  sections: ReportSection[];
  versions: SectionVersion[];
};

/**
 * The org id the console gives a run before its customer connects. The worker
 * fills in the org the customer signs in to. The console has the same value
 * (blackcloud-next lib/health-check/schemas.ts, ORG_AWAITING_CONNECT).
 */
export const ORG_AWAITING_CONNECT = "awaiting-connect";

export type ConnectJobInput = {
  link: ConnectLink;
  state: LinkState;
  /** The org the console's run expects, or ORG_AWAITING_CONNECT when it'll take whichever the customer connects. */
  runOrgId: string;
  /** Sealed by the console with HC_CONNECT_KEY (connect-crypto.ts). Null once settled. */
  sealedPending: string | null;
  sealedCode: string | null;
  /** Set when the link's run is a free scan: once connected, the worker queues it itself. */
  scanRunId?: string;
};

export type LinkConnected = {
  at: string;
  orgId: string;
  userId: string;
  /** Set when the read-only check couldn't prove anything (DECISIONS 64): shown to the consultant. */
  note: string | null;
};

/**
 * The documents of a released run (the console's HealthCheckDocuments row).
 * The console asks for them by setting the row to queued; the worker moves it
 * to making, then done or failed.
 */
export type DocumentsState = "queued" | "making" | "done" | "failed";

export type DocumentFile = { output: ReportSection["output"]; name: string; driveFileId: string; url: string };

export type DocumentsJobInput = {
  run: Run;
  /** The intake as it stands now, with the customer's answers. */
  intake: Intake | null;
  sections: ReportSection[];
  /** The current version of each section. */
  versions: SectionVersion[];
  overrides: NumberOverride[];
  /** The consultant the cover names, as the console holds it now. Null leaves the cover without a name. */
  preparedBy: string | null;
  state: DocumentsState | null;
  /** From an earlier make, so the documents go back to the same Drive folder. */
  driveFolderId: string | null;
  files: DocumentFile[];
};

export type DocumentsMade = {
  at: string;
  clientName: string;
  folderName: string;
  driveFolderId: string;
  driveFolderUrl: string;
  files: DocumentFile[];
  warnings: string[];
};

/** Raised by a store when the console has moved a run on, so the worker mustn't write to it. */
export class RunClosedError extends Error {
  constructor(runId: string, status: string) {
    super(`Run ${runId} is ${status.replace("_", " ")}, so the worker leaves it alone`);
    this.name = "RunClosedError";
  }
}

/**
 * The worker's view of the console's database. Writes are whole: a store
 * replaces what a run or link held rather than adding to it, so a job that's
 * delivered twice writes the same rows twice and ends in the same place.
 */
export interface WorkerStore {
  loadRunJob(runId: string): Promise<RunJobInput | null>;
  /** Status, steps and budget as the engine has them now. Throws RunClosedError once the run is in review or released. */
  saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void>;
  /** Everything the console reads from a finished run, in one transaction, replacing what was there. Throws RunClosedError like saveRunProgress. */
  saveRunOutput(runId: string, output: RunOutputRecord): Promise<void>;
  /** The run failed before or outside the engine. Marks it failed with the reason on a "worker" step. */
  recordRunFailure(runId: string, reason: string, at: string): Promise<void>;

  loadConnectJob(linkId: string): Promise<ConnectJobInput | null>;
  /** Moves a pending link to used, records who connected, and clears its sealed secrets. A run still awaiting its org takes the connected one. */
  markLinkUsed(linkId: string, connected: LinkConnected): Promise<void>;
  /** Moves a pending link to refused with a reason the console can show, and clears its sealed secrets. Never a token. */
  refuseLink(linkId: string, reason: string, at: string): Promise<void>;

  loadDocumentsJob(runId: string): Promise<DocumentsJobInput | null>;
  /** Moves queued (or making, on a second delivery) to making. False if the console hasn't asked, so there's nothing to do. */
  startDocuments(runId: string, at: string): Promise<boolean>;
  saveDocuments(runId: string, made: DocumentsMade): Promise<void>;
  /** A reason the console can show. Never a token or a name. */
  failDocuments(runId: string, reason: string, at: string): Promise<void>;

  /** A consenting run's benchmark figures, all or none. The rows carry nothing that leads back to the run (rule 11). */
  addBenchmarkRows(rows: BenchmarkRow[]): Promise<void>;
  /** Every benchmark row, for comparing a run with other customers. They name no one, so reading them all is safe. */
  loadBenchmarkRows(): Promise<BenchmarkRow[]>;

  /** Records that the worker put a run on the queue itself, as the console does when it queues one. */
  recordRunQueued(runId: string, at: string): Promise<void>;

  /** A free scan's teaser, for the console to show the prospect. One row a run. */
  saveScanResult(runId: string, result: ScanResult, madeAt: string): Promise<void>;

  /** The run's customer microsite, sealed: one row a run, its version moving on with each make. */
  saveMicrosite(runId: string, sealedPages: string, madeAt: string, expiresAt: string): Promise<void>;

  /** Every run the console has released, with when its last output was released. Null when no release date can be read. */
  listReleasedRuns(): Promise<ReleasedRun[]>;
}

/** A released run, for the retention sweep. The date is ISO UTC. */
export type ReleasedRun = { runId: string; releasedAt: string | null };
