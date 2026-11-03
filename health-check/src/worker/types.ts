import { z } from "zod";
import type { Intake } from "../narrative/intake.ts";
import type { Calibration, ConnectLink, Finding, ManualAnswer, ManualStep, Metric, ReportSection, Run, Score, SectionVersion } from "../schemas/index.ts";

// What the worker takes from the queue, and what it needs from the console's
// database. Jobs carry ids only (the contract with the console, lane C2), so
// nothing about a customer ever sits in Redis.

const RunJobPayload = z.object({ kind: z.literal("run"), runId: z.string().min(1).max(100), correlation_id: z.string().max(300).optional() }).strict();
const ConnectJobPayload = z.object({ kind: z.literal("connect"), linkId: z.string().uuid(), correlation_id: z.string().max(300).optional() }).strict();
/**
 * What lib/health-check/jobs.ts in the console sends today, before C2 moves it
 * to the contract. One step per job isn't needed: the engine's steps resume
 * themselves, so a step job is read as a run job for its run.
 */
const LegacyStepPayload = z.object({ runId: z.string().min(1).max(100), stepId: z.string().min(1).max(100), correlation_id: z.string().max(300) }).strict();

export type WorkerJob = { kind: "run"; runId: string } | { kind: "connect"; linkId: string };

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
  const legacy = LegacyStepPayload.safeParse(data);
  if (legacy.success) return { kind: "run", runId: legacy.data.runId };
  throw new BadJobError("The job isn't a run or connect job with ids only");
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

export type ConnectJobInput = {
  link: ConnectLink;
  state: LinkState;
  /** Sealed by the console with HC_CONNECT_KEY (connect-crypto.ts). Null once settled. */
  sealedPending: string | null;
  sealedCode: string | null;
};

export type LinkConnected = {
  at: string;
  orgId: string;
  userId: string;
  /** Set when the read-only check couldn't prove anything (DECISIONS 64): shown to the consultant. */
  note: string | null;
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
  /** Moves a pending link to used, records who connected, and clears its sealed secrets. */
  markLinkUsed(linkId: string, connected: LinkConnected): Promise<void>;
  /** Moves a pending link to refused with a reason the console can show, and clears its sealed secrets. Never a token. */
  refuseLink(linkId: string, reason: string, at: string): Promise<void>;
}
