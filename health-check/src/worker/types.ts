import type { OrgStage } from "../connect/org-stages.ts";
import type { StepTiming } from "../engine/run.ts";
import type { OverallScore } from "../domains/scoring.ts";
import type { Inventory } from "../schemas/inventory.ts";
import type { Complexity } from "../schemas/complexity.ts";
import type { ScanResult } from "../scan/result.ts";
import type { PulseResult } from "../pulse/result.ts";
import type { FactsFile, FactsReport } from "../facts/summary.ts";
import { z } from "zod";
import type { Intake } from "../narrative/intake.ts";
import type { BenchmarkRow, Calibration, ConnectLink, Finding, ManualAnswer, ManualStep, Metric, NumberOverride, Release, ReportSection, Run, Score, SectionVersion } from "../schemas/index.ts";
import type { Survey } from "../survey/questions.ts";
import type { Interview } from "../interviews/questions.ts";
import type { StoredSummaries, SummariesToSave } from "../handbook/summary-row.ts";
import type { RunDigest } from "../schemas/digest.ts";
import type { RunLosses } from "./release.ts";

// What the worker takes from the queue, and what it needs from the console's
// database. Jobs carry ids only (the contract with the console, lane C2), so
// nothing about a customer ever sits in Redis.

const RunJobPayload = z.object({ kind: z.literal("run"), runId: z.string().min(1).max(100), correlation_id: z.string().max(300).optional() }).strict();
const ConnectJobPayload = z.object({ kind: z.literal("connect"), linkId: z.string().uuid(), correlation_id: z.string().max(300).optional() }).strict();
/**
 * `deliver: "portal"` asks for a released automatic run's three PDFs, sealed
 * for the portal and never put anywhere else (DECISIONS.md 698). Left out, the
 * documents go to Drive as they always have.
 */
const DocumentsJobPayload = z.object({ kind: z.literal("documents"), runId: z.string().min(1).max(100), deliver: z.literal("portal").optional(), correlation_id: z.string().max(300).optional() }).strict();
/** The console's readers changed on a released run: share its Drive folder to match (DECISIONS.md 251). */
const ShareJobPayload = z.object({ kind: z.literal("share"), runId: z.string().min(1).max(100), correlation_id: z.string().max(300).optional() }).strict();
/**
 * What lib/health-check/jobs.ts in the console sends today, before C2 moves it
 * to the contract. One step per job isn't needed: the engine's steps resume
 * themselves, so a step job is read as a run job for its run.
 */
const LegacyStepPayload = z.object({ runId: z.string().min(1).max(100), stepId: z.string().min(1).max(100), correlation_id: z.string().max(300) }).strict();

export type WorkerJob = { kind: "run"; runId: string } | { kind: "connect"; linkId: string } | { kind: "documents"; runId: string; deliver?: "portal" } | { kind: "share"; runId: string };

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
  if (documents.success) return { kind: "documents", runId: documents.data.runId, ...(documents.data.deliver ? { deliver: documents.data.deliver } : {}) };
  const share = ShareJobPayload.safeParse(data);
  if (share.success) return { kind: "share", runId: share.data.runId };
  const legacy = LegacyStepPayload.safeParse(data);
  if (legacy.success) return { kind: "run", runId: legacy.data.runId };
  throw new BadJobError("The job isn't a run, connect, documents or share job with ids only");
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
  /** The team survey's answers so far. Null or missing when nobody has answered. */
  survey?: Survey | null;
  /** The stakeholder interviews recorded so far, by role. Null or missing when there are none. */
  interviews?: Interview[] | null;
  /**
   * Why an automatic run waits for a person (the console's "heldReason",
   * DECISIONS.md 693), or null. A held run is never read or released by code:
   * only a person clears a hold. Missing reads as none.
   */
  heldReason?: string | null;
};

/**
 * The run fields the worker owns. Everything else on the run (customer
 * reference, who made it, benchmark consent, modules, org) is the console's,
 * and the worker never writes it.
 */
/** A pulse as the customer and a person's Show see it (DECISIONS.md 746). */
export type PulseState = { hasResult: boolean; kept: boolean; openFlags: string[] };

/** A run as a pulse's baseline chain sees it (DECISIONS.md 746). */
export type RunLineage = { orgId: string; product: string; reviewMode: "automatic" | "consultant"; baselineRunId: string | null };

/** A saved calibration's currency as a run read it, for nameCalibrationCurrency's guard (DECISIONS.md 745). */
export type CurrencyWas = { currency: string; currencyFrom: "org" | "customer" };

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
  /**
   * The team survey's metrics, as the run's report made them (DECISIONS.md
   * 645): stored beside the org's, in their own domain, so the sections that
   * cite them cite rows the run holds. Written after every other metric, in a
   * savepoint of their own, so a refusal costs these alone. Empty or left out
   * when the run has no survey; under five answers, the one saying so.
   */
  surveyMetrics?: Metric[];
  scores: Score[];
  manualSteps: ManualStep[];
  sections: ReportSection[];
  versions: SectionVersion[];
  /** What the facts step found, in names and counts (DECISIONS.md 324). Null when the run folder has no facts.json. */
  facts: FactsReport | null;
  /**
   * The whole of facts.json (DECISIONS.md 433): the names we assume, booleans
   * and field types, never a record or a value. Null when there's no summary,
   * or when the file names a fact we don't assume or is over the size cap.
   */
  factsFile: FactsFile | null;
  /**
   * Claude's summaries of the run's Apex code, for a person to approve before
   * the org handbook prints them (DECISIONS.md 494): only with the worker's
   * switch on and a handbook-summaries.json holding at least one. Never the code.
   */
  handbookSummaries?: SummariesToSave;
  /**
   * The org's inventory (DECISIONS.md 605): set-up names and counts, never a
   * record or who. Only when the run folder has an inventory.json that parses.
   */
  inventory?: Inventory;
  /**
   * The org's complexity score and its working (DECISIONS.md 614): counts of
   * set-up, never a record. Only when the run folder has a complexity.json that parses.
   */
  complexity?: Complexity;
  /**
   * The run's overall score and grade as its report has them (DECISIONS.md
   * 626), from the engine's own `overallScore`, so the console never copies the
   * formula; null where the engine gives none. Left out, the console's columns
   * aren't written at all.
   */
  overall?: RunOverall | null;
  /**
   * A pulse's digest for the customer's connector (DECISIONS.md 632), from the
   * report its result is made from; null for a pulse whose digest couldn't be
   * made, which takes away any earlier row so a stale one is never served.
   * Left out for every other run: a check's is written when its documents are made.
   */
  digest?: RunDigest | null;
  /**
   * An automatic run's release or hold (DECISIONS.md 693), written in the same
   * transaction as everything above, after it. Released: the digest (never
   * null, and written or the whole save rolls back), then one release row per
   * output naming nobody, then status `released`. Held: `heldReason` set and one
   * HealthCheckRunFlag row with its counts, raised once. Left out for every
   * consultant-led run, which saves exactly as before.
   */
  automatic?: AutomaticOutcome;
  /**
   * What an automatic Barometer run lost to Salesforce not answering (DECISIONS.md 723), stored beside the run in
   * the same transaction so the org's next run can tell "the org, again" from an outage. Left out where it can't be
   * told (more refused reads than run.json keeps) and for every other run; any progress save takes it away again, so
   * only the attempt that decided the run's release or hold leaves it.
   */
  readsLost?: RunLosses;
  /**
   * An automatic customer's pulse held for a person (DECISIONS.md 746): its flags, the first the reason, raised in
   * the same transaction, and any earlier result of the run taken away, so the portal (which lists a pulse once it
   * has a result) never shows it. No "heldReason": the console allows one only on an automatic run, which a pulse
   * isn't in its records. Left out for every other run.
   */
  pulseHold?: { reason: string; counts: Record<string, number>; also: Array<{ kind: string; counts: Record<string, number> }>; at: string; kept?: PulseResult };
};

/**
 * An automatic run's outcome as the store writes it. The reason words are src/worker/release.ts's. A hold raises its
 * own flag and one for each of `also`, the other checks that failed (DECISIONS.md 715), all with the same time.
 */
export type AutomaticOutcome =
  | { decision: "release"; releases: Release[] }
  | { decision: "hold"; reason: string; counts: Record<string, number>; also?: Array<{ kind: string; counts: Record<string, number> }>; at: string };

/** Whether an automatic run has its calibration, has been queued, or is held. */
export type AutomaticRunState = { calibrated: boolean; queued: boolean; held: boolean; status: string };

/** The run kinds a flag is raised for, beside the hold reasons: the console's HealthCheckRunFlag."kind" (migration 20261211). */
export const RUN_FAILED_FLAG = "run_failed";
/** An automatic run stopped at its API budget (DECISIONS.md 715; the console's migration 20261217). */
export const STOPPED_BUDGET_FLAG = "stopped_budget";

/** What the console's HealthCheckRun keeps of a run's overall score (DECISIONS.md 626): a whole number from 0 to 100, and its grade. */
export type RunOverall = { score: number; grade: "A" | "B" | "C" | "D" | "E" };

const GRADE_LETTERS: ReadonlySet<string> = new Set(["A", "B", "C", "D", "E"]);

/** The engine's overall score as the console keeps it, or null where there's none, or one its check constraints would refuse. */
export function runOverall(overall: OverallScore): RunOverall | null {
  if (overall.status !== "measured") return null;
  const { score, grade } = overall;
  if (!Number.isInteger(score) || score < 0 || score > 100 || !GRADE_LETTERS.has(grade)) return null;
  return { score, grade: grade as RunOverall["grade"] };
}

/**
 * The org id the console gives a run before its customer connects. The worker
 * fills in the org the customer signs in to. The console has the same value
 * (blackcloud-next lib/health-check/schemas.ts, ORG_AWAITING_CONNECT).
 */
export const ORG_AWAITING_CONNECT = "awaiting-connect";

/** The run step that says a free scan's revoke wasn't confirmed by Salesforce (DECISIONS.md 652). */
export const DISCONNECT_STEP = "disconnect";
/** What that step says, for the console to show. Fixed text: never a token, a code or Salesforce's words. */
export const SCAN_REVOKE_NOT_CONFIRMED =
  "Salesforce didn't confirm that it revoked this scan's connection. We deleted our copy, so we can't use it again, but Salesforce may treat it as valid until it expires or someone in the org revokes it.";

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
  /** The link's run, whatever its product: where the org's stages are written (DECISIONS.md 611). */
  runId?: string;
  /**
   * Set when the link's run is an automatic full check (DECISIONS.md 692): once
   * connected, the worker calibrates it from the org's stages and queues it.
   * Never set with scanRunId.
   */
  automaticRunId?: string;
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

/**
 * The console's DocumentOutput: the three reviewed outputs, the one page summary
 * (DECISIONS 137, 139), the action lists and tickets (197) and the org handbook
 * (477). A kind the console doesn't know makes it refuse the whole documents row.
 */
export type DocumentFile = { output: ReportSection["output"] | "ceo_one_pager" | "action_lists" | "tickets" | "org_handbook"; name: string; driveFileId: string; url: string };

export type DocumentsJobInput = {
  run: Run;
  /** The intake as it stands now, with the customer's answers. */
  intake: Intake | null;
  /** The team survey as it stands now. The documents print the one the sections were reviewed from. */
  survey?: Survey | null;
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
  /**
   * The console's row of Claude's summaries for the run (DECISIONS.md 495): its
   * status, items and counts, never who decided it. Null with the worker's
   * switch off, with no row, or with one that doesn't read.
   */
  handbookSummaries?: StoredSummaries | null;
  /**
   * An automatic run's hold (the console's "heldReason", DECISIONS.md 693), or
   * null. Read only for an automatic run; missing reads as none. The portal's
   * documents are never made for a held run (698).
   */
  heldReason?: string | null;
};

/**
 * One of an automatic run's documents as the portal keeps it (the console's
 * HealthCheckPortalDocument, DECISIONS.md 698). Held for us, `sealed` is
 * HELD_PLACEHOLDER, which nothing can open and the portal never serves.
 */
export type PortalDocumentRow = { runId: string; output: "executive_summary" | "full_health_check" | "ceo_one_pager"; sealed: Buffer; madeAt: string; expiresAt: string; heldForUs: boolean };

/** The `documents_held` flag a make raises when any document is held for us: counts only. */
export const DOCUMENTS_HELD_FLAG = "documents_held";

/** What a documents job asks the store to read beyond the run: the summaries only with the worker's switch on. */
export type DocumentsJobReads = { handbookSummaries?: boolean };

export type DocumentsMade = {
  at: string;
  clientName: string;
  folderName: string;
  driveFolderId: string;
  driveFolderUrl: string;
  files: DocumentFile[];
  warnings: string[];
};

/**
 * One person's read access to a released run's Drive folder, as the worker
 * records it in the console's HealthCheckDriveShare (DECISIONS.md 252). One
 * row a run, folder and address: shared, then removed, and shared again if
 * they're added back. Failed keeps the reason, never a token.
 */
export type DriveShareState = "shared" | "removed" | "failed";
export type DriveShareRemoved = "reader_removed" | "folder_changed" | "retention";
export type DriveShare = {
  runId: string;
  email: string;
  folderId: string;
  permissionId: string | null;
  state: DriveShareState;
  sharedAt: string | null;
  removedAt: string | null;
  removedWhy: DriveShareRemoved | null;
  error: string | null;
  updatedAt: string;
};

/** What a share job needs: whether the run may be shared yet, its folder, who should have it, and who has. */
export type ShareJobInput = {
  runId: string;
  /** Rule 10: only a released run's folder is ever shared. */
  released: boolean;
  /** Its last release, ISO UTC, or null when there's none we can read. Twelve months on, nobody has the folder (DECISIONS.md 254). */
  releasedAt: string | null;
  /** The run's "Health check n" folder, from its last documents make. Null until there's one. */
  folderId: string | null;
  /** The microsite's readers, lower-cased. */
  readers: string[];
  shares: DriveShare[];
  /** False when the console's share table isn't there yet, so nothing can be recorded and nothing is shared. */
  recordable: boolean;
};

/** How a delivery ended: the job's outcome, or "error" when it threw and goes back to the queue (or, on the last, BullMQ gives up). */
export const DELIVERY_OUTCOMES = ["done", "stopped_budget", "skipped", "failed", "error"] as const;
export type DeliveryOutcome = (typeof DELIVERY_OUTCOMES)[number];

/** One delivery of a run job, as the console's HealthCheckRunDelivery row (DECISIONS.md 722). */
export type RunDelivery = {
  runId: string;
  startedAt: string;
  endedAt: string;
  outcome: DeliveryOutcome;
  /** BullMQ's last try of this job. */
  final: boolean;
  /** A person's Run again of an automatic run (its job id), not the first run's job or a retry of it. */
  again: boolean;
  steps: StepTiming[];
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
  /**
   * Takes the run for this delivery before anything is written (DECISIONS.md
   * 538, 540): in one step, a run awaiting manual steps becomes analysing and
   * any other open status stays as it is. Throws RunClosedError once the run is
   * in review or released, so a run the console has moved on is refused before
   * the folder is written, not at the save after it.
   */
  claimRun(runId: string): Promise<void>;
  /** Status, steps and budget as the engine has them now. Throws RunClosedError once the run is in review or released. */
  saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void>;
  /** Everything the console reads from a finished run, in one transaction, replacing what was there. Throws RunClosedError like saveRunProgress. */
  saveRunOutput(runId: string, output: RunOutputRecord): Promise<void>;
  /** The run failed before or outside the engine. Marks it failed with the reason on a "worker" step. */
  recordRunFailure(runId: string, reason: string, at: string): Promise<void>;
  /**
   * The stopped_budget flag, counts `{}`, on a run the console has as automatic
   * and stopped at its budget (DECISIONS.md 715): raised once while open, so a
   * delivery made again raises nothing more; a cleared one is opened again.
   * True when a row was written (raised or opened again).
   */
  flagBudgetStop(runId: string, at: string): Promise<boolean>;
  /**
   * One delivery of a run job that took the run (DECISIONS.md 722): when it
   * began and ended on the worker's clock, how it ended, and each step it
   * read with its milliseconds and calls. Times and our own words only, never
   * anything from the org. True when a row was written; false when the
   * console has no HealthCheckRunDelivery table yet, or the run is gone. A
   * store without it keeps no run times.
   */
  recordDelivery?(delivery: RunDelivery): Promise<boolean>;

  loadConnectJob(linkId: string): Promise<ConnectJobInput | null>;
  /**
   * Moves a pending link to used, records who connected, and clears its sealed
   * secrets. A run still awaiting its org takes the connected one. With a My
   * Domain host (`myDomainHost`), the link keeps it too, for the portal's links
   * into Setup (DECISIONS.md 626); a database without that column marks the link
   * used as before. The host is never logged.
   */
  markLinkUsed(linkId: string, connected: LinkConnected, instanceHost?: string | null): Promise<void>;
  /** Moves a pending link to refused with a reason the console can show, and clears its sealed secrets. Never a token. */
  refuseLink(linkId: string, reason: string, at: string): Promise<void>;
  /**
   * The latest moment, ISO UTC, after `since` that a free scan other than
   * `exceptRunId` was made for or connected to the org whose id starts with
   * `orgId15`, or null (DECISIONS.md 610). Made is the run's startedAt with its
   * orgId matching; connected is a link's usedAt with its connectedOrgId matching.
   */
  lastScanOfOrg(orgId15: string, exceptRunId: string, since: string): Promise<string | null>;
  /**
   * The org's opportunity stages for the run's calibration screen, replacing
   * any row the run had (DECISIONS.md 611). False when the console has no
   * table for them yet. Throws StoreError, never naming a stage.
   */
  saveOrgStages(runId: string, stages: OrgStage[], readAt: string): Promise<boolean>;
  /**
   * An automatic run's calibration, read from the org's settings (DECISIONS.md
   * 692), with its source. Only where the run has none: "saved" when this
   * wrote it, "exists" when the run already had one, which is left as it is,
   * and "no_source_column" when the console's table can't hold the source, so
   * nothing is written (without it the row would read as a person's
   * confirmation). Throws StoreError, naming no stage.
   */
  saveCalibration(runId: string, calibration: Calibration): Promise<"saved" | "exists" | "no_source_column">;
  /**
   * Names a saved automatic calibration's currency from the org's settings
   * (DECISIONS.md 745): the "currency" column and the standing in "fields"
   * (currencyFrom "connect_api", no suggestion) and nothing else, in one
   * statement, only while the row is as the caller read it: from the org's
   * settings, confirmed by no person, with the same currency and standing.
   * "updated" when this wrote it; "unchanged" when the row no longer matched
   * (another delivery named it, or a person confirmed one), so nothing was
   * written. Throws StoreError by SQLSTATE alone.
   */
  nameCalibrationCurrency(runId: string, was: CurrencyWas, code: string): Promise<"updated" | "unchanged">;
  /**
   * The currency the org's customer named in the portal (KAN-41; the
   * console's HealthCheckCustomerAnswer, migration 20261219): the newest
   * answer of the active customer whose org this is, by its 15-character id.
   * Null with none, with a code that isn't a currency's, or where the console
   * has no table for it yet. One read-only statement; throws StoreError on
   * any other fault, naming no value.
   */
  customerCurrency(orgId15: string): Promise<string | null>;
  /**
   * Why an automatic run waits for a person, in the console's "heldReason"
   * (migration 20261211). Only an automatic run's, and only where none is set,
   * with its HealthCheckRunFlag row raised in the same statement (DECISIONS.md
   * 693), so a hold always reaches a person. False when the console's table
   * has no such column or table yet, or refused it, so the caller says so.
   * Never throws for that.
   */
  holdRun(runId: string, reason: "calibration_unreadable", at: string, counts?: Record<string, number>): Promise<boolean>;
  /**
   * The overall score of the org's last released run of this product, other
   * than this one, or null with none (DECISIONS.md 693). Only a run whose
   * status is released counts: a held or failed run's score never does.
   */
  lastReleasedScore(runId: string, orgId15: string, product: string): Promise<number | null>;
  /**
   * The org's previous automatic Barometer run, any status, by start time: the newest started before this one,
   * never this one (a Run again is the same run). Null with none; `losses` null where that run's are not known
   * (stored before DECISIONS.md 723, or never decided). One read-only statement. Throws on a database fault: the
   * caller treats that as not known.
   */
  previousRunLosses(runId: string, orgId15: string): Promise<{ runId: string; losses: RunLosses | null } | null>;
  /**
   * Where an automatic run stands after its connect (DECISIONS.md 693), so a
   * redelivered connect can finish what a dropped one started. Null for a run
   * that isn't there.
   */
  automaticRunState(runId: string): Promise<AutomaticRunState | null>;
  /**
   * What a run is, for a pulse to follow its baselines back (DECISIONS.md
   * 746): its org, product, review mode and baseline, or null when there is no
   * such run. One read-only statement; throws on a store fault.
   */
  runLineage(runId: string): Promise<RunLineage | null>;
  /**
   * Where a pulse stands for the customer (DECISIONS.md 746): whether it has a
   * result (so the portal lists it), whether a held one's result is kept for a
   * person's Show, and its open flags. Null for no such run. Read-only; throws
   * on a store fault.
   */
  pulseState(runId: string): Promise<PulseState | null>;
  /**
   * A run's stored overall score (DECISIONS.md 626), or null with none, no run or
   * a console without the column. For a pulse's "before" when its baseline's
   * folder isn't on this worker (746). Read-only; throws on any other fault.
   */
  runOverallScore(runId: string): Promise<number | null>;
  /**
   * A person's Show of an automatic customer's held pulse (DECISIONS.md 746), as
   * the console's Show of a held automatic check, in one transaction: refused
   * unless the run is a pulse whose calibration was read from the org's
   * settings, it has a kept result and no result, and its open flags are all
   * showable (never no_digest or digest_held, whose result was never kept). Then
   * the kept result becomes its result, the kept copy goes, the open flags are
   * cleared by that person, and an audit row says so. The caller has checked
   * the pulse's chain ends at an automatic check of its org (showHeldPulse in
   * pulse-show.ts).
   */
  showHeldPulse(runId: string, by: string, at: string): Promise<{ ok: true; flagsCleared: string[] } | { ok: false; why: string }>;

  /** With `handbookSummaries` unset or false, nothing reads the summaries' table. */
  loadDocumentsJob(runId: string, reads?: DocumentsJobReads): Promise<DocumentsJobInput | null>;
  /** Moves queued (or making, on a second delivery) to making. False if the console hasn't asked, so there's nothing to do. */
  startDocuments(runId: string, at: string): Promise<boolean>;
  /**
   * With a digest (DECISIONS.md 632), it's written in the same transaction, in
   * a savepoint of its own: a console without the table, or one that refuses
   * the row, costs the digest only, never the documents' row. With null (a
   * make whose digest couldn't be made) any earlier row is deleted the same
   * way, so the connector never serves figures the documents no longer print.
   * Left out, the table isn't named. With `surveyMetrics` (DECISIONS.md 645),
   * the run's `survey.*` metric rows are replaced by the printed report's, in
   * the same transaction and only when the row was marked done, in a savepoint
   * of their own; left out, they're left alone. True when the row was marked
   * done, so the caller knows these were written (a console that moved the row
   * on meanwhile gets none of it).
   */
  saveDocuments(runId: string, made: DocumentsMade, digest?: RunDigest | null, surveyMetrics?: Metric[]): Promise<boolean>;
  /** A reason the console can show. Never a token or a name. */
  failDocuments(runId: string, reason: string, at: string): Promise<void>;
  /**
   * One make of the portal's documents (DECISIONS.md 698), in one transaction:
   * each row upserted on (runId, output), replacing what a make before left,
   * only while the run is a released, unheld automatic run; then, with
   * `heldCounts`, the `documents_held` flag through FLAG_CONFLICT: an open
   * flag is left as it is, a cleared one opened again. Throws, having
   * written nothing, when any of it is refused.
   */
  savePortalDocuments(runId: string, rows: PortalDocumentRow[], heldCounts: Record<string, number> | null): Promise<void>;
  /**
   * Deletes every portal document past its `expiresAt`: how many, or null
   * when the console has no such table yet (42P01).
   */
  deleteExpiredPortalDocuments(now: string): Promise<number | null>;

  /** The run's readers and shares, for a share job or the retention sweep. Null when there's no such run. */
  loadShareJob(runId: string): Promise<ShareJobInput | null>;
  /** One person's share on one folder, replacing the row for that run, folder and address. */
  saveShare(share: DriveShare): Promise<void>;

  /** A consenting run's benchmark figures, all or none. The rows carry nothing that leads back to the run (rule 11). */
  addBenchmarkRows(rows: BenchmarkRow[]): Promise<void>;
  /** Every benchmark row, for comparing a run with other customers. They name no one, so reading them all is safe. */
  loadBenchmarkRows(): Promise<BenchmarkRow[]>;

  /** Records that the worker put a run on the queue itself, as the console does when it queues one. */
  recordRunQueued(runId: string, at: string): Promise<void>;

  /**
   * Whether Salesforce confirmed revoking a free scan's connection (DECISIONS.md
   * 652): not confirmed puts the run's `disconnect` step in as failed with
   * SCAN_REVOKE_NOT_CONFIRMED, replacing any earlier one; confirmed takes it
   * away. Whatever the run's status, since a scan lets go after its results.
   * Never a token.
   */
  noteScanRevoke(runId: string, confirmed: boolean, at: string): Promise<void>;
  /**
   * The run behind a connect link, when the console's run says product scan,
   * or null for any other product, a link with no run, or no link
   * (DECISIONS.md 652): only a positive read makes a connection a scan's.
   */
  scanRunOfLink(linkId: string): Promise<string | null>;
  /** A run's product and status, or null when there's no such run (DECISIONS.md 652). */
  runState(runId: string): Promise<{ product: string; status: string } | null>;
  /**
   * For each connection name some run names, the products of the runs naming
   * it, so the scan sweep needn't decrypt a customer's connection to know it
   * isn't a scan's (DECISIONS.md 652). Names no run names are left out.
   */
  connectionProducts(names: string[]): Promise<Map<string, Array<{ runId: string; product: string }>>>;

  /** A free scan's teaser, for the console to show the prospect. One row a run. */
  saveScanResult(runId: string, result: ScanResult, madeAt: string): Promise<void>;

  /** A monthly pulse's result (the console's HealthCheckPulseResult): one row a run, replaced on a second delivery. */
  savePulseResult(runId: string, result: PulseResult, madeAt: string): Promise<void>;
  /** Every pulse run, with when it finished. A pulse is never released, so retention counts from this. */
  listPulseRuns(): Promise<PulseRun[]>;

  /** The run's customer microsite, sealed: one row a run, its version moving on with each make. */
  saveMicrosite(runId: string, sealedPages: string, madeAt: string, expiresAt: string): Promise<void>;

  /** Every run the console has released, with when its last output was released. Null when no release date can be read. */
  listReleasedRuns(): Promise<ReleasedRun[]>;
}

/** A pulse run, for the retention sweep. Null when it hasn't finished, or the date can't be read. ISO UTC. */
export type PulseRun = { runId: string; finishedAt: string | null };

/** A released run, for the retention sweep. The date is ISO UTC. */
export type ReleasedRun = { runId: string; releasedAt: string | null };
