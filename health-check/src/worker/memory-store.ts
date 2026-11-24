import { isMyDomainHost } from "../connect/instance-host.ts";
import { StoreError, type OrgStage } from "../connect/org-stages.ts";
import type { ScanResult } from "../scan/result.ts";
import type { PulseResult } from "../pulse/result.ts";
import { DIGEST_KEYS, type RunDigest } from "../schemas/digest.ts";
import { unstorableAt } from "./inventory.ts";
import { Run, type BenchmarkRow, type Calibration, type ConnectLink, type Metric, type Release, type ReportSection } from "../schemas/index.ts";
import { assertChecks, CONSOLE_CHECKS } from "./console-checks.ts";
import { storedFromRow, waitingRow, type ConsoleSummariesRow, type SummariesToSave } from "../handbook/summary-row.ts";
import { DISCONNECT_STEP, ORG_AWAITING_CONNECT, RunClosedError, SCAN_REVOKE_NOT_CONFIRMED, type ConnectJobInput, type DocumentsJobInput, type DocumentsJobReads, type DriveShare, type ShareJobInput, type DocumentsMade, type LinkConnected, type LinkState, type PulseRun, type ReleasedRun, type RunJobInput, type RunOutputRecord, type RunProgress, type WorkerStore, type AutomaticOutcome, type AutomaticRunState, RUN_FAILED_FLAG, STOPPED_BUDGET_FLAG, DOCUMENTS_HELD_FLAG, type PortalDocumentRow } from "./types.ts";

// The WorkerStore for tests: the console's tables as plain maps, with the same
// rules a database store must keep. Writes replace; a run in review or
// released refuses the worker; only a pending link can be settled.

/** The run's overall score and grade are the console's columns (DECISIONS.md 626), kept beside the output, never in it, and only once written. */
export type StoredRun = RunJobInput & { runDir: string | null; output: Omit<RunOutputRecord, "progress" | "runDir" | "overall" | "digest"> | null; overallScore?: number | null; overallGrade?: string | null };
/** `instanceHost` is the console's column (DECISIONS.md 626), there only once written. */
export type StoredLink = Omit<ConnectJobInput, "runOrgId" | "runId"> & { runId: string | null; reason: string | null; connected: LinkConnected | null; refusedAt: string | null; instanceHost?: string };

const CLOSED = new Set(["in_review", "released"]);

/** A section as the console's HealthCheckSection row holds its status and approval. */
const sectionCheckRow = (s: ReportSection): Record<string, unknown> => ({ status: s.status, approvedBy: s.approval?.approvedBy ?? null, approvedAt: s.approval?.approvedAt ?? null });

export class MemoryWorkerStore implements WorkerStore {
  /**
   * As a console database without the migration that adds the run's overall
   * score and grade and the link's host (DECISIONS.md 626): every save and
   * every link is as it was before those columns, which are never written.
   */
  readonly #withoutNewerColumns: boolean;

  constructor(options: { withoutNewerColumns?: boolean } = {}) {
    this.#withoutNewerColumns = options.withoutNewerColumns ?? false;
  }

  readonly runs = new Map<string, StoredRun>();
  readonly links = new Map<string, StoredLink>();
  /** Every write, in order, so tests can see what a job did. */
  readonly writes: Array<{ method: string; id: string; detail?: unknown }> = [];

  addRun(input: RunJobInput): void {
    this.runs.set(input.run.id, { ...structuredClone(input), runDir: null, output: null });
  }

  /** A link for a run, which needn't be added: without one the link takes whichever org connects. */
  addLink(link: ConnectLink, state: LinkState, sealed: { pending: string | null; code: string | null } = { pending: null, code: null }, runId?: string): void {
    this.links.set(link.id, { link: structuredClone(link), runId: runId ?? null, state, sealedPending: sealed.pending, sealedCode: sealed.code, reason: null, connected: null, refusedAt: null });
  }

  #run(runId: string): StoredRun {
    const run = this.runs.get(runId);
    if (!run) throw new Error(`No run ${runId}`);
    return run;
  }

  #open(runId: string): StoredRun {
    const run = this.#run(runId);
    if (CLOSED.has(run.run.status)) throw new RunClosedError(runId, run.run.status);
    return run;
  }

  async loadRunJob(runId: string): Promise<RunJobInput | null> {
    const run = this.runs.get(runId);
    if (!run) return null;
    const { runDir: _dir, output: _output, ...input } = structuredClone(run);
    // As the Postgres store reads it: an automatic run's hold, or none (DECISIONS.md 693).
    return input.run.reviewMode === "automatic" ? { ...input, heldReason: this.held.get(runId) ?? null } : input;
  }

  /** As the Postgres store's one statement: only awaiting manual steps changes, and a closed run refuses. */
  async claimRun(runId: string): Promise<void> {
    const run = this.#open(runId);
    if (run.run.status === "awaiting_manual") run.run = Run.parse({ ...run.run, status: "analysing" });
    this.writes.push({ method: "claimRun", id: runId, detail: run.run.status });
  }

  async saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
    const run = this.#open(runId);
    run.run = Run.parse({ ...run.run, ...structuredClone(progress) });
    run.runDir = runDir;
    this.writes.push({ method: "saveRunProgress", id: runId, detail: progress.status });
  }

  async saveRunOutput(runId: string, output: RunOutputRecord): Promise<void> {
    const run = this.#open(runId);
    // Every row held to the console's own checks before anything is kept, as one transaction would be (DECISIONS.md 693).
    for (const s of output.sections) assertChecks(sectionCheckRow(s), CONSOLE_CHECKS.sectionApproved, CONSOLE_CHECKS.sectionApproval);
    if (output.automatic && run.run.status === "failed") throw new RunClosedError(runId, "failed");
    if (output.automatic) this.#checkAutomatic(run, output);
    const { progress, runDir, overall, digest, automatic, ...rest } = structuredClone(output);
    run.run = Run.parse({ ...run.run, ...progress });
    run.runDir = runDir;
    run.output = rest;
    // As the Postgres store's own statements: null where there's no score, untouched where none was given.
    if (overall !== undefined && !this.#withoutNewerColumns) Object.assign(run, { overallScore: overall?.score ?? null, overallGrade: overall?.grade ?? null });
    if (rest.handbookSummaries) this.#saveSummaries(runId, rest.handbookSummaries);
    if (digest !== undefined) this.#saveDigest(runId, digest);
    this.writes.push({ method: "saveRunOutput", id: runId, detail: progress.status });
    if (automatic) this.#writeAutomatic(run, automatic);
  }

  /** The console's HealthCheckRelease rows, by run id. A test may add a person's or an earlier run's. */
  readonly releases = new Map<string, Release[]>();
  /** The console's HealthCheckRunFlag rows, by run id and kind. */
  readonly flags = new Map<string, { runId: string; kind: string; raisedAt: string; counts: Record<string, number>; postedAt: string | null; clearedBy: string | null; clearedAt: string | null }>();

  /** As the console's cron and a person do: a flag posted to staff, then cleared. Tests only. */
  postAndClearFlag(runId: string, kind: string, by: string, at: string): void {
    const flag = this.flags.get(`${runId}|${kind}`);
    if (!flag) throw new Error(`No ${kind} flag on ${runId}`);
    Object.assign(flag, { postedAt: at, clearedBy: by, clearedAt: at });
    assertChecks(flag, CONSOLE_CHECKS.flagCleared);
  }

  /** As the database would refuse the release or hold, before anything is written: its checks, its unique index, and a digest it must hold. */
  #checkAutomatic(run: StoredRun, output: RunOutputRecord): void {
    const a = output.automatic!;
    const reviewMode = run.run.reviewMode === "automatic" ? "automatic" : null;
    // The run's real hold, as the database has it: the Postgres store's statements match no row on a held run (DECISIONS.md 693).
    const heldReason = this.held.get(run.run.id) ?? null;
    assertChecks({ reviewMode }, CONSOLE_CHECKS.reviewMode);
    if (heldReason !== null || (a.decision === "release" && run.run.status === "failed")) throw new Error(`Run ${run.run.id} couldn't be ${a.decision === "release" ? "released" : "held"}: the console doesn't have it as an unreleased, unheld automatic run`);
    if (a.decision === "release") {
      if (!output.digest || !this.digestTable || unstorableAt(output.digest, [], DIGEST_KEYS)) throw new Error(`Run ${run.run.id}'s digest couldn't be written, so it isn't released`);
      for (const r of a.releases) {
        assertChecks({ kind: r.kind, releasedBy: r.releasedBy }, CONSOLE_CHECKS.releaseKind, CONSOLE_CHECKS.releasedBy);
        if ((this.releases.get(run.run.id) ?? []).some((held) => held.output === r.output)) throw Object.assign(new Error("duplicate key value violates unique constraint \"HealthCheckRelease_runId_output_key\""), { code: "23505" });
      }
      assertChecks({ status: "released", heldReason, reviewMode }, CONSOLE_CHECKS.runStatus, CONSOLE_CHECKS.held, CONSOLE_CHECKS.heldReason);
      // As the Postgres store's statement: only an automatic run is released by code.
      if (reviewMode !== "automatic") throw new Error(`Run ${run.run.id} isn't automatic, so it can't be released by code`);
      return;
    }
    assertChecks({ status: output.progress.status, heldReason: a.reason, reviewMode }, CONSOLE_CHECKS.runStatus, CONSOLE_CHECKS.held, CONSOLE_CHECKS.heldReason);
    for (const f of [{ kind: a.reason, counts: a.counts }, ...(a.also ?? [])]) assertChecks({ kind: f.kind, counts: f.counts, clearedBy: null, clearedAt: null }, CONSOLE_CHECKS.flagKind, CONSOLE_CHECKS.flagCounts, CONSOLE_CHECKS.flagCleared);
  }

  #writeAutomatic(run: StoredRun, a: AutomaticOutcome): void {
    const runId = run.run.id;
    if (a.decision === "release") {
      this.releases.set(runId, [...(this.releases.get(runId) ?? []), ...structuredClone(a.releases)]);
      run.run = Run.parse({ ...run.run, status: "released" });
      this.writes.push({ method: "releaseAutomatic", id: runId, detail: a.releases.map((r) => r.output) });
      return;
    }
    this.held.set(runId, a.reason);
    this.writes.push({ method: "holdRun", id: runId, detail: a.reason });
    this.#raiseFlag(runId, a.reason, a.at, a.counts);
    for (const f of a.also ?? []) this.#raiseFlag(runId, f.kind, a.at, f.counts);
  }

  /** As the Postgres store's FLAG_CONFLICT: an open flag is left as it is; a cleared one is opened again with the new time and counts, unposted (DECISIONS.md 698). */
  #raiseFlag(runId: string, kind: string, at: string, counts: Record<string, number>): void {
    const row = { runId, kind, raisedAt: at, counts: structuredClone(counts), postedAt: null, clearedBy: null, clearedAt: null };
    assertChecks(row, CONSOLE_CHECKS.flagKind, CONSOLE_CHECKS.flagCounts, CONSOLE_CHECKS.flagCleared);
    const key = `${runId}|${kind}`;
    const open = this.flags.get(key);
    if (open && open.clearedAt === null) return;
    this.flags.set(key, row);
    this.writes.push({ method: "raiseFlag", id: runId, detail: kind });
  }

  /** A run's flags, by kind. */
  flagsOf(runId: string): string[] {
    return [...this.flags.values()].filter((f) => f.runId === runId).map((f) => f.kind);
  }

  async lastReleasedScore(runId: string, orgId15: string, product: string): Promise<number | null> {
    let best: { at: string; score: number } | null = null;
    for (const run of this.runs.values()) {
      if (run.run.id === runId || run.run.status !== "released" || run.run.product !== product || run.run.orgId.slice(0, 15) !== orgId15) continue;
      if (typeof run.overallScore !== "number") continue;
      const at = (this.releases.get(run.run.id) ?? []).map((r) => r.releasedAt).sort().at(-1);
      if (at && (!best || at > best.at)) best = { at, score: run.overallScore };
    }
    return best?.score ?? null;
  }

  async automaticRunState(runId: string): Promise<AutomaticRunState | null> {
    const run = this.runs.get(runId);
    if (!run) return null;
    return { calibrated: run.calibration !== null, queued: this.queued.includes(runId), held: this.held.has(runId), status: run.run.status };
  }

  /** The console's HealthCheckRunDigest, keyed by run id (DECISIONS.md 632). */
  readonly digests = new Map<string, { madeAt: string; version: number; digest: RunDigest }>();
  /** False stands in for a console without the digest's table: the digest is left out and everything else saves. */
  digestTable = true;

  /**
   * As the Postgres store's savepoint: replaced whole; without the table, left
   * out with the rest saved; and with none to write, or one holding a string
   * jsonb refuses, any earlier row deleted, so a stale digest is never served.
   */
  #saveDigest(runId: string, digest: RunDigest | null): void {
    if (!this.digestTable) return;
    if (digest && !unstorableAt(digest, [], DIGEST_KEYS)) {
      this.digests.set(runId, structuredClone({ madeAt: digest.madeAt, version: digest.version, digest }));
      this.writes.push({ method: "saveRunDigest", id: runId });
      return;
    }
    if (this.digests.delete(runId)) this.writes.push({ method: "deleteRunDigest", id: runId });
  }

  /** The digest a run has, or null: what the console's tolerant reader returns. */
  getRunDigest(runId: string): RunDigest | null {
    return structuredClone(this.digests.get(runId)?.digest ?? null);
  }

  /** The console's HealthCheckHandbookSummaries, keyed by run id (DECISIONS.md 494). */
  readonly summaries = new Map<string, ConsoleSummariesRow>();

  /** As the Postgres store's one statement: a new row, or a waiting one replaced with nothing struck; a decided row untouched; a row the console couldn't read never written. */
  #saveSummaries(runId: string, summaries: SummariesToSave): void {
    const checked = waitingRow(runId, summaries);
    if (!checked.ok || !checked.row) return;
    const held = this.summaries.get(runId);
    if (!held) this.summaries.set(runId, checked.row);
    else if (held.status === "waiting") this.summaries.set(runId, { ...held, model: checked.row.model, madeAt: checked.row.madeAt, items: checked.row.items, leftOut: checked.row.leftOut });
  }

  /** As the console strikes a summary out: only while the row waits. */
  strikeSummary(runId: string, index: number): void {
    const row = this.summaries.get(runId);
    if (row?.status !== "waiting" || !row.items[index]) throw new Error(`No waiting summary ${index} for run ${runId}`);
    row.items[index] = { ...row.items[index]!, struck: true };
  }

  /** As the console decides: only a waiting row, with who and when. */
  decideSummaries(runId: string, status: "approved" | "left_out", by = "consultant@example.com", at = "2026-11-05T10:00:00.000Z"): void {
    const row = this.summaries.get(runId);
    if (row?.status !== "waiting") throw new Error(`Run ${runId}'s summaries aren't waiting`);
    this.summaries.set(runId, { ...row, status, decidedBy: by, decidedAt: at });
  }

  async recordRunFailure(runId: string, reason: string, at: string): Promise<void> {
    const run = this.#open(runId);
    const steps = run.run.steps.filter((s) => s.id !== "worker");
    steps.push({ id: "worker", version: 1, status: "failed", calls: 0, finishedAt: at, error: reason });
    run.run = Run.parse({ ...run.run, status: "failed", steps });
    this.writes.push({ method: "recordRunFailure", id: runId, detail: reason });
    // An automatic run that fails is a person's to look at (DECISIONS.md 693). Counts only: the reason is in the step.
    if (run.run.reviewMode === "automatic") this.#raiseFlag(runId, RUN_FAILED_FLAG, at, {});
  }

  /** As the Postgres store's one statement: only an automatic run stopped at its budget. */
  async flagBudgetStop(runId: string, at: string): Promise<boolean> {
    const run = this.runs.get(runId);
    if (run?.run.reviewMode !== "automatic" || run.run.status !== "stopped_budget") return false;
    const before = this.writes.length;
    this.#raiseFlag(runId, STOPPED_BUDGET_FLAG, at, {});
    return this.writes.length > before;
  }

  /** As the Postgres store: the run's disconnect step in or out, whatever the run's status. */
  async noteScanRevoke(runId: string, confirmed: boolean, at: string): Promise<void> {
    const run = this.#run(runId);
    const steps = run.run.steps.filter((s) => s.id !== DISCONNECT_STEP);
    if (!confirmed) steps.push({ id: DISCONNECT_STEP, version: 1, status: "failed", calls: 0, finishedAt: at, error: SCAN_REVOKE_NOT_CONFIRMED });
    run.run = Run.parse({ ...run.run, steps });
    this.writes.push({ method: "noteScanRevoke", id: runId, detail: confirmed ? "confirmed" : "not confirmed" });
  }

  async scanRunOfLink(linkId: string): Promise<string | null> {
    const runId = this.links.get(linkId)?.runId;
    const run = runId ? this.runs.get(runId) : undefined;
    return run?.run.product === "scan" ? run.run.id : null;
  }

  async runState(runId: string): Promise<{ product: string; status: string } | null> {
    const run = this.runs.get(runId);
    return run ? { product: run.run.product, status: run.run.status } : null;
  }

  async connectionProducts(names: string[]): Promise<Map<string, Array<{ runId: string; product: string }>>> {
    const found = new Map<string, Array<{ runId: string; product: string }>>();
    for (const run of this.runs.values()) {
      if (!run.connectionName || !names.includes(run.connectionName)) continue;
      found.set(run.connectionName, [...(found.get(run.connectionName) ?? []), { runId: run.run.id, product: run.run.product }]);
    }
    return found;
  }

  async loadConnectJob(linkId: string): Promise<ConnectJobInput | null> {
    const link = this.links.get(linkId);
    if (!link) return null;
    const run = link.runId ? this.runs.get(link.runId) : undefined;
    const runOrgId = run?.run.orgId || ORG_AWAITING_CONNECT;
    const job: ConnectJobInput = { link: link.link, state: link.state, runOrgId, sealedPending: link.sealedPending, sealedCode: link.sealedCode };
    const withRun = run ? { ...job, runId: run.run.id } : job;
    if (run?.run.product === "scan") return structuredClone({ ...withRun, scanRunId: run.run.id });
    // An automatic full check (DECISIONS.md 692), as the Postgres store reads it.
    return structuredClone(run?.run.product === "barometer" && run.run.reviewMode === "automatic" ? { ...withRun, automaticRunId: run.run.id } : withRun);
  }

  /** False stands in for a console without HealthCheckCalibration's "source" column: nothing is written. */
  calibrationSourceColumn = true;

  /** As the Postgres store's one statement: written only where the run has none. */
  async saveCalibration(runId: string, calibration: Calibration): Promise<"saved" | "exists" | "no_source_column"> {
    if (!this.calibrationSourceColumn) return "no_source_column";
    const run = this.runs.get(runId);
    if (!run) throw new StoreError("the console's database refused the calibration (SQLSTATE 23503)");
    if (run.calibration) return "exists";
    run.calibration = structuredClone(calibration);
    // No detail: the stage names are the org's own.
    this.writes.push({ method: "saveCalibration", id: runId });
    return "saved";
  }

  /** The console's "heldReason", by run id. False as a console without the column. */
  readonly held = new Map<string, string>();
  heldReasonColumn = true;

  async holdRun(runId: string, reason: "calibration_unreadable", at: string, counts: Record<string, number> = {}): Promise<boolean> {
    const run = this.runs.get(runId);
    if (!this.heldReasonColumn || !run || run.run.reviewMode !== "automatic" || run.run.status === "released" || this.held.has(runId)) return false;
    this.held.set(runId, reason);
    this.writes.push({ method: "holdRun", id: runId, detail: reason });
    // In the same statement as the hold (DECISIONS.md 693).
    this.#raiseFlag(runId, reason, at, { ...counts });
    return true;
  }

  /** As the Postgres store's one statement: another scan made for the org, or connected to it, after `since`. */
  async lastScanOfOrg(orgId15: string, exceptRunId: string, since: string): Promise<string | null> {
    const after = Date.parse(since);
    const scan = (runId: string | null | undefined) => {
      const run = runId ? this.runs.get(runId) : undefined;
      return run?.run.product === "scan" && run.run.id !== exceptRunId ? run : undefined;
    };
    const moments: number[] = [];
    for (const run of this.runs.values()) {
      if (scan(run.run.id) && run.run.orgId.slice(0, 15) === orgId15 && Date.parse(run.run.startedAt) > after) moments.push(Date.parse(run.run.startedAt));
    }
    for (const link of this.links.values()) {
      if (scan(link.runId) && link.connected && link.connected.orgId.slice(0, 15) === orgId15 && Date.parse(link.connected.at) > after) moments.push(Date.parse(link.connected.at));
    }
    return moments.length ? new Date(Math.max(...moments)).toISOString() : null;
  }

  /** The console's HealthCheckOrgStages, keyed by run id (DECISIONS.md 611). False as a console without the table. */
  readonly orgStages = new Map<string, { readAt: string; stages: OrgStage[] }>();
  orgStagesTable = true;

  async saveOrgStages(runId: string, stages: OrgStage[], readAt: string): Promise<boolean> {
    if (!this.orgStagesTable) return false;
    if (!this.runs.has(runId)) throw new StoreError("the console's database refused it (SQLSTATE 23503)");
    this.orgStages.set(runId, structuredClone({ readAt, stages }));
    // No detail: the stage names are the org's own and stay in the one row.
    this.writes.push({ method: "saveOrgStages", id: runId });
    return true;
  }

  #pending(linkId: string): StoredLink {
    const link = this.links.get(linkId);
    if (!link) throw new Error(`No link ${linkId}`);
    if (link.state !== "pending") throw new Error(`Link ${linkId} is ${link.state}, not pending`);
    return link;
  }

  async markLinkUsed(linkId: string, connected: LinkConnected, instanceHost?: string | null): Promise<void> {
    const link = this.#pending(linkId);
    Object.assign(link, { state: "used", sealedPending: null, sealedCode: null, connected: structuredClone(connected), reason: connected.note });
    if (isMyDomainHost(instanceHost) && !this.#withoutNewerColumns) link.instanceHost = instanceHost;
    link.link.usedAt = connected.at;
    const run = link.runId ? this.runs.get(link.runId) : undefined;
    if (run?.run.orgId === ORG_AWAITING_CONNECT) run.run.orgId = connected.orgId;
    this.writes.push({ method: "markLinkUsed", id: linkId, detail: connected });
  }

  async refuseLink(linkId: string, reason: string, at: string): Promise<void> {
    const link = this.#pending(linkId);
    Object.assign(link, { state: "refused", sealedPending: null, sealedCode: null, reason, refusedAt: at });
    this.writes.push({ method: "refuseLink", id: linkId, detail: reason });
  }

  /** The released runs' documents, keyed by run id: what the job reads, and its row as it now stands. */
  readonly documents = new Map<string, { input: DocumentsJobInput; made: DocumentsMade | null; error: string | null }>();

  addDocumentsJob(input: DocumentsJobInput): void {
    this.documents.set(input.run.id, { input: structuredClone(input), made: null, error: null });
  }

  async loadDocumentsJob(runId: string, reads: DocumentsJobReads = {}): Promise<DocumentsJobInput | null> {
    const doc = this.documents.get(runId);
    if (!doc) return null;
    // As the Postgres store reads it: only with the switch on, held to the console's schema, and only what the handbook prints from.
    const row = reads.handbookSummaries === true ? this.summaries.get(runId) : undefined;
    const read = row === undefined ? null : storedFromRow((({ decidedBy: _by, decidedAt: _at, model: _model, madeAt: _made, ...rest }) => rest)(row));
    // As the Postgres store reads it: an automatic run's hold, or none (DECISIONS.md 698).
    const held = doc.input.run.reviewMode === "automatic" ? { heldReason: this.held.get(runId) ?? null } : {};
    return structuredClone({ ...doc.input, ...held, handbookSummaries: read?.ok ? read.stored : null });
  }

  async startDocuments(runId: string, _at: string): Promise<boolean> {
    const doc = this.documents.get(runId);
    if (!doc || (doc.input.state !== "queued" && doc.input.state !== "making")) return false;
    doc.input.state = "making";
    this.writes.push({ method: "startDocuments", id: runId });
    return true;
  }

  async saveDocuments(runId: string, made: DocumentsMade, digest?: RunDigest | null, surveyMetrics?: Metric[]): Promise<boolean> {
    const doc = this.documents.get(runId);
    if (doc?.input.state !== "making") return false;
    Object.assign(doc, { made: structuredClone(made), error: null });
    Object.assign(doc.input, { state: "done", driveFolderId: made.driveFolderId, files: structuredClone(made.files) });
    this.writes.push({ method: "saveDocuments", id: runId, detail: made.folderName });
    if (digest !== undefined) this.#saveDigest(runId, digest);
    // As the Postgres store's: the run's survey rows replaced by the printed report's (DECISIONS.md 645).
    const output = this.runs.get(runId)?.output;
    if (surveyMetrics !== undefined && output) {
      output.surveyMetrics = structuredClone(surveyMetrics);
      this.writes.push({ method: "saveSurveyMetrics", id: runId, detail: surveyMetrics.length });
    }
    return true;
  }

  /** Every metric row the run holds, as the console's HealthCheckMetric would: the org's, then the survey's (DECISIONS.md 645). */
  storedMetrics(runId: string): Metric[] {
    const output = this.runs.get(runId)?.output;
    return structuredClone([...(output?.metrics ?? []), ...(output?.surveyMetrics ?? [])]);
  }

  /** The benchmark table. */
  readonly benchmarkRows: BenchmarkRow[] = [];

  async addBenchmarkRows(rows: BenchmarkRow[]): Promise<void> {
    this.benchmarkRows.push(...structuredClone(rows));
    this.writes.push({ method: "addBenchmarkRows", id: "benchmark", detail: rows.length });
  }

  /** Runs the worker queued itself, in order. */
  readonly queued: string[] = [];

  async recordRunQueued(runId: string, _at: string): Promise<void> {
    this.queued.push(runId);
    this.writes.push({ method: "recordRunQueued", id: runId });
  }

  /** The free scans' teasers, keyed by run id. */
  readonly scanResults = new Map<string, { result: ScanResult; madeAt: string }>();

  async saveScanResult(runId: string, result: ScanResult, madeAt: string): Promise<void> {
    this.scanResults.set(runId, { result: structuredClone(result), madeAt });
    this.writes.push({ method: "saveScanResult", id: runId });
  }

  /** The pulses' results, keyed by run id. */
  readonly pulseResults = new Map<string, { result: PulseResult; madeAt: string }>();

  async savePulseResult(runId: string, result: PulseResult, madeAt: string): Promise<void> {
    this.pulseResults.set(runId, { result: structuredClone(result), madeAt });
    this.writes.push({ method: "savePulseResult", id: runId });
  }

  async listPulseRuns(): Promise<PulseRun[]> {
    return [...this.runs.values()].filter((r) => r.run.product === "pulse").map((r) => ({ runId: r.run.id, finishedAt: r.run.finishedAt ?? null }));
  }

  /** The microsites, keyed by run id. */
  readonly microsites = new Map<string, { version: number; sealedPages: string; madeAt: string; expiresAt: string }>();

  async saveMicrosite(runId: string, sealedPages: string, madeAt: string, expiresAt: string): Promise<void> {
    const version = (this.microsites.get(runId)?.version ?? 0) + 1;
    this.microsites.set(runId, { version, sealedPages, madeAt, expiresAt });
    this.writes.push({ method: "saveMicrosite", id: runId, detail: version });
  }

  async loadBenchmarkRows(): Promise<BenchmarkRow[]> {
    return structuredClone(this.benchmarkRows);
  }

  /** When each run's last output was released, as the console's release records hold it. */
  readonly releasedAt = new Map<string, string>();

  async listReleasedRuns(): Promise<ReleasedRun[]> {
    return [...this.runs.values()].filter((r) => r.run.status === "released").map((r) => ({ runId: r.run.id, releasedAt: this.releasedAt.get(r.run.id) ?? null }));
  }

  /** The microsite's readers, by run id, as the console keeps them. */
  readonly readers = new Map<string, string[]>();
  /** The console's share rows. */
  shares: DriveShare[] = [];
  /** False stands in for a console without the share table. */
  sharesRecordable = true;

  async loadShareJob(runId: string): Promise<ShareJobInput | null> {
    const run = this.runs.get(runId)?.run ?? this.documents.get(runId)?.input.run;
    if (!run) return null;
    return {
      runId,
      released: run.status === "released",
      releasedAt: this.releasedAt.get(runId) ?? null,
      folderId: this.documents.get(runId)?.input.driveFolderId ?? null,
      readers: [...(this.readers.get(runId) ?? [])],
      shares: structuredClone(this.shares.filter((s) => s.runId === runId)),
      recordable: this.sharesRecordable,
    };
  }

  async saveShare(share: DriveShare): Promise<void> {
    const at = this.shares.findIndex((s) => s.runId === share.runId && s.folderId === share.folderId && s.email === share.email);
    if (at === -1) this.shares.push(structuredClone(share));
    else this.shares[at] = structuredClone(share);
    this.writes.push({ method: "saveShare", id: share.runId, detail: share.state });
  }

  /** The console's HealthCheckPortalDocument, keyed by run id and output (DECISIONS.md 698). */
  readonly portalDocuments = new Map<string, PortalDocumentRow>();
  /** False stands in for a console without the table (42P01). */
  portalTable = true;

  /** As the Postgres store's one transaction: every row held to the table's CHECKs and the run's state first, then all written, then the flag. */
  async savePortalDocuments(runId: string, rows: PortalDocumentRow[], heldCounts: Record<string, number> | null): Promise<void> {
    if (!this.portalTable) throw Object.assign(new Error('relation "HealthCheckPortalDocument" does not exist'), { code: "42P01" });
    const run = this.documents.get(runId)?.input.run ?? this.runs.get(runId)?.run;
    if (!run || run.status !== "released" || run.reviewMode !== "automatic" || this.held.has(runId)) throw new Error(`Run ${runId} isn't a released, unheld automatic run, so its portal documents weren't saved`);
    for (const row of rows) {
      // The migration's CHECKs, word for word in effect: the three outputs, 1 byte to 15 MiB, kept 7 days at most.
      if (!["executive_summary", "full_health_check", "ceo_one_pager"].includes(row.output)) throw new Error('new row violates check constraint "HealthCheckPortalDocument_output_check"');
      if (row.sealed.length < 1 || row.sealed.length > 15728640) throw new Error('new row violates check constraint "HealthCheckPortalDocument_sealed_check"');
      const made = Date.parse(row.madeAt);
      const expires = Date.parse(row.expiresAt);
      if (!(expires > made && expires <= made + 7 * 86_400_000)) throw new Error('new row violates check constraint "HealthCheckPortalDocument_expiry_check"');
    }
    for (const row of rows) this.portalDocuments.set(`${row.runId}|${row.output}`, { ...row, sealed: Buffer.from(row.sealed) });
    this.writes.push({ method: "savePortalDocuments", id: runId, detail: rows.map((r) => `${r.output}${r.heldForUs ? " held" : ""}`) });
    if (heldCounts) this.#raiseFlag(runId, DOCUMENTS_HELD_FLAG, rows[0]?.madeAt ?? new Date(0).toISOString(), heldCounts);
  }

  async deleteExpiredPortalDocuments(now: string): Promise<number | null> {
    if (!this.portalTable) return null;
    let deleted = 0;
    for (const [key, row] of this.portalDocuments) {
      if (Date.parse(row.expiresAt) < Date.parse(now)) {
        this.portalDocuments.delete(key);
        deleted++;
      }
    }
    if (deleted) this.writes.push({ method: "deleteExpiredPortalDocuments", id: "portal", detail: deleted });
    return deleted;
  }

  async failDocuments(runId: string, reason: string, _at: string): Promise<void> {
    const doc = this.documents.get(runId);
    if (!doc || (doc.input.state !== "queued" && doc.input.state !== "making")) return;
    doc.input.state = "failed";
    doc.error = reason;
    this.writes.push({ method: "failDocuments", id: runId, detail: reason });
  }
}
