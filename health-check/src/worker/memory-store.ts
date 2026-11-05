import type { ScanResult } from "../scan/result.ts";
import type { PulseResult } from "../pulse/result.ts";
import { Run, type BenchmarkRow, type ConnectLink } from "../schemas/index.ts";
import { ORG_AWAITING_CONNECT, RunClosedError, type ConnectJobInput, type DocumentsJobInput, type DocumentsMade, type LinkConnected, type LinkState, type PulseRun, type ReleasedRun, type RunJobInput, type RunOutputRecord, type RunProgress, type WorkerStore } from "./types.ts";

// The WorkerStore for tests: the console's tables as plain maps, with the same
// rules a database store must keep. Writes replace; a run in review or
// released refuses the worker; only a pending link can be settled.

export type StoredRun = RunJobInput & { runDir: string | null; output: Omit<RunOutputRecord, "progress" | "runDir"> | null };
export type StoredLink = Omit<ConnectJobInput, "runOrgId"> & { runId: string | null; reason: string | null; connected: LinkConnected | null; refusedAt: string | null };

const CLOSED = new Set(["in_review", "released"]);

export class MemoryWorkerStore implements WorkerStore {
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
    return input;
  }

  async saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
    const run = this.#open(runId);
    run.run = Run.parse({ ...run.run, ...structuredClone(progress) });
    run.runDir = runDir;
    this.writes.push({ method: "saveRunProgress", id: runId, detail: progress.status });
  }

  async saveRunOutput(runId: string, output: RunOutputRecord): Promise<void> {
    const run = this.#open(runId);
    const { progress, runDir, ...rest } = structuredClone(output);
    run.run = Run.parse({ ...run.run, ...progress });
    run.runDir = runDir;
    run.output = rest;
    this.writes.push({ method: "saveRunOutput", id: runId, detail: progress.status });
  }

  async recordRunFailure(runId: string, reason: string, at: string): Promise<void> {
    const run = this.#open(runId);
    const steps = run.run.steps.filter((s) => s.id !== "worker");
    steps.push({ id: "worker", version: 1, status: "failed", calls: 0, finishedAt: at, error: reason });
    run.run = Run.parse({ ...run.run, status: "failed", steps });
    this.writes.push({ method: "recordRunFailure", id: runId, detail: reason });
  }

  async loadConnectJob(linkId: string): Promise<ConnectJobInput | null> {
    const link = this.links.get(linkId);
    if (!link) return null;
    const run = link.runId ? this.runs.get(link.runId) : undefined;
    const runOrgId = run?.run.orgId || ORG_AWAITING_CONNECT;
    const job: ConnectJobInput = { link: link.link, state: link.state, runOrgId, sealedPending: link.sealedPending, sealedCode: link.sealedCode };
    return structuredClone(run?.run.product === "scan" ? { ...job, scanRunId: run.run.id } : job);
  }

  #pending(linkId: string): StoredLink {
    const link = this.links.get(linkId);
    if (!link) throw new Error(`No link ${linkId}`);
    if (link.state !== "pending") throw new Error(`Link ${linkId} is ${link.state}, not pending`);
    return link;
  }

  async markLinkUsed(linkId: string, connected: LinkConnected): Promise<void> {
    const link = this.#pending(linkId);
    Object.assign(link, { state: "used", sealedPending: null, sealedCode: null, connected: structuredClone(connected), reason: connected.note });
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

  async loadDocumentsJob(runId: string): Promise<DocumentsJobInput | null> {
    const doc = this.documents.get(runId);
    return doc ? structuredClone(doc.input) : null;
  }

  async startDocuments(runId: string, _at: string): Promise<boolean> {
    const doc = this.documents.get(runId);
    if (!doc || (doc.input.state !== "queued" && doc.input.state !== "making")) return false;
    doc.input.state = "making";
    this.writes.push({ method: "startDocuments", id: runId });
    return true;
  }

  async saveDocuments(runId: string, made: DocumentsMade): Promise<void> {
    const doc = this.documents.get(runId);
    if (doc?.input.state !== "making") return;
    Object.assign(doc, { made: structuredClone(made), error: null });
    Object.assign(doc.input, { state: "done", driveFolderId: made.driveFolderId, files: structuredClone(made.files) });
    this.writes.push({ method: "saveDocuments", id: runId, detail: made.folderName });
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

  async failDocuments(runId: string, reason: string, _at: string): Promise<void> {
    const doc = this.documents.get(runId);
    if (!doc || (doc.input.state !== "queued" && doc.input.state !== "making")) return;
    doc.input.state = "failed";
    doc.error = reason;
    this.writes.push({ method: "failDocuments", id: runId, detail: reason });
  }
}
