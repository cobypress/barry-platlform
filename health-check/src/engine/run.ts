import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { BUDGET_DEFAULTS, RULE_SET_VERSION, SF_DEFAULTS } from "../config.ts";
import { modulesFor } from "../domains/scope.ts";
import { scoreDomains } from "../domains/scoring.ts";
import type { DomainModule } from "../domains/types.ts";
import { orgExtractor } from "../extract/configuration.ts";
import { fieldDescribeExtractor } from "../extract/fields.ts";
import { monthsBefore, type ExtractContext, type Extractor, type PartialStore } from "../extract/types.ts";
import type { UserRow } from "../extract/users.ts";
import { usersExtractor } from "../extract/users.ts";
import { renderRawNotes } from "../notes/raw-notes.ts";
import { Pseudonymiser, type PseudonymEntry } from "../privacy/pseudonymiser.ts";
import { registerSharedNames } from "../privacy/names.ts";
import { Run, type Calibration, type ManualAnswer, type RunScope, type RunStep } from "../schemas/index.ts";
import { ApiBudget, BudgetStopError } from "../salesforce/budget.ts";
import { SalesforceClient } from "../salesforce/client.ts";
import type { ConnectionProvider } from "../salesforce/connection.ts";
import type { Transport } from "../salesforce/transport.ts";
import { analyse, type AnalysisOutput } from "./evaluate.ts";

export type RunOptions = {
  provider: ConnectionProvider;
  calibration: Calibration;
  modules: DomainModule[];
  /** Parent folder. The run writes to <outDir>/<runId>. */
  outDir: string;
  now?: Date;
  /** An existing run id to continue. */
  resume?: string;
  /**
   * The id for a new run, when someone else chose it: the worker uses the
   * console's run id, so the folder and the database row share one name.
   */
  runId?: string;
  /** Called, and awaited, each time run.json is saved. Gets a copy. */
  onProgress?: (run: Run) => void | Promise<void>;
  stopAtShare?: number;
  answers?: ManualAnswer[];
  createdBy?: string;
  customerRef?: string | null;
  transport?: Transport;
  apiVersion?: string;
  pageSize?: number;
  log?: (message: string) => void;
  /** Complete by default. Essentials reads set-up only, and never CRM records. */
  scope?: RunScope;
  /** "scan" for the free set-up teaser. */
  product?: "barometer" | "scan" | "pulse";
  /** The run this one re-checks: its first health check, whose findings the report compares against. */
  baselineRunId?: string | null;
};

export type RunResult = { run: Run; dir: string; output: AnalysisOutput | null };

// Rule 8. Run folders hold CRM records and the pseudonym map, and neither may
// ever land in a repository, so refuse any folder inside a git work tree.
export function assertOutsideGit(path: string): void {
  let dir = resolve(path);
  for (;;) {
    if (existsSync(join(dir, ".git"))) throw new Error(`Refusing to write run data inside the git repository at ${dir}. Use a folder outside it.`);
    const parent = dirname(dir);
    if (parent === dir) return;
    dir = parent;
  }
}

const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/;

const writeJson = (path: string, value: unknown): void => writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
const readJson = <T>(path: string): T => JSON.parse(readFileSync(path, "utf8")) as T;

class FilePartialStore implements PartialStore {
  readonly #dir: string;

  constructor(dir: string) {
    this.#dir = dir;
  }

  #path(key: string): string {
    return join(this.#dir, `${key}.partial.jsonl`);
  }

  read<R>(key: string): R[] {
    if (!existsSync(this.#path(key))) return [];
    return readFileSync(this.#path(key), "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line) as R);
  }

  append<R>(key: string, rows: R[]): void {
    if (rows.length > 0) appendFileSync(this.#path(key), `${rows.map((r) => JSON.stringify(r)).join("\n")}\n`, { mode: 0o600 });
  }

  clear(key: string): void {
    rmSync(this.#path(key), { force: true });
  }
}

export function collectExtractors(modules: DomainModule[]): Extractor[] {
  // Field describes go first, because other reads plan their SELECT from them.
  const seen = new Map<string, Extractor>([
    [orgExtractor.id, orgExtractor],
    [fieldDescribeExtractor.id, fieldDescribeExtractor],
  ]);
  for (const module of modules) for (const extractor of module.extractors) if (!seen.has(extractor.id)) seen.set(extractor.id, extractor);
  return [...seen.values()];
}

const calibrationHash = (calibration: Calibration): string => createHash("sha256").update(JSON.stringify(calibration)).digest("hex").slice(0, 16);

const checkpointPath = (dir: string, id: string): string => join(dir, "checkpoints", `${id}.json`);

function loadPseudonyms(dir: string): Pseudonymiser {
  const path = join(dir, "pseudonym-map.json");
  return new Pseudonymiser(existsSync(path) ? readJson<PseudonymEntry[]>(path) : []);
}

/** Extract, then analyse. Stops cleanly at the API budget. Call again with `resume` to carry on. */
export async function runHealthCheck(options: RunOptions): Promise<RunResult> {
  const { calibration, provider } = options;
  const log = options.log ?? (() => {});
  const now = options.now ?? new Date();
  assertOutsideGit(options.outDir);

  const budget = new ApiBudget({ stopAtShare: options.stopAtShare ?? BUDGET_DEFAULTS.stopAtShare });
  const client = await SalesforceClient.connect(provider, { budget, transport: options.transport, apiVersion: options.apiVersion, sleep: options.transport ? async () => {} : undefined });

  if (calibration.orgId && calibration.orgId.slice(0, 15) !== client.orgId.slice(0, 15)) {
    throw new Error("This calibration was written for a different org.");
  }

  if (options.resume && options.runId && options.resume !== options.runId) throw new Error("resume and runId name different runs");
  const runId = options.resume ?? options.runId ?? `hc-${now.toISOString().slice(0, 10)}-${randomUUID().slice(0, 8)}`;
  // The id becomes a folder name, and a worker's ids come from a database.
  if (!RUN_ID.test(runId)) throw new Error("A run id is letters, digits, hyphens and underscores only");
  const dir = join(options.outDir, runId);
  mkdirSync(join(dir, "checkpoints"), { recursive: true, mode: 0o700 });

  const runPath = join(dir, "run.json");
  // A resumed run keeps the scope it started with.
  const scope: RunScope = options.resume && existsSync(runPath) ? Run.parse(readJson(runPath)).scope : (options.scope ?? "complete");
  if (options.resume && options.scope && options.scope !== scope) throw new Error(`This run is ${scope}, so it can't be resumed as ${options.scope}.`);
  const modules = modulesFor(scope, options.modules);
  const extractors = collectExtractors(modules);
  const hash = calibrationHash(calibration);
  const windowEnd = now;
  const windowStart = monthsBefore(windowEnd, calibration.analysisWindowMonths);

  let run: Run;
  if (options.resume) {
    if (!existsSync(runPath)) throw new Error(`No run "${runId}" in ${options.outDir}`);
    run = Run.parse(readJson(runPath));
    if (run.orgId.slice(0, 15) !== client.orgId.slice(0, 15)) throw new Error("This run belongs to a different org.");
    if (run.calibrationHash !== hash) throw new Error("The calibration has changed since this run started. Start a new run so its numbers stay consistent.");
  } else {
    if (existsSync(runPath)) throw new Error(`A run "${runId}" already exists in ${options.outDir}. Resume it instead`);
    run = Run.parse({
      id: runId,
      product: options.product ?? "barometer",
      customerRef: options.customerRef ?? null,
      orgId: client.orgId,
      isSandbox: false,
      connectionKind: client.connectionKind,
      status: "extracting",
      modules: modules.map((m) => m.id),
      scope,
      ruleSetVersion: RULE_SET_VERSION,
      calibrationHash: hash,
      baselineRunId: options.baselineRunId ?? null,
      asOf: now.toISOString(),
      analysisWindow: { from: windowStart.toISOString().slice(0, 10), to: windowEnd.toISOString().slice(0, 10) },
      apiVersion: client.apiVersion,
      budget: { ...budget.snapshot(), stoppedReason: null },
      steps: extractors.map((e): RunStep => ({ id: e.id, version: e.version, status: "pending", calls: 0, finishedAt: null, error: null })),
      benchmarkOptIn: false,
      createdBy: options.createdBy ?? "cli",
      startedAt: now.toISOString(),
      finishedAt: null,
    });
    writeJson(join(dir, "calibration.json"), calibration);
  }

  // A resumed run keeps its original date and window, so all its steps agree.
  const asOf = new Date(run.asOf);
  const extracts = new Map<string, unknown>();
  const partial = new FilePartialStore(join(dir, "checkpoints"));
  const ctx: ExtractContext = {
    client,
    calibration,
    now: asOf,
    windowStart: new Date(`${run.analysisWindow.from}T00:00:00Z`),
    partial,
    pageSize: options.pageSize ?? SF_DEFAULTS.pageSize,
    get: <T>(extractor: Extractor<T>): T => {
      if (!extracts.has(extractor.id)) throw new Error(`Extractor "${extractor.id}" must run first`);
      return extracts.get(extractor.id) as T;
    },
  };

  let callsCounted = 0;
  const saveRun = async (): Promise<void> => {
    run.budget = { ...budget.snapshot(), callsMade: run.budget.callsMade + budget.callsMade - callsCounted, stoppedReason: run.budget.stoppedReason };
    callsCounted = budget.callsMade;
    writeJson(runPath, Run.parse(run));
    await options.onProgress?.(structuredClone(run));
  };
  run.status = "extracting";
  run.budget.stoppedReason = null;

  for (const extractor of extractors) {
    let step = run.steps.find((s) => s.id === extractor.id);
    if (!step) {
      step = { id: extractor.id, version: extractor.version, status: "pending", calls: 0, finishedAt: null, error: null };
      run.steps.push(step);
    }
    const path = checkpointPath(dir, extractor.id);
    if (step.status === "done" && step.version === extractor.version && existsSync(path)) {
      extracts.set(extractor.id, readJson(path));
      log(`  ${extractor.id}: reused from checkpoint`);
      continue;
    }
    const before = budget.callsMade;
    try {
      const output = await extractor.run(ctx);
      extracts.set(extractor.id, output);
      writeJson(path, output);
      for (const key of extractor.partialKeys ?? []) partial.clear(key);
      step.status = "done";
      step.version = extractor.version;
      step.calls += budget.callsMade - before;
      step.finishedAt = new Date().toISOString();
      step.error = null;
      log(`  ${extractor.id}: done (${budget.callsMade - before} calls)`);
      await saveRun();
    } catch (err) {
      step.calls += budget.callsMade - before;
      if (err instanceof BudgetStopError) {
        run.status = "stopped_budget";
        run.budget.stoppedReason = err.message;
        await saveRun();
        log(`Stopped at the API budget: ${err.message}`);
        return { run, dir, output: null };
      }
      step.status = "failed";
      step.error = err instanceof Error ? err.message : String(err);
      run.status = "failed";
      await saveRun();
      throw err;
    }
  }

  const org = extracts.get(orgExtractor.id) as { status: string; data?: Array<{ IsSandbox: boolean; Name?: string }> };
  run.isSandbox = org.data?.[0]?.IsSandbox ?? false;
  // The company information in Setup is the name the customer goes by, unless one was given.
  run.customerRef ??= org.data?.[0]?.Name?.trim() || null;
  run.status = "analysing";
  await saveRun();
  const output = analyseRun(dir, run, calibration, modules, extracts, options.answers ?? []);
  run.status = output.manualSteps.some((s) => s.status === "pending") ? "awaiting_manual" : "in_review";
  run.finishedAt = new Date().toISOString();
  await saveRun();
  // Re-render so the notes show the final status and call count.
  renderOutputs(dir, run, calibration, output);
  return { run, dir, output };
}

function analyseRun(dir: string, run: Run, calibration: Calibration, modules: DomainModule[], extracts: Map<string, unknown>, answers: ManualAnswer[]): AnalysisOutput {
  const pseudonyms = loadPseudonyms(dir);
  const users = extracts.get(usersExtractor.id) as { status: string; data?: UserRow[] } | undefined;
  const directory = new Map((users?.data ?? []).map((u) => [u.Id, { name: u.Name, aliases: [u.Username, u.Email] }]));
  // Registering everyone up front, in id order, keeps tokens stable and scrubs
  // a name even if no finding set out to mention that user.
  for (const [id, entry] of [...directory].sort(([a], [b]) => a.localeCompare(b))) pseudonyms.label("user", id, entry.name, entry.aliases);
  if (run.customerRef) pseudonyms.label("customer", run.orgId, run.customerRef);
  // A first name two people share gets its own token, saved with the map so a
  // report rendered later can restore it.
  registerSharedNames(pseudonyms);

  const output = analyse({
    modules,
    extracts,
    calibration,
    now: new Date(run.asOf),
    window: run.analysisWindow,
    answers,
    pseudonyms,
    userDirectory: directory,
    scope: run.scope,
  });
  writeJson(join(dir, "pseudonym-map.json"), pseudonyms.entries());
  renderOutputs(dir, run, calibration, output);
  return output;
}

function renderOutputs(dir: string, run: Run, calibration: Calibration, output: AnalysisOutput): void {
  writeJson(join(dir, "findings.json"), output.findings);
  writeJson(join(dir, "metrics.json"), output.metrics);
  writeJson(join(dir, "manual-steps.json"), output.manualSteps);
  const scores = scoreDomains(run.modules, output.findings, run.ruleSetVersion);
  writeJson(join(dir, "scores.json"), scores);
  const notes = renderRawNotes({ run, calibration, ...output, scores, pseudonyms: loadPseudonyms(dir) });
  writeFileSync(join(dir, "raw-notes.md"), notes, { mode: 0o600 });
}
