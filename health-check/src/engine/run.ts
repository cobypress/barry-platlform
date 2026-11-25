import { appendFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash, randomUUID } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { BUDGET_DEFAULTS, RULE_SET_VERSION, SF_DEFAULTS } from "../config.ts";
import { fileLines, readCheckpoint, trimToLastLine, writeCheckpoint } from "./checkpoint.ts";
import { shareRepeatedValues } from "./share-values.ts";
import { modulesFor } from "../domains/scope.ts";
import { ALL_MODULES } from "../domains/index.ts";
import { RULES_FILE, rulesOf } from "./rules-run.ts";
import { scoreDomains } from "../domains/scoring.ts";
import type { DomainModule } from "../domains/types.ts";
import { customObjectsExtractor, orgExtractor, schemaExtractor, setupAuditExtractor, type ObjectSummary, type CustomObjectRow, type SetupAuditSummary } from "../extract/configuration.ts";
import { FACTS_FILE, factsExtractor, type Facts } from "../extract/facts.ts";
import { apexExtractor, metadataExtractor, type ApexData } from "../extract/automation.ts";
import { apexSourceStore } from "../extract/apex-source.ts";
import { codeLintExtractor, type CodeLintData, type CodeLintTools } from "../extract/code-lint.ts";
import { notRun } from "../extract/lint/findings.ts";
import { LINT_TOTALS_FILE, lintTotals } from "../extract/lint/totals.ts";
import { componentChangesExtractor, type ComponentChangesData } from "../extract/component-changes.ts";
import type { MetadataBundle } from "../extract/metadata-parse.ts";
import { fieldDescribeExtractor } from "../extract/fields.ts";
import { completeProfile, gateFor, orgProfileExtractor } from "../extract/org-profile.ts";
import { buildInventory, errorClass, INVENTORY_FILE, inventoryExtractorsFor } from "../extract/inventory.ts";
import { buildComplexity, COMPLEXITY_FILE } from "../complexity/read.ts";
import { summariseFacts } from "../facts/summary.ts";
import { LIMITS_AT_START_FILE, monthsBefore, noteRefusals, parseLimitsReading, type ConnectedUser, type LimitsReading, type ExtractContext, type Extractor, type PartialStore, type Sourced } from "../extract/types.ts";
import { buildHandbook, HANDBOOK_FILE, labelApexModifiers, labelComponentModifiers, recentChanges } from "../handbook/build.ts";
import type { UserRow } from "../extract/users.ts";
import { usersExtractor } from "../extract/users.ts";
import { renderRawNotes } from "../notes/raw-notes.ts";
import { ACTION_LISTS_FILE, buildActionLists } from "../actions/lists.ts";
import { Pseudonymiser, UNKNOWN_USER, type PseudonymEntry } from "../privacy/pseudonymiser.ts";
import { registerSharedNames } from "../privacy/names.ts";
import { Run, type Calibration, type ManualAnswer, type OrgProfile, type RefusedRead, type RunScope, type RunStep, type SkippedRead } from "../schemas/index.ts";
import { ApiBudget, BudgetStopError } from "../salesforce/budget.ts";
import { classifyError, lastDelivery, readGate, runStoppedWords, SalesforceClient, SalesforceError } from "../salesforce/client.ts";
import type { ConnectionProvider } from "../salesforce/connection.ts";
import type { Transport } from "../salesforce/transport.ts";
import { allFindings, allMetrics, analyse, type AnalysisOutput } from "./evaluate.ts";
import { stagesExtractor, type StageRow } from "../extract/opportunities.ts";
import { inferredMeanings, STAGE_MEANINGS_FILE } from "../domains/historic-pipeline/cohort.ts";

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
  /**
   * The worker's last delivery of this run (DECISIONS.md 702): a transient
   * fault of one read then costs that read rather than the run.
   */
  lastDelivery?: boolean;
  /** "scan" for the free set-up teaser. */
  product?: "barometer" | "scan" | "pulse";
  /** How the run is reviewed, written to run.json for its documents to read. Consultant-led unless said (DECISIONS.md 692). */
  reviewMode?: "consultant" | "automatic";
  /** The run this one re-checks: its first health check, whose findings the report compares against. */
  baselineRunId?: string | null;
  /**
   * The worker's HC_HANDBOOK_SUMMARIES switch: the Apex step keeps the redacted
   * code Claude may summarise in apex-source.json (DECISIONS.md 491). Off unless
   * the worker says so, so the command line never keeps code.
   */
  keepApexSource?: boolean;
  /**
   * Who connected (DECISIONS.md 498), for record-type use, which counts only
   * what that user sees whole. The caller gives it on a resume too, from the
   * same place: it's never kept in the run folder.
   */
  connectedUser?: ConnectedUser;
  /**
   * Reads the org's inventory too, and writes inventory.json (DECISIONS.md 605).
   * The worker turns it on; off, a run reads and writes exactly what it did before.
   */
  inventory?: boolean;
  /**
   * Runs PMD and ESLint over the org's code too, beside our own scan, and only
   * stores what they find (DECISIONS.md 670). The worker gives it on a health
   * check, with PMD where it found it; off, a run reads and writes exactly what it did before.
   */
  codeLint?: CodeLintTools;
  /**
   * Every module the engine knows, for the run folder's list of the rules it
   * knows (DECISIONS.md 718). The engine's own unless a test says otherwise.
   */
  knownModules?: DomainModule[];
  /**
   * Told after each step this delivery read (never one reused from its
   * checkpoint): how long its reads took and how many calls they made, for
   * the console's run times (DECISIONS.md 722). Nothing else reads it, and it
   * changes nothing in the run folder.
   */
  onStepTimed?: (timing: StepTiming) => void;
};

/** One step's reads on one delivery: our step id, milliseconds on the worker's clock, calls, and how it ended. */
export type StepTiming = { id: string; ms: number; calls: number; outcome: "done" | "failed" | "stopped_budget" };

/** What the lint findings say when the step itself failed: our words, never the error's. */
export const LINT_STEP_FAILED = "PMD's step failed, so nothing it found was kept.";

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

export class FilePartialStore implements PartialStore {
  readonly #dir: string;

  constructor(dir: string) {
    this.#dir = dir;
  }

  #path(key: string): string {
    return join(this.#dir, `${key}.partial.jsonl`);
  }

  read<R>(key: string): R[] {
    if (!existsSync(this.#path(key))) return [];
    // Half a line from a crash mid-append is dropped, so the read resumes from the last whole page row.
    trimToLastLine(this.#path(key));
    // A line at a time: a big org's partial read can be hundreds of megabytes.
    const rows: R[] = [];
    for (const line of fileLines(this.#path(key))) if (line) rows.push(JSON.parse(line) as R);
    return rows;
  }

  append<R>(key: string, rows: R[]): void {
    if (rows.length > 0) appendFileSync(this.#path(key), `${rows.map((r) => JSON.stringify(r)).join("\n")}\n`, { mode: 0o600 });
  }

  clear(key: string): void {
    rmSync(this.#path(key), { force: true });
  }
}

/** `extra` are steps no module owns, such as the inventory's: after the modules', before the facts. */
export function collectExtractors(modules: DomainModule[], extra: Extractor[] = []): Extractor[] {
  // Field describes go first, because other reads plan their SELECT from them.
  const seen = new Map<string, Extractor>([
    [orgExtractor.id, orgExtractor],
    [fieldDescribeExtractor.id, fieldDescribeExtractor],
    // What the org has, before any read it rules out (DECISIONS.md 710).
    [orgProfileExtractor.id, orgProfileExtractor],
  ]);
  for (const module of modules) for (const extractor of module.extractors) if (!seen.has(extractor.id)) seen.set(extractor.id, extractor);
  for (const extractor of extra) if (!seen.has(extractor.id)) seen.set(extractor.id, extractor);
  // The facts step goes last on every run, so it can see which settings the modules retrieved.
  seen.delete(factsExtractor.id);
  return [...seen.values(), factsExtractor];
}

/**
 * A confirmed calibration hashes without its source, as every one did before
 * decision 688 added it, so a run started before then still resumes. One read
 * from the org's settings keeps it, and so never hashes like a confirmed one.
 */
const calibrationHash = (calibration: Calibration): string => {
  const { source, ...rest } = calibration;
  return createHash("sha256").update(JSON.stringify(source === "confirmed" ? rest : calibration)).digest("hex").slice(0, 16);
};

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

  // An automatic run spends at most a tenth of the org's daily limit as well (DECISIONS.md 710).
  const budget = new ApiBudget({ stopAtShare: options.stopAtShare ?? BUDGET_DEFAULTS.stopAtShare, ...(options.reviewMode === "automatic" ? { maxShareOfLimit: BUDGET_DEFAULTS.automaticShareOfLimit } : {}) });
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
  const inventorySteps = options.inventory ? inventoryExtractorsFor(scope) : [];
  const lintSteps: Extractor[] = options.codeLint ? [codeLintExtractor] : [];
  const extractors = collectExtractors(modules, [...inventorySteps, ...lintSteps]);
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
      reviewMode: options.reviewMode ?? "consultant",
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
    // /limits as this first sitting's connection read it, before any call of the run's
    // own: a resumed run's limit steps take this, not its new connection's (DECISIONS.md 726).
    writeJson(join(dir, LIMITS_AT_START_FILE), { all: client.allLimitsAtConnect, reported: client.limitsAtConnect, unread: client.limitsUnread });
  }
  const limitsPath = join(dir, LIMITS_AT_START_FILE);
  // A run begun before the reading was kept, or a damaged file, reads the connection's, as before.
  let limitsAtStart: LimitsReading | null = null;
  try {
    limitsAtStart = existsSync(limitsPath) ? parseLimitsReading(readJson(limitsPath)) : null;
  } catch {
    limitsAtStart = null;
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
    ...(options.keepApexSource === true ? { apexSource: apexSourceStore(dir) } : {}),
    ...(options.connectedUser ? { connectedUser: options.connectedUser } : {}),
    ...(options.codeLint ? { codeLint: options.codeLint } : {}),
    ...(limitsAtStart ? { limitsAtStart } : {}),
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
  // The profile run.json keeps, once its step has run or been reused, with every read it ruled out (DECISIONS.md 710).
  const keepProfile = (profile: OrgProfile): void => {
    run.orgProfile = { ...structuredClone(profile), skipped: run.orgProfile?.skipped ?? [] };
  };

  for (const extractor of extractors) {
    let step = run.steps.find((s) => s.id === extractor.id);
    if (!step) {
      step = { id: extractor.id, version: extractor.version, status: "pending", calls: 0, finishedAt: null, error: null };
      run.steps.push(step);
    }
    const path = checkpointPath(dir, extractor.id);
    if (step.status === "done" && step.version === extractor.version && existsSync(path)) {
      extracts.set(extractor.id, shareRepeatedValues(readCheckpoint(path)));
      if (extractor.id === orgProfileExtractor.id) keepProfile(extracts.get(extractor.id) as OrgProfile);
      log(`  ${extractor.id}: reused from checkpoint`);
      continue;
    }
    // A step left part-done by an earlier version of its extractor starts afresh:
    // its partial rows are that version's, such as fill rates' 0 for a count never read (DECISIONS.md 668).
    if (step.version !== extractor.version) {
      for (const key of extractor.partialKeys ?? []) partial.clear(key);
      step.version = extractor.version;
    }
    const before = budget.callsMade;
    const began = performance.now();
    // Once per step and delivery: a save that throws after the step is timed done mustn't time it again as failed.
    let timedOnce = false;
    const timed = (outcome: StepTiming["outcome"]): void => {
      if (timedOnce) return;
      timedOnce = true;
      try {
        options.onStepTimed?.({ id: extractor.id, ms: Math.max(0, Math.round(performance.now() - began)), calls: budget.callsMade - before, outcome });
      } catch {
        // A courtesy for the console's run times: it never stops a read.
      }
    };
    const refused: RefusedRead[] = [];
    const skipped: SkippedRead[] = [];
    // Once the profile is known, a read it rules out isn't sent (DECISIONS.md 710).
    const profile = extracts.get(orgProfileExtractor.id) as OrgProfile | undefined;
    const gate = profile ? gateFor(profile, extractor.id, (s) => skipped.push(s)) : null;
    try {
      // Held for the whole analysis, so each repeated value is held once (DECISIONS.md 346).
      // A read Salesforce refused costs its checks, never the run; its code is kept for us (DECISIONS.md 702).
      // On the worker's last delivery, a transient fault of one read costs that read (DECISIONS.md 702).
      const delivered = (): Promise<unknown> => (options.lastDelivery === true ? lastDelivery.run(true, () => extractor.run(ctx)) : extractor.run(ctx));
      const read = (): Promise<unknown> => (gate ? readGate.run(gate, delivered) : delivered());
      const output = shareRepeatedValues(await noteRefusals((r) => refused.push({ step: extractor.id, ...r }), read));
      keepRefused(run, refused, log);
      if (extractor.id === orgProfileExtractor.id) keepProfile(output as OrgProfile);
      keepSkipped(run, skipped, log);
      extracts.set(extractor.id, output);
      writeCheckpoint(path, output);
      for (const key of extractor.partialKeys ?? []) partial.clear(key);
      step.status = "done";
      step.version = extractor.version;
      step.calls += budget.callsMade - before;
      step.finishedAt = new Date().toISOString();
      step.error = null;
      log(`  ${extractor.id}: done (${budget.callsMade - before} calls)`);
      timed("done");
      await saveRun();
    } catch (err) {
      step.calls += budget.callsMade - before;
      timed(err instanceof BudgetStopError ? "stopped_budget" : "failed");
      if (err instanceof BudgetStopError) {
        run.status = "stopped_budget";
        run.budget.stoppedReason = err.message;
        await saveRun();
        log(`Stopped at the API budget: ${err.message}`);
        return { run, dir, output: null };
      }
      step.status = "failed";
      // Salesforce's code and message go to the log alone; run.json, which the console shows, gets plain words (DECISIONS.md 702).
      const words = runStoppedWords(err);
      if (words && err instanceof SalesforceError) log(`  ${extractor.id}: stopped on ${err.code.replace(/[^A-Za-z0-9_]/g, "").slice(0, 60)} (HTTP ${err.status})`);
      step.error = words ?? (err instanceof Error ? err.message : String(err));
      // No finding reads the inventory, so its step failing costs only its own
      // section, and the run goes on; a later delivery tries it again (DECISIONS.md 609).
      // The same for PMD and ESLint's step: a run never fails because a linter did (DECISIONS.md 670).
      // Salesforce not answering before the worker's last delivery isn't the step failing: the run stops, and the
      // queue's next delivery reads the step again, rather than lose its whole section over a blip (DECISIONS.md 702).
      const retried = options.lastDelivery !== true && classifyError(err) === "instance_unavailable";
      if ((inventorySteps.includes(extractor) || lintSteps.includes(extractor)) && !retried) {
        // The lint findings then say PMD didn't run, rather than show nothing as on a scan (DECISIONS.md 673).
        if (lintSteps.includes(extractor)) extracts.set(extractor.id, { pmd: notRun("pmd", LINT_STEP_FAILED), eslint: notRun("eslint", LINT_STEP_FAILED) } satisfies CodeLintData);
        await saveRun();
        log(`  ${extractor.id}: failed, and the run carries on without it`);
        continue;
      }
      run.status = "failed";
      await saveRun();
      throw err;
    }
  }

  // Names, booleans and types only, so it's kept as long as the findings (DECISIONS.md 288).
  writeJson(join(dir, FACTS_FILE), extracts.get(factsExtractor.id));
  // Names and counts only, so it's kept as long as the findings (DECISIONS.md 605).
  // Dated as the run reads the org, as the facts are, so every delivery writes the same file.
  // A fault here costs only the file, never the run: the worker then saves a
  // row whose every section says it wasn't written (DECISIONS.md 618).
  if (options.inventory) {
    try {
      writeJson(join(dir, INVENTORY_FILE), buildInventory(extracts, scope, asOf, new Map(run.steps.flatMap((s) => (s.status === "failed" && s.error ? [[s.id, s.error]] : []))), log));
    } catch (err) {
      rmSync(join(dir, INVENTORY_FILE), { force: true, recursive: true });
      // The error's class alone: its message could name a path or quote what the org gave.
      log(`  The inventory couldn't be written (${errorClass(err)}), so the run has none.`);
    }
  }
  // Every PMD rule's counts for the benchmark, with no name in them, kept as long
  // as the findings (DECISIONS.md 673). Only on a run that had the step; a fault
  // here costs only the file.
  if (lintSteps.length > 0) {
    const path = join(dir, LINT_TOTALS_FILE);
    try {
      writeJson(path, lintTotals(extracts.get(codeLintExtractor.id) as CodeLintData));
    } catch (err) {
      rmSync(path, { force: true });
      log(`  PMD's totals couldn't be kept (${errorClass(err)}), so the run has none.`);
    }
  }
  // How big and tangled the build is, beside the health score and never part
  // of it (DECISIONS.md 614). Counts of set-up only, so on every run. Nothing
  // reads it back, so a fault here costs only the file, never the run.
  try {
    writeJson(join(dir, COMPLEXITY_FILE), buildComplexity(extracts, asOf));
  } catch (err) {
    rmSync(join(dir, COMPLEXITY_FILE), { force: true });
    log(`  The complexity score couldn't be worked out, so the run has none: ${err instanceof Error ? err.message : String(err)}`);
  }
  // Which objects' rules weren't read, for the raw notes. Render reads only the run folder (DECISIONS.md 425).
  const bundle = extracts.get(metadataExtractor.id) as Sourced<MetadataBundle> | undefined;
  run.retrieveFallback = bundle?.status === "ok" ? bundle.data.retrieveFallback : undefined;
  const org = extracts.get(orgExtractor.id) as { status: string; data?: Array<{ IsSandbox: boolean; Name?: string }> };
  run.isSandbox = org.data?.[0]?.IsSandbox ?? false;
  if (run.orgProfile) completeProfile(run.orgProfile, extracts.get(schemaExtractor.id) as Record<string, Sourced<ObjectSummary>> | undefined);
  // The company information in Setup is the name the customer goes by, unless one was given.
  run.customerRef ??= org.data?.[0]?.Name?.trim() || null;
  run.status = "analysing";
  await saveRun();
  const output = analyseRun(dir, run, calibration, modules, extracts, options.answers ?? [], options.knownModules ?? ALL_MODULES);
  // Each issue's records by name, for the action lists. Beside the pseudonym map, not in checkpoints/.
  writeJson(join(dir, ACTION_LISTS_FILE), buildActionLists(output.records, extracts, client.instanceUrl));
  run.status = output.manualSteps.some((s) => s.status === "pending") ? "awaiting_manual" : "in_review";
  run.finishedAt = new Date().toISOString();
  await saveRun();
  // Re-render so the notes show the final status and call count.
  renderOutputs(dir, run, calibration, output);
  return { run, dir, output };
}

/** The most refused reads run.json keeps: enough to see a pattern, never a file that grows without end. */
export const MAX_REFUSED = 200;

/**
 * Each read a step had refused, once, into run.json and the log (DECISIONS.md
 * 702): what was read, and Salesforce's code and status. For us: the customer
 * reads only the plain reason on the checks it cost.
 */
export function keepRefused(run: Run, refused: RefusedRead[], log: (message: string) => void): void {
  for (const read of refused) {
    const kept = (run.readsRefused ??= []);
    if (kept.some((k) => k.step === read.step && k.read === read.read && k.code === read.code)) continue;
    // Past the cap, one lost read is still kept: it is what keeps an automatic run from release (DECISIONS.md 702).
    if (kept.length >= MAX_REFUSED && !(read.lost && !kept.some((k) => k.lost))) continue;
    kept.push(read);
    log(`  ${read.step}: ${read.lost ? "Salesforce didn't answer" : "Salesforce refused"} a read of ${read.read} (${read.code}, HTTP ${read.status}), so the checks it feeds are not measurable`);
  }
}

/** Each read a step didn't make because the profile ruled it out, once, into run.json and the log (DECISIONS.md 710). */
export function keepSkipped(run: Run, skipped: SkippedRead[], log: (message: string) => void): void {
  if (!run.orgProfile) return;
  for (const read of skipped) {
    if (run.orgProfile.skipped.some((k) => k.step === read.step && k.read === read.read)) continue;
    run.orgProfile.skipped.push(read);
    log(`  ${read.step}: didn't read ${read.read}, as the org's edition doesn't include it (${read.capability})`);
  }
}

function analyseRun(dir: string, run: Run, calibration: Calibration, modules: DomainModule[], extracts: Map<string, unknown>, answers: ManualAnswer[], known: DomainModule[]): AnalysisOutput {
  const pseudonyms = loadPseudonyms(dir);
  const users = extracts.get(usersExtractor.id) as { status: string; data?: UserRow[] } | undefined;
  const directory = new Map((users?.data ?? []).map((u) => [u.Id, { name: u.Name, aliases: [u.Username, u.Email] }]));
  // Registering everyone up front, in id order, keeps tokens stable and scrubs
  // a name even if no finding set out to mention that user.
  for (const [id, entry] of [...directory].sort(([a], [b]) => a.localeCompare(b))) pseudonyms.label("user", id, entry.name, entry.aliases);
  // Then whoever made a setup change the user list doesn't hold, named as the
  // trail names them (DECISIONS.md 461): before the analysis, so a rule that
  // names the same person gives the same token, and before the map is saved,
  // so every token in handbook.json can be restored.
  const changes = recentChanges(extracts.get(setupAuditExtractor.id) as Sourced<SetupAuditSummary> | undefined, (id, trailName) => pseudonyms.label("user", id, directory.get(id)?.name ?? trailName ?? UNKNOWN_USER));
  // Then whoever last changed a class or trigger and is in neither, named as
  // Salesforce named them beside it (DECISIONS.md 473). After those, so no
  // token above moves, and before the shared names, so theirs are counted.
  const apex = extracts.get(apexExtractor.id) as ApexData | undefined;
  const apexModifiers = labelApexModifiers(apex, (id, name) => pseudonyms.label("user", id, directory.get(id)?.name ?? name ?? UNKNOWN_USER));
  // Then whoever last changed a flow or validation rule and is in none of
  // those, the same way (DECISIONS.md 482): after Apex's, so no token above
  // moves, and before the shared names too.
  const components = extracts.get(componentChangesExtractor.id) as ComponentChangesData | undefined;
  const componentModifiers = labelComponentModifiers(components, (id, name) => pseudonyms.label("user", id, directory.get(id)?.name ?? name ?? UNKNOWN_USER));
  const modifiers = new Map([...apexModifiers, ...componentModifiers]);
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
    // An automatic run never waits for a person: what nobody answered is skipped, and the rules that needed it say not measurable (DECISIONS.md 693).
    unanswered: run.reviewMode === "automatic" ? "skip" : "ask",
  });
  writeJson(join(dir, "pseudonym-map.json"), pseudonyms.entries());
  // Which rules this analysis ran, and which the engine knows, for the digest (DECISIONS.md 718).
  // Written with every analysis, so it always goes with the findings it ran beside.
  writeJson(join(dir, RULES_FILE), rulesOf(output.rulesRun, known));
  // The customer's own configuration, for their handbook: kept as long as the
  // findings, and read by nothing else here (DECISIONS.md 451 to 457).
  writeJson(
    join(dir, HANDBOOK_FILE),
    buildHandbook(extracts.get(metadataExtractor.id) as Sourced<MetadataBundle> | undefined, apex, extracts.get(customObjectsExtractor.id) as Sourced<CustomObjectRow[]> | undefined, changes, modifiers, components),
  );
  // The documents read only the run folder, and the raw extracts are purged, so
  // what a stage the calibration doesn't name was taken to mean is kept beside
  // the findings (DECISIONS.md 600).
  const flags = extracts.get(stagesExtractor.id) as Sourced<StageRow[]> | undefined;
  const meanings = inferredMeanings(calibration, flags?.status === "ok" ? flags.data : null);
  const meaningsPath = join(dir, STAGE_MEANINGS_FILE);
  if (Object.keys(meanings).length > 0) writeJson(meaningsPath, meanings);
  else rmSync(meaningsPath, { force: true });
  renderOutputs(dir, run, calibration, output);
  return output;
}

function renderOutputs(dir: string, run: Run, calibration: Calibration, output: AnalysisOutput): void {
  // The path insights too, last (DECISIONS.md 703): the run keeps every finding; the documents choose theirs.
  writeJson(join(dir, "findings.json"), allFindings(output));
  writeJson(join(dir, "metrics.json"), allMetrics(output));
  writeJson(join(dir, "manual-steps.json"), output.manualSteps);
  const scores = scoreDomains(run.modules, output.findings, run.ruleSetVersion);
  writeJson(join(dir, "scores.json"), scores);
  const factsPath = join(dir, FACTS_FILE);
  const facts = existsSync(factsPath) ? summariseFacts(readJson<Facts>(factsPath)) : null;
  const notes = renderRawNotes({ run, calibration, ...output, findings: allFindings(output), metrics: allMetrics(output), scores, facts, pseudonyms: loadPseudonyms(dir) });
  writeFileSync(join(dir, "raw-notes.md"), notes, { mode: 0o600 });
}
