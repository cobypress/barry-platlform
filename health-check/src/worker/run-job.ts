import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, knownModuleIds, modulesForRun } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { INTERVIEWS_FILE, loadRun, OUTPUT_FILES, SURVEY_FILE } from "../render/render-run.ts";
import { INTERVIEW_VERSION } from "../interviews/questions.ts";
import { OAuthConnectionProvider, revokeConnection } from "../salesforce/oauth-provider.ts";
import { OAuthError } from "../salesforce/oauth.ts";
import { buildScanResult } from "../scan/result.ts";
import { buildPulseResult } from "../pulse/result.ts";
import { deleteRunFolder } from "../retention/purge.ts";
import type { StoredConnection } from "../salesforce/token-store.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import { withJobLock } from "./lock.ts";
import { progressOf, RunClosedError, type RunJobInput } from "./types.ts";
import { withoutComments } from "../survey/questions.ts";
import { FACTS_FILE } from "../extract/facts.ts";
import { APEX_SOURCE_FILE } from "../extract/apex-source.ts";
import { HandbookSummaries, summariseApex, SUMMARIES_FILE, SUMMARIES_PARTIAL_FILE } from "../handbook/summaries.ts";
import type { SummariesToSave } from "../handbook/summary-row.ts";
import { FactsFile, factsReport, type FactsReport } from "../facts/summary.ts";
import { ASSUMPTIONS } from "../facts/assumptions.ts";
import { Timestamp } from "../schemas/common.ts";
import { z } from "zod";

// A run job: the engine's extract and analysis for one console run, through
// the customer's stored OAuth connection. The run folder is named after the
// console's run id, so a job delivered again (a crash, a retry, the console
// asking twice) resumes through the engine's own checkpoints: finished steps
// are reused, and the results replace what the store held rather than adding.

const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,99}$/;

/** States in which the run is the console's, not the worker's. */
const CLOSED = new Set(["in_review", "released"]);

export const SECTIONS_FILE = "report-sections.json";

/**
 * In the worker, a connection name is approved by the console's records, not by
 * HC_APPROVED_ORG_ALIASES: it exists only because a customer's admin authorised
 * it through a consultant's link, and the run names it. What the alias list
 * guards against, a typo reaching the wrong org, is checked instead by matching
 * the stored connection's org to the run's org before anything is read.
 */
export const approvedFromConsole = (name: string): NodeJS.ProcessEnv => ({ HC_APPROVED_ORG_ALIASES: name }) as NodeJS.ProcessEnv;

function problemWith(input: RunJobInput, deps: WorkerDeps): string | null {
  if (!input.calibration) return "The calibration hasn't been confirmed, so the run can't start.";
  if (input.run.connectionKind !== "oauth") return "This run is set to use a CLI login. The worker only reads through a connection a customer's admin authorised.";
  if (!input.connectionName) return "No connection is named for this run. Send the customer a connect link first.";
  const known = knownModuleIds(deps.modules ?? ALL_MODULES);
  const unknown = input.run.modules.filter((m) => !known.has(m));
  if (unknown.length) return `The worker doesn't know these modules: ${unknown.join(", ")}.`;
  return null;
}

/**
 * Claude drafts each section that has a brief, so the console gets prose a
 * consultant would write rather than the engine's fill-in text. The findings
 * matter more than the prose, so nothing here fails the run: if drafting breaks,
 * the sections are the engine's own text, as they are without a key. Why any
 * section kept the engine's text goes in the run folder, for a person to read.
 */
async function draftSections(runId: string, dir: string, input: ReportInput, client: NarrativeClient, pseudonyms: Pseudonymiser, now: Date, log: (line: string) => void): Promise<BuiltReport> {
  const write = (rejections: Rejection[]): void => writeFileSync(join(dir, OUTPUT_FILES.rejections), `${JSON.stringify(rejections, null, 2)}\n`, { mode: 0o600 });
  const report = buildReport(input);
  let rejections: Rejection[];
  try {
    rejections = await applyNarrative(report, client, pseudonyms, now);
  } catch (err) {
    // Part-way through, some sections may hold drafts and others not. Start again from the engine's text.
    const reason = safeReason(err);
    log(`Drafting stopped for ${runId}, so its sections are the engine's own text: ${reason}`);
    write([{ sectionId: runId, reasons: [`Drafting stopped, so every section is the engine's own text: ${reason}`], unavailable: true }]);
    return buildReport(input);
  }
  write(rejections);
  const drafted = Object.values(report.outputs).flatMap((o) => o.versions).filter((v) => v.source === "claude_draft").length;
  const rejected = rejections.filter((r) => !r.unavailable).length;
  log(`Claude drafted ${drafted} of ${report.briefs.size} sections for ${runId}.${rejected ? ` ${rejected} kept the engine's text and need a person (${OUTPUT_FILES.rejections}).` : ""}`);
  const outage = rejections.find((r) => r.unavailable);
  if (outage) log(`Claude couldn't be reached while drafting ${runId}, so the sections not yet drafted are the engine's own text: ${outage.reasons.join(" ")}`);
  return report;
}

export async function processRunJob(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  if (!RUN_ID.test(runId)) return { outcome: "failed", detail: "The run id isn't one the worker can use as a folder name" };
  return withJobLock(deps.locksDir, `run-${runId}`, () => runLocked(runId, deps, attempt));
}

async function runLocked(runId: string, deps: WorkerDeps, attempt: Attempt): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const now = () => (deps.now ?? (() => new Date()))();
  const input = await deps.store.loadRunJob(runId);
  if (!input) return { outcome: "skipped", detail: `No run ${runId}` };
  if (CLOSED.has(input.run.status)) return { outcome: "skipped", detail: `Run ${runId} is ${input.run.status.replace("_", " ")}, so there's nothing for the worker to do` };

  const fail = async (reason: string): Promise<JobOutcome> => {
    // Failed for good: nothing will resume to use the Apex code kept for Claude's summaries (DECISIONS.md 491).
    forgetApexSource(join(deps.runsDir, runId), runId, log);
    await deps.store.recordRunFailure(runId, reason, now().toISOString());
    return { outcome: "failed", detail: reason };
  };

  const problem = problemWith(input, deps);
  if (problem) return fail(problem);
  const name = input.connectionName!;

  let stored: StoredConnection;
  try {
    stored = await deps.tokens.load(name);
  } catch (err) {
    return fail(`The connection "${name}" couldn't be loaded: ${safeReason(err)}`);
  }
  if (stored.orgId.slice(0, 15) !== input.run.orgId.slice(0, 15)) {
    return fail(`The connection "${name}" is for a different org from this run, so nothing was read.`);
  }

  const dir = join(deps.runsDir, runId);
  const resume = existsSync(join(dir, "run.json")) ? runId : undefined;
  const modules = modulesForRun(input.run.modules, deps.modules ?? ALL_MODULES);
  const provider = new OAuthConnectionProvider(name, { store: deps.tokens, client: deps.oauthClient, transport: deps.oauthTransport, env: approvedFromConsole(name) });
  const secrets = [stored.accessToken, stored.refreshToken];
  // A health check or essentials run only, with Claude to ask: a scan and a
  // pulse never keep code, and nor does a worker that can't send it anywhere (DECISIONS.md 492).
  const apexSummaries = deps.apexSummaries === true && deps.narrative !== undefined && input.run.product === "barometer";

  let result: Awaited<ReturnType<typeof runHealthCheck>>;
  try {
    result = await runHealthCheck({
      provider,
      calibration: input.calibration!,
      modules,
      outDir: deps.runsDir,
      now: now(),
      resume,
      runId,
      answers: input.answers,
      createdBy: input.run.createdBy,
      customerRef: input.run.customerRef,
      scope: input.run.scope,
      product: input.run.product,
      baselineRunId: input.run.baselineRunId,
      keepApexSource: apexSummaries,
      stopAtShare: deps.stopAtShare,
      transport: deps.dataTransport,
      apiVersion: deps.apiVersion,
      log,
      // Progress is a courtesy: a database blip mustn't stop an extract that's
      // spending the customer's API calls. The final write is the one that counts.
      onProgress: async (run) => {
        // A finished status goes in only with the results, in one write, so the
        // console never shows a run in review with nothing to review.
        const progress = progressOf(run);
        if (progress.status === "awaiting_manual" || progress.status === "in_review") progress.status = "analysing";
        // The engine only learns whether the org is a sandbox at the end, so until then the console's word stands.
        progress.isSandbox = input.run.isSandbox;
        try {
          await deps.store.saveRunProgress(runId, progress, dir);
        } catch (err) {
          log(`Couldn't record progress for ${runId}: ${safeReason(err, secrets)}`);
        }
      },
    });
  } catch (err) {
    const reason = safeReason(err, secrets);
    // The engine resumes from its checkpoints, so another delivery is cheap and safe.
    if (!attempt.final) throw new Error(reason);
    return fail(`The run stopped with an error: ${reason}`);
  }

  if (result.run.status === "stopped_budget") {
    return { outcome: "stopped_budget", detail: result.run.budget.stoppedReason ?? "Stopped at the API budget" };
  }

  try {
    // The engine's intake and the sections the console reviews. Building sections
    // needs no browser. They start as the engine's own text, as `hc render`
    // writes it, and with a key Claude then drafts the ones that have a brief.
    if (input.intake) writeFileSync(join(dir, "intake.json"), `${JSON.stringify(input.intake, null, 2)}\n`, { mode: 0o600 });
    // The survey as it stands now is the one the sections are built and reviewed
    // from, so the documents print what was approved. Comments never reach the folder.
    if (input.survey) writeFileSync(join(dir, SURVEY_FILE), `${JSON.stringify(withoutComments(input.survey), null, 2)}\n`, { mode: 0o600 });
    // The interviews as they stand now, likewise. One deleted since an earlier delivery goes from the folder too.
    if (input.interviews?.length) writeFileSync(join(dir, INTERVIEWS_FILE), `${JSON.stringify({ version: INTERVIEW_VERSION, interviews: input.interviews }, null, 2)}\n`, { mode: 0o600 });
    else rmSync(join(dir, INTERVIEWS_FILE), { force: true });
    const { input: reportInput, pseudonyms } = loadRun(dir);
    // Before the sections, because these are kept as they're made and the
    // sections aren't: a job stopped part way through picks the summaries up
    // where they were and drafts the sections once, where the other order would
    // pay for every section again on each delivery. Each catches its own
    // failures, so neither costs the other. With no code kept, nothing is asked.
    await summariseApex({ dir, runId, enabled: apexSummaries, client: deps.narrative, pseudonyms, now: now(), log });
    // A scan and a pulse are never reviewed, so Claude has nothing to draft for either.
    const unreviewed = input.run.product === "scan" || input.run.product === "pulse";
    const report = deps.narrative && !unreviewed ? await draftSections(runId, dir, reportInput, deps.narrative, pseudonyms, now(), log) : buildReport(reportInput);
    const outputs = Object.values(report.outputs);
    writeFileSync(join(dir, SECTIONS_FILE), `${JSON.stringify(outputs, null, 2)}\n`, { mode: 0o600 });
    const facts = factsOf(dir, log);
    // Only with the switch on, as it stands now: off, the console's table is never named.
    const handbookSummaries = apexSummaries ? summariesOf(dir, runId, log) : null;
    await deps.store.saveRunOutput(runId, {
      progress: progressOf(result.run),
      runDir: dir,
      findings: result.output!.findings,
      metrics: result.output!.metrics,
      scores: reportInput.scores ?? [],
      manualSteps: result.output!.manualSteps,
      // The engine leaves its sections as drafts, and the console approves only
      // sections in review. It has no step that starts review, so a run that's
      // in review hands over its drafts in review, as the console's own import does.
      // A section Claude's draft couldn't settle stays needs_input.
      sections: outputs.flatMap((o) => o.sections).map((s) => (result.run.status === "in_review" && s.status === "draft" ? { ...s, status: "in_review" as const } : s)),
      versions: outputs.flatMap((o) => o.versions),
      facts: facts.summary,
      factsFile: facts.file,
      ...(handbookSummaries ? { handbookSummaries } : {}),
    });
  } catch (err) {
    if (err instanceof RunClosedError) return { outcome: "skipped", detail: err.message };
    // Another delivery reuses every checkpoint, so it costs no data reads. On
    // the last one, the run mustn't be left at "analysing" with no reason given.
    const reason = safeReason(err, secrets);
    if (!attempt.final) throw new Error(reason);
    return fail(`The run finished, but its results couldn't be saved: ${reason}`);
  }
  if (input.run.product === "scan") await finishScan(runId, dir, name, deps, now().toISOString(), log);
  if (input.run.product === "pulse") await finishPulse(runId, dir, deps, now().toISOString(), log);
  return { outcome: "done", detail: `Run ${runId} is ${result.run.status.replace("_", " ")}` };
}

/** Deletes the Apex code kept for Claude's summaries, and any summaries made from it. Logs a sentence, never a path. */
function forgetApexSource(dir: string, runId: string, log: (line: string) => void): void {
  try {
    for (const name of [APEX_SOURCE_FILE, SUMMARIES_PARTIAL_FILE]) rmSync(join(dir, name), { force: true });
  } catch {
    log(`The Apex code kept for run ${runId}'s summaries couldn't be deleted. The retention sweep deletes it within seven days.`);
  }
}

/**
 * Claude's summaries for the console to hold until a person decides them
 * (DECISIONS.md 494), from handbook-summaries.json as it is once the summaries
 * have run: this delivery's, or an earlier one's when the code was gone
 * before this one asked. None from a file with no summaries in it, since a
 * row of nothing would hold the release for a decision about nothing. A file
 * that doesn't parse is logged without a word of it, and not sent.
 */
function summariesOf(dir: string, runId: string, log: (line: string) => void): SummariesToSave | null {
  const path = join(dir, SUMMARIES_FILE);
  if (!existsSync(path)) return null;
  let raw: unknown;
  try {
    raw = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    raw = null;
  }
  const parsed = HandbookSummaries.safeParse(raw);
  if (!parsed.success) {
    log(`Run ${runId}'s file of Claude's summaries didn't parse, so the console gets none of them.`);
    return null;
  }
  if (parsed.data.items.length === 0) return null;
  const { model, madeAt, items, leftOut } = parsed.data;
  return { model, madeAt, items, leftOut };
}

/** The most of facts.json the worker sends. A real one is a few kilobytes; more means the step changed. */
export const FACTS_FILE_MAX_BYTES = 256 * 1024;

/** The longest detail the console keeps. A fact that couldn't be checked carries Salesforce's error text, which has no limit of its own. */
export const FACTS_DETAIL_MAX = 1000;

const ASSUMED_IDS = new Set(ASSUMPTIONS.map((a) => a.id));

// What the console reads the file back through, blackcloud-next
// lib/health-check/run-facts.ts (`FactsFile`). It reads the whole file as null
// if any one value breaks a cap, so the worker checks the same caps first and
// keeps to the summary rather than store a file the run page can't show. Change
// the two together.
const ConsoleName = z.string().regex(/^[a-z0-9][a-z0-9._-]*$/);
const ConsoleCount = z.number().int().nonnegative();
export const ConsoleFactsFile = z
  .object({
    version: z.number().int().positive(),
    checkedAt: Timestamp,
    apiVersion: z.string().min(1).max(20),
    connectionKind: ConsoleName.max(40),
    calls: ConsoleCount,
    facts: z
      .array(
        z.object({
          id: ConsoleName.max(100),
          question: ConsoleName.max(100),
          covers: z.enum(["whole", "part"]),
          says: z.string().min(1).max(500),
          outcome: z.enum(["confirmed", "contradicted", "open", "not_checked"]),
          // The console's figure, not FACTS_DETAIL_MAX, so a change to the cut can't move the check with it.
          detail: z.string().max(1000),
          observed: z
            .record(z.string().min(1).max(200), z.union([z.boolean(), z.string().max(100), z.null()]))
            .refine((o) => Object.keys(o).length <= 50),
          affects: z.array(ConsoleName.max(100)).max(50),
        }),
      )
      .max(100),
  })
  .refine((f) => new Set(f.facts.map((x) => x.id)).size === f.facts.length);

/**
 * A detail as sent: on one line, with no control character, and cut inside the
 * console's limit with an ellipsis. Postgres's jsonb can't hold U+0000, and a
 * failed insert there rolls back the whole run's results, not only the facts.
 */
function sentDetail(detail: string): string {
  const line = detail.replace(/[\u0000-\u001F\u007F-\u009F]/g, " ").replace(/\s+/g, " ").trim();
  if (line.length <= FACTS_DETAIL_MAX) return line;
  let cut = line.slice(0, FACTS_DETAIL_MAX - 1);
  // Never half an emoji: a lone surrogate isn't valid in the console's jsonb.
  if (/[\uD800-\uDBFF]$/.test(cut)) cut = cut.slice(0, -1);
  return `${cut}…`;
}

/**
 * The facts step's summary for the console, scans included, read before a
 * scan's folder is deleted, and beside it the whole file (DECISIONS.md 433). A
 * run with no facts.json, or one that doesn't parse, sends neither: the findings
 * already say what each check could measure. The file goes only while it is
 * what decision 288 describes, the facts we assume and nothing else, so one
 * naming a fact we don't assume, or too big to be only those, keeps to the
 * summary. Each detail is sent on one line and cut to what the console keeps,
 * so one long error from Salesforce doesn't lose every fact; facts.json on disk
 * keeps it whole. A file that still doesn't fit the console's caps keeps to the
 * summary too. The log line says which check failed and never quotes the file.
 */
export function factsOf(dir: string, log: (line: string) => void): { summary: FactsReport | null; file: FactsFile | null } {
  const path = join(dir, FACTS_FILE);
  if (!existsSync(path)) return { summary: null, file: null };
  let raw: unknown;
  try {
    raw = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    raw = null;
  }
  const parsed = FactsFile.safeParse(raw);
  if (!parsed.success) {
    log("The run's facts.json didn't parse, so the console gets no facts summary or file.");
    return { summary: null, file: null };
  }
  const summary = factsReport(parsed.data);
  // Parsing dropped any key the schema doesn't name, so with the details cut this is the file as sent.
  const file: FactsFile = { ...parsed.data, facts: parsed.data.facts.map((f) => ({ ...f, detail: sentDetail(f.detail) })) };
  if (file.facts.some((f) => !ASSUMED_IDS.has(f.id))) {
    log("The run's facts.json names a fact we don't assume, so the console gets the facts summary but not the file.");
    return { summary, file: null };
  }
  if (Buffer.byteLength(JSON.stringify(file), "utf8") >= FACTS_FILE_MAX_BYTES) {
    log("The run's facts.json is larger than the worker sends, so the console gets the facts summary but not the file.");
    return { summary, file: null };
  }
  // Only the detail carries Salesforce's text, so a NUL anywhere else means the step changed: refuse rather than mend.
  if (!ConsoleFactsFile.safeParse(file).success || holdsNul(file)) {
    log("The run's facts.json doesn't fit the console's shape for it, so the console gets the facts summary but not the file.");
    return { summary, file: null };
  }
  return { summary, file };
}

/** Whether any string in the value, or any object key, holds U+0000, which Postgres's jsonb refuses. */
function holdsNul(value: unknown): boolean {
  if (typeof value === "string") return value.includes("\u0000");
  if (Array.isArray(value)) return value.some(holdsNul);
  if (value && typeof value === "object") return Object.entries(value).some(([k, v]) => k.includes("\u0000") || holdsNul(v));
  return false;
}

/**
 * A monthly pulse, once its findings are saved: what changed since its baseline,
 * for the console to show. Unlike a scan, it keeps its run folder, because the
 * next pulse compares against it, and its connection, because an Ongoing
 * customer stays connected. Retention deletes the folder in time (DECISIONS.md
 * 136). A failure here is logged, as a scan's is.
 */
async function finishPulse(runId: string, dir: string, deps: WorkerDeps, at: string, log: (line: string) => void): Promise<void> {
  try {
    // loadRun reads the baseline from the folder beside this one, if it's there and readable.
    const { input } = loadRun(dir);
    const result = buildPulseResult(buildReport(input));
    await deps.store.savePulseResult(runId, result, at);
    log(`Pulse ${runId}'s result is ready${result.baselineRunId ? `, against ${result.baselineRunId}` : ", with no baseline we could read"}.`);
  } catch (err) {
    log(`Pulse ${runId}'s result couldn't be saved: ${safeReason(err)}`);
  }
}

/**
 * A free scan, once its findings are saved: the teaser for the prospect, then
 * the connection given back, because a scan never reads the org again. A
 * failure here is logged; the console shows "working on it" until the row exists.
 */
async function finishScan(runId: string, dir: string, name: string, deps: WorkerDeps, at: string, log: (line: string) => void): Promise<void> {
  try {
    const { input } = loadRun(dir);
    await deps.store.saveScanResult(runId, buildScanResult(buildReport(input)), at);
    log(`Scan ${runId}'s result is ready.`);
  } catch (err) {
    log(`Scan ${runId}'s result couldn't be saved: ${safeReason(err)}`);
  }
  // Nothing of a prospect's org stays on the worker: the teaser is all a scan keeps,
  // and the privacy notice promises it. A failed teaser leaves "the scan failed" to show.
  try {
    deleteRunFolder(dir, runId);
    log(`Scan ${runId}'s run folder is deleted.`);
  } catch (err) {
    log(`Scan ${runId}'s run folder couldn't be deleted: ${safeReason(err)}`);
  }
  try {
    await revokeConnection({ name, store: deps.tokens, transport: deps.oauthTransport });
    log(`Scan ${runId}'s connection is revoked.`);
  } catch (err) {
    deps.tokens.delete(name);
    log(`Salesforce didn't confirm revoking scan ${runId}'s connection, so the worker deleted its copy: ${err instanceof OAuthError ? err.code : "error"}.`);
  }
}
