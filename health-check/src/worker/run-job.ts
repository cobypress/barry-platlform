import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { loadRun, OUTPUT_FILES, SURVEY_FILE } from "../render/render-run.ts";
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
  const known = new Set((deps.modules ?? SALES_CLOUD_MODULES).map((m) => m.id));
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
  const modules = (deps.modules ?? SALES_CLOUD_MODULES).filter((m) => input.run.modules.includes(m.id));
  const provider = new OAuthConnectionProvider(name, { store: deps.tokens, client: deps.oauthClient, transport: deps.oauthTransport, env: approvedFromConsole(name) });
  const secrets = [stored.accessToken, stored.refreshToken];

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
    const { input: reportInput, pseudonyms } = loadRun(dir);
    // A scan and a pulse are never reviewed, so Claude has nothing to draft for either.
    const unreviewed = input.run.product === "scan" || input.run.product === "pulse";
    const report = deps.narrative && !unreviewed ? await draftSections(runId, dir, reportInput, deps.narrative, pseudonyms, now(), log) : buildReport(reportInput);
    const outputs = Object.values(report.outputs);
    writeFileSync(join(dir, SECTIONS_FILE), `${JSON.stringify(outputs, null, 2)}\n`, { mode: 0o600 });
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
