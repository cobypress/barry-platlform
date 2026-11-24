import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, knownModuleIds, modulesForRun } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { applyNarrative, type Rejection } from "../narrative/apply.ts";
import type { NarrativeClient } from "../narrative/client.ts";
import { buildReport, surveyMetricsOf, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { BASELINE_OVERRIDES_FILE, BASELINE_READ_FILE, INTERVIEWS_FILE, keepSurveyMetrics, loadRun, OUTPUT_FILES, SURVEY_FILE, writeBaselineRead, writeRows } from "../render/render-run.ts";
import { BASELINE_ROWS_UNREAD, baselineRefusedSummary } from "../render/render-released.ts";
import { INTERVIEW_VERSION } from "../interviews/questions.ts";
import { OAuthConnectionProvider, ReconnectionNeeded } from "../salesforce/oauth-provider.ts";
import { buildScanResult } from "../scan/result.ts";
import { buildPulseResult } from "../pulse/result.ts";
import { buildDigest } from "../digest/build.ts";
import type { RunDigest } from "../schemas/digest.ts";
import type { ReportSection } from "../schemas/index.ts";
import { deleteRunFolder } from "../retention/purge.ts";
import type { StoredConnection } from "../salesforce/token-store.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import { appFor, appNotSetUp, needsReconnection } from "./apps.ts";
import { withJobLock } from "./lock.ts";
import { automaticRefusal } from "./automatic.ts";
import { recalibrateForRunAgain, type Recalibrated } from "./connect-job.ts";
import { checkedDigest } from "../digest/checked.ts";
import { checkSections, decideRelease, type ReleaseDecision } from "./release.ts";
import { letGoAfterRun, type SeenRun } from "./scan-release.ts";
import { progressOf, RunClosedError, runOverall, type RunJobInput } from "./types.ts";
import { withoutComments } from "../survey/questions.ts";
import { SURVEY_METRIC_IDS } from "../survey/summary.ts";
import { FACTS_FILE } from "../extract/facts.ts";
import { inventoryOf } from "./inventory.ts";
import { COMPLEXITY_FILE } from "../complexity/read.ts";
import { heldCounts } from "../complexity/held.ts";
import { Complexity } from "../schemas/complexity.ts";
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

export const ORG_SETTINGS_ON_CONSULTANT_RUN = "The calibration was read from the org's own settings and no person has confirmed it, so a consultant-led run can't start. Confirm the calibration first.";

/** Whether this is an automatic run a hold is waiting on. */
const heldFor = (input: RunJobInput): boolean => input.run.reviewMode === "automatic" && typeof input.heldReason === "string";
const heldOutcome = (runId: string, input: RunJobInput): JobOutcome => ({ outcome: "skipped", detail: `Run ${runId} is held (${input.heldReason}), so it waits for a person and nothing was read` });

function problemWith(input: RunJobInput, deps: WorkerDeps): string | null {
  // First, before anything else is checked or loaded: an automatic run reads nothing it isn't allowed to (DECISIONS.md 688).
  const automatic = automaticRefusal(input.run, deps.automatic);
  if (automatic) return automatic;
  if (!input.calibration) return "The calibration hasn't been confirmed, so the run can't start.";
  // Only an automatic run may start from a calibration nobody confirmed (DECISIONS.md 692).
  if (input.calibration.source === "org_settings" && input.run.reviewMode !== "automatic") return ORG_SETTINGS_ON_CONSULTANT_RUN;
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

/** What a run job may ask beyond its run: `again`, a person's "Run again" of an automatic run (DECISIONS.md 698). */
export type RunJobOptions = { again?: string };

/**
 * A person's "Run again" (DECISIONS.md 698): the console adds the run job under
 * the id `run-<runId>-again-<ms>`, the time of the press in milliseconds (1 to
 * 16 digits, no leading zero), and nothing more in its data. The time, or null
 * for any other id: the first run's fixed `run-<runId>`, a scan's or a
 * consultant-led run's `run-<runId>-<ms>`, or none.
 */
export function runAgainToken(runId: string, jobId: string | undefined): string | null {
  if (!jobId || !jobId.startsWith(`run-${runId}-again-`)) return null;
  const token = jobId.slice(`run-${runId}-again-`.length);
  return /^[1-9][0-9]{0,15}$/.test(token) ? token : null;
}

/** The run folder's record of which "Run again" started it afresh, so a retry of that same job resumes rather than starting again. */
export const AGAIN_FILE = "again.json";

export async function processRunJob(runId: string, deps: WorkerDeps, attempt: Attempt, options: RunJobOptions = {}): Promise<JobOutcome> {
  if (!RUN_ID.test(runId)) return { outcome: "failed", detail: "The run id isn't one the worker can use as a folder name" };
  return withJobLock(deps.locksDir, `run-${runId}`, () => runThenLetGo(runId, deps, attempt, options));
}

/** The press time of the "Run again" that last started this folder afresh, or null: none, or a mark that isn't one, which any press is newer than. */
function againOf(dir: string): string | null {
  try {
    const read = JSON.parse(readFileSync(join(dir, AGAIN_FILE), "utf8")) as { again?: unknown };
    return typeof read.again === "string" && /^[1-9][0-9]{0,15}$/.test(read.again) ? read.again : null;
  } catch {
    return null;
  }
}

/** The answer a "Run again" gets when the console hasn't set the run back first: nothing read, nothing removed. */
/** The answer an older "Run again" gets once a newer press has started the run afresh: nothing read, nothing removed. */
export const AGAIN_SUPERSEDED = "A newer Run again has started this run afresh, so this older one read nothing and left its folder as it was.";

export const AGAIN_NOT_RESET = "Run again starts an automatic run afresh only once it's set back to extracting with no hold, so nothing was read and its folder is as it was.";

/**
 * A free scan gives the org's access back however its run ends (DECISIONS.md
 * 652), inside the run's lock, once the job has an outcome: done, failed,
 * stopped at the budget, or skipped as closed or gone. An error going back to
 * the queue keeps the connection while another delivery will come, since that
 * one resumes through it; on the last delivery nothing will, so it lets go
 * first. Only a run the console says is a scan is ever let go.
 */
async function runThenLetGo(runId: string, deps: WorkerDeps, attempt: Attempt, options: RunJobOptions = {}): Promise<JobOutcome> {
  const seen: SeenRun = {};
  let outcome: JobOutcome;
  try {
    outcome = await runLocked(runId, deps, attempt, seen, options);
  } catch (err) {
    if (attempt.final) await letGoAfterRun(runId, seen, deps);
    throw err;
  }
  await letGoAfterRun(runId, seen, deps);
  return outcome;
}

async function runLocked(runId: string, deps: WorkerDeps, attempt: Attempt, seen: SeenRun, options: RunJobOptions = {}): Promise<JobOutcome> {
  const log = deps.log ?? (() => {});
  const now = () => (deps.now ?? (() => new Date()))();
  const first = await deps.store.loadRunJob(runId);
  if (!first) return { outcome: "skipped", detail: `No run ${runId}` };
  Object.assign(seen, { product: first.run.product, connectionName: first.connectionName });
  if (CLOSED.has(first.run.status)) return { outcome: "skipped", detail: `Run ${runId} is ${first.run.status.replace("_", " ")}, so there's nothing for the worker to do` };
  // A failed automatic run is a person's to run again (DECISIONS.md 693): a stray second message never runs or releases it.
  if (first.run.reviewMode === "automatic" && first.run.status === "failed") return { outcome: "skipped", detail: `Automatic run ${runId} failed, so it waits for a person to run it again and nothing was read` };
  // A held automatic run waits for a named person, always: code never reads it again or releases it (DECISIONS.md 693).
  if (heldFor(first)) return heldOutcome(runId, first);
  // A person's "Run again" of an automatic run reads the org afresh (DECISIONS.md 698): its folder is cleared and
  // nothing is resumed, so a run held for what it read is read again. Only a run the console has set back to
  // extracting, with its hold cleared (both checked above and again below), and only once per "Run again": a retry
  // of the same job finds its own mark and resumes. Any other run, and a consultant-led run's, resumes as before.
  const again = options.again !== undefined && first.run.reviewMode === "automatic" ? options.again : undefined;
  if (options.again !== undefined && again === undefined) log(`Run ${runId} isn't automatic, so "Run again" resumes it as any delivery does.`);
  // Presses are ordered by their time: the folder's mark is the latest press that started it afresh. The same press
  // resumes; a newer one starts afresh; an older one (a retry still due after a newer press) is skipped, so two
  // presses never clear each other's work in turn.
  const mark = again === undefined ? null : againOf(join(deps.runsDir, runId));
  if (again !== undefined && mark !== null && BigInt(mark) > BigInt(again)) {
    log(`Run ${runId}: a newer "Run again" owns this run, so this older one read nothing and left its folder as it was.`);
    return { outcome: "skipped", detail: `Run ${runId}: ${AGAIN_SUPERSEDED}` };
  }
  const fresh = again !== undefined && mark !== again;
  if (fresh && first.run.status !== "extracting") return { outcome: "skipped", detail: `Run ${runId}: ${AGAIN_NOT_RESET}` };

  // The run is this delivery's before anything is written, to the folder or the
  // store, a failure's included: the console's move to review, landing after the
  // read above, refuses the claim here rather than the save once the folder is
  // rewritten (DECISIONS.md 537, 538, 540). A job that claims goes on to work or
  // to fail; it skips only for a run gone since, or a save the store still
  // refuses (538 (4)). Any other error from the claim goes back to the queue.
  try {
    await deps.store.claimRun(runId);
  } catch (err) {
    if (err instanceof RunClosedError) return { outcome: "skipped", detail: err.message };
    throw err;
  }

  const fail = async (reason: string): Promise<JobOutcome> => {
    // Failed for good: nothing will resume to use the Apex code kept for Claude's summaries (DECISIONS.md 491).
    forgetApexSource(join(deps.runsDir, runId), runId, log);
    await deps.store.recordRunFailure(runId, reason, now().toISOString());
    return { outcome: "failed", detail: reason };
  };

  // Read again now the run is this delivery's, and worked only from this
  // reading: an answer the console saved just before the claim is in it, and
  // none is saved after (DECISIONS.md 542, 545). A status changed since the
  // first reading doesn't matter, as the claim has settled it. A run gone in
  // between has nothing left to fail, so it's skipped with nothing written.
  // The claim may have turned awaiting manual steps to analysing, so an error
  // here is handled as the engine's are: back to the queue, or on the last
  // attempt the run fails rather than stay analysing with no reason (548). No
  // token is loaded yet, so there's no secret to take out of the text.
  let second: RunJobInput | null;
  try {
    second = await deps.store.loadRunJob(runId);
  } catch (err) {
    const reason = safeReason(err);
    if (!attempt.final) throw new Error(reason);
    return fail(`The run stopped with an error: ${reason}`);
  }
  if (!second) return { outcome: "skipped", detail: `No run ${runId}` };
  let input = second;
  Object.assign(seen, { product: input.run.product, connectionName: input.connectionName });
  // Held between the two readings (a second connect, a person): still nothing read.
  if (heldFor(input)) return heldOutcome(runId, input);

  // A person's "Run again" of an automatic run with no calibration (DECISIONS.md 700): one held before 700
  // because its currency couldn't be read. Its org is read again through its stored connection and the
  // calibration built and saved as at connect, before the folder is cleared; none built, it is held again
  // with the true cause. Only for a new press, with the switch on and a connection to read through:
  // anything else goes on to the checks below, which refuse a run with no calibration as before.
  if (fresh && input.run.reviewMode === "automatic" && !input.calibration && !automaticRefusal(input.run, deps.automatic) && input.connectionName && input.run.connectionKind === "oauth") {
    let rebuilt: Recalibrated;
    try {
      rebuilt = await recalibrateForRunAgain(runId, input.connectionName, input.run.orgId, deps, log);
    } catch (err) {
      const reason = safeReason(err);
      if (!attempt.final) throw new Error(reason);
      return fail(`The run's org couldn't be read again to calibrate it: ${reason}`);
    }
    if (rebuilt.kind === "held") return { outcome: "skipped", detail: `Run ${runId}: ${rebuilt.detail}` };
    if (rebuilt.kind === "unsaved") return fail(rebuilt.detail);
    // As the store now has it: a delivery that saved one meanwhile is the one kept.
    const saved = await deps.store.loadRunJob(runId).catch(() => null);
    input = saved?.calibration ? saved : { ...input, calibration: rebuilt.calibration };
  }

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
  if (fresh) {
    // The last guard before the folder goes: still extracting and unheld as this delivery read it after its claim.
    if (input.run.status !== "extracting" || heldFor(input)) return { outcome: "skipped", detail: `Run ${runId}: ${AGAIN_NOT_RESET}` };
    rmSync(dir, { recursive: true, force: true });
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    writeFileSync(join(dir, AGAIN_FILE), `${JSON.stringify({ again })}\n`, { mode: 0o600 });
    log(`Run ${runId} is run again from the start: its folder was cleared and the org is read afresh.`);
  }
  const resume = existsSync(join(dir, "run.json")) ? runId : undefined;
  const modules = modulesForRun(input.run.modules, deps.modules ?? ALL_MODULES);
  // The app the connection was made with, and no other (DECISIONS.md 616). One the worker no
  // longer has fails now, before anything is sent: no retry could bring it back.
  const app = stored.grant === "web-server" ? appFor(stored.clientId, deps) : deps.oauthClient;
  if (!app) return fail(appNotSetUp(name));
  const provider = new OAuthConnectionProvider(name, { store: deps.tokens, client: app, transport: deps.oauthTransport, env: approvedFromConsole(name), log });
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
      // The folder's run.json says how the run is reviewed, so its documents read the true mode (DECISIONS.md 692).
      reviewMode: input.run.reviewMode,
      baselineRunId: input.run.baselineRunId,
      keepApexSource: apexSummaries,
      // A health check runs PMD and ESLint beside our scan, and only stores what they find; a scan and a pulse don't (DECISIONS.md 670).
      ...(input.run.product === "barometer" ? { codeLint: deps.codeLint ?? { pmd: null } } : {}),
      // Who connected, from the token loaded above on every delivery, resumes too (DECISIONS.md 498).
      connectedUser: { id: stored.userId },
      // Every run the worker makes reads the org's inventory (DECISIONS.md 605).
      inventory: true,
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
    // Salesforce has ended the connection: only the admin signing in again helps, so no retry (DECISIONS.md 617).
    if (err instanceof ReconnectionNeeded || provider.lostAccess) return fail(needsReconnection(name));
    const reason = safeReason(err, secrets);
    // The engine resumes from its checkpoints, so another delivery is cheap and safe.
    if (!attempt.final) throw new Error(reason);
    return fail(`The run stopped with an error: ${reason}`);
  }

  // Refused part way through: the reads after it came back unread, so these results aren't the org's.
  if (provider.lostAccess) return fail(needsReconnection(name));

  if (result.run.status === "stopped_budget") {
    return { outcome: "stopped_budget", detail: result.run.budget.stoppedReason ?? "Stopped at the API budget" };
  }

  // An automatic run's release or hold, once decided (DECISIONS.md 693).
  let decision: ReleaseDecision | null = null;
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
    // A re-check is drafted from its earlier health check's corrections as they
    // stand now, and keeps them, so its documents are built from the same rows
    // whatever is made again later (DECISIONS.md 528). None, or no baseline: the
    // file goes, so a re-draft always matches its drafts. It records too whether
    // it read the earlier health check at all, so its documents compare with it
    // only if the drafts do (534). Written before the sections file and the
    // console's save, in that order: a run reaches review only by a delivery
    // whose writes all happened, and every delivery drafts again, so a half-done
    // one never stands (DECISIONS.md 529). A scan and a pulse are never reviewed
    // or printed, so neither keeps any.
    const { input: reportInput, pseudonyms, baselineRowsUnread } = loadRun(dir, { baselineRows: "live" });
    if (input.run.product !== "scan" && input.run.product !== "pulse") {
      // Read as none, so the draft holds the figures as read, as its documents will (DECISIONS.md 531). A pulse says so when it's built.
      if (baselineRowsUnread) log(`Run ${runId}: ${BASELINE_ROWS_UNREAD}`);
      const kept = join(dir, BASELINE_OVERRIDES_FILE);
      const rows = reportInput.baseline?.overrides ?? [];
      if (rows.length > 0) writeRows(kept, rows);
      else rmSync(kept, { force: true });
      // Only a re-check: a first health check's folder is left as it was.
      if (reportInput.run.baselineRunId) writeBaselineRead(join(dir, BASELINE_READ_FILE), reportInput.baseline != null);
    }
    // Before the sections, because these are kept as they're made and the
    // sections aren't: a job stopped part way through picks the summaries up
    // where they were and drafts the sections once, where the other order would
    // pay for every section again on each delivery. Each catches its own
    // failures, so neither costs the other. With no code kept, nothing is asked.
    await summariseApex({ dir, runId, enabled: apexSummaries, client: deps.narrative, pseudonyms, now: now(), log });
    // A scan and a pulse are never reviewed, so Claude has nothing to draft for either.
    const unreviewed = input.run.product === "scan" || input.run.product === "pulse";
    // A run awaiting manual steps is analysed again once the last answer is in,
    // and that delivery drafts: nobody can edit or approve these sections before
    // then, so Claude's wording now would be paid for and thrown away (DECISIONS.md 543).
    const awaiting = result.run.status === "awaiting_manual";
    if (deps.narrative && !unreviewed && awaiting) log(`Run ${runId} is awaiting manual steps, so its sections are the engine's own text until the answers are in.`);
    const report = deps.narrative && !unreviewed && !awaiting ? await draftSections(runId, dir, reportInput, deps.narrative, pseudonyms, now(), log) : buildReport(reportInput);
    // An automatic run's sections are checked by code, with no person (DECISIONS.md 693): before anything is written, so the folder holds what's saved.
    const automatic = input.run.reviewMode === "automatic" && input.run.product === "barometer";
    if (automatic) {
      const checked = checkSections(report, pseudonyms);
      if (checked.fellBack || checked.failed) log(`Run ${runId}: ${checked.fellBack} section${checked.fellBack === 1 ? "" : "s"} fell back to the engine's own text, and ${checked.failed} didn't pass its check.`);
    }
    const outputs = Object.values(report.outputs);
    // The survey's metrics as this report made them, stored with the run's own,
    // so its sections cite only what the run holds (DECISIONS.md 645). Before the
    // sections file, as every other write the sections rest on is.
    const surveyMetrics = surveyMetricsOf(report);
    keepSurveyMetrics(dir, surveyMetrics);
    writeFileSync(join(dir, SECTIONS_FILE), `${JSON.stringify(outputs, null, 2)}\n`, { mode: 0o600 });
    const facts = factsOf(dir, log);
    // Only with the switch on, as it stands now: off, the console's table is never named.
    const handbookSummaries = apexSummaries ? summariesOf(dir, runId, log) : null;
    // Always a row, so the console never has to guess why there's none (DECISIONS.md 618).
    const inventory = inventoryOf(dir, log, { runId, madeAt: result.run.asOf, scope: result.run.scope });
    const complexity = complexityOf(dir, log);
    // A pulse's digest, from the report its result is made from, with its results (DECISIONS.md 632). A check's waits for its documents.
    // An automatic run's, checked, masked or held, written with its release (DECISIONS.md 693): its portal has nothing else to show.
    const checkedOne = automatic ? checkedDigest({ input: reportInput, pseudonyms, inventory, madeAt: now().toISOString() }) : null;
    const digest = input.run.product === "pulse" ? digestOf(runId, () => buildDigest({ report, pseudonyms, inventory, madeAt: now().toISOString() }), log) : (checkedOne?.digest ?? null);
    const sections = outputs.flatMap((o) => o.sections);
    // The engine's own, without any survey metric: those go apart, in a savepoint of their own.
    const metrics = result.output!.metrics.filter((m) => !SURVEY_METRIC_IDS.includes(m.id));
    for (const line of danglingCitations(runId, sections, { ...result.output!, metrics: [...metrics, ...surveyMetrics] })) log(line);
    const overall = runOverall(report.overall);
    // Released by code or held for a person, decided here and written in the save's one transaction (DECISIONS.md 693).
    decision = checkedOne
      ? decideRelease({ runId, sections, checked: checkedOne, findings: result.output!.findings, overall, previousScore: await deps.store.lastReleasedScore(runId, input.run.orgId.slice(0, 15), input.run.product), now: now().toISOString() })
      : null;
    if (decision) log(decision.kind === "release" ? `Automatic run ${runId} passed every check and is released.` : `Automatic run ${runId} is held for a person: ${decision.reason} ${JSON.stringify(decision.counts)}.`);
    await deps.store.saveRunOutput(runId, {
      progress: progressOf(result.run),
      runDir: dir,
      findings: result.output!.findings,
      metrics,
      surveyMetrics,
      scores: reportInput.scores ?? [],
      manualSteps: result.output!.manualSteps,
      // The engine leaves its sections as drafts, and the console approves only
      // sections in review. It has no step that starts review, so a run that's
      // in review hands over its drafts in review, as the console's own import does.
      // A section Claude's draft couldn't settle stays needs_input.
      sections: sections.map((s) => (result.run.status === "in_review" && s.status === "draft" ? { ...s, status: "in_review" as const } : s)),
      versions: outputs.flatMap((o) => o.versions),
      facts: facts.summary,
      factsFile: facts.file,
      ...(handbookSummaries ? { handbookSummaries } : {}),
      inventory,
      ...(complexity ? { complexity } : {}),
      // The score and grade the report gives, pulses' and scans' too, for the portal (DECISIONS.md 626).
      overall,
      // A pulse's whatever it is: null takes away an earlier delivery's, so a stale digest is never served. An automatic run's likewise.
      ...(input.run.product === "pulse" || decision ? { digest } : {}),
      ...(decision ? { automatic: decision.kind === "release" ? { decision: "release" as const, releases: decision.releases } : { decision: "hold" as const, reason: decision.reason, counts: decision.counts, at: now().toISOString() } } : {}),
    });
  } catch (err) {
    if (err instanceof RunClosedError) return { outcome: "skipped", detail: err.message };
    // Another delivery reuses every checkpoint, so it costs no data reads. On
    // the last one, the run mustn't be left at "analysing" with no reason given.
    const reason = safeReason(err, secrets);
    if (!attempt.final) throw new Error(reason);
    return fail(`The run finished, but its results couldn't be saved: ${reason}`);
  }
  if (input.run.product === "scan") await finishScan(runId, dir, deps, now().toISOString(), log);
  if (input.run.product === "pulse") await finishPulse(runId, dir, deps, now().toISOString(), log);
  if (decision) return { outcome: "done", detail: decision.kind === "release" ? `Run ${runId} is released` : `Run ${runId} is held: ${decision.reason}` };
  return { outcome: "done", detail: `Run ${runId} is ${result.run.status.replace("_", " ")}` };
}

/**
 * A log line for each section about to be stored that cites a finding, metric
 * or manual step its own run doesn't hold (DECISIONS.md 643). Never a refusal:
 * a dangling citation mustn't cost a run its results. Only counts and metric
 * ids, which the engine names, are logged: never a section's text.
 */
export function danglingCitations(runId: string, sections: ReportSection[], output: { findings: Array<{ id: string }>; metrics: Array<{ id: string }>; manualSteps: Array<{ id: string }> }): string[] {
  const has = (rows: Array<{ id: string }>) => new Set(rows.map((r) => r.id));
  const [findings, metrics, steps] = [has(output.findings), has(output.metrics), has(output.manualSteps)];
  return sections.flatMap((s) => {
    const lost = { findings: s.findingIds.filter((id) => !findings.has(id)), metrics: s.metricIds.filter((id) => !metrics.has(id)), steps: s.manualStepIds.filter((id) => !steps.has(id)) };
    const count = lost.findings.length + lost.metrics.length + lost.steps.length;
    if (count === 0) return [];
    const parts = [lost.findings.length ? `${lost.findings.length} finding${lost.findings.length === 1 ? "" : "s"}` : "", lost.metrics.length ? `${lost.metrics.length} metric${lost.metrics.length === 1 ? "" : "s"} (${lost.metrics.join(", ")})` : "", lost.steps.length ? `${lost.steps.length} manual step${lost.steps.length === 1 ? "" : "s"}` : ""].filter(Boolean);
    return [`Run ${runId}: section ${s.output}:${s.key} cites ${count} ${count === 1 ? "id" : "ids"} not in the run, kept as they are: ${parts.join(", ")}.`];
  });
}

/**
 * A run's digest (DECISIONS.md 632), or null: one that can't be made never
 * costs the run its results or its documents. Logged by the error's class
 * alone, since its message could quote the report's text.
 */
export function digestOf(runId: string, make: () => RunDigest, log: (line: string) => void): RunDigest | null {
  try {
    return make();
  } catch (err) {
    log(`Run ${runId}'s digest couldn't be made (${err instanceof Error ? err.name : "error"}), so the connector has none for it.`);
    return null;
  }
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

/** The most of complexity.json the worker sends. Seven components of a few counts each is a few kilobytes. */
export const COMPLEXITY_MAX_BYTES = 256 * 1024;

/**
 * The org's complexity for the console (DECISIONS.md 614), read before a scan's
 * folder is deleted, as the inventory is. None from a run without the file, or
 * one that doesn't parse, is too big or doesn't fit its shape: the results save
 * without it, and the log says which, never quoting the file.
 */
export function complexityOf(dir: string, log: (line: string) => void): Complexity | null {
  const path = join(dir, COMPLEXITY_FILE);
  if (!existsSync(path)) return null;
  let text: string;
  let raw: unknown;
  try {
    text = readFileSync(path, "utf8");
    raw = JSON.parse(text);
  } catch {
    log("The run's complexity.json didn't parse, so the console gets no complexity score.");
    return null;
  }
  if (Buffer.byteLength(text, "utf8") >= COMPLEXITY_MAX_BYTES) {
    log("The run's complexity.json is larger than the worker sends, so the console gets no complexity score.");
    return null;
  }
  const parsed = Complexity.safeParse(raw);
  if (!parsed.success || holdsNul(parsed.data)) {
    log("The run's complexity.json doesn't fit the complexity score's shape, so the console gets no complexity score.");
    return null;
  }
  return parsed.data;
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
    // The earlier folder's corrections as they stand: a pulse is built once, here, and never printed (DECISIONS.md 528).
    const { input, baselineRowsUnread } = loadRun(dir, { baselineRows: "live" });
    if (baselineRowsUnread) log(`Pulse ${runId}: ${BASELINE_ROWS_UNREAD}`);
    const report = buildReport(input);
    // The baseline is read as its documents showed it, where that can still be carried (DECISIONS.md 522).
    if (report.baselineRefused) log(`Pulse ${runId}: ${baselineRefusedSummary(report.baselineRefused)}`);
    const result = buildPulseResult(report);
    await deps.store.savePulseResult(runId, result, at);
    log(`Pulse ${runId}'s result is ready${result.baselineRunId ? `, against ${result.baselineRunId}` : ", with no baseline we could read"}.`);
  } catch (err) {
    log(`Pulse ${runId}'s result couldn't be saved: ${safeReason(err)}`);
  }
}

/**
 * A free scan, once its findings are saved: the teaser for the prospect, and
 * its folder deleted. Its connection is given back once the job returns, as
 * on every other end of a scan (DECISIONS.md 652). A failure here is logged;
 * the console shows "working on it" until the row exists.
 */
async function finishScan(runId: string, dir: string, deps: WorkerDeps, at: string, log: (line: string) => void): Promise<void> {
  try {
    const { input } = loadRun(dir);
    // The org's counts come from the complexity score, which has checked each was read whole
    // (DECISIONS.md 623). Silent here: the results' own save has already logged any fault in the file.
    await deps.store.saveScanResult(runId, buildScanResult(buildReport(input), heldCounts(complexityOf(dir, () => {}))), at);
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
}
