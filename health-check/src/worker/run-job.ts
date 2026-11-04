import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../domains/index.ts";
import { runHealthCheck } from "../engine/run.ts";
import { buildReport } from "../narrative/sections.ts";
import { loadRun } from "../render/render-run.ts";
import { OAuthConnectionProvider } from "../salesforce/oauth-provider.ts";
import type { StoredConnection } from "../salesforce/token-store.ts";
import { safeReason, type Attempt, type JobOutcome, type WorkerDeps } from "./deps.ts";
import { withJobLock } from "./lock.ts";
import { progressOf, RunClosedError, type RunJobInput } from "./types.ts";

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
    // needs no browser and no Claude: it's the engine's own text, as `hc render`
    // writes it before any narrative.
    if (input.intake) writeFileSync(join(dir, "intake.json"), `${JSON.stringify(input.intake, null, 2)}\n`, { mode: 0o600 });
    const { input: reportInput } = loadRun(dir);
    const report = buildReport(reportInput);
    const outputs = Object.values(report.outputs);
    writeFileSync(join(dir, SECTIONS_FILE), `${JSON.stringify(outputs, null, 2)}\n`, { mode: 0o600 });
    await deps.store.saveRunOutput(runId, {
      progress: progressOf(result.run),
      runDir: dir,
      findings: result.output!.findings,
      metrics: result.output!.metrics,
      scores: reportInput.scores ?? [],
      manualSteps: result.output!.manualSteps,
      sections: outputs.flatMap((o) => o.sections),
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
  return { outcome: "done", detail: `Run ${runId} is ${result.run.status.replace("_", " ")}` };
}
