import { assertOutsideGit } from "../engine/run.ts";
import { processConnectJob } from "./connect-job.ts";
import type { Attempt, JobOutcome, WorkerDeps } from "./deps.ts";
import { processDocumentsJob } from "./documents-job.ts";
import { processRunJob } from "./run-job.ts";
import { parseJob } from "./types.ts";

// The one entry point a queue calls. It returns when the job is settled one
// way or another (done, skipped, refused or failed, with the reason recorded
// where the console reads it), and throws only when another delivery could do
// better: a transient error, or another job holding the same run or link.

export type JobHandler = (data: unknown, attempt: Attempt) => Promise<JobOutcome>;

export function createJobHandler(deps: WorkerDeps): JobHandler {
  // Rule 8: run folders and tokens never land in a repository.
  assertOutsideGit(deps.runsDir);
  assertOutsideGit(deps.tokens.dir);
  return async (data, attempt) => {
    const job = parseJob(data);
    if (job.kind === "documents") return processDocumentsJob(job.runId, deps, attempt);
    return job.kind === "run" ? processRunJob(job.runId, deps, attempt) : processConnectJob(job.linkId, deps, attempt);
  };
}
