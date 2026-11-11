import { readFileSync } from "node:fs";
import { join } from "node:path";
import { Run } from "../schemas/index.ts";

/**
 * A re-check's first health check, from the folder beside the run's own: the
 * one route to it, so every document that compares the two takes the same one
 * (DECISIONS.md 483). The id must be one we'd use as a folder name, so it can
 * never name a path, and the run must be the same org's, by the first 15
 * characters of its id. Null when none is named, or it's gone (retention), its
 * run.json can't be read, or it's another org's.
 */
export function baselineRun(dir: string, run: Pick<Run, "baselineRunId" | "orgId">): { dir: string; run: Run } | null {
  if (!run.baselineRunId || !/^[A-Za-z0-9_-]+$/.test(run.baselineRunId)) return null;
  const other = join(dir, "..", run.baselineRunId);
  try {
    const baseline = Run.parse(JSON.parse(readFileSync(join(other, "run.json"), "utf8")));
    return baseline.orgId.slice(0, 15) === run.orgId.slice(0, 15) ? { dir: other, run: baseline } : null;
  } catch {
    return null;
  }
}
