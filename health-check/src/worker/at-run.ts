import { z } from "zod";
import { Timestamp } from "../schemas/common.ts";
import type { WorkerConfig } from "./env.ts";

// What each run records of the worker that ran it (DECISIONS.md 774): which of
// its feature switches were on, and which build it was. Only the host's owner
// can read the worker's log, so a switch set on the wrong service, misspelt or
// set to "On" was invisible from the console. Staff read this on the run's page.
//
// Never a raw value from the environment. Each switch is the parsed config's
// boolean, picked by name below, and whether its variable was there at all.
// The build is the host's commit, read and checked as hex in env.ts.

/** "set_not_on": the variable is there, and isn't exactly the word that turns it on ("On", "true", " on"). Its value is never kept. */
export const SwitchState = z.enum(["unset", "on", "set_not_on"]);
export type SwitchState = z.infer<typeof SwitchState>;

/**
 * The worker's boolean feature switches, and nothing else: no org list, key,
 * address, folder or model name. `on` reads the parsed config's boolean, so a
 * setting that isn't a boolean can't be added here without the type checker
 * refusing it. A test pins this exact list.
 */
export const WORKER_SWITCHES = [
  { key: "automatic", variable: "HC_AUTOMATIC", on: (c: WorkerConfig): boolean => c.automatic.on },
  { key: "documentsOnDemand", variable: "HC_DOCUMENTS_ON_DEMAND", on: (c: WorkerConfig): boolean => c.portalDocuments.on },
  { key: "handbookSummaries", variable: "HC_HANDBOOK_SUMMARIES", on: (c: WorkerConfig): boolean => c.apexSummaries },
  { key: "stageNames", variable: "HC_STAGE_NAMES", on: (c: WorkerConfig): boolean => c.stageNames },
  { key: "stageNamesModel", variable: "HC_STAGE_NAMES_MODEL", on: (c: WorkerConfig): boolean => c.stageNamesModel },
  { key: "stageNamesModelConsole", variable: "HC_STAGE_NAMES_MODEL_CONSOLE", on: (c: WorkerConfig): boolean => c.stageNamesModelConsole },
  { key: "areaReasons", variable: "HC_AREA_REASONS", on: (c: WorkerConfig): boolean => c.areaReasons },
  { key: "leadJourney", variable: "HC_LEAD_JOURNEY", on: (c: WorkerConfig): boolean => c.leadJourney },
  { key: "casePaths", variable: "HC_CASE_PATHS", on: (c: WorkerConfig): boolean => c.casePaths },
] as const;
export type SwitchKey = (typeof WORKER_SWITCHES)[number]["key"];

const Switch = z.object({ on: z.boolean(), state: SwitchState }).strict().refine((s) => s.on === (s.state === "on"), "on is true exactly when the state is on");
const switchShape = Object.fromEntries(WORKER_SWITCHES.map((s) => [s.key, Switch])) as Record<SwitchKey, typeof Switch>;

/** A commit as Render gives it (RENDER_GIT_COMMIT): hex, 7 to 40 characters, kept lower case. */
export const BUILD_COMMIT = /^[0-9a-f]{7,40}$/;

/** The "worker" JSON column on HealthCheckRun (the console's migration 20261221). */
export const WorkerAtRun = z
  .object({
    version: z.literal(1),
    /** When this delivery took the run: a run delivered again records the delivery that ran it last. */
    at: Timestamp,
    /** The commit the worker was built from, or null where the host doesn't say. */
    build: z.string().regex(BUILD_COMMIT).nullable(),
    switches: z.object(switchShape).strict(),
  })
  .strict();
export type WorkerAtRun = z.infer<typeof WorkerAtRun>;

/** Built once at start from the parsed config; `env` is asked only whether each switch's variable is there. */
export function workerAtRun(config: WorkerConfig, env: NodeJS.ProcessEnv): Omit<WorkerAtRun, "at"> {
  const switches = {} as Record<SwitchKey, { on: boolean; state: SwitchState }>;
  for (const s of WORKER_SWITCHES) {
    const on = s.on(config);
    // Empty counts as unset, as env.ts reads every other variable.
    const present = env[s.variable] !== undefined && env[s.variable] !== "";
    switches[s.key] = { on, state: on ? "on" : present ? "set_not_on" : "unset" };
  }
  return { version: 1, build: config.build, switches };
}

/** The row for one delivery, checked against the schema the console reads, so a row it can't read is never written. */
export const atRunRow = (worker: Omit<WorkerAtRun, "at">, at: string): WorkerAtRun => WorkerAtRun.parse({ ...worker, at });

/** What the worker says at start, so its log and the run's page agree. Names and states only. */
export function atRunStartLine(worker: Omit<WorkerAtRun, "at">): string {
  const set = WORKER_SWITCHES.filter((s) => worker.switches[s.key].state === "set_not_on").map((s) => s.variable);
  return (
    `Each run records this worker's build (${worker.build ? worker.build.slice(0, 7) : "not given by the host"}) and which of its switches are on, for staff on the run's page.` +
    (set.length ? ` Set but not on, as they aren't exactly the word that turns them on: ${set.join(", ")}.` : "")
  );
}
