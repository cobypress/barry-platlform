import { mkdtempSync, readdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../domains/index.ts";
import { usersExtractor, type UserRow } from "../extract/users.ts";
import { analyse, type AnalysisOutput } from "../engine/evaluate.ts";
import type { DomainModule } from "../domains/types.ts";
import { runHealthCheck, type RunOptions, type RunResult } from "../engine/run.ts";
import { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { Calibration, DateRange, ManualAnswer } from "../schemas/index.ts";
import { loadCalibration, loadRecording, ReplayTransport, syntheticProvider } from "./replay.ts";
import { AS_OF } from "./synthetic-org.ts";

export const tempDir = (): string => mkdtempSync(join(tmpdir(), "hc-test-"));

export const WINDOW: DateRange = { from: "2024-10-31", to: "2026-10-31" };

/** A full run from the recording, in a temp folder. */
export function runFromRecording(overrides: Partial<RunOptions> = {}): Promise<RunResult> {
  return runHealthCheck({
    provider: syntheticProvider,
    calibration: loadCalibration(),
    modules: SALES_CLOUD_MODULES,
    outDir: tempDir(),
    now: new Date(AS_OF),
    transport: new ReplayTransport(loadRecording()),
    ...overrides,
  });
}

/** Every checkpoint from a run, ready to be changed and analysed again without any network. */
export function loadExtracts(dir: string): Map<string, unknown> {
  const folder = join(dir, "checkpoints");
  const entries = readdirSync(folder).filter((f) => f.endsWith(".json")).map((f) => [f.replace(/\.json$/, ""), JSON.parse(readFileSync(join(folder, f), "utf8"))] as const);
  return new Map(entries);
}

export function analyseExtracts(extracts: Map<string, unknown>, options: { answers?: ManualAnswer[]; calibration?: Calibration; now?: Date; window?: DateRange; modules?: DomainModule[] } = {}): AnalysisOutput {
  const users = extracts.get(usersExtractor.id) as { status: string; data?: UserRow[] } | undefined;
  const directory = new Map((users?.data ?? []).map((u) => [u.Id, { name: u.Name, aliases: [u.Username, u.Email] }]));
  const pseudonyms = new Pseudonymiser();
  for (const [id, entry] of [...directory].sort(([a], [b]) => a.localeCompare(b))) pseudonyms.label("user", id, entry.name, entry.aliases);
  return analyse({
    modules: options.modules ?? SALES_CLOUD_MODULES,
    extracts,
    calibration: options.calibration ?? loadCalibration(),
    now: options.now ?? new Date(AS_OF),
    window: options.window ?? WINDOW,
    answers: options.answers ?? [],
    pseudonyms,
    userDirectory: directory,
  });
}

export const clone = <T>(value: T): T => structuredClone(value);
