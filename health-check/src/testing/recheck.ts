import { cpSync, existsSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import type { Finding, NumberOverride, Run } from "../schemas/index.ts";
import { STORAGE_RULE } from "../progress/compare.ts";
import { LIMITS_AT_START_FILE } from "../extract/types.ts";

// A re-check of the synthetic org, with its earlier health check in the folder
// beside it, for what the earlier run's corrections move on the progress page
// (DECISIONS.md 522, 528). The earlier run carries the three kinds of figure an
// override can move there: the storage reading, a sized amount, and a figure in
// a title the "Fixed" list names. No synthetic finding's title holds a fact
// (measured in session 24), so the title and its figure are planted, on a rule
// the synthetic org passes and the earlier run is made to fail.

export const EARLIER_AS_OF = "2026-07-14T12:00:00.000Z";
export const RECHECK_AS_OF = "2026-11-09T12:00:00.000Z";
/** Passed by the synthetic org, with no figure in its wording: an issue in the earlier run, so the re-check lists it as fixed. */
export const TITLED_RULE = "pipe.calibration-contradicts-org";
/** A sized amount the value model counts in full, so a correction moves the earlier worth. */
export const SIZED_RULE = "dq.open-opps-past-close";
/** Sized amounts that share their value with a fact, so a correction of either is refused (decision 511, item 3). */
export const SHARED_RULES = ["utl.dormant-licensed-users", "utl.never-logged-in"] as const;

const AT = "2026-07-20T12:00:00.000Z";
const readJson = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const writeJson = (dir: string, file: string, value: unknown): void => writeFileSync(join(dir, file), JSON.stringify(value));
const stamped = (run: Run, at: string): Run["steps"] => run.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt: at } : s));

/** Dates a planted folder's kept /limits reading, which a re-check's storage comparison goes by (DECISIONS.md 726), as its step is dated. */
export function stampLimitsReading(dir: string, at: string): void {
  const path = join(dir, LIMITS_AT_START_FILE);
  if (existsSync(path)) writeJson(dir, LIMITS_AT_START_FILE, { ...readJson<Record<string, unknown>>(dir, LIMITS_AT_START_FILE), readAt: at });
}

export type EarlierRows = {
  /** Each carried alone and together by applyOverrides on the earlier run. */
  storage: NumberOverride;
  sized: NumberOverride;
  titled: NumberOverride;
  /** Each refused. */
  refused: NumberOverride;
  refusedToo: NumberOverride;
};

/**
 * The recorded run copied to `earlier` as a re-check's earlier health check,
 * read 118 days before the planted re-check and with the planted title, and
 * the override rows. Writes no overrides.json: a test or a documents job does.
 */
export function plantEarlier(recorded: string, earlier: string): { earlierId: string; rows: EarlierRows } {
  const earlierId = basename(earlier);
  cpSync(recorded, earlier, { recursive: true });
  const run = readJson<Run>(recorded, "run.json");
  writeJson(earlier, "run.json", { ...run, id: earlierId, asOf: EARLIER_AS_OF, baselineRunId: null, steps: stamped(run, EARLIER_AS_OF) });
  stampLimitsReading(earlier, EARLIER_AS_OF);
  const findings = readJson<Finding[]>(recorded, "findings.json");
  const titled = findings.find((f) => f.ruleId === TITLED_RULE && f.status === "passed");
  const storage = findings.find((f) => f.ruleId === STORAGE_RULE)!;
  const sized = findings.find((f) => f.ruleId === SIZED_RULE && f.status === "issue");
  const [shared, sharedToo] = SHARED_RULES.map((rule) => findings.find((f) => f.ruleId === rule && f.status === "issue"));
  if (!titled || !sized || !shared || !sharedToo || [sized, shared, sharedToo].some((f) => f.value.kind === "not_sized")) throw new Error("The synthetic run's findings have changed");
  writeJson(earlier, "findings.json", findings.map((f): Finding => (f.id === titled.id ? { ...f, status: "issue", severity: "medium", title: `${f.title} (4,321 records)`, facts: { ...f.facts, records: 4321 } } : f)));
  const row = (id: string, f: Finding, field: string, previousValue: number, newValue: number): NumberOverride => ({ id, runId: earlierId, target: { type: "finding", id: f.id, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Reviewer", overriddenAt: AT });
  const amount = (f: Finding): number => (f.value.kind === "not_sized" ? NaN : f.value.amount);
  return {
    earlierId,
    rows: {
      storage: row("ov-0001", storage, "facts.DataStorageMBUsed", 8_940, 8_200),
      sized: row("ov-0002", sized, "value.amount", amount(sized), 250_000),
      titled: row("ov-0003", titled, "facts.records", 4_321, 4_123),
      refused: row("ov-0004", shared, "value.amount", amount(shared), 900),
      refusedToo: row("ov-0005", sharedToo, "value.amount", amount(sharedToo), 900),
    },
  };
}

/**
 * Copies the recorded run to `dir` as the re-check, its storage risen since,
 * and plants its earlier health check beside it as `<dir's name>-earlier`.
 */
export function plantRecheck(recorded: string, dir: string): { earlier: string; earlierId: string; rows: EarlierRows } {
  const earlier = join(dir, "..", `${basename(dir)}-earlier`);
  const { earlierId, rows } = plantEarlier(recorded, earlier);
  cpSync(recorded, dir, { recursive: true });
  const run = readJson<Run>(recorded, "run.json");
  writeJson(dir, "run.json", { ...run, baselineRunId: earlierId, asOf: RECHECK_AS_OF, steps: stamped(run, RECHECK_AS_OF) });
  stampLimitsReading(dir, RECHECK_AS_OF);
  const findings = readJson<Finding[]>(recorded, "findings.json");
  writeJson(
    dir,
    "findings.json",
    findings.map((f): Finding => (f.ruleId === STORAGE_RULE ? { ...f, facts: { DataStorageMBUsed: 9_410, DataStorageMBMax: 10_240, DataStorageMBShare: 91.9, FileStorageMBUsed: 5_000, FileStorageMBMax: 20_480, FileStorageMBShare: 24.4 } } : f)),
  );
  return { earlier, earlierId, rows };
}
