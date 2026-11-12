import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import type { Finding, NumberOverride, Run } from "../schemas/index.ts";
import { STORAGE_RULE } from "../progress/compare.ts";

// A re-check of the synthetic org, with its earlier health check in the folder
// beside it, for what an earlier make's overrides.json moves on the progress
// page (DECISIONS.md 522). Storage is read 118 days apart, one rule is fixed in
// between, and the earlier run carries the three kinds of figure an override
// can move there: the storage reading, a sized amount, and a figure in a title
// the "Fixed" list names. No synthetic finding's title holds a fact (measured
// in session 24), so the title's figure is planted.

export const EARLIER_AS_OF = "2026-07-14T12:00:00.000Z";
export const RECHECK_AS_OF = "2026-11-09T12:00:00.000Z";
/** Not sized, one finding, and no figure in its wording, so a planted one is found once. Passed on the re-check. */
export const TITLED_RULE = "utl.eac-advanced-sync";
/** A sized amount the value model counts in full, so a correction moves the earlier worth. */
export const SIZED_RULE = "dq.open-opps-past-close";
/** A sized amount that shares its value with a fact, so a correction of it is refused (decision 511, item 3). */
export const SHARED_RULE = "utl.dormant-licensed-users";

const AT = "2026-07-20T12:00:00.000Z";
const readJson = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const writeJson = (dir: string, file: string, value: unknown): void => writeFileSync(join(dir, file), JSON.stringify(value));
const stamped = (run: Run, at: string): Run["steps"] => run.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt: at } : s));

export type Recheck = {
  earlier: string;
  earlierId: string;
  /** Each carried alone and together by applyOverrides on the earlier run, and the one it refuses. */
  rows: { storage: NumberOverride; sized: NumberOverride; titled: NumberOverride; refused: NumberOverride };
};

/**
 * Copies the recorded run to `dir` as the re-check and beside it as its earlier
 * health check, `<dir's name>-earlier`, and returns the override rows. Writes
 * no overrides.json: a test puts one there, or a documents job does.
 */
export function plantRecheck(recorded: string, dir: string): Recheck {
  const earlierId = `${basename(dir)}-earlier`;
  const earlier = join(dir, "..", earlierId);
  cpSync(recorded, dir, { recursive: true });
  cpSync(recorded, earlier, { recursive: true });
  const run = readJson<Run>(recorded, "run.json");
  writeJson(earlier, "run.json", { ...run, id: earlierId, asOf: EARLIER_AS_OF, baselineRunId: null, steps: stamped(run, EARLIER_AS_OF) });
  writeJson(dir, "run.json", { ...run, baselineRunId: earlierId, asOf: RECHECK_AS_OF, steps: stamped(run, RECHECK_AS_OF) });

  const findings = readJson<Finding[]>(recorded, "findings.json");
  const titled = findings.find((f) => f.ruleId === TITLED_RULE && f.status === "issue")!;
  const storage = findings.find((f) => f.ruleId === STORAGE_RULE)!;
  const sized = findings.find((f) => f.ruleId === SIZED_RULE && f.status === "issue")!;
  const shared = findings.find((f) => f.ruleId === SHARED_RULE && f.status === "issue")!;
  if (sized.value.kind === "not_sized" || shared.value.kind === "not_sized") throw new Error("The synthetic run's sized findings have changed");
  writeJson(earlier, "findings.json", findings.map((f): Finding => (f.id === titled.id ? { ...f, title: `${f.title} (4,321 records)`, facts: { ...f.facts, records: 4321 } } : f)));
  writeJson(
    dir,
    "findings.json",
    findings.map((f): Finding => {
      if (f.id === storage.id) return { ...f, facts: { DataStorageMBUsed: 9_410, DataStorageMBMax: 10_240, DataStorageMBShare: 91.9, FileStorageMBUsed: 5_000, FileStorageMBMax: 20_480, FileStorageMBShare: 24.4 } };
      if (f.id === titled.id) return { ...f, status: "passed", severity: "info", value: { kind: "not_sized", reason: "Nothing to size." }, segmentExemptReason: f.segmentExemptReason ?? "A single org-wide figure." };
      return f;
    }),
  );
  const row = (id: string, f: Finding, field: string, previousValue: number, newValue: number): NumberOverride => ({ id, runId: earlierId, target: { type: "finding", id: f.id, field }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: AT });
  return {
    earlier,
    earlierId,
    rows: {
      storage: row("ov-0001", storage, "facts.DataStorageMBUsed", 8_940, 8_200),
      sized: row("ov-0002", sized, "value.amount", sized.value.amount, 250_000),
      titled: row("ov-0003", titled, "facts.records", 4_321, 4_123),
      refused: row("ov-0004", shared, "value.amount", shared.value.amount, 900),
    },
  };
}
