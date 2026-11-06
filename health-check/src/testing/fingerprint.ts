import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

// What a run concluded, as one hash per file, so a change made for speed or
// memory can be shown to change nothing a customer would see (DECISIONS.md
// 341). The run's own times and ids are left out: they differ on every run.

/** The files compared, in a run folder. The sections and raw notes are what every deck is printed from. */
export const FINGERPRINTED = ["findings.json", "metrics.json", "scores.json", "manual-steps.json", "action-lists.json", "raw-notes.md", "report-sections.json", "layout-report.json"] as const;

/** Run ids and wall-clock times, which differ between two runs of the same org. */
const VOLATILE = /hc-\d{4}-\d{2}-\d{2}-[0-9a-f]{8}|"(startedAt|finishedAt|updatedAt|createdAt|madeAt|generatedAt)":\s*"[^"]*"/g;

export function fingerprint(dir: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const file of FINGERPRINTED) {
    const path = join(dir, file);
    if (!existsSync(path)) continue;
    out[file] = createHash("sha256").update(readFileSync(path, "utf8").replace(VOLATILE, "")).digest("hex").slice(0, 16);
  }
  return out;
}
