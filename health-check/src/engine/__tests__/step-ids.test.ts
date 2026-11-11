import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { collectExtractors } from "../run.ts";
import { ALL_MODULES } from "../../domains/index.ts";

// The console shows a line of wording for each step a run is on, keyed by the
// step id: STEP_WORDING in blackcloud-next's lib/health-check/run-progress.ts.
// The two repos share no code, so nothing tells the console when a step is
// added here, and an id it has no wording for shows as "Checking <id>"
// (twenty-one did, decision 468). This list is the contract, word for word,
// and the console's test holds the same copy. When it changes, change
// STEP_WORDING and that test's list with it, and say so in the ledger's note
// for Coby, since the console ships separately.

const CONSOLE_FILE = "blackcloud-next lib/health-check/run-progress.ts (STEP_WORDING) and its test's copy of this list";

/** Every step a run can take, in run order: collectExtractors(ALL_MODULES). */
const STEP_IDS = [
  "org",
  "field-describe",
  "labels",
  "schema",
  "field-usage",
  "record-counts",
  "layouts",
  "custom-objects",
  "duplicate-rules",
  "setup-audit",
  "metadata",
  "apex",
  "component-changes",
  "opportunity-stages",
  "users",
  "opportunities",
  "report-definitions",
  "logins",
  "login-activity",
  "org-documentation",
  "access",
  "ai-signals",
  "scheduled-jobs",
  "classic",
  "api-limits",
  "licences",
  "ownership",
  "run-as",
  "reports",
  "activity-capture",
  "crm-health",
  "leads",
  "action-records",
  "duplicate-accounts",
  "prior-wins",
  "security-health-check",
  "privileged-permissions",
  "api-only",
  "async-jobs",
  "next-steps",
  "forecast-quotas",
  "forecast",
  "activity",
  "won-accounts",
  "service-setup",
  "service-cases",
  "service-activity",
  "service-escalation-rules",
  "cpq-setup",
  "cpq-quotes",
  "pardot-setup",
  "pardot-leads",
  "facts",
];

// An extractor no module lists never runs, so the console needn't word it.
// Each one goes here with why it isn't run; today there are none.
const NOT_RUN: Record<string, string> = {};

const EXTRACT_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "extract");

describe("the step ids the console words", () => {
  test("every step a run can take is on the list, in run order", () => {
    assert.deepEqual(
      collectExtractors(ALL_MODULES).map((e) => e.id),
      STEP_IDS,
      `The steps a run can take have changed. Change ${CONSOLE_FILE} to match, then this list, and say so in the ledger's note for Coby: a step with no wording shows as "Checking <id>".`,
    );
  });

  test("every extractor in src/extract is on the list, or named as never run", async () => {
    // A step written but not yet listed by any module would slip past the first
    // test, then show as "Checking <id>" the day a module lists it.
    const files = readdirSync(EXTRACT_DIR).filter((f) => f.endsWith(".ts"));
    const found = new Set<string>();
    const missing: string[] = [];
    for (const file of files) {
      const exports = (await import(join(EXTRACT_DIR, file))) as Record<string, unknown>;
      for (const [name, value] of Object.entries(exports)) {
        const id = typeof value === "object" && value !== null ? (value as { id?: unknown }).id : undefined;
        if (typeof id !== "string") continue;
        found.add(id);
        if (!STEP_IDS.includes(id) && !(id in NOT_RUN)) missing.push(`${id} (${name} in ${file})`);
      }
    }
    // Every step is exported from src/extract, so this shows the scan read the extractors and didn't pass on finding none.
    assert.deepEqual(STEP_IDS.filter((id) => !found.has(id)), [], `The scan of ${EXTRACT_DIR} missed steps the run takes.`);
    assert.deepEqual(
      missing,
      [],
      `These extractors aren't on the list. If a module runs them, add them there and to ${CONSOLE_FILE}, and say so in the ledger's note for Coby. If none does, name them in NOT_RUN with why.`,
    );
    // An exception that has since been listed would hide nothing, but would mislead the next reader.
    assert.deepEqual(Object.keys(NOT_RUN).filter((id) => STEP_IDS.includes(id)), [], "An id in NOT_RUN is run after all: take it out of NOT_RUN.");
  });
});
