import { Calibration } from "../schemas/calibration.ts";
import type { OrgStage } from "./org-stages.ts";

// An automatic run's calibration, built at connect from the org's opportunity
// stages and nothing else (DECISIONS.md 692). No person confirms it, so it
// holds only what Salesforce's own flags say, and the design's conservative
// defaults for the rest: every check that needs a missing input says not
// measurable, with the reason, as it does today (rule 5).
//
// - A stage that is closed and won is named won.
// - An active stage that isn't closed, and has a sort order, is named open, in
//   Salesforce's sort order (the owner's answer of 23 November 2026). That
//   order is unconfirmed: the source says so, and every check that reads the
//   order says so too, at no more than medium confidence (cohort.ts).
// - A stage that is closed and not won is left out, so the run reads it as
//   lost from the flags (598) and nothing that splits lost from no decision is
//   measured from it (600). "No decision" is never inferred.
// - An inactive open stage, or one with no sort order, is left out too: its
//   place in the order would be a guess, so a check that needs it stays not
//   measurable, as for any stage the calibration doesn't name.
// Mirrors the console's calibrationStageRows with nothing saved
// (lib/health-check/org-stages.ts), which this replaces for an automatic run.

/** Who set it up, in the field the schema requires. Never printed as a person: `calibrationConfirmation` reads the source. */
export const ORG_SETTINGS_CALIBRATOR = "Read from Salesforce";

/** The run's hold reason, as the console's HealthCheckRun."heldReason" takes it (migration 20261211). */
export const CALIBRATION_UNREADABLE = "calibration_unreadable";

export type AutomaticCalibration = { ok: true; calibration: Calibration } | { ok: false; heldReason: typeof CALIBRATION_UNREADABLE; reason: string };

/** Why no calibration could be built, in plain words, naming no stage. */
export const NO_WON_STAGE = "No stage in the org is set up as closed and won, so we can't tell a won deal from any other, and the run wasn't started.";
export const STAGES_UNREAD = "The org's opportunity stages couldn't be read, so the run wasn't started.";

/**
 * The calibration for an automatic full check, or why there is none. `stages`
 * null means they couldn't be read. `orgId` is the org that connected, and
 * `at` when the stages were read.
 */
export function automaticCalibration(stages: OrgStage[] | null, options: { orgId: string; at: string }): AutomaticCalibration {
  if (!stages) return { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: STAGES_UNREAD };
  const won = stages.filter((s) => s.isClosed && s.isWon);
  if (won.length === 0) return { ok: false, heldReason: CALIBRATION_UNREADABLE, reason: NO_WON_STAGE };
  // In sort order, ties by API name, whatever order they came in.
  const open = stages
    .filter((s) => s.isActive && !s.isClosed && !s.isWon && s.sortOrder !== null)
    .sort((a, b) => a.sortOrder! - b.sortOrder! || (a.apiName < b.apiName ? -1 : a.apiName > b.apiName ? 1 : 0));
  const calibration = Calibration.parse({
    version: 1,
    orgId: options.orgId,
    confirmedBy: ORG_SETTINGS_CALIBRATOR,
    confirmedAt: options.at,
    source: "org_settings",
    stages: [...open.map((s, i) => ({ name: s.apiName, category: "open", order: i + 1 })), ...won.map((s) => ({ name: s.apiName, category: "won" }))],
    scope: { recordTypes: "all", businessLines: [] },
    users: [],
    fields: {},
    targets: { source: "none" },
    licenceCosts: {},
    effortMinutes: {},
  });
  return { ok: true, calibration };
}

/** Whether the open stages' order came from Salesforce's sort order with no person's confirmation. */
export const orderFromOrg = (calibration: Pick<Calibration, "source" | "stages">): boolean =>
  calibration.source === "org_settings" && calibration.stages.some((s) => s.category === "open");
