import { Calibration, isCurrencyCode } from "../schemas/calibration.ts";
import { NO_CURRENCY } from "../engine/currency.ts";
import type { OrgCurrency } from "./org-currency.ts";
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

/**
 * Why there is no calibration (DECISIONS.md 700): the hold is still the one word
 * calibration_unreadable, and its flag's counts say which, as numbers only.
 */
export type HoldCause = "stages_unread" | "no_won_stage" | "calibration_refused";
/** How many stages were read and how many of them are won: numbers only, for the hold's flag, so the console can word the cause. */
export type HoldCounts = { stages: number; won: number };

export type AutomaticCalibration =
  | { ok: true; calibration: Calibration }
  | { ok: false; heldReason: typeof CALIBRATION_UNREADABLE; cause: HoldCause; reason: string; counts: HoldCounts };

/** Why no calibration could be built, in plain words, naming no stage. */
export const NO_WON_STAGE = "No stage in the org is set up as closed and won, so we can't tell a won deal from any other, and the run wasn't started.";
export const STAGES_UNREAD = "The org's opportunity stages couldn't be read, so the run wasn't started.";
/** A calibration the schema refused: something in the org's stages we can't hold (DECISIONS.md 693). Names no stage. */
export const CALIBRATION_REFUSED = "The org's stages couldn't be made into a calibration, so the run wasn't started.";

const REASONS: Record<HoldCause, string> = { stages_unread: STAGES_UNREAD, no_won_stage: NO_WON_STAGE, calibration_refused: CALIBRATION_REFUSED };

/**
 * The calibration for an automatic full check, or why there is none. `stages`
 * null means they couldn't be read. `orgId` is the org that connected, `at`
 * when the stages were read, and `currency` what connect learnt of it
 * (DECISIONS.md 700): the org's own code, one currency not known by name, or
 * unread. A plain code is the org's own; null is unread. The currency never
 * holds a run: with no code the calibration keeps ISO 4217's "XXX" and says
 * which case it is, so the schema's pounds never apply. Only no stages, no won
 * stage, or a calibration the schema refuses holds it, and the hold says
 * which, with its counts. Never throws.
 *
 * `answered` is the currency the org's customer named in the portal (KAN-41).
 * It names the currency only where connect read that the org has one
 * currency (INVALID_FIELD, "single"): never where Salesforce gave the org's
 * own code (several currencies), and never where the read failed, since that
 * org may hold several and its amounts can't all be in the one named. The
 * locale's suggestion never stands in for it.
 *
 * A currency the org's settings named ("settings", DECISIONS.md 740) is
 * Salesforce's own answer, so it wins over the customer's: `answered` is then
 * not used (the caller logs a difference).
 */
export function automaticCalibration(stages: OrgStage[] | null, options: { orgId: string; at: string; currency: OrgCurrency | string | null; answered?: string | null }): AutomaticCalibration {
  const won = (stages ?? []).filter((s) => s.isClosed && s.isWon);
  const hold = (cause: HoldCause): AutomaticCalibration => ({ ok: false, heldReason: CALIBRATION_UNREADABLE, cause, reason: REASONS[cause], counts: { stages: stages?.length ?? 0, won: won.length } });
  if (!stages) return hold("stages_unread");
  if (won.length === 0) return hold("no_won_stage");
  const given: OrgCurrency = typeof options.currency === "string" ? { kind: "code", code: options.currency } : (options.currency ?? { kind: "unread" });
  // The settings' code was checked against the list where it was read; one that isn't on it here is one currency, unnamed.
  const currency: OrgCurrency = given.kind === "settings" && !isCurrencyCode(given.code) ? { kind: "single", suggested: null } : given;
  const answered = currency.kind === "single" && isCurrencyCode(options.answered) ? options.answered : null;
  // In sort order, ties by API name, whatever order they came in.
  const open = stages
    .filter((s) => s.isActive && !s.isClosed && !s.isWon && s.sortOrder !== null)
    .sort((a, b) => a.sortOrder! - b.sortOrder! || (a.apiName < b.apiName ? -1 : a.apiName > b.apiName ? 1 : 0));
  const parsed = Calibration.safeParse({
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
    // The org's own code is written as before 700, with no mark.
    ...(currency.kind === "code"
      ? { currency: currency.code }
      : currency.kind === "settings"
        ? { currency: currency.code, currencyFrom: "connect_api" }
        : answered
          ? { currency: answered, currencyFrom: "customer" }
          : { currency: NO_CURRENCY, currencyFrom: currency.kind === "single" ? "org" : "unread", ...(currency.kind === "single" && currency.suggested ? { currencySuggested: currency.suggested } : {}) }),
  });
  if (!parsed.success) return hold("calibration_refused");
  return { ok: true, calibration: parsed.data };
}

/** Whether the open stages' order came from Salesforce's sort order with no person's confirmation. */
export const orderFromOrg = (calibration: Pick<Calibration, "source" | "stages">): boolean =>
  calibration.source === "org_settings" && calibration.stages.some((s) => s.category === "open");
