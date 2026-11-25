import { randomUUID } from "node:crypto";
import type { CheckedDigest } from "../digest/checked.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { checkNumbers, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { pageDataset } from "../render/page-dataset.ts";
import { asPrinted } from "../render/render-released.ts";
import { HELD_FOR_US_MASKS } from "../render/render-released.ts";
import type { RunDigest } from "../schemas/digest.ts";
import { editionIsWhy, unansweredIsWhy } from "../salesforce/refusal-words.ts";
import { canRelease, Release, ReportOutput, type Finding, type ReportSection } from "../schemas/index.ts";
import type { RunOverall } from "./types.ts";

// How an automatic run is released or held (DECISIONS.md 693): by code, with
// no person, and never a made-up approver or releaser. Rule 10 clause by
// clause: every section the customer could read passed its number check as
// written and as a page prints it, with a draft that failed fallen back to the
// engine's own wording; the digest the portal serves traced, masked or held,
// with the org's words marked (src/digest/checked.ts); and nothing held for us.
// Then the checks of the run as a whole that a person must look at: reads
// Salesforce didn't answer in a way that looks like an outage, most of the
// rules couldn't measure, or the score moved a long way since the last
// released run. Every check that fails raises its flag; the first, in
// HOLD_REASONS' order, is the run's one heldReason (DECISIONS.md 715).

/**
 * The words the console's HealthCheckRun."heldReason" and HealthCheckRunFlag."kind" take (migrations 20261211, 20261215,
 * 20261217), in the order they decide the one heldReason when several apply. What the customer could read comes first
 * (a section, the digest), so a person sees first what stops it being shown; then lost reads, which leave the report
 * honest but thin; then what a person judges of the whole run. reads_lost before mostly_unmeasured: an outage that
 * costs most of the checks is said as the outage, its cause, with mostly_unmeasured's flag beside it.
 */
export const HOLD_REASONS = ["section_failed", "no_digest", "digest_held", "reads_lost", "mostly_unmeasured", "score_moved"] as const;
export type HoldReason = (typeof HOLD_REASONS)[number];

/** The owner's answer of 23 November 2026: a move this large, with nothing changed in the org, waits for a person. */
export const SCORE_MOVE_POINTS = 15;

/** Counts only, never text: the flag's JSON is posted to staff and must hold nothing of the org's. */
export type FlagCounts = Record<string, number>;

/** A hold: the first check that failed is `reason`, with its counts; `also`, every later one that failed too, each a flag of its own. */
export type ReleaseDecision =
  | { kind: "release"; releases: Release[]; digest: RunDigest }
  | { kind: "hold"; reason: HoldReason; counts: FlagCounts; also: Array<{ kind: HoldReason; counts: FlagCounts }>; digest: RunDigest | null };

/** Whether a section's text passes the number check as stored and as the page prints it, against the set every output is checked with. */
function passes(report: BuiltReport, section: ReportSection, versionId: string | null, pseudonyms: Pseudonymiser, set: FigureSet): boolean {
  const built = report.outputs[section.output];
  const version = built?.versions.find((v) => v.id === versionId);
  if (!version || !version.numberCheck.passed) return false;
  return checkNumbers(asPrinted(version.body, pseudonyms, section.output), set).passed;
}

/**
 * Each section of an automatic run, checked: one whose current text passes is
 * `checked`, with no approver. A Claude draft that fails falls back to the
 * engine's own text for that section (its first version), which is checked
 * again and is `checked` only if it passes. One that still fails stays
 * `needs_input`, and the run is held. Changes the report in place, so the
 * versions saved are the ones checked. Returns how many fell back.
 */
export function checkSections(report: BuiltReport, pseudonyms: Pseudonymiser): { fellBack: number; failed: number } {
  const set = pageDataset(report.dataset, pseudonyms);
  let fellBack = 0;
  let failed = 0;
  for (const built of Object.values(report.outputs)) {
    built.sections = built.sections.map((section) => {
      if (passes(report, section, section.currentVersionId, pseudonyms, set)) return { ...section, status: "checked", approval: null };
      const engine = built.versions.find((v) => v.sectionId === section.id && v.source === "engine");
      if (engine && engine.id !== section.currentVersionId) {
        fellBack++;
        if (passes(report, section, engine.id, pseudonyms, set)) return { ...section, currentVersionId: engine.id, status: "checked", approval: null };
        failed++;
        return { ...section, currentVersionId: engine.id, status: "needs_input", approval: null };
      }
      failed++;
      return { ...section, status: "needs_input", approval: null };
    });
  }
  return { fellBack, failed };
}

/**
 * Rules that ran, and those of them whose every finding is not measurable.
 * `edition`, only when there are any: how many of those the org's edition
 * left unmeasured (DECISIONS.md 710). They still count: a report mostly
 * unmeasured is hollow whatever the cause, so a person sees it, and the
 * count tells them the edition is why.
 */
export function unmeasuredRules(findings: Finding[]): { rules: number; notMeasurable: number; edition?: number } {
  const all = new Map<string, boolean>();
  const edition = new Map<string, boolean>();
  for (const f of findings) {
    all.set(f.ruleId, (all.get(f.ruleId) ?? true) && f.status === "not_measurable");
    edition.set(f.ruleId, (edition.get(f.ruleId) ?? true) && f.status === "not_measurable" && editionIsWhy(f.notMeasurableReason ?? ""));
  }
  const byEdition = [...edition.values()].filter(Boolean).length;
  return { rules: all.size, notMeasurable: [...all.values()].filter(Boolean).length, ...(byEdition > 0 ? { edition: byEdition } : {}) };
}

/** Rules whose every finding is not measurable, with Salesforce not answering named in a reason: the checks lost reads cost (DECISIONS.md 715). */
export function unansweredRules(findings: Finding[]): number {
  const rules = new Map<string, { all: boolean; unanswered: boolean }>();
  for (const f of findings) {
    const was = rules.get(f.ruleId) ?? { all: true, unanswered: false };
    rules.set(f.ruleId, { all: was.all && f.status === "not_measurable", unanswered: was.unanswered || (f.status === "not_measurable" && unansweredIsWhy(f.notMeasurableReason ?? "")) });
  }
  return [...rules.values()].filter((r) => r.all && r.unanswered).length;
}

export type ReleaseInput = {
  runId: string;
  /** The sections as checkSections left them. */
  sections: ReportSection[];
  checked: CheckedDigest;
  findings: Finding[];
  overall: RunOverall | null;
  /** The overall score of the same org's last released run of the same product, or null with none. */
  previousScore: number | null;
  /**
   * Reads Salesforce didn't answer on the last delivery, in a way that looks
   * like an outage (run-job.ts lossLooksLikeOutage), or null with none: the
   * run is held `reads_lost` rather than released with those checks missing.
   */
  readsLost?: { lost: number; sameAsLast?: SameAsLast } | null;
  now: string;
};

/**
 * What one automatic run lost to Salesforce not answering, as the worker
 * stores it beside the run (DECISIONS.md 723): the sorted step ids that lost a
 * read, and whether any of those losses was a server fault or an unreadable
 * answer (SERVER_FAULT_CODES). Our own step ids and a boolean, never the org's
 * words. `{ steps: [], server: false }` is a run that lost nothing.
 */
export type RunLosses = { steps: string[]; server: boolean };

/** The codes that say Salesforce's own server failed, as lossLooksLikeOutage reads them (run-job.ts, DECISIONS.md 702). */
export const SERVER_FAULT_CODES: readonly string[] = ["Server", "BAD_SOAP_RESPONSE"];

/** A step id as the engine names its steps; anything else read back isn't one of ours. */
const STEP_ID = /^[A-Za-z0-9][A-Za-z0-9:._-]{0,99}$/;

/**
 * The run's losses from its refused reads, or null when they can't be told:
 * past `cap` kept reads, run.json keeps only one lost read more, so a step
 * that lost reads may be missing from the list (engine/run.ts keepRefused).
 */
export function lossesOf(readsRefused: ReadonlyArray<{ step: string; code: string; lost?: true }> | undefined, cap: number): RunLosses | null {
  const kept = readsRefused ?? [];
  if (kept.length >= cap) return null;
  const lost = kept.filter((r) => r.lost === true);
  return { steps: [...new Set(lost.map((r) => r.step))].sort(), server: lost.some((r) => SERVER_FAULT_CODES.includes(r.code)) };
}

/** Losses as stored, read back strictly: anything that isn't exactly what the worker writes is not known. */
export function readLosses(stored: unknown): RunLosses | null {
  const value = typeof stored === "string" ? (() => { try { return JSON.parse(stored) as unknown; } catch { return null; } })() : stored;
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const { steps, server } = value as { steps?: unknown; server?: unknown };
  if (!Array.isArray(steps) || typeof server !== "boolean" || !steps.every((s) => typeof s === "string" && STEP_ID.test(s))) return null;
  const sorted = [...new Set(steps as string[])].sort();
  return { steps: sorted, server };
}

/** 1: the org's previous run lost reads in the same steps, with no server fault in either; 0: they differ. */
export type SameAsLast = 0 | 1;

/**
 * Whether this run's losses repeat the org's previous run's (DECISIONS.md 723), for the person a reads_lost hold
 * goes to; it never releases anything. Undefined, so the flag carries no key, when there is no previous run, either
 * run's losses aren't known, or the steps are the same but a server fault in either says Salesforce may be why.
 */
export function sameAsLast(current: RunLosses | null, previous: RunLosses | null | undefined): SameAsLast | undefined {
  if (!current || !previous) return undefined;
  const same = current.steps.length === previous.steps.length && current.steps.every((s, i) => s === previous.steps[i]);
  if (!same) return 0;
  if (current.server || previous.server || current.steps.length === 0) return undefined;
  return 1;
}

/**
 * Release or hold. Every check is made and each that fails is a flag, so a
 * person sees all of them; the first in HOLD_REASONS' order is the hold.
 * Release rows name nobody.
 */
export function decideRelease(input: ReleaseInput): ReleaseDecision {
  const { sections, checked } = input;
  const failed: Array<{ kind: HoldReason; counts: FlagCounts }> = [];
  const fail = (kind: HoldReason, counts: FlagCounts): void => void failed.push({ kind, counts });
  const release = canRelease(sections, "automatic");
  if (!release.ok) fail("section_failed", { sections: sections.length, notChecked: sections.filter((s) => s.status !== "checked").length });
  if (!checked.digest) fail("no_digest", {});
  else if (checked.held) fail("digest_held", { masks: checked.masks, ceiling: HELD_FOR_US_MASKS, unmatched: checked.unmatched, unmarked: checked.unmarked });
  // The digest's own two counts ride with it, since the console shows a reads_lost run only when both are 0 (its
  // decision 713); with no digest there is nothing they could describe, so they are left out and Show stays refused.
  if (input.readsLost) {
    // sameAsLast only informs the person (DECISIONS.md 723): absent unless it is known.
    const { sameAsLast: same } = input.readsLost;
    fail("reads_lost", { lost: input.readsLost.lost, notMeasurable: unansweredRules(input.findings), ...(checked.digest ? { unmatched: checked.unmatched, unmarked: checked.unmarked } : {}), ...(same !== undefined ? { sameAsLast: same } : {}) });
  }
  const unmeasured = unmeasuredRules(input.findings);
  if (unmeasured.notMeasurable * 2 > unmeasured.rules) fail("mostly_unmeasured", unmeasured);
  if (input.overall && input.previousScore !== null && Math.abs(input.overall.score - input.previousScore) >= SCORE_MOVE_POINTS) {
    fail("score_moved", { score: input.overall.score, previous: input.previousScore, moved: Math.abs(input.overall.score - input.previousScore) });
  }
  if (failed.length > 0) {
    failed.sort((a, b) => HOLD_REASONS.indexOf(a.kind) - HOLD_REASONS.indexOf(b.kind));
    const [first, ...also] = failed;
    return { kind: "hold", reason: first!.kind, counts: first!.counts, also, digest: checked.digest };
  }
  // One row per output with sections, as the console's own release writes them.
  const releases = ReportOutput.options
    .map((output) => ({ output, sectionIds: sections.filter((s) => s.output === output).map((s) => s.id) }))
    .filter((r) => r.sectionIds.length > 0)
    .map((r) => Release.parse({ ...r, id: randomUUID(), runId: input.runId, kind: "automatic", releasedBy: null, releasedAt: input.now }));
  // No digest is no_digest, a hold, so one is here.
  return { kind: "release", releases, digest: checked.digest! };
}

// ─── An automatic customer's monthly pulse (DECISIONS.md 746) ────────────────
//
// No person reviews a pulse, and for an automatic customer no consultant is on
// the account either, so what it shows is released by code under the same
// checks as the customer's automatic check, as they apply to a pulse:
// - no_digest, digest_held: what the portal and the connector read of a pulse
//   is its digest and its result; a digest that can't be vouched for holds it.
// - reads_lost: an outage leaves a month's reading thin, and its "can't tell"s
//   would be shown as the org's state.
// - mostly_unmeasured: a reading mostly unmeasured is hollow, whatever the cause.
// - score_moved: against the pulse's own baseline (the score it shows "before"),
//   since a pulse is never "released" and the last released check may be a year old.
// - section_failed doesn't apply: a pulse's sections are never served or printed.
// - stopped_budget isn't a hold here: a pulse stopped at its budget saves no
//   result, so nothing of it is shown, as on every pulse.

/** The holds that apply to an automatic customer's pulse, in HOLD_REASONS' order. */
export const PULSE_HOLD_REASONS = ["no_digest", "digest_held", "reads_lost", "mostly_unmeasured", "score_moved"] as const satisfies readonly HoldReason[];
export type PulseHoldReason = (typeof PULSE_HOLD_REASONS)[number];

export type PulseDecision =
  | { kind: "release"; digest: RunDigest }
  | { kind: "hold"; reason: PulseHoldReason; counts: FlagCounts; also: Array<{ kind: PulseHoldReason; counts: FlagCounts }>; digest: RunDigest | null };

/** Release or hold an automatic customer's pulse: every check that fails is a flag, the first in order the hold. */
export function decidePulse(input: { checked: CheckedDigest; findings: Finding[]; score: { now: number | null; before: number | null }; readsLost?: { lost: number } | null }): PulseDecision {
  const { checked } = input;
  const failed: Array<{ kind: PulseHoldReason; counts: FlagCounts }> = [];
  if (!checked.digest) failed.push({ kind: "no_digest", counts: {} });
  else if (checked.held) failed.push({ kind: "digest_held", counts: { masks: checked.masks, ceiling: HELD_FOR_US_MASKS, unmatched: checked.unmatched, unmarked: checked.unmarked } });
  if (input.readsLost) failed.push({ kind: "reads_lost", counts: { lost: input.readsLost.lost, notMeasurable: unansweredRules(input.findings), ...(checked.digest ? { unmatched: checked.unmatched, unmarked: checked.unmarked } : {}) } });
  const unmeasured = unmeasuredRules(input.findings);
  if (unmeasured.notMeasurable * 2 > unmeasured.rules) failed.push({ kind: "mostly_unmeasured", counts: unmeasured });
  const { now, before } = input.score;
  if (now !== null && before !== null && Math.abs(now - before) >= SCORE_MOVE_POINTS) failed.push({ kind: "score_moved", counts: { score: now, previous: before, moved: Math.abs(now - before) } });
  if (failed.length > 0) {
    failed.sort((a, b) => PULSE_HOLD_REASONS.indexOf(a.kind) - PULSE_HOLD_REASONS.indexOf(b.kind));
    const [first, ...also] = failed;
    return { kind: "hold", reason: first!.kind, counts: first!.counts, also, digest: checked.digest };
  }
  return { kind: "release", digest: checked.digest! };
}

/**
 * The holds a person may Show a pulse past (DECISIONS.md 746): those whose result passed the digest's own check, so
 * what is shown is vouched for and only the person's judgement of the whole was missing. score_moved (is the move
 * real?), reads_lost (is the month's reading good enough despite the outage?, as a held check's Show allows, 715),
 * mostly_unmeasured (does this org simply share little?). Never no_digest or digest_held: their content failed the
 * check, so no result is kept for them and only Run again can follow.
 */
export const SHOWABLE_PULSE_HOLDS: readonly PulseHoldReason[] = ["reads_lost", "mostly_unmeasured", "score_moved"];
