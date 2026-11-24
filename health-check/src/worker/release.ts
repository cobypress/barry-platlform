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
  readsLost?: { lost: number } | null;
  now: string;
};

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
    fail("reads_lost", { lost: input.readsLost.lost, notMeasurable: unansweredRules(input.findings), ...(checked.digest ? { unmatched: checked.unmatched, unmarked: checked.unmarked } : {}) });
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
