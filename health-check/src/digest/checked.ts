import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import { buildDataset, checkNumbers, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { pageDataset } from "../render/page-dataset.ts";
import { heldPattern, MASK, maskInput } from "../render/repair.ts";
import { replaceMarked, type Marked } from "../fix/marked.ts";
import { HELD_FOR_US_MASKS } from "../render/render-released.ts";
import { RunDigest } from "../schemas/digest.ts";
import type { Inventory } from "../schemas/inventory.ts";
import { buildDigest, ID_MAPS, STRUCTURAL_KEYS } from "./build.ts";

// An automatic run's digest, checked before the run is released (DECISIONS.md
// 693). The portal shows the digest and nothing else of an automatic run, and
// no person reads it first, so rule 10 is held here, by code: every figure in
// every text the digest serves traces to the run's data, through the same set
// every customer output is checked against (pageDataset, 682); a figure that
// doesn't is withheld where the run's own data writes it (maskInput, 682) and
// the digest made again from the masked data; and every sentence a finding
// serves carries its org-word marks (655), so the portal can set the customer's
// words apart from ours. Past the held-for-us ceiling, with a figure still
// untraced, or with a sentence unmarked, the digest is held for a person.

/** Why a checked digest can't be released. The console's HealthCheckRun."heldReason" words (migration 20261211). */
export type DigestHold = "no_digest" | "digest_held";

export type CheckedDigest = {
  /** The digest as it would be served, masks in it; null when none could be made. */
  digest: RunDigest | null;
  /** Each "(detail withheld)" in the digest's texts. */
  masks: number;
  /** Figures still untraced after masking. */
  unmatched: number;
  /** A finding's sentence served without its org-word marks. */
  unmarked: number;
  /** Null when the digest may be released. */
  held: DigestHold | null;
};

export type CheckedDigestFor = {
  /** The run's data as loaded, before any mask. */
  input: ReportInput;
  pseudonyms: Pseudonymiser;
  inventory: Inventory | null;
  madeAt: string;
  /** How a report is built from the data. buildReport unless a test says otherwise. */
  build?: (input: ReportInput) => BuiltReport;
};

/** Every string the digest serves as text: everything outside its ids, codes, hashes and dates, as guardDigest reads it. */
export function digestTexts(value: unknown, key = ""): string[] {
  if (STRUCTURAL_KEYS.has(key) || ID_MAPS.has(key) || key === "orgWords") return [];
  if (typeof value === "string") return [value];
  if (Array.isArray(value)) return value.flatMap((v) => digestTexts(v, key));
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => digestTexts(v, k));
  return [];
}

/** The raw of every figure in the digest's texts that doesn't trace to the set, each once. */
export function untraced(digest: RunDigest, set: FigureSet): string[] {
  return [...new Set(digestTexts(digest).flatMap((text) => checkNumbers(text, set).unmatched.map((u) => u.raw)))];
}

/**
 * The set the digest is checked against: every output's (pageDataset), and the
 * two texts of a finding only the digest serves, each with its finding as the
 * source, as a finding's own summary is in the report's set: the hours' basis
 * the fix table wrote (636) and the fix prompt (628). Both are the engine's,
 * made by code from the finding, so they hold its figures by construction.
 */
export function digestDataset(report: BuiltReport, pseudonyms: Pseudonymiser): FigureSet {
  const set = pageDataset(report.dataset, pseudonyms);
  const extraText = report.teamFindings.flatMap((f) => [...(f.effortHours ? [{ source: f.id, text: f.effortHours.basis }] : []), ...(f.fixPrompt ? [{ source: f.id, text: f.fixPrompt }] : [])]);
  const sources = new Set(extraText.map((t) => t.source));
  const extra = buildDataset({ findings: [], metrics: [], extraText });
  const mine = <T extends { source: string }>(rows: T[]): T[] => rows.filter((r) => sources.has(r.source));
  return {
    figures: [...set.figures, ...mine(extra.figures)],
    dates: [...set.dates, ...mine(extra.dates)],
    identifiers: [...set.identifiers, ...mine(extra.identifiers)],
    tokens: [...set.tokens, ...mine(extra.tokens)],
    labels: [...set.labels, ...mine(extra.labels)],
  };
}

const MARKED = ["title", "summary", "recommendation", "notMeasurableReason"] as const;

/** A finding's sentences the digest serves with no marks: unknown, so the portal couldn't set the org's words apart. Empty text needs none. */
export function unmarkedSentences(digest: RunDigest): number {
  return digest.findings.reduce((n, f) => n + MARKED.filter((k) => typeof f[k] === "string" && f[k] !== "" && !f.orgWords?.[k]).length, 0);
}

/**
 * A finding's sentence a mask changed loses its marks (682: they no longer fit
 * its hash). Each is marked again here, from the sentence as it was, with each
 * mask placed as fixed words of ours, the way the fix prompt carries its marks
 * (672): a mask that would cut through the org's words leaves the sentence
 * unknown, and so held. Only where the result is exactly the masked sentence.
 */
function remarked(before: RunDigest, after: RunDigest, raws: string[]): RunDigest {
  const was = new Map(before.findings.map((f) => [f.id, f]));
  const findings = after.findings.map((f) => {
    const old = was.get(f.id);
    if (!old) return f;
    const marks = { ...f.orgWords };
    for (const key of MARKED) {
      const [then, now, held] = [old[key], f[key], old.orgWords?.[key]];
      if (marks[key] || !held || typeof then !== "string" || typeof now !== "string" || then === now) continue;
      let m: Marked = { text: then, spans: held.orgWords };
      for (const raw of raws) m = replaceMarked(m, heldPattern(raw), MASK, true);
      if (m.spans && m.text === now) marks[key] = { orgWordsMarked: true, orgWords: m.spans };
    }
    return Object.keys(marks).length > 0 ? { ...f, orgWords: marks } : f;
  });
  const parsed = RunDigest.safeParse({ ...after, findings });
  return parsed.success ? parsed.data : after;
}

const masksIn = (digest: RunDigest): number => digestTexts(digest).reduce((n, text) => n + text.split(MASK).length - 1, 0);

export function checkedDigest({ input, pseudonyms, inventory, madeAt, build = buildReport }: CheckedDigestFor): CheckedDigest {
  const none: CheckedDigest = { digest: null, masks: 0, unmatched: 0, unmarked: 0, held: "no_digest" };
  let report: BuiltReport;
  let digest: RunDigest;
  try {
    report = build(input);
    digest = buildDigest({ report, pseudonyms, inventory, madeAt });
  } catch {
    return none;
  }
  // The set the data as stored gives, so a mask never makes another figure untraceable (682).
  const set = digestDataset(report, pseudonyms);
  const raws = untraced(digest, set);
  if (raws.length > 0) {
    try {
      const masked = build(maskInput(input, raws).input);
      digest = remarked(digest, buildDigest({ report: masked, pseudonyms, inventory, madeAt }), raws);
    } catch {
      return none;
    }
  }
  const unmatched = untraced(digest, set).length;
  const masks = masksIn(digest);
  const unmarked = unmarkedSentences(digest);
  const held = unmatched > 0 || masks > HELD_FOR_US_MASKS || unmarked > 0 ? "digest_held" : null;
  return { digest, masks, unmatched, unmarked, held };
}
