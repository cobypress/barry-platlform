import { buildReport, type BuiltReport, type ReportInput } from "../narrative/sections.ts";
import { buildDataset, checkNumbers, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { pageDataset } from "../render/page-dataset.ts";
import { heldPattern, MASK, maskInput } from "../render/repair.ts";
import { replaceMarked, type Marked } from "../fix/marked.ts";
import { HELD_FOR_US_MASKS } from "../render/render-released.ts";
import { RunDigest, type SentenceOrgWords } from "../schemas/digest.ts";
import type { Inventory } from "../schemas/inventory.ts";
import { buildDigest } from "./build.ts";

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

/**
 * The digest's ids, codes, hashes and dates, by their full path (DECISIONS.md
 * 693), never by a key's name alone: a free-text field added later under a name
 * such as "key", "from", "to", "group" or "kind" is text, checked and held to
 * its marks, until it is listed here on review. Within these, nothing is text.
 */
export const STRUCTURAL_PATHS: ReadonlySet<string> = new Set([
  "areas[].domain", "areas[].findingIds", "areas[].notMeasurableKind", "areas[].status", "asOf", "baselineRunId",
  "findings[].confidence", "findings[].domain", "findings[].effortSize", "findings[].fixComplexity", "findings[].id", "findings[].notMeasurableKind",
  "findings[].recommendationType", "findings[].ruleId", "findings[].severity", "findings[].status", "findings[].timeBand", "findings[].value.kind",
  "madeAt", "metrics[].dateRange.from", "metrics[].dateRange.to", "metrics[].domain", "metrics[].id", "metrics[].notMeasurableKind",
  "metrics[].status", "overall.areaDomains", "overall.grade", "overall.status", "plan.alsoFor", "plan.hoursTotal.findingIds",
  "plan.hoursTotal.notTotalledFindingIds", "plan.hoursTotal.partialFindingIds", "plan.hoursTotal.withoutHoursFindingIds", "plan.later", "plan.phases[].findingIds", "plan.phases[].from",
  "plan.phases[].key", "plan.phases[].to", "plan.quickWins", "product", "reviewed", "runId",
  "value.byKind.conversion_uplift.lineIds", "value.byKind.licence_waste.lineIds", "value.byKind.revenue_recoverable.lineIds", "value.byKind.time_cost.lineIds", "value.currency", "value.exposure.lineIds",
  "value.groups[].counted", "value.groups[].id", "value.groups[].kind", "value.groups[].lineIds", "value.leftOut", "value.lines[].group",
  "value.lines[].id", "value.lines[].kind", "value.lines[].ruleId", "value.total.lineIds", "version", "withheldNames",
]);

/** Every string the digest serves as text: everything outside its structural paths and its marks. */
export function digestTexts(value: unknown): string[] {
  return digestTextsAt(value).map((t) => t.text);
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

type Marks = SentenceOrgWords | null | undefined;
/** Where a text's marks are held in the digest: read them, and write new ones. */
type Slot = { get: (d: RunDigest, at: number[]) => Marks; set: (d: RunDigest, at: number[], marks: SentenceOrgWords) => void };

const OURS: SentenceOrgWords = { orgWordsMarked: true, orgWords: [] };
type AnyMarks = Record<string, unknown> | undefined;
const slotOf = <K extends string>(owner: (d: RunDigest, at: number[]) => { orgWords?: unknown } | undefined, key: K, item = false): Slot => ({
  get: (d, at) => {
    const held = (owner(d, at)?.orgWords as AnyMarks)?.[key];
    return (item ? (held as Marks[] | undefined)?.[at.at(-1)!] : held) as Marks;
  },
  set: (d, at, marks) => {
    const o = owner(d, at)! as { orgWords?: Record<string, unknown> };
    o.orgWords ??= {};
    if (!item) o.orgWords[key] = marks;
    else {
      const list = (o.orgWords[key] as Marks[] | undefined) ?? [];
      list[at.at(-1)!] = marks;
      o.orgWords[key] = list;
    }
  },
});
const finding = (d: RunDigest, at: number[]) => d.findings[at[0]!];
const metric = (d: RunDigest, at: number[]) => d.metrics[at[0]!];
const area = (d: RunDigest, at: number[]) => d.areas[at[0]!];

/**
 * Every text the digest serves, by its path with indices as [], and where its
 * marks are (DECISIONS.md 693). A text at a path not here has no marks, so it
 * is held: a field added to the digest is held until it's marked here, and a
 * test reads every string field of the schema and fails on one not listed.
 * `fixed` is a field whose schema allows only words of ours.
 */
export const TEXT_SLOTS: Readonly<Record<string, Slot | "fixed">> = {
  "findings[].title": slotOf(finding, "title"),
  "findings[].summary": slotOf(finding, "summary"),
  "findings[].recommendation": slotOf(finding, "recommendation"),
  "findings[].notMeasurableReason": slotOf(finding, "notMeasurableReason"),
  "findings[].value.reason": slotOf(finding, "valueReason"),
  "findings[].value.assumptions[]": slotOf(finding, "valueAssumptions", true),
  "findings[].value.unit": slotOf(finding, "valueUnit"),
  "findings[].value.period": slotOf(finding, "valuePeriod"),
  "findings[].effortHours.basis": slotOf(finding, "hoursBasis"),
  "findings[].fixPrompt": slotOf(finding, "fixPrompt"),
  "areas[].label": slotOf(area, "label"),
  "areas[].reason": slotOf(area, "reason"),
  "metrics[].label": slotOf(metric, "label"),
  "metrics[].definition": slotOf(metric, "definition"),
  "metrics[].reason": slotOf(metric, "reason"),
  "metrics[].assumptions[]": slotOf(metric, "assumptions", true),
  "metrics[].exclusions[]": slotOf(metric, "exclusions", true),
  "metrics[].unit": "fixed",
  "value.groups[].label": slotOf((d, at) => d.value.groups[at[0]!], "label"),
  "value.currencyNote": slotOf((d) => d.value, "currencyNote"),
  "value.currencySuggested": slotOf((d) => d.value, "currencySuggested"),
  "editionNote": slotOf((d) => d, "editionNote"),
  "plan.phases[].title": slotOf((d, at) => d.plan.phases[at[0]!], "title"),
  "plan.hoursTotal.note": slotOf((d) => d.plan.hoursTotal, "note"),
  "overall.reason": slotOf((d) => (d.overall.status === "not_measurable" ? d.overall : undefined), "reason"),
  "overall.caveat": slotOf((d) => (d.overall.status === "measured" ? d.overall : undefined), "caveat"),
};

export type DigestText = { path: string; at: number[]; text: string };

/** Every text the digest serves, as digestTexts reads them, with where each is. */
export function digestTextsAt(value: unknown, path = "", at: number[] = [], key = ""): DigestText[] {
  if (STRUCTURAL_PATHS.has(path) || key === "orgWords") return [];
  if (typeof value === "string") return [{ path, at, text: value }];
  if (Array.isArray(value)) return value.flatMap((v, i) => digestTextsAt(v, `${path}[]`, [...at, i], key));
  if (value && typeof value === "object") return Object.entries(value).flatMap(([k, v]) => digestTextsAt(v, path ? `${path}.${k}` : k, at, k));
  return [];
}

/** A text's marks, or null when it has none it can be served with. An empty text needs none. */
export function marksOf(d: RunDigest, t: DigestText): SentenceOrgWords | null {
  if (t.text === "") return OURS;
  const slot = TEXT_SLOTS[t.path];
  if (slot === "fixed") return OURS;
  return slot?.get(d, t.at) ?? null;
}

const METRIC_TEXT = /^metrics\[\]\./;

/**
 * Every text that lost its marks to a mask (682: they no longer fit its hash)
 * marked again, from the same text before the mask, with each mask placed as
 * fixed words of ours, the way the fix prompt carries its marks (672): a mask
 * that would cut through the org's words leaves the text unknown. Only where
 * the result is exactly the masked text.
 */
function remarked(before: RunDigest, after: RunDigest, raws: string[]): RunDigest {
  const out = structuredClone(after);
  const was = new Map(digestTextsAt(before).map((t) => [`${t.path}@${t.at.join(",")}`, t]));
  for (const t of digestTextsAt(out)) {
    const slot = TEXT_SLOTS[t.path];
    const old = was.get(`${t.path}@${t.at.join(",")}`);
    if (!slot || slot === "fixed" || !old || old.text === t.text || slot.get(out, t.at)) continue;
    const held = slot.get(before, old.at);
    if (!held) continue;
    let m: Marked = { text: old.text, spans: held.orgWords };
    for (const raw of raws) m = replaceMarked(m, heldPattern(raw), MASK, true);
    if (m.spans && m.text === t.text) slot.set(out, t.at, { orgWordsMarked: true, orgWords: m.spans });
  }
  const parsed = RunDigest.safeParse(out);
  return parsed.success ? parsed.data : after;
}

/**
 * Rule 10's marks, enforced (DECISIONS.md 693). A metric's text with no marks
 * is withheld whole, as "(detail withheld)", and counted with the masks; any
 * other text with none is counted as unmarked, and the digest is held.
 */
export function enforceMarks(digest: RunDigest): { digest: RunDigest; withheld: number; dropped: number; unmarked: number } {
  const out = structuredClone(digest);
  let withheld = 0;
  let unmarked = 0;
  // A metric whose label can't be marked goes whole: a value beside "(detail withheld)" would say nothing true.
  const dropped = new Set<number>();
  for (const t of digestTextsAt(digest)) {
    if (marksOf(digest, t)) continue;
    const slot = TEXT_SLOTS[t.path];
    if (t.path === "metrics[].label") dropped.add(t.at[0]!);
    else if (METRIC_TEXT.test(t.path) && slot && slot !== "fixed") {
      setText(out, t, MASK);
      slot.set(out, t.at, OURS);
      withheld++;
    } else unmarked++;
  }
  out.metrics = out.metrics.filter((_m, i) => !dropped.has(i));
  const parsed = RunDigest.safeParse(out);
  return parsed.success ? { digest: parsed.data, withheld, dropped: dropped.size, unmarked } : { digest, withheld: 0, dropped: 0, unmarked: unmarked + withheld + dropped.size };
}

/** Puts a text in place by its path. */
function setText(d: RunDigest, t: DigestText, text: string): void {
  const keys = t.path.split(".");
  let node: unknown = d;
  let i = 0;
  keys.forEach((k, n) => {
    const name = k.replace(/\[\]/g, "");
    const arrays = (k.match(/\[\]/g) ?? []).length;
    const last = n === keys.length - 1;
    if (last && arrays === 0) return void ((node as Record<string, unknown>)[name] = text);
    node = (node as Record<string, unknown>)[name];
    for (let a = 0; a < arrays; a++) {
      const idx = t.at[i++]!;
      if (last && a === arrays - 1) return void ((node as unknown[])[idx] = text);
      node = (node as unknown[])[idx];
    }
  });
}

/** A text the digest serves with no marks, after the metrics' are withheld: unknown, so the portal couldn't set the org's words apart. */
export function unmarkedSentences(digest: RunDigest): number {
  return digestTextsAt(digest).filter((t) => !marksOf(digest, t)).length;
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
  // A metric's text that can't be marked is withheld, and counted with the masks; any other is held.
  const enforced = enforceMarks(digest);
  digest = enforced.digest;
  // Each metric left out whole counts as one mask, as each "(detail withheld)" does.
  const masks = masksIn(digest) + enforced.dropped;
  const unmarked = enforced.unmarked;
  const held = unmatched > 0 || masks > HELD_FOR_US_MASKS || unmarked > 0 ? "digest_held" : null;
  return { digest, masks, unmatched, unmarked, held };
}
