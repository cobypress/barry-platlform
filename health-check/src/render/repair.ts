import type { ReportInput } from "../narrative/sections.ts";
import { extractFigures } from "../numbercheck/index.ts";
import type { Finding, ManualStep, Metric } from "../schemas/index.ts";

// Decision 682, item 5. A figure no output can trace that the engine's own data
// holds is masked where it's written: that token, and nothing else, becomes
// MASK in the stored field it came from, before anything is built, so every
// output made from the report (the PDFs, the raw notes, the readout kit, the
// action lists, the tickets, the digest and the microsite) prints the mask.
// Nothing is removed: no sentence, row, segment or subject, and no value, count
// or order changes, so nothing beside the mask can come to mean something else.
// A consultant's or Claude's words are never masked here: those fall back whole
// (fallBackSections), and a person's words (the intake, the interviews, the
// survey) are never changed.

/** What a customer reads in place of a figure we couldn't trace. No figure, so the check never reads it as one. */
export const MASK = "(detail withheld)";

/** The one line a document with any mask carries, in its footer. */
export const withheldLine = (n: number): string => `A detail is withheld in ${n} ${n === 1 ? "place" : "places"} because we couldn't trace it to your data.`;

/** Where a figure was masked: the finding's rule, the metric or the manual step, and the field. Never the figure. */
export type SourceRepair = { raw: string; ruleId?: string; metricId?: string; stepId?: string; field: string };

const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * The figure exactly as the check read it, at its own boundaries, each time it's
 * written, in any script: a held "4" never matches in "4,321", "4.5", "4%", "£4",
 * "A4" or "café4". An identifier is matched in any case, as the check compares
 * it; anything else as written.
 */
export function heldPattern(raw: string): RegExp {
  const kind = extractFigures(raw).figures[0]?.kind;
  return new RegExp(`(?<![\\p{L}\\p{N}_£$€]|\\p{N}[.,])${escapeRegExp(raw)}(?![\\p{L}\\p{N}_%]|[.,]\\p{N})`, kind === "identifier" ? "giu" : "gu");
}

/** The text with each of these figures masked where it's written, and how many times. */
export function maskText(text: string, patterns: RegExp[]): { text: string; masked: number } {
  let masked = 0;
  let out = text;
  for (const pattern of patterns) {
    out = out.replace(pattern, () => {
      masked++;
      return MASK;
    });
  }
  return { text: out, masked };
}

type Hit = (field: string) => void;

function masker(patterns: RegExp[], hit: Hit) {
  return <T extends string | null | undefined>(value: T, field: string): T => {
    if (!value) return value;
    const { text, masked } = maskText(value, patterns);
    if (masked) hit(field);
    return text as T;
  };
}

function maskFinding(f: Finding, patterns: RegExp[], hit: Hit): Finding {
  const t = masker(patterns, hit);
  const value = f.value.kind === "not_sized" ? { ...f.value, reason: t(f.value.reason, "value.reason") } : { ...f.value, period: t(f.value.period, "value.period"), assumptions: f.value.assumptions.map((a) => t(a, "value.assumptions")) };
  return {
    ...f,
    title: t(f.title, "title"),
    summary: t(f.summary, "summary"),
    recommendation: t(f.recommendation, "recommendation"),
    notMeasurableReason: t(f.notMeasurableReason, "notMeasurableReason"),
    owner: t(f.owner, "owner"),
    effort: { ...f.effort, note: t(f.effort.note, "effort.note") },
    dependencies: f.dependencies.map((d) => t(d, "dependencies")),
    guards: f.guards.map((g) => ({ ...g, detail: t(g.detail, "guards.detail") })),
    evidence: f.evidence.map((e) => ({ ...e, note: t(e.note, "evidence.note") })),
    subjects: f.subjects.map((s) => ({ ...s, label: t(s.label, "subjects.label") })),
    value,
  } as Finding;
}

function maskMetric(m: Metric, patterns: RegExp[], hit: Hit): Metric {
  const t = masker(patterns, hit);
  const base = { ...m, label: t(m.label, "label"), definition: t(m.definition, "definition"), assumptions: m.assumptions.map((a) => t(a, "assumptions")), exclusions: m.exclusions.map((e) => t(e, "exclusions")) } as Metric;
  if (base.status !== "measured") return { ...base, reason: t(base.reason, "reason") } as Metric;
  return {
    ...base,
    // Keys and dimensions masked alike, so a lookup of one segment by another's key still finds it.
    segments: base.segments.map((s) => ({ ...s, key: t(s.key, "segments.key"), dimension: t(s.dimension, "segments.dimension"), notMeasurableReason: t(s.notMeasurableReason, "segments.notMeasurableReason") })),
    series: base.series.map((p) => ({ ...p, notMeasurableReason: t(p.notMeasurableReason, "series.notMeasurableReason") })),
  } as Metric;
}

function maskStep(s: ManualStep, patterns: RegExp[], hit: Hit): ManualStep {
  const t = masker(patterns, hit);
  return { ...s, title: t(s.title, "title"), whyItMatters: t(s.whyItMatters, "whyItMatters"), whereToGo: t(s.whereToGo, "whereToGo"), whatToCapture: t(s.whatToCapture, "whatToCapture"), ifSkipped: t(s.ifSkipped, "ifSkipped") } as ManualStep;
}

/** The run's own data with every one of these figures masked where the engine wrote it, and where each was. */
export function maskInput(input: ReportInput, raws: string[]): { input: ReportInput; repairs: SourceRepair[] } {
  if (raws.length === 0) return { input, repairs: [] };
  const repairs: SourceRepair[] = [];
  const each = <T>(rows: T[], mask: (row: T, patterns: RegExp[], hit: Hit) => T, where: (row: T) => Omit<SourceRepair, "raw" | "field">): T[] => {
    let out = rows;
    for (const raw of raws) {
      const patterns = [heldPattern(raw)];
      out = out.map((row) => mask(row, patterns, (field) => repairs.push({ raw, ...where(row), field })));
    }
    return out;
  };
  return {
    input: {
      ...input,
      findings: each(input.findings, maskFinding, (f) => ({ ruleId: f.ruleId })),
      metrics: each(input.metrics, maskMetric, (m) => ({ metricId: m.id })),
      manualSteps: each(input.manualSteps, maskStep, (s) => ({ stepId: s.id })),
    },
    repairs,
  };
}

/**
 * The last resort, for a figure no stored field holds (fixed text in a
 * template, a survey metric built inside the report, a name a token restored
 * to that reads as a figure, a date the page rewrote): each occurrence of it in
 * the document's visible text, at its own boundaries, masked. Styles and
 * scripts are left alone. Pages are laid out again in the browser, so a page
 * number can't be mapped back to the HTML: every occurrence in the document is
 * masked, and only that token.
 */
export function maskPage(html: string, raws: string[]): { html: string; masked: number } {
  if (raws.length === 0) return { html, masked: 0 };
  const patterns = raws.map((raw) => heldPattern(escapeXml(raw)));
  let masked = 0;
  const out = html.replace(/(<(style|script)\b[\s\S]*?<\/\2>)|>([^<]+)(?=<)/gi, (whole, kept: string | undefined, _tag, text: string | undefined) => {
    if (kept || text === undefined) return whole;
    const result = maskText(text, patterns);
    masked += result.masked;
    return `>${result.text}`;
  });
  return { html: out, masked };
}

const escapeXml = (text: string): string => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");

/** How many masks a document's HTML shows. */
export const masksIn = (html: string): number => html.split(MASK).length - 1;

/** The document's footer on its last page says how many details are withheld, once, when any are. */
export function withheldNote(html: string): string {
  const n = masksIn(html);
  if (n === 0) return html;
  const at = html.lastIndexOf("<span>Black Cloud Barometer");
  if (at < 0) return html;
  const end = at + "<span>Black Cloud Barometer".length;
  return `${html.slice(0, end)} | ${escapeXml(withheldLine(n))}${html.slice(end)}`;
}
