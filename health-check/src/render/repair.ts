import type { ReportInput } from "../narrative/sections.ts";
import { extractFigures } from "../numbercheck/index.ts";
import type { Finding, ManualStep, Metric } from "../schemas/index.ts";

// Decision 682, item 4. A figure no output can trace that the engine's own data
// holds is masked where it's written: that token, and nothing else, becomes
// MASK in each stored field that prints it, before anything is built, so every
// output made from the report (the PDFs, the raw notes, the readout kit, the
// action lists, the tickets, the digest and the microsite) shows the mask.
// Nothing is removed: no sentence, row, segment or subject, and no value, count
// or order changes. A consultant's or Claude's words are never masked here:
// those fall back whole (fallBackSections), and a person's words (the intake,
// the interviews, the survey) are never changed.

/** What a customer reads in place of a figure we couldn't trace. No figure, so the check never reads it as one. */
export const MASK = "(detail withheld)";

/** The one line a document with any mask carries, in its footer. */
export const withheldLine = (n: number): string => `A detail is withheld in ${n} ${n === 1 ? "place" : "places"} because we couldn't trace it to your data.`;

/** Where the line goes when a document has no footer to carry it: no count, since a count there would be a figure to trace. */
export const WITHHELD_NOTE = "Some details are withheld because we couldn't trace them to your data.";

/** Where a figure was masked: the finding's rule, the metric or the manual step, and the field. Never the figure. */
export type SourceRepair = { raw: string; ruleId?: string; metricId?: string; stepId?: string; field: string };

/**
 * Every string field of what's stored, by path ("[]" for each item of a list),
 * either masked or never printed as prose (ids, enums, dates, hashes, queries,
 * who answered). A field the stored data has that is on neither list fails
 * src/render/__tests__/held-repair.test.ts until it is put on one.
 */
export const FIELDS = {
  finding: {
    masked: ["dependencies[]", "effort.note", "effortHours.basis", "evidence[].exclusions[]", "evidence[].note", "fixPrompt", "guards[].detail", "guards[].evidence[].note", "guards[].evidence[].exclusions[]", "guards[].subject", "notMeasurableReason", "owner", "recommendation", "segmentExemptReason", "segmentsChecked[].dimension", "segmentsChecked[].note", "subjects[].label", "summary", "title", "value.assumptions[]", "value.period", "value.reason"],
    unmasked: ["confidence", "domain", "effort.size", "effortHours.partial", "fixComplexity", "id", "manualStepIds[]", "notMeasurableKind", "recommendationType", "ruleId", "severity", "status", "subjects[].type", "timeBand", "value.kind", "value.unit", "guards[].guardId", "guards[].status", ...evidence("evidence[]"), ...evidence("guards[].evidence[]")],
  },
  metric: {
    masked: ["assumptions[]", "definition", "evidence[].exclusions[]", "evidence[].note", "exclusions[]", "label", "reason", "segmentExemptReason", "segments[].dimension", "segments[].key", "segments[].notMeasurableReason", "series[].notMeasurableReason"],
    unmasked: ["dateRange.from", "dateRange.to", "domain", "id", "notMeasurableKind", "series[].date", "status", "unit", ...evidence("evidence[]")],
  },
  step: {
    masked: ["fields[].label", "ifSkipped", "title", "whatToCapture", "whereToGo", "whyItMatters"],
    unmasked: ["answerMethods[]", "domain", "fields[].key", "fields[].type", "id", "key", "raisedBy", "ruleIds[]", "status"],
  },
} as const;

function evidence(at: string): string[] {
  return ["answeredAt", "answeredBy", "api", "capturedAt", "dateRange.from", "dateRange.to", "kind", "manualStepId", "path", "query", "status"].map((k) => `${at}.${k}`);
}

/** The org-word marks are hashes of the text they were made for: never prose, and a masked sentence no longer matches its hash (decision 655). */
export const UNMASKED_PREFIXES = ["orgWords."];

const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * The figure exactly as the check read it, at its own boundaries, each time it's
 * written, in any script: a held "4" never matches in "4,321", "4.5", "4%", "£4",
 * "A4" or "café4". An identifier is matched in any case, as the check compares
 * it, but only whole: never as part of a longer word the check would read as
 * one ("Ai1" isn't in "Ai1-beta"); anything else as written.
 */
export function heldPattern(raw: string): RegExp {
  const identifier = extractFigures(raw).figures[0]?.kind === "identifier";
  const before = identifier ? String.raw`(?<![\p{L}\p{N}_£$€]|[\p{L}\p{N}][._-]|\p{N}[.,])` : String.raw`(?<![\p{L}\p{N}_£$€]|\p{N}[.,])`;
  const after = identifier ? String.raw`(?![\p{L}\p{N}_%]|[._-][\p{L}\p{N}]|[.,]\p{N})` : String.raw`(?![\p{L}\p{N}_%]|[.,]\p{N})`;
  return new RegExp(`${before}${escapeRegExp(raw)}${after}`, identifier ? "giu" : "gu");
}

/** A web address or an email, which a mask never goes inside. */
const LINK_OR_EMAIL = /(?:https?:\/\/|www\.)[^\s<>"')]+|[\p{L}\p{N}._%+-]+@[\p{L}\p{N}-]+(?:\.[\p{L}\p{N}-]+)+/gu;

/** The text with each of these figures masked where it's written, and how many times. Never inside a link or an email. */
export function maskText(text: string, patterns: RegExp[], mask = MASK): { text: string; masked: number } {
  const links = [...text.matchAll(LINK_OR_EMAIL)].map((m) => [m.index, m.index + m[0].length] as const);
  const inLink = (at: number, end: number): boolean => links.some(([a, b]) => at < b && end > a);
  let masked = 0;
  let out = text;
  for (const pattern of patterns) {
    const spans = [...out.matchAll(pattern)].filter((m) => !inLink(m.index, m.index + m[0].length));
    if (spans.length === 0) continue;
    // Links are found in the text as it was; a mask changes lengths only after the last one is placed.
    let rebuilt = "";
    let from = 0;
    for (const m of spans) {
      rebuilt += out.slice(from, m.index) + mask;
      from = m.index + m[0].length;
      masked++;
    }
    out = rebuilt + out.slice(from);
    links.splice(0, links.length, ...[...out.matchAll(LINK_OR_EMAIL)].map((m) => [m.index, m.index + m[0].length] as const));
  }
  return { text: out, masked };
}

type Hit = (field: string) => void;

/** Each string at a masked path, masked; everything else as it was. */
function maskAt(value: unknown, path: string, masked: ReadonlySet<string>, patterns: RegExp[], hit: Hit): unknown {
  if (typeof value === "string") {
    if (!masked.has(path)) return value;
    const result = maskText(value, patterns);
    if (result.masked) hit(path);
    return result.text;
  }
  if (Array.isArray(value)) return value.map((item) => maskAt(item, `${path}[]`, masked, patterns, hit));
  if (value && typeof value === "object") {
    if (UNMASKED_PREFIXES.some((p) => `${path}.`.startsWith(p))) return value;
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, maskAt(v, path ? `${path}.${k}` : k, masked, patterns, hit)]));
  }
  return value;
}

const MASKED = { finding: new Set<string>(FIELDS.finding.masked), metric: new Set<string>(FIELDS.metric.masked), step: new Set<string>(FIELDS.step.masked) };

function maskFinding(f: Finding, patterns: RegExp[], hit: Hit): Finding {
  let any = false;
  const out = maskAt(f, "", MASKED.finding, patterns, (field) => {
    any = true;
    hit(field);
  }) as Finding;
  // The fix prompt was made from the finding's words as read (DECISIONS.md 628) and can't be made
  // again here, so a masked finding has none, as a correction that changes its text has none (overrides.ts).
  if (any) {
    delete out.fixPrompt;
    if (out.orgWords) {
      const { fixPrompt: _dropped, ...rest } = out.orgWords as Record<string, unknown>;
      out.orgWords = rest as Finding["orgWords"];
    }
  }
  return out;
}

/** "(detail withheld A)", "(detail withheld B)": letters, so none reads as a figure. */
const lettered = (i: number): string => `(detail withheld ${i < 26 ? String.fromCharCode(65 + i) : `${String.fromCharCode(65 + Math.floor(i / 26) - 1)}${String.fromCharCode(65 + (i % 26))}`})`;

function maskMetric(m: Metric, patterns: RegExp[], hit: Hit): Metric {
  return maskAt(m, "", MASKED.metric, patterns, hit) as Metric;
}

/**
 * Two keys masks made the same would merge two rows of a chart or a table, so
 * each masked key of such a pair is masked again from the key as stored, with
 * its own letter, every held figure at once.
 */
function keepKeysApart(original: Metric, masked: Metric, patterns: RegExp[]): Metric {
  if (masked.status !== "measured" || original.status !== "measured") return masked;
  const keys = masked.segments.map((s) => `${s.dimension}\u0000${s.key}`);
  if (new Set(keys).size === keys.length) return masked;
  const changed = [...new Set(original.segments.filter((s, i) => s.key !== masked.segments[i]!.key).map((s) => s.key))];
  return {
    ...masked,
    segments: masked.segments.map((s, i) => {
      const stored = original.segments[i]!.key;
      return s.key === stored ? s : { ...s, key: maskText(stored, patterns, lettered(changed.indexOf(stored))).text };
    }),
  } as Metric;
}

function maskStep(s: ManualStep, patterns: RegExp[], hit: Hit): ManualStep {
  return maskAt(s, "", MASKED.step, patterns, hit) as ManualStep;
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
      metrics: each(input.metrics, maskMetric, (m) => ({ metricId: m.id })).map((m, i) => keepKeysApart(input.metrics[i]!, m, raws.map(heldPattern))),
      manualSteps: each(input.manualSteps, maskStep, (s) => ({ stepId: s.id })),
    },
    repairs,
  };
}

const escapeXml = (text: string): string => text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");

/** Page furniture, references and chart ticks: never masked and never counted. */
const FURNITURE = /<(\w+)\b[^>]*(?:\bdata-chrome\b|\bdata-ref\b|\bclass="[^"]*\btick\b[^"]*")[^>]*>[\s\S]*?<\/\1>/g;

/** The HTML with furniture, styles and scripts set aside, a function over its text, then put back. */
function aside(html: string, fn: (text: string) => string): string {
  const kept: string[] = [];
  const hold = (m: string): string => `\u0000${kept.push(m) - 1}\u0000`;
  const work = html.replace(/<(style|script)\b[\s\S]*?<\/\1>/gi, hold).replace(FURNITURE, hold);
  return fn(work).replace(/\u0000(\d+)\u0000/g, (_m, i: string) => kept[Number(i)]!);
}

/**
 * The last resort, for a figure no stored field holds (fixed text in a
 * template, a survey metric built inside the report, a name a token restored
 * to that reads as a figure, a date the page rewrote): each occurrence of it in
 * the document's visible text, at its own boundaries, masked. Never in the
 * page furniture, a reference, a chart tick, a link or an email. Pages are laid
 * out again in the browser, so a page number can't be mapped back to the HTML:
 * every occurrence in the document is masked, and only that token.
 */
export function maskPage(html: string, raws: string[]): { html: string; masked: number } {
  if (raws.length === 0) return { html, masked: 0 };
  const patterns = raws.map((raw) => heldPattern(escapeXml(raw)));
  let masked = 0;
  const out = aside(html, (work) =>
    work.replace(/(>|\u0000\d+\u0000)([^<\u0000]+)/g, (_whole, lead: string, text: string) => {
      const result = maskText(text, patterns);
      masked += result.masked;
      return `${lead}${result.text}`;
    }),
  );
  return { html: out, masked };
}

/** How many masks a document shows in its checked text: furniture, references and ticks aren't counted. */
export function masksIn(html: string): number {
  let n = 0;
  aside(html, (work) => {
    n = work.split(MASK).length - 1 + (work.match(/\(detail withheld [A-Z]{1,2}\)/g)?.length ?? 0);
    return work;
  });
  return n;
}

/**
 * The document says how many details are withheld, once, when any are: in the
 * footer of its last page, or, with no footer, in a note at the end of its last
 * page, with no count. Never left out.
 */
export function withheldNote(html: string): string {
  const n = masksIn(html);
  if (n === 0) return html;
  const span = "<span>Black Cloud Barometer";
  const at = html.lastIndexOf(span);
  if (at >= 0) return `${html.slice(0, at + span.length)} | ${escapeXml(withheldLine(n))}${html.slice(at + span.length)}`;
  const end = html.lastIndexOf("</section>");
  const note = `<p class="small-note">${escapeXml(WITHHELD_NOTE)}</p>`;
  return end >= 0 ? `${html.slice(0, end)}${note}${html.slice(end)}` : `${html}${note}`;
}

/** A figure kind that is a number a customer would read as a claim: masking one where it's printed holds the documents for us. */
export const NUMERIC_KINDS: ReadonlySet<string> = new Set(["number", "percent", "currency", "ratio", "duration", "date", "points", "fraction", "magnitude"]);
