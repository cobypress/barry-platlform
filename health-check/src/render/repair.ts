import type { ReportInput } from "../narrative/sections.ts";
import { extractFigures } from "../numbercheck/index.ts";
import type { Finding, ManualStep, Metric } from "../schemas/index.ts";

// Decision 682, item 5, at the source. A figure no output can trace that lives
// in the engine's own data (a finding's field, a metric's table row or label) is
// taken out of the run's data before anything is built, so every output made
// from it (the PDFs, the raw notes, the readout kit, the action lists, the
// tickets, the digest and the microsite) is made without it. A consultant's or
// Claude's words are never touched here: those fall back whole (fallBackSections).

/** What stands in for a sentence of ours that couldn't be traced. No figure, so it can't be held itself. */
export const LEFT_OUT_SENTENCE = "We've left a detail out here because we couldn't trace it to your data.";

/** Said once under a table a row was taken from. */
export const ROW_LEFT_OUT = "A row is left out because we couldn't trace a detail in it to your data.";

/** Where a figure was taken out: the finding's rule or the metric, and the field. Never the figure. */
export type SourceRepair = { raw: string; ruleId?: string; metricId?: string; stepId?: string; field: string };

const escapeRegExp = (text: string): string => text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * The figure exactly as the check read it, at its own boundaries, each time it's
 * written: a held "4" never matches in "4,321", "4.5", "4%", "£4" or "A4". An
 * identifier is matched in any case, as the check compares it; anything else as written.
 */
export function heldPattern(raw: string): RegExp {
  const kind = extractFigures(raw).figures[0]?.kind;
  return new RegExp(`(?<![\\w£$€]|\\d[.,])${escapeRegExp(raw)}(?![\\w%]|[.,]\\d)`, kind === "identifier" ? "i" : "");
}

/** The text with each sentence holding one of these replaced, a run of them by one fixed sentence. */
export function leaveOutSentences(text: string, patterns: RegExp[]): string {
  if (!patterns.some((p) => p.test(text))) return text;
  const parts = text.split(/(?<=[.!?])\s+/).map((part) => (patterns.some((p) => p.test(part)) ? LEFT_OUT_SENTENCE : part));
  return parts.filter((part, i) => part !== LEFT_OUT_SENTENCE || parts[i - 1] !== LEFT_OUT_SENTENCE).join(" ");
}

type Hit = (field: string) => void;

function text<T extends string | null | undefined>(value: T, patterns: RegExp[], field: string, hit: Hit): T {
  if (!value) return value;
  const out = leaveOutSentences(value, patterns);
  if (out !== value) hit(field);
  return out as T;
}

function repairFinding(f: Finding, patterns: RegExp[], hit: Hit): Finding {
  const t = <T extends string | null | undefined>(value: T, field: string): T => text(value, patterns, field, hit);
  const value = f.value.kind === "not_sized" ? { ...f.value, reason: t(f.value.reason, "value.reason") } : { ...f.value, period: t(f.value.period, "value.period"), assumptions: f.value.assumptions.map((a) => t(a, "value.assumptions")) };
  const subjects = f.subjects.filter((s) => {
    const held = patterns.some((p) => p.test(s.label));
    if (held) hit("subjects");
    return !held;
  });
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
    value,
    subjects,
  } as Finding;
}

function repairMetric(m: Metric, patterns: RegExp[], hit: Hit): Metric {
  const t = <T extends string | null | undefined>(value: T, field: string): T => text(value, patterns, field, hit);
  const base = { ...m, label: t(m.label, "label"), definition: t(m.definition, "definition"), assumptions: m.assumptions.map((a) => t(a, "assumptions")), exclusions: m.exclusions.map((e) => t(e, "exclusions")) } as Metric;
  if (base.status !== "measured") return { ...base, reason: t(base.reason, "reason") } as Metric;
  // A table row is dropped whole, and the table says once that one was.
  const segments = base.segments.filter((s) => !patterns.some((p) => p.test(s.key) || p.test(s.dimension)));
  const dropped = segments.length < base.segments.length;
  if (dropped) hit("segments");
  return {
    ...base,
    segments: segments.map((s) => ({ ...s, notMeasurableReason: t(s.notMeasurableReason, "segments.notMeasurableReason") })),
    series: base.series.map((p) => ({ ...p, notMeasurableReason: t(p.notMeasurableReason, "series.notMeasurableReason") })),
    exclusions: dropped && !base.exclusions.includes(ROW_LEFT_OUT) ? [...base.exclusions, ROW_LEFT_OUT] : base.exclusions,
  } as Metric;
}

function repairStep(s: ManualStep, patterns: RegExp[], hit: Hit): ManualStep {
  const t = <T extends string | null | undefined>(value: T, field: string): T => text(value, patterns, field, hit);
  return { ...s, title: t(s.title, "title"), whyItMatters: t(s.whyItMatters, "whyItMatters"), whereToGo: t(s.whereToGo, "whereToGo"), whatToCapture: t(s.whatToCapture, "whatToCapture"), ifSkipped: t(s.ifSkipped, "ifSkipped") } as ManualStep;
}

/**
 * The run's own data with every figure here taken out where the engine wrote
 * it, and where each was found. The intake, the survey and the interviews are
 * people's words and are never changed.
 */
export function repairInput(input: ReportInput, raws: string[]): { input: ReportInput; repairs: SourceRepair[] } {
  if (raws.length === 0) return { input, repairs: [] };
  const repairs: SourceRepair[] = [];
  const each = <T>(rows: T[], fix: (row: T, patterns: RegExp[], hit: Hit) => T, where: (row: T) => Omit<SourceRepair, "raw" | "field">): T[] => {
    let out = rows;
    for (const raw of raws) {
      const patterns = [heldPattern(raw)];
      out = out.map((row) => fix(row, patterns, (field) => repairs.push({ raw, ...where(row), field })));
    }
    return out;
  };
  return {
    input: {
      ...input,
      findings: each(input.findings, repairFinding, (f) => ({ ruleId: f.ruleId })),
      metrics: each(input.metrics, repairMetric, (m) => ({ metricId: m.id })),
      manualSteps: each(input.manualSteps, repairStep, (s) => ({ stepId: s.id })),
    },
    repairs,
  };
}
