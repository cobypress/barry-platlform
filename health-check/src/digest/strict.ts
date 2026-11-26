import { buildDataset, checkNumbers, scopeDataset, SOURCE, type FigureSet } from "../numbercheck/index.ts";
import type { DatasetFigure } from "../numbercheck/dataset.ts";
import { orgFree } from "../numbercheck/dataset.ts";
import { countOf, effortHours } from "../fix/hours.ts";
import { FIX_TABLE } from "../fix/table.ts";
import { assembleMarked } from "../fix/prompt.ts";
import { PROSE_SOURCES, type BuiltReport } from "../narrative/sections.ts";
import { MONEY_FACTS } from "../value/model.ts";
import type { Finding, Metric } from "../schemas/index.ts";
import type { RunDigest } from "../schemas/digest.ts";
import type { OrgSpan } from "../engine/org-words.ts";
import { digestTextsAt, marksOf, type DigestText } from "./checked.ts";

// The strict check, in shadow (DECISIONS.md 768, KAN-102). The digest's check
// reads a finding's sentences against a set that holds those same sentences, so
// it can't show a number is the one its rule computed (764). Here each finding's
// and each metric's texts are checked against that finding's or metric's own
// computed values alone, scoped by source as the documents' sections are
// (scopeDataset): its facts, evidence counts and dates, value, segments, the
// values its rule names (`named`, kept in the run folder only), the constants
// its fix table declares, and the run's own dates. Never any prose, never
// another finding's figures, never the org's words (orgFree). It changes
// nothing: it only counts what it would have masked.

import type { NamedValues } from "../engine/named.ts";
export { NAMED_VALUES_FILE, type NamedValues } from "../engine/named.ts";
/** The strict check's shadow counts, written by the worker. Run folder only: never in the digest, never stored, never shown. */
export const STRICT_SHADOW_FILE = "strict-shadow.json";

export const STRICT_KINDS = [
  "title", "summary", "recommendation", "notMeasurableReason", "valueTexts", "hoursBasis", "fixPrompt",
  "metricLabel", "metricDefinition", "metricAssumptions", "metricExclusions", "metricReason",
  "readiness", "paths", "plan", "score", "value", "other",
] as const;
export type StrictKind = (typeof STRICT_KINDS)[number];

/** One figure the strict check couldn't trace: the rule (or block), the kind of text, and the figure as written. Never an org word. */
export type StrictUntraced = { ruleId: string; kind: StrictKind; figure: string };

export type StrictShadow = {
  version: 1;
  /** Figures read, and those the strict check couldn't trace, over every marked text. */
  checked: number;
  untraced: number;
  byKind: Partial<Record<StrictKind, { checked: number; untraced: number }>>;
  figures: StrictUntraced[];
};

const KIND_BY_PATH: Readonly<Record<string, StrictKind>> = {
  "findings[].title": "title",
  "findings[].summary": "summary",
  "findings[].recommendation": "recommendation",
  "findings[].notMeasurableReason": "notMeasurableReason",
  "findings[].value.reason": "valueTexts",
  "findings[].value.assumptions[]": "valueTexts",
  "findings[].value.unit": "valueTexts",
  "findings[].value.period": "valueTexts",
  "findings[].effortHours.basis": "hoursBasis",
  "findings[].fixPrompt": "fixPrompt",
  "metrics[].label": "metricLabel",
  "metrics[].definition": "metricDefinition",
  "metrics[].assumptions[]": "metricAssumptions",
  "metrics[].exclusions[]": "metricExclusions",
  "metrics[].reason": "metricReason",
  "metrics[].unit": "metricLabel",
};

export function strictKindOf(path: string): StrictKind {
  const known = KIND_BY_PATH[path];
  if (known) return known;
  if (path.startsWith("readiness.")) return "readiness";
  if (path.startsWith("paths.")) return "paths";
  if (path.startsWith("plan.")) return "plan";
  if (path.startsWith("overall.") || path.startsWith("areas[].")) return "score";
  if (path.startsWith("value.")) return "value";
  return "other";
}

/** A finding as only its computed material: every text emptied, every note, guard detail and exclusion gone. */
function computedFinding(f: Finding): Finding {
  return {
    ...f,
    title: "",
    summary: "",
    recommendation: "",
    notMeasurableReason: null,
    effort: { size: f.effort.size },
    dependencies: [],
    guards: [],
    value: f.value.kind === "not_sized" ? { kind: "not_sized", reason: "" } : { ...f.value, period: "", assumptions: [] },
    evidence: f.evidence.map((e) => ({ ...e, note: undefined, exclusions: [] })),
    orgWords: undefined,
    fixPrompt: undefined,
    effortHours: undefined,
  } as Finding;
}

/** A metric as only its computed material. */
function computedMetric(m: Metric): Metric {
  const base = { ...m, label: "", definition: "", assumptions: [], exclusions: [], orgWords: undefined } as Metric;
  if (base.status === "not_measurable") return { ...base, reason: "" } as Metric;
  return { ...base, segments: base.segments.map((s) => ({ ...s, notMeasurableReason: null })), series: base.series.map((p) => ({ ...p, notMeasurableReason: null })) } as Metric;
}

const PERCENT_NAME = /rate|share|percent|pct/i;

/** The hours' constants and the fix prompt's fixed text, each a source of its own, so neither backs any other text of the finding. */
export const HOURS = "#hours";
export const PROMPT = "#prompt";

/**
 * The constants a finding's fix table declares and the figures its hours are
 * worked out from (src/fix/hours.ts): the count, each end of the minutes, in
 * minutes and in whole hours and minutes as the basis says them, the cap, the
 * range, and the units covered and left. Nothing read from the basis itself.
 */
function hoursFigures(f: Finding): DatasetFigure[] {
  const rule = FIX_TABLE[f.ruleId]?.hours;
  if (!rule) return [];
  const source = `${f.id}${HOURS}`;
  const out: DatasetFigure[] = [];
  const number = (value: number) => out.push({ source, kind: "number", value });
  const duration = (value: number, unit: string) => out.push({ source, kind: "duration", value, unit });
  const hours = effortHours(rule, f.facts);
  if (hours) for (const h of [hours.low, hours.high]) (number(h), duration(h, "hour"));
  if (rule.kind === "fixed") return out;
  const count = countOf(rule, f.facts);
  if (count === null) return out;
  number(count);
  for (const minutes of rule.minutes) {
    number(minutes);
    duration(minutes, "minute");
    if (minutes >= 60) (duration(Math.floor(minutes / 60), "hour"), number(Math.floor(minutes / 60)), minutes % 60 && duration(minutes % 60, "minute"));
  }
  number(rule.cap);
  duration(rule.cap, "hour");
  const covered = Math.max(1, Math.floor((rule.cap * 60) / rule.minutes[1]));
  if (covered < count) (number(covered), number(count - covered));
  return out;
}

/**
 * A computed value of a finding or metric, in the units the set already holds
 * it in under that source (as the documents' set takes a fact's unit from how
 * the engine wrote it: the value is the computed one, only its unit is read),
 * else a percentage by its name, else a plain number.
 */
function typed(source: string, name: string, value: number, held: readonly DatasetFigure[]): DatasetFigure[] {
  const kinds = held.filter((r) => r.value === value && r.kind !== "number").map((r) => ({ ...r, source }));
  return [{ source, kind: PERCENT_NAME.test(name) ? "percent" : "number", value }, ...kinds];
}

/**
 * The strict set: every finding's and metric's computed material under its own
 * id, the values each names, its fix table's constants, and the run-wide
 * computed sources (the run, summary counts, scores, plan, overall, value
 * model, structure). Nothing read from a finding's, metric's or step's text,
 * the calibration's, the intake, the cover or a person's words; nothing in the
 * org's words; no label. Scope it per text with scopeDataset.
 */
export function strictDataset(report: BuiltReport, set: FigureSet, named: NamedValues = {}): FigureSet {
  const findings = [...new Map([...report.teamFindings, ...(report.pathInsights ?? [])].map((f) => [f.id, f])).values()];
  const own = new Set([...findings.map((f) => f.id), ...report.metrics.map((m) => m.id), ...report.input.findings.map((f) => f.id), ...report.input.metrics.map((m) => m.id), ...report.input.manualSteps.map((s) => s.id), ...PROSE_SOURCES]);
  const computed = buildDataset({ findings: findings.map(computedFinding), metrics: report.metrics.map(computedMetric), currency: report.input.calibration.currency });
  const mine = <T extends { source: string }>(rows: T[]): T[] => rows.filter((r) => own.has(r.source));
  const runWide = <T extends { source: string; said?: string }>(rows: T[]): T[] => rows.filter((r) => !own.has(r.source) && !r.said);
  const free = orgFree(set);
  const bySource = new Map<string, DatasetFigure[]>();
  for (const r of free.figures) bySource.set(r.source, [...(bySource.get(r.source) ?? []), r]);
  const extra: DatasetFigure[] = [];
  const currency = report.input.calibration.currency;
  for (const f of findings) {
    extra.push(...hoursFigures(f));
    // A target's money facts are money (DECISIONS.md 369), as the documents' set holds them.
    for (const name of MONEY_FACTS[f.ruleId] ?? []) {
      const amount = f.facts[name];
      if (typeof amount === "number" && amount > 0) extra.push({ source: f.id, kind: "currency", value: amount, ...(currency ? { currency } : {}) });
    }
    for (const [name, value] of Object.entries(f.facts)) extra.push(...typed(f.id, name, value, bySource.get(f.id) ?? []).slice(1));
  }
  for (const [id, values] of Object.entries(named)) {
    if (!own.has(id)) continue;
    for (const [name, value] of Object.entries(values)) if (Number.isFinite(value)) extra.push(...typed(id, name, value, bySource.get(id) ?? []));
  }
  // The fix prompt's fixed text, its rule's (src/fix/table.ts), put together with none of the finding's sentences in it.
  const prompts = findings.flatMap((f) => {
    const rule = FIX_TABLE[f.ruleId]?.prompt;
    if (!rule || !f.fixPrompt) return [];
    const fixed = assembleMarked({ ...f, title: "", summary: "", recommendation: "", orgWords: undefined }, rule);
    return [{ source: `${f.id}${PROMPT}`, text: fixed.text, orgWords: fixed.spans }];
  });
  const promptRows = orgFree(buildDataset({ findings: [], metrics: [], extraText: prompts, orgNames: findings.flatMap((f) => f.subjects.map((s) => s.label)) }));
  const isPrompt = (r: { source: string }) => r.source.endsWith(PROMPT);
  const ours = orgFree(computed);
  return {
    figures: [...runWide(free.figures), ...mine(ours.figures), ...extra, ...promptRows.figures.filter(isPrompt)],
    dates: [...runWide(free.dates), ...mine(ours.dates), ...promptRows.dates.filter(isPrompt)],
    identifiers: [...runWide(free.identifiers), ...mine(ours.identifiers), ...promptRows.identifiers.filter(isPrompt)],
    tokens: [...runWide(free.tokens), ...mine(ours.tokens), ...promptRows.tokens.filter(isPrompt)],
    labels: [],
  };
}

/** The sources a digest text's figures may trace to under the strict check, or null for the whole computed set (a run-wide line). */
export function strictScope(digest: RunDigest, report: BuiltReport, t: DigestText): string[] | null {
  const at = t.at[0]!;
  if (t.path.startsWith("findings[].")) {
    const id = digest.findings[at]!.id;
    if (t.path === "findings[].effortHours.basis") return [id, `${id}${HOURS}`, SOURCE.run];
    if (t.path === "findings[].fixPrompt") return [id, `${id}${PROMPT}`, SOURCE.run];
    return [id, SOURCE.run];
  }
  if (t.path.startsWith("metrics[].")) return [digest.metrics[at]!.id, SOURCE.run];
  if (t.path.startsWith("readiness.pillars[].")) {
    const p = digest.readiness!.pillars[at]!;
    return [...p.findingIds, ...p.metricIds, SOURCE.run];
  }
  if (t.path.startsWith("paths.insights[].")) return [digest.paths!.insights[at]!.id, SOURCE.run];
  if (t.path === "paths.headline.text") return digest.paths?.headline ? [digest.paths.headline.insightId, SOURCE.run] : null;
  void report;
  return null;
}

/** Whose a text is, for the shadow's references: its finding's rule, its metric, or the block. */
function ownerOf(digest: RunDigest, report: BuiltReport, t: DigestText): string {
  const at = t.at[0]!;
  if (t.path.startsWith("findings[].")) return digest.findings[at]!.ruleId;
  if (t.path.startsWith("metrics[].")) return digest.metrics[at]!.id;
  if (t.path.startsWith("readiness.pillars[].")) return `readiness:${digest.readiness!.pillars[at]!.key}`;
  if (t.path.startsWith("paths.insights[].")) {
    const id = digest.paths!.insights[at]!.id;
    return report.pathInsights?.find((f) => f.id === id)?.ruleId ?? "paths";
  }
  return t.path.split(/[.[]/)[0]!;
}

/** A figure as the shadow records it: a number of ours as written, never a word or a token, which can be the org's. */
const written = (raw: string): string => (raw.startsWith("[[") ? "(token)" : /[A-Za-z]\d|\d[A-Za-z]|_/.test(raw) && !/^\d+(?:st|nd|rd|th)$/.test(raw) ? "(identifier)" : raw);

/** The figures in one text that the strict set, scoped to its sources, can't trace. Its figures in the org's words are set aside (764). */
export function strictUntraced(text: string, orgWords: readonly OrgSpan[], set: FigureSet): { checked: number; unmatched: string[] } {
  const out = checkNumbers(text, set, { orgWords: [...orgWords] });
  return { checked: out.checked, unmatched: out.unmatched.map((u) => u.raw) };
}

/**
 * What the strict check would mask on a digest, counted and never applied
 * (DECISIONS.md 768). Only marked texts are read: one with no marks is never
 * served as traced (withheld whole, or the run held) whatever it holds.
 */
export function strictShadow(digest: RunDigest, report: BuiltReport, set: FigureSet, named: NamedValues = {}): StrictShadow {
  const strict = strictDataset(report, set, named);
  const scopes = new Map<string, FigureSet>();
  const scoped = (sources: string[] | null): FigureSet => {
    if (!sources) return strict;
    const key = sources.join("\u0000");
    let s = scopes.get(key);
    if (!s) scopes.set(key, (s = scopeDataset(strict, sources)));
    return s;
  };
  const shadow: StrictShadow = { version: 1, checked: 0, untraced: 0, byKind: {}, figures: [] };
  for (const t of digestTextsAt(digest)) {
    const marks = marksOf(digest, t);
    if (!marks || t.text === "") continue;
    const kind = strictKindOf(t.path);
    const out = strictUntraced(t.text, marks.orgWords, scoped(strictScope(digest, report, t)));
    const k = (shadow.byKind[kind] ??= { checked: 0, untraced: 0 });
    k.checked += out.checked;
    k.untraced += out.unmatched.length;
    shadow.checked += out.checked;
    shadow.untraced += out.unmatched.length;
    const ruleId = ownerOf(digest, report, t);
    for (const raw of out.unmatched) shadow.figures.push({ ruleId, kind, figure: written(raw) });
  }
  return shadow;
}

/** The shadow as one log line: counts only, by kind. */
export function shadowLine(runId: string, s: StrictShadow): string {
  const kinds = STRICT_KINDS.filter((k) => s.byKind[k]?.untraced).map((k) => `${k} ${s.byKind[k]!.untraced}`);
  return `Run ${runId}: the strict check (shadow) would mask ${s.untraced} of ${s.checked} figures${kinds.length ? ` (${kinds.join(", ")})` : ""}.`;
}
