import { buildDataset, checkNumbers, scopeDataset, SOURCE, type FigureSet } from "../numbercheck/index.ts";
import type { DatasetFigure } from "../numbercheck/dataset.ts";
import { orgFree } from "../numbercheck/dataset.ts";
import { countOf } from "../fix/hours.ts";
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

import type { NamedUnit, NamedValues, NotFigures } from "../engine/named.ts";
import { extractFigures } from "../numbercheck/extract.ts";
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

/** The hours' constants and the fix prompt's fixed text, each a source of its own, so neither backs any other text of the finding. */
export const HOURS = "#hours";
export const PROMPT = "#prompt";
/**
 * The run's dates alone (its as-of day and analysis window): what a finding's or
 * metric's texts may take from the run. Never the run's numbers (its API calls,
 * budget share and API version), which would back any count a sentence wrote.
 */
export const RUN_DATES = "run#dates";

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
  // Its range as the finding holds it, worked out by the fix table (src/fix/index.ts) from its facts.
  const hours = f.effortHours;
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

/** A name's last word, case aside, and the unit it says. Only the last whole word: "generated", "separate" and "operator" say nothing. */
const UNIT_WORD: Readonly<Record<string, DatasetFigure["kind"] | "date" | `duration:${string}`>> = {
  pct: "percent", percent: "percent", rate: "percent", share: "percent",
  money: "currency", points: "points",
  day: "duration:day", days: "duration:day", hour: "duration:hour", hours: "duration:hour", week: "duration:week", weeks: "duration:week",
  month: "duration:month", months: "duration:month", minute: "duration:minute", minutes: "duration:minute", year: "duration:year", years: "duration:year",
};
const EXPLICIT: Readonly<Record<NamedUnit, DatasetFigure["kind"] | "date" | `duration:${string}`>> = {
  money: "currency", percent: "percent", points: "points", ratio: "ratio", date: "date", day: "duration:day", hour: "duration:hour", week: "duration:week", month: "duration:month", minute: "duration:minute", year: "duration:year",
};

/** The words of a name: "worstSegmentRate" is worst, segment, rate; "DataStorageMBShare" ends in share. */
const wordsOf = (name: string): string[] => name.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2").split(/[^A-Za-z0-9]+/).filter(Boolean).map((w) => w.toLowerCase());

/** The unit a value is named with: given with it ("name@unit"), or said by its name's last word. */
export function unitOf(key: string): DatasetFigure["kind"] | "date" | `duration:${string}` | null {
  const [name, unit] = key.replace(/#\d+$/, "").split("@") as [string, string | undefined];
  if (unit !== undefined) return (EXPLICIT as Record<string, DatasetFigure["kind"] | "date" | `duration:${string}`>)[unit] ?? null;
  const last = wordsOf(name).at(-1);
  return last !== undefined && Object.hasOwn(UNIT_WORD, last) ? UNIT_WORD[last]! : null;
}

/**
 * A computed value of a finding or metric as a row of the set: a plain number,
 * and in the unit it was named with (DECISIONS.md 768): given beside the value,
 * `[limit, "percent"]`, where its name doesn't say it; else its name's last word
 * (`…Money`, `…Pct`, `…Rate`, `…Share`, `…Points`, `…Days` and the other spans
 * of time), whole and case aside. Never a unit read from the finding's own text,
 * as the documents' set takes a fact's: here nothing of the prose is read.
 */
function typed(source: string, key: string, value: number, currency: string | undefined): DatasetFigure[] {
  const out: DatasetFigure[] = [{ source, kind: "number", value }];
  const unit = unitOf(key);
  if (unit === "currency") out.push({ source, kind: "currency", value, ...(currency ? { currency } : {}) });
  else if (unit?.startsWith("duration:")) out.push({ source, kind: "duration", value, unit: unit.slice("duration:".length) });
  else if (unit) out.push({ source, kind: unit as DatasetFigure["kind"], value });
  return out;
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
  const extra: DatasetFigure[] = [];
  const currency = report.input.calibration.currency;
  for (const f of findings) {
    extra.push(...hoursFigures(f));
    // A target's money facts are money (DECISIONS.md 369), as the documents' set holds them.
    for (const name of MONEY_FACTS[f.ruleId] ?? []) {
      const amount = f.facts[name];
      if (typeof amount === "number" && amount > 0) extra.push({ source: f.id, kind: "currency", value: amount, ...(currency ? { currency } : {}) });
    }
    for (const [name, value] of Object.entries(f.facts)) extra.push(...typed(f.id, name, value, currency));
  }
  const namedDates: FigureSet["dates"] = [];
  for (const [id, values] of Object.entries(named)) {
    if (!own.has(id)) continue;
    for (const [name, value] of Object.entries(values)) {
      if (!Number.isFinite(value)) continue;
      // A named date (DECISIONS.md 772): a day, or a month, of the finding's own, as a row of dates and nothing else.
      if (unitOf(name) === "date") namedDates.push({ source: id, y: Math.floor(value / 10000), m: Math.floor(value / 100) % 100, ...(value % 100 ? { d: value % 100 } : {}) });
      else extra.push(...typed(id, name, value, currency));
    }
  }
  // The fix prompt's fixed text, its rule's (src/fix/table.ts), put together with none of the finding's sentences in it.
  const prompts = findings.flatMap((f) => {
    const rule = FIX_TABLE[f.ruleId]?.prompt;
    if (!rule || !f.fixPrompt) return [];
    const fixed = assembleMarked({ ...f, title: "", summary: "", recommendation: "", orgWords: undefined }, rule);
    return [{ source: `${f.id}${PROMPT}`, text: fixed.text, orgWords: fixed.spans }];
  });
  // And the hours' fixed words (what a unit is, the bulk reason, a fixed basis), the fix table's own, beside its numbers.
  const hoursWords = findings.flatMap((f) => {
    const rule = FIX_TABLE[f.ruleId]?.hours;
    if (!rule || !f.effortHours) return [];
    return [{ source: `${f.id}${HOURS}`, text: rule.kind === "fixed" ? rule.basis : [rule.one, rule.many, rule.bulk ?? ""].join("\n") }];
  });
  const promptRows = orgFree(buildDataset({ findings: [], metrics: [], extraText: [...prompts, ...hoursWords], orgNames: findings.flatMap((f) => f.subjects.map((s) => s.label)) }));
  const isPrompt = (r: { source: string }) => r.source.endsWith(PROMPT) || r.source.endsWith(HOURS);
  const ours = orgFree(computed);
  // The run's dates again, under a source of their own: all a finding's or metric's texts may take from the run.
  const runDates = free.dates.filter((d) => d.source === SOURCE.run).map((d) => ({ ...d, source: RUN_DATES }));
  return {
    figures: [...runWide(free.figures), ...mine(ours.figures), ...extra, ...promptRows.figures.filter(isPrompt)],
    dates: [...runWide(free.dates), ...mine(ours.dates), ...promptRows.dates.filter(isPrompt), ...runDates, ...namedDates],
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
    if (t.path === "findings[].effortHours.basis") return [id, `${id}${HOURS}`, RUN_DATES];
    if (t.path === "findings[].fixPrompt") return [id, `${id}${PROMPT}`, RUN_DATES];
    return [id, RUN_DATES];
  }
  if (t.path.startsWith("metrics[].")) return [digest.metrics[at]!.id, RUN_DATES];
  // An area's texts (its reason, DECISIONS.md 769) speak only of its own checks.
  if (t.path.startsWith("areas[].")) {
    const area = digest.areas[at]!;
    return [...("findingIds" in area ? area.findingIds : digest.findings.filter((f) => f.domain === area.domain).map((f) => f.id)), RUN_DATES];
  }
  if (t.path.startsWith("readiness.pillars[].")) {
    const p = digest.readiness!.pillars[at]!;
    return [...p.findingIds, ...p.metricIds, RUN_DATES];
  }
  if (t.path.startsWith("paths.insights[].")) return [digest.paths!.insights[at]!.id, RUN_DATES];
  if (t.path === "paths.headline.text") return digest.paths?.headline ? [digest.paths.headline.insightId, RUN_DATES] : null;
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
  if (t.path.startsWith("areas[].")) return `areas:${digest.areas[at]!.domain}`;
  // Any other block by its path, "plan:phases", "value:groups", "overall:reason", so every owner is a key the snapshot can hold.
  const [root, next] = t.path.split(".").map((k) => k.replace(/\[\]/g, ""));
  return `${root}:${next ?? "text"}`;
}

/** The kinds of figure recorded as written: numbers of ours. Any other (an identifier, a token, a link, an email, a malformed figure) is the org's, or could be. */
const WRITTEN_KINDS: ReadonlySet<string> = new Set(["number", "percent", "points", "currency", "ratio", "fraction", "duration", "date", "magnitude"]);

/** The figures in one text that the strict set, scoped to its sources, can't trace, each as the shadow may record it. Its figures in the org's words are set aside (764). */
export function strictUntraced(text: string, orgWords: readonly OrgSpan[], set: FigureSet): { checked: number; unmatched: string[] } {
  const out = checkNumbers(text, set, { orgWords: [...orgWords] });
  // Each figure's kind, as the reader read it: by kind, never by its shape.
  const kinds = new Map<string, string>();
  for (const f of extractFigures(text, { labels: set.labels.map((l) => l.value) }).figures) if (!kinds.has(f.raw) || !WRITTEN_KINDS.has(f.kind)) kinds.set(f.raw, f.kind);
  return { checked: out.checked, unmatched: out.unmatched.map((u) => (WRITTEN_KINDS.has(kinds.get(u.raw) ?? "") ? u.raw : kinds.get(u.raw) === "token" ? "(token)" : "(identifier)")) };
}

/** How long the shadow may take before it gives up, so it can never hold a release up (DECISIONS.md 768). */
export const SHADOW_BUDGET_MS = 10_000;

/** The shadow gave up: too long. Its name is what the worker logs. */
export class ShadowTimeout extends Error {
  constructor() {
    // One word, so it's no sentence the engine-texts scan would list as ours.
    super("timeout");
    this.name = "timeout";
  }
}

/**
 * What the strict check would mask on a digest, counted and never applied
 * (DECISIONS.md 768). Only marked texts are read: one with no marks is never
 * served as traced (withheld whole, or the run held) whatever it holds.
 */
/** The id whose word() marks a text may use: its finding's, metric's or insight's. */
function wordsOwner(digest: RunDigest, t: DigestText): string | null {
  const at = t.at[0]!;
  if (t.path.startsWith("findings[].")) return digest.findings[at]!.id;
  if (t.path.startsWith("metrics[].")) return digest.metrics[at]!.id;
  if (t.path.startsWith("paths.insights[].")) return digest.paths!.insights[at]!.id;
  return null;
}

/** Where in a text its own sentences' word() pieces sit (DECISIONS.md 773): each sentence, wherever the text holds it whole. */
export function notFigureSpans(text: string, marks: NotFigures[string] | undefined): OrgSpan[] {
  const out: OrgSpan[] = [];
  for (const { sentence, spans } of marks ?? []) {
    for (let at = text.indexOf(sentence); at !== -1; at = text.indexOf(sentence, at + 1)) for (const s of spans) out.push({ start: at + s.start, end: at + s.end });
  }
  return out.sort((a, b) => a.start - b.start);
}

export function strictShadow(digest: RunDigest, report: BuiltReport, set: FigureSet, named: NamedValues = {}, budgetMs = SHADOW_BUDGET_MS, now: () => number = () => performance.now(), notFigures: NotFigures = {}): StrictShadow {
  const until = now() + budgetMs;
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
    // Checked in the loop and nowhere else: nothing of the shadow outlives its call, so nothing can fire after a release.
    if (now() > until) throw new ShadowTimeout();
    const marks = marksOf(digest, t);
    if (!marks || t.text === "") continue;
    const kind = strictKindOf(t.path);
    // A number word of ours that claims nothing is set aside, as a figure in the org's words is (764).
    const owner = wordsOwner(digest, t);
    const aside = owner ? notFigureSpans(t.text, notFigures[owner]) : [];
    const out = strictUntraced(t.text, aside.length ? [...marks.orgWords, ...aside].sort((a, b) => a.start - b.start) : marks.orgWords, scoped(strictScope(digest, report, t)));
    const k = (shadow.byKind[kind] ??= { checked: 0, untraced: 0 });
    k.checked += out.checked;
    k.untraced += out.unmatched.length;
    shadow.checked += out.checked;
    shadow.untraced += out.unmatched.length;
    const ruleId = ownerOf(digest, report, t);
    for (const figure of out.unmatched) shadow.figures.push({ ruleId, kind, figure });
  }
  return shadow;
}

/** The shadow as one log line: counts only, by kind. */
export function shadowLine(runId: string, s: StrictShadow): string {
  const kinds = STRICT_KINDS.filter((k) => s.byKind[k]?.untraced).map((k) => `${k} ${s.byKind[k]!.untraced}`);
  return `Run ${runId}: the strict check (shadow) would mask ${s.untraced} of ${s.checked} figures${kinds.length ? ` (${kinds.join(", ")})` : ""}.`;
}
