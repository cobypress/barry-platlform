import type { Finding, ManualStep, Metric, NumberOverride, Run, Score } from "../schemas/index.ts";
import { extractFigures, type Figure, type PartialDate } from "./extract.ts";

// Everything a report is allowed to say, with where each figure came from. Facts
// are named numbers with no unit, so their unit comes from how the engine's own
// text wrote them. A figure with no known unit is a plain number and can't back
// a percentage or an amount of money.

export type DatasetFigure = {
  source: string;
  kind: "number" | "percent" | "points" | "currency" | "ratio" | "duration";
  value: number;
  currency?: string;
  unit?: string;
};

export type FigureSet = {
  figures: DatasetFigure[];
  dates: Array<PartialDate & { source: string }>;
  identifiers: Array<{ source: string; value: string }>;
  tokens: Array<{ source: string; value: string }>;
  /** Names containing digits, which are masked rather than read as figures. */
  labels: Array<{ source: string; value: string }>;
};

/** A figure the report's structure needs, such as a 90-day plan. Claude never gets these. */
export type StructuralFigure = Omit<DatasetFigure, "source">;

export type DatasetInput = {
  findings: Finding[];
  metrics: Metric[];
  manualSteps?: ManualStep[];
  scores?: Score[];
  run?: Run;
  currency?: string;
  overrides?: NumberOverride[];
  structural?: StructuralFigure[];
  /** Engine or consultant text the report may quote, such as the intake. */
  extraText?: Array<{ source: string; text: string }>;
};

export const SOURCE = { run: "run", summary: "summary", structure: "structure" } as const;

const emptySet = (): FigureSet => ({ figures: [], dates: [], identifiers: [], tokens: [], labels: [] });

class Collector {
  readonly set = emptySet();
  readonly source: string;

  constructor(set: FigureSet, source: string) {
    this.set = set;
    this.source = source;
  }

  number(value: number | null | undefined, kind: DatasetFigure["kind"] = "number", extra: { currency?: string; unit?: string } = {}): void {
    if (value === null || value === undefined || !Number.isFinite(value)) return;
    this.set.figures.push({ source: this.source, kind, value, ...extra });
  }

  date(iso: string | null | undefined): void {
    if (!iso) return;
    const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
    this.set.dates.push({ source: this.source, y: y!, m: m!, d: d! });
  }

  label(value: string): void {
    if (/\d/.test(value)) this.set.labels.push({ source: this.source, value });
  }

  /** Reads figures out of engine text, using the same reader the check uses. */
  text(text: string | null | undefined): Figure[] {
    if (!text) return [];
    const { figures } = extractFigures(text);
    for (const figure of figures) {
      if (figure.kind === "token") this.set.tokens.push({ source: this.source, value: figure.raw });
      else if (figure.kind === "identifier") this.set.identifiers.push({ source: this.source, value: figure.raw.toLowerCase() });
      else if (figure.kind === "date" && figure.date) this.set.dates.push({ source: this.source, ...figure.date });
      else if (figure.kind === "fraction" && figure.value !== undefined) this.number(figure.value, "percent");
      else if (figure.value !== undefined && figure.kind !== "malformed" && figure.kind !== "magnitude") {
        this.number(figure.value, figure.kind as DatasetFigure["kind"], { ...(figure.currency ? { currency: figure.currency } : {}), ...(figure.unit ? { unit: figure.unit } : {}) });
      }
    }
    return figures;
  }
}

// A fact the engine wrote out is already in the set, in the unit it was written.
// Only one the text never shows needs its unit worked out from its name.
function addFact(c: Collector, name: string, value: number, written: Figure[]): void {
  if (written.some((f) => f.value === value && f.kind !== "malformed")) return;
  c.number(value, /rate|share|percent|pct/i.test(name) ? "percent" : "number");
}

function addFinding(set: FigureSet, f: Finding, currency: string | undefined): void {
  const c = new Collector(set, f.id);
  const written = [f.title, f.summary, f.recommendation, f.notMeasurableReason, f.effort.note, ...f.dependencies].flatMap((text) => c.text(text));
  for (const guard of f.guards) c.text(guard.detail);
  for (const subject of f.subjects) {
    c.label(subject.label);
    c.text(subject.label.startsWith("[[") ? subject.label : null);
  }
  for (const segment of f.segmentsChecked) c.number(segment.segments);
  if (f.value.kind === "not_sized") c.text(f.value.reason);
  else {
    if (f.value.kind === "hours") c.number(f.value.amount, "duration", { unit: "hour" });
    else c.number(f.value.amount, "currency", { currency: /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : (currency ?? f.value.unit) });
    c.text(f.value.period);
    for (const assumption of f.value.assumptions) c.text(assumption);
  }
  for (const [name, value] of Object.entries(f.facts)) {
    // A fact that matches the sized value is the same money, so it can back a £ figure.
    if (f.value.kind !== "not_sized" && f.value.kind !== "hours" && value === f.value.amount) continue;
    addFact(c, name, value, written);
  }
  for (const e of f.evidence) {
    c.number(e.count);
    c.date(e.dateRange?.from);
    c.date(e.dateRange?.to);
    c.text(e.note);
    for (const exclusion of e.exclusions) c.text(exclusion);
  }
}

function metricKind(m: Metric, currency: string | undefined): { kind: DatasetFigure["kind"]; extra: { currency?: string; unit?: string } } {
  if (m.unit === "percent") return { kind: "percent", extra: {} };
  if (m.unit === "days") return { kind: "duration", extra: { unit: "day" } };
  if (m.unit === "currency") return { kind: "currency", extra: currency ? { currency } : {} };
  if (m.unit === "ratio") return { kind: "ratio", extra: {} };
  return { kind: "number", extra: {} };
}

function addMetric(set: FigureSet, m: Metric, currency: string | undefined): void {
  const c = new Collector(set, m.id);
  c.text(m.label);
  c.text(m.definition);
  for (const text of [...m.assumptions, ...m.exclusions]) c.text(text);
  for (const e of m.evidence) {
    c.number(e.count);
    c.date(e.dateRange?.from);
    c.date(e.dateRange?.to);
  }
  if (m.status === "not_measurable") return void c.text(m.reason);
  const { kind, extra } = metricKind(m, currency);
  const add = (value: number | null): void => {
    c.number(value, kind, extra);
    // A days figure with no word after it reads as a plain number in a table.
    if (kind === "duration") c.number(value, "number");
  };
  add(m.value);
  c.number(m.n);
  c.date(m.dateRange?.from);
  c.date(m.dateRange?.to);
  for (const [name, value] of Object.entries(m.stats)) {
    if (/^p\d+$|median|mean/i.test(name)) add(value);
    else if (/days/i.test(name)) c.number(value, "duration", { unit: "day" });
    else c.number(value);
  }
  for (const segment of m.segments) {
    c.label(segment.key);
    c.number(segment.n);
    add(segment.value);
    c.text(segment.notMeasurableReason);
  }
  for (const point of m.series) {
    c.number(point.n);
    add(point.value);
    c.date(point.date);
    c.text(point.notMeasurableReason);
  }
}

function addManualStep(set: FigureSet, s: ManualStep): void {
  const c = new Collector(set, s.id);
  for (const text of [s.title, s.whyItMatters, s.whereToGo, s.whatToCapture, s.ifSkipped]) c.text(text);
}

function addRun(set: FigureSet, run: Run): void {
  const c = new Collector(set, SOURCE.run);
  c.date(run.asOf);
  c.date(run.analysisWindow.from);
  c.date(run.analysisWindow.to);
  c.number(run.budget.callsMade);
  c.number(Math.round(run.budget.stopAtShare * 100), "percent");
  c.number(Number(run.apiVersion));
  set.identifiers.push({ source: SOURCE.run, value: run.id.toLowerCase() }, { source: SOURCE.run, value: run.ruleSetVersion.toLowerCase() });
}

/** Counts the engine works out for the summary sections. */
function addSummary(set: FigureSet, all: Finding[], metrics: Metric[], manualSteps: ManualStep[]): void {
  const c = new Collector(set, SOURCE.summary);
  // A report passes each finding twice, as written and at team level. Count once.
  const findings = [...new Map(all.map((f) => [f.id, f])).values()];
  const issues = findings.filter((f) => f.status === "issue");
  c.number(findings.length);
  c.number(issues.length);
  c.number(findings.filter((f) => f.status === "passed").length);
  c.number(findings.filter((f) => f.status === "not_measurable").length);
  c.number(metrics.length);
  c.number(metrics.filter((m) => m.status === "measured").length);
  c.number(manualSteps.length);
  c.number(manualSteps.filter((s) => s.status === "pending").length);
  for (const severity of ["critical", "high", "medium", "low", "info"]) c.number(issues.filter((f) => f.severity === severity).length);
  const byDomain = new Map<string, Finding[]>();
  for (const f of findings) byDomain.set(f.domain, [...(byDomain.get(f.domain) ?? []), f]);
  c.number(byDomain.size);
  for (const group of byDomain.values()) {
    c.number(group.length);
    c.number(group.filter((f) => f.status === "issue").length);
  }
}

/** An override replaces the figure it names. The old value can no longer be quoted from that source. */
function applyOverrides(set: FigureSet, overrides: NumberOverride[]): void {
  for (const override of overrides) {
    const hits = set.figures.filter((f) => f.source === override.target.id && f.value === override.previousValue);
    set.figures = set.figures.filter((f) => !hits.includes(f));
    const kinds = hits.length ? [...new Map(hits.map((h) => [`${h.kind}|${h.currency ?? ""}|${h.unit ?? ""}`, h])).values()] : [{ kind: "number" as const, source: override.target.id, value: 0 }];
    for (const hit of kinds) set.figures.push({ ...hit, source: override.target.id, value: override.newValue });
  }
}

export function buildDataset(input: DatasetInput): FigureSet {
  const set = emptySet();
  const steps = input.manualSteps ?? [];
  for (const finding of input.findings) addFinding(set, finding, input.currency);
  for (const metric of input.metrics) addMetric(set, metric, input.currency);
  for (const step of steps) addManualStep(set, step);
  if (input.run) addRun(set, input.run);
  addSummary(set, input.findings, input.metrics, steps);
  for (const score of input.scores ?? []) {
    if (score.status === "measured") {
      const c = new Collector(set, `score:${score.domain}`);
      c.number(score.score);
      c.number(score.score, "points");
      c.number(100);
    }
  }
  for (const extra of input.extraText ?? []) new Collector(set, extra.source).text(extra.text);
  for (const figure of input.structural ?? []) set.figures.push({ ...figure, source: SOURCE.structure });
  applyOverrides(set, input.overrides ?? []);
  return set;
}

/** Limits a dataset to some sources, so a section can only quote what it's built from. */
export function scopeDataset(set: FigureSet, sources: Iterable<string>): FigureSet {
  const allowed = new Set(sources);
  const keep = <T extends { source: string }>(rows: T[]): T[] => rows.filter((r) => allowed.has(r.source));
  return { figures: keep(set.figures), dates: keep(set.dates), identifiers: keep(set.identifiers), tokens: keep(set.tokens), labels: keep(set.labels) };
}
