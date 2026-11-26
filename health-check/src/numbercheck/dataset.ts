import type { Finding, ManualStep, Metric, NumberOverride, Run, Score } from "../schemas/index.ts";
import { extractFigures, inOrgWords, type Figure, type PartialDate } from "./extract.ts";
import { kindTallies } from "../narrative/unmeasured.ts";
import { passedOnMerit, unscoredVersion } from "../domains/scoring.ts";
import type { PseudonymEntry } from "../privacy/pseudonymiser.ts";
import { spansIfCurrent, type MarkedSentence, type OrgSpan } from "../engine/org-words.ts";

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
  /**
   * Set when the figure is only what someone told us, such as "said in
   * interview, Sales director". It can be quoted only as what was said.
   */
  said?: string;
  /**
   * Read from words the org chose, inside a text of ours (DECISIONS.md 764): a
   * stage named "Stage 9 of 12" puts 9 and 12 here. The documents' checks read
   * it as before; the digest's never does (orgFree), so the org's digits can't
   * make a number in our words count as traced.
   */
  org?: true;
};

/** A row of the set that isn't a figure. `org` as on DatasetFigure. */
type Row = { source: string; value: string; org?: true };

export type FigureSet = {
  figures: DatasetFigure[];
  dates: Array<PartialDate & { source: string; org?: true }>;
  identifiers: Row[];
  tokens: Row[];
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
  /**
   * Engine or consultant text the report may quote, such as the intake. `said` marks what a person told us.
   * `orgWords`: the text's marks, where the org's words in it are known; `org`: the whole text is someone
   * else's (a customer's or a person's words), never ours (DECISIONS.md 764).
   */
  extraText?: Array<{ source: string; text: string; said?: string; orgWords?: OrgSpan[] | null; org?: true }>;
  /** Names the org chose that a text with no marks may hold, such as its stage and record type names (DECISIONS.md 764). */
  orgNames?: string[];
};

export const SOURCE = { run: "run", summary: "summary", structure: "structure" } as const;

const emptySet = (): FigureSet => ({ figures: [], dates: [], identifiers: [], tokens: [], labels: [] });

/** Where the org's words lie in a text: its marks, or else every place a name the org chose is written whole. */
function orgRanges(text: string, spans: OrgSpan[] | null | undefined, names: readonly string[]): OrgSpan[] {
  if (spans) return spans;
  const out: OrgSpan[] = [];
  for (const name of names) {
    for (let at = text.indexOf(name); at !== -1; at = text.indexOf(name, at + 1)) {
      const end = at + name.length;
      if (!/\w/.test(text[at - 1] ?? "") && !/\w/.test(text[end] ?? "")) out.push({ start: at, end });
    }
  }
  return out;
}

/** The names, of those given, a text can hold a figure in: only those the checker reads something in. Longest first. */
export function orgNamesWithFigures(names: Iterable<string>): string[] {
  return [...new Set(names)].filter((n) => n.trim() !== "" && !n.startsWith("[[") && extractFigures(n).figures.length > 0).sort((a, b) => b.length - a.length);
}

/** The set as the digest checks against it (DECISIONS.md 764): nothing read from the org's words, and no label, which is only ever the org's. */
export function orgFree(set: FigureSet): FigureSet {
  const ours = <T extends { org?: true }>(rows: T[]): T[] => rows.filter((r) => !r.org);
  return { figures: ours(set.figures), dates: ours(set.dates), identifiers: ours(set.identifiers), tokens: ours(set.tokens), labels: [] };
}

/** A marking's spans while it still fits the text, or null. */
const current = (text: string | null | undefined, marked: MarkedSentence | null | undefined): OrgSpan[] | null => (text && marked ? spansIfCurrent(text, marked) : null);

class Collector {
  readonly set = emptySet();
  readonly source: string;
  readonly said: string | undefined;
  readonly names: readonly string[];

  constructor(set: FigureSet, source: string, said?: string, names: readonly string[] = []) {
    this.set = set;
    this.source = source;
    this.said = said;
    this.names = names;
  }

  number(value: number | null | undefined, kind: DatasetFigure["kind"] = "number", extra: { currency?: string; unit?: string; org?: true } = {}): void {
    if (value === null || value === undefined || !Number.isFinite(value)) return;
    const { org, ...rest } = extra;
    this.set.figures.push({ source: this.source, kind, value, ...rest, ...(this.said ? { said: this.said } : {}), ...(org ? { org } : {}) });
  }

  date(iso: string | null | undefined): void {
    if (!iso) return;
    const [y, m, d] = iso.slice(0, 10).split("-").map(Number);
    this.set.dates.push({ source: this.source, y: y!, m: m!, d: d! });
  }

  label(value: string): void {
    if (/\d/.test(value)) this.set.labels.push({ source: this.source, value });
  }

  /**
   * The letters-and-digits words in a name the org chose, as identifiers and
   * nothing else, never a number (DECISIONS.md 682). A page prints the parts of
   * a name a segment holds whole ("Invoice2__c" from "Invoice2__c.Account__c to
   * Account", an object from a dimension), which its label alone doesn't mask.
   */
  words(name: string): void {
    // "Object.Field" is read as one word, and a page can print either half alone.
    // Never an email's or a link's: their pieces must not become words we accept.
    const halves = /@|:\/\//.test(name) ? [] : [name.replace(/\./g, " ")];
    for (const text of new Set([name, ...halves])) {
      for (const figure of extractFigures(text).figures) if (figure.word) this.set.identifiers.push({ source: this.source, value: figure.raw.toLowerCase(), org: true });
    }
  }

  /**
   * Reads figures out of engine text, using the same reader the check uses.
   * Each figure inside the org's words (its marks, or else a name the org chose
   * written in it) is the org's, `org` (inOrgWords, DECISIONS.md 764). `org`
   * true: the whole text is someone else's. Returns the figures of ours.
   */
  text(text: string | null | undefined, marks?: OrgSpan[] | null, org = false): Figure[] {
    if (!text) return [];
    const { figures } = extractFigures(text);
    const theirs = org ? figures.map(() => true) : inOrgWords(text, figures, orgRanges(text, marks, this.names));
    const ours = (f: Figure): boolean => !theirs[figures.indexOf(f)];
    for (const figure of figures) {
      const flag = ours(figure) ? {} : { org: true as const };
      if (figure.kind === "token") this.set.tokens.push({ source: this.source, value: figure.raw, ...flag });
      else if (figure.kind === "identifier") this.set.identifiers.push({ source: this.source, value: figure.raw.toLowerCase(), ...flag });
      else if (figure.kind === "date" && figure.date) this.set.dates.push({ source: this.source, ...figure.date, ...flag });
      else if (figure.kind === "fraction" && figure.value !== undefined) this.number(figure.value, "percent", flag);
      else if (figure.value !== undefined && figure.kind !== "malformed" && figure.kind !== "magnitude") {
        this.number(figure.value, figure.kind as DatasetFigure["kind"], { ...(figure.currency ? { currency: figure.currency } : {}), ...(figure.unit ? { unit: figure.unit } : {}), ...flag });
      }
    }
    return figures.filter(ours);
  }
}

// A fact the engine wrote out is already in the set, in the unit it was written.
// Only one the text never shows needs its unit worked out from its name.
function addFact(c: Collector, name: string, value: number, written: Figure[]): void {
  // `written` is our words' figures only: one the org's name holds says nothing of the fact.
  if (written.some((f) => f.value === value && f.kind !== "malformed")) return;
  c.number(value, /rate|share|percent|pct/i.test(name) ? "percent" : "number");
}

function addFinding(set: FigureSet, f: Finding, currency: string | undefined, names: readonly string[]): void {
  const c = new Collector(set, f.id, undefined, names);
  const m = f.orgWords;
  // Only what our words write: a fact written only inside the org's words is added as a figure of its own.
  const written = [
    ...c.text(f.title, current(f.title, m?.title)),
    ...c.text(f.summary, current(f.summary, m?.summary)),
    ...c.text(f.recommendation, current(f.recommendation, m?.recommendation)),
    ...c.text(f.notMeasurableReason, current(f.notMeasurableReason, m?.notMeasurableReason)),
    ...[f.effort.note, ...f.dependencies].flatMap((text) => c.text(text)),
  ];
  // A guard's detail and an evidence note carry no marks, and quote what the org named freely: a report's
  // path ("reports/Pipeline/Top 10 Deals.report"), a file, a component. The digest serves neither, so none
  // of their figures is one the digest may trace to (DECISIONS.md 764); the documents read them as before.
  for (const guard of f.guards) c.text(guard.detail, null, true);
  for (const subject of f.subjects) {
    c.label(subject.label);
    c.words(subject.label);
    c.text(subject.label.startsWith("[[") ? subject.label : null);
  }
  for (const segment of f.segmentsChecked) c.number(segment.segments);
  if (f.value.kind === "not_sized") c.text(f.value.reason, current(f.value.reason, m?.valueReason));
  else {
    if (f.value.kind === "hours") c.number(f.value.amount, "duration", { unit: "hour" });
    else c.number(f.value.amount, "currency", { currency: /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : (currency ?? f.value.unit) });
    c.text(f.value.period, current(f.value.period, m?.valuePeriod));
    const held = m?.valueAssumptions?.length === f.value.assumptions.length ? m.valueAssumptions : undefined;
    f.value.assumptions.forEach((assumption, i) => c.text(assumption, current(assumption, held?.[i])));
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
    c.text(e.note, null, true);
    for (const exclusion of e.exclusions) c.text(exclusion, null, true);
  }
}

function metricKind(m: Metric, currency: string | undefined): { kind: DatasetFigure["kind"]; extra: { currency?: string; unit?: string } } {
  if (m.unit === "percent") return { kind: "percent", extra: {} };
  if (m.unit === "days") return { kind: "duration", extra: { unit: "day" } };
  if (m.unit === "currency") return { kind: "currency", extra: currency ? { currency } : {} };
  if (m.unit === "ratio") return { kind: "ratio", extra: {} };
  return { kind: "number", extra: {} };
}

function addMetric(set: FigureSet, m: Metric, currency: string | undefined, names: readonly string[]): void {
  const c = new Collector(set, m.id, undefined, names);
  const w = m.orgWords;
  c.text(m.label, current(m.label, w?.label));
  c.text(m.definition, current(m.definition, w?.definition));
  const listed = (texts: string[], marks: Array<MarkedSentence | null> | undefined) => texts.forEach((text, i) => c.text(text, marks?.length === texts.length ? current(text, marks[i]) : null));
  listed(m.assumptions, w?.assumptions);
  listed(m.exclusions, w?.exclusions);
  for (const e of m.evidence) {
    c.number(e.count);
    c.date(e.dateRange?.from);
    c.date(e.dateRange?.to);
  }
  if (m.status === "not_measurable") return void c.text(m.reason, current(m.reason, w?.reason));
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
    c.words(segment.key);
    c.words(segment.dimension);
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

function addManualStep(set: FigureSet, s: ManualStep, names: readonly string[]): void {
  const c = new Collector(set, s.id, undefined, names);
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
  const passed = findings.filter((f) => f.status === "passed").length;
  c.number(passed);
  // The passes the outputs count: never a version shown and not scored (DECISIONS.md 778, 779). Only where it differs, so a run without one has the set it had.
  const onMerit = findings.filter(passedOnMerit).length;
  if (onMerit !== passed) c.number(onMerit);
  c.number(findings.filter((f) => f.status === "not_measurable").length);
  // What couldn't be measured, by kind (DECISIONS.md 627).
  for (const n of kindTallies(findings)) c.number(n);
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
    const scored = group.filter((f) => !unscoredVersion(f)).length;
    if (scored !== group.length) c.number(scored);
    c.number(group.filter((f) => f.status === "issue").length);
    for (const n of kindTallies(group)) c.number(n);
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
  // Every name the org chose that a text with no marks may hold (DECISIONS.md 764).
  const names = orgNamesWithFigures([
    ...(input.orgNames ?? []),
    ...input.findings.flatMap((f) => f.subjects.map((s) => s.label)),
    ...input.metrics.flatMap((m) => (m.status === "measured" ? m.segments.flatMap((s) => [s.key, s.dimension]) : [])),
  ]);
  for (const finding of input.findings) addFinding(set, finding, input.currency, names);
  for (const metric of input.metrics) addMetric(set, metric, input.currency, names);
  for (const step of steps) addManualStep(set, step, names);
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
  for (const extra of input.extraText ?? []) new Collector(set, extra.source, extra.said, names).text(extra.text, extra.orgWords, extra.org === true);
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

/** A Salesforce record id, 15 or 18 letters and digits. */
const RECORD_ID = /^[A-Za-z0-9]{15}(?:[A-Za-z0-9]{3})?$/;

/**
 * The set a printed page is checked against: the report's, with the record id
 * each of its tokens restores to when the run's pseudonym map labels a record
 * by its own id (DECISIONS.md 683). The engine doesn't read an account's name,
 * so "[[account:A0001]]" restores to the account's id; a section's check sees
 * the token, which is in the data, and the page prints the id. Only ids from
 * the run's own map, and only for tokens the data holds, so a run already made
 * is covered when its documents are made again, and no other id-shaped word is.
 * A name that isn't an id is left as it was.
 */
export function withRestoredIds(set: FigureSet, entries: PseudonymEntry[]): FigureSet {
  const tokens = new Set(set.tokens.map((t) => t.value));
  const ids = entries.filter((e) => e.name === e.id && RECORD_ID.test(e.id) && tokens.has(e.token)).map((e) => ({ source: e.token, value: e.id.toLowerCase() }));
  return ids.length ? { ...set, identifiers: [...set.identifiers, ...ids] } : set;
}
