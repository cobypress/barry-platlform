import { AlignmentType, BorderStyle, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, ShadingType, Table, TableCell, TableRow, TextRun, WidthType } from "docx";
import { colour, font } from "../design/tokens.ts";
import { currentBody, type BuiltReport } from "../narrative/sections.ts";
import { issuesOf } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Evidence, type Finding, type ManualStep, type Metric, type NumberOverride } from "../schemas/index.ts";
import { longDate, money, ukDates } from "./format.ts";

// The working record behind the reports: every check, what it found, and how to
// reproduce it. Tokens become names here, at the last moment, because this is
// what the customer's admin reads. Nothing else in the pipeline sees a name.

const hex = (value: string): string => value.replace("#", "");
const CARBON = hex(colour.carbon);
const FONT = font.family;

type Block = Paragraph | Table;

function evidenceSource(e: Evidence): string {
  if (e.kind === "soql") return `${e.api === "tooling" ? "Tooling query" : "Query"}: ${e.query}`;
  if (e.kind === "manual") return `Manual step ${e.manualStepId} (${e.status}${e.answeredBy ? `, by ${e.answeredBy} on ${e.answeredAt?.slice(0, 10)}` : ""})`;
  if (e.kind === "metadata") return `Metadata: ${e.path}`;
  if (e.kind === "rest") return `API: ${e.path}`;
  return `Engine: ${e.path}`;
}

function evidenceDetail(e: Evidence): string {
  return [
    e.count === null ? "Count: not available" : `Count: ${e.count}`,
    e.dateRange ? `Period: ${e.dateRange.from} to ${e.dateRange.to}` : "Period: not date bound",
    e.exclusions.length ? `Excludes: ${e.exclusions.join("; ")}` : "Excludes: nothing",
    ...(e.note ? [e.note] : []),
  ].join(" | ");
}

class Notes {
  readonly #restore: (text: string) => string;
  readonly #name: (text: string) => string;
  readonly blocks: Block[] = [];

  constructor(pseudonyms: Pseudonymiser) {
    this.#name = (text) => pseudonyms.restore(text);
    this.#restore = (text) => ukDates(pseudonyms.restore(text));
  }

  run(text: string, options: { bold?: boolean; code?: boolean; colour?: string } = {}): TextRun {
    // A query is kept exactly as it ran, so it can be pasted back in.
    return new TextRun({ text: options.code ? this.#name(text) : this.#restore(text), bold: options.bold, font: options.code ? "Courier New" : FONT, size: options.code ? 16 : 20, color: options.colour ?? CARBON });
  }

  heading(text: string, level: 1 | 2 | 3): void {
    const map = { 1: HeadingLevel.HEADING_1, 2: HeadingLevel.HEADING_2, 3: HeadingLevel.HEADING_3 };
    this.blocks.push(new Paragraph({ heading: map[level], keepNext: true, spacing: { before: level === 3 ? 200 : 320, after: 80 }, children: [new TextRun({ text: this.#restore(text), font: FONT, bold: true, size: { 1: 32, 2: 26, 3: 22 }[level], color: CARBON })] }));
  }

  para(text: string): void {
    for (const line of text.split(/\n+/)) if (line.trim()) this.blocks.push(new Paragraph({ spacing: { after: 100 }, children: [this.run(line)] }));
  }

  labelled(label: string, text: string): void {
    this.blocks.push(new Paragraph({ spacing: { after: 80 }, children: [this.run(`${label}: `, { bold: true }), this.run(text)] }));
  }

  bullet(text: string, code = false): void {
    this.blocks.push(new Paragraph({ bullet: { level: 0 }, spacing: { after: 40 }, children: [this.run(text, { code })] }));
  }

  table(headers: string[], rows: string[][], widths: number[]): void {
    const total = widths.reduce((a, b) => a + b, 0);
    const edge = { style: BorderStyle.SINGLE, size: 4, color: hex(colour.rule) };
    const borders = { top: edge, bottom: edge, left: edge, right: edge };
    const cell = (text: string, width: number, head: boolean): TableCell =>
      new TableCell({
        width: { size: width, type: WidthType.DXA },
        borders,
        margins: { top: 40, bottom: 40, left: 80, right: 80 },
        ...(head ? { shading: { type: ShadingType.CLEAR, fill: CARBON, color: "auto" } } : {}),
        children: [new Paragraph({ children: [new TextRun({ text: this.#restore(text), font: FONT, size: 18, bold: head, color: head ? hex(colour.ivory) : CARBON })] })],
      });
    this.blocks.push(
      new Table({
        width: { size: total, type: WidthType.DXA },
        columnWidths: widths,
        rows: [new TableRow({ tableHeader: true, children: headers.map((h, i) => cell(h, widths[i]!, true)) }), ...rows.map((row) => new TableRow({ cantSplit: true, children: row.map((c, i) => cell(c, widths[i]!, false)) }))],
      }),
      new Paragraph({ spacing: { after: 120 }, children: [] }),
    );
  }
}

function valueLine(f: Finding, currency: string): string {
  if (f.value.kind === "not_sized") return `Not sized. ${f.value.reason}`;
  const amount = f.value.kind === "hours" ? `${f.value.amount} hours` : money(f.value.amount, /^[A-Z]{3}$/.test(f.value.unit) ? f.value.unit : currency);
  return `${amount} ${f.value.period}. Assumptions: ${f.value.assumptions.join("; ")}.`;
}

function addFinding(notes: Notes, f: Finding, currency: string): void {
  notes.heading(f.title, 3);
  notes.para(`${f.ruleId}, version ${f.ruleVersion}. Finding ${f.id}.`);
  if (f.status === "issue") notes.labelled("Rating", `Severity ${f.severity}, confidence ${f.confidence}, effort ${f.effort.size}, owner ${f.owner}`);
  else notes.labelled("Result", f.status === "passed" ? "Passed" : "Not measurable");
  notes.para(f.summary);
  if (f.recommendation) notes.labelled(`Recommendation (${f.recommendationType})`, f.recommendation);
  if (f.status === "issue") notes.labelled("Value", valueLine(f, currency));
  if (f.dependencies.length) notes.labelled("Dependencies", f.dependencies.join("; "));
  if (f.subjects.length) notes.labelled("Applies to", f.subjects.map((s) => s.label).join(", "));
  if (f.segmentsChecked.length) notes.labelled("Segments checked", f.segmentsChecked.map((s) => `${s.dimension} (${s.segments})`).join(", "));
  if (f.segmentExemptReason) notes.labelled("Not segmented because", f.segmentExemptReason);
  notes.labelled("Evidence", "");
  for (const e of f.evidence) {
    notes.bullet(evidenceSource(e), e.kind === "soql");
    notes.bullet(evidenceDetail(e));
  }
  if (f.guards.length) notes.table(["Subject", "Guard", "Result", "Detail"], f.guards.map((g) => [g.subject, g.guardId, g.status, g.detail]), [2000, 2400, 900, 4000]);
}

function addMetric(notes: Notes, m: Metric): void {
  if (m.status === "not_measurable") {
    notes.heading(m.label, 3);
    notes.labelled("Not measurable", m.reason);
    notes.labelled("Definition", m.definition);
    return;
  }
  notes.heading(`${m.label}: ${m.value}${m.unit === "percent" ? "%" : m.unit === "days" ? " days" : ""}`, 3);
  notes.labelled("Definition", m.definition);
  notes.labelled("Sample", `${m.n}${m.dateRange ? `, ${m.dateRange.from} to ${m.dateRange.to}` : ""}`);
  if (Object.keys(m.stats).length) notes.labelled("Detail", Object.entries(m.stats).map(([k, v]) => `${k} ${v}`).join(", "));
  if (m.exclusions.length) notes.labelled("Excludes", m.exclusions.join("; "));
  if (m.assumptions.length) notes.labelled("Assumptions", m.assumptions.join("; "));
  if (m.segments.length) notes.table(["Segment", "Key", "Sample", "Value"], m.segments.map((s) => [s.dimension, s.key, String(s.n), s.value === null ? (s.notMeasurableReason ?? "Not measurable") : String(s.value)]), [2300, 3200, 1000, 2800]);
  if (m.series.length) notes.table(["Month end", "Value", "Sample"], m.series.map((p) => [p.date, p.value === null ? (p.notMeasurableReason ?? "Not measurable") : String(p.value), String(p.n)]), [2300, 5000, 2000]);
}

function addManualStep(notes: Notes, s: ManualStep): void {
  notes.heading(`${s.title} (${s.status})`, 3);
  notes.labelled("Why it matters", s.whyItMatters);
  notes.labelled("Where to go", s.whereToGo);
  notes.labelled("What to capture", s.whatToCapture);
  notes.labelled("If skipped", s.ifSkipped);
}

function addQueries(notes: Notes, findings: Finding[], metrics: Metric[]): void {
  const seen = new Map<string, Evidence>();
  for (const e of [...findings.flatMap((f) => f.evidence), ...metrics.flatMap((m) => m.evidence)]) {
    const key = evidenceSource(e);
    if (e.kind !== "engine" && e.kind !== "manual" && !seen.has(key)) seen.set(key, e);
  }
  for (const [source] of [...seen].sort(([a], [b]) => a.localeCompare(b))) notes.bullet(source, true);
}

function addOverrides(notes: Notes, overrides: NumberOverride[]): void {
  if (overrides.length) {
    notes.table(["Target", "Field", "From", "To", "Reason", "By", "When"], overrides.map((o) => [`${o.target.type} ${o.target.id}`, o.target.field, String(o.previousValue), String(o.newValue), o.reason, o.overriddenBy, longDate(o.overriddenAt)]), [1900, 1100, 700, 700, 2600, 1300, 1000]);
  }
}

export function buildRawNotes(report: BuiltReport, pseudonyms: Pseudonymiser): Notes {
  const { run, calibration, findings, metrics, manualSteps, overrides = [] } = report.input;
  const built = report.outputs.raw_notes;
  const notes = new Notes(pseudonyms);
  const release = canRelease(built.sections);

  notes.heading("Black Cloud Barometer: raw notes", 1);
  notes.para(`Run ${run.id}. As of ${run.asOf.slice(0, 10)}. Rule set ${run.ruleSetVersion}. API version ${run.apiVersion}.`);
  if (!release.ok) notes.blocks.push(new Paragraph({ spacing: { after: 160 }, children: [notes.run("Draft: not every section has been approved, so this document can't be released.", { bold: true, colour: hex(colour.alert) })] }));
  notes.para("These notes are the working record behind the reports: every check we ran, what it found and how to reproduce it.");

  for (const section of built.sections) {
    notes.heading(section.title, 2);
    notes.para(currentBody(built, section.key));
    if (section.key === "manual_steps") manualSteps.forEach((s) => addManualStep(notes, s));
    if (section.key.startsWith("findings_")) {
      const own = findings.filter((f) => section.findingIds.includes(f.id));
      const order = [...issuesOf(own), ...own.filter((f) => f.status === "not_measurable"), ...own.filter((f) => f.status === "passed")];
      order.forEach((f) => addFinding(notes, f, calibration.currency));
    }
    if (section.key === "pipeline_metrics") metrics.forEach((m) => addMetric(notes, m));
    if (section.key === "queries") addQueries(notes, findings, metrics);
    if (section.key === "overrides") addOverrides(notes, overrides);
  }
  return notes;
}

export async function renderRawNotesDocx(report: BuiltReport, pseudonyms: Pseudonymiser): Promise<Buffer> {
  const notes = buildRawNotes(report, pseudonyms);
  const doc = new Document({
    title: "Black Cloud Barometer: raw notes",
    creator: "Black Cloud",
    styles: { default: { document: { run: { font: FONT, size: 20, color: CARBON } } } },
    sections: [
      {
        properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, bottom: 1200, left: 1300, right: 1300 } } },
        footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ children: ["Page ", PageNumber.CURRENT], font: FONT, size: 16, color: hex(colour.muted) })] })] }) },
        children: notes.blocks,
      },
    ],
  });
  return Packer.toBuffer(doc);
}
