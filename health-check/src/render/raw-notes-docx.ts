import { AlignmentType, BorderStyle, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, ShadingType, Table, TableCell, TableRow, TextRun, WidthType } from "docx";
import { colour, font } from "../design/tokens.ts";
import { currentBody, type BuiltReport } from "../narrative/sections.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Calibration, type Evidence, type Finding, type ManualStep, type Metric, type NumberOverride } from "../schemas/index.ts";
import { longDate, money, plainWords, ukDates } from "./format.ts";
import { INVENTORY_COLUMNS, inventoryRows, OBJECT_INVENTORY } from "../domains/configuration/inventory.ts";
import { inventoryLine } from "../notes/raw-notes.ts";
import { noteSections, rawNotesOrder, valueItemId } from "./references.ts";
import { KIND_LABELS, type ValueGroup } from "../value/model.ts";
import { comparisonId, comparisonNotes, formatFigure, type Comparison } from "../benchmark/compare.ts";
import { domainLabel, domainsInOrder } from "../narrative/select.ts";
import { OURS, practiceFor, practiceId } from "../practice/index.ts";
import { TRANSITIONS, transitionMatrix } from "../domains/historic-pipeline/process.ts";
import { DOCUMENTED, readDocument, sharingLabel } from "../domains/configuration/documented.ts";

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
    this.#restore = (text) => plainWords(ukDates(pseudonyms.restore(text)));
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

function addFinding(notes: Notes, f: Finding, currency: string, ref: string): void {
  notes.heading(`${ref} ${f.title}`, 3);
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

function addValueGroup(notes: Notes, g: ValueGroup, currency: string, ref: string): void {
  const m = (n: number) => money(n, currency);
  const pct = (n: number) => `${Math.round(n * 100)}%`;
  notes.heading(`${ref} ${g.label}`, 3);
  notes.labelled("Kind", g.kind === "exposure" ? "Exposure, shown beside the total and never added to it" : KIND_LABELS[g.kind]);
  if (g.kind !== "exposure") notes.labelled("Share we count", `${pct(g.realisation.low)} in the low case, ${pct(g.realisation.expected)} expected, ${pct(g.realisation.high)} in the high case`);
  if (g.lines.length > 1) notes.labelled("Counted once", "These figures describe the same money, so only the largest counts.");
  notes.table(
    ["Finding", "Figure over 12 months", "How", "Low", "Expected", "High", "Counted"],
    g.lines.map((l) => [`${l.title} (${l.findingId})`, m(l.annual), l.basis, m(l.low), m(l.expected), m(l.high), l === g.counted ? "Yes" : "No, overlaps"]),
    [2500, 1200, 2000, 900, 900, 900, 900],
  );
}

function addMetric(notes: Notes, m: Metric, ref: string, calibration: Calibration): void {
  if (m.status === "not_measurable") {
    notes.heading(`${ref} ${m.label}`, 3);
    notes.labelled("Not measurable", m.reason);
    notes.labelled("Definition", m.definition);
    return;
  }
  notes.heading(`${ref} ${m.label}: ${m.value}${m.unit === "percent" ? "%" : m.unit === "days" ? " days" : ""}`, 3);
  notes.labelled("Definition", m.definition);
  notes.labelled("Sample", `${m.n}${m.dateRange ? `, ${m.dateRange.from} to ${m.dateRange.to}` : ""}`);
  if (m.id === OBJECT_INVENTORY) notes.labelled("In short", inventoryLine(m) ?? "");
  if (Object.keys(m.stats).length) notes.labelled("Detail", Object.entries(m.stats).map(([k, v]) => `${k} ${v}`).join(", "));
  if (m.exclusions.length) notes.labelled("Excludes", m.exclusions.join("; "));
  if (m.assumptions.length) notes.labelled("Assumptions", m.assumptions.join("; "));
  if (m.id === OBJECT_INVENTORY) {
    // Every object, with its own columns: the full report shows only the busiest.
    const headings = ["Records", "Fields", "Record types", "Layouts", "Validation rules", "Flows", "Process Builders", "Workflow Rules", "Triggers", "No description"];
    notes.table(["Object", ...headings], inventoryRows(m).map((row) => [row.object, ...INVENTORY_COLUMNS.map((c) => (row.counts[c.key] === null ? "Not read" : String(row.counts[c.key])))]), [1606, 900, 700, 700, 700, 700, 600, 700, 700, 700, 800]);
    const unread = inventoryRows(m).flatMap((row) => INVENTORY_COLUMNS.filter((c) => row.counts[c.key] === null).map((c) => `${row.object}, ${c.dimension}: ${row.reasons[c.key] ?? "not read"}`));
    if (unread.length) notes.labelled("Not read", unread.join("; "));
  } else if (m.id === TRANSITIONS) {
    // From stage to stage, as a table: a list of every pair is hard to read.
    const { from, to, count } = transitionMatrix(m, calibration);
    const width = Math.floor(7300 / to.length);
    notes.table(["From", ...to], from.map((f) => [f, ...to.map((t) => String(count(f, t) ?? ""))]), [2000, ...to.map(() => width)]);
  } else if (m.segments.length) notes.table(["Segment", "Key", "Sample", "Value"], m.segments.map((s) => [s.dimension, s.key, String(s.n), s.value === null ? (s.notMeasurableReason ?? "Not measurable") : String(s.value)]), [2300, 3200, 1000, 2800]);
  if (m.series.length) notes.table(["Month end", "Value", "Sample"], m.series.map((p) => [p.date, p.value === null ? (p.notMeasurableReason ?? "Not measurable") : String(p.value), String(p.n)]), [2300, 5000, 2000]);
}

/** The documented org, every list in full. A part that couldn't be measured says why, as any metric does. */
function addDocumented(notes: Notes, metrics: Metric[], ids: string[], ref: (id: string) => string, calibration: Calibration): void {
  const doc = readDocument(metrics);
  const yes = (b: boolean): string => (b ? "Yes" : "No");
  for (const id of ids) {
    const m = metrics.find((x) => x.id === id);
    if (!m) continue;
    if (m.status !== "measured") {
      addMetric(notes, m, ref(id), calibration);
      continue;
    }
    notes.heading(`${ref(id)} ${m.label}: ${m.value}`, 3);
    notes.labelled("Definition", m.definition);
    if (m.exclusions.length) notes.labelled("Excludes", m.exclusions.join("; "));
    if (m.assumptions.length) notes.labelled("Assumptions", m.assumptions.join("; "));
    if (id === DOCUMENTED.model && doc.model) {
      notes.table(["Object", "Records", "Custom fields", "Record types"], doc.model.objects.map((o) => [o.name, o.records === null ? "No count kept" : String(o.records), o.customFields === null ? "Not read" : String(o.customFields), o.recordTypes.map((t) => `${t.name}${t.active ? "" : " (inactive)"}`).join(", ") || "None"]), [2400, 1300, 1300, 4300]);
      const filled = doc.model.objects.filter((o) => o.mostFilled.length);
      if (filled.length) notes.table(["Object", "Most-filled custom field", "Filled", "Records"], filled.flatMap((o) => o.mostFilled.map((f) => [o.name, f.field, `${f.filledPct}%`, String(f.total)])), [2000, 4300, 1300, 1700]);
    }
    if (id === DOCUMENTED.relationships && doc.relationships) notes.table(["From", "Field", "To", "Kind"], doc.relationships.links.map((l) => [l.from, l.field, l.to, l.kind === "master-detail" ? "Master-detail" : "Lookup"]), [2300, 2800, 2300, 1900]);
    if (id === DOCUMENTED.automation && doc.automation) {
      for (const o of doc.automation.objects) {
        notes.labelled(o.object, `${o.steps.reduce((sum, s) => sum + s.items.length, 0)} items`);
        notes.table(["When it runs", "Name", "Active", "Description"], o.steps.flatMap((s) => s.items.map((i) => [`${s.label}${s.step === null ? "" : ` (step ${s.step})`}`, i.name, yes(i.active), i.described === null ? "Not read" : yes(i.described)])), [2800, 3700, 1200, 1600]);
      }
    }
    if (id === DOCUMENTED.integrations && doc.integrations) {
      const i = doc.integrations;
      if (i.connectedApps.length) notes.table(["Connected app", "Who can use it"], i.connectedApps.map((a) => [a.name, a.adminApprovedOnly ? "Admin-approved users only" : "Users can authorise it themselves"]), [4600, 4700]);
      if (i.namedCredentials.length) notes.table(["Named credential"], i.namedCredentials.map((c) => [c]), [9300]);
      if (i.remoteSites.length) notes.table(["Remote site", "Active"], i.remoteSites.map((r) => [r.host, yes(r.active)]), [7000, 2300]);
      if (i.integrationUsers.length) notes.table(["Integration users", "Active users"], i.integrationUsers.map((u) => [u.group, String(u.users)]), [7000, 2300]);
      if (i.apiLogins.length) notes.table(["API logins in the last 90 days, by application", "Logins", "Users"], i.apiLogins.map((a) => [a.application, String(a.logins), String(a.users)]), [5500, 1900, 1900]);
    }
    if (id === DOCUMENTED.access && doc.access) {
      const a = doc.access;
      if (a.roleCount !== null && a.roleDepth !== null) notes.labelled("Role hierarchy", `${a.roleCount} roles, ${a.roleDepth} levels deep`);
      notes.table(["Profile", "Active users"], a.profiles.map((p) => [p.name, String(p.users)]), [7000, 2300]);
      notes.table(["Role", "Active users"], a.roles.map((p) => [p.name, String(p.users)]), [7000, 2300]);
      if (a.permissionSets.length) notes.table(["Permission set", "Active users holding it"], a.permissionSets.map((p) => [p.name, String(p.users)]), [7000, 2300]);
    }
    if (id === DOCUMENTED.sharing && doc.sharing) notes.table(["Object", "Internal default", "External default"], doc.sharing.objects.map((o) => [o.object, o.internal ? sharingLabel(o.internal) : "Not set", o.external ? sharingLabel(o.external) : "Not set"]), [3100, 3100, 3100]);
  }
}

/** Each area's statements, with where each comes from. Black Cloud's own practice says so. */
function addPractice(notes: Notes, domains: string[], ref: (id: string) => string): void {
  for (const domain of domains) {
    const items = practiceFor(domain);
    if (items.length === 0) continue;
    notes.heading(domainLabel(domain), 3);
    items.forEach((p, i) => {
      notes.labelled(ref(practiceId(domain, i)), p.text);
      if (!p.source) return notes.labelled("Source", `${OURS}, not a Salesforce source`);
      notes.labelled("Source", `${p.source.name}: ${p.source.title}. Read on ${p.source.checked}`);
      notes.labelled("What it says", p.source.says);
      notes.bullet(p.source.url, true);
    });
  }
}

function addComparison(notes: Notes, c: Comparison, ref: string): void {
  notes.heading(`${ref} ${c.metric.label}${c.value === null ? "" : `: ${formatFigure(c.value, c.metric.unit)}`}`, 3);
  for (const line of comparisonNotes(c)) notes.labelled(line.label, line.text);
}

function addManualStep(notes: Notes, s: ManualStep, ref: string): void {
  notes.heading(`${ref} ${s.title} (${s.status})`, 3);
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
  notes.para("Every figure in the executive summary and the full report carries a reference such as R4.3. R4 is section 4 of these notes, and R4.3 is the third item in it.");

  const sections = noteSections(report);
  notes.labelled("Contents", "");
  for (const s of sections) notes.bullet(`${s.ref} ${s.title}`);

  for (const s of sections) {
    const ref = (id: string): string => s.items.find((item) => item.id === id)?.ref ?? s.ref;
    notes.heading(`${s.ref} ${s.title}`, 2);
    notes.para(currentBody(built, s.key));
    if (s.key === "manual_steps") manualSteps.forEach((step) => addManualStep(notes, step, ref(step.id)));
    if (s.key.startsWith("findings_")) rawNotesOrder(findings.filter((f) => s.items.some((item) => item.id === f.id))).forEach((f) => addFinding(notes, f, calibration.currency, ref(f.id)));
    if (s.key === "pipeline_metrics" || s.key === "investment" || s.key === "adoption" || s.key === "sales_process") metrics.filter((m) => s.items.some((item) => item.id === m.id)).forEach((m) => addMetric(notes, m, ref(m.id), calibration));
    if (s.key === "practice") addPractice(notes, domainsInOrder(report.teamFindings), ref);
    if (s.key === "org_documented") addDocumented(notes, metrics, s.items.map((item) => item.id), ref, calibration);
    if (s.key === "value_model") report.value.groups.forEach((g) => addValueGroup(notes, g, calibration.currency, ref(valueItemId(g.id))));
    if (s.key === "comparisons") report.comparisons.forEach((c) => addComparison(notes, c, ref(comparisonId(c.metric.key))));
    if (s.key === "queries") addQueries(notes, findings, metrics);
    if (s.key === "overrides") addOverrides(notes, overrides);
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
