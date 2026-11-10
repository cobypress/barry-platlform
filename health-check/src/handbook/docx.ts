import { AlignmentType, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, TextRun } from "docx";
import { colour, font } from "../design/tokens.ts";
import { longDate } from "../render/format.ts";
import type { Handbook, HandbookCondition, HandbookFlow, HandbookStep, HandbookValidationRule, ValidationRuleCoverage } from "./schema.ts";

// The org handbook as a Word document (DECISIONS.md 443 and 444): handbook.json
// made readable, and nothing more. Code alone writes it, from the file: no
// Claude text, no figure from the value model and no name added. The admin's
// own text prints as written, so a reader can trust that what isn't here
// doesn't exist, or is said to be missing.

/** The document in the run's documents folder, beside the reports. */
export const HANDBOOK_DOCX = "org-handbook.docx";

const hex = (value: string): string => value.replace("#", "");
const CARBON = hex(colour.carbon);
const MUTED = hex(colour.muted);
const FONT = font.family;
const CODE = "Courier New";

/**
 * Text Word can hold. XML 1.0 has no place for most control characters, a lone
 * half of a surrogate pair, U+FFFE or U+FFFF, and one of them makes a file Word
 * calls corrupt. The docx package drops the control characters silently but
 * not the rest, so each becomes U+FFFD here: the reader sees something was
 * there. Tab, line feed and carriage return are XML's own and are kept.
 */
export function wordSafe(text: string): string {
  return text.replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F￾￿]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g, "�");
}

const count = (n: number): string => n.toLocaleString("en-GB");
const plural = (n: number, one: string, many = `${one}s`): string => `${count(n)} ${n === 1 ? one : many}`;
const sentence = (text: string): string => (/[.!?]$/.test(text.trim()) ? text.trim() : `${text.trim()}.`);
/** "A, B and C", as a reader would write it. */
const listed = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);

type Run = { text: string; bold?: boolean; code?: boolean; muted?: boolean };

/** The document as it's built: every piece of text goes through `wordSafe` on its way in. */
class Doc {
  readonly blocks: Paragraph[] = [];

  #run(r: Run, breakBefore = false): TextRun {
    return new TextRun({ text: wordSafe(r.text), bold: r.bold, font: r.code ? CODE : FONT, size: r.code ? 18 : 20, color: r.muted ? MUTED : CARBON, ...(breakBefore ? { break: 1 } : {}) });
  }

  /** Runs, with a line break wherever the text has one, so the admin's lines stay their own. */
  #runs(parts: Run[], breakFirst = false): TextRun[] {
    const out: TextRun[] = [];
    parts.forEach((part, p) => {
      part.text
        .replace(/\r\n?/g, "\n")
        .split("\n")
        .forEach((line, i) => out.push(this.#run({ ...part, text: part.code ? line.replace(/\t/g, "    ") : line }, i > 0 || (breakFirst && p === 0 && i === 0))));
    });
    return out;
  }

  heading(text: string, level: 1 | 2 | 3 | 4): void {
    const style = { 1: HeadingLevel.HEADING_1, 2: HeadingLevel.HEADING_2, 3: HeadingLevel.HEADING_3, 4: HeadingLevel.HEADING_4 }[level];
    const size = { 1: 32, 2: 26, 3: 22, 4: 20 }[level];
    this.blocks.push(new Paragraph({ heading: style, keepNext: true, spacing: { before: level >= 3 ? 200 : 320, after: 80 }, children: [new TextRun({ text: wordSafe(text), font: FONT, bold: true, size, color: CARBON })] }));
  }

  para(...parts: Array<Run | string>): void {
    this.blocks.push(new Paragraph({ spacing: { after: 100 }, children: this.#runs(parts.map((p) => (typeof p === "string" ? { text: p } : p))) }));
  }

  labelled(label: string, ...parts: Array<Run | string>): void {
    this.blocks.push(new Paragraph({ spacing: { after: 60 }, children: this.#runs([{ text: `${label}: `, bold: true }, ...parts.map((p) => (typeof p === "string" ? { text: p } : p))]) }));
  }

  /** A formula, in a fixed-width face, set apart so it can be copied out whole. */
  code(text: string): void {
    this.blocks.push(new Paragraph({ spacing: { before: 40, after: 100 }, indent: { left: 360 }, children: this.#runs([{ text, code: true }]) }));
  }

  /**
   * A first line, then more, each on a line of its own in one paragraph: a large
   * flow has thousands of steps, and one paragraph each keeps the file small.
   * The first line hangs, so the lines under it read as its own.
   */
  lines(first: Run[], rest: Run[][]): void {
    const children = [...this.#runs(first), ...rest.flatMap((line, i) => this.#runs(line, first.length > 0 || i > 0))];
    this.blocks.push(new Paragraph({ spacing: { after: 80 }, indent: first.length ? { left: 360, hanging: 360 } : { left: 360 }, keepLines: true, children }));
  }
}

// ─── Words ────────────────────────────────────────────────────────────────────

/**
 * Flow Builder's names for the kinds of step detail.ts knows. A kind not here
 * prints under Salesforce's own name for it (decision 444), never a guess.
 */
export const STEP_WORDS: Readonly<Record<string, string>> = {
  actionCalls: "Action",
  apexPluginCalls: "Apex plug-in",
  assignments: "Assignment",
  collectionProcessors: "Collection sort or filter",
  customErrors: "Custom error",
  decisions: "Decision",
  loops: "Loop",
  orchestratedStages: "Stage",
  recordCreates: "Create records",
  recordDeletes: "Delete records",
  recordLookups: "Get records",
  recordRollbacks: "Roll back records",
  recordUpdates: "Update records",
  screens: "Screen",
  steps: "Step",
  subflows: "Subflow",
  transforms: "Transform",
  waits: "Pause",
};

/** What a step's detail is, by its kind: the action, the subflow, the loop's collection or the class. */
const DETAIL_WORDS: Readonly<Record<string, string>> = { actionCalls: "Action", apexPluginCalls: "Apex class", loops: "Goes through", subflows: "Runs the flow" };

const TYPE_WORDS: Readonly<Record<string, string>> = { Flow: "Screen flow", AutoLaunchedFlow: "Autolaunched flow", Workflow: "Process Builder process", InvocableProcess: "Process Builder process, started by another process", Orchestrator: "Orchestration" };
const STATUS_WORDS: Readonly<Record<string, string>> = { Obsolete: "Inactive", InvalidDraft: "Draft with errors" };

const OPERATOR_WORDS: Readonly<Record<string, string>> = {
  EqualTo: "equals",
  NotEqualTo: "doesn't equal",
  GreaterThan: "is greater than",
  GreaterThanOrEqualTo: "is greater than or equal to",
  LessThan: "is less than",
  LessThanOrEqualTo: "is less than or equal to",
  Contains: "contains",
  StartsWith: "starts with",
  EndsWith: "ends with",
  IsNull: "is empty:",
  IsChanged: "has changed:",
};

/** Salesforce's own word with ours beside it, so an admin can search for either. */
const named = (words: Readonly<Record<string, string>>, raw: string): string => (words[raw] ? `${words[raw]} (${raw})` : raw);

export const stepKindWords = (kind: string): string => STEP_WORDS[kind] ?? kind;

/** What starts a flow, from its start element, in words. */
function startWords(flow: HandbookFlow): string {
  const { object, triggerType, recordTriggerType } = flow.start;
  const what = object ? `${/^[AEIOU]/i.test(object) ? "an" : "a"} ${object} record` : "a record";
  const when = ({ Create: "created", Update: "updated", CreateAndUpdate: "created or updated", Delete: "deleted" } as Record<string, string>)[recordTriggerType ?? ""];
  const change = when ? `is ${when}` : recordTriggerType ? `is saved (Salesforce's trigger: ${recordTriggerType})` : "is saved";
  if (triggerType === "RecordBeforeSave") return `When ${what} ${change}, before the change is saved`;
  if (triggerType === "RecordAfterSave") return `When ${what} ${change}, after the change is saved`;
  if (triggerType === "RecordBeforeDelete") return `When ${what} is about to be deleted`;
  if (triggerType === "Scheduled") return object ? `On a schedule, for ${object} records` : "On a schedule";
  if (triggerType === "PlatformEvent") return object ? `When a platform event, ${object}, is published` : "When a platform event is published";
  if (triggerType) return `${triggerType}, as Salesforce names it${object ? `, on ${object}` : ""}`;
  // A screen flow, a subflow and a Process Builder process start in ways their start element doesn't hold, so this says only what it holds.
  return `Its start names no trigger, so its metadata doesn't say what runs it${object ? `. Its object is ${object}` : ""}`;
}

function scheduleWords(schedule: NonNullable<HandbookFlow["start"]["schedule"]>): string {
  const time = schedule.startTime && /^(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?Z$/.exec(schedule.startTime);
  const parts = [
    schedule.frequency ?? "Frequency not given",
    schedule.startDate ? `from ${/^\d{4}-\d{2}-\d{2}$/.test(schedule.startDate) ? longDate(schedule.startDate) : schedule.startDate}` : "",
    schedule.startTime ? `at ${time ? `${time[1]}:${time[2]} UTC` : schedule.startTime}` : "",
  ];
  return parts.filter(Boolean).join(", ");
}

/** "the first 500 of 612 steps" (decision 453). */
const firstOf = (shown: number, total: number, what: string): string => `the first ${count(shown)} of ${count(total)} ${what}`;

function conditionWords(c: HandbookCondition): Run[] {
  const operator = c.operator ? (OPERATOR_WORDS[c.operator] ?? c.operator) : "(no operator given)";
  const value: Run[] = c.value === null ? [{ text: "(no value)", muted: true }] : [{ text: c.value }];
  const cut = c.valueCut || (c.valueLength !== null && c.value !== null && c.valueLength > c.value.length);
  return [{ text: `${c.field} ${operator} ` }, ...value, ...(cut ? [{ text: ` [cut: ${firstOf(c.value?.length ?? 0, c.valueLength ?? 0, "characters")}]`, bold: true }] : [])];
}

// ─── The parts ────────────────────────────────────────────────────────────────

/**
 * One part of the handbook. A later part (Apex) is one more of these: what it
 * says on the first page, and the part itself. Each says "we couldn't read
 * this", "there's none" or what it holds, and the three never look alike
 * (decisions 453 and 454).
 */
type Part = { covers: (h: Handbook) => string; write: (doc: Doc, h: Handbook) => void };

const unavailableWords = (what: string, reason: string): string => `We couldn't read your ${what}, so this handbook doesn't list them. ${sentence(reason)}`;

/** Active first, then by label, then by API name, so the order is the same every time. */
export function flowOrder(flows: HandbookFlow[]): HandbookFlow[] {
  const label = (f: HandbookFlow): string => f.label || f.name;
  return [...flows].sort((a, b) => Number(b.status === "Active") - Number(a.status === "Active") || label(a).localeCompare(label(b), "en-GB") || a.name.localeCompare(b.name, "en-GB"));
}

const flowCounts = (items: HandbookFlow[]): string => {
  const active = items.filter((f) => f.status === "Active").length;
  return `${plural(items.length, "flow")}, ${count(active)} of them active`;
};

const FLOWS: Part = {
  covers: (h) => {
    if (h.flows.status !== "ok") return unavailableWords("flows", h.flows.reason);
    return h.flows.items.length ? `Flows: ${flowCounts(h.flows.items)}.` : "Flows: your org has none.";
  },
  write: (doc, h) => {
    doc.heading("Flows", 2);
    if (h.flows.status !== "ok") return doc.para({ text: unavailableWords("flows", h.flows.reason), bold: true });
    if (h.flows.items.length === 0) return doc.para("Your org has no flows: there were none to read, active or not.");
    doc.para(`Your org has ${flowCounts(h.flows.items)}. Active flows come first, then the rest, each in order of its label.`);
    doc.para("Each flow's steps are numbered in the order the flow runs them, from its start, following each path to its end before the next. A step no path reaches is listed after the rest and marked. A kind of step we have no plain name for appears under Salesforce's own name for it.");
    for (const flow of flowOrder(h.flows.items)) writeFlow(doc, flow);
  },
};

function writeFlow(doc: Doc, flow: HandbookFlow): void {
  doc.heading(flow.label || flow.name, 3);
  doc.labelled("API name", flow.name);
  doc.labelled("Status", named(STATUS_WORDS, flow.status));
  doc.labelled("Type", named(TYPE_WORDS, flow.processType));
  doc.labelled("Description", ...(flow.description === null ? [{ text: "none given", muted: true }] : [flow.description]));
  doc.labelled("What starts it", `${startWords(flow)}.`);
  if (flow.start.schedule) doc.labelled("Schedule", `${scheduleWords(flow.start.schedule)}.`);
  writeEntry(doc, flow.start.entry);

  // Reached steps first, then any no path reaches, each in the file's order (decision 453).
  const steps = [...flow.steps.filter((s) => s.reached), ...flow.steps.filter((s) => !s.reached)];
  const total = Math.max(flow.stepsTotal, steps.length);
  if (total === 0) return doc.labelled("Steps", { text: "none. This flow has no steps.", muted: true });
  const cut = flow.cut || total > steps.length;
  doc.labelled("Steps", cut ? `${plural(total, "step")}. This lists ${firstOf(steps.length, total, "steps")}, in the order the flow runs them; the rest aren't in this handbook.` : `${plural(total, "step")}.`);
  const number = new Map(steps.map((s, i) => [s.name, i + 1]));
  const where = (name: string | null): string => {
    if (name === null) return "the flow ends here";
    const n = number.get(name);
    if (n === undefined) return `${name}, which isn't among the steps listed here`;
    const step = steps[n - 1]!;
    return `step ${n}, ${step.label ?? step.name}`;
  };
  steps.forEach((step, i) => {
    if (!step.reached && (i === 0 || steps[i - 1]!.reached)) doc.para({ text: "No path from the flow's start reaches the steps below, so they never run as the flow stands.", bold: true });
    writeStep(doc, step, i + 1, where);
  });
}

function writeEntry(doc: Doc, entry: HandbookFlow["start"]["entry"]): void {
  const total = Math.max(entry.conditionsTotal, entry.conditions.length);
  if (total === 0 && entry.formula === null) return doc.labelled("Entry conditions", { text: "none", muted: true });
  if (total > 0) {
    const cut = entry.cut || total > entry.conditions.length;
    doc.labelled("Entry conditions", cut ? `${plural(total, "condition")}. This lists ${firstOf(entry.conditions.length, total, "conditions")}.` : `${plural(total, "condition")}.`);
    if (entry.logic) doc.labelled("Combined as", ({ and: "all of them must be met", or: "any one of them is enough" } as Record<string, string>)[entry.logic.toLowerCase()] ?? entry.logic);
    doc.lines([], entry.conditions.map((c, i) => [{ text: `${i + 1}. ` }, ...conditionWords(c)]));
  }
  if (entry.formula !== null) {
    doc.labelled("Entry formula", entry.formulaCut || (entry.formulaLength ?? 0) > entry.formula.length ? { text: `cut: this is ${firstOf(entry.formula.length, entry.formulaLength ?? entry.formula.length, "characters")}.`, bold: true } : "");
    doc.code(entry.formula);
  }
}

function writeStep(doc: Doc, step: HandbookStep, n: number, where: (name: string | null) => string): void {
  const title: Run[] = [{ text: `${n}. ${stepKindWords(step.kind)}: ${step.label ?? step.name}`, bold: true }];
  if (step.label !== null && step.label !== step.name) title.push({ text: `  ${step.name}`, muted: true });
  if (!step.reached) title.push({ text: "  (no path reaches this step)", bold: true });
  const lines: Run[][] = [];
  if (step.object) lines.push([{ text: `On: ${step.object}` }]);
  const fieldsTotal = Math.max(step.fieldsTotal, step.fields.length);
  if (fieldsTotal > 0) lines.push([{ text: `Fields: ${step.fields.join(", ")}` }, ...(fieldsTotal > step.fields.length ? [{ text: ` [cut: ${firstOf(step.fields.length, fieldsTotal, "fields")}]`, bold: true }] : [])]);
  if (step.detail) lines.push([{ text: `${DETAIL_WORDS[step.kind] ?? "Uses"}: ${step.detail}` }]);
  for (const o of step.outcomes) lines.push([{ text: `${o.label}: ${where(o.next)}` }]);
  // A step with outcomes goes where they say; one without goes on, or ends the flow.
  if (step.next !== null || step.outcomes.length === 0) lines.push([{ text: `Then: ${where(step.next)}` }]);
  if (step.fault !== null) lines.push([{ text: `If it fails: ${where(step.fault)}` }]);
  doc.lines(title, lines);
}

/** "your custom objects and Account, Contact, Lead and Opportunity", from what the file says was read (decision 455). */
export function coverageWords(covers: ValidationRuleCoverage): string {
  if (covers.standardObjects === "all") return "every object";
  return covers.standardObjects.length ? `your custom objects and ${listed(covers.standardObjects)}` : "your custom objects";
}

const ruleCounts = (items: HandbookValidationRule[]): string => `${plural(items.length, "validation rule")}, ${count(items.filter((r) => r.active).length)} of them active`;

const VALIDATION_RULES: Part = {
  covers: (h) => {
    const v = h.validationRules;
    if (v.status !== "ok") return unavailableWords("validation rules", v.reason);
    return v.items.length ? `Validation rules: ${ruleCounts(v.items)}, on ${coverageWords(v.covers)}.` : `Validation rules: your org has none on ${coverageWords(v.covers)}.`;
  },
  write: (doc, h) => {
    doc.heading("Validation rules", 2);
    const v = h.validationRules;
    if (v.status !== "ok") return doc.para({ text: unavailableWords("validation rules", v.reason), bold: true });
    // What was read comes first, so "none on Case" is never read into a list that didn't look at Case.
    const others = v.covers.standardObjects === "all" ? "" : " We didn't read the rules on your other standard objects, so a rule on one of those isn't listed here.";
    doc.para(`These are the validation rules on ${coverageWords(v.covers)}.${others}`);
    if (v.items.length === 0) return doc.para(`Your org has no validation rules on ${coverageWords(v.covers)}: there were none to read, active or not.`);
    doc.para(`There are ${ruleCounts(v.items)}. They're grouped by object, and each rule's formula is set in a fixed-width font as it was written, so it can be copied back into Salesforce.`);
    const objects = [...new Set(v.items.map((r) => r.object))].sort((a, b) => a.localeCompare(b, "en-GB"));
    for (const object of objects) {
      const rules = v.items.filter((r) => r.object === object).sort((a, b) => a.name.localeCompare(b.name, "en-GB"));
      doc.heading(`${object}: ${plural(rules.length, "rule")}`, 3);
      for (const rule of rules) writeRule(doc, rule);
    }
  },
};

function writeRule(doc: Doc, rule: HandbookValidationRule): void {
  doc.heading(rule.name, 4);
  doc.labelled("Active", rule.active ? "Yes" : "No");
  doc.labelled("Description", ...(rule.description === null ? [{ text: "none given", muted: true }] : [rule.description]));
  if (rule.formula === null) {
    doc.labelled("Formula", { text: "we couldn't read this rule's formula.", bold: true });
  } else {
    const total = rule.formulaLength ?? rule.formula.length;
    doc.labelled("Formula", rule.formulaCut || total > rule.formula.length ? { text: `cut: this is ${firstOf(rule.formula.length, total, "characters")}.`, bold: true } : "");
    doc.code(rule.formula);
  }
  doc.labelled("Error message", ...(rule.message === null ? [{ text: "none in what we read", muted: true }] : [rule.message]));
  doc.labelled("Where the message shows", rule.messageField === null ? "at the top of the page" : `beside the ${rule.messageField} field`);
}

/** The parts, in the order the handbook runs. */
const PARTS: Part[] = [FLOWS, VALIDATION_RULES];

/** The handbook as its document's blocks. Exported for the tests, which read the words without unzipping. */
export function handbookBlocks(handbook: Handbook, options: { clientName: string; runDate: string }): Paragraph[] {
  const doc = new Doc();
  doc.heading(`Org handbook: ${options.clientName}`, 1);
  doc.para(`This handbook sets out how your Salesforce org is configured: what each flow does, step by step, and what each validation rule checks. We read it from your org on ${longDate(options.runDate)}, for your health check, and it describes the org as it was that day.`);
  doc.para("Everything in it comes from your org's own configuration. Names, descriptions, conditions, formulas and messages are as your admins wrote them, except that anything that looks like a password or a key is replaced with [REDACTED]. We haven't added to it or summarised it. Where we couldn't read something, or list only part of it, we say so.");
  doc.heading("What it covers", 2);
  for (const part of PARTS) doc.para(part.covers(handbook));
  for (const part of PARTS) part.write(doc, handbook);
  return doc.blocks;
}

/** The handbook as a Word document, from a parsed `Handbook`. */
export async function renderHandbookDocx(handbook: Handbook, options: { clientName: string; runDate: string }): Promise<Buffer> {
  const doc = new Document({
    title: wordSafe(`Org handbook: ${options.clientName}`),
    creator: "Black Cloud",
    styles: { default: { document: { run: { font: FONT, size: 20, color: CARBON } } } },
    sections: [
      {
        properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1200, bottom: 1200, left: 1300, right: 1300 } } },
        footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, children: [new TextRun({ children: ["Page ", PageNumber.CURRENT], font: FONT, size: 16, color: MUTED })] })] }) },
        children: handbookBlocks(handbook, options),
      },
    ],
  });
  return Buffer.from(await Packer.toBuffer(doc));
}
