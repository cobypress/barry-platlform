import { AlignmentType, Document, Footer, HeadingLevel, Packer, PageNumber, Paragraph, TextRun } from "docx";
import { colour, font } from "../design/tokens.ts";
import { longDate } from "../render/format.ts";
import { UNKNOWN_USER, type Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { compareHandbooks, type Changed, type Compared, type Component, type Detail, type HandbookComparison, type KindComparison, type NotCompared } from "./compare.ts";
import type { EarlierHandbook } from "./read.ts";
import { CHANGES_NOT_KEPT, TRIGGER_EVENTS, type Handbook, type HandbookChange, type HandbookApexClass, type HandbookLastChange, type HandbookApexTouches, type HandbookApexTrigger, type HandbookCondition, type HandbookFlow, type HandbookFlowItem, type HandbookPath, type HandbookStep, type HandbookValidationRule, type HandbookValidationRuleItem, type LastChanges, type ValidationRuleCoverage } from "./schema.ts";

// The org handbook as a Word document (DECISIONS.md 443 and 444): handbook.json
// made readable, and nothing more. Code alone writes it, from the file: no
// Claude text, no figure from the value model, and no name added but who made
// each recent change and who last changed each class, trigger, flow and
// validation rule, restored from the run's pseudonym map (decisions 421, 461,
// 473 and 482). The admin's own text prints as written, so a reader can trust
// that what isn't here doesn't exist, or is said to be missing.

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

  /** A line set as the labelled lines around it are, with no label of its own. */
  line(parts: Run[]): void {
    this.blocks.push(new Paragraph({ spacing: { after: 60 }, children: this.#runs(parts) }));
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

/** Our word for one of Salesforce's, or undefined. Its own keys only: a kind named "constructor" isn't a word we have. */
const wordFor = <T = string>(words: Readonly<Record<string, T>>, raw: string): T | undefined => (Object.hasOwn(words, raw) ? words[raw] : undefined);

/** Salesforce's own word with ours beside it, so an admin can search for either. */
const named = (words: Readonly<Record<string, string>>, raw: string): string => {
  const ours = wordFor(words, raw);
  return ours ? `${ours} (${raw})` : raw;
};

export const stepKindWords = (kind: string): string => wordFor(STEP_WORDS, kind) ?? kind;

/** What starts a flow, from its start element, in words. */
function startWords(flow: HandbookFlow): string {
  const { object, triggerType, recordTriggerType } = flow.start;
  const what = object ? `${/^[AEIOU]/i.test(object) ? "an" : "a"} ${object} record` : "a record";
  const when = recordTriggerType ? wordFor({ Create: "created", Update: "updated", CreateAndUpdate: "created or updated", Delete: "deleted" }, recordTriggerType) : undefined;
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

// Flow Builder's units for a scheduled path's offset. One not here prints as
// Salesforce records it, beside its number, never turned into words.
const UNIT_WORDS: Readonly<Record<string, [string, string]>> = { Minutes: ["minute", "minutes"], Hours: ["hour", "hours"], Days: ["day", "days"], Weeks: ["week", "weeks"], Months: ["month", "months"] };

/**
 * When a later path runs, from what the file keeps and nothing else (decision
 * 446): "30 days before CloseDate", "1 hour after the record is saved". Words
 * only when the number, a unit we know and what it counts from are all there;
 * otherwise Salesforce's own values, as it records them.
 */
export function whenWords(when: HandbookPath["when"]): string {
  if (when === null) return "when it runs isn't recorded";
  const { offsetNumber, offsetUnit, timeSource, recordField } = when;
  const unit = offsetUnit === null ? undefined : wordFor(UNIT_WORDS, offsetUnit);
  // A field it counts from, the moment the record is saved, or Salesforce's own
  // word for another. With no time source, what it counts from isn't said.
  const from =
    timeSource === null ? null
    : timeSource === "RecordField" ? recordField
    : timeSource === "RecordTriggerEvent" ? "the record is saved"
    : `what Salesforce calls ${timeSource}${recordField ? ` (${recordField})` : ""}`;
  if (unit && offsetNumber !== null && from !== null) {
    const n = Math.abs(offsetNumber);
    return `${count(n)} ${n === 1 ? unit[0] : unit[1]} ${offsetNumber < 0 ? "before" : "after"} ${from}`;
  }
  const kept = [offsetNumber !== null ? `offset ${offsetNumber}` : "", offsetUnit ? `unit ${offsetUnit}` : "", timeSource ? `counted from ${timeSource}` : "", recordField ? `field ${recordField}` : ""].filter(Boolean);
  return kept.length ? `as Salesforce records it: ${kept.join(", ")}` : "when it runs isn't recorded";
}

/** When a path runs, in words, under Salesforce's own name for a kind we have none for. */
function pathWhen(path: HandbookPath): string {
  if (path.kind === "immediate") return "runs straight away";
  if (path.kind === "scheduled") return whenWords(path.when);
  if (path.kind === "afterCommit") return "after the save is committed";
  return `a path Salesforce calls ${path.kind}${path.when ? `, ${whenWords(path.when)}` : ""}`;
}

const upperFirst = (text: string): string => `${text[0]!.toUpperCase()}${text.slice(1)}`;

/** A path as "What starts it" lists it: "'30 days before close': 30 days before CloseDate". */
function pathWords(path: HandbookPath): string {
  const when = pathWhen(path);
  if (path.label !== null) return `'${path.label}': ${when}`;
  return path.kind === "scheduled" ? `Scheduled, ${when}` : upperFirst(when);
}

/** The same path, on the step it starts at. */
function pathStartWords(path: HandbookPath): string {
  const when = pathWhen(path);
  if (path.label !== null) return `Starts the path '${path.label}': ${when}`;
  if (path.kind === "scheduled") return `Starts the scheduled path: ${when}`;
  if (path.kind === "immediate" || path.kind === "afterCommit") return `Starts the path that ${path.kind === "immediate" ? "" : "runs "}${when}`;
  return `Starts ${when}`;
}

/** A flow that starts more than one way, so its paths are listed. One with only its immediate path prints as before. */
const manyPaths = (flow: HandbookFlow): boolean => flow.start.pathsCut || flow.start.pathsTotal > 1 || flow.start.paths.some((p) => p.kind !== "immediate");

/** "the first 500 of 612 steps" (decision 453). */
const firstOf = (shown: number, total: number, what: string): string => `the first ${count(shown)} of ${count(total)} ${what}`;

function conditionWords(c: HandbookCondition): Run[] {
  const operator = c.operator ? (wordFor(OPERATOR_WORDS, c.operator) ?? c.operator) : "(no operator given)";
  const value: Run[] = c.value === null ? [{ text: "(no value)", muted: true }] : [{ text: c.value }];
  const cut = c.valueCut || (c.valueLength !== null && c.value !== null && c.valueLength > c.value.length);
  return [{ text: `${c.field} ${operator} ` }, ...value, ...(cut ? [{ text: ` [cut: ${firstOf(c.value?.length ?? 0, c.valueLength ?? 0, "characters")}]`, bold: true }] : [])];
}

// ─── The parts ────────────────────────────────────────────────────────────────

/**
 * One part of the handbook: what it says on the first page, and the part
 * itself. Each says "we couldn't read this", "there's none" or what it holds,
 * and the three never look alike (decisions 453 and 454).
 */
/** `alsoCovers` is a further line for "What it covers", or null when there's nothing to say. */
type Part = { covers: (h: Handbook) => string; alsoCovers?: (h: Handbook) => string | null; write: (doc: Doc, h: Handbook, names: Names) => void };

/** A user's name from their token, or null when the run's names can't give it. */
type Names = (token: string) => string | null;

const unavailableWords = (what: string, reason: string): string => `We couldn't read your ${what}, so this handbook doesn't list them. ${sentence(reason)}`;

/** Active first, then by label, then by API name, so the order is the same every time. */
export function flowOrder<F extends HandbookFlow>(flows: F[]): F[] {
  const label = (f: HandbookFlow): string => f.label || f.name;
  return [...flows].sort((a, b) => Number(b.status === "Active") - Number(a.status === "Active") || label(a).localeCompare(label(b), "en-GB") || a.name.localeCompare(b.name, "en-GB"));
}

const flowCounts = (items: HandbookFlow[]): string => {
  const active = items.filter((f) => f.status === "Active").length;
  return `${plural(items.length, "flow")}, ${count(active)} of them active`;
};

/**
 * Said once, under a part's own line in "What it covers" (decision 482): that
 * each flow or validation rule listed shows its last change, or that this run
 * couldn't read them. A file from before they were kept says nothing.
 */
function lastChangedSaid(one: string, many: string, part: { status: "ok"; items: Array<{ lastChanged: HandbookLastChange | null }>; lastChanges: LastChanges } | { status: "unavailable" }): string | null {
  if (part.status !== "ok" || part.items.length === 0) return null;
  if (part.lastChanges === "refused") return `We couldn't read when your ${many} were last changed, or by whom, so this handbook doesn't say.`;
  if (part.lastChanges !== "read") return null;
  // Salesforce's list can lack one, so "each" is said only of those it gave.
  const missing = part.items.filter((i) => i.lastChanged === null).length;
  if (missing === part.items.length) return `Salesforce's list of changes had nothing for your ${many}, so none shows when it was last changed.`;
  // A date alone would otherwise read as a name left off.
  const dateAlone = part.items.some((i) => i.lastChanged?.by === null) ? ", or the date alone where Salesforce records no user" : "";
  const gap = missing ? ` Salesforce's list of changes had nothing for ${plural(missing, one)}, so ${missing === 1 ? "that one shows" : "those show"} neither.` : "";
  return `Each ${one} shows when it was last changed and by whom${dateAlone}.${gap}`;
}

const FLOWS: Part = {
  covers: (h) => {
    if (h.flows.status !== "ok") return unavailableWords("flows", h.flows.reason);
    return h.flows.items.length ? `Flows: ${flowCounts(h.flows.items)}.` : "Flows: your org has none.";
  },
  alsoCovers: (h) => lastChangedSaid("flow", "flows", h.flows),
  write: (doc, h, names) => {
    doc.heading("Flows", 2);
    if (h.flows.status !== "ok") return doc.para({ text: unavailableWords("flows", h.flows.reason), bold: true });
    if (h.flows.items.length === 0) return doc.para("Your org has no flows: there were none to read, active or not.");
    doc.para(`Your org has ${flowCounts(h.flows.items)}. Active flows come first, then the rest, each in order of its label.`);
    doc.para("Each flow's steps are numbered in the order the flow runs them, from its start, following each path to its end before the next. A flow that starts in more than one way, such as one with a scheduled path, lists its steps path by path: the steps that run straight away first, then each later path's in turn, and the step each path starts at says so. A step no path reaches is listed after the rest and marked. A kind of step we have no plain name for appears under Salesforce's own name for it.");
    for (const flow of flowOrder(h.flows.items)) writeFlow(doc, flow, names);
  },
};

function writeFlow(doc: Doc, flow: HandbookFlowItem, names: Names): void {
  doc.heading(flow.label || flow.name, 3);
  doc.labelled("API name", flow.name);
  doc.labelled("Status", named(STATUS_WORDS, flow.status));
  for (const line of lastChangedLine(flow.lastChanged, names)) doc.line(line);
  doc.labelled("Type", named(TYPE_WORDS, flow.processType));
  doc.labelled("Description", ...(flow.description === null ? [{ text: "none given", muted: true }] : [flow.description]));
  doc.labelled("What starts it", `${startWords(flow)}.`);

  // Reached steps first, then any no path reaches, each in the file's order (decision 453).
  const steps = [...flow.steps.filter((s) => s.reached), ...flow.steps.filter((s) => !s.reached)];
  const number = new Map(steps.map((s, i) => [s.name, i + 1]));
  const where = (name: string | null): string => {
    if (name === null) return "the flow ends here";
    const n = number.get(name);
    if (n === undefined) return `${name}, which isn't among the steps listed here`;
    const step = steps[n - 1]!;
    return `step ${n}, ${step.label ?? step.name}`;
  };

  // Every way it starts, each with the step it leads to, so a step that runs
  // 30 days later never reads as one that runs straight away (decision 446).
  const starts = new Map<string, HandbookPath[]>();
  if (manyPaths(flow)) {
    const total = Math.max(flow.start.pathsTotal, flow.start.paths.length);
    const cut = flow.start.pathsCut || total > flow.start.paths.length;
    doc.lines([{ text: "It starts in ", bold: true }, { text: `${plural(total, "way")}${cut ? `. This lists ${firstOf(flow.start.paths.length, total, "ways")}` : ""}:` }], flow.start.paths.map((p) => [{ text: `${pathWords(p)}: ${p.next === null ? "it leads to no step" : where(p.next)}` }]));
    for (const p of flow.start.paths) if (p.next !== null) starts.set(p.next, [...(starts.get(p.next) ?? []), p]);
  }
  if (flow.start.schedule) doc.labelled("Schedule", `${scheduleWords(flow.start.schedule)}.`);
  writeEntry(doc, flow.start.entry);

  const total = Math.max(flow.stepsTotal, steps.length);
  if (total === 0) return doc.labelled("Steps", { text: "none. This flow has no steps.", muted: true });
  const cut = flow.cut || total > steps.length;
  doc.labelled("Steps", cut ? `${plural(total, "step")}. This lists ${firstOf(steps.length, total, "steps")}, in the order the flow runs them; the rest aren't in this handbook.` : `${plural(total, "step")}.`);
  // None reached means the start names no step the file has: saying they never
  // run would be our claim, not the file's, so this says only what it holds.
  const noStart = steps.length > 0 && steps.every((s) => !s.reached);
  if (noStart) doc.para({ text: "This flow's metadata doesn't say where it starts, so its steps are listed in the order Salesforce gives them.", bold: true });
  steps.forEach((step, i) => {
    if (!noStart && !step.reached && (i === 0 || steps[i - 1]!.reached)) doc.para({ text: "No path from the flow's start reaches the steps below, so they never run as the flow stands.", bold: true });
    writeStep(doc, step, i + 1, where, !noStart, starts.get(step.name) ?? []);
  });
}

function writeEntry(doc: Doc, entry: HandbookFlow["start"]["entry"]): void {
  const total = Math.max(entry.conditionsTotal, entry.conditions.length);
  if (total === 0 && entry.formula === null) return doc.labelled("Entry conditions", { text: "none", muted: true });
  if (total > 0) {
    const cut = entry.cut || total > entry.conditions.length;
    doc.labelled("Entry conditions", cut ? `${plural(total, "condition")}. This lists ${firstOf(entry.conditions.length, total, "conditions")}.` : `${plural(total, "condition")}.`);
    if (entry.logic) doc.labelled("Combined as", wordFor({ and: "all of them must be met", or: "any one of them is enough" }, entry.logic.toLowerCase()) ?? entry.logic);
    doc.lines([], entry.conditions.map((c, i) => [{ text: `${i + 1}. ` }, ...conditionWords(c)]));
  }
  if (entry.formula !== null) {
    doc.labelled("Entry formula", entry.formulaCut || (entry.formulaLength ?? 0) > entry.formula.length ? { text: `cut: this is ${firstOf(entry.formula.length, entry.formulaLength ?? entry.formula.length, "characters")}.`, bold: true } : "");
    doc.code(entry.formula);
  }
}

function writeStep(doc: Doc, step: HandbookStep, n: number, where: (name: string | null) => string, markUnreached: boolean, starts: HandbookPath[]): void {
  const title: Run[] = [{ text: `${n}. ${stepKindWords(step.kind)}: ${step.label ?? step.name}`, bold: true }];
  if (step.label !== null && step.label !== step.name) title.push({ text: `  ${step.name}`, muted: true });
  if (markUnreached && !step.reached) title.push({ text: "  (no path reaches this step)", bold: true });
  // The paths that start here, first, so the step is read as theirs.
  const lines: Run[][] = starts.map((p) => [{ text: pathStartWords(p), bold: true }]);
  if (step.object) lines.push([{ text: `On: ${step.object}` }]);
  const fieldsTotal = Math.max(step.fieldsTotal, step.fields.length);
  if (fieldsTotal > 0) lines.push([{ text: `Fields: ${step.fields.join(", ")}` }, ...(fieldsTotal > step.fields.length ? [{ text: ` [cut: ${firstOf(step.fields.length, fieldsTotal, "fields")}]`, bold: true }] : [])]);
  if (step.detail) lines.push([{ text: `${wordFor(DETAIL_WORDS, step.kind) ?? "Uses"}: ${step.detail}` }]);
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
  alsoCovers: (h) => lastChangedSaid("validation rule", "validation rules", h.validationRules),
  write: (doc, h, names) => {
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
      for (const rule of rules) writeRule(doc, rule, names);
    }
  },
};

function writeRule(doc: Doc, rule: HandbookValidationRuleItem, names: Names): void {
  doc.heading(rule.name, 4);
  doc.labelled("Active", rule.active ? "Yes" : "No");
  for (const line of lastChangedLine(rule.lastChanged, names)) doc.line(line);
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

// ─── Apex ─────────────────────────────────────────────────────────────────────

type ApexItem = HandbookApexClass | HandbookApexTrigger;
/** An Apex part as the file has it (decision 456): cut at its most, by name, or unavailable. */
type Capped<T> = { status: "ok"; items: T[]; total: number; cut: boolean } | { status: "unavailable"; reason: string };

/** "convertLead" is Apex's name; a reader says "convert lead". */
const CHANGE_WORDS: Readonly<Record<string, string>> = { convertLead: "convert lead" };

/** As Salesforce shows an API version: 62 is "62.0". */
const apiVersion = (v: number): string => (Number.isInteger(v) ? v.toFixed(1) : String(v));

const apexTotal = (part: Extract<Capped<ApexItem>, { status: "ok" }>): number => Math.max(part.total, part.items.length);
const apexCut = (part: Extract<Capped<ApexItem>, { status: "ok" }>): boolean => part.cut || apexTotal(part) > part.items.length;

/** Counts from the file. A cut part is counted by its total alone: what wasn't kept can't be told active or not. */
function apexCounts(part: Extract<Capped<ApexItem>, { status: "ok" }>, one: string, many: string): string {
  const total = apexTotal(part);
  if (apexCut(part)) return `${plural(total, one, many)}. This handbook lists ${firstOf(part.items.length, total, many)}, by name`;
  return `${plural(total, one, many)}, ${count(part.items.filter((i) => i.status === "Active").length)} of them active`;
}

/**
 * What a class or trigger touches, line by line (decision 457). An empty list
 * is "none" only when no flag says the code reads more than its list: a query
 * built as it runs, or a search, reads objects the code doesn't name.
 */
function touchesLines(touches: HandbookApexTouches | null): Run[][] {
  if (touches === null) return [[{ text: "The code couldn't be read, so what it touches isn't known.", bold: true }]];
  const lines: Run[][] = [];
  const total = Math.max(touches.queriesTotal, touches.queries.length);
  if (touches.queries.length > 0) {
    lines.push([{ text: `Queries: ${touches.queries.join(", ")}` }, ...(total > touches.queries.length ? [{ text: ` [cut: ${firstOf(touches.queries.length, total, "objects")}]`, bold: true }] : [])]);
    if (touches.dynamicQueries) lines.push([{ text: "It also builds queries as it runs, so the objects those read aren't known from the code." }]);
  } else if (touches.dynamicQueries) {
    lines.push([{ text: "Queries: it builds them as it runs, so the objects they read aren't known from the code." }]);
  } else if (!touches.searches) {
    lines.push([{ text: "Queries: none in its own code" }]);
  }
  if (touches.searches) lines.push([{ text: `${lines.length ? "It also searches" : "It searches"}, and the objects a search reads aren't listed.` }]);
  lines.push([{ text: `Changes it makes: ${touches.changes.length ? touches.changes.map((c) => wordFor(CHANGE_WORDS, c) ?? c).join(", ") : "none in its own code"}` }]);
  return lines;
}

/**
 * "Last changed 3 Mar 2026 by Jo Ellis", the date as the recent changes give
 * theirs, without the time (decision 473), for a class, trigger, flow or
 * validation rule (482). Who is restored from the token alone, as a recent
 * change's is; with no user, the date stands alone.
 */
function lastChangedLine(change: HandbookLastChange | null, names: Names): Run[][] {
  if (change === null) return [];
  return [[{ text: `Last changed ${lastChangedWords(change, names)}` }]];
}

/** "3 Mar 2026 by Jo Ellis", from this run's token alone. */
function lastChangedWords(change: HandbookLastChange, names: Names): string {
  const on = /^\d{4}-\d{2}-\d{2}/.test(change.at) ? longDate(change.at) : change.at;
  const by = change.by === null ? "" : ` by ${names(change.by) ?? "a user whose name we couldn't look up"}`;
  return `${on}${by}`;
}

/** One class or trigger as one paragraph: an org can have thousands, and a heading and a paragraph a line each would bury the rest. */
function writeApex(doc: Doc, item: ApexItem, names: Names, trigger?: Run[][]): void {
  // A trigger can't be a test, so only a class says whether it is one.
  const test = trigger ? "" : ` Test: ${item.isTest === null ? "couldn't be told: the code couldn't be read" : item.isTest ? "Yes" : "No"}.`;
  doc.lines([{ text: item.name, bold: true }], [...(trigger ?? []), [{ text: `API version ${apiVersion(item.apiVersion)}. Status: ${item.status}.${test}` }], ...lastChangedLine(item.lastChanged, names), ...touchesLines(item.touches)]);
}

/** What the reading is and isn't, said once, where the first Apex listed starts. */
const READING =
  "What each one touches is read from its code's text: the objects named in its queries, and the kinds of change it makes. It doesn't say which object a change is made to, because the code names a variable there, not an object. And it can miss what the code does through another class, a method it calls or a library.";

/** A part with Apex listed in it, so the reading has been said. */
const listsApex = (part: Capped<ApexItem>): boolean => part.status === "ok" && part.items.length > 0;

function apexPart<T extends ApexItem>(what: { title: string; one: string; many: string; get: (h: Handbook) => Capped<T>; intro: (h: Handbook) => string; write: (doc: Doc, item: T, names: Names) => void }): Part {
  return {
    covers: (h) => {
      const part = what.get(h);
      if (part.status !== "ok") return unavailableWords(what.title, part.reason);
      return apexTotal(part) ? `${what.title}: ${apexCounts(part, what.one, what.many)}.` : `${what.title}: your org has none.`;
    },
    write: (doc, h, names) => {
      doc.heading(what.title, 2);
      const part = what.get(h);
      if (part.status !== "ok") return doc.para({ text: unavailableWords(what.title, part.reason), bold: true });
      if (apexTotal(part) === 0) return doc.para(`Your org has no ${what.title}: there were none to read.`);
      doc.para(`Your org has ${apexCounts(part, what.one, what.many)}.${apexCut(part) ? " The rest aren't in this handbook." : " They're listed by name."}`);
      doc.para(what.intro(h));
      for (const item of part.items) what.write(doc, item, names);
    },
  };
}

// In the file's order, which is by name (decision 456).
const APEX_CLASSES = apexPart<HandbookApexClass>({ title: "Apex classes", one: "class", many: "classes", get: (h) => h.apexClasses, intro: () => READING, write: (doc, item, names) => writeApex(doc, item, names) });

/** Events in Apex's order, whatever order the file has them in. */
const eventWords = (events: HandbookApexTrigger["events"]): Run =>
  events === null ? { text: "The events weren't read.", bold: true } : { text: `Runs: ${[...events].sort((a, b) => TRIGGER_EVENTS.indexOf(a) - TRIGGER_EVENTS.indexOf(b)).join(", ")}` };

/**
 * Said once, under the Apex parts' own lines in "What it covers" (decision
 * 473): that each listed class and trigger shows its last change, or that this
 * run couldn't read them. A file from before they were kept says nothing.
 */
function lastChangedCovers(h: Handbook): string | null {
  const listed = [
    { what: "class", whose: "classes", part: h.apexClasses },
    { what: "trigger", whose: "triggers", part: h.apexTriggers },
  ].flatMap(({ what, whose, part }) => (part.status === "ok" && part.items.length > 0 ? [{ what, whose, lastChanges: part.lastChanges, noUser: (part.items as ApexItem[]).some((i) => i.lastChanged?.by === null) }] : []));
  const read = listed.filter((p) => p.lastChanges === "read");
  const refused = listed.filter((p) => p.lastChanges === "refused");
  const said: string[] = [];
  // A date alone would otherwise read as a name left off.
  const dateAlone = read.some((p) => p.noUser) ? ", or the date alone where Salesforce records no user" : "";
  if (read.length) said.push(`Each Apex ${read.map((p) => p.what).join(" and ")} shows when it was last changed and by whom${dateAlone}.`);
  if (refused.length) said.push(`We couldn't read when your Apex ${refused.map((p) => p.whose).join(" and ")} were last changed, or by whom, so this handbook doesn't say.`);
  return said.length ? said.join(" ") : null;
}

const APEX_TRIGGERS: Part = {
  ...apexPart<HandbookApexTrigger>({
    title: "Apex triggers",
    one: "trigger",
    many: "triggers",
    get: (h) => h.apexTriggers,
    intro: (h) => `Each trigger shows the object it's on and the events it runs on. A custom object we couldn't name shows as the id Salesforce gives it. ${listsApex(h.apexClasses) ? "What it touches is read as the classes' is, with the same limits." : READING}`,
    write: (doc, item, names) => writeApex(doc, item, names, [[{ text: `On: ${item.object}` }], [eventWords(item.events)]]),
  }),
  alsoCovers: lastChangedCovers,
};

// ─── Recent changes ───────────────────────────────────────────────────────────

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

/** "28 Nov 2026 at 14:05 UTC", or the date alone when the time isn't in UTC as Salesforce gives it. */
function changedAt(at: string): string {
  if (!/^\d{4}-\d{2}-\d{2}/.test(at)) return at;
  const time = /^\d{4}-\d{2}-\d{2}T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z|\+0000|\+00:00)$/.exec(at);
  return time ? `${longDate(at)} at ${time[1]}:${time[2]} UTC` : longDate(at);
}

/**
 * Who made a change. The file holds a token, never a name, and only the token
 * is restored (decision 421): a name the run can't give is "a user", never the
 * token, which means nothing to a reader.
 */
function changedBy(change: HandbookChange, names: Names): string {
  const who = change.by === null ? "no user recorded" : (names(change.by) ?? "a user whose name we couldn't look up");
  return change.loggedInAs === null ? who : `${who} (${change.loggedInAs} logged in as them)`;
}

type Changes = Extract<Handbook["recentChanges"], { status: "ok" }>;

const changeCount = (part: Changes): number => Math.max(part.total, part.items.length);
const changesCut = (part: Changes): boolean => part.cut || part.total > part.items.length;

/** "from 3 Jun 2026 to 28 Nov 2026", newest last, from the items' own dates. */
function changeSpan(items: HandbookChange[]): string {
  const days = items.map((c) => c.at.slice(0, 10)).sort();
  const first = days[0]!;
  const last = days.at(-1)!;
  return first === last ? `on ${longDate(first)}` : `from ${longDate(first)} to ${longDate(last)}`;
}

const userAdmin = (n: number): string => `${plural(n, "change")} to users`;

const RECENT_CHANGES: Part = {
  covers: (h) => {
    const part = h.recentChanges;
    if (part.status !== "ok") return part.reason === CHANGES_NOT_KEPT ? `Recent changes: ${CHANGES_NOT_KEPT.replace(/^T/, "t")}` : unavailableWords("recent setup changes", part.reason);
    const kept = `Salesforce keeps this trail for ${plural(part.days, "day")}.`;
    if (part.items.length === 0) return `Recent changes: none in the last ${plural(part.days, "day")}${part.leftOutUserAdmin ? `, apart from ${userAdmin(part.leftOutUserAdmin)}` : ""}. ${kept}`;
    const n = plural(changeCount(part), "setup change");
    return changesCut(part) ? `Recent changes: the newest ${n} we read, ${changeSpan(part.items)}. ${kept}` : `Recent changes: ${n} ${changeSpan(part.items)}. ${kept}`;
  },
  write: (doc, h, names) => {
    doc.heading("Recent changes", 2);
    const part = h.recentChanges;
    if (part.status !== "ok") return doc.para({ text: part.reason === CHANGES_NOT_KEPT ? CHANGES_NOT_KEPT : unavailableWords("recent setup changes", part.reason), bold: true });
    const leftOut = part.leftOutUserAdmin
      ? ` We left out ${userAdmin(part.leftOutUserAdmin)}, such as password resets and email changes, because they're about people rather than how the org is set up.`
      : "";
    if (part.items.length === 0) return doc.para(`No setup changes were recorded in the last ${plural(part.days, "day")}${part.leftOutUserAdmin ? `, apart from ${userAdmin(part.leftOutUserAdmin)}, which this handbook leaves out` : ""}.`);
    doc.para(`These are the changes made in Setup over the last ${plural(part.days, "day")}, as Salesforce's Setup Audit Trail records them, newest first, under a heading for each month. Each gives who made it and Salesforce's own description of the change.${leftOut}`);
    if (changesCut(part)) doc.para({ text: `Salesforce had more changes than we read: we read the newest ${count(part.total + part.leftOutUserAdmin)}, so older ones from the ${plural(part.days, "day")} aren't here.`, bold: true });
    let month = "";
    for (const change of part.items) {
      const m = /^(\d{4})-(\d{2})/.exec(change.at);
      const heading = m ? `${MONTHS[Number(m[2]) - 1] ?? m[2]} ${m[1]}` : "Date not recorded";
      if (heading !== month) doc.heading((month = heading), 3);
      writeChange(doc, change, names);
    }
  },
};

/** One change as one paragraph: when and who, the section, then Salesforce's sentence as written. */
function writeChange(doc: Doc, change: HandbookChange, names: Names): void {
  const first: Run[] = [{ text: changedAt(change.at), bold: true }, { text: `, ${changedBy(change, names)}: ${change.section ?? "section not recorded"}` }];
  if (change.action !== null) first.push({ text: `  ${change.action}`, muted: true });
  const total = change.whatLength ?? change.what?.length ?? 0;
  const what: Run[] =
    change.what === null
      ? [{ text: "Salesforce gave no description of this change.", muted: true }]
      : [{ text: change.what }, ...(change.whatCut || total > change.what.length ? [{ text: ` [cut: ${firstOf(change.what.length, total, "characters")}]`, bold: true }] : [])];
  doc.lines(first, [what]);
}

// ─── Changed since your last health check ─────────────────────────────────────

export const SINCE_TITLE = "Changed since your last health check";

/** Said for every baseline there's no handbook to read from: gone, unreadable or another org's alike (decision 483). */
export const NO_EARLIER = "There was no earlier handbook to compare with, so this handbook doesn't say what changed since your last health check.";

/** Our words for each detail a change names. When it was last changed has words of its own. */
const CHANGED_WORDS: Readonly<Record<Exclude<Detail, "lastChanged">, string>> = {
  label: "label",
  status: "status",
  type: "type",
  description: "description",
  start: "what starts it",
  schedule: "schedule",
  entry: "entry conditions",
  paths: "the ways it starts",
  steps: "steps",
  active: "whether it's active",
  formula: "formula",
  message: "error message",
  messageField: "where the message shows",
  apiVersion: "API version",
  isTest: "whether it's a test",
  touches: "what it touches",
  object: "the object it's on",
  events: "events",
};

type SinceKind = { key: keyof HandbookComparison; title: string; one: string; many: string; name: (c: Component) => string };

const SINCE_KINDS: SinceKind[] = [
  { key: "flows", title: "Flows", one: "flow", many: "flows", name: (c) => (c.label && c.label !== c.name ? `${c.label} (${c.name})` : c.name) },
  { key: "validationRules", title: "Validation rules", one: "validation rule", many: "validation rules", name: (c) => `${c.name} on ${c.object}` },
  { key: "apexClasses", title: "Apex classes", one: "Apex class", many: "Apex classes", name: (c) => c.name },
  { key: "apexTriggers", title: "Apex triggers", one: "Apex trigger", many: "Apex triggers", name: (c) => (c.object ? `${c.name} on ${c.object}` : c.name) },
];

const notComparedWords = (kind: SinceKind, unreadIn: NotCompared["unreadIn"]): string =>
  unreadIn === "now" ? `We couldn't read your ${kind.many} in this health check, so we haven't compared them with your earlier one.`
  : unreadIn === "before" ? `We couldn't read your ${kind.many} in your earlier health check, so we haven't compared them.`
  : `We couldn't read your ${kind.many} in either health check, so we haven't compared them.`;

/** "status, steps and last changed 3 Mar 2026 by Jo Ellis": the details, never what they were. */
function changedWords(change: Changed, names: Names): string {
  const words = change.details.map((d) => (d === "lastChanged" ? (change.lastChanged ? `last changed ${lastChangedWords(change.lastChanged, names)}` : "when it was last changed") : CHANGED_WORDS[d]));
  return listed(words);
}

/** What one kind's comparison left out, and why, each in a sentence of its own. */
function sinceNotes(kind: SinceKind, c: Compared): string[] {
  const notes: string[] = [];
  const these = (n: number): string => (n === 1 ? "that one" : "those");
  if (c.routes === "differ") notes.push(`Your earlier health check read your ${kind.many} by a different route from this one, and the two routes can give a different version of a flow, so we've compared only which were added or removed and when each was last changed.`);
  if (c.routes === "unknown") notes.push(`We can't tell whether your earlier health check read your ${kind.many} the same way as this one, and two ways of reading them can give a different version of a flow, so we've compared only which were added or removed and when each was last changed.`);
  if (c.stepsCut) notes.push(`${upperFirst(plural(c.stepsCut, kind.one, kind.many))} had more steps than this handbook keeps in one of the two health checks, so for ${these(c.stepsCut)} we compared how many steps each has and the steps both kept.`);
  if (c.leftOutObjects.length) notes.push(`We left out the rules on ${listed(c.leftOutObjects)}: only one of the two health checks read the rules on ${c.leftOutObjects.length === 1 ? "that object" : "those objects"}, so a rule there isn't called added or removed.`);
  if (c.unnamed) notes.push(`We left out ${plural(c.unnamed, kind.one, kind.many)} whose object one of the two health checks could name only by its id, so ${c.unnamed === 1 ? "it isn't" : "they aren't"} called added, removed or changed.`);
  if (c.range) {
    const after = c.range.leftOut ? ` This health check's ${plural(c.range.leftOut, kind.one, kind.many)} after that ${c.range.leftOut === 1 ? "isn't" : "aren't"} compared, so none is called added or removed.` : "";
    notes.push(`One of the two health checks had more ${kind.many} than this handbook keeps, so we compared them only as far as both lists go, up to ${c.range.upTo}.${after}`);
  }
  if (c.unread) {
    const which = { flows: "some of their details", validationRules: "the formula", apexClasses: "the code", apexTriggers: "the code or the events, or could name the object only by its id" }[kind.key];
    const what = { flows: "those details aren't", validationRules: `${c.unread === 1 ? "its" : "their"} formula, error message and where the message shows aren't`, apexClasses: "whether it's a test and what it touches aren't", apexTriggers: "those details aren't" }[kind.key];
    notes.push(`For ${plural(c.unread, kind.one, kind.many)}, one of the two health checks couldn't read ${which}, so ${what} compared.`);
  }
  const { before, after } = c.lastChanges;
  if (c.common > 0 && (before !== "read" || after !== "read")) {
    const who = before !== "read" && after !== "read" ? "Neither health check says" : before !== "read" ? "Your earlier health check doesn't say" : "This health check doesn't say";
    notes.push(`${who} when each ${kind.one} was last changed, so that isn't compared.`);
  }
  return notes;
}

/** "Added: 2 flows", then one line each, or "none". */
function sinceList(doc: Doc, label: string, kind: SinceKind, lines: string[]): void {
  doc.lines([{ text: `${label}: `, bold: true }, { text: lines.length ? plural(lines.length, kind.one, kind.many) : "none" }], lines.map((text) => [{ text }]));
}

function writeSinceKind(doc: Doc, kind: SinceKind, c: KindComparison, names: Names): void {
  doc.heading(kind.title, 3);
  if (c.status !== "compared") return doc.para({ text: notComparedWords(kind, c.unreadIn), bold: true });
  if (c.added.length + c.removed.length + c.changed.length === 0) {
    doc.para(`No ${kind.many} were added, removed or changed.`);
  } else {
    sinceList(doc, "Added", kind, c.added.map(kind.name));
    sinceList(doc, "Removed", kind, c.removed.map(kind.name));
    sinceList(doc, "Changed", kind, c.changed.map((change) => `${kind.name(change)}: ${changedWords(change, names)}`));
  }
  for (const note of sinceNotes(kind, c)) doc.para(note);
}

/**
 * The part a re-check's handbook gains (decision 483), before the recent
 * changes: what was added, removed and changed since the earlier health check,
 * from the two files and nothing else. With a baseline named and no handbook
 * to read, one sentence. Every count is the comparison's own.
 */
function sincePart(earlier: EarlierHandbook | null, comparison: HandbookComparison | null, runDate: string): Part {
  const since = earlier ? `since your health check of ${longDate(earlier.asOf)}` : "";
  return {
    covers: () => (earlier ? `${SINCE_TITLE}: what was added, removed or changed in your flows, validation rules, Apex classes and Apex triggers ${since}.` : `${SINCE_TITLE}: there was no earlier handbook to compare with.`),
    alsoCovers: () =>
      earlier
        ? "It compares every detail this handbook keeps of each one, apart from who last changed it, which we don't match from one health check to the next. It doesn't compare the order of steps no path reaches, the part of a formula or a condition's value past the 4,000 characters this handbook keeps, or the recent changes, which have their own part. Where the two health checks can't be compared fairly, because one couldn't read something or read less of it, the part says so rather than call anything added, removed or changed."
        : null,
    write: (doc, _h, names) => {
      doc.heading(SINCE_TITLE, 2);
      if (!earlier || !comparison) return doc.para(NO_EARLIER);
      doc.para(`This compares your org on ${longDate(runDate)} with your health check of ${longDate(earlier.asOf)}. For your flows, validation rules, Apex classes and Apex triggers, it lists what was added, what was removed and what changed, and for each one that changed, which of its details differ, not what they were before. One that was renamed shows as one removed and one added.`);
      for (const kind of SINCE_KINDS) writeSinceKind(doc, kind, comparison[kind.key], names);
    },
  };
}

/** The parts, in the order the handbook runs. */
const PARTS: Part[] = [FLOWS, VALIDATION_RULES, APEX_CLASSES, APEX_TRIGGERS, RECENT_CHANGES];

type HandbookOptions = {
  clientName: string;
  runDate: string;
  /** The run's pseudonym map, which turns who made each recent change, and who last changed each class, trigger, flow and validation rule, back into a name. Without it, none is named. */
  pseudonyms?: Pseudonymiser | null;
  /**
   * A re-check's earlier handbook (decision 483). Undefined with no baseline,
   * and the document is as it was; null when one was named and there's no
   * handbook to read. Its tokens are never restored: only this run's are.
   */
  earlier?: EarlierHandbook | null;
};

/** The handbook as its document's blocks. Exported for the tests, which read the words without unzipping. */
export function handbookBlocks(handbook: Handbook, options: HandbookOptions): Paragraph[] {
  // The token alone, so text in the file that looks like one prints as written.
  // The engine's placeholder for an id with no name isn't a name (decision 474).
  const names: Names = (token) => {
    const name = options.pseudonyms?.restore(token);
    return name && name !== token && name !== UNKNOWN_USER ? name : null;
  };
  const doc = new Doc();
  doc.heading(`Org handbook: ${options.clientName}`, 1);
  doc.para(`This handbook sets out how your Salesforce org is configured: what each flow does, step by step, what each validation rule checks, what each Apex class and trigger touches, and what was changed in Setup recently, and by whom. We read it from your org on ${longDate(options.runDate)}, for your health check, and it describes the org as it was that day.`);
  doc.para("Everything in it comes from your org's own configuration. Names, descriptions, conditions, formulas and messages are as your admins wrote them, except that anything that looks like a password or a key is replaced with [REDACTED]. We haven't added to it or summarised it, except that what each piece of Apex touches is read from its code, as that part explains. Where we couldn't read something, or list only part of it, we say so.");
  doc.heading("What it covers", 2);
  // A re-check's part comes before the recent changes, the last part.
  const { earlier } = options;
  const parts = earlier === undefined ? PARTS : [...PARTS.slice(0, -1), sincePart(earlier, earlier ? compareHandbooks(earlier.handbook, handbook) : null, options.runDate), ...PARTS.slice(-1)];
  for (const part of parts) {
    doc.para(part.covers(handbook));
    const also = part.alsoCovers?.(handbook);
    if (also) doc.para(also);
  }
  for (const part of parts) part.write(doc, handbook, names);
  return doc.blocks;
}

/** The handbook as a Word document, from a parsed `Handbook`. */
export async function renderHandbookDocx(handbook: Handbook, options: HandbookOptions): Promise<Buffer> {
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
