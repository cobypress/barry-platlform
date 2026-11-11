import { redactSecrets } from "../privacy/secrets.ts";
import { MAX_CONDITIONS, MAX_FIELDS, MAX_PATHS, MAX_STEPS, MAX_TEXT, type HandbookCondition, type HandbookFlow, type HandbookOutcome, type HandbookPath, type HandbookStep, type HandbookValidationRule } from "./schema.ts";

// What each flow and validation rule does, for the handbook (DECISIONS.md 451).
// The retrieve gives XML and Tooling gives JSON with the same element names, so
// both routes come through here. The XML parser gives strings and "" for an
// empty element; Tooling gives typed values and null for an absent one. Every
// read below treats the two alike, so the routes give the same detail.

/**
 * Which reading of the flows and validation rules this is (DECISIONS.md 484),
 * written into the metadata step's output when it reads, on both routes, and
 * into handbook.json as `readBy.metadata`. A re-check names which details of a
 * component differ only when both files hold the same number, so a change in
 * how we read never reads as a change in the org. Raise it with any change that
 * moves what the handbook keeps for a flow or rule nobody touched: here, in the
 * retrieve's parser and its decoding (metadata-parse.ts), in what Tooling hands
 * over (metadata-tooling.ts), or in the redaction. `source-pin.test.ts` holds
 * it to a hash of the text of those files (485), and `pin.test.ts` to a hash
 * of the synthetic org's detail.
 */
export const METADATA_READING = 1;

type Node = Record<string, any>;

const list = (value: unknown): unknown[] => (value == null ? [] : [value].flat().filter((v) => v != null));
const nodes = (value: unknown): Node[] => list(value).filter((v): v is Node => typeof v === "object");

/** A value as text, or null when there's none. */
function plain(value: unknown): string | null {
  if (value == null || typeof value === "object") return null;
  const text = String(value);
  return text === "" ? null : text;
}

// Anyone can type a key into a label, a description or a formula's literal, so
// every piece of free text is redacted before it's kept (decision 452). The XML
// is redacted whole before it's parsed, but a quote there is often &quot;,
// which the patterns only see once the parser has turned it back into a quote.
const free = (value: unknown): string | null => {
  const text = plain(value);
  return text === null ? null : redactSecrets(text);
};

type Capped = { text: string | null; cut: boolean; length: number | null };

/** Redacted, then cut at the limit, so a secret is never half kept. */
export function capped(value: unknown): Capped {
  const text = free(value);
  if (text === null) return { text: null, cut: false, length: null };
  if (text.length <= MAX_TEXT) return { text, cut: false, length: text.length };
  let kept = text.slice(0, MAX_TEXT);
  // Never half a character outside the Basic Multilingual Plane.
  if (/[\uD800-\uDBFF]$/.test(kept)) kept = kept.slice(0, -1);
  return { text: kept, cut: true, length: text.length };
}

// A flow value is one of these, the rest empty. Tooling may also send 5 where
// the XML says 5.0.
const VALUE_KEYS = ["stringValue", "numberValue", "booleanValue", "dateValue", "dateTimeValue", "elementReference", "formulaExpression", "apexValue", "sobjectValue", "setupReference", "transformValueReference"];

function valueText(value: unknown): string | null {
  if (value == null || typeof value !== "object") return plain(value);
  const node = value as Node;
  for (const key of VALUE_KEYS) {
    const text = plain(node[key]);
    if (text !== null) return key === "numberValue" && Number.isFinite(Number(text)) ? String(Number(text)) : text;
  }
  // A kind of value we don't know is kept as it is, rather than lost.
  for (const key of Object.keys(node).sort()) {
    const text = plain(node[key]);
    if (text !== null) return text;
  }
  return null;
}

const target = (connector: unknown): string | null => (connector != null && typeof connector === "object" ? plain((connector as Node).targetReference) : null);

// Kinds that are always steps. Any other element is one when it has a place on
// the canvas or a connector, so a kind Salesforce adds later is kept under its
// own name rather than dropped (decision 453). Variables, constants, formulas,
// choices and text templates have neither.
const STEP_KINDS = new Set(["actionCalls", "apexPluginCalls", "assignments", "collectionProcessors", "customErrors", "decisions", "loops", "orchestratedStages", "recordCreates", "recordDeletes", "recordLookups", "recordRollbacks", "recordUpdates", "screens", "steps", "subflows", "transforms", "waits"]);
const KNOWN_CONNECTORS = new Set(["connector", "faultConnector", "defaultConnector", "nextValueConnector", "noMoreValuesConnector"]);
const isConnectorKey = (key: string): boolean => /connector$/i.test(key);

function isStep(kind: string, element: Node): boolean {
  if (plain(element.name) === null) return false;
  return STEP_KINDS.has(kind) || element.locationX != null || Object.keys(element).some((key) => isConnectorKey(key) && target(element[key]) !== null);
}

function outcomesOf(kind: string, element: Node): HandbookOutcome[] {
  const out: HandbookOutcome[] = [];
  const named = (item: Node): string => free(item.label) ?? plain(item.name) ?? "";
  for (const rule of nodes(element.rules)) out.push({ label: named(rule), next: target(rule.connector) });
  for (const event of nodes(element.waitEvents)) out.push({ label: named(event), next: target(event.connector) });
  if (plain(element.defaultConnectorLabel) !== null || target(element.defaultConnector) !== null) {
    out.push({ label: free(element.defaultConnectorLabel) ?? "Default Outcome", next: target(element.defaultConnector) });
  }
  // A loop's two ways out, under the names Flow Builder gives them.
  if (kind === "loops" || target(element.nextValueConnector) !== null || target(element.noMoreValuesConnector) !== null) {
    out.push({ label: "For Each Item", next: target(element.nextValueConnector) }, { label: "After Last Item", next: target(element.noMoreValuesConnector) });
  }
  // Any other way out, under Salesforce's own name for it.
  for (const key of Object.keys(element).filter((k) => isConnectorKey(k) && !KNOWN_CONNECTORS.has(k)).sort()) {
    const next = target(element[key]);
    if (next !== null) out.push({ label: key, next });
  }
  return out;
}

function fieldsOf(element: Node): string[] {
  const fields = [
    ...nodes(element.filters).map((f) => plain(f.field)),
    ...list(element.queriedFields).map(plain),
    ...nodes(element.inputAssignments).map((a) => plain(a.field)),
    ...nodes(element.outputAssignments).map((a) => plain(a.field)),
    ...nodes(element.assignmentItems).map((a) => plain(a.assignToReference)),
    ...nodes(element.rules).flatMap((r) => nodes(r.conditions).map((c) => plain(c.leftValueReference))),
  ];
  return [...new Set(fields.filter((f): f is string => f !== null))];
}

function detailOf(element: Node): string | null {
  const action = plain(element.actionName);
  if (action) return plain(element.actionType) ? `${action} (${plain(element.actionType)})` : action;
  return plain(element.flowName) ?? plain(element.collectionReference) ?? plain(element.apexClass) ?? plain(element.inputReference);
}

function stepOf(kind: string, element: Node): HandbookStep {
  const fields = fieldsOf(element);
  return {
    kind,
    name: plain(element.name)!,
    label: free(element.label),
    object: plain(element.object),
    fields: fields.slice(0, MAX_FIELDS),
    fieldsTotal: fields.length,
    detail: detailOf(element),
    outcomes: outcomesOf(kind, element),
    next: target(element.connector),
    fault: target(element.faultConnector),
    reached: false,
  };
}

/**
 * Steps in the order the flow runs them: depth first from the start, path by
 * path (its own connector, then each scheduled path in turn), following each
 * step's next, then its outcomes, then its fault path, each step once.
 * Steps no path reaches follow, by kind and then as the file lists them.
 */
function orderSteps(flow: Node): HandbookStep[] {
  const all: HandbookStep[] = [];
  for (const kind of Object.keys(flow).sort()) {
    if (kind === "start") continue;
    for (const element of nodes(flow[kind])) if (isStep(kind, element)) all.push(stepOf(kind, element));
  }
  const byName = new Map(all.map((s) => [s.name, s]));
  const start: Node = flow.start != null && typeof flow.start === "object" ? flow.start : {};
  // A Process Builder, or a flow from before the start element, names its first step instead.
  const first = [target(start.connector), ...nodes(start.scheduledPaths).map((p) => target(p.connector)), target(start.connector) ? null : plain(flow.startElementReference)];
  const reached: HandbookStep[] = [];
  const stack = first.filter((n): n is string => n !== null).reverse();
  while (stack.length > 0) {
    const step = byName.get(stack.pop()!);
    if (!step || step.reached) continue;
    step.reached = true;
    reached.push(step);
    const ways = [step.next, ...step.outcomes.map((o) => o.next), step.fault].filter((n): n is string => n !== null);
    stack.push(...ways.reverse());
  }
  return [...reached, ...all.filter((s) => !s.reached)];
}

/** A whole number as Salesforce records it: 5 from Tooling, "5" from the XML. */
function numberOf(value: unknown): number | null {
  const text = plain(value);
  return text !== null && Number.isFinite(Number(text)) ? Number(text) : null;
}

// Salesforce's pathType for the path that runs once the save is committed (Flow
// Builder's "Run Asynchronously"). A scheduled path has none.
const PATH_KINDS: Readonly<Record<string, string>> = { AsyncAfterCommit: "afterCommit" };

/**
 * Every way the flow starts (decision 446): the start's connector first, then
 * its scheduled paths in the file's order. Each `when` is kept as Salesforce
 * records it, and nothing is worked out from it here.
 */
function pathsOf(start: Node): Pick<HandbookFlow["start"], "paths" | "pathsTotal" | "pathsCut"> {
  const immediate: HandbookPath[] = target(start.connector) !== null ? [{ kind: "immediate", label: null, when: null, next: target(start.connector) }] : [];
  const later = nodes(start.scheduledPaths).map((p): HandbookPath => {
    const type = plain(p.pathType);
    const when = { offsetNumber: numberOf(p.offsetNumber), offsetUnit: plain(p.offsetUnit), timeSource: plain(p.timeSource), recordField: plain(p.recordField) };
    return {
      kind: type === null ? "scheduled" : Object.hasOwn(PATH_KINDS, type) ? PATH_KINDS[type]! : type,
      label: free(p.label),
      when: Object.values(when).some((v) => v !== null) ? when : null,
      next: target(p.connector),
    };
  });
  const all = [...immediate, ...later];
  return { paths: all.slice(0, MAX_PATHS), pathsTotal: all.length, pathsCut: all.length > MAX_PATHS };
}

function entryOf(start: Node): HandbookFlow["start"]["entry"] {
  const all: HandbookCondition[] = nodes(start.filters).map((f) => {
    const value = capped(valueText(f.value));
    return { field: plain(f.field) ?? "", operator: plain(f.operator), value: value.text, valueCut: value.cut, valueLength: value.length };
  });
  const formula = capped(start.filterFormula);
  return {
    logic: plain(start.filterLogic),
    formula: formula.text,
    formulaCut: formula.cut,
    formulaLength: formula.length,
    conditions: all.slice(0, MAX_CONDITIONS),
    conditionsTotal: all.length,
    cut: all.length > MAX_CONDITIONS,
  };
}

/** One flow's detail. `object` is the summary's, which also finds a Process Builder's. */
export function flowDetail(name: string, flow: Node, object: string | null): HandbookFlow {
  const start: Node = flow.start != null && typeof flow.start === "object" ? flow.start : {};
  const schedule: Node | null = start.schedule != null && typeof start.schedule === "object" ? start.schedule : null;
  const steps = orderSteps(flow);
  return {
    name,
    label: free(flow.label) ?? "",
    status: plain(flow.status) ?? "Unknown",
    processType: plain(flow.processType) ?? "Flow",
    description: free(flow.description),
    start: {
      object,
      triggerType: plain(start.triggerType),
      recordTriggerType: plain(start.recordTriggerType),
      schedule: schedule ? { frequency: plain(schedule.frequency), startDate: plain(schedule.startDate), startTime: plain(schedule.startTime) } : null,
      entry: entryOf(start),
      ...pathsOf(start),
    },
    steps: steps.slice(0, MAX_STEPS),
    stepsTotal: steps.length,
    cut: steps.length > MAX_STEPS,
  };
}

/**
 * One validation rule's detail. On the Tooling route a formula that wasn't read
 * comes in undefined, and is kept as null rather than guessed.
 */
export function validationRuleDetail(object: string, name: string, rule: Node): HandbookValidationRule {
  const formula = capped(rule.errorConditionFormula);
  return {
    object,
    name,
    active: String(rule.active) === "true",
    description: free(rule.description),
    formula: formula.text,
    formulaCut: formula.cut,
    formulaLength: formula.length,
    message: free(rule.errorMessage),
    messageField: plain(rule.errorDisplayField),
  };
}
