import { XMLParser } from "fast-xml-parser";
import { redactSecrets } from "../privacy/secrets.ts";

export type FlowSummary = {
  name: string;
  label: string;
  status: string;
  processType: string;
  apiVersion: number;
  object: string | null;
  triggerType: string | null;
  scheduled: boolean;
  dmlOrActions: number;
  withFault: number;
  loops: number;
  hardcodedUserIds: number;
  hardcodedOtherIds: number;
  hardcodedEmails: number;
  /** Record updates that write back to the record that started the flow. */
  sameRecordUpdates: number;
  /** Has a description, the only place Salesforce keeps why a flow exists. */
  described: boolean;
  path: string;
};

export type WorkflowSummary = {
  object: string;
  path: string;
  rules: Array<{ name: string; active: boolean; timeTriggers: number; actions: number; described: boolean }>;
};

export type ValidationRuleSummary = { object: string; name: string; active: boolean; described: boolean };
// Own custom fields only. Help text is what a user sees, a description is for whoever maintains the field.
export type CustomFieldSummary = { object: string; name: string; described: boolean; helpText: boolean };
export type RecordTypeSummary = { object: string; name: string; active: boolean };

// Where a user or field is mentioned. Values are metadata paths. Raw XML isn't
// kept: what the rules and guards need is here, and less kept is less to leak.
export type ReferenceIndex = { users: Record<string, string[]>; fields: Record<string, string[]> };

export type MetadataBundle = {
  /** How it was read: one Metadata API retrieve, or Tooling item by item for a connection that can't use the Metadata API. */
  route?: "retrieve" | "tooling";
  /**
   * Kinds of metadata that exist but whose contents couldn't be searched, such as
   * assignment rules on the Tooling route. A clean reference search isn't proof
   * while this has anything in it.
   */
  unsearched?: string[];
  flows: FlowSummary[];
  workflows: WorkflowSummary[];
  validationRules: ValidationRuleSummary[];
  customFields: CustomFieldSummary[];
  recordTypes: RecordTypeSummary[];
  /** Every object with a definition in the retrieve, whatever it holds. */
  objects: string[];
  references: ReferenceIndex;
  filesRead: number;
};

// Arrays are forced so one child element parses the same as many.
const ARRAY_TAGS = new Set(["rules", "actions", "workflowTimeTriggers", "validationRules", "fields", "recordCreates", "recordUpdates", "recordDeletes", "actionCalls", "loops", "recordTypes"]);
const parser = new XMLParser({ ignoreAttributes: true, parseTagValue: false, isArray: (name) => ARRAY_TAGS.has(name) });

const USER_ID = /\b005[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?\b/g;
const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+/g;
const CUSTOM_FIELD = /\b[A-Za-z][A-Za-z0-9_]*__c\b/g;
// A record id as a whole element value, which is how Flow stores a hardcoded one.
const ELEMENT_USER_ID = />005[A-Za-z0-9]{12,15}</g;
const ELEMENT_OTHER_ID = />(?:00G|00E|00e|012|701|0ML|00X|0D2)[A-Za-z0-9]{12,15}</g;
const ELEMENT_EMAIL = />[\w.%+-]+@[\w-]+(?:\.[\w-]+)+</g;

export const normaliseUserKey = (value: string): string => (/^005/.test(value) ? value.slice(0, 15) : value.toLowerCase());

function addRefs(index: Record<string, string[]>, values: Iterable<string>, path: string): void {
  for (const value of new Set(values)) (index[value] ??= []).push(path);
}

/** Indexes user ids, emails and custom field names mentioned in a piece of metadata or code. */
export function indexReferences(index: ReferenceIndex, text: string, path: string): void {
  const users = [...(text.match(USER_ID) ?? []), ...(text.match(EMAIL) ?? [])].map(normaliseUserKey);
  addRefs(index.users, users, path);
  addRefs(index.fields, text.match(CUSTOM_FIELD) ?? [], path);
}

const hasText = (value: unknown): boolean => typeof value === "string" ? value.trim().length > 0 : value !== undefined && value !== null && String(value).trim().length > 0;

const distinct = (text: string, re: RegExp): number => new Set(text.match(re) ?? []).size;

// A Process Builder keeps its object in processMetadataValues, not on start.
function flowObject(flow: Record<string, any>): string | null {
  if (flow.start?.object) return String(flow.start.object);
  const values = [flow.processMetadataValues ?? []].flat() as Array<{ name?: string; value?: { stringValue?: string } }>;
  return values.find((v) => v.name === "ObjectType")?.value?.stringValue ?? null;
}

// An update targets the triggering record through $Record, or as a lookup on the
// same object filtered to $Record.Id. A lookup with other filters updates siblings.
function isSameRecordUpdate(update: Record<string, any>, object: string | null): boolean {
  if (String(update.inputReference ?? "") === "$Record") return true;
  if (!object || update.object !== object) return false;
  const filters = [update.filters ?? []].flat() as Array<{ value?: { elementReference?: string } }>;
  return filters.length === 0 || filters.some((f) => f.value?.elementReference === "$Record.Id");
}

type IdCounts = Pick<FlowSummary, "hardcodedUserIds" | "hardcodedOtherIds" | "hardcodedEmails">;

// The retrieve gives XML and Tooling gives JSON with the same element names, so
// both go through one summary. Tooling sends null for an absent element, and
// typed values where the XML parser gives strings, hence the != null checks.
function summariseFlow(name: string, path: string, flow: Record<string, any>, ids: IdCounts): FlowSummary {
  const start = flow.start ?? {};
  const list = (value: unknown): Array<Record<string, any>> => (value == null ? [] : [value].flat());
  const risky = [...list(flow.recordCreates), ...list(flow.recordUpdates), ...list(flow.recordDeletes), ...list(flow.actionCalls)];
  return {
    name,
    label: String(flow.label ?? ""),
    status: String(flow.status ?? "Unknown"),
    processType: String(flow.processType ?? "Flow"),
    apiVersion: Number(flow.apiVersion ?? 0),
    object: flowObject(flow),
    triggerType: start.triggerType ? String(start.triggerType) : null,
    scheduled: Boolean(start.schedule),
    dmlOrActions: risky.length,
    withFault: risky.filter((e) => e.faultConnector != null).length,
    loops: list(flow.loops).length,
    ...ids,
    described: hasText(flow.description),
    sameRecordUpdates: list(flow.recordUpdates).filter((u) => isSameRecordUpdate(u, start.object ? String(start.object) : null)).length,
    path,
  };
}

function parseFlow(path: string, xml: string): FlowSummary {
  const flow = (parser.parse(xml) as { Flow: Record<string, any> }).Flow;
  const name = path.replace(/^flows\//, "").replace(/\.flow(-meta\.xml)?$/, "");
  return summariseFlow(name, path, flow, {
    hardcodedUserIds: distinct(xml, ELEMENT_USER_ID),
    hardcodedOtherIds: distinct(xml, ELEMENT_OTHER_ID),
    hardcodedEmails: distinct(xml, ELEMENT_EMAIL),
  });
}

/** Every string and number in a parsed document, which is what an XML element's text would be. */
function leafValues(value: unknown, out: string[] = []): string[] {
  if (value === null || value === undefined) return out;
  if (typeof value === "object") for (const child of Object.values(value)) leafValues(child, out);
  else out.push(String(value));
  return out;
}

const whole = (re: RegExp): RegExp => new RegExp(`^${re.source.slice(1, -1)}$`);
const LEAF_USER_ID = whole(ELEMENT_USER_ID);
const LEAF_OTHER_ID = whole(ELEMENT_OTHER_ID);
const LEAF_EMAIL = whole(ELEMENT_EMAIL);

/**
 * A flow from Tooling's `Flow.Metadata`, already redacted. Ids and emails are
 * counted over whole values, as the XML route counts whole elements.
 */
export function summariseFlowMetadata(name: string, metadata: Record<string, any>): FlowSummary {
  const values = leafValues(metadata);
  const count = (re: RegExp): number => new Set(values.filter((v) => re.test(v))).size;
  return summariseFlow(name, `flows/${name}.flow`, metadata, { hardcodedUserIds: count(LEAF_USER_ID), hardcodedOtherIds: count(LEAF_OTHER_ID), hardcodedEmails: count(LEAF_EMAIL) });
}

/** A Workflow Rule from Tooling's `WorkflowRule.Metadata`. */
export function summariseWorkflowRule(name: string, metadata: Record<string, any>): WorkflowSummary["rules"][number] {
  const count = (value: unknown): number => (value == null ? 0 : [value].flat().length);
  return { name, active: String(metadata.active) === "true", timeTriggers: count(metadata.workflowTimeTriggers), actions: count(metadata.actions), described: hasText(metadata.description) };
}

export { hasText };

function parseWorkflow(path: string, xml: string): WorkflowSummary {
  const workflow = (parser.parse(xml) as { Workflow?: Record<string, any> }).Workflow ?? {};
  return {
    object: path.replace(/^workflows\//, "").replace(/\.workflow(-meta\.xml)?$/, ""),
    path,
    rules: ((workflow.rules ?? []) as Array<Record<string, any>>).map((r) => ({
      name: String(r.fullName ?? ""),
      active: String(r.active) === "true",
      timeTriggers: (r.workflowTimeTriggers ?? []).length,
      actions: (r.actions ?? []).length,
      described: hasText(r.description),
    })),
  };
}

function parseObject(path: string, xml: string, index: ReferenceIndex): { object: string; validationRules: ValidationRuleSummary[]; customFields: CustomFieldSummary[]; recordTypes: RecordTypeSummary[] } {
  const object = path.replace(/^objects\//, "").replace(/\.object(-meta\.xml)?$/, "");
  const parsed = (parser.parse(xml) as { CustomObject?: Record<string, any> }).CustomObject ?? {};
  const customFields = ((parsed.fields ?? []) as Array<Record<string, any>>)
    .map((f) => ({ object, name: String(f.fullName ?? ""), described: hasText(f.description), helpText: hasText(f.inlineHelpText) }))
    .filter((f) => /^[A-Za-z][A-Za-z0-9_]*__c$/.test(f.name) && !/__.*__c$/.test(f.name));
  const validationRules = ((parsed.validationRules ?? []) as Array<Record<string, any>>).map((rule) => {
    const name = String(rule.fullName ?? "");
    // Only the formula: the whole file also defines every field on the object,
    // and a field defining itself isn't a reference to it.
    indexReferences(index, String(rule.errorConditionFormula ?? ""), `${path}#validationRules.${name}`);
    return { object, name, active: String(rule.active) === "true", described: hasText(rule.description) };
  });
  const recordTypes = ((parsed.recordTypes ?? []) as Array<Record<string, any>>).map((r) => ({ object, name: String(r.fullName ?? ""), active: String(r.active) === "true" }));
  return { object, validationRules, customFields, recordTypes };
}

/** Turns the unzipped retrieve into summaries. A file that can't be parsed fails the extractor. */
export function parseMetadataBundle(files: Record<string, string>): MetadataBundle {
  const bundle: MetadataBundle = { route: "retrieve", unsearched: [], flows: [], workflows: [], validationRules: [], customFields: [], recordTypes: [], objects: [], references: { users: {}, fields: {} }, filesRead: 0 };
  for (const [path, raw] of Object.entries(files).sort(([a], [b]) => a.localeCompare(b))) {
    if (path === "package.xml") continue;
    const xml = redactSecrets(raw);
    bundle.filesRead++;
    if (path.startsWith("flows/")) {
      bundle.flows.push(parseFlow(path, xml));
      indexReferences(bundle.references, xml, path);
    } else if (path.startsWith("workflows/")) {
      bundle.workflows.push(parseWorkflow(path, xml));
      indexReferences(bundle.references, xml, path);
    } else if (path.startsWith("objects/")) {
      const parsed = parseObject(path, xml, bundle.references);
      bundle.validationRules.push(...parsed.validationRules);
      bundle.customFields.push(...parsed.customFields);
      bundle.recordTypes.push(...parsed.recordTypes);
      bundle.objects.push(parsed.object);
    } else {
      indexReferences(bundle.references, xml, path);
    }
  }
  return bundle;
}
