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
  path: string;
};

export type WorkflowSummary = {
  object: string;
  path: string;
  rules: Array<{ name: string; active: boolean; timeTriggers: number; actions: number }>;
};

export type ValidationRuleSummary = { object: string; name: string; active: boolean };

// Where a user or field is mentioned. Values are metadata paths. Raw XML isn't
// kept: what the rules and guards need is here, and less kept is less to leak.
export type ReferenceIndex = { users: Record<string, string[]>; fields: Record<string, string[]> };

export type MetadataBundle = {
  flows: FlowSummary[];
  workflows: WorkflowSummary[];
  validationRules: ValidationRuleSummary[];
  references: ReferenceIndex;
  filesRead: number;
};

// Arrays are forced so one child element parses the same as many.
const ARRAY_TAGS = new Set(["rules", "actions", "workflowTimeTriggers", "validationRules", "recordCreates", "recordUpdates", "recordDeletes", "actionCalls", "loops"]);
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

const distinct = (text: string, re: RegExp): number => new Set(text.match(re) ?? []).size;

// A Process Builder keeps its object in processMetadataValues, not on start.
function flowObject(flow: Record<string, any>): string | null {
  if (flow.start?.object) return String(flow.start.object);
  const values = [flow.processMetadataValues ?? []].flat() as Array<{ name?: string; value?: { stringValue?: string } }>;
  return values.find((v) => v.name === "ObjectType")?.value?.stringValue ?? null;
}

function parseFlow(path: string, xml: string): FlowSummary {
  const flow = (parser.parse(xml) as { Flow: Record<string, any> }).Flow;
  const start = flow.start ?? {};
  const risky = [...(flow.recordCreates ?? []), ...(flow.recordUpdates ?? []), ...(flow.recordDeletes ?? []), ...(flow.actionCalls ?? [])] as Array<Record<string, unknown>>;
  return {
    name: path.replace(/^flows\//, "").replace(/\.flow(-meta\.xml)?$/, ""),
    label: String(flow.label ?? ""),
    status: String(flow.status ?? "Unknown"),
    processType: String(flow.processType ?? "Flow"),
    apiVersion: Number(flow.apiVersion ?? 0),
    object: flowObject(flow),
    triggerType: start.triggerType ? String(start.triggerType) : null,
    scheduled: Boolean(start.schedule),
    dmlOrActions: risky.length,
    withFault: risky.filter((e) => e.faultConnector !== undefined).length,
    loops: (flow.loops ?? []).length,
    hardcodedUserIds: distinct(xml, ELEMENT_USER_ID),
    hardcodedOtherIds: distinct(xml, ELEMENT_OTHER_ID),
    hardcodedEmails: distinct(xml, ELEMENT_EMAIL),
    path,
  };
}

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
    })),
  };
}

function parseObject(path: string, xml: string, index: ReferenceIndex): ValidationRuleSummary[] {
  const object = path.replace(/^objects\//, "").replace(/\.object(-meta\.xml)?$/, "");
  const parsed = (parser.parse(xml) as { CustomObject?: Record<string, any> }).CustomObject ?? {};
  return ((parsed.validationRules ?? []) as Array<Record<string, any>>).map((rule) => {
    const name = String(rule.fullName ?? "");
    // Only the formula: the whole file also defines every field on the object,
    // and a field defining itself isn't a reference to it.
    indexReferences(index, String(rule.errorConditionFormula ?? ""), `${path}#validationRules.${name}`);
    return { object, name, active: String(rule.active) === "true" };
  });
}

/** Turns the unzipped retrieve into summaries. A file that can't be parsed fails the extractor. */
export function parseMetadataBundle(files: Record<string, string>): MetadataBundle {
  const bundle: MetadataBundle = { flows: [], workflows: [], validationRules: [], references: { users: {}, fields: {} }, filesRead: 0 };
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
      bundle.validationRules.push(...parseObject(path, xml, bundle.references));
    } else {
      indexReferences(bundle.references, xml, path);
    }
  }
  return bundle;
}
