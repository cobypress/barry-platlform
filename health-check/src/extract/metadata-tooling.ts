import { isUnavailable } from "../salesforce/client.ts";
import { redactSecrets } from "../privacy/secrets.ts";
import { validationRuleDetail } from "../handbook/detail.ts";
import type { HandbookValidationRule } from "../handbook/schema.ts";
import { flowDetailMetadata, hasText, indexReferences, summariseFlowMetadata, summariseWorkflowRule, type MetadataBundle, type UnreadKind, type WorkflowSummary } from "./metadata-parse.ts";
import { readableReason } from "./types.ts";
import type { ExtractContext } from "./types.ts";

// The same bundle as the retrieve, read through Tooling for a connection that
// can't use the Metadata API: a user holding only Barometer Read Only gets
// "use of the Metadata API requires a user with the ModifyAllData or
// ModifyMetadata permissions", and both are write permissions (DECISIONS.md 60).
// Every read here was tried as that user on bc-hc-dev first (DECISIONS.md 73).
//
// Tooling only returns `Metadata` for one row per query, so flows, Workflow Rules,
// their actions and validation rules cost one call each. That's why the retrieve
// stays the first choice, and why each item is checkpointed as it's read.

const CORE = ["Account", "Contact", "Lead", "Opportunity"];
// Our ceiling on one-at-a-time reads. Past it, flows make the bundle unavailable,
// and anything else becomes unsearched.
export const MAX_ITEM_READS = 2_000;
const OBJECT_CHUNK = 10;
const OWN_FIELD = /^[A-Za-z][A-Za-z0-9_]*__c$/;
const NAMESPACED_FIELD = /__.*__c$/;
const ID = /^[A-Za-z0-9]{15,18}$/;

export const TOOLING_PARTIAL_KEYS = ["metadata-tooling"];

type Row = Record<string, any>;
type ItemRead = { key: string; fullName: string | null; metadata: Row | null };

const own = (row: Row): boolean => !row.NamespacePrefix;

// Whole, then value by value: in JSON a quote in a formula is \", which the
// patterns don't see, and an item is kept in the partial checkpoint as it's read.
const redactValues = (value: unknown): unknown =>
  typeof value === "string" ? redactSecrets(value) : Array.isArray(value) ? value.map(redactValues) : value !== null && typeof value === "object" ? Object.fromEntries(Object.entries(value).map(([k, v]) => [k, redactValues(v)])) : value;
const quote = (values: string[]): string => values.map((v) => `'${v.replace(/'/g, "\\'")}'`).join(", ");

class ItemReader {
  readonly #ctx: ExtractContext;
  readonly #done: Map<string, ItemRead>;
  reads = 0;

  constructor(ctx: ExtractContext) {
    this.#ctx = ctx;
    this.#done = new Map(ctx.partial.read<ItemRead>(TOOLING_PARTIAL_KEYS[0]!).map((r) => [r.key, r]));
  }

  /** One item's Metadata, redacted before it's kept anywhere. Resumes from the checkpoint. */
  async read(type: string, id: string): Promise<ItemRead> {
    const key = `${type}/${id}`;
    const saved = this.#done.get(key);
    if (saved) return saved;
    if (!ID.test(id)) throw new Error(`Not an id: ${id}`);
    this.reads++;
    const rows = await this.#ctx.client.queryAll<Row>(`SELECT Id, FullName, Metadata FROM ${type} WHERE Id = '${id}'`, { api: "tooling" });
    const raw = rows[0]?.Metadata ?? null;
    let metadata: Row | null = null;
    if (raw) {
      try {
        metadata = redactValues(JSON.parse(redactSecrets(JSON.stringify(raw)))) as Row;
      } catch {
        metadata = null;
      }
    }
    const item: ItemRead = { key, fullName: rows[0]?.FullName ?? null, metadata };
    this.#done.set(key, item);
    this.#ctx.partial.append(TOOLING_PARTIAL_KEYS[0]!, [item]);
    return item;
  }
}

async function tooling(ctx: ExtractContext, soql: string): Promise<Row[]> {
  return ctx.client.queryAll<Row>(soql, { api: "tooling" });
}

/**
 * Optional reads: a failure leaves the kind unsearched rather than failing the
 * bundle. When rules count the kind too, it's also marked unread, with why.
 */
async function optional<T>(bundle: MetadataBundle, kind: string, read: () => Promise<T>, unread?: UnreadKind): Promise<T | null> {
  try {
    return await read();
  } catch (err) {
    if (!isUnavailable(err)) throw err;
    bundle.unsearched!.push(kind);
    if (unread) markUnread(bundle, unread, readableReason(err.message));
    return null;
  }
}

function markUnread(bundle: MetadataBundle, kind: UnreadKind, reason: string): void {
  bundle.unread ??= {};
  bundle.unread[kind] ??= reason;
}

export async function readBundleThroughTooling(ctx: ExtractContext): Promise<MetadataBundle> {
  const bundle: MetadataBundle = { route: "tooling", unsearched: [], flows: [], workflows: [], validationRules: [], customFields: [], recordTypes: [], objects: [], references: { users: {}, fields: {} }, filesRead: 0, handbook: { flows: [], validationRules: [] } };
  const reader = new ItemReader(ctx);
  const index = (metadata: Row | null, path: string): void => {
    if (metadata) indexReferences(bundle.references, JSON.stringify(metadata), path);
  };

  // Objects. Tooling's CustomObject needs more than View Setup, and EntityDefinition
  // doesn't. A custom object's DurableId is its 01I id, in 15 characters.
  const entities = await tooling(ctx, "SELECT DurableId, QualifiedApiName, NamespacePrefix FROM EntityDefinition WHERE IsCustomizable = true AND IsCustomSetting = false");
  const nameById = new Map(entities.map((e) => [String(e.DurableId).slice(0, 15), String(e.QualifiedApiName)]));
  const objectName = (value: string): string => nameById.get(value.slice(0, 15)) ?? value;
  const customObjects = entities.filter((e) => own(e) && /__c$/.test(e.QualifiedApiName)).map((e) => String(e.QualifiedApiName)).sort();
  bundle.objects = [...CORE, ...customObjects];

  // Flows, Process Builders included. The retrieve gives each flow's active
  // version, or its latest when none is active, so this does the same.
  const definitions = (await tooling(ctx, "SELECT Id, DeveloperName, ActiveVersionId, LatestVersionId, NamespacePrefix FROM FlowDefinition WHERE NamespacePrefix = null")).filter(own);
  const versions = definitions.map((d) => ({ name: String(d.DeveloperName), id: (d.ActiveVersionId ?? d.LatestVersionId) as string | null })).filter((v) => v.id);
  if (versions.length > MAX_ITEM_READS) throw new ToolingGapError(`There are ${versions.length} flows, more than the ${MAX_ITEM_READS} we read one at a time.`);
  for (const version of versions.sort((a, b) => a.name.localeCompare(b.name))) {
    const { metadata } = await reader.read("Flow", version.id!);
    if (!metadata) throw new ToolingGapError(`Flow ${version.name} came back without its definition.`);
    const summary = summariseFlowMetadata(version.name, metadata);
    bundle.flows.push(summary);
    bundle.handbook!.flows.push(flowDetailMetadata(version.name, metadata));
    index(metadata, summary.path);
    bundle.filesRead++;
  }

  // Workflow Rules, grouped by object as the retrieve's files are.
  const rules = await optional(bundle, "Workflow Rules", async () => (await tooling(ctx, "SELECT Id, Name, TableEnumOrId, NamespacePrefix FROM WorkflowRule")).filter(own), "workflowRules");
  if (rules && rules.length + versions.length > MAX_ITEM_READS) {
    bundle.unsearched!.push("Workflow Rules");
    markUnread(bundle, "workflowRules", `There are ${rules.length} Workflow Rules and ${versions.length} flows, more than the ${MAX_ITEM_READS} we read one at a time.`);
  } else if (rules) {
    const byObject = new Map<string, WorkflowSummary>();
    for (const rule of [...rules].sort((a, b) => String(a.Name).localeCompare(String(b.Name)))) {
      const object = objectName(String(rule.TableEnumOrId));
      const { metadata } = await reader.read("WorkflowRule", rule.Id);
      if (!metadata) {
        bundle.unsearched!.push("Workflow Rules");
        markUnread(bundle, "workflowRules", `Workflow Rule ${String(rule.Name)} came back without its definition.`);
        continue;
      }
      const summary = byObject.get(object) ?? { object, path: `workflows/${object}.workflow`, rules: [] };
      summary.rules.push(summariseWorkflowRule(String(rule.Name), metadata));
      byObject.set(object, summary);
      index(metadata, summary.path);
    }
    bundle.workflows = [...byObject.values()].sort((a, b) => a.object.localeCompare(b.object));
    bundle.filesRead += bundle.workflows.length;
  }

  // Workflow actions live in the same file as the rules in a retrieve. They're
  // read for what they mention: a field update's field, an alert's recipients.
  for (const [type, kind] of [
    ["WorkflowFieldUpdate", "workflow field updates"],
    ["WorkflowAlert", "workflow email alerts"],
    ["WorkflowTask", "workflow tasks"],
    ["WorkflowOutboundMessage", "workflow outbound messages"],
  ] as const) {
    const actions = await optional(bundle, kind, async () => (await tooling(ctx, `SELECT Id, NamespacePrefix FROM ${type}`)).filter(own));
    if (!actions || actions.length === 0) continue;
    if (reader.reads + actions.length > MAX_ITEM_READS) {
      bundle.unsearched!.push(kind);
      continue;
    }
    for (const action of actions) {
      const item = await optional(bundle, kind, () => reader.read(type, action.Id));
      if (!item) break;
      // A definition we didn't get can't count as searched: an alert's recipients
      // could name the very user a guard is about (rule 3).
      if (!item.metadata) {
        bundle.unsearched!.push(kind);
        continue;
      }
      const [object, name] = String(item.fullName ?? `Unknown.${action.Id}`).split(".");
      index(item.metadata, `workflows/${object}.workflow#${type}.${name ?? action.Id}`);
    }
  }

  // Validation rules. The list has what the rules need, and the formula, for
  // what it mentions, is one more read each.
  const validation = await optional(bundle, "validation rules", async () =>
    (await tooling(ctx, "SELECT Id, ValidationName, Active, Description, EntityDefinitionId, NamespacePrefix FROM ValidationRule")).filter(own), "validationRules",
  );
  if (validation) {
    const search = reader.reads + validation.length <= MAX_ITEM_READS;
    if (!search) bundle.unsearched!.push("validation rule formulas");
    for (const rule of [...validation].sort((a, b) => `${objectName(a.EntityDefinitionId)}.${a.ValidationName}`.localeCompare(`${objectName(b.EntityDefinitionId)}.${b.ValidationName}`))) {
      const object = objectName(String(rule.EntityDefinitionId));
      const name = String(rule.ValidationName);
      bundle.validationRules.push({ object, name, active: String(rule.Active) === "true", described: hasText(rule.Description) });
      const item = search ? await optional(bundle, "validation rule formulas", () => reader.read("ValidationRule", rule.Id)) : null;
      bundle.handbook!.validationRules.push(toolingValidationRule(object, rule, item?.metadata ?? null));
      if (!search) continue;
      if (item?.metadata) indexReferences(bundle.references, String(item.metadata.errorConditionFormula ?? ""), `objects/${object}.object#validationRules.${name}`);
      else if (item) bundle.unsearched!.push("validation rule formulas");
    }
  }

  // Custom fields of our own, with their description and help text, on the core
  // objects and every custom object of your own.
  const descriptions = new Map<string, boolean>();
  const help = new Map<string, boolean>();
  for (let i = 0; i < bundle.objects.length; i += OBJECT_CHUNK) {
    const chunk = quote(bundle.objects.slice(i, i + OBJECT_CHUNK));
    for (const row of await tooling(ctx, `SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, Description, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN (${chunk})`)) {
      descriptions.set(`${row.EntityDefinition?.QualifiedApiName}.${row.QualifiedApiName}`, hasText(row.Description));
    }
    for (const row of await tooling(ctx, `SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, InlineHelpText FROM EntityParticle WHERE EntityDefinition.QualifiedApiName IN (${chunk})`)) {
      help.set(`${row.EntityDefinition?.QualifiedApiName}.${row.QualifiedApiName}`, hasText(row.InlineHelpText));
    }
  }
  bundle.customFields = [...descriptions]
    .map(([key, described]) => {
      const dot = key.indexOf(".");
      return { object: key.slice(0, dot), name: key.slice(dot + 1), described, helpText: help.get(key) ?? false };
    })
    .filter((f) => OWN_FIELD.test(f.name) && !NAMESPACED_FIELD.test(f.name))
    .sort((a, b) => `${a.object}.${a.name}`.localeCompare(`${b.object}.${b.name}`));

  // Record types. Tooling has no DeveloperName on RecordType, so the name here is
  // the label. Only counts use it.
  const recordTypes = await tooling(ctx, "SELECT Id, Name, SobjectType, IsActive, NamespacePrefix FROM RecordType");
  bundle.recordTypes = recordTypes
    .filter((r) => bundle.objects.includes(String(r.SobjectType)))
    .map((r) => ({ object: String(r.SobjectType), name: String(r.Name), label: String(r.Name), active: String(r.IsActive) === "true" }))
    .sort((a, b) => `${a.object}.${a.name}`.localeCompare(`${b.object}.${b.name}`));

  // Approval processes and assignment rules have no Tooling definition we can
  // read. If any exist, a reference search can't call itself complete.
  const approvals = await optional(bundle, "approval processes", () => ctx.client.queryAll<Row>("SELECT Id FROM ProcessDefinition WHERE Type = 'Approval'"));
  if (approvals && approvals.length > 0) bundle.unsearched!.push("approval processes");
  const assignment = await optional(bundle, "assignment rules", () => ctx.client.queryAll<Row>("SELECT Id FROM AssignmentRule"));
  if (assignment && assignment.length > 0) bundle.unsearched!.push("assignment rules");

  bundle.unsearched = [...new Set(bundle.unsearched)];
  return bundle;
}

/**
 * A validation rule's handbook detail on this route: whether it's active and its
 * description from the list, as the summary takes them, and the formula, message
 * and field from the definition already read for references. With no
 * definition, those three are null rather than guessed.
 */
export function toolingValidationRule(object: string, row: Row, metadata: Row | null): HandbookValidationRule {
  const definition = metadata ? { errorConditionFormula: metadata.errorConditionFormula, errorMessage: metadata.errorMessage, errorDisplayField: metadata.errorDisplayField } : {};
  return validationRuleDetail(object, String(row.ValidationName), { ...definition, active: row.Active, description: row.Description });
}

/** The Tooling route can't give a whole picture. Reported as unavailable, like a failed read. */
export class ToolingGapError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ToolingGapError";
  }
}
