import { apexExtractor, metadataExtractor } from "../../extract/automation.ts";
import { unreadReason } from "../../extract/metadata-parse.ts";
import { CORE_OBJECTS, customObjectsExtractor, isOwnCustomField, layoutsExtractor, recordCountsExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { objectResolver } from "../object-names.ts";
import { opportunitiesGate } from "../gate.ts";
import type { RuleContext } from "../../engine/rule.ts";
import type { Evidence, Metric, MetricSegment } from "../../schemas/index.ts";

// Where the complexity lives: a count of what each busy object carries, for
// whoever has to pick the org up. A real customer's complaint was an org nobody
// new could use or manage, with no record of its automation.

export const OBJECT_INVENTORY = "configuration.object_inventory";

type Ctx = Omit<RuleContext, "metrics">;

/** In table order. The dimension is what a segment is filed under. */
export const INVENTORY_COLUMNS = [
  { key: "records", dimension: "records", heading: "Records" },
  { key: "fields", dimension: "custom fields", heading: "Custom fields" },
  { key: "recordTypes", dimension: "record types", heading: "Record types" },
  { key: "layouts", dimension: "page layouts", heading: "Page layouts" },
  { key: "validationRules", dimension: "validation rules", heading: "Validation rules" },
  { key: "flows", dimension: "record-triggered flows", heading: "Record-triggered flows" },
  { key: "processBuilders", dimension: "Process Builders", heading: "Process Builders" },
  { key: "workflowRules", dimension: "Workflow Rules", heading: "Workflow Rules" },
  { key: "triggers", dimension: "Apex triggers", heading: "Apex triggers" },
  { key: "undocumented", dimension: "automation with no description", heading: "No description" },
] as const;

export type InventoryColumn = (typeof INVENTORY_COLUMNS)[number]["key"];
export type InventoryRow = { object: string; counts: Record<InventoryColumn, number | null>; reasons: Partial<Record<InventoryColumn, string>> };

const AUTOMATION: InventoryColumn[] = ["validationRules", "flows", "processBuilders", "workflowRules", "triggers"];
export const automationOf = (row: InventoryRow): number => AUTOMATION.reduce((sum, key) => sum + (row.counts[key] ?? 0), 0);

const OWN_CUSTOM_OBJECT = /^[A-Za-z][A-Za-z0-9_]*__c$/;
const NAMESPACED = /__.+__c$/;
const isOwnCustomObject = (name: string): boolean => OWN_CUSTOM_OBJECT.test(name) && !NAMESPACED.test(name);
const countBy = <T>(items: T[], key: (item: T) => string | null): Map<string, number> => {
  const out = new Map<string, number>();
  for (const item of items) {
    const k = key(item);
    if (k !== null) out.set(k, (out.get(k) ?? 0) + 1);
  }
  return out;
};

const isRecordTriggered = (triggerType: string | null): boolean => /^Record(After|Before)/.test(triggerType ?? "");

const DEFINITION =
  "For each core object, and each custom object of your own with records: its custom fields, active record types, page layouts, and the active validation rules, record-triggered flows, Process Builders, Workflow Rules and Apex triggers that run on it, with how many of those have no description (for a trigger, no comment in the code). The value is the number of objects counted.";

const base = { id: OBJECT_INVENTORY, domain: "configuration" as const, label: "Objects in the complexity inventory", unit: "count" as const, definition: DEFINITION };

export function objectInventoryMetric(ctx: Ctx): Metric[] {
  const metadata = ctx.data(metadataExtractor);
  const { layouts } = ctx.data(layoutsExtractor);
  const { triggers } = ctx.data(apexExtractor);
  const ids = ctx.data(customObjectsExtractor);
  const counts = ctx.data(recordCountsExtractor);
  const metadataSource = { kind: "metadata", path: "objects, flows, workflows" } as const;
  const evidence: Evidence[] = [
    ctx.ev(metadataSource, { count: metadata.status === "ok" ? metadata.data.objects.length : null, note: "Object definitions read" }),
    ctx.ev(layouts.source, { count: layouts.status === "ok" ? layouts.data.length : null }),
    ctx.ev(triggers.source, { count: triggers.status === "ok" ? triggers.data.length : null }),
    ctx.ev(ids.source, { count: ids.status === "ok" ? ids.data.length : null }),
    ctx.ev(counts.source, { count: counts.status === "ok" ? counts.data.length : null }),
  ];
  const exclusions = ["Objects from installed packages", "Custom objects with no records", "Inactive validation rules, flows, Workflow Rules, triggers and record types", "Flows that aren't record-triggered, and Apex classes, because they don't belong to one object"];
  const assumptions = ["Record counts are the approximate figures Salesforce keeps for each object", "Ranked by how much automation runs on each object, then by custom fields"];
  if (metadata.status !== "ok") return [{ ...base, status: "not_measurable", reason: `Object metadata couldn't be retrieved: ${metadata.reason}`, evidence, exclusions, assumptions }];
  // Every object's row counts Workflow Rules and validation rules, so one read
  // only in part would undercount them all (rule 5).
  const unread = unreadReason(metadata.data, ["workflowRules", "validationRules"]);
  if (unread) return [{ ...base, status: "not_measurable", reason: unread, evidence, exclusions, assumptions }];
  const gate = opportunitiesGate(ctx);
  if (gate) return [{ ...base, status: "not_measurable", reason: gate, evidence, exclusions, assumptions }];

  const bundle = metadata.data;
  const records = new Map(counts.status === "ok" ? counts.data.map((c) => [c.name, c.count]) : []);
  const custom =
    counts.status === "ok"
      ? counts.data.filter((c) => c.count > 0 && isOwnCustomObject(c.name)).map((c) => c.name)
      : bundle.objects.filter(isOwnCustomObject);
  if (counts.status !== "ok") assumptions.push(`Record counts couldn't be read, so every custom object of your own is counted, with or without records: ${counts.reason}`);
  const objects = [...new Set([...CORE_OBJECTS, ...custom.sort((a, b) => a.localeCompare(b))])];
  const defined = new Set(bundle.objects);
  // Core objects are counted from their describe, as the field sprawl check counts
  // them, and custom objects from their definition in the retrieve.
  const schema = ctx.data(schemaExtractor);
  const described = (object: string) => {
    const summary = schema[object];
    return summary?.status === "ok" ? summary.data : null;
  };

  const objectOf = objectResolver(ids);
  const layoutCount = layouts.status === "ok" ? countBy(layouts.data, (l) => objectOf(l.EntityDefinitionId)) : null;
  const activeTriggers = triggers.status === "ok" ? triggers.data.filter((t) => t.Status === "Active") : [];
  const triggerCount = triggers.status === "ok" ? countBy(activeTriggers, (t) => objectOf(t.TableEnumOrId)) : null;
  const unnamedLayouts = layouts.status === "ok" ? layouts.data.filter((l) => objectOf(l.EntityDefinitionId) === null).length : 0;
  const unnamedTriggers = activeTriggers.filter((t) => objectOf(t.TableEnumOrId) === null).length;
  if (unnamedLayouts + unnamedTriggers > 0) {
    exclusions.push(`Page layouts (${unnamedLayouts}) and Apex triggers (${unnamedTriggers}) on custom objects we couldn't name${ids.status === "ok" ? "" : `, because the list of custom objects couldn't be read: ${ids.reason}`}`);
  }
  if (layouts.status !== "ok") exclusions.push(`Page layouts, which couldn't be read: ${layouts.reason}`);
  if (triggers.status !== "ok") exclusions.push(`Apex triggers, which couldn't be read: ${triggers.reason}`);

  const flows = bundle.flows.filter((f) => f.status === "Active");
  const recordFlows = flows.filter((f) => f.processType !== "Workflow" && isRecordTriggered(f.triggerType));
  const processes = flows.filter((f) => f.processType === "Workflow");
  const rules = bundle.workflows.flatMap((w) => w.rules.filter((r) => r.active).map((r) => ({ ...r, object: w.object })));
  const validation = bundle.validationRules.filter((v) => v.active);

  const rows: InventoryRow[] = objects.map((object) => {
    const reasons: InventoryRow["reasons"] = {};
    const describe = described(object);
    const fromDefinition = (fromDescribe: (d: NonNullable<typeof describe>) => number, fromMetadata: number, column: InventoryColumn): number | null => {
      if (describe) return fromDescribe(describe);
      if (defined.has(object)) return fromMetadata;
      reasons[column] = "Not in the metadata retrieve";
      return null;
    };
    const own = <T extends { object: string | null }>(items: T[]): T[] => items.filter((i) => i.object === object);
    const ownTriggers = activeTriggers.filter((t) => objectOf(t.TableEnumOrId) === object);
    const undocumented = [...own(validation), ...own(recordFlows), ...own(processes), ...own(rules)].filter((x) => !x.described).length + ownTriggers.filter((t) => t.commented === false).length;
    if (layoutCount === null) reasons.layouts = "Page layouts couldn't be read";
    if (triggerCount === null) reasons.triggers = "Apex triggers couldn't be read";
    if (!records.has(object)) reasons.records = "Salesforce keeps no count for this object";
    return {
      object,
      reasons,
      counts: {
        records: records.get(object) ?? null,
        fields: fromDefinition((d) => d.fields.filter(isOwnCustomField).length, bundle.customFields.filter((f) => f.object === object).length, "fields"),
        recordTypes: fromDefinition((d) => d.recordTypes.filter((r) => r.active).length, bundle.recordTypes.filter((r) => r.object === object && r.active).length, "recordTypes"),
        layouts: layoutCount === null ? null : (layoutCount.get(object) ?? 0),
        validationRules: own(validation).length,
        flows: own(recordFlows).length,
        processBuilders: own(processes).length,
        workflowRules: own(rules).length,
        triggers: triggerCount === null ? null : (triggerCount.get(object) ?? 0),
        undocumented,
      },
    };
  });
  rows.sort((a, b) => automationOf(b) - automationOf(a) || (b.counts.fields ?? 0) - (a.counts.fields ?? 0) || a.object.localeCompare(b.object));

  const segments: MetricSegment[] = rows.flatMap((row) =>
    INVENTORY_COLUMNS.map((c) => {
      const value = row.counts[c.key];
      return { dimension: c.dimension, key: row.object, n: value ?? 0, value, notMeasurableReason: value === null ? (row.reasons[c.key] ?? "Not read") : null };
    }),
  );
  const sum = (key: InventoryColumn): number => rows.reduce((total, row) => total + (row.counts[key] ?? 0), 0);
  const automation = rows.reduce((total, row) => total + automationOf(row), 0);
  return [
    {
      ...base,
      status: "measured",
      value: rows.length,
      n: rows.length,
      dateRange: null,
      segments,
      segmentExemptReason: null,
      series: [],
      stats: {
        coreObjects: CORE_OBJECTS.length,
        customObjects: rows.length - CORE_OBJECTS.length,
        customFields: sum("fields"),
        automation,
        undocumented: sum("undocumented"),
        objectsWithAutomation: rows.filter((row) => automationOf(row) > 0).length,
      },
      evidence,
      exclusions,
      assumptions,
    },
  ];
}

/** Rebuilds the table from a measured inventory, in ranked order, for the documents. */
export function inventoryRows(metric: Metric | undefined): InventoryRow[] {
  if (metric?.status !== "measured") return [];
  const rows = new Map<string, InventoryRow>();
  for (const s of metric.segments) {
    const column = INVENTORY_COLUMNS.find((c) => c.dimension === s.dimension);
    if (!column) continue;
    const row = rows.get(s.key) ?? { object: s.key, counts: Object.fromEntries(INVENTORY_COLUMNS.map((c) => [c.key, null])) as InventoryRow["counts"], reasons: {} };
    row.counts[column.key] = s.value;
    if (s.notMeasurableReason) row.reasons[column.key] = s.notMeasurableReason;
    rows.set(s.key, row);
  }
  return [...rows.values()];
}
