import { apexExtractor, metadataExtractor, type ApexTriggerRow } from "../../extract/automation.ts";
import { loginActivityExtractor } from "../../extract/adoption.ts";
import { CORE_OBJECTS, customObjectsExtractor, duplicateRulesExtractor, fieldUsageExtractor, isOwnCustomField, orgExtractor, recordCountsExtractor, schemaExtractor } from "../../extract/configuration.ts";
import { orgDocumentationExtractor } from "../../extract/documentation.ts";
import { unreadReason } from "../../extract/metadata-parse.ts";
import type { Sourced } from "../../extract/types.ts";
import { loginsExtractor, usersExtractor } from "../../extract/users.ts";
import { pct, type RuleContext } from "../../engine/rule.ts";
import type { Evidence, Metric, MetricSegment } from "../../schemas/index.ts";
import { partialHistory } from "../adoption/index.ts";
import { opportunitiesGate } from "../gate.ts";
import { objectResolver } from "../object-names.ts";

// Roadmap 3.4, "Your org, documented": a description, not a judgement, so it
// raises no findings. It travels as metrics, the one shape every output and the
// number check already read, with each list held in its segments. This file is
// the only place that writes or reads those segments (`readDocument`), so the
// full report, the raw notes and the microsite can't describe the org
// differently. Names here are set-up names (objects, fields, flows, profiles),
// never a person's.

type Ctx = Omit<RuleContext, "metrics">;
type Measured = Extract<Metric, { status: "measured" }>;

export const DOCUMENTED = {
  model: "documented.data_model",
  relationships: "documented.relationships",
  automation: "documented.automation",
  integrations: "documented.integrations",
  access: "documented.users_and_access",
  sharing: "documented.sharing",
} as const;
export const DOCUMENTED_METRIC_IDS: string[] = Object.values(DOCUMENTED);

/**
 * What runs when a record is saved, in Salesforce's order ("Triggers and Order
 * of Execution", Apex Developer Guide, API 68.0). `step` is the guide's own step
 * number. Delete and undelete aren't part of that order, so they come last.
 */
export const AUTOMATION_STEPS = [
  { key: "before-flow", label: "Before-save flows", step: 3 },
  { key: "before-trigger", label: "Apex before triggers", step: 4 },
  { key: "validation", label: "Validation rules", step: 5 },
  { key: "duplicate", label: "Duplicate rules", step: 6 },
  { key: "after-trigger", label: "Apex after triggers", step: 8 },
  { key: "workflow", label: "Workflow Rules", step: 11 },
  { key: "process", label: "Process Builders", step: 13 },
  { key: "after-flow", label: "After-save flows", step: 14 },
  { key: "trigger", label: "Apex triggers, timing not read", step: null },
  { key: "delete", label: "On delete or undelete", step: null },
] as const;
export type StepKey = (typeof AUTOMATION_STEPS)[number]["key"];

/** Plain names for the sharing models Salesforce reports. Anything else is shown as it comes. */
const SHARING_LABELS: Record<string, string> = {
  None: "Private",
  Private: "Private",
  Read: "Public Read Only",
  Edit: "Public Read/Write",
  ReadWrite: "Public Read/Write",
  ReadEditTransfer: "Public Read/Write/Transfer",
  FullAccess: "Public Full Access",
  ControlledByParent: "Controlled by parent",
  ControlledByLeadOrContact: "Controlled by lead or contact",
  ControlledByCampaign: "Controlled by campaign",
};
export const sharingLabel = (model: string): string => SHARING_LABELS[model] ?? model;

const MOST_FILLED = 10;
const COUNT_ON_SAVE = "count on save";
const SEP = " | ";
const OWN_CUSTOM_OBJECT = /^[A-Za-z][A-Za-z0-9_]*__c$/;
const NAMESPACED = /__.+__c$/;

const base = (id: string, label: string, definition: string) => ({ id, domain: "configuration" as const, label, unit: "count" as const, definition });
const DEFINITIONS = {
  model: base(DOCUMENTED.model, "Objects documented", "The core objects, and custom objects of your own with records: their records, custom fields and record types, with the most-filled custom fields where field use was measured. The value is the number of objects."),
  relationships: base(DOCUMENTED.relationships, "Relationships between documented objects", "Lookup and master-detail fields from one documented object to another. Lookups to users, queues and other set-up objects are left out. The value is the number of relationships."),
  automation: base(DOCUMENTED.automation, "Automation on save, by object", "Every record-triggered flow, Apex trigger, validation rule, duplicate rule, Workflow Rule and Process Builder, active or not, by object, in the order Salesforce runs them when a record is saved. The value is the number listed."),
  integrations: base(DOCUMENTED.integrations, "Integration points", "Connected apps, named credentials and remote site settings, with integration users and the API logins we can see. The value is the number of connected apps, named credentials and remote sites."),
  access: base(DOCUMENTED.access, "Active users", "Active users by profile and by role, the depth of the role hierarchy, and each permission set with how many active users hold it. The value is the number of active users."),
  sharing: base(DOCUMENTED.sharing, "Objects with a sharing model", "The organisation-wide default for each documented object, for internal and external users. The value is the number of objects with one."),
};

// ─── Building ────────────────────────────────────────────────────────────────

const seg = (dimension: string, key: string, value: number | null, n = value ?? 0, notMeasurableReason: string | null = null): MetricSegment => ({ dimension, key, n: Math.max(0, Math.round(n)), value, notMeasurableReason });

function measured(d: (typeof DEFINITIONS)[keyof typeof DEFINITIONS], evidence: Evidence[], value: number, segments: MetricSegment[], extra: { exclusions?: string[]; assumptions?: string[]; stats?: Record<string, number> } = {}): Metric {
  return { ...d, status: "measured", evidence, exclusions: extra.exclusions ?? [], assumptions: extra.assumptions ?? [], value, n: value, dateRange: null, segments, segmentExemptReason: segments.length ? null : "There's nothing to list.", series: [], stats: extra.stats ?? {} };
}
const notMeasurable = (d: (typeof DEFINITIONS)[keyof typeof DEFINITIONS], evidence: Evidence[], reason: string): Metric => ({ ...d, status: "not_measurable", evidence, exclusions: [], assumptions: [], reason });
const cantRead = (what: string, s: Sourced<unknown>): string | null => (s.status === "ok" ? null : `${what} couldn't be read: ${s.reason}`);

function dataModel(ctx: Ctx): Metric[] {
  const doc = ctx.data(orgDocumentationExtractor);
  const counts = ctx.data(recordCountsExtractor);
  const metadata = ctx.data(metadataExtractor);
  const schema = ctx.data(schemaExtractor);
  const evidence = [ctx.ev(counts.source, { count: counts.status === "ok" ? counts.data.length : null }), ctx.ev(metadata.source, { count: metadata.status === "ok" ? metadata.data.objects.length : null, note: "Object definitions read" })];
  const records = new Map(counts.status === "ok" ? counts.data.map((c) => [c.name, c.count]) : []);
  const bundle = metadata.status === "ok" ? metadata.data : null;
  const exclusions = ["Objects from installed packages", "Custom objects with no records"];
  const assumptions = ["Record counts are the approximate figures Salesforce keeps for each object"];
  if (counts.status !== "ok") assumptions.push(`Record counts couldn't be read, so every custom object of your own is listed: ${counts.reason}`);
  if (!bundle) exclusions.push(`Custom fields and record types on custom objects, because object metadata couldn't be read: ${metadata.status === "ok" ? "" : metadata.reason}`);

  // Field use is counted from records, which a set-up review never reads.
  const usage = ctx.scope === "essentials" ? null : ctx.data(fieldUsageExtractor);
  if (!usage) exclusions.push("The most-filled custom fields, because a set-up review doesn't read records");
  const segments: MetricSegment[] = [];
  for (const object of doc.objects) {
    const describe = schema[object]?.status === "ok" ? schema[object].data : null;
    const count = records.get(object) ?? null;
    segments.push(seg("records", object, count, count ?? 0, count === null ? "Salesforce keeps no count for this object" : null));
    const fields = describe ? describe.fields.filter(isOwnCustomField).length : bundle?.objects.includes(object) ? bundle.customFields.filter((f) => f.object === object).length : null;
    segments.push(seg("custom fields", object, fields, fields ?? 0, fields === null ? "Not in the metadata read" : null));
    const types = describe ? describe.recordTypes.map((r) => ({ name: r.developerName, active: r.active })) : (bundle?.recordTypes.filter((r) => r.object === object).map((r) => ({ name: r.name, active: r.active })) ?? []);
    segments.push(seg("record types", object, types.filter((t) => t.active).length));
    for (const t of types.sort((a, b) => a.name.localeCompare(b.name))) segments.push(seg(`record type on ${object}`, t.name, t.active ? 1 : 0, 1));
    const used = usage?.[object];
    if (used?.status === "ok") {
      const ranked = used.data.fields.filter((f) => f.total > 0).sort((a, b) => b.filled / b.total - a.filled / a.total || a.field.localeCompare(b.field)).slice(0, MOST_FILLED);
      for (const f of ranked) segments.push(seg(`most filled on ${object}`, f.field, pct(f.filled, f.total), f.total));
    }
  }
  const out: Metric[] = [measured(DEFINITIONS.model, evidence, doc.objects.length, segments, { exclusions, assumptions })];

  const rel = doc.relationships;
  const relEvidence = [ctx.ev(rel.source, { count: rel.status === "ok" ? rel.data.length : null })];
  if (rel.status !== "ok") out.push(notMeasurable(DEFINITIONS.relationships, relEvidence, `Relationship fields couldn't be read: ${rel.reason}`));
  else {
    const documented = new Set(doc.objects);
    const links = rel.data
      .flatMap((r) => r.referenceTo.filter((to) => documented.has(to)).map((to) => ({ kind: /^Master-Detail/i.test(r.dataType) ? "master-detail" : "lookup", key: `${r.object}.${r.field} to ${to}` })))
      .sort((a, b) => a.key.localeCompare(b.key));
    out.push(measured(DEFINITIONS.relationships, relEvidence, links.length, links.map((l) => seg(l.kind, l.key, 1)), { exclusions: ["Lookups to objects outside the ones documented, such as users and queues"], assumptions: ["A field whose type starts Master-Detail is a master-detail relationship, and any other field that refers to an object is a lookup"] }));
  }
  return out;
}

type Item = { object: string; step: StepKey; name: string; active: boolean; described: boolean | null };

const triggerSteps = (t: ApexTriggerRow): StepKey[] => {
  const flags = [t.UsageBeforeInsert, t.UsageBeforeUpdate, t.UsageAfterInsert, t.UsageAfterUpdate, t.UsageBeforeDelete, t.UsageAfterDelete, t.UsageAfterUndelete];
  if (flags.every((f) => f === undefined)) return ["trigger"];
  const steps: StepKey[] = [];
  if (t.UsageBeforeInsert || t.UsageBeforeUpdate) steps.push("before-trigger");
  if (t.UsageAfterInsert || t.UsageAfterUpdate) steps.push("after-trigger");
  if (t.UsageBeforeDelete || t.UsageAfterDelete || t.UsageAfterUndelete) steps.push("delete");
  return steps;
};

function automation(ctx: Ctx): Metric {
  const metadata = ctx.data(metadataExtractor);
  const { triggers } = ctx.data(apexExtractor);
  const duplicates = ctx.data(duplicateRulesExtractor);
  const ids = ctx.data(customObjectsExtractor);
  const evidence = [ctx.ev({ kind: "metadata", path: "flows, workflows, objects" }, { count: metadata.status === "ok" ? metadata.data.filesRead : null }), ctx.ev(triggers.source, { count: triggers.status === "ok" ? triggers.data.length : null }), ctx.ev(duplicates.source, { count: duplicates.status === "ok" ? duplicates.data.length : null })];
  if (metadata.status !== "ok") return notMeasurable(DEFINITIONS.automation, evidence, `Flows, Workflow Rules and validation rules couldn't be read: ${metadata.reason}`);
  // A list missing whole kinds would read as an object with less automation than it has (rule 5).
  const unread = unreadReason(metadata.data, ["workflowRules", "validationRules"]);
  if (unread) return notMeasurable(DEFINITIONS.automation, evidence, unread);
  const bundle = metadata.data;
  const items: Item[] = [];
  for (const f of bundle.flows) {
    if (!f.object) continue;
    const step: StepKey | null = f.processType === "Workflow" ? "process" : f.triggerType === "RecordBeforeSave" ? "before-flow" : f.triggerType === "RecordAfterSave" ? "after-flow" : f.triggerType === "RecordBeforeDelete" ? "delete" : null;
    if (step) items.push({ object: f.object, step, name: f.label || f.name, active: f.status === "Active", described: f.described });
  }
  for (const v of bundle.validationRules) items.push({ object: v.object, step: "validation", name: v.name, active: v.active, described: v.described });
  for (const w of bundle.workflows) for (const r of w.rules) items.push({ object: w.object, step: "workflow", name: r.name, active: r.active, described: r.described });
  const exclusions = ["Flows that aren't record-triggered, and Apex classes, because they don't belong to one object", "Objects from installed packages"];
  const objectOf = objectResolver(ids);
  if (triggers.status === "ok") {
    let unnamed = 0;
    for (const t of triggers.data) {
      const object = objectOf(t.TableEnumOrId);
      if (!object) {
        unnamed++;
        continue;
      }
      for (const step of triggerSteps(t)) items.push({ object, step, name: t.Name, active: t.Status === "Active", described: t.commented });
    }
    if (unnamed) exclusions.push(`${unnamed} Apex triggers on custom objects we couldn't name`);
  } else exclusions.push(`Apex triggers, which couldn't be read: ${triggers.reason}`);
  if (duplicates.status === "ok") for (const d of duplicates.data) items.push({ object: d.SobjectType, step: "duplicate", name: d.DeveloperName, active: d.IsActive, described: null });
  else exclusions.push(`Duplicate rules, which couldn't be read: ${duplicates.reason}`);

  const order = new Map(AUTOMATION_STEPS.map((s, i) => [s.key as string, i]));
  const own = items.filter((i) => !NAMESPACED.test(i.object));
  // Busiest object first, then the steps in the order they run.
  const weight = new Map<string, number>();
  for (const i of own) if (i.active) weight.set(i.object, (weight.get(i.object) ?? 0) + 1);
  own.sort((a, b) => (weight.get(b.object) ?? 0) - (weight.get(a.object) ?? 0) || a.object.localeCompare(b.object) || order.get(a.step)! - order.get(b.step)! || a.name.localeCompare(b.name));
  const state = (d: boolean | null): string => (d === null ? "not read" : d ? "described" : "no description");
  // Each object and step's count, active out of all, so a table of counts traces back here.
  const cells = new Map<string, { active: number; all: number }>();
  for (const i of own) {
    const cell = cells.get(`${i.object}${SEP}${i.step}`) ?? { active: 0, all: 0 };
    cell.all++;
    if (i.active) cell.active++;
    cells.set(`${i.object}${SEP}${i.step}`, cell);
  }
  return measured(
    DEFINITIONS.automation,
    evidence,
    own.length,
    [...own.map((i) => seg([i.object, i.step, state(i.described)].join(SEP), i.name, i.active ? 1 : 0, 1)), ...[...cells].map(([key, c]) => seg(COUNT_ON_SAVE, key, c.active, c.all))],
    {
      exclusions,
      assumptions: ["The order is Salesforce's for a record saved by insert, update or upsert. Within one step, Salesforce doesn't guarantee which runs first", "For an Apex trigger, a description means a comment in its code", "Duplicate rules are listed without a description, which we don't read"],
      stats: { objects: new Set(own.map((i) => i.object)).size, active: own.filter((i) => i.active).length },
    },
  );
}

function integrations(ctx: Ctx): Metric {
  const doc = ctx.data(orgDocumentationExtractor);
  const users = ctx.data(usersExtractor);
  const logins = ctx.data(loginsExtractor);
  const activity = ctx.data(loginActivityExtractor);
  const parts = [doc.connectedApps, doc.namedCredentials, doc.remoteSites] as const;
  const evidence = [...parts.map((p) => ctx.ev(p.source, { count: p.status === "ok" ? p.data.length : null })), ctx.ev(logins.source, { count: logins.status === "ok" ? logins.data.rowsRead : null })];
  if (parts.every((p) => p.status !== "ok")) return notMeasurable(DEFINITIONS.integrations, evidence, cantRead("Connected apps, named credentials and remote site settings", parts[0])!);
  const exclusions = [cantRead("Connected apps", doc.connectedApps), cantRead("Named credentials", doc.namedCredentials), cantRead("Remote site settings", doc.remoteSites)].filter((x): x is string => x !== null);
  exclusions.push("Installed packages' own connected apps, named credentials and remote sites", "Remote sites are shown by host only");
  const segments: MetricSegment[] = [];
  const own = <T extends { NamespacePrefix?: string | null }>(rows: T[]): T[] => rows.filter((r) => !r.NamespacePrefix);
  if (doc.connectedApps.status === "ok") for (const a of [...doc.connectedApps.data].sort((x, y) => x.Name.localeCompare(y.Name))) segments.push(seg("connected app", a.Name, a.OptionsAllowAdminApprovedUsersOnly ? 1 : 0, 1));
  if (doc.namedCredentials.status === "ok") for (const c of own(doc.namedCredentials.data).sort((x, y) => x.MasterLabel.localeCompare(y.MasterLabel))) segments.push(seg("named credential", c.MasterLabel, 1));
  if (doc.remoteSites.status === "ok") for (const r of own(doc.remoteSites.data).sort((x, y) => x.host.localeCompare(y.host))) segments.push(seg("remote site", r.host, r.IsActive ? 1 : 0, 1));
  const points = segments.length;

  // Who integrates: integration licences, and the accounts the calibration marks as integrations.
  if (users.status === "ok") {
    const marked = new Set(ctx.calibration.users.filter((u) => u.kind === "integration").map((u) => u.userId.slice(0, 15)));
    const groups = new Map<string, number>();
    for (const u of users.data.filter((x) => x.IsActive)) {
      const licence = u.Profile?.UserLicense?.Name ?? "";
      const key = /\bintegration\b/i.test(licence) ? licence : marked.has(u.Id.slice(0, 15)) ? "Marked as an integration in the calibration" : null;
      if (key) groups.set(key, (groups.get(key) ?? 0) + 1);
    }
    for (const [key, n] of [...groups].sort(([a], [b]) => a.localeCompare(b))) segments.push(seg("integration users", key, n));
  } else exclusions.push(`Integration users, because users couldn't be read: ${users.reason}`);

  // A connection without Monitor Login History sees only its own logins (DECISIONS.md 112).
  const partial = users.status === "ok" && activity.status === "ok" ? partialHistory(users.data.filter((u) => u.IsActive), activity.data) : null;
  if (logins.status !== "ok") exclusions.push(`API logins, because login history couldn't be read: ${logins.reason}`);
  else if (partial) exclusions.push(`API logins, because ${partial.charAt(0).toLowerCase()}${partial.slice(1)}`);
  else {
    const byApp = new Map<string, { logins: number; users: number }>();
    for (const api of Object.values(logins.data.apiByUser)) {
      const apps = api.applications.length ? api.applications : ["No application named"];
      for (const app of apps) {
        const entry = byApp.get(app) ?? { logins: 0, users: 0 };
        // A user's logins are counted once, under their first application, so the total stays true.
        if (app === apps[0]) entry.logins += api.logins;
        entry.users++;
        byApp.set(app, entry);
      }
    }
    for (const [app, e] of [...byApp].sort(([a], [b]) => a.localeCompare(b))) segments.push(seg("API logins by application", app, e.logins, e.users));
  }
  const apiTotal = logins.status === "ok" && !partial ? Object.values(logins.data.apiByUser).reduce((sum, a) => sum + a.logins, 0) : null;
  const kinds = (dimension: string): number => segments.filter((s) => s.dimension === dimension).length;
  return measured(DEFINITIONS.integrations, evidence, points, segments, {
    exclusions,
    assumptions: ["API logins are successful logins with an API type in the last 90 days. A user with several applications has their logins under the first"],
    stats: { connectedApps: kinds("connected app"), namedCredentials: kinds("named credential"), remoteSites: kinds("remote site"), ...(apiTotal === null ? {} : { apiLogins: apiTotal }) },
  });
}

/** How many levels the role hierarchy has, top role included. A loop counts each role once. */
export function roleDepth(roles: Array<{ Id: string; ParentRoleId: string | null }>): number {
  const parent = new Map(roles.map((r) => [r.Id, r.ParentRoleId]));
  let deepest = 0;
  for (const r of roles) {
    const seen = new Set<string>();
    let at: string | null = r.Id;
    while (at && parent.has(at) && !seen.has(at)) {
      seen.add(at);
      at = parent.get(at) ?? null;
    }
    deepest = Math.max(deepest, seen.size);
  }
  return deepest;
}

function access(ctx: Ctx): Metric {
  const users = ctx.data(usersExtractor);
  const doc = ctx.data(orgDocumentationExtractor);
  const evidence = [ctx.ev(users.source, { count: users.status === "ok" ? users.data.length : null }), ctx.ev(doc.roles.source, { count: doc.roles.status === "ok" ? doc.roles.data.length : null }), ctx.ev(doc.permissionSets.source, { count: doc.permissionSets.status === "ok" ? doc.permissionSets.data.length : null })];
  if (users.status !== "ok") return notMeasurable(DEFINITIONS.access, evidence, `Users couldn't be read: ${users.reason}`);
  const active = users.data.filter((u) => u.IsActive);
  const by = (key: (u: (typeof active)[number]) => string): Array<[string, number]> => {
    const m = new Map<string, number>();
    for (const u of active) m.set(key(u), (m.get(key(u)) ?? 0) + 1);
    return [...m].sort(([a, x], [b, y]) => y - x || a.localeCompare(b));
  };
  const segments: MetricSegment[] = [...by((u) => u.Profile?.Name ?? "No profile").map(([k, n]) => seg("profile", k, n)), ...by((u) => u.UserRole?.Name ?? "No role").map(([k, n]) => seg("role", k, n))];
  const exclusions = ["Inactive users", "Profiles' own permission sets, which nobody assigns"];
  const stats: Record<string, number> = {};
  if (doc.roles.status === "ok") Object.assign(stats, { roles: doc.roles.data.length, roleDepth: roleDepth(doc.roles.data) });
  else exclusions.push(`The role hierarchy, which couldn't be read: ${doc.roles.reason}`);
  if (doc.permissionSets.status === "ok") {
    for (const s of [...doc.permissionSets.data].sort((a, b) => b.assignees - a.assignees || a.Label.localeCompare(b.Label))) segments.push(seg("permission set", s.NamespacePrefix ? `${s.Label} (${s.NamespacePrefix})` : s.Label, s.assignees));
    stats.permissionSets = doc.permissionSets.data.length;
  } else exclusions.push(`Permission sets, which couldn't be read: ${doc.permissionSets.reason}`);
  return measured(DEFINITIONS.access, evidence, active.length, segments, { exclusions, stats });
}

function sharing(ctx: Ctx): Metric {
  const doc = ctx.data(orgDocumentationExtractor);
  const org = ctx.data(orgExtractor);
  const evidence = [ctx.ev(doc.sharing.source, { count: doc.sharing.status === "ok" ? doc.sharing.data.length : null }), ctx.ev(org.source, { count: org.status === "ok" ? org.data.length : null })];
  const exclusions = ["Sharing rules, which can't be counted through a read-only connection (QUESTIONS.md)"];
  let rows: Array<{ object: string; internal: string | null; external: string | null }>;
  if (doc.sharing.status === "ok") rows = doc.sharing.data;
  else if (org.status === "ok" && org.data[0]) {
    // The organisation's own defaults cover the core objects, internal users only.
    const o = org.data[0];
    rows = [
      { object: "Account", internal: o.DefaultAccountAccess, external: null },
      { object: "Contact", internal: o.DefaultContactAccess, external: null },
      { object: "Lead", internal: o.DefaultLeadAccess, external: null },
      { object: "Opportunity", internal: o.DefaultOpportunityAccess, external: null },
    ];
    exclusions.push(`Custom objects and external access, because the sharing model couldn't be read for each object: ${doc.sharing.reason}`);
  } else return notMeasurable(DEFINITIONS.sharing, evidence, `Sharing models couldn't be read: ${doc.sharing.reason}`);
  const order = new Map(doc.objects.map((o, i) => [o, i]));
  rows = rows.filter((r) => r.internal || r.external).sort((a, b) => (order.get(a.object) ?? 99) - (order.get(b.object) ?? 99) || a.object.localeCompare(b.object));
  const segments = rows.flatMap((r) => [...(r.internal ? [seg("internal", `${r.object}${SEP}${r.internal}`, 1)] : []), ...(r.external ? [seg("external", `${r.object}${SEP}${r.external}`, 1)] : [])]);
  return measured(DEFINITIONS.sharing, evidence, rows.length, segments, { exclusions, assumptions: ["Salesforce reports a private default as None, which we show as Private (QUESTIONS.md)"] });
}

export function documentedMetrics(ctx: Ctx): Metric[] {
  const gate = opportunitiesGate(ctx);
  if (gate) return Object.values(DEFINITIONS).map((d) => notMeasurable(d, [ctx.ev(ctx.data(recordCountsExtractor).source, { count: null })], gate));
  return [...dataModel(ctx), automation(ctx), integrations(ctx), access(ctx), sharing(ctx)];
}

// ─── Reading ─────────────────────────────────────────────────────────────────

export type DocumentedObject = { name: string; records: number | null; customFields: number | null; recordTypes: Array<{ name: string; active: boolean }>; mostFilled: Array<{ field: string; filledPct: number; total: number }> };
export type Relationship = { from: string; field: string; to: string; kind: "lookup" | "master-detail" };
export type AutomationItem = { name: string; active: boolean; described: boolean | null };
export type ObjectAutomation = { object: string; steps: Array<{ key: StepKey; label: string; step: number | null; active: number; all: number; items: AutomationItem[] }> };
export type OrgDocument = {
  model: { metric: Measured; objects: DocumentedObject[] } | null;
  relationships: { metric: Measured; links: Relationship[] } | null;
  automation: { metric: Measured; objects: ObjectAutomation[] } | null;
  integrations: { metric: Measured; connectedApps: Array<{ name: string; adminApprovedOnly: boolean }>; namedCredentials: string[]; remoteSites: Array<{ host: string; active: boolean }>; integrationUsers: Array<{ group: string; users: number }>; apiLogins: Array<{ application: string; logins: number; users: number }>; apiTotal: number | null } | null;
  access: { metric: Measured; profiles: Array<{ name: string; users: number }>; roles: Array<{ name: string; users: number }>; roleCount: number | null; roleDepth: number | null; permissionSets: Array<{ name: string; users: number }> } | null;
  sharing: { metric: Measured; objects: Array<{ object: string; internal: string | null; external: string | null }> } | null;
};

const asMeasured = (metrics: Metric[], id: string): Measured | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m : null;
};

/** The document the metrics describe. A part that couldn't be measured is null, and left out wherever it's shown. */
export function readDocument(metrics: Metric[]): OrgDocument {
  const model = asMeasured(metrics, DOCUMENTED.model);
  const rel = asMeasured(metrics, DOCUMENTED.relationships);
  const auto = asMeasured(metrics, DOCUMENTED.automation);
  const integ = asMeasured(metrics, DOCUMENTED.integrations);
  const acc = asMeasured(metrics, DOCUMENTED.access);
  const shr = asMeasured(metrics, DOCUMENTED.sharing);
  const of = (m: Measured, dimension: string) => m.segments.filter((s) => s.dimension === dimension);

  const objects = model
    ? of(model, "records").map((r): DocumentedObject => ({
        name: r.key,
        records: r.value,
        customFields: of(model, "custom fields").find((s) => s.key === r.key)?.value ?? null,
        recordTypes: of(model, `record type on ${r.key}`).map((s) => ({ name: s.key, active: s.value === 1 })),
        mostFilled: of(model, `most filled on ${r.key}`).map((s) => ({ field: s.key, filledPct: s.value ?? 0, total: s.n })),
      }))
    : [];

  const links = rel
    ? rel.segments.flatMap((s): Relationship[] => {
        const m = /^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+) to ([A-Za-z0-9_]+)$/.exec(s.key);
        return m ? [{ from: m[1]!, field: m[2]!, to: m[3]!, kind: s.dimension === "master-detail" ? "master-detail" : "lookup" }] : [];
      })
    : [];

  const automationObjects: ObjectAutomation[] = [];
  if (auto) {
    const counts = new Map(of(auto, COUNT_ON_SAVE).map((s) => [s.key, { active: s.value ?? 0, all: s.n }]));
    for (const s of auto.segments) {
      if (s.dimension === COUNT_ON_SAVE) continue;
      const [object, key, state] = s.dimension.split(SEP) as [string, StepKey, string];
      let entry = automationObjects.find((o) => o.object === object);
      if (!entry) automationObjects.push((entry = { object, steps: [] }));
      const def = AUTOMATION_STEPS.find((x) => x.key === key)!;
      let step = entry.steps.find((x) => x.key === key);
      if (!step) entry.steps.push((step = { key, label: def.label, step: def.step, ...(counts.get(`${object}${SEP}${key}`) ?? { active: 0, all: 0 }), items: [] }));
      step.items.push({ name: s.key, active: s.value === 1, described: state === "not read" ? null : state === "described" });
    }
  }

  return {
    model: model ? { metric: model, objects } : null,
    relationships: rel ? { metric: rel, links } : null,
    automation: auto ? { metric: auto, objects: automationObjects } : null,
    integrations: integ
      ? {
          metric: integ,
          connectedApps: of(integ, "connected app").map((s) => ({ name: s.key, adminApprovedOnly: s.value === 1 })),
          namedCredentials: of(integ, "named credential").map((s) => s.key),
          remoteSites: of(integ, "remote site").map((s) => ({ host: s.key, active: s.value === 1 })),
          integrationUsers: of(integ, "integration users").map((s) => ({ group: s.key, users: s.value ?? 0 })),
          apiLogins: of(integ, "API logins by application").map((s) => ({ application: s.key, logins: s.value ?? 0, users: s.n })),
          apiTotal: integ.stats.apiLogins ?? null,
        }
      : null,
    access: acc
      ? {
          metric: acc,
          profiles: of(acc, "profile").map((s) => ({ name: s.key, users: s.value ?? 0 })),
          roles: of(acc, "role").map((s) => ({ name: s.key, users: s.value ?? 0 })),
          roleCount: acc.stats.roles ?? null,
          roleDepth: acc.stats.roleDepth ?? null,
          permissionSets: of(acc, "permission set").map((s) => ({ name: s.key, users: s.value ?? 0 })),
        }
      : null,
    sharing: shr
      ? {
          metric: shr,
          objects: [...new Set(shr.segments.map((s) => s.key.split(SEP)[0]!))].map((object) => ({
            object,
            internal: of(shr, "internal").find((s) => s.key.startsWith(`${object}${SEP}`))?.key.split(SEP)[1] ?? null,
            external: of(shr, "external").find((s) => s.key.startsWith(`${object}${SEP}`))?.key.split(SEP)[1] ?? null,
          })),
        }
      : null,
  };
}

/** The objects to draw: the busiest by records, up to `limit`, with the relationships between them. */
export function diagramObjects(doc: OrgDocument, limit = 12): { objects: DocumentedObject[]; links: Relationship[] } {
  const objects = [...(doc.model?.objects ?? [])].sort((a, b) => (b.records ?? -1) - (a.records ?? -1) || a.name.localeCompare(b.name)).slice(0, limit);
  const shown = new Set(objects.map((o) => o.name));
  const links = (doc.relationships?.links ?? []).filter((l) => shown.has(l.from) && shown.has(l.to) && l.from !== l.to);
  return { objects, links };
}

export const isCoreObject = (name: string): boolean => (CORE_OBJECTS as readonly string[]).includes(name);
export const isOwnCustomObject = (name: string): boolean => OWN_CUSTOM_OBJECT.test(name) && !NAMESPACED.test(name);
