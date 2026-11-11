import type { ApexClassRow, ApexData, ApexTriggerRow, LastChange, LastChanged, TriggerUsage } from "../extract/automation.ts";
import type { ComponentChanges, ComponentChangesData } from "../extract/component-changes.ts";
import { SETUP_AUDIT_DAYS, type CustomObjectRow, type SetupAuditSummary } from "../extract/configuration.ts";
import { unreadReason, type MetadataBundle } from "../extract/metadata-parse.ts";
import type { Sourced } from "../extract/types.ts";
import { objectResolver } from "../domains/object-names.ts";
import { capped as cutText } from "./detail.ts";
import { CHANGES_NOT_KEPT, Handbook, HANDBOOK_VERSION, HandbookRecentChanges, MAX_APEX_CLASSES, MAX_APEX_TRIGGERS, MAX_CHANGES, TRIGGER_EVENTS, type HandbookApexClass, type HandbookApexTrigger, type HandbookFlow, type HandbookReadBy, type HandbookLastChange, type LastChanges } from "./schema.ts";

/**
 * In the run folder, beside the findings: kept by `hc purge`, deleted with the
 * folder, and read by nothing else in the engine or the worker (DECISIONS.md 452).
 */
export const HANDBOOK_FILE = "handbook.json";

const unavailable = (reason: string) => ({ status: "unavailable" as const, reason });

function allUnavailable(reason: string): Handbook {
  const part = unavailable(reason);
  // No part holds a reading, so none is said.
  return { version: HANDBOOK_VERSION, readBy: { metadata: null, apex: null }, flows: part, validationRules: part, apexClasses: part, apexTriggers: part, recentChanges: part };
}

/**
 * Which reading made the file (decision 484), as each step stamped its own
 * output when it read, so a checkpoint reused from before the stamp was kept
 * says null whatever code builds the file now. Null too where the step didn't
 * run, or its metadata wasn't read; never this code's own number.
 */
function readByOf(metadata: Sourced<MetadataBundle> | undefined, apex: ApexData | undefined): HandbookReadBy {
  return { metadata: (metadata?.status === "ok" ? metadata.data.handbook?.handbookReading : undefined) ?? null, apex: apex?.handbookReading ?? null };
}

/** A rule's place in the list of changes: its object and name, which a dot can't confuse. */
const ruleKey = (object: string, name: string): string => JSON.stringify([object, name]);

/**
 * Each flow and validation rule with its last change (decision 482): a flow's
 * by its developer name, which is the flow's name on either route, and a
 * rule's by its object and name, so a rule of the same name on another object
 * is never taken for it. One with no line in Salesforce's list has none here.
 */
function metadataParts(metadata: Sourced<MetadataBundle> | undefined, components: ComponentChangesData | undefined, modifiers: Modifiers): Pick<Handbook, "flows" | "validationRules"> {
  const both = (reason: string) => ({ flows: unavailable(reason), validationRules: unavailable(reason) });
  if (!metadata) return both("This run didn't read the org's metadata.");
  if (metadata.status !== "ok") return both(`Metadata couldn't be retrieved: ${metadata.reason}`);
  const detail = metadata.data.handbook;
  if (!detail) return both("This run's metadata was read before the handbook's detail was kept.");
  const unread = unreadReason(metadata.data, ["validationRules"]);
  // A checkpoint from version 7 of the step kept no flow's paths, and a flow
  // listed without its later paths would print them as ordinary steps (decision
  // 446). Its validation rules are whole, so they stay.
  const pathless = detail.flows.some((f) => !Array.isArray((f.start as Partial<HandbookFlow["start"]> | undefined)?.paths));
  const { flows, validationRules } = components ?? {};
  const flowLines = new Map(flows?.status === "read" ? flows.items.map((c) => [c.name, c]) : []);
  const ruleLines = new Map(validationRules?.status === "read" ? validationRules.items.map((c) => [ruleKey(c.object, c.name), c]) : []);
  return {
    flows: pathless
      ? unavailable("This run's metadata was read before the handbook kept every way a flow starts.")
      : { status: "ok", items: detail.flows.map((f) => ({ ...f, lastChanged: handbookChange(flowLines.get(f.name), modifiers) })), lastChanges: lastChangesOf(flows) },
    validationRules: unread
      ? unavailable(unread)
      : { status: "ok", covers: detail.validationRulesCover, items: detail.validationRules.map((r) => ({ ...r, lastChanged: handbookChange(ruleLines.get(ruleKey(r.object, r.name)), modifiers) })), lastChanges: lastChangesOf(validationRules) },
  };
}

// The Usage fields in TRIGGER_EVENTS' order.
const USAGE: Array<keyof TriggerUsage> = ["UsageBeforeInsert", "UsageBeforeUpdate", "UsageBeforeDelete", "UsageAfterInsert", "UsageAfterUpdate", "UsageAfterDelete", "UsageAfterUndelete"];

/** Null when the row says no event, as one from before they were read does: a trigger runs on at least one. */
function events(row: TriggerUsage): HandbookApexTrigger["events"] {
  const on = TRIGGER_EVENTS.filter((_, i) => row[USAGE[i]!] === true);
  return on.length > 0 ? on : null;
}

/** Who last changed a class or trigger, by its Id: the step's own record and the tokens its users were given. */
type LastChangeOf = (row: { Id: string }) => HandbookLastChange | null;

function apexClass(row: ApexClassRow | ApexTriggerRow, lastChange: LastChangeOf): HandbookApexClass {
  // The class itself annotated, read from its code without comments: the row's
  // isTest finds @isTest anywhere, a comment's included. No scan means the body
  // couldn't be read, so whether it's a test can't be told.
  return { name: row.Name, apiVersion: row.ApiVersion, status: row.Status, isTest: row.scan ? row.scan.testClass : null, touches: row.touches ?? null, lastChanged: lastChange(row) };
}

// A trigger on a custom object can name it by its 01I id, which no admin can
// read. The custom objects list names it, as the automation rules do; an id it
// doesn't name stays as Salesforce gave it rather than a guess.
const apexTrigger =
  (objectOf: (value: string) => string | null, lastChange: LastChangeOf) =>
  (row: ApexTriggerRow): HandbookApexTrigger => ({ ...apexClass(row, lastChange), object: objectOf(row.TableEnumOrId) ?? row.TableEnumOrId, events: events(row) });

const byName = (a: { Name: string }, b: { Name: string }): number => (a.Name < b.Name ? -1 : a.Name > b.Name ? 1 : 0);

/** Each read on its own, so one can be there while the other says why it isn't (decision 456). */
function capped<R extends { Name: string }, I>(rows: Sourced<R[]>, max: number, item: (row: R) => I, lastChanges: LastChanges) {
  if (rows.status !== "ok") return unavailable(rows.reason);
  return { status: "ok" as const, items: [...rows.data].sort(byName).slice(0, max).map(item), total: rows.data.length, cut: rows.data.length > max, lastChanges };
}

/** A user's token, from the run's pseudonymiser: the name is the run's user list's, or else the one Salesforce gave beside the id. */
export type LabelUser = (id: string, trailName: string | null) => string;

/** The tokens for who last changed each class, trigger, flow and validation rule, by user id (decisions 473 and 482). */
export type Modifiers = ReadonlyMap<string, string>;

const byId = ([a]: [string, unknown], [b]: [string, unknown]): number => (a < b ? -1 : a > b ? 1 : 0);

/** Everyone in these last changes, labelled once each in id order, named as Salesforce first named them. */
function labelInIdOrder(changes: Iterable<LastChange>, label: LabelUser): Modifiers {
  const named = new Map<string, string | null>();
  for (const c of changes) if (c.byId && !named.get(c.byId)) named.set(c.byId, c.byName);
  return new Map([...named].sort(byId).map(([id, name]) => [id, label(id, name)]));
}

/**
 * Labels everyone who last changed a class or trigger, in id order, as the
 * trail's users are, so a run analysed again gives each the same token. The
 * run labels its user list and the trail's users first, so theirs don't move:
 * this only adds those neither holds, named as Salesforce named them.
 */
export function labelApexModifiers(apex: ApexData | undefined, label: LabelUser): Modifiers {
  const parts = [apex?.lastChanged?.classes, apex?.lastChanged?.triggers];
  return labelInIdOrder(parts.flatMap((part) => (part?.status === "read" ? Object.values(part.byRow) : [])), label);
}

/**
 * The same for each flow and validation rule (decision 482). The run labels
 * them after Apex's, so no token Apex's labelling gave moves.
 */
export function labelComponentModifiers(components: ComponentChangesData | undefined, label: LabelUser): Modifiers {
  const parts: Array<ComponentChanges<LastChange> | undefined> = [components?.flows, components?.validationRules];
  return labelInIdOrder(parts.flatMap((part) => (part?.status === "read" ? part.items : [])), label);
}

/**
 * One last change as the file keeps it. Null when it wasn't read, or Salesforce
 * gave no date; and when its user has no token, which a run never leaves, so
 * nobody is ever shown as "no user" for want of one.
 */
function handbookChange(c: LastChange | undefined, modifiers: Modifiers): HandbookLastChange | null {
  if (!c?.at) return null;
  if (c.byId === null) return { at: c.at, by: null };
  const by = modifiers.get(c.byId);
  return by ? { at: c.at, by } : null;
}

/** A class's or trigger's last change, by its Id. */
function lastChangeOf(state: LastChanged | undefined, modifiers: Modifiers): LastChangeOf {
  return (row) => handbookChange(state?.status === "read" ? state.byRow[row.Id] : undefined, modifiers);
}

/** Undefined when the run had no such read: its step came before they were kept. */
const lastChangesOf = (state: { status: "read" | "refused" } | undefined): LastChanges => (state === undefined ? "not_kept" : state.status);

function apexParts(apex: ApexData | undefined, customObjects: Sourced<CustomObjectRow[]> | undefined, modifiers: Modifiers): Pick<Handbook, "apexClasses" | "apexTriggers"> {
  if (!apex) return { apexClasses: unavailable("This run didn't read the org's Apex."), apexTriggers: unavailable("This run didn't read the org's Apex.") };
  const objectOf = customObjects ? objectResolver(customObjects) : () => null;
  const { classes, triggers } = apex.lastChanged ?? {};
  return {
    apexClasses: capped(apex.classes, MAX_APEX_CLASSES, (row) => apexClass(row, lastChangeOf(classes, modifiers)), lastChangesOf(classes)),
    apexTriggers: capped(apex.triggers, MAX_APEX_TRIGGERS, apexTrigger(objectOf, lastChangeOf(triggers, modifiers)), lastChangesOf(triggers)),
  };
}

const TRAIL_NOT_READ = "This run didn't read the org's setup audit trail.";

const newestFirst = (a: { CreatedDate: string }, b: { CreatedDate: string }): number => (a.CreatedDate < b.CreatedDate ? 1 : a.CreatedDate > b.CreatedDate ? -1 : 0);

/**
 * The org's recent setup changes, newest first (decisions 461 and 462), from
 * the setup audit step's output, which keeps no user administration. Who made
 * each is a token, never a name: their users are labelled in id order, as the
 * run's own are, so a run analysed again gives each the same token.
 */
export function recentChanges(audit: Sourced<SetupAuditSummary> | undefined, label: LabelUser): HandbookRecentChanges {
  if (!audit) return unavailable(TRAIL_NOT_READ);
  if (audit.status !== "ok") return unavailable(audit.reason);
  const { changes, excludedUserAdmin } = audit.data;
  if (!Array.isArray(changes)) return unavailable(CHANGES_NOT_KEPT);
  const named = new Map<string, string | null>();
  for (const c of changes) if (c.CreatedById && !named.get(c.CreatedById)) named.set(c.CreatedById, c.CreatedByName);
  const tokens = new Map([...named].sort(byId).map(([id, name]) => [id, label(id, name)]));
  // Salesforce gives them newest first; sorted here too, so no other order can print as that.
  const items = [...changes]
    .sort(newestFirst)
    .slice(0, MAX_CHANGES)
    .map((c) => {
      const what = cutText(c.Display);
      return { at: c.CreatedDate, section: c.Section, action: c.Action, what: what.text, whatCut: what.cut, whatLength: what.length, by: c.CreatedById ? tokens.get(c.CreatedById)! : null, loggedInAs: c.DelegateUser };
    });
  const part = HandbookRecentChanges.safeParse({ status: "ok", items, total: changes.length, cut: audit.truncated || changes.length > MAX_CHANGES, leftOutUserAdmin: excludedUserAdmin, days: SETUP_AUDIT_DAYS });
  // One part that doesn't fit says so, and the other four are kept.
  return part.success ? part.data : unavailable("The recent changes didn't fit their format, so they weren't kept.");
}

/**
 * The handbook's data from the metadata, apex, custom objects and component
 * changes steps' output, any of which is undefined when the run didn't have
 * that step, the recent changes made from the setup audit step's, and the
 * tokens of who last changed each class, trigger, flow and validation rule. A
 * part that couldn't be read says why, so the documents can tell "no flows"
 * from "we couldn't read them" (decision 454). Checked against the schema here,
 * so a file that doesn't fit is never written: its parts say so instead, and
 * the run carries on.
 */
export function buildHandbook(
  metadata: Sourced<MetadataBundle> | undefined,
  apex?: ApexData,
  customObjects?: Sourced<CustomObjectRow[]>,
  changes: HandbookRecentChanges = unavailable(TRAIL_NOT_READ),
  modifiers: Modifiers = new Map(),
  components?: ComponentChangesData,
): Handbook {
  const handbook = Handbook.safeParse({ version: HANDBOOK_VERSION, readBy: readByOf(metadata, apex), ...metadataParts(metadata, components, modifiers), ...apexParts(apex, customObjects, modifiers), recentChanges: changes });
  if (handbook.success) return handbook.data;
  const where = handbook.error.issues[0]?.path.join(".") ?? "";
  return allUnavailable(`The handbook's data didn't fit its format${where ? ` at ${where}` : ""}, so it wasn't kept.`);
}
