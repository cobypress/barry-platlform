import type { ApexClassRow, ApexData, ApexTriggerRow, TriggerUsage } from "../extract/automation.ts";
import { unreadReason, type MetadataBundle } from "../extract/metadata-parse.ts";
import type { Sourced } from "../extract/types.ts";
import { Handbook, HANDBOOK_VERSION, MAX_APEX_CLASSES, MAX_APEX_TRIGGERS, TRIGGER_EVENTS, type HandbookApexClass, type HandbookApexTrigger } from "./schema.ts";

/**
 * In the run folder, beside the findings: kept by `hc purge`, deleted with the
 * folder, and read by nothing else in the engine or the worker (DECISIONS.md 452).
 */
export const HANDBOOK_FILE = "handbook.json";

const unavailable = (reason: string) => ({ status: "unavailable" as const, reason });

function allUnavailable(reason: string): Handbook {
  const part = unavailable(reason);
  return { version: HANDBOOK_VERSION, flows: part, validationRules: part, apexClasses: part, apexTriggers: part };
}

function metadataParts(metadata: Sourced<MetadataBundle> | undefined): Pick<Handbook, "flows" | "validationRules"> {
  const both = (reason: string) => ({ flows: unavailable(reason), validationRules: unavailable(reason) });
  if (!metadata) return both("This run didn't read the org's metadata.");
  if (metadata.status !== "ok") return both(`Metadata couldn't be retrieved: ${metadata.reason}`);
  const detail = metadata.data.handbook;
  if (!detail) return both("This run's metadata was read before the handbook's detail was kept.");
  const unread = unreadReason(metadata.data, ["validationRules"]);
  return {
    flows: { status: "ok", items: detail.flows },
    validationRules: unread ? unavailable(unread) : { status: "ok", covers: detail.validationRulesCover, items: detail.validationRules },
  };
}

// The Usage fields in TRIGGER_EVENTS' order.
const USAGE: Array<keyof TriggerUsage> = ["UsageBeforeInsert", "UsageBeforeUpdate", "UsageBeforeDelete", "UsageAfterInsert", "UsageAfterUpdate", "UsageAfterDelete", "UsageAfterUndelete"];

/** Null when the row says no event, as one from before they were read does: a trigger runs on at least one. */
function events(row: TriggerUsage): HandbookApexTrigger["events"] {
  const on = TRIGGER_EVENTS.filter((_, i) => row[USAGE[i]!] === true);
  return on.length > 0 ? on : null;
}

function apexClass(row: ApexClassRow | ApexTriggerRow): HandbookApexClass {
  // The class itself annotated, read from its code without comments: the row's
  // isTest finds @isTest anywhere, a comment's included. No scan means the body
  // couldn't be read, so whether it's a test can't be told.
  return { name: row.Name, apiVersion: row.ApiVersion, status: row.Status, isTest: row.scan ? row.scan.testClass : null, touches: row.touches ?? null };
}

const apexTrigger = (row: ApexTriggerRow): HandbookApexTrigger => ({ ...apexClass(row), object: row.TableEnumOrId, events: events(row) });

const byName = (a: { Name: string }, b: { Name: string }): number => (a.Name < b.Name ? -1 : a.Name > b.Name ? 1 : 0);

/** Each read on its own, so one can be there while the other says why it isn't (decision 456). */
function capped<R extends { Name: string }, I>(rows: Sourced<R[]>, max: number, item: (row: R) => I) {
  if (rows.status !== "ok") return unavailable(rows.reason);
  return { status: "ok" as const, items: [...rows.data].sort(byName).slice(0, max).map(item), total: rows.data.length, cut: rows.data.length > max };
}

function apexParts(apex: ApexData | undefined): Pick<Handbook, "apexClasses" | "apexTriggers"> {
  if (!apex) return { apexClasses: unavailable("This run didn't read the org's Apex."), apexTriggers: unavailable("This run didn't read the org's Apex.") };
  return { apexClasses: capped(apex.classes, MAX_APEX_CLASSES, apexClass), apexTriggers: capped(apex.triggers, MAX_APEX_TRIGGERS, apexTrigger) };
}

/**
 * The handbook's data from the metadata and apex steps' output, either of which
 * is undefined when the run didn't have that step. A part that couldn't be read
 * says why, so the documents can tell "no flows" from "we couldn't read them"
 * (decision 454). Checked against the schema here, so a file that doesn't fit is
 * never written: its parts say so instead, and the run carries on.
 */
export function buildHandbook(metadata: Sourced<MetadataBundle> | undefined, apex?: ApexData): Handbook {
  const handbook = Handbook.safeParse({ version: HANDBOOK_VERSION, ...metadataParts(metadata), ...apexParts(apex) });
  if (handbook.success) return handbook.data;
  const where = handbook.error.issues[0]?.path.join(".") ?? "";
  return allUnavailable(`The handbook's data didn't fit its format${where ? ` at ${where}` : ""}, so it wasn't kept.`);
}
