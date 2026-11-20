import { existsSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import type { z } from "zod";
import { INVENTORY_FILE } from "../extract/inventory.ts";
import { Timestamp } from "../schemas/common.ts";
import { FillRateObject, Inventory, INVENTORY_VERSION, NAME_MAX } from "../schemas/inventory.ts";
import { RunScope } from "../schemas/run.ts";

// The org's inventory as the worker hands it to the console (DECISIONS.md 605
// and 618). It is never dropped whole and never silently: each of the five
// sections is checked on its own, and within fill rates each object, so a part
// that doesn't fit is saved as unavailable with why and the rest is saved as
// it was. A run with no file at all still gets a row that says so. The log
// names the section and what failed, by the schema's paths, never a value.

/** The most of the inventory the worker sends, as the JSON the console stores. A big org's is a few megabytes. */
export const INVENTORY_MAX_BYTES = 8 * 1024 * 1024;
/** The most of inventory.json the worker reads at all. Fill rates stop at 300 objects, so a real file is well under it. */
export const INVENTORY_READ_MAX_BYTES = 64 * 1024 * 1024;

export const SECTIONS = ["fillRates", "packages", "connectedApps", "riskyPermissions", "limits"] as const;
type Section = (typeof SECTIONS)[number];

export const NO_FILE = "The inventory wasn't written for this run, so none of it can be shown.";
export const UNREADABLE = "The inventory couldn't be read back after the run, so none of it can be shown.";
export const TOO_BIG_TO_READ = "The inventory was far larger than we can store, so none of it can be shown.";
export const SECTION_MISFIT = "This part of the inventory didn't fit the shape we store, so it isn't shown.";
export const OBJECT_MISFIT = "This object's counts didn't fit the shape we store, so they aren't shown.";
export const FILL_RATES_TOO_BIG = "The fill rates were too large to store, so they aren't shown.";
export const SECTION_TOO_BIG = "This part of the inventory was too large to store, so it isn't shown.";

/** Who the inventory is for, and what to date it by when the file can't say: the run's own as-of time and scope. */
export type InventoryFor = { runId: string; madeAt: string; scope: RunScope };

/** Every section unavailable, for the same reason. */
export function inventoryUnavailable(made: Omit<InventoryFor, "runId">, reason: string): Inventory {
  const none = { unavailable: reason };
  return { version: INVENTORY_VERSION, madeAt: made.madeAt, scope: made.scope, fillRates: none, packages: none, connectedApps: none, riskyPermissions: none, limits: none };
}

/**
 * The inventory for the console, from the run folder's inventory.json. Always
 * a row: with no file, or one that can't be read, every section says why.
 */
export function inventoryOf(dir: string, log: (line: string) => void, made: InventoryFor): Inventory {
  const path = join(dir, INVENTORY_FILE);
  const say = (line: string): void => log(`Run ${made.runId}'s inventory: ${line}`);
  if (!existsSync(path)) {
    say("there's no inventory.json, so every section is saved as unavailable.");
    return inventoryUnavailable(made, NO_FILE);
  }
  let raw: unknown;
  try {
    if (statSync(path).size > INVENTORY_READ_MAX_BYTES) {
      say("inventory.json is larger than the worker reads, so every section is saved as unavailable.");
      return inventoryUnavailable(made, TOO_BIG_TO_READ);
    }
    raw = JSON.parse(readFileSync(path, "utf8"));
  } catch {
    say("inventory.json didn't parse, so every section is saved as unavailable.");
    return inventoryUnavailable(made, UNREADABLE);
  }
  return settleInventory(raw, say, made);
}

/** The schema of each section, as the inventory holds it. */
const SECTION_SCHEMA: Record<Section, z.ZodType> = {
  fillRates: Inventory.shape.fillRates,
  packages: Inventory.shape.packages,
  connectedApps: Inventory.shape.connectedApps,
  riskyPermissions: Inventory.shape.riskyPermissions,
  limits: Inventory.shape.limits,
};

/**
 * Each section checked on its own (DECISIONS.md 618): one that doesn't fit, or
 * holds a character Postgres's jsonb refuses (a NUL, or half of a UTF-16
 * pair, which jsonb refuses with a class 22 error that would roll back the
 * whole save on every retry), is `{ unavailable }`; within fill
 * rates, the object is. Then the whole is held under the cap by keeping fill
 * rates for the objects with the most records.
 */
export function settleInventory(raw: unknown, say: (line: string) => void, made: Omit<InventoryFor, "runId">): Inventory {
  const file = raw !== null && typeof raw === "object" && !Array.isArray(raw) ? (raw as Record<string, unknown>) : null;
  if (!file || file.version !== INVENTORY_VERSION) {
    say(`inventory.json ${file ? "is a version the worker doesn't store" : "isn't an object"}, so every section is saved as unavailable.`);
    return inventoryUnavailable(made, UNREADABLE);
  }
  const extra = Object.keys(file).filter((k) => !["version", "madeAt", "scope", ...SECTIONS].includes(k)).length;
  if (extra) say(`${extra} unknown top-level ${extra === 1 ? "key was" : "keys were"} left out.`);
  const madeAt = Timestamp.safeParse(file.madeAt).success ? (file.madeAt as string) : (say("madeAt didn't fit its shape, so the run's as-of time is used."), made.madeAt);
  const scope = RunScope.safeParse(file.scope).success ? (file.scope as RunScope) : (say("scope didn't fit its shape, so the run's scope is used."), made.scope);

  const sections = {} as Record<Section, unknown>;
  for (const key of SECTIONS) {
    const value = file[key];
    const parsed = SECTION_SCHEMA[key].safeParse(value);
    const bad = parsed.success ? unstorableAt(parsed.data) : null;
    if (parsed.success && bad === null) {
      sections[key] = parsed.data;
      continue;
    }
    if (key === "fillRates") {
      const byObject = fillRatesByObject(value, say);
      if (byObject) {
        sections[key] = byObject;
        continue;
      }
    }
    say(`${key} ${parsed.success ? `held ${bad!.what} at ${bad!.at}` : `didn't fit its shape (${issuesOf(parsed.error)})`}, so it's saved as unavailable.`);
    sections[key] = { unavailable: SECTION_MISFIT };
  }
  const inventory = { version: INVENTORY_VERSION, madeAt, scope, ...sections } as Inventory;
  return withinCap(inventory, say);
}

/**
 * Fill rates object by object: one that doesn't fit is that object's
 * unavailable, and the section's notes count them. Null when the section's own
 * frame (its rows and notes) doesn't fit, which costs the section.
 */
function fillRatesByObject(value: unknown, say: (line: string) => void): Inventory["fillRates"] | null {
  if (value === null || typeof value !== "object" || !Array.isArray((value as { rows?: unknown }).rows)) return null;
  const { rows, notes } = value as { rows: unknown[]; notes?: unknown };
  if (!Array.isArray(notes) || !notes.every((n) => typeof n === "string" && n.length > 0 && storable(n))) return null;
  if (Object.keys(value).some((k) => k !== "rows" && k !== "notes")) return null;
  const kept: FillRateObject[] = [];
  let misfits = 0;
  let unnamed = 0;
  rows.forEach((row, i) => {
    const parsed = FillRateObject.safeParse(row);
    const bad = parsed.success ? unstorableAt(parsed.data) : null;
    if (parsed.success && bad === null) {
      kept.push(parsed.data);
      return;
    }
    say(`fillRates object ${i} ${parsed.success ? `held ${bad!.what} at ${bad!.at}` : `didn't fit its shape (${issuesOf(parsed.error)})`}, so it's saved as unavailable.`);
    misfits++;
    const name = (row as { object?: unknown } | null)?.object;
    if (typeof name === "string" && name.trim() && storable(name)) kept.push({ object: cut(name), unavailable: OBJECT_MISFIT });
    else unnamed++;
  });
  const added: string[] = [];
  if (misfits - unnamed > 0) added.push(`${misfits - unnamed === 1 ? "One object's counts" : `${misfits - unnamed} objects' counts`} didn't fit the shape we store, so ${misfits - unnamed === 1 ? "it's" : "they're"} marked unavailable.`);
  if (unnamed) added.push(`${unnamed === 1 ? "One object" : `${unnamed} objects`} couldn't be named, so ${unnamed === 1 ? "it's" : "they're"} left out.`);
  return { rows: kept, notes: [...(notes as string[]), ...added] };
}

/**
 * The inventory under the cap. Fill rates are by far the largest section, so
 * they give way first: kept for the objects with the most records, the rest
 * left out and counted in the notes. Only if that isn't enough is a section
 * made unavailable, the largest first.
 */
function withinCap(inventory: Inventory, say: (line: string) => void): Inventory {
  const size = (v: unknown): number => Buffer.byteLength(JSON.stringify(v), "utf8");
  if (size(inventory) < INVENTORY_MAX_BYTES) return inventory;
  const total = size(inventory);
  const fill = inventory.fillRates;
  if ("rows" in fill) {
    // Most records first; an unavailable object, with no count, last. Room is kept for the note.
    const order = fill.rows.map((row, i) => ({ i, records: "records" in row ? row.records : -1, bytes: size(row) + 1 })).sort((a, b) => b.records - a.records || a.i - b.i);
    let room = INVENTORY_MAX_BYTES - (total - size(fill.rows)) - 1024;
    const keep = new Set<number>();
    for (const o of order) {
      if (o.bytes > room) break;
      room -= o.bytes;
      keep.add(o.i);
    }
    const left = fill.rows.length - keep.size;
    if (keep.size > 0) {
      say(`the inventory is larger than the worker sends, so fill rates keep the ${keep.size} objects with the most records and leave out ${left}.`);
      const rows = fill.rows.filter((_, i) => keep.has(i));
      const note = `The inventory was too large to store whole, so fill rates are kept for the ${keep.size} objects with the most records, and ${left === 1 ? "one object with fewer was" : `${left} objects with fewer were`} left out.`;
      inventory = { ...inventory, fillRates: { rows, notes: [...fill.notes, note] } };
      if (size(inventory) < INVENTORY_MAX_BYTES) return inventory;
    }
  }
  if ("rows" in inventory.fillRates) {
    say("the inventory is larger than the worker sends, so fillRates is saved as unavailable.");
    inventory = { ...inventory, fillRates: { unavailable: FILL_RATES_TOO_BIG } };
  }
  for (const key of [...SECTIONS].sort((a, b) => size(inventory[b]) - size(inventory[a]))) {
    if (size(inventory) < INVENTORY_MAX_BYTES) break;
    if (key === "fillRates") continue;
    say(`the inventory is still larger than the worker sends, so ${key} is saved as unavailable.`);
    inventory = { ...inventory, [key]: { unavailable: SECTION_TOO_BIG } };
  }
  return inventory;
}

const cut = (name: string): string => {
  if (name.length <= NAME_MAX) return name;
  let out = "";
  for (const ch of name) {
    if (out.length + ch.length > NAME_MAX - 1) break;
    out += ch;
  }
  return `${out}…`;
};

/** The schema's own keys: a path is logged with these and indices only, so a key read from the file is never printed. */
const KNOWN_KEYS = new Set([
  ...SECTIONS,
  "rows", "notes", "unavailable", "checked", "notInOrg",
  "object", "records", "fields", "skipped", "field", "filled", "share", "reason",
  "packageId", "namespace", "name", "version", "versionName", "installed", "deprecated", "managed", "licence", "status", "allowed", "siteWide", "used", "expiresOn",
  "adminApprovedOnly", "firstAuthorised", "lastUsed", "users", "uses",
  "kind", "apiName", "grants", "activeUsers", "max",
]);

const pathOf = (path: ReadonlyArray<PropertyKey>, keys: ReadonlySet<string> = KNOWN_KEYS): string => path.map((p) => (typeof p === "number" ? String(p) : keys.has(String(p)) ? String(p) : "?")).join(".") || "(top)";

type Issue = { code: string; path: ReadonlyArray<PropertyKey>; errors?: Issue[][] };

/** A union's issues as its nearest branch has them, so the log names the field that failed rather than the union. */
function flat(issue: Issue, prefix: ReadonlyArray<PropertyKey> = []): Issue[] {
  const path = [...prefix, ...issue.path];
  if (issue.code === "invalid_union" && issue.errors?.length) {
    const nearest = [...issue.errors].sort((a, b) => a.length - b.length)[0]!;
    if (nearest.length) return nearest.flatMap((i) => flat(i, path));
  }
  return [{ code: issue.code, path }];
}

/** Where and what failed, by path and Zod's issue code: never the message, which can quote a value. */
function issuesOf(error: z.ZodError): string {
  const all = (error.issues as unknown as Issue[]).flatMap((i) => flat(i));
  const issues = all.slice(0, 5).map((i) => `${pathOf(i.path)} ${i.code}`);
  return `${issues.join(", ")}${all.length > 5 ? ` and ${all.length - 5} more` : ""}`;
}

/** A string Postgres's jsonb takes: no NUL, and no half of a UTF-16 pair, which JSON.stringify writes as an escape jsonb refuses. */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;
// String.prototype.isWellFormed, which the project's TypeScript library doesn't yet name.
const storable = (s: string): boolean => !s.includes("\u0000") && !LONE_SURROGATE.test(s);
const what = (s: string): string => (s.includes("\u0000") ? "a NUL character" : "a broken character (half of a UTF-16 pair)");

/**
 * Where the first string Postgres's jsonb would refuse is, in a key or a value,
 * and what's wrong with it; or null. The path names `keys` and indices only.
 * The run digest uses it too, with its own keys (DECISIONS.md 632).
 */
export function unstorableAt(value: unknown, path: PropertyKey[] = [], keys: ReadonlySet<string> = KNOWN_KEYS): { at: string; what: string } | null {
  if (typeof value === "string") return storable(value) ? null : { at: pathOf(path, keys), what: what(value) };
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const found = unstorableAt(value[i], [...path, i], keys);
      if (found) return found;
    }
    return null;
  }
  if (value && typeof value === "object") {
    for (const [k, v] of Object.entries(value)) {
      if (!storable(k)) return { at: pathOf([...path, k], keys), what: what(k) };
      const found = unstorableAt(v, [...path, k], keys);
      if (found) return found;
    }
  }
  return null;
}
