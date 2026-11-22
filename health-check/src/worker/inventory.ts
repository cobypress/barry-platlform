import { existsSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import type { z } from "zod";
import { INVENTORY_FILE } from "../extract/inventory.ts";
import { Timestamp } from "../schemas/common.ts";
import { FillRateField, FillRateObject, Inventory, INVENTORY_VERSION, NAME_MAX } from "../schemas/inventory.ts";
import { RunScope } from "../schemas/run.ts";

// The org's inventory as the worker hands it to the console (DECISIONS.md 605
// and 618). It is never dropped whole and never silently: each of the five
// sections is checked on its own, and within fill rates each object and each
// field (DECISIONS.md 668), so only that part is marked, with why, and the rest
// is saved as it was. A run with no file at all still gets a row that says so. The log
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

/** How fill rates are put right object by object; a test may give its own to prove the save's guard catches what it gets wrong. */
export type FillRatesRepair = (value: unknown, say: (line: string) => void) => unknown;

/**
 * Each section checked on its own (DECISIONS.md 618): one that doesn't fit, or
 * holds a character Postgres's jsonb refuses (a NUL, or half of a UTF-16
 * pair, which jsonb refuses with a class 22 error that would roll back the
 * whole save on every retry), is `{ unavailable }`; within fill rates, the
 * field is, or at worst the object (DECISIONS.md 668). Then the whole is held
 * under the cap by keeping fill rates for the objects with the most records,
 * and checked once more before it's saved.
 */
export function settleInventory(raw: unknown, say: (line: string) => void, made: Omit<InventoryFor, "runId">, repair: FillRatesRepair = fillRatesByObject): Inventory {
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
    // Fill rates alone are put right part by part. A list's row that doesn't fit
    // costs its whole section: the org page counts a list's rows without its
    // notes, so a list with rows left out would read as complete (DECISIONS.md 668).
    if (key === "fillRates") {
      const byObject = repair(value, say);
      if (byObject) {
        sections[key] = byObject;
        continue;
      }
    }
    say(`${key} ${parsed.success ? `held ${bad!.what} at ${bad!.at}` : `didn't fit its shape (${issuesOf(parsed.error)})`}, so it's saved as unavailable.`);
    sections[key] = { unavailable: SECTION_MISFIT };
  }
  const inventory = { version: INVENTORY_VERSION, madeAt, scope, ...sections } as Inventory;
  return guarded(withinCap(inventory, say), say);
}

/**
 * The last check before the save (DECISIONS.md 668): every fill-rate object,
 * then every section, against the schema and for what jsonb refuses. A repair
 * that went wrong costs its object, or its section, never the save.
 */
function guarded(inventory: Inventory, say: (line: string) => void): Inventory {
  const out = { ...inventory };
  const fill = out.fillRates as unknown;
  if (fill && typeof fill === "object" && Array.isArray((fill as { rows?: unknown }).rows)) {
    const rows = (fill as { rows: unknown[] }).rows;
    let caught = 0;
    const kept = rows.flatMap((row): unknown[] => {
      if (check(FillRateObject, row).ok) return [row];
      caught++;
      const name = (row as { object?: unknown } | null)?.object;
      return nameable(name) ? [{ object: cut(name), unavailable: OBJECT_MISFIT }] : [];
    });
    if (caught) {
      say(`the last check before saving caught ${caught} fill-rate ${caught === 1 ? "object" : "objects"} that still didn't fit, so ${caught === 1 ? "it's" : "they're"} saved as unavailable.`);
      out.fillRates = { ...(fill as object), rows: kept } as Inventory["fillRates"];
    }
  }
  for (const key of SECTIONS) {
    const got = check(SECTION_SCHEMA[key], out[key]);
    if (got.ok) continue;
    say(`at the last check before saving, ${key} still ${got.why}, so it's saved as unavailable.`);
    out[key] = { unavailable: SECTION_MISFIT } as never;
  }
  return out;
}

/** Why a part failed, by the schema's paths and Zod's codes: never a value. */
const whyOf = (parsed: z.ZodSafeParseResult<unknown>, bad: { at: string; what: string } | null): string =>
  parsed.success ? `held ${bad!.what} at ${bad!.at}` : `didn't fit its shape (${issuesOf(parsed.error)})`;

/** A value checked against a schema and for what jsonb refuses. */
function check<T>(schema: z.ZodType<T>, value: unknown): { ok: true; data: T } | { ok: false; why: string; countOnly: boolean } {
  const parsed = schema.safeParse(value);
  const bad = parsed.success ? unstorableAt(parsed.data) : null;
  if (parsed.success && bad === null) return { ok: true, data: parsed.data };
  // Whether every issue is in a field's count or share, so the reason can say its count didn't fit.
  const countOnly = !parsed.success && parsed.error.issues.every((i) => i.path.length === 1 && (i.path[0] === "filled" || i.path[0] === "share"));
  return { ok: false, why: whyOf(parsed, bad), countOnly };
}

const nameable = (name: unknown): name is string => typeof name === "string" && name.trim().length > 0 && storable(name);

/** A field whose count or share didn't fit: listed among the fields not counted, with this reason. */
export const FIELD_MISFIT = "This field's count didn't fit the shape we store, so its fill isn't measured.";
/** A field whose entry didn't fit for another reason (its name, a key we don't store, a reason not given). */
export const FIELD_ENTRY_MISFIT = "This field's entry didn't fit the shape we store, so its fill isn't shown.";

type Repaired = { object: FillRateObject; notMeasured: number; unnamed: number; repeated: number };

/**
 * A counted object whose fields don't all fit, kept with those fields moved
 * to its fields not counted (DECISIONS.md 668): one field never costs the
 * object's other counts. A field that can't be named is left out, and a field
 * named again is kept once: its first count, else its first reason from the
 * file, else the first the worker gave it; both are counted. Null when the
 * object's own frame (its name, records, keys) doesn't fit.
 */
function repaired(row: unknown): Repaired | null {
  if (row === null || typeof row !== "object" || Array.isArray(row)) return null;
  const { fields, skipped } = row as { fields?: unknown; skipped?: unknown };
  if (!Array.isArray(fields) || !Array.isArray(skipped)) return null;
  if (!check(FillRateObject, { ...row, fields: [], skipped: [] }).ok) return null;
  const kept: z.infer<typeof FillRateField>[] = [];
  const notCounted: Array<{ field: string; reason: string }> = [];
  const misfit: Array<{ field: string; reason: string }> = [];
  let notMeasured = 0;
  let unnamed = 0;
  const instead = (entry: unknown, reason: string): void => {
    const name = (entry as { field?: unknown } | null)?.field;
    if (nameable(name)) {
      misfit.push({ field: cut(name), reason });
      notMeasured++;
    } else unnamed++;
  };
  for (const f of fields) {
    const got = check(FillRateField, f);
    if (got.ok) kept.push(got.data);
    else instead(f, got.countOnly ? FIELD_MISFIT : FIELD_ENTRY_MISFIT);
  }
  for (const s of skipped) {
    // A field not counted is checked as an object holding it alone.
    const got = check(FillRateObject, { object: "x", records: 0, fields: [], skipped: [s] });
    if (got.ok && "skipped" in got.data) notCounted.push(got.data.skipped[0]!);
    else instead(s, FIELD_ENTRY_MISFIT);
  }
  // Each field once: the first count, else the first reason.
  const seen = new Set<string>();
  const once = <T extends { field: string }>(list: T[]): T[] => list.filter((f) => !seen.has(f.field) && Boolean(seen.add(f.field)));
  const counted = once(kept);
  const reasons = once([...notCounted, ...misfit]);
  const repeated = kept.length + notCounted.length + misfit.length - counted.length - reasons.length;
  // A misfit dropped as a repeat is counted as a repeat, not as not measured.
  const movedKept = reasons.filter((r) => misfit.includes(r)).length;
  const whole = check(FillRateObject, { ...row, fields: counted, skipped: reasons });
  return whole.ok ? { object: whole.data, notMeasured: movedKept, unnamed, repeated } : null;
}

const plural = (n: number, one: string, many: string): string => (n === 1 ? one : `${n} ${many}`);

/** Objects logged one line each before the rest are summed in one line. */
export const OBJECT_LINES_MAX = 5;

/**
 * Fill rates object by object, and within an object field by field: a field
 * that doesn't fit is listed as not measured and the object's other counts
 * kept; an object whose own frame doesn't fit is that object's unavailable.
 * The section's notes count each. Null when the section's own frame (its rows
 * and notes) doesn't fit, which costs the section.
 */
function fillRatesByObject(value: unknown, say: (line: string) => void): Inventory["fillRates"] | null {
  if (value === null || typeof value !== "object" || Array.isArray(value) || !Array.isArray((value as { rows?: unknown }).rows)) return null;
  const { rows, notes } = value as { rows: unknown[]; notes?: unknown };
  if (!Array.isArray(notes) || !notes.every((n) => typeof n === "string" && n.length > 0 && storable(n))) return null;
  if (Object.keys(value).some((k) => k !== "rows" && k !== "notes")) return null;
  const kept: FillRateObject[] = [];
  let misfits = 0;
  let unnamed = 0;
  let fieldMisfits = 0;
  let fieldsUnnamed = 0;
  let repeats = 0;
  let lines = 0;
  let mended = 0;
  const line = (text: string): void => {
    if (lines++ < OBJECT_LINES_MAX) say(text);
  };
  rows.forEach((row, i) => {
    const got = check(FillRateObject, row);
    if (got.ok) {
      kept.push(got.data);
      return;
    }
    const fixed = repaired(row);
    if (fixed) {
      kept.push(fixed.object);
      mended++;
      fieldMisfits += fixed.notMeasured;
      fieldsUnnamed += fixed.unnamed;
      repeats += fixed.repeated;
      const parts = [
        ...(fixed.notMeasured ? [`${plural(fixed.notMeasured, "one field is", "fields are")} listed as not measured`] : []),
        ...(fixed.unnamed ? [`${plural(fixed.unnamed, "one field", "fields")} that couldn't be named ${fixed.unnamed === 1 ? "is" : "are"} left out`] : []),
        ...(fixed.repeated ? [`${plural(fixed.repeated, "one repeated field is", "repeated fields are")} kept once`] : []),
      ];
      line(`fillRates object ${i} ${got.why}, so ${parts.join(" and ") || "it's put right"}, and the object's other counts are kept.`);
      return;
    }
    line(`fillRates object ${i} ${got.why}, so it's saved as unavailable.`);
    misfits++;
    const name = (row as { object?: unknown } | null)?.object;
    if (nameable(name)) kept.push({ object: cut(name), unavailable: OBJECT_MISFIT });
    else unnamed++;
  });
  // Past the first few, one line with counts only.
  if (lines > OBJECT_LINES_MAX) say(`fillRates: ${lines - OBJECT_LINES_MAX} more objects didn't fit their shape; in all, ${mended} kept with ${fieldMisfits} fields not measured, ${fieldsUnnamed} unnamed fields left out and ${repeats} repeats kept once, and ${misfits} saved as unavailable.`);
  const added: string[] = [];
  const named = misfits - unnamed;
  if (named > 0) added.push(`${plural(named, "One object's counts", "objects' counts")} didn't fit the shape we store, so ${named === 1 ? "it's" : "they're"} marked unavailable.`);
  if (unnamed) added.push(`${plural(unnamed, "One object", "objects")} couldn't be named, so ${unnamed === 1 ? "it's" : "they're"} left out.`);
  if (fieldMisfits) added.push(`${plural(fieldMisfits, "One field", "fields")} didn't fit the shape we store, so ${fieldMisfits === 1 ? "it's" : "they're"} listed among the fields not counted, not shown as empty.`);
  if (fieldsUnnamed) added.push(`${plural(fieldsUnnamed, "One field", "fields")} couldn't be named, so ${fieldsUnnamed === 1 ? "it's" : "they're"} left out of ${fieldsUnnamed === 1 ? "its object" : "their objects"}.`);
  if (repeats) added.push(`${plural(repeats, "One field was", "fields were")} listed twice in ${repeats === 1 ? "its object" : "their objects"}, so only the first is kept.`);
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
