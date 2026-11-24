import { redactSecrets } from "../privacy/secrets.ts";
import { BudgetStopError } from "../salesforce/budget.ts";
import { isUnavailable, runStoppedWords, type OrgLimit } from "../salesforce/client.ts";
import { INVENTORY_VERSION, NAME_MAX, type ConnectedAppRow, type FillRateObject, type Inventory, type LimitRow, type PackageRow, type RiskyPermissionRow } from "../schemas/inventory.ts";
import type { RunScope } from "../schemas/index.ts";
import { CORE_OBJECTS, recordCountsExtractor } from "./configuration.ts";
import { orgDocumentationExtractor, type ConnectedAppRow as AppSetting } from "./documentation.ts";
import { accessExtractor, holdersOf, type AccessData, type AccessAssignment } from "./security.ts";
import { accessOf, isPartlyHidden, recordTypeUseExtractor } from "./record-types.ts";
import { licencesExtractor, type PackageLicenseRow } from "./users.ts";
import { readableReason, readName, readSoql, refusalReason, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

// The org's inventory (DECISIONS.md 603 to 605): the whole picture of fields,
// packages, connected apps, risky permissions and limits, kept as data for the
// console. No check reads it, so it moves no finding. Each step keeps names
// Salesforce gives set-up, and counts: never a field's value, and never who.
// A step reuses what an earlier step of the run already read, and reads it
// itself only when that step wasn't part of the run.

export const INVENTORY_FILE = "inventory.json";

/** An earlier step's output, or null when that step isn't part of this run. */
function earlier<T>(ctx: ExtractContext, extractor: Extractor<T>): T | null {
  try {
    return ctx.get(extractor);
  } catch {
    return null;
  }
}

const iso = (value: string | null | undefined): string | null => {
  if (!value) return null;
  const date = new Date(value.replace(/\+0000$/, "Z"));
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
};

/**
 * Names held to what the console keeps (DECISIONS.md 618): a longer one is cut
 * whole characters at a time and ends in "…", and the section's notes count
 * them, so a long name costs neither its row nor the section.
 */
function namer(): { fit: (value: string, max?: number) => string; notes: () => string[] } {
  let cut = 0;
  return {
    fit(value, max = NAME_MAX) {
      if (value.length <= max) return value;
      cut++;
      let out = "";
      for (const ch of value) {
        if (out.length + ch.length > max - 1) break;
        out += ch;
      }
      return `${out}…`;
    },
    notes: () => (cut === 0 ? [] : [`${cut === 1 ? "One name was" : `${cut} names were`} longer than we keep, so ${cut === 1 ? "it's" : "they're"} cut short and end in "…".`]),
  };
}

// ---------------------------------------------------------------------------
// Field fill rates (DECISIONS.md 604 and 609)

/** The most objects counted. A big org's smaller objects beyond it are named as a count, not read. */
export const FILL_RATE_MAX_OBJECTS = 300;
/** Fields counted in one query, as the field usage step counts them. */
export const FILL_RATE_FIELDS_PER_QUERY = 50;
/** Counts in one composite call: few, so one slow count can't time out many (DECISIONS.md 609). */
export const FILL_RATE_BATCH = 5;
/** An object with more records than this is counted in calls of its own. */
export const FILL_RATE_BIG_OBJECT = 1_000_000;
/** How far the record count resource and COUNT(Id) may differ before neither is trusted: a tenth, and more than this many records. */
const COUNTS_DIFFER_BY = 100;

export const NOT_SEEN_WHOLE = "The connected user can't see every record, so a count would show only part.";
export const COUNT_TOO_SLOW = "The count took too long.";
export const COUNTS_DISAGREE = "Salesforce's record count and the records the connected user can count differ by more than a tenth, so some records may be hidden from them.";

type DescribeField = { name: string; type: string; calculated: boolean; aggregatable: boolean };

/** Why a field isn't counted, or null when it is. */
export function skipReason(f: DescribeField): string | null {
  if (f.type === "id") return "An id always has a value.";
  if (f.type === "boolean") return "A checkbox always holds true or false, so it's never empty.";
  if (f.calculated) return "A formula field is worked out by Salesforce, not filled in.";
  if (!f.aggregatable) return `Salesforce can't count a field of this type (${f.type}) in an aggregate query.`;
  return null;
}

/** Counts only: the records, and how many hold a value in each field. Never a field's value. */
export const fillRateQuery = (object: string, fields: string[]): string =>
  fields.length ? `SELECT COUNT(Id) n, ${fields.map((f, i) => `COUNT(${f}) f${i}`).join(", ")} FROM ${object}` : `SELECT COUNT(Id) n FROM ${object}`;

/**
 * Why a count failed, in a sentence of ours: a budget stop isn't a failure and
 * goes up. Salesforce's message is never kept (DECISIONS.md 702): its answer
 * about the read is our refusal sentence, anything else our plain words.
 */
function failedCount(err: unknown, what: string): string {
  if (err instanceof BudgetStopError) throw err;
  if (err instanceof Error && (err.name === "TimeoutError" || err.name === "AbortError")) return COUNT_TOO_SLOW;
  if (isUnavailable(err)) return refusalReason(err, what);
  return runStoppedWords(err) ?? "The count couldn't be read.";
}

export const COUNT_NOT_READ = "Salesforce's count for this field didn't come back as a whole number, so its fill isn't measured.";
export const RECORDS_NOT_READ = "Salesforce's count of this object's records didn't come back as a whole number, so its fields' fill isn't measured.";

/**
 * A count as Salesforce gave it, or null when it isn't one: missing, null, a
 * fraction, below zero, too large, or anything Number() would make NaN. A
 * count written as digits in a string is read. Never a guess (DECISIONS.md 668).
 */
export function countOf(value: unknown): number | null {
  const n = typeof value === "number" ? value : typeof value === "string" && /^\s*\d+\s*$/.test(value) ? Number(value) : NaN;
  return Number.isSafeInteger(n) && n >= 0 ? n : null;
}

export type FillRateData = { objects: FillRateObject[]; leftOut: number; scopeNote: string | null };

type Scope = { objects: string[]; hidden: FillRateObject[]; leftOut: number; scopeNote: string | null; counts: Map<string, number> };

/**
 * The objects counted: the core four, then every object with records, most
 * first, up to the cap, of those the connected user sees whole. The others
 * are listed unavailable, at no cost (DECISIONS.md 609).
 */
async function fillRateScope(ctx: ExtractContext): Promise<Scope> {
  let counts = earlier(ctx, recordCountsExtractor);
  if (!counts) {
    const source: SourceInfo = { kind: "rest", path: "limits/recordCount" };
    try {
      counts = { status: "ok", data: await ctx.client.recordCounts(), source, truncated: false };
    } catch (err) {
      if (!isUnavailable(err)) throw err;
      counts = { status: "unavailable", reason: refusalReason(err, readName(source)), source };
    }
  }
  const byObject = new Map(counts.status === "ok" ? counts.data.map((c) => [c.name, c.count]) : []);
  const scopeNote = counts.status === "ok" ? null : `Only ${CORE_OBJECTS.join(", ")} were counted: the record count of each object couldn't be read (${counts.reason}).`;
  const withRecords = counts.status === "ok" ? counts.data.filter((c) => c.count > 0).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)).map((c) => c.name) : [];
  const all = [...new Set([...CORE_OBJECTS, ...withRecords])];

  // Record-type use's reading of the user's access when that step ran, so the two agree.
  const access = earlier(ctx, recordTypeUseExtractor)?.access ?? (await accessOf(ctx));
  if (access.status !== "ok") {
    const reason = `We couldn't tell whether the connected user sees every record: ${access.reason}`;
    const shown = all.slice(0, FILL_RATE_MAX_OBJECTS);
    return { objects: [], hidden: shown.map((object) => ({ object, unavailable: reason })), leftOut: all.length - shown.length, scopeNote, counts: byObject };
  }
  const whole = (object: string): boolean => !isPartlyHidden(object) && (access.viewAllData || access.viewAllRecords.includes(object));
  const seen = all.filter(whole);
  const hidden = all.filter((o) => !whole(o)).map((object): FillRateObject => ({ object, unavailable: NOT_SEEN_WHOLE }));
  return { objects: seen.slice(0, FILL_RATE_MAX_OBJECTS), hidden, leftOut: Math.max(0, seen.length - FILL_RATE_MAX_OBJECTS), scopeNote, counts: byObject };
}

type Planned = { object: string; fields: string[]; skipped: Array<{ field: string; reason: string }>; queries: string[] };

export const fillRatesExtractor: Extractor<Sourced<FillRateData>> = {
  id: "inventory-fill-rates",
  // 3: a field whose count isn't a whole number is listed as not measured, never read as 0 (DECISIONS.md 668).
  version: 3,
  // Counts of records: a set-up only scan never runs it (DECISIONS.md 604).
  records: true,
  partialKeys: ["inventory-fill-rates"],
  async run(ctx) {
    const source: SourceInfo = { kind: "soql", api: "rest", query: fillRateQuery("<object>", ["<field>"]) };
    const { objects, hidden, leftOut, scopeNote, counts } = await fillRateScope(ctx);
    // Each object is kept as it's finished, failed counts included, so a resumed run moves past it.
    const done = new Map(ctx.partial.read<FillRateObject>("inventory-fill-rates").map((o) => [o.object, o]));
    const keep = (rows: FillRateObject[]): void => {
      ctx.partial.append("inventory-fill-rates", rows);
      for (const o of rows) done.set(o.object, o);
    };
    const pending: Planned[] = [];

    const flush = async (): Promise<void> => {
      const queries = pending.flatMap((p) => p.queries);
      const results: Array<{ ok: true; row: Record<string, number> } | { ok: false; reason: string }> = [];
      for (let i = 0; i < queries.length; i += FILL_RATE_BATCH) {
        const chunk = queries.slice(i, i + FILL_RATE_BATCH);
        // Each count is an API call in the budget, not each composite call (DECISIONS.md 609).
        ctx.client.budget.assertCanSpend(chunk.length);
        try {
          const answers = await ctx.client.queryBatch<Record<string, number>>(chunk);
          results.push(...answers.map((a, j) => (a.ok ? { ok: true as const, row: a.result.records[0] ?? {} } : { ok: false as const, reason: refusalReason(a.error, readName({ kind: "soql", api: "rest", query: chunk[j]! })) })));
        } catch (err) {
          // A budget stop sent nothing, and goes up from here.
          const reason = failedCount(err, "This information");
          results.push(...chunk.map(() => ({ ok: false as const, reason })));
        }
        // The call went out, and the client recorded it once; the rest of its counts are recorded here.
        if (chunk.length > 1) ctx.client.budget.recordCall(chunk.length - 1);
      }
      let at = 0;
      const finished: FillRateObject[] = [];
      for (const plan of pending) {
        const own = results.slice(at, at + plan.queries.length);
        at += plan.queries.length;
        const failed = own.find((r) => !r.ok);
        if (failed && !failed.ok) {
          finished.push({ object: plan.object, unavailable: failed.reason });
          continue;
        }
        const rows = own.map((r) => (r.ok ? r.row : {}));
        // A count Salesforce didn't give as a whole number is never read as one: no record count, no object.
        const records = countOf(rows[0]?.n);
        if (records === null) {
          finished.push({ object: plan.object, unavailable: RECORDS_NOT_READ });
          continue;
        }
        const listed = counts.get(plan.object);
        if (listed !== undefined && Math.abs(listed - records) > Math.max(COUNTS_DIFFER_BY, Math.max(listed, records) / 10)) {
          finished.push({ object: plan.object, unavailable: COUNTS_DISAGREE });
          continue;
        }
        const fields: Array<{ field: string; filled: number; share: number | null }> = [];
        const notMeasured: Array<{ field: string; reason: string }> = [];
        plan.fields.forEach((field, i) => {
          // Each field against its own query's records: a record added between two queries moves only that one's.
          const row = rows[Math.floor(i / FILL_RATE_FIELDS_PER_QUERY)]!;
          const n = countOf(row.n);
          const count = countOf(row[`f${i % FILL_RATE_FIELDS_PER_QUERY}`]);
          // Not measured, said so, and never 0: the field's answer wasn't a count (DECISIONS.md 668).
          if (n === null || count === null) {
            notMeasured.push({ field, reason: COUNT_NOT_READ });
            return;
          }
          const filled = Math.min(count, n);
          fields.push({ field, filled, share: n > 0 ? Math.round((filled / n) * 10_000) / 10_000 : null });
        });
        finished.push({ object: plan.object, records, fields, skipped: [...plan.skipped, ...notMeasured] });
      }
      keep(finished);
      pending.length = 0;
    };

    for (const object of objects) {
      if (done.has(object)) continue;
      let described: { fields: DescribeField[] };
      try {
        described = await ctx.client.describe<{ fields: DescribeField[] }>(object);
      } catch (err) {
        keep([{ object, unavailable: failedCount(err, object) }]);
        continue;
      }
      const counted = described.fields.filter((f) => skipReason(f) === null).map((f) => f.name);
      const skipped = described.fields.flatMap((f) => {
        const reason = skipReason(f);
        return reason ? [{ field: f.name, reason }] : [];
      });
      const queries: string[] = [];
      for (let i = 0; i < counted.length; i += FILL_RATE_FIELDS_PER_QUERY) queries.push(fillRateQuery(object, counted.slice(i, i + FILL_RATE_FIELDS_PER_QUERY)));
      // An object with no field to count still has its records counted.
      if (queries.length === 0) queries.push(fillRateQuery(object, []));
      const big = (counts.get(object) ?? 0) > FILL_RATE_BIG_OBJECT;
      // A big object goes alone, and nothing joins it.
      if (pending.length && (big || pending.reduce((sum, p) => sum + p.queries.length, 0) + queries.length > FILL_RATE_BATCH)) await flush();
      pending.push({ object, fields: counted, skipped, queries });
      if (big) await flush();
    }
    await flush();
    const counted = objects.map((o) => done.get(o)!);
    return { status: "ok", source, truncated: leftOut > 0, data: { objects: [...counted, ...hidden], leftOut, scopeNote } };
  },
};

// ---------------------------------------------------------------------------
// Installed packages

type InstalledRow = {
  SubscriberPackageId?: string | null;
  SubscriberPackage: { Name: string | null; NamespacePrefix: string | null } | null;
  SubscriberPackageVersion?: { Name?: string | null; MajorVersion?: number | null; MinorVersion?: number | null; PatchVersion?: number | null; BuildNumber?: number | null; IsDeprecated?: boolean | null; IsManaged?: boolean | null } | null;
};
export type PackagesData = { installed: Sourced<InstalledRow[]>; licences: Sourced<PackageLicenseRow[]> };

const VERSION_FIELDS = "SubscriberPackageVersion.Name, SubscriberPackageVersion.MajorVersion, SubscriberPackageVersion.MinorVersion, SubscriberPackageVersion.PatchVersion, SubscriberPackageVersion.BuildNumber";
export const PACKAGES_QUERY = `SELECT SubscriberPackageId, SubscriberPackage.Name, SubscriberPackage.NamespacePrefix, ${VERSION_FIELDS}, SubscriberPackageVersion.IsDeprecated, SubscriberPackageVersion.IsManaged FROM InstalledSubscriberPackage`;
/** Without the two flags, which we haven't confirmed Tooling gives through the relationship (QUESTIONS.md). */
export const PACKAGES_QUERY_PLAIN = `SELECT SubscriberPackageId, SubscriberPackage.Name, SubscriberPackage.NamespacePrefix, ${VERSION_FIELDS} FROM InstalledSubscriberPackage`;
const LICENCES_QUERY = "SELECT NamespacePrefix, AllowedLicenses, UsedLicenses, Status, ExpirationDate FROM PackageLicense";

export const packagesExtractor: Extractor<PackagesData> = {
  id: "inventory-packages",
  version: 2,
  async run(ctx) {
    let installed = await readSoql<InstalledRow>(ctx, PACKAGES_QUERY, { api: "tooling" });
    if (installed.status !== "ok") {
      const plain = await readSoql<InstalledRow>(ctx, PACKAGES_QUERY_PLAIN, { api: "tooling" });
      if (plain.status === "ok") installed = { ...plain, missingFields: ["SubscriberPackageVersion.IsDeprecated", "SubscriberPackageVersion.IsManaged"] };
    }
    const licences = earlier(ctx, licencesExtractor)?.packageLicenses ?? (await readSoql<PackageLicenseRow>(ctx, LICENCES_QUERY));
    return { installed, licences };
  },
};

function packageRows(data: PackagesData): Inventory["packages"] {
  const { installed, licences } = data;
  if (installed.status !== "ok" && licences.status !== "ok") return { unavailable: `Neither the installed packages nor their licences could be read: ${installed.reason}` };
  const notes: string[] = [];
  if (installed.status !== "ok") notes.push(`The installed packages couldn't be read, so only packages with a licence are listed: ${installed.reason}`);
  if (licences.status !== "ok") notes.push(`The packages' licences couldn't be read: ${licences.reason}`);
  if (installed.status === "ok" && installed.missingFields?.length) notes.push("Salesforce didn't say whether each version is deprecated or managed.");
  // Each installed package by its id, so packages without a namespace are each listed (DECISIONS.md 609).
  const byPackage = new Map<string, PackageRow>();
  const ns = (value: string | null | undefined): string | null => (value ? value.toLowerCase() : null);
  const number = (v: number | null | undefined): string => (typeof v === "number" ? String(v) : "?");
  const names = namer();
  const fit = (value: string | null | undefined, max?: number): string | null => (value ? names.fit(value, max) : null);
  let longIds = 0;
  // Read without the flags, nothing that came back is taken for them.
  const flagsRead = installed.status === "ok" && !installed.missingFields?.length;
  for (const p of installed.status === "ok" ? installed.data : []) {
    const v = p.SubscriberPackageVersion ?? null;
    const flag = (value: boolean | null | undefined): boolean | null => (flagsRead && typeof value === "boolean" ? value : null);
    // An id is never cut: one longer than Salesforce's 18 characters isn't an id we know, so it isn't kept.
    const id = p.SubscriberPackageId && p.SubscriberPackageId.length > 18 ? (longIds++, null) : (p.SubscriberPackageId ?? null);
    byPackage.set(p.SubscriberPackageId ?? `row:${byPackage.size}`, {
      packageId: id,
      namespace: fit(p.SubscriberPackage?.NamespacePrefix),
      name: fit(p.SubscriberPackage?.Name),
      version: v && typeof v.MajorVersion === "number" ? fit([v.MajorVersion, v.MinorVersion, v.PatchVersion, v.BuildNumber].map(number).join("."), 100) : null,
      versionName: fit(v?.Name),
      installed: true,
      deprecated: flag(v?.IsDeprecated),
      managed: flag(v?.IsManaged),
      licence: null,
    });
  }
  // A seat count is kept only as Salesforce gave it: no count is null, never 0 (rule 5, DECISIONS.md 618).
  const seats = (n: number | null | undefined): number | null => (typeof n === "number" && Number.isInteger(n) && n >= 0 ? n : null);
  let noUsed = 0;
  let noAllowed = 0;
  for (const l of licences.status === "ok" ? licences.data : []) {
    // Salesforce gives a site licence as -1 seats allowed; anything else that isn't a count is unknown, not site-wide.
    const siteWide = l.AllowedLicenses === -1;
    const allowed = siteWide ? null : seats(l.AllowedLicenses);
    const used = seats(l.UsedLicenses);
    if (!siteWide && allowed === null) noAllowed++;
    if (used === null) noUsed++;
    const licence = {
      status: fit(l.Status, 100),
      allowed,
      siteWide,
      used,
      expiresOn: l.ExpirationDate ? l.ExpirationDate.slice(0, 10) : null,
    };
    // Licences only name a namespace, so they match by it; one without a namespace stands alone.
    const row = ns(l.NamespacePrefix) ? [...byPackage.values()].find((p) => ns(p.namespace) === ns(l.NamespacePrefix)) : undefined;
    if (row) row.licence = licence;
    else byPackage.set(`licence:${byPackage.size}`, { packageId: null, namespace: fit(l.NamespacePrefix), name: null, version: null, versionName: null, installed: false, deprecated: null, managed: null, licence });
  }
  const rows = [...byPackage.values()].sort((a, b) => (a.namespace ?? "").localeCompare(b.namespace ?? "") || (a.name ?? "").localeCompare(b.name ?? "") || (a.packageId ?? "").localeCompare(b.packageId ?? ""));
  if (noUsed) notes.push(`${noUsed === 1 ? "One licence gave" : `${noUsed} licences gave`} no count of seats used, so ${noUsed === 1 ? "its use isn't" : "their use isn't"} shown.`);
  if (noAllowed) notes.push(`${noAllowed === 1 ? "One licence gave" : `${noAllowed} licences gave`} no number of seats allowed, so ${noAllowed === 1 ? "its allowance isn't" : "their allowance isn't"} shown.`);
  if (longIds) notes.push(`${longIds === 1 ? "One package's id was" : `${longIds} packages' ids were`} longer than a Salesforce id, so ${longIds === 1 ? "it isn't" : "they aren't"} shown.`);
  return { rows, notes: [...notes, ...names.notes()] };
}

// ---------------------------------------------------------------------------
// Connected apps

/** The most tokens read. Past it, every count is a floor, and a note says so. */
export const OAUTH_TOKEN_MAX_ROWS = 50_000;
export const ONLY_OWN_TOKENS = "The connected user can only see its own authorisations.";
export const NOT_TOLD_WHO = "The run wasn't told which user connected, so we can't tell whether it sees everyone's authorisations.";
export const OAUTH_TOKEN_QUERY = "SELECT AppName, UserId, CreatedDate, LastUsedDate, UseCount FROM OauthToken";
const APPS_QUERY = "SELECT Name, OptionsAllowAdminApprovedUsersOnly FROM ConnectedApplication";

/** Each app's use, worked out while the tokens are held: who held them goes no further than this step. */
export type AppUse = { name: string; users: number; firstAuthorised: string | null; lastUsed: string | null; uses: number | null };
export type ConnectedAppsData = { apps: Sourced<AppSetting[]>; use: Sourced<AppUse[]> };

export const connectedAppsExtractor: Extractor<ConnectedAppsData> = {
  id: "inventory-connected-apps",
  version: 2,
  async run(ctx) {
    const apps = earlier(ctx, orgDocumentationExtractor)?.connectedApps ?? (await readSoql<AppSetting>(ctx, APPS_QUERY));
    const tokens = await readSoql<{ AppName: string | null; UserId: string | null; CreatedDate: string | null; LastUsedDate: string | null; UseCount: number | null }>(ctx, OAUTH_TOKEN_QUERY, { maxRecords: OAUTH_TOKEN_MAX_ROWS });
    if (tokens.status !== "ok") return { apps, use: tokens };
    // A user without Manage Users sees only their own tokens, which would show
    // every app as theirs alone (DECISIONS.md 609). Without their id we can't tell.
    const own = ctx.connectedUser?.id?.slice(0, 15);
    if (!own) return { apps, use: { status: "unavailable", reason: NOT_TOLD_WHO, source: tokens.source } };
    if (tokens.data.every((t) => t.UserId?.slice(0, 15) === own)) return { apps, use: { status: "unavailable", reason: ONLY_OWN_TOKENS, source: tokens.source } };
    const byApp = new Map<string, { users: Set<string>; first: string | null; last: string | null; uses: number | null }>();
    for (const t of tokens.data) {
      const name = t.AppName?.trim();
      if (!name) continue;
      const app = byApp.get(name) ?? byApp.set(name, { users: new Set(), first: null, last: null, uses: null }).get(name)!;
      if (t.UserId) app.users.add(t.UserId.slice(0, 15));
      const created = iso(t.CreatedDate);
      const used = iso(t.LastUsedDate);
      if (created && (!app.first || created < app.first)) app.first = created;
      if (used && (!app.last || used > app.last)) app.last = used;
      if (typeof t.UseCount === "number") app.uses = (app.uses ?? 0) + t.UseCount;
    }
    const use = [...byApp].map(([name, a]): AppUse => ({ name, users: a.users.size, firstAuthorised: a.first, lastUsed: a.last, uses: a.uses }));
    return { apps, use: { status: "ok", source: tokens.source, truncated: tokens.truncated, data: use } };
  },
};

function connectedAppRows(data: ConnectedAppsData): Inventory["connectedApps"] {
  const { apps, use } = data;
  if (apps.status !== "ok" && use.status !== "ok") return { unavailable: `Neither the connected apps nor their OAuth tokens could be read: ${apps.reason}` };
  const notes: string[] = [];
  if (apps.status !== "ok") notes.push(`The org's connected apps couldn't be read, so we can't say which are installed: ${apps.reason}`);
  if (use.status !== "ok") notes.push(use.reason === ONLY_OWN_TOKENS || use.reason === NOT_TOLD_WHO ? `No app's use is shown: ${use.reason}` : `The OAuth tokens couldn't be read, so no app's use is shown: ${use.reason}`);
  if (use.status === "ok" && use.truncated) notes.push(`More than ${OAUTH_TOKEN_MAX_ROWS.toLocaleString("en-GB")} OAuth tokens: we read that many, so each app's users and uses are at least what's shown.`);
  const used = new Map(use.status === "ok" ? use.data.map((u) => [u.name, u]) : []);
  const rows = new Map<string, ConnectedAppRow>();
  const names = namer();
  const blank = (name: string, installed: boolean | null, adminApprovedOnly: boolean | null): ConnectedAppRow => {
    const u = used.get(name);
    return { name: names.fit(name), installed, adminApprovedOnly, firstAuthorised: u?.firstAuthorised ?? null, lastUsed: u?.lastUsed ?? null, users: use.status === "ok" ? (u?.users ?? 0) : null, uses: u?.uses ?? (use.status === "ok" ? 0 : null) };
  };
  // An app Salesforce gave without a name can't be listed by one: it's counted in the notes instead.
  const unnamed = apps.status === "ok" ? apps.data.filter((a) => !a.Name?.trim()).length : 0;
  for (const a of apps.status === "ok" ? apps.data.filter((a) => a.Name?.trim()) : []) rows.set(a.Name, blank(a.Name, true, typeof a.OptionsAllowAdminApprovedUsersOnly === "boolean" ? a.OptionsAllowAdminApprovedUsersOnly : null));
  // An app only users authorised has tokens and no entry of its own.
  for (const name of used.keys()) if (!rows.has(name)) rows.set(name, blank(name, apps.status === "ok" ? false : null, null));
  if (unnamed) notes.push(`${unnamed === 1 ? "One connected app has" : `${unnamed} connected apps have`} no name in Salesforce, so ${unnamed === 1 ? "it isn't" : "they aren't"} listed.`);
  return { rows: [...rows.values()].sort((a, b) => a.name.localeCompare(b.name)), notes: [...notes, ...names.notes()] };
}

// ---------------------------------------------------------------------------
// Risky permissions

/**
 * The permissions checked, by PermissionSet field and Salesforce's label. A
 * field the org's describe doesn't have, by its name or its label, isn't
 * checked, and the inventory says so (DECISIONS.md 603).
 */
export const RISKY_PERMISSIONS: ReadonlyArray<{ field: string; label: string }> = [
  { field: "PermissionsModifyAllData", label: "Modify All Data" },
  { field: "PermissionsViewAllData", label: "View All Data" },
  { field: "PermissionsAuthorApex", label: "Author Apex" },
  { field: "PermissionsCustomizeApplication", label: "Customize Application" },
  { field: "PermissionsManageUsers", label: "Manage Users" },
  { field: "PermissionsManageProfilesPermissionsets", label: "Manage Profiles and Permission Sets" },
  { field: "PermissionsManageSharing", label: "Manage Sharing" },
  { field: "PermissionsManagePasswordPolicies", label: "Manage Password Policies" },
  { field: "PermissionsManageLoginAccessPolicies", label: "Manage Login Access Policies" },
  { field: "PermissionsEditReadonlyFields", label: "Edit Read Only Fields" },
  { field: "PermissionsExportReport", label: "Export Reports" },
  { field: "PermissionsDataExport", label: "Weekly Data Export" },
  { field: "PermissionsApiEnabled", label: "API Enabled" },
  // Not confirmed against an org's describe (QUESTIONS.md): its label finds it if the name is wrong.
  { field: "PermissionsApproveUninstalledApps", label: "Approve Uninstalled Connected Apps" },
  { field: "PermissionsModifyMetadata", label: "Modify Metadata" },
];

export const riskyPermissionsQuery = (fields: string[]): string =>
  `SELECT Id, Name, Label, IsOwnedByProfile, Profile.Name, NamespacePrefix, ${fields.join(", ")} FROM PermissionSet WHERE ${fields.map((f) => `${f} = true`).join(" OR ")}`;
const ACCESS_QUERIES = {
  assignments: "SELECT AssigneeId, PermissionSetId, PermissionSetGroupId FROM PermissionSetAssignment WHERE Assignee.IsActive = true",
  groupComponents: "SELECT PermissionSetId, PermissionSetGroupId FROM PermissionSetGroupComponent",
};

type GrantRow = { Id: string; Name: string; Label: string; IsOwnedByProfile: boolean; Profile: { Name: string } | null; NamespacePrefix: string | null } & Record<string, unknown>;
/** Counts by set, worked out while the assignments are held, so no person's id is kept. */
export type RiskyPermissionsData = {
  checked: Array<{ field: string; label: string }>;
  notInOrg: string[];
  sets: Sourced<Array<{ kind: RiskyPermissionRow["kind"]; name: string; apiName: string; namespace: string | null; grants: string[]; activeUsers: number | null }>>;
  holdersGap: string | null;
};

export const riskyPermissionsExtractor: Extractor<RiskyPermissionsData> = {
  id: "inventory-risky-permissions",
  version: 1,
  async run(ctx) {
    const source: SourceInfo = { kind: "rest", path: "sobjects/PermissionSet/describe" };
    let fields: Array<{ name: string; label?: string }>;
    try {
      fields = (await ctx.client.describe<{ fields: Array<{ name: string; label?: string }> }>("PermissionSet")).fields;
    } catch (err) {
      if (!isUnavailable(err)) throw err;
      return { checked: [], notInOrg: [], sets: { status: "unavailable", reason: refusalReason(err, "PermissionSet"), source }, holdersGap: null };
    }
    const byName = new Map(fields.map((f) => [f.name, f]));
    const byLabel = new Map(fields.filter((f) => f.name.startsWith("Permissions") && f.label).map((f) => [f.label!.toLowerCase(), f]));
    const checked: Array<{ field: string; label: string }> = [];
    const notInOrg: string[] = [];
    for (const p of RISKY_PERMISSIONS) {
      const found = byName.get(p.field) ?? byLabel.get(p.label.toLowerCase());
      if (found) checked.push({ field: found.name, label: p.label });
      else notInOrg.push(p.label);
    }
    if (checked.length === 0) return { checked, notInOrg, sets: { status: "unavailable", reason: "The org's PermissionSet has none of the permissions we check.", source }, holdersGap: null };
    const read = await readSoql<GrantRow>(ctx, riskyPermissionsQuery(checked.map((c) => c.field)));
    if (read.status !== "ok") return { checked, notInOrg, sets: read, holdersGap: null };

    let access: Pick<AccessData, "assignments" | "groupComponents"> | null = earlier(ctx, accessExtractor);
    if (!access) {
      access = {
        assignments: await readSoql<AccessAssignment>(ctx, ACCESS_QUERIES.assignments),
        groupComponents: await readSoql<{ PermissionSetId: string; PermissionSetGroupId: string }>(ctx, ACCESS_QUERIES.groupComponents),
      };
    }
    const holders = holdersOf(access as AccessData);
    const gap = access.assignments.status !== "ok" ? access.assignments.reason : access.groupComponents.status !== "ok" ? access.groupComponents.reason : null;
    const rows = read.data.map((s) => ({
      kind: s.IsOwnedByProfile ? ("profile" as const) : ("permission set" as const),
      name: (s.IsOwnedByProfile ? s.Profile?.Name : s.Label) || s.Label || s.Name,
      apiName: s.Name,
      namespace: s.NamespacePrefix ?? null,
      grants: checked.filter((c) => s[c.field] === true).map((c) => c.label),
      activeUsers: holders ? (holders.get(s.Id)?.size ?? 0) : null,
    }));
    return { checked, notInOrg, sets: { ...read, data: rows.filter((r) => r.grants.length > 0) }, holdersGap: gap };
  },
};

function riskyRows(data: RiskyPermissionsData): Inventory["riskyPermissions"] {
  if (data.sets.status !== "ok") return { unavailable: data.sets.reason };
  const notes: string[] = [];
  if (data.notInOrg.length) notes.push(`Not in this org's permissions, so not checked: ${data.notInOrg.join(", ")}.`);
  if (data.holdersGap) notes.push(`Who holds each profile and permission set couldn't be read: ${data.holdersGap}`);
  const names = namer();
  const fit = (value: string | null | undefined): string | null => (value ? names.fit(value) : null);
  // A set with no API name can't be told apart from another: counted in the notes instead.
  const unnamed = data.sets.data.filter((r) => !r.apiName).length;
  const rows = data.sets.data
    .filter((r) => r.apiName)
    .map((r) => ({ ...r, name: names.fit(r.name || r.apiName), apiName: names.fit(r.apiName), namespace: fit(r.namespace) }))
    .sort((a, b) => a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name) || a.apiName.localeCompare(b.apiName));
  if (unnamed) notes.push(`${unnamed === 1 ? "One profile or permission set has" : `${unnamed} profiles and permission sets have`} no API name in Salesforce, so ${unnamed === 1 ? "it isn't" : "they aren't"} listed.`);
  notes.push(...names.notes());
  return { rows, notes, checked: data.checked.map((c) => c.label), notInOrg: data.notInOrg };
}

// ---------------------------------------------------------------------------
// Org limits

/** The client keeps only a limit Salesforce gives a maximum and a remaining figure, so one without either isn't listed. */
export const LIMITS_LISTED = "Only limits Salesforce gives both a maximum and a remaining figure are listed.";

/** Every limit /limits gave as the connection first read it, which costs no call. */
export const limitsExtractor: Extractor<Sourced<Record<string, OrgLimit>>> = {
  id: "inventory-limits",
  version: 1,
  run(ctx) {
    const source = { kind: "rest", path: `/services/data/v${ctx.client.apiVersion}/limits` } as const;
    const limits = ctx.client.allLimitsAtConnect;
    // Refused at connect, it says why in our words (DECISIONS.md 702).
    const unread = ctx.client.limitsUnread;
    if (unread) return Promise.resolve({ status: "unavailable", reason: unread, source });
    if (Object.keys(limits).length === 0) return Promise.resolve({ status: "unavailable", reason: "The org reported no limits.", source });
    return Promise.resolve({ status: "ok", data: limits, source, truncated: false });
  },
};

function limitRows(data: Sourced<Record<string, OrgLimit>>): Inventory["limits"] {
  if (data.status !== "ok") return { unavailable: data.reason };
  const names = namer();
  // As Salesforce reports them (DECISIONS.md 618): more remaining than the maximum is shown below zero and named, never clamped.
  const rows: LimitRow[] = Object.entries(data.data)
    .filter(([name, l]) => name && Number.isFinite(l.Max) && Number.isFinite(l.Remaining))
    .map(([name, l]) => ({ name: names.fit(name), max: l.Max, used: l.Max - l.Remaining }))
    .sort((a, b) => a.name.localeCompare(b.name));
  const over = rows.filter((r) => r.used < 0).map((r) => r.name);
  const notes = [LIMITS_LISTED];
  if (over.length) notes.push(`Salesforce reported more remaining than the maximum for ${over.join(", ")}, so ${over.length === 1 ? "its" : "their"} use is shown below zero, as reported.`);
  return { rows, notes: [...notes, ...names.notes()] };
}

// ---------------------------------------------------------------------------
// The file

export const INVENTORY_EXTRACTORS: Extractor[] = [fillRatesExtractor, packagesExtractor, connectedAppsExtractor, riskyPermissionsExtractor, limitsExtractor];

/** The inventory steps a run of this scope takes: a set-up only scan leaves out any that counts records, as modulesFor does. */
export function inventoryExtractorsFor(scope: RunScope): Extractor[] {
  return INVENTORY_EXTRACTORS.filter((e) => scope === "complete" || !e.records);
}

export const SETUP_ONLY_FILL_RATES = "A set-up only scan doesn't count records, so fill rates weren't read.";

/** An error's class name, for a log line: never its message. */
export const errorClass = (err: unknown): string => (err !== null && typeof err === "object" ? (err.constructor?.name ?? "Object") : typeof err);

/** The fields listed as not measured because their count wasn't a whole number, and the objects they're in. */
function notMeasuredIn(objects: FillRateObject[]): { fields: number; objects: number } {
  let fields = 0;
  let within = 0;
  for (const o of objects) {
    const n = "skipped" in o ? o.skipped.filter((s) => s.reason === COUNT_NOT_READ).length : 0;
    fields += n;
    if (n) within++;
  }
  return { fields, objects: within };
}

export const NOT_PUT_TOGETHER = "This part of the inventory couldn't be put together from what was read, so it isn't shown.";

/**
 * The inventory from the run's extracts. A step that didn't run makes its
 * section unavailable, with why. Each section is put together on its own, so
 * a fault in one costs only that one (DECISIONS.md 618), and is logged by the
 * section's key and the error's class name: never its message, which can
 * quote what the org gave.
 */
export function buildInventory(extracts: Map<string, unknown>, scope: RunScope, madeAt: Date, failed: Map<string, string> = new Map(), log: (line: string) => void = () => {}): Inventory {
  const got = <T>(e: Extractor<T>): T | undefined => extracts.get(e.id) as T | undefined;
  // A step that failed says why, with any secret in Salesforce's or the network's message taken out.
  const notRunOf = (e: Extractor): { unavailable: string } => {
    const reason = failed.get(e.id);
    return { unavailable: reason ? `This part of the inventory couldn't be read: ${redactSecrets(reason)}` : "This step didn't run." };
  };
  const apart = <S>(key: string, make: () => S): S | { unavailable: string } => {
    try {
      return make();
    } catch (err) {
      log(`  The inventory's ${key} couldn't be put together (${errorClass(err)}), so it's saved as unavailable.`);
      return { unavailable: NOT_PUT_TOGETHER };
    }
  };
  const fill = got(fillRatesExtractor);
  const fillRates = apart("fillRates", (): Inventory["fillRates"] => {
    if (!fill) return scope === "complete" ? notRunOf(fillRatesExtractor) : { unavailable: SETUP_ONLY_FILL_RATES };
    if (fill.status !== "ok") return { unavailable: fill.reason };
    const { fields, objects } = notMeasuredIn(fill.data.objects);
    // Counts only: how many fields and objects, never which (DECISIONS.md 668).
    if (fields) log(`  The inventory's fill rates: ${fields} ${fields === 1 ? "field" : "fields"} in ${objects} ${objects === 1 ? "object" : "objects"} came back without a whole-number count, so ${fields === 1 ? "it's" : "they're"} listed as not measured.`);
    return {
      rows: fill.data.objects,
      notes: [
        ...(fill.data.scopeNote ? [fill.data.scopeNote] : []),
        ...(fill.data.leftOut ? [`${fill.data.leftOut} more objects with records weren't counted: we count the ${FILL_RATE_MAX_OBJECTS} with the most records.`] : []),
        ...(fields ? [`${fields === 1 ? "One field's count" : `${fields} fields' counts`} didn't come back from Salesforce as a whole number, so ${fields === 1 ? "it's" : "they're"} listed among the fields not counted, not shown as empty.`] : []),
      ],
    };
  });
  const packages = got(packagesExtractor);
  const apps = got(connectedAppsExtractor);
  const risky = got(riskyPermissionsExtractor);
  const limits = got(limitsExtractor);
  return {
    version: INVENTORY_VERSION,
    madeAt: madeAt.toISOString(),
    scope,
    fillRates,
    packages: apart("packages", () => (packages ? packageRows(packages) : notRunOf(packagesExtractor))),
    connectedApps: apart("connectedApps", () => (apps ? connectedAppRows(apps) : notRunOf(connectedAppsExtractor))),
    riskyPermissions: apart("riskyPermissions", () => (risky ? riskyRows(risky) : notRunOf(riskyPermissionsExtractor))),
    limits: apart("limits", () => (limits ? limitRows(limits) : notRunOf(limitsExtractor))),
  };
}
