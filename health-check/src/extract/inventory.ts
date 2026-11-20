import { isUnavailable, type OrgLimit } from "../salesforce/client.ts";
import { INVENTORY_VERSION, type ConnectedAppRow, type FillRateObject, type Inventory, type LimitRow, type PackageRow, type RiskyPermissionRow } from "../schemas/inventory.ts";
import type { RunScope } from "../schemas/index.ts";
import { CORE_OBJECTS, recordCountsExtractor } from "./configuration.ts";
import { orgDocumentationExtractor, type ConnectedAppRow as AppSetting } from "./documentation.ts";
import { accessExtractor, holdersOf, type AccessData, type AccessAssignment } from "./security.ts";
import { licencesExtractor, type PackageLicenseRow } from "./users.ts";
import { readableReason, readSoql, type ExtractContext, type Extractor, type Sourced, type SourceInfo } from "./types.ts";

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

// ---------------------------------------------------------------------------
// Field fill rates (DECISIONS.md 604)

/** The most objects counted. A big org's smaller objects beyond it are named as a count, not read. */
export const FILL_RATE_MAX_OBJECTS = 300;
/** Fields counted in one query, as the field usage step counts them. */
export const FILL_RATE_FIELDS_PER_QUERY = 50;
/** Subrequests in one composite call: Salesforce's most. */
const BATCH = 25;

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
export const fillRateQuery = (object: string, fields: string[]): string => `SELECT COUNT(Id) n, ${fields.map((f, i) => `COUNT(${f}) f${i}`).join(", ")} FROM ${object}`;

export type FillRateData = { objects: FillRateObject[]; leftOut: number; scopeNote: string | null };

/** The objects counted: the core four, then every object with records, most first, up to the cap. */
async function fillRateObjects(ctx: ExtractContext): Promise<{ objects: string[]; leftOut: number; scopeNote: string | null }> {
  let counts = earlier(ctx, recordCountsExtractor);
  if (!counts) {
    const source: SourceInfo = { kind: "rest", path: "limits/recordCount" };
    try {
      counts = { status: "ok", data: await ctx.client.recordCounts(), source, truncated: false };
    } catch (err) {
      if (!isUnavailable(err)) throw err;
      counts = { status: "unavailable", reason: readableReason(err.message), source };
    }
  }
  if (counts.status !== "ok") return { objects: [...CORE_OBJECTS], leftOut: 0, scopeNote: `Only ${CORE_OBJECTS.join(", ")} were counted: the record count of each object couldn't be read (${counts.reason}).` };
  const withRecords = counts.data.filter((c) => c.count > 0).sort((a, b) => b.count - a.count || a.name.localeCompare(b.name)).map((c) => c.name);
  const all = [...new Set([...CORE_OBJECTS, ...withRecords])];
  return { objects: all.slice(0, FILL_RATE_MAX_OBJECTS), leftOut: Math.max(0, all.length - FILL_RATE_MAX_OBJECTS), scopeNote: null };
}

type Planned = { object: string; records?: number; fields: string[]; skipped: Array<{ field: string; reason: string }>; queries: string[] };

export const fillRatesExtractor: Extractor<Sourced<FillRateData>> = {
  id: "inventory-fill-rates",
  version: 1,
  // Counts of records: a set-up only scan never runs it (DECISIONS.md 604).
  records: true,
  partialKeys: ["inventory-fill-rates"],
  async run(ctx) {
    const source: SourceInfo = { kind: "soql", api: "rest", query: fillRateQuery("<object>", ["<field>"]) };
    const { objects, leftOut, scopeNote } = await fillRateObjects(ctx);
    // Each object is kept as it's finished, so a run stopped at the budget picks up where it was.
    const done = new Map(ctx.partial.read<FillRateObject>("inventory-fill-rates").map((o) => [o.object, o]));
    const pending: Planned[] = [];

    const flush = async (): Promise<void> => {
      const queries = pending.flatMap((p) => p.queries);
      const results = queries.length ? await ctx.client.queryBatch<Record<string, number>>(queries) : [];
      let at = 0;
      const finished: FillRateObject[] = [];
      for (const plan of pending) {
        const own = results.slice(at, at + plan.queries.length);
        at += plan.queries.length;
        const failed = own.find((r) => !r.ok);
        if (failed && !failed.ok) {
          finished.push({ object: plan.object, unavailable: readableReason(failed.error.message) });
          continue;
        }
        const rows = own.map((r) => (r.ok ? r.result.records[0]! : {}));
        const records = rows.length ? Number(rows[0]!.n ?? 0) : (plan.records ?? 0);
        const fields = plan.fields.map((field, i) => {
          const filled = Number(rows[Math.floor(i / FILL_RATE_FIELDS_PER_QUERY)]![`f${i % FILL_RATE_FIELDS_PER_QUERY}`] ?? 0);
          return { field, filled, share: records > 0 ? Math.round((filled / records) * 10_000) / 10_000 : null };
        });
        finished.push({ object: plan.object, records, fields, skipped: plan.skipped });
      }
      ctx.partial.append("inventory-fill-rates", finished);
      for (const o of finished) done.set(o.object, o);
      pending.length = 0;
    };

    for (const object of objects) {
      if (done.has(object)) continue;
      let described: { fields: DescribeField[] };
      try {
        described = await ctx.client.describe<{ fields: DescribeField[] }>(object);
      } catch (err) {
        if (!isUnavailable(err)) throw err;
        const row: FillRateObject = { object, unavailable: readableReason(err.message) };
        ctx.partial.append("inventory-fill-rates", [row]);
        done.set(object, row);
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
      if (queries.length === 0) queries.push(`SELECT COUNT(Id) n FROM ${object}`);
      if (pending.reduce((sum, p) => sum + p.queries.length, 0) + queries.length > BATCH) await flush();
      pending.push({ object, fields: counted, skipped, queries });
    }
    await flush();
    return { status: "ok", source, truncated: leftOut > 0, data: { objects: objects.map((o) => done.get(o)!), leftOut, scopeNote } };
  },
};

// ---------------------------------------------------------------------------
// Installed packages

type InstalledRow = {
  SubscriberPackage: { Name: string | null; NamespacePrefix: string | null } | null;
  SubscriberPackageVersion?: { Name?: string | null; MajorVersion?: number | null; MinorVersion?: number | null; PatchVersion?: number | null; BuildNumber?: number | null; IsDeprecated?: boolean | null; IsManaged?: boolean | null } | null;
};
export type PackagesData = { installed: Sourced<InstalledRow[]>; licences: Sourced<PackageLicenseRow[]> };

const VERSION_FIELDS = "SubscriberPackageVersion.Name, SubscriberPackageVersion.MajorVersion, SubscriberPackageVersion.MinorVersion, SubscriberPackageVersion.PatchVersion, SubscriberPackageVersion.BuildNumber";
export const PACKAGES_QUERY = `SELECT SubscriberPackage.Name, SubscriberPackage.NamespacePrefix, ${VERSION_FIELDS}, SubscriberPackageVersion.IsDeprecated, SubscriberPackageVersion.IsManaged FROM InstalledSubscriberPackage`;
/** Without the two flags, which we haven't confirmed Tooling gives through the relationship (QUESTIONS.md). */
export const PACKAGES_QUERY_PLAIN = `SELECT SubscriberPackage.Name, SubscriberPackage.NamespacePrefix, ${VERSION_FIELDS} FROM InstalledSubscriberPackage`;
const LICENCES_QUERY = "SELECT NamespacePrefix, AllowedLicenses, UsedLicenses, Status, ExpirationDate FROM PackageLicense";

export const packagesExtractor: Extractor<PackagesData> = {
  id: "inventory-packages",
  version: 1,
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
  const byNamespace = new Map<string, PackageRow>();
  const key = (ns: string | null | undefined): string => (ns ?? "").toLowerCase();
  const number = (v: number | null | undefined): string => (typeof v === "number" ? String(v) : "?");
  // Read without the flags, nothing that came back is taken for them.
  const flagsRead = installed.status === "ok" && !installed.missingFields?.length;
  for (const p of installed.status === "ok" ? installed.data : []) {
    const v = p.SubscriberPackageVersion ?? null;
    const flag = (value: boolean | null | undefined): boolean | null => (flagsRead && typeof value === "boolean" ? value : null);
    byNamespace.set(key(p.SubscriberPackage?.NamespacePrefix), {
      namespace: p.SubscriberPackage?.NamespacePrefix ?? null,
      name: p.SubscriberPackage?.Name ?? null,
      version: v && typeof v.MajorVersion === "number" ? [v.MajorVersion, v.MinorVersion, v.PatchVersion, v.BuildNumber].map(number).join(".") : null,
      versionName: v?.Name ?? null,
      installed: true,
      deprecated: flag(v?.IsDeprecated),
      managed: flag(v?.IsManaged),
      licence: null,
    });
  }
  for (const l of licences.status === "ok" ? licences.data : []) {
    const licence = {
      status: l.Status,
      allowed: l.AllowedLicenses < 0 ? null : l.AllowedLicenses,
      siteWide: l.AllowedLicenses < 0,
      used: Math.max(0, l.UsedLicenses),
      expiresOn: l.ExpirationDate ? l.ExpirationDate.slice(0, 10) : null,
    };
    const row = byNamespace.get(key(l.NamespacePrefix));
    if (row) row.licence = licence;
    else byNamespace.set(key(l.NamespacePrefix), { namespace: l.NamespacePrefix, name: null, version: null, versionName: null, installed: false, deprecated: null, managed: null, licence });
  }
  const rows = [...byNamespace.values()].sort((a, b) => (a.namespace ?? "").localeCompare(b.namespace ?? ""));
  return { rows, notes };
}

// ---------------------------------------------------------------------------
// Connected apps

/** The most tokens read. Past it, every count is a floor, and a note says so. */
export const OAUTH_TOKEN_MAX_ROWS = 50_000;
export const OAUTH_TOKEN_QUERY = "SELECT AppName, UserId, CreatedDate, LastUsedDate, UseCount FROM OauthToken";
const APPS_QUERY = "SELECT Name, OptionsAllowAdminApprovedUsersOnly FROM ConnectedApplication";

/** Each app's use, worked out while the tokens are held: who held them goes no further than this step. */
export type AppUse = { name: string; users: number; firstAuthorised: string | null; lastUsed: string | null; uses: number | null };
export type ConnectedAppsData = { apps: Sourced<AppSetting[]>; use: Sourced<AppUse[]> };

export const connectedAppsExtractor: Extractor<ConnectedAppsData> = {
  id: "inventory-connected-apps",
  version: 1,
  async run(ctx) {
    const apps = earlier(ctx, orgDocumentationExtractor)?.connectedApps ?? (await readSoql<AppSetting>(ctx, APPS_QUERY));
    const tokens = await readSoql<{ AppName: string | null; UserId: string | null; CreatedDate: string | null; LastUsedDate: string | null; UseCount: number | null }>(ctx, OAUTH_TOKEN_QUERY, { maxRecords: OAUTH_TOKEN_MAX_ROWS });
    if (tokens.status !== "ok") return { apps, use: tokens };
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
  if (use.status !== "ok") notes.push(`The OAuth tokens couldn't be read, so no app's use is shown: ${use.reason}`);
  if (use.status === "ok" && use.truncated) notes.push(`More than ${OAUTH_TOKEN_MAX_ROWS.toLocaleString("en-GB")} OAuth tokens: we read that many, so each app's users and uses are at least what's shown.`);
  const used = new Map(use.status === "ok" ? use.data.map((u) => [u.name, u]) : []);
  const rows = new Map<string, ConnectedAppRow>();
  const blank = (name: string, installed: boolean | null, adminApprovedOnly: boolean | null): ConnectedAppRow => {
    const u = used.get(name);
    return { name, installed, adminApprovedOnly, firstAuthorised: u?.firstAuthorised ?? null, lastUsed: u?.lastUsed ?? null, users: use.status === "ok" ? (u?.users ?? 0) : null, uses: u?.uses ?? (use.status === "ok" ? 0 : null) };
  };
  for (const a of apps.status === "ok" ? apps.data : []) rows.set(a.Name, blank(a.Name, true, typeof a.OptionsAllowAdminApprovedUsersOnly === "boolean" ? a.OptionsAllowAdminApprovedUsersOnly : null));
  // An app only users authorised has tokens and no entry of its own.
  for (const name of used.keys()) if (!rows.has(name)) rows.set(name, blank(name, apps.status === "ok" ? false : null, null));
  return { rows: [...rows.values()].sort((a, b) => a.name.localeCompare(b.name)), notes };
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
      return { checked: [], notInOrg: [], sets: { status: "unavailable", reason: readableReason(err.message), source }, holdersGap: null };
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
  const rows = [...data.sets.data].sort((a, b) => a.kind.localeCompare(b.kind) || a.name.localeCompare(b.name) || a.apiName.localeCompare(b.apiName));
  return { rows, notes, checked: data.checked.map((c) => c.label), notInOrg: data.notInOrg };
}

// ---------------------------------------------------------------------------
// Org limits

/** Every limit /limits gave as the connection first read it, which costs no call. */
export const limitsExtractor: Extractor<Sourced<Record<string, OrgLimit>>> = {
  id: "inventory-limits",
  version: 1,
  run(ctx) {
    const source = { kind: "rest", path: `/services/data/v${ctx.client.apiVersion}/limits` } as const;
    const limits = ctx.client.allLimitsAtConnect;
    if (Object.keys(limits).length === 0) return Promise.resolve({ status: "unavailable", reason: "The org reported no limits.", source });
    return Promise.resolve({ status: "ok", data: limits, source, truncated: false });
  },
};

function limitRows(data: Sourced<Record<string, OrgLimit>>): Inventory["limits"] {
  if (data.status !== "ok") return { unavailable: data.reason };
  const rows: LimitRow[] = Object.entries(data.data)
    .map(([name, l]) => ({ name, max: l.Max, used: l.Max - l.Remaining }))
    .sort((a, b) => a.name.localeCompare(b.name));
  return { rows, notes: [] };
}

// ---------------------------------------------------------------------------
// The file

export const INVENTORY_EXTRACTORS: Extractor[] = [fillRatesExtractor, packagesExtractor, connectedAppsExtractor, riskyPermissionsExtractor, limitsExtractor];

/** The inventory steps a run of this scope takes: a set-up only scan leaves out any that counts records, as modulesFor does. */
export function inventoryExtractorsFor(scope: RunScope): Extractor[] {
  return INVENTORY_EXTRACTORS.filter((e) => scope === "complete" || !e.records);
}

export const SETUP_ONLY_FILL_RATES = "A set-up only scan doesn't count records, so fill rates weren't read.";

/** The inventory from the run's extracts. A step that didn't run makes its section unavailable, with why. */
export function buildInventory(extracts: Map<string, unknown>, scope: RunScope, madeAt: Date): Inventory {
  const got = <T>(e: Extractor<T>): T | undefined => extracts.get(e.id) as T | undefined;
  const notRun = { unavailable: "This step didn't run." };
  const fill = got(fillRatesExtractor);
  const fillRates: Inventory["fillRates"] = !fill
    ? { unavailable: scope === "complete" ? notRun.unavailable : SETUP_ONLY_FILL_RATES }
    : fill.status !== "ok"
      ? { unavailable: fill.reason }
      : {
          rows: fill.data.objects,
          notes: [
            ...(fill.data.scopeNote ? [fill.data.scopeNote] : []),
            ...(fill.data.leftOut ? [`${fill.data.leftOut} more objects with records weren't counted: we count the ${FILL_RATE_MAX_OBJECTS} with the most records.`] : []),
          ],
        };
  const packages = got(packagesExtractor);
  const apps = got(connectedAppsExtractor);
  const risky = got(riskyPermissionsExtractor);
  const limits = got(limitsExtractor);
  return {
    version: INVENTORY_VERSION,
    madeAt: madeAt.toISOString(),
    scope,
    fillRates,
    packages: packages ? packageRows(packages) : notRun,
    connectedApps: apps ? connectedAppRows(apps) : notRun,
    riskyPermissions: risky ? riskyRows(risky) : notRun,
    limits: limits ? limitRows(limits) : notRun,
  };
}
