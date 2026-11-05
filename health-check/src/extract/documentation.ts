import { CORE_OBJECTS, customObjectsExtractor, recordCountsExtractor } from "./configuration.ts";
import { readSoql, type ExtractContext, type Extractor, type Sourced } from "./types.ts";

// "Your org, documented" (roadmap 3.4): the set-up the other steps don't
// already read. All of it is set-up, never a record, so a set-up review reads
// it too. Every field here is in the Object Reference or the Tooling API guide,
// API 68.0 (QUESTIONS.md has what they leave open).

export type RelationshipRow = { object: string; field: string; dataType: string; referenceTo: string[] };
export type SharingRow = { object: string; internal: string | null; external: string | null };
export type RoleRow = { Id: string; Name: string; ParentRoleId: string | null };
export type PermissionSetSummary = { Id: string; Name: string; Label: string; IsCustom: boolean; NamespacePrefix: string | null; assignees: number };
export type ConnectedAppRow = { Name: string; OptionsAllowAdminApprovedUsersOnly: boolean | null };
export type NamedCredentialRow = { DeveloperName: string; MasterLabel: string; NamespacePrefix: string | null };
export type RemoteSiteRow = { host: string; IsActive: boolean; NamespacePrefix: string | null };

export type OrgDocumentation = {
  /** The objects documented: the core four, then custom objects of our own with records. */
  objects: string[];
  relationships: Sourced<RelationshipRow[]>;
  sharing: Sourced<SharingRow[]>;
  roles: Sourced<RoleRow[]>;
  permissionSets: Sourced<PermissionSetSummary[]>;
  connectedApps: Sourced<ConnectedAppRow[]>;
  namedCredentials: Sourced<NamedCredentialRow[]>;
  remoteSites: Sourced<RemoteSiteRow[]>;
};

const OBJECT_CHUNK = 10;
const OWN_CUSTOM_OBJECT = /^[A-Za-z][A-Za-z0-9_]*__c$/;
const NAMESPACED = /__.+__c$/;
const quote = (names: string[]): string => names.map((n) => `'${n}'`).join(", ");

/** The same objects the complexity inventory counts, so the two never disagree. */
export function documentedObjects(ctx: Pick<ExtractContext, "get">): string[] {
  const counts = ctx.get(recordCountsExtractor);
  const own = (name: string): boolean => OWN_CUSTOM_OBJECT.test(name) && !NAMESPACED.test(name);
  const custom =
    counts.status === "ok"
      ? counts.data.filter((c) => c.count > 0 && own(c.name)).map((c) => c.name)
      : (() => {
          const ids = ctx.get(customObjectsExtractor);
          return ids.status === "ok" ? ids.data.filter((o) => !o.NamespacePrefix).map((o) => `${o.DeveloperName}__c`) : [];
        })();
  return [...new Set([...CORE_OBJECTS, ...custom.sort((a, b) => a.localeCompare(b))])];
}

/** A chunked read whose parts become one source: any part failing fails it all, so a list is never half there. */
async function chunked<R, T>(ctx: ExtractContext, objects: string[], soql: (names: string) => string, map: (row: R) => T[]): Promise<Sourced<T[]>> {
  const source = { kind: "soql", api: "tooling", query: soql("...") } as const;
  const out: T[] = [];
  for (let i = 0; i < objects.length; i += OBJECT_CHUNK) {
    const part = await readSoql<R>(ctx, soql(quote(objects.slice(i, i + OBJECT_CHUNK))), { api: "tooling" });
    if (part.status !== "ok") return { status: "unavailable", reason: part.reason, source };
    out.push(...part.data.flatMap(map));
  }
  return { status: "ok", data: out, source, truncated: false };
}

// ReferenceTo is a RelationshipReferenceTo: an object holding the list, or null.
const targets = (value: unknown): string[] => {
  const list = (value as { referenceTo?: unknown } | null)?.referenceTo;
  return Array.isArray(list) ? list.map(String) : [];
};

/** The host only: a path or query could carry a key, and the host is what says who it talks to. */
const hostOf = (url: string): string => {
  try {
    return new URL(url).host || url;
  } catch {
    return url.replace(/^[a-z]+:\/\//i, "").split(/[/?#]/)[0] ?? url;
  }
};

export const orgDocumentationExtractor: Extractor<OrgDocumentation> = {
  id: "org-documentation",
  version: 1,
  async run(ctx) {
    const objects = documentedObjects(ctx);
    const relationships = await chunked<{ EntityDefinition: { QualifiedApiName: string }; QualifiedApiName: string; DataType: string; ReferenceTo: unknown }, RelationshipRow>(
      ctx,
      objects,
      (names) => `SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, DataType, ReferenceTo FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN (${names})`,
      (r) => (targets(r.ReferenceTo).length ? [{ object: r.EntityDefinition.QualifiedApiName, field: r.QualifiedApiName, dataType: r.DataType, referenceTo: targets(r.ReferenceTo) }] : []),
    );
    const sharing = await chunked<{ QualifiedApiName: string; InternalSharingModel: string | null; ExternalSharingModel: string | null }, SharingRow>(
      ctx,
      objects,
      (names) => `SELECT QualifiedApiName, InternalSharingModel, ExternalSharingModel FROM EntityDefinition WHERE QualifiedApiName IN (${names})`,
      (r) => [{ object: r.QualifiedApiName, internal: r.InternalSharingModel ?? null, external: r.ExternalSharingModel ?? null }],
    );
    const roles = await readSoql<RoleRow>(ctx, "SELECT Id, Name, ParentRoleId FROM UserRole");
    // Profiles have a permission set of their own each, which isn't one anyone assigns.
    const sets = await readSoql<Omit<PermissionSetSummary, "assignees">>(ctx, "SELECT Id, Name, Label, IsCustom, NamespacePrefix FROM PermissionSet WHERE IsOwnedByProfile = false");
    // Only the set's id, so no person's id is kept: the count is all that's needed.
    const held = await readSoql<{ PermissionSetId: string }>(ctx, "SELECT PermissionSetId FROM PermissionSetAssignment WHERE PermissionSet.IsOwnedByProfile = false AND Assignee.IsActive = true");
    const counts = new Map<string, number>();
    if (held.status === "ok") for (const row of held.data) counts.set(row.PermissionSetId, (counts.get(row.PermissionSetId) ?? 0) + 1);
    const permissionSets: Sourced<PermissionSetSummary[]> =
      sets.status !== "ok"
        ? sets
        : held.status !== "ok"
          ? { status: "unavailable", reason: `Who holds each permission set couldn't be read: ${held.reason}`, source: held.source }
          : { ...sets, data: sets.data.map((s) => ({ Id: s.Id, Name: s.Name, Label: s.Label, IsCustom: s.IsCustom, NamespacePrefix: s.NamespacePrefix ?? null, assignees: counts.get(s.Id) ?? 0 })) };
    const connectedApps = await readSoql<ConnectedAppRow>(ctx, "SELECT Name, OptionsAllowAdminApprovedUsersOnly FROM ConnectedApplication");
    const namedCredentials = await readSoql<NamedCredentialRow>(ctx, "SELECT DeveloperName, MasterLabel, NamespacePrefix FROM NamedCredential");
    const sites = await readSoql<{ EndpointUrl: string; IsActive: boolean; NamespacePrefix: string | null }>(ctx, "SELECT EndpointUrl, IsActive, NamespacePrefix FROM RemoteProxy", { api: "tooling" });
    const remoteSites: Sourced<RemoteSiteRow[]> = sites.status === "ok" ? { ...sites, data: sites.data.map((s) => ({ host: hostOf(s.EndpointUrl), IsActive: s.IsActive, NamespacePrefix: s.NamespacePrefix ?? null })) } : sites;
    return { objects, relationships, sharing, roles, permissionSets, connectedApps, namedCredentials, remoteSites };
  },
};
