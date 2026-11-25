import { redactSecrets } from "../privacy/secrets.ts";
import { readSoql, type Extractor, type Sourced, type SourceInfo } from "./types.ts";
import type { UserRow } from "./users.ts";

export type HealthRiskRow = { RiskType: string; Setting: string; SettingGroup: string | null; OrgValue: string | null; StandardValue: string | null };
export type HealthCheckData = { score: Sourced<Array<{ Score: string | null }>>; risks: Sourced<HealthRiskRow[]> };

const clean = (value: string | null): string | null => (value === null ? null : redactSecrets(value));

// Tooling only, and read directly: Tooling through composite batch comes back
// empty. Score is a string in the API, so the rule parses it and refuses a blank.
export const securityHealthExtractor: Extractor<HealthCheckData> = {
  id: "security-health-check",
  version: 1,
  async run(ctx) {
    const score = await readSoql<{ Score: string | null }>(ctx, "SELECT Score FROM SecurityHealthCheck", { api: "tooling" });
    const risks = await readSoql<HealthRiskRow>(ctx, "SELECT RiskType, Setting, SettingGroup, OrgValue, StandardValue FROM SecurityHealthCheckRisks", { api: "tooling" });
    if (risks.status !== "ok") return { score, risks };
    // Values are org settings, not secrets, but they end up in findings, so they're treated as untrusted text.
    return { score, risks: { ...risks, data: risks.data.map((r) => ({ ...r, OrgValue: clean(r.OrgValue), StandardValue: clean(r.StandardValue) })) } };
  },
};

export type PermissionSetRow = {
  Id: string;
  Label: string;
  IsOwnedByProfile: boolean;
  Profile: { Name: string } | null;
  PermissionsModifyAllData: boolean;
  PermissionsViewAllData: boolean;
};
export type PrivilegedPermissions = {
  sets: Sourced<PermissionSetRow[]>;
  assignments: Sourced<Array<{ AssigneeId: string; PermissionSetId: string }>>;
};

const PRIVILEGED = "PermissionsModifyAllData = true OR PermissionsViewAllData = true";

export const privilegedPermissionsExtractor: Extractor<PrivilegedPermissions> = {
  id: "privileged-permissions",
  version: 1,
  async run(ctx) {
    return {
      sets: await readSoql(ctx, `SELECT Id, Label, IsOwnedByProfile, Profile.Name, PermissionsModifyAllData, PermissionsViewAllData FROM PermissionSet WHERE ${PRIVILEGED}`),
      assignments: await readSoql(
        ctx,
        "SELECT AssigneeId, PermissionSetId FROM PermissionSetAssignment WHERE PermissionSet.PermissionsModifyAllData = true OR PermissionSet.PermissionsViewAllData = true",
      ),
    };
  },
};

/** The rule that judges each privileged permission: the inventory's count links to it (DECISIONS.md 735). */
export const PRIVILEGED_RULES = { PermissionsModifyAllData: "sec.modify-all-data", PermissionsViewAllData: "sec.view-all-data" } as const;
export type PrivilegedPermission = keyof typeof PRIVILEGED_RULES;

/** A profile's or permission set's grant, as the reads give it. */
export type GrantingSet = { Id: string; IsOwnedByProfile: boolean; Profile: { Name: string } | null };

/** Who holds a permission, and how. */
export type Holding = {
  active: UserRow[];
  holders: UserRow[];
  holds: (u: UserRow) => boolean;
  fromProfile: (u: UserRow) => boolean;
  /** Everyone assigned a granting permission set, active or not. */
  fromSet: Set<string>;
  /** The granting sets that aren't a profile's own. */
  added: Set<string>;
};

/**
 * Who holds a permission: the active users whose profile grants it, or who are
 * assigned a permission set that does. A permission set group's assignment
 * counts through the group's own set, as the reads give it (DECISIONS.md 487);
 * a mute isn't read. One count for the Modify and View All Data checks and the
 * inventory's holders, so the two can't disagree (DECISIONS.md 735).
 */
export function holding(granting: GrantingSet[], assignments: ReadonlyArray<{ AssigneeId: string; PermissionSetId: string | null }>, users: UserRow[]): Holding {
  const profiles = new Set(granting.flatMap((s) => (s.IsOwnedByProfile && s.Profile ? [s.Profile.Name] : [])));
  const added = new Set(granting.filter((s) => !s.IsOwnedByProfile).map((s) => s.Id));
  const fromSet = new Set(assignments.filter((a) => a.PermissionSetId !== null && added.has(a.PermissionSetId)).map((a) => a.AssigneeId));
  const fromProfile = (u: UserRow): boolean => profiles.has(u.Profile?.Name ?? "");
  const holds = (u: UserRow): boolean => fromProfile(u) || fromSet.has(u.Id);
  const active = users.filter((u) => u.IsActive);
  return { active, holders: active.filter(holds), holds, fromProfile, fromSet, added };
}

/** Who holds Modify or View All Data, from the reads their checks make, or why it can't be told, in the checks' own words. */
export function privilegedHolding(data: PrivilegedPermissions, users: Sourced<UserRow[]>, permission: PrivilegedPermission): { ok: true; holding: Holding } | { ok: false; reason: string; source: SourceInfo } {
  const { sets, assignments } = data;
  if (users.status !== "ok") return { ok: false, reason: `Users couldn't be read: ${users.reason}`, source: users.source };
  if (sets.status !== "ok") return { ok: false, reason: `Permission sets couldn't be read: ${sets.reason}`, source: sets.source };
  if (assignments.status !== "ok") return { ok: false, reason: `Permission set assignments couldn't be read: ${assignments.reason}`, source: assignments.source };
  return { ok: true, holding: holding(sets.data.filter((s) => s[permission]), assignments.data, users.data) };
}

export type ProfileSetRow = { Id: string; IsCustom: boolean; Profile: { Name: string; UserType: string | null } | null };
/** A set or a group, never both. Each active user also has one row for their profile's own set. */
export type AccessAssignment = { AssigneeId: string; PermissionSetId: string | null; PermissionSetGroupId: string | null };
export type AccessData = {
  profiles: Sourced<ProfileSetRow[]>;
  assignments: Sourced<AccessAssignment[]>;
  groupComponents: Sourced<Array<{ PermissionSetId: string; PermissionSetGroupId: string }>>;
};

// Who holds which permission set, for the least-privilege checks (DECISIONS.md
// 202). Assignments aren't filtered on the set's owner: a permission set group's
// assignment may carry no PermissionSetId, and a filter on it would drop the row.
// Sets inside an assigned group are used even with no assignment of their own.
export const accessExtractor: Extractor<AccessData> = {
  id: "access",
  version: 1,
  async run(ctx) {
    return {
      profiles: await readSoql<ProfileSetRow>(ctx, "SELECT Id, IsCustom, Profile.Name, Profile.UserType FROM PermissionSet WHERE IsOwnedByProfile = true"),
      assignments: await readSoql<AccessAssignment>(ctx, "SELECT AssigneeId, PermissionSetId, PermissionSetGroupId FROM PermissionSetAssignment WHERE Assignee.IsActive = true"),
      groupComponents: await readSoql(ctx, "SELECT PermissionSetId, PermissionSetGroupId FROM PermissionSetGroupComponent"),
    };
  },
};

/**
 * Who holds each permission set, by set id: directly, or through a permission set
 * group they're assigned. Null when the assignments or the groups couldn't be read.
 * Profiles' own sets are in it too; callers pick the sets they mean.
 */
export function holdersOf(access: AccessData): Map<string, Set<string>> | null {
  if (access.assignments.status !== "ok" || access.groupComponents.status !== "ok") return null;
  const inGroup = new Map<string, string[]>();
  for (const c of access.groupComponents.data) inGroup.set(c.PermissionSetGroupId, [...(inGroup.get(c.PermissionSetGroupId) ?? []), c.PermissionSetId]);
  const holders = new Map<string, Set<string>>();
  const add = (set: string, user: string): void => void (holders.get(set) ?? holders.set(set, new Set()).get(set)!).add(user);
  for (const a of access.assignments.data) {
    if (a.PermissionSetId) add(a.PermissionSetId, a.AssigneeId);
    if (a.PermissionSetGroupId) for (const set of inGroup.get(a.PermissionSetGroupId) ?? []) add(set, a.AssigneeId);
  }
  return holders;
}
