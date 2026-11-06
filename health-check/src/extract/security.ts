import { redactSecrets } from "../privacy/secrets.ts";
import { readSoql, type Extractor, type Sourced } from "./types.ts";

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
