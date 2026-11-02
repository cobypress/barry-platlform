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
