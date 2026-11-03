import { isUnavailable, type SalesforceClient } from "../salesforce/client.ts";

// After an admin connects, we check the user they authorised as can't write,
// from the org's own permission records. The allowlist already stops the engine
// sending a write, but a token for an admin would still be one bug away from
// changing the org. This makes "the token can't write" a fact we've read.
//
// Profiles count too: each has its own permission set, which shows up in
// PermissionSetAssignment, and so do permission set groups.

// System permissions that let a user change data or setup on their own.
export const WRITE_PERMISSIONS = [
  "PermissionsModifyAllData",
  "PermissionsModifyMetadata",
  "PermissionsCustomizeApplication",
  "PermissionsAuthorApex",
  "PermissionsManageUsers",
  "PermissionsEditTask",
  "PermissionsEditEvent",
] as const;

export type ReadOnlyCheck =
  | { status: "read_only"; permissionSets: string[] }
  | { status: "can_write"; permissionSets: string[]; systemPermissions: string[]; objects: string[] }
  | { status: "unknown"; reason: string };

const USER_ID = /^005[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$/;

export async function checkReadOnlyUser(client: SalesforceClient, userId: string): Promise<ReadOnlyCheck> {
  if (!USER_ID.test(userId)) throw new Error("Not a user id");
  const assigned = `SELECT PermissionSetId FROM PermissionSetAssignment WHERE AssigneeId = '${userId}'`;
  try {
    const sets = await client.query<Record<string, unknown> & { Name: string }>(`SELECT Name, ${WRITE_PERMISSIONS.join(", ")} FROM PermissionSet WHERE Id IN (${assigned})`);
    const objects = await client.query<{ SobjectType: string }>(
      `SELECT SobjectType FROM ObjectPermissions WHERE ParentId IN (${assigned}) AND (PermissionsCreate = true OR PermissionsEdit = true OR PermissionsDelete = true)`,
    );
    if (sets.truncated || objects.truncated) return { status: "unknown", reason: "The permission records were cut short" };
    if (sets.records.length === 0) return { status: "unknown", reason: "No permission sets were visible for the user, so nothing was proven" };
    const names = sets.records.map((s) => s.Name).sort();
    const systemPermissions = [...new Set(sets.records.flatMap((s) => WRITE_PERMISSIONS.filter((p) => s[p] === true)))].sort();
    const writable = [...new Set(objects.records.map((o) => o.SobjectType))].sort();
    if (systemPermissions.length === 0 && writable.length === 0) return { status: "read_only", permissionSets: names };
    return { status: "can_write", permissionSets: names, systemPermissions, objects: writable };
  } catch (err) {
    if (isUnavailable(err)) return { status: "unknown", reason: `The permission records couldn't be read (${err.code})` };
    throw err;
  }
}
