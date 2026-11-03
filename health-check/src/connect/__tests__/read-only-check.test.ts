import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { checkReadOnlyUser } from "../read-only-check.ts";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import type { ConnectionProvider } from "../../salesforce/connection.ts";
import type { Transport } from "../../salesforce/transport.ts";

const USER = "005000000000001AAA";

const provider: ConnectionProvider = {
  kind: "oauth",
  connect: async () => ({ instanceUrl: "https://x.example.com", accessToken: "t", orgId: "00D000000000001AAA" }),
  refresh: async () => ({ instanceUrl: "https://x.example.com", accessToken: "t", orgId: "00D000000000001AAA" }),
};

async function client(sets: unknown[] | { error: string }, objects: unknown[] = []): Promise<{ sf: SalesforceClient; queries: string[] }> {
  const queries: string[] = [];
  const transport: Transport = {
    async send(request) {
      const url = new URL(request.url);
      const ok = (json: unknown) => ({ status: 200, header: () => null, text: JSON.stringify(json) });
      if (url.pathname === "/services/data/") return ok([{ version: "68.0" }]);
      if (url.pathname.endsWith("/limits")) return ok({ DailyApiRequests: { Max: 1000, Remaining: 990 } });
      const q = url.searchParams.get("q") ?? "";
      queries.push(q);
      if (/FROM PermissionSet WHERE/.test(q)) {
        if (!Array.isArray(sets)) return { status: 400, header: () => null, text: JSON.stringify([{ errorCode: sets.error, message: "no" }]) };
        return ok({ records: sets, totalSize: sets.length });
      }
      return ok({ records: objects, totalSize: objects.length });
    },
  };
  return { sf: await SalesforceClient.connect(provider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport }), queries };
}

const set = (name: string, extra: Record<string, boolean> = {}) => ({
  Name: name,
  PermissionsModifyAllData: false,
  PermissionsModifyMetadata: false,
  PermissionsCustomizeApplication: false,
  PermissionsAuthorApex: false,
  PermissionsManageUsers: false,
  PermissionsEditTask: false,
  PermissionsEditEvent: false,
  ...extra,
});

describe("checkReadOnlyUser", () => {
  test("a user with only read access is read only", async () => {
    const { sf, queries } = await client([set("Barometer_Read_Only"), set("X00e000000000001")]);
    assert.deepEqual(await checkReadOnlyUser(sf, USER), { status: "read_only", permissionSets: ["Barometer_Read_Only", "X00e000000000001"] });
    assert.ok(queries.every((q) => q.includes(`AssigneeId = '${USER}'`)));
  });

  test("a write system permission anywhere, the profile included, means it can write", async () => {
    const { sf } = await client([set("Barometer_Read_Only"), set("X00e000000000001", { PermissionsModifyAllData: true })]);
    const result = await checkReadOnlyUser(sf, USER);
    assert.equal(result.status, "can_write");
    assert.deepEqual(result.status === "can_write" && result.systemPermissions, ["PermissionsModifyAllData"]);
  });

  test("create, edit or delete on any object means it can write", async () => {
    const { sf, queries } = await client([set("Barometer_Read_Only")], [{ SobjectType: "Account" }, { SobjectType: "Account" }]);
    const result = await checkReadOnlyUser(sf, USER);
    assert.deepEqual(result.status === "can_write" && result.objects, ["Account"]);
    assert.match(queries[1]!, /PermissionsCreate = true OR PermissionsEdit = true OR PermissionsDelete = true/);
  });

  test("permission records we can't read prove nothing", async () => {
    const { sf } = await client({ error: "INSUFFICIENT_ACCESS" });
    assert.deepEqual(await checkReadOnlyUser(sf, USER), { status: "unknown", reason: "The permission records couldn't be read (INSUFFICIENT_ACCESS)" });
  });

  test("seeing no permission sets at all proves nothing", async () => {
    const { sf } = await client([]);
    assert.equal((await checkReadOnlyUser(sf, USER)).status, "unknown");
  });

  test("only takes a user id", async () => {
    const { sf } = await client([]);
    await assert.rejects(checkReadOnlyUser(sf, "005' OR Id != '"), /user id/);
  });
});
