import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { OrgNotApprovedError, SfCliConnectionProvider, approvedAliases } from "../sf-cli-provider.ts";

const env = (aliases: string, extra: Record<string, string> = {}): NodeJS.ProcessEnv => ({ HC_APPROVED_ORG_ALIASES: aliases, ...extra }) as NodeJS.ProcessEnv;
const display = JSON.stringify({ status: 0, result: { accessToken: "t", instanceUrl: "https://x.example.com", id: "00D000000000001AAA", username: "a@example.com" } });

describe("SfCliConnectionProvider", () => {
  test("refuses an alias that isn't approved, without calling the CLI", () => {
    let called = false;
    assert.throws(
      () => new SfCliConnectionProvider("some-customer", { env: env(""), exec: async () => ((called = true), display) }),
      OrgNotApprovedError,
    );
    assert.equal(called, false);
  });

  test("an approved list doesn't approve anything else", () => {
    assert.throws(() => new SfCliConnectionProvider("other", { env: env("dev-org") }), OrgNotApprovedError);
    assert.throws(() => new SfCliConnectionProvider("dev-org-2", { env: env("dev-org") }), OrgNotApprovedError);
  });

  test("won't run in production", () => {
    assert.throws(() => new SfCliConnectionProvider("dev-org", { env: env("dev-org", { NODE_ENV: "production" }) }), /development only/);
  });

  test("reads the login for an approved alias", async () => {
    let args: string[] = [];
    const provider = new SfCliConnectionProvider("dev-org", { env: env(" dev-org , other "), exec: async (a) => ((args = a), display) });
    const connection = await provider.connect();
    assert.deepEqual(args, ["org", "display", "--target-org", "dev-org", "--json"]);
    assert.equal(connection.instanceUrl, "https://x.example.com");
    assert.equal(connection.orgId, "00D000000000001AAA");
  });

  test("a CLI failure doesn't repeat what the CLI said", async () => {
    const provider = new SfCliConnectionProvider("dev-org", { env: env("dev-org"), exec: async () => { throw new Error("token=SECRET"); } });
    await assert.rejects(provider.connect(), (err: Error) => !err.message.includes("SECRET"));
  });

  test("output that won't parse isn't repeated in the error", async () => {
    const provider = new SfCliConnectionProvider("dev-org", { env: env("dev-org"), exec: async () => '{"accessToken":"SECRET"' });
    await assert.rejects(provider.connect(), (err: Error) => !err.message.includes("SECRET"));
  });

  test("no session is an error", async () => {
    const provider = new SfCliConnectionProvider("dev-org", { env: env("dev-org"), exec: async () => JSON.stringify({ result: {} }) });
    await assert.rejects(provider.connect(), /no active session/);
  });

  test("parses the approved list", () => {
    assert.deepEqual(approvedAliases(env("a, b,,c ")), ["a", "b", "c"]);
    assert.deepEqual(approvedAliases({} as NodeJS.ProcessEnv), []);
  });
});
