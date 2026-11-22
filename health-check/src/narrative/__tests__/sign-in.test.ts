import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AnthropicError } from "@anthropic-ai/sdk";
import { WorkloadIdentityError } from "@anthropic-ai/sdk/lib/credentials/types";
import { ClaudeWatch, federationCachePath, identityTokenFacts, prepareSignInCache } from "../sign-in.ts";
import { anthropicClient, federationClient, isSignInFailure, type MessagesApi, type NarrativeRequest } from "../client.ts";
import { fakeAnthropic, syntheticJwt } from "./fake-anthropic.ts";

// The worker's Claude sign-in by OIDC federation (DECISIONS.md 667), through the real SDK with a stand-in for
// Anthropic's token endpoint that, like the real one, takes an identity token with a jti claim once only.

const temp = (): string => mkdtempSync(join(tmpdir(), "hc-sign-in-"));
const NOW = 1_795_000_000;

const ENV = (tokenFile: string): NodeJS.ProcessEnv => ({
  ANTHROPIC_FEDERATION_RULE_ID: "fdrl_synthetic",
  ANTHROPIC_ORGANIZATION_ID: "00000000-0000-4000-8000-000000000001",
  ANTHROPIC_SERVICE_ACCOUNT_ID: "svac_synthetic",
  ANTHROPIC_WORKSPACE_ID: "wrkspc_synthetic",
  ANTHROPIC_IDENTITY_TOKEN_FILE: tokenFile,
});

const REQUEST: NarrativeRequest = { model: "claude-opus-5-5", system: "s", prompt: "p", schema: { type: "object" }, effort: "low", maxTokens: 16 };

function tokenFileIn(dir: string, jti = "jti-1"): string {
  const file = join(dir, "identity-token");
  writeFileSync(file, syntheticJwt({ iss: "https://oidc.render.com/tea-synthetic", sub: "synthetic", aud: "api.anthropic.com", iat: Math.floor(Date.now() / 1000), exp: Math.floor(Date.now() / 1000) + 3600, jti }));
  return file;
}

describe("Claude by OIDC federation", () => {
  test("a process that starts again in the same instance uses the access token the last one kept, and exchanges nothing", async () => {
    const dir = temp();
    const tokenFile = tokenFileIn(dir);
    const anthropic = fakeAnthropic();
    const cache = join(dir, "claude-sign-in");
    const kept = federationCachePath(ENV(tokenFile), cache);
    assert.equal(prepareSignInCache(cache, kept, () => {}), kept);

    const first = await federationClient(ENV(tokenFile), kept, { fetch: anthropic.fetch }).complete(REQUEST);
    assert.equal(first.text, '{"ok":true}');
    assert.equal(anthropic.exchanges, 1);
    assert.equal(statSync(kept).mode & 0o777, 0o600);
    assert.equal(statSync(cache).mode & 0o777, 0o700);

    // The recycle: a new process, the same unrotated identity token on the same disk.
    const again = await federationClient(ENV(tokenFile), kept, { fetch: anthropic.fetch }).complete(REQUEST);
    assert.equal(again.text, '{"ok":true}');
    assert.equal(anthropic.exchanges, 1);
    assert.equal(anthropic.refused, 0);

    // Without the kept token, which is what 22 Nov 2026 on Render may have been: the second exchange is refused.
    const fresh = federationClient(ENV(tokenFile), null, { fetch: anthropic.fetch }).complete(REQUEST);
    await assert.rejects(fresh, (err: unknown) => err instanceof WorkloadIdentityError && isSignInFailure(err) && /status 401/.test(err.message));
    assert.equal(anthropic.refused, 1);
  });

  test("a change to one of the four ids never reads the old token", () => {
    const base = ENV("/x");
    const path = federationCachePath(base, "/d");
    for (const name of ["ANTHROPIC_FEDERATION_RULE_ID", "ANTHROPIC_ORGANIZATION_ID", "ANTHROPIC_SERVICE_ACCOUNT_ID", "ANTHROPIC_WORKSPACE_ID"]) {
      assert.notEqual(federationCachePath({ ...base, [name]: "changed" }, "/d"), path, name);
    }
    assert.equal(federationCachePath({ ...base, ANTHROPIC_IDENTITY_TOKEN_FILE: "/y" }, "/d"), path);
    assert.ok(!path.includes("fdrl_") && !path.includes("svac_"));
  });
});

describe("the folder the access token is kept in", () => {
  const file = (dir: string): string => join(dir, "federation-0000000000000000.json");

  test("a loose folder that was already there is closed to 0700", () => {
    const dir = join(temp(), "claude-sign-in");
    mkdirSync(dir);
    chmodSync(dir, 0o777);
    const lines: string[] = [];
    assert.equal(prepareSignInCache(dir, file(dir), (l) => lines.push(l)), file(dir));
    assert.equal(statSync(dir).mode & 0o777, 0o700);
    assert.deepEqual(lines, ["Claude's access token is kept on the disk, so a worker that starts again in place uses it while it lasts instead of signing in again."]);
  });

  test("a planted link is deleted without touching what it points at", () => {
    const root = temp();
    const dir = join(root, "claude-sign-in");
    mkdirSync(dir, { mode: 0o700 });
    const planted = join(root, "planted.json");
    writeFileSync(planted, JSON.stringify({ access_token: "sk-ant-oat01-planted", expires_at: 4_000_000_000 }), { mode: 0o600 });
    symlinkSync(planted, file(dir));
    const lines: string[] = [];
    assert.equal(prepareSignInCache(dir, file(dir), (l) => lines.push(l)), file(dir));
    assert.equal(existsSync(file(dir)), false);
    assert.match(readFileSync(planted, "utf8"), /planted/);
    assert.ok(lines.includes("A kept Claude access token wasn't safe to use (a link, another user's, or readable by others), so it was deleted and the worker signs in afresh."), lines.join("\n"));
    assert.ok(!lines.join("\n").includes("sk-ant"));
  });

  test("a group-readable file is deleted, so the SDK doesn't refuse every call over it", async () => {
    const root = temp();
    const dir = join(root, "claude-sign-in");
    mkdirSync(dir, { mode: 0o700 });
    writeFileSync(file(dir), JSON.stringify({ access_token: "sk-ant-oat01-loose", expires_at: 4_000_000_000 }));
    chmodSync(file(dir), 0o640);
    // Left in place, the SDK refuses it on every call, and that is a sign-in failure.
    const refused = federationClient(ENV(tokenFileIn(root)), file(dir), { fetch: fakeAnthropic().fetch }).complete(REQUEST);
    await assert.rejects(refused, (err: unknown) => err instanceof WorkloadIdentityError && isSignInFailure(err) && /group\/world-readable/.test(err.message));
    const lines: string[] = [];
    assert.equal(prepareSignInCache(dir, file(dir), (l) => lines.push(l)), file(dir));
    assert.equal(existsSync(file(dir)), false);
    assert.equal(lines.length, 2);
    const anthropic = fakeAnthropic();
    await federationClient(ENV(tokenFileIn(root, "jti-2")), file(dir), { fetch: anthropic.fetch }).complete(REQUEST);
    assert.equal(anthropic.exchanges, 1);
  });

  test("a folder that can't be made is said once, the token is kept in memory, and the start goes on", () => {
    const root = temp();
    chmodSync(root, 0o500);
    try {
      const dir = join(root, "claude-sign-in");
      const lines: string[] = [];
      assert.equal(prepareSignInCache(dir, file(dir), (l) => lines.push(l)), null);
      assert.deepEqual(lines, ["Claude's access token can't be kept on the disk (EACCES), so a worker that starts again in place signs in afresh."]);
    } finally {
      chmodSync(root, 0o700);
    }
  });

  test("a folder that is a link isn't used", () => {
    const root = temp();
    const elsewhere = join(root, "elsewhere");
    mkdirSync(elsewhere, { mode: 0o700 });
    const dir = join(root, "claude-sign-in");
    symlinkSync(elsewhere, dir);
    const lines: string[] = [];
    assert.equal(prepareSignInCache(dir, file(dir), (l) => lines.push(l)), null);
    assert.ok(lstatSync(dir).isSymbolicLink());
    assert.match(lines.join("\n"), /can't be kept on the disk \(its folder is a link/);
  });
});

describe("what the log says about sign-in", () => {
  test("the identity token's age, expiry and single use are said, and none of the token", () => {
    const dir = temp();
    const tokenFile = join(dir, "identity-token");
    const jwt = syntheticJwt({ sub: "srv-synthetic", iat: NOW - 12 * 60, exp: NOW + 48 * 60, jti: "jti-secretish" });
    writeFileSync(tokenFile, jwt);
    const line = identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN_FILE: tokenFile }, new Date(NOW * 1000));
    assert.equal(line, "The identity token was issued 12 minutes ago and expires in 48 minutes; it carries a single-use id, so Anthropic takes it once only.");
    for (const part of [...jwt.split("."), "srv-synthetic", "jti-secretish"]) assert.ok(!line.includes(part));
    const old = syntheticJwt({ iat: NOW - 3700, exp: NOW - 100 });
    assert.match(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: old }, new Date(NOW * 1000)), /issued 62 minutes ago and expired 2 minutes ago; it carries no single-use id/);
    assert.equal(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN_FILE: join(dir, "missing") }), "The identity token file couldn't be read (ENOENT).");
    assert.equal(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: "not-a-jwt" }), "The identity token isn't a JWT the worker can read.");
  });

  test("odd claims are said as unknown, never as nonsense", () => {
    const at = new Date(NOW * 1000);
    const said = (claims: unknown): string => identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: `a.${Buffer.from(JSON.stringify(claims)).toString("base64url")}.c` }, at);
    assert.equal(said({ iat: -5, exp: 1e300 }), "The identity token was issued at an unknown time and has an unknown expiry; it carries no single-use id.");
    assert.equal(said({ iat: "x", exp: null }), "The identity token was issued at an unknown time and has an unknown expiry; it carries no single-use id.");
    assert.equal(said({ iat: NOW + 600, exp: NOW + 4200 }), "The identity token was issued 10 minutes in the future and expires in 70 minutes; it carries no single-use id.");
    assert.equal(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: `a.${Buffer.from('{"iat":1e400}').toString("base64url")}.c` }, at), "The identity token was issued at an unknown time and has an unknown expiry; it carries no single-use id.");
    for (const odd of ["a.bnVsbA.c", "a.NQ.c", "a.W10.c"]) assert.equal(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: odd }, at), "The identity token isn't a JWT the worker can read.", odd);
    assert.equal(identityTokenFacts({ ANTHROPIC_IDENTITY_TOKEN: `a.${"A".repeat(20_000)}.c` }, at), "The identity token is 20 KiB, over the 16 KiB Anthropic takes.");
  });

  test("sign-in failures are told apart by the SDK's own classes", () => {
    assert.equal(new WorkloadIdentityError("x").name, "Error", "the SDK leaves name unset, so it can't be matched on");
    assert.equal(isSignInFailure(new WorkloadIdentityError("Token exchange failed with status 401", 401)), true);
    assert.equal(isSignInFailure(new WorkloadIdentityError("Identity token is 17 KiB, exceeds the 16 KiB assertion limit")), true);
    assert.equal(isSignInFailure(new AnthropicError("Failed to read identity token file at /x: Error: ENOENT")), true);
    assert.equal(isSignInFailure(new WorkloadIdentityError("Token exchange failed with status 500", 500)), false);
    assert.equal(isSignInFailure(new WorkloadIdentityError("Token exchange failed with status 429", 429)), false);
    assert.equal(isSignInFailure(new WorkloadIdentityError("Failed to reach token endpoint https://api.anthropic.com/v1/oauth/token: TypeError: fetch failed")), false);
    assert.equal(isSignInFailure(Object.assign(new Error("x"), { status: 403 })), true);
    assert.equal(isSignInFailure(Object.assign(new Error("x"), { status: 500 })), false);
    assert.equal(isSignInFailure(Object.assign(new Error("x"), { name: "WorkloadIdentityError" })), false);
  });

  test("the log says once when sign-in stops working and once when it works again, and a busy API isn't a sign-in failure", async () => {
    const lines: string[] = [];
    const watch = new ClaudeWatch((l) => lines.push(l), () => "Facts.");
    let fail: unknown = null;
    const sdk = {
      messages: {
        stream: () => ({
          finalMessage: async () => {
            if (fail) throw fail;
            return { model: "claude-opus-5-5", stop_reason: "end_turn", content: [{ type: "text", text: "{}" }] };
          },
        }),
      },
    } as unknown as MessagesApi;
    const client = anthropicClient(sdk, watch.observe);
    // Quiet until the startup check has said its line.
    fail = new WorkloadIdentityError("Token exchange failed with status 401: Authentication failed", 401);
    await assert.rejects(client.complete(REQUEST));
    fail = null;
    await client.complete(REQUEST);
    assert.deepEqual(lines, []);
    watch.begin();
    fail = new WorkloadIdentityError("Token exchange failed with status 401: Authentication failed", 401);
    await assert.rejects(client.complete(REQUEST));
    await assert.rejects(client.complete(REQUEST));
    fail = Object.assign(new Error("Overloaded"), { status: 529 });
    await assert.rejects(client.complete(REQUEST));
    fail = null;
    await client.complete(REQUEST);
    await client.complete(REQUEST);
    assert.deepEqual(lines, [
      "Claude sign-in stopped working, so runs use the engine's own text until it works again. WorkloadIdentityError: Token exchange failed with status 401: Authentication failed Facts.",
      "Claude sign-in works again: claude-opus-5-5 answered. Runs from now on get Claude's drafts.",
    ]);
  });

  test("a missing identity token file, through the real SDK, is said as sign-in stopping", async () => {
    const lines: string[] = [];
    const watch = new ClaudeWatch((l) => lines.push(l));
    watch.begin();
    watch.note(true, "claude-opus-5-5 answered");
    const client = federationClient(ENV(join(temp(), "missing")), null, { fetch: fakeAnthropic().fetch, observe: watch.observe });
    await assert.rejects(client.complete(REQUEST));
    assert.equal(lines.length, 1);
    assert.match(lines[0]!, /^Claude sign-in stopped working, so runs use the engine's own text until it works again\. AnthropicError: Failed to read identity token file/);
  });

  test("a log line that throws never stops a call or replaces its error", async () => {
    const watch = new ClaudeWatch(() => {
      throw new Error("log broke");
    });
    watch.begin();
    const client = federationClient(ENV(join(temp(), "missing")), null, { fetch: fakeAnthropic().fetch, observe: watch.observe });
    await assert.rejects(client.complete(REQUEST), (err: unknown) => err instanceof AnthropicError && /Failed to read identity token file/.test(err.message));
    const dir = temp();
    const ok = federationClient(ENV(tokenFileIn(dir)), null, { fetch: fakeAnthropic().fetch, observe: watch.observe });
    assert.equal((await ok.complete(REQUEST)).text, '{"ok":true}');
  });
});
