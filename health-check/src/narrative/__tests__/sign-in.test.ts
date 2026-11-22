import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ClaudeWatch, federationCachePath, identityTokenFacts, isSignInFailure } from "../sign-in.ts";
import { anthropicClient, federationClient, type MessagesApi, type NarrativeRequest } from "../client.ts";

// The worker's Claude sign-in by OIDC federation (DECISIONS.md 667), through the real SDK with a stand-in for
// Anthropic's token endpoint that, like the real one, takes an identity token with a jti claim once only.

const temp = (): string => mkdtempSync(join(tmpdir(), "hc-sign-in-"));
const NOW = 1_795_000_000;

function syntheticJwt(claims: Record<string, unknown>): string {
  const part = (o: unknown): string => Buffer.from(JSON.stringify(o)).toString("base64url");
  return `${part({ alg: "RS256", kid: "synthetic" })}.${part(claims)}.c3ludGhldGljc2lnbmF0dXJl`;
}

const ENV = (tokenFile: string): NodeJS.ProcessEnv => ({
  ANTHROPIC_FEDERATION_RULE_ID: "fdrl_synthetic",
  ANTHROPIC_ORGANIZATION_ID: "00000000-0000-4000-8000-000000000001",
  ANTHROPIC_SERVICE_ACCOUNT_ID: "svac_synthetic",
  ANTHROPIC_WORKSPACE_ID: "wrkspc_synthetic",
  ANTHROPIC_IDENTITY_TOKEN_FILE: tokenFile,
});

const REQUEST: NarrativeRequest = { model: "claude-opus-5-5", system: "s", prompt: "p", schema: { type: "object" }, effort: "low", maxTokens: 16 };

function sse(): Response {
  const events: [string, unknown][] = [
    ["message_start", { type: "message_start", message: { id: "msg_synthetic", type: "message", role: "assistant", model: "claude-opus-5-5", content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } }],
    ["content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }],
    ["content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: '{"ok":true}' } }],
    ["content_block_stop", { type: "content_block_stop", index: 0 }],
    ["message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 3 } }],
    ["message_stop", { type: "message_stop" }],
  ];
  return new Response(events.map(([e, d]) => `event: ${e}\ndata: ${JSON.stringify(d)}\n\n`).join(""), { status: 200, headers: { "content-type": "text/event-stream" } });
}

/** Anthropic's side: exchanges each jti once, and answers messages only with a token it minted. */
function fakeAnthropic(): { fetch: typeof fetch; exchanges: number; refused: number } {
  const used = new Set<string>();
  const minted = new Set<string>();
  const state = { exchanges: 0, refused: 0, fetch: undefined as unknown as typeof fetch };
  state.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    const url = String(input instanceof Request ? input.url : input);
    if (url.endsWith("/v1/oauth/token")) {
      const body = JSON.parse(String(init?.body)) as { assertion: string };
      const claims = JSON.parse(Buffer.from(body.assertion.split(".")[1]!, "base64url").toString()) as { jti?: string };
      if (claims.jti && used.has(claims.jti)) {
        state.refused++;
        return new Response(JSON.stringify({ type: "error", error: { type: "authentication_error", message: "Authentication failed" } }), { status: 401 });
      }
      if (claims.jti) used.add(claims.jti);
      state.exchanges++;
      const token = `sk-ant-oat01-synthetic${state.exchanges}`;
      minted.add(token);
      return new Response(JSON.stringify({ access_token: token, token_type: "Bearer", expires_in: 600, scope: "workspace:developer" }), { status: 200, headers: { "content-type": "application/json" } });
    }
    const auth = new Headers(init?.headers).get("authorization") ?? "";
    if (!minted.has(auth.replace(/^Bearer /, ""))) return new Response(JSON.stringify({ type: "error", error: { type: "authentication_error", message: "invalid token" } }), { status: 401 });
    return sse();
  }) as typeof fetch;
  return state;
}

describe("Claude by OIDC federation", () => {
  test("a process that starts again in the same instance uses the access token the last one kept, and exchanges nothing", async () => {
    const dir = temp();
    const tokenFile = join(dir, "identity-token");
    writeFileSync(tokenFile, syntheticJwt({ iss: "https://oidc.render.com/tea-synthetic", sub: "synthetic", aud: "api.anthropic.com", iat: NOW, exp: NOW + 3600, jti: "jti-1" }));
    const anthropic = fakeAnthropic();
    const cache = join(dir, "claude-sign-in");

    const first = await federationClient(ENV(tokenFile), federationCachePath(ENV(tokenFile), cache), { fetch: anthropic.fetch }).complete(REQUEST);
    assert.equal(first.text, '{"ok":true}');
    assert.equal(anthropic.exchanges, 1);
    const kept = federationCachePath(ENV(tokenFile), cache);
    assert.equal(statSync(kept).mode & 0o777, 0o600);
    assert.equal(statSync(cache).mode & 0o777, 0o700);

    // The recycle: a new process, the same unrotated identity token on the same disk.
    const again = await federationClient(ENV(tokenFile), federationCachePath(ENV(tokenFile), cache), { fetch: anthropic.fetch }).complete(REQUEST);
    assert.equal(again.text, '{"ok":true}');
    assert.equal(anthropic.exchanges, 1);
    assert.equal(anthropic.refused, 0);

    // Without the kept token, which is what 22 Nov 2026 on Render looked like: the second exchange is refused.
    const fresh = federationClient(ENV(tokenFile), federationCachePath(ENV(tokenFile), temp()), { fetch: anthropic.fetch }).complete(REQUEST);
    await assert.rejects(fresh, (err: unknown) => isSignInFailure(err) && /status 401/.test((err as Error).message));
    assert.equal(anthropic.refused, 1);
  });

  test("a changed rule, organisation, service account or workspace never reads the old token", () => {
    const base = ENV("/x");
    const path = federationCachePath(base, "/d");
    for (const name of ["ANTHROPIC_FEDERATION_RULE_ID", "ANTHROPIC_ORGANIZATION_ID", "ANTHROPIC_SERVICE_ACCOUNT_ID", "ANTHROPIC_WORKSPACE_ID"]) {
      assert.notEqual(federationCachePath({ ...base, [name]: "changed" }, "/d"), path, name);
    }
    assert.equal(federationCachePath({ ...base, ANTHROPIC_IDENTITY_TOKEN_FILE: "/y" }, "/d"), path);
    assert.ok(!path.includes("fdrl_") && !path.includes("svac_"));
  });

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
    await client.complete(REQUEST);
    assert.deepEqual(lines, []);
    fail = Object.assign(new Error("Token exchange failed with status 401: Authentication failed"), { name: "WorkloadIdentityError", statusCode: 401 });
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
    assert.equal(isSignInFailure(Object.assign(new Error("x"), { status: 403 })), true);
    assert.equal(isSignInFailure(Object.assign(new Error("x"), { status: 500 })), false);
  });
});
