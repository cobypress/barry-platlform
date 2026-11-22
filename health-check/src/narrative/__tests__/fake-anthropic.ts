// A stand-in for Anthropic's token endpoint and Messages API, for tests that run the real SDK (DECISIONS.md 667).
// Like the real endpoint, it takes an identity token with a jti claim once only.

export function syntheticJwt(claims: Record<string, unknown>): string {
  const part = (o: unknown): string => Buffer.from(JSON.stringify(o)).toString("base64url");
  return `${part({ alg: "RS256", kid: "synthetic" })}.${part(claims)}.c3ludGhldGljc2lnbmF0dXJl`;
}

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
export function fakeAnthropic(): { fetch: typeof fetch; exchanges: number; refused: number } {
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
