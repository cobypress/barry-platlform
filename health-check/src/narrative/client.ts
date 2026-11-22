import Anthropic, { AnthropicError } from "@anthropic-ai/sdk";
import { WorkloadIdentityError } from "@anthropic-ai/sdk/lib/credentials/types";

// The one place this service talks to Claude. Everything else takes a
// NarrativeClient, so tests use a stub and no key is ever needed to run them.
//
// Request shape, checked against the Anthropic docs on 2026-11-02 (see
// ARCHITECTURE.md): no temperature, top_p or top_k, no forced tool_choice, no
// assistant prefill. Thinking can't be turned off on Opus 5.5, so it's left out
// and effort is set explicitly. Structured output goes through output_config.

export type NarrativeRequest = {
  model: string;
  system: string;
  prompt: string;
  /** JSON Schema the reply must follow. */
  schema: Record<string, unknown>;
  effort: "low" | "medium" | "high" | "xhigh" | "max";
  maxTokens: number;
};

export type NarrativeReply = {
  /** "refusal" and "max_tokens" mean the text can't be used. */
  stopReason: string | null;
  text: string;
  model: string;
};

export interface NarrativeClient {
  complete(request: NarrativeRequest): Promise<NarrativeReply>;
}

/** The part of the SDK we use, so a test can stand in for it. */
export type MessagesApi = {
  messages: { stream(params: Anthropic.MessageStreamParams): { finalMessage(): Promise<Anthropic.Message> } };
};

export function buildMessageParams(request: NarrativeRequest): Anthropic.MessageStreamParams {
  return {
    model: request.model,
    max_tokens: request.maxTokens,
    system: request.system,
    messages: [{ role: "user", content: request.prompt }],
    output_config: { effort: request.effort, format: { type: "json_schema", schema: request.schema } },
  };
}

/** Told how each call went, so the worker can say when Claude stops and starts letting it in (DECISIONS.md 667). */
export type ClaudeObserver = (outcome: { ok: true; model: string } | { ok: false; error: unknown }) => void;

/** An observer that throws (its log, say) never stops a call or replaces the call's own error. */
function tell(observe: ClaudeObserver | undefined, outcome: Parameters<ClaudeObserver>[0]): void {
  try {
    observe?.(outcome);
  } catch {
    // Nothing to do: the call's result matters more than the log line.
  }
}

/**
 * A failure that means Claude won't let this worker in, rather than a busy or
 * unreachable API. Checked by class, since the SDK's error classes leave
 * `name` as "Error". A federation error is a sign-in failure unless the token
 * endpoint couldn't be reached, or answered 429, 5xx or a 200 the SDK couldn't
 * use. A missing, unreadable
 * or empty identity token file is a plain AnthropicError from the SDK's file
 * reader, matched by its fixed wording.
 */
export function isSignInFailure(err: unknown): boolean {
  if (err instanceof WorkloadIdentityError) {
    // No status: the worker's own side (the token's size, the kept file's safety), unless the endpoint wasn't reached.
    if (err.statusCode === null) return !err.message.startsWith("Failed to reach token endpoint");
    // A refusal is a 4xx other than 429; a 200 with a body the SDK can't use, a 429 or a 5xx is the endpoint's trouble.
    return err.statusCode >= 400 && err.statusCode < 500 && err.statusCode !== 429;
  }
  if (err instanceof Anthropic.AuthenticationError || err instanceof Anthropic.PermissionDeniedError) return true;
  if (err instanceof AnthropicError && /^(Failed to read identity token file|Identity token file at .* is empty|Identity token value is empty)/.test(err.message)) return true;
  // A stand-in client's error in tests, or any other error carrying a status.
  const status = (err as { status?: unknown } | null)?.status;
  return status === 401 || status === 403;
}

/**
 * Wraps the SDK. Server-side refusal fallbacks are left off on purpose: they'd
 * send customer metadata to a different model without anyone deciding to, and a
 * refused section simply keeps the engine's text for a consultant to review.
 */
export function anthropicClient(sdk: MessagesApi = new Anthropic(), observe?: ClaudeObserver): NarrativeClient {
  return {
    async complete(request) {
      let message: Anthropic.Message;
      try {
        message = await sdk.messages.stream(buildMessageParams(request)).finalMessage();
      } catch (error) {
        tell(observe, { ok: false, error });
        throw error;
      }
      tell(observe, { ok: true, model: message.model });
      // Check why it stopped first: a refusal can arrive with no content at all.
      const text = message.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
      return { stopReason: message.stop_reason, text, model: message.model };
    },
  };
}

/** For the worker, which reads its key with the rest of its environment rather than leaving the SDK to find it. */
export function anthropicClientWithKey(apiKey: string, observe?: ClaudeObserver): NarrativeClient {
  return anthropicClient(new Anthropic({ apiKey }), observe);
}

/**
 * The SDK's OIDC federation, with its access token cached at credentialsPath
 * (the SDK writes it with mode 0600 in a 0700 folder). A process that starts
 * again in the same instance, after a recycle or a crash, uses the token the
 * last one got while it's still fresh, instead of exchanging the identity token
 * again: Anthropic takes an identity token with a jti once only, and "a
 * restarted process that presents a token it already exchanged" fails
 * (Anthropic's workload identity federation docs, "Token lifetime and
 * refresh"). Once the cached token nears its end, the SDK exchanges whatever
 * the token file then holds, as in a process that never stopped. A config
 * passed in code means the SDK reads no key from the environment.
 */
export function federationClient(env: NodeJS.ProcessEnv, credentialsPath: string | null, options: { fetch?: typeof fetch; observe?: ClaudeObserver } = {}): NarrativeClient {
  const sdk = new Anthropic({
    ...(options.fetch ? { fetch: options.fetch } : {}),
    config: {
      version: "1.0",
      authentication: {
        type: "oidc_federation",
        federation_rule_id: env.ANTHROPIC_FEDERATION_RULE_ID ?? "",
        service_account_id: env.ANTHROPIC_SERVICE_ACCOUNT_ID || undefined,
        // Unset, the SDK falls back to ANTHROPIC_IDENTITY_TOKEN.
        identity_token: env.ANTHROPIC_IDENTITY_TOKEN_FILE ? { source: "file", path: env.ANTHROPIC_IDENTITY_TOKEN_FILE } : undefined,
        // Null: in memory only, as with variables alone.
        credentials_path: credentialsPath ?? undefined,
      },
      organization_id: env.ANTHROPIC_ORGANIZATION_ID,
      workspace_id: env.ANTHROPIC_WORKSPACE_ID || undefined,
    },
  });
  return anthropicClient(sdk, options.observe);
}
