import Anthropic from "@anthropic-ai/sdk";

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

/**
 * Wraps the SDK. Server-side refusal fallbacks are left off on purpose: they'd
 * send customer metadata to a different model without anyone deciding to, and a
 * refused section simply keeps the engine's text for a consultant to review.
 */
export function anthropicClient(sdk: MessagesApi = new Anthropic()): NarrativeClient {
  return {
    async complete(request) {
      const message = await sdk.messages.stream(buildMessageParams(request)).finalMessage();
      // Check why it stopped first: a refusal can arrive with no content at all.
      const text = message.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
      return { stopReason: message.stop_reason, text, model: message.model };
    },
  };
}
