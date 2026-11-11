import { findNames, maskNames } from "../privacy/names.ts";
import { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { redactSecrets } from "../privacy/secrets.ts";
import type { NarrativeClient, NarrativeReply, NarrativeRequest } from "./client.ts";
import { APEX_SUMMARY_PROMPT } from "./apex-prompt.ts";
import { SYSTEM_PROMPT } from "./prompt.ts";

// Rule 7: nothing that leaves for Claude may hold a real name. Findings already
// hold tokens, so this is the backstop, and every prompt passes through it.
//
// scrub() only catches full names, usernames, emails and ids. People write first
// names, surnames and possessives ("Sam's pipeline"), and metadata labels can
// hold exactly that, so maskNames() replaces the parts of every known name too
// (see src/privacy/names.ts for what it covers and what it doesn't).

/** Everything that goes to Claude goes through here. */
export function guardPrompt(text: string, pseudonyms: Pseudonymiser): string {
  return redactSecrets(maskNames(pseudonyms.scrub(text), pseudonyms));
}

/**
 * Field by field, before anything is JSON encoded. Escaped quotes in encoded text
 * would slip past the secret patterns, which is why the whole prompt isn't enough.
 */
export function guardDeep<T>(value: T, pseudonyms: Pseudonymiser): T {
  if (typeof value === "string") return guardPrompt(value, pseudonyms) as T;
  if (Array.isArray(value)) return value.map((v) => guardDeep(v, pseudonyms)) as T;
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, guardDeep(v, pseudonyms)])) as T;
  return value;
}

/** True when text would change on its way to Claude, or still holds a name part. For checking Claude's own replies. */
export function holdsNameOrSecret(text: string, pseudonyms: Pseudonymiser): boolean {
  return guardPrompt(text, pseudonyms) !== text || findNames(text, pseudonyms).length > 0;
}

// System prompts are our own constants. Customer text goes in the prompt, where
// it's guarded; a system prompt built from anything else is refused, so nobody
// can route around the guard by putting text there.
// The second is the Apex summaries' own (DECISIONS.md 492), the only one Apex code goes with.
export const SYSTEM_PROMPTS: ReadonlySet<string> = new Set([SYSTEM_PROMPT, APEX_SUMMARY_PROMPT]);

/** A prompt the last check wouldn't send. The message names tokens, never the text it found. */
export class PromptRefused extends Error {
  readonly reasons: string[];
  constructor(reasons: string[]) {
    super(reasons.join(" "));
    this.name = "PromptRefused";
    this.reasons = reasons;
  }
}

/** Why a guarded prompt still can't go. Empty means it can. Fails closed. */
export function promptProblems(request: Pick<NarrativeRequest, "system" | "prompt">, pseudonyms: Pseudonymiser): string[] {
  const problems: string[] = [];
  if (!SYSTEM_PROMPTS.has(request.system)) problems.push("The system prompt isn't one of ours, so it wasn't sent.");
  try {
    for (const leak of findNames(request.prompt, pseudonyms)) {
      problems.push(leak.what === "email" ? "The prompt still held an email address after scrubbing, so it wasn't sent." : `The prompt still held part of the ${leak.what === "id" ? "id" : "name"} behind ${leak.token} after scrubbing, so it wasn't sent.`);
    }
  } catch (err) {
    problems.push(`The name check failed, so the prompt wasn't sent: ${err instanceof Error ? err.name : "unknown error"}.`);
  }
  return problems;
}

/**
 * The only way a prompt reaches Claude. It guards the prompt, then checks the
 * result independently, the way the number check treats a draft: anything left
 * of a known name stops the send, and the caller records it as a rejection.
 */
export async function sendPrompt(client: NarrativeClient, request: NarrativeRequest, pseudonyms: Pseudonymiser): Promise<NarrativeReply> {
  const guarded = { ...request, prompt: guardPrompt(request.prompt, pseudonyms) };
  const problems = promptProblems(guarded, pseudonyms);
  if (problems.length) throw new PromptRefused(problems);
  return client.complete(guarded);
}

/**
 * One small request through the guard, so the worker can say at startup
 * whether Claude will answer, rather than a run finding out hours later. It
 * holds nothing about a customer. The result says which model answered or why
 * the call failed, never a secret.
 */
export async function checkClaude(client: NarrativeClient, model: string): Promise<{ ok: boolean; detail: string }> {
  try {
    const reply = await sendPrompt(
      client,
      {
        model,
        system: SYSTEM_PROMPT,
        prompt: "This is a connection check, not a report section. Answer with ok set to true.",
        schema: { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"], additionalProperties: false },
        effort: "low",
        maxTokens: 1024,
      },
      new Pseudonymiser([]),
    );
    return { ok: true, detail: `${reply.model} answered` };
  } catch (err) {
    const status = typeof (err as { status?: unknown }).status === "number" ? ` (${(err as { status: number }).status})` : "";
    const message = err instanceof Error ? err.message.replace(/sk-ant-[A-Za-z0-9_-]+/g, "[removed]").slice(0, 300) : "an unexpected error";
    return { ok: false, detail: `${err instanceof Error ? err.name : "Error"}${status}: ${message}` };
  }
}
