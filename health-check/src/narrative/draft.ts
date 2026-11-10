import { z } from "zod";
import { MODELS } from "../config.ts";
import { checkNumbers, scopeDataset, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { NarrativeClient } from "./client.ts";
import { guardDeep, holdsNameOrSecret, PromptRefused, sendPrompt } from "./guard.ts";
import { buildPrompt, DRAFT_SCHEMA, SYSTEM_PROMPT, type PromptSource } from "./prompt.ts";

// Claude explains and code calculates. A draft is only accepted if every
// paragraph's figures trace to the sources that paragraph cites. Anything else
// is rejected, and the section keeps the engine's own text.

const Draft = z.object({
  paragraphs: z.array(z.object({ text: z.string().min(1), sources: z.array(z.string()) })).min(1),
});

export type SectionBrief = {
  title: string;
  purpose: string;
  sources: PromptSource[];
  /** The most words the section's page has room for, said in the prompt and checked on the reply (DECISIONS.md 440). */
  maxWords: number;
  model?: string;
  /** The engine has only a placeholder for this section, so an accepted draft means it no longer needs a person's input. */
  fillsPlaceholder?: boolean;
  /** People appear by role only: a draft that keeps a person's token is refused (DECISIONS.md 184). */
  rolesOnly?: boolean;
};

export type DraftResult =
  | { status: "accepted"; body: string; model: string; attempts: number; words: number }
  | { status: "rejected"; reasons: string[]; attempts: number; unavailable?: true };

export const DRAFT_DEFAULTS = {
  effort: "high",
  maxTokens: 16_000,
  // One redraft with the failures listed. After that a person is better value.
  maxAttempts: 2,
} as const;

/**
 * How far over its target a draft may run before it's sent back. A quarter, so
 * a draft a sentence over isn't asked for again: the carry-over step protects
 * the page from what gets through (DECISIONS.md 440).
 */
export const LENGTH_TOLERANCE = 1.25;

/** Words as a reader counts them: every run of characters between spaces. */
export const wordCount = (paragraphs: string[]): number => paragraphs.join(" ").split(/\s+/).filter(Boolean).length;

function styleProblems(text: string): string[] {
  return /\p{Extended_Pictographic}/u.test(text.replace(/[©®™]/g, "")) ? ["Remove the emoji."] : [];
}

const PERSON_TOKEN = /\[\[(?:user|person|contact):[A-Z]\d{4}\]\]/;

function review(text: string, set: FigureSet, allowed: Set<string>, pseudonyms: Pseudonymiser, rolesOnly = false): { paragraphs: string[]; problems: string[] } {
  let parsed: z.infer<typeof Draft>;
  try {
    parsed = Draft.parse(JSON.parse(text));
  } catch {
    return { paragraphs: [], problems: ["The reply wasn't valid JSON in the required shape."] };
  }
  const problems: string[] = [];
  for (const paragraph of parsed.paragraphs) {
    const unknown = paragraph.sources.filter((id) => !allowed.has(id));
    if (unknown.length) problems.push(`Cited sources that aren't in this section: ${unknown.join(", ")}.`);
    const scope = paragraph.sources.filter((id) => allowed.has(id));
    const outcome = checkNumbers(paragraph.text, scopeDataset(set, scope));
    for (const miss of outcome.unmatched) problems.push(`"${miss.raw}": ${miss.reason}.`);
    problems.push(...styleProblems(paragraph.text));
    if (holdsNameOrSecret(paragraph.text, pseudonyms)) problems.push("A paragraph holds a person's name or a secret.");
    if (rolesOnly && PERSON_TOKEN.test(paragraph.text)) problems.push("Refer to people by their role, not by a token such as [[user:U0001]].");
  }
  return { paragraphs: parsed.paragraphs.map((p) => p.text), problems };
}

export async function draftSection(input: { brief: SectionBrief; dataset: FigureSet; client: NarrativeClient; pseudonyms: Pseudonymiser }): Promise<DraftResult> {
  const { brief, dataset, client, pseudonyms } = input;
  const allowed = new Set(brief.sources.map((s) => s.id));
  const sources = guardDeep(brief.sources, pseudonyms);
  let feedback: string[] = [];
  let reasons: string[] = [];
  // The last draft whose only fault was its length. If nothing better comes,
  // it's kept: a long draft is better than none (DECISIONS.md 440).
  let long: DraftResult | undefined;

  for (let attempt = 1; attempt <= DRAFT_DEFAULTS.maxAttempts; attempt++) {
    const prompt = buildPrompt({ ...brief, sources, feedback });
    let reply;
    try {
      // sendPrompt guards the whole prompt again and refuses it if a name survives.
      reply = await sendPrompt(client, {
        model: brief.model ?? MODELS.synthesis,
        system: SYSTEM_PROMPT,
        prompt,
        schema: DRAFT_SCHEMA,
        effort: DRAFT_DEFAULTS.effort,
        maxTokens: DRAFT_DEFAULTS.maxTokens,
      }, pseudonyms);
    } catch (err) {
      if (err instanceof PromptRefused) return long ?? { status: "rejected", reasons: err.reasons, attempts: attempt };
      // Nothing is wrong with the section: Claude couldn't be reached.
      return long ?? { status: "rejected", reasons: [`The call to Claude failed: ${err instanceof Error ? err.message : String(err)}`], attempts: attempt, unavailable: true };
    }
    // Neither of these gets better by asking again with the same input.
    if (reply.stopReason === "refusal") return long ?? { status: "rejected", reasons: ["Claude declined to write this section."], attempts: attempt };
    if (reply.stopReason === "max_tokens") return long ?? { status: "rejected", reasons: ["The reply was cut off, so it can't be trusted."], attempts: attempt };

    const { paragraphs, problems } = review(reply.text, dataset, allowed, pseudonyms, brief.rolesOnly);
    if (problems.length === 0) {
      const words = wordCount(paragraphs);
      const accepted: DraftResult = { status: "accepted", body: paragraphs.join("\n\n"), model: reply.model, attempts: attempt, words };
      if (words <= brief.maxWords * LENGTH_TOLERANCE) return accepted;
      // Code never cuts Claude's words: it asks again.
      long = accepted;
      feedback = [`Your draft is ${words} words; the limit is ${brief.maxWords}. Cut it to ${brief.maxWords} words or fewer, keeping every figure as it is.`];
      continue;
    }
    reasons = problems;
    feedback = problems;
  }
  return long ?? { status: "rejected", reasons, attempts: DRAFT_DEFAULTS.maxAttempts };
}
