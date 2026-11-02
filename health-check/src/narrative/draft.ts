import { z } from "zod";
import { MODELS } from "../config.ts";
import { checkNumbers, scopeDataset, type FigureSet } from "../numbercheck/index.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import type { NarrativeClient } from "./client.ts";
import { guardDeep, guardPrompt } from "./guard.ts";
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
  model?: string;
};

export type DraftResult =
  | { status: "accepted"; body: string; model: string; attempts: number }
  | { status: "rejected"; reasons: string[]; attempts: number };

export const DRAFT_DEFAULTS = {
  effort: "high",
  maxTokens: 16_000,
  // One redraft with the failures listed. After that a person is better value.
  maxAttempts: 2,
} as const;

function styleProblems(text: string): string[] {
  return /\p{Extended_Pictographic}/u.test(text.replace(/[©®™]/g, "")) ? ["Remove the emoji."] : [];
}

function review(text: string, set: FigureSet, allowed: Set<string>, pseudonyms: Pseudonymiser): { paragraphs: string[]; problems: string[] } {
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
    if (guardPrompt(paragraph.text, pseudonyms) !== paragraph.text) problems.push("A paragraph holds a person's name or a secret.");
  }
  return { paragraphs: parsed.paragraphs.map((p) => p.text), problems };
}

export async function draftSection(input: { brief: SectionBrief; dataset: FigureSet; client: NarrativeClient; pseudonyms: Pseudonymiser }): Promise<DraftResult> {
  const { brief, dataset, client, pseudonyms } = input;
  const allowed = new Set(brief.sources.map((s) => s.id));
  const sources = guardDeep(brief.sources, pseudonyms);
  let feedback: string[] = [];
  let reasons: string[] = [];

  for (let attempt = 1; attempt <= DRAFT_DEFAULTS.maxAttempts; attempt++) {
    const prompt = guardPrompt(buildPrompt({ ...brief, sources, feedback }), pseudonyms);
    let reply;
    try {
      reply = await client.complete({
        model: brief.model ?? MODELS.synthesis,
        system: SYSTEM_PROMPT,
        prompt,
        schema: DRAFT_SCHEMA,
        effort: DRAFT_DEFAULTS.effort,
        maxTokens: DRAFT_DEFAULTS.maxTokens,
      });
    } catch (err) {
      return { status: "rejected", reasons: [`The call to Claude failed: ${err instanceof Error ? err.message : String(err)}`], attempts: attempt };
    }
    // Neither of these gets better by asking again with the same input.
    if (reply.stopReason === "refusal") return { status: "rejected", reasons: ["Claude declined to write this section."], attempts: attempt };
    if (reply.stopReason === "max_tokens") return { status: "rejected", reasons: ["The reply was cut off, so it can't be trusted."], attempts: attempt };

    const { paragraphs, problems } = review(reply.text, dataset, allowed, pseudonyms);
    if (problems.length === 0) return { status: "accepted", body: paragraphs.join("\n\n"), model: reply.model, attempts: attempt };
    reasons = problems;
    feedback = problems;
  }
  return { status: "rejected", reasons, attempts: DRAFT_DEFAULTS.maxAttempts };
}
