import type { NarrativeClient, NarrativeReply, NarrativeRequest } from "../narrative/client.ts";
import { STAGE_NAMES_SYSTEM_PROMPT } from "../narrative/stage-prompt.ts";

// The engine's stand-in for a model reading closed stages' names (DECISIONS.md
// 766): no key, no network. Every request is kept; a stage request, one stage
// each, is answered by `answer`, given the stage as the prompt sent it, and any
// other request (a report section's draft) fails as an unreachable Claude
// would, so the section keeps the engine's own words.

export type SentStage = { label: string; apiName: string };

/** The stage a stage prompt sent, read back from between its markers, or null. */
export function sentStage(request: Pick<NarrativeRequest, "prompt">): SentStage | null {
  const m = /<<<STAGE\n([\s\S]*)\nSTAGE>>>/.exec(request.prompt);
  return m ? (JSON.parse(m[1]!) as SentStage) : null;
}

/** A reply holding `reading`, or the raw text given. */
export const reply = (reading: unknown, stopReason: string | null = "end_turn"): NarrativeReply => ({ stopReason, text: typeof reading === "string" && !["lost", "no_decision", "unsure"].includes(reading) ? reading : JSON.stringify({ reading }), model: "stand-in" });

export type FakeStageModel = NarrativeClient & { requests: NarrativeRequest[]; stageRequests: () => NarrativeRequest[]; sent: () => SentStage[]; inFlight: number; mostAtOnce: number };

export function fakeStageModel(answer: (stage: SentStage, request: NarrativeRequest) => NarrativeReply | Promise<NarrativeReply>): FakeStageModel {
  const fake: FakeStageModel = {
    requests: [],
    stageRequests: () => fake.requests.filter((r) => r.system === STAGE_NAMES_SYSTEM_PROMPT),
    sent: () => fake.stageRequests().map((r) => sentStage(r)!),
    inFlight: 0,
    mostAtOnce: 0,
    async complete(request) {
      fake.requests.push(request);
      if (request.system !== STAGE_NAMES_SYSTEM_PROMPT) throw new Error("fetch failed: the stand-in only reads stages");
      fake.inFlight++;
      fake.mostAtOnce = Math.max(fake.mostAtOnce, fake.inFlight);
      try {
        return await answer(sentStage(request)!, request);
      } finally {
        fake.inFlight--;
      }
    },
  };
  return fake;
}

/** Answers every stage with one reading. */
export const answerAll = (reading: "lost" | "no_decision" | "unsure") => () => reply(reading);
