import type { NarrativeClient, NarrativeReply, NarrativeRequest } from "../narrative/client.ts";
import { STAGE_NAMES_SYSTEM_PROMPT } from "../narrative/stage-prompt.ts";

// The engine's stand-in for a model reading closed stages' names (DECISIONS.md
// 766): no key, no network. Every request is kept; a stage request is answered
// by `answer`, given the stages as the prompt sent them, and any other request
// (a report section's draft) fails as an unreachable Claude would, so the
// section keeps the engine's own words.

export type SentStage = { id: string; label: string; apiName: string };

/** The stages a stage prompt sent, read back from between its markers. */
export function sentStages(request: Pick<NarrativeRequest, "prompt">): SentStage[] {
  const m = /<<<STAGES\n([\s\S]*)\nSTAGES>>>/.exec(request.prompt);
  return m ? (JSON.parse(m[1]!) as SentStage[]) : [];
}

export const reply = (answers: unknown, stopReason: string | null = "end_turn"): NarrativeReply => ({ stopReason, text: typeof answers === "string" ? answers : JSON.stringify({ answers }), model: "stand-in" });

export function fakeStageModel(answer: (stages: SentStage[], request: NarrativeRequest) => NarrativeReply | Promise<NarrativeReply>): NarrativeClient & { requests: NarrativeRequest[]; stageRequests: () => NarrativeRequest[] } {
  const requests: NarrativeRequest[] = [];
  return {
    requests,
    stageRequests: () => requests.filter((r) => r.system === STAGE_NAMES_SYSTEM_PROMPT),
    async complete(request) {
      requests.push(request);
      if (request.system !== STAGE_NAMES_SYSTEM_PROMPT) throw new Error("fetch failed: the stand-in only reads stages");
      return answer(sentStages(request), request);
    },
  };
}

/** Answers every stage sent with one reading. */
export const answerAll = (reading: "lost" | "no_decision" | "unsure") => (stages: SentStage[]) => reply(stages.map((s) => ({ id: s.id, reading })));
