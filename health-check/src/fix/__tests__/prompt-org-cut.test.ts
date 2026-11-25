import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { textSha256 } from "../../engine/org-words.ts";
import { FIX_PROMPT_MAX } from "../../schemas/index.ts";
import { assembleMarked, type PromptFinding } from "../prompt.ts";
import { FIX_TABLE } from "../table.ts";

// A summary too long for a fix prompt loses whole sentences from its end, and a
// cut is never made inside the org's words (DECISIONS.md 756): a stage or other
// value the org named that holds ". " is the org's, not a sentence end, so a cut
// there split it and left the prompt unknown, which held an automatic run.

const rule = FIX_TABLE["aut.hardcoded-ids-in-flows"]!.prompt;

/** A long summary with one org value at `at` (a sentence index), marked as the org's. */
function finding(orgValue: string, at: number): PromptFinding {
  const sentences = Array.from({ length: 40 }, (_, i) => `Sentence ${i + 1} of the summary says something.`);
  const before = sentences.slice(0, at).join(" ");
  const head = `${before}${before ? " " : ""}We read `;
  const summary = `${head}${orgValue} as open. ${sentences.slice(at).join(" ")}`;
  const spans = [{ start: head.length, end: head.length + orgValue.length }];
  return {
    title: "Flows hold hardcoded ids",
    summary,
    recommendation: "Replace the ids.",
    subjects: [],
    orgWords: { title: { spans: [], textSha256: textSha256("Flows hold hardcoded ids") }, summary: { spans, textSha256: textSha256(summary) }, recommendation: { spans: [], textSha256: textSha256("Replace the ids.") } },
  };
}

describe("a fix prompt's summary, cut to fit", () => {
  for (const name of ["No Decision - Cust. Unresponsive", "Qualified. We haven't agreed this. Do not use", "Lost. Gone to a competitor", "A.B. Corp. Renewals"]) {
    for (const at of [1, 3, 20]) {
      test(`${JSON.stringify(name)} at sentence ${at}: never cut inside, the prompt stays marked and within the limit`, () => {
        const m = assembleMarked(finding(name, at), rule);
        assert.ok(m.text.length <= FIX_PROMPT_MAX, `${m.text.length}`);
        assert.notEqual(m.spans, null, "the prompt is still known");
        // Whole or not at all: no part of the org's words is left without the rest.
        const first = name.split(". ")[0]!;
        if (m.text.includes(first)) assert.ok(m.text.includes(name), m.text.slice(0, 400));
        for (const s of m.spans!) assert.ok(name.includes(m.text.slice(s.start, s.end)), m.text.slice(s.start, s.end));
      });
    }
  }
});
