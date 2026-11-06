import { z } from "zod";
import type { Facts } from "../extract/facts.ts";

// The facts step's line in the raw notes' assumptions (DECISIONS.md 290): how many
// things we checked about how the org is built, and what differed. A fact that
// differed has already made the checks that rest on it not measurable.

/** facts.json as read back from a run folder, which may come from another machine. */
export const FactsFile = z.object({
  version: z.number(),
  checkedAt: z.string(),
  apiVersion: z.string(),
  connectionKind: z.string(),
  calls: z.number(),
  facts: z.array(
    z.object({
      id: z.string(),
      question: z.string(),
      covers: z.enum(["whole", "part"]),
      says: z.string(),
      outcome: z.enum(["confirmed", "contradicted", "open", "not_checked"]),
      detail: z.string(),
      observed: z.record(z.string(), z.union([z.boolean(), z.string(), z.null()])),
      affects: z.array(z.string()),
    }),
  ),
});

export type FactsSummary = { checked: number; differed: Array<{ detail: string; affects: string[] }> };

export function summariseFacts(facts: Facts): FactsSummary {
  const checked = facts.facts.filter((f) => f.outcome !== "not_checked");
  return { checked: checked.length, differed: checked.filter((f) => f.outcome === "contradicted").map((f) => ({ detail: f.detail, affects: f.affects })) };
}

const things = (n: number): string => `${n} ${n === 1 ? "thing" : "things"}`;

/** The sentences, with each affected check named by its title where the run has one. */
export function factsText(summary: FactsSummary, titleOf: (ruleId: string) => string | undefined): string[] {
  if (summary.checked === 0) return [];
  if (summary.differed.length === 0) return [`We checked ${things(summary.checked)} about how this org is built, and each was as we expected.`];
  const lines = [`We checked ${things(summary.checked)} about how this org is built; ${summary.differed.length} differed from what we expected, and those checks say so.`];
  for (const d of summary.differed) {
    const titles = [...new Set(d.affects.map((id) => titleOf(id)).filter((t): t is string => Boolean(t)))];
    lines.push(`${d.detail} ${titles.length ? `${titles.length === 1 ? "The check that rests on it says so" : "The checks that rest on it say so"}: ${titles.map((t) => `"${t}"`).join(", ")}.` : "Nothing we report rests on it."}`);
  }
  return lines;
}
