import { z } from "zod";
import type { CodeLintData } from "../code-lint.ts";
import { PMD_RULES } from "./pmd.ts";

// Every PMD rule's result for the whole org, kept as counts (DECISIONS.md 673):
// the owner's rule is that most things are stored so we can benchmark on them,
// and only a chosen few are shown. Counts only: no class, trigger, line, source
// or message, so the file outlives the checkpoints, as complexity.json does.

export const LINT_TOTALS_FILE = "lint-totals.json";

const Count = z.number().int().nonnegative();

/** One rule across the org: findings, those the code marked to be ignored, and the classes and triggers with at least one finding. */
export const RuleTotal = z.strictObject({ found: Count, suppressed: Count, components: Count });
export type RuleTotal = z.infer<typeof RuleTotal>;

export const LintTotals = z.strictObject({
  version: z.literal(1),
  pmd: z.discriminatedUnion("status", [
    z.strictObject({
      status: z.literal("ran"),
      toolVersion: z.string().regex(/^\d+\.\d+\.\d+$/),
      /** What PMD read: files it couldn't parse, too large or unreadable aren't counted. */
      classes: Count,
      triggers: Count,
      lines: Count,
      /** Every rule in our ruleset, whether or not it fired. */
      rules: z.record(z.string().regex(/^[A-Za-z]+$/), RuleTotal),
    }),
    // No reason: one can quote Salesforce, and the benchmark needs only that it wasn't measured.
    z.strictObject({ status: z.literal("not_run") }),
  ]),
});
export type LintTotals = z.infer<typeof LintTotals>;

/** The run's totals from its code-lint step. A step from before the tallies were kept (version 1) can't be counted, so reads as not run. */
export function lintTotals(data: CodeLintData): LintTotals {
  const pmd = data.pmd;
  if (pmd.status !== "ran" || !pmd.tally || !pmd.read) return LintTotals.parse({ version: 1, pmd: { status: "not_run" } });
  const rules: Record<string, RuleTotal> = {};
  for (const rule of Object.keys(PMD_RULES)) {
    const mine = pmd.tally.filter((t) => t.rule === rule);
    rules[rule] = {
      found: mine.reduce((sum, t) => sum + t.found, 0),
      suppressed: mine.reduce((sum, t) => sum + t.suppressed, 0),
      components: mine.filter((t) => t.found > 0).length,
    };
  }
  // Parsed, so nothing but the counts above can ride along.
  return LintTotals.parse({ version: 1, pmd: { status: "ran", toolVersion: pmd.toolVersion, classes: pmd.read.classes, triggers: pmd.read.triggers, lines: pmd.read.lines, rules } });
}
