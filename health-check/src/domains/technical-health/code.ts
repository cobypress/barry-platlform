import type { ApexClassRow, ApexTriggerRow } from "../../extract/automation.ts";

// What technical health's own code checks read from the apex step's scan, in
// one place, so PMD's findings are left out exactly where those checks already
// report the same thing (DECISIONS.md 673).

export type Code = (ApexClassRow | ApexTriggerRow) & { kind: "apex class" | "apex trigger" };

/** Active code of the org's own that isn't a test, with what the scan found. Hidden bodies are counted apart. */
export function liveCode(classes: ApexClassRow[], triggers: ApexTriggerRow[]): { code: Code[]; hidden: number } {
  const all: Code[] = [...classes.map((c) => ({ ...c, kind: "apex class" as const })), ...triggers.map((t) => ({ ...t, kind: "apex trigger" as const }))].filter((c) => c.Status === "Active");
  return { code: all.filter((c) => c.scan !== null && !c.isTest), hidden: all.filter((c) => c.scan === null).length };
}

/**
 * The test classes the test-quality check reads, and those it names: with no
 * assertion, and reading the org's data. PMD's findings in the same classes
 * aren't shown again (DECISIONS.md 673), so both read them here.
 */
export function judgedTests(classes: ApexClassRow[]): { tests: ApexClassRow[]; silent: ApexClassRow[]; seeAll: ApexClassRow[] } {
  // A test data factory is @isTest with no test methods, and has nothing to assert.
  const tests = classes.filter((c) => c.Status === "Active" && c.isTest && c.scan !== null && c.scan.testMethods > 0);
  return { tests, silent: tests.filter((c) => c.scan!.asserts === 0), seeAll: tests.filter((c) => c.scan!.seeAllData) };
}

/** Code the limit-risk check names: a query or save inside a loop. PMD's loop findings in it aren't shown again (DECISIONS.md 673). */
export const loopsOverLimits = (c: Code): boolean => c.scan!.queriesInLoops + c.scan!.dmlInLoops > 0;

