import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ApiBudget, BudgetStopError, DAILY_LIMIT_EXHAUSTED } from "../../salesforce/budget.ts";
import { CONSOLE_BUDGET_STOP_OPENINGS, consoleBudgetStopCause } from "../console-checks.ts";

// Every reason the engine writes to run.budget.stoppedReason opens as the
// console reads its cause (DECISIONS.md 715): a reason it read as the wrong
// cause would tell staff to wait when waiting can't help, or the other way.

/** The reason a budget throws, made by the budget itself. */
function stop(make: (b: ApiBudget) => void, options: ConstructorParameters<typeof ApiBudget>[0]): string {
  const budget = new ApiBudget(options);
  try {
    make(budget);
  } catch (err) {
    if (err instanceof BudgetStopError) return err.message;
    throw err;
  }
  throw new Error("The budget didn't stop");
}

const REASONS: Array<[string, string, "allowance" | "cap" | "unread"]> = [
  ["the org's usage at its share", stop((b) => (b.observe(7_400, 15_000), b.assertCanSpend(200)), { stopAtShare: 0.5 }), "allowance"],
  ["Salesforce says the daily limit is used up", DAILY_LIMIT_EXHAUSTED, "allowance"],
  ["the run's cap of calls", stop((b) => (b.observe(0, 1_000_000), b.recordCall(5_000), b.assertCanSpend()), { stopAtShare: 0.5, maxCallsPerRun: 5_000 }), "cap"],
  // DECISIONS.md 710: an automatic run's tenth of the org's daily limit. Run again stops at the same point, so it is a cap.
  ["an automatic run's tenth of the limit", stop((b) => (b.observe(0, 15_000), b.recordCall(1_500), b.assertCanSpend()), { stopAtShare: 0.5, maxShareOfLimit: 0.1 }), "cap"],
  ["the limit unread, at the small allowance", stop((b) => (b.limitUnread(), b.recordCall(1_000), b.assertCanSpend()), { stopAtShare: 0.5 }), "unread"],
  ["the limit not read yet", stop((b) => b.assertCanSpend(), { stopAtShare: 0.5 }), "unread"],
];

describe("each budget stop opens as the console reads its cause", () => {
  for (const [name, reason, cause] of REASONS) {
    test(`${name}: ${cause}`, () => {
      assert.equal(consoleBudgetStopCause(reason), cause, reason);
    });
  }

  test("the tenth of the limit says the share and the org's limit, after the cap's opening", () => {
    assert.equal(REASONS[3]![1], "This run reached its cap of 10% of the org's 15000 daily API calls");
  });

  const roots = [join(process.cwd(), "..", "..", "hc-console-merge"), join(process.cwd(), "..", "..", "worktrees", "hc-console-merge")];
  const root = roots.find((r) => existsSync(join(r, CONSOLE_BUDGET_STOP_OPENINGS.file)));
  test("the openings are the console's own patterns, word for word", { skip: root ? false : "the console's checkout isn't beside this one" }, () => {
    const text = readFileSync(join(root!, CONSOLE_BUDGET_STOP_OPENINGS.file), "utf8");
    const body = text.slice(text.indexOf("export function budgetStopCause"));
    for (const cause of ["allowance", "cap", "unread"] as const) {
      for (const pattern of CONSOLE_BUDGET_STOP_OPENINGS[cause]) {
        const line = body.split("\n").find((l) => l.includes(pattern));
        assert.ok(line, pattern);
        assert.ok(line.includes(`return "${cause}"`), `${pattern} → ${cause}`);
      }
    }
  });
});
