import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { checkNumbers } from "../check.ts";
import type { FigureSet } from "../dataset.ts";

// The console in blackcloud-next runs a copy of this number check. Both repos
// run every case in the same file, so a change here that the copy doesn't
// follow fails one side or the other. Update both files together.
type GoldenCase = { name: string; text: string; dataset: Partial<FigureSet>; expected: "pass" | "fail"; unmatched?: string[] };

const golden = JSON.parse(readFileSync(new URL("../../../fixtures/numbercheck-golden.json", import.meta.url), "utf8")) as { cases: GoldenCase[] };

describe("golden number check cases shared with the console", () => {
  test("cover passes and failures", () => {
    assert.ok(golden.cases.some((c) => c.expected === "pass") && golden.cases.some((c) => c.expected === "fail"));
  });

  for (const c of golden.cases) {
    test(c.name, () => {
      const outcome = checkNumbers(c.text, { figures: [], dates: [], identifiers: [], tokens: [], labels: [], ...c.dataset });
      assert.equal(outcome.passed ? "pass" : "fail", c.expected, JSON.stringify(outcome.unmatched));
      if (c.unmatched) assert.deepEqual(outcome.unmatched.map((u) => u.raw), c.unmatched);
    });
  }
});
