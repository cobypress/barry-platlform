import { test } from "node:test";
import assert from "node:assert/strict";
import { org, orgWordsOf, orgWordsStore, said, withOrgWords } from "../org-words.ts";

// The process's store of org-worded sentences, once full (DECISIONS.md 663):
// marks fall back to unknown, as decision 655 says, and the worker is told
// once, with counts and never org text. Its own file, since a full store
// stays full for the rest of the process.

test("a full store says so once, with no org text, and every mark after it is unknown", (t) => {
  const warned = t.mock.method(console, "warn", () => {});
  const { limit } = orgWordsStore();
  for (let i = 0; i <= limit + 5; i++) said`Stage ${org(`Secret stage ${i}`)} is held`;
  assert.deepEqual(orgWordsStore(), { size: limit, limit, full: true });
  assert.equal(warned.mock.callCount(), 1);
  const line = String(warned.mock.calls[0]!.arguments[0]);
  assert.match(line, /store is full .*until the worker restarts/);
  assert.ok(!line.includes("Secret"));
  // A sentence quoting one the open ledger doesn't hold is unknown, never the engine's.
  withOrgWords(() => {
    const outer = said`Before: ${"literal words"}`;
    assert.equal(orgWordsOf(outer), null);
  });
});
