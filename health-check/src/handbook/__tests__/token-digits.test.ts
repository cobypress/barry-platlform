import { test } from "node:test";
import assert from "node:assert/strict";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import { UserToken } from "../schema.ts";

// Decision 464: the handbook's user token is built from the pseudonymiser's own
// pattern, and still takes exactly what /^\[\[user:U\d{4,}\]\]$/ took.
test("a change's user token takes the 10,000th user and nothing it didn't take before", () => {
  const p = new Pseudonymiser();
  for (let n = 1; n < 10_000; n++) p.label("user", `filler-${String(n).padStart(5, "0")}`, "Sales");
  const nth = p.label("user", "005000000000001AAA", "Ana Okafor");
  assert.equal(nth, "[[user:U10000]]");
  for (const ok of [nth, "[[user:U0001]]", "[[user:U9999]]", "[[user:U123456]]"]) assert.ok(UserToken.safeParse(ok).success, ok);
  const refused = ["[[user:U001]]", "[[user:X0001]]", "[[person:U0001]]", "[[contact:C10000]]", "x[[user:U0001]]", "[[user:U0001]]x", "[[user:U0001]] ", "[[user:U10000]][[user:U0001]]", "[[user:U]]", "Ana Okafor"];
  for (const bad of refused) assert.ok(!UserToken.safeParse(bad).success, bad);
});
