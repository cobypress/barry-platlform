import { test } from "node:test";
import assert from "node:assert/strict";
import { money } from "../../render/format.ts";
import { org, said, withOrgWords, orgWordsOf } from "../org-words.ts";
import { plural } from "../rule.ts";
import { andList, grouped, moneySaid, seatsSaid } from "../said-forms.ts";

// The said forms print what the plain forms beside them print (DECISIONS.md
// 657), so converting a rule to them moves no character.

test("grouped and moneySaid print what toLocaleString and money print", () => {
  for (const n of [0, 7, 999, 1000, 17880.5, 250000, 1234567.891, -4200]) assert.equal(grouped(n), n.toLocaleString("en-GB"));
  for (const c of ["GBP", "USD", "EUR", "AUD"]) for (const n of [0, 12.5, 1999.5, 1234567]) assert.equal(moneySaid(n, c), money(n, c));
});

test("plural prints as before", () => {
  assert.equal(plural(1, "stage"), "1 stage");
  assert.equal(plural(2, "stage"), "2 stages");
  assert.equal(plural(2, "entry", "entries"), "2 entries");
  const noun: string = "flow";
  assert.equal(plural(3, noun), "3 flows");
});

test("andList joins as the plain lists do and keeps each part's marks", () => {
  withOrgWords(() => {
    const plain = (items: string[]): string => (items.length <= 1 ? (items[0] ?? "") : `${items.slice(0, -1).join(", ")} and ${items.at(-1)}`);
    for (const items of [[], ["Alpha"], ["Alpha", "Beta"], ["Alpha", "Beta", "Gamma"]]) {
      const list = andList(items.map((i) => said`${org(i)}`));
      assert.equal(list, plain(items));
      assert.deepEqual(orgWordsOf(list), items.length === 0 ? [] : items.map((i) => ({ start: list.indexOf(i), end: list.indexOf(i) + i.length })));
    }
    // An unknown part makes the list unknown.
    assert.equal(orgWordsOf(andList([said`${org("A")}`, "B" as never])), null);
  });
});

test("an unknown currency is the org's words", () => {
  withOrgWords(() => {
    const text = moneySaid(1200, "AUD");
    assert.deepEqual(orgWordsOf(text), [{ start: 0, end: 3 }]);
    assert.deepEqual(orgWordsOf(moneySaid(1200, "GBP")), []);
  });
});

test("seatsSaid prints as plural over the seat's name, and marks the name as the org's (decision 663)", () => {
  withOrgWords(() => {
    for (const n of [0, 1, 2, 15]) assert.equal(seatsSaid(n, "Sales Cloud"), plural(n, `${"Sales Cloud"} seat`));
    assert.deepEqual(orgWordsOf(seatsSaid(15, "Sales Cloud")), [{ start: 3, end: 14 }]);
  });
});
