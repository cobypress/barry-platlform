import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { READ_NEEDS } from "../../extract/org-profile.ts";
import { refusalSpans } from "../../salesforce/refusal-words.ts";
import { metricMarks, passedPartsHold } from "../automatic-words.ts";
import { org, orgWordsOf, passedOn, passedRangesOf, said, saidMade, withOrgWords } from "../org-words.ts";

// A reason passed on inside a sentence said makes (DECISIONS.md 743), held to
// decision 693: a text equal to a value the org chose is never ours, by this
// route either. passedOn records nothing alone; the sentence keeps where each
// reason sits, and once the run's values are known a reason equal to one, whole
// or after a head, leaves the sentence unknown.

const OURS = "The org reported no limits.";

describe("a reason passed on inside a sentence of ours", () => {
  test("is known by the passed-on route, its object marked, while no value of the org's equals it", () => {
    withOrgWords(() => {
      const s = said`Field usage on ${org("Acc")} couldn't be read: ${passedOn("Foo__c couldn't be read, because Salesforce refused this read.")}`;
      assert.deepEqual(orgWordsOf(s)?.map((x) => s.slice(x.start, x.end)), ["Acc", "Foo__c"]);
      assert.ok(metricMarks(s, new Set(["Something else"])));
    });
  });

  test("is never vouched for alone: passedOn records nothing, so the reason by itself isn't one said made", () => {
    withOrgWords(() => {
      said`Outer: ${passedOn(OURS)}`;
      assert.equal(saidMade(OURS), false);
      assert.equal(metricMarks(OURS, new Set([OURS])), null);
    });
  });

  test("leaves the sentence unknown where the org chose a value equal to the reason, whole or after a head", () => {
    withOrgWords(() => {
      const whole = said`Outer: ${passedOn(OURS)}`;
      assert.deepEqual(passedRangesOf(whole), [{ start: "Outer: ".length, end: whole.length }]);
      assert.ok(metricMarks(whole, new Set()));
      assert.equal(metricMarks(whole, new Set([OURS])), null);
      assert.equal(passedPartsHold(whole, new Set([OURS])), false);
      const headed = said`Outer: ${passedOn(`Users couldn't be read: ${OURS}`)}`;
      assert.equal(metricMarks(headed, new Set([OURS])), null, "the tail after a head is the org's value");
      // And a reason passed on twice: the inner sentence passed on again keeps where its reason sits.
      const inner = said`Inner: ${passedOn(OURS)}`;
      const twice = said`Outer3: ${passedOn(inner)}`;
      assert.ok(metricMarks(twice, new Set()));
      assert.equal(metricMarks(twice, new Set([OURS])), null, "a reason passed on twice is held to the org's values");
      // And through a sentence quoted inside another.
      const quoted = said`Again: ${whole}`;
      assert.equal(metricMarks(quoted, new Set([OURS])), null);
    });
  });
});

test("every sentence a read the edition keeps from us stands for is the recogniser's own (refusalReason gives it, DECISIONS.md 743)", () => {
  for (const need of READ_NEEDS) assert.deepEqual(refusalSpans(need.sentence), [], need.sentence);
});
