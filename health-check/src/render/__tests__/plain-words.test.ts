import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { plainWords, ukDates } from "../format.ts";

// Engine sentences as a customer reads them (night 3 reading pass, DECISIONS.md 328).

const say = (text: string): string => plainWords(ukDates(text));

describe("the calibration, in sentences the generic swap used to break", () => {
  test("each reads as English", () => {
    assert.equal(say("The expected stages are the ones the calibration marks as required: Legal Review"), "The expected stages are the ones you told us are required: Legal Review");
    assert.equal(say("4 of them at 10 minutes a year each, agreed in the calibration"), "4 of them at 10 minutes a year each, as agreed with you");
    assert.equal(say("The calibration has no price for a seat of: acmecpq."), "We have no agreed price for a seat of acmecpq.");
    assert.equal(say("Marked as an integration in the calibration"), "Marked by you as integrations");
    for (const text of ["the ones the calibration marks as required", "agreed in the calibration", "The calibration has no price"]) assert.doesNotMatch(say(text), /what we agreed with you (marks|has)|in what we agreed/);
  });
});

describe("engine words and formats", () => {
  test("guards are checks, and Health Check is Salesforce's tool", () => {
    assert.equal(say("Every guard passed: no owned records."), "Every check passed: no owned records.");
    assert.equal(say("These seats can't be counted as savings until their guards pass."), "These seats can't be counted as savings until every check on them passes.");
    assert.equal(say("Your Health Check score is 62. We use 80 as a working target."), "Salesforce's own security tool, Health Check, scores your settings at 62. We use 80 as a working target.");
  });

  test("months, seat prices, one deal, and a campaign shortfall read as a person writes them", () => {
    assert.equal(say("from 8 in 2026-02 to 5 in 2026-09"), "from 8 in Feb 2026 to 5 in Sep 2026");
    assert.equal(say("a run on 2026-02-03 and run hc-2026-10-31-ab12cd34"), "a run on 3 Feb 2026 and run hc-2026-10-31-ab12cd34");
    assert.equal(say("15 Salesforce seats at 1200 GBP a year, from the calibration"), "15 Salesforce seats at £1,200 a year, from what we agreed with you");
    assert.equal(say("1 deals affected"), "1 deal affected");
    assert.equal(say("12 deals affected"), "12 deals affected");
    assert.equal(say("£315,000 campaign spend to date"), "£315,000 more spent than won back, to date");
    assert.equal(say("2 of the 21 affected deals have no amount. We don't impute one"), "2 of the 21 affected deals have no amount. We don't estimate one");
  });
});
