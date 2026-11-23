import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { EDITION_PLANS, LIST_PRICE_CURRENCY, LIST_PRICE_MAX_AGE_DAYS, LIST_PRICE_SOURCES, LIST_PRICES, listPrice, listPriceIsStale, UNMAPPED_EDITIONS } from "../list-prices.ts";
import { costPerSeat, isPriced, type SeatPricing } from "../seat-cost.ts";

// The edition price map and the list prices behind it (DECISIONS.md 679).

const DATE = /^\d{4}-\d{2}-\d{2}$/;
const NOW = new Date("2026-11-30T00:00:00Z");
const listOnly = (edition: string | null, extra: Partial<SeatPricing> = {}): SeatPricing => ({ calibration: { licenceCosts: {}, currency: "GBP" }, edition, now: NOW, ...extra });

// Published on Salesforce's UK pricing page, read on 2026-11-23: a user a month, times 12.
const YEARLY: Record<string, number> = { "Starter Suite": 240, "Pro Suite": 960, Core: 1872, Advanced: 3792, Max: 5280 };

describe("the edition map (point 2)", () => {
  test("every mapped edition names a plan we hold a published price for", () => {
    for (const [edition, plan] of Object.entries(EDITION_PLANS)) assert.ok(LIST_PRICES.some((p) => p.plan === plan), `${edition} maps to ${plan}, which has no price`);
  });

  test("no edition is both mapped and left unmapped on purpose", () => {
    for (const edition of Object.keys(UNMAPPED_EDITIONS)) assert.ok(!Object.hasOwn(EDITION_PLANS, edition), `${edition} is mapped and listed as unmapped`);
  });

  test("each mapped edition and licence type gives its published price a year", () => {
    for (const [edition, plan] of Object.entries(EDITION_PLANS)) {
      for (const price of LIST_PRICES.filter((p) => p.plan === plan)) {
        const result = listPrice(price.licence, edition, price.currency);
        assert.ok(result.status === "priced" && result.annual === price.monthly * 12, `${edition}, ${price.licence}`);
      }
    }
  });

  test("an edition left unmapped gives the reason, never a figure", () => {
    assert.ok(Object.keys(UNMAPPED_EDITIONS).length > 0);
    for (const [edition, why] of Object.entries(UNMAPPED_EDITIONS)) {
      assert.ok(why.length > 0, edition);
      const result = costPerSeat("Salesforce", listOnly(edition));
      assert.ok(!isPriced(result), edition);
      assert.equal("reason" in result ? result.reason : "", `The calibration has no cost for it, and there's no list price: We haven't confirmed which of Salesforce's current plans ${edition} is priced as.`);
    }
  });

  test("an edition we've never heard of gives the reason too", () => {
    assert.equal(listPrice("Salesforce", "constructor", "GBP").status, "unpriced");
    assert.equal(listPrice("Salesforce", "Some New Edition", "GBP").status, "unpriced");
  });
});

describe("the dated source table (point 3)", () => {
  test("every source is a Salesforce UK page with the date we read it", () => {
    assert.ok(LIST_PRICE_SOURCES.length > 0);
    for (const s of LIST_PRICE_SOURCES) {
      assert.match(s.url, /^https:\/\/www\.salesforce\.com\/uk\//);
      assert.match(s.read, DATE, s.url);
      assert.ok(!Number.isNaN(Date.parse(s.read)), s.url);
    }
  });

  test("every price names one of those sources and carries its date", () => {
    for (const p of LIST_PRICES) {
      const source = LIST_PRICE_SOURCES.find((s) => s.url === p.url);
      assert.ok(source, `${p.plan} has no source`);
      assert.match(p.checked, DATE, p.plan);
      assert.equal(p.checked, source.read, p.plan);
      assert.equal(p.currency, LIST_PRICE_CURRENCY, p.plan);
    }
  });

  test(`a price read more than ${LIST_PRICE_MAX_AGE_DAYS} days before the check says so on the figure`, () => {
    const plans = { "Test Edition": "Core" };
    const fresh = costPerSeat("Salesforce", listOnly("Test Edition", { plans, now: new Date("2027-02-20T00:00:00Z") }));
    const stale = costPerSeat("Salesforce", listOnly("Test Edition", { plans, now: new Date("2027-02-22T00:00:00Z") }));
    assert.ok(isPriced(fresh) && isPriced(stale));
    assert.doesNotMatch(fresh.note, /more than/);
    assert.match(stale.note, /read on 2026-11-23\)\. We read that price more than 90 days before this check, so Salesforce may have changed it since$/);
    assert.equal(stale.annual, fresh.annual);
  });

  test("a price with no date, or one we can't read, counts as old", () => {
    assert.equal(listPriceIsStale("", NOW), true);
    assert.equal(listPriceIsStale("not a date", NOW), true);
    assert.equal(listPriceIsStale("2026-11-23", NOW), false);
  });

  test("nothing is fetched at run time", () => {
    for (const file of ["list-prices.ts", "seat-cost.ts"]) {
      const text = readFileSync(join(import.meta.dirname, "..", file), "utf8");
      assert.doesNotMatch(text, /\bfetch\(|node:https?|from "undici"/, file);
    }
  });
});

describe("the wording (point 4)", () => {
  test("a list price figure says it is Salesforce's published UK list price in pounds, before any discount, and when we read it", () => {
    const p = costPerSeat("Salesforce", listOnly("Test Edition", { plans: { "Test Edition": "Core" } }));
    assert.ok(isPriced(p) && p.basis === "list_price");
    assert.equal(
      p.note,
      "1872 GBP a year at Salesforce's published UK list price in pounds, before any discount you've negotiated, for Core (156 GBP a user a month, billed annually; https://www.salesforce.com/uk/sales/pricing/, read on 2026-11-23)",
    );
    assert.doesNotMatch(p.note, /—|–/);
  });

  test("the calibration's own figure isn't called a list price", () => {
    const p = costPerSeat("Salesforce", listOnly("Test Edition", { plans: { "Test Edition": "Core" }, calibration: { licenceCosts: { Salesforce: 1000 }, currency: "GBP" } }));
    assert.ok(isPriced(p) && p.basis === "calibration");
    assert.doesNotMatch(p.note, /list price/);
  });
});

describe("value over the map (point 5)", () => {
  test("each plan and licence gives 12 times its published monthly price, in pounds, as a whole number", () => {
    for (const price of LIST_PRICES) {
      const p = costPerSeat(price.licence, listOnly("Test Edition", { plans: { "Test Edition": price.plan } }));
      assert.ok(isPriced(p), price.plan);
      assert.equal(p.annual, YEARLY[price.plan], price.plan);
      assert.ok(Number.isInteger(p.annual), price.plan);
      assert.equal(p.currency, "GBP");
    }
  });

  test("a licence type with no published price gives the reason, not a figure", () => {
    const p = costPerSeat("Salesforce Platform", listOnly("Test Edition", { plans: { "Test Edition": "Core" } }));
    assert.ok(!isPriced(p));
    assert.match(p.reason, /doesn't publish a GBP list price for a Salesforce Platform seat on Core/);
  });

  test("a check in another currency gets no list price: we don't convert pounds", () => {
    for (const currency of ["USD", "EUR"]) {
      const p = costPerSeat("Salesforce", listOnly("Test Edition", { plans: { "Test Edition": "Core" }, calibration: { licenceCosts: {}, currency } }));
      assert.ok(!isPriced(p), currency);
      assert.equal(p.reason, `The calibration has no cost for it, and there's no list price: We hold only Salesforce's UK list prices, in pounds, and this check reports in ${currency}, so we don't convert them.`);
    }
  });

  test("the edition unread gives the reason, not a figure", () => {
    const p = costPerSeat("Salesforce", listOnly(null, { plans: { "Test Edition": "Core" } }));
    assert.ok(!isPriced(p));
    assert.match(p.reason, /edition couldn't be read/);
  });
});
