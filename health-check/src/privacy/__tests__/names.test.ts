import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { findNames, maskNames, registerSharedNames, SHARED_NAME_PREFIX } from "../names.ts";
import { Pseudonymiser, type PseudonymEntry } from "../pseudonymiser.ts";
import { guardPrompt } from "../../narrative/guard.ts";

// Synthetic people only. Ids are zero-padded and emails are at example.com.
const id = (n: number): string => `005${String(n).padStart(12, "0")}AAA`;

function people(...names: string[]): Pseudonymiser {
  const p = new Pseudonymiser();
  names.forEach((name, i) => p.label("user", id(i + 1), name, [`${name.toLowerCase().replace(/[^a-z]+/g, ".")}@example.com`]));
  return p;
}

const guard = (text: string, p: Pseudonymiser): string => guardPrompt(text, p);

describe("first names and other parts of a name", () => {
  const p = people("Sam Reed", "Dana Cole");

  test("a first name on its own gets the full name's token", () => {
    assert.equal(guard("Sam said the forecast is a guess", p), "[[user:U0001]] said the forecast is a guess");
  });
  test("so a restored report names the person in full", () => {
    assert.equal(p.restore(guard("Sam said the forecast is a guess", p)), "Sam Reed said the forecast is a guess");
  });
  test("a surname on its own", () => assert.equal(guard("Ask Cole.", p), "Ask [[user:U0002]]."));
  test("possessives, straight and curly", () => {
    assert.equal(guard("Sam's deals and Cole’s", p), "[[user:U0001]]'s deals and [[user:U0002]]’s");
  });
  test("any case, including the start of a sentence and capitals", () => {
    assert.equal(guard("sam agreed. SAM agreed. Sam agreed.", p), "[[user:U0001]] agreed. [[user:U0001]] agreed. [[user:U0001]] agreed.");
  });
  test("not inside a longer word", () => assert.equal(guard("Samsung and Colette", p), "Samsung and Colette"));
  test("names in quotes", () => assert.equal(guard(`"Sam" and 'Dana'`, p), `"[[user:U0001]]" and '[[user:U0002]]'`));
  test("names inside an API name", () => assert.equal(guard("Sam_Reed_Approval", p), "[[user:U0001]]_Approval"));
  test("an invisible character can't split a name", () => assert.equal(guard("S​am and Da­na", p), "[[user:U0001]] and [[user:U0002]]"));
  test("full-width letters fold to plain ones", () => assert.equal(guard("Ｓａｍ", p), "[[user:U0001]]"));
  test("the 15-character form of an id", () => assert.equal(guard(`/lightning/r/User/${id(1).slice(0, 15)}/view`, p), "/lightning/r/User/[[user:U0001]]/view"));
});

describe("hyphens, apostrophes, accents and particles", () => {
  const p = people("Mary-Jane O'Brien", "Zoë García-López", "Anna van der Berg");

  test("a hyphenated name whole, in parts and run together", () => {
    assert.equal(guard("Mary-Jane, Mary, Jane, MaryJane and Mary Jane", p), "[[user:U0001]], [[user:U0001]], [[user:U0001]], [[user:U0001]] and [[user:U0001]]");
  });
  test("an apostrophe name with or without its apostrophe", () => {
    assert.equal(guard("O'Brien, O’Brien, OBrien and Brien", p), "[[user:U0001]], [[user:U0001]], [[user:U0001]] and [[user:U0001]]");
  });
  test("accents written or left off, either way round", () => {
    assert.equal(guard("Zoe Garcia, Zoë, garcía, Lopez", p), "[[user:U0002]], [[user:U0002]], [[user:U0002]], [[user:U0002]]");
    const plain = people("Jose Lopez");
    assert.equal(guard("José López", plain), "[[user:U0001]]");
  });
  test("a decomposed accent goes with the name", () => {
    assert.equal(guard("Zoë said", p), "[[user:U0002]] said");
  });
  test("particles are masked inside a run, never alone", () => {
    assert.equal(guard("van der Berg will hire a van from de Vries", p), "[[user:U0003]] will hire a van from de Vries");
  });
});

describe("two people with the same first name", () => {
  const p = people("Sam Reed", "Sam Patel");

  test("the shared first name still goes, with a token of its own", () => {
    const out = guard("Sam said so", p);
    assert.doesNotMatch(out, /Sam/);
    assert.notEqual(out, "[[user:U0001]] said so");
    assert.notEqual(out, "[[user:U0002]] said so");
  });
  test("which restores to the first name as written, so nobody is misnamed", () => {
    assert.equal(p.restore(guard("Sam said so", p)), "Sam said so");
  });
  test("the surname still picks out the person", () => {
    assert.equal(p.restore(guard("Patel and reed", p)), "Sam Patel and Sam Reed");
  });
  test("the shared token is saved with the run and survives a reload", () => {
    const first = people("Sam Reed", "Sam Patel");
    registerSharedNames(first);
    const saved = JSON.parse(JSON.stringify(first.entries())) as PseudonymEntry[];
    assert.ok(saved.some((e) => e.id === `${SHARED_NAME_PREFIX}sam` && e.name === "Sam"));
    const reloaded = new Pseudonymiser(saved);
    assert.equal(guard("Sam said so", reloaded), guard("Sam said so", first));
    assert.equal(reloaded.entries().length, saved.length, "no new tokens on reload");
  });
  test("a shared token is a user token, so executive text still says a single person", () => {
    assert.match(guard("Sam said so", p), /^\[\[user:U\d{4}\]\]/);
  });
  test("a first name shared with someone else's surname is ambiguous too", () => {
    const q = people("Reed Taylor", "Sam Reed");
    assert.equal(q.restore(guard("Reed called", q)), "Reed called");
  });
});

describe("names that are also ordinary words", () => {
  const p = people("Will Price", "Rose May", "Mark Bell");

  test("are masked with a capital", () => {
    assert.equal(guard("Will, Rose and Mark agreed. Bell too.", p), "[[user:U0001]], [[user:U0002]] and [[user:U0003]] agreed. [[user:U0003]] too.");
  });
  test("are left in lower case", () => {
    const text = "You will mark the rose bush; it may ring a bell at a good price.";
    assert.equal(guard(text, p), text);
  });
  test("are over-scrubbed at the start of a sentence, by choice", () => {
    assert.equal(guard("May we start?", p), "[[user:U0002]] we start?");
  });
  test("a capitalised common word inside a hyphenated word is left", () => {
    const q = people("Ben Cross");
    assert.equal(guard("Cross-sell and up-Cross", q), "Cross-sell and up-Cross");
  });
  test("a shared-user name never damages ordinary text", () => {
    const q = people("Shared Sales Desk", "Integration User");
    const text = "Sales Cloud integration for the Shared desk team.";
    assert.equal(guard(text, q), text);
  });
});

describe("nicknames, where cheap", () => {
  test("a common diminutive and its formal name go both ways", () => {
    const p = people("Samuel Okafor", "Liz Grant");
    assert.equal(guard("Sam and Sammy met Elizabeth", p), "[[user:U0001]] and [[user:U0001]] met [[user:U0002]]");
  });
  test("someone's own first name beats another's nickname", () => {
    const p = people("Sam Reed", "Samuel Okafor");
    assert.equal(guard("Samuel called", p), "[[user:U0002]] called");
    assert.equal(guard("Sam called", p), "[[user:U0001]] called");
  });
  test("a mailbox name that isn't the name", () => {
    const p = new Pseudonymiser();
    p.label("user", id(1), "Alex Morgan", ["amorgan@example.com"]);
    assert.equal(guard("amorgan says hi", p), "[[user:U0001]] says hi");
  });
});

describe("companies", () => {
  test("the customer's name needs a capital, and generic company words are left alone", () => {
    const p = new Pseudonymiser();
    p.label("customer", "00D000000000001AAA", "Acme Holdings");
    assert.equal(guard("Acme said acme pricing and Holdings are fine", p), "[[customer:C0001]] said acme pricing and Holdings are fine");
  });
  test("an account known only by its id masks nothing", () => {
    const p = new Pseudonymiser();
    p.label("account", "001000000000001AAA", "001000000000001AAA");
    assert.equal(maskNames("AAA and 001", p), "AAA and 001");
  });
});

describe("findNames, the last check", () => {
  const p = people("Sam Reed", "Will Price");

  test("finds a name part the guard would have masked", () => {
    assert.deepEqual(findNames("Ask Sam", p), [{ token: "[[user:U0001]]", what: "name" }]);
  });
  test("finds a part in any case, inside an API name, or split by an invisible character", () => {
    for (const text of ["ask reed", "Flow_Reed_Approval", "R​eed"]) assert.equal(findNames(text, p).length, 1, text);
  });
  test("finds an email and an id", () => {
    assert.ok(findNames("x@example.org", p).some((l) => l.what === "email"));
    assert.ok(findNames(`owner ${id(1).slice(0, 15)}`, p).some((l) => l.what === "id"));
  });
  test("ignores tokens and lower-case common words", () => {
    assert.deepEqual(findNames("[[user:U0001]] will price it", p), []);
  });
  test("never quotes what it found", () => {
    assert.doesNotMatch(JSON.stringify(findNames("Sam Reed", p)), /Sam|Reed/);
  });
  test("is clean on everything the guard returns", () => {
    const text = "Sam's, REED, Will, Price-led, sam.reed@example.com, S​am, Ｓａｍ, Sam_Reed_Flow";
    assert.deepEqual(findNames(guard(text, p), p), []);
  });
});

describe("the golden cases shared with the console", () => {
  type Golden = { people: Array<{ kind: string; id: string; name: string; aliases: string[] }>; cases: Array<{ name: string; text: string; guarded: string; restored: string; flaggedBefore: boolean }> };
  const golden = JSON.parse(readFileSync(new URL("../../../fixtures/scrub-golden.json", import.meta.url), "utf8")) as Golden;
  const build = (): Pseudonymiser => {
    const p = new Pseudonymiser();
    for (const e of golden.people) p.label(e.kind, e.id, e.name, e.aliases);
    return p;
  };

  test("only synthetic people", () => {
    for (const e of golden.people) {
      assert.match(e.id, /^[0-9A-Za-z]{3}0{9,}\d+AAA$/, e.id);
      for (const alias of e.aliases) assert.match(alias, /@example\.com$/, alias);
    }
  });
  test("cover both flagged and clean text", () => {
    assert.ok(golden.cases.some((c) => c.flaggedBefore) && golden.cases.some((c) => !c.flaggedBefore));
  });
  for (const c of golden.cases) {
    test(c.name, () => {
      const p = build();
      const guarded = guardPrompt(c.text, p);
      assert.equal(guarded, c.guarded);
      assert.deepEqual(findNames(guarded, p), []);
      assert.equal(p.restore(guarded), c.restored);
      assert.equal(findNames(c.text, build()).length > 0, c.flaggedBefore);
    });
  }
});
