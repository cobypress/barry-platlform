import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { Pseudonymiser } from "../pseudonymiser.ts";
import { redactSecrets } from "../secrets.ts";

const people = (): Pseudonymiser => {
  const p = new Pseudonymiser();
  p.label("user", "005000000000001AAA", "Alex Morgan", ["alex.morgan@example.com", "amorgan@example.org"]);
  p.label("user", "005000000000002AAA", "Sales", ["sales@example.com"]);
  return p;
};

describe("Pseudonymiser", () => {
  test("gives one stable token per record", () => {
    const p = people();
    assert.equal(p.label("user", "005000000000001AAA", "Alex Morgan"), "[[user:U0001]]");
    assert.equal(p.label("user", "005000000000002AAA", "Sales"), "[[user:U0002]]");
    assert.equal(p.label("account", "001000000000001AAA", "Acme Ltd"), "[[account:A0001]]");
  });

  test("scrubs a full name, in any case", () => {
    assert.equal(people().scrub("Spoke to alex morgan today"), "Spoke to [[user:U0001]] today");
  });
  test("scrubs usernames and ids", () => {
    assert.equal(people().scrub("amorgan@example.org and 005000000000001AAA"), "[[user:U0001]] and [[user:U0001]]");
  });
  test("scrubs any other email, known or not", () => {
    assert.equal(people().scrub("from jo@elsewhere.example"), "from [[email]]");
  });
  test("consultant chat goes through the same path", () => {
    const text = "Alex Morgan said the deal with alex.morgan@example.com slipped";
    assert.doesNotMatch(people().scrub(text), /Alex|Morgan|example\.com/i);
  });
  test("a one-word name doesn't damage ordinary text", () => {
    assert.equal(people().scrub("Sales Cloud is in Sales"), "Sales Cloud is in Sales");
  });
  test("but its username is still scrubbed", () => assert.equal(people().scrub("sales@example.com"), "[[user:U0002]]"));
  test("Salesforce's own Integration User doesn't damage ordinary text, but its username and id are still scrubbed", () => {
    const p = new Pseudonymiser();
    p.label("user", "005000000000009AAA", "Integration User", ["integration@example.com"]);
    assert.equal(p.scrub("Give each integration user its own licence"), "Give each integration user its own licence");
    assert.equal(p.scrub("integration@example.com and 005000000000009AAA"), "[[user:U0001]] and [[user:U0001]]");
  });
  test("doesn't match inside a longer word", () => {
    const p = new Pseudonymiser();
    p.label("user", "005000000000001AAA", "Al Lee");
    assert.equal(p.scrub("Dal Leeds"), "Dal Leeds");
  });
  test("a name with regex characters is safe", () => {
    const p = new Pseudonymiser();
    p.label("user", "005000000000001AAA", "Ann (Temp) Lee");
    assert.equal(p.scrub("Ask Ann (Temp) Lee"), "Ask [[user:U0001]]");
  });
  test("scrubDeep reaches every string and leaves keys and numbers alone", () => {
    const out = people().scrubDeep({ a: ["Alex Morgan"], b: { c: "x", "Alex Morgan": 3 }, d: 5 });
    assert.deepEqual(out, { a: ["[[user:U0001]]"], b: { c: "x", "Alex Morgan": 3 }, d: 5 });
  });

  test("restore turns tokens back into names, for reports only", () => {
    const p = people();
    assert.equal(p.restore(p.scrub("Alex Morgan")), "Alex Morgan");
  });
  test("an unknown token stays visible rather than vanishing", () => {
    assert.equal(people().restore("[[user:U9999]]"), "[[user:U9999]]");
  });
  test("survives a save and reload, and keeps counting", () => {
    const p = new Pseudonymiser(JSON.parse(JSON.stringify(people().entries())));
    assert.equal(p.restore("[[user:U0001]]"), "Alex Morgan");
    assert.equal(p.label("user", "005000000000003AAA", "New Person"), "[[user:U0003]]");
  });
});

describe("Pseudonymiser at scale", () => {
  test("scrubs a big org's action list in a moment, and a name labelled later is still scrubbed", () => {
    // A 1,000-user org and 50,000 action list lines: the scrub once rebuilt its whole name list on every line (night 3 scale test).
    const p = new Pseudonymiser();
    for (let n = 1; n <= 1000; n++) p.label("user", `005${String(n).padStart(12, "0")}AAA`, `Synthetic User ${n}`, [`user${n}@example.com`]);
    const started = performance.now();
    for (let i = 0; i < 50_000; i++) p.scrub(`Open since 2026-01-0${i % 9}, owned by Synthetic User ${(i % 1000) + 1}`);
    const seconds = (performance.now() - started) / 1000;
    assert.ok(seconds < 2, `${seconds.toFixed(1)}s`);
    assert.equal(p.scrub("Owned by Synthetic User 7"), "Owned by [[user:U0007]]");
    p.label("user", "005000000009999AAA", "Late Joiner", []);
    assert.equal(p.scrub("Owned by Late Joiner"), "Owned by [[user:U1001]]");
  });
});

describe("redactSecrets", () => {
  const redacted: Array<[string, string, RegExp]> = [
    ["an XML password element", "<password>hunter2hunter</password>", /hunter2/],
    ["a consumer secret", "<consumerSecret>ABC123DEF</consumerSecret>", /ABC123/],
    ["an API key in a flow", "<apiKey>abcd1234</apiKey>", /abcd1234/],
    ["an Apex assignment", `String apiKey = 'abcd1234efgh';`, /abcd1234/],
    ["a JSON secret", `"client_secret": "abcd1234efgh"`, /abcd1234/],
    ["a bearer token", "Authorization: Bearer abcdefghijklmnopqrstuvwxyz", /abcdefghijkl/],
    ["an AWS key", "key AKIAABCDEFGHIJKLMNOP end", /AKIAABCD/],
    ["a Slack token", "xoxb-1234567890-abcdefghij", /xoxb-/],
    ["a JWT", "eyJhbGciOiJIUzI1.eyJzdWIiOiIxMjM0.SflKxwRJSMeKKF2QT4", /eyJhbGci/],
    ["a Salesforce session id", "00D000000000001!AQEAQabcdefghijklmnopqrstuvwxyz", /AQEAQ/],
    ["a private key", "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----", /abc/],
  ];
  for (const [name, input, leak] of redacted) test(name, () => assert.doesNotMatch(redactSecrets(input), leak));

  test("leaves ordinary metadata alone", () => {
    const xml = "<Flow><label>Password reset reminder</label><status>Active</status></Flow>";
    assert.equal(redactSecrets(xml), xml);
  });
});
