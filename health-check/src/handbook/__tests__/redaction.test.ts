import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { parseMetadataBundle } from "../../extract/metadata-parse.ts";
import { redactSecrets } from "../../privacy/secrets.ts";

// A quoted secret that holds the other kind of quote (decision 472). The privacy
// tests are locked to builders, so the new cases are tested from here.

// The patterns as they were before decision 472, as the reference: the new list
// must hide everything this one hid.
const R = "[REDACTED]";
const BEFORE: ReadonlyArray<[RegExp, string]> = [
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, R],
  [/(<([A-Za-z]*(?:password|secret|apiKey|privateKey|accessToken|refreshToken|clientSecret|consumerSecret)[A-Za-z]*)>)[^<]*(<\/\2>)/gi, `$1${R}$3`],
  [/((?:password|passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|auth[_-]?token)["']?\s*[:=]\s*["'])[^"'\n]{4,}(["'])/gi, `$1${R}$2`],
  [/\bBearer\s+[A-Za-z0-9._~+/!-]{16,}=*/g, `Bearer ${R}`],
  [/\bAKIA[0-9A-Z]{16}\b/g, R],
  [/\bsk-[A-Za-z0-9_-]{20,}\b/g, R],
  [/\bxox[abprs]-[A-Za-z0-9-]{10,}\b/g, R],
  [/\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/g, R],
  [/\b00D[A-Za-z0-9]{12,15}![A-Za-z0-9._]{20,}/g, R],
];
const redactedBefore = (text: string): string => BEFORE.reduce((out, [pattern, replacement]) => out.replace(pattern, replacement), text);

// Each generated secret is made of marks found nowhere else in its string, so
// what's left of it is the marks still in the output.
const MARKS = ["Zq1", "Zq2", "Zq3", "Zq8", "Zq9"];
const shown = (text: string): string[] => MARKS.filter((m) => text.includes(m));

const SEPARATORS = [": ", "=", " = ", '": '];
const OPENS = ["'", '"'];
const CLOSES = ["'", '"', ""];
const BODIES = ["Zq1Zq2Zq3", 'Zq1"Zq2Zq3', "Zq1'Zq2Zq3", "Zq1 Zq2 Zq3", "Zq1&Zq2<Zq3", `Zq1"Zq2'Zq3`, "Zq1\nZq2Zq3", 'ab"Zq1Zq2', "ab'Zq1Zq2", "Zq1"];
// A second key after the first, so a value that runs on can't hide it.
const AFTER = ["", " && x = 'y'", " api_key='Zq8Zq9'", ' secret: "Zq8\'Zq9"'];

function* generated(names: string[]): Generator<string> {
  for (const name of names)
    for (const separator of SEPARATORS)
      for (const open of OPENS)
        for (const close of CLOSES)
          for (const body of BODIES) for (const after of AFTER) yield `IF(${name}${separator}${open}${body}${close}${after}, 1, 0)`;
}

describe("a quoted secret holding the other kind of quote (decision 472)", () => {
  test("redacts strictly more than before: whatever the old patterns hid stays hidden, over generated keys, quotes and values", () => {
    let changed = 0;
    let more = 0;
    for (const text of generated(["password", "api_key", "Client-Secret", "auth_token"])) {
      const before = redactedBefore(text);
      const now = redactSecrets(text);
      if (before === text) continue;
      changed++;
      const kept = shown(before);
      assert.deepEqual(shown(now).filter((m) => !kept.includes(m)), [], `${JSON.stringify(text)} gave ${JSON.stringify(now)}, before ${JSON.stringify(before)}`);
      if (shown(now).length < kept.length) more++;
    }
    // The grid reaches both sides: cases the old patterns redacted, and some of
    // them where the new one hides more.
    assert.ok(changed > 300, `${changed}`);
    assert.ok(more > 50, `${more}`);
  });

  test("text with no secret's name comes out the same as before, and unchanged", () => {
    let count = 0;
    for (const text of generated(["Name", "Status", "Secretary__c", "Password__c"])) {
      if (/api_key|secret:/.test(text)) continue;
      count++;
      assert.equal(redactSecrets(text), redactedBefore(text), JSON.stringify(text));
      assert.equal(redactSecrets(text), text, JSON.stringify(text));
    }
    assert.ok(count > 300, `${count}`);
  });

  test("a value broken by a line end inside its quotes is left as it was before", () => {
    const text = "api_key: 'ab\"cd\nefghijk'";
    assert.equal(redactSecrets(text), redactedBefore(text));
    assert.equal(redactSecrets(text), text);
  });

  // Each an XML value: a validation rule's formula and a flow's filter value.
  const INPUTS = [
    ["api_key: &apos;ab&#34;cdefghijk&apos;", "cdefghijk"],
    ["api_key: &apos;ab&quot;cdefghijk&apos;", "cdefghijk"],
    ["api_key: &quot;ab&#39;cdefghijk&quot;", "cdefghijk"],
    ["password=&apos;Tr0ub&#34;adorX9&apos;", "adorX9"],
  ] as const;
  const rule = (value: string) => ({ "objects/A__c.object": `<CustomObject><validationRules><fullName>R</fullName><active>true</active><errorConditionFormula>${value}</errorConditionFormula></validationRules></CustomObject>` });
  const flow = (value: string) => ({ "flows/F.flow": `<Flow><label>F</label><start><filters><field>A</field><operator>EqualTo</operator><value><stringValue>${value}</stringValue></value></filters></start></Flow>` });

  test("through the retrieve parser, a validation rule's formula keeps none of the secret", () => {
    for (const [value, body] of INPUTS) {
      const bundle = parseMetadataBundle(rule(value));
      const formula = bundle.handbook!.validationRules[0]!.formula!;
      assert.match(formula, /^(api_key: |password=)['"]\[REDACTED\]['"]$/, value);
      assert.ok(!JSON.stringify(bundle).includes(body), value);
    }
  });

  test("through the retrieve parser, a flow's filter value keeps none of the secret", () => {
    for (const [value, body] of INPUTS) {
      const bundle = parseMetadataBundle(flow(value));
      const [condition] = bundle.handbook!.flows[0]!.start.entry.conditions;
      assert.match(condition!.value!, /^(api_key: |password=)['"]\[REDACTED\]['"]$/, value);
      assert.ok(!JSON.stringify(bundle).includes(body), value);
    }
  });

  // Ordinary formulas with both kinds of quote near a secret's name, and no
  // secret: each came through unchanged before, and still does. The last opens
  // a quote after a key and never closes it with the same kind, so a value
  // allowed to end at either kind would swallow the rest of the formula.
  const ORDINARY = [
    `IF(ISPICKVAL(Type, "Secret"), 'a', "b")`,
    `Password__c = "x" & 'yz'`,
    `TEXT(Status) = 'Closed' && Name = "it's secret"`,
    `AND(Secret__c = 'it"s', password = "x", 'abcd' = "efgh")`,
    `CONTAINS(Notes__c, "password: '") && Status__c = "Open"`,
  ];

  test("ordinary formulas holding both kinds of quote near a secret's name come through unchanged", () => {
    for (const text of ORDINARY) {
      assert.equal(redactedBefore(text), text);
      assert.equal(redactSecrets(text), text);
    }
    const quoted = (text: string) => text.replaceAll('"', "&quot;").replaceAll("'", "&apos;");
    for (const text of ORDINARY) assert.equal(parseMetadataBundle(rule(quoted(text))).handbook!.validationRules[0]!.formula, text);
  });
});
