import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { draftSection, type SectionBrief } from "../draft.ts";
import type { NarrativeClient, NarrativeReply } from "../client.ts";
import { teamLevelFinding, teamLevelText } from "../select.ts";
import { MODELS } from "../../config.ts";
import { buildDataset } from "../../numbercheck/index.ts";
import { findNames, maskNames } from "../../privacy/names.ts";
import { Pseudonymiser } from "../../privacy/pseudonymiser.ts";
import type { Finding } from "../../schemas/index.ts";

// Decision 464. label() pads a token's number to four digits with no ceiling, so
// the 10,000th of a kind is [[user:U10000]]. Every pattern that finds a token has
// to take it. These tests mint the 10,000th through label() rather than writing
// the token by hand, so they follow whatever label() really makes.

const SRC = fileURLToPath(new URL("../../", import.meta.url));
const NTH = 10_000;

/**
 * A pseudonymiser holding 9,999 fillers of `kind`, then `last`. A filler's name is
 * one ordinary word and its id has a hyphen, so it adds nothing to scrub() or the
 * names index and the test stays quick.
 */
function mint(kind: string, last: { id: string; name: string; aliases?: string[] }, p = new Pseudonymiser()): { p: Pseudonymiser; token: string } {
  for (let n = 1; n < NTH; n++) p.label(kind, `filler-${kind}-${String(n).padStart(5, "0")}`, "Sales");
  const token = p.label(kind, last.id, last.name, last.aliases);
  return { p, token };
}

const ANA = { id: "005000000000001AAA", name: "Ana Okafor", aliases: ["ana.okafor@example.com"] };

describe("the 10,000th token of a kind", () => {
  const { p, token } = mint("user", ANA);
  const first = p.entries()[0]!.token;

  test("label() makes it five digits", () => {
    assert.equal(token, "[[user:U10000]]");
    assert.equal(first, "[[user:U0001]]");
  });

  test("restore() names it, and still names a four-digit one", () => {
    assert.equal(p.restore(`Owner: ${token}.`), "Owner: Ana Okafor.");
    assert.equal(p.restore(`Owner: ${first}.`), "Owner: Sales.");
    assert.equal(p.restore(`${token}${first}`), "Ana OkaforSales", "two tokens side by side are each restored");
  });

  test("an unknown five-digit token stays visible, as an unknown four-digit one does", () => {
    assert.equal(p.restore("By [[user:U10001]]."), "By [[user:U10001]].");
    assert.equal(p.restore("By [[user:U9999]] and [[person:P0001]]."), "By Sales and [[person:P0001]].");
    assert.equal(p.restore("By [[user:U123456]]."), "By [[user:U123456]].");
  });

  test("scrubbing its name gives its token, and restore() round-trips", () => {
    const text = "Ana Okafor (ana.okafor@example.com) owns 40 opportunities.";
    const scrubbed = p.scrub(text);
    assert.equal(scrubbed, `${token} (${token}) owns 40 opportunities.`);
    assert.equal(p.restore(scrubbed), "Ana Okafor (Ana Okafor) owns 40 opportunities.");
  });

  test("a pseudonymiser rebuilt from a saved map holding it carries on at 10,001", () => {
    const rebuilt = new Pseudonymiser(p.entries());
    const next = rebuilt.label("user", "005000000000002AAA", "Ben Adeyemi");
    assert.equal(next, "[[user:U10001]]");
    assert.ok(!p.entries().some((e) => e.token === next), "the new token is nobody else's");
    assert.equal(rebuilt.restore(token), "Ana Okafor");
    assert.equal(rebuilt.label("contact", "003000000000001AAA", "Cara Nwosu"), "[[contact:C0001]]", "another kind still starts at 1");
  });

  test("the digits stop at the closing brackets, so nothing after a token is taken with it", () => {
    assert.equal(p.restore(`${token}0000]] and 20000`), "Ana Okafor0000]] and 20000");
    assert.equal(teamLevelText(`Rep ${token} closed 20000 and rep ${first}.`), "One rep closed 20000 and another rep.");
    assert.equal(p.restore("[[user:U10000 ]] and [[user:U10000]"), "[[user:U10000 ]] and [[user:U10000]", "a broken token is left alone");
  });
});

describe("the names guard and a five-digit token", () => {
  // The guard must treat the whole token as a token. A kind's word can be a part of
  // someone's name, so if the guard read inside a five-digit token it would mask
  // that word (corrupting the token) and findNames() would call it a leak.
  const people = new Pseudonymiser();
  people.label("user", "005000000000003AAA", "Rosa Customer");
  const { p, token } = mint("customer", { id: "00D000000000001AAA", name: "Acme Holdings" }, people);

  test("maskNames() leaves the token whole, and still masks a name beside it", () => {
    assert.equal(token, "[[customer:C10000]]");
    const rosa = p.entries()[0]!.token;
    assert.equal(maskNames(`Billed to ${token}.`, p), `Billed to ${token}.`);
    assert.equal(maskNames(`Rosa billed ${token}.`, p), `${rosa} billed ${token}.`);
    assert.equal(maskNames("Billed to [[customer:C0002]].", p), "Billed to [[customer:C0002]].");
  });

  test("findNames() finds no name inside the token", () => {
    assert.deepEqual(findNames(`Billed to ${token}.`, p), []);
    assert.equal(findNames(`Billed to ${token} by Rosa.`, p).length, 1, "a name beside it is still found");
  });
});

describe("team-level text and a five-digit token", () => {
  const { token } = mint("user", ANA);

  test("teamLevelText() rewrites it as it does a four-digit one", () => {
    assert.equal(teamLevelText(`Rep ${token} is low.`), "One rep is low.");
    assert.equal(teamLevelText(`Win rate for rep ${token} is low.`), "Win rate for one rep is low.");
    assert.equal(teamLevelText(`Worst by rep: ${token}, at 93.3%.`), "The worst rep is at 93.3%.");
    assert.equal(teamLevelText(`${token} made 90% of changes.`), "One person made 90% of changes.");
    assert.equal(teamLevelText(`Win rate for rep ${token} is low`, "title"), "Win rate for a single rep is low");
    assert.equal(teamLevelText("Rep [[user:U0014]] is low."), "One rep is low.");
  });

  test("teamLevelFinding() drops a subject labelled with it", () => {
    const finding = { title: `Win rate for rep ${token} is low`, summary: "", recommendation: "", subjects: [{ type: "user", label: token }, { type: "stage", label: "Prospecting" }] } as unknown as Finding;
    assert.deepEqual(teamLevelFinding(finding).subjects, [{ type: "stage", label: "Prospecting" }]);
  });
});

describe("a roles-only draft and a five-digit token", () => {
  const { p, token } = mint("user", ANA);
  const brief: SectionBrief = { title: "What your people told us", purpose: "Say what the people we interviewed told us.", sources: [], maxWords: 200, rolesOnly: true };
  const dataset = buildDataset({ findings: [], metrics: [], currency: "GBP" });
  const reply = (text: string): NarrativeReply => ({ stopReason: "end_turn", text: JSON.stringify({ paragraphs: [{ text, sources: [] }] }), model: MODELS.synthesis });
  const draft = (text: string) => draftSection({ brief, dataset, client: { async complete() { return reply(text); } } satisfies NarrativeClient, pseudonyms: p });

  test("is refused, as one holding a four-digit token is", async () => {
    const five = await draft(`${token} told us the forecast is built in a spreadsheet.`);
    assert.equal(five.status, "rejected");
    assert.ok(five.status === "rejected" && five.reasons.some((r) => /Refer to people by their role/.test(r)), JSON.stringify(five));
    const four = await draft("[[user:U0001]] told us the forecast is built in a spreadsheet.");
    assert.equal(four.status, "rejected");
    const role = await draft("Your sales director told us the forecast is built in a spreadsheet.");
    assert.equal(role.status, "accepted", "the stub client is not what refuses");
  });
});

describe("one definition of a token's digits", () => {
  // A token's code spelled out in a regex: its letter, then a digit (\d, [0-9] or
  // [\d]) with a count or a repeat, or written out four times or more; or the same
  // digits on a line with an escaped "[[" (decision 475). The letter is a bare
  // capital, a class holding a capital ([UPC], [A-Za-z]), a group of those
  // ((?:U|P|C), ([A-Z])) or an interpolation (${letter}), and the digits may open
  // their own group. Any file is read, not only one that writes "[[" itself, since
  // a token's brackets can come from elsewhere. Doubled backslashes are folded
  // first so a pattern in an ordinary string counts too.
  const DIGIT = String.raw`(?:\\d|\[0-9\]|\[\\d\])`;
  const COUNTED = `${DIGIT}(?:\\{\\d|[+*]|${DIGIT}{3})`;
  // Only a count that can reach four, for a bare capital opening a group: a
  // time's T(\d{2}) is never a token's code.
  const REACHES = `${DIGIT}(?:\\{(?:[4-9]|\\d\\d|\\d+,)|[+*]|${DIGIT}{3})`;
  const OPEN = String.raw`(?:\((?:\?:|\?<\w+>)?)?`;
  // A negated class ([^A-Z]) is anything but a letter, so it doesn't count.
  const CLASS = String.raw`\[(?!\^)(?:[^\]\\]|\\.)*[A-Z](?:[^\]\\]|\\.)*\]`;
  const GROUP = String.raw`\((?:\?:|\?<\w+>)?(?:[A-Z]|${CLASS})(?:\|(?:[A-Z]|${CLASS}))*\)`;
  const INTERPOLATED = String.raw`\$\{[^}]*\}`;
  const BARE = String.raw`(?<![A-Za-z])[A-Z]`;
  const SPELLED = new RegExp(`(?:${BARE}|(?:${CLASS}|${GROUP}|${INTERPOLATED})${OPEN})${COUNTED}|${BARE}\\((?:\\?:|\\?<\\w+>)?${REACHES}`);
  const DIGITS = new RegExp(COUNTED);
  const spellsDigits = (source: string): number[] => {
    const text = source.replace(/\\\\/g, "\\");
    return text.split("\n").flatMap((line, i) => (SPELLED.test(line) || (line.includes(String.raw`\[\[`) && DIGITS.test(line)) ? [i + 1] : []));
  };
  const DEFINES = join(SRC, "privacy", "pseudonymiser.ts");

  const files = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
      const path = join(dir, e.name);
      if (e.isDirectory()) return e.name === "__tests__" || e.name === "node_modules" ? [] : files(path);
      return e.name.endsWith(".ts") ? [path] : [];
    });

  test("the check catches each pattern decision 464 replaced", () => {
    const old = [
      String.raw`const TOKEN = /\[\[([a-z]+):([A-Z]\d{4})\]\]/g;`,
      String.raw`const TOKEN = /\[\[[a-z]+(?::[A-Z]\d{4})?\]\]/g;`,
      String.raw`const USER_TOKEN = /\[\[user:[A-Z]\d{4}\]\]/;`,
      String.raw`.replace(/Worst by rep: \[\[user:[A-Z]\d{4}\]\], at/g, "The worst rep is at")`,
      String.raw`const PERSON_TOKEN = /\[\[(?:user|person|contact):[A-Z]\d{4}\]\]/;`,
      String.raw`export const UserToken = z.string().regex(/^\[\[user:U\d{4,}\]\]$/);`,
      String.raw`const T = new RegExp("\\[\\[user:U\\d{4}\\]\\]");`,
      String.raw`const T = /\[\[user:U\d+\]\]/;`,
      String.raw`const T = new RegExp(String.raw${"`"}\[\[user:${"${letter}"}\d{4}\]\]${"`"});`,
    ];
    for (const line of old) assert.deepEqual(spellsDigits(line), [1], line);
    assert.deepEqual(spellsDigits(String.raw`const ISO = /\d{4}-\d{2}/;` + "\n" + String.raw`const T = /\[\[[^\]]*\]\]/g;`), [], "a date or an any-content token is fine");
  });

  test("the check catches the other spellings of the same digits (decision 475)", () => {
    // Each beside a token letter, on its own in a file and below a line holding an
    // escaped "[["; then without the letter, on the "[[" line itself.
    const OTHER = String.raw`const T = /\[\[[^\]]*\]\]/g;`;
    const spellings = [
      String.raw`const C = /[A-Z][0-9]{4}/;`,
      String.raw`const C = /U[0-9]{4,}/;`,
      String.raw`const C = /[A-Z][0-9]+/;`,
      String.raw`const C = /U[0-9]*/;`,
      String.raw`const C = /[A-Z][\d]{4}/;`,
      String.raw`const C = /U[\d]+/;`,
      String.raw`const C = /[A-Z][\d]*/;`,
      String.raw`const C = /[A-Z]\d\d\d\d/;`,
      String.raw`const C = /U\d\d\d\d\d?/;`,
      String.raw`const C = /[A-Z][0-9][0-9][0-9][0-9]/;`,
      String.raw`const C = /U[0-9]\d[0-9]\d/;`,
      String.raw`const C = new RegExp("[A-Z][0-9]{4}");`,
      String.raw`const C = new RegExp("U\\d\\d\\d\\d");`,
    ];
    for (const line of spellings) {
      assert.deepEqual(spellsDigits(line), [1], line);
      assert.deepEqual(spellsDigits(`${OTHER}\n${line}`), [2], line);
    }
    const onTheLine = [String.raw`/\[\[user:[A-Z][0-9]{4}\]\]/`, String.raw`/\[\[user:\w[\d]+\]\]/`, String.raw`/\[\[user:\w\d\d\d\d\]\]/`, String.raw`/\[\[user:\w[0-9][0-9][0-9][0-9]\]\]/`];
    for (const line of onTheLine) assert.deepEqual(spellsDigits(line), [1], line);
  });

  test("the check catches a token's letter written as a class, a group or an interpolation (decision 475)", () => {
    // Each alone in a file with no "[[", as written, then with each spelling of
    // the digits in place of its own.
    const letters = (digits: string) => [
      String.raw`const code = String.raw${"`"}${"${letter}"}${digits}${"`"};`,
      String.raw`const CODE = /^([A-Z])(${digits})$/;`,
      String.raw`const CODE = /^[UPC]${digits}$/;`,
      String.raw`const CODE = /^(?:U|P|C)${digits}$/;`,
      String.raw`const CODE = /^[A-Za-z]${digits}$/;`,
      String.raw`const CODE = new RegExp(${"`"}${"${kind}"}:${"${letter}"}${digits}${"`"});`,
      String.raw`const CODE = /^U(${digits})$/;`,
    ];
    const asWritten = [...letters(String.raw`\d{4,}`).slice(0, 5), letters(String.raw`[0-9]{4,}`)[5]!, letters(String.raw`\d{4,}`)[6]!];
    assert.equal(asWritten[0], String.raw`const code = String.raw${"`"}${"${letter}"}\d{4,}${"`"};`, "the pseudonymiser's own line");
    for (const line of asWritten) assert.deepEqual(spellsDigits(line), [1], line);
    const spellings = [String.raw`\d{4}`, String.raw`\d{4,}`, String.raw`\d+`, String.raw`\d*`, String.raw`\d\d\d\d`, String.raw`[0-9]{4,}`, String.raw`[\d]+`, String.raw`[0-9][0-9][0-9][0-9]`];
    for (const digits of spellings) for (const line of letters(digits)) assert.deepEqual(spellsDigits(line), [1], line);
  });

  test("digits that can't be a token's code stay quiet", () => {
    const OTHER = String.raw`const T = /\[\[[^\]]*\]\]/g;`;
    const quiet = [
      String.raw`const ISO = /\d{4}-\d{2}/;`,
      String.raw`const YEAR = /^\d\d\d\d-\d\d$/;`,
      String.raw`const ID = /^[0-9]{15}$/;`,
      String.raw`const ID = /^[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?$/;`,
      String.raw`const RUN = /^hc-[0-9]{4}-[0-9]{2}-[\d]{2}$/;`,
      String.raw`const FY = /FY\d{4}/;`,
      String.raw`const Q = /Q[0-9]/;`,
      // Three written out is short of any token, which has four digits or more.
      String.raw`const CODE = /^R\d\d\d$/;`,
      // The nearest shapes src holds: a time's T(\d{2}), dates and years in groups,
      // money, and digits after a class with no capital or a negated one.
      String.raw`const time = /^\d{4}-\d{2}-\d{2}T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?(?:Z|\+0000|\+00:00)$/;`,
      String.raw`const DATE = /\b(\d{4})-(\d{2})-(\d{2})(?:T[\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)?(?![\w-])/g;`,
      String.raw`const MONTH = /(?<![\w-])(\d{4})-(0[1-9]|1[0-2])(?![\w-])/g;`,
      String.raw`const YEAR = /(?<![\w.,£$€-])(?:19|20)\d{2}(?![\w%])/g;`,
      String.raw`const FIGURE = String.raw${"`"}[-+\u2212]?[£$€]?[-\u2212]?(?:[1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{0,8}|0)${"`"};`,
      String.raw`const SLASHED = /\d{2}[-/]\d{4}/;`,
      String.raw`const AFTER = /[^A-Z]\d{4}/;`,
    ];
    for (const line of quiet) {
      assert.deepEqual(spellsDigits(line), [], line);
      assert.deepEqual(spellsDigits(`${OTHER}\n${line}`), [], line);
    }
  });

  test("no source file but the pseudonymiser spells a token's digits", () => {
    const all = files(SRC);
    assert.ok(all.length > 100, `read ${all.length} files`);
    assert.ok(all.includes(DEFINES));
    const offenders = all.filter((f) => f !== DEFINES).flatMap((f) => spellsDigits(readFileSync(f, "utf8")).map((line) => `${relative(SRC, f)}:${line}`));
    assert.deepEqual(offenders, [], "build the pattern from tokenSource() or tokenCode() in src/privacy/pseudonymiser.ts");
    assert.match(readFileSync(DEFINES, "utf8"), /String\.raw`\$\{letter\}\\d\{4,\}`/, "the pseudonymiser defines it");
  });
});
