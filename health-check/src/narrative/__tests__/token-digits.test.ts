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
  // A token's code spelled out in a regex: a letter, or [A-Z], then \d with a count;
  // or any counted \d on the same line as an escaped "[[". Doubled backslashes are
  // folded first so a pattern in an ordinary string counts too.
  const SPELLED = /(?:\[A-Z\]|(?<![A-Za-z])[A-Z])\\d(?:\{\d|[+*])/;
  const DIGITS = /\\d(?:\{\d|[+*])/;
  const spellsDigits = (source: string): number[] => {
    const text = source.replace(/\\\\/g, "\\");
    if (!text.includes(String.raw`\[\[`)) return [];
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

  test("no source file but the pseudonymiser spells a token's digits", () => {
    const all = files(SRC);
    assert.ok(all.length > 100, `read ${all.length} files`);
    assert.ok(all.includes(DEFINES));
    const offenders = all.filter((f) => f !== DEFINES).flatMap((f) => spellsDigits(readFileSync(f, "utf8")).map((line) => `${relative(SRC, f)}:${line}`));
    assert.deepEqual(offenders, [], "build the pattern from tokenSource() or tokenCode() in src/privacy/pseudonymiser.ts");
    assert.match(readFileSync(DEFINES, "utf8"), /String\.raw`\$\{letter\}\\d\{4,\}`/, "the pseudonymiser defines it");
  });
});
