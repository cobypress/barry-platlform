import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { ALL_MODULES } from "../../src/domains/index.ts";
import { REMAKE, RULE_WHERE_FILE, ruleWhere, ruleWhereText, type RuleWhere } from "../rule-where.ts";

// The rules' Setup labels for the customer portal (DECISIONS.md 626): made
// from the rules by `npm run rule-where`, committed, and copied to the console's
// docs/health-check/engine-rules/where.json. The file is read here as parsed
// from disk, as the console reads its copy.

const text = readFileSync(RULE_WHERE_FILE, "utf8");
const committed = JSON.parse(text) as RuleWhere;
const rules = ALL_MODULES.flatMap((m) => m.rules);
const STALE = `fixtures/rule-where.json is out of date. If a rule's version moved, decide whether the new version loosened it, tightened it, or both, and declare which in src/engine/loosened.ts (LOOSENED, DECISIONS.md 732; TIGHTENED, DECISIONS.md 754). Make it again from health-check/ with \`${REMAKE}\`, and copy it to the console's docs/health-check/engine-rules/where.json`;

describe("the rules' Setup labels exported for the console (decision 626)", () => {
  test("the exported where.json equals the rules' where fields: made again from the rules, it is the same bytes", () => {
    const remade = ruleWhereText();
    const moved = [...new Set([...Object.keys(committed), ...rules.map((r) => r.id)])].filter((id) => JSON.stringify(committed[id]) !== JSON.stringify(ruleWhere()[id]));
    assert.ok(remade === text, `${STALE}. What moved: ${moved.slice(0, 20).join(", ") || "the order or spacing"}`);
  });

  test("holds every rule once, with its version, and where only for a rule that gives one", () => {
    assert.deepEqual(Object.keys(committed).sort(), rules.map((r) => r.id).sort());
    for (const rule of rules) {
      const entry = committed[rule.id]!;
      assert.equal(entry.version, rule.version, rule.id);
      assert.deepEqual(entry.where, rule.where ? [...rule.where] : undefined, rule.id);
      assert.deepEqual(Object.keys(entry), ["version", ...(rule.where ? ["where"] : []), ...(entry.loosenedFrom ? ["loosenedFrom", "loosenedOn"] : []), ...(entry.tightenedFrom ? ["tightenedFrom", "tightenedOn"] : []), ...(entry.changedFrom ? ["changedFrom", "changedOn"] : [])], rule.id);
    }
    assert.ok(Object.values(committed).filter((e) => e.where).length >= 29, "623's 29 scan rules keep their labels");
  });

  test("labels only: each starts at Setup, and none is a URL, an id or empty", () => {
    for (const [id, { where }] of Object.entries(committed)) {
      if (!where) continue;
      assert.equal(where[0], "Setup", id);
      for (const label of where) {
        assert.ok(label.trim().length > 0 && label === label.trim(), id);
        assert.doesNotMatch(label, /[/:?#=@]|\b[a-zA-Z0-9]{15}(?:[a-zA-Z0-9]{3})?\b/, id);
      }
    }
  });

  test("a rule whose version or labels change makes the file stale", () => {
    const [first, ...rest] = rules;
    const changed = [{ ...first!, version: first!.version + 1 }, ...rest];
    assert.notEqual(ruleWhereText(changed), text);
    const relabelled = rules.map((r) => (r.where ? { ...r, where: [...r.where, "Elsewhere"] } : r));
    assert.notEqual(ruleWhereText(relabelled), text);
    assert.throws(() => ruleWhere([first!, first!]), /Two rules have the id/);
  });
});
