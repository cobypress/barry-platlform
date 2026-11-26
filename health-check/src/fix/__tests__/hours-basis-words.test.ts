import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { FIX_TABLE } from "../table.ts";

// The hours' basis is marked as the engine's words alone as it is made
// (DECISIONS.md 663), not by said, so this holds what it is made from: the
// fix table's own string literals and whole-number counts, and nothing else.
// A change that lets any other text in fails here until review reads it.

const ROOT = join(import.meta.dirname, "../../..");
const read = (path: string): string => readFileSync(join(ROOT, path), "utf8");

/** Every string literal in a source file, as its text. */
const literals = (source: string): Set<string> => new Set([...source.matchAll(/"((?:[^"\\]|\\.)*)"/g)].map((m) => JSON.parse(`"${m[1]}"`) as string));

test("every word of every rule's hours is a string literal written in the fix table", () => {
  const written = literals(read("src/fix/table.ts"));
  const missing: string[] = [];
  for (const [id, rule] of Object.entries(FIX_TABLE)) {
    const h = rule.hours;
    const words = h.kind === "fixed" ? [h.basis] : [h.one, h.many, ...(h.bulk ? [h.bulk] : []), ...(h.facts === "all" ? [] : h.facts)];
    for (const w of words) if (!written.has(w)) missing.push(`${id}: ${w}`);
    if (h.kind === "each") assert.ok(h.minutes.every(Number.isInteger) && Number.isInteger(h.cap), id);
  }
  assert.deepEqual(missing, []);
});

test("the basis builder interpolates only the table's words and numbers it works out", () => {
  const source = read("src/fix/hours.ts");
  const body = source.slice(source.indexOf("export function effortHours("), source.indexOf("/** The most hours a small fix"));
  const pieces = [...body.matchAll(/\$\{([^}]*)\}/g)].map((m) => m[1]!.trim()).sort();
  // The table's words (rule.basis, one, many, bulk), counts and caps (numbers), the duration durationText writes from the table's minutes, and a verb chosen by a count.
  assert.deepEqual(pieces, [
    "count",
    "count - covered",
    "count - covered === 1 ? \"is\" : \"are\"",
    "count === 1 ? rule.one : rule.many",
    "covered",
    "covered === 1 ? rule.one : rule.many",
    "durationText(rule.minutes)",
    "each",
    "each",
    "each",
    "rule.basis",
    "rule.bulk",
    "rule.cap",
    "rule.one",
  ]);
  // Its inputs: a rule from the table and the finding's facts, which the schema holds to numbers.
  assert.match(body, /^export function effortHours\(rule: HoursRule, facts: Readonly<Record<string, number>>\): EffortHours \| null \{/);
});

test("the basis is only ever made from the fix table's rule for the finding", () => {
  const files = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap((e) => (e.isDirectory() ? (e.name === "__tests__" ? [] : files(join(dir, e.name))) : e.name.endsWith(".ts") ? [join(dir, e.name)] : []));
  const calls = files(join(ROOT, "src")).flatMap((p) => [...readFileSync(p, "utf8").matchAll(/\beffortHours\(([^)]*)\)/g)].map((m) => `${relative(ROOT, p)}: ${m[1]}`));
  assert.deepEqual(calls.filter((c) => !c.startsWith("src/fix/hours.ts: rule: HoursRule")), ["src/fix/index.ts: rule.hours, f.facts"]);
  // The fix table's rule, or its own version's where one differs (FIX_BY_VERSION, DECISIONS.md 777), which keeps the hours as given.
  assert.match(read("src/fix/index.ts"), /const rule = fixRuleOf\(ruleId, version\);/);
  assert.match(read("src/fix/table.ts"), /export function fixRuleOf\(ruleId: string, version\?: number\): FixRule \| undefined \{\n  const byVersion = Object\.hasOwn\(FIX_BY_VERSION, ruleId\) \? FIX_BY_VERSION\[ruleId\]! : undefined;\n  if \(version !== undefined && byVersion && Object\.hasOwn\(byVersion, version\)\) return byVersion\[version\];\n  return Object\.hasOwn\(FIX_TABLE, ruleId\) \? FIX_TABLE\[ruleId\] : undefined;\n\}/);
});
