import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";

// Every call to eng (DECISIONS.md 655), by file and what it's given. eng
// vouches that a string the types can't see as literal holds only the engine's
// words; a value read from the org must go through org instead. A new call, or
// a changed argument, fails here until review signs it off by adding it below.

const SIGNED_OFF: string[] = [
  "src/domains/automation/index.ts: [...set].sort().join(\", \")",
  "src/domains/automation/index.ts: g.kind",
  "src/domains/automation/index.ts: plural(active.length, \"active flow\")",
  "src/domains/automation/index.ts: plural(active.length, \"active flow\")",
  "src/domains/automation/index.ts: plural(active.length, \"active Process Builder\")",
  "src/domains/automation/index.ts: plural(active.length, \"active Workflow Rule\")",
  "src/domains/automation/index.ts: plural(missing, \"element\")",
  "src/domains/automation/index.ts: plural(objects, \"object\")",
  "src/domains/automation/shared.ts: plural(count, \"active Apex trigger\")",
  // plural's own words: its first overload holds both nouns to literal types, so nothing here is the org's (DECISIONS.md 657).
  "src/engine/rule.ts: `${n} ${n === 1 ? one : many}`",
  // A number the engine formats: digits, commas and a point.
  "src/engine/said-forms.ts: n.toLocaleString(\"en-GB\")",
  // The quarter's first day, which quarterBounds works out from the run's date and the fiscal start month, a number.
  "src/domains/current-pipeline/index.ts: quarter.from",
  // A phrase from USER_GUARD_BLOCKERS, our own table of the guards' words; an id it doesn't know goes through org.
  "src/domains/utilisation/index.ts: phrase",
];

const ROOT = join(import.meta.dirname, "../../..");

function sources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const path = join(dir, e.name);
    if (e.isDirectory()) return e.name === "__tests__" ? [] : sources(path);
    return e.name.endsWith(".ts") ? [path] : [];
  });
}

/** The argument of each eng( call, to its matching bracket. */
function calls(text: string): string[] {
  const out: string[] = [];
  for (const m of text.matchAll(/\beng\(/g)) {
    let depth = 1;
    let i = m.index + m[0].length;
    const start = i;
    for (; i < text.length && depth > 0; i++) depth += text[i] === "(" ? 1 : text[i] === ")" ? -1 : 0;
    out.push(text.slice(start, i - 1));
  }
  return out;
}

test("every call to eng is one review signed off", () => {
  const found = sources(join(ROOT, "src"))
    .filter((p) => !p.endsWith(join("engine", "org-words.ts")))
    .flatMap((p) => calls(readFileSync(p, "utf8")).map((arg) => `${relative(ROOT, p)}: ${arg}`))
    .sort();
  assert.deepEqual(found, [...SIGNED_OFF].sort());
});
