import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";

// What every said template interpolates (DECISIONS.md 655), read from the
// source. The type check refuses a run-time string, but not an `any` already
// assigned to a variable or field typed `Said` or a literal: so each piece must
// be one this test can see is safe (a string literal, a choice between safe
// pieces, `org(…)`, `eng(…)`, which org-words-eng.test.ts signs off, or a
// sentence made in place by said or joinSaid), or be listed below, by file and
// expression, signed off in review as a number or a sentence said made.

const SIGNED_OFF: string[] = [
  // Fixed sentences of our own, declared as string literals in notes.ts.
  "src/domains/automation/index.ts: END_OF_SUPPORT",
  "src/domains/automation/index.ts: END_OF_SUPPORT",
  "src/domains/automation/index.ts: MIGRATE_TO_FLOW_NOTE",
  "src/domains/automation/index.ts: MIGRATE_TO_FLOW_NOTE",
  // Numbers: constants, counts and lengths worked out by the rule.
  "src/domains/automation/index.ts: MIXED_AUTOMATION_LIMIT",
  "src/domains/automation/index.ts: active.length",
  "src/domains/automation/index.ts: active.length",
  "src/domains/automation/index.ts: affected.length",
  "src/domains/automation/index.ts: affected.length",
  "src/domains/automation/index.ts: g.missing",
  "src/domains/automation/index.ts: g.total",
  "src/domains/automation/index.ts: missing",
  "src/domains/automation/index.ts: missing",
  "src/domains/automation/index.ts: rate",
  "src/domains/automation/index.ts: rate",
  "src/domains/automation/index.ts: total",
  "src/domains/automation/index.ts: total",
  "src/domains/technical-health/index.ts: byObject.size",
  "src/domains/technical-health/index.ts: o.n",
  // Sentences said or joinSaid made in the same function: unnamedNote's return and the breakdown.
  "src/domains/automation/index.ts: breakdown",
  "src/domains/automation/index.ts: breakdown",
  "src/domains/automation/index.ts: note",
  "src/domains/automation/index.ts: note",
  "src/domains/technical-health/index.ts: note",
  "src/domains/technical-health/index.ts: note",
  // A Said parameter, every caller passing said`…` (both callers are converted rules).
  "src/domains/automation/shared.ts: consequence",
  // A choice between our fixed name for an object we couldn't name (a string constant) and a literal, or org words.
  `src/domains/automation/shared.ts: count === 1 ? UNNAMED : "custom objects we couldn't name"`,
  "src/domains/technical-health/index.ts: o.label === null ? UNNAMED : org(o.label)",
  // The said forms (DECISIONS.md 657): a number grouped() formatted.
  "src/engine/said-forms.ts: whole",
  "src/engine/said-forms.ts: whole",
];

const ROOT = join(import.meta.dirname, "../../..");

function sources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const path = join(dir, e.name);
    if (e.isDirectory()) return e.name === "__tests__" ? [] : sources(path);
    return e.name.endsWith(".ts") ? [path] : [];
  });
}

const CLOSE: Record<string, string> = { "(": ")", "[": "]", "{": "}" };

/** The index just past the string, template or bracket that opens at `i`. */
function skip(text: string, i: number): number {
  const c = text[i]!;
  if (c === '"' || c === "'") {
    for (i++; text[i] !== c; i++) if (text[i] === "\\") i++;
    return i + 1;
  }
  if (c === "`") return templateEnd(text, i + 1, () => {});
  const close = CLOSE[c]!;
  for (i++; text[i] !== close; ) i = /["'`([{]/.test(text[i]!) ? skip(text, i) : i + 1;
  return i + 1;
}

/** Walks a template body from `i`, handing each interpolation's text to `piece`; returns the index past its closing backtick. */
function templateEnd(text: string, i: number, piece: (expr: string) => void): number {
  for (; text[i] !== "`"; i++) {
    if (text[i] === "\\") i++;
    else if (text[i] === "$" && text[i + 1] === "{") {
      const end = skip(text, i + 1);
      piece(text.slice(i + 2, end - 1).trim());
      i = end - 1;
    }
  }
  return i + 1;
}

/** The interpolations of every said template in a file, nested ones included. */
function pieces(text: string): string[] {
  const out: string[] = [];
  for (const m of text.matchAll(/\bsaid`/g)) templateEnd(text, m.index + m[0].length, (e) => out.push(e));
  return out;
}

/** The whole expression is one call or template starting with `head`. */
const whole = (e: string, head: string): boolean => {
  if (!e.startsWith(head)) return false;
  const open = head.length - 1;
  return skip(e, open) === e.length;
};

/** The top-level `?` and `:` of a conditional, or null. */
function choice(e: string): [string, string] | null {
  let q = -1;
  let depth = 0;
  for (let i = 0; i < e.length; i++) {
    const c = e[i]!;
    if (/["'`([{]/.test(c)) {
      i = skip(e, i) - 1;
      continue;
    }
    if (c === "?" && q === -1 && depth === 0) q = i;
    else if (c === "?" && q !== -1) depth++;
    else if (c === ":" && q !== -1) {
      if (depth === 0) return [e.slice(q + 1, i).trim(), e.slice(i + 1).trim()];
      depth--;
    }
  }
  return null;
}

function safe(e: string): boolean {
  if (/^"(?:[^"\\]|\\.)*"$/.test(e) || /^'(?:[^'\\]|\\.)*'$/.test(e)) return true;
  if (whole(e, "org(") || whole(e, "eng(") || whole(e, "joinSaid(") || whole(e, "said`")) return true;
  const both = choice(e);
  return both !== null && safe(both[0]) && safe(both[1]);
}

test("every piece a said template interpolates is safe as written, or signed off", () => {
  const found = sources(join(ROOT, "src"))
    .filter((p) => !p.endsWith(join("engine", "org-words.ts")))
    .flatMap((p) => pieces(readFileSync(p, "utf8")).filter((e) => !safe(e)).map((e) => `${relative(ROOT, p)}: ${e}`))
    .sort();
  assert.deepEqual(found, [...SIGNED_OFF].sort());
});

test("the scan reads what it should", () => {
  assert.deepEqual(pieces("said`a ${org(x)} b ${n === 1 ? \"is\" : said`c ${y}`} ${`${z}!`}`"), ["org(x)", 'n === 1 ? "is" : said`c ${y}`', "`${z}!`", "y"]);
  assert.equal(safe('n === 1 ? "is" : said`c`'), true);
  assert.equal(safe("label"), false);
  assert.equal(safe("org(a) + b"), false);
  assert.equal(safe("`${z}!`"), false);
});
