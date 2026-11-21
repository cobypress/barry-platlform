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
  // best practice (DECISIONS.md 657).
  // Numbers: counts, rates, limits and constants the rule works out (each typed number).
  "src/domains/best-practice/index.ts: OWNER_SKEW_RECORDS",
  "src/domains/best-practice/index.ts: afterSave.length",
  "src/domains/best-practice/index.ts: afterSave.length",
  "src/domains/best-practice/index.ts: hits.length",
  "src/domains/best-practice/index.ts: jobs.data.length",
  "src/domains/best-practice/index.ts: jobs.data.length",
  "src/domains/best-practice/index.ts: result.hits",
  "src/domains/best-practice/index.ts: result.hits",
  "src/domains/best-practice/index.ts: result.n",
  "src/domains/best-practice/index.ts: result.n",
  "src/domains/best-practice/index.ts: result.rate",
  "src/domains/best-practice/index.ts: result.rate",
  // Sentences said made: a said or joinSaid result held in a variable, a helper or option returning one, plural over literal nouns, or a said form (each typed Said).
  "src/domains/best-practice/index.ts: grouped(OWNER_SKEW_RECORDS)",
  "src/domains/best-practice/index.ts: grouped(skewed[0]!.records)",
  "src/domains/best-practice/index.ts: noted(JOB_OWNER_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(JOB_OWNER_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(ROLE_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(ROLE_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(SAME_RECORD_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(SAME_RECORD_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(SKEW_PRACTICE)",
  "src/domains/best-practice/index.ts: noted(SKEW_PRACTICE)",
  "src/domains/best-practice/index.ts: plural(hits.length, \"active after-save flow\")",
  "src/domains/best-practice/index.ts: plural(skewed.length, \"owner and object pair\")",
  "src/domains/best-practice/index.ts: practice.checkedOn",
  "src/domains/best-practice/index.ts: practice.position",
  "src/domains/best-practice/index.ts: practice.source",
  // data quality (DECISIONS.md 657).
  // Numbers: counts, rates, limits and constants the rule works out (each typed number).
  "src/domains/data-quality/index.ts: OWNER_GAP_LIMIT",
  "src/domains/data-quality/index.ts: RECORD_TYPE_MIN_AGE_DAYS",
  "src/domains/data-quality/index.ts: RECORD_TYPE_MIN_RECENT",
  "src/domains/data-quality/index.ts: RECORD_TYPE_MIN_RECENT",
  "src/domains/data-quality/index.ts: RECORD_TYPE_OBJECTS",
  "src/domains/data-quality/index.ts: STALE_ACTIVITY_DAYS",
  "src/domains/data-quality/index.ts: STALE_ACTIVITY_DAYS",
  "src/domains/data-quality/index.ts: STALE_ACTIVITY_DAYS",
  "src/domains/data-quality/index.ts: accountRate",
  "src/domains/data-quality/index.ts: accountRate",
  "src/domains/data-quality/index.ts: emailRate",
  "src/domains/data-quality/index.ts: emailRate",
  "src/domains/data-quality/index.ts: hits",
  "src/domains/data-quality/index.ts: hits",
  "src/domains/data-quality/index.ts: hits",
  "src/domains/data-quality/index.ts: hits",
  "src/domains/data-quality/index.ts: hits",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: n",
  "src/domains/data-quality/index.ts: noAccount.data",
  "src/domains/data-quality/index.ts: noEmail.data",
  "src/domains/data-quality/index.ts: open.data",
  "src/domains/data-quality/index.ts: open.data",
  "src/domains/data-quality/index.ts: options.segmentLimit",
  "src/domains/data-quality/index.ts: pct(worst.stale, worst.open)",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rate",
  "src/domains/data-quality/index.ts: rest",
  "src/domains/data-quality/index.ts: result.hits",
  "src/domains/data-quality/index.ts: result.hits",
  "src/domains/data-quality/index.ts: result.hits",
  "src/domains/data-quality/index.ts: result.n",
  "src/domains/data-quality/index.ts: result.n",
  "src/domains/data-quality/index.ts: result.n",
  "src/domains/data-quality/index.ts: result.rate",
  "src/domains/data-quality/index.ts: result.rate",
  "src/domains/data-quality/index.ts: result.rate",
  "src/domains/data-quality/index.ts: share",
  "src/domains/data-quality/index.ts: stale.data",
  "src/domains/data-quality/index.ts: stale.data",
  "src/domains/data-quality/index.ts: strandedTotal",
  "src/domains/data-quality/index.ts: stranded[o]!",
  "src/domains/data-quality/index.ts: total",
  "src/domains/data-quality/index.ts: totals[o]!",
  "src/domains/data-quality/index.ts: worst!.hits",
  "src/domains/data-quality/index.ts: worst!.n",
  "src/domains/data-quality/index.ts: worst!.rate",
  "src/domains/data-quality/index.ts: worst.rate",
  "src/domains/data-quality/index.ts: worst.rate",
  // Sentences said made: a said or joinSaid result held in a variable, a helper or option returning one, plural over literal nouns, or a said form (each typed Said).
  "src/domains/data-quality/index.ts: andList(missing.map((o) => said`${o}`))",
  "src/domains/data-quality/index.ts: dimensionWords(worst!.dimension)",
  "src/domains/data-quality/index.ts: dimensionWords(worst.dimension)",
  "src/domains/data-quality/index.ts: dimensionWords(worst.dimension)",
  "src/domains/data-quality/index.ts: grouped(total)",
  "src/domains/data-quality/index.ts: has(noRecords.length)",
  "src/domains/data-quality/index.ts: has(noneRecent.length)",
  "src/domains/data-quality/index.ts: hidden",
  "src/domains/data-quality/index.ts: listedSaid(kinds, 4)",
  "src/domains/data-quality/index.ts: listedSaid(noRecords.map(namedSaid), LISTED_TYPES)",
  "src/domains/data-quality/index.ts: listedSaid(noneRecent.map(namedSaid), LISTED_TYPES)",
  "src/domains/data-quality/index.ts: options.issueTitle",
  "src/domains/data-quality/index.ts: options.recommendation",
  "src/domains/data-quality/index.ts: options.scopeLabel",
  "src/domains/data-quality/index.ts: options.scopeLabel",
  "src/domains/data-quality/index.ts: options.sentence(result.hits, result.n, result.rate)",
  "src/domains/data-quality/index.ts: owners",
  "src/domains/data-quality/index.ts: plural(candidates.length, \"record type\")",
  "src/domains/data-quality/index.ts: plural(contacts.data, \"contact\")",
  "src/domains/data-quality/index.ts: plural(flagged.length, \"record type\")",
  "src/domains/data-quality/index.ts: plural(flagged.length, \"record type\")",
  "src/domains/data-quality/index.ts: plural(groups.data.length, \"group\")",
  "src/domains/data-quality/index.ts: plural(inGroups, \"account\")",
  "src/domains/data-quality/index.ts: plural(judged.length, \"record type\")",
  "src/domains/data-quality/index.ts: plural(judged.length, \"record type\")",
  "src/domains/data-quality/index.ts: plural(why.notRead.length, \"object is\", \"objects are\")",
  "src/domains/data-quality/index.ts: plural(why.notSeenWhole.length, \"object\")",
  "src/domains/data-quality/index.ts: plural(why.refused.length + why.truncated.length, \"object\")",
  "src/domains/data-quality/index.ts: plural(why.under.length, \"object has\", \"objects have\")",
  "src/domains/data-quality/index.ts: skippedNote",
  "src/domains/data-quality/index.ts: skippedNote",
  // Our own fixed words: literal types from `as const` tables, literal unions or helpers returning literals, none read from the org (each read in review).
  "src/domains/data-quality/index.ts: o",
  "src/domains/data-quality/index.ts: o",
  "src/domains/data-quality/index.ts: o",
  // configuration (DECISIONS.md 657).
  // Numbers: counts, rates, limits and constants the rule works out (each typed number).
  "src/domains/configuration/index.ts: FIELD_SPRAWL_LIMIT",
  "src/domains/configuration/index.ts: FIELD_SPRAWL_LIMIT",
  "src/domains/configuration/index.ts: MIN_CHANGES_FOR_CONCENTRATION",
  "src/domains/configuration/index.ts: MIN_RECORDS_FOR_UNUSED",
  "src/domains/configuration/index.ts: PICKLIST_LIMIT",
  "src/domains/configuration/index.ts: STAGE_LIMIT",
  "src/domains/configuration/index.ts: UNUSED_FILL_LIMIT",
  "src/domains/configuration/index.ts: UNUSED_FILL_LIMIT",
  "src/domains/configuration/index.ts: active.length",
  "src/domains/configuration/index.ts: bare.length",
  "src/domains/configuration/index.ts: bare.length",
  "src/domains/configuration/index.ts: byInactive",
  "src/domains/configuration/index.ts: changes",
  "src/domains/configuration/index.ts: changes",
  "src/domains/configuration/index.ts: changes",
  "src/domains/configuration/index.ts: changes",
  "src/domains/configuration/index.ts: counts.length",
  "src/domains/configuration/index.ts: fields.length",
  "src/domains/configuration/index.ts: fields.length",
  "src/domains/configuration/index.ts: layouts.data.length",
  "src/domains/configuration/index.ts: n",
  "src/domains/configuration/index.ts: p.n",
  "src/domains/configuration/index.ts: people.length",
  "src/domains/configuration/index.ts: rate",
  "src/domains/configuration/index.ts: rate",
  "src/domains/configuration/index.ts: sized.length",
  "src/domains/configuration/index.ts: topShare",
  "src/domains/configuration/index.ts: topShare",
  "src/domains/configuration/index.ts: total",
  "src/domains/configuration/index.ts: unused.length",
  "src/domains/configuration/index.ts: unused.length",
  "src/domains/configuration/index.ts: x.n",
  // Sentences said made: a said or joinSaid result held in a variable, a helper or option returning one, plural over literal nouns, or a said form (each typed Said).
  "src/domains/configuration/index.ts: idle",
  "src/domains/configuration/index.ts: p.words",
  "src/domains/configuration/index.ts: parts",
  "src/domains/configuration/index.ts: plural(active.length, \"active stage\")",
  "src/domains/configuration/index.ts: plural(layouts.data.length, \"page layout\")",
  "src/domains/configuration/index.ts: plural(scan.data.fields.length, \"scanned custom field\")",
  "src/domains/configuration/index.ts: plural(unused.length, \"active stage\")",
  "src/domains/configuration/index.ts: plural(unused.length, \"field\")",
  "src/domains/configuration/index.ts: plural(unused.length, \"field\")",
  "src/domains/configuration/index.ts: tooMany",
  // Our own fixed words: literal types from `as const` tables, literal unions or helpers returning literals, none read from the org (each read in review).
  "src/domains/configuration/index.ts: o",
  // adoption (DECISIONS.md 657).
  // Numbers: counts, rates, limits and constants the rule works out (each typed number).
  "src/domains/adoption/index.ts: BARELY_DAYS",
  "src/domains/adoption/index.ts: BARELY_DAYS",
  "src/domains/adoption/index.ts: FALL_SHARE",
  "src/domains/adoption/index.ts: FALL_SHARE",
  "src/domains/adoption/index.ts: FALL_SPAN_MONTHS + 1",
  "src/domains/adoption/index.ts: MIN_GROUP",
  "src/domains/adoption/index.ts: MIN_GROUP",
  "src/domains/adoption/index.ts: MIN_GROUP",
  "src/domains/adoption/index.ts: RECENT_DAYS",
  "src/domains/adoption/index.ts: RECENT_DAYS",
  "src/domains/adoption/index.ts: classic.filter((u) => profileOf(u) === p).length",
  "src/domains/adoption/index.ts: classic.length",
  "src/domains/adoption/index.ts: expected",
  "src/domains/adoption/index.ts: f.drop",
  "src/domains/adoption/index.ts: f.now",
  "src/domains/adoption/index.ts: f.peak",
  "src/domains/adoption/index.ts: seen",
  "src/domains/adoption/index.ts: share",
  "src/domains/adoption/index.ts: who.length",
  "src/domains/adoption/index.ts: x.barely",
  "src/domains/adoption/index.ts: x.g.users.length",
  // Sentences said made: a said or joinSaid result held in a variable, a helper or option returning one, plural over literal nouns, or a said form (each typed Said).
  "src/domains/adoption/index.ts: f.words",
  "src/domains/adoption/index.ts: plural(months.length, \"whole month\")",
  "src/domains/adoption/index.ts: plural(unread, \"user\")",
  "src/domains/adoption/index.ts: plural(who.length, \"active user\")",
  "src/domains/adoption/index.ts: window",
  // Our own fixed words: literal types from `as const` tables, literal unions or helpers returning literals, none read from the org (each read in review).
  "src/domains/adoption/index.ts: g.dimension",
  "src/domains/adoption/index.ts: x.g.dimension",
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
