import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { isSentence, scanEngineTextsByDir, SENTENCE_ONLY_DIRS } from "../engine-texts-scan.ts";

// What the extractors' folder adds to the engine's own texts (DECISIONS.md 743),
// listed in extract-texts.txt for a reviewer to read in the diff. The folder holds
// queries, permission and edition names, field lists and shell lines beside the
// sentences a read's reason is made of, and a text keyed here is ours wherever it
// stands, whole or passed on after a head: so only its sentences are keyed. A new
// one fails here until it is added to the list, read and found to be a sentence of ours.

const SRC = join(import.meta.dirname, "../..");
const by = scanEngineTextsByDir(SRC);
const elsewhere = new Set([...by].filter(([dir]) => !SENTENCE_ONLY_DIRS.includes(dir)).flatMap(([, texts]) => [...texts]));
const added = [...by.get("extract")!].filter((t) => !elsewhere.has(t)).sort();
const listed = readFileSync(join(import.meta.dirname, "extract-texts.txt"), "utf8").split("\n").filter(Boolean);

/** What must never be keyed as a sentence of ours: a name a value can hold, an edition, a field list, a query or code. */
const NOT_OURS: RegExp[] = [
  /\b(Modify All Data|View All Data|Manage Users|Author Apex|Customize Application|View Setup)\b/,
  /\b(Professional|Enterprise|Unlimited|Performance|Developer|Essentials) Edition\b$/,
  /^[A-Za-z_]+(\.[A-Za-z_]+)?(, [A-Za-z_]+(\.[A-Za-z_]+)?)+$/,
  /\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY)\b.*\b(FROM|WHERE|LIMIT)\b/,
  /[{};$`]|=>|--[a-z]/,
];

test("the extractors' texts keyed are exactly the listed ones", () => {
  assert.deepEqual(added, listed);
});

test("each is a sentence of ours: a capital first, three words or more, a full stop last, and none a name, an edition, a field list or code", () => {
  for (const t of added) {
    assert.ok(isSentence(t), t);
    for (const p of NOT_OURS) assert.ok(!p.test(t), `${t} (${p})`);
  }
});

test("the rule refuses what the review found keyed when the whole folder was", () => {
  for (const t of ["Modify All Data", "View All Data", "Manage Users", "Author Apex", "Performance Edition", "Tooling API", "open deals", "Best Practices", "Code Style", "Id, Name, OwnerId"]) assert.equal(isSentence(t), false, t);
});
