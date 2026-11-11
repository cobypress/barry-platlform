import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { MODELS } from "../../config.ts";
import { APEX_SOURCE_MAX_ITEMS } from "../../extract/apex-source.ts";
import { summaryCountWords } from "../docx.ts";
import { Handbook, HANDBOOK_VERSION, type HandbookApexClass, type HandbookApexTrigger } from "../schema.ts";
import { HandbookSummaries, SUMMARY_LIMITS } from "../summaries.ts";
import { CONSOLE_SUMMARY_LIMITS, ConsoleSummariesRow, PRINT_MAX, printableSummaries, printableText, storedFromRow, waitingRow, type StoredSummaries, type SummariesToSave } from "../summary-row.ts";

// Claude's summaries as the console keeps them (DECISIONS.md 494 and 495): the
// console's limits against everything the worker can write, and which
// approved summaries the handbook prints.

const LEFT_OUT = { tests: 0, tooLarge: 0, overLimit: 0, unreadable: 0, refused: 0, notWritten: 0 };
const MADE_AT = "2026-11-03T10:00:00.000Z";

function file(items: SummariesToSave["items"], over: Partial<SummariesToSave> = {}): SummariesToSave {
  return { model: MODELS.handbookSummaries, madeAt: MADE_AT, items, leftOut: LEFT_OUT, ...over };
}

const item = (n: number, over: Partial<SummariesToSave["items"][number]> = {}) => ({ kind: "class" as const, name: `Synthetic${String(n).padStart(4, "0")}`, summary: "Keeps the ledger in step.", ...over });

describe("the console's limits, against everything the worker can write", () => {
  test("are the console's, word for word, and the engine's own limits sit inside them", () => {
    assert.deepEqual(CONSOLE_SUMMARY_LIMITS, { items: 250, name: 255, summary: 1200, model: 100 });
    // Raising one of the engine's limits past the console's fails here, before a row the console can't read is ever written.
    assert.ok(APEX_SOURCE_MAX_ITEMS <= CONSOLE_SUMMARY_LIMITS.items);
    assert.ok(SUMMARY_LIMITS.name <= CONSOLE_SUMMARY_LIMITS.name);
    assert.ok(SUMMARY_LIMITS.characters <= CONSOLE_SUMMARY_LIMITS.summary);
    assert.ok(MODELS.handbookSummaries.length <= CONSOLE_SUMMARY_LIMITS.model);
    assert.equal(PRINT_MAX, SUMMARY_LIMITS.characters, "printing allows what the worker writes and no more");
  });

  test("the largest file the engine's own schema allows passes the console's schema as copied", () => {
    // 200 items; one with the longest name; one at 1,200 characters; one of 600 astral characters, which is 1,200 by .length, as both schemas count.
    const astral = "\u{1D49C}".repeat(600);
    assert.equal(astral.length, 1_200);
    const items = Array.from({ length: APEX_SOURCE_MAX_ITEMS }, (_, i) => item(i));
    items[0] = item(0, { name: "N".repeat(SUMMARY_LIMITS.name) });
    items[1] = item(1, { summary: "s".repeat(SUMMARY_LIMITS.characters) });
    items[2] = item(2, { kind: "class", summary: astral });
    const largest = HandbookSummaries.parse({ version: 1, ...file(items, { leftOut: { tests: 9, tooLarge: 9, overLimit: 9, unreadable: 9, refused: 9, notWritten: 9 } }) });
    const row = waitingRow("hc-2026-10-31-00000001", largest);
    assert.ok(row.ok);
    assert.equal(row.row.items.length, 200);
    assert.ok(row.row.items.every((i) => i.struck === false));
    assert.equal(row.row.items[2]!.summary, astral);
    assert.ok(ConsoleSummariesRow.safeParse(row.row).success);
  });

  test("one over each limit is refused, and only the count of problems comes back", () => {
    const over: Array<[string, SummariesToSave]> = [
      ["251 items", file(Array.from({ length: 251 }, (_, i) => item(i)))],
      ["a 256-character name", file([item(1, { name: "N".repeat(256) })])],
      ["a 1,201-character summary", file([item(1, { summary: "s".repeat(1_201) })])],
      ["a 101-character model", file([item(1)], { model: "m".repeat(101) })],
    ];
    for (const [what, summaries] of over) {
      const row = waitingRow("hc-2026-10-31-00000001", summaries);
      assert.ok(!row.ok, what);
      assert.deepEqual(Object.keys(row), ["ok", "issues"], what);
      assert.ok(row.issues >= 1, what);
    }
    // And at each limit, it's taken.
    for (const summaries of [file(Array.from({ length: 250 }, (_, i) => item(i))), file([item(1, { name: "N".repeat(255) })]), file([item(1, { summary: "s".repeat(1_200) })]), file([item(1)], { model: "m".repeat(100) })]) {
      assert.ok(waitingRow("hc-2026-10-31-00000001", summaries).ok);
    }
  });

  test("the console's rule on who decided: waiting exactly when nobody and no time did", () => {
    const row = (status: string, decidedBy: string | null, decidedAt: string | null) => ConsoleSummariesRow.safeParse({ runId: "r", status, model: "m", madeAt: MADE_AT, items: [], leftOut: LEFT_OUT, decidedBy, decidedAt }).success;
    assert.ok(row("waiting", null, null));
    assert.ok(row("approved", "consultant@example.com", MADE_AT));
    assert.ok(!row("approved", null, null));
    assert.ok(!row("waiting", "consultant@example.com", MADE_AT));
    assert.ok(!row("left_out", "consultant@example.com", null));
  });

  test("a row read back gives its status, items and counts, never who decided it", () => {
    const read = storedFromRow({ runId: "r", status: "approved", model: "m", madeAt: MADE_AT, items: [{ ...item(1), struck: true }], leftOut: LEFT_OUT });
    assert.ok(read.ok);
    assert.deepEqual(Object.keys(read.stored).sort(), ["items", "leftOut", "status"]);
    // A key the console's schema doesn't have, a missing one, or a value outside it: none.
    for (const bad of [{ decidedBy: "consultant@example.com" }, { status: "done" }, { items: [item(1)] }, { model: "" }, { madeAt: "yesterday" }]) {
      assert.ok(!storedFromRow({ runId: "r", status: "approved", model: "m", madeAt: MADE_AT, items: [], leftOut: LEFT_OUT, ...bad }).ok, JSON.stringify(bad));
    }
  });
});

// A handbook with two classes and a trigger, one of them named as the class is.
const apexClass = (name: string): HandbookApexClass => ({ name, apiVersion: 62, status: "Active", isTest: false, touches: null, lastChanged: null });
const apexTrigger = (name: string): HandbookApexTrigger => ({ ...apexClass(name), object: "Account", events: ["before insert"] });
function handbook(classes: string[] = ["LeadRouter", "Shared"], triggers: string[] = ["Shared"]): Handbook {
  return Handbook.parse({
    version: HANDBOOK_VERSION,
    readBy: { metadata: null, apex: null, build: null },
    flows: { status: "ok", items: [], lastChanges: "read" },
    validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: [] }, items: [], lastChanges: "read" },
    apexClasses: { status: "ok", items: classes.map(apexClass), total: classes.length, cut: false, lastChanges: "read" },
    apexTriggers: { status: "ok", items: triggers.map(apexTrigger), total: triggers.length, cut: false, lastChanges: "read" },
    recentChanges: { status: "ok", items: [], total: 0, cut: false, leftOutUserAdmin: 0, days: 180 },
  });
}

const stored = (items: StoredSummaries["items"], status: StoredSummaries["status"] = "approved", leftOut = LEFT_OUT): StoredSummaries => ({ status, items, leftOut });
const one = (kind: "class" | "trigger", name: string, summary = "Routes each new lead.", struck = false) => ({ kind, name, summary, struck });

describe("which approved summaries the handbook prints", () => {
  test("none unless a person approved the row", () => {
    const items = [one("class", "LeadRouter")];
    assert.equal(printableSummaries(handbook(), null), null);
    assert.equal(printableSummaries(handbook(), undefined), null);
    assert.equal(printableSummaries(handbook(), stored(items, "waiting")), null);
    assert.equal(printableSummaries(handbook(), stored(items, "left_out")), null);
    assert.deepEqual(printableSummaries(handbook(), stored(items))!.items, [{ kind: "class", name: "LeadRouter", summary: "Routes each new lead." }]);
  });

  test("one struck out isn't printed, and is counted with the rest", () => {
    const printed = printableSummaries(handbook(), stored([one("class", "LeadRouter"), one("trigger", "Shared", "Checks each account.", true)]))!;
    assert.deepEqual(printed.items.map((i) => i.name), ["LeadRouter"]);
    assert.equal(printed.counts.rest, 1);
    assert.equal(printableSummaries(handbook(), stored([one("class", "LeadRouter", "x", true)])), null, "all struck prints none");
  });

  test("text that can't be printed as it stands isn't, and each is counted with the rest", () => {
    for (const summary of ["Runs for [[user:U0003]] alone.", "Runs for [[user-9999]].", "Masks [[id]].", "[[email]]", "Two\nlines.", "Two\r\nlines.", "Two\u2028lines.", "   ", "s".repeat(1_201)]) {
      assert.ok(!printableText(summary), JSON.stringify(summary.slice(0, 40)));
      const printed = printableSummaries(handbook(), stored([one("class", "LeadRouter"), one("class", "Shared", summary)]))!;
      assert.deepEqual(printed.items.map((i) => i.name), ["LeadRouter"], JSON.stringify(summary.slice(0, 40)));
      assert.equal(printed.counts.rest, 1);
    }
    assert.ok(printableText("s".repeat(1_200)));
    assert.ok(printableText("A single square bracket [ is fine, as is ]]."));
  });

  test("a summary is printed only against exactly one entry of its own kind", () => {
    const h = handbook(["LeadRouter", "Shared"], ["Shared", "OnlyATrigger"]);
    const printed = printableSummaries(h, stored([one("class", "LeadRouter"), one("class", "NotInTheHandbook"), one("class", "OnlyATrigger"), one("trigger", "LeadRouter"), one("trigger", "Shared", "The trigger's own.")]))!;
    assert.deepEqual(printed.items, [{ kind: "class", name: "LeadRouter", summary: "Routes each new lead." }, { kind: "trigger", name: "Shared", summary: "The trigger's own." }]);
    assert.equal(printed.counts.rest, 3);
    // Names are matched exactly: Apex is case-blind, but the file and the row both hold Salesforce's own spelling.
    assert.equal(printableSummaries(h, stored([one("class", "leadrouter")])), null);
  });

  test("two entries alike in handbook.json, or two summaries for one entry, print neither", () => {
    // Salesforce can't give two classes one name in the code we read (no namespace), so only a wrong file has this.
    const twice = handbook(["LeadRouter", "LeadRouter", "Shared"]);
    const printed = printableSummaries(twice, stored([one("class", "LeadRouter"), one("class", "Shared")]))!;
    assert.deepEqual(printed.items.map((i) => i.name), ["Shared"]);
    assert.equal(printed.counts.rest, 1);
    const doubled = printableSummaries(handbook(), stored([one("class", "LeadRouter", "First."), one("class", "LeadRouter", "Second."), one("class", "Shared")]))!;
    assert.deepEqual(doubled.items.map((i) => i.name), ["Shared"]);
    assert.equal(doubled.counts.rest, 2);
  });

  test("a part the run couldn't read matches nothing", () => {
    const h = { ...handbook(), apexClasses: { status: "unavailable" as const, reason: "Salesforce refused the read." } };
    assert.equal(printableSummaries(h, stored([one("class", "LeadRouter")])), null);
  });

  test("the counts come from the row: its own reasons, and the rest with everything not printed", () => {
    const printed = printableSummaries(handbook(), stored([one("class", "LeadRouter"), one("class", "Shared", "x", true), one("class", "Gone")], "approved", { tests: 4, tooLarge: 1, overLimit: 12, unreadable: 2, refused: 1, notWritten: 3 }))!;
    assert.deepEqual(printed.counts, { tests: 4, tooLarge: 1, overLimit: 12, rest: 2 + 1 + 3 + 2 });
  });
});

describe("the counts, in words", () => {
  const words = (n: number, counts: { tests?: number; tooLarge?: number; overLimit?: number; rest?: number }) =>
    summaryCountWords({ items: Array.from({ length: n }, (_, i) => ({ kind: "class" as const, name: `C${i}`, summary: "s" })), counts: { tests: 0, tooLarge: 0, overLimit: 0, rest: 0, ...counts } });

  test("one summarised and nothing else: no clause left hanging", () => {
    assert.equal(words(1, {}), "Of the Apex classes and triggers we read, 1 carries a summary.");
  });

  test("every count, and the limit taken from the code that keeps the code", () => {
    assert.equal(
      words(3, { tests: 4, tooLarge: 2, overLimit: 12, rest: 5 }),
      `Of the Apex classes and triggers we read, 3 carry a summary, and the others are 4 test classes, 2 that were too large to summarise whole, 12 beyond the ${APEX_SOURCE_MAX_ITEMS} largest and 5 that we couldn't summarise or chose to leave out.`,
    );
  });

  test("singular forms", () => {
    assert.equal(words(1, { tests: 1 }), "Of the Apex classes and triggers we read, 1 carries a summary, and the other is 1 test class.");
    assert.equal(words(2, { tooLarge: 1, rest: 1 }), "Of the Apex classes and triggers we read, 2 carry a summary, and the others are 1 that was too large to summarise whole and 1 that we couldn't summarise or chose to leave out.");
    assert.equal(words(1, { overLimit: 1 }), `Of the Apex classes and triggers we read, 1 carries a summary, and the other is 1 beyond the ${APEX_SOURCE_MAX_ITEMS} largest.`);
    assert.equal(words(1, { rest: 1 }), "Of the Apex classes and triggers we read, 1 carries a summary, and the other is 1 that we couldn't summarise or chose to leave out.");
  });
});
