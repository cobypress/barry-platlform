import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { unzipSync } from "fflate";
import { XMLValidator } from "fast-xml-parser";
import { tempDir } from "../../testing/harness.ts";
import { documentXml, docxParagraphs, docxText } from "../../testing/docx-text.ts";
import { HANDBOOK_FILE } from "../build.ts";
import { renderHandbookDocx, STEP_WORDS, wordSafe } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { Handbook, HANDBOOK_VERSION, MAX_STEPS, type HandbookFlow, type HandbookStep, type HandbookValidationRule } from "../schema.ts";

// The org handbook as a Word document (DECISIONS.md 443 to 445). The file is
// made up here, so every route through the document is seen.

const RETRIEVE = { customObjects: "all" as const, standardObjects: ["Account", "Contact", "Lead", "Opportunity"] };

function step(name: string, over: Partial<HandbookStep> = {}): HandbookStep {
  return { kind: "assignments", name, label: name.replace(/_/g, " "), object: null, fields: [], fieldsTotal: 0, detail: null, outcomes: [], next: null, fault: null, reached: true, ...over };
}

function flow(name: string, over: Partial<HandbookFlow> = {}): HandbookFlow {
  return {
    name,
    label: name.replace(/_/g, " "),
    status: "Active",
    processType: "AutoLaunchedFlow",
    description: null,
    start: { object: "Opportunity", triggerType: "RecordAfterSave", recordTriggerType: "Update", schedule: null, entry: { logic: null, formula: null, formulaCut: false, formulaLength: null, conditions: [], conditionsTotal: 0, cut: false } },
    steps: [],
    stepsTotal: 0,
    cut: false,
    ...over,
  };
}

function rule(name: string, over: Partial<HandbookValidationRule> = {}): HandbookValidationRule {
  return { object: "Opportunity", name, active: true, description: null, formula: "ISBLANK(Amount)", formulaCut: false, formulaLength: 15, message: "Give an amount.", messageField: "Amount", ...over };
}

function handbook(over: Partial<Handbook> = {}): Handbook {
  return Handbook.parse({ version: HANDBOOK_VERSION, flows: { status: "ok", items: [] }, validationRules: { status: "ok", covers: RETRIEVE, items: [] }, ...over });
}

const OPTIONS = { clientName: "Synthetic Co", runDate: "2026-10-31T00:00:00.000Z" };

async function render(h: Handbook, options = OPTIONS): Promise<{ bytes: Buffer; xml: string; text: string; paragraphs: ReturnType<typeof docxParagraphs> }> {
  const bytes = await renderHandbookDocx(h, options);
  return { bytes, xml: documentXml(bytes), text: docxText(bytes), paragraphs: docxParagraphs(bytes) };
}

/** Characters XML 1.0 can't hold. One in the document makes Word call the file corrupt. */
const NOT_XML = /[\u0000-\u0008\u000B\u000C\u000E-\u001F￾￿]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/** The heading texts at one level, in order. */
const headings = (paragraphs: ReturnType<typeof docxParagraphs>, level: number): string[] => paragraphs.filter((p) => p.heading === level).map((p) => p.text);

/** The step lines of the flow under a heading, in order: each starts with its number. */
function stepLines(text: string, flowLabel: string): string[] {
  const part = text.slice(text.indexOf(`### ${flowLabel}\n`)).split(/\n### /)[0]!;
  return part.slice(part.indexOf("\nSteps: ")).split("\n\n").filter((p) => /^\d+\. /.test(p));
}

describe("the handbook as a Word document", () => {
  test("is a real Word file whose XML parses", async () => {
    const { bytes, xml } = await render(handbook({ flows: { status: "ok", items: [flow("A_Flow", { steps: [step("One")], stepsTotal: 1 })] } }));
    assert.equal(bytes.subarray(0, 2).toString(), "PK");
    const files = Object.keys(unzipSync(new Uint8Array(bytes)));
    for (const part of ["word/document.xml", "[Content_Types].xml", "word/styles.xml"]) assert.ok(files.includes(part), part);
    assert.equal(XMLValidator.validate(xml), true);
  });

  test("opens with what it is, the run's date and what it covers, counted from the file", async () => {
    const h = handbook({ flows: { status: "ok", items: [flow("One"), flow("Two", { status: "Draft" })] }, validationRules: { status: "ok", covers: RETRIEVE, items: [rule("R1"), rule("R2", { active: false }), rule("R3")] } });
    const { paragraphs, text } = await render(h);
    assert.deepEqual(headings(paragraphs, 1), ["Org handbook: Synthetic Co"]);
    assert.deepEqual(headings(paragraphs, 2), ["What it covers", "Flows", "Validation rules"]);
    assert.match(text, /We read it from your org on 31 Oct 2026/);
    assert.match(text, /Flows: 2 flows, 1 of them active\./);
    assert.match(text, /Validation rules: 3 validation rules, 2 of them active, on your custom objects and Account, Contact, Lead and Opportunity\./);
    // Plain words, no emoji.
    assert.doesNotMatch(text, /[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u);
  });

  test("lists active flows first, then the rest, each by label", async () => {
    const items = [flow("Z_Active", { label: "zebra" }), flow("Draft_A", { label: "Apple", status: "Draft" }), flow("B_Active", { label: "Banana" }), flow("Obsolete_C", { label: "Cherry", status: "Obsolete" }), flow("A_Active", { label: "apricot" })];
    const { paragraphs } = await render(handbook({ flows: { status: "ok", items } }));
    assert.deepEqual(headings(paragraphs, 3), ["apricot", "Banana", "zebra", "Apple", "Cherry"]);
  });

  test("numbers the steps in the file's order, with those no path reaches last and marked", async () => {
    const steps = [
      step("Get_Account", { kind: "recordLookups", object: "Account", next: "Check" }),
      step("Check", { kind: "decisions", outcomes: [{ label: "Big", next: "Set_Big" }, { label: "Small", next: null }] }),
      step("Set_Big", { fault: "Get_Account" }),
      step("Orphan", { reached: false }),
      step("Orphan_Two", { reached: false, next: "Nowhere_Step" }),
    ];
    const { text } = await render(handbook({ flows: { status: "ok", items: [flow("Review", { steps, stepsTotal: 5 })] } }));
    const lines = stepLines(text, "Review");
    assert.deepEqual(lines.map((l) => l.split("\n")[0]), [
      "1. Get records: Get Account  Get_Account",
      "2. Decision: Check",
      "3. Assignment: Set Big  Set_Big",
      "4. Assignment: Orphan  (no path reaches this step)",
      "5. Assignment: Orphan Two  Orphan_Two  (no path reaches this step)",
    ]);
    assert.match(lines[0]!, /\nOn: Account\nThen: step 2, Check$/);
    assert.match(lines[1]!, /\nBig: step 3, Set Big\nSmall: the flow ends here$/);
    assert.match(lines[2]!, /\nThen: the flow ends here\nIf it fails: step 1, Get Account$/);
    assert.match(lines[4]!, /Then: Nowhere_Step, which isn't among the steps listed here/);
    // Said once, before the first of them.
    assert.equal(text.match(/No path from the flow's start reaches the steps below/g)?.length, 1);
    assert.ok(text.indexOf("No path from the flow's start") < text.indexOf("4. Assignment: Orphan"));
    assert.ok(text.indexOf("No path from the flow's start") > text.indexOf("3. Assignment: Set Big"));
  });

  test("a file with an unreached step in the middle still lists it last, numbered after the rest", async () => {
    const steps = [step("First"), step("Lost", { reached: false }), step("Second")];
    const { text } = await render(handbook({ flows: { status: "ok", items: [flow("Mixed", { steps, stepsTotal: 3 })] } }));
    assert.deepEqual(stepLines(text, "Mixed").map((l) => l.split("\n")[0]), ["1. Assignment: First", "2. Assignment: Second", "3. Assignment: Lost  (no path reaches this step)"]);
  });

  test("words each kind of step the engine knows, and an unknown kind under Salesforce's own name", async () => {
    const kinds = [...Object.keys(STEP_WORDS), "promptSteps", "someFutureKind"];
    const steps = kinds.map((kind, i) => step(`S${i}`, { kind, label: `Step ${i}` }));
    const { text } = await render(handbook({ flows: { status: "ok", items: [flow("Kinds", { steps, stepsTotal: steps.length })] } }));
    const lines = stepLines(text, "Kinds").map((l) => l.split("\n")[0]);
    assert.equal(lines.length, kinds.length);
    kinds.forEach((kind, i) => assert.ok(lines[i]!.startsWith(`${i + 1}. ${STEP_WORDS[kind] ?? kind}: Step ${i}`), lines[i]));
    assert.ok(lines.some((l) => l.startsWith(`${kinds.length - 1}. promptSteps: `)));
    assert.ok(lines.some((l) => l.startsWith(`${kinds.length}. someFutureKind: `)));
    assert.match(text, /A kind of step we have no plain name for appears under Salesforce's own name for it\./);
  });

  test("a name from the file that happens to be a JavaScript word prints as written", async () => {
    const entry = { logic: "constructor", formula: null, formulaCut: false, formulaLength: null, conditions: [{ field: "Amount", operator: "toString", value: "1", valueCut: false, valueLength: 1 }], conditionsTotal: 1, cut: false };
    const odd = flow("Odd", { status: "__proto__", processType: "hasOwnProperty", start: { ...flow("x").start, recordTriggerType: "valueOf", entry }, steps: [step("S", { kind: "constructor", detail: "x" })], stepsTotal: 1 });
    const { text } = await render(handbook({ flows: { status: "ok", items: [odd] } }));
    for (const line of ["Status: __proto__", "Type: hasOwnProperty", "is saved (Salesforce's trigger: valueOf)", "Combined as: constructor", "1. Amount toString 1", "1. constructor: S", "Uses: x"]) assert.ok(text.includes(line), line);
  });

  test("says what starts a flow, its schedule and its entry conditions", async () => {
    const items = [
      flow("Before", { start: { ...flow("x").start, object: "Lead", triggerType: "RecordBeforeSave", recordTriggerType: "Create" } }),
      flow("Weekly", { start: { ...flow("x").start, object: null, triggerType: "Scheduled", recordTriggerType: null, schedule: { frequency: "Weekly", startDate: "2026-11-02", startTime: "07:00:00.000Z" } } }),
      flow("Screen", { processType: "Flow", start: { ...flow("x").start, object: null, triggerType: null, recordTriggerType: null } }),
      flow("Strange", { start: { ...flow("x").start, object: null, triggerType: "Segment", recordTriggerType: null } }),
      flow("Gated", {
        start: {
          ...flow("x").start,
          entry: { logic: "and", formula: null, formulaCut: false, formulaLength: null, conditions: [{ field: "StageName", operator: "EqualTo", value: "Closed Won", valueCut: false, valueLength: 10 }, { field: "Amount", operator: "Between", value: null, valueCut: false, valueLength: null }], conditionsTotal: 2, cut: false },
        },
      }),
    ];
    const { text } = await render(handbook({ flows: { status: "ok", items } }));
    assert.match(text, /What starts it: When a Lead record is created, before the change is saved\./);
    assert.match(text, /What starts it: When an Opportunity record is updated, after the change is saved\./);
    assert.match(text, /What starts it: On a schedule\.\n\nSchedule: Weekly, from 2 Nov 2026, at 07:00 UTC\./);
    assert.match(text, /Type: Screen flow \(Flow\)\n\nDescription: none given\n\nWhat starts it: Its start names no trigger, so its metadata doesn't say what runs it\./);
    assert.match(text, /What starts it: Segment, as Salesforce names it\./);
    assert.match(text, /Entry conditions: 2 conditions\.\n\nCombined as: all of them must be met\n\n1\. StageName equals Closed Won\n2\. Amount Between \(no value\)/);
    assert.equal(text.match(/Entry conditions: none/g)?.length, 4);
  });

  test("says each cut with its full count: steps, fields, conditions, formula and value", async () => {
    const steps = Array.from({ length: MAX_STEPS }, (_, i) => step(`S${i}`, i === 0 ? { fields: ["A__c", "B__c"], fieldsTotal: 62 } : {}));
    const long = "X".repeat(4_000);
    const cutFlow = flow("Huge", {
      steps,
      stepsTotal: 612,
      cut: true,
      start: {
        ...flow("x").start,
        entry: { logic: null, formula: long, formulaCut: true, formulaLength: 5_120, conditions: [{ field: "Description", operator: "Contains", value: long, valueCut: true, valueLength: 9_000 }], conditionsTotal: 73, cut: true },
      },
    });
    const rules = [rule("Long_Rule", { formula: long, formulaCut: true, formulaLength: 6_001 })];
    const { text } = await render(handbook({ flows: { status: "ok", items: [cutFlow] }, validationRules: { status: "ok", covers: RETRIEVE, items: rules } }));
    assert.match(text, /Steps: 612 steps\. This lists the first 500 of 612 steps, in the order the flow runs them; the rest aren't in this handbook\./);
    assert.match(text, /Fields: A__c, B__c \[cut: the first 2 of 62 fields\]/);
    assert.match(text, /Entry conditions: 73 conditions\. This lists the first 1 of 73 conditions\./);
    assert.match(text, /Entry formula: cut: this is the first 4,000 of 5,120 characters\./);
    assert.match(text, /1\. Description contains X+ \[cut: the first 4,000 of 9,000 characters\]/);
    assert.match(text, /Formula: cut: this is the first 4,000 of 6,001 characters\./);
    assert.equal(stepLines(text, "Huge").length, MAX_STEPS);
  });

  test("a part that couldn't be read says so with the reason, an empty one says the org has none, and the two never look alike", async () => {
    const unavailable = await render(handbook({ flows: { status: "unavailable", reason: "Metadata couldn't be retrieved: INSUFFICIENT_ACCESS" }, validationRules: { status: "unavailable", reason: "The retrieve timed out" } }));
    const empty = await render(handbook());
    assert.match(unavailable.text, /We couldn't read your flows, so this handbook doesn't list them\. Metadata couldn't be retrieved: INSUFFICIENT_ACCESS\./);
    assert.match(unavailable.text, /We couldn't read your validation rules, so this handbook doesn't list them\. The retrieve timed out\./);
    assert.match(empty.text, /Flows: your org has none\./);
    assert.match(empty.text, /Your org has no flows: there were none to read, active or not\./);
    assert.match(empty.text, /Your org has no validation rules on your custom objects and Account, Contact, Lead and Opportunity: there were none to read, active or not\./);
    assert.doesNotMatch(unavailable.text, /has no flows|has no validation rules|has none/);
    assert.doesNotMatch(empty.text, /couldn't read your/);
    // Each part's body says it, not only the first page.
    const flowsBody = unavailable.text.slice(unavailable.text.indexOf("## Flows"));
    assert.match(flowsBody.split("## Validation rules")[0]!, /couldn't read your flows/);
  });

  test("the validation rules open with what they cover, from the file: a list on the retrieve route, every object on Tooling", async () => {
    const items = [rule("R1")];
    const retrieve = await render(handbook({ validationRules: { status: "ok", covers: RETRIEVE, items } }));
    assert.match(retrieve.text, /## Validation rules\n\nThese are the validation rules on your custom objects and Account, Contact, Lead and Opportunity\. We didn't read the rules on your other standard objects/);
    const tooling = await render(handbook({ validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: "all" }, items } }));
    assert.match(tooling.text, /## Validation rules\n\nThese are the validation rules on every object\.\n/);
    assert.doesNotMatch(tooling.text, /other standard objects/);
    // Built from the file, never written in: a different list says that list.
    const other = await render(handbook({ validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: ["Case"] }, items } }));
    assert.match(other.text, /These are the validation rules on your custom objects and Case\./);
    const none = await render(handbook({ validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: [] }, items } }));
    assert.match(none.text, /These are the validation rules on your custom objects\./);
  });

  test("validation rules are grouped by object, each with what it checks and where its message shows", async () => {
    const items = [rule("Zed", { object: "Account", active: false, message: null, messageField: null, description: "Kept for audit." }), rule("Needs_Amount"), rule("Alpha", { object: "Account", formula: null, formulaLength: null })];
    const { text, paragraphs } = await render(handbook({ validationRules: { status: "ok", covers: RETRIEVE, items } }));
    assert.deepEqual(headings(paragraphs, 3), ["Account: 2 rules", "Opportunity: 1 rule"]);
    assert.deepEqual(headings(paragraphs, 4), ["Alpha", "Zed", "Needs_Amount"]);
    assert.match(text, /#### Alpha\n\nActive: Yes\n\nDescription: none given\n\nFormula: we couldn't read this rule's formula\./);
    assert.match(text, /#### Zed\n\nActive: No\n\nDescription: Kept for audit\.\n\nFormula: \n\nISBLANK\(Amount\)\n\nError message: none in what we read\n\nWhere the message shows: at the top of the page/);
    assert.match(text, /Error message: Give an amount\.\n\nWhere the message shows: beside the Amount field/);
  });

  test("the admin's text prints as written, a formula in a fixed-width face", async () => {
    const formula = `AND(\n\tAmount < 100 && Name <> "A & B",\n  CONTAINS(Description, '<b>')\n)`;
    const items = [rule("Quoted", { formula, formulaLength: formula.length, description: `Stops "cheap" deals < £100 & 'tiny' ones`, message: `Don't save <this> & "that"` })];
    const flows = [flow("F", { description: `Runs when x > 5 & y < "3"`, steps: [step("S", { label: `Set "A" & <B>` })], stepsTotal: 1 })];
    const { text, xml, paragraphs } = await render(handbook({ flows: { status: "ok", items: flows }, validationRules: { status: "ok", covers: RETRIEVE, items } }));
    assert.ok(text.includes(`Description: Stops "cheap" deals < £100 & 'tiny' ones`));
    assert.ok(text.includes(`Error message: Don't save <this> & "that"`));
    assert.ok(text.includes(`Description: Runs when x > 5 & y < "3"`));
    assert.ok(text.includes(`1. Assignment: Set "A" & <B>`));
    // Each of the formula's lines kept, its tab as spaces, in Courier New.
    const code = paragraphs.find((p) => p.text.startsWith("AND("))!;
    assert.equal(code.text, `AND(\n    Amount < 100 && Name <> "A & B",\n  CONTAINS(Description, '<b>')\n)`);
    assert.match(xml, /<w:rFonts w:ascii="Courier New"[^>]*\/>(?:(?!<\/w:r>).)*<w:t xml:space="preserve">AND\(<\/w:t>/s);
    assert.equal(XMLValidator.validate(xml), true);
  });

  test("characters XML can't hold don't make a corrupt file: each shows as a replacement character", async () => {
    const bad = "A\u0000B\u0001C\u0008D\u000BE\u001FF￾G￿H\uD800I\uDC00J";
    const items = [rule(`Rule${bad}`, { formula: `ISBLANK(${bad})`, formulaLength: 30, description: bad, message: bad, messageField: `F${bad}` })];
    const flows = [flow(`Flow${bad}`, { label: `Label${bad}`, description: bad, steps: [step(`S${bad}`, { label: bad, kind: `k${bad}`, fields: [bad], fieldsTotal: 1, detail: bad })], stepsTotal: 1 })];
    const { bytes, xml, text } = await render(handbook({ flows: { status: "ok", items: flows }, validationRules: { status: "ok", covers: RETRIEVE, items } }), { clientName: `Synthetic${bad}Co`, runDate: OPTIONS.runDate });
    // Every part of the package is XML Word can read.
    for (const [name, data] of Object.entries(unzipSync(new Uint8Array(bytes)))) {
      if (!name.endsWith(".xml") && !name.endsWith(".rels")) continue;
      const part = new TextDecoder().decode(data);
      assert.doesNotMatch(part, NOT_XML, name);
      assert.equal(XMLValidator.validate(part), true, name);
    }
    assert.doesNotMatch(xml, NOT_XML);
    const shown = "A�B�C�D�E�F�G�H�I�J";
    assert.ok(text.includes(`Description: ${shown}`), "the admin's text, with each bad character replaced and none dropped");
    assert.ok(text.includes(`ISBLANK(${shown})`));
    assert.ok(text.includes(`# Org handbook: Synthetic${shown}Co`));
    // Well-formed pairs are left alone.
    assert.equal(wordSafe("a😀b\tc\nd"), "a😀b\tc\nd");
  });
});

describe("reading handbook.json", () => {
  const folder = (contents?: string): string => {
    const dir = join(tempDir(), "run");
    mkdirSync(dir, { recursive: true });
    if (contents !== undefined) writeFileSync(join(dir, HANDBOOK_FILE), contents);
    return dir;
  };

  test("gives the handbook when the file fits the format", () => {
    const h = handbook({ flows: { status: "ok", items: [flow("One")] } });
    const read = readHandbook(folder(JSON.stringify(h)));
    assert.ok(read.ok);
    assert.deepEqual(read.handbook, h);
  });

  test("says there's none when there's no file, and that it doesn't fit when it doesn't", () => {
    assert.deepEqual(readHandbook(folder()), { ok: false, why: "missing" });
    for (const contents of ["", "{", "null", "[]", JSON.stringify({ ...handbook(), version: 99 }), JSON.stringify({ ...handbook(), extra: true }), JSON.stringify({ ...handbook(), flows: { status: "ok" } })]) {
      assert.deepEqual(readHandbook(folder(contents)), { ok: false, why: "format" }, contents);
    }
  });

  test("a file it can't open is unreadable, not missing", (t) => {
    if (process.getuid?.() === 0) return t.skip("root reads any file");
    const dir = folder(JSON.stringify(handbook()));
    chmodSync(join(dir, HANDBOOK_FILE), 0o000);
    try {
      assert.deepEqual(readHandbook(dir), { ok: false, why: "unreadable" });
    } finally {
      chmodSync(join(dir, HANDBOOK_FILE), 0o600);
    }
  });
});

describe("a very large org", () => {
  test("300 flows of 100 steps each and 2,000 validation rules make a document", async () => {
    const flows = Array.from({ length: 300 }, (_, f) =>
      flow(`Flow_${f}`, {
        status: f % 3 ? "Active" : "Draft",
        description: `Flow ${f} keeps the pipeline tidy.`,
        steps: Array.from({ length: 100 }, (_, s) =>
          step(`Step_${s}`, { kind: s % 2 ? "decisions" : "recordUpdates", object: "Opportunity", fields: ["StageName", "Amount", "CloseDate"], fieldsTotal: 3, next: s < 99 ? `Step_${s + 1}` : null, outcomes: s % 2 ? [{ label: "Yes", next: `Step_${Math.min(s + 2, 99)}` }] : [], fault: s % 5 ? null : "Step_99" }),
        ),
        stepsTotal: 100,
      }),
    );
    const rules = Array.from({ length: 2_000 }, (_, i) => rule(`Rule_${i}`, { object: `Object_${i % 40}__c`, formula: `AND(ISCHANGED(Status__c), ISBLANK(Field_${i}__c), Amount__c > ${i})`, formulaLength: 60, description: "Keeps the data complete." }));
    const h = Handbook.parse({ version: HANDBOOK_VERSION, flows: { status: "ok", items: flows }, validationRules: { status: "ok", covers: RETRIEVE, items: rules } });
    const started = performance.now();
    const bytes = await renderHandbookDocx(h, OPTIONS);
    const ms = Math.round(performance.now() - started);
    const paragraphs = docxParagraphs(bytes);
    console.log(`# a very large org: ${ms} ms, ${(bytes.length / 1_048_576).toFixed(1)} MB, ${paragraphs.length} paragraphs`);
    assert.equal(paragraphs.filter((p) => p.heading === 3).length, 300 + 40);
    assert.equal(paragraphs.filter((p) => p.heading === 4).length, 2_000);
    assert.equal(paragraphs.filter((p) => /^\d+\. (Decision|Update records): /.test(p.text)).length, 300 * 100);
    // Well inside what the documents job and Drive take, and quick enough not to hold a make up.
    assert.ok(bytes.length < 20 * 1_048_576, `${bytes.length} bytes`);
    assert.ok(ms < 120_000, `${ms} ms`);
  });
});
