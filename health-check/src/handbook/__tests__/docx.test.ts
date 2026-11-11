import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { unzipSync } from "fflate";
import { XMLValidator } from "fast-xml-parser";
import { tempDir } from "../../testing/harness.ts";
import { documentXml, docxParagraphs, docxText } from "../../testing/docx-text.ts";
import { HANDBOOK_FILE } from "../build.ts";
import { renderHandbookDocx, STEP_WORDS, whenWords, wordSafe } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { Handbook, HANDBOOK_VERSION, MAX_APEX_CLASSES, MAX_APEX_TRIGGERS, MAX_STEPS, type HandbookApexClass, type HandbookPath, type HandbookApexTouches, type HandbookApexTrigger, type HandbookFlowItem, type HandbookStep, type HandbookValidationRuleItem, type LastChanges } from "../schema.ts";

// The org handbook as a Word document (DECISIONS.md 443 to 445). The file is
// made up here, so every route through the document is seen.

// What the retrieve reads on the synthetic org: the four, and Case, which its record counts list (decision 460).
const RETRIEVE = { customObjects: "all" as const, standardObjects: ["Account", "Contact", "Lead", "Opportunity", "Case"] };
const NO_APEX = { status: "ok" as const, items: [], total: 0, cut: false, lastChanges: "read" as const };
const NO_CHANGES = { status: "ok" as const, items: [], total: 0, cut: false, leftOutUserAdmin: 0, days: 180 };

function step(name: string, over: Partial<HandbookStep> = {}): HandbookStep {
  return { kind: "assignments", name, label: name.replace(/_/g, " "), object: null, fields: [], fieldsTotal: 0, detail: null, outcomes: [], next: null, fault: null, reached: true, ...over };
}

function flow(name: string, over: Partial<HandbookFlowItem> = {}): HandbookFlowItem {
  return {
    name,
    label: name.replace(/_/g, " "),
    status: "Active",
    processType: "AutoLaunchedFlow",
    description: null,
    start: { object: "Opportunity", triggerType: "RecordAfterSave", recordTriggerType: "Update", schedule: null, entry: { logic: null, formula: null, formulaCut: false, formulaLength: null, conditions: [], conditionsTotal: 0, cut: false }, paths: [], pathsTotal: 0, pathsCut: false },
    steps: [],
    stepsTotal: 0,
    cut: false,
    lastChanged: null,
    ...over,
  };
}

function rule(name: string, over: Partial<HandbookValidationRuleItem> = {}): HandbookValidationRuleItem {
  return { object: "Opportunity", name, active: true, description: null, formula: "ISBLANK(Amount)", formulaCut: false, formulaLength: 15, message: "Give an amount.", messageField: "Amount", lastChanged: null, ...over };
}

function touches(over: Partial<HandbookApexTouches> = {}): HandbookApexTouches {
  return { queries: [], queriesTotal: 0, changes: [], dynamicQueries: false, searches: false, ...over };
}

function apexClass(name: string, over: Partial<HandbookApexClass> = {}): HandbookApexClass {
  return { name, apiVersion: 62, status: "Active", isTest: false, touches: touches(), lastChanged: null, ...over };
}

function trigger(name: string, over: Partial<HandbookApexTrigger> = {}): HandbookApexTrigger {
  return { ...apexClass(name), object: "Opportunity", events: ["before insert"], ...over };
}

/** An ok Apex part holding these, uncut, from a run that kept no last changes unless it says so. */
const apex = <T>(items: T[], lastChanges: LastChanges = "not_kept") => ({ status: "ok" as const, items, total: items.length, cut: false, lastChanges });

/** A part as a test gives it, which says whether its last changes were read only when that's what it tests. */
type Kept<P> = P extends { status: "ok"; lastChanges: LastChanges } ? Omit<P, "lastChanges"> & { lastChanges?: LastChanges } : P;
type Over = Omit<Partial<Handbook>, "flows" | "validationRules"> & { flows?: Kept<Handbook["flows"]>; validationRules?: Kept<Handbook["validationRules"]> };

/** From a run that kept no flow's or rule's last change, unless the part says so. */
const notKept = <P extends { status: string }>(part: P) => (part.status === "ok" && !("lastChanges" in part) ? { ...part, lastChanges: "not_kept" } : part);

function handbook(over: Over = {}): Handbook {
  const { flows = { status: "ok", items: [] }, validationRules = { status: "ok", covers: RETRIEVE, items: [] }, ...rest } = over;
  return Handbook.parse({ version: HANDBOOK_VERSION, readBy: { metadata: null, apex: null, build: null }, flows: notKept(flows), validationRules: notKept(validationRules), apexClasses: NO_APEX, apexTriggers: NO_APEX, recentChanges: NO_CHANGES, ...rest });
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
    assert.deepEqual(headings(paragraphs, 2), ["What it covers", "Flows", "Validation rules", "Apex classes", "Apex triggers", "Recent changes"]);
    assert.match(text, /We read it from your org on 31 Oct 2026/);
    assert.match(text, /Flows: 2 flows, 1 of them active\./);
    assert.match(text, /Validation rules: 3 validation rules, 2 of them active, on your custom objects and Account, Contact, Lead, Opportunity and Case\./);
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

  test("a flow whose start names no first step says its metadata doesn't say where it starts, and marks no step", async () => {
    // detail.ts marks every step unreached when the start names none: that's the file's silence, not a fact about the flow.
    const steps = [step("Assign_Lead", { next: "Notify" }), step("Notify")].map((s) => ({ ...s, reached: false }));
    const items = [flow("No_Start", { steps, stepsTotal: 2 }), flow("Some_Reached", { steps: [step("First"), step("Lost", { reached: false })], stepsTotal: 2 })];
    const { text } = await render(handbook({ flows: { status: "ok", items } }));
    const noStart = text.slice(text.indexOf("### No Start\n")).split(/\n### /)[0]!;
    assert.match(noStart, /\nSteps: 2 steps\.\n\nThis flow's metadata doesn't say where it starts, so its steps are listed in the order Salesforce gives them\.\n\n1\. Assignment: Assign Lead  Assign_Lead\nThen: step 2, Notify\n\n2\. Assignment: Notify\nThen: the flow ends here/);
    assert.doesNotMatch(noStart, /no path reaches this step|No path from the flow's start|never run/);
    // A flow with a start that reaches some steps keeps the old words, once, and marks the rest.
    const some = text.slice(text.indexOf("### Some Reached\n")).split(/\n### /)[0]!;
    assert.match(some, /1\. Assignment: First\nThen: the flow ends here\n\nNo path from the flow's start reaches the steps below, so they never run as the flow stands\.\n\n2\. Assignment: Lost  \(no path reaches this step\)/);
    assert.doesNotMatch(some, /doesn't say where it starts/);
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

  describe("every way a flow starts (decision 446)", () => {
    const immediate = (next: string): HandbookPath => ({ kind: "immediate", label: null, when: null, next });
    const closeDate = { offsetNumber: -30, offsetUnit: "Days", timeSource: "RecordField", recordField: "CloseDate" };
    /** The adversary's flow: Set_Flag straight away, Email_Owner 30 days before the close date. */
    const reminder = flow("Close_Reminder", {
      start: { ...flow("x").start, recordTriggerType: "CreateAndUpdate", paths: [immediate("Set_Flag"), { kind: "scheduled", label: "30 days before close", when: closeDate, next: "Email_Owner" }], pathsTotal: 2 },
      steps: [step("Set_Flag"), step("Email_Owner", { kind: "actionCalls", detail: "emailSimple (emailSimple)" })],
      stepsTotal: 2,
    });

    test("the adversary's flow lists both paths under what starts it, and the email's step says it's the scheduled path's", async () => {
      const { text } = await render(handbook({ flows: { status: "ok", items: [reminder] } }));
      const part = text.slice(text.indexOf("### Close Reminder\n")).split(/\n## /)[0]!;
      assert.match(
        part,
        /\nWhat starts it: When an Opportunity record is created or updated, after the change is saved\.\n\nIt starts in 2 ways:\nRuns straight away: step 1, Set Flag\n'30 days before close': 30 days before CloseDate: step 2, Email Owner\n\nEntry conditions: none\n/,
      );
      const [first, second] = stepLines(text, "Close Reminder");
      assert.equal(first, "1. Assignment: Set Flag  Set_Flag\nStarts the path that runs straight away\nThen: the flow ends here");
      assert.equal(second, "2. Action: Email Owner  Email_Owner\nStarts the path '30 days before close': 30 days before CloseDate\nAction: emailSimple (emailSimple)\nThen: the flow ends here");
      // The order the steps come in is said once, for every flow.
      assert.match(text, /lists its steps path by path: the steps that run straight away first, then each later path's in turn, and the step each path starts at says so\./);
    });

    test("a flow with only its immediate path prints as a flow from before paths were kept", async () => {
      const steps = [step("Set_Flag", { next: "Notify" }), step("Notify")];
      const one = await render(handbook({ flows: { status: "ok", items: [flow("Plain", { start: { ...flow("x").start, paths: [immediate("Set_Flag")], pathsTotal: 1 }, steps, stepsTotal: 2 })] } }));
      const none = await render(handbook({ flows: { status: "ok", items: [flow("Plain", { steps, stepsTotal: 2 })] } }));
      assert.equal(one.text, none.text);
      assert.doesNotMatch(one.text, /It starts in|Starts the path|Runs straight away/);
    });

    test("a path after the save is committed, one Salesforce names a way we have no word for, and one with no step", async () => {
      const paths: HandbookPath[] = [
        immediate("A"),
        { kind: "afterCommit", label: null, when: null, next: "B" },
        { kind: "afterCommit", label: "Run async", when: null, next: "B" },
        { kind: "ApprovalRecall", label: null, when: null, next: "C" },
        { kind: "scheduled", label: null, when: { offsetNumber: 1, offsetUnit: "Hours", timeSource: "RecordTriggerEvent", recordField: null }, next: null },
        { kind: "scheduled", label: null, when: closeDate, next: "Gone" },
      ];
      const f = flow("Many", { start: { ...flow("x").start, paths, pathsTotal: paths.length }, steps: [step("A"), step("B"), step("C")], stepsTotal: 3 });
      const { text } = await render(handbook({ flows: { status: "ok", items: [f] } }));
      assert.ok(
        text.includes(
          "It starts in 6 ways:\nRuns straight away: step 1, A\nAfter the save is committed: step 2, B\n'Run async': after the save is committed: step 2, B\nA path Salesforce calls ApprovalRecall: step 3, C\nScheduled, 1 hour after the record is saved: it leads to no step\nScheduled, 30 days before CloseDate: Gone, which isn't among the steps listed here\n",
        ),
        text,
      );
      const lines = stepLines(text, "Many");
      assert.equal(lines[1], "2. Assignment: B\nStarts the path that runs after the save is committed\nStarts the path 'Run async': after the save is committed\nThen: the flow ends here");
      assert.equal(lines[2], "3. Assignment: C\nStarts a path Salesforce calls ApprovalRecall\nThen: the flow ends here");
    });

    test("paths cut at the most the file keeps say so with the full count", async () => {
      const paths = Array.from({ length: 50 }, (_, i): HandbookPath => (i === 0 ? immediate("A") : { kind: "scheduled", label: `P${i}`, when: closeDate, next: "A" }));
      const f = flow("Cut", { start: { ...flow("x").start, paths, pathsTotal: 73, pathsCut: true }, steps: [step("A")], stepsTotal: 1 });
      const { text } = await render(handbook({ flows: { status: "ok", items: [f] } }));
      assert.match(text, /It starts in 73 ways\. This lists the first 50 of 73 ways:\n/);
    });

    test("when a path runs is put in words only from what the file keeps", () => {
      const when = (over: Partial<NonNullable<HandbookPath["when"]>>) => whenWords({ ...closeDate, ...over });
      assert.equal(when({}), "30 days before CloseDate");
      assert.equal(when({ offsetNumber: 2, offsetUnit: "Weeks" }), "2 weeks after CloseDate");
      assert.equal(when({ offsetNumber: 1, offsetUnit: "Hours", timeSource: "RecordTriggerEvent", recordField: null }), "1 hour after the record is saved");
      assert.equal(when({ offsetNumber: 0, offsetUnit: "Minutes" }), "0 minutes after CloseDate");
      // A unit we don't know, or no number, isn't turned into words: the sign stays Salesforce's.
      assert.equal(when({ offsetUnit: "Fortnights" }), "as Salesforce records it: offset -30, unit Fortnights, counted from RecordField, field CloseDate");
      assert.equal(when({ offsetNumber: null }), "as Salesforce records it: unit Days, counted from RecordField, field CloseDate");
      // A time source we don't know, under Salesforce's own word for it.
      assert.equal(when({ timeSource: "RecordEvent" }), "30 days before what Salesforce calls RecordEvent (CloseDate)");
      assert.equal(when({ recordField: null }), "as Salesforce records it: offset -30, unit Days, counted from RecordField");
      // Nor is a field taken to be what it counts from when no time source says so.
      assert.equal(when({ timeSource: null }), "as Salesforce records it: offset -30, unit Days, field CloseDate");
      assert.equal(whenWords(null), "when it runs isn't recorded");
      assert.equal(whenWords({ offsetNumber: null, offsetUnit: null, timeSource: null, recordField: null }), "when it runs isn't recorded");
      // Words of the file's own that happen to be JavaScript's aren't units.
      assert.equal(when({ offsetUnit: "constructor" }), "as Salesforce records it: offset -30, unit constructor, counted from RecordField, field CloseDate");
    });
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
    const unavailable = await render(handbook({ flows: { status: "unavailable", reason: "Metadata couldn't be retrieved: INSUFFICIENT_ACCESS" }, validationRules: { status: "unavailable", reason: "The retrieve timed out" }, apexClasses: { status: "unavailable", reason: "x" }, apexTriggers: { status: "unavailable", reason: "x" } }));
    const empty = await render(handbook());
    assert.match(unavailable.text, /We couldn't read your flows, so this handbook doesn't list them\. Metadata couldn't be retrieved: INSUFFICIENT_ACCESS\./);
    assert.match(unavailable.text, /We couldn't read your validation rules, so this handbook doesn't list them\. The retrieve timed out\./);
    assert.match(empty.text, /Flows: your org has none\./);
    assert.match(empty.text, /Your org has no flows: there were none to read, active or not\./);
    assert.match(empty.text, /Your org has no validation rules on your custom objects and Account, Contact, Lead, Opportunity and Case: there were none to read, active or not\./);
    assert.doesNotMatch(unavailable.text, /has no flows|has no validation rules|has none/);
    assert.doesNotMatch(empty.text, /couldn't read your/);
    // Each part's body says it, not only the first page.
    const flowsBody = unavailable.text.slice(unavailable.text.indexOf("## Flows"));
    assert.match(flowsBody.split("## Validation rules")[0]!, /couldn't read your flows/);
  });

  test("the validation rules open with what they cover, from the file: a list on the retrieve route, every object on Tooling", async () => {
    const items = [rule("R1")];
    const retrieve = await render(handbook({ validationRules: { status: "ok", covers: RETRIEVE, items } }));
    assert.match(retrieve.text, /## Validation rules\n\nThese are the validation rules on your custom objects and Account, Contact, Lead, Opportunity and Case\. We didn't read the rules on your other standard objects/);
    // Where the record counts couldn't be read, or the wider retrieve failed, the four.
    const four = await render(handbook({ validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: ["Account", "Contact", "Lead", "Opportunity"] }, items } }));
    assert.match(four.text, /These are the validation rules on your custom objects and Account, Contact, Lead and Opportunity\. We didn't read the rules on your other standard objects/);
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

describe("the handbook's Apex", () => {
  /** The paragraph for one class or trigger: its name, then its lines. */
  const entry = (text: string, name: string): string => text.split("\n\n").find((p) => p.startsWith(`${name}\n`))!;

  test("comes after the validation rules, classes then triggers, each counted on the first page", async () => {
    const h = handbook({ apexClasses: apex([apexClass("Alpha"), apexClass("Beta", { status: "Inactive" }), apexClass("Gamma")]), apexTriggers: apex([trigger("OnDeal")]) });
    const { paragraphs, text } = await render(h);
    assert.deepEqual(headings(paragraphs, 2), ["What it covers", "Flows", "Validation rules", "Apex classes", "Apex triggers", "Recent changes"]);
    const covers = text.slice(text.indexOf("## What it covers"), text.indexOf("## Flows"));
    assert.match(covers, /Validation rules: your org has none on [^\n]+\n\nApex classes: 3 classes, 2 of them active\.\n\nApex triggers: 1 trigger, 1 of them active\.\n/);
    // In the file's order, which is by name: the document doesn't sort them again.
    const unsorted = await render(handbook({ apexClasses: apex([apexClass("Zed"), apexClass("Able")]) }));
    assert.ok(unsorted.text.indexOf("\nZed\n") < unsorted.text.indexOf("\nAble\n"));
    // What the reading is, said once, at the top of the classes; the triggers point to it.
    const classes = text.slice(text.indexOf("## Apex classes"), text.indexOf("## Apex triggers"));
    assert.match(classes, /^## Apex classes\n\nYour org has 3 classes, 2 of them active\. They're listed by name\.\n\nWhat each one touches is read from its code's text: the objects named in its queries, and the kinds of change it makes\. It doesn't say which object a change is made to[^\n]*it can miss what the code does through another class[^\n]*\n\nAlpha\n/);
    assert.equal(text.match(/read from its code's text/g)?.length, 1);
    assert.match(text, /## Apex triggers\n\nYour org has 1 trigger, 1 of them active\. They're listed by name\.\n\nEach trigger shows the object it's on and the events it runs on\. A custom object we couldn't name shows as the id Salesforce gives it\. What it touches is read as the classes' is, with the same limits\.\n\nOnDeal\n/);
    // With no class listed, the triggers say what the reading is themselves.
    const alone = await render(handbook({ apexClasses: { status: "unavailable", reason: "No Apex" }, apexTriggers: apex([trigger("OnDeal")]) }));
    assert.match(alone.text.slice(alone.text.indexOf("## Apex triggers")), /read from its code's text/);
  });

  test("a class with queries and changes and neither flag lists them, one paragraph for the class", async () => {
    const h = handbook({ apexClasses: apex([apexClass("LeadRouter", { apiVersion: 61, touches: touches({ queries: ["Account", "Contact"], queriesTotal: 2, changes: ["insert", "update"] }) })]) });
    const { text, paragraphs } = await render(h);
    assert.equal(entry(text, "LeadRouter"), "LeadRouter\nAPI version 61.0. Status: Active. Test: No.\nQueries: Account, Contact\nChanges it makes: insert, update");
    assert.equal(paragraphs.filter((p) => p.text.startsWith("LeadRouter")).length, 1);
    assert.doesNotMatch(text, /builds queries as it runs|searches/);
    // A class that touches nothing it names says so; a test says it is one.
    const quiet = await render(handbook({ apexClasses: apex([apexClass("QuietTest", { isTest: true })]) }));
    assert.equal(entry(quiet.text, "QuietTest"), "QuietTest\nAPI version 62.0. Status: Active. Test: Yes.\nQueries: none in its own code\nChanges it makes: none in its own code");
  });

  test("convertLead is printed as convert lead", async () => {
    const { text } = await render(handbook({ apexClasses: apex([apexClass("Converter", { touches: touches({ changes: ["update", "convertLead"] }) })]) }));
    assert.match(entry(text, "Converter"), /\nChanges it makes: update, convert lead$/);
    assert.doesNotMatch(text, /convertLead/);
  });

  test("a cut list of queried objects says so with the full count", async () => {
    const queries = Array.from({ length: 50 }, (_, i) => `Object_${String(i).padStart(2, "0")}__c`);
    const { text } = await render(handbook({ apexClasses: apex([apexClass("Wide", { touches: touches({ queries, queriesTotal: 63 }) })]) }));
    assert.match(entry(text, "Wide"), /\nQueries: Object_00__c, [^\n]*Object_49__c \[cut: the first 50 of 63 objects\]\n/);
    const whole = await render(handbook({ apexClasses: apex([apexClass("Whole", { touches: touches({ queries, queriesTotal: 50 }) })]) }));
    assert.doesNotMatch(whole.text, /\[cut:/);
  });

  test("a query built as it runs, or a search, is said, and an empty list beside one is never 'none'", async () => {
    const items = [
      apexClass("Both", { touches: touches({ queries: ["Account"], queriesTotal: 1, dynamicQueries: true, searches: true }) }),
      apexClass("DynamicOnly", { touches: touches({ dynamicQueries: true }) }),
      apexClass("SearchOnly", { touches: touches({ searches: true }) }),
      apexClass("DynamicAndSearch", { touches: touches({ dynamicQueries: true, searches: true, changes: ["delete"] }) }),
    ];
    const { text } = await render(handbook({ apexClasses: apex(items) }));
    assert.equal(entry(text, "Both").split("\n").slice(2).join("\n"), "Queries: Account\nIt also builds queries as it runs, so the objects those read aren't known from the code.\nIt also searches, and the objects a search reads aren't listed.\nChanges it makes: none in its own code");
    assert.equal(entry(text, "DynamicOnly").split("\n").slice(2).join("\n"), "Queries: it builds them as it runs, so the objects they read aren't known from the code.\nChanges it makes: none in its own code");
    assert.equal(entry(text, "SearchOnly").split("\n").slice(2).join("\n"), "It searches, and the objects a search reads aren't listed.\nChanges it makes: none in its own code");
    assert.equal(entry(text, "DynamicAndSearch").split("\n").slice(2).join("\n"), "Queries: it builds them as it runs, so the objects they read aren't known from the code.\nIt also searches, and the objects a search reads aren't listed.\nChanges it makes: delete");
    // With a flag up, no line says it queries nothing.
    for (const name of ["DynamicOnly", "SearchOnly", "DynamicAndSearch"]) assert.doesNotMatch(entry(text, name), /Queries: none/, name);
  });

  test("code that couldn't be read says what it touches isn't known, and whether it's a test couldn't be told", async () => {
    const items = [apexClass("Hidden", { isTest: null, touches: null }), apexClass("Unscanned", { isTest: null })];
    const { text } = await render(handbook({ apexClasses: apex(items), apexTriggers: apex([trigger("HiddenTrigger", { isTest: null, touches: null })]) }));
    assert.equal(entry(text, "Hidden"), "Hidden\nAPI version 62.0. Status: Active. Test: couldn't be told: the code couldn't be read.\nThe code couldn't be read, so what it touches isn't known.");
    assert.doesNotMatch(entry(text, "Hidden"), /Queries|Changes|none/);
    assert.match(entry(text, "Unscanned"), /Test: couldn't be told: the code couldn't be read\.\nQueries: none in its own code/);
    assert.match(entry(text, "HiddenTrigger"), /\nThe code couldn't be read, so what it touches isn't known\.$/);
    // A trigger can't be a test, so it never says whether it is one, whatever the file holds.
    assert.doesNotMatch(entry(text, "HiddenTrigger"), /Test:/);
  });

  test("a trigger has no Test line, test or not, and a class keeps its", async () => {
    const { text } = await render(handbook({ apexClasses: apex([apexClass("Svc"), apexClass("SvcTest", { isTest: true })]), apexTriggers: apex([trigger("OnDeal"), trigger("OddTrigger", { isTest: true })]) }));
    for (const name of ["OnDeal", "OddTrigger"]) assert.doesNotMatch(entry(text, name), /Test:/, name);
    assert.match(entry(text, "OnDeal"), /\nAPI version 62\.0\. Status: Active\.\n/);
    assert.match(entry(text, "Svc"), /Test: No\./);
    assert.match(entry(text, "SvcTest"), /Test: Yes\./);
  });

  test("a trigger says its object as the file gives it and its events in Apex's order, or that they weren't read", async () => {
    const items = [
      trigger("CustomTrigger", { object: "Invoice__c", events: ["after update", "before insert", "after undelete"], touches: touches({ queries: ["Invoice__c"], queriesTotal: 1, changes: ["upsert"] }) }),
      trigger("Unnamed", { object: "01I000000000001AAA" }),
      trigger("Unread", { object: "Account", events: null }),
    ];
    const { text } = await render(handbook({ apexTriggers: apex(items) }));
    assert.equal(entry(text, "CustomTrigger"), "CustomTrigger\nOn: Invoice__c\nRuns: before insert, after update, after undelete\nAPI version 62.0. Status: Active.\nQueries: Invoice__c\nChanges it makes: upsert");
    assert.match(entry(text, "Unnamed"), /^Unnamed\nOn: 01I000000000001AAA\n/);
    assert.match(entry(text, "Unread"), /^Unread\nOn: Account\nThe events weren't read\.\nAPI version/);
    assert.doesNotMatch(entry(text, "Unread"), /Runs:/);
  });

  test("a cut part says how many it lists of how many, by name, on the first page and in the part", async () => {
    const classes = { status: "ok" as const, items: Array.from({ length: 3 }, (_, i) => apexClass(`C${i}`)), total: 6_200, cut: true, lastChanges: "not_kept" as const };
    const triggers = { status: "ok" as const, items: [trigger("T0")], total: 1_042, cut: true, lastChanges: "not_kept" as const };
    const { text } = await render(handbook({ apexClasses: classes, apexTriggers: triggers }));
    assert.match(text, /Apex classes: 6,200 classes\. This handbook lists the first 3 of 6,200 classes, by name\./);
    assert.match(text, /Apex triggers: 1,042 triggers\. This handbook lists the first 1 of 1,042 triggers, by name\./);
    assert.match(text, /## Apex classes\n\nYour org has 6,200 classes\. This handbook lists the first 3 of 6,200 classes, by name\. The rest aren't in this handbook\./);
    assert.match(text, /## Apex triggers\n\nYour org has 1,042 triggers\. This handbook lists the first 1 of 1,042 triggers, by name\. The rest aren't in this handbook\./);
    // Never "N of them active" counted from part of the list.
    assert.doesNotMatch(text, /(classes|triggers), \d+ of them active/);
  });

  test("each Apex part unavailable says so with its reason, empty says the org has none, and the two never look alike", async () => {
    const unavailable = await render(handbook({ apexClasses: { status: "unavailable", reason: "Apex isn't available in Professional Edition" }, apexTriggers: { status: "unavailable", reason: "The trigger query was refused" } }));
    const empty = await render(handbook());
    for (const [title, reason] of [["Apex classes", "Apex isn't available in Professional Edition."], ["Apex triggers", "The trigger query was refused."]]) {
      const said = `We couldn't read your ${title}, so this handbook doesn't list them. ${reason}`;
      // On the first page and in the part's own body.
      assert.equal(unavailable.text.split(said).length - 1, 2, title);
      assert.ok(unavailable.text.slice(unavailable.text.indexOf(`## ${title}`)).startsWith(`## ${title}\n\n${said}`), title);
      assert.match(empty.text, new RegExp(`${title}: your org has none\\.`));
      assert.match(empty.text, new RegExp(`## ${title}\\n\\nYour org has no ${title}: there were none to read\\.`));
    }
    assert.doesNotMatch(unavailable.text, /has no Apex|Apex (classes|triggers): your org has none/);
    assert.doesNotMatch(empty.text, /couldn't read your Apex/);
    // One can be there while the other says why it isn't (decision 456).
    const mixed = await render(handbook({ apexClasses: apex([apexClass("Only")]), apexTriggers: { status: "unavailable", reason: "Refused" } }));
    assert.match(mixed.text, /Apex classes: 1 class, 1 of them active\.\n\nWe couldn't read your Apex triggers/);
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
  test("300 flows of 100 steps each, 2,000 validation rules, 5,000 Apex classes and 1,000 triggers make a document", async () => {
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
    // The most of each the file keeps (decision 456), each class touching a few objects.
    const wide = touches({ queries: ["Account", "Contact", "Opportunity", "Invoice__c"], queriesTotal: 4, changes: ["insert", "update"], dynamicQueries: true });
    const classes = Array.from({ length: MAX_APEX_CLASSES }, (_, i) => apexClass(`Class_${String(i).padStart(4, "0")}`, { isTest: i % 4 === 0, touches: wide }));
    const triggers = Array.from({ length: MAX_APEX_TRIGGERS }, (_, i) => trigger(`Trigger_${String(i).padStart(4, "0")}`, { object: `Object_${i % 40}__c`, events: ["before insert", "after update"], touches: wide }));
    const h = handbook({ flows: { status: "ok", items: flows }, validationRules: { status: "ok", covers: RETRIEVE, items: rules }, apexClasses: apex(classes), apexTriggers: apex(triggers) });
    const started = performance.now();
    const bytes = await renderHandbookDocx(h, OPTIONS);
    const ms = Math.round(performance.now() - started);
    const paragraphs = docxParagraphs(bytes);
    console.log(`# a very large org: ${ms} ms, ${(bytes.length / 1_048_576).toFixed(1)} MB, ${paragraphs.length} paragraphs`);
    assert.equal(paragraphs.filter((p) => p.heading === 3).length, 300 + 40);
    assert.equal(paragraphs.filter((p) => p.heading === 4).length, 2_000);
    assert.equal(paragraphs.filter((p) => /^\d+\. (Decision|Update records): /.test(p.text)).length, 300 * 100);
    // One paragraph for each class and trigger.
    assert.equal(paragraphs.filter((p) => /^Class_\d{4}\nAPI version /.test(p.text)).length, MAX_APEX_CLASSES);
    assert.equal(paragraphs.filter((p) => /^Trigger_\d{4}\nOn: /.test(p.text)).length, MAX_APEX_TRIGGERS);
    // Well inside what the documents job and Drive take, and quick enough not to hold a make up.
    assert.ok(bytes.length < 20 * 1_048_576, `${bytes.length} bytes`);
    assert.ok(ms < 120_000, `${ms} ms`);
  });
});
