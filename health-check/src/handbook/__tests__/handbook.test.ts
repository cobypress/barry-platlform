import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { scanApex } from "../../extract/apex-scan.ts";
import { readTouches } from "../../extract/apex-touches.ts";
import { RETRIEVE_TYPES, type ApexClassRow, type ApexData, type ApexTriggerRow } from "../../extract/automation.ts";
import { flowDetailMetadata, parseMetadataBundle, type MetadataBundle } from "../../extract/metadata-parse.ts";
import { toolingValidationRule } from "../../extract/metadata-tooling.ts";
import type { Sourced } from "../../extract/types.ts";
import { deleteRunFolder, purgeRun } from "../../retention/purge.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, HANDBOOK_MARKER, HANDBOOK_PLANTED, METADATA_FILES } from "../../testing/synthetic-org.ts";
import { buildHandbook, HANDBOOK_FILE } from "../build.ts";
import { flowDetail, validationRuleDetail } from "../detail.ts";
import { Handbook, MAX_APEX_CLASSES, MAX_APEX_TRIGGERS, MAX_CONDITIONS, MAX_FIELDS, MAX_STEPS, MAX_TEXT, type HandbookFlow, type ValidationRuleCoverage } from "../schema.ts";

// The org handbook's data (DECISIONS.md 451 to 457).

type Run = { dir: string; runId: string; calls: number; itemReads: number };

async function run(options: ConstructorParameters<typeof Simulator>[0] = {}): Promise<Run> {
  const transport = new Simulator(options);
  const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport, log: () => {} });
  const step = result.run.steps.find((s) => s.id === "metadata")!;
  const itemReads = transport.requests.filter((r) => decodeURIComponent(r.url).includes("SELECT Id, FullName, Metadata")).length;
  return { dir: result.dir, runId: result.run.id, calls: step.calls, itemReads };
}

const read = (dir: string, file: string): string => readFileSync(join(dir, file), "utf8");
/** The Apex parts, for a file whose other parts a test is about. */
const APEX_UNREAD = { apexClasses: { status: "unavailable", reason: "x" }, apexTriggers: { status: "unavailable", reason: "x" } } as const;
const handbookOf = (r: Run): Handbook => Handbook.parse(JSON.parse(read(r.dir, HANDBOOK_FILE)));
const flows = (h: Handbook): HandbookFlow[] => (h.flows.status === "ok" ? h.flows.items : assert.fail("flows unavailable"));

let retrieve: Run;
let tooling: Run;
before(async () => {
  retrieve = await run();
  tooling = await run({ readOnly: true });
});

describe("handbook.json from the synthetic org", () => {
  const plantedFlows = Object.keys(METADATA_FILES).filter((p) => p.startsWith("flows/")).map((p) => p.slice(6).replace(/\.flow$/, "")).sort();

  test("holds every planted flow and validation rule", () => {
    const h = handbookOf(retrieve);
    assert.deepEqual(flows(h).map((f) => f.name).sort(), plantedFlows);
    assert.ok(h.validationRules.status === "ok");
    assert.deepEqual(h.validationRules.items.map((v) => `${v.object}.${v.name}`), ["Invoice__c.Due_Date_Required", "Invoice__c.Old_Check", "Opportunity.Needs_Competitor"]);
  });

  test("a record-triggered flow's steps are in the order it runs them, with the step nothing reaches last", () => {
    const f = flows(handbookOf(retrieve)).find((x) => x.name === "Opportunity_Stage_Review")!;
    assert.deepEqual(
      f.steps.map((s) => [s.name, s.kind, s.reached]),
      [
        ["Get_Account", "recordLookups", true],
        ["Check_Size", "decisions", true],
        // The decision's first outcome, followed to its end before the next.
        ["Each_Line", "loops", true],
        ["Set_Next_Step", "assignments", true],
        ["Run_Approval", "subflows", true],
        // A kind the parser has no case for, kept under Salesforce's own name.
        ["Summarise_Deal", "promptSteps", true],
        ["Notify_Owner", "actionCalls", true],
        // The lookup's fault path comes after everything its next step leads to.
        ["Log_Failure", "recordCreates", true],
        ["Old_Step", "assignments", false],
      ],
    );
    assert.equal(f.stepsTotal, 9);
    assert.equal(f.cut, false);
  });

  test("what starts it: the object, the trigger, and the entry conditions with their logic", () => {
    const f = flows(handbookOf(retrieve)).find((x) => x.name === "Opportunity_Stage_Review")!;
    assert.deepEqual(f.start, {
      object: "Opportunity",
      triggerType: "RecordAfterSave",
      recordTriggerType: "CreateAndUpdate",
      schedule: null,
      entry: {
        logic: "1 AND (2 OR 3)",
        formula: null,
        formulaCut: false,
        formulaLength: null,
        conditions: [
          { field: "StageName", operator: "EqualTo", value: "Negotiation", valueCut: false, valueLength: 11 },
          { field: "Amount", operator: "GreaterThan", value: "5000", valueCut: false, valueLength: 4 },
          { field: "IsPrivate", operator: "EqualTo", value: "false", valueCut: false, valueLength: 5 },
        ],
        conditionsTotal: 3,
        cut: false,
      },
    });
  });

  test("each step's paths, object, fields and detail", () => {
    const steps = new Map(flows(handbookOf(retrieve)).find((x) => x.name === "Opportunity_Stage_Review")!.steps.map((s) => [s.name, s]));
    assert.deepEqual(steps.get("Check_Size")!.outcomes, [
      { label: "Large Deal", next: "Each_Line" },
      { label: "Medium Deal", next: "Notify_Owner" },
      { label: "Small Deal", next: "Set_Next_Step" },
    ]);
    assert.deepEqual(steps.get("Check_Size")!.fields, ["$Record.Amount", "$Record.Probability"]);
    assert.deepEqual(steps.get("Each_Line")!.outcomes, [
      { label: "For Each Item", next: "Set_Next_Step" },
      { label: "After Last Item", next: "Run_Approval" },
    ]);
    assert.equal(steps.get("Each_Line")!.detail, "Line_Items");
    const lookup = steps.get("Get_Account")!;
    assert.deepEqual([lookup.object, lookup.fields, lookup.next, lookup.fault], ["Account", ["Id", "Industry"], "Check_Size", "Log_Failure"]);
    assert.equal(steps.get("Run_Approval")!.detail, "Discount_Approval_Steps");
    assert.equal(steps.get("Notify_Owner")!.detail, "chatterPost (chatterPost)");
    assert.deepEqual([steps.get("Log_Failure")!.object, steps.get("Log_Failure")!.fields], ["Task", ["Subject"]]);
  });

  test("a scheduled flow keeps its schedule", () => {
    const f = flows(handbookOf(retrieve)).find((x) => x.name === "Weekly_Renewal_Check")!;
    assert.equal(f.start.triggerType, "Scheduled");
    assert.deepEqual(f.start.schedule, { frequency: "Weekly", startDate: "2026-11-02", startTime: "07:00:00.000Z" });
    assert.deepEqual(f.steps.map((s) => [s.name, s.reached]), [["Find_Renewals", true]]);
  });

  test("a validation rule keeps its formula, its message and the field the message shows on", () => {
    const h = handbookOf(retrieve);
    assert.ok(h.validationRules.status === "ok");
    assert.deepEqual(h.validationRules.items.find((v) => v.name === "Needs_Competitor"), {
      object: "Opportunity",
      name: "Needs_Competitor",
      active: true,
      description: "A competitor is needed to report on losses.",
      formula: "ISBLANK(Competitor__c)",
      formulaCut: false,
      formulaLength: 22,
      message: "Name the competitor before you save this deal.",
      messageField: "Competitor__c",
    });
  });

  test("the key planted in a formula is in neither handbook.json nor the metadata checkpoint, on either route", () => {
    for (const r of [retrieve, tooling]) {
      const handbook = read(r.dir, HANDBOOK_FILE);
      assert.ok(handbook.includes(HANDBOOK_MARKER), "the formula is there");
      assert.ok(handbook.includes(`api_key = \\"[REDACTED]\\"`), "with the key redacted");
      assert.ok(!handbook.includes(HANDBOOK_PLANTED));
      assert.ok(!read(r.dir, "checkpoints/metadata.json").includes(HANDBOOK_PLANTED));
    }
  });
});

describe("the Tooling route's item reads", () => {
  // Each item is kept in a partial checkpoint as it's read, and a run that stops
  // part-way leaves it there until the purge. In JSON a quote is \", which the
  // patterns don't see, so each value is redacted on its own too.
  test("a run that stops after the validation rules were read leaves no key in its partial checkpoint", async () => {
    const simulator = new Simulator({ readOnly: true });
    const dropping: Transport = {
      send: (request) => (decodeURIComponent(request.url).includes("FROM FieldDefinition") ? Promise.reject(new Error("socket hang up")) : simulator.send(request)),
    };
    const outDir = tempDir();
    await assert.rejects(runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport: dropping, log: () => {} }), /socket hang up/);
    const [runId] = readdirSync(outDir);
    const partial = readFileSync(join(outDir, runId!, "checkpoints", "metadata-tooling.partial.jsonl"), "utf8");
    assert.ok(partial.includes(HANDBOOK_MARKER), "the formula was read");
    assert.ok(!partial.includes(HANDBOOK_PLANTED));
  });
});

describe("the two routes", () => {
  // Only what they cover differs, as designed (decision 455): the synthetic org
  // has no rule on a standard object the retrieve doesn't name.
  test("the retrieve and Tooling give the same detail for the synthetic org", () => {
    const detail = (r: Run) => (JSON.parse(read(r.dir, "checkpoints/metadata.json")) as Sourced<MetadataBundle> & { status: "ok" }).data.handbook!;
    assert.ok(detail(retrieve).flows.length > 0);
    assert.deepEqual([detail(tooling).flows, detail(tooling).validationRules], [detail(retrieve).flows, detail(retrieve).validationRules]);
    const [fromTooling, fromRetrieve] = [handbookOf(tooling), handbookOf(retrieve)];
    assert.deepEqual(fromTooling.flows, fromRetrieve.flows);
    assert.ok(fromTooling.validationRules.status === "ok" && fromRetrieve.validationRules.status === "ok");
    assert.deepEqual(fromTooling.validationRules.items, fromRetrieve.validationRules.items);
    assert.notDeepEqual(fromTooling.validationRules.covers, fromRetrieve.validationRules.covers);
  });

  describe("which objects' validation rules each route covers (decision 455)", () => {
    const covers = (r: Run): ValidationRuleCoverage => {
      const part = handbookOf(r).validationRules;
      return part.status === "ok" ? part.covers : assert.fail("validation rules unavailable");
    };

    test("the retrieve: every custom object, and the standard objects it names, taken from the list it asks for", () => {
      const named = RETRIEVE_TYPES.find((t) => t.name === "CustomObject")!.members.filter((m) => m !== "*");
      assert.deepEqual(covers(retrieve), { customObjects: "all", standardObjects: named });
      assert.deepEqual(named, ["Account", "Contact", "Lead", "Opportunity"]);
    });

    test("Tooling, which lists every rule: all of them", () => {
      assert.deepEqual(covers(tooling), { customObjects: "all", standardObjects: "all" });
    });

    test("a list of validation rules that doesn't say what it covers doesn't fit", () => {
      const items = handbookOf(retrieve).validationRules;
      assert.ok(items.status === "ok");
      const { covers: _, ...bare } = items;
      assert.equal(Handbook.safeParse({ version: 2, flows: { status: "ok", items: [] }, validationRules: bare, ...APEX_UNREAD }).success, false);
      assert.equal(Handbook.safeParse({ version: 2, flows: { status: "ok", items: [] }, validationRules: items, ...APEX_UNREAD }).success, true);
      // Unavailable says why instead, with nothing to cover.
      assert.equal(Handbook.safeParse({ version: 2, flows: { status: "ok", items: [] }, validationRules: { status: "unavailable", reason: "x", covers: items.covers }, ...APEX_UNREAD }).success, false);
    });

    test("a bundle with no checkpointed coverage gives no list rather than one that claims too much", () => {
      const bundle = parseMetadataBundle({});
      const { validationRulesCover: _, ...old } = bundle.handbook!;
      const built = buildHandbook({ status: "ok", data: { ...bundle, handbook: old as MetadataBundle["handbook"] }, source: { kind: "metadata", path: "CustomObject" }, truncated: false });
      assert.equal(built.validationRules.status, "unavailable");
    });
  });

  // The simulator's Tooling view is the XML parsed, so it can't show a real
  // Tooling difference. This is the same flow as Tooling sends it: typed values,
  // null for anything absent, and every list a list.
  const NO_VALUE = { stringValue: null, numberValue: null, booleanValue: null, dateValue: null, dateTimeValue: null, elementReference: null, apexValue: null, sobjectValue: null, formulaExpression: null, setupReference: null, setupReferenceType: null, transformValueReference: null };
  const to = (name: string) => ({ targetReference: name, isGoTo: null, processMetadataValues: [] });
  const node = { locationX: 176, locationY: 158, description: null, processMetadataValues: [] };
  const XML = `<?xml version="1.0" encoding="UTF-8"?><Flow xmlns="http://soap.sforce.com/2006/04/metadata">
    <apiVersion>62.0</apiVersion><description>Routes &quot;big&quot; deals.</description><label>Route</label><processType>AutoLaunchedFlow</processType><status>Active</status>
    <decisions><name>Big</name><label>Big?</label><locationX>176</locationX><locationY>158</locationY><defaultConnectorLabel>Default Outcome</defaultConnectorLabel>
      <rules><name>Yes</name><conditionLogic>and</conditionLogic><conditions><leftValueReference>$Record.Amount</leftValueReference><operator>GreaterThan</operator><rightValue><numberValue>10.0</numberValue></rightValue></conditions><connector><targetReference>Each</targetReference></connector><label>Yes</label></rules>
    </decisions>
    <loops><name>Each</name><label>Each</label><locationX>176</locationX><locationY>158</locationY><collectionReference>Items</collectionReference><nextValueConnector><targetReference>Mark</targetReference></nextValueConnector></loops>
    <recordUpdates><name>Mark</name><label>Mark</label><locationX>176</locationX><locationY>158</locationY><connector><targetReference>Each</targetReference></connector><faultConnector><targetReference>Big</targetReference></faultConnector><inputAssignments><field>Description</field><value><stringValue>x</stringValue></value></inputAssignments><inputReference>$Record</inputReference></recordUpdates>
    <start><locationX>50</locationX><locationY>0</locationY><connector><targetReference>Big</targetReference></connector><filterLogic>and</filterLogic>
      <filters><field>Amount</field><operator>GreaterThan</operator><value><numberValue>5000.0</numberValue></value></filters>
      <filters><field>Description</field><operator>EqualTo</operator><value><stringValue>secret: &quot;abcd-1234-efgh&quot;</stringValue></value></filters>
      <filters><field>IsClosed</field><operator>EqualTo</operator><value><booleanValue>false</booleanValue></value></filters>
      <object>Opportunity</object><recordTriggerType>Update</recordTriggerType><triggerType>RecordAfterSave</triggerType></start>
    <variables><name>Items</name><dataType>SObject</dataType><isCollection>true</isCollection><isInput>false</isInput><isOutput>false</isOutput><objectType>OpportunityLineItem</objectType></variables>
  </Flow>`;
  const JSON_FLOW = {
    apiVersion: 62,
    description: 'Routes "big" deals.',
    label: "Route",
    processType: "AutoLaunchedFlow",
    status: "Active",
    actionCalls: [],
    assignments: [],
    recordCreates: [],
    recordLookups: [],
    screens: [],
    subflows: [],
    waits: [],
    processMetadataValues: [],
    decisions: [
      {
        ...node,
        name: "Big",
        label: "Big?",
        defaultConnector: null,
        defaultConnectorLabel: "Default Outcome",
        rules: [
          { name: "Yes", label: "Yes", conditionLogic: "and", description: null, doesRequireRecordChangedToMeetCriteria: false, processMetadataValues: [], connector: to("Each"), conditions: [{ leftValueReference: "$Record.Amount", operator: "GreaterThan", processMetadataValues: [], rightValue: { ...NO_VALUE, numberValue: 10 } }] },
        ],
      },
    ],
    loops: [{ ...node, name: "Each", label: "Each", collectionReference: "Items", iterationOrder: null, assignNextValueToReference: null, nextValueConnector: to("Mark"), noMoreValuesConnector: null }],
    recordUpdates: [
      { ...node, name: "Mark", label: "Mark", object: null, filterLogic: null, filters: [], inputReference: "$Record", connector: to("Each"), faultConnector: to("Big"), inputAssignments: [{ field: "Description", processMetadataValues: [], value: { ...NO_VALUE, stringValue: "x" } }] },
    ],
    start: {
      ...node,
      locationX: 50,
      locationY: 0,
      connector: to("Big"),
      doesRequireRecordChangedToMeetCriteria: false,
      filterFormula: null,
      filterLogic: "and",
      filters: [
        { field: "Amount", operator: "GreaterThan", value: { ...NO_VALUE, numberValue: 5000 } },
        { field: "Description", operator: "EqualTo", value: { ...NO_VALUE, stringValue: 'secret: "abcd-1234-efgh"' } },
        { field: "IsClosed", operator: "EqualTo", value: { ...NO_VALUE, booleanValue: false } },
      ],
      object: "Opportunity",
      recordTriggerType: "Update",
      schedule: null,
      scheduledPaths: [],
      triggerType: "RecordAfterSave",
    },
    variables: [{ name: "Items", dataType: "SObject", description: null, isCollection: true, isInput: false, isOutput: false, objectType: "OpportunityLineItem", processMetadataValues: [], scale: null, value: null }],
  };

  test("a flow read from the retrieve's XML and from Tooling's JSON gives the same detail", () => {
    const fromXml = parseMetadataBundle({ "flows/Route.flow": XML }).handbook!.flows[0]!;
    const fromJson = flowDetailMetadata("Route", JSON_FLOW);
    assert.deepEqual(fromJson, fromXml);
    // And it's the detail it should be, not two wrong answers that agree.
    assert.deepEqual(fromXml.steps.map((s) => [s.name, s.reached]), [["Big", true], ["Each", true], ["Mark", true]]);
    assert.deepEqual(fromXml.start.entry.conditions.map((c) => c.value), ["5000", 'secret: "[REDACTED]"', "false"]);
    assert.equal(fromXml.description, 'Routes "big" deals.');
    assert.equal(fromXml.steps.find((s) => s.name === "Mark")!.detail, "$Record");
  });

  test("a validation rule read from the retrieve's XML and from Tooling gives the same detail", () => {
    const xml = `<CustomObject><validationRules><fullName>Needs_Reason</fullName><active>true</active><description>Why a deal was lost.</description><errorConditionFormula>AND(ISPICKVAL(StageName, &quot;Closed Lost&quot;), ISBLANK(Loss_Reason__c))</errorConditionFormula><errorDisplayField>Loss_Reason__c</errorDisplayField><errorMessage>Say why the deal was lost.</errorMessage></validationRules></CustomObject>`;
    const fromXml = parseMetadataBundle({ "objects/Opportunity.object": xml }).handbook!.validationRules[0]!;
    const row = { Id: "03d000000000001AAA", ValidationName: "Needs_Reason", Active: true, Description: "Why a deal was lost.", EntityDefinitionId: "Opportunity", NamespacePrefix: null };
    const metadata = { active: true, description: "Why a deal was lost.", errorConditionFormula: 'AND(ISPICKVAL(StageName, "Closed Lost"), ISBLANK(Loss_Reason__c))', errorDisplayField: "Loss_Reason__c", errorMessage: "Say why the deal was lost.", shouldEvaluateOnClient: false, urls: null };
    assert.deepEqual(toolingValidationRule("Opportunity", row, metadata), fromXml);
    assert.equal(fromXml.formula, 'AND(ISPICKVAL(StageName, "Closed Lost"), ISBLANK(Loss_Reason__c))');
  });

  test("a validation rule whose definition wasn't read keeps no formula rather than a guess", () => {
    const row = { ValidationName: "Needs_Reason", Active: false, Description: null };
    assert.deepEqual(toolingValidationRule("Opportunity", row, null), {
      object: "Opportunity",
      name: "Needs_Reason",
      active: false,
      description: null,
      formula: null,
      formulaCut: false,
      formulaLength: null,
      message: null,
      messageField: null,
    });
  });
});

describe("caps (decision 453)", () => {
  const steps = (n: number) => ({ assignments: Array.from({ length: n }, (_, i) => ({ name: `Step_${String(i).padStart(4, "0")}` })) });
  const updates = (n: number) => ({ recordUpdates: [{ name: "Update", object: "Account", inputAssignments: Array.from({ length: n }, (_, i) => ({ field: `Field_${i}` })) }] });
  const entry = (n: number, value = "x") => ({ start: { filters: Array.from({ length: n }, (_, i) => ({ field: `Field_${i}`, operator: "EqualTo", value: { stringValue: value } })) } });

  test(`a flow keeps ${MAX_STEPS} steps, and marks one more as cut with the full count`, () => {
    assert.equal(MAX_STEPS, 500);
    const at = flowDetail("At", steps(500), null);
    assert.deepEqual([at.steps.length, at.stepsTotal, at.cut], [500, 500, false]);
    const over = flowDetail("Over", steps(501), null);
    assert.deepEqual([over.steps.length, over.stepsTotal, over.cut], [500, 501, true]);
    assert.ok(Handbook.safeParse({ version: 2, flows: { status: "ok", items: [over] }, validationRules: { status: "ok", covers: { customObjects: "all", standardObjects: "all" }, items: [] }, ...APEX_UNREAD }).success);
  });

  test(`a step keeps ${MAX_FIELDS} fields, with the full count beside them`, () => {
    assert.equal(MAX_FIELDS, 50);
    const at = flowDetail("At", updates(50), null).steps[0]!;
    assert.deepEqual([at.fields.length, at.fieldsTotal], [50, 50]);
    const over = flowDetail("Over", updates(51), null).steps[0]!;
    assert.deepEqual([over.fields.length, over.fieldsTotal, over.fields.at(-1)], [50, 51, "Field_49"]);
  });

  test(`an entry keeps ${MAX_CONDITIONS} conditions, and marks one more as cut`, () => {
    assert.equal(MAX_CONDITIONS, 50);
    const at = flowDetail("At", entry(50), null).start.entry;
    assert.deepEqual([at.conditions.length, at.conditionsTotal, at.cut], [50, 50, false]);
    const over = flowDetail("Over", entry(51), null).start.entry;
    assert.deepEqual([over.conditions.length, over.conditionsTotal, over.cut], [50, 51, true]);
  });

  test(`a formula or value keeps ${MAX_TEXT} characters, and marks one more as cut with its length`, () => {
    assert.equal(MAX_TEXT, 4_000);
    const at = validationRuleDetail("A__c", "At", { errorConditionFormula: "a".repeat(4_000) });
    assert.deepEqual([at.formula!.length, at.formulaCut, at.formulaLength], [4_000, false, 4_000]);
    const over = validationRuleDetail("A__c", "Over", { errorConditionFormula: "a".repeat(4_001) });
    assert.deepEqual([over.formula!.length, over.formulaCut, over.formulaLength], [4_000, true, 4_001]);
    const value = flowDetail("Value", entry(1, "b".repeat(4_001)), null).start.entry.conditions[0]!;
    assert.deepEqual([value.value!.length, value.valueCut, value.valueLength], [4_000, true, 4_001]);
    const formula = flowDetail("Formula", { start: { filterFormula: "c".repeat(4_001) } }, null).start.entry;
    assert.deepEqual([formula.formula!.length, formula.formulaCut, formula.formulaLength], [4_000, true, 4_001]);
    // Never half a character at the cut.
    const wide = validationRuleDetail("A__c", "Wide", { errorConditionFormula: `${"a".repeat(3_999)}😀` });
    assert.deepEqual([wide.formula, wide.formulaCut, wide.formulaLength], ["a".repeat(3_999), true, 4_001]);
  });
});

describe("secrets (decision 452)", () => {
  test("every formula, value, label, description and message is redacted before it's kept", () => {
    const key = "Bearer abcdefghijklmnop0123456789";
    const flow = flowDetail(
      "Flow",
      {
        label: key,
        description: key,
        start: { filterFormula: key, filters: [{ field: "A", operator: "EqualTo", value: { stringValue: key } }] },
        decisions: [{ name: "D", label: key, rules: [{ name: "R", label: key }], defaultConnectorLabel: key }],
      },
      null,
    );
    const rule = validationRuleDetail("A__c", "Rule", { errorConditionFormula: key, errorMessage: key, description: key });
    const kept = JSON.stringify([flow, rule]);
    assert.ok(!kept.includes("abcdefghijklmnop"), kept);
    assert.equal(kept.match(/Bearer \[REDACTED\]/g)!.length, 10);
  });
});

describe("when the metadata couldn't be read (decision 454)", () => {
  test("a run whose metadata step is unavailable still writes handbook.json, both parts saying why", async () => {
    const r = await run({ readOnly: true, failQuery: (soql) => (/FROM FlowDefinition/.test(soql) ? { errorCode: "INVALID_TYPE", message: "sObject type 'FlowDefinition' is not supported." } : null) });
    const h = handbookOf(r);
    for (const part of [h.flows, h.validationRules]) {
      assert.equal(part.status, "unavailable");
      assert.ok(part.status === "unavailable");
      assert.match(part.reason, /^Metadata couldn't be retrieved: The Metadata API refused this connection .*FlowDefinition isn't available/);
    }
  });

  test("a run with no metadata step writes it too, saying so", async () => {
    const modules = SALES_CLOUD_MODULES.filter((m) => m.id === "current_pipeline");
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), log: () => {} });
    assert.ok(!result.run.steps.some((s) => s.id === "metadata"));
    assert.deepEqual(handbookOf({ dir: result.dir, runId: result.run.id, calls: 0, itemReads: 0 }), buildHandbook(undefined));
  });

  test("validation rules that couldn't be listed are unavailable, while the flows are there", async () => {
    const r = await run({ readOnly: true, failQuery: (soql) => (/FROM ValidationRule\b/.test(soql) ? { errorCode: "INVALID_TYPE", message: "sObject type 'ValidationRule' is not supported." } : null) });
    const h = handbookOf(r);
    assert.equal(h.flows.status, "ok");
    assert.ok(h.validationRules.status === "unavailable");
    assert.match(h.validationRules.reason, /^Validation rules couldn't be read: ValidationRule isn't available/);
  });

  test("a validation rule whose definition is withheld keeps its name, with no formula", async () => {
    const r = await run({ readOnly: true, withholdMetadata: (soql) => /FROM ValidationRule\b/.test(soql) });
    const h = handbookOf(r);
    assert.ok(h.validationRules.status === "ok");
    assert.equal(h.validationRules.items.length, 3);
    assert.ok(h.validationRules.items.every((v) => v.formula === null && v.message === null));
  });

  test("no metadata step, a failed one, or detail that doesn't fit: both parts say why", () => {
    const source = { kind: "metadata" as const, path: "Flow" };
    const noApex = { status: "unavailable", reason: "This run didn't read the org's Apex." };
    assert.deepEqual(buildHandbook(undefined), { version: 2, flows: { status: "unavailable", reason: "This run didn't read the org's metadata." }, validationRules: { status: "unavailable", reason: "This run didn't read the org's metadata." }, apexClasses: noApex, apexTriggers: noApex });
    const failed = buildHandbook({ status: "unavailable", reason: "Salesforce said no.", source });
    assert.deepEqual(failed.flows, { status: "unavailable", reason: "Metadata couldn't be retrieved: Salesforce said no." });
    const bundle = parseMetadataBundle({});
    const old = buildHandbook({ status: "ok", data: { ...bundle, handbook: undefined }, source, truncated: false });
    assert.equal(old.flows.status, "unavailable");
    const tooMany = flowDetail("Big", { assignments: Array.from({ length: 500 }, (_, i) => ({ name: `Step_${i}` })) }, null);
    tooMany.steps.push(tooMany.steps[0]!);
    const misfit = buildHandbook({ status: "ok", data: { ...bundle, handbook: { flows: [tooMany], validationRules: [], validationRulesCover: { customObjects: "all", standardObjects: [] } } }, source, truncated: false });
    assert.ok(misfit.flows.status === "unavailable");
    assert.match(misfit.flows.reason, /^The handbook's data didn't fit its format at flows\.items\.0\.steps, so it wasn't kept\.$/);
  });
});

describe("retention", () => {
  test("hc purge keeps handbook.json, and deleting the run folder removes it", async () => {
    const r = await run();
    const path = join(r.dir, HANDBOOK_FILE);
    assert.ok(existsSync(path));
    assert.ok(existsSync(join(r.dir, "checkpoints")));
    assert.equal(purgeRun(r.dir).purged, true);
    assert.ok(!existsSync(join(r.dir, "checkpoints")));
    assert.ok(existsSync(path), "kept by the purge");
    Handbook.parse(JSON.parse(readFileSync(path, "utf8")));
    deleteRunFolder(r.dir, r.runId);
    assert.ok(!existsSync(path), "deleted with the folder");
  });
});

describe("the API budget", () => {
  test("the metadata step makes no more calls than before the handbook", () => {
    // Measured on the base, before the handbook: 3 on the retrieve route, and on
    // Tooling 27 with 13 item reads for the six flows then planted. Tooling reads
    // one item per flow, Workflow Rule and validation rule, so the two draft flows
    // planted for the handbook add two, and nothing else does.
    const flowFiles = Object.keys(METADATA_FILES).filter((p) => p.startsWith("flows/")).length;
    assert.equal(flowFiles, 8);
    assert.equal(retrieve.calls, 3);
    assert.equal(retrieve.itemReads, 0);
    assert.equal(tooling.calls, 27 + 2);
    assert.equal(tooling.itemReads, 13 + 2);
  });
});

describe("Apex (decisions 456 and 457)", () => {
  const source = { kind: "soql" as const, api: "rest" as const, query: "SELECT Id FROM ApexClass" };
  const ok = <T>(data: T[]) => ({ status: "ok" as const, data, source, truncated: false });
  const BODY = "public class A { void x() { update [SELECT Id FROM Account]; } }";
  const cls = (name: string, extra: Partial<ApexClassRow> = {}): ApexClassRow => ({ Id: "01p000000000001AAA", Name: name, ApiVersion: 62, Status: "Active", LengthWithoutComments: 60, commented: false, isTest: false, scan: scanApex(BODY), touches: readTouches(BODY), ...extra });
  const NO_USAGE = { UsageBeforeInsert: false, UsageBeforeUpdate: false, UsageBeforeDelete: false, UsageAfterInsert: false, UsageAfterUpdate: false, UsageAfterDelete: false, UsageAfterUndelete: false };
  const trg = (name: string, extra: Partial<ApexTriggerRow> = {}): ApexTriggerRow => ({ ...cls(name), Id: "01q000000000001AAA", TableEnumOrId: "Account", ...NO_USAGE, UsageBeforeInsert: true, ...extra });
  const apex = (classes: ApexData["classes"], triggers: ApexData["triggers"]): ApexData => ({ classes, triggers, orgCoverage: ok([]), classCoverage: ok([]), references: { users: {}, fields: {} } });
  const named = (n: number) => Array.from({ length: n }, (_, i) => `Code_${String(n - i).padStart(5, "0")}`);

  describe("from the recorded synthetic org", () => {
    let dir: string;
    before(async () => {
      dir = (await runFromRecording()).dir;
    });

    test("a version 2 file that validates against the schema, with both parts read", () => {
      const raw = JSON.parse(read(dir, HANDBOOK_FILE));
      assert.equal(raw.version, 2);
      const h = Handbook.parse(raw);
      assert.ok(h.apexClasses.status === "ok" && h.apexTriggers.status === "ok");
      assert.deepEqual([h.apexClasses.total, h.apexClasses.cut, h.apexTriggers.total, h.apexTriggers.cut], [7, false, 5, false]);
      // A file of the first version doesn't fit.
      assert.equal(Handbook.safeParse({ ...raw, version: 1 }).success, false);
    });

    test("lists OpportunityHandler, which queries Opportunity and updates, and not the field its comment names", () => {
      const h = Handbook.parse(JSON.parse(read(dir, HANDBOOK_FILE)));
      assert.ok(h.apexClasses.status === "ok");
      assert.deepEqual(h.apexClasses.items.find((c) => c.name === "OpportunityHandler"), {
        name: "OpportunityHandler",
        apiVersion: 60,
        status: "Active",
        isTest: false,
        touches: { queries: ["Opportunity"], queriesTotal: 1, changes: ["update"], dynamicQueries: false, searches: false },
      });
      assert.deepEqual(h.apexClasses.items.map((c) => c.name), [...h.apexClasses.items.map((c) => c.name)].sort());
      assert.equal(h.apexClasses.items.find((c) => c.name === "OpportunityHandlerTest")!.isTest, true);
    });

    test("each trigger with its object as Salesforce gives it, and its events in order", () => {
      const h = Handbook.parse(JSON.parse(read(dir, HANDBOOK_FILE)));
      assert.ok(h.apexTriggers.status === "ok");
      const invoice = h.apexTriggers.items.find((t) => t.name === "InvoiceTrigger")!;
      assert.deepEqual([invoice.object, invoice.events], ["01I000000000001AAA", ["before insert", "before update"]]);
      assert.deepEqual(h.apexTriggers.items.find((t) => t.name === "OpportunityTrigger")!.events, ["before insert"]);
    });

    test("the apex step's checkpoint keeps the reading and drops the body", () => {
      const checkpoint = read(dir, "checkpoints/apex.json");
      assert.match(checkpoint, /"touches":\{"queries":\["Opportunity"\]/);
      assert.ok(!checkpoint.includes("public class") && !checkpoint.includes("uses Competitor__c"));
    });
  });

  test(`at most ${MAX_APEX_CLASSES} classes and ${MAX_APEX_TRIGGERS} triggers, by name, a cut part marked with the full count`, () => {
    assert.deepEqual([MAX_APEX_CLASSES, MAX_APEX_TRIGGERS], [5_000, 1_000]);
    const at = buildHandbook(undefined, apex(ok(named(5_000).map((n) => cls(n))), ok(named(1_000).map((n) => trg(n)))));
    assert.ok(at.apexClasses.status === "ok" && at.apexTriggers.status === "ok");
    assert.deepEqual([at.apexClasses.items.length, at.apexClasses.total, at.apexClasses.cut], [5_000, 5_000, false]);
    assert.deepEqual([at.apexTriggers.items.length, at.apexTriggers.total, at.apexTriggers.cut], [1_000, 1_000, false]);
    const over = buildHandbook(undefined, apex(ok(named(5_001).map((n) => cls(n))), ok(named(1_001).map((n) => trg(n)))));
    assert.ok(over.apexClasses.status === "ok" && over.apexTriggers.status === "ok");
    assert.deepEqual([over.apexClasses.items.length, over.apexClasses.total, over.apexClasses.cut], [5_000, 5_001, true]);
    assert.deepEqual([over.apexTriggers.items.length, over.apexTriggers.total, over.apexTriggers.cut], [1_000, 1_001, true]);
    // The first by name are kept, whatever order Salesforce sent them in.
    assert.deepEqual([over.apexClasses.items[0]!.name, over.apexClasses.items.at(-1)!.name], ["Code_00001", "Code_05000"]);
  });

  test("a trigger's events in a fixed order, and null when the row has none of the Usage fields", () => {
    const usage = { UsageAfterUndelete: true, UsageBeforeInsert: true, UsageAfterUpdate: true };
    const old = trg("Old");
    for (const key of Object.keys(NO_USAGE)) delete (old as Record<string, unknown>)[key];
    const h = buildHandbook(undefined, apex(ok([]), ok([trg("Many", { ...NO_USAGE, ...usage }), old, trg("None", NO_USAGE)])));
    assert.ok(h.apexTriggers.status === "ok");
    const events = new Map(h.apexTriggers.items.map((t) => [t.name, t.events]));
    assert.deepEqual(events.get("Many"), ["before insert", "after update", "after undelete"]);
    assert.equal(events.get("Old"), null);
    // Never an empty list: a trigger runs on something, so none at all says nothing was read.
    assert.equal(events.get("None"), null);
  });

  test("a hidden body, and a checkpoint row from before the reading, read as not read rather than touching nothing", () => {
    const { touches: _, ...before } = cls("Before");
    const hidden = cls("Hidden", { scan: null, commented: null, touches: readTouches("(hidden)") });
    const h = buildHandbook(undefined, apex(ok([before as ApexClassRow, hidden]), ok([])));
    assert.ok(h.apexClasses.status === "ok");
    const items = new Map(h.apexClasses.items.map((c) => [c.name, c]));
    assert.deepEqual([items.get("Before")!.touches, items.get("Before")!.isTest], [null, false]);
    assert.deepEqual([items.get("Hidden")!.touches, items.get("Hidden")!.isTest], [null, null]);
  });

  test("a test is a class annotated @isTest, not one whose comment names it", () => {
    // The row's isTest finds @isTest anywhere, as the rules that use it expect; the handbook asks the scan.
    const body = (code: string) => ({ isTest: /@isTest/i.test(code), scan: scanApex(code), touches: readTouches(code) });
    const svc = cls("Svc", body("public class Svc { // Tests live in the @isTest class SvcTest\n public void run() { } }"));
    const svcTest = cls("SvcTest", body("@isTest private class SvcTest { @isTest static void runs() { new Svc().run(); } }"));
    assert.equal(svc.isTest, true);
    const h = buildHandbook(undefined, apex(ok([svc, svcTest]), ok([])));
    assert.ok(h.apexClasses.status === "ok");
    assert.deepEqual(h.apexClasses.items.map((c) => [c.name, c.isTest]), [["Svc", false], ["SvcTest", true]]);
  });

  test("each part is ok or unavailable on its own, with its source's reason", () => {
    const failed = (reason: string) => ({ status: "unavailable" as const, reason, source });
    const noClasses = buildHandbook(undefined, apex(failed("ApexClass isn't available in this org, or to the connected user."), ok([trg("T")])));
    assert.deepEqual(noClasses.apexClasses, { status: "unavailable", reason: "ApexClass isn't available in this org, or to the connected user." });
    assert.equal(noClasses.apexTriggers.status, "ok");
    const noTriggers = buildHandbook(undefined, apex(ok([cls("C")]), failed("ApexTrigger isn't available in this org, or to the connected user.")));
    assert.equal(noTriggers.apexClasses.status, "ok");
    assert.deepEqual(noTriggers.apexTriggers, { status: "unavailable", reason: "ApexTrigger isn't available in this org, or to the connected user." });
    // Nor does the metadata's state reach them, or theirs the metadata's.
    assert.equal(noClasses.flows.status, "unavailable");
    const metadataFailed = buildHandbook({ status: "unavailable", reason: "Salesforce said no.", source: { kind: "metadata", path: "Flow" } }, apex(ok([cls("C")]), ok([])));
    assert.deepEqual([metadataFailed.flows.status, metadataFailed.apexClasses.status, metadataFailed.apexTriggers.status], ["unavailable", "ok", "ok"]);
  });

  test("Apex that doesn't fit the format leaves no part kept, as the rest does", () => {
    const bad = cls("Bad", { touches: { queries: ["Not an identifier"], queriesTotal: 1, changes: [], dynamicQueries: false, searches: false } });
    const h = buildHandbook(undefined, apex(ok([bad]), ok([])));
    for (const part of [h.flows, h.validationRules, h.apexClasses, h.apexTriggers]) {
      assert.ok(part.status === "unavailable");
      assert.equal(part.reason, "The handbook's data didn't fit its format at apexClasses.items.0.touches.queries.0, so it wasn't kept.");
    }
  });

  test("a Professional Edition org's reason passes through unchanged", async () => {
    const simulator = new Simulator({ failQuery: (soql) => (/FROM Apex(Class|Trigger) WHERE/.test(soql) ? { errorCode: "INVALID_TYPE", message: `sObject type '${/FROM (\w+)/.exec(soql)![1]}' is not supported.` } : null) });
    const professional: Transport = {
      send: async (request) => {
        const response = await simulator.send(request);
        return decodeURIComponent(request.url).includes("FROM Organization") ? { ...response, text: response.text.replace("Enterprise Edition", "Professional Edition") } : response;
      },
    };
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: professional, log: () => {} });
    const h = Handbook.parse(JSON.parse(read(result.dir, HANDBOOK_FILE)));
    for (const part of [h.apexClasses, h.apexTriggers]) assert.deepEqual(part, { status: "unavailable", reason: "Professional Edition has no Apex, so there's no code to check." });
    assert.equal(h.flows.status, "ok");
  });

  test("a run with no apex step says so", () => {
    const h = buildHandbook(undefined);
    assert.deepEqual([h.apexClasses, h.apexTriggers], [{ status: "unavailable", reason: "This run didn't read the org's Apex." }, { status: "unavailable", reason: "This run didn't read the org's Apex." }]);
  });
});
