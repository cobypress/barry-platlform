import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { RETRIEVED_STANDARD_OBJECTS } from "../automation.ts";
import { parseMetadataBundle } from "../metadata-parse.ts";
import { METADATA_FILES, userId } from "../../testing/synthetic-org.ts";

// As the engine reads the synthetic org: the four in full, and Case, which its
// record counts list, for its validation rules only (DECISIONS.md 460).
const bundle = parseMetadataBundle(METADATA_FILES, RETRIEVED_STANDARD_OBJECTS, ["Case"]);
const flow = (name: string) => bundle.flows.find((f) => f.name === name)!;

describe("flows", () => {
  test("reads the core attributes", () => {
    const f = flow("Opportunity_Route_Deal");
    assert.equal(f.status, "Active");
    assert.equal(f.object, "Opportunity");
    assert.equal(f.triggerType, "RecordAfterSave");
    assert.equal(f.apiVersion, 62);
  });
  test("counts writes and fault paths", () => {
    assert.deepEqual([flow("Opportunity_Route_Deal").dmlOrActions, flow("Opportunity_Route_Deal").withFault], [1, 0]);
    assert.deepEqual([flow("Lead_Assign").dmlOrActions, flow("Lead_Assign").withFault], [1, 1]);
  });
  test("one child element parses the same as many", () => assert.equal(flow("Renewal_Notice").dmlOrActions, 1));
  test("finds a Process Builder's object in processMetadataValues", () => assert.equal(flow("Opportunity_Old_Process").object, "Opportunity"));
  test("spots hardcoded user ids and emails", () => {
    assert.equal(flow("Opportunity_Route_Deal").hardcodedUserIds, 1);
    assert.equal(flow("Renewal_Notice").hardcodedEmails, 1);
  });
});

describe("workflow rules and objects", () => {
  test("reads which rules are active", () => {
    const opportunity = bundle.workflows.find((w) => w.object === "Opportunity")!;
    assert.deepEqual(opportunity.rules.map((r) => [r.name, r.active]), [["Notify_Large_Deal", true], ["Old_Rule", false]]);
  });
  test("reads validation rules", () =>
    assert.deepEqual(bundle.validationRules, [
      { object: "Case", name: "Reason_When_Escalated", active: true, described: false },
      { object: "Invoice__c", name: "Due_Date_Required", active: true, described: true },
      { object: "Invoice__c", name: "Old_Check", active: false, described: false },
      { object: "Opportunity", name: "Needs_Competitor", active: true, described: true },
    ]));
  test("reads record types, and which are active", () =>
    assert.deepEqual(bundle.recordTypes.map((r) => [r.object, r.name, r.active]), [["Empty_Object__c", "Default", true], ["Invoice__c", "Standard", true], ["Invoice__c", "Credit_Note", true], ["Invoice__c", "Old_Format", false]]));
  test("lists every object defined, including one with nothing in it", () =>
    assert.deepEqual(bundle.objects, ["Empty_Object__c", "Invoice__c", "Opportunity", "Project__c", "Project_Line__c"]));
  test("a single record type parses the same as many", () => {
    const one = parseMetadataBundle({ "objects/A__c.object": "<CustomObject><recordTypes><fullName>Only</fullName><active>true</active></recordTypes></CustomObject>" });
    // With no <label>, the developer name is the only name there is.
    assert.deepEqual(one.recordTypes, [{ object: "A__c", name: "Only", label: "Only", active: true }]);
  });
  test("reads which of your own custom fields have help text or a description", () =>
    assert.deepEqual(
      bundle.customFields.map((f) => [f.name, f.described, f.helpText]),
      [["Invoice_Total__c", true, false], ["Due_Date__c", false, false], ["Paid__c", false, false], ["Old_Region__c", false, false], ["Legacy_Code__c", false, false], ["Deal_Score__c", false, false], ["Competitor__c", false, true], ["Project_Code__c", false, false]],
    ));
  test("reads which flows and workflow rules have a description", () => {
    assert.deepEqual(bundle.flows.filter((f) => f.described).map((f) => f.name), ["Lead_Assign"]);
    assert.deepEqual(bundle.workflows.flatMap((w) => w.rules).filter((r) => r.described).map((r) => r.name), ["Notify_Overdue"]);
  });
  test("package.xml isn't counted", () => assert.equal(bundle.filesRead, Object.keys(METADATA_FILES).length - 1));
});

describe("the reference index", () => {
  test("indexes user ids by their 15-character form", () => {
    assert.deepEqual(bundle.references.users[userId(7).slice(0, 15)], ["flows/Opportunity_Route_Deal.flow"]);
  });
  test("a 15-character id still matches an 18-character search", () => {
    const short = parseMetadataBundle({ "flows/A.flow": `<Flow><x>${userId(7).slice(0, 15)}</x></Flow>` });
    assert.ok(short.references.users[userId(7).slice(0, 15)]);
  });
  test("indexes emails in lower case", () => {
    const b = parseMetadataBundle({ "workflows/Lead.workflow": "<Workflow><x>Sam.Reed@Example.com</x></Workflow>" });
    assert.ok(b.references.users["sam.reed@example.com"]);
  });
  test("indexes custom fields used in flows and validation formulas", () => {
    assert.deepEqual(bundle.references.fields.Legacy_Code__c, ["flows/Opportunity_Route_Deal.flow"]);
    assert.deepEqual(bundle.references.fields.Competitor__c, ["objects/Opportunity.object#validationRules.Needs_Competitor"]);
    // A standard object read for its rules only still has its formulas searched.
    assert.deepEqual(bundle.references.fields.Escalation_Reason__c, ["objects/Case.object#validationRules.Reason_When_Escalated"]);
  });
  test("a field's own definition isn't a reference to it", () => assert.equal(bundle.references.fields.Old_Region__c, undefined));
  test("approval processes are searched too", () => {
    const b = parseMetadataBundle({ "approvalProcesses/Opp.approvalProcess": `<ApprovalProcess><x>${userId(9)}</x></ApprovalProcess>` });
    assert.ok(b.references.users[userId(9).slice(0, 15)]);
  });
});

describe("standard objects read for their validation rules only (DECISIONS.md 460)", () => {
  const CASE = `<CustomObject>
    <fields><fullName>Escalation_Reason__c</fullName><type>Text</type></fields>
    <recordTypes><fullName>Support</fullName><active>true</active><label>Support</label></recordTypes>
    <validationRules><fullName>Needs_Reason</fullName><active>true</active><errorConditionFormula>ISBLANK(Escalation_Reason__c)</errorConditionFormula><errorMessage>Say why.</errorMessage></validationRules>
  </CustomObject>`;
  const files = { "objects/Case.object": CASE, "objects/A__c.object": "<CustomObject><fields><fullName>B__c</fullName></fields></CustomObject>" };

  test("keeps the rules, their detail and what their formulas mention, and not the object, its fields or its record types", () => {
    const b = parseMetadataBundle(files, ["Account"], ["Case"]);
    assert.deepEqual(b.validationRules, [{ object: "Case", name: "Needs_Reason", active: true, described: false }]);
    assert.deepEqual(b.handbook!.validationRules.map((r) => [r.object, r.name, r.formula, r.message]), [["Case", "Needs_Reason", "ISBLANK(Escalation_Reason__c)", "Say why."]]);
    assert.deepEqual(b.references.fields.Escalation_Reason__c, ["objects/Case.object#validationRules.Needs_Reason"]);
    assert.deepEqual(b.objects, ["A__c"]);
    assert.deepEqual(b.customFields.map((f) => `${f.object}.${f.name}`), ["A__c.B__c"]);
    assert.deepEqual(b.recordTypes, []);
    assert.equal(b.filesRead, 2);
  });

  test("a standard object not named either way is read in full, as before", () => {
    const b = parseMetadataBundle(files, ["Account"]);
    assert.deepEqual(b.objects, ["A__c", "Case"]);
    assert.deepEqual(b.customFields.map((f) => `${f.object}.${f.name}`), ["A__c.B__c", "Case.Escalation_Reason__c"]);
    assert.deepEqual(b.recordTypes.map((r) => `${r.object}.${r.name}`), ["Case.Support"]);
    assert.deepEqual(b.validationRules.map((r) => r.object), ["Case"]);
  });

  test("says it covers the objects named in full, then the ones read for their rules, in the order given", () => {
    assert.deepEqual(parseMetadataBundle({}, RETRIEVED_STANDARD_OBJECTS, ["Case", "Quote", "User"]).handbook!.validationRulesCover, { customObjects: "all", standardObjects: ["Account", "Contact", "Lead", "Opportunity", "Case", "Quote", "User"] });
    assert.deepEqual(parseMetadataBundle({}, RETRIEVED_STANDARD_OBJECTS).handbook!.validationRulesCover.standardObjects, ["Account", "Contact", "Lead", "Opportunity"]);
  });
});

describe("secrets", () => {
  test("are gone before anything is kept", () => {
    const b = parseMetadataBundle({ "flows/A.flow": "<Flow><status>Active</status><apiKey>supersecretvalue</apiKey></Flow>" });
    assert.doesNotMatch(JSON.stringify(b), /supersecretvalue/);
  });

  test("same-record updates: $Record, an unfiltered update, and a filter on the triggering record count, siblings don't", () => {
    const flow = (updates: string): string => `<Flow><apiVersion>62.0</apiVersion><label>F</label><status>Active</status><start><object>Opportunity</object><triggerType>RecordAfterSave</triggerType></start>${updates}</Flow>`;
    const count = (updates: string): number => parseMetadataBundle({ "flows/F.flow": flow(updates) }).flows[0]!.sameRecordUpdates;
    assert.equal(count("<recordUpdates><name>A</name><inputReference>$Record</inputReference></recordUpdates>"), 1);
    assert.equal(count("<recordUpdates><name>A</name><object>Opportunity</object></recordUpdates>"), 1);
    assert.equal(count("<recordUpdates><name>A</name><object>Opportunity</object><filters><field>Id</field><value><elementReference>$Record.Id</elementReference></value></filters></recordUpdates>"), 1);
    assert.equal(count("<recordUpdates><name>A</name><object>Opportunity</object><filters><field>AccountId</field><value><elementReference>$Record.AccountId</elementReference></value></filters></recordUpdates>"), 0);
    assert.equal(count("<recordUpdates><name>A</name><object>Account</object></recordUpdates>"), 0);
  });
});
