import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { parseMetadataBundle } from "../metadata-parse.ts";
import { METADATA_FILES, userId } from "../../testing/synthetic-org.ts";

const bundle = parseMetadataBundle(METADATA_FILES);
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
    assert.deepEqual(one.recordTypes, [{ object: "A__c", name: "Only", active: true }]);
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
  });
  test("a field's own definition isn't a reference to it", () => assert.equal(bundle.references.fields.Old_Region__c, undefined));
  test("approval processes are searched too", () => {
    const b = parseMetadataBundle({ "approvalProcesses/Opp.approvalProcess": `<ApprovalProcess><x>${userId(9)}</x></ApprovalProcess>` });
    assert.ok(b.references.users[userId(9).slice(0, 15)]);
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
