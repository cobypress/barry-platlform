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
  test("reads validation rules", () => assert.deepEqual(bundle.validationRules, [{ object: "Opportunity", name: "Needs_Competitor", active: true }]));
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
});
