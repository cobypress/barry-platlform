import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { sfId } from "../../testing/synthetic-org.ts";
import { automationOf, INVENTORY_COLUMNS, inventoryRows, OBJECT_INVENTORY } from "../configuration/inventory.ts";
import { Metric } from "../../schemas/index.ts";

let extracts: Map<string, unknown>;

before(async () => {
  const { dir } = await runFromRecording();
  extracts = loadExtracts(dir);
});

const inventory = (changed = extracts): Metric => {
  const m = analyseExtracts(changed).metrics.find((x) => x.id === OBJECT_INVENTORY);
  assert.ok(m, "the inventory metric should be there");
  return m;
};
const measured = (m: Metric) => {
  assert.equal(m.status, "measured", m.status === "not_measurable" ? m.reason : "");
  return m as Extract<Metric, { status: "measured" }>;
};
const row = (m: Metric, object: string) => inventoryRows(m).find((r) => r.object === object);
const unavailable = { status: "unavailable", reason: "no access", source: { kind: "soql", api: "tooling", query: "x" } };

describe("the object inventory", () => {
  test("counts the core objects and the custom objects with records, busiest first", () => {
    const m = measured(inventory());
    assert.equal(m.value, 7);
    // Ranked by automation, then custom fields. Empty_Object__c has no records.
    assert.deepEqual(inventoryRows(m).map((r) => r.object), ["Opportunity", "Invoice__c", "Lead", "Account", "Project__c", "Contact", "Project_Line__c"]);
    assert.deepEqual(m.stats, { coreObjects: 4, customObjects: 3, customFields: 167, automation: 12, undocumented: 8, objectsWithAutomation: 4 });
    assert.ok(Metric.safeParse(m).success);
  });

  test("names a custom object from the id its layouts and triggers use, in either length", () => {
    assert.deepEqual(row(inventory(), "Invoice__c")?.counts, {
      records: 12_000,
      fields: 3,
      // The inactive record type and validation rule aren't counted.
      recordTypes: 2,
      layouts: 2,
      validationRules: 1,
      flows: 1,
      processBuilders: 0,
      workflowRules: 0,
      triggers: 1,
      // The flow has no description. The rule has one, and the trigger has a comment.
      undocumented: 1,
    });
  });

  test("counts every kind of automation on a core object, and which have no description", () => {
    const opportunity = row(inventory(), "Opportunity")!;
    assert.deepEqual(opportunity.counts, { records: null, fields: 5, recordTypes: 2, layouts: 2, validationRules: 1, flows: 1, processBuilders: 1, workflowRules: 1, triggers: 2, undocumented: 5 });
    assert.equal(automationOf(opportunity), 6);
    assert.equal(opportunity.reasons.records, "Salesforce keeps no count for this object");
  });

  test("every object has every column, so the table can be rebuilt from the metric", () => {
    const m = measured(inventory());
    assert.equal(m.segments.length, 7 * INVENTORY_COLUMNS.length);
    for (const s of m.segments) assert.ok(s.value !== null || s.notMeasurableReason, `${s.dimension} ${s.key}`);
  });

  test("leaves out objects from installed packages, even with records", () => {
    const changed = clone(extracts);
    (changed.get("record-counts") as { data: Array<{ name: string; count: number }> }).data.push({ name: "pkg__Widget__c", count: 30 });
    assert.equal(row(inventory(changed), "pkg__Widget__c"), undefined);
  });

  test("an unreadable list of custom objects leaves their layouts and triggers out, and says so", () => {
    const changed = clone(extracts);
    changed.set("custom-objects", unavailable);
    const m = measured(inventory(changed));
    assert.deepEqual([row(m, "Invoice__c")?.counts.layouts, row(m, "Invoice__c")?.counts.triggers, row(m, "Project__c")?.counts.layouts], [0, 0, 0]);
    assert.ok(m.exclusions.some((e) => e.startsWith("Page layouts (3) and Apex triggers (1) on custom objects we couldn't name") && e.includes("no access")), m.exclusions.join(" | "));
  });

  test("two objects that would share a name are both left unnamed, not guessed", () => {
    const changed = clone(extracts);
    (changed.get("custom-objects") as { data: unknown[] }).data.push({ Id: sfId("01I", 5), DeveloperName: "Invoice", NamespacePrefix: null });
    const m = measured(inventory(changed));
    assert.deepEqual([row(m, "Invoice__c")?.counts.layouts, row(m, "Invoice__c")?.counts.triggers], [0, 0]);
    assert.ok(m.exclusions.some((e) => e.startsWith("Page layouts (2) and Apex triggers (1)")));
  });

  test("unreadable layouts are not read, never zero", () => {
    const changed = clone(extracts);
    (changed.get("layouts") as { layouts: unknown }).layouts = unavailable;
    const m = measured(inventory(changed));
    assert.equal(row(m, "Opportunity")?.counts.layouts, null);
    assert.equal(row(m, "Opportunity")?.reasons.layouts, "Page layouts couldn't be read");
    assert.ok(m.exclusions.some((e) => e.startsWith("Page layouts, which couldn't be read")));
  });

  test("without record counts, every custom object of your own is counted and the assumption is stated", () => {
    const changed = clone(extracts);
    changed.set("record-counts", unavailable);
    const m = measured(inventory(changed));
    assert.ok(row(m, "Empty_Object__c"), "an object with no records can't be told apart without counts");
    assert.ok(m.assumptions.some((a) => a.startsWith("Record counts couldn't be read")));
  });

  test("a custom object missing from the retrieve shows not read, not zero", () => {
    const changed = clone(extracts);
    (changed.get("record-counts") as { data: Array<{ name: string; count: number }> }).data.push({ name: "Missing__c", count: 5 });
    const missing = row(inventory(changed), "Missing__c")!;
    assert.deepEqual([missing.counts.fields, missing.counts.recordTypes], [null, null]);
    assert.equal(missing.reasons.fields, "Not in the metadata retrieve");
  });

  test("is not measurable without the metadata retrieve", () => {
    const changed = clone(extracts);
    changed.set("metadata", unavailable);
    const m = inventory(changed);
    assert.equal(m.status, "not_measurable");
    assert.match(m.status === "not_measurable" ? m.reason : "", /^Object metadata couldn't be retrieved/);
  });

  test("follows DECISIONS 37 when Opportunity can't be read", () => {
    const changed = clone(extracts);
    (changed.get("opportunities") as { opportunities: unknown }).opportunities = unavailable;
    assert.equal(inventory(changed).status, "not_measurable");
  });
});
