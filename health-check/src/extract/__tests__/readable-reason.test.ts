import { test } from "node:test";
import assert from "node:assert/strict";
import { readableReason } from "../types.ts";

// Real error shapes from a live org. The query is already on the evidence, so a
// reason that reaches report text keeps only what went wrong.
const invalidType = "INVALID_TYPE: \nDeveloperName, IsActive, IsAmount FROM ForecastingType\n                                       ^\nERROR at Row:1:Column:51\nsObject type 'ForecastingType' is not supported. If you are attempting to use a custom object, be sure to append the '__c' after the entity name.";
const invalidField = "INVALID_FIELD: \nSELECT Id, Foo__c FROM Account\n           ^\nERROR at Row:1:Column:12\nNo such column 'Foo__c' on entity 'Account'. If you are attempting to use a custom field, be sure to append the '__c' after the custom field name.";
const other = "MALFORMED_QUERY: \nSELECT FROM\n       ^\nERROR at Row:1:Column:8\nunexpected token: FROM";

test("a missing object names the object, not the query", () => {
  assert.equal(readableReason(invalidType), "ForecastingType isn't available in this org, or to the connected user.");
});

test("a missing field names the field", () => {
  assert.equal(readableReason(invalidField), "Account.Foo__c isn't in this org.");
});

test("any other query error keeps its code and last line, never the query", () => {
  const reason = readableReason(other);
  assert.equal(reason, "MALFORMED_QUERY: unexpected token: FROM");
  assert.doesNotMatch(reason, /\n|\^/);
});

test("a message with no quoted query is unchanged", () => {
  assert.equal(readableReason("INSUFFICIENT_ACCESS: no"), "INSUFFICIENT_ACCESS: no");
});
