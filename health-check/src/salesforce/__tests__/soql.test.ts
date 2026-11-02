import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { assertSafeSoql, UnfilterableFieldError, UnsafeSoqlError } from "../soql.ts";

describe("platform behaviour: fields that can't be filtered", () => {
  for (const field of ["Section", "Display", "Action"]) {
    test(`SetupAuditTrail.${field} in WHERE`, () => {
      assert.throws(() => assertSafeSoql(`SELECT Id FROM SetupAuditTrail WHERE ${field} = 'x'`), UnfilterableFieldError);
    });
  }
  for (const field of ["Status", "Application"]) {
    test(`LoginHistory.${field} in WHERE`, () => {
      assert.throws(() => assertSafeSoql(`SELECT Id FROM LoginHistory WHERE LoginTime > 2026-01-01T00:00:00Z AND ${field} = 'x'`), UnfilterableFieldError);
    });
  }
  test("case doesn't help", () => assert.throws(() => assertSafeSoql("select id from loginhistory where status = 'x'"), UnfilterableFieldError));
  test("HAVING is a filter too", () => assert.throws(() => assertSafeSoql("SELECT COUNT(Id) FROM LoginHistory GROUP BY UserId HAVING Status = 'x'"), UnfilterableFieldError));
  test("selecting those fields is fine", () => assertSafeSoql("SELECT Status, Application FROM LoginHistory WHERE LoginTime > 2026-01-01T00:00:00Z"));
  test("ordering by a filterable date is fine", () => assertSafeSoql("SELECT Section FROM SetupAuditTrail WHERE CreatedDate > 2026-01-01T00:00:00Z ORDER BY CreatedDate DESC"));
  test("a related field with the same name isn't caught", () => assertSafeSoql("SELECT Id FROM SetupAuditTrail WHERE CreatedBy.Action = 'x'"));
  test("the same word on another object is fine", () => assertSafeSoql("SELECT Id FROM Lead WHERE Status = 'Open'"));
  test("a value inside a string is fine", () => assertSafeSoql("SELECT Id FROM LoginHistory WHERE UserId = 'Status = x'"));
});

describe("side effects and non-queries", () => {
  test("rejects anything that isn't a SELECT", () => {
    for (const soql of ["", "   ", "DELETE FROM Account", "UPDATE Account SET Name = 'x'", "FIND {x}"]) assert.throws(() => assertSafeSoql(soql), UnsafeSoqlError);
  });
  test("rejects two statements", () => assert.throws(() => assertSafeSoql("SELECT Id FROM Account; DELETE FROM Account"), UnsafeSoqlError));
  test("rejects UPDATE TRACKING and UPDATE VIEWSTAT", () => {
    assert.throws(() => assertSafeSoql("SELECT Id FROM FAQ__kav UPDATE TRACKING"), UnsafeSoqlError);
    assert.throws(() => assertSafeSoql("SELECT Id FROM FAQ__kav UPDATE VIEWSTAT"), UnsafeSoqlError);
  });
  test("a clause word inside a string isn't a clause", () => assertSafeSoql("SELECT Id FROM Account WHERE Name = 'FOR VIEW; thing'"));
  test("an escaped quote doesn't end the string early", () => assertSafeSoql("SELECT Id FROM Account WHERE Name = 'it\\'s FOR UPDATE'"));
});
