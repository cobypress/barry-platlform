import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { MAX_QUERIED_OBJECTS, readTouches, type ApexTouches } from "../apex-touches.ts";

// What a class or trigger touches, read from its text (DECISIONS.md 457).

const touches = (body: string): ApexTouches => readTouches(body) ?? assert.fail("read as hidden");

describe("the objects its queries read", () => {
  test("the outermost FROM is kept", () => {
    assert.deepEqual(touches("public class A { void x() { List<Account> a = [SELECT Id, Name FROM Account WHERE Name != null LIMIT 10]; } }").queries, ["Account"]);
  });

  test("the object of a query nested in a WHERE is kept, however deep its parentheses", () => {
    const t = touches("class A { void x() { delete [SELECT Id FROM Account WHERE (Id IN (SELECT AccountId FROM Contact) OR Name = :n) AND Id NOT IN (SELECT AccountId FROM Case)]; } }");
    assert.deepEqual(t.queries, ["Account", "Case", "Contact"]);
  });

  test("a child relationship subquery in the SELECT list isn't an object, and isn't kept", () => {
    const t = touches("class A { void x() { for (Account a : [SELECT Id, (SELECT Id FROM Contacts), (SELECT Id FROM Opportunities WHERE IsClosed = false) FROM Account]) { } } }");
    assert.deepEqual(t.queries, ["Account"]);
    assert.equal(t.queriesTotal, 1);
  });

  test("a query inside a comment or a string isn't read", () => {
    const t = touches("class A { // [SELECT Id FROM Commented__c]\n /* [SELECT Id FROM Blocked__c] */ String q = '[SELECT Id FROM Quoted__c]'; void x() { Lead l = [SELECT Id FROM Lead WHERE Company = 'FROM Inside__c']; } }");
    assert.deepEqual(t.queries, ["Lead"]);
  });

  test("sorted, each object once under the first spelling the code uses", () => {
    const t = touches("class A { void x() { a = [select Id from opportunity]; b = [SELECT Id FROM Opportunity]; c = [SELECT Id FROM Invoice__c]; d = [SELECT Id FROM account]; } }");
    assert.deepEqual(t.queries, ["account", "Invoice__c", "opportunity"]);
    assert.equal(t.queriesTotal, 3);
  });

  test(`more than ${MAX_QUERIED_OBJECTS} objects are cut, with the full count beside them`, () => {
    assert.equal(MAX_QUERIED_OBJECTS, 50);
    const body = (n: number) => `class A { void x() { ${Array.from({ length: n }, (_, i) => `q = [SELECT Id FROM Object_${String(i).padStart(3, "0")}__c];`).join(" ")} } }`;
    const at = touches(body(50));
    assert.deepEqual([at.queries.length, at.queriesTotal], [50, 50]);
    const over = touches(body(51));
    assert.deepEqual([over.queries.length, over.queriesTotal, over.queries.at(-1)], [50, 51, "Object_049__c"]);
  });
});

describe("the kinds of change it makes", () => {
  test("statements and Database methods, in a fixed order", () => {
    const t = touches("class A { void x() { if (ok) upsert recs; else delete old; Database.convertLead(lc); insert new Account(Name = 'x'); undelete [SELECT Id FROM Account ALL ROWS]; merge a b; } }");
    assert.deepEqual(t.changes, ["insert", "upsert", "delete", "undelete", "merge", "convertLead"]);
  });

  test("Database.insert is read as an insert", () => {
    assert.deepEqual(touches("class A { void x() { Database.SaveResult[] r = Database.insert(recs, false); } }").changes, ["insert"]);
  });

  test("update or delete as a method or variable name isn't a change", () => {
    const t = touches("class A { void x() { obj.update(x); myList.delete(); cache.remove(k); Integer updateCount = 0; updated = records.size(); this.insert = false; } }");
    assert.deepEqual(t.changes, []);
  });

  test("a change in a comment or a string isn't read", () => {
    assert.deepEqual(touches("class A { // update records;\n String s = '; delete everything'; }").changes, []);
  });
});

describe("queries built at run time", () => {
  test("Database.query and its kin say the objects they read aren't known", () => {
    for (const call of ["Database.query(q)", "Database.queryWithBinds(q, binds, AccessLevel.USER_MODE)", "Database.countQuery(q)", "Database.getQueryLocator(q)"]) {
      assert.equal(touches(`class A { void x() { ${call}; } }`).dynamicQueries, true, call);
    }
  });

  test("getQueryLocator given an inline query reads that query's object instead", () => {
    const t = touches("class B implements Database.Batchable<SObject> { public Database.QueryLocator start(Database.BatchableContext c) { return Database.getQueryLocator([SELECT Id FROM Contact]); } }");
    assert.deepEqual([t.dynamicQueries, t.queries], [false, ["Contact"]]);
  });

  test("none, or one only in a comment or a string, is none", () => {
    assert.equal(touches("class A { // Database.query(q)\n String s = 'Database.query(q)'; }").dynamicQueries, false);
  });
});

describe("a body Salesforce hides", () => {
  test("gives null for the whole reading, never empty lists", () => {
    assert.equal(readTouches("(hidden)"), null);
    assert.equal(readTouches(undefined), null);
    assert.equal(readTouches(""), null);
    assert.deepEqual(readTouches("class A { }"), { queries: [], queriesTotal: 0, changes: [], dynamicQueries: false });
  });
});
