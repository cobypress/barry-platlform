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

  test("UPDATE TRACKING and UPDATE VIEWSTAT in a query or a search aren't an update, nor is FOR UPDATE", () => {
    for (const query of [
      "[FIND :term IN ALL FIELDS RETURNING KnowledgeArticleVersion(Id, Title WHERE PublishStatus = 'Online') UPDATE TRACKING]",
      "[SELECT Id FROM FAQ__kav WHERE (Id = :x) UPDATE TRACKING]",
      "[SELECT Id FROM FAQ__kav WITH DATA CATEGORY Geography__c AT (usa__c, uk__c) UPDATE VIEWSTAT]",
      "[SELECT Id FROM Account FOR UPDATE]",
    ]) {
      assert.deepEqual(touches(`class A { void x() { List<SObject> r = ${query}; } }`).changes, [], query);
    }
  });

  test("a real update on the statement after a query is still read, and so is one given a query", () => {
    assert.deepEqual(touches("class A { void x() { Account a = [SELECT Id FROM Account WHERE (Id = :x) LIMIT 1]; update a; } }").changes, ["update"]);
    assert.deepEqual(touches("class A { void x() { update [SELECT Id FROM Account WHERE (Id = :x)]; } }").changes, ["update"]);
  });
});

describe("queries built at run time", () => {
  test("Database.query and its kin say the objects they read aren't known", () => {
    for (const call of ["Database.query(q)", "Database.queryWithBinds(q, binds, AccessLevel.USER_MODE)", "Database.countQuery(q)", "Database.getQueryLocator(q)"]) {
      assert.equal(touches(`class A { void x() { ${call}; } }`).dynamicQueries, true, call);
    }
  });

  test("the WithBinds forms, a cursor and a search built at run time say the same", () => {
    for (const call of [
      "Database.getQueryLocatorWithBinds(q, binds, AccessLevel.USER_MODE)",
      "Database.countQueryWithBinds(q, binds, AccessLevel.USER_MODE)",
      "Database.getCursor(q)",
      "Database.getCursorWithBinds(q, binds, AccessLevel.USER_MODE)",
      "Search.query(q)",
    ]) {
      const t = touches(`class A { void x() { ${call}; } }`);
      assert.deepEqual([t.dynamicQueries, t.queries], [true, []], call);
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

describe("searches", () => {
  test("an inline search, Search.query or Search.find says it searches, and only Search.query is also a query built at run time", () => {
    const inline = touches("class A { void x() { List<List<SObject>> r = [FIND :term IN ALL FIELDS RETURNING Account(Id), Contact(Id)]; } }");
    assert.deepEqual([inline.searches, inline.dynamicQueries, inline.queries], [true, false, []]);
    const query = touches("class A { void x() { List<List<SObject>> r = Search.query(q); } }");
    assert.deepEqual([query.searches, query.dynamicQueries], [true, true]);
    assert.equal(touches("class A { void x() { Search.SearchResults r = Search.find(q); } }").searches, true);
  });

  test("a class that only queries, or names a search in a comment or a string, doesn't search", () => {
    assert.equal(touches("class A { void x() { Account a = [SELECT Id FROM Account]; Database.query(q); } }").searches, false);
    assert.equal(touches("class A { // [FIND 'x' RETURNING Account]\n String s = 'Search.query(q)'; String find = 'x'; }").searches, false);
  });
});

describe("a body Salesforce hides", () => {
  test("gives null for the whole reading, never empty lists", () => {
    assert.equal(readTouches("(hidden)"), null);
    assert.equal(readTouches(undefined), null);
    assert.equal(readTouches(""), null);
    assert.deepEqual(readTouches("class A { }"), { queries: [], queriesTotal: 0, changes: [], dynamicQueries: false, searches: false });
  });
});
