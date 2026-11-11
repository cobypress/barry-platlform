import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { codeOnly, scanApex } from "../apex-scan.ts";

describe("reading Apex as text", () => {
  test("comments go and strings are blanked, with every offset kept", () => {
    const body = "String a = 'x // y'; // note\n/* block */ Integer b;";
    const code = codeOnly(body);
    assert.equal(code.length, body.length);
    assert.doesNotMatch(code, /note|block|x \/\/ y/);
    assert.equal(code.indexOf("Integer"), body.indexOf("Integer"));
  });

  test("a URL in a string isn't a comment, and a real one is", () => {
    assert.equal(scanApex("public class A { String u = 'https://example.com'; }").commented, false);
    assert.equal(scanApex("// Syncs invoices.\npublic class A { }").commented, true);
    assert.equal(scanApex("public class A { /* why */ }").commented, true);
  });

  test("a query and a save inside a loop are counted, once each however deeply nested", () => {
    const scan = scanApex("public class R { void go(List<Lead> ls) { for (Lead l : ls) { while (true) { Account a = [SELECT Id FROM Account]; update l; } } } }");
    assert.equal(scan.queriesInLoops, 1);
    assert.equal(scan.dmlInLoops, 1);
  });

  test("a SOQL for loop queries once, and a save after the loop isn't inside it", () => {
    const scan = scanApex("public class H { void go() { for (Opportunity o : [SELECT Id FROM Opportunity]) { o.Name = 'x'; } update opps; } }");
    assert.equal(scan.queriesInLoops, 0);
    assert.equal(scan.dmlInLoops, 0);
  });

  test("Database methods count, and so does a loop body without braces", () => {
    const scan = scanApex("class A { void x() { while (more) Database.insert(recs); for (Id i : ids) Database.query(q); } }");
    assert.equal(scan.dmlInLoops, 1);
    assert.equal(scan.queriesInLoops, 1);
  });

  test("every Database query method the handbook reads counts in a loop, once, and not outside one, in a comment or in a string (decision 470)", () => {
    for (const method of ["queryWithBinds", "countQueryWithBinds", "getQueryLocatorWithBinds", "getCursor", "getCursorWithBinds"]) {
      const call = `Database.${method}(q, binds, AccessLevel.USER_MODE)`;
      assert.equal(scanApex(`class A { void x() { for (Id i : ids) { Object r = ${call}; } } }`).queriesInLoops, 1, `${method} in a loop`);
      assert.equal(scanApex(`class A { void x() { while (more) Database . ${method} (q); } }`).queriesInLoops, 1, `${method} spaced, in a loop without braces`);
      assert.equal(scanApex(`class A { void x() { Object r = ${call}; for (Id i : ids) { n++; } } }`).queriesInLoops, 0, `${method} before a loop`);
      assert.equal(scanApex(`class A { void x() { for (Id i : ids) { n++; // ${call}\n /* ${call} */ } } }`).queriesInLoops, 0, `${method} in a comment`);
      assert.equal(scanApex(`class A { void x() { for (Id i : ids) { String s = '${call}'; } } }`).queriesInLoops, 0, `${method} in a string`);
    }
  });

  test("a search in a loop isn't a query, and nor is a method that only ends in Database (decision 470)", () => {
    assert.equal(scanApex("class A { void x() { for (Id i : ids) { Search.query(q); Search.find(q); } } }").queriesInLoops, 0);
    assert.equal(scanApex("class A { void x() { for (Id i : ids) { MyDatabase.query(q); MyDatabase.getCursor(q); Database.fetch(q); } } }").queriesInLoops, 0);
    // A Database class reached through another name is read as it always was.
    assert.equal(scanApex("class A { void x() { for (Id i : ids) { x.Database.query(q); } } }").queriesInLoops, 1);
  });

  test("UPDATE TRACKING or VIEWSTAT in a query inside a loop isn't a save, and a real one beside it still is", () => {
    for (const query of ["[SELECT Id FROM FAQ__kav WHERE (Id = :x) UPDATE TRACKING]", "[FIND :t RETURNING FAQ__kav(Id WHERE PublishStatus = 'Online') UPDATE VIEWSTAT]"]) {
      assert.equal(scanApex(`class A { void x() { for (Id i : ids) { List<SObject> r = ${query}; } } }`).dmlInLoops, 0, query);
      assert.equal(scanApex(`class A { void x() { for (Id i : ids) { List<SObject> r = ${query}; update r; } } }`).dmlInLoops, 1, query);
    }
  });

  test("a variable called update isn't a save, and a query in a comment isn't a query", () => {
    const scan = scanApex("class A { void x() { for (Integer i = 0; i < 3; i++) { Boolean update = true; // [SELECT Id FROM Account]\n } } }");
    assert.equal(scan.queriesInLoops, 0);
    assert.equal(scan.dmlInLoops, 0);
  });

  test("test methods and assertions, in every form", () => {
    const scan = scanApex("@isTest\nprivate class T { @isTest static void a() { System.assertEquals(1, 1); } static testMethod void b() { Assert.areEqual(1, 1); } }");
    assert.equal(scan.testClass, true);
    assert.equal(scan.testMethods, 2);
    assert.equal(scan.asserts, 2);
    assert.equal(scan.seeAllData, false);
  });

  test("a test data factory has no test methods, and SeeAllData is seen", () => {
    assert.equal(scanApex("@isTest public class TestDataFactory { public static Lead lead() { return new Lead(); } }").testMethods, 0);
    const all = scanApex("@isTest(SeeAllData=true) private class S { @isTest static void a() { new S(); } }");
    assert.equal(all.seeAllData, true);
    assert.equal(all.testMethods, 1);
    assert.equal(all.asserts, 0);
  });

  test("a class with test methods but no class annotation still counts against the size limit", () => {
    assert.equal(scanApex("public class Old { static testMethod void t() { } }").testClass, false);
  });

  test("callout addresses: written in, through a named credential, and one in a comment that doesn't count", () => {
    const scan = scanApex("public class S { // req.setEndpoint('https://old.example.com');\n void s() { r.setEndpoint('https://new.example.com/api?x=\\'y\\''); r.setEndpoint('callout:Finance_API/v2'); r.setEndpoint(base); } }");
    assert.equal(scan.literalEndpoints, 1);
    assert.equal(scan.namedCredentialEndpoints, 1);
  });
});

describe("a test class with another annotation beside @isTest (night 3 review)", () => {
  test("is still a test class, with no test methods of its own", () => {
    for (const body of [
      "@isTest\n@SuppressWarnings('PMD.ExcessivePublicCount')\npublic class TestDataFactory { public static Lead lead() { return new Lead(); } }",
      "@SuppressWarnings('PMD')\n@isTest\npublic class TestDataFactory { public static Lead lead() { return new Lead(); } }",
      "@isTest(SeeAllData=false) @SuppressWarnings('PMD') private class Factory { }",
    ]) {
      const scan = scanApex(body);
      assert.equal(scan.testClass, true, body);
      assert.equal(scan.testMethods, 0, body);
    }
    // A class that isn't a test, with an annotation, stays one that isn't.
    assert.equal(scanApex("@SuppressWarnings('PMD')\npublic class Real { @isTest static void t() { } }").testClass, false);
  });
});
