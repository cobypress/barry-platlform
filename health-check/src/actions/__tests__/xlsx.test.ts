import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { strFromU8, unzipSync } from "fflate";
import { columnName, writeXlsx, type Sheet } from "../xlsx.ts";

const parts = (bytes: Uint8Array): Record<string, string> => Object.fromEntries(Object.entries(unzipSync(bytes)).map(([k, v]) => [k, strFromU8(v)]));
const sheet = (over: Partial<Sheet> = {}): Sheet => ({ name: "Issues", columns: [{ header: "Id", width: 10 }, { header: "Link", width: 20 }], rows: [], ...over });

describe("the workbook writer", () => {
  test("names columns as a spreadsheet does", () => {
    assert.deepEqual([0, 25, 26, 27, 51, 52, 701, 702].map(columnName), ["A", "Z", "AA", "AB", "AZ", "BA", "ZZ", "AAA"]);
  });

  test("writes the parts a spreadsheet needs, with a frozen header and a filter", () => {
    const files = parts(writeXlsx([sheet({ notes: ["A note."], rows: [["x", 1]] })]));
    for (const p of ["[Content_Types].xml", "_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels", "xl/styles.xml", "xl/worksheets/sheet1.xml"]) assert.ok(files[p], p);
    const xml = files["xl/worksheets/sheet1.xml"]!;
    assert.match(xml, /<pane ySplit="2" topLeftCell="A3"/);
    assert.match(xml, /<autoFilter ref="A2:B3"\/>/);
    assert.match(files["xl/workbook.xml"]!, /_xlnm\._FilterDatabase/);
  });

  test("text that looks like a formula stays text, and markup is escaped", () => {
    const xml = parts(writeXlsx([sheet({ rows: [["=HYPERLINK(\"http://x\")", "<b>&\u0007"]] })]))["xl/worksheets/sheet1.xml"]!;
    assert.doesNotMatch(xml, /<f>/);
    assert.match(xml, /t="inlineStr" s="4"><is><t xml:space="preserve">=HYPERLINK\(&quot;http:\/\/x&quot;\)<\/t>/);
    assert.match(xml, /&lt;b&gt;&amp;<\/t>/);
  });

  test("links out go through the sheet's relationships, and links between sheets by location", () => {
    const files = parts(writeXlsx([sheet({ rows: [["a", { text: "Open", url: "https://synthetic.example.com/006000000000001AAA" }], ["b", { text: "See", sheet: "R1.1" }]] }), sheet({ name: "R1.1" })]));
    assert.match(files["xl/worksheets/_rels/sheet1.xml.rels"]!, /Target="https:\/\/synthetic\.example\.com\/006000000000001AAA" TargetMode="External"/);
    // No notes, so the header is row 1 and the rows start at 2.
    assert.match(files["xl/worksheets/sheet1.xml"]!, /<hyperlink ref="B2" r:id="rId1"\/>/);
    assert.match(files["xl/worksheets/sheet1.xml"]!, /<hyperlink ref="B3" location="'R1\.1'!A1" display="See"\/>/);
  });

  test("refuses a sheet name a spreadsheet can't hold, or the same name twice", () => {
    assert.throws(() => writeXlsx([sheet({ name: "a/b" })]));
    assert.throws(() => writeXlsx([sheet({ name: "x".repeat(32) })]));
    assert.throws(() => writeXlsx([sheet(), sheet({ name: "ISSUES" })]));
  });

  test("the same sheets make the same bytes", () => {
    assert.deepEqual(writeXlsx([sheet({ rows: [["a", 1]] })]), writeXlsx([sheet({ rows: [["a", 1]] })]));
  });
});
