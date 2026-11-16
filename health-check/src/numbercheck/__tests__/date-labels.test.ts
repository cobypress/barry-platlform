import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { checkNumbers, extractFigures, type FigureSet } from "../index.ts";

// A label is a name holding a digit, claimed before dates are read. Claimed out
// of the middle of a date in digits, it left the rest to be read as a figure:
// "2026-08" out of "2026-08-18" left -18, and held engine text on a real run
// (DECISIONS.md 551, 553).

const read = (text: string, labels: string[] = []) => extractFigures(text, { labels }).figures;
const brief = (text: string, labels: string[] = []) => read(text, labels).map((f) => `${f.kind}:${f.raw}`);
const EMPTY: FigureSet = { figures: [], dates: [], identifiers: [], tokens: [], labels: [] };

const LINE = "What the checks left out:\nConverted leads\nLeads created since 2026-08-18\nInactive users";

describe("a label inside a date in digits", () => {
  test("the line that held a section reads its date whatever part of it is a label", () => {
    assert.deepEqual(brief(LINE), ["date:2026-08-18"]);
    for (const labels of [["2026-08"], ["08"], ["2026"], ["18"], ["since 2026"], ["2026-08", "08", "18"]]) assert.deepEqual(read(LINE, labels), read(LINE), JSON.stringify(labels));
  });

  test("through the check: a date the data holds passes, one it doesn't is unmatched as it is with no label", () => {
    const monthKey = [{ source: "adoption.monthly_active_users", value: "2026-08" }];
    assert.equal(checkNumbers(LINE, { ...EMPTY, dates: [{ source: "f-000000000001", y: 2026, m: 8, d: 18 }], labels: monthKey }).passed, true);
    const without = checkNumbers(LINE, { ...EMPTY, labels: monthKey });
    assert.deepEqual(without, checkNumbers(LINE, EMPTY));
    assert.deepEqual(without.unmatched, [{ raw: "2026-08-18", reason: "this date isn't in the data" }]);
  });

  test("a label that covers the whole date is claimed as before", () => {
    // Pinned at the base (aec83c7).
    const text = "Renewal 2026-08-18 review is due";
    assert.deepEqual(brief(text), ["date:2026-08-18"]);
    for (const labels of [["2026-08-18"], ["Renewal 2026-08-18"], ["2026-08-18 review"], ["Renewal 2026-08-18 review"]]) assert.deepEqual(brief(text, labels), [], JSON.stringify(labels));
    for (const [text, labels] of [
      ["Leads created since 2026-08-18 here", ["since 2026-08-18"]],
      ["Ran at 2026-08-18T10:15:00Z here", ["2026-08-18T10:15:00Z"]],
      ["Ran at 2026-08-18T10:15+01:00 here", ["at 2026-08-18T10:15+01:00"]],
      ["Closed on 18/08/2026 here", ["18/08/2026"]],
      ["Closed on 18/08/26 here", ["on 18/08/26"]],
    ] as const) assert.deepEqual(brief(text, [...labels]), [], `${text} ${labels}`);
  });

  test("a figure beside the date is still read, and a label still claims what stands outside it", () => {
    for (const labels of [["2026-08"], ["18"]]) assert.deepEqual(brief("Leads created since 2026-08-18: 412", labels), ["date:2026-08-18", "number:412"], JSON.stringify(labels));
    assert.deepEqual(brief("2026-08-18 -18", ["2026-08"]), ["date:2026-08-18", "number:-18"]);
    assert.deepEqual(brief("2026-08-18 -18", ["18"]), ["date:2026-08-18"]);
    assert.deepEqual(brief("since 2026-08-18, 18 of 412", ["2026-08"]), ["date:2026-08-18", "number:18", "number:412"]);
    assert.equal(read("since 2026-08-18, 18 of 412", ["2026-08"])[1]!.linkedTo, 2);
    assert.deepEqual(brief("since 2026-08-18, 18 of 412", ["18"]), ["date:2026-08-18", "number:412"]);
  });

  test("a label that touches the date at either end is still claimed", () => {
    // Every covered date starts with a digit, so a label can only end where one
    // starts once a longer label has claimed the date itself.
    assert.deepEqual(brief("Wave 2:2026-08-18 review"), ["number:2", "date:2026-08-18"]);
    assert.deepEqual(brief("Wave 2:2026-08-18 review", ["2026-08-18 review", "Wave 2:"]), []);
    // A time can end on its colon, so a label can start where it ends.
    assert.deepEqual(brief("Ran at 2026-08-18T10:15:(Q3 2026)"), ["date:2026-08-18T10:15:", "date:Q3 2026"]);
    assert.deepEqual(brief("Ran at 2026-08-18T10:15:(Q3 2026)", ["(Q3 2026)"]), ["date:2026-08-18T10:15:"]);
    assert.deepEqual(brief("Pricebook 2026 2026-08-18", ["Pricebook 2026"]), ["date:2026-08-18"]);
    assert.deepEqual(brief("Leads created since 2026-08-18", ["since"]), ["date:2026-08-18"]);
  });

  test("each covered form, with each label that overlaps part of it, reads as with no label", () => {
    const cases: Array<[string, string[]]> = [
      ["Leads created since 2026-08-18 here", ["2026", "18", "08", "2026-08"]],
      ["Ran at 2026-08-18T10:15:00Z here", ["2026", "08", "2026-08", "15", "00Z"]],
      ["Ran at 2026-08-18T10:15+01:00 here", ["2026", "08", "2026-08", "01"]],
      ["Closed on 18/08/2026 here", ["18", "2026", "08", "18/08"]],
      ["Closed on 18/08/26 here", ["18", "26", "08", "18/08"]],
    ];
    for (const [text, labels] of cases) {
      assert.equal(read(text).length, 1, text);
      for (const label of labels) assert.deepEqual(read(text, [label]), read(text), `${text} ${label}`);
    }
  });

  test("a label beside a letter in a date with a time isn't claimed, as before", () => {
    for (const [text, label] of [["Ran at 2026-08-18T10:15:00Z here", "10"], ["Ran at 2026-08-18T10:15:00Z here", "2026-08-18"], ["Ran at 2026-08-18T10:15+01:00 here", "2026-08-18"]] as const) {
      assert.deepEqual(read(text, [label]), read(text), `${text} ${label}`);
    }
  });

  test("dates with a month, a quarter or a year in words read as at the base", () => {
    // Pinned at the base (aec83c7). Three leave a figure: decision 553 admits them, and a change to them should be seen.
    const pinned: Array<[string, Array<[string, string[]]>]> = [
      ["Closed on 18 August 2026 here", [["18", ["date:August 2026"]], ["2026", ["date:18 August"]], ["18 August", ["number:2026"]], ["18 August 2026", []], ["on 18 August 2026", []]]],
      ["Closed on August 18, 2026 here", [["18", ["number:2026"]], ["2026", ["number:18"]], ["August 18", ["number:2026"]], ["August 18, 2026", []], ["on August 18, 2026", []]]],
      ["Closed on 18 August here", [["18", []], ["18 August", []], ["on 18 August", []]]],
      ["Closed on August 18th here", [["18", ["date:August 18th"]], ["18th", []], ["August 18th", []], ["on August 18th", []]]],
      ["Closed in August 2026 here", [["2026", []], ["August 2026", []], ["in August 2026", []]]],
      ["Closed in Q3 2026 here", [["Q3", ["date:2026"]], ["3", ["date:Q3 2026"]], ["2026", ["date:Q3"]], ["Q3 2026", []], ["in Q3 2026", []]]],
      ["Closed in Q3 FY26 here", [["Q3", ["date:FY26"]], ["3", ["date:Q3 FY26"]], ["26", ["date:Q3 FY26"]], ["FY26", ["date:Q3"]], ["Q3 FY", ["date:Q3 FY26"]], ["Q3 FY26", []], ["in Q3 FY26", []]]],
      ["Closed in FY2026 here", [["2026", ["date:FY2026"]], ["FY2026", []], ["in FY2026", []]]],
    ];
    for (const [text, labels] of pinned) for (const [label, expected] of labels) assert.deepEqual(brief(text, [label]), expected, `${text} ${label}`);
    for (const labels of [["FY 2026"], ["Q3 2026"], ["Pricebook 2026"]]) assert.deepEqual(read(LINE, labels), read(LINE), JSON.stringify(labels));
  });

  test("a date the reader doesn't accept reads with a label as it does with none", () => {
    // The ISO rule checks no range, so this is a date to it.
    assert.deepEqual(brief("Since 2026-13-45 here"), ["date:2026-13-45"]);
    assert.deepEqual(brief("On 31/02/2026 here"), ["date:31/02/2026"]);
    // No UK reading: blocked whole, with a label too.
    assert.deepEqual(brief("On 12/31/2025 here"), ["malformed:12/31/2025"]);
    for (const [text, labels] of [["Since 2026-13-45 here", ["2026-13", "2026", "13", "45"]], ["On 31/02/2026 here", ["31", "02", "2026", "31/02"]], ["On 12/31/2025 here", ["12", "31", "2025", "12/31"]]] as const) {
      for (const label of labels) assert.deepEqual(read(text, [label]), read(text), `${text} ${label}`);
    }
  });

  test("a label inside an identifier or a token reads as at the base", () => {
    assert.deepEqual(brief("Owner [[USER_0001]] since 2026-08-18", ["0001"]), ["token:[[USER_0001]]", "date:2026-08-18"]);
    assert.deepEqual(brief("Record 001000000000000001AAA and 0010000000000001AA", ["0001"]), ["identifier:001000000000000001AAA", "identifier:0010000000000001AA"]);
  });
});
