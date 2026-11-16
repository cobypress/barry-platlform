import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../../domains/index.ts";
import { wordCount } from "../../narrative/draft.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Score } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { carryOver } from "../carry-over.ts";
import { EXECUTIVE_MAX_PAGES, executiveHtml } from "../executive.ts";
import { deckRangeFor, fullReportHtml, fullReportHtmlAt, slideCounts } from "../full-report.ts";
import { document as htmlDocument } from "../html.ts";
import { checkLayout } from "../layout.ts";
import { launchBrowser } from "../pdf.ts";

// On the first Developer Edition run, Claude's wording ran ten slides of the
// full report and a page of the executive summary past their footers. The
// samples use the engine's shorter text, so this test makes every section
// Claude may word several times longer, as a real run can.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const issue = output!.findings.find((f) => f.status === "issue")!;
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast can't be trusted.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [issue.id], assessment: "The data supports this." }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, intake };

const FILLER = [
  "Where we could not read a part of the org with the access we were given, we say so and explain why, rather than estimate a figure that would only look precise.",
  "Most of what we could not measure comes down to how little data the org holds, so we cannot yet tell a well-kept org from an unused one, and the same checks are worth running again once the team is using it day to day.",
  "Some areas were outside what we could see: Salesforce only shows when the person carrying out the review last opened each dashboard, which tells us nothing about how the team uses them.",
  "Other checks need information from you. We do not know which field records why a deal was lost, so we cannot tell a blank reason from a missing field, and we have not agreed with you what a healthy pipeline looks like.",
].join(" ");

/** Every section with a brief, as a verbose draft: its own text, then four long paragraphs. */
function verbose(report: BuiltReport): BuiltReport {
  for (const key of ["executive_summary", "full_health_check"] as ReportOutputKey[]) {
    const built = report.outputs[key];
    for (const section of built.sections) {
      if (!report.briefs.has(section.id)) continue;
      const version = built.versions.find((v) => v.id === section.currentVersionId)!;
      version.body = [version.body, FILLER, FILLER, FILLER, FILLER].join("\n\n");
    }
  }
  return report;
}

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

async function open(html: string): Promise<Page> {
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: "load" });
  await page.evaluate(() => document.fonts.ready.then(() => undefined));
  await page.emulateMedia({ media: "print" });
  return page;
}

const overflow = (problems: Array<{ page: number; check: string }>): number[] => [...new Set(problems.filter((p) => p.check === "overflow").map((p) => p.page))];

describe("carry-over: nothing runs past a footer", () => {
  for (const [name, make] of [
    ["the full report", (r: BuiltReport) => fullReportHtml(r, pseudonyms)],
    ["the executive summary", (r: BuiltReport) => executiveHtml(r, pseudonyms)],
  ] as const) {
    test(`${name}, with every worded section several times longer`, async () => {
      const html = make(verbose(buildReport(input)));
      const page = await open(html);
      try {
        const before = await checkLayout(page, { minPages: 1, maxPages: 999 });
        assert.ok(overflow(before.problems).length > 0, "the long text overflows without carry-over");
        const added = await carryOver(page);
        const afterwards = await checkLayout(page, { minPages: 1, maxPages: 999 });
        assert.deepEqual(overflow(afterwards.problems), [], JSON.stringify(afterwards.problems.filter((p) => p.check === "overflow").slice(0, 5)));
        assert.equal(afterwards.pages, before.pages + added);
        // Every footer numbered in order, and every continued page says so.
        const facts = await page.evaluate(() => ({
          numbers: [...document.querySelectorAll(".page")].map((p) => p.querySelector(":scope > [data-chrome] > span:last-child")?.textContent ?? null),
          continued: [...document.querySelectorAll(".page")].filter((p) => /, continued$/.test(p.querySelector("h1, h2")?.textContent ?? "")).length,
        }));
        facts.numbers.forEach((n, i) => assert.ok(n === null || n === String(i + 1), `page ${i + 1} is numbered ${n}`));
        assert.ok(facts.continued >= added, `${facts.continued} continued titles for ${added} added pages`);
        // Not a word lost: the text that printed is the text that was built.
        const words = (s: string) => s.replace(/, continued/g, "").replace(/\s+/g, " ").trim();
        const shown = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.childNodes].filter((n) => !(n instanceof HTMLElement && n.hasAttribute("data-chrome"))).map((n) => n.textContent).join(" ")).join(" "));
        for (const sentence of FILLER.split(/(?<=\.) /)) assert.ok(words(shown).includes(sentence), sentence);
      } finally {
        await page.close();
      }
    });
  }

  test("the executive summary, several times longer: a part's heading goes over with the table it heads (DECISIONS.md 508 and 510)", async () => {
    // Before the labels (fe5b440), "Potential revenue in open deals" sat alone at the foot of the
    // pipeline's first continued page and its table opened the next. It is a part's own heading (an
    // H3), so 508 takes it over with its table, and 510 leaves that as it is.
    const page = await open(executiveHtml(verbose(buildReport(input)), pseudonyms));
    try {
      await carryOver(page);
      const heading = await page.evaluate(() => {
        const pages = [...document.querySelectorAll(".page")];
        const h = [...document.querySelectorAll("h3, h4, h5")].find((x) => x.textContent === "Potential revenue in open deals")!;
        const at = pages.indexOf(h.closest(".page")!);
        const before = [...pages[at - 1]!.querySelectorAll("h2, h3, figure, table, p")].at(-1);
        return { tag: h.tagName, next: h.nextElementSibling?.tagName, page: at + 1, title: pages[at]!.querySelector("h2")?.textContent, endsBefore: before?.tagName };
      });
      assert.deepEqual(heading, { tag: "H3", next: "TABLE", page: 9, title: "Your pipeline today, continued", endsBefore: "FIGURE" });
    } finally {
      await page.close();
    }
  });

  test("the contents page points at where each part starts after pages are added", async () => {
    const page = await open(fullReportHtml(verbose(buildReport(input)), pseudonyms));
    try {
      // Each entry's page, by its eyebrow and title, before and after.
      const targets = () =>
        page.evaluate(() => {
          const pages = [...document.querySelectorAll<HTMLElement>(".page")];
          return [...document.querySelectorAll<HTMLElement>("[data-goto]")].map((pg) => {
            const target = pages[Number(pg.textContent) - 1];
            return `${target?.querySelector(".eyebrow")?.textContent ?? ""} | ${target?.querySelector("h1, h2")?.textContent ?? ""}`;
          });
        });
      const before = await targets();
      assert.ok(before.length > 10);
      assert.ok((await carryOver(page)) > 0);
      const afterwards = await targets();
      assert.deepEqual(afterwards, before);
    } finally {
      await page.close();
    }
  });

  test("a sample that fits is left exactly as it was", async () => {
    const html = fullReportHtml(buildReport(input), pseudonyms);
    const page = await open(html);
    try {
      const before = await page.evaluate(() => document.body.innerHTML);
      assert.equal(await carryOver(page), 0);
      assert.equal(await page.evaluate(() => document.body.innerHTML), before);
    } finally {
      await page.close();
    }
  });
});

// A list split across pages keeps its label (DECISIONS.md 507 and 508): the part
// that goes on starts with the nearest heading that stayed above the split, marked
// as continued. Hand-built pages with fixed heights put each split where it's wanted:
// the content runs from 40px to 540px, a heading is 30px and a list item 40px.

const SPLIT_CSS = `
.page { width: 600px; height: 600px; padding: 40px 40px 60px; font-size: 14px; }
.footer { bottom: 20px; left: 40px; right: 40px; }
h2, h3 { height: 30px; line-height: 30px; font-size: 16px; overflow: hidden; white-space: nowrap; }
ul { margin: 0; padding: 0 0 0 18px; }
li { height: 40px; }
.lists { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
p { margin: 0; line-height: 20px; white-space: pre-line; }
table { border-collapse: collapse; }
td { height: 40px; padding: 0; }
`;
const gap = (px: number): string => `<div style="height:${px}px"></div>`;
const lis = (name: string, n: number): string => Array.from({ length: n }, (_, i) => `<li>${name}${i + 1}</li>`).join("");
const items = (name: string, n: number): string => `<ul>${lis(name, n)}</ul>`;
const part = (label: string, name: string, n: number): string => `<h3>${label}</h3>${items(name, n)}`;
const sheet = (body: string): string => htmlDocument("Test", "a4", SPLIT_CSS, [`<section class="page" data-page="1"><h2>Title</h2>${body}<div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);

/** Each page's headings and list items in order, as "h3:Label" and "li:A1", once carried over. `shown` adds paragraphs or cells. */
async function split(body: string, shown = "h2, h3, li"): Promise<{ added: number; pages: string[][]; page: Page }> {
  const page = await open(sheet(body));
  const added = await carryOver(page);
  const pages = await page.evaluate((selector) => [...document.querySelectorAll(".page")].map((p) => [...p.querySelectorAll(selector)].map((e) => `${e.tagName.toLowerCase()}:${e.textContent}`)), shown);
  return { added, pages, page };
}
const li = (name: string, from: number, to: number): string[] => Array.from({ length: to - from + 1 }, (_, i) => `li:${name}${from + i}`);

describe("carry-over: a list split across pages keeps its label (DECISIONS.md 507 and 508)", () => {
  test("a headed block split inside: the label once at the start of the carried block, before its list", async () => {
    const { added, pages, page } = await split(`${gap(100)}<div>${part("Label", "A", 10)}</div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", ...li("A", 1, 8)],
        ["h2:Title, continued", "h3:Label, continued", ...li("A", 9, 10)],
      ]);
      // Inside the carried block, the copy comes first and only once.
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")[1]!.querySelector(":scope > div")!.children].map((c) => c.tagName)), ["H3", "UL"]);
    } finally {
      await page.close();
    }
  });

  test("a block moved whole keeps its own heading as it was, with nothing copied over it", async () => {
    // It starts past the footer, below a heading of the page's own that stays.
    const { pages, page } = await split(`<h3>Section</h3>${gap(430)}<div style="margin-top:20px">${part("Label", "A", 3)}</div>`);
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Section"],
        ["h2:Title, continued", "h3:Label", ...li("A", 1, 3)],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a block with no heading of its own gains none", async () => {
    const { pages, page } = await split(`${gap(100)}<div>${items("A", 10)}</div>`);
    try {
      assert.deepEqual(pages, [
        ["h2:Title", ...li("A", 1, 9)],
        ["h2:Title, continued", "li:A10"],
      ]);
    } finally {
      await page.close();
    }
  });

  test("split across three pages, the label is marked continued once on the second and on the third", async () => {
    const { added, pages, page } = await split(`<div>${part("Label", "A", 30)}</div>`);
    try {
      assert.equal(added, 2);
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", ...li("A", 1, 11)],
        ["h2:Title, continued", "h3:Label, continued", ...li("A", 12, 22)],
        ["h2:Title, continued", "h3:Label, continued", ...li("A", 23, 30)],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a heading that ends in a reference tag is marked before the tag, once, over three pages", async () => {
    const { pages, page } = await split(`<div><h3>Label<span class="ref" data-ref>R4.1</span></h3>${items("A", 30)}</div>`);
    try {
      assert.equal(pages.length, 3);
      const labels = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelector("h3")!.innerHTML));
      assert.deepEqual(labels, ['Label<span class="ref" data-ref="">R4.1</span>', 'Label, continued<span class="ref" data-ref="">R4.1</span>', 'Label, continued<span class="ref" data-ref="">R4.1</span>']);
    } finally {
      await page.close();
    }
  });

  test("three columns side by side, one split: only that one is labelled, and the empty shell of another gains nothing", async () => {
    const { pages, page } = await split(`${gap(100)}<div class="lists"><div>${part("Left (3)", "L", 3)}</div><div>${part("Middle (12)", "M", 12)}</div><div>${part("Right (3)", "R", 3)}</div></div>`);
    try {
      assert.deepEqual(pages[1], ["h2:Title, continued", "h3:Middle (12), continued", ...li("M", 9, 12)]);
      // The left column still crossed (the row was as tall as the middle one) when its turn came, so its shell went over empty.
      assert.match(await page.evaluate(() => document.querySelectorAll(".page")[1]!.querySelector(".lists")!.innerHTML), /^<div><\/div><div><h3 data-carried-label="">Middle \(12\), continued<\/h3><ul>/);
    } finally {
      await page.close();
    }
  });

  test("three columns side by side, all split: each carries its own label", async () => {
    const { pages, page } = await split(`${gap(100)}<div class="lists"><div>${part("Left (12)", "L", 12)}</div><div>${part("Middle (12)", "M", 12)}</div><div>${part("Right (12)", "R", 12)}</div></div>`);
    try {
      assert.deepEqual(pages[1], ["h2:Title, continued", "h3:Left (12), continued", ...li("L", 9, 12), "h3:Middle (12), continued", ...li("M", 9, 12), "h3:Right (12), continued", ...li("R", 9, 12)]);
    } finally {
      await page.close();
    }
  });

  test("a column of three headed parts: the label is the nearest heading that stays, and none where the split falls between parts", async () => {
    // Inside the third part.
    let { pages, page } = await split(`<div>${part("A", "A", 3)}${part("B", "B", 3)}${part("C", "C", 6)}</div>`);
    await page.close();
    assert.deepEqual(pages[1], ["h2:Title, continued", "h3:C, continued", ...li("C", 4, 6)]);
    // Inside the second: the third part goes over after it with its own heading.
    ({ pages, page } = await split(`<div>${part("A", "A", 3)}${part("B", "B", 10)}${part("C", "C", 2)}</div>`));
    await page.close();
    assert.deepEqual(pages[1], ["h2:Title, continued", "h3:B, continued", ...li("B", 8, 10), "h3:C", ...li("C", 1, 2)]);
    // Between the second and the third: the third's heading fits but its first item doesn't, so the heading goes with its list.
    ({ pages, page } = await split(`<div>${part("A", "A", 6)}${part("B", "B", 3)}${part("C", "C", 3)}</div>`));
    await page.close();
    assert.deepEqual(pages, [
      ["h2:Title", "h3:A", ...li("A", 1, 6), "h3:B", ...li("B", 1, 3)],
      ["h2:Title, continued", "h3:C", ...li("C", 1, 3)],
    ]);
  });

  test("a heading with a sub-heading: the sub-heading's label", async () => {
    const { pages, page } = await split(`<div><h3>Main</h3><h3 class="cap">Sub</h3>${items("S", 12)}</div>`);
    try {
      assert.deepEqual(pages[1], ["h2:Title, continued", "h3:Sub, continued", ...li("S", 11, 12)]);
      assert.equal(await page.evaluate(() => document.querySelectorAll(".page")[1]!.querySelector("h3")!.className), "cap");
    } finally {
      await page.close();
    }
  });

  test("a heading whose list starts past the footer goes over with it, whole and unmarked, when something else stays", async () => {
    const { pages, page } = await split(`${gap(440)}<div><h3>Label</h3><ul style="margin-top:20px">${lis("A", 3)}</ul></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title"], ["h2:Title, continued", "h3:Label", ...li("A", 1, 3)]]);
    } finally {
      await page.close();
    }
  });

  test("the same block alone on its page: nothing moves, so the page keeps it and the run ends", async () => {
    // Only headings would stay, so the page is left as it was, past its footer, and the layout check reports it.
    const { added, pages, page } = await split(`<div><h3 style="height:470px">Label</h3><ul style="margin-top:20px">${lis("A", 3)}</ul></div>`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", "h3:Label", ...li("A", 1, 3)]]);
      const layout = await checkLayout(page, { minPages: 1, maxPages: 1 });
      assert.ok(layout.problems.some((p) => p.page === 1 && p.check === "overflow"), JSON.stringify(layout.problems));
    } finally {
      await page.close();
    }
  });
});

// A label is copied only over the rest of the thing it heads (DECISIONS.md 510): a
// list, a table or a paragraph split in two that is the first thing under its heading.
// Whatever else goes over (a note after the list, a paragraph after another block)
// goes unlabelled. A paragraph's line is 20px, one sentence a line.

const sentences = (...words: string[]): string => words.map((w) => `${w}.`).join("\n");
const note = "<p>Note.</p>";

/**
 * The labels the carry-over copied on each page, by their text, and whether any original heading was marked.
 * A part's labels only (H3 to H5): the page's own eyebrow and title are marked as copies too (DECISIONS.md 570), and have tests of their own.
 */
const PART_COPIES = "h3[data-carried-label], h4[data-carried-label], h5[data-carried-label]";
const marks = (page: Page): Promise<{ copies: string[][]; originals: number }> =>
  page.evaluate((selector) => ({
    copies: [...document.querySelectorAll(".page")].map((p) => [...p.querySelectorAll(selector)].map((h) => h.textContent ?? "")),
    originals: [...document.querySelectorAll(selector)].filter((h) => !/, continued$/.test(h.textContent ?? "")).length,
  }), PART_COPIES);

describe("carry-over: a label only over what it heads (DECISIONS.md 510)", () => {
  test("a note after a list, carried alone, goes without a label", async () => {
    // The list fits; its note is the first thing past the footer.
    const { pages, page } = await split(`${gap(110)}<div>${part("Label", "A", 8)}${note}</div>`, "h2, h3, li, p");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", ...li("A", 1, 8)],
        ["h2:Title, continued", "p:Note."],
      ]);
      assert.deepEqual((await marks(page)).copies, [[], []]);
    } finally {
      await page.close();
    }
  });

  test("the list split and its note carried after it: one label, over the list, marked as a copy", async () => {
    const { pages, page } = await split(`${gap(100)}<div>${part("Label", "A", 10)}${note}</div>`, "h2, h3, li, p");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", ...li("A", 1, 8)],
        ["h2:Title, continued", "h3:Label, continued", ...li("A", 9, 10), "p:Note."],
      ]);
      // The copy is marked, so the layout check can leave it out; the heading it copies isn't.
      assert.deepEqual(await marks(page), { copies: [[], ["Label, continued"]], originals: 0 });
    } finally {
      await page.close();
    }
  });

  test("a paragraph after another block, cut by sentence, goes without a label (the verdict page's shape)", async () => {
    // The heading heads the block; the closing paragraph after it belongs to no part.
    const { pages, page } = await split(`<h3>Label</h3>${gap(380)}<p>${sentences("One", "Two", "Three", "Four")}</p>`, "h2, h3, li, p");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", "p:One.\nTwo.\nThree."],
        ["h2:Title, continued", "p:Four."],
      ]);
      assert.deepEqual((await marks(page)).copies, [[], []]);
    } finally {
      await page.close();
    }
  });

  test("a paragraph that is the first thing under its heading, cut by sentence, is labelled", async () => {
    const { pages, page } = await split(`${gap(330)}<div><h3>Label</h3><p>${sentences("One", "Two", "Three", "Four", "Five", "Six")}</p></div>`, "h2, h3, li, p");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", "p:One.\nTwo.\nThree.\nFour.\nFive."],
        ["h2:Title, continued", "h3:Label, continued", "p:Six."],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a table split by its rows is labelled", async () => {
    const rows = Array.from({ length: 10 }, (_, i) => `<tr><td>R${i + 1}</td></tr>`).join("");
    const { pages, page } = await split(`${gap(100)}<div><h3>Label</h3><table><tbody>${rows}</tbody></table></div>`, "h2, h3, td");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", ...Array.from({ length: 8 }, (_, i) => `td:R${i + 1}`)],
        ["h2:Title, continued", "h3:Label, continued", "td:R9", "td:R10"],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a wrapper split between its parts goes without the label above it: the next part starts with its own heading", async () => {
    const { pages, page } = await split(`<div><h3>Outer</h3><div>${part("A", "A", 6)}${part("B", "B", 3)}${part("C", "C", 3)}</div></div>`);
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Outer", "h3:A", ...li("A", 1, 6), "h3:B", ...li("B", 1, 3)],
        ["h2:Title, continued", "h3:C", ...li("C", 1, 3)],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a page's own title over a block moved whole keeps the rule it had before 507: it goes with the block when content stays above it", async () => {
    const { pages, page } = await split(`${gap(300)}<h2>Second</h2><figure style="margin:20px 0 0;height:200px"></figure>`, "h2, h3, figure");
    try {
      assert.deepEqual(pages, [["h2:Title"], ["h2:Title, continued", "h2:Second", "figure:"]]);
    } finally {
      await page.close();
    }
  });

  test("a list split after a note under the same heading goes without a label: the list isn't the first thing it heads", async () => {
    const { pages, page } = await split(`${gap(100)}<div><h3>Label</h3>${note}${items("A", 10)}</div>`, "h2, h3, li, p");
    try {
      assert.deepEqual(pages, [
        ["h2:Title", "h3:Label", "p:Note.", ...li("A", 1, 8)],
        ["h2:Title, continued", "li:A9", "li:A10"],
      ]);
    } finally {
      await page.close();
    }
  });
});

// A continued page's eyebrow and title are copies, marked as a copied label is, so
// they never count as its content (DECISIONS.md 570). The rest of a cut list item
// is the item going on: no bullet, and in a numbered list no number of its own
// (571 and 572). A long item's line is 20px, one sentence a line.

const ITEM_CSS = `
.eyebrow { height: 20px; line-height: 20px; }
ol { margin: 0; padding: 0 0 0 24px; }
li.long { height: auto; line-height: 20px; white-space: pre-line; }
`;
/** A page with an eyebrow over its title, as every slide has. */
const headed = (body: string): string => htmlDocument("Test", "a4", `${SPLIT_CSS}${ITEM_CSS}`, [`<section class="page" data-page="1"><div class="eyebrow">Area</div><h2>Title</h2>${body}<div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);
/** As `sheet`, with long items' styles. */
const listSheet = (body: string): string => htmlDocument("Test", "a4", `${SPLIT_CSS}${ITEM_CSS}`, [`<section class="page" data-page="1"><h2>Title</h2>${body}<div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);
const longItem = (name: string, n: number): string => `<li class="long">${sentences(...Array.from({ length: n }, (_, i) => `${name}s${i + 1}`))}</li>`;
const listed = (names: string[]): string => names.map((n) => `<li>${n}</li>`).join("");

/**
 * The marker the browser prints for each list item the selector finds, by the item's own
 * words: "4." in a numbered list, "•" in a bulleted one, null for none. Read from
 * Chromium's accessibility tree, where the marker is a node of its own.
 */
async function markers(page: Page, selector: string): Promise<Array<{ text: string; marker: string | null }>> {
  const cdp = await page.context().newCDPSession(page);
  try {
    const texts = await page.evaluate((s) => {
      const found = [...document.querySelectorAll(s)];
      (window as unknown as { found: Element[] }).found = found;
      return found.map((li) => (li.textContent ?? "").replace(/\s+/g, " ").trim());
    }, selector);
    const out = [];
    for (const [i, text] of texts.entries()) {
      const { result } = await cdp.send("Runtime.evaluate", { expression: `window.found[${i}]` });
      const { nodes } = await cdp.send("Accessibility.queryAXTree", { objectId: result.objectId!, role: "ListMarker" });
      const name = nodes[0]?.name?.value;
      out.push({ text, marker: typeof name === "string" ? name.trim() : null });
    }
    return out;
  } finally {
    await cdp.detach();
  }
}

/** Each page's list items: words, whether marked as a rest, and the marker the browser draws. */
const itemsOn = (page: Page): Promise<Array<Array<{ text: string; rest: boolean; style: string }>>> =>
  page.evaluate(() =>
    [...document.querySelectorAll(".page")].map((p) =>
      [...p.querySelectorAll("li")].map((li) => ({ text: (li.textContent ?? "").replace(/\s+/g, " ").trim(), rest: li.hasAttribute("data-carried-rest"), style: getComputedStyle(li).listStyleType })),
    ),
  );

describe("carry-over: a continued page's heading is a copy, and a cut item goes on without a marker (DECISIONS.md 570, 571 and 572)", () => {
  test("the continued page's eyebrow and title are marked as copies; the page's own are not", async () => {
    const page = await open(headed(`<div class="chapter">${items("A", 14)}</div>`));
    try {
      assert.equal(await carryOver(page), 1);
      const heads = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.children].slice(0, 2).map((c) => `${c.tagName}${c.hasAttribute("data-carried-label") ? "[marked]" : ""}:${c.textContent}`)));
      assert.deepEqual(heads, [
        ["DIV:Area", "H2:Title"],
        ["DIV[marked]:Area", "H2[marked]:Title, continued"],
      ]);
    } finally {
      await page.close();
    }
  });

  test("a continued slide holding only 'And 5 more' under the copied heading is an empty section; one holding a list is not", async () => {
    const emptyOn = async (body: string): Promise<number[]> => {
      const page = await open(headed(body));
      try {
        assert.equal(await carryOver(page), 1);
        return (await checkLayout(page, { minPages: 1, maxPages: 9 })).problems.filter((p) => p.check === "empty section").map((p) => p.page);
      } finally {
        await page.close();
      }
    };
    // The list starts past the footer, below content that stays: it goes over whole.
    const stays = `<p style="height:420px">Words of the slide's own that stay on the first page.</p>`;
    assert.deepEqual(await emptyOn(`<div class="chapter">${stays}<ul><li>And 5 more on the pages that follow.</li></ul></div>`), [2]);
    assert.deepEqual(await emptyOn(`<div class="chapter">${stays.replace("420", "300")}<ul>${listed(Array.from({ length: 8 }, (_, i) => `An issue that is on the list, number ${i + 1}`))}</ul></div>`), []);
  });

  test("a bulleted item cut at a sentence end: the rest is marked, has no bullet and starts at the item's own text edge", async () => {
    const page = await open(listSheet(`${gap(370)}<ul><li>A1</li>${longItem("B", 4)}<li>A3</li></ul>`));
    try {
      assert.equal(await carryOver(page), 1);
      assert.deepEqual(await itemsOn(page), [
        [
          { text: "A1", rest: false, style: "disc" },
          { text: "Bs1. Bs2. Bs3.", rest: false, style: "disc" },
        ],
        [
          { text: "Bs4.", rest: true, style: "none" },
          { text: "A3", rest: false, style: "disc" },
        ],
      ]);
      assert.deepEqual(await markers(page, "li"), [
        { text: "A1", marker: "•" },
        { text: "Bs1. Bs2. Bs3.", marker: "•" },
        { text: "Bs4.", marker: null },
        { text: "A3", marker: "•" },
      ]);
      // Where each half's words begin on its page.
      const edges = await page.evaluate(() =>
        [...document.querySelectorAll("li.long")].map((li) => {
          const range = document.createRange();
          range.selectNodeContents(li);
          return range.getClientRects()[0]!.left - li.closest(".page")!.getBoundingClientRect().left;
        }),
      );
      assert.equal(edges.length, 2);
      assert.ok(Math.abs(edges[0]! - edges[1]!) <= 1, JSON.stringify(edges));
    } finally {
      await page.close();
    }
  });

  test("a paragraph cut at a sentence end gains nothing", async () => {
    const page = await open(sheet(`${gap(330)}<div><h3>Label</h3><p>${sentences("One", "Two", "Three", "Four", "Five", "Six")}</p></div>`));
    try {
      assert.equal(await carryOver(page), 1);
      const rest = await page.evaluate(() => ({ marked: document.querySelectorAll("[data-carried-rest]").length, attributes: [...document.querySelectorAll(".page")[1]!.querySelector("p")!.attributes].map((a) => a.name) }));
      assert.deepEqual(rest, { marked: 0, attributes: [] });
    } finally {
      await page.close();
    }
  });

  test("a numbered list of five, its third item cut: the rest holds the third's number unseen, and the fourth still prints 4", async () => {
    const page = await open(listSheet(`${gap(330)}<ol>${listed(["N1", "N2"])}${longItem("N3", 4)}${listed(["N4", "N5"])}</ol>`));
    try {
      assert.equal(await carryOver(page), 1);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll("ol")].map((ol) => ol.start)), [1, 3]);
      assert.deepEqual(await markers(page, "li"), [
        { text: "N1", marker: "1." },
        { text: "N2", marker: "2." },
        { text: "N3s1. N3s2. N3s3.", marker: "3." },
        { text: "N3s4.", marker: null },
        { text: "N4", marker: "4." },
        { text: "N5", marker: "5." },
      ]);
      assert.deepEqual((await itemsOn(page))[1]![0], { text: "N3s4.", rest: true, style: "none" });
    } finally {
      await page.close();
    }
  });

  test("a numbered list split between whole items keeps counting as before", async () => {
    const page = await open(listSheet(`${gap(330)}<ol>${listed(["N1", "N2", "N3", "N4", "N5"])}</ol>`));
    try {
      assert.equal(await carryOver(page), 1);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll("ol")].map((ol) => ol.start)), [1, 4]);
      assert.deepEqual((await markers(page, "li")).map((m) => m.marker), ["1.", "2.", "3.", "4.", "5."]);
      assert.equal(await page.evaluate(() => document.querySelectorAll("[data-carried-rest]").length), 0);
    } finally {
      await page.close();
    }
  });

  test("an item taller than a page, cut twice: the third page's list starts at the same number, and the next item is one more", async () => {
    const page = await open(listSheet(`<ol>${listed(["N1", "N2"])}${longItem("N3", 50)}${listed(["N4"])}</ol>`));
    try {
      assert.equal(await carryOver(page), 2);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll("ol")].map((ol) => ol.start)), [1, 3, 3]);
      const shown = await markers(page, "li");
      assert.deepEqual(
        shown.map((m) => m.marker),
        ["1.", "2.", "3.", null, null, "4."],
      );
      assert.deepEqual((await itemsOn(page)).map((p) => p.map((i) => i.rest)), [[false, false, false], [true], [true, false]]);
    } finally {
      await page.close();
    }
  });

  test("the executive summary's value and investment pages, made to split, carry their eyebrow and title onto the continued page", async () => {
    // Neither splits on anything a run holds: the value page names as many lines as the
    // room it measures, and the investment page holds our own sentences and figures (issue
    // titles 1,200 characters longer, with and without the customer's spend, split neither).
    // So each page as built is pushed down by a block that stays, and what's below it goes on.
    const report = buildReport({ ...input, intake: { ...input.intake!, salesforceSpendAnnual: 250_000 } });
    const html = executiveHtml(report, pseudonyms);
    assert.equal(html.split('<div class="money">').length - 1, 2, "both pages");
    const page = await open(html.replaceAll('<div class="money">', '<div class="money"><div style="height:640px"></div>'));
    try {
      await carryOver(page);
      const continued = await page.evaluate(() =>
        [...document.querySelectorAll(".page")].filter((p) => p.querySelector(".money") && p.querySelector(":scope > h2[data-carried-label]")).map((p) => [...p.children].slice(0, 2).map((c) => `${c.hasAttribute("data-carried-label") ? "[marked]" : ""}${c.textContent}`)),
      );
      assert.deepEqual(continued, [
        ["[marked]What it's worth", "[marked]What fixing this is worth, continued"],
        ["[marked]Your investment", "[marked]What you pay, and what you use, continued"],
      ]);
    } finally {
      await page.close();
    }
  });

  test("the investment page with no figure for the bill: its line sits the title's own margin below it, as before the title left the block (DECISIONS.md 572)", async () => {
    // No money figure and no spend: the page opens with the line saying there's no figure.
    const findings = input.findings.map((f) => (f.status === "issue" ? { ...f, value: { kind: "not_sized" as const, reason: "Synthetic: no figure." } } : f));
    const html = executiveHtml(buildReport({ ...input, findings }), pseudonyms);
    assert.match(html, /<h2>What you pay, and what you use<\/h2><div class="money"><p class="range">/);
    const page = await open(html);
    try {
      const gap = await page.evaluate(() => {
        const h2 = [...document.querySelectorAll("h2")].find((h) => h.textContent === "What you pay, and what you use")!;
        const line = h2.nextElementSibling!.firstElementChild!;
        return { gap: line.getBoundingClientRect().top - h2.getBoundingClientRect().bottom, margin: parseFloat(getComputedStyle(h2).marginBottom) };
      });
      assert.ok(Math.abs(gap.gap - gap.margin) < 0.01, JSON.stringify(gap));
    } finally {
      await page.close();
    }
  });

  test("the full report's agent-readiness fixes, one cut across two slides: the numbers printed run 1 to N, none missing and none twice", async () => {
    // Two or more fixes sit side by side in two columns, where the carry-over moves an item
    // whole; a single fix is one column, and a title with sentences in it is cut there.
    // The title's sentences are a team's words, as "Licences you can release: dormant users" has a part of its own.
    const first = buildReport(input).readiness;
    const linked = new Set(first.pillars.flatMap((p) => p.findingIds));
    const kept = first.fixes[0]!.id;
    const MORE = [" It holds for the Enterprise Sales North America teams.", " It holds for the Asia Pacific Partner Channel teams.", " It holds for the teams that sell through partners.", " It holds for the renewals desk as well."];
    const findings = input.findings.map((f) =>
      f.id === kept ? { ...f, title: `${f.title}.${[...MORE, ...MORE].join("")}`.replace(/\.$/, "") } : linked.has(f.id) && f.status === "issue" ? { ...f, status: "passed" as const } : f,
    );
    const report = buildReport({ ...input, findings });
    const page = await open(fullReportHtml(report, pseudonyms));
    try {
      await carryOver(page);
      const shown = await markers(page, "ol.readiness-fixes > li");
      assert.ok((await page.evaluate(() => document.querySelectorAll("ol.readiness-fixes > li[data-carried-rest]").length)) > 0, JSON.stringify(shown));
      // Across the slides it is printed on.
      const on = await page.evaluate(() => [...document.querySelectorAll("ol.readiness-fixes")].map((ol) => [...document.querySelectorAll(".page")].indexOf(ol.closest(".page")!)));
      assert.equal(new Set(on).size, 2, JSON.stringify(on));
      assert.deepEqual(
        shown.filter((m) => m.marker !== null).map((m) => m.marker),
        report.readiness.fixes.map((_, i) => `${i + 1}.`),
        JSON.stringify(shown),
      );
    } finally {
      await page.close();
    }
  });
});

// Each section Claude drafts has a word target, set by the room its page gives
// it (DECISIONS.md 440). This holds the target to the page: a draft of exactly
// that many words, in long words with no figures, fits every sample with no
// continued page, at the deck's own density and at every other.

/** Plain words averaging six letters, longer than most prose, so real text at its target takes less room. */
const VOCABULARY = "pipeline forecast report account manager customer renewal business process records review changes service context visible problem careful planning owners pressure decision priority adoption training measure quality storage options history security profile control clearly matters outcome ordinary regular team deals stage close data level often enough always your with that their more each than when what only most work time".split(" ");

/** Exactly n words, in sentences of thirteen, a paragraph for every fifty words or part of fifty, up to four. */
function draftOf(n: number): string {
  const words = Array.from({ length: n }, (_, i) => {
    const word = VOCABULARY[(i * 7) % VOCABULARY.length]!;
    const opening = i % 13 === 0 ? `${word.charAt(0).toUpperCase()}${word.slice(1)}` : word;
    return i % 13 === 12 || i === n - 1 ? `${opening}.` : opening;
  });
  const size = Math.ceil(n / Math.min(4, Math.ceil(n / 50)));
  const paragraphs: string[] = [];
  for (let i = 0; i < n; i += size) paragraphs.push(words.slice(i, i + size).join(" "));
  return paragraphs.join("\n\n");
}

/** Every section with a brief, as Claude's draft at its target. */
function atTarget(report: BuiltReport, words: (target: number) => number = (target) => target): BuiltReport {
  for (const key of ["executive_summary", "full_health_check"] as ReportOutputKey[]) {
    const built = report.outputs[key];
    for (const section of built.sections) {
      const brief = report.briefs.get(section.id);
      if (!brief) continue;
      const version = built.versions.find((v) => v.id === section.currentVersionId)!;
      version.body = draftOf(words(brief.maxWords));
      // The roadmap prints written text only, never the engine's.
      version.source = "claude_draft";
    }
  }
  return report;
}

/** The pages carry-over had to add, and the pages after it. */
async function continued(html: string): Promise<{ added: number; pages: number }> {
  const page = await open(html);
  try {
    const added = await carryOver(page);
    return { added, pages: (await checkLayout(page, { minPages: 1, maxPages: 999 })).pages };
  } finally {
    await page.close();
  }
}

/** Actions the executive plan shows, so a longer introduction can't quietly cost one. */
const planActions = (html: string): number => html.split('<section class="phase').slice(1).reduce((sum, block) => sum + (block.split("</section>")[0]!.match(/<li>/g) ?? []).length, 0);

const SAMPLES: Array<[string, NonNullable<Parameters<typeof runFromRecording>[0]>]> = [
  ["Sales Cloud", { modules: SALES_CLOUD_MODULES }],
  ["Sales and Service Cloud", { modules: [...SALES_CLOUD_MODULES, ...SERVICE_CLOUD_MODULES] }],
  ["Sales Cloud and CPQ", { modules: [...SALES_CLOUD_MODULES, ...CPQ_MODULES] }],
  ["every module", { modules: ALL_MODULES }],
  ["a set-up review", { scope: "essentials" }],
];

describe("a draft at its word target fits its page (DECISIONS.md 440)", () => {
  test("the test's drafts are the length they claim, in long words with no figure", () => {
    for (const n of [1, 20, 49, 50, 51, 120, 333]) {
      const text = draftOf(n);
      assert.equal(wordCount(text.split("\n\n")), n);
      assert.doesNotMatch(text, /\d/);
    }
    const letters = draftOf(600).replace(/[^a-z]/gi, "").length / 600;
    assert.ok(letters >= 6, `${letters} letters a word`);
  });

  for (const [name, options] of SAMPLES) {
    test(name, async () => {
      // As the sample run is: an intake with a linked problem, five interviews and the scores.
      const sample = await runFromRecording({ answers: loadAnswers(), ...options });
      const names = new Pseudonymiser(JSON.parse(readFileSync(join(sample.dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
      const scores = (JSON.parse(readFileSync(join(sample.dir, "scores.json"), "utf8")) as unknown[]).map((x) => Score.parse(x));
      const linked = sample.output!.findings.find((f) => f.status === "issue")!;
      const sampleInput: ReportInput = { run: sample.run, calibration: loadCalibration(), findings: sample.output!.findings, metrics: sample.output!.metrics, manualSteps: sample.output!.manualSteps, scores, interviews: syntheticInterviews(), intake: { ...intake, statedProblems: [{ ...intake.statedProblems[0]!, findingIds: [linked.id] }] } };
      const report = atTarget(buildReport(sampleInput));
      assert.ok([...report.briefs.keys()].some((id) => id.endsWith(":interviews")) && [...report.briefs.keys()].some((id) => id.endsWith(":stated_vs_actual")), "every kind of brief is in the sample");

      const executive = executiveHtml(report, names);
      const summary = await continued(executive);
      assert.equal(summary.added, 0, "the executive summary needed a continued page");
      assert.ok(summary.pages <= EXECUTIVE_MAX_PAGES, `${summary.pages} executive pages`);
      assert.ok(planActions(executive) >= planActions(executiveHtml(buildReport(sampleInput), names)), "the plan's introduction cost it an action");

      const range = deckRangeFor(sample.run);
      const deck = await continued(fullReportHtml(report, names));
      assert.equal(deck.added, 0, "the full report needed a continued slide");
      assert.ok(deck.pages >= range.min && deck.pages <= range.max, `${deck.pages} slides`);
      // A smaller org gets a roomier deck, with the dividers and roadmap text a dense one leaves out.
      for (let step = 0; step < slideCounts(report, names).length; step++) {
        assert.equal((await continued(fullReportHtmlAt(report, names, step))).added, 0, `the full report at density step ${step} needed a continued slide`);
      }
    });
  }
});
