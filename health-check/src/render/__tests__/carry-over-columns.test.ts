import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { carryOver } from "../carry-over.ts";
import { executiveHtml } from "../executive.ts";
import { fullReportHtml, fullReportHtmlAt } from "../full-report.ts";
import { document as htmlDocument } from "../html.ts";
import { checkLayout } from "../layout.ts";
import { launchBrowser } from "../pdf.ts";

// A list set in CSS columns is one list folded: the order built is the order read,
// down one column and then the next. When it crosses the footer, items go on from
// the end, whole and in order, so the slide holds the first k and the continued
// slide the rest, and a numbered list goes on from k + 1 (DECISIONS.md 580).
// A grid's columns are blocks side by side, and keep their own rule.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const input: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

/** Issue titles longer by n characters, as a long team name makes them. */
const TEAMS = " for the Enterprise Sales North America and Asia Pacific Partner Channel teams";
const lengthened = (n: number): ReportInput => ({ ...input, findings: input.findings.map((f) => (f.status === "issue" ? { ...f, title: f.title + TEAMS.repeat(20).slice(0, n) } : f)) });

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

/**
 * Every word a person reads, sorted, so a carry-over can be checked for dropping or
 * doubling any: the footers, the copied headings and the contents' page numbers are
 * the carry-over's own, and a table's head is repeated on purpose. A cut sentence
 * still has all its words.
 */
const words = (page: Page): Promise<string[]> =>
  page.evaluate(() => {
    const out: string[] = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      if (n.parentElement?.closest("[data-chrome], [data-carried-label], [data-goto], thead, style")) continue;
      out.push(...(n.textContent ?? "").split(/\s+/).filter(Boolean));
    }
    return out.sort();
  });

/** Words held by an element that isn't painted, each with its slide: the layout check would read them. */
const hiddenWords = (page: Page): Promise<string[]> =>
  page.evaluate(() => {
    const pages = [...document.querySelectorAll(".page")];
    const out: string[] = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) {
      const el = n.parentElement;
      if (el && (n.textContent ?? "").trim() && getComputedStyle(el).visibility !== "visible") out.push(`slide ${pages.indexOf(el.closest(".page")!) + 1}: ${n.textContent}`);
    }
    return out;
  });

/** Carries over, and checks that nothing was dropped or doubled, and that nothing hidden holds words (DECISIONS.md 579). Returns how many slides it added. */
async function carried(page: Page): Promise<number> {
  const was = await words(page);
  const added = await carryOver(page);
  assert.deepEqual(await words(page), was, "every word once, before and after the carry-over");
  assert.deepEqual(await hiddenWords(page), [], "words in what isn't printed");
  return added;
}

type Read = { slide: number; start: number | null; items: Array<{ n: number | null; text: string }> };

/**
 * Each list the selector finds, in document order: the slide it sits on, its start, and its
 * items in reading order (by left edge, then top), each with the number printed beside it
 * (the list's start plus the item's place in it; none in a bulleted list) and its words
 * without reference tags.
 */
const readLists = (page: Page, selector: string): Promise<Read[]> =>
  page.evaluate((s) => {
    const pages = [...document.querySelectorAll(".page")];
    const words = (el: Element): string => {
      const copy = el.cloneNode(true) as Element;
      for (const ref of copy.querySelectorAll("[data-ref]")) ref.remove();
      return (copy.textContent ?? "").replace(/\s+/g, " ").trim();
    };
    return [...document.querySelectorAll<HTMLElement>(s)].map((list) => {
      const kids = [...list.children];
      const at = (el: Element): DOMRect => el.getBoundingClientRect();
      const read = [...kids].sort((a, b) => Math.round(at(a).left) - Math.round(at(b).left) || at(a).top - at(b).top);
      const start = list instanceof HTMLOListElement ? list.start : null;
      return { slide: pages.indexOf(list.closest(".page")!) + 1, start, items: read.map((k) => ({ n: start === null ? null : start + kids.indexOf(k), text: words(k) })) };
    });
  }, selector);

/** The footer's top on each slide, and the lowest any item of a list reaches there: nothing may cross. */
const crossing = (page: Page, selector: string): Promise<string[]> =>
  page.evaluate((s) => {
    const pages = [...document.querySelectorAll(".page")];
    return [...document.querySelectorAll(s)].flatMap((item) => {
      const footer = item.closest(".page")!.querySelector(":scope > [data-chrome]")!;
      return item.getBoundingClientRect().bottom > footer.getBoundingClientRect().top + 1.5 ? [`slide ${pages.indexOf(item.closest(".page")!) + 1}: ${(item.textContent ?? "").slice(0, 40)}`] : [];
    });
  }, selector);

/** Marks the first block the selector finds, so it and its continued copies are read as one ("[data-target]"). */
const mark = (page: Page, selector: string): Promise<void> => page.evaluate((s) => document.querySelector<HTMLElement>(s)!.setAttribute("data-target", ""), selector);

/**
 * Pushes the marked block down its real slide with a spacer, so that only about `keep` of
 * it fits above the footer: the block keeps its own CSS and items, and the spacer stays as
 * content above it.
 */
const pushDown = (page: Page, keep = 0.55): Promise<void> =>
  page.evaluate((k) => {
    const block = document.querySelector<HTMLElement>("[data-target]")!;
    const footer = block.closest(".page")!.querySelector(":scope > [data-chrome]")!;
    const room = footer.getBoundingClientRect().top - block.getBoundingClientRect().top;
    const spacer = document.createElement("div");
    spacer.style.height = `${Math.max(1, room - block.getBoundingClientRect().height * k)}px`;
    block.before(spacer);
  }, keep);

describe("the agent-readiness fixes in two columns go on in order, under the number of their place (DECISIONS.md 580)", () => {
  // Measured on 0bb998e: at 80 characters the slide printed fixes 1, 2, 4 and 5 as 1. to 4.,
  // and the third as "5." on the next; at 120, fixes 1 and 5 as 1. and 2., the rest as 3. to 5.
  const SPLITS: Record<number, number[]> = { 0: [5], 40: [5], 80: [4, 1], 120: [2, 3], 200: [5] };
  for (const [n, split] of Object.entries(SPLITS).map(([n, s]) => [Number(n), s] as const)) {
    test(`issue titles ${n} characters longer: number n beside the n-th fix as built, on every slide`, async () => {
      const report = buildReport(lengthened(n));
      // As printed: a title's trailing space, where the added words were cut, isn't.
      const fixes = report.readiness.fixes.map((f) => f.title.trim());
      assert.equal(fixes.length, 5);
      const page = await open(fullReportHtml(report, pseudonyms));
      try {
        const built = (await readLists(page, "ol.readiness-fixes"))[0]!;
        assert.deepEqual(
          built.items.map((i) => i.text),
          fixes,
          "as built, the fixes are the report's, in its order",
        );
        await carried(page);
        const lists = await readLists(page, "ol.readiness-fixes");
        assert.deepEqual(
          lists.map((l) => l.items.length),
          split,
          JSON.stringify(lists),
        );
        // Read slide by slide, each number sits beside the fix of that place, once.
        assert.deepEqual(
          lists.flatMap((l) => l.items),
          fixes.map((text, i) => ({ n: i + 1, text })),
        );
        assert.deepEqual(
          lists.map((l) => l.slide),
          lists.map((_, i) => lists[0]!.slide + i),
        );
        assert.deepEqual(await crossing(page, "ol.readiness-fixes > li"), []);
        // Each continued slide begins with the list's heading marked as continued; the first keeps its own.
        const headings = await page.evaluate(() =>
          [...document.querySelectorAll("ol.readiness-fixes")].map((ol) => {
            const page = ol.closest(".page")!;
            const own = [...page.children].filter((c) => !c.hasAttribute("data-chrome") && !(c.matches(".eyebrow, h2") && c.hasAttribute("data-carried-label")));
            return { first: own[0]!.textContent, before: ol.previousElementSibling?.textContent, copy: ol.previousElementSibling?.hasAttribute("data-carried-label") };
          }),
        );
        headings.slice(1).forEach((h) => assert.deepEqual(h, { first: "What to fix before an agent goes live, in order, continued", before: "What to fix before an agent goes live, in order, continued", copy: true }));
        assert.equal(headings[0]!.before, "What to fix before an agent goes live, in order");
        assert.equal(headings[0]!.copy, false);
      } finally {
        await page.close();
      }
    });
  }
});

describe("the other lists set in columns go on in order (DECISIONS.md 580)", () => {
  // The executive summary's references split on a run: from about 120 characters more an issue title.
  for (const [n, slides] of [
    [200, 2],
    [400, 3],
  ] as const) {
    test(`the executive summary's references, issue titles ${n} characters longer: ascending across ${slides} slides`, async () => {
      const page = await open(executiveHtml(buildReport(lengthened(n)), pseudonyms));
      try {
        const refs = async (): Promise<string[][]> => page.evaluate(() => [...document.querySelectorAll(".ref-list")].map((l) => [...l.children].sort((a, b) => Math.round(a.getBoundingClientRect().left) - Math.round(b.getBoundingClientRect().left) || a.getBoundingClientRect().top - b.getBoundingClientRect().top).map((row) => row.querySelector("[data-ref]")!.textContent ?? "")));
        const built = (await refs()).flat();
        assert.deepEqual(built, [...built].sort((a, b) => a.localeCompare(b, "en", { numeric: true })), "built in order of reference");
        await carried(page);
        const read = await refs();
        assert.equal(read.length, slides, JSON.stringify(read));
        assert.ok(read.every((slide) => slide.length > 0));
        assert.deepEqual(read.flat(), built);
        assert.deepEqual(await crossing(page, ".ref-row"), []);
      } finally {
        await page.close();
      }
    });
  }

  // The rest are paginated by the builders with room to spare: issue titles up to 400
  // characters longer (every finding's up to 150), recommendations up to 600, owners up to
  // 200, words in wide capitals, on the deck as printed and at each density step that sets
  // lists in columns, split none of them. So each is pushed down its own slide.

  for (const columns of [2, 3]) {
    test(`the contents in ${columns} columns, pushed down its slide: in page order across the two, numbered on, each page number pointing at its part's slide`, async () => {
      const html = fullReportHtml(buildReport(input), pseudonyms);
      // The synthetic run's contents are long enough for three; two is the rule for a shorter one.
      assert.match(html, /<ol class="contents long">/);
      const page = await open(columns === 3 ? html : html.replace('<ol class="contents long">', '<ol class="contents">'));
      try {
        assert.equal(await page.evaluate(() => parseInt(getComputedStyle(document.querySelector("ol.contents")!).columnCount)), columns);
        // Where each part starts, by the page it was built as.
        await page.evaluate(() => document.querySelectorAll<HTMLElement>(".page").forEach((p) => (p.dataset.built = p.dataset.page)));
        await mark(page, "ol.contents");
        const built = (await readLists(page, "[data-target]"))[0]!;
        await pushDown(page);
        await carried(page);
        const lists = await readLists(page, "[data-target]");
        assert.equal(lists.length, 2, JSON.stringify(lists));
        assert.ok(lists[0]!.items.length > 0 && lists[1]!.items.length > 0);
        assert.equal(lists[1]!.slide, lists[0]!.slide + 1);
        // Number n beside the n-th part as built (its words, the page number being a reference tag).
        assert.deepEqual(
          lists.flatMap((l) => l.items),
          built.items.map((item, i) => ({ n: i + 1, text: item.text })),
        );
        const pointed = await page.evaluate(() => {
          const pages = [...document.querySelectorAll<HTMLElement>(".page")];
          return [...document.querySelectorAll<HTMLElement>("ol.contents .pg")].map((pg) => ({ printed: Number(pg.textContent), part: pages.findIndex((p) => p.dataset.built === pg.dataset.goto) + 1 }));
        });
        assert.ok(pointed.length > 0);
        for (const p of pointed) assert.equal(p.printed, p.part, JSON.stringify(pointed));
        assert.deepEqual(await crossing(page, "ol.contents > li"), []);
      } finally {
        await page.close();
      }
    });
  }

  test("the roadmap's dense action list, pushed down its slide: the items on it and its continued slide, read in order, are the order built", async () => {
    // A dense deck: the first density step that sets issues in two columns.
    const html = fullReportHtmlAt(buildReport(input), pseudonyms, 6);
    assert.match(html, /<ul class="action-list two-text">/);
    const page = await open(html);
    try {
      await mark(page, "ul.action-list.two-text");
      const built = (await readLists(page, "[data-target]"))[0]!;
      assert.ok(built.items.length >= 4, JSON.stringify(built));
      await pushDown(page);
      await carried(page);
      const lists = await readLists(page, "[data-target]");
      assert.equal(lists.length, 2, JSON.stringify(lists));
      assert.ok(lists.every((l) => l.items.length > 0));
      assert.equal(lists[1]!.slide, lists[0]!.slide + 1);
      assert.deepEqual(
        lists.flatMap((l) => l.items.map((i) => i.text)),
        built.items.map((i) => i.text),
      );
      assert.deepEqual(await crossing(page, "[data-target] > li"), []);
    } finally {
      await page.close();
    }
  });

  for (const [kind, step] of [
    ["two-text", null],
    ["three-text", 6],
  ] as const) {
    test(`definitions in ${kind === "two-text" ? "two" : "three"} columns, pushed down their slide: read in order across it and its continued slide`, async () => {
      const report = buildReport(input);
      const page = await open(step === null ? fullReportHtml(report, pseudonyms) : fullReportHtmlAt(report, pseudonyms, step));
      const selector = `div.${kind}`;
      try {
        await mark(page, selector);
        const built = (await readLists(page, "[data-target]"))[0]!;
        assert.ok(built.items.length >= 4, JSON.stringify(built));
        assert.ok(await page.evaluate(() => [...document.querySelector("[data-target]")!.children].every((p) => p.matches("p.definition"))));
        await pushDown(page);
        await carried(page);
        const lists = await readLists(page, "[data-target]");
        assert.equal(lists.length, 2, JSON.stringify(lists));
        assert.ok(lists.every((l) => l.items.length > 0));
        assert.deepEqual(
          lists.flatMap((l) => l.items.map((i) => i.text)),
          built.items.map((i) => i.text),
        );
        assert.deepEqual(await crossing(page, "[data-target] > p"), []);
      } finally {
        await page.close();
      }
    });
  }
});

// Hand-built slides with fixed heights put each split where it's wanted: the content
// runs from 40px to 540px, a heading is 30px and a list item 40px, as in carry-over.test.ts.

const SPLIT_CSS = `
.page { width: 600px; height: 600px; padding: 40px 40px 60px; font-size: 14px; }
.footer { bottom: 20px; left: 40px; right: 40px; }
h2, h3 { height: 30px; line-height: 30px; font-size: 16px; overflow: hidden; white-space: nowrap; }
ul, ol { margin: 0; padding: 0 0 0 24px; }
li { height: 40px; }
.lists { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
.two-up { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
article { height: 100px; }
p { margin: 0; line-height: 20px; white-space: pre-line; }
ol.cols { columns: 2; column-gap: 10px; }
ol.cols.three { columns: 3; }
ol.cols li { break-inside: avoid; }
li.long { height: auto; line-height: 20px; white-space: pre-line; }
`;
const gap = (px: number): string => `<div style="height:${px}px"></div>`;
const lis = (name: string, n: number, from = 1): string => Array.from({ length: n }, (_, i) => `<li>${name}${from + i}</li>`).join("");
const cols = (name: string, n: number, cls = "cols"): string => `<ol class="${cls}">${lis(name, n)}</ol>`;
const sheet = (body: string): string => htmlDocument("Test", "a4", SPLIT_CSS, [`<section class="page" data-page="1"><h2>Title</h2>${body}<div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);
const sentences = (...words: string[]): string => words.map((w) => `${w}.`).join("\n");

/** Each slide's headings and list items, as "h3:Label" and "li:A1", the items in reading order with their printed number. */
async function split(body: string): Promise<{ added: number; slides: string[][]; page: Page }> {
  const page = await open(sheet(body));
  const added = await carried(page);
  const slides = await page.evaluate(() =>
    [...document.querySelectorAll(".page")].map((p) => {
      const at = (el: Element): DOMRect => el.getBoundingClientRect();
      const out: string[] = [];
      for (const el of p.querySelectorAll("h2, h3, ol, ul, p")) {
        if (el.matches("h2, h3, p")) {
          if (!el.closest("li")) out.push(`${el.tagName.toLowerCase()}:${el.textContent}`);
          continue;
        }
        const kids = [...el.children];
        const start = el instanceof HTMLOListElement ? el.start : null;
        for (const li of [...kids].sort((a, b) => Math.round(at(a).left) - Math.round(at(b).left) || at(a).top - at(b).top)) out.push(`li:${start === null ? "" : `${start + kids.indexOf(li)}.`}${li.hasAttribute("data-carried-rest") ? "(rest)" : ""}${(li.textContent ?? "").replace(/\s+/g, " ")}`);
      }
      return out;
    }),
  );
  return { added, slides, page };
}
/** Numbered items name1 to name(to), from `from`, as split() reads them. */
const nums = (name: string, from: number, to: number): string[] => Array.from({ length: to - from + 1 }, (_, i) => `li:${from + i}.${name}${from + i}`);

describe("a list in columns goes on from the end, whole and in order (DECISIONS.md 580)", () => {
  test("(a) two columns, nine items, room for six: the slide holds 1 to 6, the next 7 to 9 from 7", async () => {
    const { added, slides, page } = await split(`${gap(330)}${cols("A", 9)}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(slides, [
        ["h2:Title", ...nums("A", 1, 6)],
        ["h2:Title, continued", ...nums("A", 7, 9)],
      ]);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll("ol")].map((ol) => ol.start)), [1, 7]);
      assert.deepEqual(await crossing(page, "li"), []);
    } finally {
      await page.close();
    }
  });

  test("(b) three columns, twelve items, room for two rows: 1 to 6, then 7 to 12", async () => {
    const { slides, page } = await split(`${gap(370)}${cols("A", 12, "cols three")}`);
    try {
      assert.deepEqual(slides, [
        ["h2:Title", ...nums("A", 1, 6)],
        ["h2:Title, continued", ...nums("A", 7, 12)],
      ]);
      assert.deepEqual(await crossing(page, "li"), []);
    } finally {
      await page.close();
    }
  });

  test("(c) items of very unequal height, the last tall: order kept and nothing lost", async () => {
    const { slides, page } = await split(`${gap(250)}<ol class="cols">${lis("A", 6)}<li style="height:200px">A7</li>${lis("A", 3, 8)}</ol>`);
    try {
      assert.equal(slides.length, 2, JSON.stringify(slides));
      assert.ok(slides.every((s) => s.some((x) => x.startsWith("li:"))), JSON.stringify(slides));
      assert.deepEqual(
        slides.flatMap((s) => s.filter((x) => x.startsWith("li:"))),
        nums("A", 1, 10),
      );
      assert.deepEqual(await crossing(page, "li"), []);
    } finally {
      await page.close();
    }
  });

  test("(d) split over three slides: 1 to 22, 23 to 44, 45 to 50, each list starting where the last left off, under its label", async () => {
    const { added, slides, page } = await split(`<h3>Label</h3>${cols("A", 50)}`);
    try {
      assert.equal(added, 2);
      assert.deepEqual(slides, [
        ["h2:Title", "h3:Label", ...nums("A", 1, 22)],
        ["h2:Title, continued", "h3:Label, continued", ...nums("A", 23, 44)],
        ["h2:Title, continued", "h3:Label, continued", ...nums("A", 45, 50)],
      ]);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll("ol")].map((ol) => ol.start)), [1, 23, 45]);
      assert.deepEqual(await crossing(page, "li"), []);
    } finally {
      await page.close();
    }
  });

  test("(e) first on its slide, its first item taller than the slide: that item stays and is reported, the rest go once, and the run ends", async () => {
    const { added, slides, page } = await split(`<ol class="cols"><li style="height:600px">A1</li>${lis("A", 4, 2)}</ol>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(slides, [
        ["h2:Title", "li:1.A1"],
        ["h2:Title, continued", ...nums("A", 2, 5)],
      ]);
      const layout = await checkLayout(page, { minPages: 1, maxPages: 2 });
      assert.ok(layout.problems.some((p) => p.page === 1 && p.check === "overflow"), JSON.stringify(layout.problems));
    } finally {
      await page.close();
    }
  });

  test("(f) content above and not even the first item fits: the list goes whole with its heading, unlabelled", async () => {
    const { slides, page } = await split(`${gap(420)}<h3>Label</h3>${cols("A", 6)}`);
    try {
      assert.deepEqual(slides, [["h2:Title"], ["h2:Title, continued", "h3:Label", ...nums("A", 1, 6)]]);
      assert.equal(await page.evaluate(() => document.querySelectorAll("[data-carried-label]:not(h2)").length), 0);
    } finally {
      await page.close();
    }
  });

  test("(g) a list in columns with one item, crossing: cut at a sentence end as on base, its rest with no number", async () => {
    // One item isn't side by side with anything, so it takes the old path (572's one-fix case).
    const { added, slides, page } = await split(`${gap(330)}<ol class="cols"><li class="long">${sentences(...Array.from({ length: 10 }, (_, i) => `N1s${i + 1}`))}</li></ol>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(slides, [
        ["h2:Title", "li:1.N1s1. N1s2. N1s3. N1s4. N1s5. N1s6. N1s7."],
        ["h2:Title, continued", "li:1.(rest)N1s8. N1s9. N1s10."],
      ]);
    } finally {
      await page.close();
    }
  });

  test("(h) a grid of three columns and a two-up of cards: as on base, each crossing column or card going on alone", async () => {
    let { slides, page } = await split(`${gap(100)}<div class="lists"><div><h3>Left (3)</h3><ul>${lis("L", 3)}</ul></div><div><h3>Middle (12)</h3><ul>${lis("M", 12)}</ul></div><div><h3>Right (3)</h3><ul>${lis("R", 3)}</ul></div></div>`);
    await page.close();
    assert.deepEqual(slides, [
      ["h2:Title", "h3:Left (3)", "li:L1", "li:L2", "li:L3", "h3:Middle (12)", ...Array.from({ length: 8 }, (_, i) => `li:M${i + 1}`), "h3:Right (3)", "li:R1", "li:R2", "li:R3"],
      ["h2:Title, continued", "h3:Middle (12), continued", "li:M9", "li:M10", "li:M11", "li:M12"],
    ]);
    ({ slides, page } = await split(`${gap(330)}<div class="two-up">${["C1", "C2", "C3", "C4"].map((c) => `<article><p>${c}</p></article>`).join("")}</div>`));
    await page.close();
    assert.deepEqual(slides, [
      ["h2:Title", "p:C1", "p:C2"],
      ["h2:Title, continued", "p:C3", "p:C4"],
    ]);
  });

  test("(i) a list in columns inside a grid column that crosses: the grid's rule for the column, the list's for its items", async () => {
    const { slides, page } = await split(`${gap(100)}<div class="lists"><div><h3>Left (24)</h3>${cols("L", 24)}</div><div><h3>Middle (3)</h3><ul>${lis("M", 3)}</ul></div><div><h3>Right (3)</h3><ul>${lis("R", 3)}</ul></div></div>`);
    try {
      assert.deepEqual(slides, [
        ["h2:Title", "h3:Left (24)", ...nums("L", 1, 16), "h3:Middle (3)", "li:M1", "li:M2", "li:M3", "h3:Right (3)", "li:R1", "li:R2", "li:R3"],
        ["h2:Title, continued", "h3:Left (24), continued", ...nums("L", 17, 24)],
      ]);
      assert.deepEqual(await crossing(page, "li"), []);
    } finally {
      await page.close();
    }
  });

  test("(j) a list in columns followed by a note: the note goes on after the carried items, once", async () => {
    const { slides, page } = await split(`${gap(330)}${cols("A", 9)}<p>Note.</p>`);
    try {
      assert.deepEqual(slides, [
        ["h2:Title", ...nums("A", 1, 6)],
        ["h2:Title, continued", ...nums("A", 7, 9), "p:Note."],
      ]);
    } finally {
      await page.close();
    }
  });

  test("(k) an item that takes no room first, then one taller than the room: the tall one is the item that stays, and the slide isn't left with only its heading (DECISIONS.md 577)", async () => {
    // The hidden item was kept as the one that stays and counted as content: the first slide
    // printed only its heading, and the tall item went on alone to a slide of its own.
    const { added, slides, page } = await split(`<h3>Label</h3><ol class="cols"><li style="display:none">Hidden</li><li style="height:520px">A1</li>${lis("A", 4, 2)}</ol>`);
    try {
      assert.equal(added, 1);
      // Read without numbers or the hidden item, which prints neither.
      assert.deepEqual(
        slides.map((s) => s.map((x) => x.replace(/^li:\d+\./, "li:")).filter((x) => x !== "li:Hidden")),
        [
          ["h2:Title", "h3:Label", "li:A1"],
          ["h2:Title, continued", "h3:Label, continued", "li:A2", "li:A3", "li:A4", "li:A5"],
        ],
      );
      // A1 is taller than the room under the label: it stays and is reported, as a block alone is.
      assert.deepEqual(await crossing(page, "li"), ["slide 1: A1"]);
    } finally {
      await page.close();
    }
  });

  test("(l) a grid with nothing above it, a list in columns that splits in its first column and a card past the footer in its second: the items that stay are content, so the card goes on", async () => {
    // The card's margin keeps it past the footer on every slide: it goes on to the last and is reported there.
    const { added, slides, page } = await split(`<div class="two-up"><div><h3>Left</h3>${cols("A", 30)}</div><article style="height:150px;margin-top:400px"><p>Card</p></article></div>`);
    try {
      assert.equal(added, 2);
      assert.deepEqual(slides, [
        ["h2:Title", "h3:Left", ...nums("A", 1, 22)],
        ["h2:Title, continued", "h3:Left, continued", ...nums("A", 23, 30)],
        ["h2:Title, continued", "p:Card"],
      ]);
    } finally {
      await page.close();
    }
  });

  test("(m) a heading in columns, the block beside it going on: a heading kept alone isn't content, so a card crossing beside the columns stays with it", async () => {
    // Nothing above. Counted as content, the heading let the card go on and was left alone on the slide.
    const { added, slides, page } = await split(`<div class="two-up"><div style="columns:2;column-gap:10px"><h3>Group</h3><p style="height:480px;break-inside:avoid">Words that can't be cut</p></div><article style="height:480px"><p>Card</p></article></div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(slides, [
        ["h2:Title", "h3:Group", "p:Card"],
        ["h2:Title, continued", "p:Words that can't be cut"],
      ]);
    } finally {
      await page.close();
    }
  });
});
