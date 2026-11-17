import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../../domains/index.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Score, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { carryOver } from "../carry-over.ts";
import { executiveHtml } from "../executive.ts";
import { fullReportHtml, fullReportHtmlAt, slideCounts } from "../full-report.ts";
import { document as htmlDocument } from "../html.ts";
import { checkLayout } from "../layout.ts";
import { launchBrowser } from "../pdf.ts";

// A block whose words fit but whose own bottom padding or margin crosses the footer
// moves to the next slide only when something stays above it (DECISIONS.md 574).
// Alone under a slide's heading it stays: the next slide would be the same slide, and
// the carry-over moved it again and again up to its limit of 200 added slides.

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

type Carried = { added: number; pages: number; headingOnly: string[]; dropped: string[]; doubled: string[] };

/**
 * Carries a page over and checks the two things 574 holds on any deck. `headingOnly`: every
 * slide but a cover holding nothing but its own eyebrow and title and what the carry-over
 * copied. `dropped` and `doubled`: the text built against the text printed, node by node,
 * outside the footers, the copies and the contents page's numbers. The rest of a cut
 * sentence joins back onto its first half; a table's head is copied over its rows on purpose,
 * and when the table goes on whole its head is the copy.
 */
async function carry(page: Page): Promise<Carried> {
  await page.evaluate(() => {
    const kept = (n: Node): boolean => !!n.parentElement?.closest(".page") && !n.parentElement.closest("[data-chrome], [data-carried-label], [data-goto], style, script") && !!(n.textContent ?? "").trim();
    const nodes: Array<{ node: Text; text: string }> = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) if (kept(n)) nodes.push({ node: n as Text, text: n.textContent ?? "" });
    (window as unknown as { __built: typeof nodes }).__built = nodes;
  });
  const added = await carryOver(page);
  const facts = await page.evaluate(() => {
    const kept = (n: Node): boolean => !!n.parentElement?.closest(".page") && !n.parentElement.closest("[data-chrome], [data-carried-label], [data-goto], style, script") && !!(n.textContent ?? "").trim();
    const built = (window as unknown as { __built: Array<{ node: Text; text: string }> }).__built;
    const pages = [...document.querySelectorAll<HTMLElement>(".page")];
    const headingOnly = pages.flatMap((p, i) => {
      if (p.classList.contains("cover")) return [];
      const own = (el: Element): boolean => el.parentElement === p && (el.classList.contains("eyebrow") || el.tagName === "H1" || el.tagName === "H2");
      const walker = document.createTreeWalker(p, NodeFilter.SHOW_TEXT);
      for (let n = walker.nextNode(); n; n = walker.nextNode()) {
        const el = n.parentElement!;
        if (!(n.textContent ?? "").trim() || el.closest("[data-chrome], [data-carried-label]")) continue;
        let at: Element | null = el;
        while (at && at.parentElement !== p) at = at.parentElement;
        if (at && !own(at)) return [];
      }
      if (p.querySelector(":scope > :not([data-chrome]) :is(svg, img, canvas), :scope > :is(svg, img, figure)")) return [];
      return [`slide ${i + 1}: ${[...p.children].filter((c) => !c.hasAttribute("data-chrome")).map((c) => (c.textContent ?? "").trim()).join(" / ").slice(0, 120)}`];
    });
    const known = new Set<Node>(built.map((b) => b.node));
    const now: Text[] = [];
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = walker.nextNode(); n; n = walker.nextNode()) if (kept(n)) now.push(n as Text);
    // New text outside a copied table head is the rest of a cut sentence, or doubled.
    const fresh = now.filter((n) => !known.has(n) && !n.parentElement!.closest("thead"));
    const used = new Set<Node>();
    const dropped: string[] = [];
    for (const { node, text } of built) {
      if (!node.isConnected || !kept(node)) {
        // A table that went on whole left its head behind in the copy that went with it.
        if (node.parentElement?.closest("thead") && now.some((n) => n.parentElement!.closest("thead") && n.textContent === text)) continue;
        dropped.push(text);
        continue;
      }
      const shown = node.textContent ?? "";
      if (shown === text) continue;
      if (!text.startsWith(shown)) {
        dropped.push(text);
        continue;
      }
      let rest = text.slice(shown.length).replace(/^\s+/, "");
      while (rest) {
        // A rest that opens inside an inline element (a title in bold) keeps the space before it.
        const next = fresh.find((f) => !used.has(f) && rest.startsWith((f.textContent ?? "").replace(/^\s+/, "")));
        if (!next) break;
        used.add(next);
        rest = rest.slice((next.textContent ?? "").replace(/^\s+/, "").length).replace(/^\s+/, "");
      }
      if (rest) dropped.push(text);
    }
    // Nothing the carry-over hides holds words: the layout check reads every word, printed or not (DECISIONS.md 579).
    const hidden: string[] = [];
    const all = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n = all.nextNode(); n; n = all.nextNode()) {
      const el = n.parentElement;
      if (el && (n.textContent ?? "").trim() && getComputedStyle(el).visibility !== "visible") hidden.push(`slide ${pages.indexOf(el.closest<HTMLElement>(".page")!) + 1}: ${n.textContent}`);
    }
    return { pages: pages.length, headingOnly, dropped, doubled: fresh.filter((f) => !used.has(f)).map((f) => f.textContent ?? ""), hidden };
  });
  const { hidden, ...held } = facts;
  assert.deepEqual(hidden, [], "words in what isn't printed");
  return { added, ...held };
}

/** Renders and carries over a whole document, as the documents job does. */
async function carried(html: string): Promise<Carried> {
  const context = await browser.newContext({ offline: true });
  try {
    const page = await context.newPage();
    await page.setContent(html, { waitUntil: "load" });
    await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
    await page.emulateMedia({ media: "print" });
    return await carry(page);
  } finally {
    await context.close();
  }
}

function assertHolds(name: string, result: Carried): void {
  assert.deepEqual(result.headingOnly, [], `${name}: a slide holding only its heading`);
  assert.deepEqual(result.dropped, [], `${name}: text dropped`);
  assert.deepEqual(result.doubled, [], `${name}: text printed twice`);
}

// Hand-built slides with fixed heights put the crossing where it's wanted: the content
// runs from 40px to 540px, a heading is 30px, a list item 40px, a line 20px. A padded
// block's own bottom padding is 30px, so one holding 460px of words under the title
// fits to 530px and crosses with its padding.

const CSS = `
.page { width: 600px; height: 600px; padding: 40px 40px 60px; font-size: 14px; }
.footer { bottom: 20px; left: 40px; right: 40px; }
h2, h3 { height: 30px; line-height: 30px; font-size: 16px; overflow: hidden; white-space: nowrap; }
ul { margin: 0; padding: 0 0 0 18px; }
li { height: 40px; }
p { margin: 0; line-height: 20px; }
.padded { padding: 0 0 30px; background: #eee; }
.cols { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; }
.cols3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; align-items: start; }
`;
const sheet = (body: string): string => htmlDocument("Test", "a4", CSS, [`<section class="page" data-page="1"><h2>Title</h2>${body}<div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);
const words = (text: string, height: number): string => `<p style="height:${height}px">${text}</p>`;
const padded = (text: string, height = 460): string => `<div class="padded">${words(text, height)}</div>`;
const items = (name: string, n: number): string => `<ul>${Array.from({ length: n }, (_, i) => `<li>${name}${i + 1}</li>`).join("")}</ul>`;
const li = (name: string, n: number): string[] => Array.from({ length: n }, (_, i) => `li:${name}${i + 1}`);

/** What every slide prints, in order: its headings, paragraphs and list items. */
async function slides(body: string): Promise<{ added: number; pages: string[][]; result: Carried; page: Page }> {
  const page = await open(sheet(body));
  const result = await carry(page);
  const pages = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.querySelectorAll("h2, h3, p, li")].map((e) => `${e.tagName.toLowerCase()}:${e.textContent}`)));
  return { added: result.added, pages, result, page };
}

describe("carry-over: a block that fits but for its own padding or margin (DECISIONS.md 574)", () => {
  test("(a) a padded block alone under the title, its words fitting and its padding crossing: nothing is added", async () => {
    const { added, pages, result, page } = await slides(padded("Words."));
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", "p:Words."]]);
      assertHolds("(a)", result);
    } finally {
      await page.close();
    }
  });

  test("(b) the same with a paragraph above: it moves once, whole, and two slides print", async () => {
    // 90px to 530px of words, its padding to 560px; on the continued slide it fits to 540px.
    const { added, pages, result, page } = await slides(`${words("Above.", 20)}${padded("Words.", 440)}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:Above."],
        ["h2:Title, continued", "p:Words."],
      ]);
      assertHolds("(b)", result);
    } finally {
      await page.close();
    }
  });

  test("(c) the same with a paragraph above, its padding still crossing on the continued slide: two slides, the block on the second", async () => {
    const { added, pages, result, page } = await slides(`${words("Above.", 20)}${padded("Words.")}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:Above."],
        ["h2:Title, continued", "p:Words."],
      ]);
      assertHolds("(c)", result);
    } finally {
      await page.close();
    }
  });

  test("(d) a padded block first, then a block past the footer: the first stays, the second goes on, nothing lost and nothing printed twice", async () => {
    const { added, pages, result, page } = await slides(`${padded("First.")}<div><h3>Next</h3>${items("A", 3)}</div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:First."],
        ["h2:Title, continued", "h3:Next", ...li("A", 3)],
      ]);
      assertHolds("(d)", result);
    } finally {
      await page.close();
    }
  });

  test("(e) a wrapper first on the slide, a padded block first in it and a list after: the block stays, the list goes on inside the wrapper", async () => {
    const { added, pages, result, page } = await slides(`<div class="wrap">${padded("First.")}${items("A", 3)}</div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:First."],
        ["h2:Title, continued", ...li("A", 3)],
      ]);
      assert.equal(await page.evaluate(() => document.querySelectorAll(".page")[1]!.querySelector(":scope > .wrap > ul") !== null), true, "the list inside the wrapper's shell");
      assertHolds("(e)", result);
    } finally {
      await page.close();
    }
  });

  test("(f) a block whose bottom margin alone crosses, alone under the title: nothing is added, and nothing past the footer is reported", async () => {
    const WORDS = "Words that stay on the slide, long enough to count as its content.";
    // Here, as on both decks, the lowest the content may reach is the page's padding, above the
    // footer. The margin prints nothing, but the page's scroll height takes it in, so the layout
    // check says the content is larger than the page. It says nothing runs into the footer.
    let { added, pages, result, page } = await slides(`<div style="margin-bottom:30px">${words(WORDS, 460)}</div>`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", `p:${WORDS}`]]);
      assertHolds("(f)", result);
      const layout = await checkLayout(page, { minPages: 1, maxPages: 1 });
      assert.deepEqual(layout.problems, [{ page: 1, check: "overflow", detail: "content is larger than the page" }]);
    } finally {
      await page.close();
    }
    // Where the footer's top is the lowest instead, a margin that crosses it warns of nothing.
    const LOW = `${CSS} .page { padding-bottom: 20px; } .footer { height: 20px; }`;
    page = await open(htmlDocument("Test", "a4", LOW, [`<section class="page" data-page="1"><h2>Title</h2><div style="margin-bottom:30px">${words(WORDS, 480)}</div><div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]));
    try {
      ({ added } = await carry(page));
      assert.equal(added, 0);
      assert.deepEqual(await page.evaluate(() => document.querySelectorAll(".page").length), 1);
      assert.deepEqual((await checkLayout(page, { minPages: 1, maxPages: 1 })).problems, []);
    } finally {
      await page.close();
    }
  });

  test("(g) a list with a bottom margin whose items all fit, alone under the title: nothing is added", async () => {
    // Eleven items to 510px, the list's margin to 550px.
    const { added, pages, result, page } = await slides(`<ul style="margin-bottom:40px">${Array.from({ length: 11 }, (_, i) => `<li>A${i + 1}</li>`).join("")}</ul>`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", ...li("A", 11)]]);
      assertHolds("(g)", result);
    } finally {
      await page.close();
    }
  });

  test("(h) under a part's label with content above the label: the label goes on with the block it heads (DECISIONS.md 508), and the first slide doesn't end with it", async () => {
    // Words above to 130px, the label to 160px, the block's words to 530px and its padding to 560px.
    const { added, pages, result, page } = await slides(`${words("Above.", 60)}<h3>Label</h3>${padded("Words.", 370)}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:Above."],
        ["h2:Title, continued", "h3:Label", "p:Words."],
      ]);
      assertHolds("(h)", result);
    } finally {
      await page.close();
    }
  });

  test("(i) the same with nothing above the label: nothing moves", async () => {
    const { added, pages, result, page } = await slides(`<h3>Label</h3>${padded("Words.", 430)}`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", "h3:Label", "p:Words."]]);
      assertHolds("(i)", result);
    } finally {
      await page.close();
    }
  });

  test("the session 34 probe: a quick-wins panel whose words fit a slide of their own and whose padding doesn't prints on two slides, the panel on the second", async () => {
    // Its own page, word for word: a 600px slide, 200px of words, then the panel's 440px.
    // The panel's padding runs past the page's padding on the second slide (its background is
    // there), which the layout check reports. The probe's panel holds 26 characters, under the
    // 40 an empty section is held for; with 40 or more that warning goes, as it would for any slide.
    const PROBE_CSS = `.page { width: 600px; height: 600px; padding: 40px 40px 60px; font-size: 14px; } .footer { bottom: 20px; left: 40px; right: 40px; } h2 { height: 30px; line-height: 30px; font-size: 16px; } .panel { padding: 0 0 30px; background: #eee; }`;
    const probe = (wins: string): string => htmlDocument("Probe", "a4", PROBE_CSS, [`<section class="page" data-page="1"><div class="eyebrow">Roadmap</div><h2>Days 1 to 30: Stabilise</h2><p style="height:200px">Words that stay on the first page.</p><div class="panel"><p style="height:440px">${wins}</p></div><div class="footer" data-chrome><span>Footer</span><span>1</span></div></section>`]);
    const OVERFLOW = { page: 2, check: "overflow", detail: "content is larger than the page" };
    for (const [wins, problems] of [
      ["Quick wins whose words fit", [OVERFLOW, { page: 2, check: "empty section", detail: 'only "Quick wins whose words fit"' }]],
      ["Quick wins whose words fit on a slide of their own", [OVERFLOW]],
    ] as const) {
      const page = await open(probe(wins));
      try {
        const result = await carry(page);
        assert.deepEqual({ added: result.added, pages: result.pages }, { added: 1, pages: 2 });
        assertHolds("the probe", result);
        const shown = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.children].filter((c) => !c.hasAttribute("data-chrome")).map((c) => (c.textContent ?? "").trim()).join(" | ")));
        assert.deepEqual(shown, ["Roadmap | Days 1 to 30: Stabilise | Words that stay on the first page.", `Roadmap | Days 1 to 30: Stabilise, continued | ${wins}`]);
        assert.deepEqual((await checkLayout(page, { minPages: 1, maxPages: 5 })).problems, problems);
      } finally {
        await page.close();
      }
    }
  });
});

// Columns side by side leave the shell of a column whose content stays. A block holding only
// such shells stays, and what follows it goes on recorded; carried words are never thrown away;
// and what takes no room neither stays nor goes (DECISIONS.md 576).

/** What every slide holds at its top level, past its footer: the shells a block left show here. */
const tops = (page: Page): Promise<string[][]> =>
  page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.children].filter((c) => !c.hasAttribute("data-chrome")).map((c) => `${c.tagName.toLowerCase()}${c.className ? `.${c.className}` : ""}`)));
const A = "The first column's words fit above the footer.";
const B = "The second column's words.";
const AFTER = "The closing paragraph under the columns.";

describe("carry-over: nothing a slide gives up is thrown away, and what takes no room neither stays nor goes (DECISIONS.md 576)", () => {
  // The adversary's three shapes. The panel's words run to 530px and its padding to 560px.
  for (const [name, body, expected, shape] of [
    [
      "a column holding a padded block, then a paragraph",
      `<div class="cols"><div>${padded(A)}</div><div>${words(B, 20)}</div></div>${words(AFTER, 20)}`,
      [["h2:Title", `p:${A}`, `p:${B}`], ["h2:Title, continued", `p:${AFTER}`]],
      [["h2", "div.cols"], ["h2", "p"]],
    ],
    [
      // Lost at 0bb998e too.
      "the padded block as the column itself, then a paragraph",
      `<div class="cols"><div class="padded">${words(A, 460)}</div><div>${words(B, 20)}</div></div>${words(AFTER, 20)}`,
      [["h2:Title", `p:${A}`, `p:${B}`], ["h2:Title, continued", `p:${AFTER}`]],
      [["h2", "div.cols"], ["h2", "p"]],
    ],
    [
      // The first column's words stay, so the panel beside them goes on whole, then stays alone.
      "a column holding a padded block second, then a paragraph",
      `<div class="cols"><div>${words(B, 20)}</div><div>${padded(A)}</div></div>${words(AFTER, 20)}`,
      [["h2:Title", `p:${B}`], ["h2:Title, continued", `p:${A}`], ["h2:Title, continued", `p:${AFTER}`]],
      [["h2", "div.cols"], ["h2", "div.cols"], ["h2", "p"]],
    ],
  ] as const) {
    test(`${name}: the paragraph prints once, on the continued slide, with no empty shell above it`, async () => {
      const { pages, result, page } = await slides(body);
      try {
        assert.deepEqual(pages, expected);
        assert.equal(pages.flat().filter((x) => x === `p:${AFTER}`).length, 1);
        assert.deepEqual(await tops(page), shape);
        assertHolds(name, result);
      } finally {
        await page.close();
      }
    });
  }

  test("two columns each crossing by words that can't be cut, then a paragraph: the columns stay, counted as the slide's content, and the paragraph goes on", async () => {
    // A single sentence can't be split and has nothing above it, so neither column places anything:
    // the grid that stays is what counts as the slide's content.
    const { pages, result, page } = await slides(`<div class="cols"><div>${words("Left words", 480)}</div><div>${words("Right words", 480)}</div></div>${words(AFTER, 20)}`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left words", "p:Right words"], ["h2:Title, continued", `p:${AFTER}`]]);
      assertHolds("unsplit columns", result);
    } finally {
      await page.close();
    }
  });

  test("words that leave a slide are printed on the next, whatever was recorded: a wrapper holding a grid and a paragraph, then a paragraph after it", async () => {
    // The grid one level down: the paragraph inside the wrapper and the one after it both go on.
    const INSIDE = "The paragraph inside the wrapper.";
    const { pages, result, page } = await slides(`<div class="wrap"><div class="cols"><div>${padded(A)}</div><div>${words(B, 20)}</div></div>${words(INSIDE, 20)}</div>${words(AFTER, 20)}`);
    try {
      assert.deepEqual(pages, [["h2:Title", `p:${A}`, `p:${B}`], ["h2:Title, continued", `p:${INSIDE}`, `p:${AFTER}`]]);
      assertHolds("the wrapper", result);
    } finally {
      await page.close();
    }
  });

  test("a block with height and no words that goes on beside a column that went on first is kept: only the shells made are taken for shells", async () => {
    // The first column's words go on, so something has moved before the second column is looked
    // into; there a rule crosses and goes on inside its wrapper's shell. Holding nothing printed,
    // that shell is still not one of empty shells, and the rule is printed on the continued slide.
    const { added, pages, result, page } = await slides(
      `<div class="cols"><div>${words("Left above.", 20)}${words("Left words", 480)}</div><div>${words("Right above.", 20)}<div class="wrap">${words("Inner.", 20)}<div class="rule" style="height:460px;background:#000"></div></div></div></div>`,
    );
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [["h2:Title", "p:Left above.", "p:Right above.", "p:Inner."], ["h2:Title, continued", "p:Left words"]]);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelectorAll(".rule").length)), [0, 1]);
      assertHolds("the rule", result);
    } finally {
      await page.close();
    }
  });

  test("a body of empty shells is still thrown away: a column holding a padded block and nothing after the grid adds no slide", async () => {
    const { added, pages, result, page } = await slides(`<div class="cols"><div>${padded(A)}</div><div>${words(B, 20)}</div></div>`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", `p:${A}`, `p:${B}`]]);
      assertHolds("shells only", result);
    } finally {
      await page.close();
    }
  });

  test("a grid with a paragraph above whose column crosses by its padding alone, nothing after it, prints as at 0bb998e", async () => {
    // Run on both: at 0bb998e and here the panel goes on whole in its column's shell, and the
    // panel that is the column itself stays with the grid.
    for (const [body, expected, shape] of [
      [`${words("Above.", 20)}<div class="cols"><div>${padded(A, 440)}</div><div>${words(B, 20)}</div></div>`, [["h2:Title", "p:Above.", `p:${B}`], ["h2:Title, continued", `p:${A}`]], [["h2", "p", "div.cols"], ["h2", "div.cols"]]],
      [`${words("Above.", 20)}<div class="cols"><div class="padded">${words(A, 440)}</div><div>${words(B, 20)}</div></div>`, [["h2:Title", "p:Above.", `p:${A}`, `p:${B}`]], [["h2", "p", "div.cols"]]],
    ] as const) {
      const { pages, result, page } = await slides(body);
      try {
        assert.deepEqual(pages, expected);
        assert.deepEqual(await tops(page), shape);
        assertHolds("above the grid", result);
      } finally {
        await page.close();
      }
    }
  });

  test("an empty element after a block that stays isn't moved alone: one slide", async () => {
    for (const empty of ['<div class="spacer"></div>', '<p class="note"></p>']) {
      const { added, pages, result, page } = await slides(`${padded("Words.")}${empty}`);
      try {
        assert.equal(added, 0, empty);
        assert.deepEqual(pages, [["h2:Title", "p:Words.", ...(empty.startsWith("<p") ? ["p:"] : [])]]);
        assertHolds(empty, result);
      } finally {
        await page.close();
      }
    }
  });

  test("an emptied column's shell on a continued slide doesn't count as content: the panel beside it stays there, two slides", async () => {
    // The second column's panel crosses by its words and goes on; on the continued slide the first
    // column's shell has no height, and the panel, crossing by its padding alone, stays.
    const { added, pages, result, page } = await slides(`<div class="cols"><div>${padded("Left.")}</div><div>${words("Right above.", 20)}${padded("Right.")}</div></div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [["h2:Title", "p:Left.", "p:Right above."], ["h2:Title, continued", "p:Right."]]);
      assertHolds("the emptied column", result);
    } finally {
      await page.close();
    }
  });

  test("three columns side by side, two going on: the emptied shells on each continued slide count for nothing, and no slide holds only its heading", async () => {
    const { added, pages, result, page } = await slides(`<div class="cols3"><div>${padded("One.")}</div><div>${words("Two above.", 20)}${padded("Two.")}</div><div>${words("Three above.", 20)}${padded("Three.")}</div></div>`);
    try {
      assert.equal(added, 2);
      assert.deepEqual(pages, [["h2:Title", "p:One.", "p:Two above.", "p:Three above."], ["h2:Title, continued", "p:Two."], ["h2:Title, continued", "p:Three."]]);
      assertHolds("three columns", result);
    } finally {
      await page.close();
    }
  });

  test("an empty block before the first content isn't content above: a padded block after it, alone, stays", async () => {
    const { added, pages, result, page } = await slides(`<div class="spacer"></div>${padded("Words.")}`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", "p:Words."]]);
      assert.deepEqual(await tops(page), [["h2", "div.spacer", "div.padded"]]);
      assertHolds("empty first", result);
    } finally {
      await page.close();
    }
  });

  test("an empty block between a paragraph that stays and a block that moves whole: as at 2bf07e9, the empty block stays with the paragraph", async () => {
    const { added, pages, result, page } = await slides(`${words("Above.", 20)}<div class="spacer"></div>${padded("Words.", 440)}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [["h2:Title", "p:Above."], ["h2:Title, continued", "p:Words."]]);
      assert.deepEqual(await tops(page), [["h2", "p", "div.spacer"], ["h2", "div.padded"]]);
      assertHolds("empty between", result);
    } finally {
      await page.close();
    }
  });

  test("a block with height and no words is content: a chart stays as content above, and a chart that crosses goes on", async () => {
    // A chart above a block that fits but for its padding: the block moves whole, as under any content.
    const chart = (height: number): string => `<figure><svg width="300" height="${height}" style="display:block"><rect width="300" height="${height}"></rect></svg></figure>`;
    let { added, pages, result, page } = await slides(`${chart(100)}${padded("Words.", 370)}`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [["h2:Title"], ["h2:Title, continued", "p:Words."]]);
      assert.deepEqual(await tops(page), [["h2", "figure"], ["h2", "div.padded"]]);
      assertHolds("a chart above", result);
    } finally {
      await page.close();
    }
    // A chart under a paragraph, crossing the footer by its own height: it goes on whole.
    ({ added, pages, result, page } = await slides(`${words("Above.", 200)}${chart(300)}`));
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [["h2:Title", "p:Above."], ["h2:Title, continued"]]);
      assert.deepEqual(await tops(page), [["h2", "p"], ["h2", "figure"]]);
      assertHolds("a chart", result);
    } finally {
      await page.close();
    }
  });
});

// A copy the carry-over made that nothing went into takes no room, whatever its padding or
// border: it neither stays nor goes, and it holds its place unpainted. A block with words of
// its own beside its child blocks isn't looked into (DECISIONS.md 577).

/** Each slide's words in the order they print, past the footers and the copied headings (so a continued slide's title too). */
const reading = (page: Page): Promise<string[]> =>
  page.evaluate(() =>
    [...document.querySelectorAll(".page")].map((p) => {
      const out: string[] = [];
      const walker = document.createTreeWalker(p, NodeFilter.SHOW_TEXT);
      for (let n = walker.nextNode(); n; n = walker.nextNode()) if (!n.parentElement!.closest("[data-chrome], [data-carried-label], style")) out.push(n.textContent ?? "");
      return out.join(" ").replace(/\s+/g, " ").trim();
    }),
  );

/**
 * On the continued slide: the copy at `copy` there, whether it paints and how tall it is ("gone"
 * and 0 if it isn't there); and the left edge of the column at `beside`, on each slide, from the
 * slide's own edge.
 */
const heldPlace = (page: Page, copy: string, beside: string): Promise<{ visibility: string; height: number; left: number[] }> =>
  page.evaluate(
    ([copy, beside]) => {
      const pages = [...document.querySelectorAll(".page")];
      const shell = pages[1]!.querySelector(copy);
      return {
        visibility: shell ? getComputedStyle(shell).visibility : "gone",
        height: shell?.getBoundingClientRect().height ?? 0,
        left: pages.map((p) => Math.round((p.querySelector(beside)!.getBoundingClientRect().left - p.getBoundingClientRect().left) * 100) / 100),
      };
    },
    [copy, beside] as const,
  );

/** Every element inside `within` on the continued slide that isn't painted. */
const unpainted = (page: Page, within: string): Promise<string[]> =>
  page.evaluate((within) => [...document.querySelectorAll(".page")[1]!.querySelectorAll(`${within}, ${within} *`)].filter((e) => getComputedStyle(e).visibility !== "visible").map((e) => e.outerHTML.slice(0, 60)), within);

// A grid on the first slide that stacks on the slides after it, or sets its columns in CSS
// columns there: no builder does either, and it is the shape that puts a copy left empty
// beside a column under the vertical loop or the columns rule on the continued slide.
const RESTACKED = `<style>.restack { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; } .page:not(:first-of-type) .restack { display: block; } .page:not(:first-of-type) .restack.folded { columns: 2; column-gap: 16px; } .restack.folded > * { break-inside: avoid; }</style>`;

describe("carry-over: an emptied copy holds its place and nothing else (DECISIONS.md 577)", () => {
  test("a padded column whose words stay beside a column whose padded block goes on: two slides, the copy left on the second holding its place unpainted", async () => {
    // The first column crosses by its padding and leaves its copy, padding and all, on the
    // continued slide. Taken for content there, it let the block beside it go on again: three
    // slides, the second holding only its copied heading and an empty box.
    const { pages, result, page } = await slides(`<div class="cols"><div class="padded">${words("Left.", 460)}</div><div>${words("Right above.", 20)}${padded("Right.")}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left.", "p:Right above."], ["h2:Title, continued", "p:Right."]]);
      assertHolds("the padded column", result);
      const held = await heldPlace(page, ".cols > .padded:first-child", ".cols > :last-child");
      assert.equal(held.left[1], held.left[0], "the column beside it where it was");
      assert.equal(held.visibility, "hidden");
      assert.ok(held.height >= 30, `the copy is ${held.height}px tall`);
      assert.deepEqual(await unpainted(page, ".cols > :last-child"), [], "what went on is printed");
    } finally {
      await page.close();
    }
  });

  test("a bordered column beside a column whose last words can't be cut: two slides, the copy left on the second holding its place unpainted", async () => {
    const { pages, result, page } = await slides(`<style>.boxed { padding: 10px; border: 1px solid #000; }</style><div class="cols"><div class="boxed">${words("Left.", 450)}</div><div>${words("Right above.", 20)}${words("Right one long sentence", 480)}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left.", "p:Right above."], ["h2:Title, continued", "p:Right one long sentence"]]);
      assertHolds("the bordered column", result);
      const held = await heldPlace(page, ".cols > .boxed:first-child", ".cols > :last-child");
      assert.equal(held.left[1], held.left[0], "the column beside it where it was");
      assert.equal(held.visibility, "hidden");
      assert.ok(held.height >= 20, `the copy is ${held.height}px tall`);
    } finally {
      await page.close();
    }
  });

  test("a copy that holds what went on is printed: a padded column's carried words, and a table's copy with its rows", async () => {
    // The padded column itself is cut: its second paragraph goes on inside its copy, background and all.
    let { pages, result, page } = await slides(`<div class="cols"><div class="padded">${words("Left one.", 20)}${words("Left two, which can't be cut", 480)}</div><div>${words("Right.", 20)}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left one.", "p:Right."], ["h2:Title, continued", "p:Left two, which can't be cut"]]);
      assertHolds("the carried column", result);
      assert.deepEqual(await unpainted(page, ".cols"), []);
    } finally {
      await page.close();
    }
    // A table cut between its rows: its copy holds its head and the rows that went on.
    const rows = Array.from({ length: 8 }, (_, i) => `<tr><td style="height:40px;padding:0">Row ${i + 1}</td></tr>`).join("");
    ({ pages, result, page } = await slides(`${words("Above.", 300)}<table style="border-collapse:collapse"><thead><tr><th style="height:30px;padding:0">Head</th></tr></thead><tbody>${rows}</tbody></table>`));
    try {
      assert.deepEqual(await reading(page), ["Title Above. Head Row 1 Row 2 Row 3", "Head Row 4 Row 5 Row 6 Row 7 Row 8"]);
      assertHolds("the table", result);
      assert.deepEqual(await unpainted(page, "table"), []);
    } finally {
      await page.close();
    }
  });

  test("a table first in a grid, its rows fitting and its last collapsed border crossing, beside words that go on: two slides, the copy of its head left unprinted in its place", async () => {
    // The table's copy holds only the head we copied. Taken for content, it printed "Head cell"
    // alone on a slide of its own, and the words beside it went on to a third.
    const cell = "padding:0;border-bottom:9px solid #000";
    const { result, page } = await slides(`<div class="cols"><table style="border-collapse:collapse;width:100%"><thead><tr><th style="height:30px;${cell}">Head cell</th></tr></thead><tbody><tr><td style="height:439px;${cell}">Row</td></tr></tbody></table><div>${words("Right above.", 20)}${words("Right one long sentence", 480)}</div></div>`);
    try {
      // The copied head's words are taken out of its unprinted copy (DECISIONS.md 579).
      assert.deepEqual(await reading(page), ["Title Head cell Row Right above.", "Right one long sentence"]);
      assertHolds("the table's head", result);
      const held = await heldPlace(page, ".cols > table:first-child", ".cols > :last-child");
      assert.equal(held.left[1], held.left[0], "the column beside it where it was");
      assert.equal(held.visibility, "hidden");
    } finally {
      await page.close();
    }
  });

  test("a block that stacks on the continued slide: an emptied copy above the column that went on isn't content above it, two slides", async () => {
    // Stacked, the copy is the first thing on the continued slide. Counted as staying there,
    // it let the block under it go on again, alone.
    const { pages, result, page } = await slides(`${RESTACKED}<div class="restack"><div class="padded">${words("Left.", 460)}</div><div>${words("Right above.", 20)}${padded("Right.", 440)}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left.", "p:Right above."], ["h2:Title, continued", "p:Right."]]);
      assertHolds("stacked, the copy first", result);
      assert.equal((await heldPlace(page, ".restack > .padded:first-child", ".restack > :last-child")).visibility, "hidden");
    } finally {
      await page.close();
    }
  });

  test("a block that stacks on the continued slide: an emptied copy that crosses under the column that went on isn't moved alone, two slides", async () => {
    const { pages, result, page } = await slides(`${RESTACKED}<div class="restack"><div>${words("Left above.", 20)}${words("Left words", 460)}</div><div class="padded">${words("Right.", 460)}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left above.", "p:Right."], ["h2:Title, continued", "p:Left words"]]);
      assertHolds("stacked, the copy crossing", result);
      assert.equal((await heldPlace(page, ".restack > .padded:last-child", ".restack > :first-child")).visibility, "hidden");
    } finally {
      await page.close();
    }
  });

  test("a block set in CSS columns on the continued slide: an emptied copy is not the item that stays, two slides", async () => {
    // With nothing else on the slide, the one item that stays is the first that takes room:
    // the column beside the copy stays, crossing, and is reported, as an item taller than a slide is (580).
    const { pages, result, page } = await slides(`${RESTACKED}<div class="restack folded"><div class="padded">${words("Left.", 460)}</div><div>${words("Right above.", 20)}${padded("Right.")}</div></div>`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Left.", "p:Right above."], ["h2:Title, continued", "p:Right."]]);
      assertHolds("in columns", result);
      assert.equal(await page.evaluate(() => parseInt(getComputedStyle(document.querySelectorAll(".page")[1]!.querySelector(".restack")!).columnCount, 10)), 2);
    } finally {
      await page.close();
    }
  });
});

describe("carry-over: a block with words of its own beside its child blocks goes whole (DECISIONS.md 577)", () => {
  const LOOSE = "Loose words in the block itself.";
  const INNER = "Inner block that crosses";
  for (const [name, body, expected] of [
    ["under a paragraph, the words before the inner block", `${words("Above.", 380)}<div class="wrap">${LOOSE} ${words(INNER, 100)}</div>`, ["Title Above.", `${LOOSE} ${INNER}`]],
    ["under a paragraph, the words after the inner block", `${words("Above.", 380)}<div class="wrap">${words(INNER, 100)} ${LOOSE}</div>`, ["Title Above.", `${INNER} ${LOOSE}`]],
    ["a grid's column, the words before the inner block", `${words("Above.", 100)}<div class="cols"><div>${words("Left.", 20)}</div><div class="wrap">${LOOSE} ${words(INNER, 400)}</div></div>`, ["Title Above. Left.", `${LOOSE} ${INNER}`]],
    ["a grid's column, the words after the inner block", `${words("Above.", 100)}<div class="cols"><div>${words("Left.", 20)}</div><div class="wrap">${words(INNER, 400)} ${LOOSE}</div></div>`, ["Title Above. Left.", `${INNER} ${LOOSE}`]],
  ] as const) {
    test(`${name}: every word once, in the order written`, async () => {
      // Looked into, the inner block went on and the block, empty of elements, was removed with its words.
      const { result, page } = await slides(body);
      try {
        assert.deepEqual(await reading(page), expected);
        assertHolds(name, result);
      } finally {
        await page.close();
      }
    });
  }
});

describe("carry-over: the lines of 576 that only a built shape needs (DECISIONS.md 577)", () => {
  test("a table whose rows fit and whose last collapsed border crosses, then a paragraph: the table goes on whole with its head, and no copied head is left over the paragraph", async () => {
    // The copied head is ours, so the table's copy holds only copies and the table moves whole.
    const rows = Array.from({ length: 5 }, (_, i) => `<tr><td style="height:30px;padding:0;border-bottom:9px solid #000">Row${i + 1}</td></tr>`).join("");
    for (const above of [288, 291]) {
      const { result, page } = await slides(`${words("Spacer words.", above)}<table style="border-collapse:collapse;width:100%"><thead><tr><th style="height:30px;padding:0;border-bottom:9px solid #000">Head cell</th></tr></thead><tbody>${rows}</tbody></table>${words("After words close the slide.", 20)}`);
      try {
        assert.deepEqual(await reading(page), ["Title Spacer words.", "Head cell Row1 Row2 Row3 Row4 Row5 After words close the slide."], `above ${above}px`);
        assertHolds("the table", result);
      } finally {
        await page.close();
      }
    }
  });

  test("a block of no height holding a box placed over it that crosses, then a paragraph: the box goes on with what follows", async () => {
    // The anchor has no height, but what it holds does: it is not a thing that takes no room.
    const { pages, result, page } = await slides(`${words("Above.", 450)}<div class="anchor" style="position:relative"><div class="mark" style="position:absolute;top:0;left:0;width:100px;height:60px;background:#000"></div></div>${words("After.", 20)}`);
    try {
      assert.deepEqual(pages, [["h2:Title", "p:Above."], ["h2:Title, continued", "p:After."]]);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelectorAll(".mark").length)), [0, 1]);
      assertHolds("the anchored box", result);
    } finally {
      await page.close();
    }
  });
});

// A copy the carry-over hides holds no words, so the layout check warns of a nearly empty slide
// as it would without it; and a child with no box of its own whose children are shown takes room
// (DECISIONS.md 579).

/**
 * Slide `n`'s "empty section" warnings, and what that slide prints which the layout check counts
 * as its content: words that show, outside its footer, references and copied headings, a node each.
 */
async function emptySection(page: Page, n: number): Promise<{ warned: string[]; printed: string[] }> {
  const warned = (await checkLayout(page, { minPages: 1, maxPages: 10 })).problems.filter((p) => p.page === n && p.check === "empty section").map((p) => p.detail);
  const printed = await page.evaluate((n) => {
    const out: string[] = [];
    const walker = document.createTreeWalker(document.querySelectorAll(".page")[n - 1]!, NodeFilter.SHOW_TEXT);
    for (let t = walker.nextNode(); t; t = walker.nextNode()) {
      const el = t.parentElement!;
      if ((t.textContent ?? "").trim() && !el.closest("[data-chrome], [data-ref], .tick, [data-carried-label]") && getComputedStyle(el).visibility === "visible") out.push((t.textContent ?? "").trim());
    }
    return out;
  }, n);
  return { warned, printed };
}

describe("carry-over: nothing hidden holds words (DECISIONS.md 579)", () => {
  const cell = "padding:0;border-bottom:9px solid #000";
  /** A table whose one row fits and whose last collapsed border crosses, first in a grid. */
  const table = (head: string, th = `height:30px;${cell}`): string =>
    `<table style="border-collapse:collapse;width:100%"><thead><tr><th style="${th}">${head}</th></tr></thead><tbody><tr><td style="height:439px;${cell}">Row</td></tr></tbody></table>`;

  for (const head of ["Kind of storage and share used", "Kind"]) {
    test(`a table first in a grid beside a short note that goes on, its head "${head}": the continued slide is warned as nearly empty, quoting only what it prints`, async () => {
      // The copy of the head was hidden with its words in it: with the long head the slide went
      // unwarned, and with the short one the warning quoted a word it doesn't print.
      const { result, page } = await slides(`<div class="cols">${table(head)}<div>${words("Right above.", 20)}${words("And 2 more.", 480)}</div></div>`);
      try {
        assert.equal(result.pages, 2);
        assertHolds(head, result);
        assert.deepEqual(await reading(page), [`Title ${head} Row Right above.`, "And 2 more."]);
        const { warned, printed } = await emptySection(page, 2);
        assert.deepEqual(printed, ["And 2 more."]);
        assert.deepEqual(warned, [`only "${printed.join(" | ")}"`]);
        const held = await heldPlace(page, ".cols > table:first-child", ".cols > :last-child");
        assert.equal(held.left[1], held.left[0], "the column beside it where it was");
        assert.equal(held.visibility, "hidden");
      } finally {
        await page.close();
      }
    });
  }

  for (const head of ["Quarter on quarter change in pipeline value by team", "Head cell"]) {
    test(`a table first in a grid beside an empty tall box that goes on, its head "${head}": the continued slide is warned as empty`, async () => {
      const { result, page } = await slides(`<div class="cols">${table(head)}<div>${words("Right above.", 20)}<div class="box" style="height:480px;background:#ddd"></div></div></div>`);
      try {
        assert.equal(result.pages, 2);
        assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelectorAll(".box").length)), [0, 1]);
        // The slide holds a box and no words, which the heading-only reading, by words alone,
        // can't tell from nothing: the layout check's warning is what says so.
        assert.deepEqual(result.dropped, []);
        assert.deepEqual(result.doubled, []);
        const { warned, printed } = await emptySection(page, 2);
        assert.deepEqual(printed, []);
        assert.deepEqual(warned, ['only ""']);
      } finally {
        await page.close();
      }
    });
  }

  test("a copied head with no height of its own set keeps the height it had, its words taken out", async () => {
    const { result, page } = await slides(`<div class="cols">${table("Kind of storage and share used", "padding:4px;border-bottom:9px solid #000")}<div>${words("Right above.", 20)}${words("And 2 more.", 480)}</div></div>`);
    try {
      assert.equal(result.pages, 2);
      const heads = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelector(".cols > table > thead")!.getBoundingClientRect().height));
      assert.equal(heads[1], heads[0], "the copy's head as tall as the head it copies");
      assert.equal((await heldPlace(page, ".cols > table:first-child", ".cols > :last-child")).visibility, "hidden");
    } finally {
      await page.close();
    }
  });

  test("a real table head whose rows stay on its slide keeps its words: the table copied from, and a table that went on whole beside the hidden copy", async () => {
    const REAL = `<table class="real" style="border-collapse:collapse;width:100%"><thead><tr><th style="height:30px;padding:0">Real head</th></tr></thead><tbody><tr><td style="height:30px;padding:0">Real row</td></tr></tbody></table>`;
    const { result, page } = await slides(`<div class="cols">${table("Kind of storage and share used")}<div>${words("Right above.", 20)}${words("Middle.", 300)}${words("Moved.", 200)}</div></div>${REAL}`);
    try {
      assert.deepEqual(await reading(page), ["Title Kind of storage and share used Row Right above. Middle.", "Moved. Real head Real row"]);
      assertHolds("the real heads", result);
      assert.equal((await heldPlace(page, ".cols > table:first-child", ".cols > :last-child")).visibility, "hidden");
      // Each slide's heads that show, by their words.
      const shown = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [...p.querySelectorAll("thead")].filter((h) => getComputedStyle(h).visibility === "visible").map((h) => (h.textContent ?? "").trim())));
      assert.deepEqual(shown, [["Kind of storage and share used"], ["Real head"]]);
    } finally {
      await page.close();
    }
  });
});

describe("carry-over: a child that isn't shown takes no room only when nothing inside it is shown (DECISIONS.md 579)", () => {
  test("a grid of three, its third column a display: contents block whose paragraphs cross: a second slide holds the ones that cross, every word once and in order", async () => {
    // With no box of its own, the block was passed over side by side as if it took no room: no
    // slide was added and the paragraphs past the footer were cut off by the slide's own edge.
    const c = Array.from({ length: 14 }, (_, i) => `C${i + 1}.`);
    const { added, pages, result, page } = await slides(`<div class="cols3"><div>${words("A.", 20)}</div><div>${words("B.", 20)}</div><div style="display:contents"><div>${c.map((x) => words(x, 40)).join("")}</div></div></div>`);
    try {
      assert.equal(added, 1);
      assert.deepEqual(pages, [
        ["h2:Title", "p:A.", "p:B.", ...c.slice(0, 11).map((x) => `p:${x}`)],
        ["h2:Title, continued", ...c.slice(11).map((x) => `p:${x}`)],
      ]);
      assert.deepEqual(await reading(page), [`Title A. B. ${c.slice(0, 11).join(" ")}`, c.slice(11).join(" ")]);
      assertHolds("display: contents", result);
    } finally {
      await page.close();
    }
  });

  test("one under another, a display: contents block is passed over and never counted, as on 32159c1: a padded block after it, crossing by its padding alone, stays", async () => {
    // 579 is about side by side and columns; the list of blocks keeps its own rule for a child
    // with no box of its own, which never took it for content above.
    const { added, pages, result, page } = await slides(`<div style="display:contents">${words("Above.", 20)}</div>${padded("Words.", 440)}`);
    try {
      assert.equal(added, 0);
      assert.deepEqual(pages, [["h2:Title", "p:Above.", "p:Words."]]);
      assertHolds("display: contents above", result);
    } finally {
      await page.close();
    }
  });
});

// A hidden copy keeps the box it was measured with: taking a copied head's words out moves
// nothing that is printed and changes the size of nothing (DECISIONS.md 581).

type Box = [number, number, number, number];

/**
 * Carries a shape over and reads every box on its slides, from the slide's edge: each element
 * that is printed, and each copy the carry-over hid. Then it puts back what the slides were
 * judged with, in the same page: each hidden head's words, from the head it copies, and each
 * copy's own style as the block it copies has it, so a hold is taken off. Read again, whatever
 * moved or changed size by more than half a pixel is in `moved`. `restored` is the words put
 * back, so a reading with none put back can't pass for one that holds. `judged` reads the slides
 * as the carry-over leaves them, before anything is put back.
 */
async function held(body: string, judged: (page: Page) => Promise<void> = async () => {}): Promise<{ moved: string[]; restored: string[]; result: Carried; page: Page }> {
  const page = await open(sheet(body));
  await page.evaluate(() => document.querySelectorAll(".page *").forEach((el, i) => el.setAttribute("data-held", String(i))));
  const result = await carry(page);
  await judged(page);
  const read = (): Promise<Record<string, Box>> =>
    page.evaluate(() => {
      const out: Record<string, [number, number, number, number]> = {};
      document.querySelectorAll(".page").forEach((p, n) => {
        const edge = p.getBoundingClientRect();
        const seen = new Map<string, number>();
        for (const el of p.querySelectorAll<HTMLElement>("*")) {
          if (getComputedStyle(el).visibility !== "visible" && el.style.visibility !== "hidden") continue;
          const id = el.getAttribute("data-held") ?? "new";
          const k = seen.get(id) ?? 0;
          seen.set(id, k + 1);
          const r = el.getBoundingClientRect();
          out[`slide ${n + 1} ${el.tagName.toLowerCase()}${el.className ? `.${el.className}` : ""} ${id}${k ? ` (${k + 1})` : ""}${el.style.visibility === "hidden" ? " hidden" : ""}`] = [r.left - edge.left, r.top - edge.top, r.width, r.height];
        }
      });
      return out;
    });
  const out = await read();
  const restored = await page.evaluate(() => {
    const source = (el: Element): Element => document.querySelector(`[data-held="${el.getAttribute("data-held")}"]`)!;
    const hidden = new Set([...document.querySelectorAll<HTMLElement>(".page *")].filter((el) => el.style.visibility === "hidden"));
    const words: string[] = [];
    for (const head of document.querySelectorAll(".page thead")) {
      if (source(head) === head || getComputedStyle(head).visibility === "visible") continue;
      head.innerHTML = source(head).innerHTML;
      words.push((head.textContent ?? "").trim());
    }
    for (const el of document.querySelectorAll<HTMLElement>(".page [data-held]")) {
      const from = source(el);
      if (from === el) continue;
      const style = from.getAttribute("style");
      if (style === null) el.removeAttribute("style");
      else el.setAttribute("style", style);
      if (hidden.has(el)) el.style.visibility = "hidden";
    }
    return words;
  });
  const within = await read();
  const shown = (b: Box | undefined): string => (b ? `${b[0].toFixed(1)},${b[1].toFixed(1)} ${b[2].toFixed(1)}x${b[3].toFixed(1)}` : "none");
  const moved = [...new Set([...Object.keys(within), ...Object.keys(out)])]
    .filter((k) => !within[k] || !out[k] || within[k]!.some((v, i) => Math.abs(v - out[k]![i]!) > 0.5))
    .map((k) => `${k}: ${shown(within[k])} as the slide was judged, ${shown(out[k])} as it is left`);
  return { moved, restored, result, page };
}

/** The box of `selector` on slide `n`, from the slide's edge, to a tenth of a pixel. */
const place = (page: Page, n: number, selector: string): Promise<{ x: number; width: number; height: number }> =>
  page.evaluate(
    ([n, selector]) => {
      const slide = document.querySelectorAll(".page")[n - 1]!;
      const r = slide.querySelector(selector)!.getBoundingClientRect();
      const tenth = (v: number): number => Math.round(v * 10) / 10;
      return { x: tenth(r.left - slide.getBoundingClientRect().left), width: tenth(r.width), height: tenth(r.height) };
    },
    [n, selector] as const,
  );

describe("carry-over: a hidden copy keeps the box it was measured with (DECISIONS.md 581)", () => {
  const LONG_WORD = "Opportunity.Forecast_Category_Name__c.LastModifiedDate";
  const KIND = "Kind of storage and share used";
  const COLLAPSED = "border-collapse:collapse";
  const cell = "padding:0;border-bottom:9px solid #000";
  /** A table whose one row fits and whose last collapsed border crosses: its copy on the continued slide holds only its head. */
  const headed = (head: string, style: string, th = `height:30px;${cell}`): string =>
    `<table style="${style}"><thead><tr><th style="${th}">${head}</th></tr></thead><tbody><tr><td style="height:439px;${cell}">Row</td></tr></tbody></table>`;
  const beside = `<div class="beside">${words("Right above.", 20)}${words("And 2 more.", 480)}</div>`;

  // The adversary's shapes. The figures are the column beside the copy on the continued slide as
  // 6eea6e0 printed it, its copy hidden with the head's words still in, and as the first slide has it.
  for (const [name, head, body, expected] of [
    ["(a1) a grid of 1fr 1fr, the table as wide as its column and its head one word that can't break", LONG_WORD, `<div class="cols">${headed(LONG_WORD, `${COLLAPSED};width:100%`)}${beside}</div>`, { x: 504.5, width: 55.5 }],
    ["(a2) a grid of auto 1fr, the table as wide as its head", KIND, `<div class="cols" style="grid-template-columns:auto 1fr">${headed(KIND, COLLAPSED)}${beside}</div>`, { x: 278.9, width: 281.1 }],
    ["(a3) a grid of max-content 1fr, the table as wide as its head", KIND, `<div class="cols" style="grid-template-columns:max-content 1fr">${headed(KIND, COLLAPSED)}${beside}</div>`, { x: 278.9, width: 281.1 }],
    ["(a4) a flex row, the table as wide as its head", KIND, `<div class="cols" style="display:flex">${headed(KIND, COLLAPSED)}${beside}</div>`, { x: 278.9, width: 83.6 }],
  ] as const) {
    test(`${name}: the column beside the hidden copy is where it was and as wide, and the slide is warned of only what it prints`, async () => {
      // The copy's width came from its head's words: emptied, it shrank after the slide was judged,
      // and the column beside it moved left and widened (on be6e1a9, to x=308.0 or 56.0).
      let left: { beside: { x: number; width: number }; visibility: string; section: { warned: string[]; printed: string[] } } | undefined;
      const { moved, restored, result, page } = await held(body, async (page) => {
        const { x, width } = await place(page, 2, ".beside");
        left = { beside: { x, width }, visibility: (await heldPlace(page, ".cols > table:first-child", ".beside")).visibility, section: await emptySection(page, 2) };
      });
      try {
        assert.equal(result.pages, 2);
        assertHolds(name, result);
        assert.deepEqual(restored, [head]);
        assert.deepEqual(moved, []);
        assert.deepEqual(left!.beside, expected);
        assert.equal(left!.visibility, "hidden");
        assert.deepEqual(left!.section, { printed: ["And 2 more."], warned: [`only "And 2 more."`] });
      } finally {
        await page.close();
      }
    });
  }

  test("(b) a grid of auto 1fr, a chart as wide as its column beside the hidden copy: the chart keeps its size and the slide doesn't run into its footer", async () => {
    // On be6e1a9 the column widened to 504px and the square chart with it, past the footer: a slide
    // that fitted when it was judged no longer did.
    const chart = `<figure style="margin:0"><svg viewBox="0 0 100 100" style="width:100%;height:auto;display:block"><rect width="100" height="100"></rect></svg></figure>`;
    let left: { figure: { x: number; width: number; height: number }; problems: unknown[] } | undefined;
    const { moved, restored, result, page } = await held(`<div class="cols" style="grid-template-columns:auto 1fr">${headed(KIND, COLLAPSED)}<div class="beside">${words("Right above.", 200)}${chart}</div></div>`, async (page) => {
      left = { figure: await place(page, 2, ".beside figure"), problems: (await checkLayout(page, { minPages: 1, maxPages: 10 })).problems.filter((p) => p.page === 2) };
    });
    try {
      assert.equal(result.pages, 2);
      assertHolds("(b)", result);
      assert.deepEqual(restored, [KIND]);
      assert.deepEqual(moved, []);
      assert.deepEqual(left!.figure, { x: 278.9, width: 281.1, height: 281.1 });
      // The slide prints the chart and no words, which is all the layout check says of it.
      assert.deepEqual(left!.problems, [{ page: 2, check: "empty section", detail: 'only ""' }]);
    } finally {
      await page.close();
    }
  });

  test("two hidden copies on one slide, two table columns emptied beside a third column that goes on: both keep their boxes, and so does the column", async () => {
    // As measured on 6eea6e0, and as the first slide has them: the tables 222.9 and 93.6 wide.
    let left: Array<{ x: number; width: number; height: number }> = [];
    const { moved, restored, result, page } = await held(`<div class="cols" style="grid-template-columns:auto auto 1fr">${headed(KIND, COLLAPSED)}${headed("Second head", COLLAPSED)}${beside}</div>`, async (page) => {
      left = [await place(page, 2, ".cols > table:nth-child(1)"), await place(page, 2, ".cols > table:nth-child(2)"), await place(page, 2, ".beside")];
    });
    try {
      assert.equal(result.pages, 2);
      assertHolds("two copies", result);
      assert.deepEqual(restored, [KIND, "Second head"]);
      assert.deepEqual(moved, []);
      assert.deepEqual(left, [
        { x: 40, width: 222.9, height: 34.5 },
        { x: 278.9, width: 93.6, height: 34.5 },
        { x: 388.5, width: 171.5, height: 480 },
      ]);
    } finally {
      await page.close();
    }
  });

  test("two hidden copies on one slide, each table first in a small grid of its own: every copy is measured before any is emptied", async () => {
    // Each column's copy holds its table's copy, and emptying the column empties the table in it:
    // measured after that, the table was held at no width at all.
    const PAIR = `<style>.pair { display: grid; grid-template-columns: auto 1fr; gap: 8px; align-items: start; }</style>`;
    const { moved, restored, result, page } = await held(`${PAIR}<div class="cols3"><div class="pair">${headed("Storage used", COLLAPSED)}${words("Beside.", 20)}</div><div class="pair">${headed("Second head", COLLAPSED)}${words("Beside two.", 20)}</div>${beside}</div>`);
    try {
      assert.equal(result.pages, 2);
      assertHolds("two small grids", result);
      assert.deepEqual(restored, ["Storage used", "Second head"]);
      assert.deepEqual(moved, []);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")[1]!.querySelectorAll<HTMLElement>(".pair, .pair > table")].map((e) => e.style.visibility)), ["hidden", "hidden", "hidden", "hidden"]);
    } finally {
      await page.close();
    }
  });

  // Each part of the hold, and the shape that needs it.
  for (const [name, head, body] of [
    [
      // Stacked on the continued slide, the copy sits above the column that went on.
      "the height: a grid that stacks on the continued slide, the copied head with no height of its own over the column that went on",
      KIND,
      `${RESTACKED}<div class="restack">${headed(KIND, `${COLLAPSED};width:100%`, "padding:4px;border-bottom:9px solid #000")}<div class="beside">${words("Right above.", 200)}${words("And 2 more.", 300)}</div></div>`,
    ],
    [
      "the least width: a flex row whose items shrink to share it",
      KIND,
      `<div class="cols" style="display:flex">${headed(KIND, COLLAPSED)}<div class="beside">${words("Right above.", 20)}${words("And 2 more words, ".repeat(6).trim(), 480)}</div></div>`,
    ],
    ["the most width: a flex row whose items grow to fill it", KIND, `<div class="cols" style="display:flex">${headed(KIND, `${COLLAPSED};flex:1 1 auto`)}<div class="beside" style="flex:1 1 auto">${words("Right above.", 20)}${words("And 2 more.", 480)}</div></div>`],
    [
      "the border box: a padded column sized by its content box, holding a table in a small grid",
      "Storage used",
      `<style>.boxed { display: grid; grid-template-columns: auto 1fr; gap: 8px; align-items: start; padding: 0 10px; background: #eee; box-sizing: content-box; }</style><div class="cols3"><div class="boxed">${headed("Storage used", COLLAPSED)}${words("Beside.", 20)}</div><div>${words("Middle.", 20)}</div>${beside}</div>`,
    ],
  ] as const) {
    test(`the hold: ${name}`, async () => {
      const { moved, restored, result, page } = await held(body);
      try {
        assert.equal(result.pages, 2);
        assertHolds(name, result);
        assert.deepEqual(restored, [head]);
        assert.deepEqual(moved, []);
      } finally {
        await page.close();
      }
    });
  }

  test("only a hidden copy is held: a table cut between its rows, in a grid whose columns share what is left over, lays out on the continued slide as it always did", async () => {
    // Held at its width, the table's copy would take a second share, and the paragraph that went
    // on beside it would move right and narrow.
    const rows = Array.from({ length: 14 }, (_, i) => `<tr><td style="height:40px;padding:0">Row ${i + 1}</td></tr>`).join("");
    const { moved, restored, result, page } = await held(
      `${words("Above.", 20)}<div class="cols" style="grid-template-columns:auto auto"><table style="${COLLAPSED}"><thead><tr><th style="height:30px;padding:0">Head of the carried table</th></tr></thead><tbody>${rows}</tbody></table><p class="beside" style="height:460px">Words beside, which go on whole.</p></div>`,
    );
    try {
      assert.equal(result.pages, 2);
      assertHolds("the carried table", result);
      assert.deepEqual(restored, []);
      assert.deepEqual(moved, []);
      assert.deepEqual(await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => [p.querySelectorAll("tbody > tr").length, p.querySelectorAll(".beside").length])), [[10, 0], [4, 1]]);
      assert.deepEqual(await unpainted(page, ".cols"), []);
    } finally {
      await page.close();
    }
  });

  test("only a copy that loses words is held: a hidden padded column in a grid whose columns share what is left over lays out as it always did", async () => {
    // Nothing is taken out of it, so nothing of it changes; held, it would take a second share.
    const { moved, restored, result, page } = await held(`<div class="cols" style="grid-template-columns:auto auto"><div class="padded">${words("Left.", 460)}</div><div class="beside">${words("Right above.", 20)}${padded("Right.")}</div></div>`);
    try {
      assert.equal(result.pages, 2);
      assertHolds("the padded column", result);
      assert.deepEqual(restored, []);
      assert.deepEqual(moved, []);
      assert.equal((await heldPlace(page, ".cols > .padded:first-child", ".beside")).visibility, "hidden");
    } finally {
      await page.close();
    }
  });
});

// The decks. The synthetic run as the suites build it, and with every issue's title longer
// by a team's name, as a customer's own words make it.

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const issue = output!.findings.find((f) => f.status === "issue")!;
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast can't be trusted.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [issue.id], assessment: "The data supports this." }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
const full: ReportInput = { ...base, intake };
const TEAMS = " for the Enterprise Sales North America and Asia Pacific Partner Channel teams";


/** Every issue's title longer by `extra` characters, as the session 34 probe built it; unsized unless `sized`. */
const lengthened = (extra: number, sized: boolean): BuiltReport =>
  buildReport({ ...base, findings: output!.findings.map((f) => (f.status === "issue" ? { ...f, ...(sized ? {} : { value: { kind: "not_sized" as const, reason: "Synthetic: no figure." } }), title: f.title + TEAMS.repeat(20).slice(0, extra) } : f)) });

describe("the decks: no slide holds only its heading, and every word prints once (DECISIONS.md 574)", () => {
  test("the session 34 deck: every issue unsized and its title 200 characters longer, full report: the quick-wins panel stays under its continued heading", async () => {
    // At 0bb998e: 119 slides built, 200 added (the limit), 185 holding only "Roadmap / Days 1 to 30: Stabilise, continued".
    const result = await carried(fullReportHtml(lengthened(200, false), pseudonyms));
    assertHolds("the session 34 deck", result);
    assert.deepEqual({ added: result.added, pages: result.pages }, { added: 15, pages: 119 + 15 });
  });

  for (const extra of [170, 185, 200, 205, 215, 230]) {
    for (const sized of [false, true]) {
      test(`every issue's title ${extra} characters longer, ${sized ? "sized" : "unsized"}: both documents`, async () => {
        const report = lengthened(extra, sized);
        for (const [name, html] of [["full report", fullReportHtml(report, pseudonyms)], ["executive summary", executiveHtml(report, pseudonyms)]] as const) {
          const result = await carried(html);
          assertHolds(`${name}, ${extra} ${sized ? "sized" : "unsized"}`, result);
          assert.ok(result.added < 200, `${name}: ${result.added} added`);
        }
      });
    }
  }
});

// The decks the other render suites build, copied here so the invariant is held on each.

const FILLER = [
  "Where we could not read a part of the org with the access we were given, we say so and explain why, rather than estimate a figure that would only look precise.",
  "Most of what we could not measure comes down to how little data the org holds, so we cannot yet tell a well-kept org from an unused one, and the same checks are worth running again once the team is using it day to day.",
  "Some areas were outside what we could see: Salesforce only shows when the person carrying out the review last opened each dashboard, which tells us nothing about how the team uses them.",
  "Other checks need information from you. We do not know which field records why a deal was lost, so we cannot tell a blank reason from a missing field, and we have not agreed with you what a healthy pipeline looks like.",
].join(" ");

/** As carry-over.test.ts: every section with a brief, as a verbose draft. */
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

/** As carry-over.test.ts: Claude's drafts at their word targets. */
const VOCABULARY = "pipeline forecast report account manager customer renewal business process records review changes service context visible problem careful planning owners pressure decision priority adoption training measure quality storage options history security profile control clearly matters outcome ordinary regular team deals stage close data level often enough always your with that their more each than when what only most work time".split(" ");
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
function atTarget(report: BuiltReport): BuiltReport {
  for (const key of ["executive_summary", "full_health_check"] as ReportOutputKey[]) {
    const built = report.outputs[key];
    for (const section of built.sections) {
      const brief = report.briefs.get(section.id);
      if (!brief) continue;
      const version = built.versions.find((v) => v.id === section.currentVersionId)!;
      version.body = draftOf(brief.maxWords);
      version.source = "claude_draft";
    }
  }
  return report;
}
const SAMPLES: Array<[string, NonNullable<Parameters<typeof runFromRecording>[0]>]> = [
  ["Sales Cloud", { modules: SALES_CLOUD_MODULES }],
  ["Sales and Service Cloud", { modules: [...SALES_CLOUD_MODULES, ...SERVICE_CLOUD_MODULES] }],
  ["Sales Cloud and CPQ", { modules: [...SALES_CLOUD_MODULES, ...CPQ_MODULES] }],
  ["every module", { modules: ALL_MODULES }],
  ["a set-up review", { scope: "essentials" }],
];

// As pdf.test.ts: the re-check whose progress slide is at its fullest, and past it.
const STORAGE = "tech.storage-use";
const SEGMENT = "pipe.segment-divergence";
const TEAM_TITLE = "Slippage rate for the Enterprise Sales North America team is well above the overall figure";
const longest = [...new Map(output!.findings.filter((f) => f.status === "issue" && f.ruleId !== STORAGE).map((f) => [f.ruleId, f])).values()].sort((a, b) => b.title.length - a.title.length);
const fixed = new Set(longest.filter((_, i) => i % 3 === 0).slice(0, 6).map((f) => f.ruleId));
const added = new Set(longest.filter((_, i) => i % 3 === 1).slice(0, 6).map((f) => f.ruleId));
const unknown = longest.filter((_, i) => i % 3 === 2)[6]!.ruleId;
const ordered = (findings: Finding[]): Finding[] => [...findings].sort((a, b) => (b.status === "issue" ? b.title.length : 0) - (a.status === "issue" ? a.title.length : 0));
const passed = (f: Finding): Finding => ({ ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } });
const limitsAt = (iso: string, extra: Partial<typeof run> = {}) => ({ ...run, ...extra, steps: run.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt: iso } : s)) });
const kinds = (data: [number, number], file: [number, number]) => ({
  DataStorageMBUsed: data[0], DataStorageMBMax: data[1], DataStorageMBShare: Math.round((data[0] / data[1]) * 1000) / 10,
  FileStorageMBUsed: file[0], FileStorageMBMax: file[1], FileStorageMBShare: Math.round((file[0] / file[1]) * 1000) / 10,
});
function recheck(retitle: (f: Finding) => Finding = (f) => f): BuiltReport {
  const input: ReportInput = { ...full, interviews: syntheticInterviews() };
  const then = kinds([812_345, 900_000], [712_345, 800_000]);
  const now = kinds([962_345, 950_000], [862_345, 850_000]);
  const fixedNow = new Set([...fixed, SEGMENT]);
  const first = output!.findings.find((f) => f.ruleId === SEGMENT && f.status === "issue" && f.title === "Win rate for the Sales EU team is well below the overall figure")!;
  const before = ordered(output!.findings.map((f) => (f.ruleId === STORAGE ? { ...f, facts: then } : f === first ? { ...f, title: TEAM_TITLE } : added.has(f.ruleId) && f.status === "issue" ? passed(f) : f)));
  const later = ordered(
    output!.findings.map(retitle).map((f): Finding =>
      f.ruleId === STORAGE ? { ...f, facts: now } : fixedNow.has(f.ruleId) && f.status === "issue" ? passed(f) : f.ruleId === unknown && f.status === "issue" ? { ...f, status: "not_measurable", notMeasurableReason: "Couldn't be read this time.", value: { kind: "not_sized", reason: "Not measured." } } : f,
    ),
  );
  const earlier: ReportInput = { ...input, run: limitsAt("2026-07-14T12:00:00.000Z"), findings: before, scores: scoreDomains(run.modules, before) };
  const report = buildReport({ ...input, run: limitsAt("2026-11-09T12:00:00.000Z", { id: `${run.id}-recheck`, baselineRunId: run.id }), findings: later, scores: scoreDomains(run.modules, later), baseline: earlier });
  return { ...report, progress: { ...report.progress!, fixed: [...report.progress!.fixed].sort((a, b) => Number(b.ruleId === SEGMENT) - Number(a.ruleId === SEGMENT)) } };
}
const customer = recheck();
const longer = (extra: number): BuiltReport => {
  const p = customer.progress!;
  const tail = TEAMS.repeat(Math.ceil(extra / TEAMS.length)).slice(0, extra);
  return { ...customer, progress: { ...p, ...Object.fromEntries((["fixed", "remaining", "added"] as const).map((k) => [k, p[k].map((c) => ({ ...c, title: c.title + tail }))])) } };
};

describe("the other suites' decks: no slide holds only its heading, and every word prints once (DECISIONS.md 574)", () => {
  test("carry-over.test.ts: every worded section several times longer, both documents", async () => {
    for (const [name, make] of [
      ["full report", (r: BuiltReport) => fullReportHtml(r, pseudonyms)],
      ["executive summary", (r: BuiltReport) => executiveHtml(r, pseudonyms)],
    ] as const) {
      const result = await carried(make(verbose(buildReport(full))));
      assertHolds(name, result);
      assert.ok(result.added > 0, `${name}: something was carried`);
    }
  });

  test("pdf.test.ts: the progress slide past its fullest, at the planted length and longer, both documents; and a chapter's lists split", async () => {
    const chapter = buildReport({ ...base, findings: output!.findings.map((f) => (f.status === "issue" && (f.domain === "configuration" || f.domain === "automation") ? { ...f, title: f.title + TEAMS.repeat(3).slice(0, f.domain === "configuration" ? 200 : 160) } : f)) });
    for (const [name, report] of [["past its fullest", customer], ["the planted length", longer(2 * TEAMS.length)], ["300 more", longer(300)], ["a chapter's lists", chapter]] as const) {
      assertHolds(`${name}, full report`, await carried(fullReportHtml(report, pseudonyms)));
      assertHolds(`${name}, executive summary`, await carried(executiveHtml(report, pseudonyms)));
    }
  });

  test("pdf.test.ts: the interviews table whose own box crosses on its continued slide stays there, where it moved on to the limit before", async () => {
    // Every integrations issue 220 characters longer. Its rows fit; half its last row's rule, which
    // a collapsed table's own box takes in, crosses by less than a pixel. At 0bb998e: 318 slides, the
    // table moved on from 197 slides in turn, each left holding only "Your people / What they told us, continued".
    const report = recheck((f) => (f.status === "issue" && f.domain === "integrations" ? { ...f, title: f.title + TEAMS.repeat(3).slice(0, 220) } : f));
    const result = await carried(fullReportHtml(report, pseudonyms));
    assertHolds("the interviews table", result);
    assert.deepEqual({ added: result.added, pages: result.pages }, { added: 4, pages: 122 });
  });

  for (const [name, options] of SAMPLES) {
    test(`the sample "${name}" at its word targets, at every density step: nothing is carried`, async () => {
      const sample = await runFromRecording({ answers: loadAnswers(), ...options });
      const names = new Pseudonymiser(JSON.parse(readFileSync(join(sample.dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
      const scores = (JSON.parse(readFileSync(join(sample.dir, "scores.json"), "utf8")) as unknown[]).map((x) => Score.parse(x));
      const linked = sample.output!.findings.find((f) => f.status === "issue")!;
      const report = atTarget(buildReport({ run: sample.run, calibration: loadCalibration(), findings: sample.output!.findings, metrics: sample.output!.metrics, manualSteps: sample.output!.manualSteps, scores, interviews: syntheticInterviews(), intake: { ...intake, statedProblems: [{ ...intake.statedProblems[0]!, findingIds: [linked.id] }] } }));
      const decks: Array<[string, string]> = [["executive summary", executiveHtml(report, names)], ["full report", fullReportHtml(report, names)]];
      for (let step = 0; step < slideCounts(report, names).length; step++) decks.push([`full report at density step ${step}`, fullReportHtmlAt(report, names, step)]);
      for (const [deck, html] of decks) {
        const result = await carried(html);
        assertHolds(deck, result);
        assert.equal(result.added, 0, deck);
      }
    });
  }
});

// The real builders' grids with content after them, in the decks' own CSS (DECISIONS.md 576).

/**
 * One slide of a real deck, alone, its grid lowered by padding over its columns until the lowest
 * column's own edge, past the last thing printed in it, is all that crosses the limit; or, where
 * a column has no such edge, until it crosses by a hair. Returns the edge it measured.
 */
async function lowered(html: string, grid: string): Promise<{ result: Carried; after: string[]; printed: string; edge: number; lowered: number }> {
  const context = await browser.newContext({ offline: true });
  try {
    const page = await context.newPage();
    await page.setContent(html, { waitUntil: "load" });
    await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
    await page.emulateMedia({ media: "print" });
    const shape = await page.evaluate((grid) => {
      const g = document.querySelector<HTMLElement>(grid)!;
      const slide = g.closest<HTMLElement>(".page")!;
      for (const p of document.querySelectorAll(".page")) if (p !== slide) p.remove();
      // The carry-over's limit: the footer's top or the slide's padding, whichever is higher, and its tolerance of 1.5px.
      const footer = slide.querySelector<HTMLElement>(":scope > [data-chrome]")!;
      const limit = Math.min(footer.getBoundingClientRect().top, slide.getBoundingClientRect().bottom - parseFloat(getComputedStyle(slide).paddingBottom)) + 1.5;
      const cols = [...g.children];
      const outer = Math.max(...cols.map((c) => c.getBoundingClientRect().bottom + parseFloat(getComputedStyle(c).marginBottom)));
      const inner = Math.max(...cols.flatMap((c) => [...c.querySelectorAll("*")].map((d) => d.getBoundingClientRect().bottom)));
      const edge = outer - inner;
      const lowered = limit - outer + (edge > 0 ? edge / 2 : 1);
      g.style.paddingTop = `${lowered}px`;
      const after = [...g.parentElement!.children].filter((e) => g.compareDocumentPosition(e) & Node.DOCUMENT_POSITION_FOLLOWING && !e.hasAttribute("data-chrome"));
      return { edge, lowered, after: after.map((e) => (e.textContent ?? "").replace(/\s+/g, " ").trim()) };
    }, grid);
    const result = await carry(page);
    const printed = await page.evaluate(() => {
      const out: string[] = [];
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      for (let n = walker.nextNode(); n; n = walker.nextNode()) if (n.parentElement?.closest(".page") && !n.parentElement.closest("[data-chrome], [data-carried-label], style")) out.push(n.textContent ?? "");
      return out.join("").replace(/\s+/g, "");
    });
    return { result, printed, ...shape };
  } finally {
    await context.close();
  }
}

/**
 * How often a block's words show, spaces aside: its opening and its close, each counted beyond
 * what the block itself repeats, so a paragraph cut across two slides still counts once.
 */
function times(printed: string, block: string): [number, number] {
  const text = block.replace(/\s+/g, "");
  const count = (s: string, part: string): number => s.split(part).length - 1;
  return [text.slice(0, 30), text.slice(-30)].map((part) => count(printed, part) - count(text, part) + 1) as [number, number];
}

describe("the real builders' grids: every word after the grid is printed once (DECISIONS.md 576)", () => {
  test("the progress slide's lists, a column's list fitting but for its last item's margin: the areas and notes after them print once, on the continued slide", async () => {
    // Lost at 0bb998e and 2bf07e9: the lists' columns left only their shells, the notes went into
    // the block's shell unrecorded, and the slide's carried body was thrown away.
    const { result, after, printed, edge, lowered: by } = await lowered(fullReportHtml(customer, pseudonyms), ".progress > .lists");
    assert.ok(edge > 0 && by >= 0, `the edge ${edge}px, lowered by ${by}px`);
    assert.equal(after.length, 4);
    for (const text of after) assert.deepEqual(times(printed, text), [1, 1], text);
    assert.equal(result.added, 1);
    assertHolds("the progress slide", result);
  });

  test("the money slide's split, its figure's column fitting but for the chart's margin: the notes after it print once", async () => {
    // The table's column has no edge of its own (the total row has no bottom rule), so the chart's
    // column crosses instead; the headline figure stays above the chart, which goes on whole and is
    // recorded, at 0bb998e and 2bf07e9 alike. The shape can't lose its notes; this holds that it doesn't.
    const { result, after, printed, edge, lowered: by } = await lowered(fullReportHtml(buildReport(full), pseudonyms), ".money > .split");
    assert.ok(edge > 0 && by >= 0, `the edge ${edge}px, lowered by ${by}px`);
    assert.ok(after.length > 0);
    for (const text of after) assert.deepEqual(times(printed, text), [1, 1], text);
    assertHolds("the money slide", result);
  });

  test("the wins slide's chart pair, a chart crossing by a hair: the small note after it prints once", async () => {
    // Its columns are the charts themselves, which have no margin or padding and are never looked
    // into, so no column can cross by its own edge or leave a shell: a chart crossing goes on whole.
    const { result, after, printed, edge, lowered: by } = await lowered(fullReportHtml(buildReport(full), pseudonyms), ".chart-pair:has(~ .small-note)");
    assert.ok(edge === 0 && by >= 0, `the edge ${edge}px, lowered by ${by}px`);
    assert.equal(after.length, 1);
    assert.deepEqual(times(printed, after[0]!), [1, 1]);
    assertHolds("the wins slide", result);
  });
});

describe("the real builders: blank text between tags isn't words of a block's own (DECISIONS.md 577)", () => {
  test("the full report's ratings table, written over several lines, pushed down its slide: it is cut between its rows, as any table is", async () => {
    // The one block on the decks with blank text beside its children: the builder writes this
    // table's rows a line each. Taken for words, it would make the table move whole.
    const context = await browser.newContext({ offline: true });
    try {
      const page = await context.newPage();
      await page.setContent(fullReportHtml(buildReport(full), pseudonyms), { waitUntil: "load" });
      await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
      await page.emulateMedia({ media: "print" });
      const built = await page.evaluate(() => {
        const slide = [...document.querySelectorAll<HTMLElement>(".page")].find((p) => p.querySelector(":scope > h2")?.textContent === "What the ratings mean")!;
        for (const p of document.querySelectorAll(".page")) if (p !== slide) p.remove();
        const table = slide.querySelector("table")!;
        const body = table.tBodies[0]!;
        // A spacer above it, so that about half its rows fit.
        const footer = slide.querySelector(":scope > [data-chrome]")!;
        const spacer = document.createElement("div");
        spacer.style.height = `${footer.getBoundingClientRect().top - table.getBoundingClientRect().top - table.getBoundingClientRect().height / 2}px`;
        table.before(spacer);
        return { rows: body.rows.length, blank: [...body.childNodes].some((n) => n.nodeType === Node.TEXT_NODE && !(n.textContent ?? "").trim()) };
      });
      assert.ok(built.blank, "the rows are written a line each");
      const result = await carry(page);
      const rows = await page.evaluate(() => [...document.querySelectorAll(".page")].map((p) => p.querySelectorAll("tbody > tr").length));
      assert.equal(rows.length, 2, JSON.stringify(rows));
      assert.ok(rows.every((n) => n > 0), JSON.stringify(rows));
      assert.equal(rows[0]! + rows[1]!, built.rows);
      assertHolds("the ratings table", result);
    } finally {
      await context.close();
    }
  });
});
