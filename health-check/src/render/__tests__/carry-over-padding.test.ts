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
    return { pages: pages.length, headingOnly, dropped, doubled: fresh.filter((f) => !used.has(f)).map((f) => f.textContent ?? "") };
  });
  return { added, ...facts };
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
