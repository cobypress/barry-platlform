import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../../domains/index.ts";
import { LENGTH_TOLERANCE, wordCount } from "../../narrative/draft.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, currentBody, VALUE_TOTAL, WORD_TARGETS, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { checkNumbers, scopeDataset } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Score, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { EXECUTIVE_MAX_PAGES, executiveHtml, executiveMaxPages, hasBothVoices, introLines, planIntroHeight, RECHECK_EXECUTIVE_MAX_PAGES } from "../executive.ts";
import { deckRangeFor, fullReportHtml, fullReportHtmlAt, slideCounts, threeColumnItemHeight } from "../full-report.ts";
import { estimateLines, paginateColumns, printed, renderContext } from "../html.ts";
import { compactMoney } from "../format.ts";
import { moneyBasisLines, moneyLabelLines, moneyPageColumn, moneyPageRow, restOf } from "../money.ts";
import { launchBrowser, layoutOf } from "../pdf.ts";

// DECISIONS.md 442: a page laid out by estimate is held to measured text. Each
// test prints Claude's sections at their word target and at the most decision
// 440 lets through without a redraft, with the carry-over off, so a page that
// only fits because a continued page caught it fails here.

/**
 * Prose as Claude writes it for these pages, with more long words than most:
 * nearly seven letters a word. On this text Chromium fits the fewest characters
 * a line of any we measured.
 */
const PROSE =
  "Opportunities without a next step sit in the forecast as though they will close, and the managers reviewing them cannot tell which ones still matter. Recommendations from earlier reviews were documented but never prioritised, so validation rules, approval processes and record-triggered automation now overlap across several objects. Administrators spend their time investigating reporting discrepancies rather than improving adoption. Integrations update customer records overnight, overwriting information that salespeople entered during the day. Begin by agreeing consistent stage definitions, closing opportunities that are realistically lost, and giving each territory manager a weekly dashboard of exceptions. Afterwards, consolidate the overlapping automation, retire unused configuration, and document every integration's responsibilities before the next release.";
const WORDS = PROSE.split(" ");
/** Where each sentence starts, so a draft can open at any of them: where its lines break moves with it. */
const OPENINGS = WORDS.flatMap((_, i) => (i === 0 || WORDS[i - 1]!.endsWith(".") ? [i] : []));

/** Exactly n words of the prose from the given word, ending in a full stop, a paragraph for each fifty words or part of fifty. */
function draft(n: number, from = 0): string {
  const words = Array.from({ length: n }, (_, i) => WORDS[(from + i) % WORDS.length]!);
  words[n - 1] = `${words[n - 1]!.replace(/[.,]$/, "")}.`;
  const paragraphs: string[] = [];
  for (let i = 0; i < n; i += 50) paragraphs.push(words.slice(i, i + 50).join(" "));
  return paragraphs.join("\n\n");
}

/** The most words a draft may have and still not be sent back. */
const mostAllowed = (target: number): number => Math.floor(target * LENGTH_TOLERANCE);

/** Sets the draft of each section with a brief whose key passes, as Claude's. */
function drafted(report: BuiltReport, text: (key: string, target: number) => string | null): BuiltReport {
  for (const key of ["executive_summary", "full_health_check"] as ReportOutputKey[]) {
    const built = report.outputs[key];
    for (const section of built.sections) {
      const brief = report.briefs.get(section.id);
      const body = brief ? text(section.key, brief.maxWords) : null;
      if (body === null) continue;
      const version = built.versions.find((v) => v.id === section.currentVersionId)!;
      version.body = body;
      version.source = "claude_draft";
    }
  }
  return report;
}

const PAGE = /(?=<section class="page[ "])/;
const pagesOf = (html: string): string[] => html.replace(/<\/body><\/html>$/, "").split(PAGE).slice(1);
/** A document of only the given pages, under the same styles, so many drafts go through one layout check. */
const documentOf = (html: string, pages: string[]): string => `${html.split(PAGE)[0]}${pages.join("\n")}</body></html>`;
const titleOf = (page: string): string => /<h2>([^<]*)/.exec(page)?.[1] ?? "";
/** A three-column list's pages, not the references' (those are set by count, not estimate). */
const isThreeColumn = (page: string): boolean => page.includes('<div class="three-text">');

const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast can't be trusted.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [], assessment: "The data supports this." }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });

const SAMPLES: Array<[string, NonNullable<Parameters<typeof runFromRecording>[0]>]> = [
  ["Sales Cloud", { modules: SALES_CLOUD_MODULES }],
  ["Sales and Service Cloud", { modules: [...SALES_CLOUD_MODULES, ...SERVICE_CLOUD_MODULES] }],
  ["Sales Cloud and CPQ", { modules: [...SALES_CLOUD_MODULES, ...CPQ_MODULES] }],
  ["every module", { modules: ALL_MODULES }],
  ["a set-up review", { scope: "essentials" }],
];

type Sample = { input: ReportInput; names: Pseudonymiser };
/** As the sample run is: an intake with a linked problem, five interviews and the scores. */
async function sampleOf(options: NonNullable<Parameters<typeof runFromRecording>[0]>): Promise<Sample> {
  const sample = await runFromRecording({ answers: loadAnswers(), ...options });
  const names = new Pseudonymiser(JSON.parse(readFileSync(join(sample.dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const scores = (JSON.parse(readFileSync(join(sample.dir, "scores.json"), "utf8")) as unknown[]).map((x) => Score.parse(x));
  const linked = sample.output!.findings.find((f) => f.status === "issue")!;
  return {
    names,
    input: { run: sample.run, calibration: loadCalibration(), findings: sample.output!.findings, metrics: sample.output!.metrics, manualSteps: sample.output!.manualSteps, scores, interviews: syntheticInterviews(), intake: { ...intake, statedProblems: [{ ...intake.statedProblems[0]!, findingIds: [linked.id] }] } },
  };
}

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

/** Every problem the layout check finds on these pages, each labelled with what was printed there. */
async function problemsOn(html: string, pages: Array<{ page: string; label: string }>, report: BuiltReport): Promise<string[]> {
  if (pages.length === 0) return [];
  const layout = await layoutOf(browser, documentOf(html, pages.map((p) => p.page)), { minPages: 1, maxPages: 9999, dataset: report.dataset });
  return layout.problems.map((p) => `${pages[p.page - 1]?.label ?? "the document"}: ${p.check}, ${p.detail}`);
}

describe("the test's drafts", () => {
  test("are the length they claim, in long words with no figure", () => {
    for (const n of [1, 20, 25, 35, 43, 49, 50, 51, 90, 112]) for (const from of OPENINGS) assert.equal(wordCount(draft(n, from).split("\n\n")), n);
    const letters = PROSE.replace(/[^a-z]/gi, "").length / WORDS.length;
    assert.ok(letters >= 6.5, `${letters} letters a word`);
    assert.doesNotMatch(PROSE, /\d/);
    for (const word of ["Opportunities", "Recommendations", "configuration"]) assert.ok(PROSE.includes(word), word);
    assert.ok(OPENINGS.length >= 6);
  });
});

describe("pages of columns", () => {
  test("fill a column at a time, and a page only when the last column is full", () => {
    const ids = (pages: Array<Array<{ id: number }>>) => pages.map((p) => p.map((x) => x.id));
    const items = (heights: number[]) => heights.map((h, id) => ({ id, h }));
    // Two tall items don't share a column, however much room three columns have between them.
    assert.deepEqual(ids(paginateColumns(items([300, 300, 300, 300]), (x) => x.h, 3, 525)), [[0, 1, 2], [3]]);
    assert.deepEqual(ids(paginateColumns(items(Array(16).fill(100)), (x) => x.h, 3, 525)), [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], [15]]);
    // One taller than a column still gets a column, rather than a page of nothing.
    assert.deepEqual(ids(paginateColumns(items([100, 600, 100]), (x) => x.h, 3, 525)), [[0, 1, 2]]);
    assert.deepEqual(paginateColumns([], () => 1, 3, 525), []);
  });
});

/** Opens a document as printing does, for measuring. */
async function opened(html: string): Promise<Page> {
  const page = await browser.newPage();
  await page.setContent(html, { waitUntil: "load" });
  await page.evaluate(() => document.fonts.ready.then(() => undefined));
  await page.emulateMedia({ media: "print" });
  return page;
}

/** The prose from every word, at every length up to n: where a line breaks, and so how many characters it holds, moves with each. */
const windows = (n: number): string[] => WORDS.flatMap((_, from) => Array.from({ length: n }, (_, i) => draft(i + 1, from).replace(/\n\n/g, " ")));

// The estimates themselves, item by item, against the boxes Chromium draws: the
// pages below fit only as long as no estimate comes in under the real thing.
describe("an estimate never counts a line too few", () => {
  test("a three-column list's item, on every list in the sample and on drafts of every length", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(input);
    const steps = slideCounts(report, names).length;
    // The step with "What we couldn't measure, and what passed", and the densest, between them every list.
    const pages = [steps - 3, steps - 2].flatMap((step) => pagesOf(fullReportHtmlAt(report, names, step)).filter(isThreeColumn));
    const html = fullReportHtmlAt(report, names, steps - 2);
    const page = await opened(documentOf(html, pages));
    try {
      // Each draft as a detail under a one-word title, and each shorter one as a title over a one-word detail,
      // alone in the first list's first column: the line counted for the other part is the line it takes,
      // so a part counted a line short can't hide behind it.
      const drafts = [...windows(60).map((detail) => ({ title: "Pipeline", detail })), ...windows(15).map((title) => ({ title, detail: "Passed." }))];
      const items = await page.evaluate((drafts) => {
        const measure = (p: HTMLElement) => {
          const copy = p.cloneNode(true) as HTMLElement;
          const refs = copy.querySelectorAll("[data-ref]").length;
          const title = copy.querySelector("strong")?.textContent ?? "";
          for (const x of copy.querySelectorAll("strong, [data-ref]")) x.remove();
          return { title, refs, detail: copy.textContent ?? "", height: p.getBoundingClientRect().height + parseFloat(getComputedStyle(p).marginBottom) };
        };
        const printed = [...document.querySelectorAll<HTMLElement>(".three-text .definition")].map(measure);
        const list = document.querySelector<HTMLElement>(".three-text")!;
        const drafted = drafts.map(({ title, detail }) => {
          list.innerHTML = "";
          const p = document.createElement("p");
          p.className = "definition";
          const strong = document.createElement("strong");
          strong.textContent = title;
          p.append(strong, document.createElement("br"), detail);
          list.append(p);
          return measure(p);
        });
        return [...printed, ...drafted];
      }, drafts);
      assert.ok(items.length > drafts.length + 50, `${items.length} items`);
      const under = items.filter((x) => threeColumnItemHeight({ title: x.title, detail: x.detail, ref: Array.from({ length: x.refs }, () => "ref") }) < x.height);
      assert.deepEqual(under.map((x) => `${x.height}px: ${x.title} | ${x.detail.slice(0, 60)}`), []);
    } finally {
      await page.close();
    }
  });

  test("the plan's introduction, on drafts of every length", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const page = await opened(executiveHtml(buildReport(input), names));
    try {
      const drafts = windows(60);
      const heights = await page.evaluate((drafts) => {
        const plan = [...document.querySelectorAll(".page")].find((p) => p.querySelector(".eyebrow")?.textContent === "90-day plan")!;
        const intro = plan.querySelector<HTMLElement>("h2 + p")!;
        return drafts.map((text) => {
          intro.textContent = text;
          return intro.getBoundingClientRect().height + parseFloat(getComputedStyle(intro).marginBottom);
        });
      }, drafts);
      assert.deepEqual(drafts.flatMap((text, i) => (planIntroHeight(text) < heights[i]! ? [`${heights[i]}px: ${text}`] : [])), []);
    } finally {
      await page.close();
    }
  });

  test("the money table's label and basis, on the page and on the slides, on labels of every length to sixteen words (DECISIONS.md 438)", async () => {
    // Each draft set as a line's label, its reference beside it and counted as the tag it
    // prints (DECISIONS.md 496), in the column the page or the slide gives it there; and
    // every basis the samples print, where it prints.
    const drafts = windows(16);
    /** Each draft's lines in the first row of the given table, with the width it was set at, and every basis's. */
    const measure = async (html: string, which: (page: string) => boolean, index: number) => {
      const page = await opened(html);
      try {
        const at = pagesOf(html).map(which).reduce<number[]>((found, yes, i) => (yes ? [...found, i] : found), [])[index]!;
        return await page.evaluate(
          ({ drafts, at }) => {
            const td = document.querySelectorAll<HTMLElement>(".page")[at]!.querySelector<HTMLElement>("table.grid tbody td")!;
            const style = getComputedStyle(td);
            const inner = () => td.clientWidth - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight);
            const basis = td.querySelector<HTMLElement>(".basis");
            const ref = td.querySelector<HTMLElement>(".ref")!;
            const bases = [...document.querySelectorAll<HTMLElement>(".basis")].map((b) => {
              const cell = b.closest("td")!;
              const cs = getComputedStyle(cell);
              return { text: b.textContent ?? "", width: cell.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight), lines: Math.round(b.getBoundingClientRect().height / parseFloat(getComputedStyle(b).lineHeight)) };
            });
            const labels = drafts.map((text) => {
              td.replaceChildren(document.createTextNode(text), ref, ...(basis ? [basis] : []));
              const height = td.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom) - (basis ? basis.getBoundingClientRect().height + 1 : 0);
              return { text, tag: ref.textContent ?? "", width: inner(), lines: Math.round(height / parseFloat(style.lineHeight)) };
            });
            return { labels, bases };
          },
          { drafts, at },
        );
      } finally {
        await page.close();
      }
    };
    const { input, names } = await sampleOf(SAMPLES[2]![1]);
    const isMoney = (p: string) => p.includes(`<div class="eyebrow">What it&#39;s worth</div>`);
    const onPage = await measure(executiveHtml(buildReport(input), names), isMoney, 0);
    const worst = buildReport(withManyLines(input));
    const onWorst = await measure(executiveHtml(worst, names), isMoney, 0);
    const full = fullReportHtml(worst, names);
    const beside = await measure(full, isMoney, 0);
    const continued = await measure(full, isMoney, 1);
    const under = [
      ...[...onPage.labels, ...onWorst.labels].filter((x) => moneyLabelLines(x.text, x.tag, x.width, "page") < x.lines).map((x) => `page, ${x.lines} lines at ${x.width}px: ${x.text}`),
      ...[...beside.labels, ...continued.labels].filter((x) => moneyLabelLines(x.text, x.tag, x.width, "slide") < x.lines).map((x) => `slide, ${x.lines} lines at ${x.width}px: ${x.text}`),
      ...[...onPage.bases, ...onWorst.bases].filter((x) => moneyBasisLines(x.text, x.width) < x.lines).map((x) => `basis, ${x.lines} lines at ${x.width}px: ${x.text}`),
    ];
    assert.ok(onPage.labels.length === drafts.length && onPage.bases.length >= 7 && continued.labels[0]!.width > 2 * beside.labels[0]!.width, "measured on the page, beside the figure and across a continued slide");
    // Labels of more than one line, so the test can't pass on one-line labels alone.
    assert.ok(onPage.labels.some((x) => x.lines >= 2) && beside.labels.some((x) => x.lines >= 3) && onPage.bases.some((x) => x.lines === 2));
    assert.deepEqual(under, []);
  });

  test("the plan's introduction is set a word at a time, so the engine's own is counted on the two lines it takes", async () => {
    // DECISIONS.md 436: counted by characters, its 184 took three lines, and the Sales Cloud plan lost its twelfth action.
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(input);
    const intro = currentBody(report.outputs.executive_summary, "plan_90_day");
    assert.equal(intro.length, 184);
    assert.equal(estimateLines(intro, 85), 3);
    assert.equal(introLines(intro), 2);
    const html = executiveHtml(report, names);
    const page = await opened(html);
    try {
      const height = await page.evaluate(() => {
        const plan = [...document.querySelectorAll(".page")].find((p) => p.querySelector(".eyebrow")?.textContent === "90-day plan")!;
        return plan.querySelector<HTMLElement>("h2 + p")!.getBoundingClientRect().height;
      });
      assert.equal(Math.round(height / 20.25), 2);
    } finally {
      await page.close();
    }
    const actions = html.split('<section class="phase').slice(1).reduce((sum, block) => sum + (block.split("</section>")[0]!.match(/<li>/g) ?? []).length, 0);
    assert.equal(actions, 12);
    // A word that doesn't fit starts the next line, however few characters the line has.
    assert.equal(introLines("mmmm ".repeat(10).trim()), 1);
    assert.equal(introLines("mmmm ".repeat(11).trim()), 2);
    // And narrow letters fit more than a character count allows.
    assert.equal(introLines("i ".repeat(80).trim()), 1);
  });

  test("the plan's introduction is measured as it prints, where our words for the engine's are longer and an entity is one character", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const t = renderContext(names, loadCalibration().currency, false).t;
    // Each entity is the one character it prints, at that character's width.
    assert.equal(printed(t(`It's "a" & <b>.`)), `It's "a" & <b>.`);
    // A space no line breaks at stays inside its word.
    assert.equal(printed("90&#160;days").split(/\s+/).length, 1);
    /** The plan with the engine's introduction changed: its lines in Chromium as stored and as printed, and the actions it shows. */
    const planWith = async (from: string, to: string) => {
      const report = buildReport(input);
      const built = report.outputs.executive_summary;
      const version = built.versions.find((v) => v.id === built.sections.find((s) => s.key === "plan_90_day")!.currentVersionId)!;
      const stored = version.body.replace(from, to);
      assert.notEqual(stored, version.body);
      version.body = stored;
      const html = executiveHtml(report, names);
      const page = await opened(html);
      try {
        const lines = await page.evaluate((stored) => {
          const plan = [...document.querySelectorAll(".page")].find((p) => p.querySelector(".eyebrow")?.textContent === "90-day plan")!;
          const intro = plan.querySelector<HTMLElement>("h2 + p")!;
          const asPrinted = intro.getBoundingClientRect().height;
          intro.textContent = stored;
          return [intro.getBoundingClientRect().height, asPrinted].map((h) => Math.round(h / 20.25));
        }, stored);
        const actions = html.split('<section class="phase').slice(1).reduce((sum, block) => sum + (block.split("</section>")[0]!.match(/<li>/g) ?? []).length, 0);
        return { stored, shown: printed(t(stored)), lines, actions };
      } finally {
        await page.close();
      }
    };
    // "The calibration" prints as "What we agreed with you": two lines as stored,
    // three as printed, and the third costs the plan the action the engine's own
    // introduction leaves room for.
    const longer = await planWith("The full report has", "The calibration has");
    assert.match(longer.shown, /What we agreed with you has/);
    assert.deepEqual([introLines(longer.stored), introLines(longer.shown), ...longer.lines, longer.actions], [2, 3, 2, 3, 11]);
    // An apostrophe is written &#39; on the page, five characters that print as one:
    // counted as written it would take a third line, and an action, it doesn't.
    const apostrophe = await planWith("Urgent fixes come first", "Urgent fixes don't wait");
    assert.match(t(apostrophe.stored), /don&#39;t/);
    assert.deepEqual([introLines(t(apostrophe.stored)), introLines(apostrophe.shown), ...apostrophe.lines, apostrophe.actions], [3, 2, 2, 2, 12]);
  });
});

// The full report's three-column lists: "Each area at a glance" holds each area's
// text and the pipeline's; "After 90 days", the metric definitions and "What we
// couldn't measure, and what passed" hold the engine's text, but share the
// estimate, and print with every section at its length around them.
const THREE_COLUMN_LISTS = ["Each area at a glance", "After 90 days", "Metric definitions", "What we couldn&#39;t measure, and what passed"];

describe("a three-column list fits its columns at every density step that sets one", () => {
  for (const [name, options] of SAMPLES) {
    test(name, async () => {
      const { input, names } = await sampleOf(options);
      const seen = new Set<string>();
      const problems: string[] = [];
      for (const [length, words] of [["at its target", (target: number) => target], ["at the most allowed", mostAllowed]] as const) {
        // Opening the drafts at different sentences moves where their lines break.
        for (const from of [OPENINGS[0]!, OPENINGS[3]!]) {
          const report = drafted(buildReport(input), (_, target) => draft(words(target), from));
          const steps = slideCounts(report, names).length;
          const pages: Array<{ page: string; label: string }> = [];
          let html = "";
          for (let step = 0; step < steps; step++) {
            html = fullReportHtmlAt(report, names, step);
            for (const page of pagesOf(html).filter(isThreeColumn)) {
              const title = titleOf(page).replace(/, continued$/, "");
              seen.add(title);
              pages.push({ page, label: `${title}, step ${step}, ${length}, from word ${from}` });
            }
          }
          problems.push(...(await problemsOn(html, pages, report)));
        }
      }
      assert.deepEqual(problems, []);
      // Each list this sample can set was set, so the test can't pass on nothing.
      for (const title of THREE_COLUMN_LISTS) assert.ok(seen.has(title), `${title} at some step`);
      assert.deepEqual([...seen].filter((t) => !THREE_COLUMN_LISTS.includes(t)), []);
    });
  }

  test("the area text is checked at every length up to the most allowed, not only at the target", async () => {
    // Lane B found area text of 61 to 70 words overflowing (10 November): which
    // lengths overflow depends on how the items fall into the columns.
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(input);
    const step = slideCounts(report, names).length - 2;
    const pages: Array<{ page: string; label: string }> = [];
    let html = "";
    for (let n = Math.floor(WORD_TARGETS.domain / 2); n <= mostAllowed(WORD_TARGETS.domain); n++) {
      drafted(report, (key) => (key.startsWith("domain_") || key === "pipeline_deep_dive" ? draft(n) : null));
      html = fullReportHtmlAt(report, names, step);
      pages.push(...pagesOf(html).filter((p) => isThreeColumn(p) && titleOf(p).startsWith("Each area at a glance")).map((page) => ({ page, label: `${n} words` })));
    }
    assert.ok(pages.length > 0, "the densest step sets the areas in three columns");
    assert.deepEqual(await problemsOn(html, pages, report), []);
  });
});

describe("the 90-day plan's introduction fits with the plan below it", () => {
  for (const [name, options] of SAMPLES) {
    test(name, async () => {
      // Every length up to the most allowed, from each sentence: lane B's 13-word
      // introduction overflowed in three samples (10 November), below its target.
      const { input, names } = await sampleOf(options);
      const report = buildReport(input);
      const pages: Array<{ page: string; label: string }> = [];
      let html = "";
      for (const from of OPENINGS) {
        for (let n = 1; n <= mostAllowed(WORD_TARGETS.plan_90_day); n++) {
          drafted(report, (key) => (key === "plan_90_day" ? draft(n, from) : null));
          html = executiveHtml(report, names);
          pages.push(...pagesOf(html).filter((p) => p.includes('<div class="eyebrow">90-day plan</div>')).map((page) => ({ page, label: `${n} words from word ${from}` })));
        }
      }
      assert.ok(pages.length >= OPENINGS.length * mostAllowed(WORD_TARGETS.plan_90_day));
      assert.deepEqual(await problemsOn(html, pages, report), []);
    });
  }
});

// The executive summary's Verdict page, with both a survey and interviews.
const sha = (text: string): string => createHash("sha256").update(text).digest("hex").slice(0, 16);
const verdictPage = (html: string): string => pagesOf(html).find((p) => p.includes('<div class="eyebrow">Verdict</div>'))!;
const footers = (html: string): string[] => pagesOf(html).map((p) => /<div class="footer" data-chrome><span>[^<]*<\/span><span>(\d+)<\/span><\/div>/.exec(p)?.[1] ?? "cover");

describe("with both a survey and interviews, what people say takes the page after the Verdict", () => {
  for (const [name, options] of SAMPLES) {
    test(name, async () => {
      const { input, names } = await sampleOf(options);
      const report = drafted(buildReport({ ...input, survey: syntheticSurvey(12) }), (_, target) => draft(target));
      const html = executiveHtml(report, names);
      const pages = pagesOf(html);
      const at = pages.findIndex((p) => p.includes('<div class="eyebrow">Verdict</div>'));
      const verdict = pages[at]!;
      const voices = pages[at + 1]!;
      assert.match(voices, /^<section class="page[^"]*"[^>]*><div class="eyebrow">Survey and interviews<\/div><h2>What your people say<\/h2><h3 class="sub">What your team says/);
      assert.match(voices, /<h3 class="sub">What your people told us<\/h3>/);
      assert.doesNotMatch(verdict, /What your team says|What your people told us/);
      // The sources line and the line on what we couldn't measure stay with the verdict's figures.
      assert.match(verdict, /Every figure here comes from your Salesforce org, or from your team's answers to our survey\. Where a figure is what someone told us in an interview/);
      assert.match(verdict, /Not measurable: |couldn't measure/);
      // Numbered in order, the new page included.
      assert.deepEqual(footers(html), ["cover", ...pages.slice(1).map((_, i) => String(i + 2))]);
      // Every check on the whole summary, the number check on both pages among them, with nothing carried over.
      const layout = await layoutOf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
      assert.deepEqual(layout.problems, [], JSON.stringify(layout.problems.slice(0, 8)));
    });
  }

  // The voices' page is one more, so the ceiling rises by one, as a re-check's does
  // for its "since last time" page. Without it, a run that also has the
  // investment page left the plan no page and still printed one, past the ceiling.
  for (const [name, options] of [SAMPLES[2]!, SAMPLES[3]!]) {
    test(`${name}, with the investment page too, stays within its ceiling`, async () => {
      const { input, names } = await sampleOf(options);
      const report = buildReport({ ...input, intake: { ...input.intake!, salesforceSpendAnnual: 250_000 }, survey: syntheticSurvey(12) });
      const html = executiveHtml(report, names);
      assert.ok(html.includes('<div class="eyebrow">Your investment</div>'), "the investment page");
      assert.equal(executiveMaxPages(report), EXECUTIVE_MAX_PAGES + 1);
      const layout = await layoutOf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
      assert.deepEqual(layout.problems, []);
      assert.ok(layout.pages <= executiveMaxPages(report), `${layout.pages} pages`);
    });
  }

  test("the ceiling rises only with both voices, on a first check and a re-check", async () => {
    const { input } = await sampleOf(SAMPLES[0]![1]);
    const survey = syntheticSurvey(12);
    const fixed = new Set(input.findings.filter((f) => f.status === "issue").map((f) => f.ruleId).slice(0, 3));
    const later = input.findings.map((f): Finding => (fixed.has(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
    const recheck = (extra: Partial<ReportInput>) => buildReport({ ...input, ...extra, run: { ...input.run, id: `${input.run.id}-recheck`, baselineRunId: input.run.id }, findings: later, baseline: input });
    const cases: Array<[string, Partial<ReportInput>, boolean]> = [["neither", { interviews: undefined }, false], ["a survey", { interviews: undefined, survey }, false], ["interviews", {}, false], ["both", { survey }, true]];
    for (const [label, extra, both] of cases) {
      const first = buildReport({ ...input, ...extra });
      const again = recheck(extra);
      assert.ok(again.progress, label);
      assert.equal(hasBothVoices(first), both, label);
      assert.equal(executiveMaxPages(first), EXECUTIVE_MAX_PAGES + (both ? 1 : 0), label);
      assert.equal(executiveMaxPages(again), RECHECK_EXECUTIVE_MAX_PAGES + (both ? 1 : 0), label);
    }
  });

  test("a thin org's line stays on the Verdict page", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const thin = new Set(["automation", "security", "integrations", "forecast", "data_quality"]);
    const findings = input.findings.map((f): Finding => (f.status === "issue" && thin.has(f.domain) ? { ...f, status: "not_measurable", notMeasurableReason: "Fewer than 30 records in the window, so the share can't be read" } : f));
    const report = buildReport({ ...input, findings, survey: syntheticSurvey(12) });
    const html = executiveHtml(report, names);
    assert.match(verdictPage(html), /says less than a full report would/);
    assert.match(pagesOf(html)[pagesOf(html).indexOf(verdictPage(html)) + 1]!, /What your people say/);
  });

  test("with one or neither, the Verdict page is as it was, byte for byte", async () => {
    // Hashes of the Verdict page before decision 442, on the Sales Cloud sample. With
    // interviews alone, each theme became its own spaced paragraph, the heading above
    // them giving the gap back (DECISIONS.md 437).
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const survey = syntheticSurvey(12);
    const cases = { neither: { interviews: undefined }, survey: { interviews: undefined, survey }, interviews: {} };
    const verdicts = Object.fromEntries(
      Object.entries(cases).map(([key, extra]) => {
        const html = executiveHtml(buildReport({ ...input, ...extra }), names);
        assert.ok(!html.includes("What your people say"), key);
        return [key, verdictPage(html)];
      }),
    );
    // Each case prints what it's named for, so the hashes hold the voice beside the verdict.
    assert.match(verdicts.survey!, /<h3 class="sub">What your team says/);
    assert.match(verdicts.interviews!, /<h3 class="sub">What your people told us<\/h3>/);
    assert.deepEqual(Object.fromEntries(Object.entries(verdicts).map(([key, page]) => [key, sha(page)])), { neither: "3e24b50f30626758", survey: "d6592a20fda36044", interviews: "fc49259b482d76bf" });
  });
});

// DECISIONS.md 435: the money page ("What it's worth") is held to its page by
// measure. A survey can join the time lines into one that's the largest of
// several, whose basis takes a second line, and a figure for what the customer
// pays moves the seats off this page and draws the chart taller.
const moneyPageOf = (html: string): string | undefined => pagesOf(html).find((p) => p.includes(`<div class="eyebrow">What it&#39;s worth</div>`));
const MONEY_CASES: Array<[string, (input: ReportInput) => Partial<ReportInput>]> = [
  ["no survey, no spend", () => ({})],
  ["no survey, with spend", (input) => ({ intake: { ...input.intake!, salesforceSpendAnnual: 250_000 } })],
  ["a survey, no spend", () => ({ survey: syntheticSurvey(12) })],
  ["a survey, with spend", (input) => ({ intake: { ...input.intake!, salesforceSpendAnnual: 250_000 }, survey: syntheticSurvey(12) })],
];

/**
 * The sample with nine lines of money of its own, each a value group labelled
 * with the finding's title, at every length to sixteen words, and a survey and
 * a figure for spend, so the page can't hold them all.
 */
function withManyLines(input: ReportInput): ReportInput {
  const template = input.findings.find((f) => f.status === "issue")!;
  const extra = Array.from({ length: 9 }, (_, i): Finding => ({
    ...template,
    id: `f-${String(900 + i).padStart(12, "0")}`,
    ruleId: `synthetic.money_${i}`,
    title: draft(8 + i, OPENINGS[i % OPENINGS.length]!).replace(/\.$/, ""),
    severity: "low",
    value: { kind: "licence_cost", amount: 31_000 + 4_700 * i, unit: "GBP", period: "per year", assumptions: ["A synthetic line for the money page."] },
  }));
  return { ...input, findings: [...input.findings, ...extra], intake: { ...input.intake!, salesforceSpendAnnual: 250_000 }, survey: syntheticSurvey(12) };
}

/**
 * The sample with `n` lines of money of its own, each labelled with `words` words
 * of the prose from a different place, the amount of each from its place.
 */
function withMoneyLines(input: ReportInput, n: number, words: number, amount = (i: number) => 31_000 + 4_700 * i): ReportInput {
  const template = input.findings.find((f) => f.status === "issue")!;
  const extra = Array.from({ length: n }, (_, i): Finding => ({
    ...template,
    id: `f-${String(900 + i).padStart(12, "0")}`,
    ruleId: `synthetic.money_${i}`,
    title: draft(words, i * 3).replace(/\.$/, ""),
    severity: "low",
    value: { kind: "licence_cost", amount: amount(i), unit: "GBP", period: "per year", assumptions: ["A synthetic line for the money page."] },
  }));
  return { ...input, findings: [...input.findings, ...extra] };
}
/** Every line of money in the table, largest first. */
const moneyOf = (report: BuiltReport) => report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);

describe("the money page fits its page, with a survey or without and a figure for spend or without", () => {
  for (const [name, options] of SAMPLES) {
    test(name, async () => {
      const { input, names } = await sampleOf(options);
      const problems: string[] = [];
      for (const [label, extra] of MONEY_CASES) {
        const report = buildReport({ ...input, ...extra(input) });
        const html = executiveHtml(report, names);
        assert.ok(moneyPageOf(html), `${label}: the money page`);
        // The whole summary, every check, with nothing carried over.
        const layout = await layoutOf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
        problems.push(...layout.problems.map((p) => `${label}, page ${p.page}: ${p.check}, ${p.detail}`));
      }
      assert.deepEqual(problems, []);
    });
  }

  test("the chart takes its shorter height before the table names a line fewer, and only when it must", async () => {
    const chartOf = (page: string) => /viewBox="0 0 800 (\d+)"/.exec(page)?.[1];
    const namedOn = (page: string) => [...page.matchAll(/<span class="basis">/g)].length;
    // Every module, with a survey and spend: the tall chart overflowed by 13px. The shorter one makes room for every line.
    const every = await sampleOf(SAMPLES[3]![1]);
    const full = buildReport({ ...every.input, ...MONEY_CASES[3]![1](every.input) });
    const crowded = moneyPageOf(executiveHtml(full, every.names))!;
    assert.equal(chartOf(crowded), "250");
    assert.equal(namedOn(crowded), full.value.groups.filter((g) => g.kind !== "exposure").length);
    assert.doesNotMatch(crowded, /smaller line/);
    // Sales Cloud with spend has the room, so the chart keeps its height.
    const sales = await sampleOf(SAMPLES[0]![1]);
    const roomy = moneyPageOf(executiveHtml(buildReport({ ...sales.input, ...MONEY_CASES[1]![1](sales.input) }), sales.names))!;
    assert.equal(chartOf(roomy), "320");
  });

  test("every module and CPQ without spend name all their lines, and the last row never stands for one line the page could hold (DECISIONS.md 438)", async () => {
    const namedOn = (page: string) => [...page.matchAll(/<span class="basis">/g)].length;
    const lastRow = (page: string) => /<tr><td>(And \d+ smaller lines?), in the raw notes\./.exec(page)?.[1] ?? null;
    for (const [name, options] of [SAMPLES[2]!, SAMPLES[3]!]) {
      const { input, names } = await sampleOf(options);
      // Without a survey: session 3 named seven and said "And 1 smaller line" with the eighth's room to spare.
      const report = buildReport(input);
      const all = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
      assert.equal(all.length, 8, name);
      const html = executiveHtml(report, names);
      const page = moneyPageOf(html)!;
      assert.equal(namedOn(page), 8, name);
      assert.equal(lastRow(page), null, name);
      // With a survey, the eighth's basis takes a second line and the page can't hold it.
      // Its label fits the row, but the row is shorter than the line with its basis, so
      // it stands for the eighth alone and the page names the seven it holds.
      const surveyed = buildReport({ ...input, survey: syntheticSurvey(12) });
      const crowded = moneyPageOf(executiveHtml(surveyed, names))!;
      assert.deepEqual([namedOn(crowded), lastRow(crowded)], [7, "And 1 smaller line"], name);
      // Where the eighth's label would take more room than the row's own words, the row stands for it alone too.
      const ordered = surveyed.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
      ordered[7]!.label = draft(16, OPENINGS[1]!).replace(/\.$/, "");
      const longer = executiveHtml(surveyed, names);
      assert.deepEqual([namedOn(moneyPageOf(longer)!), lastRow(moneyPageOf(longer)!)], [7, "And 1 smaller line"], name);
      // Each against its own report's figures, with nothing carried over.
      assert.deepEqual(await problemsOn(html, [{ page, label: `${name}, no survey` }], report), []);
      assert.deepEqual(await problemsOn(longer, [{ page: crowded, label: `${name}, a survey` }, { page: moneyPageOf(longer)!, label: `${name}, a survey and a long eighth label` }], surveyed), []);
    }
  });

  test("with a survey and no spend, every module's and CPQ's eighth line, set in the last row's place, runs past the page: the row is right to stand for it (DECISIONS.md 448)", async () => {
    // Measured: 25.8px under the notes and a last row of 31.75px, against the eighth
    // line's 58.09px with its basis on two lines. It would run 0.56px into the margin.
    for (const [name, options] of [SAMPLES[2]!, SAMPLES[3]!]) {
      const { input, names } = await sampleOf(options);
      const report = buildReport({ ...input, ...MONEY_CASES[2]![1](input) });
      const html = executiveHtml(report, names);
      const money = moneyPageOf(html)!;
      assert.match(money, /<tr><td>And 1 smaller line, in the raw notes\./, name);
      const eighth = moneyPageRow(renderContext(names, report.value.currency, false), report, 7);
      const page = await opened(documentOf(html, [money]));
      try {
        const m = await page.evaluate((eighth) => {
          const sheet = document.querySelector<HTMLElement>(".page")!;
          const room = sheet.getBoundingClientRect().bottom - parseFloat(getComputedStyle(sheet).paddingBottom);
          const bottom = () => Math.max(...[...sheet.querySelectorAll<HTMLElement>(".money *")].map((x) => x.getBoundingClientRect().bottom));
          const rest = [...sheet.querySelectorAll<HTMLElement>("table.grid tbody tr")].at(-2)!;
          const free = room - bottom();
          const last = rest.getBoundingClientRect().height;
          // The line in the row's place, with the reference the row gave it: its own.
          const ref = rest.querySelector(".ref")!;
          rest.insertAdjacentHTML("afterend", eighth);
          const line = rest.nextElementSibling as HTMLElement;
          rest.remove();
          line.querySelector("td")!.insertBefore(ref, line.querySelector(".basis"));
          return { free, last, line: line.getBoundingClientRect().height, past: bottom() - room };
        }, eighth);
        assert.ok(m.free >= 0 && m.line > m.last + m.free && m.past > 0, `${name}: ${JSON.stringify(m)}`);
      } finally {
        await page.close();
      }
    }
  });

  test("with more lines than the page holds, on long labels, it names fewer and adds up the rest", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(withManyLines(input));
    const html = executiveHtml(report, names);
    const page = moneyPageOf(html)!;
    const all = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    assert.ok(all.length >= 15, `${all.length} lines`);
    assert.deepEqual(await problemsOn(html, [{ page, label: "the money page" }], report), []);
    // The chart took its shorter height, and the table named no more than the most it
    // can. Set in the label column the browser gives them, the most now fit (DECISIONS.md 438).
    assert.match(page, /viewBox="0 0 800 250"/);
    const named = [...page.matchAll(/<span class="basis">/g)].length;
    assert.ok(named >= 1 && named <= 7, `${named} lines named`);
    // The largest, in order, then the rest in one row, and the rows add up to the total beneath them.
    const m = (n: number) => compactMoney(n, report.value.currency);
    const rows = /<tbody>([\s\S]*)<\/tbody>/.exec(page)![1]!.split("</tr>").filter(Boolean);
    assert.equal(rows.length, named + 2);
    all.slice(0, named).forEach((g, i) => assert.ok(rows[i]!.includes(`<td class="num">${m(g.counted.expected)}</td>`), `${g.label}: ${rows[i]}`));
    const rest = rows[named]!;
    assert.match(rest, new RegExp(`And ${all.length - named} smaller lines, in the raw notes\\.`));
    assert.ok(rest.includes(`<td class="num">${m(restOf(all, named, "expected"))}</td><td class="num">${m(restOf(all, named, "low"))} to ${m(restOf(all, named, "high"))}</td>`), rest);
    const { total } = report.value;
    assert.ok(rows[named + 1]!.includes(`<td class="num">${m(total.expected)}</td><td class="num">${m(total.low)} to ${m(total.high)}</td>`), rows[named + 1]);
    for (const k of ["low", "expected", "high"] as const) {
      const named_ = all.slice(0, named).reduce((sum, g) => sum + g.counted[k], 0);
      assert.ok(Math.abs(named_ + restOf(all, named, k) - total[k]) < 0.01 * all.length, `${k}: ${named_} and ${restOf(all, named, k)} make ${total[k]}`);
    }
  });

  test("the count in its last row is a figure from the data at every cut the page can make", async () => {
    // The estimate can name any number of lines from one up, so the count it leaves
    // is held for every cut, not only the slide's or the one this sample makes.
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(withManyLines(input));
    const n = report.value.groups.filter((g) => g.kind !== "exposure").length;
    const page = moneyPageOf(executiveHtml(report, names))!;
    const label = /<tr><td>(And \d+ smaller lines, in the raw notes\.)/.exec(page)![1]!;
    assert.ok(n >= 15, `${n} lines`);
    const counts = Array.from({ length: n - 1 }, (_, i) => n - 1 - i).map((c) => label.replace(/\d+ smaller lines/, c === 1 ? "1 smaller line" : `${c} smaller lines`));
    // Against the data the layout check reads, and against the money's own figures
    // alone: on this sample every count from 1 to 15 is some other figure too, so
    // only the second shows the count is held for itself.
    const money = scopeDataset(report.dataset, [VALUE_TOTAL]);
    const untraced = counts.flatMap((text) => [report.dataset, money].flatMap((set) => checkNumbers(text, set).unmatched.map((u) => `${text} ${u.raw}: ${u.reason}`)));
    assert.deepEqual(untraced, []);
  });
  test("its label column is the browser's, never wider and within 2px, on every sample and on tables of 9, 17 and 40 lines (DECISIONS.md 438)", async () => {
    // The first build allowed for every cut's last row and counted the figures too wide,
    // so the column was 18px narrower than the browser's and named a line fewer.
    const off: string[] = [];
    let compared = 0;
    for (const [name, options] of SAMPLES) {
      const { input, names } = await sampleOf(options);
      const have = moneyOf(buildReport(input)).length;
      const longer = name === SAMPLES[0]![0] || name === SAMPLES[2]![0] ? [9, 17, 40] : [];
      const tables: Array<[string, ReportInput]> = [[name, input], ...longer.map((n): [string, ReportInput] => [`${name}, ${n} lines`, withMoneyLines(input, n - have, 16)])];
      for (const [label, table] of tables)
        for (const [kind, extra] of MONEY_CASES) {
          const report = buildReport({ ...table, ...extra(table) });
          const html = executiveHtml(report, names);
          const page = await opened(documentOf(html, [moneyPageOf(html)!]));
          try {
            const { column, crowded } = await page.evaluate(() => {
              const table = document.querySelector<HTMLElement>(".money table.grid")!;
              const td = table.querySelector<HTMLElement>("tbody td")!;
              const style = getComputedStyle(td);
              // Where every label and basis fits its line, the browser shares out the room
              // left over between the columns, and the label column decides no line.
              const wide = table.cloneNode(true) as HTMLElement;
              wide.style.width = "max-content";
              table.after(wide);
              const crowded = wide.getBoundingClientRect().width > table.getBoundingClientRect().width;
              wide.remove();
              return { column: td.getBoundingClientRect().width - parseFloat(style.paddingLeft) - parseFloat(style.paddingRight), crowded };
            });
            if (!crowded) continue;
            compared += 1;
            const estimate = moneyPageColumn(renderContext(names, report.value.currency, false), report);
            if (estimate > column || column - estimate > 2) off.push(`${label}, ${kind}: ${estimate.toFixed(1)}px against the browser's ${column.toFixed(1)}px`);
          } finally {
            await page.close();
          }
        }
    }
    assert.deepEqual(off, []);
    // Only the set-up review without a survey has labels short enough that none wraps.
    assert.ok(compared >= 42, `${compared} compared`);
  });

  test("it names at least as many lines as it did before decision 438, on every sample with 0, 1, 2 and 9 lines more, in every case, and every check passes", async () => {
    // Lines the page named at 589049f, with these inputs: counted by building the base
    // commit's executive summary (`git archive 589049f`) from the same samples, extra
    // lines and cases, in order no survey and no spend, spend, a survey, both.
    const BEFORE: Array<[string, number, number, number[]]> = [
      [SAMPLES[0]![0], 0, 0, [7, 7, 7, 7]], [SAMPLES[0]![0], 1, 4, [7, 8, 6, 8]], [SAMPLES[0]![0], 1, 16, [6, 8, 6, 7]], [SAMPLES[0]![0], 2, 4, [7, 7, 7, 7]], [SAMPLES[0]![0], 2, 16, [6, 7, 6, 7]], [SAMPLES[0]![0], 9, 4, [7, 7, 6, 7]], [SAMPLES[0]![0], 9, 16, [6, 6, 6, 6]],
      [SAMPLES[1]![0], 0, 0, [7, 7, 7, 7]], [SAMPLES[1]![0], 1, 4, [7, 8, 6, 8]], [SAMPLES[1]![0], 1, 16, [6, 8, 6, 7]], [SAMPLES[1]![0], 2, 4, [7, 7, 7, 7]], [SAMPLES[1]![0], 2, 16, [6, 7, 6, 7]], [SAMPLES[1]![0], 9, 4, [7, 7, 6, 7]], [SAMPLES[1]![0], 9, 16, [6, 6, 6, 6]],
      [SAMPLES[2]![0], 0, 0, [7, 8, 7, 8]], [SAMPLES[2]![0], 1, 4, [7, 7, 7, 7]], [SAMPLES[2]![0], 1, 16, [7, 7, 7, 7]], [SAMPLES[2]![0], 2, 4, [7, 7, 7, 7]], [SAMPLES[2]![0], 2, 16, [6, 7, 7, 7]], [SAMPLES[2]![0], 9, 4, [6, 7, 7, 7]], [SAMPLES[2]![0], 9, 16, [6, 6, 7, 7]],
      [SAMPLES[3]![0], 0, 0, [7, 8, 7, 8]], [SAMPLES[3]![0], 1, 4, [7, 7, 7, 7]], [SAMPLES[3]![0], 1, 16, [7, 7, 7, 7]], [SAMPLES[3]![0], 2, 4, [7, 7, 7, 7]], [SAMPLES[3]![0], 2, 16, [6, 7, 7, 7]], [SAMPLES[3]![0], 9, 4, [6, 7, 7, 7]], [SAMPLES[3]![0], 9, 16, [6, 6, 7, 7]],
      [SAMPLES[4]![0], 0, 0, [3, 3, 3, 3]], [SAMPLES[4]![0], 1, 4, [4, 4, 4, 4]], [SAMPLES[4]![0], 1, 16, [4, 4, 4, 4]], [SAMPLES[4]![0], 2, 4, [5, 5, 5, 5]], [SAMPLES[4]![0], 2, 16, [5, 5, 5, 5]], [SAMPLES[4]![0], 9, 4, [7, 7, 7, 7]], [SAMPLES[4]![0], 9, 16, [5, 6, 5, 6]],
    ];
    const fewer: string[] = [];
    const problems: string[] = [];
    for (const [name, options] of SAMPLES) {
      const { input, names } = await sampleOf(options);
      for (const [, n, words, before] of BEFORE.filter((x) => x[0] === name)) {
        // Small lines, at £900 and £470 more each, so most come last and the cut decides them.
        const more = withMoneyLines(input, n, words, (i) => 900 + 470 * i);
        for (const [i, [kind, extra]] of MONEY_CASES.entries()) {
          const report = buildReport({ ...more, ...extra(more) });
          const html = executiveHtml(report, names);
          const page = moneyPageOf(html)!;
          const named = [...page.matchAll(/<span class="basis">/g)].length;
          const label = `${name} and ${n} lines of ${words} words, ${kind}`;
          if (named < before[i]!) fewer.push(`${label}: ${named} named, ${before[i]} before`);
          problems.push(...(await problemsOn(html, [{ page, label }], report)));
        }
      }
    }
    assert.equal(BEFORE.length, SAMPLES.length * 7);
    assert.deepEqual(fewer, []);
    assert.deepEqual(problems, []);
  });
});

// DECISIONS.md 439: the full report's money is held to its slide by measure. One
// slide where it holds, the notes beneath the figure and the table; otherwise one
// continued slide, and never more, as the deck has a ceiling.
const moneySlidesOf = (html: string): string[] => pagesOf(html).filter((p) => p.includes(`<div class="money"><div class="eyebrow">What it&#39;s worth</div>`));

/**
 * What's wrong with the money slides as printed: more than two, apart, a line
 * missing or out of order, a last row that doesn't add up the rest, the total or
 * the notes anywhere but the last, or a continued slide with one line.
 */
function slidesWrong(html: string, report: BuiltReport, names: Pseudonymiser): string[] {
  const slides = moneySlidesOf(html);
  const wrong: string[] = [];
  if (slides.length < 1 || slides.length > 2) return [`${slides.length} slides`];
  const at = pagesOf(html).indexOf(slides[0]!);
  if (pagesOf(html)[at + slides.length - 1] !== slides.at(-1)) wrong.push("apart");
  if (slides.map(titleOf).join() !== ["What fixing this is worth", "What fixing this is worth, continued"].slice(0, slides.length).join()) wrong.push(`titled ${slides.map(titleOf).join("; ")}`);
  const t = renderContext(names, report.value.currency, false).t;
  const m = (n: number) => compactMoney(n, report.value.currency);
  const all = moneyOf(report);
  const named = slides.map((slide) => [...slide.matchAll(/<tr><td>(.*?)<span class="ref"[^>]*>[^<]*<\/span><\/td><td class="num">([^<]*)<\/td>/g)].filter((x) => !/smaller lines?, in the raw notes/.test(x[1]!)).map((x) => `${x[1]} ${x[2]}`));
  const k = named.flat().length;
  if (named.flat().join("\n") !== all.slice(0, k).map((g) => `${t(g.label)} ${m(g.counted.expected)}`).join("\n")) wrong.push("lines not in order");
  if (slides.length === 2 && named[1]!.length < 2) wrong.push(`${named[1]!.length} line on the continued slide`);
  const rests = slides.map((slide) => /<tr><td>And (\d+) smaller lines?, in the raw notes\.<span class="ref"[^>]*>[^<]*<\/span><\/td><td class="num">([^<]*)<\/td><td class="num">([^<]*)<\/td><\/tr>/.exec(slide));
  const rest = rests.at(-1);
  if (rests.slice(0, -1).some(Boolean)) wrong.push("a last row before the last slide");
  if (k < all.length && !(rest && rest[1] === String(all.length - k) && rest[2] === m(restOf(all, k, "expected")) && rest[3] === `${m(restOf(all, k, "low"))} to ${m(restOf(all, k, "high"))}`)) wrong.push(`${all.length - k} lines left, last row ${rest?.slice(1).join(", ") ?? "missing"}`);
  if (k === all.length && rest) wrong.push("a last row with every line named");
  const { total } = report.value;
  const totals = slides.map((s) => (s.match(/<tr class="total"><td>Total, each counted once<\/td>/g) ?? []).length);
  if (totals.join() !== [...slides.slice(1).map(() => 0), 1].join() || !slides.at(-1)!.includes(`<tr class="total"><td>Total, each counted once</td><td class="num">${m(total.expected)}</td><td class="num">${m(total.low)} to ${m(total.high)}</td></tr>`)) wrong.push(`totals ${totals.join(", ")}`);
  const notes = slides.map((s) => (s.match(/<p class="note">/g) ?? []).length);
  if (notes.slice(0, -1).some((n) => n > 0) || notes.at(-1) === 0) wrong.push(`notes ${notes.join(", ")}`);
  // On one slide, the notes run beneath the figure and the table, across the slide.
  if (slides.length === 1 && !/<\/table><\/div><\/div><p class="note">/.test(slides[0]!)) wrong.push("the notes beside the figure");
  return wrong;
}

describe("the full report's money fits its slides, two at most", () => {
  test("the worst case, at every length to sixteen words: every line named, in order, the total once on the last, and every check passing", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(withManyLines(input));
    const html = fullReportHtml(report, names);
    const slides = moneySlidesOf(html);
    assert.ok(moneyOf(report).length >= 15, `${moneyOf(report).length} lines`);
    assert.equal(slides.length, 2);
    assert.deepEqual(slidesWrong(html, report, names), []);
    assert.doesNotMatch(slides.join(""), /smaller line/);
    // The layout check on each slide, the number check among it, with nothing carried over.
    assert.deepEqual(await problemsOn(html, slides.map((page, i) => ({ page, label: `money slide ${i + 1}` })), report), []);
  });

  test("with 8, 16 and 40 lines more of sixteen words: two slides at most, every line named where they hold them and the rest added up where they don't, the first of two as full as it can be, every check passing", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const problems: string[] = [];
    const shapes: string[] = [];
    const room: string[] = [];
    for (const n of [8, 16, 40])
      for (const [kind, extra] of [MONEY_CASES[0]!, MONEY_CASES[3]!]) {
        const more = withMoneyLines(input, n, 16);
        const report = buildReport({ ...more, ...extra(more) });
        const html = fullReportHtml(report, names);
        const slides = moneySlidesOf(html);
        const label = `${n} lines more, ${kind}`;
        problems.push(...slidesWrong(html, report, names).map((w) => `${label}: ${w}`));
        problems.push(...(await problemsOn(html, slides.map((page, i) => ({ page, label: `${label}, money slide ${i + 1}` })), report)));
        shapes.push(`${n}: ${slides.length} slides, ${/And (\d+) smaller lines/.exec(slides.join(""))?.[1] ?? 0} added up`);
        if (slides.length < 2) continue;
        // The first of two names every line it has room for (DECISIONS.md 448): the next
        // line, the continued slide's first, takes more than the room under the first's
        // table. At 40 lines the 70.5px left under seven is the next line's room: it's
        // three lines of label, 80.6px.
        const page = await opened(documentOf(html, slides));
        try {
          const m = await page.evaluate(() => {
            const [first, continued] = [...document.querySelectorAll<HTMLElement>(".page")];
            const bottom = first!.getBoundingClientRect().bottom - parseFloat(getComputedStyle(first!).paddingBottom);
            const table = first!.querySelector<HTMLElement>("table.grid")!;
            const free = bottom - table.getBoundingClientRect().bottom;
            const before = table.getBoundingClientRect().height;
            table.querySelector("tbody")!.append(continued!.querySelector("tbody tr")!.cloneNode(true));
            return { free, next: table.getBoundingClientRect().height - before };
          });
          room.push(`${label}: ${m.free.toFixed(1)}px free, the next line ${m.next.toFixed(1)}px`);
          if (!(m.free >= 0 && m.next > m.free)) problems.push(room.at(-1)!);
        } finally {
          await page.close();
        }
      }
    assert.deepEqual(problems, []);
    // Two slides at each length without a survey, and at 8 and 16 with one.
    assert.equal(room.length, 5, room.join("; "));
    // The adversary's case: eight long lines more overflowed the continued slide when its notes printed larger than counted.
    assert.ok(shapes.slice(0, 2).every((x) => x === "8: 2 slides, 0 added up"), shapes.join("; "));
    // Past what two hold, the rest added up; on one slide where the deck is at its ceiling (the test below).
    assert.ok(shapes.slice(2).every((x) => /: [12] slides, [1-9]\d* added up$/.test(x)) && shapes[4]!.startsWith("40: 2 slides"), shapes.join("; "));
  });

  test("on every sample with 0, 1, 2 and 9 lines more, with a survey or without and a figure for spend or without", async () => {
    const problems: string[] = [];
    const counts: string[] = [];
    for (const [name, options] of SAMPLES) {
      const { input, names } = await sampleOf(options);
      for (const n of [0, 1, 2, 9]) {
        const more = withMoneyLines(input, n, 4);
        for (const [kind, extra] of MONEY_CASES) {
          const report = buildReport({ ...more, ...extra(more) });
          const html = fullReportHtml(report, names);
          const slides = moneySlidesOf(html);
          const label = `${name} and ${n} lines, ${kind}`;
          counts.push(`${label}: ${slides.length}`);
          problems.push(...slidesWrong(html, report, names).map((w) => `${label}: ${w}`));
          problems.push(...(await problemsOn(html, slides.map((page, i) => ({ page, label: `${label}, money slide ${i + 1}` })), report)));
        }
      }
    }
    assert.deepEqual(problems, []);
    // Some carry on, so the continued slide is checked here too.
    assert.ok(counts.some((c) => c.endsWith(": 2")), counts.join("; "));
  });

  test("CPQ and every module: one slide with all eight lines, the total and the notes beneath, the chart at its full height", async () => {
    // The first build named seven beside the figure and left one £170 line, the total and the notes on a continued slide.
    for (const [name, options] of [SAMPLES[2]!, SAMPLES[3]!]) {
      const { input, names } = await sampleOf(options);
      for (const [kind, extra] of MONEY_CASES) {
        const report = buildReport({ ...input, ...extra(input) });
        const html = fullReportHtml(report, names);
        const slides = moneySlidesOf(html);
        assert.equal(moneyOf(report).length, 8, name);
        assert.equal(slides.length, 1, `${name}, ${kind}`);
        assert.equal([...slides[0]!.matchAll(/<tr><td>/g)].length, 8, `${name}, ${kind}`);
        assert.match(slides[0]!, /viewBox="0 0 560 320"/);
        assert.deepEqual(slidesWrong(html, report, names), [], `${name}, ${kind}`);
        assert.deepEqual(await problemsOn(html, [{ page: slides[0]!, label: `${name}, ${kind}` }], report), []);
      }
    }
  });

  test("the notes print at 14px wherever the slides print them, the size the estimate counts", async () => {
    // A continued slide has no split, and its notes printed at 17px under a rule for the split's alone.
    const sales = await sampleOf(SAMPLES[0]![1]);
    const cpq = await sampleOf(SAMPLES[2]![1]);
    const sizes: string[] = [];
    for (const [label, { input, names }, n] of [["one slide", cpq, 0], ["a continued slide", sales, 8]] as const) {
      const html = fullReportHtml(buildReport(withMoneyLines(input, n, 16)), names);
      const page = await opened(documentOf(html, moneySlidesOf(html)));
      try {
        const notes = await page.evaluate(() => [...document.querySelectorAll<HTMLElement>(".money .note")].map((p) => `${getComputedStyle(p).fontSize} ${getComputedStyle(p).lineHeight}`));
        assert.ok(notes.length >= 1, label);
        sizes.push(...notes.map((x) => `${label}: ${x}`));
      } finally {
        await page.close();
      }
    }
    assert.ok(sizes.length >= 2 && sizes.every((x) => x.endsWith(": 14px 20.3px")), sizes.join("; "));
  });

  test("the figures' headings sit over their figures, right-aligned, on the slides and on the page", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(withMoneyLines(input, 40, 16));
    const full = fullReportHtml(report, names);
    const executive = executiveHtml(report, names);
    const page = await opened(documentOf(full, [...moneySlidesOf(full), moneyPageOf(executive)!]));
    try {
      // Each heading's text against the furthest right its column's figures reach.
      const gaps = await page.evaluate(() =>
        [...document.querySelectorAll<HTMLElement>(".money table.grid")].flatMap((table, t) =>
          [1, 2].map((col) => {
            const right = (cell: Element) => {
              const range = document.createRange();
              range.selectNodeContents(cell);
              return range.getBoundingClientRect().right;
            };
            const head = right(table.querySelectorAll("thead th")[col]!);
            const figures = Math.max(...[...table.querySelectorAll("tbody tr")].map((tr) => right(tr.children[col]!)));
            return { at: `table ${t + 1}, column ${col + 1}`, gap: Math.round(Math.abs(head - figures) * 10) / 10 };
          }),
        ),
      );
      assert.equal(gaps.length, 6);
      assert.deepEqual(gaps.filter((x) => x.gap > 1), []);
    } finally {
      await page.close();
    }
  });

  test("with a survey, interviews and 40 lines more, the deck keeps the density step it had with one money slide", async () => {
    // At 589049f, before the second slide, Sales Cloud built at step 8 with 120 slides and
    // Sales and Service Cloud at step 9 with 130, each at its ceiling. A second money
    // slide took them a step denser, the second to the step that gives up the area
    // chapters; so where it's the second slide that takes the deck past its ceiling,
    // the money adds the rest up on one (DECISIONS.md 439).
    for (const [[name, options], step, slides] of [[SAMPLES[0]!, 8, 120], [SAMPLES[1]!, 9, 130]] as const) {
      const { input, names } = await sampleOf(options);
      const report = buildReport({ ...withMoneyLines(input, 40, 16, (i) => 900 + 470 * i), survey: syntheticSurvey(12) });
      const counts = slideCounts(report, names);
      const { max } = deckRangeFor(report.input.run);
      assert.equal(max, slides, name);
      assert.deepEqual([counts.findIndex((c) => c <= max), counts.find((c) => c <= max)], [step, slides], `${name}: ${counts.join(", ")}`);
      const html = fullReportHtml(report, names);
      assert.equal(pagesOf(html).length, slides, name);
      const money = moneySlidesOf(html);
      assert.equal(money.length, 1, name);
      assert.match(money[0]!, /And \d+ smaller lines, in the raw notes\./);
      assert.deepEqual(slidesWrong(html, report, names), [], name);
      assert.deepEqual(await problemsOn(html, [{ page: money[0]!, label: name }], report), []);
    }
  });

  test("the count in the continued slide's last row is a figure from the data at every cut the slides can make", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    const report = buildReport(withMoneyLines(input, 40, 16));
    const n = moneyOf(report).length;
    const label = /<tr><td>(And \d+ smaller lines, in the raw notes\.)/.exec(moneySlidesOf(fullReportHtml(report, names)).join(""))![1]!;
    assert.ok(n >= 40, `${n} lines`);
    // Two slides can name any number of lines past the page's most, so every count to n - 1 is held.
    const counts = Array.from({ length: n - 1 }, (_, i) => n - 1 - i).map((c) => label.replace(/\d+ smaller lines/, c === 1 ? "1 smaller line" : `${c} smaller lines`));
    const money = scopeDataset(report.dataset, [VALUE_TOTAL]);
    const untraced = counts.flatMap((text) => [report.dataset, money].flatMap((set) => checkNumbers(text, set).unmatched.map((u) => `${text} ${u.raw}: ${u.reason}`)));
    assert.deepEqual(untraced, []);
  });
});

// DECISIONS.md 448: the page and the slides fill their room before adding lines up.
describe("the money names at least as many lines as before decision 448, and stays inside the margins", () => {
  test("on the page and the slides, on every sample with lines more of four and sixteen words, small and large, in every case", async () => {
    // Lines named at 60684b8, or more where decision 448 names more (the 60684b8 count in
    // the comment): counted by building that commit's documents (`git archive 60684b8`)
    // from the same samples, extra lines and cases, in order no survey and no spend,
    // spend, a survey, both. Small lines are £900 and £470 more each, so most come last
    // and the cut decides them; large ones £31,000 and £4,700 more, so they come first.
    const NAMED: Array<["page" | "slides", number, number, number, "small" | "large", number[]]> = [
      ["page", 0, 0, 0, "small", [7, 7, 7, 7]], ["page", 0, 1, 4, "small", [8, 8, 7, 8]], ["page", 0, 1, 16, "small", [7, 8, 7, 8]], ["page", 0, 2, 4, "small", [7, 7, 7, 7]], ["page", 0, 2, 16, "small", [7, 7, 7, 7]], ["page", 0, 9, 4, "small", [7, 7, 7, 7]], ["page", 0, 9, 16, "small", [7, 7, 7, 7]] /* [6, 7, 7, 7] */, ["page", 0, 2, 16, "large", [7, 7, 7, 7]] /* [6, 7, 6, 7] */, ["page", 0, 10, 16, "large", [6, 7, 6, 7]], ["page", 0, 33, 16, "large", [6, 6, 6, 6]],
      ["slides", 0, 0, 4, "large", [7, 7, 7, 7]], ["slides", 0, 1, 4, "large", [8, 8, 8, 8]], ["slides", 0, 2, 4, "large", [9, 9, 9, 9]] /* [7, 7, 9, 9] */, ["slides", 0, 9, 4, "large", [16, 16, 16, 16]], ["slides", 0, 8, 16, "large", [15, 15, 15, 15]], ["slides", 0, 12, 16, "large", [13, 13, 13, 13]] /* [11, 11, 11, 11] */, ["slides", 0, 16, 16, "large", [12, 12, 12, 12]] /* [11, 11, 11, 11] */, ["slides", 0, 40, 16, "large", [13, 13, 5, 5]],
      ["page", 1, 0, 0, "small", [7, 7, 7, 7]], ["page", 1, 1, 4, "small", [8, 8, 7, 8]], ["page", 1, 1, 16, "small", [7, 8, 7, 8]], ["page", 1, 2, 4, "small", [7, 7, 7, 7]], ["page", 1, 2, 16, "small", [7, 7, 7, 7]], ["page", 1, 9, 4, "small", [7, 7, 7, 7]], ["page", 1, 9, 16, "small", [7, 7, 7, 7]] /* [6, 7, 7, 7] */,
      ["slides", 1, 0, 4, "large", [7, 7, 7, 7]], ["slides", 1, 1, 4, "large", [8, 8, 8, 8]], ["slides", 1, 2, 4, "large", [9, 9, 9, 9]], ["slides", 1, 9, 4, "large", [16, 16, 16, 16]],
      ["page", 2, 0, 0, "small", [8, 8, 7, 8]], ["page", 2, 1, 4, "small", [7, 7, 7, 7]], ["page", 2, 1, 16, "small", [7, 7, 7, 7]], ["page", 2, 2, 4, "small", [7, 7, 7, 7]], ["page", 2, 2, 16, "small", [7, 7, 7, 7]], ["page", 2, 9, 4, "small", [7, 7, 7, 7]], ["page", 2, 9, 16, "small", [7, 7, 7, 7]], ["page", 2, 1, 16, "large", [7, 7, 7, 7]], ["page", 2, 9, 16, "large", [7, 7, 7, 7]] /* [6, 7, 6, 7] */, ["page", 2, 32, 16, "large", [6, 6, 6, 6]] /* [5, 6, 6, 6] */,
      ["slides", 2, 0, 4, "large", [8, 8, 8, 8]], ["slides", 2, 1, 4, "large", [9, 9, 9, 9]] /* [7, 7, 9, 9] */, ["slides", 2, 2, 4, "large", [10, 10, 10, 10]], ["slides", 2, 9, 4, "large", [17, 17, 17, 17]],
      ["page", 3, 0, 0, "small", [8, 8, 7, 8]], ["page", 3, 1, 4, "small", [7, 7, 7, 7]], ["page", 3, 1, 16, "small", [7, 7, 7, 7]], ["page", 3, 2, 4, "small", [7, 7, 7, 7]], ["page", 3, 2, 16, "small", [7, 7, 7, 7]], ["page", 3, 9, 4, "small", [7, 7, 7, 7]], ["page", 3, 9, 16, "small", [7, 7, 7, 7]],
      ["slides", 3, 0, 4, "large", [8, 8, 8, 8]], ["slides", 3, 1, 4, "large", [9, 9, 9, 9]], ["slides", 3, 2, 4, "large", [10, 10, 10, 10]], ["slides", 3, 9, 4, "large", [17, 17, 17, 17]],
      ["page", 4, 0, 0, "small", [3, 3, 3, 3]], ["page", 4, 1, 4, "small", [4, 4, 4, 4]], ["page", 4, 1, 16, "small", [4, 4, 4, 4]], ["page", 4, 2, 4, "small", [5, 5, 5, 5]], ["page", 4, 2, 16, "small", [5, 5, 5, 5]], ["page", 4, 9, 4, "small", [7, 7, 7, 7]], ["page", 4, 9, 16, "small", [6, 7, 6, 7]],
      ["slides", 4, 0, 4, "large", [3, 3, 3, 3]], ["slides", 4, 1, 4, "large", [4, 4, 4, 4]], ["slides", 4, 2, 4, "large", [5, 5, 5, 5]], ["slides", 4, 9, 4, "large", [10, 10, 12, 12]],
    ];
    const fewer: string[] = [];
    const past: string[] = [];
    for (const [s, [name, options]] of SAMPLES.entries()) {
      const { input, names } = await sampleOf(options);
      for (const doc of ["page", "slides"] as const) {
        const printed: Array<{ page: string; label: string }> = [];
        let head = "";
        for (const [, , n, words, size, before] of NAMED.filter((x) => x[0] === doc && x[1] === s)) {
          const more = withMoneyLines(input, n, words, size === "small" ? (i) => 900 + 470 * i : undefined);
          for (const [i, [kind, extra]] of MONEY_CASES.entries()) {
            const report = buildReport({ ...more, ...extra(more) });
            const html = doc === "page" ? executiveHtml(report, names) : fullReportHtml(report, names);
            const pages = doc === "page" ? [moneyPageOf(html)!] : moneySlidesOf(html);
            const named = pages.join("").match(doc === "page" ? /<span class="basis">/g : /<tr><td>(?!And \d+ smaller lines?, in the raw notes)/g)?.length ?? 0;
            const label = `${name} and ${n} ${size} lines of ${words} words, ${kind}, the ${doc}`;
            if (named < before[i]!) fewer.push(`${label}: ${named} named, ${before[i]} before`);
            head = html;
            printed.push(...pages.map((page, k) => ({ page, label: `${label} ${k + 1}` })));
          }
        }
        // The estimate is never under the browser's measure, so nothing it sets runs into
        // the margin: the layout check allows that, as the footer sits lower.
        const page = await opened(documentOf(head, printed.map((p) => p.page)));
        try {
          const free = await page.evaluate(() =>
            [...document.querySelectorAll<HTMLElement>(".page")].map((sheet) => sheet.getBoundingClientRect().bottom - parseFloat(getComputedStyle(sheet).paddingBottom) - Math.max(...[...sheet.querySelectorAll<HTMLElement>(".money *")].map((x) => x.getBoundingClientRect().bottom))),
          );
          past.push(...free.flatMap((f, k) => (f < 0 ? [`${printed[k]!.label}: ${(-f).toFixed(2)}px into the margin`] : [])));
          assert.equal(free.length, printed.length);
        } finally {
          await page.close();
        }
      }
    }
    assert.equal(NAMED.length, 65);
    assert.deepEqual(fewer, []);
    assert.deepEqual(past, []);
  });
});

describe("a numeric column's heading sits over its figures, right-aligned, in every table (DECISIONS.md 447)", () => {
  test("on every table with one in the full report and the executive summary, the full report's \"Result\" among them", async () => {
    // The "Measure / Result" table had its heading at the left of a right-aligned column.
    const gaps: string[] = [];
    const seen = new Set<string>();
    for (const [name, options] of [SAMPLES[0]!, SAMPLES[3]!]) {
      const { input, names } = await sampleOf(options);
      const report = buildReport({ ...input, survey: syntheticSurvey(12) });
      for (const html of [fullReportHtml(report, names), executiveHtml(report, names)]) {
        const page = await opened(documentOf(html, pagesOf(html).filter((p) => p.includes('<th class="num">'))));
        try {
          // Each numeric heading's text against the furthest right its column's figures reach.
          const found = await page.evaluate(() =>
            [...document.querySelectorAll<HTMLTableElement>("table.grid")].flatMap((table) => {
              const heads = [...table.querySelectorAll<HTMLElement>("thead th")];
              const right = (cell: Element) => {
                const range = document.createRange();
                range.selectNodeContents(cell);
                return range.getBoundingClientRect().right;
              };
              const rows = [...table.querySelectorAll("tbody tr")].filter((tr) => tr.children.length === heads.length);
              return heads.flatMap((th, col) => (th.classList.contains("num") && rows.length > 0 ? [{ head: th.textContent ?? "", table: heads.map((h) => h.textContent).join(" / "), gap: Math.round(Math.abs(right(th) - Math.max(...rows.map((tr) => right(tr.children[col]!)))) * 10) / 10 }] : []));
            }),
          );
          for (const x of found) {
            seen.add(x.table);
            if (x.gap > 1) gaps.push(`${name}, ${x.table}: "${x.head}" ${x.gap}px from its figures`);
          }
        } finally {
          await page.close();
        }
      }
    }
    assert.ok([...seen].some((t) => t.startsWith("Measure / Result /")), [...seen].join("; "));
    assert.ok([...seen].some((t) => t.startsWith("Where it comes from / Expected / Range")), [...seen].join("; "));
    assert.ok(seen.size >= 9, [...seen].join("; "));
    assert.deepEqual(gaps, []);
  });
});

describe("each theme under \"What your people told us\" is its own paragraph (DECISIONS.md 437)", () => {
  test("on the Verdict page with interviews alone, and on the voices' page with a survey too, with the page's gap between them", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    for (const [label, extra] of [["interviews alone", {}], ["both voices", { survey: syntheticSurvey(12) }]] as const) {
      const report = buildReport({ ...input, ...extra });
      const themes = currentBody(report.outputs.executive_summary, "interviews").split("\n\n");
      assert.ok(themes.length >= 2, `${label}: ${themes.length} themes`);
      // Set apart with a line between them, as an edit might, they still print one to a paragraph.
      const built = report.outputs.executive_summary;
      for (const body of [themes.join("\n\n"), themes.join("\n")]) {
        built.versions.find((v) => v.id === built.sections.find((s) => s.key === "interviews")!.currentVersionId)!.body = body;
        const html = executiveHtml(report, names);
        const section = /<h3 class="sub">What your people told us<\/h3>([\s\S]*?)(?=<\/div>|<p style|<\/section>|<div class="footer")/.exec(html)![1]!.trim();
        assert.equal(section, themes.map((t) => `<p class="theme">${t}</p>`).join(""), label);
        assert.doesNotMatch(section, /<br>/);
      }
      const page = await opened(executiveHtml(report, names));
      try {
        const gaps = await page.evaluate(() => {
          const ps = [...document.querySelectorAll<HTMLElement>("p.theme")];
          const lead = document.querySelector<HTMLElement>(".lead p")!;
          return { between: ps.slice(1).map((p, i) => p.getBoundingClientRect().top - ps[i]!.getBoundingClientRect().bottom), lead: parseFloat(getComputedStyle(lead).marginBottom) };
        });
        assert.deepEqual(gaps.between.map(Math.round), themes.slice(1).map(() => gaps.lead), label);
      } finally {
        await page.close();
      }
    }
  });
});
