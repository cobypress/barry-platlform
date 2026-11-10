import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../../domains/index.ts";
import { LENGTH_TOLERANCE, wordCount } from "../../narrative/draft.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, WORD_TARGETS, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Score, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { executiveHtml, executiveMaxPages, planIntroHeight } from "../executive.ts";
import { fullReportHtmlAt, slideCounts, threeColumnItemHeight } from "../full-report.ts";
import { paginateColumns } from "../html.ts";
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
    // Hashes of the Verdict page before decision 442, on the Sales Cloud sample.
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
    assert.deepEqual(Object.fromEntries(Object.entries(verdicts).map(([key, page]) => [key, sha(page)])), { neither: "3e24b50f30626758", survey: "d6592a20fda36044", interviews: "28f733269a6ba3b4" });
  });
});
