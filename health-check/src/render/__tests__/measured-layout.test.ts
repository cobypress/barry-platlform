import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { ALL_MODULES, CPQ_MODULES, SALES_CLOUD_MODULES, SERVICE_CLOUD_MODULES } from "../../domains/index.ts";
import { LENGTH_TOLERANCE, wordCount } from "../../narrative/draft.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, currentBody, WORD_TARGETS, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { Score, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { syntheticSurvey } from "../../testing/survey.ts";
import { EXECUTIVE_PAGES } from "../render-run.ts";
import { EXECUTIVE_MAX_PAGES, executiveHtml, executiveMaxPages, hasBothVoices, introLines, planIntroHeight, RECHECK_EXECUTIVE_MAX_PAGES } from "../executive.ts";
import { fullReportHtmlAt, slideCounts, threeColumnItemHeight } from "../full-report.ts";
import { estimateLines, paginateColumns } from "../html.ts";
import { compactMoney } from "../format.ts";
import { restOf } from "../money.ts";
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

  test("with more lines than the page holds, on long labels, it names fewer and adds up the rest", async () => {
    const { input, names } = await sampleOf(SAMPLES[0]![1]);
    // Lines of their own, each a value group labelled with the finding's title, at
    // every length to sixteen words, so the page can't hold them all.
    const template = input.findings.find((f) => f.status === "issue")!;
    const extra = Array.from({ length: 9 }, (_, i): Finding => ({
      ...template,
      id: `f-${String(900 + i).padStart(12, "0")}`,
      ruleId: `synthetic.money_${i}`,
      title: draft(8 + i, OPENINGS[i % OPENINGS.length]!).replace(/\.$/, ""),
      severity: "low",
      value: { kind: "licence_cost", amount: 31_000 + 4_700 * i, unit: "GBP", period: "per year", assumptions: ["A synthetic line for the money page."] },
    }));
    const report = buildReport({ ...input, findings: [...input.findings, ...extra], intake: { ...input.intake!, salesforceSpendAnnual: 250_000 }, survey: syntheticSurvey(12) });
    const html = executiveHtml(report, names);
    const page = moneyPageOf(html)!;
    const all = report.value.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
    assert.ok(all.length >= 15, `${all.length} lines`);
    assert.deepEqual(await problemsOn(html, [{ page, label: "the money page" }], report), []);
    // The chart took its shorter height, and the table named fewer lines than the most it can.
    assert.match(page, /viewBox="0 0 800 250"/);
    const named = [...page.matchAll(/<span class="basis">/g)].length;
    assert.ok(named >= 1 && named < 7, `${named} lines named`);
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
