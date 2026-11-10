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
