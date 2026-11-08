import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser, Page } from "playwright";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput, type ReportOutputKey } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { carryOver } from "../carry-over.ts";
import { executiveHtml } from "../executive.ts";
import { fullReportHtml } from "../full-report.ts";
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
