import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { PRACTICE_ON_SLIDE, practiceFor } from "../../practice/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { ACCOUNT_ENGAGEMENT_DOMAINS, CPQ_DOMAINS, SALES_CLOUD_DOMAINS, SERVICE_CLOUD_DOMAINS, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { chapterSlide } from "../chapter.ts";
import { renderRawNotesDocx } from "../raw-notes-docx.ts";
import { strFromU8, unzipSync } from "fflate";
import { fullReportHtml } from "../full-report.ts";
import { renderContext } from "../html.ts";
import { layoutOf, launchBrowser } from "../pdf.ts";
import { referenceMap, noteSections } from "../references.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };

/** Every area a run can have, the optional modules' included. */
const AREAS = [...SALES_CLOUD_DOMAINS, ...SERVICE_CLOUD_DOMAINS, ...CPQ_DOMAINS, ...ACCOUNT_ENGAGEMENT_DOMAINS];

/**
 * Every area full of issues, with the longest recommendations a real org
 * gives, and a check that couldn't be measured, whose note takes a line of the
 * panel too (night 3 scale test), so each panel is at its fullest.
 */
function messy(): ReportInput {
  const issues = output!.findings.filter((f) => f.status === "issue");
  const longest = [...issues].sort((a, b) => b.recommendation.length - a.recommendation.length);
  const unmeasured = output!.findings.find((f) => f.status === "not_measurable")!;
  const findings: Finding[] = [];
  AREAS.forEach((domain, d) => {
    for (let i = 0; i < 12; i++) {
      const source = longest[(d + i) % Math.min(longest.length, 6)]!;
      findings.push({ ...source, id: `f-${String(d).padStart(2, "0")}${String(i).padStart(10, "0")}`, domain, severity: (["critical", "high", "medium", "low"] as const)[i % 4]! });
    }
    findings.push({ ...unmeasured, id: `n-${String(d).padStart(2, "0")}${"0".repeat(10)}`, domain });
  });
  return { ...base, findings: [...findings, ...output!.findings.filter((f) => f.status !== "issue")] };
}

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("what good looks like on each chapter's opening slide", () => {
  test("every area shows three statements with their sources, and fits with the fullest panel", async () => {
    const report = buildReport(messy());
    const r = renderContext(pseudonyms, "GBP", false, referenceMap(noteSections(report)));
    const html = fullReportHtml(report, pseudonyms);
    const pages = AREAS.map((domain) => chapterSlide(r, report, domain));
    for (const [i, page] of pages.entries()) {
      const domain = AREAS[i]!;
      assert.match(page, /What good looks like/, domain);
      assert.equal((page.match(/<ul class="good">[\s\S]*?<\/ul>/)?.[0].match(/<li>/g) ?? []).length, Math.min(PRACTICE_ON_SLIDE, practiceFor(domain).length), domain);
      assert.match(page, /class="src">(Trailhead|Salesforce Well-Architected|Salesforce Architects|Black Cloud practice)</, domain);
    }
    // Each chapter slide alone, after the real deck, so it's measured with the deck's own styles.
    const extra = pages.map((p, i) => `<section class="page" data-page="${900 + i}">${p}<div class="footer" data-chrome><span>Black Cloud Barometer</span><span>${900 + i}</span></div></section>`).join("");
    const deck = html.replace("</body>", `${extra}</body>`);
    const layout = await layoutOf(browser, deck, { minPages: 1, maxPages: 999, dataset: report.dataset });
    const first = layout.pages - pages.length + 1;
    assert.deepEqual(layout.problems.filter((p) => p.page >= first).map((p) => ({ ...p, domain: AREAS[p.page - first] })), []);
  });

  test("a chapter with a check it couldn't measure still fits: the note under the panel takes room from the first steps", async () => {
    // Service operations as the 500-user scale test had it, which ran into the footer (night 3).
    const template = (status: Finding["status"]) => output!.findings.find((f) => f.status === status)!;
    const own = (i: number, status: Finding["status"], title: string, recommendation = "", severity: Finding["severity"] = "info"): Finding => ({ ...template(status), id: `s-${String(i).padStart(12, "0")}`, domain: "service_operations", title, recommendation, severity });
    const findings: Finding[] = [
      own(1, "issue", "Your case backlog is growing", "Find where the extra cases sit by priority and team, then add capacity there or stop the cause, such as a product fault or a missing help article, before the backlog ages.", "medium"),
      own(2, "issue", "Too many open cases are old", "Review the oldest cases each week, close what's resolved or abandoned, and escalate the rest. Ask why they stalled: a missing answer, a waiting customer or a queue nobody watches.", "medium"),
      own(3, "issue", "High-priority cases aren't resolved any faster", "Make priority drive the work: route high-priority cases to their own queue or a higher routing priority, and give them a resolution target the team can see.", "medium"),
      own(4, "issue", "Cases don't say where they came from", "Set Origin automatically for every channel (Email-to-Case, web forms, messaging and the phone), and make it required when an agent creates a case by hand.", "low"),
      own(5, "not_measurable", "Entitlement milestones met"),
      ...["Time to first reply on email cases", "Closed cases that are reopened", "Open cases waiting in a queue", "Cases record why the customer got in touch", "Open cases are linked to a customer"].map((t, i) => own(6 + i, "passed", t)),
    ];
    const report = buildReport({ ...base, findings: [...output!.findings, ...findings] });
    const r = renderContext(pseudonyms, "GBP", false, referenceMap(noteSections(report)));
    const page = chapterSlide(r, report, "service_operations");
    assert.match(page, /1 more check couldn&#39;t be measured here/);
    const deck = fullReportHtml(report, pseudonyms).replace("</body>", `<section class="page" data-page="999">${page}<div class="footer" data-chrome><span>Black Cloud Barometer</span><span>999</span></div></section></body>`);
    const layout = await layoutOf(browser, deck, { minPages: 1, maxPages: 999, dataset: report.dataset });
    assert.deepEqual(layout.problems.filter((p) => p.page === layout.pages), []);
  });

  test("the raw notes have one section numbering every statement, with each source's link and date", async () => {
    const report = buildReport(base);
    const section = noteSections(report).find((s) => s.key === "practice")!;
    assert.equal(section.title, "What good looks like");
    const domains = [...new Set(report.teamFindings.map((f) => f.domain))];
    assert.equal(section.items.length, domains.reduce((sum, d) => sum + practiceFor(d).length, 0));
    const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    assert.match(text, new RegExp(`${section.ref} +What good looks like`));
    assert.match(text, /https:\/\/architect\.salesforce\.com\/docs\/architect\/well-architected\/guide\/trust\.html/);
    assert.match(text, /Read on 5 Nov 2026/);
    assert.match(text, /Black Cloud practice, not a Salesforce source/);
  });

  test("the microsite's issues page has each area's statements under its heading", () => {
    const report = buildReport(base);
    const issues = buildMicrositePages(report, pseudonyms).pages.find((p) => p.slug === "issues")!.html;
    for (const domain of new Set(report.teamFindings.filter((f) => f.status === "issue").map((f) => f.domain))) {
      for (const p of practiceFor(domain)) assert.ok(issues.includes(p.text.replace(/'/g, "&#39;")), `${domain}: ${p.text}`);
    }
  });
});
