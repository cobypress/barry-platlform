import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { PRACTICE_ON_SLIDE, practiceFor } from "../../practice/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { SALES_CLOUD_DOMAINS, type Finding } from "../../schemas/index.ts";
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

/** Every area full of issues, with the longest recommendations a real org gives, so each panel is at its fullest. */
function messy(): ReportInput {
  const issues = output!.findings.filter((f) => f.status === "issue");
  const longest = [...issues].sort((a, b) => b.recommendation.length - a.recommendation.length);
  const findings: Finding[] = [];
  SALES_CLOUD_DOMAINS.forEach((domain, d) => {
    for (let i = 0; i < 12; i++) {
      const source = longest[(d + i) % Math.min(longest.length, 6)]!;
      findings.push({ ...source, id: `f-${String(d).padStart(2, "0")}${String(i).padStart(10, "0")}`, domain, severity: (["critical", "high", "medium", "low"] as const)[i % 4]! });
    }
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
    const pages = SALES_CLOUD_DOMAINS.map((domain) => chapterSlide(r, report, domain));
    for (const [i, page] of pages.entries()) {
      const domain = SALES_CLOUD_DOMAINS[i]!;
      assert.match(page, /What good looks like/, domain);
      assert.equal((page.match(/<ul class="good">[\s\S]*?<\/ul>/)?.[0].match(/<li>/g) ?? []).length, Math.min(PRACTICE_ON_SLIDE, practiceFor(domain).length), domain);
      assert.match(page, /class="src">(Trailhead|Salesforce Well-Architected|Salesforce Architects|Black Cloud practice)</, domain);
    }
    // Each chapter slide alone, after the real deck, so it's measured with the deck's own styles.
    const extra = pages.map((p, i) => `<section class="page" data-page="${900 + i}">${p}<div class="footer" data-chrome><span>Black Cloud Barometer</span><span>${900 + i}</span></div></section>`).join("");
    const deck = html.replace("</body>", `${extra}</body>`);
    const layout = await layoutOf(browser, deck, { minPages: 1, maxPages: 999, dataset: report.dataset });
    const first = layout.pages - pages.length + 1;
    assert.deepEqual(layout.problems.filter((p) => p.page >= first).map((p) => ({ ...p, domain: SALES_CLOUD_DOMAINS[p.page - first] })), []);
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

describe("the deck's wording, as a customer reads it (night 3 reading pass, DECISIONS.md 327)", () => {
  const html = fullReportHtml(buildReport(base), pseudonyms);

  test("an area's first issue page isn't called \"Other issues\", and the issues left off the opening slide are said to follow", () => {
    assert.doesNotMatch(html, /<h2>Other issues<\/h2>/);
    assert.match(html, /<h2>The issues in detail<\/h2>/);
    const lists = html.match(/<h3>What(&#39;|')s (broken|below par)<\/h3><ul>[\s\S]*?<\/ul>/g) ?? [];
    assert.ok(lists.length > 0);
    for (const list of lists) assert.doesNotMatch(list, /in the raw notes/, list);
    assert.match(html, /more on the pages that follow\./);
  });

  test("passed checks are \"Checked and fine\", not \"What's working\" under a problem's title", () => {
    assert.match(html, /<h3>Checked and fine<\/h3>/);
    assert.doesNotMatch(html, /<h3>What&#39;s working<\/h3>|<h3>What's working<\/h3>/);
  });

  test("an issue with no money figure says so once, instead of repeating its severity as a risk", () => {
    assert.match(html, />Not costed</);
    assert.doesNotMatch(html, />(Low|Medium|High|Critical) risk</);
  });

  test("every item after the 90 days carries its reference", () => {
    const start = html.indexOf("After 90 days");
    const after = html.slice(start, html.indexOf("What happens next", start));
    const items = after.match(/<strong>[^<]+<\/strong>(<span class="ref"[^>]*>|<\/td>|<br>)/g) ?? [];
    assert.ok(items.length > 0);
    assert.ok(items.every((x) => x.includes('class="ref"')), items.find((x) => !x.includes('class="ref"')));
  });

  test("an area's first steps say each recommendation once", () => {
    for (const panel of html.match(/<h3>What to do first<\/h3><ul>[\s\S]*?<\/ul>/g) ?? []) {
      const items = [...panel.matchAll(/<li>([\s\S]*?)<\/li>/g)].map((m) => m[1]);
      assert.equal(new Set(items).size, items.length, panel);
    }
  });
});
