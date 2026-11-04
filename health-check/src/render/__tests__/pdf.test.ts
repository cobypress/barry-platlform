import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Browser } from "playwright";
import type { NarrativeClient } from "../../narrative/client.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, currentBody, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { buildPlan, topIssues, verdictOf } from "../../narrative/select.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { SALES_CLOUD_DOMAINS, type Finding, type Metric, type NumberOverride, type ReportSection } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { EXECUTIVE_PLAN_ITEMS, executiveHtml } from "../executive.ts";
import { plainWords, ukDates } from "../format.ts";
import { DECK_RANGE, fullReportHtml, INVENTORY_SLIDE_ROWS, slideCounts } from "../full-report.ts";
import { INVENTORY_COLUMNS, OBJECT_INVENTORY } from "../../domains/configuration/inventory.ts";
import { document } from "../html.ts";
import { layoutOf, launchBrowser, pdfPageCount, renderPdf } from "../pdf.ts";
import { EXECUTIVE_PAGES, OUTPUT_FILES, renderRun } from "../render-run.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps };
const issue = output!.findings.find((f) => f.status === "issue")!;
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "The director says the forecast can't be trusted.", statedProblems: [{ id: "p1", statement: "We don't trust the forecast.", findingIds: [issue.id], assessment: "The data supports this." }], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z" });
const full: ReportInput = { ...base, intake };

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

const execRange = { minPages: EXECUTIVE_PAGES.min, maxPages: EXECUTIVE_PAGES.max };
const deckRange = { minPages: DECK_RANGE.min, maxPages: DECK_RANGE.max };
const noFonts = (html: string): string => html.replace(/url\(data:[^)]+\)/g, "");
const RAW_DATE = /(?<![\w-])\d{4}-\d{2}-\d{2}(?![\w-])/;
const pageText = (html: string): string => noFonts(html).replace(/<style>[\s\S]*?<\/style>/, "").replace(/<[^>]+>/g, " ");

/** Every two-column slide has cards on both sides. */
function assertBothColumnsUsed(html: string): void {
  for (const block of html.split(/class="two-up(?: roomy)?"/).slice(1)) {
    const body = block.split("</section>")[0]!;
    const columns = body.split('class="col"').slice(1);
    assert.equal(columns.length, 2, "two columns");
    for (const column of columns) assert.match(column, /^><article/, "a column with a card in it");
  }
}

const describeProblems = (problems: unknown[]): string => JSON.stringify(problems.slice(0, 8));

describe("the executive summary", () => {
  test("comes to about 6 pages, with every layout check and number check passing", async () => {
    const report = buildReport(full);
    const { pdf, layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { ...execRange, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
    assert.equal(pdfPageCount(pdf), layout.pages);
    assert.equal(pdf.subarray(0, 5).toString(), "%PDF-");
    assert.ok(layout.pages >= 5 && layout.pages <= 7, `${layout.pages} pages`);
  });

  test("holds a verdict, scorecard, top five with value, and the plan", () => {
    const report = buildReport(full);
    const html = executiveHtml(report, pseudonyms);
    for (const text of ["Verdict", verdictOf(report.teamFindings).headline, "How each area stands", "Your pipeline today", "Potential revenue in open deals", "Top 5 issues", "Days 1 to 30", "Days 31 to 60", "Days 61 to 90", "Synthetic Co"]) assert.ok(html.includes(text), text);
    for (const f of topIssues(report.teamFindings, 5)) {
      const value = f.value.kind === "not_sized" ? "Not sized" : ukDates(`£${f.value.amount.toLocaleString("en-GB")} ${f.value.period}`);
      assert.ok(html.includes(value), value);
    }
    assert.match(html, /data-expect-chart="scorecard"/);
    assert.match(html, /data-expect-chart="win-rate-by-segment"/);
  });

  test("shows UK dates, never an ISO date", () => {
    const report = buildReport(full);
    assert.ok(report.teamFindings.some((f) => f.value.kind !== "not_sized" && RAW_DATE.test(f.value.period)), "the sample has a dated period");
    assert.doesNotMatch(pageText(executiveHtml(report, pseudonyms)), RAW_DATE);
  });

  test("reads as a customer would: no shorthand, raw amounts or actions without a subject", () => {
    const report = buildReport(full);
    const html = executiveHtml(report, pseudonyms);
    const text = pageText(html) + pageText(fullReportHtml(report, pseudonyms));
    assert.doesNotMatch(text, /Effort (?:XS|S|M|L|XL)\b/);
    assert.doesNotMatch(text, /Amount of \d/);
    assert.doesNotMatch(text, /for team /);
    const { phases: plan, later } = buildPlan(report.teamFindings);
    const shown = plan.reduce((n, p) => n + Math.min(p.findings.length, EXECUTIVE_PLAN_ITEMS), 0);
    assert.equal(html.match(/<li><strong>[^<]+\.<\/strong>/g)?.length, shown, "every plan action leads with its issue");
    const deckActions = [...fullReportHtml(report, pseudonyms).matchAll(/<ul class="action-list[^"]*">([\s\S]*?)<\/ul>/g)].flatMap((m) => m[1]!.split("<li>").slice(1));
    assert.ok(deckActions.length > 0);
    for (const item of deckActions) assert.match(item, /^<strong>/, "every roadmap action in the deck leads with its issue");
    // Urgent wording in a later phase contradicts itself.
    for (const f of [...plan.slice(1).flatMap((p) => p.findings), ...later]) assert.doesNotMatch(f.recommendation, /\b(?:now|this month)\b/i, f.title);
    assert.ok(later.length > 0);
    assert.match(fullReportHtml(report, pseudonyms), /<h2>After 90 days<\/h2>/, "actions after 90 days are listed in the deck");
  });

  test("names no one", () => {
    const html = executiveHtml(buildReport(full), pseudonyms).replace(/url\(data:[^)]+\)/g, "");
    // The customer's own name belongs on the cover. People never appear.
    for (const entry of pseudonyms.entries()) if (entry.token.startsWith("[[user:") && entry.name.includes(" ") && entry.name !== "Unknown user") assert.ok(!html.includes(entry.name), entry.name);
    assert.doesNotMatch(html, /\[\[/);
  });
});

describe("the full health check", () => {
  test("is 16:9, 40 to 60 slides, with every check passing", async () => {
    const report = buildReport(full);
    const { pdf, layout } = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
    assert.equal(pdfPageCount(pdf), layout.pages);
    const box = /\/MediaBox\s*\[\s*0 0 ([\d.]+) ([\d.]+)/.exec(pdf.toString("latin1"));
    assert.ok(box, "a page box");
    assert.ok(Math.abs(Number(box[1]) / Number(box[2]) - 16 / 9) < 0.01, `${box[1]} by ${box[2]}`);
  });

  test("covers what the brief lists", () => {
    const html = fullReportHtml(buildReport(full), pseudonyms);
    for (const text of ["What you told us", "How each area stands", "Configuration", "Automation", "Data quality", "Utilisation", "Historic pipeline", "Win rate by segment", "Stage funnel", "Sales cycle", "lippage", "Pipeline over time", "What you told us, against what we found", "Days 1 to 30", "How we worked", "Metric definitions", "Checks that passed", "What we couldn&#39;t measure"]) assert.ok(html.includes(text), text);
    for (const chart of ["scorecard", "win-rate-by-segment", "stage-funnel", "pipeline-over-time", "delivery-outlook", "wins-by-customer", "top-accounts", "won-by-type", "won-by-source", "lost-by-stage", "stage-velocity"]) assert.match(html, new RegExp(`data-expect-chart="${chart}"`));
    for (const text of ["Open pipeline by stage", "Qualified (retired)", "Quarter by quarter", "Delivery outlook", "Where deals drop out", "closed in the quarter first forecast"]) assert.ok(html.includes(text), text);
  });

  test("shows UK dates, never an ISO date", () => {
    assert.doesNotMatch(pageText(fullReportHtml(buildReport(full), pseudonyms)), RAW_DATE);
  });

  test("every card slide uses both columns", () => {
    const html = fullReportHtml(buildReport(full), pseudonyms);
    assert.match(html, /class="two-up/, "the sample is dense enough to test this");
    assert.match(html, /class="two-up roomy"/, "short slides get larger cards");
    assertBothColumnsUsed(html);
  });

  test("a lone card on a dense slide takes the full width", () => {
    const issues = full.findings.filter((f) => f.status === "issue");
    const one = issues.filter((f) => f.severity === "medium").slice(0, 1).map((f) => ({ ...f, domain: "activity" }));
    const rest = issues.filter((f) => f.domain !== "activity" && f.id !== one[0]!.id);
    const html = fullReportHtml(buildReport({ ...full, findings: [...one, ...rest, ...full.findings.filter((f) => f.status !== "issue")] }), pseudonyms);
    assertBothColumnsUsed(html);
  });

  test("every issue appears somewhere", () => {
    const report = buildReport(full);
    const html = fullReportHtml(report, pseudonyms);
    const escape = (t: string): string => plainWords(ukDates(t)).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    assert.doesNotMatch(html, /raw notes have all of it/, "an ordinary org stays out of compact mode");
    // The roadmap repeats recommendations, so count the issues' own lines.
    assert.equal((html.match(/What to do:/g) ?? []).length, report.teamFindings.filter((x) => x.status === "issue" && x.recommendation).length);
    for (const f of report.teamFindings.filter((x) => x.status === "issue")) {
      assert.ok(html.includes(escape(f.summary)), `summary: ${f.title}`);
      assert.ok(html.includes(escape(f.recommendation)), `recommendation: ${f.title}`);
    }
  });

  test("says what's missing when there's no intake, rather than making it up", () => {
    const html = fullReportHtml(buildReport(base), pseudonyms);
    assert.equal((html.match(/Needs input from a consultant/g) ?? []).length, 2);
    assert.match(html, /stated problems haven&#39;t been recorded/);
  });

  test("names no one, and lists domains nobody assessed as making no claim", () => {
    const html = fullReportHtml(buildReport({ ...full, findings: full.findings.filter((f) => f.domain !== "forecast") }), pseudonyms).replace(/url\(data:[^)]+\)/g, "");
    // The customer's own name belongs on the cover. People never appear.
    for (const entry of pseudonyms.entries()) if (entry.token.startsWith("[[user:") && entry.name.includes(" ") && entry.name !== "Unknown user") assert.ok(!html.includes(entry.name), entry.name);
    assert.doesNotMatch(html, /\[\[/);
    assert.match(html, /Where we can&#39;t give a view yet/);
    assert.match(html, /Not part of this health check\./);
  });
});

describe("where the complexity lives", () => {
  const slideOf = (html: string): string => html.split('<section class="page').find((page) => page.includes("Where the complexity lives"))!;

  /** The sample's inventory, with more custom objects than the slide can hold. */
  function crowded(extra: number): ReportInput {
    const metrics = full.metrics.map((m): Metric => {
      if (m.id !== OBJECT_INVENTORY || m.status !== "measured") return m;
      const names = Array.from({ length: extra }, (_, i) => `Customer_Onboarding_Checklist_Item_${String(i + 1).padStart(2, "0")}__c`);
      const added = names.flatMap((key, i) => INVENTORY_COLUMNS.map((c) => ({ dimension: c.dimension, key, n: i, value: i, notMeasurableReason: null })));
      return { ...m, value: m.value + extra, n: m.n + extra, segments: [...m.segments, ...added] };
    });
    return { ...full, metrics };
  }

  test("sits after configuration's issues, busiest object first", () => {
    const html = fullReportHtml(buildReport(full), pseudonyms);
    const slide = slideOf(html);
    assert.ok(slide, "the slide is there");
    assert.match(slide, /<div class="eyebrow">Configuration<\/div>/);
    assert.match(slide, /We counted 7 objects: your 4 core objects and 3 custom objects of your own with records\. Of the 14 rules, flows and triggers on them, 8 have no description\./);
    const rows = [...slide.matchAll(/<tr><td>([^<]+)<\/td>/g)].map((m) => m[1]);
    assert.deepEqual(rows, ["Opportunity", "Invoice__c", "Lead", "Account", "Project__c", "Contact", "Project_Line__c"]);
    assert.doesNotMatch(slide, /Only the busiest are shown/);
  });

  test("moves to automation when configuration wasn't assessed", () => {
    const html = fullReportHtml(buildReport({ ...full, findings: full.findings.filter((f) => f.domain !== "configuration") }), pseudonyms);
    assert.match(slideOf(html), /<div class="eyebrow">Automation<\/div>/);
  });

  test("a long list is cut to the busiest, says so, and nothing overflows", async () => {
    const input = crowded(20);
    const report = buildReport(input);
    const html = fullReportHtml(report, pseudonyms);
    const slide = slideOf(html);
    assert.equal([...slide.matchAll(/<tr><td>/g)].length, INVENTORY_SLIDE_ROWS);
    assert.match(slide, /Only the busiest are shown here\. The raw notes list every object\./);
    const { layout } = await renderPdf(browser, html, { ...deckRange, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("a not measurable inventory says why, rather than showing an empty table", () => {
    const metrics = full.metrics.map((m): Metric => (m.id === OBJECT_INVENTORY ? { id: m.id, domain: m.domain, label: m.label, definition: m.definition, unit: m.unit, evidence: m.evidence, exclusions: m.exclusions, assumptions: m.assumptions, status: "not_measurable", reason: "Object metadata couldn't be retrieved: timed out" } : m));
    const slide = slideOf(fullReportHtml(buildReport({ ...full, metrics }), pseudonyms));
    assert.match(slide, /Not measurable\.<\/strong> Object metadata couldn&#39;t be retrieved: timed out/);
    assert.doesNotMatch(slide, /<table/);
  });
});

describe("a large, messy org", () => {
  const LONG = " This sentence is here to make the summary as long as a real one can get, with a good deal of explanation about what we saw.";

  function messy(): ReportInput {
    const issues = output!.findings.filter((f) => f.status === "issue");
    const findings: Finding[] = [];
    SALES_CLOUD_DOMAINS.forEach((domain, d) => {
      for (let i = 0; i < 12; i++) {
        const source = issues[(d * 12 + i) % issues.length]!;
        findings.push({ ...source, id: `f-${String(d).padStart(2, "0")}${String(i).padStart(10, "0")}`, domain, severity: (["critical", "high", "medium", "low"] as const)[i % 4]!, summary: source.summary + (i % 3 === 0 ? LONG : ""), recommendation: `${source.recommendation} (${domain} ${i === 0 ? "first" : "next"})` });
      }
    });
    return { ...full, findings: [...findings, ...output!.findings.filter((f) => f.status !== "issue")] };
  }

  test("still fits 60 slides, and nothing overflows", async () => {
    const report = buildReport(messy());
    const { layout } = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("a moderately large org keeps its detail, and nothing overflows", async () => {
    const input = messy();
    const report = buildReport({ ...input, findings: input.findings.filter((f) => f.status !== "issue" || Number(f.id.slice(-2)) < 7) });
    const html = fullReportHtml(report, pseudonyms);
    assert.doesNotMatch(html, /raw notes have all of it/, String(slideCounts(report, pseudonyms)));
    assertBothColumnsUsed(html);
    const { layout } = await renderPdf(browser, html, { ...deckRange });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("the deck has a ceiling however large the org is", async () => {
    const input = messy();
    const issues = input.findings.filter((f) => f.status === "issue");
    const huge = Array.from({ length: 8 }, (_, n) => issues.map((f) => ({ ...f, id: `${f.id}${n}`, recommendation: `${f.recommendation} ${n}` }))).flat();
    const report = buildReport({ ...input, findings: huge });
    const { layout } = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
    assert.match(fullReportHtml(report, pseudonyms), /raw notes have all of it/);
  });

  test("the executive summary still fits", async () => {
    const report = buildReport(messy());
    const { layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { ...execRange });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("the 90-day plan stays on its pages however long the recommendations get", async () => {
    const input = messy();
    const LONGER = " Agree an owner and a date for each part, and check on it at the next review.";
    for (const times of [1, 2, 4]) {
      const findings = input.findings.map((f) => (f.status === "issue" ? { ...f, recommendation: f.recommendation + LONGER.repeat(times) } : f));
      const report = buildReport({ ...input, findings });
      const html = executiveHtml(report, pseudonyms);
      const { layout } = await renderPdf(browser, html, { ...execRange, dataset: report.dataset });
      assert.deepEqual(layout.problems, [], `${times}: ${describeProblems(layout.problems)}`);
      const items = [...html.matchAll(/<section class="phase">[\s\S]*?<\/section>/g)].flatMap((m) => m[0].split("<li>").slice(1));
      assert.ok(items.length >= 3, `${times}: every phase keeps at least one action`);
      for (const item of items) assert.match(item, /^<strong>/, "every plan action leads with its issue");
      assert.match(html, /The full report lists the rest of this phase\./);
    }
  });
});

describe("a thin org", () => {
  test("gets a shorter deck flagged for review, never padding", async () => {
    // Nothing found and nothing measured: pages that say nothing are left out, so the deck is short.
    const report = buildReport({ ...full, findings: [], metrics: [] });
    const { layout } = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange });
    assert.ok(layout.pages < DECK_RANGE.min);
    assert.deepEqual([...new Set(layout.problems.map((p) => p.check))], ["page count"], describeProblems(layout.problems));
  });

  test("a clean org's executive summary still renders", async () => {
    const report = buildReport({ ...base, findings: output!.findings.filter((f) => f.status === "passed") });
    const { layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: 4, maxPages: 6 });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });
});

describe("the layout checks fire", () => {
  const page = (inner: string, n = 1): string => `<section class="page" data-page="${n}">${inner}</section>`;
  const ok = "<p>A perfectly ordinary paragraph with enough words in it to count as content.</p>";
  const footer = '<div class="footer" data-chrome style="bottom:20px"><span>Footer</span><span>1</span></div>';
  const doc = (...pages: string[]): string => document("Test", "a4", "", pages);
  const range = { minPages: 1, maxPages: 3 };
  const checks = async (html: string, expectation = range): Promise<string[]> => (await layoutOf(browser, html, expectation)).problems.map((p) => p.check);

  test("a good page passes", async () => {
    assert.deepEqual(await checks(doc(page(ok))), []);
  });

  test("overflow off the bottom", async () => {
    assert.ok((await checks(doc(page(`${ok}<div style="height:2000px;background:#000"></div>`)))).includes("overflow"));
  });

  test("overflow off the side", async () => {
    assert.ok((await checks(doc(page(`${ok}<div style="width:3000px;height:10px"></div>`)))).includes("overflow"));
  });

  test("a long unbroken string", async () => {
    assert.ok((await checks(doc(page(`${ok}<p style="white-space:nowrap">${"word ".repeat(200)}</p>`)))).includes("overflow"));
  });

  test("content running into the footer", async () => {
    assert.ok((await checks(doc(page(`${ok}<div style="height:1040px"></div>${footer}`)))).includes("overflow"));
  });

  test("a chart slot with nothing in it", async () => {
    assert.ok((await checks(doc(page(`${ok}<figure data-expect-chart="funnel"></figure>`)))).includes("missing chart"));
  });

  test("a chart that drew no marks", async () => {
    assert.ok((await checks(doc(page(`${ok}<figure data-expect-chart="funnel"><svg width="400" height="200"></svg></figure>`)))).includes("missing chart"));
  });

  test("a chart gap with its reason is fine", async () => {
    assert.deepEqual(await checks(doc(page(`${ok}<figure data-expect-chart="funnel"><div class="chart-gap">No closed deals.</div></figure>`))), []);
  });

  test("an empty section", async () => {
    assert.ok((await checks(doc(page(ok), page("<h2>Title only</h2>", 2)))).includes("empty section"));
  });

  test("a page holding only its footer", async () => {
    assert.ok((await checks(doc(page(footer)))).includes("empty section"));
  });

  test("a raw ISO date", async () => {
    assert.ok((await checks(doc(page(`${ok}<p>From 2024-10-31 onwards.</p>`)))).includes("raw date"));
    assert.deepEqual(await checks(doc(page(`${ok}<p>Run hc-2026-10-31-ab1301a5.</p>`))), []);
  });

  test("a leftover token, or a template gap", async () => {
    for (const bad of ["[[user:U0001]] did it", "Value: undefined", "Win rate NaN%", "[object Object]", "Owner: null"]) {
      assert.ok((await checks(doc(page(`${ok}<p>${bad}</p>`)))).includes("leftover"), bad);
    }
  });

  test("the wrong number of pages", async () => {
    assert.ok((await checks(doc(page(ok)), { minPages: 2, maxPages: 3 })).includes("page count"));
  });

  test("a font that didn't load", async () => {
    const html = doc(page(ok)).replace(/@font-face \{[^}]*\}/g, "");
    assert.ok((await checks(html)).includes("font"));
  });

  test("an invented figure on the page, even after every earlier gate", async () => {
    const report = buildReport(full);
    const html = executiveHtml(report, pseudonyms).replace("Issues</span>", "Issues, worth £9,123,456</span>");
    const { layout } = await renderPdf(browser, html, { ...execRange, dataset: report.dataset });
    assert.ok(layout.problems.some((p) => p.check === "number check" && p.detail.includes("9,123,456")), describeProblems(layout.problems));
  });
});

describe("escaping", () => {
  test("a name or label can't inject markup into a page", () => {
    const p = new Pseudonymiser([{ token: "[[user:U0001]]", id: "005000000000001AAA", name: "<img src=x onerror=alert(1)>", aliases: [] }]);
    const f: Finding = { ...issue, title: "Issue for [[team:T0001]]", owner: "<b>Sales</b>", summary: "Rep <script>alert(1)</script> did this." };
    const html = executiveHtml(buildReport({ ...base, findings: [f] }), p).replace(/url\(data:[^)]+\)/g, "");
    assert.doesNotMatch(html, /<script|<b>Sales/);
    assert.match(html, /&lt;script&gt;/);
  });

  test("rendering can't reach the network", async () => {
    const html = document("Test", "a4", "", ['<section class="page"><p>A paragraph with enough words to count as content on its page.</p><img src="https://example.com/x.png" alt=""></section>']);
    const context = await browser.newContext({ offline: true });
    await context.close();
    const { layout } = await renderPdf(browser, html, { minPages: 1, maxPages: 1 });
    assert.deepEqual(layout.problems, []);
  });
});

describe("rule 10", () => {
  function approveAll(report: BuiltReport): void {
    for (const built of Object.values(report.outputs)) {
      for (const section of built.sections as ReportSection[]) {
        section.status = "approved";
        section.approval = { approvedBy: "Synthetic Consultant", approvedAt: "2026-10-31T14:00:00.000Z" };
      }
    }
  }

  test("an unapproved report is marked as a draft on every page", () => {
    const html = executiveHtml(buildReport(full), pseudonyms);
    assert.equal((html.match(/Draft, not approved for release/g) ?? []).length, (html.match(/class="page /g) ?? []).length);
  });

  test("the mark goes only when every section is approved", () => {
    const report = buildReport(full);
    approveAll(report);
    assert.doesNotMatch(executiveHtml(report, pseudonyms), /Draft, not approved/);
    assert.doesNotMatch(fullReportHtml(report, pseudonyms), /Draft, not approved/);
    for (const [key, html] of [["executive_summary", executiveHtml], ["full_health_check", fullReportHtml]] as const) {
      const section = report.outputs[key].sections[1]!;
      section.status = "draft";
      section.approval = null;
      assert.match(html(report, pseudonyms), /Draft, not approved/);
    }
  });
});

describe("overrides", () => {
  test("a report shows the corrected figure", () => {
    const f = output!.findings.find((x) => /should already have closed/.test(x.title))!;
    const override: NumberOverride = { id: "o1", runId: run.id, target: { type: "finding", id: f.id, field: "facts.rate" }, previousValue: 27.6, newValue: 19.5, reason: "Two were test records.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-10-31T13:00:00.000Z" };
    const html = executiveHtml(buildReport({ ...full, overrides: [override] }), pseudonyms);
    assert.match(html, /19\.5%/);
    assert.doesNotMatch(html, /27\.6%/);
  });
});

describe("a run folder", () => {
  test("gets all its documents, written privately, and the checks recorded", async () => {
    writeFileSync(join(dir, "intake.json"), JSON.stringify(intake));
    const result = await renderRun(dir);
    assert.equal(result.passed, true, JSON.stringify(result.layout));
    for (const file of Object.values(OUTPUT_FILES)) {
      const path = join(dir, file);
      assert.ok(statSync(path).size > 0, file);
      assert.equal(statSync(path).mode & 0o777, 0o600, file);
    }
    const layout = JSON.parse(readFileSync(join(dir, OUTPUT_FILES.layout), "utf8")) as { executive: { passed: boolean }; full: { passed: boolean } };
    assert.ok(layout.executive.passed && layout.full.passed);
  });

  test("hc render does the same from the command line", () => {
    const result = spawnSync(process.execPath, [fileURLToPath(new URL("../../cli/hc.ts", import.meta.url)), "render", "--run", dir], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stdout + result.stderr);
    assert.match(result.stdout, /full: \d+ pages/);
  });

  test("Claude's drafts reach the page, and rejected ones are recorded", async () => {
    const client: NarrativeClient = {
      complete: async ({ prompt, model }) => ({ stopReason: "end_turn", model, text: JSON.stringify({ paragraphs: [{ text: prompt.startsWith("Section: Verdict") ? "We think your pipeline needs attention first." : "You could save £9,999,999.", sources: [] }] }) }),
    };
    const result = await renderRun(dir, { client });
    assert.equal(currentBody(result.report.outputs.executive_summary, "verdict"), "We think your pipeline needs attention first.");
    assert.ok(result.rejections.length > 0);
    assert.equal(JSON.parse(readFileSync(join(dir, OUTPUT_FILES.rejections), "utf8")).length, result.rejections.length);
    assert.equal(result.passed, true, JSON.stringify(result.layout));
  });

  test("refuses to write inside a git work tree", async () => {
    await assert.rejects(() => renderRun(fileURLToPath(new URL("../../..", import.meta.url))), /Refusing to write/);
  });

  test("a damaged findings file stops the render", async () => {
    const broken = mkdtempSync(join(tmpdir(), "hc-render-"));
    for (const file of ["run.json", "calibration.json", "metrics.json", "manual-steps.json", "pseudonym-map.json"]) writeFileSync(join(broken, file), readFileSync(join(dir, file)));
    writeFileSync(join(broken, "findings.json"), JSON.stringify([{ id: "f" }]));
    await assert.rejects(() => renderRun(broken));
  });
});
