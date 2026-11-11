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
import { buildPlan, domainLabel, topIssues, verdictOf } from "../../narrative/select.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { SALES_CLOUD_DOMAINS, type Finding, type Metric, type NumberOverride, type ReportSection } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { EXECUTIVE_PLAN_ITEMS, executiveHtml, executiveMaxPages } from "../executive.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { syntheticInterviews } from "../../testing/interviews.ts";
import { compactMoney, plainWords, ukDates } from "../format.ts";
import { yearlyByFinding } from "../../value/model.ts";
import { DECK_RANGE, deckRangeFor, fullReportHtml, fullReportHtmlAt, INVENTORY_SLIDE_ROWS, slideCounts } from "../full-report.ts";
import { INVENTORY_COLUMNS, OBJECT_INVENTORY } from "../../domains/configuration/inventory.ts";
import { document } from "../html.ts";
import { layoutOf, launchBrowser, pdfPageCount, renderPdf } from "../pdf.ts";
import { EXECUTIVE_PAGES, OUTPUT_FILES, renderRun } from "../render-run.ts";
import { heldFigures } from "../render-released.ts";
import { carryOver } from "../carry-over.ts";
import { storageSentences } from "../../progress/compare.ts";

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
  test("comes to about 12 pages, with every layout check and number check passing", async () => {
    const report = buildReport(full);
    const { pdf, layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { ...execRange, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
    assert.equal(pdfPageCount(pdf), layout.pages);
    assert.equal(pdf.subarray(0, 5).toString(), "%PDF-");
    assert.ok(layout.pages >= 8 && layout.pages <= 13, `${layout.pages} pages`);
  });

  test("holds a verdict, scorecard, top five with value, and the plan", () => {
    const report = buildReport(full);
    const html = executiveHtml(report, pseudonyms);
    for (const text of ["Verdict", verdictOf(report.teamFindings).headline, "How each area stands", "Your pipeline today", "Potential revenue in open deals", "Top 5 issues", "Days 1 to 30", "Days 31 to 60", "Days 61 to 90", "Synthetic Co"]) assert.ok(html.includes(text), text);
    const yearly = yearlyByFinding(report.value);
    for (const f of topIssues(report.teamFindings, 5)) {
      // A segment's gap is shown as the value model's year (DECISIONS.md 405).
      const value = f.value.kind === "not_sized" ? "Not sized" : f.ruleId === "pipe.segment-divergence" ? `About ${compactMoney(yearly.get(f.id)!, "GBP")} a year ${f.value.period.split(",")[0]}` : ukDates(`£${f.value.amount.toLocaleString("en-GB")} ${f.value.period}`);
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
    // Every action on the plan's pages, urgent or quick win, leads with its issue (the quick wins' box is sized to fit, DECISIONS.md 366).
    const planPages = html.slice(html.indexOf('<div class="eyebrow">90-day plan</div>'), html.indexOf('<div class="eyebrow">Next steps</div>'));
    const items = planPages.match(/<li>/g)?.length ?? 0;
    assert.ok(items >= Math.min(plan[0]!.findings.length, EXECUTIVE_PLAN_ITEMS));
    assert.equal(planPages.match(/<li><strong>[^<]+\.<\/strong>/g)?.length, items, "every plan action leads with its issue");
    assert.match(planPages, /Quick wins, also in days 1 to 30/);
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
  test("is 16:9, within its slide range, with every check passing", async () => {
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
    // By label, as the org shows them (DECISIONS.md 412).
    assert.deepEqual(rows, ["Opportunity", "Invoice", "Lead", "Account", "Project", "Contact", "Project Line"]);
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

  test("still fits its slide ceiling, and nothing overflows", async () => {
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

describe("an issue with a long money working, alone on its slide (DECISIONS.md 416)", () => {
  // As a real org's weighting can run: one line for each stage its deals sit in.
  const STAGES = ["Prospecting", "Qualified", "Discovery", "Solution design", "Proposal", "Commercials", "Negotiation", "Legal review", "Procurement", "Contracting"];
  function longWorking(): { input: ReportInput; finding: Finding } {
    const source = output!.findings.find((f) => f.ruleId === "dq.opps-inactive-owner" && f.status === "issue")!;
    assert.ok(source.value.kind === "revenue");
    const assumptions = [
      ...source.value.assumptions.slice(0, 2),
      ...STAGES.map((stage, i) => `${i + 1} deals with £${(i + 3) * 17_000} weighted at ${40 + i}%: of ${300 + i * 7} deals closed 2024-10-31 to 2026-10-31 that were in the ${stage} stage at some point, ${120 + i * 5} went on to win`),
      "Lost and no decision deals both count as not won",
    ];
    // Critical, so it has a slide of its own at every step but the last, which sets every issue as a card.
    const finding: Finding = { ...source, severity: "critical", value: { ...source.value, assumptions } };
    return { input: { ...full, findings: output!.findings.map((f) => (f.id === source.id ? finding : f)) }, finding };
  }

  test("fits at every density step, with the money's reference in sight", async () => {
    const { input, finding } = longWorking();
    const report = buildReport(input);
    const steps = slideCounts(report, pseudonyms).length;
    for (let step = 0; step < steps; step++) {
      const html = fullReportHtmlAt(report, pseudonyms, step);
      const { layout } = await renderPdf(browser, html, { minPages: 1, maxPages: 999, dataset: report.dataset });
      assert.deepEqual(layout.problems, [], `step ${step}: ${describeProblems(layout.problems)}`);
      // A slide, card or table row of its own, by its title.
      const slide = html.split("<section ").find((s) => s.includes(`>${finding.title.replace(/'/g, "&#39;")}<`))!;
      assert.ok(slide, `step ${step}: the issue has a slide`);
      assert.match(slide, /<span class="ref" data-ref>R\d+\.\d+<\/span>/, `step ${step}: the money's reference`);
      if (slide.includes('class="panel"')) assert.match(slide, /<strong>[^<]+<\/strong><span class="ref" data-ref>R\d+\.\d+<\/span><\/p><p class="note">[^]*The full working is at<span class="ref" data-ref>R\d+\.\d+<\/span>\./, `step ${step}: the figure keeps its reference, and the cut working says where the rest is`);
    }
  });

  test("a working that fits is printed whole", () => {
    const { input, finding } = longWorking();
    assert.ok(finding.value.kind === "revenue");
    const short = { ...finding, value: { ...finding.value, assumptions: finding.value.assumptions.slice(0, 3) } };
    const html = fullReportHtmlAt(buildReport({ ...input, findings: input.findings.map((f) => (f.id === finding.id ? short : f)) }), pseudonyms, 0);
    const slide = html.split("<section ").find((x) => x.includes(`<h2>${finding.title.replace(/'/g, "&#39;")}</h2>`))!;
    assert.doesNotMatch(slide, /The full working is at/);
    assert.ok(slide.includes("Lost and no decision deals") === false && slide.includes("Raw exposure"), "its three lines, all of them");
  });
});

describe("a re-check with an intake and interviews (DECISIONS.md 418)", () => {
  // As the sample's re-check: the first check's intake and interviews, and three issues fixed since.
  function recheck(): BuiltReport {
    const input: ReportInput = { ...full, interviews: syntheticInterviews() };
    const first = buildReport(input);
    const fixed = new Set(first.teamFindings.filter((f) => f.status === "issue").map((f) => f.ruleId).slice(0, 3));
    const later = output!.findings.map((f): Finding => (fixed.has(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
    return buildReport({ ...input, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: input });
  }

  test("the executive summary fits its pages, the plan's included, with every check passing", async () => {
    const report = recheck();
    assert.ok(report.progress, "a re-check");
    const html = executiveHtml(report, pseudonyms);
    assert.match(html, /What we&#39;d do first|What we'd do first/);
    const { layout } = await renderPdf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("so does the full report", async () => {
    const report = recheck();
    const { layout } = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange, dataset: report.dataset });
    assert.deepEqual(layout.problems, [], describeProblems(layout.problems));
  });

  test("the slide's heading sits on the page, spaced as it was inside the progress block (DECISIONS.md 505)", async () => {
    // Inside the block, the pair's top margin folded into the heading's; beside it, the heading's alone sets the gap.
    const report = recheck();
    for (const [deck, html] of [["executive", executiveHtml(report, pseudonyms)], ["full report", fullReportHtml(report, pseudonyms)]] as const) {
      const page = await browser.newPage();
      try {
        await page.setContent(html, { waitUntil: "load" });
        await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
        await page.emulateMedia({ media: "print" });
        const gaps = await page.evaluate(() => {
          const slide = [...window.document.querySelectorAll<HTMLElement>(".page")].find((p) => p.querySelector(":scope > .eyebrow")?.textContent === "Since your last health check")!;
          const [eyebrow, h2, block] = [...slide.children] as HTMLElement[];
          const box = (el: Element) => el.getBoundingClientRect();
          const pair = slide.querySelector(".progress > .pair")!;
          return {
            order: [eyebrow!.className, h2!.tagName, block!.className],
            top: box(eyebrow!).top - box(slide).top - parseFloat(getComputedStyle(slide).paddingTop),
            headingGap: box(h2!).top - box(eyebrow!).bottom - parseFloat(getComputedStyle(h2!).marginTop),
            pairGap: box(pair).top - box(h2!).bottom - parseFloat(getComputedStyle(h2!).marginBottom),
          };
        });
        assert.deepEqual(gaps.order, ["eyebrow", "H2", "progress"], deck);
        for (const [name, gap] of [["top", gaps.top], ["eyebrow to heading", gaps.headingGap], ["heading to figures", gaps.pairGap]] as const) assert.ok(Math.abs(gap) < 0.01, `${deck}, ${name}: ${gap}px more than the margins`);
      } finally {
        await page.close();
      }
    }
  });

  test("both still fit with a storage sentence for each kind on the progress page (DECISIONS.md 500)", async () => {
    // The earlier check read its limits 118 days before, with less of each kind used: a rise
    // with an average is the longest sentence, so this is the progress page at its fullest.
    const plain = recheck();
    const limitsAt = (iso: string) => ({ ...plain.input.run, steps: plain.input.run.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt: iso } : s)) });
    const less = (f: Finding): Finding => ({ ...f, facts: { ...f.facts, DataStorageMBUsed: f.facts.DataStorageMBUsed! - 470, FileStorageMBUsed: f.facts.FileStorageMBUsed! - 520 } });
    const earlier: ReportInput = { ...plain.input.baseline!, run: { ...limitsAt("2026-07-14T12:00:00.000Z"), id: run.id, baselineRunId: null }, findings: plain.input.baseline!.findings.map((f) => (f.ruleId === "tech.storage-use" ? less(f) : f)) };
    const report = buildReport({ ...plain.input, run: limitsAt("2026-11-09T12:00:00.000Z"), baseline: earlier });
    const html = executiveHtml(report, pseudonyms);
    assert.match(html, /Data storage: [\d,]+ of [\d,]+ MB used then, [\d,]+ of [\d,]+ MB now, up 470 MB in 118 days \(an average of 119 MB every 30 days\)\./);
    assert.match(html, /File storage: [\d,]+ of [\d,]+ MB used then, [\d,]+ of [\d,]+ MB now, up 520 MB in 118 days \(an average of 132 MB every 30 days\)\./);
    const exec = await renderPdf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset });
    assert.deepEqual(exec.layout.problems, [], describeProblems(exec.layout.problems));
    const deck = await renderPdf(browser, fullReportHtml(report, pseudonyms), { ...deckRange, dataset: report.dataset });
    assert.deepEqual(deck.layout.problems, [], describeProblems(deck.layout.problems));
  });
});

describe("the progress page at its fullest with the engine's own titles, and past it with the customer's (DECISIONS.md 501 and 505)", () => {
  // The longest titles the engine writes itself, six in each of the three lists the
  // page names (each longer than it names), one rule that can't be told, area
  // scores that move, and both storage sentences at their longest: six-digit
  // figures, allowances that differ between the runs, a rise with an average.
  // A title carrying the customer's own words, such as a team's name, has no
  // longest: the last case plants one and the slide carries on (DECISIONS.md 505).
  const STORAGE = "tech.storage-use";
  const longest = [...new Map(output!.findings.filter((f) => f.status === "issue" && f.ruleId !== STORAGE).map((f) => [f.ruleId, f])).values()].sort((a, b) => b.title.length - a.title.length);
  const fixed = new Set(longest.filter((_, i) => i % 3 === 0).slice(0, 6).map((f) => f.ruleId));
  const added = new Set(longest.filter((_, i) => i % 3 === 1).slice(0, 6).map((f) => f.ruleId));
  const unknown = longest.filter((_, i) => i % 3 === 2)[6]!.ruleId;
  // Issues first, longest title first, so the titles the page names are the longest.
  const ordered = (findings: Finding[]): Finding[] => [...findings].sort((a, b) => (b.status === "issue" ? b.title.length : 0) - (a.status === "issue" ? a.title.length : 0));
  const passed = (f: Finding): Finding => ({ ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } });
  const limitsAt = (iso: string, extra: Partial<typeof run> = {}) => ({ ...run, ...extra, steps: run.steps.map((s) => (s.id === "api-limits" ? { ...s, finishedAt: iso } : s)) });
  /** `planted`: a rule fixed since the earlier check, one of its issues then (titled `replacing`) titled in the customer's words. */
  const storage = (then: Record<string, number>, now: Record<string, number>, planted?: { ruleId: string; replacing: string; title: string }): BuiltReport => {
    const input: ReportInput = { ...full, interviews: syntheticInterviews() };
    const fixedNow = planted ? new Set([...fixed, planted.ruleId]) : fixed;
    const first = planted && output!.findings.find((f) => f.ruleId === planted.ruleId && f.status === "issue" && f.title === planted.replacing);
    assert.ok(!planted || first, "the planted title replaces one the synthetic run has");
    const before = ordered(output!.findings.map((f) => (f.ruleId === STORAGE ? { ...f, facts: then } : f === first ? { ...f, title: planted!.title } : added.has(f.ruleId) && f.status === "issue" ? passed(f) : f)));
    const later = ordered(
      output!.findings.map((f): Finding =>
        f.ruleId === STORAGE ? { ...f, facts: now } : fixedNow.has(f.ruleId) && f.status === "issue" ? passed(f) : f.ruleId === unknown && f.status === "issue" ? { ...f, status: "not_measurable", notMeasurableReason: "Couldn't be read this time.", value: { kind: "not_sized", reason: "Not measured." } } : f,
      ),
    );
    const earlier: ReportInput = { ...input, run: limitsAt("2026-07-14T12:00:00.000Z"), findings: before, scores: scoreDomains(run.modules, before) };
    return buildReport({ ...input, run: limitsAt("2026-11-09T12:00:00.000Z", { id: `${run.id}-recheck`, baselineRunId: run.id }), findings: later, scores: scoreDomains(run.modules, later), baseline: earlier });
  };
  const kinds = (data: [number, number], file: [number, number]) => ({
    DataStorageMBUsed: data[0], DataStorageMBMax: data[1], DataStorageMBShare: Math.round((data[0] / data[1]) * 1000) / 10,
    FileStorageMBUsed: file[0], FileStorageMBMax: file[1], FileStorageMBShare: Math.round((file[0] / file[1]) * 1000) / 10,
  });
  const cases: Array<[string, BuiltReport, RegExp]> = [
    ["a rise in both, with allowances that changed", storage(kinds([812_345, 900_000], [712_345, 800_000]), kinds([912_345, 950_000], [812_345, 850_000])), /Data storage: 812,345 of 900,000 MB used then, 912,345 of 950,000 MB now, up 100,000 MB in 118 days \(an average of 25,424 MB every 30 days\)\. File storage: /],
    ["both past their allowance now", storage(kinds([812_345, 900_000], [712_345, 800_000]), kinds([962_345, 950_000], [862_345, 850_000])), /Data storage: 812,345 of 900,000 MB used then, 962,345 MB used now, over the 950,000 MB allowance, up 150,000 MB in 118 days \(an average of 38,136 MB every 30 days\)\. File storage: /],
  ];

  for (const [name, report, sentences] of cases) {
    test(`${name}: both decks fit as printDocuments renders them, with nothing carried onto a continued page`, async () => {
      const p = report.progress!;
      for (const list of [p.fixed, p.remaining, p.added]) assert.ok(list.length > 5, "each list is longer than the page names");
      assert.ok(p.unknown.length > 0);
      assert.ok(p.areas.filter((a) => a.before !== null && a.after !== null && a.before !== a.after).length >= 4, "four area moves");
      const range = deckRangeFor(report.input.run);
      for (const [deck, html, pages] of [["executive", executiveHtml(report, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report) }], ["full report", fullReportHtml(report, pseudonyms), { minPages: range.min, maxPages: range.max }]] as const) {
        assert.match(html, sentences, deck);
        const printed = await renderPdf(browser, html, { ...pages, dataset: report.dataset, carryOver: true });
        assert.deepEqual(printed.layout.problems, [], `${deck}: ${describeProblems(printed.layout.problems)}`);
        // Laid out with nothing carried over, the progress page still fits: the slide holds it all.
        const alone = await layoutOf(browser, html, { ...pages, dataset: report.dataset });
        assert.deepEqual(alone.problems, [], `${deck}: ${describeProblems(alone.problems)}`);
        assert.equal(alone.pages, printed.layout.pages, deck);
      }
    });
  }

  // The adversary's case: both kinds past their allowance at six digits, and a team
  // named in the Fixed list's first title (pipe.segment-divergence words it so).
  const TEAM_TITLE = "Slippage rate for the Enterprise Sales North America team is well above the overall figure";
  const CLOSING = "Some of the change in money can come from the business itself, not only from fixes.";
  const SEGMENT = "pipe.segment-divergence";
  const planted = storage(kinds([812_345, 900_000], [712_345, 800_000]), kinds([962_345, 950_000], [862_345, 850_000]), { ruleId: SEGMENT, replacing: "Win rate for the Sales EU team is well below the overall figure", title: TEAM_TITLE });
  // The Fixed list follows the earlier check's ranking; first, as it is when that rule ranked highest.
  const customer: BuiltReport = { ...planted, progress: { ...planted.progress!, fixed: [...planted.progress!.fixed].sort((a, b) => Number(b.ruleId === SEGMENT) - Number(a.ruleId === SEGMENT)) } };

  /** Each slide of the progress page once carried over as printDocuments does: its eyebrow, heading, and the text under them. */
  async function progressSlides(html: string): Promise<{ pages: number; slides: Array<{ eyebrow: string; heading: string; notes: string[] }> }> {
    const context = await browser.newContext({ offline: true });
    try {
      const page = await context.newPage();
      await page.setContent(html, { waitUntil: "load" });
      await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
      await page.emulateMedia({ media: "print" });
      await carryOver(page);
      return await page.evaluate(() => {
        const pages = [...window.document.querySelectorAll<HTMLElement>(".page")];
        const first = pages.findIndex((p) => p.querySelector("h2")?.textContent === "What has changed");
        const slides = [];
        // The slide, then each page carried on from it: one with a continued heading, or with none at all.
        for (let i = first; i >= 0 && i < pages.length && pages[i]!.querySelector(".progress"); i++) {
          const p = pages[i]!;
          const heading = p.querySelector("h2")?.textContent ?? "";
          if (i > first && heading && !/, continued$/.test(heading)) break;
          slides.push({ eyebrow: p.querySelector(".eyebrow")?.textContent ?? "", heading, notes: [...p.querySelectorAll(".progress > p.note")].map((n) => n.textContent ?? "") });
        }
        return { pages: pages.length, slides };
      });
    } finally {
      await context.close();
    }
  }

  test("past its fullest, with a title in the customer's own words: the closing note goes to a continued slide that carries the heading (DECISIONS.md 505)", async () => {
    const p = customer.progress!;
    assert.equal(p.fixed[0]!.title, TEAM_TITLE, "the Fixed list opens with the customer's title");
    for (const list of [p.fixed, p.remaining, p.added]) assert.ok(list.length >= 6, "six or more rules a list");
    assert.equal(p.unknown.length, 1);
    const stored = storageSentences(p).join(" ");
    assert.match(stored, /962,345 MB used now, over the 950,000 MB allowance.* 862,345 MB used now, over the 850,000 MB allowance/);
    const range = deckRangeFor(customer.input.run);
    const html = fullReportHtml(customer, pseudonyms);
    const printed = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: customer.dataset, carryOver: true });
    assert.equal(printed.layout.passed, true);
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    const carried = await progressSlides(html);
    assert.equal(carried.pages, printed.layout.pages);
    // What the customer reads: the slide whole, storage included, then the closing note under the slide's heading.
    assert.deepEqual(
      carried.slides.map((s) => [s.eyebrow, s.heading]),
      [
        ["Since your last health check", "What has changed"],
        ["Since your last health check", "What has changed, continued"],
      ],
      JSON.stringify(carried.slides),
    );
    assert.ok(carried.slides[0]!.notes.includes(stored), "the storage paragraph whole on the first slide");
    assert.ok(!carried.slides[0]!.notes.includes(CLOSING));
    assert.deepEqual(carried.slides[1]!.notes, [CLOSING]);
    // The same case with no storage sentence: the slide holds it all, so storage costs exactly the one slide.
    const without: BuiltReport = { ...customer, progress: { ...p, storage: null } };
    const withoutHtml = fullReportHtml(without, pseudonyms);
    const plain = await renderPdf(browser, withoutHtml, { minPages: range.min, maxPages: range.max, dataset: without.dataset, carryOver: true });
    assert.deepEqual(plain.layout.problems, [], describeProblems(plain.layout.problems));
    assert.equal(printed.layout.pages, plain.layout.pages + 1);
    assert.deepEqual((await progressSlides(withoutHtml)).slides.map((s) => s.heading), ["What has changed"]);
  });

  test("past its fullest, with a title in the customer's own words: the executive summary keeps within its re-check ceiling", async () => {
    const printed = await renderPdf(browser, executiveHtml(customer, pseudonyms), { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(customer), dataset: customer.dataset, carryOver: true });
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    assert.ok(printed.layout.pages <= executiveMaxPages(customer), `${printed.layout.pages} pages`);
  });

  // Longer still, a list itself splits across the two slides, and what goes over
  // keeps its label (DECISIONS.md 507 and 508). Titles grow as a long team name makes them.
  const TEAMS = " for the Enterprise Sales North America and Asia Pacific Partner Channel teams";
  const longer = (lists: Array<"fixed" | "remaining" | "added">, extra: number): BuiltReport => {
    const p = customer.progress!;
    const tail = TEAMS.repeat(Math.ceil(extra / TEAMS.length)).slice(0, extra);
    return { ...customer, progress: { ...p, ...Object.fromEntries(lists.map((k) => [k, p[k].map((c) => ({ ...c, title: c.title + tail }))])) } };
  };
  type Column = { label: string | null; first: string | null; items: string[] };
  type ListSlide = { eyebrow: string; heading: string; columns: Column[] };

  /** The progress slide's lists as built, then the slide and its continued slide once carried over: each column's label, what opens it, and its items without their references. */
  async function carriedLists(html: string): Promise<{ built: ListSlide; slides: ListSlide[]; continued: string }> {
    const context = await browser.newContext({ offline: true });
    try {
      const page = await context.newPage();
      await page.setContent(html, { waitUntil: "load" });
      await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
      await page.emulateMedia({ media: "print" });
      const lists = () =>
        page.evaluate(() => {
          const pages = [...window.document.querySelectorAll<HTMLElement>(".page")];
          const first = pages.findIndex((p) => p.querySelector("h2")?.textContent === "What has changed");
          const text = (el: Element): string => {
            const copy = el.cloneNode(true) as Element;
            for (const ref of copy.querySelectorAll("[data-ref]")) ref.remove();
            return copy.textContent ?? "";
          };
          const slides = [];
          for (let i = first; i >= 0 && i < pages.length && pages[i]!.querySelector(".progress"); i++) {
            const p = pages[i]!;
            const heading = p.querySelector("h2")?.textContent ?? "";
            if (i > first && !/, continued$/.test(heading)) break;
            const columns = [...p.querySelectorAll(".progress .lists > div")].map((col) => ({ label: col.querySelector(":scope > h3")?.textContent ?? null, first: col.firstElementChild?.tagName ?? null, items: [...col.querySelectorAll("li")].map(text) }));
            slides.push({ eyebrow: p.querySelector(".eyebrow")?.textContent ?? "", heading, columns });
          }
          return { slides, continued: pages[first + 1]?.outerHTML ?? "" };
        });
      const built = (await lists()).slides[0]!;
      await carryOver(page);
      return { built, ...(await lists()) };
    } finally {
      await context.close();
    }
  }

  /** The slide and one continued slide; `split` the columns carried on, each under its label marked continued; every title printed once, in order, under its own label. */
  function assertLabelled({ built, slides }: { built: ListSlide; slides: ListSlide[] }, split: number[]): void {
    assert.deepEqual(slides.map((s) => [s.eyebrow, s.heading]), [["Since your last health check", "What has changed"], ["Since your last health check", "What has changed, continued"]], JSON.stringify(slides));
    const [first, next] = slides as [ListSlide, ListSlide];
    // No item on the continued slide without its label above it.
    for (const col of next.columns) if (col.items.length) assert.equal(col.first, "H3", JSON.stringify(col));
    // Labels only over what was carried: a column's empty shell gains none.
    assert.deepEqual(next.columns.filter((c) => c.items.length).map((c) => c.label), split.map((i) => `${built.columns[i]!.label}, continued`));
    assert.deepEqual(next.columns.flatMap((c) => (c.label ? [c.label] : [])), split.map((i) => `${built.columns[i]!.label}, continued`));
    for (const col of built.columns) {
      const stays = first.columns.find((c) => c.label === col.label)!;
      const goes = next.columns.find((c) => c.label === `${col.label}, continued`);
      assert.ok(stays.items.length > 0, `${col.label}: some of it stays`);
      assert.deepEqual([...stays.items, ...(goes?.items ?? [])], col.items, `${col.label}: each title once, in order`);
    }
  }

  test("a list split across the two slides keeps its label: one of the three on the full report (DECISIONS.md 507 and 508)", async () => {
    const report = longer(["remaining"], 2 * TEAMS.length);
    const range = deckRangeFor(report.input.run);
    const html = fullReportHtml(report, pseudonyms);
    const printed = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: report.dataset, carryOver: true });
    assert.equal(printed.layout.passed, true);
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    const carried = await carriedLists(html);
    // The label's count is the whole list's, as on the first slide.
    assert.deepEqual(carried.built.columns.map((c) => c.label), [`Fixed (${report.progress!.fixed.length})`, `Still open (${report.progress!.remaining.length})`, `New since then (${report.progress!.added.length})`]);
    assertLabelled(carried, [1]);
  });

  test("all three split, with every optional part at its longest: labelled, and no further slide on either document (DECISIONS.md 507 and 508)", async () => {
    const report = longer(["fixed", "remaining", "added"], 2 * TEAMS.length);
    const range = deckRangeFor(report.input.run);
    const html = fullReportHtml(report, pseudonyms);
    const printed = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: report.dataset, carryOver: true });
    assert.equal(printed.layout.passed, true);
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    assertLabelled(await carriedLists(html), [0, 1, 2]);
    // Rendered the same way with the carry-over as it was before the labels (fe5b440), this case printed 114 slides and 13 pages.
    assert.equal(printed.layout.pages, 114);
    const executive = executiveHtml(report, pseudonyms);
    const summary = await renderPdf(browser, executive, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset, carryOver: true });
    assert.equal(summary.layout.passed, true);
    assert.deepEqual(summary.layout.problems, [], describeProblems(summary.layout.problems));
    assert.equal(summary.layout.pages, 13);
    // The executive summary's slide is larger: at this length its lists stay whole, and nothing it carries is a list item without its label.
    for (const slide of (await carriedLists(executive)).slides) for (const col of slide.columns) if (col.items.length) assert.equal(col.first, "H3", JSON.stringify(col));
  });

  test("on the executive summary too, once titles are long enough to split its lists (DECISIONS.md 507 and 508)", async () => {
    // Measured: its lists split only once every title is about 380 characters longer than the
    // engine's own, after every note has already gone over. No title the engine writes is that
    // long, so this length is planted to hold the label there as well.
    const report = longer(["fixed", "remaining", "added"], 400);
    const html = executiveHtml(report, pseudonyms);
    const printed = await renderPdf(browser, html, { minPages: EXECUTIVE_PAGES.min, maxPages: executiveMaxPages(report), dataset: report.dataset, carryOver: true });
    assert.equal(printed.layout.passed, true);
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    assertLabelled(await carriedLists(html), [0, 1, 2]);
  });

  test("the 505 case's continued slide is as it was before the labels, character for character", async () => {
    const html = fullReportHtml(customer, pseudonyms);
    const { continued } = await carriedLists(html);
    assert.equal(
      continued.replace(/<div class="footer"[\s\S]*$/, ""),
      '<section class="page tight" style="justify-content: flex-start;" data-page="8"><div class="eyebrow">Since your last health check</div><h2>What has changed, continued</h2><div class="progress"><p class="note">Some of the change in money can come from the business itself, not only from fixes.</p></div>',
    );
  });
});

describe("a list split across a chapter slide keeps the label it sits under (DECISIONS.md 508)", () => {
  // Issue titles in two areas as long as a team's name can make them. The deck is the one
  // the documents job prints (fullReportHtml picks the density step as render-run.ts does).
  // Configuration's "What's below par" and Automation's split inside their lists.
  const TEAMS = " for the Enterprise Sales North America and Asia Pacific Partner Channel teams";
  const extra: Record<string, number> = { configuration: 200, automation: 160 };
  const findings = output!.findings.map((f) => (f.status === "issue" && extra[f.domain] ? { ...f, title: f.title + TEAMS.repeat(3).slice(0, extra[f.domain]) } : f));
  const report = buildReport({ ...base, findings });

  test("each carried list sits under the label of its own band, and no label sits directly above another heading", async () => {
    const range = deckRangeFor(report.input.run);
    const html = fullReportHtml(report, pseudonyms);
    const printed = await renderPdf(browser, html, { minPages: range.min, maxPages: range.max, dataset: report.dataset, carryOver: true });
    assert.equal(printed.layout.passed, true);
    assert.deepEqual(printed.layout.problems, [], describeProblems(printed.layout.problems));
    // 120 before the labels too (fe5b440, rendered the same way): they cost no slide.
    assert.equal(printed.layout.pages, 120);

    const context = await browser.newContext({ offline: true });
    try {
      const page = await context.newPage();
      await page.setContent(html, { waitUntil: "load" });
      await page.evaluate(() => window.document.fonts.ready.then(() => undefined));
      await page.emulateMedia({ media: "print" });
      await carryOver(page);
      const facts = await page.evaluate(() => {
        const pages = [...window.document.querySelectorAll<HTMLElement>(".page")];
        const text = (el: Element): string => {
          const copy = el.cloneNode(true) as Element;
          for (const ref of copy.querySelectorAll("[data-ref]")) ref.remove();
          return copy.textContent ?? "";
        };
        // A chapter's continued slide has no title of its own (listed for later in 508): the area is the slide before's.
        const carried = pages.flatMap((p, i) => {
          if (!p.querySelector(".chapter") || p.querySelector(".chapter > h2")) return [];
          const left = p.querySelector(".cols > div:first-child");
          const groups: Array<{ label: string; items: string[] }> = [];
          for (const el of left ? [...left.children] : []) {
            if (el.tagName === "H3") groups.push({ label: el.textContent ?? "", items: [] });
            else if (groups.length) groups.at(-1)!.items.push(...[...el.querySelectorAll("li")].map(text));
            else groups.push({ label: "", items: [...el.querySelectorAll("li")].map(text) });
          }
          return [{ area: pages[i - 1]!.querySelector(".chapter > .eyebrow")?.textContent ?? "", groups }];
        });
        const stacked = [...window.document.querySelectorAll("h3, h4, h5")].filter((h) => /, continued$/.test(h.textContent ?? "") && /^H[1-5]$/.test(h.nextElementSibling?.tagName ?? "")).map((h) => h.textContent);
        return { carried, stacked };
      });
      assert.deepEqual(facts.stacked, []);
      assert.deepEqual(facts.carried.map((c) => [c.area, c.groups[0]!.label]), [
        ["Configuration", "What's below par, continued"],
        ["Automation", "What's below par, continued"],
      ]);
      // Each carried title's finding is in the band its label names.
      const band = (f: Finding): string => (f.status === "passed" ? "Checked and fine" : f.severity === "critical" || f.severity === "high" ? "What's broken" : "What's below par");
      const domainOf = (area: string): string => report.teamFindings.find((f) => domainLabel(f.domain) === area)!.domain;
      for (const { area, groups } of facts.carried) {
        const own = report.teamFindings.filter((f) => f.domain === domainOf(area));
        for (const { label, items } of groups) {
          for (const item of items.filter((i) => !/^And \d+ more/.test(i))) {
            const matches = own.filter((f) => plainWords(ukDates(pseudonyms.restore(f.title))) === item);
            assert.ok(matches.length > 0, `${area}: ${item}`);
            for (const f of matches) assert.equal(label.replace(/, continued$/, ""), band(f), `${area}: ${item}`);
          }
        }
      }
    } finally {
      await context.close();
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
    const { layout } = await renderPdf(browser, executiveHtml(report, pseudonyms), { minPages: 4, maxPages: 10 });
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

  test("content inside a parent that stops short, running into the footer (DECISIONS.md 417)", async () => {
    // The panel of an issue's own slide sits in a grid: its parent can end above the footer while it runs on.
    assert.ok((await checks(doc(page(`${ok}<div style="height:10px"><div style="height:1000px"></div></div>${footer}`)))).includes("overflow"));
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
    // And it holds the documents back, named so a person can find it (DECISIONS.md 430).
    const held = heldFigures({ executive: layout, full: { pages: 0, passed: true, problems: [] } });
    assert.ok(held.some((h) => h.document === "The executive summary" && h.page > 0 && h.figure.includes("9,123,456") && h.reason.length > 0), JSON.stringify(held));
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
