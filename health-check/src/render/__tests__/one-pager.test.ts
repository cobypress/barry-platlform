import { after, before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { Browser } from "playwright";
import { strFromU8, unzipSync } from "fflate";
import { scoreDomains } from "../../domains/scoring.ts";
import { Intake } from "../../narrative/intake.ts";
import { buildReport, type BuiltReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { SALES_CLOUD_DOMAINS, type Finding } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { executiveHtml } from "../executive.ts";
import { chooseComparisons, onePagerHtml, ONE_PAGER_PAGES } from "../one-pager.ts";
import { launchBrowser, renderPdf } from "../pdf.ts";
import { kitReferences, readoutKit, renderReadoutKitDocx } from "../readout-kit.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const intake = Intake.parse({ customerName: "Synthetic Co", summary: "Synthetic.", statedProblems: [], capturedBy: "Synthetic Consultant", capturedAt: "2026-10-30T09:00:00.000Z", salesforceSpendAnnual: 48_500, industry: "professional-services", headcountBand: "1001-5000" });
const base: ReportInput = { run, calibration: loadCalibration(), findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings), intake, preparedBy: "Synthetic Consultant" };
const LONG = " This sentence is here to make the summary as long as a real one can get, with a good deal of explanation about what we saw.";

/** Every area full of issues with long text, as in the full report's messy-org test. */
function messy(): ReportInput {
  const issues = output!.findings.filter((f) => f.status === "issue");
  const findings: Finding[] = [];
  SALES_CLOUD_DOMAINS.forEach((domain, d) => {
    for (let i = 0; i < 12; i++) {
      const source = issues[(d * 12 + i) % issues.length]!;
      findings.push({ ...source, id: `f-${String(d).padStart(2, "0")}${String(i).padStart(10, "0")}`, domain, severity: (["critical", "high", "medium", "low"] as const)[i % 4]!, summary: source.summary + LONG, recommendation: `${source.recommendation}${LONG}`, title: `${source.title} across the ${domain.replace(/_/g, " ")} teams and their managers` });
    }
  });
  const all = [...findings, ...output!.findings.filter((f) => f.status !== "issue")];
  return { ...base, findings: all, scores: scoreDomains(run.modules, all) };
}

/** Ninety days on: three rules fixed. */
function recheck(): ReportInput {
  const fixed = new Set(output!.findings.filter((f) => f.status === "issue").map((f) => f.ruleId).slice(0, 3));
  const later = output!.findings.map((f): Finding => (fixed.has(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
  return { ...base, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: base };
}

const visible = (html: string): string => html.replace(/<style>[\s\S]*?<\/style>/g, "").replace(/<(span|b)[^>]*data-ref[^>]*>[\s\S]*?<\/\1>/g, "").replace(/<[^>]+>/g, " ").replace(/&#39;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");

let browser: Browser;
before(async () => {
  browser = await launchBrowser();
});
after(async () => {
  await browser.close();
});

describe("the CEO one-pager", () => {
  for (const [name, input] of [["the synthetic org", base], ["a large, messy org", messy()], ["a re-check", recheck()]] as const) {
    test(`is exactly one page, and passes the layout and number checks, for ${name}`, async () => {
      const report = buildReport(input);
      const { layout, pdf } = await renderPdf(browser, onePagerHtml(report, pseudonyms), { minPages: ONE_PAGER_PAGES.min, maxPages: ONE_PAGER_PAGES.max, dataset: report.dataset });
      assert.deepEqual(layout.problems, []);
      assert.equal(layout.pages, 1);
      assert.ok(pdf.length > 1000);
    });
  }

  test("has the score, the worth with its range, what they pay and the multiple, the comparison with its source, three actions and the re-check", () => {
    const text = visible(onePagerHtml(buildReport(base), pseudonyms));
    for (const want of [/Synthetic Co/, /31 Oct 2026/, /Prepared by Synthetic Consultant, Black Cloud/, /Barometer score/, /Out of 100, grade [A-E]/, /Over the next 12 months, expected\. Between £[\d.]+[km] and £[\d.]+[km]/, /£48\.5k/, /times what you pay for Salesforce a year/, /How you compare/, /Ebsta/, /The three things to do first/, /days 1 to 30/, /again after 90 days/]) assert.match(text, want);
    assert.equal((onePagerHtml(buildReport(base), pseudonyms).match(/<li>/g) ?? []).length, 3);
  });

  test("says no figure the executive summary doesn't", () => {
    for (const input of [base, recheck()]) {
      const report = buildReport(input);
      const exec = visible(executiveHtml(report, pseudonyms));
      const figures = visible(onePagerHtml(report, pseudonyms)).match(/£?\d[\d,.]*[km%]?/g) ?? [];
      for (const figure of new Set(figures.map((f) => f.replace(/\.$/, "")))) assert.ok(exec.includes(figure), figure);
    }
  });

  test("a re-check says how the score has moved", () => {
    const report = buildReport(recheck());
    if (report.progress?.score.before !== null && report.progress?.score.after !== null) assert.match(visible(onePagerHtml(report, pseudonyms)), /Since your last health check, your Barometer score has gone from \d+ to \d+/);
  });
});

/** The first plan action's title with a person's token in it, so restoring names shows. */
function withName(): BuiltReport {
  const report = buildReport(base);
  const first = report.teamFindings.find((f) => f.status === "issue" && (f.severity === "critical" || f.severity === "high"))!;
  first.title = `${first.title.replace(/\.$/, "")} for [[user:U0001]]`;
  return report;
}

describe("names", () => {
  test("come back on the one-pager, and no token is left", () => {
    const html = onePagerHtml(withName(), pseudonyms);
    assert.doesNotMatch(html, /\[\[/);
    assert.match(html, /Alex Morgan/);
  });

  test("come back in the readout kit, and no token is left", async () => {
    const xml = strFromU8(unzipSync(new Uint8Array(await renderReadoutKitDocx(withName(), pseudonyms)))["word/document.xml"]!);
    assert.doesNotMatch(xml, /\[\[/);
    assert.match(xml, /Alex Morgan/);
  });
});

describe("the readout kit", () => {
  test("has a part for each part of the executive summary, each with points to make", () => {
    const keys = readoutKit(buildReport(base)).map((p) => p.key);
    assert.deepEqual(keys, ["verdict", "money", "investment", "scorecard", "compare", "pipeline", "top_issues", "risk", "plan", "next"]);
    for (const part of readoutKit(buildReport(base))) assert.ok(part.points.length >= 1 && part.points.every((p) => p.length > 10), part.key);
    assert.deepEqual(readoutKit(buildReport(recheck())).map((p) => p.key).slice(0, 2), ["verdict", "progress"]);
  });

  test("every answer resolves to a raw notes reference", () => {
    for (const input of [base, recheck(), messy()]) {
      const refs = kitReferences(buildReport(input));
      assert.ok(refs.size > 5);
      for (const [id, ref] of refs) assert.match(ref ?? "", /^R\d+(\.\d+)?$/, id);
    }
  });

  test("says what not to promise where money is talked about", () => {
    const money = readoutKit(buildReport(base)).find((p) => p.key === "money")!;
    assert.match(money.notPromise.join(" "), /potential.*own data.*count only a share.*guarantee/);
    assert.match(money.notPromise.join(" "), /Exposure is money at risk/);
  });

  test("is marked Black Cloud only, and prints each reference beside its question", async () => {
    const report = buildReport(base);
    const xml = strFromU8(unzipSync(new Uint8Array(await renderReadoutKitDocx(report, pseudonyms)))["word/document.xml"]!);
    const text = [...xml.matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((m) => m[1]).join(" ");
    assert.match(text, /Black Cloud only\. Never send this to the customer/);
    assert.match(text, /How is the score worked out\? +Answer: R\d+/);
    for (const part of readoutKit(report)) assert.ok(text.includes(part.title.replace(/'/g, "&apos;")) || text.includes(part.title), part.title);
  });
});

describe("which comparisons the one-pager shows (DECISIONS.md 354)", () => {
  // Made up: a figure, and published research it's better or worse than.
  const made = (key: string, value: number, published: number, side: "better" | "worse") =>
    ({ metric: { key, label: key, unit: "percent", better: "higher" }, value, reason: null, published: [{ figure: { value: published }, difference: value - published, side }], publishedReason: null, own: { status: "not_measurable", reason: "Synthetic.", tried: [] } }) as never;

  test("the furthest from what they're compared with, whichever way", () => {
    const chosen = chooseComparisons([made("near", 21, 20, "better"), made("far-up", 60, 20, "better"), made("far-down", 5, 20, "worse")], 2);
    assert.deepEqual(chosen.map((c: { metric: { key: string } }) => c.metric.key), ["far-up", "far-down"]);
  });

  test("at least one that's behind, when any is, even if the flattering ones are further out", () => {
    const chosen = chooseComparisons([made("up-a", 90, 20, "better"), made("up-b", 80, 20, "better"), made("down", 15, 20, "worse")], 2);
    assert.deepEqual(chosen.map((c: { metric: { key: string } }) => c.metric.key), ["up-a", "down"]);
  });

  test("on the sample, the one behind says it's worse, not only higher", () => {
    assert.match(onePagerHtml(buildReport(base), pseudonyms), /so yours is 43 points higher, which is worse\./);
  });

  test("only flattering ones when nothing is behind", () => {
    const chosen = chooseComparisons([made("up-a", 90, 20, "better"), made("up-b", 80, 20, "better")], 2);
    assert.deepEqual(chosen.map((c: { metric: { key: string } }) => c.metric.key), ["up-a", "up-b"]);
  });
});
