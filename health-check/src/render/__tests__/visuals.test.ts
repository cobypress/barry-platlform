import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { colour } from "../../design/tokens.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { buildMicrositePages } from "../../microsite/pages.ts";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { scorecard } from "../../narrative/select.ts";
import { checkNumbers } from "../../numbercheck/index.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { activeUsersChart, lightningChart, loginChannelsChart, processFlowChart, scorecardChart, scorecardOptions, waterfallChart, type Chart } from "../charts.ts";
import { DECK_RANGE, fullReportHtml } from "../full-report.ts";

const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
const calibration = loadCalibration();
const input: ReportInput = { run, calibration, findings: output!.findings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, output!.findings) };
const report = buildReport(input);
const metric = (id: string): Metric | undefined => report.metrics.find((m) => m.id === id);
const STAGES = calibration.stages.filter((s) => s.category !== "excluded").sort((a, b) => (a.order ?? 99) - (b.order ?? 99)).map((s) => ({ name: s.name, category: s.category === "open" ? ("open" as const) : s.category === "won" ? ("won" as const) : ("lost" as const) }));
const gone = (id: string): Metric => ({ ...metric(id)!, status: "not_measurable", reason: "Synthetic gap." }) as Metric;

const charts = (): Chart[] => [
  activeUsersChart(metric("adoption.monthly_active_users")),
  loginChannelsChart(metric("adoption.mobile_login_share")),
  lightningChart(metric("adoption.lightning_share")),
  waterfallChart(report.value),
  processFlowChart(metric("historic.stage_transitions"), STAGES),
  scorecardChart(scorecard(report.teamFindings, input.scores!), scorecardOptions(report)),
];
const svg = (chart: Chart): string => (chart.kind === "chart" ? chart.svg : assert.fail(`${chart.id} is a gap: ${chart.reason}`));
const printed = (markup: string): string =>
  [...markup.matchAll(/<text [^>]*>([^<]*)<\/text>/g)]
    .filter((m) => !m[0].includes('class="tick"'))
    .map((m) => m[1]!.replace(/&#39;/g, "'").replace(/&amp;/g, "&"))
    .join(". ");

describe("the new charts", () => {
  test("draw from the synthetic org, as well-formed SVG in the brand's flat colours, with red only for an alert", () => {
    const allowed = new Set(Object.values(colour).map((c) => c.toLowerCase()));
    for (const chart of charts()) {
      const markup = svg(chart);
      assert.match(markup, /^<svg xmlns="http:\/\/www\.w3\.org\/2000\/svg" viewBox="0 0 \d+ \d+" role="img" aria-label="[^"]+"/, chart.id);
      assert.equal((markup.match(/<text/g) ?? []).length, (markup.match(/<\/text>/g) ?? []).length, chart.id);
      assert.doesNotMatch(markup, /gradient|filter=|<image|<script|\p{Extended_Pictographic}/iu, chart.id);
      for (const [hex] of markup.matchAll(/#[0-9a-fA-F]{6}\b/g)) assert.ok(allowed.has(hex.toLowerCase()), `${chart.id}: ${hex}`);
      if (chart.id !== "scorecard") assert.ok(!markup.includes(colour.alert), chart.id);
    }
  });

  test("every printed figure traces through the number check", () => {
    for (const chart of charts()) {
      const outcome = checkNumbers(printed(svg(chart)), report.dataset);
      assert.deepEqual(outcome.unmatched, [], chart.id);
    }
  });

  test("the same input draws the same picture", () => {
    assert.deepEqual(charts(), charts());
  });

  test("with nothing to draw, each gives its reason rather than an empty frame", () => {
    assert.equal(activeUsersChart(undefined).kind, "gap");
    assert.equal(activeUsersChart(gone("adoption.monthly_active_users")).kind, "gap");
    assert.equal(loginChannelsChart(gone("adoption.mobile_login_share")).kind, "gap");
    assert.equal(lightningChart(gone("adoption.lightning_share")).kind, "gap");
    assert.equal(processFlowChart(gone("historic.stage_transitions"), STAGES).kind, "gap");
    assert.equal(scorecardChart([]).kind, "gap");
    const nothing = { ...report.value, total: { low: 0, expected: 0, high: 0 } };
    assert.equal(waterfallChart(nothing).kind, "gap");
    const flat = structuredClone(metric("adoption.monthly_active_users")) as Extract<Metric, { status: "measured" }>;
    flat.series = flat.series.slice(-1);
    assert.equal(activeUsersChart(flat).kind, "gap");
  });
});

describe("adoption", () => {
  test("the line picks out the group that fell furthest, with its peak and where it is now", () => {
    const markup = svg(activeUsersChart(metric("adoption.monthly_active_users")));
    assert.match(markup, /Everyone: 5/);
    assert.match(markup, />Standard User<\/text>/);
    assert.match(markup, />profile<\/text>/);
    assert.doesNotMatch(markup, /…/, "wrapped, not cut");
    assert.match(markup, /7 at its peak, 4 now/);
    assert.match(markup, /stroke-dasharray="6 4"/);
  });

  test("channels and Lightning are each one bar of shares, every part named", () => {
    assert.match(svg(loginChannelsChart(metric("adoption.mobile_login_share"))), /Browser: 85\.2%.*Mobile app: 2\.1%/s);
    assert.match(svg(lightningChart(metric("adoption.lightning_share"))), /Lightning: 83\.3%, 10 users.*Classic: 16\.7%, 2 users/s);
  });

  test("they get a slide right after the adoption chapter opens, and sit on the microsite under Adoption", () => {
    const html = fullReportHtml(report, pseudonyms);
    const glance = html.indexOf("Adoption and usage</div><h2>At a glance");
    const usage = html.indexOf("Who uses Salesforce, and how");
    assert.ok(glance > 0 && usage > glance, "after the chapter's At a glance slide");
    assert.ok(html.slice(glance, usage).split('<section class="page').length === 2, "the very next slide");
    const issues = buildMicrositePages(report, pseudonyms).pages.find((p) => p.slug === "issues")!.html;
    const adoption = issues.slice(issues.indexOf("<h2>Adoption and usage</h2>"));
    for (const id of ["active-users", "login-channels", "lightning"]) assert.match(adoption.slice(0, adoption.indexOf("<h2>", 4)), new RegExp(`data-chart="${id}"`), id);
  });
});

describe("the money", () => {
  test("each kind builds to the total, and exposure stands apart, never stacked", () => {
    const markup = svg(waterfallChart(report.value));
    const bars = [...markup.matchAll(/<rect x="[\d.]+" y="([\d.]+)" width="[\d.]+" height="([\d.]+)" rx="\d+" fill="(#[0-9A-Fa-f]{6})"\/>/g)].map((m) => ({ top: Number(m[1]), height: Number(m[2]), fill: m[3] }));
    const steps = bars.filter((b) => b.fill === colour.chartreuse);
    const total = bars.find((b) => b.fill === colour.ivory)!;
    // The steps, end to end, reach exactly as high as the total.
    assert.ok(Math.abs(steps.reduce((sum, b) => sum + b.height, 0) - total.height) < 3);
    assert.match(markup, /Kept apart/);
    assert.match(markup, /fill="none" stroke="#B4B6A8" stroke-width="1\.2" stroke-dasharray="5 4"/);
  });

  test("the executive page and the slide show the waterfall in place of the bars", () => {
    const html = fullReportHtml(report, pseudonyms);
    assert.match(html, /data-chart="value-waterfall"/);
    assert.doesNotMatch(html.slice(html.indexOf("What fixing this is worth</h2>")), /^[\s\S]{0,4000}class="kinds"/);
  });
});

describe("the sales process", () => {
  test("stages run left to right, a rarely used one is named and left out, and skips and moves back are drawn", () => {
    const markup = svg(processFlowChart(metric("historic.stage_transitions"), STAGES));
    const order = ["Prospecting", "Discovery", "Proposal", "Negotiation", "Won"].map((s) => markup.indexOf(`>${s}<`));
    assert.deepEqual(order, [...order].sort((a, b) => a - b));
    assert.ok(order.every((i) => i > 0));
    assert.match(markup, /Rarely used, left out: Qualified/);
    assert.match(markup, />401</, "Prospecting to Discovery is the main flow, not a skip");
    assert.match(markup, /<path d="M[^"]+Q[^"]+" fill="none" stroke="#FCFFF3" stroke-width="[\d.]+" stroke-dasharray="6 4"\/>/, "a move back");
    assert.match(markup, /<path d="M[^"]+Q[^"]+" fill="none" stroke="#FCFFF3" stroke-width="[\d.]+"\/>/, "a skip");
  });

  test("moves back fan out: no two leave from the same point, and every count sits on its own pill", () => {
    const markup = svg(processFlowChart(metric("historic.stage_transitions"), STAGES));
    const backs = [...markup.matchAll(/<path d="M([\d.]+) ([\d.]+)Q[^ ]+ [\d.]+ ([\d.]+) [\d.]+" fill="none" stroke="#FCFFF3" stroke-width="[\d.]+" stroke-dasharray="6 4"\/>/g)];
    assert.ok(backs.length >= 2);
    const points = backs.flatMap((m) => [m[1], m[3]]);
    assert.equal(new Set(points).size, points.length, "every arc starts and ends at its own point");
    const pills = [...markup.matchAll(/<rect [^>]*rx="9" fill="#21221C"/g)];
    assert.equal(pills.length, backs.length);
    // Pills come after every line, so they're drawn on top.
    assert.ok(markup.lastIndexOf('stroke-dasharray="6 4"/>') < markup.indexOf('rx="9" fill="#21221C"'));
  });

  test("the adoption slide keeps its references out of the heading", () => {
    const html = fullReportHtml(report, pseudonyms);
    const slide = html.slice(html.indexOf("Who uses Salesforce, and how") - 300, html.indexOf("Who uses Salesforce, and how") + 40);
    assert.match(slide, /<div class="eyebrow">Adoption and usage<span class="ref" data-ref>/);
    assert.match(slide, /<h2>Who uses Salesforce, and how<\/h2>/);
  });
});

describe("the scorecard", () => {
  test("ranks the area that holds the score back most first, and shows each weight", () => {
    const rows = scorecard(report.teamFindings, input.scores!);
    const scored = rows.filter((r) => r.score !== null);
    assert.ok(scored.length >= 2);
    const markup = svg(scorecardChart(rows, scorecardOptions(report)));
    const at = (label: string): number => markup.indexOf(`>${label}<`);
    const ranked = [...scored].sort((a, b) => (b.score === null ? 0 : (100 - b.score) * scorecardOptions(report).weightOf!(b.domain)) - (a.score === null ? 0 : (100 - a.score) * scorecardOptions(report).weightOf!(a.domain)));
    for (let i = 1; i < ranked.length; i++) assert.ok(at(ranked[i - 1]!.label) < at(ranked[i]!.label), `${ranked[i - 1]!.label} before ${ranked[i]!.label}`);
    assert.match(markup, />Weight</);
  });

  test("on a re-check, each area's last score is marked, and the figures still trace", () => {
    const fixed = new Set(report.teamFindings.filter((f) => f.status === "issue").map((f) => f.ruleId).slice(0, 3));
    const later = output!.findings.map((f): Finding => (fixed.has(f.ruleId) && f.status === "issue" ? { ...f, status: "passed", value: { kind: "not_sized", reason: "Passed." } } : f));
    const recheck = buildReport({ ...input, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: later, scores: scoreDomains(run.modules, later), baseline: input });
    assert.ok(recheck.progress);
    const markup = svg(scorecardChart(scorecard(recheck.teamFindings, scoreDomains(run.modules, later)), scorecardOptions(recheck)));
    assert.match(markup, /A white mark is the score last time/);
    if (recheck.progress!.areas.some((a) => a.before !== null && a.after !== null && a.before !== a.after)) assert.match(markup, /, was \d+/);
    assert.deepEqual(checkNumbers(printed(markup), recheck.dataset).unmatched, []);
  });
});

describe("the deck", () => {
  test("stays within its ceiling on the synthetic org", () => {
    const pages = fullReportHtml(report, pseudonyms).split('<section class="page').length - 1;
    assert.ok(pages <= DECK_RANGE.max, String(pages));
  });
});
