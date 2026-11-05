import { currentBody, POTENTIAL_REVENUE_RULES, TOP_ISSUE_COUNT, type BuiltReport } from "../narrative/sections.ts";
import { areasOf, buildPlan, isSetupReview, scorecard, topIssues, verdictOf, type PlanPhase } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Finding, type Metric } from "../schemas/index.ts";
import { deliveryOutlookChart, scorecardChart, winRateBySegment } from "./charts.ts";
import { compactMoney, longDate, monthYear } from "./format.ts";
import { chartSlot, coverScore, document, documentTitle, effortText, estimateLines, paginate, paragraphs, renderContext, severityChip, valueFigure, valueText, type Render } from "./html.ts";
import { noteSections, referenceMap, valueItemId } from "./references.ts";
import { investmentPage, MONEY_CSS, moneyPage } from "./money.ts";
import { PROGRESS_CSS, progressPage } from "./progress.ts";
import { nextStepsPage, riskPage } from "./exec-pages.ts";
import { COMPARE_CSS, compareSlide } from "./compare.ts";
import { comparisonId } from "../benchmark/compare.ts";

// About 7 pages, A4: verdict, scorecard, pipeline, top five issues with value, 90-day plan.

const CSS = `
.page { font-size: 13.5px; line-height: 1.5; }
.footer { left: 18mm; right: 18mm; bottom: 9mm; }
.cover { justify-content: space-between; padding-bottom: 30mm; }
.cover h1 { font-size: 44px; line-height: 1.15; font-weight: 700; margin-top: 12px; }
.cover .sub { font-size: 18px; color: var(--panelMuted); }
.cover .cover-score { font-size: 20px; }
.cover .cover-score .for { font-size: 14px; }
.cover .for { font-size: 12px; color: var(--panelMuted); margin-bottom: 2px; }
h2 { font-size: 30px; line-height: 1.2; font-weight: 600; margin: 6px 0 18px; }
.lead p { font-size: 16px; line-height: 1.55; margin-bottom: 10px; }
.headline { font-size: 34px; font-weight: 700; line-height: 1.15; margin: 8px 0 18px; }
.stat b { font-size: 26px; }
.issue { background: var(--snow); border-radius: var(--radius-m); padding: 14px 16px; margin-bottom: 12px; }
.issue h3 { font-size: 15px; font-weight: 600; line-height: 1.3; margin: 6px 0; }
.issue .meta { display: flex; gap: 8px; align-items: center; color: var(--muted); font-size: 12px; flex-wrap: wrap; }
.issue .value { font-weight: 600; margin-top: 6px; }
.issue .value small { display: block; font-weight: 400; color: var(--muted); font-size: 11.5px; }
.issue .action { margin-top: 6px; }
table.grid { font-size: 12.5px; margin-top: 8px; }
h3.sub { font-size: 15px; font-weight: 600; margin: 22px 0 4px; }
.phase { margin-bottom: 12px; }
.phase h3 { font-size: 15px; font-weight: 600; margin-bottom: 6px; }
.phase li { margin-bottom: 4px; line-height: 1.45; }
.phase ul { margin: 0; padding-left: 18px; }
.phase .owner { color: var(--muted); font-size: 12px; }
.ref-list { columns: 2; column-gap: 24px; font-size: 11.5px; line-height: 1.35; }
.ref-row { break-inside: avoid; margin-bottom: 5px; display: grid; grid-template-columns: 44px 1fr; }
`;

// A4 at 96dpi, less margins, heading and footer.
const ISSUE_PAGE_BUDGET = 760;
const CHARS_PER_LINE = 88;

function issueHeight(f: Finding): number {
  const value = f.value.kind === "not_sized" ? f.value.reason : f.value.assumptions.join(". ");
  return 112 + 20 * estimateLines(f.summary, CHARS_PER_LINE) + 20 * estimateLines(f.recommendation, CHARS_PER_LINE) + 17 * estimateLines(value, CHARS_PER_LINE + 12);
}

function issueCard(r: Render, f: Finding, rank: number): string {
  const value = valueText(r, f);
  return `<article class="issue"><div class="meta"><span>${rank}</span>${severityChip(f)}<span>Confidence ${f.confidence}</span><span>${effortText(f)}</span><span>Owner: ${r.t(f.owner)}</span>${r.ref(f.id)}</div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p><p class="action"><strong>What to do:</strong> ${r.t(f.recommendation)}</p><p class="value">${value.amount}<small>${value.note}</small></p></article>`;
}

/** A summary page holds about a dozen actions. The full report has each phase in full. */
export const EXECUTIVE_PLAN_ITEMS = 4;

// Measured in Chromium: about 95 characters and 19.6px a line in a phase, 40px
// for a phase heading and its gap, 18px for the note, and 925px between the page
// heading and the bottom padding. Each figure here errs high, and the layout
// check measures the real thing.
const PLAN_CHARS = 90;
const INTRO_CHARS = 100;
const PLAN_LINE = 20;
const PLAN_PAGE_BUDGET = 915;
const planItemHeight = (r: Render, f: Finding): number => 4 + PLAN_LINE * estimateLines(`${r.t(f.title)}. ${r.t(f.recommendation)} ${r.t(f.owner)}`, PLAN_CHARS);
const phaseHeight = (r: Render, items: Finding[], more: boolean): number => 44 + (items.length ? items.reduce((sum, f) => sum + planItemHeight(r, f), 0) : 22) + (more ? 18 : 0);

type PlanBlock = { phase: PlanPhase; items: Finding[] };

/** The most pages the summary may take, the money and references pages included. The layout check holds it to this. */
export const EXECUTIVE_MAX_PAGES = 14;

type PlanPage = Array<PlanBlock | { intro: true }>;

function planPages(r: Render, blocks: PlanBlock[], introHeight: number): PlanPage[] {
  return paginate<PlanBlock | { intro: true }>([{ intro: true }, ...blocks], (b) => ("intro" in b ? introHeight : phaseHeight(r, b.items, b.phase.findings.length > b.items.length)), PLAN_PAGE_BUDGET);
}

/**
 * Each phase shows up to four actions, and long recommendations move later
 * phases onto another page. Only when the plan wouldn't fit the pages left does
 * it lose actions, from the longest phase first, so it never runs into the
 * footer or past the page limit, whatever the text length.
 */
function planLayout(r: Render, phases: PlanPhase[], introHeight: number, pagesLeft: number): PlanPage[] {
  const blocks: PlanBlock[] = phases.map((phase) => ({ phase, items: phase.findings.slice(0, EXECUTIVE_PLAN_ITEMS) }));
  const height = (b: PlanBlock): number => phaseHeight(r, b.items, true);
  for (;;) {
    const pages = planPages(r, blocks, introHeight);
    const tooLong = blocks.some((b) => b.items.length > 1 && height(b) > PLAN_PAGE_BUDGET);
    if (!tooLong && pages.length <= Math.max(1, pagesLeft)) return pages;
    const longest = blocks.filter((b) => b.items.length > 1).sort((a, b) => height(b) - height(a))[0];
    if (!longest) return pages;
    longest.items.pop();
  }
}

function phaseBlock(r: Render, { phase, items }: PlanBlock): string {
  // The title says what the action is about. A recommendation on its own often says "these deals".
  const rows = items.map((f) => `<li><strong>${r.t(f.title)}.</strong>${r.ref(f.id)} ${r.t(f.recommendation)} <span class="owner">${r.t(f.owner)}</span></li>`);
  const more = phase.findings.length > items.length ? `<p class="owner">The full report lists the rest of this phase.</p>` : "";
  const body = rows.length ? `<ul>${rows.join("")}</ul>${more}` : "<p>Nothing planned for this phase.</p>";
  return `<section class="phase"><h3>Days ${phase.from} to ${phase.to}: ${phase.title}</h3>${body}</section>`;
}

const measuredMetric = (metrics: Metric[], id: string): Extract<Metric, { status: "measured" }> | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m : null;
};

/** What's open, what it's worth weighted, what's likely to land by month, and the potential revenue in stalled deals. */
function pipelinePage(r: Render, report: BuiltReport, body: string): string {
  const { metrics, teamFindings } = report;
  const open = measuredMetric(metrics, "current.open_deals");
  const value = measuredMetric(metrics, "current.open_pipeline");
  const weighted = measuredMetric(metrics, "current.weighted_pipeline");
  const outlook = measuredMetric(metrics, "current.delivery_outlook_value");
  const stat = (figure: string, label: string): string => `<div class="stat"><b>${figure}</b><span>${label}</span></div>`;
  const stats = [
    open ? stat(String(open.value), "Open deals") : "",
    value ? stat(compactMoney(value.value, r.currency), "Open pipeline") : "",
    weighted ? stat(compactMoney(weighted.value, r.currency), "Weighted by stage") : "",
    outlook ? stat(compactMoney(outlook.value, r.currency), `Expected by ${monthYear(outlook.series.at(-1)!.date)}`) : "",
  ].join("");
  // One line per rule, in the order the rules are listed, so the page has a fixed ceiling.
  const potential = POTENTIAL_REVENUE_RULES.flatMap((id) => teamFindings.find((f) => f.ruleId === id && f.status === "issue") ?? []);
  const table = potential.length
    ? `<h3 class="sub">Potential revenue in open deals</h3><table class="grid"><thead><tr><th>Deals</th><th style="width:30%">Potential revenue</th></tr></thead><tbody>${potential.map((f) => `<tr><td>${r.t(f.title)}${r.ref(f.id)}</td><td>${valueFigure(r, f)}</td></tr>`).join("")}</tbody></table>`
    : "";
  const chart = metrics.some((m) => m.id === "current.delivery_outlook_value")
    ? `<h3 class="sub">Expected won value by month</h3><div style="margin-top:8px">${chartSlot(deliveryOutlookChart(metrics.find((m) => m.id === "current.delivery_outlook_value"), metrics.find((m) => m.id === "current.delivery_outlook_deals"), r.currency))}</div>`
    : "";
  return `<div class="eyebrow">Pipeline</div><h2>Your pipeline today</h2><div class="lead">${paragraphs(r, body)}</div>${stats ? `<div class="stat-row" style="margin-top:6px">${stats}</div>` : ""}${chart}${table}`;
}

/** How the one score comes from the area scores, for the scorecard pages. */
export function scoreLine(report: BuiltReport, standalone = false): string {
  const o = report.overall;
  if (o.status !== "measured") return "";
  const opening = standalone ? `Your Barometer score is ${o.score} out of 100, grade ${o.grade}` : ` Together they give a Barometer score of ${o.score} out of 100, grade ${o.grade}`;
  return `${opening}: the average of the area scores, weighted by how directly each area affects revenue.${o.capped ? " A critical issue holds it there, however well the rest does." : ""}`;
}

const footer = (r: Render, n: number): string => `<div class="footer" data-chrome><span>Black Cloud Barometer${r.draft ? " | Draft, not approved for release" : ""}</span><span>${n}</span></div>`;

/** How a reference leads to the working, and every reference this summary used, in the order the notes hold them. */
function detailPage(r: Render, items: Array<{ ref: string; title: string }>): string {
  const lede = "Each issue in this summary carries a reference to the raw notes, the Word document that comes with it. There you'll find the working behind every figure: what we read from your Salesforce, the sample, the calculation and the assumptions. The full report has the same references for everything else.";
  // Two narrow columns, so a summary that leans on many references still fits one page.
  const rows = items.sort((a, b) => a.ref.localeCompare(b.ref, "en", { numeric: true })).map((x) => `<p class="ref-row"><b data-ref>${x.ref}</b><span>${r.t(x.title)}</span></p>`);
  return `<div class="eyebrow">Where to find the detail</div><h2>How we worked it out</h2><p style="margin-bottom:14px">${r.t(lede)}</p><div class="ref-list">${rows.join("")}</div>`;
}

export function executiveHtml(report: BuiltReport, pseudonyms: Pseudonymiser): string {
  const { run, intake, calibration, scores = [], preparedBy } = report.input;
  const built = report.outputs.executive_summary;
  const refs = referenceMap(noteSections(report));
  const base = renderContext(pseudonyms, calibration.currency, !canRelease(built.sections).ok, refs);
  // The last page lists every reference the summary used, so each tag is noted as it's printed.
  const used = new Set<string>();
  const r: Render = { ...base, ref: (id) => (used.add(id), base.ref(id)) };
  const team = report.teamFindings;
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const plan = buildPlan(team).phases;
  const customer = intake?.customerName ?? run.customerRef ?? "Customer";
  const notMeasurable = team.filter((f) => f.status === "not_measurable").length;
  const setupReview = isSetupReview(run);

  const pages: string[] = [];
  const add = (html: string, classes = ""): void => {
    pages.push(`<section class="page ${classes}" data-page="${pages.length + 1}">${html}${classes.includes("cover") ? "" : footer(r, pages.length + 1)}</section>`);
  };

  add(`<div><div class="eyebrow">Black Cloud Barometer</div><h1>${documentTitle(run)}</h1><div class="sub">Executive summary</div></div>${coverScore(report.overall)}<div>${r.draft ? '<p><span class="mark">Draft, not approved for release</span></p>' : ""}<p class="for">Prepared for</p><p>${r.t(customer)}</p>${preparedBy ? `<p>by ${r.t(preparedBy)}, Black Cloud</p>` : ""}<p>${longDate(run.asOf)}</p></div>`, "dark cover");

  add(`<div class="eyebrow">Verdict</div><div class="headline">${r.t(verdict.headline)}</div><div class="lead">${paragraphs(r, currentBody(built, "verdict").replace(`${verdict.headline}.\n\n`, ""))}</div>
    <div class="stat-row" style="margin-top:18px">${report.overall.status === "measured" ? `<div class="stat"><b>${report.overall.score}</b><span>Barometer score</span></div>` : ""}<div class="stat"><b>${verdict.issues}</b><span>Issues</span></div><div class="stat"><b>${verdict.critical}</b><span>Critical</span></div><div class="stat"><b>${verdict.high}</b><span>High</span></div><div class="stat"><b>${notMeasurable}</b><span>Not measurable</span></div></div>
    <h3 class="sub">What matters most</h3>
    <table class="grid"><thead><tr><th>Issue</th><th style="width:34%">Impact</th></tr></thead><tbody>${top.map((f) => `<tr><td>${r.t(f.title)}${r.ref(f.id)}</td><td>${valueText(r, f).amount}</td></tr>`).join("")}</tbody></table>
    <p style="margin-top:18px">Every figure here comes from your Salesforce org. Where we couldn't measure something, we say so rather than estimate it.</p>`);

  const progressed = progressPage(r, report);
  if (progressed) add(progressed);
  const worth = moneyPage(r, report);
  if (worth) add(worth);
  const invested = investmentPage(r, report);
  if (invested) add(invested);

  add(`<div class="eyebrow">Scorecard</div><h2>How each area stands</h2><p style="margin-bottom:14px">${r.t(currentBody(built, "scorecard") + scoreLine(report))}</p>${chartSlot(scorecardChart(scorecard(team, scores, areasOf(run, team))))}${setupReview ? "" : `<h3 class="sub">Win rate by segment</h3><div style="margin-top:8px">${chartSlot(winRateBySegment(report.metrics.find((m) => m.id === "historic.win_rate")))}</div>`}`);

  // Only figures with something to stand against, as in the full report.
  const compared = report.comparisons.filter((c) => c.value !== null && (c.published.length > 0 || c.own.status === "measured"));
  if (compared.length && !setupReview) {
    const page = compareSlide(r, compared);
    add(page.html, page.classes ?? "");
  }

  // A set-up review reads no deals, so it has no pipeline page.
  if (!setupReview) add(pipelinePage(r, report, currentBody(built, "pipeline")));

  const ranked = top.map((f, i) => ({ f, rank: i + 1 }));
  const issuePages = paginate(ranked, (x) => issueHeight(x.f), ISSUE_PAGE_BUDGET);
  if (issuePages.length === 0) add(`<div class="eyebrow">Top issues</div><h2>Top issues</h2><p>${r.t(currentBody(built, "top_issues"))}</p>`);
  issuePages.forEach((group, i) => {
    add(`<div class="eyebrow">Top issues</div><h2>${i === 0 ? `Top ${TOP_ISSUE_COUNT} issues` : `Top ${TOP_ISSUE_COUNT} issues, continued`}</h2>${i === 0 ? `<p style="margin-bottom:12px">${r.t(currentBody(built, "top_issues"))}</p>` : ""}${group.map((x) => issueCard(r, x.f, x.rank)).join("")}`);
  });

  const intro = currentBody(built, "plan_90_day");
  // The first page also carries the introduction, so it has less room.
  const introHeight = 14 + 20.5 * estimateLines(intro, INTRO_CHARS);
  // One page is kept back for the references.
  const risk = riskPage(r, report);
  if (risk) add(risk);

  // Two pages are kept back: what happens next, and the references.
  planLayout(r, plan, introHeight, EXECUTIVE_MAX_PAGES - pages.length - 2).forEach((group, i) => {
    const phases = group.filter((b): b is PlanBlock => !("intro" in b));
    add(`<div class="eyebrow">90-day plan</div><h2>${i === 0 ? "What we'd do first" : "What we'd do first, continued"}</h2>${i === 0 ? `<p style="margin-bottom:14px">${r.t(intro)}</p>` : ""}${phases.map((b) => phaseBlock(r, b)).join("")}`);
  });

  add(nextStepsPage(r, report));

  const referenced = [...used].flatMap((id) => {
    const ref = refs.get(id);
    const title =
      team.find((x) => x.id === id)?.title ??
      report.value.groups.find((g) => valueItemId(g.id) === id)?.label ??
      report.metrics.find((x) => x.id === id)?.label ??
      report.comparisons.find((c) => comparisonId(c.metric.key) === id)?.metric.label;
    return ref && title ? [{ ref, title }] : [];
  });
  if (referenced.length) add(detailPage(r, referenced));

  return document("Executive summary", "a4", `${CSS}\n${MONEY_CSS}\n${COMPARE_CSS}\n${PROGRESS_CSS}`, pages);
}
