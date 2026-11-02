import { currentBody, TOP_ISSUE_COUNT, type BuiltReport } from "../narrative/sections.ts";
import { buildPlan, scorecard, topIssues, verdictOf, type PlanPhase } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease, type Finding } from "../schemas/index.ts";
import { scorecardChart, winRateBySegment } from "./charts.ts";
import { longDate } from "./format.ts";
import { chartSlot, document, estimateLines, paginate, paragraphs, renderContext, severityChip, valueText, type Render } from "./html.ts";

// About 6 pages, A4: verdict, scorecard, top five issues with value, 90-day plan.

const CSS = `
.page { font-size: 13.5px; line-height: 1.5; }
.footer { left: 18mm; right: 18mm; bottom: 9mm; }
.cover { justify-content: space-between; padding-bottom: 30mm; }
.cover h1 { font-size: 44px; line-height: 1.15; font-weight: 700; margin-top: 12px; }
.cover .sub { font-size: 18px; color: var(--panelMuted); }
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
.phase { margin-bottom: 14px; }
.phase h3 { font-size: 15px; font-weight: 600; margin-bottom: 6px; }
.phase li { margin-bottom: 4px; }
.phase ul { margin: 0; padding-left: 18px; }
.phase .owner { color: var(--muted); font-size: 12px; }
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
  return `<article class="issue"><div class="meta"><span>${rank}</span>${severityChip(f)}<span>Confidence ${f.confidence}</span><span>Effort ${f.effort.size}</span><span>Owner: ${r.t(f.owner)}</span></div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p><p class="action"><strong>What to do:</strong> ${r.t(f.recommendation)}</p><p class="value">${value.amount}<small>${value.note}</small></p></article>`;
}

function phaseBlock(r: Render, phase: PlanPhase): string {
  const rows = phase.findings.slice(0, TOP_ISSUE_COUNT).map((f) => `<li>${r.t(f.recommendation)} <span class="owner">${r.t(f.owner)}</span></li>`);
  const body = rows.length ? `<ul>${rows.join("")}</ul>` : "<p>Nothing planned for this phase.</p>";
  return `<section class="phase"><h3>Days ${phase.from} to ${phase.to}: ${phase.title}</h3>${body}</section>`;
}

const footer = (r: Render, n: number): string => `<div class="footer" data-chrome><span>Black Cloud Barometer${r.draft ? " | Draft, not approved for release" : ""}</span><span>${n}</span></div>`;

export function executiveHtml(report: BuiltReport, pseudonyms: Pseudonymiser): string {
  const { run, intake, calibration, scores = [] } = report.input;
  const built = report.outputs.executive_summary;
  const r = renderContext(pseudonyms, calibration.currency, !canRelease(built.sections).ok);
  const team = report.teamFindings;
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT);
  const plan = buildPlan(team);
  const customer = intake?.customerName ?? run.customerRef ?? "Customer";
  const notMeasurable = team.filter((f) => f.status === "not_measurable").length;

  const pages: string[] = [];
  const add = (html: string, classes = ""): void => {
    pages.push(`<section class="page ${classes}" data-page="${pages.length + 1}">${html}${classes.includes("cover") ? "" : footer(r, pages.length + 1)}</section>`);
  };

  add(`<div><div class="eyebrow">Black Cloud Barometer</div><h1>Salesforce health check</h1><div class="sub">Executive summary</div></div><div>${r.draft ? '<p><span class="mark">Draft, not approved for release</span></p>' : ""}<p>${r.t(customer)}</p><p>As of ${longDate(run.asOf)}</p></div>`, "dark cover");

  add(`<div class="eyebrow">Verdict</div><div class="headline">${r.t(verdict.headline)}</div><div class="lead">${paragraphs(r, currentBody(built, "verdict").replace(`${verdict.headline}.\n\n`, ""))}</div>
    <div class="stat-row" style="margin-top:18px"><div class="stat"><b>${verdict.issues}</b><span>Issues</span></div><div class="stat"><b>${verdict.critical}</b><span>Critical</span></div><div class="stat"><b>${verdict.high}</b><span>High</span></div><div class="stat"><b>${notMeasurable}</b><span>Not measurable</span></div></div>
    <h3 class="sub">What matters most</h3>
    <table class="grid"><thead><tr><th>Issue</th><th style="width:34%">Value</th></tr></thead><tbody>${top.map((f) => `<tr><td>${r.t(f.title)}</td><td>${valueText(r, f).amount}</td></tr>`).join("")}</tbody></table>
    <p style="margin-top:18px">Every figure here comes from your Salesforce org. Where we couldn't measure something, we say so rather than estimate it.</p>`);

  add(`<div class="eyebrow">Scorecard</div><h2>How each area stands</h2><p style="margin-bottom:14px">${r.t(currentBody(built, "scorecard"))}</p>${chartSlot(scorecardChart(scorecard(team, scores)))}<h3 class="sub">Win rate by segment</h3><div style="margin-top:8px">${chartSlot(winRateBySegment(report.metrics.find((m) => m.id === "historic.win_rate")))}</div>`);

  const ranked = top.map((f, i) => ({ f, rank: i + 1 }));
  const issuePages = paginate(ranked, (x) => issueHeight(x.f), ISSUE_PAGE_BUDGET);
  if (issuePages.length === 0) add(`<div class="eyebrow">Top issues</div><h2>Top issues</h2><p>${r.t(currentBody(built, "top_issues"))}</p>`);
  issuePages.forEach((group, i) => {
    add(`<div class="eyebrow">Top issues</div><h2>${i === 0 ? `Top ${TOP_ISSUE_COUNT} issues` : `Top ${TOP_ISSUE_COUNT} issues, continued`}</h2>${i === 0 ? `<p style="margin-bottom:12px">${r.t(currentBody(built, "top_issues"))}</p>` : ""}${group.map((x) => issueCard(r, x.f, x.rank)).join("")}`);
  });

  add(`<div class="eyebrow">90-day plan</div><h2>What we'd do first</h2><p style="margin-bottom:14px">${r.t(currentBody(built, "plan_90_day"))}</p>${plan.map((p) => phaseBlock(r, p)).join("")}`);

  return document("Executive summary", "a4", CSS, pages);
}
