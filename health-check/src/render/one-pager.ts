import { differenceWords, formatFigure, comparisonId } from "../benchmark/compare.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { buildPlan, isSetupReview, verdictOf } from "../narrative/select.ts";
import { readinessLine } from "../narrative/readiness.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { canRelease } from "../schemas/index.ts";
import { scoreLine } from "./executive.ts";
import { compactMoney, longDate } from "./format.ts";
import { document, documentTitle, renderContext, type Render } from "./html.ts";
import { returnOnSpend } from "./money.ts";
import { noteSections, referenceMap } from "./references.ts";

// The CEO one-pager (roadmap Stage 4): one A4 page, built to be forwarded. It
// says nothing the executive summary doesn't: each line is that summary's own
// wording and figures, cut down. The layout check holds it to exactly one page.

export const ONE_PAGER_PAGES = { min: 1, max: 1 } as const;
export const ONE_PAGER_ACTIONS = 3;
const COMPARED = 2;

const CSS = `
.page { font-size: 12.5px; line-height: 1.45; padding: 15mm 17mm 18mm; }
.footer { left: 17mm; right: 17mm; bottom: 8mm; }
.top { display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid var(--carbon); padding-bottom: 10px; margin-bottom: 14px; }
.top h1 { font-size: 26px; line-height: 1.15; font-weight: 700; margin: 4px 0 0; }
.top .who { text-align: right; font-size: 12px; color: var(--muted); }
.top .who b { display: block; font-size: 15px; color: var(--carbon); }
.headline { font-size: 18px; font-weight: 600; line-height: 1.3; margin: 0 0 12px; }
.trio { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 14px; }
.trio .box { background: var(--snow); border-radius: var(--radius-m); padding: 10px 12px; }
.trio .label { font-size: 11px; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
.trio .big { font-size: 26px; font-weight: 700; line-height: 1.1; margin: 4px 0; }
.trio p { font-size: 11.5px; line-height: 1.35; margin: 0; }
h2 { font-size: 14px; font-weight: 600; margin: 12px 0 6px; }
.lines p { margin: 0 0 6px; }
.src { font-size: 10.5px; color: var(--muted); margin-left: 4px; }
ol.do { margin: 0; padding-left: 18px; }
ol.do li { margin-bottom: 6px; }
ol.do .owner { display: block; font-size: 11px; color: var(--muted); }
.recheck { margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--rule); }
`;

const firstSentence = (text: string): string => /^.*?[.!?](?=\s|$)/.exec(text)?.[0] ?? text;

type Compared = BuiltReport["comparisons"][number];

/** How far the figure sits from what it's compared with, as a share of that, so points and days can be ranked together. */
function distance(c: Compared): number {
  const p = c.published[0];
  const against = p ? p.figure.value : c.own.status === "measured" ? c.own.median : null;
  if (c.value === null || against === null) return 0;
  return Math.abs(c.value - against) / Math.max(Math.abs(against), 1);
}

/** Behind what it's compared with: worse than the published figure, or in the bottom quarter of our customers. */
const belowPar = (c: Compared): boolean => c.published[0]?.side === "worse" || (c.published.length === 0 && c.own.status === "measured" && c.own.position === "bottom quarter");

/**
 * The comparisons a board reads (DECISIONS.md 354): the ones furthest from what
 * they're compared with, whichever way, and always at least one that's behind
 * when any is. A page of only the flattering ones isn't credible.
 */
export function chooseComparisons(all: Compared[], count: number): Compared[] {
  const candidates = all.filter((c) => c.value !== null && (c.published.length > 0 || c.own.status === "measured")).sort((a, b) => distance(b) - distance(a) || a.metric.key.localeCompare(b.metric.key));
  const shown = candidates.slice(0, count);
  const behind = candidates.find(belowPar);
  if (behind && !shown.some(belowPar) && shown.length > 0) shown[shown.length - 1] = behind;
  return shown;
}

function comparisons(r: Render, report: BuiltReport): string {
  if (isSetupReview(report.input.run)) return "";
  const shown = chooseComparisons(report.comparisons, COMPARED);
  if (shown.length === 0) return "";
  const line = (c: (typeof shown)[number]): string => {
    const yours = formatFigure(c.value!, c.metric.unit);
    const p = c.published[0];
    if (p) return `<p>${r.t(`${c.metric.label}: yours is ${yours}. Published research: ${formatFigure(p.figure.value, c.metric.unit)} ${p.figure.statistic}, so yours is ${differenceWords(c, p)}${p.side === "worse" ? ", which is worse" : p.side === "better" ? ", which is better" : ""}.`)}<span class="src">${r.t(`${p.figure.source.publisher}, ${p.figure.source.year}`)}</span>${r.ref(comparisonId(c.metric.key))}</p>`;
    const own = c.own;
    if (own.status !== "measured") return "";
    return `<p>${r.t(`${c.metric.label}: yours is ${yours}, ${own.position}. Black Cloud customers' middle half is ${formatFigure(own.p25, c.metric.unit)} to ${formatFigure(own.p75, c.metric.unit)}.`)}<span class="src">${r.t("Black Cloud customers")}</span>${r.ref(comparisonId(c.metric.key))}</p>`;
  };
  return `<h2>How you compare</h2><div class="lines">${shown.map(line).join("")}</div>`;
}

/** The one-pager's HTML: one A4 page. */
export function onePagerHtml(report: BuiltReport, pseudonyms: Pseudonymiser): string {
  const { run, intake, calibration, preparedBy } = report.input;
  const built = report.outputs.executive_summary;
  const r = renderContext(pseudonyms, calibration.currency, !canRelease(built.sections).ok, referenceMap(noteSections(report)));
  const m = (n: number): string => compactMoney(n, report.value.currency);
  const customer = intake?.customerName ?? run.customerRef ?? "Customer";
  const o = report.overall;
  const value = report.value;
  const cost = report.investment.annualCost;
  const ratio = returnOnSpend(report);

  const score =
    o.status === "measured"
      ? `<div class="box"><div class="label">Barometer score</div><div class="big">${o.score}</div><p>${r.t(`Out of 100, grade ${o.grade}, ${o.word.toLowerCase()}.`)}</p></div>`
      : `<div class="box"><div class="label">Barometer score</div><p>${r.t(o.reason)}</p></div>`;
  const worth =
    value.total.expected > 0
      ? `<div class="box"><div class="label">What fixing it is worth</div><div class="big">${m(value.total.expected)}</div><p>${r.t(`Over the next 12 months, expected. Between ${m(value.total.low)} and ${m(value.total.high)}.`)}</p></div>`
      : `<div class="box"><div class="label">What fixing it is worth</div><p>${r.t("We haven't put a money figure on these. They matter for the risk they carry.")}</p></div>`;
  const pay =
    cost.status === "measured"
      ? `<div class="box"><div class="label">What you pay</div><div class="big">${m(cost.value)}</div><p>${r.t(`A year. ${ratio !== null ? `What fixing the issues is worth is ${ratio} times what you pay for Salesforce a year.` : cost.label + "."}`)}</p></div>`
      : `<div class="box"><div class="label">What you pay</div><p>${r.t("We don't have a figure for what you pay a year.")}</p></div>`;

  const actions = buildPlan(report.teamFindings).phases.flatMap((phase) => phase.findings.map((f) => ({ f, phase }))).slice(0, ONE_PAGER_ACTIONS);
  const todo = actions.length
    ? `<h2>The three things to do first</h2><ol class="do">${actions.map(({ f, phase }) => `<li><strong>${r.t(f.title)}.</strong> ${r.t(firstSentence(f.recommendation))}${r.ref(f.id)}<span class="owner">${r.t(`${f.owner}, days ${phase.from} to ${phase.to}`)}</span></li>`).join("")}</ol>`
    : "";
  const progress = report.progress && report.progress.score.before !== null && report.progress.score.after !== null ? `<p style="margin:-6px 0 12px">${r.t(`Since your last health check, your Barometer score has gone from ${report.progress.score.before} to ${report.progress.score.after}.`)}</p>` : "";
  const line = scoreLine(report, true);
  // AI readiness, in one line and only when the verdict is clear (DECISIONS.md 243).
  const aiLine = readinessLine(report.readiness);

  const body = `<div class="top"><div><div class="eyebrow">Black Cloud Barometer</div><h1>${r.t(customer)}</h1><div>${r.t(`${documentTitle(run)}: one page summary`)}</div></div><div class="who"><b>${longDate(run.asOf)}</b>${preparedBy ? r.t(`Prepared by ${preparedBy}, Black Cloud`) : r.t("Prepared by Black Cloud")}${r.draft ? '<br><span class="mark">Draft, not approved for release</span>' : ""}</div></div>
<p class="headline">${r.t(verdictOf(report.teamFindings).headline)}</p>${progress}${aiLine ? `<p style="font-size:11.5px;margin:-6px 0 10px">${r.t(aiLine)}</p>` : ""}
<div class="trio">${score}${worth}${pay}</div>
${line ? `<p style="font-size:11.5px;color:var(--muted);margin:-6px 0 4px">${r.t(line)}</p>` : ""}
${comparisons(r, report)}${todo}
<p class="recheck">${r.t("We'll look at your Salesforce again after 90 days, so you can see what has improved, in money as well as in the score.")}</p>`;
  const footer = `<div class="footer" data-chrome><span>Black Cloud Barometer${r.draft ? " | Draft, not approved for release" : ""}</span><span>The detail is in the executive summary and full report</span></div>`;
  return document("One page summary", "a4", CSS, [`<section class="page" data-page="1">${body}${footer}</section>`]);
}
