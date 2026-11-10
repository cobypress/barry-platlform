import { yearlyByFinding } from "../value/model.ts";
import { currentBody, TOP_ISSUE_COUNT, type BuiltOutput, type BuiltReport } from "../narrative/sections.ts";
import { VERDICT_WORDS, type ReadinessVerdict } from "../narrative/readiness.ts";
import { areasOf, buildPlan, domainLabel, isSetupReview, issuesOf, isThinData, isUnpricedSaving, QUICK_WINS_HEADING, recordAreasLine, scorecard, thinDataLine, topIssues, UNPRICED, verdictOf } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { ACCOUNT_ENGAGEMENT_DOMAINS, canRelease, CPQ_DOMAINS, SERVICE_CLOUD_DOMAINS, type Finding, type Metric, type Run } from "../schemas/index.ts";
import { INVENTORY_COLUMNS, inventoryRows, OBJECT_INVENTORY } from "../domains/configuration/inventory.ts";
import { activeUsersChart, lightningChart, loginChannelsChart, processFlowChart, scorecardOptions, type Chart, closedAsChart, deliveryOutlookChart, HALF_WIDTH, lostByStageChart, pipelineOverTime, scorecardChart, stageFunnel, stageVelocityChart, topAccountsChart, winRateBySegment, winsByCustomerChart, wonValueByChart } from "./charts.ts";
import { compactMoney, longDate, monthYear, percent, ukDates } from "./format.ts";
import type { Calibration } from "../schemas/index.ts";
import { chartSlot, coverScore, document, documentTitle, effortText, estimateLines, paginate, paginateColumns, paragraphs, renderContext, sentence, severityChip, valueFigure, valueText, wrappedLines, type Render } from "./html.ts";
import { noteSections, referenceMap } from "./references.ts";
import { scoreLine } from "./executive.ts";
import { COMPARE_CSS, compareSlide } from "./compare.ts";
import { MONEY_CSS, moneySlides } from "./money.ts";
import { CHAPTER_CSS, chapterSlide } from "./chapter.ts";
import { PROGRESS_CSS, progressPage } from "./progress.ts";
import { DOCUMENTED, DOCUMENTED_METRIC_IDS, readDocument } from "../domains/configuration/documented.ts";
import { accessBlock, automationTable, dataModelDiagram, DIAGRAM_KEY, hasDocument, integrationsBlock, objectsTable, SLIDE_ROWS, sharingTable } from "./documented.ts";
import { surveySlides } from "./survey.ts";
import { interviewSlides } from "./interviews.ts";
import { DRIVER_GAP, readDrivers, WIN_DRIVERS, type Side } from "../domains/historic-pipeline/drivers.ts";

// The full deck: 16:9, 20 to 120 slides for Sales Cloud. Issue slides flex to keep it in range,
// because a thin org and a messy one have very different numbers of findings.

/** Sales Cloud's range. Each optional module switched on raises the ceiling (DECISIONS.md 281). The
 * ceiling leaves headroom over the sample (286): one slide too many costs every area its chapter. */
export const DECK_RANGE = { min: 20, max: 120 } as const;
// A set-up review has no pipeline part, and a tidy org has few issues to show, so
// its floor is lower. The ceiling is the same: the deck still has to be read.
export const SETUP_REVIEW_DECK_RANGE = { min: 12, max: 66 } as const;
// A customer with more Salesforce is paying for a check of more of it, so a
// module gets room of its own rather than squeezing the core report. Essentials
// gets 6, not 5: with 5, Service Cloud's set-up review fell to one table per area.
export const MODULE_ROOM = { complete: { perModule: 10, cap: 150 }, essentials: { perModule: 6, cap: 84 } } as const;
/** The optional modules, each by its areas: a run has one on when it includes any of them. */
const OPTIONAL_MODULES: ReadonlyArray<readonly string[]> = [SERVICE_CLOUD_DOMAINS, CPQ_DOMAINS, ACCOUNT_ENGAGEMENT_DOMAINS];

export const optionalModulesOn = (run: Pick<Run, "modules">): number => OPTIONAL_MODULES.filter((areas) => areas.some((a) => run.modules.includes(a))).length;

export function deckRangeFor(run: Pick<Run, "scope" | "modules">): { min: number; max: number } {
  const [base, room] = isSetupReview(run) ? [SETUP_REVIEW_DECK_RANGE, MODULE_ROOM.essentials] : [DECK_RANGE, MODULE_ROOM.complete];
  return { min: base.min, max: Math.min(room.cap, base.max + room.perModule * optionalModulesOn(run)) };
}

const CSS = `
.page { font-size: 20px; line-height: 1.45; }
.page p + p { margin-top: 10px; }
.footer { left: 0.55in; right: 0.55in; bottom: 0.24in; font-size: 12px; }
.cover { justify-content: space-between; }
.cover h1 { font-size: 68px; font-weight: 700; line-height: 1.1; margin-top: 14px; }
.cover .sub { font-size: 26px; color: var(--panelMuted); }
.cover .for { font-size: 15px; color: var(--panelMuted); margin-bottom: 2px; }
.divider { justify-content: center; }
.divider h1 { font-size: 56px; font-weight: 700; line-height: 1.1; margin: 10px 0 18px; }
.divider .body { max-width: 1000px; font-size: 22px; }
h2 { font-size: 36px; font-weight: 600; line-height: 1.2; margin: 4px 0 16px; }
.eyebrow { font-size: 15px; }
.stat b { font-size: 38px; line-height: 1.2; }
.stat span { font-size: 17px; }
.columns { display: grid; grid-template-columns: 1.5fr 1fr; gap: 32px; align-items: start; }
.panel { background: var(--snow); border-radius: var(--radius-m); padding: 16px 20px; font-size: 18px; }
.panel h3 { font-size: 14px; font-weight: 600; color: var(--muted); margin: 0 0 2px; }
.panel p + h3 { margin-top: 12px; }
.panel .note { font-size: 14.5px; margin-top: 2px; }
.card { background: var(--snow); border-radius: var(--radius-m); padding: 12px 18px; margin-bottom: 10px; font-size: 16.5px; line-height: 1.4; }
.card p + p { margin-top: 4px; }
.card h3 { font-size: 18px; font-weight: 600; margin: 4px 0; }
.two-up { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; }
.two-up .card { font-size: 13.5px; line-height: 1.38; padding: 9px 13px; margin-bottom: 8px; }
.two-up .card h3 { font-size: 14.5px; margin: 3px 0; }
.two-up .card .meta { font-size: 11.5px; }
.two-up.roomy .card { font-size: 16.5px; line-height: 1.42; padding: 12px 16px; margin-bottom: 12px; }
.two-up.roomy .card h3 { font-size: 18px; margin: 4px 0; }
.two-up.roomy .card .meta { font-size: 13.5px; }
.meta > span { white-space: nowrap; flex-shrink: 0; }
.meta > span.amount { white-space: normal; flex-shrink: 1; }
.two-text { columns: 2; column-gap: 28px; }
.two-text li, .two-text tr { break-inside: avoid; }
.action-list.two-text { font-size: 15px; }
.action-list.two-text .owner { font-size: 12.5px; }
.action-list.two-text li { margin-bottom: 6px; }
.two-text .definition { break-inside: avoid; margin-bottom: 9px; font-size: 13.5px; line-height: 1.35; }
.meta { display: flex; gap: 12px; align-items: center; color: var(--muted); font-size: 15px; }
.card .meta { font-size: 13.5px; }
.chart-wrap { max-width: 1040px; }
table.grid { font-size: 18px; }
.tight table.grid { font-size: 16px; }
.tight table.grid td, .tight table.grid th { padding: 5px 8px; }
ol.contents { columns: 2; margin: 0; padding-left: 26px; }
ol.contents li { margin-bottom: 8px; }
/* The next column's item numbers hang into the gap, so a page number keeps clear of the column's edge. */
ol.contents { column-gap: 40px; }
ol.contents li { padding-right: 36px; }
ol.contents .pg { float: right; margin-left: 12px; color: var(--muted); font-variant-numeric: tabular-nums; }
ol.contents.long { columns: 3; font-size: 0.92em; }
ol.contents.long li { margin-bottom: 4px; }
.action-list { margin: 0; padding-left: 22px; font-size: 18px; }
.action-list li { margin-bottom: 8px; }
.quick-panel { margin-top: 18px; background: var(--snow); border-radius: var(--radius-m); padding: 12px 18px; }
.quick-panel h3 { font-size: 18px; margin: 0 0 8px; }
.quick-panel .action-list { font-size: 15.5px; }
.quick-panel .action-list li { margin-bottom: 4px; }
.action-list .owner { color: var(--muted); font-size: 14.5px; }
.chart-pair { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; align-items: start; }
.lede { font-size: 17px; margin-bottom: 14px; }
.small-note { font-size: 13.5px; color: var(--muted); margin-top: 10px; }
.readiness-lede p { margin: 0 0 10px; }
table.readiness { font-size: 14px; }
table.readiness td { vertical-align: top; padding-top: 5px; padding-bottom: 5px; }
table.readiness td:first-child { width: 13%; }
table.readiness td:nth-child(2) { width: 11%; }
.verdict { display: inline-block; border-radius: var(--radius-s); padding: 1px 8px; font-size: 12.5px; font-weight: 600; white-space: nowrap; border: 1px solid var(--carbon); }
.verdict.ready { background: var(--chartreuse); border-color: var(--chartreuse); color: var(--carbon); }
.verdict.not-yet { background: var(--alert); border-color: var(--alert); color: var(--ivory); }
.verdict.unknown { color: var(--muted); border-color: var(--muted); }
table.readiness + h3.sub { margin: 22px 0 6px; }
ol.readiness-fixes { columns: 2; column-gap: 36px; margin: 2px 0 0; padding-left: 22px; font-size: 14px; }
ol.readiness-fixes li { margin-bottom: 4px; break-inside: avoid; }
.data td.num, .data th.num { text-align: right; }
.tight table.data { font-size: 15px; }
.tight table.data td, .tight table.data th { padding: 5px 7px; }
table.data tr.total td { font-weight: 600; border-top: 1px solid var(--carbon); }
.panel .big { font-size: 26px; font-weight: 600; line-height: 1.2; }
h3.cap { font-size: 15px; font-weight: 600; color: var(--muted); margin: 0 0 6px; }
.three-text { columns: 3; column-gap: 24px; }
.two-text.refs { margin-top: 10px; }
.two-text.refs .definition { font-size: 17px; margin-bottom: 11px; }
.three-text.refs { margin-top: 10px; }
.three-text.refs .definition { font-size: 15.5px; margin-bottom: 10px; line-height: 1.3; }
/* 13px, about 7pt printed: the smallest the deck sets reading text (DECISIONS.md 408). */
.three-text .definition { break-inside: avoid; margin-bottom: 8px; font-size: 13px; line-height: 1.32; }
.compact-rows table.grid { font-size: 13px; line-height: 1.3; }
.compact-rows table.grid td, .compact-rows table.grid th { padding: 4px 7px; }
.process { display: grid; grid-template-columns: 300px 1fr; gap: 22px; align-items: start; }
.process .paths .stat { margin-bottom: 12px; }
.chart-pair .stack figure + figure { margin-top: 14px; }
.doc-cols { display: grid; gap: 24px; align-items: start; font-size: 12.5px; line-height: 1.35; }
.doc-cols h3 { font-size: 15px; font-weight: 600; margin: 0 0 6px; }
.doc-cols h3.cap { font-size: 12.5px; margin: 8px 0 2px; }
.doc-cols p { margin: 0 0 4px; }
.doc-cols table.grid { font-size: 12px; }
.doc-cols table.grid td, .doc-cols table.grid th { padding: 3px 6px; }
.doc-list { margin: 0; padding-left: 16px; }
.doc-list li { margin-bottom: 1px; }
table.inventory th { white-space: normal; vertical-align: bottom; font-size: 13.5px; line-height: 1.25; }
.cause { font-size: 14px; line-height: 1.38; color: var(--muted); margin-top: 12px; }
.card p.cause { font-size: 12.5px; margin-top: 6px; }
.two-up .card p.cause { font-size: 11px; line-height: 1.32; margin-top: 4px; }
.two-up.roomy .card p.cause { font-size: 13px; }
table.drivers th { white-space: normal; vertical-align: bottom; line-height: 1.25; }
table.drivers td.gap { white-space: nowrap; }
table.drivers .gap-bar { display: inline-block; height: 10px; margin-right: 8px; vertical-align: middle; background: var(--panelMuted); border-radius: 2px; }
table.drivers .gap-bar.driver { background: var(--carbon); }
table.drivers tr.unmeasured td { color: var(--muted); }
table.drivers .chip { font-size: 11.5px; margin-left: 6px; }
`;

type Slide = { html: string; classes?: string };

// In pixels of a 720px slide, less margins, heading and footer. Deliberately
// cautious: the layout check measures the real thing afterwards.
const ISSUE_SLIDE_BUDGET = 500;
const CARD_CHARS = 105;
const SMALL_CARD_CHARS = 70;
// The cause line is set smaller than the card's text, so more of it fits a line.
const CAUSE_CHARS = 130;
const SMALL_CAUSE_CHARS = 85;
const ROOMY_SCALE = 1.5;
const LIST_BUDGET = 470;

/** What separates wins from losses, for an issue linked to a driver (roadmap 3.3). Null for the rest. */
type CauseOf = (f: Finding) => string | null;
const noCause: CauseOf = () => null;

/** The driver's sentence in small type, pointing to its working. */
const causeLine = (r: Render, cause: string | null): string => (cause ? `<p class="cause">${r.t(cause)}${r.ref(WIN_DRIVERS)}</p>` : "");

// An issue's own slide, measured in Chromium: below a one-line title the columns
// have 507px before the footer, and each further title line takes 43px. In the
// panel, the padding, headings and owner take about 170px around a two-line
// figure, and the working is 14.5px on 21px lines, about 56 characters to a line.
// These hold a line or so back, and the layout check measures the real thing afterwards.
const PANEL_SPACE = 495;
const TITLE_LINE = 44;
const TITLE_CHARS = 48;
const NOTE_LINE = 21;
const NOTE_CHARS = 54;

/**
 * The money working as the panel can hold it: whole lines of the working, in
 * order, while they fit, then where the rest is (DECISIONS.md 416). The
 * figure's own reference stays beside the figure whatever is cut.
 */
function panelNote(r: Render, f: Finding, amount: string, note: string): string {
  const lines = f.value.kind === "not_sized" ? [f.value.reason] : f.value.assumptions;
  const fixed = 34 + 20 + 26 * wrappedLines(amount, 36) + 36 + 26 * wrappedLines(f.owner, 36) + (f.dependencies.length ? 36 + NOTE_LINE * wrappedLines(f.dependencies.join("; "), NOTE_CHARS) : 0);
  const room = Math.floor((PANEL_SPACE - TITLE_LINE * (wrappedLines(f.title, TITLE_CHARS) - 1) - fixed) / NOTE_LINE);
  // Measured as printed: "31 Oct 2024" is longer than its ISO date.
  const used = (kept: string[]): number => wrappedLines(ukDates(sentence(kept.join(". "))), NOTE_CHARS);
  if (used(lines) <= room) return note;
  // A line held for the pointer.
  const kept: string[] = [];
  for (const line of lines) {
    if (used([...kept, line]) > room - 1) break;
    kept.push(line);
  }
  const ref = r.ref(f.id);
  const pointer = ref ? `${r.t("The full working is at")}${ref}.` : r.t("The full working is in the raw notes.");
  return `${kept.length ? `${r.t(sentence(kept.join(". ")))} ` : ""}${pointer}`;
}

function issueSlide(r: Render, label: string, f: Finding, causeOf: CauseOf = noCause): Slide {
  const value = valueText(r, f);
  const note = panelNote(r, f, value.amount, value.note);
  const meta = `<div class="meta">${severityChip(f)}<span>Confidence ${f.confidence}</span><span>${effortText(f)}</span></div>`;
  return {
    html: `<div class="eyebrow">${r.t(label)}</div><h2>${r.t(f.title)}</h2>${meta}<div class="columns" style="margin-top:14px"><div><p>${r.t(f.summary)}</p>${f.recommendation ? `<p style="margin-top:12px"><strong>What to do:</strong> ${r.t(f.recommendation)}</p>` : ""}${causeLine(r, causeOf(f))}</div><div class="panel"><h3>Value</h3><p><strong>${value.amount}</strong>${r.ref(f.id)}</p><p class="note">${note}</p><h3>Owner</h3><p>${r.t(f.owner)}</p>${f.dependencies.length ? `<h3>Depends on</h3><p class="note">${r.t(f.dependencies.join("; "))}</p>` : ""}</div></div>`,
  };
}

function cardHeight(f: Finding, causeOf: CauseOf = noCause): number {
  const cause = causeOf(f);
  return 84 + 24 * estimateLines(f.title, CARD_CHARS) + 24 * estimateLines(f.summary, CARD_CHARS) + 24 * estimateLines(`What to do: ${f.recommendation}`, CARD_CHARS) + (cause ? 8 + 18 * estimateLines(cause, CAUSE_CHARS) : 0);
}

function smallCardHeight(f: Finding, causeOf: CauseOf = noCause): number {
  const cause = causeOf(f);
  return 62 + 19 * estimateLines(f.title, SMALL_CARD_CHARS - 8) + 19 * estimateLines(f.summary, SMALL_CARD_CHARS) + 19 * estimateLines(`What to do: ${f.recommendation}`, SMALL_CARD_CHARS) + (cause ? 6 + 15 * estimateLines(cause, SMALL_CAUSE_CHARS) : 0);
}

function card(r: Render, f: Finding, causeOf: CauseOf = noCause): string {
  return `<article class="card"><div class="meta">${severityChip(f)}<span>${effortText(f)}</span><span class="amount">${valueText(r, f).amount}</span>${r.ref(f.id)}</div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p><p><strong>What to do:</strong> ${r.t(f.recommendation)}</p>${causeLine(r, causeOf(f))}</article>`;
}

// Nothing comes before an area's first issue page, so its title isn't "Other issues".
const groupTitle = (f: Finding): string => (f.severity === "low" || f.severity === "info" ? "Smaller issues" : "The issues in detail");

function issueCards(r: Render, label: string, findings: Finding[], causeOf: CauseOf = noCause): Slide {
  return { html: `<div class="eyebrow">${r.t(label)}</div><h2>${groupTitle(findings[0]!)}</h2>${findings.map((f) => card(r, f, causeOf)).join("")}` };
}

/** Keeps reading order, and splits where the taller column is as short as it can be. */
function balance(findings: Finding[], causeOf: CauseOf): [Finding[], Finding[]] {
  const height = (group: Finding[]): number => group.reduce((sum, f) => sum + smallCardHeight(f, causeOf), 0);
  let best = 1;
  for (let i = 1; i < findings.length; i++) {
    if (Math.max(height(findings.slice(0, i)), height(findings.slice(i))) < Math.max(height(findings.slice(0, best)), height(findings.slice(best)))) best = i;
  }
  return [findings.slice(0, best), findings.slice(best)];
}

/** Cards in two columns, a slide's worth to a group, by height. */
function twoColumnGroups(findings: Finding[], causeOf: CauseOf): Finding[][] {
  const columns = paginate(findings, (f) => smallCardHeight(f, causeOf), ISSUE_SLIDE_BUDGET);
  const groups: Finding[][] = [];
  for (let i = 0; i < columns.length; i += 2) groups.push(columns.slice(i, i + 2).flat());
  return groups;
}

function twoColumnSlide(r: Render, label: string, group: Finding[], causeOf: CauseOf): Slide {
  // A lone card takes the full width. Half an empty slide looks like a mistake.
  if (group.length === 1) return issueCards(r, label, group, causeOf);
  const sides = balance(group, causeOf);
  // Few cards get larger type, so a short slide doesn't look unfinished.
  const roomy = Math.max(...sides.map((side) => side.reduce((sum, f) => sum + smallCardHeight(f, causeOf), 0))) * ROOMY_SCALE <= ISSUE_SLIDE_BUDGET;
  return { html: `<div class="eyebrow">${r.t(label)}</div><h2>${groupTitle(group[0]!)}</h2><div class="two-up${roomy ? " roomy" : ""}">${sides.map((column) => `<div class="col">${column.map((f) => card(r, f, causeOf)).join("")}</div>`).join("")}</div>` };
}

/**
 * Whether a slide's worth of cards fits one slide by height: in two balanced
 * columns, or one under another.
 */
function cardsFit(group: Finding[], density: Density, causeOf: CauseOf): boolean {
  if (density.columns) return Math.max(...balance(group, causeOf).map((side) => side.reduce((sum, f) => sum + smallCardHeight(f, causeOf), 0))) <= ISSUE_SLIDE_BUDGET;
  return group.reduce((sum, f) => sum + cardHeight(f, causeOf), 0) <= ISSUE_SLIDE_BUDGET;
}

/** `columns` sets smaller cards in two columns, so an ordinary org keeps all its detail. */
/** `threeColumnLists` sets the titles-only roadmap list in three columns, as the last step does, before giving up the chapters. */
type Density = Record<Finding["severity"], number> & { columns?: 2; threeColumnLists?: boolean; domainSummary?: boolean };

/** Past this many entries, the contents page goes to three columns. */
const CONTENTS_IN_TWO_COLUMNS = 24;

/** How many issues share a slide, by severity. Tried in order until the deck fits. */
const DENSITIES: Density[] = [
  { critical: 1, high: 1, medium: 1, low: 1, info: 1 },
  { critical: 1, high: 1, medium: 1, low: 2, info: 2 },
  { critical: 1, high: 1, medium: 2, low: 2, info: 2 },
  { critical: 1, high: 1, medium: 2, low: 3, info: 3 },
  { critical: 1, high: 2, medium: 3, low: 4, info: 4 },
  { critical: 2, high: 3, medium: 4, low: 6, info: 6 },
  { critical: 1, high: 1, medium: 6, low: 6, info: 6, columns: 2 },
  { critical: 1, high: 6, medium: 6, low: 6, info: 6, columns: 2 },
  // Up to eight cards a slide, still paginated by height, before the last step
  // gives up the area chapters (DECISIONS.md 208).
  { critical: 1, high: 8, medium: 8, low: 8, info: 8, columns: 2 },
  // The same, with "After 90 days" in three columns: smaller type on a titles-only list, every chapter kept (DECISIONS.md 224).
  { critical: 1, high: 8, medium: 8, low: 8, info: 8, columns: 2, threeColumnLists: true },
  { critical: 24, high: 24, medium: 24, low: 24, info: 24, columns: 2, domainSummary: true },
];

const severityWord = (severity: string): string => `${severity.charAt(0).toUpperCase()}${severity.slice(1)} severity`;


/** The last resort for a very large org: one table per domain, so the deck has a fixed ceiling. */
const TABLE_ROWS = 7;
const COMPACT_NOTE = "This list is cut short to keep the report readable. The raw notes have all of it.";

function issueTable(r: Render, label: string, issues: Finding[]): Slide[] {
  if (issues.length === 0) return [];
  const rows = issues.slice(0, TABLE_ROWS).map((f) => `<tr><td>${r.t(f.title)}${r.ref(f.id)}</td><td>${severityChip(f)}</td><td>${valueFigure(r, f)}</td><td>${r.t(f.owner)}</td></tr>`);
  const note = issues.length > TABLE_ROWS ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : "";
  return [{ classes: "tight", html: `<div class="eyebrow">${r.t(label)}</div><h2>Issues</h2>${note}<table class="grid"><thead><tr><th>Issue</th><th style="width:10%">Severity</th><th style="width:24%">Value</th><th style="width:18%">Owner</th></tr></thead><tbody>${rows.join("")}</tbody></table>` }];
}

/** An issue on its own slide, or a slide's worth of cards. */
export type CardPiece<T> = { one: T } | { cards: T[] };

/**
 * A single card left over by the count goes back onto the slide of cards just
 * before it, where the two fit one slide (DECISIONS.md 409). Never across an
 * issue that has its own slide, so reading order holds.
 */
export function foldLoneCards<T>(pieces: Array<CardPiece<T>>, fits: (cards: T[]) => boolean): Array<CardPiece<T>> {
  return pieces.reduce<Array<CardPiece<T>>>((out, piece) => {
    const before = out.at(-1);
    if ("cards" in piece && piece.cards.length === 1 && before && "cards" in before && fits([...before.cards, ...piece.cards])) out[out.length - 1] = { cards: [...before.cards, ...piece.cards] };
    else out.push(piece);
    return out;
  }, []);
}

function domainIssueSlides(r: Render, label: string, issues: Finding[], density: Density, causeOf: CauseOf): Slide[] {
  const pieces: Array<CardPiece<Finding>> = [];
  let queue: Finding[] = [];
  const flush = (): void => {
    const groups = density.columns ? twoColumnGroups(queue, causeOf) : paginate(queue, (f) => cardHeight(f, causeOf), ISSUE_SLIDE_BUDGET);
    pieces.push(...groups.map((cards) => ({ cards })));
    queue = [];
  };
  for (const f of issues) {
    const perSlide = density[f.severity];
    if (perSlide === 1) {
      flush();
      pieces.push({ one: f });
    } else {
      queue.push(f);
      if (queue.length >= perSlide) flush();
    }
  }
  flush();
  const folded = foldLoneCards(pieces, (cards) => cardsFit(cards, density, causeOf));
  return folded.map((piece) => ("one" in piece ? issueSlide(r, label, piece.one, causeOf) : density.columns ? twoColumnSlide(r, label, piece.cards, causeOf) : issueCards(r, label, piece.cards, causeOf)));
}

function metricRows(r: Render, metrics: Metric[], currency: string): string {
  const rows = metrics.map((m) => {
    if (m.status === "not_measurable") return `<tr><td>${r.t(m.label)}${r.ref(m.id)}</td><td class="num">Not measurable</td><td>${r.t(m.reason)}</td></tr>`;
    const value = m.unit === "percent" ? percent(m.value) : m.unit === "days" ? `${m.value} days` : m.unit === "currency" ? compactMoney(m.value, currency) : String(m.value);
    return `<tr><td>${r.t(m.label)}${r.ref(m.id)}</td><td class="num">${value}</td><td>${r.t(m.definition)}</td></tr>`;
  });
  return `<table class="grid"><thead><tr><th>Measure</th><th class="num">Result</th><th>What it means</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
}

// The pipeline deep dive. Every figure on these slides is a metric's value,
// segment, series point or stat, so the number check can trace each one.

type Measured = Extract<Metric, { status: "measured" }>;
const asMeasured = (m: Metric | undefined): Measured | null => (m?.status === "measured" ? m : null);
const segmentValue = (m: Measured | null, dimension: string, key: string): MetricSegmentLike | undefined => m?.segments.find((s) => s.dimension === dimension && s.key === key);
type MetricSegmentLike = Measured["segments"][number];
const notMeasured = (r: Render, m: Metric | undefined): string => `<p class="chart-gap"><strong>Not measurable.</strong> ${r.t(m?.status === "not_measurable" ? m.reason : "This metric wasn't part of the run.")}</p>`;

/** Open stages in the calibration's sales order, then anything else in name order. */
function stageOrder(calibration: Calibration): (a: string, b: string) => number {
  const order = new Map(calibration.stages.filter((s) => s.category === "open").map((s) => [s.name, s.order ?? 0]));
  return (a, b) => (order.get(a) ?? Number.MAX_SAFE_INTEGER) - (order.get(b) ?? Number.MAX_SAFE_INTEGER) || a.localeCompare(b);
}

function pipelineTodaySlide(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const head = `<div class="eyebrow">Pipeline today</div><h2>Open pipeline by stage</h2>`;
  const count = asMeasured(metric("current.open_deals"));
  if (!count) return { html: `${head}${notMeasured(r, metric("current.open_deals"))}` };
  const [amount, blank, age, late, weighted, rates] = ["current.open_pipeline", "current.open_no_amount", "current.open_median_age", "current.open_past_close", "current.weighted_pipeline", "historic.stage_win_rate"].map((id) => asMeasured(metric(id)));
  const retired = new Set(count.segments.filter((s) => s.dimension === "retired stage").map((s) => s.key));
  const stages = count.segments.filter((s) => s.dimension === "stage").map((s) => s.key).sort(stageOrder(calibration));
  const money = (v: number | null | undefined): string => (typeof v === "number" ? compactMoney(v, r.currency) : "Not measured");
  const plain = (v: number | null | undefined, fallback = "Not measured"): string => (typeof v === "number" ? String(v) : fallback);
  const rate = (stage: string): string => {
    const s = segmentValue(rates, "stage", stage);
    return s?.value != null ? percent(s.value) : s ? "Too few" : "Not measured";
  };
  const rows = stages.map((stage) => {
    const cell = (m: Measured | null): MetricSegmentLike | undefined => segmentValue(m, "stage", stage);
    const ageCell = cell(age);
    return `<tr><td>${r.t(stage)}${retired.has(stage) ? " (retired)" : ""}</td><td class="num">${cell(count)!.value}</td><td class="num">${money(cell(amount)?.value)}</td><td class="num">${plain(cell(blank)?.value)}</td><td class="num">${ageCell ? plain(ageCell.value, "Too few") : "Not measured"}</td><td class="num">${plain(cell(late)?.value)}</td><td class="num">${rate(stage)}</td><td class="num">${cell(weighted) ? (cell(weighted)!.value === null ? "Not weighted" : money(cell(weighted)!.value)) : "Not measured"}</td></tr>`;
  });
  rows.push(`<tr class="total"><td>All open deals</td><td class="num">${count.value}</td><td class="num">${money(amount?.value)}</td><td class="num">${plain(blank?.value)}</td><td class="num">${plain(age?.value)}</td><td class="num">${plain(late?.value)}</td><td class="num">${rates ? percent(rates.value) : "Not measured"}</td><td class="num">${money(weighted?.value)}</td></tr>`);
  const notes = [
    "Weighted value is each deal's amount at the share of closed deals from that stage that went on to win. Median age is days since the deal was created.",
    ...(weighted?.exclusions.filter((e) => /too few closed deals/.test(e)) ?? []).map((e) => `Not weighted: ${e}.`),
    ...(retired.size ? [`A retired stage is no longer active in Salesforce but still holds open deals.`] : []),
  ];
  const table = `<table class="grid data"><thead><tr><th>Stage</th><th class="num">Open deals</th><th class="num">Amount</th><th class="num">No amount</th><th class="num">Median age, days</th><th class="num">Past close date</th><th class="num">Won from here</th><th class="num">Weighted value</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
  return { classes: "tight", html: `${head}${table}${notes.map((n) => `<p class="small-note">${r.t(n)}</p>`).join("")}` };
}

function outlookSlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const value = metric("current.delivery_outlook_value");
  const count = asMeasured(metric("current.delivery_outlook_deals"));
  const m = asMeasured(value);
  const head = `<div class="eyebrow">Pipeline today</div><h2>Delivery outlook: what's likely to be won, month by month</h2>`;
  if (!m) return { html: `${head}${chartSlot(deliveryOutlookChart(value, count ?? undefined, r.currency))}` };
  const first = m.series[0]!.date;
  const last = m.series.at(-1)!.date;
  const timing = (key: string): MetricSegmentLike | undefined => segmentValue(m, "timing", key);
  const passed = timing("Expected date already passed");
  const later = timing("Expected after the outlook");
  const panel = [
    `<h3>Expected from ${monthYear(first)} to ${monthYear(last)}</h3><p class="big">${compactMoney(m.value, r.currency)}</p>${count ? `<p class="note">About ${count.value} deals</p>` : ""}`,
    passed && passed.n > 0 ? `<h3>Expected date already passed</h3><p><strong>${compactMoney(passed.value ?? 0, r.currency)}</strong> from ${passed.n} deals with no month</p>` : "",
    later && later.n > 0 ? `<h3>Expected after ${monthYear(last)}</h3><p><strong>${compactMoney(later.value ?? 0, r.currency)}</strong> from ${later.n} deals</p>` : "",
    `<h3>How we work it out</h3><p class="note">${m.assumptions.slice(1).map((a) => r.t(a)).join(". ")}.</p>`,
    m.exclusions.length ? `<p class="note">Left out: ${m.exclusions.map((e) => r.t(e)).join("; ")}.</p>` : "",
  ].join("");
  return {
    html: `${head}<p class="lede">For planning delivery capacity: the work you can expect to win, by when it's likely to close.</p><div class="columns"><div class="chart-wrap">${chartSlot(deliveryOutlookChart(value, count ?? undefined, r.currency))}</div><div class="panel">${panel}</div></div>`,
  };
}

function quarterlySlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const head = `<div class="eyebrow">Pipeline over time</div><h2>Quarter by quarter</h2>`;
  const ids = ["historic.quarterly_created", "historic.quarterly_won", "historic.quarterly_won_value", "historic.quarterly_win_rate", "historic.quarterly_median_won_size", "historic.quarterly_median_cycle"];
  const all = ids.map((id) => asMeasured(metric(id)));
  const spine = all.find((m) => m !== null);
  if (!spine) return { html: `${head}${notMeasured(r, metric(ids[0]!))}` };
  const format = [(v: number) => String(v), (v: number) => String(v), (v: number) => compactMoney(v, r.currency), (v: number) => percent(v), (v: number) => compactMoney(v, r.currency), (v: number) => String(v)];
  const rows = spine.series.map((point, i) => {
    const cells = all.map((m, k) => {
      if (!m) return "Not measured";
      const p = m.series.find((x) => x.date === point.date);
      return p?.value != null ? format[k]!(p.value) : p?.notMeasurableReason?.startsWith("Starts before") ? "Before the window" : "Too few";
    });
    const label = `${monthYear(point.date)}${i === spine.series.length - 1 ? ", to date" : ""}`;
    return `<tr><td>${label}</td>${cells.map((c) => `<td class="num">${c}</td>`).join("")}</tr>`;
  });
  const missing = ids.filter((_, k) => !all[k]).map((id) => metric(id)).filter((m): m is Extract<Metric, { status: "not_measurable" }> => m?.status === "not_measurable");
  const notes = ["Too few means fewer closed deals than the calibrated minimum. The last quarter is still running.", ...missing.map((m) => `${m.label}: ${m.reason}`)];
  return { classes: "tight", html: `${head}<table class="grid data"><thead><tr><th>Quarter starting</th><th class="num">Deals created</th><th class="num">Deals won</th><th class="num">Won value</th><th class="num">Win rate</th><th class="num">Median won deal</th><th class="num">Median cycle, days</th></tr></thead><tbody>${rows.join("")}</tbody></table>${notes.map((n) => `<p class="small-note">${r.t(n)}</p>`).join("")}` };
}

// Why these counts can differ from the lead funnel's and the new-and-repeat split (night 3 reading pass),
// and from the stage-based pages' won deals (DECISIONS.md 364).
const WINS_BASIS = "Every deal won in the window, by its own type and lead source, including any entered straight as won. The lead funnel and the new-and-repeat split count on other bases.";

function winsSlides(r: Render, metric: (id: string) => Metric | undefined): Slide[] {
  const repeat = asMeasured(metric("historic.repeat_win_share"));
  const top = asMeasured(metric("historic.top_account_share"));
  const lines = [
    repeat ? `${percent(repeat.value)} of won value came from repeat customers: accounts that had already won a deal with you.` : "",
    top ? `Your top ${top.stats.top} accounts brought in ${percent(top.value)} of won value. Accounts are counted here, not named.` : "",
  ].filter(Boolean);
  return [
    { html: `<div class="eyebrow">Where wins come from</div><h2>New and repeat customers</h2>${lines.map((l) => `<p class="lede">${r.t(l)}</p>`).join("")}<div class="chart-pair">${chartSlot(winsByCustomerChart(metric("historic.won_value_by_customer"), r.currency))}${chartSlot(topAccountsChart(metric("historic.top_account_share")))}</div>` },
    { html: `<div class="eyebrow">Where wins come from</div><h2>Wins by type and lead source</h2><div class="chart-pair">${chartSlot(wonValueByChart(metric("historic.won_value_by_type"), "type", "won-by-type", "Won value by type", r.currency))}${chartSlot(wonValueByChart(metric("historic.won_value_by_source"), "lead source", "won-by-source", "Won value by lead source", r.currency))}</div><p class="small-note">${r.t(WINS_BASIS)}</p>` },
  ];
}

function lostSlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const count = asMeasured(metric("historic.lost_by_stage"));
  const lede = count ? `<p class="lede">${count.value} lost and no decision deals closed in the window. This is the last open stage each was in.</p>` : "";
  return { html: `<div class="eyebrow">Where deals are lost</div><h2>Where deals drop out</h2>${lede}<div class="chart-pair">${chartSlot(lostByStageChart(metric("historic.lost_by_stage"), metric("historic.lost_value_by_stage"), r.currency))}${chartSlot(closedAsChart(metric("historic.lost_by_stage")))}</div>` };
}

function closeDatesSlide(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const share = metric("historic.close_date_reliability");
  const late = metric("historic.close_days_late");
  const block = (m: Metric | undefined, body: (x: Measured) => string): string => (m?.status === "measured" ? body(m) : `<h3>${r.t(m?.label ?? "Not measured")}</h3><p class="note">${r.t(m?.status === "not_measurable" ? m.reason : "This metric wasn't part of the run.")}</p>`);
  const panel = [
    block(share, (m) => `<h3>Won in the quarter first forecast</h3><p class="big">${percent(m.value)}</p><p class="note">of the ${m.n} won deals we can follow through their stages</p>`),
    block(late, (m) => `<h3>Days late against the first close date</h3><p class="big">${m.value} days</p><p class="note">For the median won deal. The typical range is ${m.stats.p25} to ${m.stats.p75} days.</p>`),
  ].join("");
  const order = calibration.stages.filter((s) => s.category === "open").sort((a, b) => (a.order ?? 0) - (b.order ?? 0)).map((s) => s.name);
  return { html: `<div class="eyebrow">Close dates and stage speed</div><h2>How far close dates hold, and where time goes</h2><div class="columns" style="grid-template-columns: 1fr 1.5fr"><div class="panel">${panel}</div><div><h3 class="cap">Median days in each stage, won and lost deals</h3>${chartSlot(stageVelocityChart(metric("historic.stage_velocity_won"), metric("historic.stage_velocity_lost"), order))}</div></div>` };
}

/** The close: what we'd do next, together. Its only figures are the plan's own. */
/** The paths deals most often take, and every move between stages as a table. */
/** Adoption's charts, after its "At a glance" slide. Empty when none could be drawn. */
function adoptionSlide(r: Render, metric: (id: string) => Metric | undefined): string {
  const charts = [activeUsersChart(metric("adoption.monthly_active_users")), loginChannelsChart(metric("adoption.mobile_login_share")), lightningChart(metric("adoption.lightning_share"))];
  if (charts.every((c) => c.kind === "gap")) return "";
  const [active, channels, lightning] = charts as [Chart, Chart, Chart];
  // References in small type beside the eyebrow, not at heading size.
  const refs = ["adoption.monthly_active_users", "adoption.mobile_login_share", "adoption.lightning_share"].map((id) => r.ref(id)).join("");
  return `<div class="eyebrow">Adoption and usage${refs}</div><h2>Who uses Salesforce, and how</h2><div class="chart-pair"><div><h3 class="cap">Monthly active users</h3>${chartSlot(active)}</div><div class="stack">${chartSlot(channels)}${chartSlot(lightning)}</div></div>`;
}

function processSlide(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const head = `<div class="eyebrow">Pipeline</div><h2>How your sales process really runs</h2>`;
  const path = (id: string, noun: string): string => {
    const m = asMeasured(metric(id));
    if (!m) return `<div class="stat"><h3 class="cap">${r.t(metric(id)?.label ?? id)}</h3>${notMeasured(r, metric(id))}</div>`;
    return `<div class="stat"><h3 class="cap">${r.t(m.label)}${r.ref(m.id)}</h3><p style="font-size:20px;margin:6px 0">${r.t(m.segments[0]!.key)}</p><span>${percent(m.value)} of ${m.n} ${noun}</span></div>`;
  };
  // The flow takes the room. The table of every move is in the raw notes.
  const moves = metric("historic.stage_transitions");
  const stages = calibration.stages
    .filter((s) => s.category !== "excluded")
    .sort((a, b) => (a.order ?? 99) - (b.order ?? 99))
    .map((s) => ({ name: s.name, category: s.category === "open" ? ("open" as const) : s.category === "won" ? ("won" as const) : ("lost" as const) }));
  const flow = `<h3 class="cap">Moves between stages${r.ref("historic.stage_transitions")}</h3>${chartSlot(processFlowChart(moves, stages))}`;
  return { classes: "tight compact-rows", html: `${head}<div class="process"><div class="paths">${path("historic.path_to_won", "won deals")}${path("historic.path_to_lost", "lost and no decision deals")}</div><div>${flow}</div></div>` };
}

/** Each factor's win rate with it and without it, ranked by the gap. A factor that couldn't be compared says why. */
function driversSlide(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const m = asMeasured(metric(WIN_DRIVERS))!;
  const drivers = readDrivers(m);
  const widest = Math.max(DRIVER_GAP, ...drivers.map((d) => d.gap ?? 0));
  const rate = (s: Side): string => (s.rate === null ? "" : percent(s.rate));
  const rows = drivers.map((d) => {
    // Which way the gap points, so a driver that went with winning isn't read as advice to do it (night 3 reading pass).
    const way = d.with.rate !== null && d.without.rate !== null && d.with.rate > d.without.rate ? "goes with winning" : "goes with losing";
    const label = `<td>${r.t(d.label)}${d.isDriver ? ` <span class="chip">Driver: ${way}</span>` : ""}</td>`;
    if (d.gap === null) return `<tr class="unmeasured">${label}<td colspan="6">Not measurable. ${r.t(d.reason ?? "")}</td></tr>`;
    // The bar is drawn, not written, so it carries no figure of its own.
    const bar = `<span class="gap-bar${d.isDriver ? " driver" : ""}" style="width:${Math.max(2, Math.round((90 * d.gap) / widest))}px"></span>`;
    return `<tr>${label}<td class="num">${d.with.n}</td><td class="num">${rate(d.with)}</td><td class="num">${d.without.n}</td><td class="num">${rate(d.without)}</td><td class="num gap">${bar}${d.gap} points</td><td class="num">${d.compared ? `${d.held} of ${d.compared}` : "None"}</td></tr>`;
  });
  const lede = `Measured from your own ${m.n} won and lost deals that closed in the window after being open. For each thing we can read for every deal, the win rate of deals with it and without it. A gap of ${DRIVER_GAP} points or more is a driver. It shows how often things went together, not proof that one caused the other.`;
  const notes = [
    "Win rate is won deals as a share of won and lost deals, as in your headline win rate.",
    ...(m.stats.noDecision ? [`${m.stats.noDecision} no decision deals are left out of it.`] : []),
    ...(m.stats.cycleLineDays !== undefined ? [`A long cycle is over ${m.stats.cycleLineDays} days from creation to close.`] : []),
    "The last column is how many teams, record types and other segments with enough deals on both sides show the gap the same way.",
    // Why this can say none while the skipped-stage issue counts many (DECISIONS.md 352).
    ...(drivers.some((d) => d.key === "skipped")
      ? ["Moving past an expected stage means moving on to a later open stage without entering it. Closing doesn't count: if it did, every won deal that closes without entering an expected stage would, which is what the skipped-stage issue counts, and no lost deal could, so the comparison would only measure winning."]
      : []),
  ];
  const table = `<table class="grid data drivers"><thead><tr><th>Factor</th><th class="num">Deals with it</th><th class="num">Win rate</th><th class="num">Deals without it</th><th class="num">Win rate</th><th class="num">Gap</th><th class="num">Same way in segments</th></tr></thead><tbody>${rows.join("")}</tbody></table>`;
  return { classes: "tight", html: `<div class="eyebrow">Pipeline${r.ref(WIN_DRIVERS)}</div><h2>What separates your wins from your losses</h2><p class="lede">${r.t(lede)}</p>${table}<p class="small-note">${r.t(notes.join(" "))}</p>` };
}

function nextStepsSlide(r: Render, plan: ReturnType<typeof buildPlan>["phases"]): Slide {
  const first = plan.find((p) => p.findings.length > 0);
  const opening = first
    ? `Name an owner for each of the ${first.findings.length} ${first.findings.length === 1 ? "action" : "actions"} in days ${first.from} to ${first.to}, starting with this: ${first.findings[0]!.title}.`
    : "Agree who owns each action on the roadmap, and when it's due.";
  const steps = [
    { title: "Walk through it together", text: "We'll take your team through what we found, answer their questions, and agree what matters most to you." },
    { title: "Start the first month", text: opening },
    { title: "Measure the change", text: "We'll look at your Salesforce again after 90 days, so you can see what has improved and what still needs work." },
  ];
  return {
    html: `<div class="eyebrow">Next steps</div><h2>What happens next</h2><div class="stat-row" style="margin-top:18px">${steps.map((s) => `<div class="stat"><h3 style="font-size:22px;margin:0 0 8px">${r.t(s.title)}</h3><p>${r.t(s.text)}</p></div>`).join("")}</div><p class="lede" style="margin-top:28px">Black Cloud can deliver this plan alongside your team. Your consultant will talk you through what that would look like.</p>`,
  };
}

/** Rows on the slide. The raw notes list every object, so this can't overflow. */
export const INVENTORY_SLIDE_ROWS = 10;

function inventorySlide(r: Render, label: string, metric: Metric | undefined): Slide {
  const head = `<div class="eyebrow">${r.t(label)}</div><h2>Where the complexity lives</h2>`;
  const m = asMeasured(metric);
  if (!m) return { html: `${head}${notMeasured(r, metric)}` };
  const all = inventoryRows(m);
  const shown = all.slice(0, INVENTORY_SLIDE_ROWS);
  // Records stay in the raw notes: this table is about what a newcomer has to learn.
  const columns = INVENTORY_COLUMNS.filter((c) => c.key !== "records");
  const cell = (v: number | null): string => `<td class="num">${v === null ? "Not read" : v}</td>`;
  const rows = shown.map((row) => `<tr><td>${r.t(row.label)}</td>${columns.map((c) => cell(row.counts[c.key])).join("")}</tr>`);
  const own = m.stats.customObjects ? `your ${m.stats.coreObjects} core objects and ${m.stats.customObjects} custom objects of your own with records` : `your ${m.stats.coreObjects} core objects, and no custom objects of your own with records`;
  const automation = m.stats.automation ? ` Of the ${m.stats.automation} rules, flows and triggers on them, ${m.stats.undocumented ? `${m.stats.undocumented} have no description` : "every one has a description"}.` : " There's no automation on them.";
  const lede = `We counted ${m.value} objects: ${own}.${automation}`;
  const notes = [
    `Ranked by how much automation runs on each object, then by custom fields.${all.length > shown.length ? " Only the busiest are shown here. The raw notes list every object." : ""} No description means no description field, or for an Apex trigger, no comment in the code.`,
    ...m.exclusions.filter((e) => /couldn't/.test(e)).map((e) => `Not counted: ${e}.`),
  ];
  return {
    classes: "tight",
    html: `${head}<p class="lede">${r.t(lede)}</p><table class="grid data inventory"><thead><tr><th>Object</th>${columns.map((c) => `<th class="num">${c.heading}</th>`).join("")}</tr></thead><tbody>${rows.join("")}</tbody></table>${notes.map((n) => `<p class="small-note">${r.t(n)}</p>`).join("")}`,
  };
}

// A dense deck keeps every chart that answers a question, on fewer slides.

function winsSlideDense(r: Render, metric: (id: string) => Metric | undefined): Slide {
  const [first] = winsSlides(r, metric);
  const lede = /<\/h2>((?:<p class="lede">[\s\S]*?<\/p>)*)/.exec(first!.html)?.[1] ?? "";
  const grid = [
    winsByCustomerChart(metric("historic.won_value_by_customer"), r.currency),
    topAccountsChart(metric("historic.top_account_share")),
    wonValueByChart(metric("historic.won_value_by_type"), "type", "won-by-type", "Won value by type", r.currency),
    wonValueByChart(metric("historic.won_value_by_source"), "lead source", "won-by-source", "Won value by lead source", r.currency),
  ].map(chartSlot);
  return { html: `<div class="eyebrow">Where wins come from</div><h2>New and repeat customers, type and lead source</h2>${lede.replace(/class="lede"/g, 'class="lede" style="font-size:15px;margin-bottom:8px"')}<div class="chart-pair" style="row-gap:12px">${grid.join("")}</div><p class="small-note">${r.t(WINS_BASIS)}</p>` };
}

function lostAndSpeedSlideDense(r: Render, metric: (id: string) => Metric | undefined, calibration: Calibration): Slide {
  const count = asMeasured(metric("historic.lost_by_stage"));
  const share = asMeasured(metric("historic.close_date_reliability"));
  const late = asMeasured(metric("historic.close_days_late"));
  const closedAs = count ? count.segments.filter((s) => s.dimension === "closed as") : [];
  const lines = [
    count ? `${count.value} lost and no decision deals closed in the window, shown by the last open stage each was in.${closedAs.length ? ` By how they closed: ${closedAs.map((s) => `${s.key} ${s.value}`).join(", ")}.` : ""}` : "",
    share && late ? `${percent(share.value)} of the ${share.n} won deals we can follow through their stages closed in the quarter first forecast, and the median won deal closed ${late.value} days after its first close date.` : "",
  ].filter(Boolean);
  const order = calibration.stages.filter((s) => s.category === "open").sort((a, b) => (a.order ?? 0) - (b.order ?? 0)).map((s) => s.name);
  return { html: `<div class="eyebrow">Where deals are lost</div><h2>Where deals drop out, and where time goes</h2>${lines.map((l) => `<p class="lede" style="font-size:15px;margin-bottom:8px">${r.t(l)}</p>`).join("")}<div class="chart-pair">${chartSlot(lostByStageChart(metric("historic.lost_by_stage"), metric("historic.lost_value_by_stage"), r.currency))}<div><h3 class="cap">Median days in each stage, won and lost deals</h3>${chartSlot(stageVelocityChart(metric("historic.stage_velocity_won"), metric("historic.stage_velocity_lost"), order))}</div></div>` };
}

/** A line of a list slide. `ref` is its reference, or several, as for an action that covers more than one issue. */
export type ListItem = { title: string; detail: string; ref?: string | string[] };
const refsOf = (r: Render, ref: ListItem["ref"]): string => (ref === undefined ? "" : [ref].flat().map((id) => r.ref(id)).join(""));
/** A list item's title with its references, as characters: a reference is about six (DECISIONS.md 408). */
const titleText = (x: ListItem): string => `${x.title}${"\u00a0".repeat(6 * (x.ref === undefined ? 0 : [x.ref].flat().length))}`;

// A three-column list as Chromium sets it (DECISIONS.md 442). A column is 375px
// wide at 13px. Measured by the height of each item, on long-worded prose, the
// long words alone and the word target test's vocabulary, from every word at
// every length to 60 words, the fewest characters a line that never counted a
// line too few was 46 on the prose, bold or not, and 51 to 53 on the rest; 52
// counted 60 of the prose's lines short. Both take 44, so a page is started early
// rather than late. A line is 17.16px (13px at 1.32), and an item's 8px margin is
// taken as 10. A column runs 542px from the top of the list to the footer, under
// a one-line title, on every list and sample; 525 keeps clear of the footer.
const THREE_COLUMN_TITLE_CHARS = 44;
const THREE_COLUMN_DETAIL_CHARS = 44;
const THREE_COLUMN_LINE = 17.2;
const THREE_COLUMN_ITEM_GAP = 10;
const THREE_COLUMN_HEIGHT = 525;

/** An item's height in a three-column list, margin and all. The title can wrap too, once its references are beside it. */
export const threeColumnItemHeight = (x: ListItem): number => THREE_COLUMN_ITEM_GAP + THREE_COLUMN_LINE * (estimateLines(titleText(x), THREE_COLUMN_TITLE_CHARS) + estimateLines(x.detail, THREE_COLUMN_DETAIL_CHARS));

/** Definitions are reference, so a dense deck sets them small, in three columns. */
function threeColumnList(r: Render, eyebrow: string, title: string, items: ListItem[]): Slide[] {
  // Items don't break, so the columns are filled one at a time, as the browser fills them.
  const pages = paginateColumns(items, threeColumnItemHeight, 3, THREE_COLUMN_HEIGHT);
  return pages.map((group, i) => ({
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2><div class="three-text">${group.map((x) => `<p class="definition"><strong>${r.t(x.title)}</strong>${refsOf(r, x.ref)}<br>${r.t(x.detail)}</p>`).join("")}</div>`,
  }));
}

const sectionBody = (r: Render, built: BuiltOutput, key: string): string => paragraphs(r, currentBody(built, key));

const VERDICT_CLASS: Record<ReadinessVerdict, string> = { ready: "ready", nearly: "nearly", not_yet: "not-yet", not_measurable: "unknown" };

/**
 * AI readiness on one slide (DECISIONS.md 242): each pillar's verdict, decided
 * by code, why, and the findings behind it, then what to fix before an agent
 * goes live. The opening is the section's text, which Claude may word.
 */
function readinessSlide(r: Render, report: BuiltReport, built: BuiltOutput): string {
  const { pillars, fixes } = report.readiness;
  // The questions are in the raw notes: here, the label is enough to read the row.
  const rows = pillars.map((p) => `<tr><td><strong>${r.t(p.label)}</strong></td><td><span class="verdict ${VERDICT_CLASS[p.verdict]}">${VERDICT_WORDS[p.verdict]}</span></td><td>${r.t(p.reason)}${p.findingIds.slice(0, 2).map((id) => r.ref(id)).join("")}</td></tr>`);
  const todo = fixes.length
    ? `<h3 class="sub">What to fix before an agent goes live, in order</h3><ol class="readiness-fixes">${fixes.map((f) => `<li>${r.t(f.title)}${r.ref(f.id)}</li>`).join("")}</ol>`
    : `<p class="small-note">${r.t("We found nothing that needs fixing first.")}</p>`;
  return `<div class="eyebrow">AI readiness</div><h2>Could an AI agent work safely here?</h2><div class="lede readiness-lede">${sectionBody(r, built, "ai_readiness")}</div><table class="grid readiness"><tbody>${rows.join("")}</tbody></table>${todo}`;
}

const needsInput = (built: BuiltOutput, key: string): boolean => built.sections.find((s) => s.key === key)?.status === "needs_input";

const inputChip = '<p style="margin-bottom:10px"><span class="chip input">Needs input from a consultant</span></p>';

function listSlides(r: Render, eyebrow: string, title: string, items: ListItem[], compact: boolean): Slide[] {
  // An empty list is left out rather than given a page that says so.
  if (items.length === 0) return [];
  const pages = paginate(items, (x) => 16 + 24 * Math.max(estimateLines(x.detail, 85), estimateLines(x.title, 34)), LIST_BUDGET);
  const cut = compact && pages.length > 1;
  return (cut ? pages.slice(0, 1) : pages).map((group, i) => ({
    classes: "tight",
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2>${cut ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : ""}<table class="grid"><tbody>${(cut ? group.slice(0, -1) : group).map((x) => `<tr><td style="width:30%"><strong>${r.t(x.title)}</strong>${refsOf(r, x.ref)}</td><td>${r.t(x.detail)}</td></tr>`).join("")}</tbody></table>`,
  }));
}

function twoColumnList(r: Render, eyebrow: string, title: string, items: ListItem[]): Slide[] {
  const pages = paginate(items, (x) => 9 + 19 * (estimateLines(titleText(x), 64) + estimateLines(x.detail, 72)), 2 * LIST_BUDGET);
  return pages.map((group, i) => ({
    html: `<div class="eyebrow">${r.t(eyebrow)}</div><h2>${r.t(title)}${i ? ", continued" : ""}</h2><div class="two-text">${group.map((x) => `<p class="definition"><strong>${r.t(x.title)}</strong>${refsOf(r, x.ref)}<br>${r.t(x.detail)}</p>`).join("")}</div>`,
  }));
}

const DOCUMENTED_INTRO = "A reference for whoever looks after Salesforce: what the org holds, what runs when a record is saved, what connects to it, who uses it and who can see what. It describes rather than judges, and the full lists are in the raw notes.";

/** The reference part: the data model, automation in the order it runs, then integrations, users and sharing together. A part with nothing measured is left out. */
function documentedSlides(r: Render, metrics: Metric[], dense: boolean): Slide[] {
  const doc = readDocument(metrics);
  if (!hasDocument(doc)) return [];
  const eyebrow = `<div class="eyebrow">Your org, documented</div>`;
  const slides: Slide[] = [];
  if (!dense) slides.push({ classes: "dark divider", html: `<div class="eyebrow">Reference</div><h1>Your org, documented</h1><div class="body">${paragraphs(r, DOCUMENTED_INTRO)}</div>` });
  if (doc.model) {
    const diagram = doc.relationships ? `${chartSlot(dataModelDiagram(doc))}<p class="small-note">${r.t(DIAGRAM_KEY)}${r.ref(DOCUMENTED.relationships)}</p>` : chartSlot(dataModelDiagram(doc));
    slides.push({ classes: "tight compact-rows", html: `${eyebrow}<h2>The data model${r.ref(DOCUMENTED.model)}</h2><div class="chart-pair"><div>${diagram}</div><div>${objectsTable(r, doc, SLIDE_ROWS)}</div></div>` });
  }
  if (doc.automation && doc.automation.objects.length) {
    slides.push({ classes: "tight compact-rows", html: `${eyebrow}<h2>What runs when a record is saved${r.ref(DOCUMENTED.automation)}</h2><p class="lede">${r.t("By object, in the order Salesforce runs each kind. Where some are inactive, a cell shows the active ones out of all.")}</p>${automationTable(r, doc, SLIDE_ROWS)}` });
  }
  const columns = [
    doc.integrations ? `<div><h3>${r.t("Integrations")}${r.ref(DOCUMENTED.integrations)}</h3>${integrationsBlock(r, doc, 6)}</div>` : "",
    doc.access ? `<div><h3>${r.t("Users and access")}${r.ref(DOCUMENTED.access)}</h3>${accessBlock(r, doc, 6)}</div>` : "",
    doc.sharing ? `<div><h3>${r.t("Who can see what")}${r.ref(DOCUMENTED.sharing)}</h3>${sharingTable(r, doc)}<p class="small-note">${r.t("The organisation-wide default for each object.")}</p></div>` : "",
  ].filter(Boolean);
  if (columns.length) slides.push({ html: `${eyebrow}<h2>Integrations, users and security</h2><div class="doc-cols" style="grid-template-columns:repeat(${columns.length},1fr)">${columns.join("")}</div>` });
  return slides;
}

/** The last page: how a reference leads to the working, and what each section of the raw notes holds. */
// Past this many sections, two columns run off the page.
const REFS_IN_TWO_COLUMNS = 22;

function detailSlide(r: Render, notes: ReturnType<typeof noteSections>): Slide {
  const lede = "Every issue and measure in this report carries a reference to the raw notes, the Word document that comes with it. Each one sets out the working behind the figure: what we read from your Salesforce, the sample, the calculation and the assumptions. The action lists (a spreadsheet and a ticket file) name each record to fix, with a link to it, and give each issue a ticket.";
  return {
    html: `<div class="eyebrow">Appendix</div><h2>Where to find the detail</h2><p class="lede">${r.t(lede)} <span data-ref>R4.3</span> is the third item in section <span data-ref>R4</span>.</p><div class="${notes.length > REFS_IN_TWO_COLUMNS ? "three-text" : "two-text"} refs">${notes.map((s) => `<p class="definition"><strong data-ref>${s.ref}</strong> ${r.t(s.title)}</p>`).join("")}</div>`,
  };
}

function buildSlides(report: BuiltReport, r: Render, density: Density | null): Slide[] {
  const compact = density === null;
  const dense = density?.columns === 2;
  const { run, intake, calibration, scores = [], preparedBy } = report.input;
  const built = report.outputs.full_health_check;
  const team = report.teamFindings;
  const metrics = report.metrics;
  const metric = (id: string): Metric | undefined => metrics.find((m) => m.id === id);
  const verdict = verdictOf(team);
  const top = topIssues(team, TOP_ISSUE_COUNT, report.added);
  const { phases: plan, later, alsoFor, quickWins } = buildPlan(team, report.added);
  const setupReview = isSetupReview(run);
  // A set-up review shows only its own areas: the record areas get one line, below.
  const domains = areasOf(run, team);
  const assessed = domains.filter((d) => team.some((f) => f.domain === d));
  const unassessed = domains.filter((d) => !assessed.includes(d));
  const customer = intake?.customerName ?? run.customerRef ?? "Customer";
  const notes = noteSections(report);
  const causes = new Map(report.causes.flatMap((c) => c.findingIds.map((id): [string, string] => [id, c.sentence])));
  const causeOf: CauseOf = (f) => causes.get(f.id) ?? null;
  const slides: Slide[] = [];
  const add = (html: string, classes?: string): void => void slides.push({ html, ...(classes ? { classes } : {}) });
  // Where each contents entry starts, as a page number, noted as its first slide goes in (DECISIONS.md 407).
  const starts = new Map<string, number>();
  const mark = (label: string, ahead = 0): void => void (starts.has(label) || starts.set(label, slides.length + 1 + ahead));

  add(`<div><div class="eyebrow">Black Cloud Barometer</div><h1>${documentTitle(run)}</h1><div class="sub">Full report</div></div>${coverScore(report.overall)}<div>${r.draft ? '<p><span class="mark">Draft, not approved for release</span></p>' : ""}<p class="for">Prepared for</p><p>${r.t(customer)}</p>${preparedBy ? `<p>by ${r.t(preparedBy)}, Black Cloud</p>` : ""}<p>${longDate(run.asOf)}</p></div>`, "dark cover");

  // Areas with issues get their own pages. The rest are summed up together, so
  // the deck spends its pages on what needs doing.
  const withIssues = assessed.filter((d) => issuesOf(team.filter((f) => f.domain === d)).length > 0);
  const quiet = assessed.filter((d) => !withIssues.includes(d));
  const working = quiet.filter((d) => team.some((f) => f.domain === d && f.status === "passed"));
  const unclear = [...quiet.filter((d) => !working.includes(d)), ...unassessed];
  // A thin org is told once, on the Overview, so the area pages don't each say it again (DECISIONS.md 450).
  // "Where we can't give a view yet" needs nothing more: it only ever holds areas with nothing measured.
  const thin = isThinData(team);
  const sized = (ids: string[]): boolean => ids.some((id) => metric(id)?.status === "measured");

  // Each deep-dive page shows only if one of its figures could be measured. The
  // others are listed together, with why, rather than as pages that say nothing.
  const pipelinePages: Slide[] = [];
  const gaps: Array<{ title: string; detail: string }> = [];
  const page = (title: string, ids: string[], make: () => Slide | Slide[]): void => {
    // A set-up review reads no deals, so the pipeline isn't a gap in it.
    if (setupReview) return;
    if (sized(ids)) {
      pipelinePages.push(...[make()].flat());
      return;
    }
    const reason = ids.map(metric).find((m): m is Extract<Metric, { status: "not_measurable" }> => m?.status === "not_measurable");
    gaps.push({ title, detail: reason?.reason ?? "This wasn't part of the health check." });
  };
  const readings = ["historic.win_rate", "historic.win_rate_by_value", "historic.win_rate_including_no_decision", "historic.no_decision_rate"];
  const cycle = ["historic.sales_cycle_won", "historic.sales_cycle_lost"];
  const slippage = ["historic.slippage_rate", "historic.slippage_quarter_cross_rate", "historic.slippage_days"];
  const rowsOf = (ids: string[]): Metric[] => ids.flatMap((id) => metric(id) ?? []);
  const quarterly = ["historic.quarterly_created", "historic.quarterly_won", "historic.quarterly_won_value", "historic.quarterly_win_rate", "historic.quarterly_median_won_size", "historic.quarterly_median_cycle"];
  const wins = ["historic.repeat_win_share", "historic.top_account_share", "historic.won_value_by_customer"];
  const sources = ["historic.won_value_by_type", "historic.won_value_by_source"];
  const lost = ["historic.lost_by_stage", "historic.lost_value_by_stage"];
  const speed = ["historic.close_date_reliability", "historic.close_days_late", "historic.stage_velocity_won", "historic.stage_velocity_lost"];
  page("Open pipeline by stage", ["current.open_deals"], () => pipelineTodaySlide(r, metric, calibration));
  page("Delivery outlook", ["current.delivery_outlook_value"], () => outlookSlide(r, metric));
  // The last resort pairs pipeline charts as the dense decks do: it's there to keep the deck short.
  const densePipeline = dense || compact;
  if (densePipeline) {
    page("How deals convert", ["historic.win_rate", "historic.stage_conversion"], () => ({ html: `<div class="eyebrow">Pipeline</div><h2>How deals convert</h2><div class="chart-pair"><div><h3 class="cap">Win rate by team and kind of deal</h3>${chartSlot(winRateBySegment(metric("historic.win_rate"), undefined, HALF_WIDTH))}</div><div><h3 class="cap">Stage funnel: deals that reached each stage or a later one</h3>${chartSlot(stageFunnel(metric("historic.stage_conversion"), HALF_WIDTH))}</div></div>` }));
  } else {
    page("Win rate by segment", ["historic.win_rate"], () => ({ html: `<div class="eyebrow">Pipeline</div><h2>Win rate by team and kind of deal</h2><div class="chart-wrap">${chartSlot(winRateBySegment(metric("historic.win_rate")))}</div>` }));
    page("Three ways of reading the win rate", readings, () => ({ classes: "tight", html: `<div class="eyebrow">Pipeline</div><h2>Three ways of reading the win rate</h2>${metricRows(r, rowsOf(readings), r.currency)}` }));
    page("Stage funnel", ["historic.stage_conversion"], () => ({ html: `<div class="eyebrow">Pipeline</div><h2>Stage funnel</h2><p class="lede">${r.t("Each stage counts the deals that reached it or a later one, so a stage few deals sit in can still show many.")}</p><div class="chart-wrap">${chartSlot(stageFunnel(metric("historic.stage_conversion")))}</div>` }));
  }
  page("Quarter by quarter", quarterly, () => quarterlySlide(r, metric));
  if (densePipeline) {
    page("Where wins come from", [...wins, ...sources], () => winsSlideDense(r, metric));
    page("Where deals are lost, and how fast they move", [...lost, ...speed], () => lostAndSpeedSlideDense(r, metric, calibration));
    page("Win rate, sales cycle and slippage", [...readings, ...cycle, ...slippage], () => ({ classes: "tight compact-rows", html: `<div class="eyebrow">Pipeline</div><h2>Win rate, sales cycle and slippage</h2>${metricRows(r, rowsOf([...readings, ...cycle, ...slippage]), r.currency)}` }));
  } else {
    const [repeatSlide, sourceSlide] = winsSlides(r, metric);
    page("New and repeat customers", wins, () => repeatSlide!);
    page("Wins by type and lead source", sources, () => sourceSlide!);
    page("Where deals drop out", lost, () => lostSlide(r, metric));
    page("Close dates and stage speed", speed, () => closeDatesSlide(r, metric, calibration));
    page("Sales cycle and slippage", [...cycle, ...slippage], () => ({ classes: "tight", html: `<div class="eyebrow">Pipeline</div><h2>Sales cycle and slippage</h2>${metricRows(r, rowsOf([...cycle, ...slippage]), r.currency)}` }));
  }
  page("How your sales process really runs", ["historic.path_to_won", "historic.path_to_lost", "historic.stage_transitions"], () => processSlide(r, metric, calibration));
  page("What separates your wins from your losses", [WIN_DRIVERS], () => driversSlide(r, metric));
  page("Pipeline over time", ["historic.pipeline_point_in_time_value"], () => ({ html: `<div class="eyebrow">Pipeline</div><h2>Pipeline over time</h2><p class="lede">${r.t("The value of your open pipeline at the end of each month.")}</p><div class="chart-wrap" style="max-width:1000px">${chartSlot(pipelineOverTime(metric("historic.pipeline_point_in_time_value"), r.currency))}</div>` }));

  // Only figures with something to stand against: the raw notes say why the rest have nothing yet.
  const compared = report.comparisons.filter((c) => c.value !== null && (c.published.length > 0 || c.own.status === "measured"));
  const comparable = !setupReview && compared.length > 0;
  // The team survey follows adoption, the chapter it speaks to most, or the last area chapter.
  const teamSays = surveySlides(r, report, sectionBody(r, built, "team_survey"));
  const surveyAfter = withIssues.includes("adoption") ? "adoption" : withIssues.at(-1);
  // What people told us sits with what the team says, after it (DECISIONS.md 186).
  const peopleSaid = interviewSlides(r, report.interviews, sectionBody(r, built, "interviews"));
  const contents = [
    "What you told us",
    "What we found against it",
    ...(report.progress ? ["Since your last health check"] : []),
    "Overview and scorecard",
    ...(report.value.total.expected > 0 ? ["What fixing this is worth"] : []),
    ...(comparable ? ["How you compare"] : []),
    ...withIssues.map(domainLabel),
    ...(teamSays.length ? ["What your team says"] : []),
    ...(peopleSaid.length ? ["What your people told us"] : []),
    ...(working.length ? ["What's working well"] : []),
    ...(pipelinePages.length || gaps.length ? ["Pipeline"] : []),
    "AI readiness",
    ...(plan.some((p) => p.findings.length > 0) ? ["Roadmap"] : []),
    "What happens next",
    ...(hasDocument(readDocument(metrics)) ? ["Your org, documented"] : []),
    "Appendix",
  ];
  // With Service Cloud and CPQ both on there can be 19 area chapters, more than two columns hold.
  // Its page numbers are filled in once every slide is in (DECISIONS.md 407).
  const contentsAt = slides.length;
  // In page order: what the team and your people said sits beside the area it speaks to. An entry with no page of its own keeps its place.
  const inPageOrder = (): string[] => {
    let last = 0;
    const keyed = contents.map((c, i) => ({ c, i, at: (last = starts.get(c) ?? last) }));
    return keyed.sort((a, b) => a.at - b.at || a.i - b.i).map((x) => x.c);
  };
  const contentsSlide = (): string => `<div class="eyebrow">Contents</div><h2>What's in this report</h2><ol class="contents${contents.length > CONTENTS_IN_TWO_COLUMNS ? " long" : ""}">${inPageOrder().map((c) => `<li>${r.t(c)}${starts.has(c) ? `<span class="pg" data-ref data-goto="${starts.get(c)}">${starts.get(c)}</span>` : ""}</li>`).join("")}</ol>`;
  add(contentsSlide());

  add(`<div class="eyebrow">How to read this report</div><h2>What the ratings mean</h2><table class="grid"><tbody>
    <tr><td style="width:24%"><strong>Barometer score</strong></td><td>One score out of 100 for your whole Salesforce, from the area scores, with a grade from A to E.</td></tr>
    <tr><td><strong>Severity</strong></td><td>How much damage this does if it's left alone.</td></tr>
    <tr><td><strong>Confidence</strong></td><td>How sure we are, given what we could read from your org.</td></tr>
    <tr><td><strong>Effort</strong></td><td>Our sizing of the fix, from very small to very large.</td></tr>
    <tr><td><strong>Impact</strong></td><td>Revenue, licence cost or hours, with its assumptions. Where there's no honest figure to put on it, the risk it carries.</td></tr>
    <tr><td><strong>Not measurable</strong></td><td>The data can't support the figure, so we report that rather than estimate it.</td></tr>
    <tr><td><strong>Reference</strong></td><td>Where the working is in the raw notes that come with this report. <span data-ref>R4.3</span> is the third item in section <span data-ref>R4</span>.</td></tr></tbody></table>`);

  // What the customer said comes first, and our answer to it straight after: it's what they'll read first.
  mark("What you told us");
  add(`<div class="eyebrow">What you told us</div><h2>In your words</h2>${needsInput(built, "stated_problems") ? inputChip : ""}${sectionBody(r, built, "stated_problems")}`);
  mark("What we found against it");
  add(`<div class="eyebrow">What we found against it</div><h2>What you told us, against what we found</h2>${needsInput(built, "stated_vs_actual") ? inputChip : ""}${sectionBody(r, built, "stated_vs_actual")}`);

  mark("Overview and scorecard");
  add(`<div class="eyebrow">Overview</div><h2>${r.t(verdict.headline)}</h2><div>${sectionBody(r, built, "overview").replace(`<p>${r.t(verdict.headline)}.</p>`, "")}</div><div class="stat-row" style="margin-top:18px">${report.overall.status === "measured" ? `<div class="stat"><b>${report.overall.score}</b><span>Barometer score</span></div>` : ""}<div class="stat"><b>${verdict.issues}</b><span>Issues</span></div><div class="stat"><b>${verdict.critical}</b><span>Critical</span></div><div class="stat"><b>${verdict.high}</b><span>High</span></div><div class="stat"><b>${team.filter((f) => f.status === "not_measurable").length}</b><span>Not measurable</span></div><div class="stat"><b>${team.filter((f) => f.status === "passed").length}</b><span>Passed</span></div></div>${thin ? `<p class="small-note">${r.t(thinDataLine(team, "The appendix"))}</p>` : ""}`);

  const progressed = progressPage(r, report);
  if (progressed) mark("Since your last health check");
  if (progressed) add(progressed, "tight");
  // Every line named, carrying on to a following slide where one can't hold them (DECISIONS.md 439).
  const worth = moneySlides(r, report);
  if (worth.length) mark("What fixing this is worth");
  for (const slide of worth) add(slide, "tight");

  add(`<div class="eyebrow">Scorecard</div><h2>How each area stands</h2>${scoreLine(report) ? `<p class="lede">${r.t(scoreLine(report, true))}</p>` : ""}<div class="chart-wrap" style="max-width:${scoreLine(report) ? 760 : 880}px">${chartSlot(scorecardChart(scorecard(team, scores, domains), scorecardOptions(report)))}</div>`);

  if (comparable) mark("How you compare");
  if (comparable) slides.push(compareSlide(r, compared));

  if (top.length) add(`<div class="eyebrow">Top issues</div><h2>${top.length === TOP_ISSUE_COUNT ? `The ${TOP_ISSUE_COUNT} that matter most` : "The issues that matter most"}</h2><table class="grid"><thead><tr><th style="width:6%"></th><th>Issue</th><th style="width:12%">Severity</th><th style="width:26%">Impact</th><th style="width:20%">Owner</th></tr></thead><tbody>${top.map((f, i) => `<tr><td>${i + 1}</td><td>${r.t(f.title)}${r.ref(f.id)}</td><td>${severityChip(f)}</td><td>${valueText(r, f).amount}</td><td>${r.t(f.owner)}</td></tr>`).join("")}</tbody></table>`);

  if (density?.domainSummary) {
    // The pipeline's opener goes too, so its text joins the areas here.
    const pipelineGlance = pipelinePages.length ? [{ title: "Pipeline", detail: currentBody(built, "pipeline_deep_dive").replace(/\n+/g, " ") }] : [];
    slides.push(...threeColumnList(r, "Areas", "Each area at a glance", [
      ...assessed.map((domain) => {
        const own = team.filter((f) => f.domain === domain);
        return { title: `${domainLabel(domain)}: ${issuesOf(own).length} issues, ${own.filter((f) => f.status === "passed").length} passed`, detail: currentBody(built, `domain_${domain}`) };
      }),
      ...pipelineGlance,
    ]));
  }
  // The inventory sits with configuration, or with automation when configuration
  // has no issues. An org with nothing of its own to count doesn't need the table.
  const inventory = asMeasured(metric(OBJECT_INVENTORY));
  const inventoryHome = withIssues.includes("configuration") ? "configuration" : "automation";
  const inventoryWorthShowing = metric(OBJECT_INVENTORY) !== undefined && (!inventory || inventory.stats.customObjects > 0 || inventory.stats.automation > 0);
  for (const domain of withIssues) {
    const label = domainLabel(domain);
    const own = team.filter((f) => f.domain === domain);
    const issues = issuesOf(own, report.added);
    mark(label);
    if (!density?.domainSummary) add(`<div class="eyebrow">Area</div><h1>${r.t(label)}</h1><div class="body">${sectionBody(r, built, `domain_${domain}`)}</div>`, "dark divider");
    // Each area opens the same way. The tightest densities drop it to keep the deck in range.
    const glance = density && !density.domainSummary ? chapterSlide(r, report, domain) : "";
    if (glance) add(glance, "tight");
    const usage = glance && domain === "adoption" ? adoptionSlide(r, metric) : "";
    if (usage) add(usage, "tight");
    slides.push(...(density ? domainIssueSlides(r, label, issues, density, causeOf) : issueTable(r, label, issues)));
    if (domain === inventoryHome && inventoryWorthShowing) slides.push(inventorySlide(r, label, metric(OBJECT_INVENTORY)));
    if (domain === surveyAfter) {
      if (teamSays.length) mark("What your team says");
      if (peopleSaid.length) mark("What your people told us", teamSays.length);
      slides.push(...teamSays, ...peopleSaid);
    }
  }
  if (surveyAfter === undefined) {
    if (teamSays.length) mark("What your team says");
    if (peopleSaid.length) mark("What your people told us", teamSays.length);
    slides.push(...teamSays, ...peopleSaid);
  }

  if (working.length && !density?.domainSummary) {
    mark("What's working well");
    slides.push(...listSlides(r, "Areas", "What's working well", working.map((d) => {
      const own = team.filter((f) => f.domain === d);
      const passed = own.filter((f) => f.status === "passed").length;
      const open = own.filter((f) => f.status === "not_measurable").length;
      return { title: domainLabel(d), detail: `No issues ${passed === 1 ? "in the one check" : `across the ${passed} checks`} we could make here.${open && !thin ? ` ${open} more couldn't be measured, and the appendix lists them.` : ""}` };
    }), compact));
  }
  const recordLine = recordAreasLine(run);
  if (unclear.length || recordLine) {
    const why = (d: string): string => {
      const score = scores.find((s) => s.domain === d);
      return score?.status === "not_measurable" ? score.reason : unassessed.includes(d) ? "Not part of this health check." : "We couldn't measure this area.";
    };
    const items = unclear.map((d) => ({ title: domainLabel(d), detail: why(d) }));
    slides.push(...listSlides(r, "Areas", "Where we can't give a view yet", recordLine ? [...items, { title: "Areas measured from records", detail: recordLine }] : items, compact));
  }

  if (pipelinePages.length || gaps.length) mark("Pipeline");
  if (pipelinePages.length) {
    const pipelineNotes = notes.find((s) => s.key === "pipeline_metrics");
    if (!density?.domainSummary) add(`<div class="eyebrow">Deep dive</div><h1>Pipeline</h1><div class="body">${sectionBody(r, built, "pipeline_deep_dive")}${pipelineNotes ? `<p class="small-note">The working for every figure in this part is in section <span data-ref>${pipelineNotes.ref}</span> of the raw notes.</p>` : ""}</div>`, "dark divider");
    slides.push(...pipelinePages);
  }
  if (gaps.length) slides.push(...listSlides(r, "Pipeline", "What your data can't tell us yet", gaps, compact));

  // Before the roadmap, so what to fix for an agent reads straight into when to fix it.
  mark("AI readiness");
  add(readinessSlide(r, report, built), "tight");

  const planned = plan.some((p) => p.findings.length > 0);
  if (planned) mark("Roadmap");
  // The engine's roadmap text only counts actions, which the boxes already show. Written text goes above them.
  const roadmap = built.versions.find((v) => v.id === built.sections.find((x) => x.key === "roadmap")?.currentVersionId);
  const roadmapText = roadmap && roadmap.source !== "engine" ? `<div style="margin-bottom:18px">${sectionBody(r, built, "roadmap")}</div>` : "";
  // Quick wins are their own box beside "Stabilise" (DECISIONS.md 366). In a roomy deck it sits on this
  // overview, so it costs no slide; a dense deck has no overview, so they get a slide after "Stabilise".
  // A line each: what to do for each is on its card in its area, and the box says so.
  const quickItem = (f: Finding): string => `<li><strong>${r.t(f.title)}.</strong>${r.ref(f.id)}${(alsoFor[f.id] ?? []).map((x) => r.ref(x.id)).join("")}${isUnpricedSaving(f) ? ` ${r.t(UNPRICED)}` : ""} <span class="owner">${r.t(f.owner)}</span></li>`;
  const quickOnOverview = !dense && planned && quickWins.length > 0;
  const quickPanel = `<div class="quick-panel"><h3>${r.t(QUICK_WINS_HEADING)}</h3><ul class="action-list">${quickWins.map(quickItem).join("")}</ul><p class="small-note">${r.t("What to do for each is on its card in its area.")}</p></div>`;
  const quickBox = quickOnOverview ? quickPanel : "";
  if (!dense && planned) add(`<div class="eyebrow">Roadmap</div><h2>The first 90 days</h2>${roadmapText}<div class="stat-row">${plan.map((p) => `<div class="stat"><b>${r.t(p.title)}</b><span>Days ${p.from} to ${p.to}</span><p style="margin-top:8px">${p.findings.length} ${p.findings.length === 1 ? "action" : "actions"}</p></div>`).join("")}${quickWins.length ? `<div class="stat"><b>${r.t("Quick wins")}</b><span>Also days 1 to 30</span><p style="margin-top:8px">${quickWins.length} ${quickWins.length === 1 ? "action" : "actions"}</p></div>` : ""}</div>${quickBox}`);
  // Quick wins are their own box beside "Stabilise", not part of it (DECISIONS.md 366).
  // Without the overview, the box follows "Stabilise" on its pages, so it takes a slide only when those are full.
  const quickInline = quickWins.length > 0 && !quickOnOverview;
  const itemHeight = (f: Finding): number => (dense ? 10 + 23 * estimateLines(`${f.title}. ${f.recommendation} ${f.owner}`, 58) : 12 + 28 * estimateLines(`${f.title}. ${f.recommendation} ${f.owner}`, 95));
  const quickHeight = 84 + quickWins.reduce((sum, f) => sum + 6 + 22 * estimateLines(`${f.title}. ${f.owner}`, dense ? 70 : 110), 0);
  type Row = { f: Finding } | { quickBox: true };
  plan.forEach((phase, k) => {
    const rows: Row[] = [...phase.findings.map((f) => ({ f })), ...(k === 0 && quickInline ? [{ quickBox: true as const }] : [])];
    if (rows.length === 0) return;
    const all = paginate(rows, (x) => ("f" in x ? itemHeight(x.f) : quickHeight), dense ? 2 * LIST_BUDGET : LIST_BUDGET);
    const cut = compact && all.length > 1;
    const pages = cut ? [all[0]!.slice(0, -1)] : all;
    pages.forEach((group, i) => {
      const actions = group.flatMap((x) => ("f" in x ? [x.f] : []));
      const list = `${actions.length ? `<ul class="action-list${dense ? " two-text" : ""}">${actions.map((f) => `<li><strong>${r.t(f.title)}.</strong>${r.ref(f.id)}${(alsoFor[f.id] ?? []).map((x) => r.ref(x.id)).join("")} ${r.t(f.recommendation)}${isUnpricedSaving(f) ? ` ${r.t(UNPRICED)}` : ""} <span class="owner">${r.t(f.owner)}</span></li>`).join("")}</ul>` : ""}${group.some((x) => "quickBox" in x) ? quickPanel : ""}`;
      add(`<div class="eyebrow">Roadmap</div><h2>Days ${phase.from} to ${phase.to}: ${phase.title}${i ? ", continued" : ""}</h2>${cut ? `<p style="font-size:15px;margin-bottom:6px">${COMPACT_NOTE}</p>` : ""}${list}`);
    });
  });

  // Titles only: each of these already has its card in its domain's section.
  // Each with its reference, so a reader can find the issue it comes from (night 3 reading pass).
  // An action that covers other issues from the same rule carries their references too (night 3 review).
  const afterPlan = later.map((f) => ({ title: f.title, detail: `${severityWord(f.severity)}. ${effortText(f)}. ${f.owner}`, ref: [f.id, ...(alsoFor[f.id] ?? []).map((x) => x.id)] }));
  // Titles only, so at the densest they take three columns, as the metric definitions do.
  if (later.length) slides.push(...(density?.domainSummary || density?.threeColumnLists ? threeColumnList(r, "Roadmap", "After 90 days", afterPlan) : dense ? twoColumnList(r, "Roadmap", "After 90 days", afterPlan) : listSlides(r, "Roadmap", "After 90 days", afterPlan, compact)));

  mark("What happens next");
  slides.push(nextStepsSlide(r, plan));
  const documented = documentedSlides(r, metrics, dense);
  if (documented.length) mark("Your org, documented");
  slides.push(...documented);

  mark("Appendix");
  if (!dense) add(`<div class="eyebrow">Appendix</div><h1>How we worked</h1><div class="body">${sectionBody(r, built, "appendix")}</div>`, "dark divider");
  slides.push(...listSlides(r, "Appendix", "Assumptions", [
    ...(dense ? [{ title: "How we worked", detail: currentBody(built, "appendix").replace(/\n+/g, " ") }] : []),
    { title: "Analysis window", detail: `${longDate(run.analysisWindow.from)} to ${longDate(run.analysisWindow.to)}` },
    // Stages and deal samples only matter where deals are read.
    ...(setupReview ? [] : [{ title: "Stage meanings", detail: calibration.stages.map((s) => `${s.name} (${s.category.replace("_", " ")})`).join(", ") }]),
    { title: "Dormant users", detail: `No login for ${calibration.dormantDays} days` },
    ...(setupReview ? [] : [{ title: "Smallest sample", detail: `At least ${calibration.minSample} deals behind a figure` }]),
    { title: "Currency", detail: calibration.currency },
  ], compact));
  // Only what the report shows a figure for needs defining. The documented org
  // is described on its own pages, and its definitions are in the raw notes.
  const definitions = metrics.filter((m) => m.status === "measured" && !DOCUMENTED_METRIC_IDS.includes(m.id)).map((m) => ({ title: m.label, detail: m.definition }));
  // Two columns at every density but the last: definitions are reference, and
  // one to a line took a slide for every few metrics.
  slides.push(...(compact ? listSlides(r, "Appendix", "Metric definitions", definitions, compact) : dense ? threeColumnList(r, "Appendix", "Metric definitions", definitions) : twoColumnList(r, "Appendix", "Metric definitions", definitions)));
  // Reference, so two to a line like the definitions.
  const reference = (title: string, items: ListItem[]): Slide[] => (compact || items.length === 0 ? listSlides(r, "Appendix", title, items, compact) : twoColumnList(r, "Appendix", title, items));
  const unmeasured = team.filter((f) => f.status === "not_measurable").map((f) => ({ title: f.title, detail: f.notMeasurableReason ?? f.summary }));
  const passing = team.filter((f) => f.status === "passed").map((f) => ({ title: f.title, detail: f.summary }));
  // At the three-column step the two lists share three columns, each item saying which it is (DECISIONS.md 244).
  if (density?.threeColumnLists && unmeasured.length && passing.length) {
    slides.push(...threeColumnList(r, "Appendix", "What we couldn't measure, and what passed", [...unmeasured.map((x) => ({ ...x, title: `Not measurable: ${x.title}` })), ...passing.map((x) => ({ ...x, title: `Passed: ${x.title}` }))]));
  } else {
    slides.push(...reference("What we couldn't measure", unmeasured));
    slides.push(...reference("Checks that passed", passing));
  }
  // Manual steps are working notes for us, not something the deck refers to.
  slides.push(detailSlide(r, notes.filter((s) => s.key !== "manual_steps")));
  slides[contentsAt] = { html: contentsSlide() };
  return slides;
}

/** Slides at each density in turn, so a test can see how close a deck is to its ceiling. */
export function slideCounts(report: BuiltReport, pseudonyms: Pseudonymiser): number[] {
  const r = renderContext(pseudonyms, report.input.calibration.currency, true, referenceMap(noteSections(report)), yearlyByFinding(report.value));
  return [...DENSITIES, null].map((density) => buildSlides(report, r, density).length);
}

export function fullReportHtml(report: BuiltReport, pseudonyms: Pseudonymiser): string {
  return fullReportHtmlAt(report, pseudonyms, null);
}

/**
 * The deck at one density step, by its place in the ladder (the last is the
 * densest), so every step can be put through the layout check. With null, the
 * roomiest step that fits the deck's range, as the deck is made.
 */
export function fullReportHtmlAt(report: BuiltReport, pseudonyms: Pseudonymiser, step: number | null): string {
  const built = report.outputs.full_health_check;
  const r = renderContext(pseudonyms, report.input.calibration.currency, !canRelease(built.sections).ok, referenceMap(noteSections(report)), yearlyByFinding(report.value));
  const ladder = [...DENSITIES, null];
  let slides: Slide[] = [];
  if (step !== null) slides = buildSlides(report, r, ladder[step] ?? null);
  else
    for (const density of ladder) {
      slides = buildSlides(report, r, density);
      if (slides.length <= deckRangeFor(report.input.run).max) break;
    }
  const pages = slides.map((slide, i) => {
    const chrome = slide.classes?.includes("cover") ? "" : `<div class="footer" data-chrome><span>Black Cloud Barometer${r.draft ? " | Draft, not approved for release" : ""}</span><span>${i + 1}</span></div>`;
    return `<section class="page ${slide.classes ?? ""}" data-page="${i + 1}">${slide.html}${chrome}</section>`;
  });
  return document("Full health check", "slide", `${CSS}\n${COMPARE_CSS}\n${MONEY_CSS}\n${CHAPTER_CSS}\n${PROGRESS_CSS}\n.money .headline-figure { font-size: 2.6em; } .money .range { margin: 4px 0 10px; } .money .kind { margin-bottom: 5px; } .money table.grid { font-size: 16px; } .money .split .kinds { font-size: 16px; } .money .note { font-size: 14px; }`, pages);
}
