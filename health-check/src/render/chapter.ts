import type { BuiltReport } from "../narrative/sections.ts";
import { bySeverity, domainLabel, issuesOf } from "../narrative/select.ts";
import type { Finding } from "../schemas/index.ts";
import { compactMoney } from "./format.ts";
import { estimateLines, type Render } from "./html.ts";
import { worthOf } from "../value/model.ts";
import { PRACTICE_ON_SLIDE, practiceFor, practiceId, sourceLabel } from "../practice/index.ts";

// Every area opens the same way (roadmap 3.1): what we looked at, what's working,
// what needs work, what it costs, and what to do first. The issue pages follow.

export const CHAPTER_CSS = `
.chapter .cols { display: grid; grid-template-columns: 1.25fr 1fr; gap: 28px; align-items: start; }
.chapter h3 { font-size: 14px; font-weight: 600; color: var(--muted); margin: 0 0 6px; }
.chapter ul { margin: 0 0 14px; padding-left: 18px; font-size: 15px; line-height: 1.38; }
.chapter li { margin-bottom: 3px; }
.chapter .panel p { margin-bottom: 8px; }
.chapter .counts { display: flex; gap: 10px; margin-bottom: 14px; }
.chapter .counts .stat { padding: 10px 12px; }
.chapter .counts .stat b { font-size: 26px; }
.chapter .counts .stat span { font-size: 13px; }
.chapter .panel { padding: 14px 18px; }
.chapter .panel p { font-size: 15px; line-height: 1.35; }
.chapter .panel ul { font-size: 13px; line-height: 1.32; margin-bottom: 10px; }
.chapter .panel li { margin-bottom: 2px; }
.chapter .panel ul.good { font-size: 12.5px; line-height: 1.3; margin-bottom: 4px; }
.chapter .src { font-size: 10.5px; color: var(--muted); white-space: nowrap; }
`;

const LIST_ITEMS = 5;
// Rows the left column holds below its counts: items, and each list's "and N more".
const LEFT_ROWS = 12;

/** How many of each list to name: five, or fewer when all three together wouldn't fit. */
function listCap(lists: Finding[][]): number {
  const rows = (cap: number): number => lists.reduce((sum, l) => sum + Math.min(l.length, cap) + (l.length > cap ? 1 : 0), 0);
  let cap = LIST_ITEMS;
  while (cap > 2 && rows(cap) > LEFT_ROWS) cap--;
  return cap;
}
const FIRST_STEPS = 3;


const list = (r: Render, items: Finding[], more: number, line: (f: Finding) => string): string =>
  `<ul>${items.map((f) => `<li>${line(f)}${r.ref(f.id)}</li>`).join("")}${more > 0 ? `<li>${r.t(`And ${more} more in the raw notes.`)}</li>` : ""}</ul>`;

function groups(own: Finding[]) {
  const issues = issuesOf(own).slice().sort(bySeverity);
  return {
    issues,
    broken: issues.filter((f) => f.severity === "critical" || f.severity === "high"),
    belowPar: issues.filter((f) => f.severity !== "critical" && f.severity !== "high"),
    passed: own.filter((f) => f.status === "passed"),
    open: own.filter((f) => f.status === "not_measurable"),
  };
}

// The panel's room for what to do first and what good looks like, in pixels of
// text, and roughly how many characters each line holds. Cautious: the layout
// check measures the real thing.
const PANEL_BUDGET = 300;
const STEP_CHARS = 64;
const GOOD_CHARS = 70;
const firstSentence = (text: string): string => /^.*?[.!?](?=\s|$)/.exec(text)?.[0] ?? text;

/**
 * What to do first, as much of it as fits beside what good looks like: the
 * full recommendations, else each one's first sentence, else fewer of them.
 * The issue pages that follow have every recommendation in full.
 */
function firstSteps(issues: Finding[], good: string[]): string[] {
  const height = (texts: string[], chars: number, line: number): number => texts.reduce((sum, t) => sum + estimateLines(t, chars) * line + 3, 0);
  const room = PANEL_BUDGET - (good.length ? 22 + height(good, GOOD_CHARS, 16.3) : 0);
  const full = issues.slice(0, FIRST_STEPS).map((f) => f.recommendation);
  for (const steps of [full, full.map(firstSentence), full.slice(0, 2).map(firstSentence)]) if (height(steps, STEP_CHARS, 17.2) <= room) return steps;
  return full.slice(0, 1).map(firstSentence);
}

/** Every count an area's chapter slide shows, for the number check. */
export function chapterFigures(own: Finding[]): number[] {
  if (own.length === 0) return [];
  const { issues, broken, belowPar, passed, open } = groups(own);
  const cap = listCap([passed, broken, belowPar]);
  const overflow = [passed, broken, belowPar].filter((l) => l.length > cap).map((l) => l.length - cap);
  return [own.length, passed.length, issues.length, open.length, ...overflow];
}

/** The chapter's opening slide for one area. Empty for an area with nothing assessed. */
export function chapterSlide(r: Render, report: BuiltReport, domain: string): string {
  const own = report.teamFindings.filter((f) => f.domain === domain);
  if (own.length === 0) return "";
  const { issues, broken, belowPar, passed, open } = groups(own);
  const worth = worthOf(report.value, new Set(own.map((f) => f.id)));
  const score = report.input.scores?.find((s) => s.domain === domain);

  const counts = `<div class="counts stat-row">${[
    score?.status === "measured" ? [String(score.score), "Score out of 100"] : null,
    [String(own.length), own.length === 1 ? "Check" : "Checks"],
    [String(passed.length), "Passed"],
    [String(issues.length), issues.length === 1 ? "Issue" : "Issues"],
  ]
    .filter((x): x is string[] => x !== null)
    .map(([b, s]) => `<div class="stat"><b>${b}</b><span>${s}</span></div>`)
    .join("")}</div>`;

  const cap = listCap([passed, broken, belowPar]);
  const left = [
    passed.length ? `<h3>What's working</h3>${list(r, passed.slice(0, cap), passed.length - cap, (f) => r.t(f.title))}` : "",
    broken.length ? `<h3>What's broken</h3>${list(r, broken.slice(0, cap), broken.length - cap, (f) => r.t(f.title))}` : "",
    belowPar.length ? `<h3>What's below par</h3>${list(r, belowPar.slice(0, cap), belowPar.length - cap, (f) => r.t(f.title))}` : "",
  ].join("");

  const good = practiceFor(domain).slice(0, PRACTICE_ON_SLIDE);
  const steps = firstSteps(issues, good.map((p) => `${p.text} ${sourceLabel(p)}`));
  const right = [
    `<h3>What it costs</h3><p>${worth > 0 ? r.t(`About ${compactMoney(worth, report.value.currency)} over the next 12 months, as part of the total on the money page.`) : r.t("Nothing here has a figure in money. Its weight is the risk it carries.")}</p>`,
    steps.length ? `<h3>What to do first</h3><ul>${steps.map((text) => `<li>${r.t(text)}</li>`).join("")}</ul>` : "",
    good.length ? `<h3>What good looks like</h3><ul class="good">${good.map((p, i) => `<li>${r.t(p.text)} <span class="src">${r.t(sourceLabel(p))}</span>${r.ref(practiceId(domain, i))}</li>`).join("")}</ul>` : "",
    open.length ? `<p class="note" style="font-size:13.5px;color:var(--muted)">${r.t(`${open.length} more ${open.length === 1 ? "check" : "checks"} couldn't be measured here. The appendix says why.`)}</p>` : "",
  ].join("");

  return `<div class="chapter"><div class="eyebrow">${r.t(domainLabel(domain))}</div><h2>${r.t("At a glance")}</h2>${counts}<div class="cols"><div>${left}</div><div class="panel">${right}</div></div></div>`;
}
