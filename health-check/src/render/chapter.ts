import type { BuiltReport } from "../narrative/sections.ts";
import { bySeverity, domainLabel, issuesOf } from "../narrative/select.ts";
import type { Finding } from "../schemas/index.ts";
import { compactMoney } from "./format.ts";
import type { Render } from "./html.ts";
import { worthOf } from "../value/model.ts";

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
`;

const LIST_ITEMS = 5;
const FIRST_STEPS = 3;


const list = (r: Render, items: Finding[], more: number, line: (f: Finding) => string): string =>
  `<ul>${items.map((f) => `<li>${line(f)}${r.ref(f.id)}</li>`).join("")}${more > 0 ? `<li>${r.t(`And ${more} more in the raw notes.`)}</li>` : ""}</ul>`;

/** The chapter's opening slide for one area. Empty for an area with nothing assessed. */
export function chapterSlide(r: Render, report: BuiltReport, domain: string): string {
  const own = report.teamFindings.filter((f) => f.domain === domain);
  if (own.length === 0) return "";
  const issues = issuesOf(own).slice().sort(bySeverity);
  const broken = issues.filter((f) => f.severity === "critical" || f.severity === "high");
  const belowPar = issues.filter((f) => f.severity !== "critical" && f.severity !== "high");
  const passed = own.filter((f) => f.status === "passed");
  const open = own.filter((f) => f.status === "not_measurable");
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

  const left = [
    passed.length ? `<h3>What's working</h3>${list(r, passed.slice(0, LIST_ITEMS), passed.length - LIST_ITEMS, (f) => r.t(f.title))}` : "",
    broken.length ? `<h3>What's broken</h3>${list(r, broken.slice(0, LIST_ITEMS), broken.length - LIST_ITEMS, (f) => r.t(f.title))}` : "",
    belowPar.length ? `<h3>What's below par</h3>${list(r, belowPar.slice(0, LIST_ITEMS), belowPar.length - LIST_ITEMS, (f) => r.t(f.title))}` : "",
  ].join("");

  const steps = issues.slice(0, FIRST_STEPS);
  const right = [
    `<h3>What it costs</h3><p>${worth > 0 ? r.t(`About ${compactMoney(worth, report.value.currency)} over the next 12 months, as part of the total on the money page.`) : r.t("Nothing here has a figure in money. Its weight is the risk it carries.")}</p>`,
    steps.length ? `<h3>What to do first</h3><ul>${steps.map((f) => `<li>${r.t(f.recommendation)}</li>`).join("")}</ul>` : "",
    open.length ? `<p class="note" style="font-size:13.5px;color:var(--muted)">${r.t(`${open.length} more ${open.length === 1 ? "check" : "checks"} couldn't be measured here. The appendix says why.`)}</p>` : "",
  ].join("");

  return `<div class="chapter"><div class="eyebrow">${r.t(domainLabel(domain))}</div><h2>${r.t("At a glance")}</h2>${counts}<div class="cols"><div>${left}</div><div class="panel">${right}</div></div></div>`;
}
