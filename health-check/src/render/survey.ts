import type { BuiltReport } from "../narrative/sections.ts";
import type { Metric } from "../schemas/index.ts";
import { SURVEY } from "../survey/summary.ts";
import type { SurveyView } from "../survey/view.ts";
import { percent } from "./format.ts";
import type { Render } from "./html.ts";

// The team survey on the page. Every figure is a survey metric or a count the
// view registers, and no comment is ever printed: the view never holds one.

/** How many blockers the deck lists. The raw notes have them all. */
export const BLOCKERS_ON_SLIDE = 4;

const measured = (view: SurveyView, id: string): Extract<Metric, { status: "measured" }> | null => {
  const m = view.metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m : null;
};

/** The headline answers as a row of figures. */
export function surveyStats(r: Render, view: SurveyView): string {
  const stat = (id: string, text: (m: Extract<Metric, { status: "measured" }>) => string, label: string): string => {
    const m = measured(view, id);
    return m ? `<div class="stat"><b>${text(m)}</b><span>${r.t(label)}${r.ref(id)}</span></div>` : "";
  };
  return `<div class="stat-row" style="margin:14px 0 6px">${[
    stat(SURVEY.responses, (m) => String(m.value), "People who answered"),
    stat(SURVEY.ease, (m) => String(m.value), "Ease of use, out of 5"),
    stat(SURVEY.trust, (m) => String(m.value), "Trust in the figures, out of 5"),
    stat(SURVEY.lost, (m) => String(m.value), "Hours lost a week, median"),
    stat(SURVEY.recommend, (m) => percent(m.value), "Would recommend it"),
  ].join("")}</div>`;
}

/** What gets in the way, most named first. Empty when nobody named anything. */
function blockerList(r: Render, view: SurveyView): string {
  const m = measured(view, SURVEY.blockers);
  const named = (m?.segments ?? []).filter((s) => s.n > 0 && s.value !== null).slice(0, BLOCKERS_ON_SLIDE);
  if (!named.length) return "";
  return `<h3 class="sub">What gets in the way most${r.ref(SURVEY.blockers)}</h3><ul>${named.map((s) => `<li>${r.t(s.key)}: ${percent(s.value!)} of people</li>`).join("")}</ul>`;
}

/** The pairings as a table: what the team says, what we found, and how they sit together. */
export function pairingTable(r: Render, view: SurveyView): string {
  return `<table class="grid"><thead><tr><th style="width:32%">Your team says</th><th style="width:40%">We found</th><th>Together</th></tr></thead><tbody>${view.pairings
    .map((p) => {
      const top = p.issues.slice(0, 2).map((f) => `<br><span style="font-size:13.5px;color:var(--muted)">${r.t(f.title)}${r.ref(f.id)}</span>`).join("");
      return `<tr><td>${r.t(p.said)}</td><td>${r.t(p.found)}${top}</td><td>${r.t(p.reading)}</td></tr>`;
    })
    .join("")}</tbody></table>`;
}

/**
 * The full report's two survey slides: what the team says, then that set
 * against what we found. None under five answers: the executive summary and
 * the raw notes carry the line saying why.
 */
export function surveySlides(r: Render, report: BuiltReport, body: string): Array<{ html: string; classes?: string }> {
  const view = report.survey;
  if (!view?.shown) return [];
  const slides = [{ classes: "tight", html: `<div class="eyebrow">Your team</div><h2>What your team says</h2>${body}${surveyStats(r, view)}${blockerList(r, view)}` }];
  if (view.pairings.length) slides.push({ classes: "tight", html: `<div class="eyebrow">Your team</div><h2>What your team says, against what we found</h2>${pairingTable(r, view)}` });
  return slides;
}
