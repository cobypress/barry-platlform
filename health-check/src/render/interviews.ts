import { INTERVIEW_ROLES } from "../interviews/questions.ts";
import { READING_LABELS, type InterviewTheme, type InterviewView } from "../interviews/readings.ts";
import type { Render } from "./html.ts";

// The interviews on the page. Every figure someone gave us sits in a sentence
// that says they said it, and the column it's in is headed "They told us", so
// nobody reads it as ours. Roles only: the view never holds a name, and the
// notes are never printed (DECISIONS.md 186).

/** Rows on one slide of the table. More go on the next. */
export const INTERVIEW_ROWS_PER_SLIDE = 5;
/** The readings in role order, then in the order they were held. */
export function byRole(view: InterviewView): InterviewTheme[] {
  const order = INTERVIEW_ROLES.map((r) => r.id);
  return view.themes.slice().sort((a, b) => order.indexOf(a.role) - order.indexOf(b.role) || Number(a.source.split(":")[1]) - Number(b.source.split(":")[1]));
}

export function interviewStats(r: Render, view: InterviewView): string {
  const count = (reading: InterviewTheme["reading"]) => view.themes.filter((t) => t.reading === reading).length;
  const stat = (n: number, label: string) => `<div class="stat"><b>${n}</b><span>${r.t(label)}</span></div>`;
  return `<div class="stat-row" style="margin:14px 0 6px">${[
    stat(view.count, view.count === 1 ? "Person interviewed" : "People interviewed"),
    ...(view.themes.length ? [stat(count("agrees"), "Agree with the data"), stat(count("disagrees"), "Disagree"), stat(count("cant_tell"), "We can't tell")] : []),
  ].join("")}</div>`;
}

function table(r: Render, rows: InterviewTheme[]): string {
  return `<table class="grid"><thead><tr><th style="width:15%">Role</th><th style="width:31%">They told us</th><th style="width:34%">What the data shows</th><th>Together</th></tr></thead><tbody>${rows
    .map((t) => {
      const top = t.issues.slice(0, 1).map((f) => `<br><span style="font-size:13.5px;color:var(--muted)">${r.t(f.title)}${r.ref(f.id)}</span>`).join("");
      const ref = t.metricId ? r.ref(t.metricId) : "";
      return `<tr><td>${r.t(t.roleLabel)}</td><td>${r.t(t.said)}</td><td>${r.t(t.found)}${ref}${top}</td><td><span class="chip">${READING_LABELS[t.reading]}</span><br><span style="font-size:13.5px">${r.t(t.why)}</span></td></tr>`;
    })
    .join("")}</tbody></table>`;
}

/**
 * The full report's interview slides: the chapter's text with the counts, then
 * every reading by role, a few to a slide. None without interviews.
 */
export function interviewSlides(r: Render, view: InterviewView | null, body: string): Array<{ html: string; classes?: string }> {
  if (!view) return [];
  const head = (title: string) => `<div class="eyebrow">Your people</div><h2>${r.t(title)}</h2>`;
  const slides = [{ classes: "tight", html: `${head("What your people told us")}${body}${interviewStats(r, view)}` }];
  const rows = byRole(view);
  for (let i = 0; i < rows.length; i += INTERVIEW_ROWS_PER_SLIDE) {
    slides.push({ classes: "tight", html: `${head(i === 0 ? "What they told us, against what we found" : "What they told us, continued")}${table(r, rows.slice(i, i + INTERVIEW_ROWS_PER_SLIDE))}` });
  }
  return slides;
}
