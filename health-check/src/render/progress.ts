import type { BuiltReport } from "../narrative/sections.ts";
import { domainLabel } from "../narrative/select.ts";
import { compactMoney } from "./format.ts";
import type { Render } from "./html.ts";

// "Since your last health check": what a re-check found changed (roadmap Stage 5).
// Shared by both decks. Its figures are the progress model's own.

export const PROGRESS_CSS = `
.progress .pair { display: flex; gap: 12px; margin: 8px 0 16px; }
.progress .pair .stat { padding: 10px 12px; }
.progress .pair .stat b { font-size: 1.4em; }
.progress .lists { display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px; }
.progress h3 { font-size: 0.85em; font-weight: 600; color: var(--muted); margin: 0 0 6px; }
.progress ul { margin: 0; padding-left: 18px; font-size: 0.75em; line-height: 1.32; }
.progress li { margin-bottom: 3px; }
.progress .note { color: var(--muted); font-size: 0.75em; margin-top: 12px; }
`;

const LIST_ITEMS = 5;

/** Empty unless the run re-checks an earlier one. */
export function progressPage(r: Render, report: BuiltReport): string {
  const p = report.progress;
  if (!p) return "";
  const m = (n: number) => compactMoney(n, report.value.currency);
  const score =
    p.score.before !== null && p.score.after !== null
      ? `<div class="stat"><b>${p.score.before} to ${p.score.after}</b><span>Barometer score</span></div>`
      : "";
  const pair = `<div class="pair stat-row">${score}<div class="stat"><b>${m(p.worth.before)} to ${m(p.worth.after)}</b><span>${r.t("What fixing the issues is worth")}</span></div><div class="stat"><b>${p.fixed.length}</b><span>${r.t("Fixed")}</span></div><div class="stat"><b>${p.added.length}</b><span>${r.t("New")}</span></div></div>`;
  const list = (title: string, items: typeof p.fixed): string =>
    items.length
      ? `<div><h3>${r.t(`${title} (${items.length})`)}</h3><ul>${items
          .slice(0, LIST_ITEMS)
          .map((c) => `<li>${r.t(c.title)}${r.ref(c.findingIds[0]!)}</li>`)
          .join("")}${items.length > LIST_ITEMS ? `<li>${r.t(`And ${items.length - LIST_ITEMS} more in the raw notes.`)}</li>` : ""}</ul></div>`
      : "";
  const moved = p.areas.filter((a) => a.before !== null && a.after !== null && a.before !== a.after).sort((a, b) => Math.abs(b.after! - b.before!) - Math.abs(a.after! - a.before!));
  const areas = moved.length ? `<p class="note">${r.t(`Biggest moves: ${moved.slice(0, 4).map((a) => `${domainLabel(a.domain)} ${a.before} to ${a.after}`).join(", ")}.`)}</p>` : "";
  const unknown = p.unknown.length ? `<p class="note">${r.t(`We can't tell yet for ${p.unknown.length} more, because they couldn't be measured this time. The raw notes list them.`)}</p>` : "";
  return `<div class="progress"><div class="eyebrow">${r.t("Since your last health check")}</div><h2>${r.t("What has changed")}</h2>${pair}<div class="lists">${list("Fixed", p.fixed)}${list("Still open", p.remaining)}${list("New since then", p.added)}</div>${areas}${unknown}<p class="note">${r.t("Some of the change in money can come from the business itself, not only from fixes.")}</p></div>`;
}
