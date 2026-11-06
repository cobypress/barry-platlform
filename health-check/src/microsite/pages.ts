import { comparisonId, formatFigure } from "../benchmark/compare.ts";
import { checkNumbers } from "../numbercheck/index.ts";
import type { BuiltReport } from "../narrative/sections.ts";
import { buildPlan, bySeverity, domainLabel, domainsInOrder, issuesOf, scorecard, verdictOf } from "../narrative/select.ts";
import type { Pseudonymiser } from "../privacy/pseudonymiser.ts";
import { compactMoney, longDate, percent } from "../render/format.ts";
import { currentBody } from "../narrative/sections.ts";
import { SURVEY } from "../survey/summary.ts";
import { READING_LABELS } from "../interviews/readings.ts";
import { byRole } from "../render/interviews.ts";
import { effortText, renderContext, valueText, type Render } from "../render/html.ts";
import { noteSections, referenceMap } from "../render/references.ts";
import { HEADLINE_KINDS, KIND_LABELS } from "../value/model.ts";
import { DOCUMENTED, readDocument } from "../domains/configuration/documented.ts";
import { practiceFor, practiceId, sourceLabel } from "../practice/index.ts";
import { activeUsersChart, lightningChart, loginChannelsChart } from "../render/charts.ts";
import { accessBlock, automationLists, automationTable, dataModelDiagram, DIAGRAM_KEY, hasDocument, integrationsBlock, objectsTable, relationshipsTable, sharingTable } from "../render/documented.ts";

// The customer microsite's pages (roadmap Stage 4). Built for a browser, not
// cut from the print decks: the console puts each page inside Black Cloud's
// frame, which allows inline styles only and runs no scripts. Names come back
// here, as in the documents, which is why only the worker builds these.

export type MicrositePage = { slug: string; title: string; html: string };
export type MicrositePages = { pages: MicrositePage[] };

// Scoped, so nothing leaks into the console's frame. Colours suit its dark page.
const STYLE = `<style>
.bcr { color: #f2f2f2; }
.bcr h1 { font-size: 26px; margin: 0 0 4px; }
.bcr h2 { font-size: 20px; margin: 28px 0 10px; }
.bcr h3 { font-size: 16px; margin: 0 0 4px; }
.bcr .muted { color: rgba(255,255,255,.6); font-size: 14px; }
.bcr .figure { font-size: 44px; font-weight: 700; line-height: 1.1; color: #bfff00; margin: 6px 0; }
.bcr .stats { display: flex; flex-wrap: wrap; gap: 10px; margin: 12px 0; }
.bcr .stat { flex: 1 1 140px; padding: 12px 14px; border-radius: 12px; background: rgba(255,255,255,.05); }
.bcr .stat b { display: block; font-size: 24px; }
.bcr .stat span { color: rgba(255,255,255,.6); font-size: 13px; }
.bcr table { width: 100%; border-collapse: collapse; font-size: 14px; }
.bcr th, .bcr td { text-align: left; padding: 8px 6px; border-bottom: 1px solid rgba(255,255,255,.08); vertical-align: top; }
.bcr td.num, .bcr th.num { text-align: right; white-space: nowrap; }
.bcr .issue { padding: 14px 16px; margin: 0 0 10px; border-radius: 12px; background: rgba(255,255,255,.04); }
.bcr .meta { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; font-size: 13px; color: rgba(255,255,255,.6); margin-bottom: 6px; }
.bcr .chip { padding: 1px 8px; border-radius: 6px; border: 1px solid rgba(255,255,255,.3); }
.bcr .chip.alert { background: #e5484d; border-color: #e5484d; color: #fff; }
.bcr .ref { font-size: 12px; color: rgba(255,255,255,.45); margin-left: 4px; white-space: nowrap; }
.bcr .charts > div { flex: 1 1 100%; max-width: 640px; }
@media (max-width: 600px) {
  .bcr table.stack thead { display: none; }
  .bcr table.stack tr { display: block; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,.08); }
  .bcr table.stack td { display: block; border: none; padding: 2px 0; }
  .bcr table.stack td[data-label]::before { content: attr(data-label); display: block; font-size: 12px; color: rgba(255,255,255,.5); }
}
.bcr ul { padding-left: 20px; }
.bcr .doc-figure { margin: 8px 0 4px; max-width: 720px; }
.bcr .doc-figure svg { display: block; width: 100%; height: auto; }
.bcr .doc-cols { display: flex; flex-wrap: wrap; gap: 24px; }
.bcr .doc-cols > div { flex: 1 1 260px; }
.bcr h3.cap { font-size: 14px; color: rgba(255,255,255,.6); margin: 12px 0 4px; }
.bcr .small-note { font-size: 13px; color: rgba(255,255,255,.6); }
.bcr .wide { overflow-x: auto; }
.bcr .good { margin: 0 0 14px; padding: 10px 14px; border-left: 3px solid #bfff00; background: rgba(255,255,255,.03); }
.bcr .good ul { margin: 4px 0 0; }
.bcr .good li { font-size: 14px; }
.bcr li { margin-bottom: 6px; }
</style>`;

const chip = (severity: string): string => `<span class="chip${severity === "critical" || severity === "high" ? " alert" : ""}">${severity.charAt(0).toUpperCase()}${severity.slice(1)}</span>`;
const page = (body: string): string => `${STYLE}<div class="bcr">${body}</div>`;

function overview(r: Render, report: BuiltReport): string {
  const { run, intake } = report.input;
  const team = report.teamFindings;
  const verdict = verdictOf(team);
  const o = report.overall;
  const v = report.value;
  const m = (n: number) => compactMoney(n, v.currency);
  const customer = intake?.customerName ?? run.customerRef ?? "Your organisation";
  const score = o.status === "measured" ? `<div class="stat"><b>${o.score}</b><span>Barometer score, grade ${o.grade}</span></div>` : "";
  const worth =
    v.total.expected > 0
      ? `<h2>What fixing this is worth</h2><p class="figure">${m(v.total.expected)}</p><p class="muted">${r.t(`Over the next 12 months, expected. Between ${m(v.total.low)} and ${m(v.total.high)}.`)}</p><table><tbody>${HEADLINE_KINDS.filter((k) => v.byKind[k].expected > 0)
          .map((k) => `<tr><td>${r.t(KIND_LABELS[k])}</td><td class="num">${m(v.byKind[k].expected)}</td></tr>`)
          .join("")}</tbody></table>${whereFrom(r, report)}`
      : "";
  const cost = report.investment.annualCost;
  const investment = cost.status === "measured" ? `<h2>Your investment</h2><p>${r.t(`${compactMoney(cost.value, v.currency)} a year. ${cost.label}.`)}</p>` : "";
  const rows = scorecard(team, report.input.scores ?? []).filter((row) => row.assessed);
  const areas = `<h2>How each area stands</h2><table><thead><tr><th>Area</th><th class="num">Score</th><th class="num">Issues</th></tr></thead><tbody>${rows
    .map((row) => `<tr><td>${r.t(row.label)}</td><td class="num">${row.score ?? r.t("Not scored")}</td><td class="num">${row.issues}</td></tr>`)
    .join("")}</tbody></table>`;
  return page(
    `<h1>${r.t(`${customer}: your Salesforce health check`)}</h1><p class="muted">${longDate(run.asOf)}</p><h2>${r.t(verdict.headline)}</h2><div class="stats">${score}<div class="stat"><b>${verdict.issues}</b><span>Issues</span></div><div class="stat"><b>${verdict.critical}</b><span>Critical</span></div><div class="stat"><b>${verdict.high}</b><span>High</span></div></div>${worth}${investment}${areas}${teamSays(r, report)}${peopleSaid(r, report)}`,
  );
}

/** Each line behind the total, with its range, adding up to it, and what's kept apart: as the executive summary's money page has them. */
function whereFrom(r: Render, report: BuiltReport): string {
  const v = report.value;
  const m = (n: number) => compactMoney(n, v.currency);
  const groups = v.groups.filter((g) => g.kind !== "exposure").sort((a, b) => b.counted.expected - a.counted.expected);
  const rows = groups.map((g) => `<tr><td>${r.t(g.label)}</td><td class="num">${m(g.counted.expected)}</td><td class="num">${m(g.counted.low)} to ${m(g.counted.high)}</td></tr>`).join("");
  const exposure = v.groups.filter((g) => g.kind === "exposure").sort((a, b) => b.counted.annual - a.counted.annual)[0];
  const apart = exposure ? `<p class="muted">${r.t(`Kept apart from this total: ${exposure.counted.title.replace(/\.$/, "")} (${m(exposure.counted.annual)}). That's at risk, not money found, so it's never added.`)}</p>` : "";
  return `<h3>Where it comes from</h3><table><thead><tr><th>Line</th><th class="num">Expected</th><th class="num">Range</th></tr></thead><tbody>${rows}<tr><td><b>${r.t("Total, each counted once")}</b></td><td class="num"><b>${m(v.total.expected)}</b></td><td class="num"><b>${m(v.total.low)} to ${m(v.total.high)}</b></td></tr></tbody></table>${apart}<p class="muted">${r.t("Not every case will come back, so we count a share of each figure, with a cautious share and all of it either side. Where several issues describe the same money, only the largest counts.")}</p>`;
}

/** What people told us in interviews, as the full report has it: the chapter's text, then each reading by role. Roles only, and never the notes. */
function peopleSaid(r: Render, report: BuiltReport): string {
  const view = report.interviews;
  if (!view) return "";
  const text = currentBody(report.outputs.full_health_check, "interviews")
    .split(/\n{2,}/)
    .map((p) => `<p>${r.t(p)}</p>`)
    .join("");
  // On a phone each row stacks, each cell under its column's name.
  const rows = byRole(view).map((t) => `<tr><td data-label="Role">${r.t(t.roleLabel)}</td><td data-label="They told us">${r.t(t.said)}</td><td data-label="What the data shows">${r.t(t.found)}</td><td data-label="Together">${r.t(`${READING_LABELS[t.reading]}. ${t.why}`)}</td></tr>`);
  return `<h2>What your people told us</h2>${text}${rows.length ? `<table class="stack"><thead><tr><th>Role</th><th>They told us</th><th>What the data shows</th><th>Together</th></tr></thead><tbody>${rows.join("")}</tbody></table>` : ""}`;
}

/** What the team survey says, as the executive summary has it, and the pairings. Only the line saying why under five answers. */
function teamSays(r: Render, report: BuiltReport): string {
  const view = report.survey;
  if (!view) return "";
  const text = `<h2>What your team says</h2><p>${r.t(currentBody(report.outputs.executive_summary, "team_survey"))}</p>`;
  if (!view.shown) return text;
  const stat = (id: string, label: string, show: (value: number) => string = String): string => {
    const m = view.metrics.find((x) => x.id === id);
    return m?.status === "measured" ? `<div class="stat"><b>${show(m.value)}</b><span>${r.t(label)}</span></div>` : "";
  };
  const stats = `<div class="stats">${stat(SURVEY.ease, "Ease of use, out of 5")}${stat(SURVEY.trust, "Trust in the figures, out of 5")}${stat(SURVEY.lost, "Hours lost a week, median")}${stat(SURVEY.recommend, "Would recommend it", percent)}</div>`;
  const pairs = view.pairings.length
    ? `<table><thead><tr><th>Your team says</th><th>We found</th><th>Together</th></tr></thead><tbody>${view.pairings.map((p) => `<tr><td>${r.t(p.said)}</td><td>${r.t(p.found)}</td><td>${r.t(p.reading)}</td></tr>`).join("")}</tbody></table>`
    : "";
  return `${text}${stats}${pairs}`;
}

/** Adoption's charts, inline: they load nothing. Only the ones that could be drawn. */
function adoptionCharts(report: BuiltReport): string {
  const metric = (id: string) => report.metrics.find((m) => m.id === id);
  // One to a row, each under the heading of what it shows: three print-sized charts side by side were too small to read.
  const charts = (
    [
      ["Monthly active users", activeUsersChart(metric("adoption.monthly_active_users"))],
      ["How people sign in", loginChannelsChart(metric("adoption.mobile_login_share"))],
      ["Lightning and Classic", lightningChart(metric("adoption.lightning_share"))],
    ] as const
  ).filter(([, c]) => c.kind === "chart");
  return charts.length ? `<div class="doc-cols charts">${charts.map(([title, c]) => `<div><h3 class="cap">${title}</h3><div class="doc-figure">${c.kind === "chart" ? c.svg : ""}</div></div>`).join("")}</div>` : "";
}

function issues(r: Render, report: BuiltReport): string {
  const team = report.teamFindings;
  const sections = domainsInOrder(team)
    .map((domain) => {
      const own = issuesOf(team.filter((f) => f.domain === domain), report.added);
      if (own.length === 0) return "";
      const cards = own
        .map((f) => {
          const value = valueText(r, f);
          return `<div class="issue"><div class="meta">${chip(f.severity)}<span>${r.t(effortText(f))}</span><span>${r.t(`Owner: ${f.owner}`)}</span>${r.ref(f.id)}</div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p>${f.recommendation ? `<p><strong>What to do:</strong> ${r.t(f.recommendation)}</p>` : ""}<p class="muted"><strong>${value.amount}</strong> ${value.note}</p></div>`;
        })
        .join("");
      const good = practiceFor(domain);
      const practice = good.length ? `<div class="good"><h3 class="cap">${r.t("What good looks like")}</h3><ul>${good.map((p, i) => `<li>${r.t(p.text)} <span class="muted">${r.t(sourceLabel(p))}</span>${r.ref(practiceId(domain, i))}</li>`).join("")}</ul></div>` : "";
      return `<h2>${r.t(domainLabel(domain))}</h2>${domain === "adoption" ? adoptionCharts(report) : ""}${practice}${cards}`;
    })
    .join("");
  return page(`<h1>Every issue we found</h1><p class="muted">${r.t("Grouped by area, most serious first. Each reference points to the raw notes, where the working is.")}</p>${sections}`);
}

function plan(r: Render, report: BuiltReport): string {
  const { phases, later, alsoFor } = buildPlan(report.teamFindings, report.added);
  // The other issues the same action covers, by their references (DECISIONS.md 363).
  const also = (f: { id: string }): string => (alsoFor[f.id] ?? []).map((x) => r.ref(x.id)).join("");
  const blocks = phases
    .filter((p) => p.findings.length > 0)
    .map((p) => `<h2>${r.t(`Days ${p.from} to ${p.to}: ${p.title}`)}</h2><ul>${p.findings.map((f) => `<li><strong>${r.t(f.title)}.</strong> ${r.t(f.recommendation)} <span class="muted">${r.t(f.owner)}</span>${r.ref(f.id)}${also(f)}</li>`).join("")}</ul>`)
    .join("");
  const after = later.length ? `<h2>After 90 days</h2><ul>${later.map((f) => `<li>${r.t(f.title)}${r.ref(f.id)}${also(f)}</li>`).join("")}</ul>` : "";
  return page(`<h1>The plan</h1><p class="muted">${r.t("Urgent fixes and quick wins come first, and beyond the quick wins, each month holds only what a team can take on.")}</p>${blocks}${after}`);
}

function compare(r: Render, report: BuiltReport): string {
  const shown = report.comparisons.filter((c) => c.value !== null && (c.published.length > 0 || c.own.status === "measured"));
  if (shown.length === 0) return "";
  const rows = shown
    .map((c) => {
      const own = c.own.status === "measured" ? r.t(`${c.own.position}, against ${c.own.group.label}`) : r.t("Not enough customers yet");
      const published = c.published.map((p) => r.t(`${formatFigure(p.figure.value, c.metric.unit)} ${p.figure.statistic} (${p.figure.source.publisher}, ${p.figure.source.year})`)).join("<br>") || r.t("None");
      return `<tr><td>${r.t(c.metric.label)}${r.ref(comparisonId(c.metric.key))}</td><td class="num">${r.t(formatFigure(c.value!, c.metric.unit))}</td><td>${own}</td><td>${published}</td></tr>`;
    })
    .join("");
  return page(`<h1>How you compare</h1><p class="muted">${r.t("Against other Black Cloud customers, and published research. We never combine them.")}</p><table><thead><tr><th>Measure</th><th class="num">Yours</th><th>Black Cloud customers</th><th>Published research</th></tr></thead><tbody>${rows}</tbody></table>`);
}

function progress(r: Render, report: BuiltReport): string {
  const p = report.progress;
  if (!p) return "";
  const m = (n: number) => compactMoney(n, report.value.currency);
  const list = (title: string, items: typeof p.fixed): string => (items.length ? `<h2>${r.t(`${title} (${items.length})`)}</h2><ul>${items.map((c) => `<li>${r.t(c.title)}${r.ref(c.findingIds[0]!)}</li>`).join("")}</ul>` : "");
  const score = p.score.before !== null && p.score.after !== null ? `<div class="stat"><b>${p.score.before} to ${p.score.after}</b><span>Barometer score</span></div>` : "";
  return page(
    `<h1>Since your last health check</h1><div class="stats">${score}<div class="stat"><b>${m(p.worth.before)} to ${m(p.worth.after)}</b><span>${r.t("What the open issues are worth")}</span></div></div>${list("Fixed", p.fixed)}${list("Still open", p.remaining)}${list("New since then", p.added)}${list("Can't tell yet", p.unknown)}<p class="muted">${r.t("Some of the change in money can come from the business itself, not only from fixes.")}</p>`,
  );
}

/** The documented org as a browser page: every row, the diagram inline (it loads nothing), and nothing at all when no part was measured. */
function org(r: Render, report: BuiltReport): string {
  const doc = readDocument(report.metrics);
  if (!hasDocument(doc)) return "";
  const diagram = dataModelDiagram(doc, 4);
  const parts = [
    doc.model
      ? `<h2>${r.t("The data model")}${r.ref(DOCUMENTED.model)}</h2>${diagram.kind === "chart" ? `<div class="doc-figure">${diagram.svg}</div><p class="small-note">${r.t(`${doc.relationships ? DIAGRAM_KEY : ""} The busiest objects are drawn, and every one is in the table.`)}</p>` : ""}${objectsTable(r, doc)}`
      : "",
    doc.relationships && doc.relationships.links.length ? `<h2>${r.t("How the objects relate")}${r.ref(DOCUMENTED.relationships)}</h2>${relationshipsTable(r, doc)}` : "",
    doc.automation && doc.automation.objects.length ? `<h2>${r.t("What runs when a record is saved")}${r.ref(DOCUMENTED.automation)}</h2><p class="muted">${r.t("By object, in the order Salesforce runs each kind. Within one kind, Salesforce doesn't guarantee which runs first.")}</p><div class="wide">${automationTable(r, doc)}</div>${automationLists(r, doc)}` : "",
    doc.integrations || doc.access
      ? `<div class="doc-cols">${doc.integrations ? `<div><h2>${r.t("Integrations")}${r.ref(DOCUMENTED.integrations)}</h2>${integrationsBlock(r, doc)}</div>` : ""}${doc.access ? `<div><h2>${r.t("Users and access")}${r.ref(DOCUMENTED.access)}</h2>${accessBlock(r, doc)}</div>` : ""}</div>`
      : "",
    doc.sharing ? `<h2>${r.t("Who can see what")}${r.ref(DOCUMENTED.sharing)}</h2>${sharingTable(r, doc)}` : "",
  ];
  return page(`<h1>${r.t("Your org, documented")}</h1><p class="muted">${r.t("A reference for whoever looks after Salesforce. It describes rather than judges.")}</p>${parts.join("")}`);
}

/** The pages, each checked against the report's figures. Throws if any figure on a page can't be traced. */
export function buildMicrositePages(report: BuiltReport, pseudonyms: Pseudonymiser): MicrositePages {
  const r = renderContext(pseudonyms, report.input.calibration.currency, false, referenceMap(noteSections(report)));
  const changed = progress(r, report);
  const pages: MicrositePage[] = [
    { slug: "overview", title: "Overview", html: overview(r, report) },
    ...(changed ? [{ slug: "progress", title: "What has changed", html: changed }] : []),
    { slug: "issues", title: "Every issue", html: issues(r, report) },
    { slug: "plan", title: "The plan", html: plan(r, report) },
  ];
  const compared = compare(r, report);
  if (compared) pages.push({ slug: "compare", title: "How you compare", html: compared });
  const documented = org(r, report);
  if (documented) pages.push({ slug: "org", title: "Your org, documented", html: documented });
  for (const p of pages) {
    const misses = checkNumbers(pageText(p.html), report.dataset).unmatched;
    if (misses.length) throw new Error(`The ${p.title.toLowerCase()} page has ${misses.length} figure${misses.length === 1 ? "" : "s"} the number check can't trace, so the microsite wasn't made`);
  }
  return { pages };
}

/** What a reader sees, for the number check: no styles, no references, cells kept apart. */
export function pageText(html: string): string {
  return html
    .replace(/<style>[\s\S]*?<\/style>/g, "")
    .replace(/<(span|b|td|strong)[^>]*data-ref[^>]*>[\s\S]*?<\/\1>/g, "")
    .replace(/<[^>]+>/g, " | ")
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ");
}
