import type { BuiltReport } from "../narrative/sections.ts";
import { bySeverity, buildPlan } from "../narrative/select.ts";
import type { Finding } from "../schemas/index.ts";
import { estimateLines, severityChip, type Render } from "./html.ts";

// Pages the executive summary has that need no figure of their own: the risks
// money can't measure, and what happens next.

const RISK_ITEMS = 6;
// A4 at 13.5px, less the heading, note and footer. The layout check measures the real thing.
const RISK_BUDGET = 680;
const riskHeight = (f: Finding): number => 72 + 21 * estimateLines(f.title, 70) + 21 * estimateLines(f.summary, 80);

/** Critical and high issues with no figure in money, worst first. Empty when there are none. */
export function riskPage(r: Render, report: BuiltReport): string {
  const risks = report.teamFindings.filter((f) => f.status === "issue" && f.value.kind === "not_sized" && (f.severity === "critical" || f.severity === "high")).sort(bySeverity);
  if (risks.length === 0) return "";
  const shown: Finding[] = [];
  let used = 0;
  for (const f of risks.slice(0, RISK_ITEMS)) {
    if (used + riskHeight(f) > RISK_BUDGET) break;
    shown.push(f);
    used += riskHeight(f);
  }
  const more = risks.length > shown.length ? `<p class="owner" style="margin-top:6px">${r.t("The full report has the rest.")}</p>` : "";
  const items = shown.map((f) => `<article class="issue"><div class="meta">${severityChip(f)}<span>Owner: ${r.t(f.owner)}</span>${r.ref(f.id)}</div><h3>${r.t(f.title)}</h3><p>${r.t(f.summary)}</p></article>`).join("");
  return `<div class="eyebrow">Risk</div><h2>What could hurt you</h2><p style="margin-bottom:12px">${r.t("Not every risk has a price. These are the most serious issues we can't put a figure on, because the cost depends on what goes wrong and when.")}</p>${items}${more}`;
}

/** What happens next. Its only figures are the plan's own. */
export function nextStepsPage(r: Render, report: BuiltReport): string {
  const first = buildPlan(report.teamFindings).phases.find((p) => p.findings.length > 0);
  const start = first
    ? `Name an owner for each of the ${first.findings.length} ${first.findings.length === 1 ? "action" : "actions"} in days ${first.from} to ${first.to}, starting with this: ${first.findings[0]!.title}.`
    : "Agree who owns each action on the roadmap, and when it's due.";
  const steps = [
    { title: "Walk through it together", text: "We'll take your team through what we found, answer their questions, and agree what matters most to you." },
    { title: "Start the first month", text: start },
    { title: "Measure the change", text: "We'll look at your Salesforce again after 90 days, so you can see what has improved, in money as well as in the score." },
  ];
  return `<div class="eyebrow">Next steps</div><h2>What happens next</h2>${steps.map((s) => `<section class="phase"><h3>${r.t(s.title)}</h3><p>${r.t(s.text)}</p></section>`).join("")}<p style="margin-top:18px">${r.t("Black Cloud can deliver this plan alongside your team. Your consultant will talk you through what that would look like.")}</p>`;
}
