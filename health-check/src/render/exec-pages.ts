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

/** What a consultant-led run's documents promise of a person's time: never printed on an automatic run, and held for us if one ever is (DECISIONS.md 698). */
export const CONSULTANT_NEXT_STEPS = {
  first: { title: "Walk through it together", text: "We'll take your team through what we found, answer their questions, and agree what matters most to you." },
  closing: "Black Cloud can deliver this plan alongside your team. Your consultant will talk you through what that would look like.",
  /** The 90-day line in the executive summary and the one-pager. */
  recheck: "We'll look at your Salesforce again after 90 days, so you can see what has improved, in money as well as in the score.",
  /** The same line in the full report. */
  recheckFull: "We'll look at your Salesforce again after 90 days, so you can see what has improved and what still needs work.",
} as const;

/**
 * An automatic run's: nobody walks the customer through it, no consultant is
 * there to talk and nobody has promised to look again (DECISIONS.md 693, 698),
 * so they say only what the customer can do, and nothing is offered for sale.
 */
export const AUTOMATIC_NEXT_STEPS = {
  first: { title: "Read it with your team", text: "Share what we found with the people who own each area, and agree what matters most to you." },
  recheck: "Check again after 90 days to see what has changed.",
} as const;

/** The first step, the 90-day line and the closing line (none on an automatic run) of "What happens next". A consultant-led run's are as they were. */
export function nextStepsWords(automatic: boolean, recheck: string): { first: { title: string; text: string }; recheck: string; closing: string | null } {
  return automatic ? { ...AUTOMATIC_NEXT_STEPS, closing: null } : { first: CONSULTANT_NEXT_STEPS.first, recheck, closing: CONSULTANT_NEXT_STEPS.closing };
}

/** What happens next. Its only figures are the plan's own. */
export function nextStepsPage(r: Render, report: BuiltReport): string {
  const words = nextStepsWords(report.input.run.reviewMode === "automatic", CONSULTANT_NEXT_STEPS.recheck);
  const first = buildPlan(report.teamFindings, report.added).phases.find((p) => p.findings.length > 0);
  const start = first
    ? `Name an owner for each of the ${first.findings.length} ${first.findings.length === 1 ? "action" : "actions"} in days ${first.from} to ${first.to}, starting with this: ${first.findings[0]!.title}.`
    : "Agree who owns each action on the roadmap, and when it's due.";
  const steps = [
    words.first,
    { title: "Start the first month", text: start },
    { title: "Measure the change", text: words.recheck },
  ];
  return `<div class="eyebrow">Next steps</div><h2>What happens next</h2>${steps.map((s) => `<section class="phase"><h3>${r.t(s.title)}</h3><p>${r.t(s.text)}</p></section>`).join("")}${words.closing ? `<p style="margin-top:18px">${r.t(words.closing)}</p>` : ""}`;
}
