import type { Finding, Metric } from "../schemas/index.ts";
import { buildPairings, hours, pairingFigures, type Pairing } from "./pairings.ts";
import { SURVEY, SURVEY_METRIC_IDS } from "./summary.ts";

// What every document shows of the team survey, built once so they all say the
// same thing. `shown` is false under five answers, and then only `reason` is
// printed: not the count, not a role, nothing else (DECISIONS.md 142).

export type SurveyView = {
  shown: boolean;
  /** Why nothing is shown. Null when it is. */
  reason: string | null;
  metrics: Metric[];
  pairings: Pairing[];
  /** The one paragraph the executive summary and the microsite print. */
  paragraph: string;
};

const valueOf = (metrics: Metric[], id: string): number | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m.value : null;
};

/** Null when the run has no survey. `metrics` are the report's, with the survey's among them. */
export function buildSurveyView(metrics: Metric[], findings: Finding[]): SurveyView | null {
  const own = metrics.filter((m) => SURVEY_METRIC_IDS.includes(m.id));
  if (own.length === 0) return null;
  const count = own.find((m) => m.id === SURVEY.responses);
  if (count?.status !== "measured") {
    const reason = count?.status === "not_measurable" ? count.reason : "We couldn't read the team survey.";
    return { shown: false, reason, metrics: own, pairings: [], paragraph: reason };
  }
  const pairings = buildPairings(own, findings);
  return { shown: true, reason: null, metrics: own, pairings, paragraph: paragraphOf(own, pairings) };
}

function paragraphOf(metrics: Metric[], pairings: Pairing[]): string {
  const n = valueOf(metrics, SURVEY.responses)!;
  const ease = valueOf(metrics, SURVEY.ease);
  const trust = valueOf(metrics, SURVEY.trust);
  const lost = valueOf(metrics, SURVEY.lost);
  const rates = [ease === null ? "" : `how easy Salesforce is ${ease} out of 5`, trust === null ? "" : `their trust in the pipeline figures ${trust} out of 5`].filter(Boolean);
  const backed = pairings.filter((p) => p.felt && p.issues.length > 0).length;
  return [
    `${n} people in your team answered our survey.`,
    rates.length ? `They rate ${rates.join(", and ")}.` : "",
    lost === null ? "" : `The median person says they lose ${hours(lost)} a week to admin that doesn't help them sell and to typing data in again.`,
    pairings.length ? `We set ${pairings.length} of their answers against what we found in Salesforce, and what we found backs up what they say on ${backed === 0 ? "none" : backed} of them.` : "",
  ]
    .filter(Boolean)
    .join(" ");
}

/** The counts the survey's pages print that no metric holds, for the number check. */
export function surveyViewFigures(view: SurveyView | null): number[] {
  if (!view?.shown) return [];
  return [...pairingFigures(view.pairings), view.pairings.length, view.pairings.filter((p) => p.felt && p.issues.length > 0).length];
}
