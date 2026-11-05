import { issuesOf } from "../narrative/select.ts";
import { percent } from "../render/format.ts";
import type { Finding, Metric } from "../schemas/index.ts";
import { SURVEY } from "./summary.ts";

// What the team says, set against what we found in the org. Code pairs them and
// code reads the result, so no figure or judgement here comes from Claude
// (DECISIONS.md 146). Each pairing names the checks it reads, so a reader can
// see why we put these two things side by side.

export type PairingKey = "trust" | "ease" | "rekeying";

export type Pairing = {
  key: PairingKey;
  title: string;
  /** What the team says, in one sentence. */
  said: string;
  /** What we found, in one sentence. */
  found: string;
  /** How the two sit together, read by the rule below. */
  reading: string;
  /** The issues behind `found`, most severe first. */
  issues: Finding[];
  /** True when the team reports a problem. */
  felt: boolean;
};

/** A median at or below this, out of 5, is a team telling us something is wrong. */
export const CONCERN_AT = 3;
/** Re-typing at or above this many hours a week, as a median, is a problem the team feels. */
export const REKEYING_AT = 1;

type Spec = {
  key: PairingKey;
  title: string;
  metric: string;
  felt: (value: number) => boolean;
  said: (value: number) => string;
  /** The issues this pairing reads. */
  issues: (f: Finding) => boolean;
  topic: string;
  /** Org measures that add to what we found, when they're measured. */
  context: (metrics: Metric[]) => string[];
};

export const hours = (n: number): string => `${n} ${n === 1 ? "hour" : "hours"}`;

const valueOf = (metrics: Metric[], id: string): number | null => {
  const m = metrics.find((x) => x.id === id);
  return m?.status === "measured" ? m.value : null;
};

const REKEYING_RULES = new Set(["dq.duplicate-rules-missing", "sec.personal-integration-accounts", "sec.self-authorised-connected-apps"]);

const SPECS: Spec[] = [
  {
    key: "trust",
    title: "Trust in the figures, against data quality and the forecast",
    metric: SURVEY.trust,
    felt: (v) => v <= CONCERN_AT,
    said: (v) => `Your team rates its trust in the pipeline figures ${v} out of 5.`,
    issues: (f) => f.domain === "data_quality" || f.domain === "forecast",
    topic: "data quality and the forecast",
    context: () => [],
  },
  {
    key: "ease",
    title: "How easy Salesforce is, against how it's used",
    metric: SURVEY.ease,
    felt: (v) => v <= CONCERN_AT,
    said: (v) => `Your team rates how easy Salesforce is to use ${v} out of 5.`,
    issues: (f) => f.domain === "adoption",
    topic: "adoption",
    context: (metrics) => {
      const active = valueOf(metrics, "adoption.monthly_active_share");
      const lightning = valueOf(metrics, "adoption.lightning_share");
      return [active === null ? "" : `${percent(active)} of users logged in last month`, lightning === null ? "" : `${percent(lightning)} are on Lightning`].filter(Boolean);
    },
  },
  {
    key: "rekeying",
    title: "Typing data in again, against integrations and duplicates",
    metric: SURVEY.rekeying,
    felt: (v) => v >= REKEYING_AT,
    said: (v) => `The median person in your team spends ${hours(v)} a week typing data in again or keeping a spreadsheet alongside Salesforce.`,
    issues: (f) => REKEYING_RULES.has(f.ruleId),
    topic: "duplicates and integrations",
    context: (metrics) => {
      const points = valueOf(metrics, "documented.integrations");
      return points === null ? [] : [`the org has ${points} integration points`];
    },
  },
];

function reading(felt: boolean, found: boolean): string {
  if (felt && found) return "What we found backs this up.";
  if (felt) return "We didn't find a cause in what we read, so it's worth asking your team what's behind it.";
  if (found) return "Your team rates this better than the data suggests, so these issues may not be felt yet.";
  return "Your team and the data agree.";
}

/** One pairing for each survey answer that's measured. Empty when the survey shows nothing. */
export function buildPairings(metrics: Metric[], findings: Finding[]): Pairing[] {
  return SPECS.flatMap((spec) => {
    const value = valueOf(metrics, spec.metric);
    if (value === null) return [];
    const issues = issuesOf(findings.filter(spec.issues));
    const context = spec.context(metrics);
    const counted = issues.length === 0 ? `We found no issues with ${spec.topic}` : `We found ${issues.length} ${issues.length === 1 ? "issue" : "issues"} with ${spec.topic}`;
    const felt = spec.felt(value);
    return [{ key: spec.key, title: spec.title, said: spec.said(value), found: `${counted}${context.length ? `, and ${context.join(" and ")}` : ""}.`, reading: reading(felt, issues.length > 0), issues, felt }];
  });
}

/** Every count the pairings print, for the number check. The rest are metric values. */
export function pairingFigures(pairings: Pairing[]): number[] {
  return [CONCERN_AT, REKEYING_AT, ...pairings.map((p) => p.issues.length)];
}
