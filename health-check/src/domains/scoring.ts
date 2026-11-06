import { RULE_SET_VERSION } from "../config.ts";
import { Score, type Finding } from "../schemas/index.ts";

// Every rule counted is worth the same maximum. A pass earns all of it, and an
// issue earns what's left after its weight. So severity still counts when a
// domain has no passes: only critical issues can take a score to zero.
//
// It's counted per rule, not per finding, because some rules raise one finding
// for each outlier, and ten segments of one problem aren't ten problems. A rule
// counts once, at its worst finding. Not measurable counts for nothing either
// way: an unknown is neither a pass nor a fault, which is why a domain with
// nothing measurable has no score at all rather than a zero.

export const SEVERITY_WEIGHTS = { critical: 8, high: 5, medium: 3, low: 1, info: 0 } as const;
export const CONFIDENCE_FACTORS = { high: 1, medium: 0.75, low: 0.5 } as const;
export const RULE_MAXIMUM = SEVERITY_WEIGHTS.critical;
/** A score needs this many measured checks, and at least half the domain's. */
export const MIN_RULES_SCORED = 2;

// Gaps in how we set the check up, not faults in the customer's org.
const SETUP_RULES = new Set(["pipe.unmapped-stages"]);

const WEIGHTS: Record<string, number> = {
  ruleMaximum: RULE_MAXIMUM,
  ...Object.fromEntries(Object.entries(SEVERITY_WEIGHTS).map(([k, v]) => [`severity.${k}`, v])),
  ...Object.fromEntries(Object.entries(CONFIDENCE_FACTORS).map(([k, v]) => [`confidence.${k}`, v])),
};

const issueWeight = (f: Finding): number => SEVERITY_WEIGHTS[f.severity] * CONFIDENCE_FACTORS[f.confidence];

export function scoreDomain(domain: string, findings: Finding[], ruleSetVersion: string = RULE_SET_VERSION): Score {
  const notMeasurable = (reason: string): Score => Score.parse({ status: "not_measurable", domain, ruleSetVersion, reason });
  const own = findings.filter((f) => f.domain === domain && !SETUP_RULES.has(f.ruleId));
  if (own.length === 0) return notMeasurable("We couldn't assess this area.");
  const rules = new Map<string, Finding[]>();
  for (const finding of own) rules.set(finding.ruleId, [...(rules.get(finding.ruleId) ?? []), finding]);

  let earned = 0;
  let rulesCounted = 0;
  const counted: string[] = [];
  for (const group of rules.values()) {
    const issues = group.filter((f) => f.status === "issue");
    const passes = group.filter((f) => f.status === "passed");
    if (issues.length === 0 && passes.length === 0) continue;
    rulesCounted++;
    earned += issues.length > 0 ? RULE_MAXIMUM - Math.max(...issues.map(issueWeight)) : RULE_MAXIMUM;
    counted.push(...issues.map((f) => f.id), ...passes.map((f) => f.id));
  }
  if (rulesCounted === 0) return notMeasurable("We couldn't measure this area.");
  // Rule 5 for scores. On a real org, one calibration check scored historic
  // pipeline 100 while every pipeline metric was not measurable.
  if (rulesCounted < MIN_RULES_SCORED || rulesCounted * 2 < rules.size) return notMeasurable("Too little of this area could be measured to score it fairly.");
  return Score.parse({ status: "measured", domain, score: Math.round((earned / (RULE_MAXIMUM * rulesCounted)) * 100), ruleSetVersion, weights: WEIGHTS, findingIds: counted });
}

/** One score per domain, in the order given. A domain with no findings still gets a row. */
export function scoreDomains(domains: string[], findings: Finding[], ruleSetVersion: string = RULE_SET_VERSION): Score[] {
  return domains.map((domain) => scoreDomain(domain, findings, ruleSetVersion));
}

// The Barometer score: one figure for the whole org, from the area scores,
// weighted by how directly each area touches revenue. The weights are our
// judgement, so they're written down here and in the raw notes, never hidden.
export const AREA_WEIGHTS: Record<string, number> = {
  current_pipeline: 1.5,
  forecast: 1.25,
  data_quality: 1.25,
  security: 1.25,
  // Whether people use Salesforce decides whether every other figure means anything (DECISIONS 99).
  adoption: 1.25,
  historic_pipeline: 1,
  activity: 1,
  utilisation: 1,
  lead_funnel: 1,
  revenue_opportunities: 1,
  automation: 1,
  // Roadmap 3.2 (DECISIONS.md 172). An integration that stops takes data with it;
  // sound code is a step further back from revenue, as configuration is.
  integrations: 1,
  technical_health: 0.75,
  configuration: 0.75,
  best_practice: 0.75,
  // Service Cloud (DECISIONS.md 156): how cases are handled touches customers
  // directly; how the set-up is built is a step further back.
  service_operations: 1.25,
  service_setup: 0.75,
  // CPQ (DECISIONS.md 220): what quotes are sold for is revenue itself; how CPQ
  // is set up is a step further back, as for Service Cloud.
  cpq_quoting: 1.25,
  cpq_setup: 0.75,
};
const DEFAULT_AREA_WEIGHT = 1;
/** An org with a critical issue can't score above this, however well the rest does. */
export const CRITICAL_CAP = 49;
/** Fewer areas scored than this, or less than half the weight, and there's no overall score. */
export const MIN_AREAS_SCORED = 4;

export const GRADES = [
  { min: 85, grade: "A", word: "Strong" },
  { min: 70, grade: "B", word: "Good" },
  { min: 55, grade: "C", word: "Fair" },
  { min: 40, grade: "D", word: "Weak" },
  { min: 0, grade: "E", word: "Poor" },
] as const;

export type OverallScore =
  | { status: "measured"; score: number; grade: string; word: string; uncapped: number; capped: boolean; areas: Array<{ domain: string; score: number; weight: number }> }
  | { status: "not_measurable"; reason: string };

export const areaWeight = (domain: string): number => AREA_WEIGHTS[domain] ?? DEFAULT_AREA_WEIGHT;

export function gradeOf(score: number): (typeof GRADES)[number] {
  return GRADES.find((g) => score >= g.min) ?? GRADES[GRADES.length - 1]!;
}

export function overallScore(scores: Score[], findings: Finding[]): OverallScore {
  const areas = scores.flatMap((s) => (s.status === "measured" ? [{ domain: s.domain, score: s.score, weight: areaWeight(s.domain) }] : []));
  const totalWeight = scores.reduce((sum, s) => sum + areaWeight(s.domain), 0);
  const scoredWeight = areas.reduce((sum, a) => sum + a.weight, 0);
  if (areas.length < MIN_AREAS_SCORED || scoredWeight * 2 < totalWeight) return { status: "not_measurable", reason: "Too few areas could be scored to give the org one score fairly." };
  const uncapped = Math.round(areas.reduce((sum, a) => sum + a.score * a.weight, 0) / scoredWeight);
  const capped = findings.some((f) => f.status === "issue" && f.severity === "critical" && !SETUP_RULES.has(f.ruleId)) && uncapped > CRITICAL_CAP;
  const score = capped ? CRITICAL_CAP : uncapped;
  const { grade, word } = gradeOf(score);
  return { status: "measured", score, grade, word, uncapped, capped, areas };
}
