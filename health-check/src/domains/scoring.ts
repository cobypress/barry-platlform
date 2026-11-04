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
