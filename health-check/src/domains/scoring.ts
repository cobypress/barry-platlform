import { RULE_SET_VERSION } from "../config.ts";
import { Score, type Finding } from "../schemas/index.ts";
import { passedOn, said } from "../engine/org-words.ts";
import { editionIsWhy } from "../salesforce/refusal-words.ts";

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
/**
 * Shown and not scored (DECISIONS.md 673): PMD's six groups in technical
 * health, so starting to measure them moves no score or grade. Read by the
 * score exactly as the set-up rules are. A test holds this to the rules' own list.
 */
export const UNSCORED_RULES: ReadonlySet<string> = new Set(["tech.lint-data-access", "tech.lint-limits", "tech.lint-injection", "tech.lint-tests", "tech.lint-swallowed-errors", "tech.lint-complexity"]);
/**
 * Shown and not scored (DECISIONS.md 703): the path insights compare the same
 * closed deals that pipe.skipped-stages, pipe.going-backwards and
 * pipe.dead-end-stage already score, so scoring them too would count one thing
 * twice, and a path that is won more often is no fault. Kept apart from
 * UNSCORED_RULES, which a re-check reads as the code checker's groups.
 */
export const INSIGHT_RULES: ReadonlySet<string> = new Set(["pipe.path-insights", "pipe.probability-vs-reality", "lead.fast-conversion", "svc.sent-back-cases", "svc.stuck-in-status"]);
/** The path insights' rule (DECISIONS.md 703): the only insight the digest serves, in its own `paths` block (758). */
export const PATH_INSIGHTS_RULE = "pipe.path-insights";
/**
 * The rules that score the same stage moves the path insights compare (the note
 * above): the issues a digest's `paths` block links to (DECISIONS.md 758).
 */
export const PATH_RELATED_RULES: ReadonlySet<string> = new Set(["pipe.skipped-stages", "pipe.going-backwards", "pipe.dead-end-stage"]);
/**
 * The measures the insights are read from, which go where they go (DECISIONS.md
 * 703). The stage probabilities against what happens (706) join them: a setting
 * held against closed deals, which is no fault to score, kept and not yet shown.
 */
export const INSIGHT_METRICS: ReadonlySet<string> = new Set(["historic.path_variants", "historic.stage_probability_vs_won", "service.status_paths"]);
/**
 * Checks added to an area that count toward its score only when they measure something (DECISIONS.md 770): not
 * measurable, they are left out of both sides of the coverage rule below and of the area's reason, so adding one can
 * never take away a score, or change the reason, an area has without it. Measured, each counts as any other check.
 */
export const COUNTED_WHEN_MEASURED: ReadonlySet<string> = new Set(["lead.source-win-rate", "svc.reopened-time", "svc.closed-straight"]);
/**
 * The same for a rule from a version on (DECISIONS.md 777): cur.stuck-in-stage 3, behind HC_STUCK_EXPECTED, says it
 * can't tell where version 2 measured, so switching it on must never take away an area's score. Version 2 counts as before.
 */
export const COUNTED_WHEN_MEASURED_FROM: Readonly<Record<string, number>> = { "cur.stuck-in-stage": 3 };
const countedWhenMeasured = (f: Finding): boolean => COUNTED_WHEN_MEASURED.has(f.ruleId) || (Object.hasOwn(COUNTED_WHEN_MEASURED_FROM, f.ruleId) && f.ruleVersion >= COUNTED_WHEN_MEASURED_FROM[f.ruleId]!);
/** The rules of COUNTED_WHEN_MEASURED among these findings with no result at all. */
const idleAdditions = (findings: Finding[]): Set<string> => {
  const idle = new Set(findings.filter(countedWhenMeasured).map((f) => f.ruleId));
  for (const f of findings) if (f.status !== "not_measurable") idle.delete(f.ruleId);
  return idle;
};
/**
 * Rule versions shown and never scored (DECISIONS.md 778): cur.stuck-in-stage 4, behind HC_STUCK_UNSCORED, keeps
 * version 2's figure and deal list as an observation. Read by the score as UNSCORED_RULES are, by version, so
 * version 2 scores as before. Kept apart from UNSCORED_RULES, which a re-check reads as the code checker's groups.
 */
export const UNSCORED_VERSIONS: Readonly<Record<string, readonly number[]>> = { "cur.stuck-in-stage": [4] };
/** Whether a finding's rule version is one the score leaves out. */
export const unscoredVersion = (f: { ruleId: string; ruleVersion: number }): boolean => Object.hasOwn(UNSCORED_VERSIONS, f.ruleId) && UNSCORED_VERSIONS[f.ruleId]!.includes(f.ruleVersion);
const notScored = (f: Finding): boolean => SETUP_RULES.has(f.ruleId) || UNSCORED_RULES.has(f.ruleId) || INSIGHT_RULES.has(f.ruleId) || unscoredVersion(f);

const WEIGHTS: Record<string, number> = {
  ruleMaximum: RULE_MAXIMUM,
  ...Object.fromEntries(Object.entries(SEVERITY_WEIGHTS).map(([k, v]) => [`severity.${k}`, v])),
  ...Object.fromEntries(Object.entries(CONFIDENCE_FACTORS).map(([k, v]) => [`confidence.${k}`, v])),
};

/**
 * An area's own words for "nothing here is in use", where one check settles it:
 * forecasting is named only when the check that it's in use found it isn't.
 */
const NOTHING_IN_USE: Record<string, (notInUse: string[]) => string | null> = {
  forecast: (notInUse) => (notInUse.includes("fct.forecasting-in-use") ? "You don't use forecasting, so there is nothing to score." : null),
};

const issueWeight = (f: Finding): number => SEVERITY_WEIGHTS[f.severity] * CONFIDENCE_FACTORS[f.confidence];

/** How a score is worded: HC_AREA_REASONS on gives an unscored area its own checks' reasons (DECISIONS.md 769). Off or absent, as before. */
export type ScoreOptions = { areaReasons?: boolean };

const NONE_MEASURED = "We couldn't measure this area.";
const TOO_LITTLE = "Too little of this area could be measured to score it fairly.";
/** The two reasons an unscored area gave before 769, either of which its checks' reasons may take the place of or follow. */
export const AREA_REASON_TODAY: readonly string[] = [NONE_MEASURED, TOO_LITTLE];
/** Past this many characters a second reason is not said beside the first: the area points to its checks instead. */
export const SHORT_REASON = 100;

/** One part of an area's reason: words of ours, or a reason its checks gave, with the findings that gave it. */
export type AreaReasonPart = { text: string; findingIds: string[] };

/**
 * An unscored area's reason from its own checks' reasons (DECISIONS.md 769),
 * or null where they give none, for the reason as it was. Only checks the
 * score counts, and only those with no result at all: the reasons each gives,
 * once a check. Ranked by the edition first (710: its words are never put
 * after another's), then by how many checks give it, then by the first check
 * to give it, so the same findings always say the same. Every check's reason
 * is said as the check says it, never reworded: one reason alone; two, when
 * each is short; else the first and a sentence pointing to the rest. Where
 * some checks were measured, the first reason is added only when most of the
 * unmeasured checks give it.
 */
export function areaReasonParts(domain: string, findings: Finding[], today: string): AreaReasonPart[] | null {
  const rules = new Map<string, Finding[]>();
  const idle = idleAdditions(findings);
  for (const f of findings) if (f.domain === domain && !notScored(f) && !idle.has(f.ruleId)) rules.set(f.ruleId, [...(rules.get(f.ruleId) ?? []), f]);
  const open = [...rules.values()].filter((group) => group.every((f) => f.status === "not_measurable"));
  const tally = new Map<string, { checks: number; first: number; findingIds: string[] }>();
  open.forEach((group, i) => {
    for (const reason of new Set(group.flatMap((f) => (f.notMeasurableReason ? [f.notMeasurableReason] : [])))) {
      const held = tally.get(reason) ?? { checks: 0, first: i, findingIds: [] };
      held.checks++;
      held.findingIds.push(...group.filter((f) => f.notMeasurableReason === reason).map((f) => f.id));
      tally.set(reason, held);
    }
  });
  if (tally.size === 0) return null;
  const byChecks = ([a, x]: [string, { checks: number; first: number }], [b, y]: [string, { checks: number; first: number }]) => y.checks - x.checks || x.first - y.first;
  const part = ([text, held]: [string, { findingIds: string[] }]): AreaReasonPart => ({ text, findingIds: held.findingIds });
  // Some checks measured: the reason more than half the unmeasured checks give, by count alone, so an edition's
  // reason one check gives never hides the one most give (review of 769). One the edition gave leads only when it is that.
  if (today === TOO_LITTLE) {
    const most = [...tally].sort(byChecks)[0]!;
    return most[1].checks * 2 > open.length ? [{ text: TOO_LITTLE, findingIds: [] }, part(most)] : null;
  }
  // Nothing measured: the edition's reason first (710), then by count.
  const ranked = [...tally].sort((x, y) => Number(editionIsWhy(y[0])) - Number(editionIsWhy(x[0])) || byChecks(x, y));
  const [lead] = ranked[0]!;
  const first = part(ranked[0]!);
  if (ranked.length === 1) return [first];
  if (ranked.length === 2 && lead.length <= SHORT_REASON && ranked[1]![0].length <= SHORT_REASON) return [first, part(ranked[1]!)];
  // The other checks, counted as checks, not readings: one check can give several reasons. With none, the first reason
  // alone; never a count, which the number check would have to trace.
  const rest = open.filter((group) => !group.some((f) => f.notMeasurableReason === lead)).length;
  if (rest === 0) return [first];
  return [first, { text: rest === 1 ? "We couldn't measure another check in this area either, and it gives its own reason." : "We couldn't measure other checks in this area either, and they give their own reasons.", findingIds: [] }];
}

/** The reason an area gave before 769 and the parts its own checks' reasons now make, or null for a reason not made so (DECISIONS.md 769). */
export function areaReasonFrom(domain: string, findings: Finding[], reason: string): { today: string; parts: AreaReasonPart[] } | null {
  for (const today of AREA_REASON_TODAY) {
    const parts = areaReasonParts(domain, findings, today);
    if (parts && parts.map((p) => p.text).join(" ") === reason) return { today, parts };
  }
  return null;
}

/** The parts as one text, as the area's score says it. */
function areaReasonOf(parts: AreaReasonPart[]): string {
  const [a, b] = parts;
  return b ? said`${passedOn(a!.text)} ${passedOn(b.text)}` : said`${passedOn(a!.text)}`;
}

export function scoreDomain(domain: string, findings: Finding[], ruleSetVersion: string = RULE_SET_VERSION, options: ScoreOptions = {}): Score {
  const notMeasurable = (reason: string): Score => Score.parse({ status: "not_measurable", domain, ruleSetVersion, reason });
  const idle = idleAdditions(findings);
  const own = findings.filter((f) => f.domain === domain && !notScored(f) && !idle.has(f.ruleId));
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
  // An area left unscored because it isn't in use says that, not that we
  // failed (DECISIONS.md 627), but only when that's the whole truth: every rule
  // is not in use, or every other one passed. An issue, or anything we couldn't
  // read, was given too little data for or need something for, keeps the
  // reason as it was. Only the words and the kind change: whether an area is
  // scored, and its score, are decided above and below as before.
  const unscored = (reason: string): Score => {
    const notInUse = [...rules].filter(([, group]) => group.every((f) => f.status === "not_measurable" && f.notMeasurableKind === "not_in_use")).map(([ruleId]) => ruleId);
    const rest = [...rules].filter(([ruleId]) => !notInUse.includes(ruleId));
    if (notInUse.length === 0 || !rest.every(([, group]) => group.every((f) => f.status === "passed"))) {
      // HC_AREA_REASONS on: why, from the area's own checks (DECISIONS.md 769). Where they give none, as before.
      const parts = options.areaReasons === true ? areaReasonParts(domain, findings, reason) : null;
      if (parts) return notMeasurable(areaReasonOf(parts));
      return notMeasurable(reason);
    }
    const words = NOTHING_IN_USE[domain]?.(notInUse) ?? (rest.length === 0 ? "Nothing this area checks is in use in your org, so there is nothing to score." : "Most of what this area checks isn't in use in your org, and the rest passed, so there is too little to score.");
    return Score.parse({ status: "not_measurable", domain, ruleSetVersion, reason: words, notMeasurableKind: "not_in_use" });
  };
  if (rulesCounted === 0) return unscored(NONE_MEASURED);
  // Rule 5 for scores. On a real org, one calibration check scored historic
  // pipeline 100 while every pipeline metric was not measurable.
  if (rulesCounted < MIN_RULES_SCORED || rulesCounted * 2 < rules.size) return unscored(TOO_LITTLE);
  return Score.parse({ status: "measured", domain, score: Math.round((earned / (RULE_MAXIMUM * rulesCounted)) * 100), ruleSetVersion, weights: WEIGHTS, findingIds: counted });
}

/** One score per domain, in the order given. A domain with no findings still gets a row. */
export function scoreDomains(domains: string[], findings: Finding[], ruleSetVersion: string = RULE_SET_VERSION, options: ScoreOptions = {}): Score[] {
  return domains.map((domain) => scoreDomain(domain, findings, ruleSetVersion, options));
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
  // Account Engagement (DECISIONS.md 267): the leads it hands over feed the pipeline, as the lead funnel does.
  account_engagement: 1,
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
  const capped = findings.some((f) => f.status === "issue" && f.severity === "critical" && !notScored(f)) && uncapped > CRITICAL_CAP;
  const score = capped ? CRITICAL_CAP : uncapped;
  const { grade, word } = gradeOf(score);
  return { status: "measured", score, grade, word, uncapped, capped, areas };
}
