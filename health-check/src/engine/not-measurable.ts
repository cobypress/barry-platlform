import type { NotMeasurableKind } from "../schemas/index.ts";

// Which of four kinds a not measurable reason is (DECISIONS.md 627), so a
// report can tell "you don't use this" and "we need something from you" from
// "we couldn't read it". The reasons are the engine's own words, so the kind is
// read from the words the reason opens with, never from text a reason quotes:
// every pattern is anchored at the start, and none reaches past a colon, after
// which a reason quotes Salesforce or a consultant.
//
// Only what the engine can truly tell goes in a benign kind. A reason that
// hedges ("or can't be read", "may not be on") stays unreadable, and so does
// any reason no pattern names, so a new or reworded reason is never called
// benign by accident.
//
// "Doesn't apply to your org" rests only on something read that states it: a
// setting, an installed-package or licence check, an edition fact or a feature
// flag. Never on a field a describe leaves out, or a read that found nothing:
// either can mean the connected user can't see it.

type Sorted = { kind: Exclude<NotMeasurableKind, "unreadable">; pattern: RegExp };

const SORTED: readonly Sorted[] = [
  // A person has to give it: a calibration entry or a manual step.
  { kind: "needs_input", pattern: /^The calibration doesn't (?:say|name)\b/ },
  { kind: "needs_input", pattern: /^No targets were given in the calibration\./ },
  { kind: "needs_input", pattern: /^The calibration keeps targets in Forecasting quotas, but no revenue forecast type is active\b/ },
  { kind: "needs_input", pattern: /^The calibration needs\b/ },
  { kind: "needs_input", pattern: /^The org's fiscal year couldn't be read, and the calibration doesn't set one\./ },
  { kind: "needs_input", pattern: /^No account is calibrated as an integration account\b/ },
  { kind: "needs_input", pattern: /^No open stage was passed through by more than [^:]*, and the calibration marks none as required\./ },
  { kind: "needs_input", pattern: /^Salesforce's setting can't tell lost from no decision for:/ },
  // What an automatic run's customer hasn't told us yet (DECISIONS.md 693).
  { kind: "needs_input", pattern: /^You haven't told us\b/ },
  { kind: "needs_input", pattern: /^The (?:unused )?seats can't be priced, because / },
  { kind: "needs_input", pattern: /^The manual step was skipped:/ },
  { kind: "needs_input", pattern: /^[^:]*, and the manual step hasn't been answered yet\./ },
  { kind: "needs_input", pattern: /^Whether [^:]* can be released can't be measured, because the [^:]* step was skipped:/ },

  // The org doesn't use the feature, and a setting or an edition fact says so.
  // Forecasting: the forecast types were read and none is an active revenue one.
  { kind: "not_in_use", pattern: /^No revenue forecast type is active\b/ },
  // Knowledge: the org's own KnowledgeSettings.enableKnowledge is false.
  { kind: "not_in_use", pattern: /^Salesforce Knowledge is switched off\b/ },
  // Apex: Organization.OrganizationType is Professional Edition, which has none.
  { kind: "not_in_use", pattern: /^Professional Edition has no Apex\b/ },

  // What was read can't support a fair figure: too few records, a field mostly
  // empty, or nothing at all. A read that found nothing is never "not in use":
  // the connected user may not see what's there.
  { kind: "too_little_data", pattern: /^No case milestone started in the last year\b/ },
  { kind: "too_little_data", pattern: /^No case was closed in the last year\./ },
  { kind: "too_little_data", pattern: /^Omni-Channel offered no work in the last year\./ },
  { kind: "too_little_data", pattern: /^Knowledge is on, but no article is published\./ },
  { kind: "too_little_data", pattern: /^No tasks were logged in the last year\b/ },
  { kind: "too_little_data", pattern: /^No account has a won deal\b/ },
  { kind: "too_little_data", pattern: /^No leads were created in the window\b/ },
  { kind: "too_little_data", pattern: /^None of the [^:]* was ordered in Salesforce\b/ },
  { kind: "too_little_data", pattern: /^[^:]* is below the (?:calibrated minimum of|[\d,]+ needed)\b/ },
  { kind: "too_little_data", pattern: /^[^:]*, below the (?:calibrated minimum of|[\d,]+ needed)\b/ },
  { kind: "too_little_data", pattern: /^No open stage has [\d,]+ won deals to compare with\./ },
  { kind: "too_little_data", pattern: /^[^:]*, fewer than the [\d,]+ (?:we need|it takes)\b/ },
  { kind: "too_little_data", pattern: /^[^:]* is too few to\b/ },
  { kind: "too_little_data", pattern: /^[^:]*, too few to\b/ },
  { kind: "too_little_data", pattern: /^Too few [^:]* to compare:/ },
  { kind: "too_little_data", pattern: /^Only [\d,]+ [^:]*(?:so a share means little|and we need [\d,]+)\b/ },
  { kind: "too_little_data", pattern: /^[\d,.]+%? (?:of [\d,]+ )?[^:]* (?:has|have) no amount, so [^:]* would mislead\./ },
  { kind: "too_little_data", pattern: /^(?:Open|Closed) deals have no amounts\b/ },
  { kind: "too_little_data", pattern: /^Won deals on accounts have no amounts\./ },
  { kind: "too_little_data", pattern: /^No campaign has an actual cost recorded\b/ },
  { kind: "too_little_data", pattern: /^No won deal has an? [^:]*, so wins can't be split by it\./ },
  { kind: "too_little_data", pattern: /^No whole (?:month|quarter)\b/ },
  { kind: "too_little_data", pattern: /^No month end falls inside the stage history\./ },
  { kind: "too_little_data", pattern: /^No stage with open deals has [\d,]+ closed deals\b/ },
  { kind: "too_little_data", pattern: /^No source has the calibrated minimum\b/ },
  { kind: "too_little_data", pattern: /^No factor has enough deals on both sides\b/ },
  // The paths deals take (DECISIONS.md 703): history read, but none from while closed deals were open, or no two alike.
  { kind: "too_little_data", pattern: /^Stage history isn't tracked for these deals:/ },
  { kind: "too_little_data", pattern: /^Each of the [\d,]+ [^:]* took a different path\b/ },
  { kind: "too_little_data", pattern: /^There are no Commit forecast rows with amounts\b/ },
  { kind: "too_little_data", pattern: /^Commit forecast rows are all zero\b/ },
  { kind: "too_little_data", pattern: /^The quota for this quarter is zero\./ },
  { kind: "too_little_data", pattern: /^The login history covers [^:]*, and a trend needs\b/ },
  { kind: "too_little_data", pattern: /^Fewer than [\d,]+ people answered\b/ },
];

// A read cut short is a limit on what we could read, whatever else the reason says.
const CUT_SHORT = /^[^:]*\b(?:row limit|read in full)\b/;

/** The kind of a not measurable reason: unreadable unless the reason's own opening words say otherwise. */
export function notMeasurableKind(reason: string): NotMeasurableKind {
  if (CUT_SHORT.test(reason)) return "unreadable";
  return SORTED.find((s) => s.pattern.test(reason))?.kind ?? "unreadable";
}
