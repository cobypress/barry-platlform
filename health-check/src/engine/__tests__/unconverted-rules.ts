// The rules that don't yet write their sentences with said and org (DECISIONS.md
// 655, 657), held by org-words-coverage.test.ts and the digest's test. This list
// may only shrink, as rules are converted.

export const UNCONVERTED_RULES: ReadonlySet<string> = new Set<string>([
  "aut.apex-coverage", "aut.old-api-versions", "cpq.discount-approvals", "cpq.discount-leakage", "cpq.inactive-rules",
  "cpq.price-rule-conflicts", "cpq.quote-to-order", "cpq.renewals-not-started", "cpq.twin-field-mismatch", "lead.campaign-return",
  "lead.conversion-by-source", "lead.converted-no-opportunity", "lead.no-first-response", "lead.response-time", "lead.source-return",
  "mkt.campaign-influence", "mkt.scoring-grading", "mkt.unowned-prospects", "mkt.unworked-handovers", "sec.health-check-risks",
  "sec.health-check-score", "sec.modify-all-data", "sec.peer-outlier-access", "sec.sharing-defaults", "sec.unused-access",
  "sec.view-all-data", "tech.apex-code-size", "tech.classic-leftovers", "tech.governor-limit-risk", "tech.storage-use",
  "tech.test-quality",
]);
