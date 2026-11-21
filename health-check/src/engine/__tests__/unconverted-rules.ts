// The rules that don't yet write their sentences with said and org (DECISIONS.md
// 655, 657), held by org-words-coverage.test.ts and the digest's test. This list
// may only shrink, as rules are converted.

export const UNCONVERTED_RULES: ReadonlySet<string> = new Set<string>([
  "cpq.discount-approvals", "cpq.discount-leakage", "cpq.inactive-rules", "cpq.price-rule-conflicts", "cpq.quote-to-order",
  "cpq.renewals-not-started", "cpq.twin-field-mismatch",
]);
