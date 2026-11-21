// The rules that don't yet write their sentences with said and org (DECISIONS.md
// 655, 657), held by org-words-coverage.test.ts and the digest's test. This list
// may only shrink, as rules are converted.

export const UNCONVERTED_RULES: ReadonlySet<string> = new Set<string>([
  // Every rule is converted (DECISIONS.md 657). A rule added here must say why it can't be.
]);
