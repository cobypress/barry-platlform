import { apexExtractor } from "../../extract/automation.ts";
import { codeLintExtractor, LINT_SOURCES } from "../../extract/code-lint.ts";
import type { LintFinding, LintKind, RuleTally } from "../../extract/lint/findings.ts";
import { notSized, plural, type Rule, type RuleContext, type RuleResult } from "../../engine/rule.ts";
import { joinSaid, org, said, type Said } from "../../engine/org-words.ts";
import { andList } from "../../engine/said-forms.ts";
import { calloutCode, setsLiteralAddress } from "../integrations/index.ts";
import { judgedTests, liveCode, loopsOverLimits } from "./code.ts";

// A chosen few of PMD's findings, shown in technical health (DECISIONS.md 673).
// Every rule's counts are kept for the benchmark; only these six groups, what
// an owner of the business should care about, are shown, and none is scored
// (UNSCORED_RULES in scoring.ts). Where our own scan already reports the same
// thing in a class or trigger, PMD's finding there is counted and said, never
// raised again. Nothing here appears unless the run had the code-lint step:
// a scan, a pulse and the command line show nothing of it.

const DOMAIN = "technical_health";
type Severity = "high" | "medium" | "low";

/** Our own check that already reports what a PMD rule finds, in the components it names. */
type Already = { by: Said; names: (ctx: RuleContext) => Set<string> | null };

type Group = {
  id: string;
  /** The rule's constant title: a pass's and a not measurable finding's. */
  title: string;
  rules: readonly string[];
  /** The group's severity, and what PMD's priority 1 or 2 raises it to. */
  severity: Severity;
  raised: Severity;
  issueTitle: Said;
  /** Follows "PMD found 3 places in 2 classes and triggers". */
  where: Said;
  why: Said;
  recommendation: Said;
  value: Said;
  owner: string;
  dependencies: string[];
  already?: Partial<Record<string, Already>>;
};

const key = (kind: LintKind, name: string): string => `${kind}\u0000${name}`;

/** The components our checks of queries and saves inside loops name, by the same conditions (tech.governor-limit-risk). */
const loopCode = (ctx: RuleContext): Set<string> | null => {
  const { classes, triggers } = ctx.data(apexExtractor);
  if (classes.status !== "ok" || triggers.status !== "ok") return null;
  return new Set(liveCode(classes.data, triggers.data).code.filter(loopsOverLimits).map((c) => key(c.kind === "apex class" ? "class" : "trigger", c.Name)));
};
/** The test classes tech.test-quality names, for one of its two reasons. */
const testClasses = (which: "silent" | "seeAll") => (ctx: RuleContext): Set<string> | null => {
  const { classes } = ctx.data(apexExtractor);
  if (classes.status !== "ok") return null;
  return new Set(judgedTests(classes.data)[which].map((c) => key("class", c.Name)));
};
/** The classes int.hardcoded-endpoints names for an address written into the code. */
const literalAddresses = (ctx: RuleContext): Set<string> | null => {
  const { classes } = ctx.data(apexExtractor);
  if (classes.status !== "ok") return null;
  return new Set(calloutCode(classes.data).filter(setsLiteralAddress).map((c) => key("class", c.Name)));
};

export const LINT_GROUPS: readonly Group[] = [
  {
    id: "tech.lint-data-access",
    title: "Access checks in code",
    rules: ["ApexCRUDViolation", "ApexSharingViolations"],
    severity: "medium",
    raised: "high",
    issueTitle: said`Some code reads or changes data without checking access`,
    where: said`where the code reads or changes records without checking what the running user may see and do, or without saying whether sharing applies`,
    why: said`Code like this can show or change data a user shouldn't reach, for example through a page or an integration.`,
    recommendation: said`Check object and field permissions before each read and save, and say on each class whether it runs with sharing, writing down why where it must run without.`,
    value: said`The cost is data reaching people who shouldn't see it, which we can't size from the code.`,
    owner: "Salesforce developer",
    dependencies: ["Check who uses each changed path has the access it needs before the checks are added"],
  },
  {
    id: "tech.lint-limits",
    title: "Other limit risks in code",
    rules: ["OperationWithLimitsInLoop", "OperationWithHighCostInLoop", "AvoidNonRestrictiveQueries"],
    severity: "medium",
    raised: "high",
    issueTitle: said`Some code risks Salesforce's limits in other ways`,
    where: said`where the code calls a limited or costly operation, such as an email, a callout or a describe, inside a loop, or queries with no filter and no limit`,
    why: said`Each works with a few records and fails or slows once enough arrive together.`,
    recommendation: said`Move limited and costly calls out of loops, and give every query a filter or a limit.`,
    value: said`The cost is work that fails as volumes rise, which we can't size before it happens.`,
    owner: "Salesforce developer",
    dependencies: ["Test each change with a bulk load in a sandbox"],
    already: { OperationWithLimitsInLoop: { by: said`our own check of queries and saves inside loops`, names: loopCode } },
  },
  {
    id: "tech.lint-injection",
    title: "Injection and unsafe redirects in code",
    rules: ["ApexSOQLInjection", "ApexOpenRedirect", "ApexBadCrypto", "ApexXSSFromEscapeFalse", "ApexXSSFromURLParam", "ApexCSRF", "ApexDangerousMethods", "ApexInsecureEndpoint"],
    severity: "medium",
    raised: "high",
    issueTitle: said`Some code is open to injection or unsafe redirects`,
    where: said`where the code builds a query, a page or a redirect from what a user can supply, uses a fixed encryption key, changes data as a page loads, or calls out over plain http`,
    why: said`Code like this can let someone read or change data they shouldn't, or send people somewhere unsafe.`,
    recommendation: said`Bind what users supply into queries rather than joining it in, escape what pages show, check every redirect against the addresses you allow, keep keys out of the code, and call out only over https.`,
    value: said`The cost is a security weakness, which we can't size from the code.`,
    owner: "Salesforce developer",
    dependencies: ["Check each other system accepts https before a callout is changed"],
    already: { ApexInsecureEndpoint: { by: said`our own check of addresses written into code`, names: literalAddresses } },
  },
  {
    id: "tech.lint-tests",
    title: "Test methods that check their results",
    rules: ["ApexUnitTestClassShouldHaveAsserts", "ApexUnitTestShouldNotUseSeeAllDataTrue"],
    severity: "low",
    raised: "medium",
    issueTitle: said`Some test methods check nothing or read live data`,
    where: said`where a test method runs code without an assertion, or reads the org's live data (SeeAllData=true)`,
    why: said`Tests like these raise coverage without proving the code works, or pass and fail with what's in the org.`,
    recommendation: said`Add an assertion to each test method, and have tests make the records they need.`,
    value: said`The cost is code that passes its tests and still breaks, which we can't size from the code.`,
    owner: "Salesforce developer",
    dependencies: [],
    already: {
      ApexUnitTestClassShouldHaveAsserts: { by: said`our own check of test classes`, names: testClasses("silent") },
      ApexUnitTestShouldNotUseSeeAllDataTrue: { by: said`our own check of test classes`, names: testClasses("seeAll") },
    },
  },
  {
    id: "tech.lint-swallowed-errors",
    title: "Errors caught and ignored in code",
    rules: ["EmptyCatchBlock"],
    severity: "low",
    raised: "medium",
    issueTitle: said`Some code catches errors and ignores them`,
    where: said`where the code catches an error and does nothing with it`,
    why: said`When something fails there, nobody is told and nothing is logged, so work can be left half done without anyone knowing.`,
    recommendation: said`Log each error the code catches, and tell the user or an admin where it matters, or let it fail where people will see it.`,
    value: said`The cost is failures nobody sees, which we can't size from the code.`,
    owner: "Salesforce developer",
    dependencies: [],
  },
  {
    id: "tech.lint-complexity",
    title: "Code complexity",
    rules: ["CognitiveComplexity", "CyclomaticComplexity", "NcssCount", "AvoidDeeplyNestedIfStmts"],
    severity: "low",
    raised: "low",
    issueTitle: said`Some code is too complex to change safely`,
    where: said`where a method or class has very many paths, statements or nested conditions`,
    why: said`Code like this is slow and risky to change, and the hardest to test.`,
    recommendation: said`Split the most complex methods into smaller ones, each with its own tests, when you next change them.`,
    value: said`The cost is slower, riskier changes, which we can't size from the code.`,
    owner: "Salesforce developer",
    dependencies: [],
  },
];

export const LINT_RULE_IDS: readonly string[] = LINT_GROUPS.map((g) => g.id);

/** How many components the summary names, and how many line numbers for each. */
const NAMED = 3;
const LINES = 5;

const subjectType = (kind: LintKind): "apex class" | "apex trigger" => (kind === "trigger" ? "apex trigger" : "apex class");

function linesOf(lines: number[], more: boolean): Said {
  const shown = lines.slice(0, LINES);
  const rest = more || lines.length > LINES;
  if (shown.length === 1 && !rest) return said`line ${shown[0]!}`;
  const numbers = shown.map((n) => said`${n}`);
  return rest ? said`lines ${joinSaid(numbers, ", ")} and others` : said`lines ${andList(numbers)}`;
}

function evaluateGroup(group: Group, ctx: RuleContext): RuleResult | RuleResult[] {
  // Only a run with the step shows these: a scan, a pulse or the command line shows nothing (DECISIONS.md 673).
  if (!ctx.ran?.(codeLintExtractor)) return [];
  const { pmd } = ctx.data(codeLintExtractor);
  if (pmd.status !== "ran") {
    return { outcome: "not_measurable", reason: `We couldn't check this, because PMD didn't run in this health check: ${pmd.reason}`, evidence: [ctx.ev(LINT_SOURCES.classes, { count: null })] };
  }
  if (!pmd.tally || !pmd.read) return { outcome: "not_measurable", reason: said`PMD ran, but its counts weren't kept in this run, so there's nothing to judge.`, evidence: [ctx.ev(LINT_SOURCES.classes, { count: null })] };
  const ours = new Set(group.rules);
  // Each of our own checks named once, by the rules it already covers.
  const named = new Map<string, Set<string> | null>();
  for (const [rule, already] of Object.entries(group.already ?? {})) named.set(rule, already!.names(ctx));
  const listedAlready = (rule: string, kind: LintKind, component: string): boolean => named.get(rule)?.has(key(kind, component)) ?? false;

  const tallies = pmd.tally.filter((t) => ours.has(t.rule));
  const shown = tallies.filter((t) => t.found > 0 && !listedAlready(t.rule, t.kind, t.component));
  const found = shown.reduce((sum, t) => sum + t.found, 0);
  const suppressed = tallies.reduce((sum, t) => sum + t.suppressed, 0);
  const already = tallies.filter((t) => listedAlready(t.rule, t.kind, t.component));
  const alreadyCount = already.reduce((sum, t) => sum + t.found, 0);
  const components = [...new Map(shown.map((t) => [key(t.kind, t.component), t])).values()];
  const checked = pmd.read.classes + pmd.read.triggers;

  const exclusions = [
    ...(pmd.skipped.tooLarge ? [`Classes and triggers too large to hand to PMD: ${pmd.skipped.tooLarge}`] : []),
    ...(pmd.skipped.unparsable ? [`Classes and triggers PMD couldn't read: ${pmd.skipped.unparsable}`] : []),
    ...(pmd.skipped.unreadable ? [`Classes and triggers whose code is hidden: ${pmd.skipped.unreadable}`] : []),
    "Installed packages' code",
    ...pmd.limits,
  ];
  const evidence = [ctx.ev(LINT_SOURCES.classes, { count: checked, exclusions, note: `Read by PMD ${pmd.toolVersion} with our own rules: ${group.rules.join(", ")}` })];
  const segments = [{ dimension: "class or trigger", segments: checked }];
  const facts = {
    found,
    components: components.length,
    checked,
    suppressed,
    alreadyListed: alreadyCount,
    ...Object.fromEntries(group.rules.map((r) => [r, shown.filter((t) => t.rule === r).reduce((sum, t) => sum + t.found, 0)])),
  };
  // What else the count leaves out, each said in numbers.
  const alreadyBy = [...new Set(already.map((t) => group.already![t.rule]!.by))];
  const notes = joinSaid(
    [
      ...(alreadyCount ? [said` ${plural(alreadyCount, "more is", "more are")} in code ${andList(alreadyBy)} already lists.`] : []),
      ...(suppressed ? [said` ${plural(suppressed, "more was", "more were")} marked to be ignored in the code.`] : []),
    ],
    "",
  );
  if (found === 0) {
    return checked === 0
      ? { outcome: "pass", summary: said`PMD found nothing of this kind.${notes}`, evidence, segmentExemptReason: "PMD read no class or trigger to segment.", facts }
      : { outcome: "pass", summary: said`PMD found nothing of this kind in the ${plural(checked, "class or trigger", "classes and triggers")} it read.${notes}`, evidence, segmentsChecked: segments, facts };
  }

  // Where, from the findings the step kept: worst first is how it bounded them, so a component may show fewer lines than it has.
  const kept = pmd.findings.filter((f: LintFinding) => ours.has(f.rule) && !f.suppressed && !listedAlready(f.rule, f.kind, f.component));
  const linesBy = new Map<string, number[]>();
  for (const f of kept) linesBy.set(key(f.kind, f.component), [...(linesBy.get(key(f.kind, f.component)) ?? []), f.line]);
  const countBy = new Map<string, number>();
  for (const t of shown) countBy.set(key(t.kind, t.component), (countBy.get(key(t.kind, t.component)) ?? 0) + t.found);
  const listed = components
    .filter((t) => linesBy.has(key(t.kind, t.component)))
    .sort((a, b) => (countBy.get(key(b.kind, b.component)) ?? 0) - (countBy.get(key(a.kind, a.component)) ?? 0) || a.component.localeCompare(b.component));
  const places = listed.slice(0, NAMED).map((t) => {
    const lines = [...new Set(linesBy.get(key(t.kind, t.component))!)].sort((a, b) => a - b);
    return said`${org(t.component)} (${linesOf(lines, lines.length < (countBy.get(key(t.kind, t.component)) ?? 0))})`;
  });
  const others = components.length - places.length;
  const placesText = places.length ? said` They include ${andList([...places, ...(others ? [said`${plural(others, "other class or trigger", "other classes and triggers")}`] : [])])}.` : said``;
  const worst = Math.min(...shown.map((t: RuleTally) => t.worst ?? 5));
  return {
    outcome: "issue",
    title: group.issueTitle,
    summary: said`PMD found ${plural(found, "place", "places")} in ${plural(components.length, "class or trigger", "classes and triggers")} of the ${checked} it read ${group.where}. ${group.why}${placesText}${notes}`,
    recommendation: group.recommendation,
    recommendationType: "change",
    severity: worst <= 2 ? group.raised : group.severity,
    // PMD reads the code, not what runs, and can't see a check made elsewhere.
    confidence: "medium",
    evidence,
    value: notSized(group.value),
    effort: { size: components.length > 10 ? "L" : "M" },
    owner: group.owner,
    dependencies: group.dependencies,
    segmentsChecked: segments,
    subjects: listed.map((t) => ({ type: subjectType(t.kind), label: t.component })),
    facts,
  };
}

export const LINT_RULES: Rule[] = LINT_GROUPS.map((group): Rule => ({
  id: group.id,
  version: 1,
  domain: DOMAIN,
  title: group.title,
  evaluate: (ctx) => evaluateGroup(group, ctx),
}));
