import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../index.ts";
import { technicalHealthModule } from "../technical-health/index.ts";
import { LINT_GROUPS, LINT_RULE_IDS } from "../technical-health/lint.ts";
import { overallScore, scoreDomains, UNSCORED_RULES } from "../scoring.ts";
import { codeLintExtractor, type CodeLintData } from "../../extract/code-lint.ts";
import { bound, notRun, rulesSeen, tallyOf, type LintFinding, type LintKind, type ToolRun } from "../../extract/lint/findings.ts";
import { PMD_NOT_INSTALLED, PMD_RULES, PMD_VERSION } from "../../extract/lint/pmd.ts";
import { NO_JAVASCRIPT } from "../../extract/lint/eslint.ts";
import { LINT_TOTALS_FILE, LintTotals, lintTotals } from "../../extract/lint/totals.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { spansIfCurrent } from "../../engine/org-words.ts";
import { compareWithBaseline, progressFigures, type ProgressSide } from "../../progress/compare.ts";
import { FIX_TABLE } from "../../fix/table.ts";
import { buildPlan, scorecard, verdictOf } from "../../narrative/select.ts";
import { planHours } from "../../digest/build.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { fakePmd } from "../../extract/__tests__/lint-fixtures/fixtures.ts";

// PMD's results: every rule kept as counts, six chosen groups shown in
// technical health, never scored, never counted twice with our own scan, and
// never read as new on a re-check of a run that didn't check them (DECISIONS.md 673).

const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const extracts = loadExtracts(dir);

type Hit = { rule: string; component: string; line: number; kind?: LintKind; severity?: number; suppressed?: true };
/** Names no class in the synthetic org has, so each can be looked for in what's kept. */
const PLANTED = ["ZephyrBillingPlanted", "QuokkaExportPlanted", "MarlinQueuePlanted", "OcelotSyncPlanted"];

/** A PMD run as runPmd would give it, from findings planted by hand. */
function ran(hits: Hit[], read = { classes: 40, triggers: 4, lines: 5_000 }): ToolRun {
  const all: LintFinding[] = hits.map((h) => ({ tool: "pmd", toolVersion: PMD_VERSION, rule: h.rule, category: null, severity: h.severity ?? 3, kind: h.kind ?? "class", component: h.component, line: h.line, column: 1, ...(h.suppressed ? { suppressed: h.suppressed } : {}) }));
  const kept = bound(all);
  return { status: "ran", tool: "pmd", toolVersion: PMD_VERSION, checked: read.classes + read.triggers, skipped: { tooLarge: 0, unparsable: 0, unreadable: 0, failed: 0, timeLimit: 0 }, found: kept.found, stored: kept.stored, components: kept.components, findings: kept.findings, rules: rulesSeen(kept.findings, PMD_RULES), tally: tallyOf(all), read, outsideRules: 0, malformed: 0, limits: [] };
}
const lint = (pmd: ToolRun): CodeLintData => ({ pmd, eslint: notRun("eslint", NO_JAVASCRIPT) });
const analyse = (data: CodeLintData | null, modules = ALL_MODULES): Finding[] =>
  analyseExtracts(data ? new Map([...extracts, [codeLintExtractor.id, data]]) : extracts, { modules, answers: loadAnswers() }).findings;
const only = (findings: Finding[], ruleId: string): Finding => {
  const found = findings.filter((f) => f.ruleId === ruleId);
  assert.equal(found.length, 1, ruleId);
  return found[0]!;
};

describe("the six groups shown (decision 673)", () => {
  test("are these rules, from our ruleset, each in one group; the other 17 are kept and not shown", () => {
    assert.deepEqual(Object.fromEntries(LINT_GROUPS.map((g) => [g.id, [...g.rules]])), {
      "tech.lint-data-access": ["ApexCRUDViolation", "ApexSharingViolations"],
      "tech.lint-limits": ["OperationWithLimitsInLoop", "OperationWithHighCostInLoop", "AvoidNonRestrictiveQueries"],
      "tech.lint-injection": ["ApexSOQLInjection", "ApexOpenRedirect", "ApexBadCrypto", "ApexXSSFromEscapeFalse", "ApexXSSFromURLParam", "ApexCSRF", "ApexDangerousMethods", "ApexInsecureEndpoint"],
      "tech.lint-tests": ["ApexUnitTestClassShouldHaveAsserts", "ApexUnitTestShouldNotUseSeeAllDataTrue"],
      "tech.lint-swallowed-errors": ["EmptyCatchBlock"],
      "tech.lint-complexity": ["CognitiveComplexity", "CyclomaticComplexity", "NcssCount", "AvoidDeeplyNestedIfStmts"],
    });
    const shown = LINT_GROUPS.flatMap((g) => g.rules);
    assert.equal(new Set(shown).size, shown.length);
    for (const r of shown) assert.ok(Object.hasOwn(PMD_RULES, r), r);
    assert.equal(Object.keys(PMD_RULES).filter((r) => !shown.includes(r)).length, 17);
    // Unscored, and each with its hours and prompt.
    assert.deepEqual([...UNSCORED_RULES].sort(), [...LINT_RULE_IDS].sort());
    for (const id of LINT_RULE_IDS) assert.ok(FIX_TABLE[id], id);
    assert.deepEqual(technicalHealthModule.rules.slice(-6).map((r) => r.id), [...LINT_RULE_IDS]);
  });

  test("without the step in the run (a scan, a pulse, the command line) they give no finding at all", () => {
    const findings = analyse(null);
    assert.ok(!findings.some((f) => LINT_RULE_IDS.includes(f.ruleId)));
  });

  test("PMD didn't run: each is not measurable in our words, never a pass; PMD's reason is on the evidence for the consultant", () => {
    const findings = analyse(lint(notRun("pmd", PMD_NOT_INSTALLED)));
    for (const id of LINT_RULE_IDS) {
      const f = only(findings, id);
      assert.equal(f.status, "not_measurable", id);
      assert.equal(f.notMeasurableReason, "We couldn't check this, because our code checker didn't run in this health check.");
      assert.equal(f.evidence[0]!.note, `PMD: ${PMD_NOT_INSTALLED}`);
    }
  });

  test("PMD ran but read nothing (every file failed to parse): not measurable, never a pass", () => {
    const findings = analyse(lint(ran([], { classes: 0, triggers: 0, lines: 0 })));
    for (const id of LINT_RULE_IDS) assert.equal(only(findings, id).notMeasurableReason, "We couldn't check this, because our code checker ran but couldn't read any of the code.", id);
  });

  test("classes unread but triggers read: the tests group, which reads classes only, is not measurable; the others measure the triggers and say what's left out", () => {
    const run = ran([{ rule: "EmptyCatchBlock", component: PLANTED[0]!, line: 2, kind: "trigger" }], { classes: 0, triggers: 4, lines: 80 });
    if (run.status === "ran") run.unread = ["class"];
    const findings = analyse(lint(run));
    assert.equal(only(findings, "tech.lint-tests").notMeasurableReason, "We couldn't check this, because our code checker couldn't read the org's Apex classes.");
    const caught = only(findings, "tech.lint-swallowed-errors");
    assert.equal(caught.status, "issue");
    assert.ok(caught.evidence[0]!.exclusions.includes("Apex classes, which couldn't be read"));
    // Triggers unread instead: tests are still measured.
    const other = ran([], { classes: 10, triggers: 0, lines: 80 });
    if (other.status === "ran") other.unread = ["trigger"];
    assert.equal(only(analyse(lint(other)), "tech.lint-tests").status, "passed");
  });

  test("PMD found nothing of a group: a pass, segmented by class or trigger", () => {
    const findings = analyse(lint(ran([])));
    for (const id of LINT_RULE_IDS) {
      const f = only(findings, id);
      assert.equal(f.status, "passed", id);
      assert.equal(f.summary, "Our code checker found nothing of this kind in the 44 classes and triggers it read.");
      assert.deepEqual(f.segmentsChecked, [{ dimension: "class or trigger", segments: 44 }]);
    }
  });

  test("an issue: the uncapped count, the classes named with their lines, the suppressed said and not raised, and never PMD's message or code", () => {
    const hits: Hit[] = [
      { rule: "ApexCRUDViolation", component: PLANTED[0]!, line: 12 },
      { rule: "ApexCRUDViolation", component: PLANTED[0]!, line: 40 },
      { rule: "ApexSharingViolations", component: PLANTED[0]!, line: 1 },
      { rule: "ApexCRUDViolation", component: PLANTED[1]!, line: 7, kind: "trigger" },
      { rule: "ApexCRUDViolation", component: PLANTED[2]!, line: 9, suppressed: true },
      { rule: "EmptyCatchBlock", component: PLANTED[3]!, line: 3 },
    ];
    const f = only(analyse(lint(ran(hits))), "tech.lint-data-access");
    assert.equal(f.status, "issue");
    assert.equal(f.title, "Some code may read or change data without checking access");
    assert.equal(
      f.summary,
      `Our code checker found 4 places in 2 classes and triggers of the 44 it read where the code reads or changes records without checking what the running user may see and do, or without saying whether sharing applies. Code like this can show or change data a user shouldn't reach, for example through a page or an integration. They include ${PLANTED[0]} (lines 1, 12 and 40) and ${PLANTED[1]} (line 7). 1 more was marked to be ignored in the code.`,
    );
    assert.deepEqual(f.subjects, [{ type: "apex class", label: PLANTED[0] }, { type: "apex trigger", label: PLANTED[1] }]);
    assert.equal(f.severity, "medium");
    assert.equal(f.confidence, "medium");
    assert.deepEqual([f.facts.found, f.facts.components, f.facts.suppressed, f.facts.ApexCRUDViolation, f.facts.ApexSharingViolations, f.facts.readClasses, f.facts.readTriggers], [4, 2, 1, 3, 1, 40, 4]);
    for (const text of [f.title, f.summary, f.recommendation, ...f.dependencies]) assert.doesNotMatch(text, /\bPMD\b/);
    // The suppressed class isn't raised.
    assert.ok(!JSON.stringify(f.subjects).includes(PLANTED[2]!));
    // Each class's name in the summary is the org's, marked.
    const spans = spansIfCurrent(f.summary, f.orgWords?.summary)!;
    assert.deepEqual(spans.map((s) => f.summary.slice(s.start, s.end)), [PLANTED[0], PLANTED[1]]);
    assert.ok(f.fixPrompt && f.effortHours);
  });

  test("every sentence of every group is marked, in an issue, a pass and not measurable: the org's words only in spans", () => {
    const hits: Hit[] = LINT_GROUPS.map((g, i) => ({ rule: g.rules[0]!, component: PLANTED[i % PLANTED.length]!, line: 3 }));
    const unread = ran([], { classes: 0, triggers: 4, lines: 80 });
    if (unread.status === "ran") unread.unread = ["class"];
    const variants = [
      analyse(lint(ran(hits))),
      analyse(lint(ran([]))),
      analyse(lint(ran([{ rule: "EmptyCatchBlock", component: PLANTED[0]!, line: 1, suppressed: true }]))),
      // Every not measurable variant (fix round 1).
      analyse(lint(notRun("pmd", PMD_NOT_INSTALLED))),
      analyse(lint(ran([], { classes: 0, triggers: 0, lines: 0 }))),
      analyse(lint(unread)),
    ];
    const statuses = new Set<string>();
    for (const findings of variants) {
      for (const f of findings.filter((x) => LINT_RULE_IDS.includes(x.ruleId))) {
        statuses.add(f.status);
        // As the recording-based check holds every other rule: the value's reason and, on an issue, the hours' basis are marked too (decision 663).
        assert.ok(f.orgWords?.valueReason, `${f.ruleId} value reason`);
        if (f.status === "issue") {
          assert.ok(f.effortHours && f.orgWords?.hoursBasis, `${f.ruleId} hours basis`);
          assert.ok(f.fixPrompt && f.orgWords?.fixPrompt, `${f.ruleId} fix prompt`);
        }
        if (f.status === "not_measurable") {
          const spans = spansIfCurrent(f.notMeasurableReason!, f.orgWords?.notMeasurableReason);
          assert.deepEqual(spans, [], `${f.ruleId} not measurable reason`);
        }
        for (const k of ["title", "summary", "recommendation"] as const) {
          const spans = spansIfCurrent(f[k], f.orgWords?.[k]);
          assert.ok(spans, `${f.ruleId} ${k}`);
          for (const s of spans) assert.ok(PLANTED.includes(f[k].slice(s.start, s.end)), `${f.ruleId} ${k}`);
          for (const name of PLANTED) if (f[k].includes(name)) assert.ok(spans.some((s) => f[k].slice(s.start, s.end) === name), `${f.ruleId} ${k} ${name}`);
        }
      }
    }
    assert.deepEqual([...statuses].sort(), ["issue", "not_measurable", "passed"]);
  });

  test("only suppressed findings: a pass that says how many were marked to be ignored", () => {
    const f = only(analyse(lint(ran([{ rule: "EmptyCatchBlock", component: PLANTED[0]!, line: 3, suppressed: true }]))), "tech.lint-swallowed-errors");
    assert.equal(f.status, "passed");
    assert.equal(f.summary, "Our code checker found nothing of this kind in the 44 classes and triggers it read. 1 more was marked to be ignored in the code.");
  });

  test("severity: the group's, raised a step only when PMD's priority 1 or 2 is in at least 3 classes and triggers; complexity never; never critical", () => {
    // In three components: raised.
    const sev = (rule: string, severity: number, id: string) => only(analyse(lint(ran(PLANTED.slice(0, 3).map((component) => ({ rule, component, line: 2, severity }))))), id).severity;
    assert.equal(sev("ApexSOQLInjection", 3, "tech.lint-injection"), "medium");
    assert.equal(sev("ApexSOQLInjection", 2, "tech.lint-injection"), "high");
    assert.equal(sev("ApexCRUDViolation", 1, "tech.lint-data-access"), "high");
    assert.equal(sev("OperationWithHighCostInLoop", 2, "tech.lint-limits"), "high");
    assert.equal(sev("EmptyCatchBlock", 3, "tech.lint-swallowed-errors"), "low");
    assert.equal(sev("EmptyCatchBlock", 2, "tech.lint-swallowed-errors"), "medium");
    assert.equal(sev("ApexUnitTestClassShouldHaveAsserts", 2, "tech.lint-tests"), "medium");
    assert.equal(sev("NcssCount", 1, "tech.lint-complexity"), "low");
    // One finding, or two components, at priority 1 among many: the group's own severity.
    const one = only(analyse(lint(ran([{ rule: "ApexCRUDViolation", component: PLANTED[0]!, line: 2, severity: 1 }, ...[1, 2, 3].map((n) => ({ rule: "ApexCRUDViolation", component: PLANTED[n]!, line: 2 }))]))), "tech.lint-data-access");
    assert.equal(one.severity, "medium");
    const two = only(analyse(lint(ran(PLANTED.slice(0, 2).map((component) => ({ rule: "ApexSOQLInjection", component, line: 2, severity: 2 }))))), "tech.lint-injection");
    assert.equal(two.severity, "medium");
    // Fewer than three read: every one read must have it.
    const few = (n: number) => only(analyse(lint(ran(PLANTED.slice(0, n).map((component) => ({ rule: "ApexCRUDViolation", component, line: 2, severity: 1 })), { classes: 2, triggers: 0, lines: 50 }))), "tech.lint-data-access").severity;
    assert.equal(few(2), "high");
    assert.equal(few(1), "medium");
  });

  test("many lines and many classes: five line numbers a class, three classes named, the rest counted", () => {
    const hits: Hit[] = [
      ...[1, 2, 3, 4, 5, 6, 7].map((line) => ({ rule: "NcssCount", component: PLANTED[0]!, line })),
      { rule: "NcssCount", component: PLANTED[1]!, line: 4 },
      { rule: "NcssCount", component: PLANTED[2]!, line: 4 },
      { rule: "NcssCount", component: PLANTED[3]!, line: 4 },
    ];
    const f = only(analyse(lint(ran(hits))), "tech.lint-complexity");
    assert.match(f.summary, new RegExp(`They include ${PLANTED[0]} \\(lines 1, 2, 3, 4, 5 and others\\), ${PLANTED[2]} \\(line 4\\), ${PLANTED[3]} \\(line 4\\) and 1 other class or trigger\\.`));
    assert.equal(f.subjects.length, 4);
  });
});

describe("never counted twice with our own scan (decision 673)", () => {
  // The synthetic org's own scan names LeadRouter for a query in a loop,
  // OpportunityHandlerTest for no assertion, LeadRouterTest for SeeAllData, and
  // LegacySync for an address written into the code.
  test("PMD's loop finding in a class our limit check names is counted apart and said, never raised; one elsewhere is", () => {
    const f = only(analyse(lint(ran([{ rule: "OperationWithLimitsInLoop", component: "LeadRouter", line: 5 }, { rule: "OperationWithLimitsInLoop", component: "LeadRouter", line: 9 }, { rule: "OperationWithLimitsInLoop", component: PLANTED[0]!, line: 3 }]))), "tech.lint-limits");
    assert.equal(f.status, "issue");
    assert.equal(f.facts.found, 1);
    assert.equal(f.facts.alreadyListed, 2);
    assert.deepEqual(f.subjects.map((s) => s.label), [PLANTED[0]]);
    assert.match(f.summary, /Our code checker found 1 place in 1 class or trigger .* 2 more are in classes our own check of queries and saves inside loops already names, for a related problem\.$/);
    // Our own check still names it, once.
    assert.ok(only(analyse(null), "tech.governor-limit-risk").subjects.some((s) => s.label === "LeadRouter"));
  });

  test("only what our own check names: other rules in the same class are shown", () => {
    const f = only(analyse(lint(ran([{ rule: "OperationWithLimitsInLoop", component: "LeadRouter", line: 5 }, { rule: "AvoidNonRestrictiveQueries", component: "LeadRouter", line: 6 }]))), "tech.lint-limits");
    assert.deepEqual([f.status, f.facts.found, f.facts.alreadyListed], ["issue", 1, 1]);
    assert.equal(f.facts.AvoidNonRestrictiveQueries, 1);
  });

  test("tests our test check names for the same reason are counted apart; http in a class our endpoint check names is raised, a different problem", () => {
    const tests = only(analyse(lint(ran([
      { rule: "ApexUnitTestClassShouldHaveAsserts", component: "OpportunityHandlerTest", line: 4 },
      { rule: "ApexUnitTestShouldNotUseSeeAllDataTrue", component: "LeadRouterTest", line: 2 },
      // LeadRouterTest asserts, so a method without one there is PMD's own news.
      { rule: "ApexUnitTestClassShouldHaveAsserts", component: "LeadRouterTest", line: 8 },
    ]))), "tech.lint-tests");
    assert.deepEqual([tests.facts.found, tests.facts.alreadyListed], [1, 2]);
    assert.deepEqual(tests.subjects.map((s) => s.label), ["LeadRouterTest"]);
    assert.match(tests.summary, /2 more are in classes our own check of test classes already names, for a related problem\./);
    const one = only(analyse(lint(ran([{ rule: "ApexUnitTestClassShouldHaveAsserts", component: "OpportunityHandlerTest", line: 4 }]))), "tech.lint-tests");
    assert.match(one.summary, /1 more is in a class our own check of test classes already names, for a related problem\.$/);
    const http = only(analyse(lint(ran([{ rule: "ApexInsecureEndpoint", component: "LegacySync", line: 3 }]))), "tech.lint-injection");
    assert.deepEqual([http.status, http.facts.alreadyListed, http.subjects.map((x) => x.label)], ["issue", 0, ["LegacySync"]]);
  });
});

describe("scores and history don't move because we started measuring (decision 673)", () => {
  const issues: Hit[] = LINT_GROUPS.map((g, i) => ({ rule: g.rules[0]!, component: PLANTED[i % PLANTED.length]!, line: 3, severity: 1 }));
  const withLint = analyse(lint(ran(issues)));
  const without = analyse(null);
  const domains = [...new Set(ALL_MODULES.map((m) => m.id))];

  test("every area's score and the overall score are the same with the six issues as without them", () => {
    assert.equal(withLint.filter((f) => LINT_RULE_IDS.includes(f.ruleId) && f.status === "issue").length, 6);
    const a = scoreDomains(domains, withLint);
    const b = scoreDomains(domains, without);
    assert.deepEqual(a, b);
    assert.deepEqual(overallScore(a, withLint), overallScore(b, without));
    // Not measurable, too: the same as never run.
    const nm = analyse(lint(notRun("pmd", PMD_NOT_INSTALLED)));
    assert.deepEqual(scoreDomains(domains, nm), b);
  });

  const side = (runId: string, findings: Finding[]): ProgressSide => {
    const scores = scoreDomains(domains, findings);
    return { runId, asOf: AS_OF, findings, scores, overall: overallScore(scores, findings), value: { total: { expected: 0 } } as ProgressSide["value"], limitsReadAt: null };
  };

  test("a re-check of a run that never had PMD reads them as checked for the first time, not new", () => {
    const p = compareWithBaseline(side("before", without), side("now", withLint));
    assert.deepEqual(p.newlyMeasured!.map((c) => c.ruleId).sort(), [...LINT_RULE_IDS].sort());
    assert.ok(!p.added.some((c) => LINT_RULE_IDS.includes(c.ruleId)));
    assert.deepEqual(p.added, compareWithBaseline(side("before", without), side("now", without)).added);
    assert.deepEqual(p.score, { before: p.score.before, after: p.score.before });
    assert.ok(progressFigures(p).includes(6));
  });

  test("against a run where PMD didn't run (not measurable), the same", () => {
    const p = compareWithBaseline(side("before", analyse(lint(notRun("pmd", PMD_NOT_INSTALLED)))), side("now", withLint));
    assert.equal(p.newlyMeasured!.length, 6);
    assert.ok(!p.added.some((c) => LINT_RULE_IDS.includes(c.ruleId)));
  });

  test("against a run where they passed, an issue is new, as for any rule", () => {
    const p = compareWithBaseline(side("before", analyse(lint(ran([])))), side("now", withLint));
    assert.deepEqual(p.added.filter((c) => LINT_RULE_IDS.includes(c.ruleId)).length, 6);
    assert.equal(p.newlyMeasured, undefined);
  });

  test("a later run without the step (a pulse) says nothing of them: not fixed, not can't tell", () => {
    const p = compareWithBaseline(side("before", withLint), side("now", without));
    for (const list of [p.fixed, p.remaining, p.added, p.unknown]) assert.ok(!list.some((c) => LINT_RULE_IDS.includes(c.ruleId)));
  });

  // The reviewer's probe (fix round 1): a pass is fixed only when as much was read.
  const before = analyse(lint(ran(issues.map((h) => ({ ...h, severity: 3 })), { classes: 40, triggers: 4, lines: 5_000 })));
  const fixedOf = (now: Finding[]) => compareWithBaseline(side("before", before), side("now", now));
  const lintIn = (list: Array<{ ruleId: string }>) => list.filter((c) => LINT_RULE_IDS.includes(c.ruleId)).map((c) => c.ruleId).sort();

  test("PMD read nothing on the re-check: never fixed, can't tell", () => {
    const p = fixedOf(analyse(lint(ran([], { classes: 0, triggers: 0, lines: 0 }))));
    assert.deepEqual(lintIn(p.fixed), []);
    assert.deepEqual(lintIn(p.unknown), [...LINT_RULE_IDS].sort());
  });

  test("PMD read fewer classes, or no triggers where it read some before: can't tell, not fixed", () => {
    for (const read of [{ classes: 39, triggers: 4, lines: 5_000 }, { classes: 40, triggers: 0, lines: 5_000 }]) {
      const p = fixedOf(analyse(lint(ran([], read))));
      assert.deepEqual(lintIn(p.fixed), [], JSON.stringify(read));
      assert.deepEqual(lintIn(p.unknown), [...LINT_RULE_IDS].sort());
    }
  });

  test("as much read or more, and a pass: fixed", () => {
    const p = fixedOf(analyse(lint(ran([], { classes: 41, triggers: 4, lines: 5_100 }))));
    assert.deepEqual(lintIn(p.fixed), [...LINT_RULE_IDS].sort());
  });

  test("a finding now under our own check (deduped to a pass) is can't tell, never fixed", () => {
    // Now our own scan names LeadRouter, so PMD's loop finding there is counted apart and the group passes.
    const then = analyse(lint(ran([{ rule: "OperationWithLimitsInLoop", component: "LeadRouter", line: 5 }])), [technicalHealthModule]);
    // The earlier run's own scan didn't name LeadRouter: planted by taking its loop away there.
    const apex = structuredClone(extracts.get("apex")) as { classes: { data: Array<{ Name: string; scan: { queriesInLoops: number; dmlInLoops: number } }> } };
    for (const c of apex.classes.data) if (c.Name === "LeadRouter") c.scan = { ...c.scan, queriesInLoops: 0, dmlInLoops: 0 };
    const earlier = analyseExtracts(new Map([...extracts, ["apex", apex], [codeLintExtractor.id, lint(ran([{ rule: "OperationWithLimitsInLoop", component: "LeadRouter", line: 5 }]))]]), { modules: [technicalHealthModule], answers: loadAnswers() }).findings;
    assert.equal(only(earlier, "tech.lint-limits").status, "issue");
    assert.equal(only(then, "tech.lint-limits").status, "passed");
    const p = compareWithBaseline(side("before", earlier), side("now", then));
    assert.ok(!p.fixed.some((c) => c.ruleId === "tech.lint-limits"));
    assert.ok(p.unknown.some((c) => c.ruleId === "tech.lint-limits"));
  });

  test("the plan's hours total, the verdict and the scorecard are the same with the six raised as without", () => {
    const raised = analyse(lint(ran(LINT_GROUPS.flatMap((g) => PLANTED.map((component) => ({ rule: g.rules[0]!, component, line: 3, severity: 1 }))))));
    const clean = analyse(lint(ran([])));
    assert.equal(raised.filter((f) => LINT_RULE_IDS.includes(f.ruleId) && f.status === "issue" && f.effortHours).length, 6);
    assert.ok(raised.some((f) => LINT_RULE_IDS.includes(f.ruleId) && f.severity === "high"));
    assert.deepEqual(verdictOf(raised), verdictOf(clean));
    const scores = scoreDomains(domains, clean);
    const card = (fs: Finding[]) => scorecard(fs, scores).map(({ issues: _i, ...rest }) => rest);
    assert.deepEqual(card(raised), card(clean));
    const withSix = planHours(buildPlan(raised))!;
    const withoutSix = planHours(buildPlan(clean))!;
    assert.deepEqual([withSix.low, withSix.high, withSix.findingIds, withSix.partialFindingIds, withSix.withoutHoursFindingIds], [withoutSix.low, withoutSix.high, withoutSix.findingIds, withoutSix.partialFindingIds, withoutSix.withoutHoursFindingIds]);
    // Each keeps its own hours, listed apart with one fixed sentence.
    assert.equal(withSix.notTotalledFindingIds!.length, 6);
    assert.equal(withSix.note, "Findings from our code checker are listed with their own hours and are not included in this total yet.");
    assert.equal(withoutSix.notTotalledFindingIds, undefined);
  });

  test("every other rule: absent from the earlier run, it is checked for the first time too (decision 779); listed there as run with nothing found, new", () => {
    const before = without.filter((f) => f.ruleId !== "tech.governor-limit-risk");
    const p = compareWithBaseline(side("before", before), side("now", without));
    assert.ok(p.newlyMeasured?.some((c) => c.ruleId === "tech.governor-limit-risk"));
    assert.ok(!p.added.some((c) => c.ruleId === "tech.governor-limit-risk"));
    const listed = compareWithBaseline({ ...side("before", before), rulesRun: [...new Set(without.map((f) => f.ruleId))] }, side("now", without));
    assert.ok(listed.added.some((c) => c.ruleId === "tech.governor-limit-risk"));
    assert.equal(listed.newlyMeasured, undefined);
  });
});

describe("every rule's totals, kept (decision 673)", () => {
  test("all 37 rules, fired or not, counts only: findings, suppressed and classes, with what PMD read", () => {
    const totals = lintTotals(lint(ran([
      { rule: "ApexCRUDViolation", component: PLANTED[0]!, line: 1 },
      { rule: "ApexCRUDViolation", component: PLANTED[0]!, line: 2 },
      { rule: "ApexCRUDViolation", component: PLANTED[1]!, line: 2, kind: "trigger" },
      { rule: "ApexCRUDViolation", component: PLANTED[2]!, line: 2, suppressed: true },
      ...Array.from({ length: 70 }, (_, i) => ({ rule: "UnusedLocalVariable", component: PLANTED[3]!, line: i + 1 })),
    ])));
    assert.equal(totals.pmd.status, "ran");
    if (totals.pmd.status !== "ran") return;
    assert.deepEqual(Object.keys(totals.pmd.rules), Object.keys(PMD_RULES));
    assert.deepEqual(totals.pmd.rules.ApexCRUDViolation, { found: 3, suppressed: 1, components: 2 });
    // Uncapped: the step keeps 50 a component, the totals every one.
    assert.deepEqual(totals.pmd.rules.UnusedLocalVariable, { found: 70, suppressed: 0, components: 1 });
    assert.deepEqual(totals.pmd.rules.ApexBadCrypto, { found: 0, suppressed: 0, components: 0 });
    assert.deepEqual([totals.pmd.classes, totals.pmd.triggers, totals.pmd.lines], [40, 4, 5_000]);
    const text = JSON.stringify(totals);
    for (const name of PLANTED) assert.ok(!text.includes(name), name);
  });

  test("PMD didn't run, or a step from before the tallies: not run, with no reason", () => {
    assert.deepEqual(lintTotals(lint(notRun("pmd", PMD_NOT_INSTALLED))), { version: 1, pmd: { status: "not_run" } });
    const old = ran([]);
    if (old.status === "ran") delete old.tally;
    assert.deepEqual(lintTotals(lint(old)), { version: 1, pmd: { status: "not_run" } });
  });

  const options = (outDir: string, extra = {}) => ({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport: new Simulator(), log: () => {}, ...extra });

  test("a run with the step writes lint-totals.json beside the findings, with no class name; one without writes none", async () => {
    const base = tempDir();
    const temp = join(base, "tmp");
    const { mkdirSync } = await import("node:fs");
    mkdirSync(temp);
    const on = await runHealthCheck(options(tempDir(), { codeLint: { pmd: fakePmd(base), pmdOptions: { tempBase: temp } } }));
    const text = readFileSync(join(on.dir, LINT_TOTALS_FILE), "utf8");
    const totals = LintTotals.parse(JSON.parse(text));
    assert.equal(totals.pmd.status, "ran");
    const apex = JSON.parse(readFileSync(join(on.dir, "checkpoints", "code-lint.json"), "utf8")) as CodeLintData;
    assert.equal(apex.pmd.status, "ran");
    for (const c of apex.pmd.status === "ran" ? apex.pmd.components : []) assert.ok(!text.includes(c.component), c.component);
    if (totals.pmd.status === "ran") assert.ok(totals.pmd.rules.ApexCRUDViolation!.found > 0);
    // Shown in the report, and scored as without the step.
    const off = await runHealthCheck(options(tempDir()));
    assert.ok(!existsSync(join(off.dir, LINT_TOTALS_FILE)));
    assert.equal(readFileSync(join(on.dir, "scores.json"), "utf8"), readFileSync(join(off.dir, "scores.json"), "utf8"));
    const pulse = await runHealthCheck(options(tempDir(), { product: "pulse" }));
    assert.ok(!existsSync(join(pulse.dir, LINT_TOTALS_FILE)));
    assert.ok(!(JSON.parse(readFileSync(join(pulse.dir, "findings.json"), "utf8")) as Finding[]).some((f) => LINT_RULE_IDS.includes(f.ruleId)));
  });

  test("a step that fails leaves the six not measurable, saying the step failed, and its totals not run", async () => {
    const throwing = { pmd: null } as { pmd: null; pmdMissing?: string };
    Object.defineProperty(throwing, "pmdMissing", { get: () => { throw new Error("unexpected"); } });
    const result = await runHealthCheck(options(tempDir(), { codeLint: throwing, modules: [technicalHealthModule] }));
    for (const id of LINT_RULE_IDS) {
      const f = only(result.output!.findings, id);
      assert.equal(f.notMeasurableReason, "We couldn't check this, because our code checker didn't run in this health check.");
      assert.match(f.evidence[0]!.note ?? "", /PMD's step failed, so nothing it found was kept\./);
    }
    assert.deepEqual(JSON.parse(readFileSync(join(result.dir, LINT_TOTALS_FILE), "utf8")), { version: 1, pmd: { status: "not_run" } });
  });
});
