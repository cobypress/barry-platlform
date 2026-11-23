import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { buildDigest } from "../build.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import { LINT_GROUPS, LINT_RULE_IDS } from "../../domains/technical-health/lint.ts";
import { codeLintExtractor } from "../../extract/code-lint.ts";
import { bound, notRun, rulesSeen, tallyOf, type LintFinding, type ToolRun } from "../../extract/lint/findings.ts";
import { PMD_NOT_INSTALLED, PMD_RULES, PMD_VERSION } from "../../extract/lint/pmd.ts";
import { NO_JAVASCRIPT } from "../../extract/lint/eslint.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { RunDigest } from "../../schemas/digest.ts";
import type { Finding } from "../../schemas/index.ts";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";

// The digest carries what the code checker read on its six groups' issues and
// passes, so the console's re-check can call a pass fixed only when as much was
// read and nothing went to our own scan's checks (DECISIONS.md 673).

const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const loaded = loadRun(dir);
const extracts = loadExtracts(dir);

function ran(hits: Array<{ rule: string; component: string }>, read = { classes: 37, triggers: 5, lines: 4_000 }): ToolRun {
  const all: LintFinding[] = hits.map((h) => ({ tool: "pmd", toolVersion: PMD_VERSION, rule: h.rule, category: null, severity: 3, kind: "class", component: h.component, line: 3, column: 1 }));
  const kept = bound(all);
  return { status: "ran", tool: "pmd", toolVersion: PMD_VERSION, checked: read.classes + read.triggers, skipped: { tooLarge: 0, unparsable: 0, unreadable: 0, failed: 0, timeLimit: 0 }, found: kept.found, stored: kept.stored, components: kept.components, findings: kept.findings, rules: rulesSeen(kept.findings, PMD_RULES), tally: tallyOf(all), read, outsideRules: 0, malformed: 0, limits: [] };
}
const lintFindings = (pmd: ToolRun): Finding[] =>
  analyseExtracts(new Map([...extracts, [codeLintExtractor.id, { pmd, eslint: notRun("eslint", NO_JAVASCRIPT) }]]), { modules: ALL_MODULES, answers: loadAnswers() }).findings.filter((f) => LINT_RULE_IDS.includes(f.ruleId));
const digestOf = (extra: Finding[]) => buildDigest({ report: buildReport({ ...loaded.input, findings: [...loaded.input.findings, ...extra] }), pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-23T09:00:00.000Z" });

describe("what the code checker read, in the digest (decision 673)", () => {
  test("on an issue of each of the six: the counts from its facts", () => {
    // LeadRouter is named by our own limit check, so one PMD loop finding there goes to it.
    const issues = lintFindings(ran([...LINT_GROUPS.map((g, i) => ({ rule: g.rules[0]!, component: `PlantedClass${i}` })), { rule: "OperationWithLimitsInLoop", component: "LeadRouter" }]));
    const d = digestOf(issues);
    for (const id of LINT_RULE_IDS) {
      const f = d.findings.find((x) => x.ruleId === id)!;
      assert.equal(f.status, "issue", id);
      assert.deepEqual(f.checkerRead, { readClasses: 37, readTriggers: 5, alreadyListed: id === "tech.lint-limits" ? 1 : 0 }, id);
    }
  });

  test("on a pass of each of the six, too", () => {
    const d = digestOf(lintFindings(ran([], { classes: 12, triggers: 0, lines: 300 })));
    for (const id of LINT_RULE_IDS) {
      const f = d.findings.find((x) => x.ruleId === id)!;
      assert.equal(f.status, "passed", id);
      assert.deepEqual(f.checkerRead, { readClasses: 12, readTriggers: 0, alreadyListed: 0 }, id);
    }
  });

  test("absent on every other finding, on not measurable ones, and when the facts lack the counts", () => {
    const plain = digestOf([]);
    assert.ok(plain.findings.length > 50);
    assert.ok(plain.findings.every((f) => f.checkerRead === undefined));
    const nm = digestOf(lintFindings(notRun("pmd", PMD_NOT_INSTALLED)));
    for (const id of LINT_RULE_IDS) assert.equal(nm.findings.find((x) => x.ruleId === id)!.checkerRead, undefined, id);
    const stripped = lintFindings(ran([])).map((f) => ({ ...f, facts: Object.fromEntries(Object.entries(f.facts).filter(([k]) => k !== "readTriggers")) }));
    for (const f of digestOf(stripped).findings.filter((x) => LINT_RULE_IDS.includes(x.ruleId))) assert.equal(f.checkerRead, undefined, f.ruleId);
  });

  test("an older digest without it still parses, and the schema refuses a count that isn't a whole number from 0", () => {
    const d = digestOf(lintFindings(ran([])));
    const old = JSON.parse(JSON.stringify(d)) as { findings: Array<Record<string, unknown>> };
    for (const f of old.findings) delete f.checkerRead;
    assert.ok(RunDigest.safeParse(old).success);
    for (const bad of [-1, 1.5, "3"]) {
      const broken = JSON.parse(JSON.stringify(d)) as { findings: Array<{ checkerRead?: Record<string, unknown> }> };
      broken.findings.find((f) => f.checkerRead)!.checkerRead!.readClasses = bad;
      assert.ok(!RunDigest.safeParse(broken).success, String(bad));
    }
  });
});
