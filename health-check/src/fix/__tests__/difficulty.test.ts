import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildDigest, quickFixesOf } from "../../digest/build.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { buildPlan } from "../../narrative/select.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { RunDigest } from "../../schemas/digest.ts";
import { Finding, type NumberOverride } from "../../schemas/index.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { buildScaleOrg, FIVE_HUNDRED_USERS, scaled } from "../../testing/scale-org.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { DIFFICULTY_BASIS, DIFFICULTY_ORDER, DIFFICULTY_WORDS, difficultyCovered, difficultyFor, difficultyOf, NOT_ESTIMATED } from "../difficulty.ts";
import { effortHours, MEDIUM_MAX_HOURS, SMALL_MAX_HOURS, timeBandOf } from "../hours.ts";
import { FIX_TABLE, NEVER_RAISES } from "../table.ts";

// Difficulty beside severity (DECISIONS.md 714): one rule, from the finding's own
// hours and its rule's fix complexity, never lower than the hours' band, and
// nothing at all where there are no hours.

const rules = ALL_MODULES.flatMap((m) => m.rules);
const readFindings = (dir: string): Finding[] => (JSON.parse(readFileSync(join(dir, "findings.json"), "utf8")) as unknown[]).map((f) => Finding.parse(f));
const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
const loaded = loadRun(dir);
const recorded = readFindings(dir);
const generated = readFindings(
  (await runHealthCheck({ provider: syntheticProvider, connectedUser: CONNECTED_USER, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ records: buildScaleOrg(scaled(FIVE_HUNDRED_USERS, 0.005)), pageSize: 2000 }) })).dir,
);
const reportWith = (overrides: NumberOverride[] = []): BuiltReport => buildReport({ ...loaded.input, overrides });
const rank = (d: string | null): number => (d === null ? -1 : DIFFICULTY_ORDER.indexOf(d as never));
const BAND_RANK = { small: 0, medium: 1, large: 2 } as const;
const WORDS = Object.values(DIFFICULTY_WORDS);

describe("the one rule, at each boundary", () => {
  test("4 hours is a quick fix unless the fix is complex; just over 4, or complex, needs planning; 16 needs planning; just over 16 is a project", () => {
    assert.equal(SMALL_MAX_HOURS, 4);
    assert.equal(MEDIUM_MAX_HOURS, 16);
    assert.equal(difficultyOf({ high: 1 }, "simple"), "quick_fix");
    assert.equal(difficultyOf({ high: 4 }, "simple"), "quick_fix");
    assert.equal(difficultyOf({ high: 4 }, "moderate"), "quick_fix");
    assert.equal(difficultyOf({ high: 1 }, "complex"), "needs_planning");
    assert.equal(difficultyOf({ high: 4 }, "complex"), "needs_planning");
    assert.equal(difficultyOf({ high: 5 }, "simple"), "needs_planning");
    assert.equal(difficultyOf({ high: 16 }, "simple"), "needs_planning");
    assert.equal(difficultyOf({ high: 16 }, "complex"), "needs_planning");
    assert.equal(difficultyOf({ high: 17 }, "simple"), "project");
    assert.equal(difficultyOf({ high: 40 }, "simple"), "project");
    // Hours that cover only part of the work are a project, whatever the capped figure.
    assert.equal(difficultyOf({ high: 2, partial: true }, "simple"), "project");
  });

  test("no hours, no difficulty: never a guess", () => {
    assert.equal(difficultyOf(undefined, "simple"), null);
    assert.equal(difficultyOf(undefined, undefined), null);
  });

  test("never lower than the hours' own band, for every rule at every count from 1 to 400: 40 hours is never a quick fix", () => {
    for (const [id, rule] of Object.entries(FIX_TABLE)) {
      for (let n = 1; n <= (rule.hours.kind === "fixed" ? 1 : 400); n++) {
        const facts = rule.hours.kind === "fixed" ? {} : Object.fromEntries((rule.hours.facts === "all" ? ["x"] : rule.hours.facts.slice(0, 1)).map((k) => [k, n]));
        const h = effortHours(rule.hours, facts)!;
        const d = difficultyOf(h, rule.complexity)!;
        assert.ok(rank(d) >= BAND_RANK[timeBandOf(h)], `${id} at ${n}: ${d} below ${timeBandOf(h)}`);
        if (h.high > SMALL_MAX_HOURS) assert.notEqual(d, "quick_fix", `${id} at ${n}`);
        if (h.high > MEDIUM_MAX_HOURS || h.partial) assert.equal(d, "project", `${id} at ${n}`);
      }
    }
  });

  test("the words read beside a severity, and none is a number, a price or a word that looks like a level for a finding without one", () => {
    assert.deepEqual(WORDS, ["Quick fix", "Needs planning", "A project"]);
    for (const w of WORDS) assert.doesNotMatch(w, /\d|£|\$|€|cost|price|estimat/i);
  });
});

describe("every rule's basis, in one table beside the rules", () => {
  test("every rule either has a basis, is listed as not estimated, or never raises an issue", () => {
    assert.deepEqual(rules.filter((r) => !difficultyCovered(r.id)).map((r) => r.id), []);
    // The table holds no rule that doesn't exist, and nothing is both.
    const ids = new Set(rules.map((r) => r.id));
    assert.deepEqual(Object.keys(DIFFICULTY_BASIS).filter((id) => !ids.has(id)), []);
    assert.deepEqual(Object.keys(NOT_ESTIMATED).filter((id) => Object.hasOwn(DIFFICULTY_BASIS, id)), []);
    // Today every rule that can raise an issue has a basis.
    assert.equal(Object.keys(NOT_ESTIMATED).length, 0);
    assert.equal(Object.keys(DIFFICULTY_BASIS).length, rules.filter((r) => !NEVER_RAISES.has(r.id)).length);
  });

  test("a rule with no basis gets no level, whatever its hours: one the table doesn't hold, or one that never raises an issue", () => {
    assert.equal(difficultyFor("no.such-rule", { high: 1 }), null);
    for (const id of NEVER_RAISES) assert.equal(difficultyFor(id, { high: 1 }), null, id);
    for (const id of Object.keys(NOT_ESTIMATED)) assert.equal(difficultyFor(id, { high: 1 }), null, id);
    // A rule with a basis gets the one rule's answer.
    assert.equal(difficultyFor("cur.stale-deals", { high: 4 }), difficultyOf({ high: 4 }, FIX_TABLE["cur.stale-deals"]!.complexity));
    assert.equal(difficultyFor("cur.stale-deals", undefined), null);
  });

  test("a per-unit rule's stated thresholds are where its difficulty really changes", () => {
    for (const [id, basis] of Object.entries(DIFFICULTY_BASIS)) {
      const rule = FIX_TABLE[id]!;
      if (rule.hours.kind === "fixed") {
        assert.equal(basis.levels, difficultyOf(effortHours(rule.hours, {})!, rule.complexity), id);
        continue;
      }
      const { quickFixUpTo, needsPlanningUpTo } = basis.levels as { quickFixUpTo: number; needsPlanningUpTo: number };
      const h = rule.hours;
      const at = (n: number) => difficultyOf(effortHours(h, { [h.facts === "all" ? "x" : h.facts[0]!]: n })!, rule.complexity);
      if (quickFixUpTo > 0) assert.equal(at(quickFixUpTo), "quick_fix", id);
      assert.notEqual(at(quickFixUpTo + 1), "quick_fix", id);
      if (needsPlanningUpTo > quickFixUpTo) assert.equal(at(needsPlanningUpTo), "needs_planning", id);
      assert.equal(at(needsPlanningUpTo + 1), "project", id);
    }
  });
});

describe("on the synthetic and the generated org", () => {
  test("every issue's difficulty is its own hours' and its rule's complexity; no pass or not measurable finding has one", () => {
    for (const f of [...recorded, ...generated]) {
      if (f.status !== "issue") {
        assert.equal(f.difficulty, undefined, f.ruleId);
        continue;
      }
      assert.equal(f.difficulty ?? null, difficultyOf(f.effortHours, f.fixComplexity), f.ruleId);
      assert.equal(f.difficulty === undefined, f.effortHours === undefined, f.ruleId);
    }
    const counts = Object.fromEntries(DIFFICULTY_ORDER.map((d) => [d, recorded.filter((f) => f.difficulty === d).length]));
    assert.ok(counts.quick_fix! > 0 && counts.needs_planning! > 0 && counts.project! > 0, JSON.stringify(counts));
  });
});

const flows = recorded.find((f) => f.status === "issue" && f.ruleId === "aut.hardcoded-ids-in-flows")!;
const correction = (newValue: number, previousValue = flows.facts.affected!): NumberOverride => ({ id: "ov-0001", runId: "run-0001", target: { type: "finding", id: flows.id, field: "facts.affected" }, previousValue, newValue, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-10-31T13:00:00.000Z" });

describe("a consultant's correction", () => {
  test("one that changes the figure drops the hours and the difficulty with them, in the report, the digest and the documents", () => {
    assert.equal(flows.difficulty, "quick_fix");
    const report = reportWith([correction(3)]);
    const after = report.teamFindings.find((f) => f.id === flows.id)!;
    assert.equal(after.effortHours, undefined);
    assert.equal(after.difficulty, undefined);
    const digest = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" });
    const df = digest.findings.find((f) => f.id === flows.id)!;
    assert.equal(df.difficulty, undefined);
    assert.ok(!(digest.plan.quickFixes ?? []).includes(flows.id));
    assert.ok(!digest.plan.hoursTotal!.findingIds.includes(flows.id));
  });

  test("one put back to the value as read keeps both", () => {
    const after = reportWith([correction(flows.facts.affected!)]).teamFindings.find((f) => f.id === flows.id)!;
    assert.equal(after.difficulty, flows.difficulty);
  });
});

describe("shown beside severity, and nothing where there is none", () => {
  const report = reportWith();
  const docs = (r: BuiltReport) => ({ executive: executiveHtml(r, loaded.pseudonyms), full: fullReportHtml(r, loaded.pseudonyms) });

  test("the documents print the difficulty, never the old effort size, and the one-pager is unchanged in kind", () => {
    const { executive, full } = docs(report);
    for (const [name, html] of Object.entries({ executive, full })) {
      assert.doesNotMatch(html, /Effort: /, name);
      assert.match(html, /<span class="difficulty">(Quick fix|Needs planning|A project)<\/span>/, name);
    }
    assert.match(full, /severity\. (Quick fix|Needs planning|A project)\. /);
    assert.doesNotMatch(onePagerHtml(report, loaded.pseudonyms), /class="difficulty"/);
  });

  test("a finding with no difficulty shows no label: no word that looks like one, and no placeholder", () => {
    const bare: BuiltReport = { ...report, teamFindings: report.teamFindings.map(({ difficulty: _d, ...f }) => f as Finding) };
    const { executive, full } = docs(bare);
    for (const [name, html] of Object.entries({ executive, full })) {
      assert.doesNotMatch(html, /class="difficulty"/, name);
      assert.doesNotMatch(html, /Not estimated|not estimated|Effort: /, name);
      assert.doesNotMatch(html, /severity\. (Quick fix|Needs planning|A project)\. /, name);
    }
  });
});

describe("the digest", () => {
  const report = reportWith();
  const digest = buildDigest({ report, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" });

  test("each finding carries its difficulty exactly as the finding holds it", () => {
    for (const f of report.teamFindings) assert.equal(digest.findings.find((d) => d.id === f.id)!.difficulty, f.difficulty, f.id);
  });

  test("quick fixes are the plan's critical and high quick fixes, in the plan's order, and move nothing", () => {
    const plan = buildPlan(report.teamFindings, report.added);
    const order = [...plan.quickWins, ...plan.phases.flatMap((p) => p.findings), ...plan.later];
    const expected = order.filter((f) => (f.severity === "critical" || f.severity === "high") && f.difficulty === "quick_fix").map((f) => f.id);
    assert.ok(expected.length > 0);
    assert.deepEqual(digest.plan.quickFixes, expected);
    const byId = new Map(report.teamFindings.map((f) => [f.id, f]));
    for (const id of digest.plan.quickFixes!) {
      assert.ok(["critical", "high"].includes(byId.get(id)!.severity), id);
      assert.equal(byId.get(id)!.difficulty, "quick_fix", id);
    }
    // The plan itself is the one it always was.
    assert.deepEqual(digest.plan.quickWins, plan.quickWins.map((f) => f.id));
    assert.deepEqual(digest.plan.phases.map((p) => p.findingIds), plan.phases.map((p) => p.findings.map((f) => f.id)));
  });

  test("a low or medium quick fix is never listed, and with none the list is absent", () => {
    const plan = buildPlan(report.teamFindings, report.added);
    const lowered = { ...plan, quickWins: plan.quickWins.map((f) => ({ ...f, severity: "low" as const })), phases: plan.phases.map((p) => ({ ...p, findings: p.findings.map((f) => ({ ...f, severity: "medium" as const })) })), later: plan.later.map((f) => ({ ...f, severity: "low" as const })) };
    assert.deepEqual(quickFixesOf(lowered), []);
    const bare = buildDigest({ report: { ...report, teamFindings: report.teamFindings.map((f) => (f.difficulty === "quick_fix" ? { ...f, difficulty: "needs_planning" as const } : f)) }, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" });
    assert.equal(bare.plan.quickFixes, undefined);
    assert.ok(!("quickFixes" in bare.plan));
  });

  test("a digest written before this change, with neither field, still parses", () => {
    const older = JSON.parse(JSON.stringify(digest)) as Record<string, any>;
    delete older.plan.quickFixes;
    for (const f of older.findings) delete f.difficulty;
    const parsed = RunDigest.parse(older);
    assert.ok(parsed.findings.every((f) => f.difficulty === undefined));
    assert.equal(RunDigest.safeParse({ ...digest, findings: digest.findings.map((f) => ({ ...f, difficulty: "easy" })) }).success, false);
  });
});
