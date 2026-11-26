import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { buildReport, type ReportInput } from "../../narrative/sections.ts";
import { Pseudonymiser, type PseudonymEntry } from "../../privacy/pseudonymiser.ts";
import { scoreDomains } from "../../domains/scoring.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { progressPage } from "../../render/progress.ts";
import { renderContext } from "../../render/html.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { compareWithBaseline, type Progress, type ProgressSide } from "../compare.ts";
import { changedAcross, type Change } from "../../engine/loosened.ts";
import type { Finding } from "../../schemas/index.ts";

// Between runs (DECISIONS.md 779): a check's first appearance is checked for the first time, never new; a reading
// that couldn't be made under a newer version, then an issue under an older one, is the check changing back; a
// declared change reads either way; a rule this run didn't run says nothing; and the pulse's counts add up.

const LISTS = ["fixed", "remaining", "added", "unknown", "newlyMeasured", "ruleChanged", "stricter", "changedBack", "notRunNow"] as const;
const where = (p: Progress, ruleId: string): string[] => LISTS.filter((k) => (p[k] ?? []).some((c) => c.ruleId === ruleId));
const f = (ruleId: string, status: "issue" | "passed" | "not_measurable", ruleVersion = 1): Finding => ({ id: `f-${ruleId}-${status}-${ruleVersion}`, ruleId, domain: "x", status, ruleVersion, title: `T ${ruleId}`, facts: {} }) as unknown as Finding;
const side = (runId: string, findings: Finding[], more: Partial<ProgressSide> = {}): ProgressSide =>
  ({ runId, asOf: "2026-10-31T12:00:00.000Z", findings, scores: [], overall: { status: "not_measurable" }, value: { currency: "GBP", total: { expected: 0 } }, limitsReadAt: null, ...more }) as unknown as ProgressSide;

describe("a check's first appearance (decision 779)", () => {
  const OTHER = "dq.open-opps-past-close";
  const RULE = "lead.source-win-rate";
  test("no finding in the baseline: checked for the first time, never new, with or without the baseline's list", () => {
    for (const lists of [{}, { rulesRun: [OTHER] }]) {
      const p = compareWithBaseline(side("a", [f(OTHER, "passed")], lists), side("b", [f(OTHER, "passed"), f(RULE, "issue")]));
      assert.deepEqual(where(p, RULE), ["newlyMeasured"], JSON.stringify(lists));
      assert.deepEqual(p.added, []);
    }
  });

  test("listed as run with nothing found, as the portal reads it (console 720): a pass then, so an issue now is new", () => {
    const p = compareWithBaseline(side("a", [f(OTHER, "passed")], { rulesRun: [OTHER, RULE] }), side("b", [f(RULE, "issue")]));
    assert.deepEqual(where(p, RULE), ["added"]);
  });

  test("a finding then keeps the comparison it had: a pass then is new, and so is a reading it couldn't make at the same version", () => {
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "passed")]), side("b", [f(RULE, "issue")])), RULE), ["added"]);
    assert.deepEqual(where(compareWithBaseline(side("a", [f(RULE, "not_measurable")]), side("b", [f(RULE, "issue")])), RULE), ["added"]);
  });
});

describe("a reading the earlier run couldn't make, under a newer version (decision 779)", () => {
  test("not measurable then under the journey's version, an issue now under the older one: the check changed back, open, never new", () => {
    for (const [rule, newer, older] of [["lead.no-first-response", 3, 2], ["lead.response-time", 2, 1], ["cur.stuck-in-stage", 3, 2]] as const) {
      const p = compareWithBaseline(side("a", [f(rule, "not_measurable", newer)]), side("b", [f(rule, "issue", older)]));
      assert.deepEqual(where(p, rule), ["changedBack"], `${rule} ${newer} to ${older}`);
    }
  });

  test("an issue under the journey's version, then a pass under the older one: the check changed, said once, carried nowhere", () => {
    const p = compareWithBaseline(side("a", [f("lead.response-time", "issue", 2)]), side("b", [f("lead.response-time", "passed", 1)]));
    assert.deepEqual(where(p, "lead.response-time"), ["ruleChanged"]);
    assert.equal(p.carried, undefined);
  });
});

describe("a declared change reads either way, from the versions it names (decision 779)", () => {
  const table: Record<string, readonly Change[]> = { r: [{ version: 3, on: "2026-11-27", from: [2] }] };
  test("across it, up or back", () => {
    assert.equal(changedAcross("r", 2, 3, table), true);
    assert.equal(changedAcross("r", 3, 2, table), true);
    assert.equal(changedAcross("r", 2, 4, table), true);
    assert.equal(changedAcross("r", 4, 2, table), true);
  });
  test("never from a version it doesn't name, nor between two versions on one side of it", () => {
    assert.equal(changedAcross("r", 1, 3, table), false, "1 isn't in from");
    assert.equal(changedAcross("r", 3, 1, table), false, "nor back to it");
    assert.equal(changedAcross("r", 3, 4, table), false);
    assert.equal(changedAcross("r", 1, 2, table), false);
    assert.equal(changedAcross("r", 2, 2, table), false);
    assert.equal(changedAcross("r", undefined, 3, table), false);
  });
});

describe("a rule this run didn't run (decision 779)", () => {
  test("its earlier issue is never 'can't tell' and nothing is said; kept apart only so what the earlier run found adds up", () => {
    const p = compareWithBaseline(side("a", [f("lead.source-win-rate", "issue"), f("dq.open-opps-past-close", "issue")]), side("b", [f("dq.open-opps-past-close", "not_measurable")]));
    assert.deepEqual(where(p, "lead.source-win-rate"), ["notRunNow"]);
    assert.deepEqual(where(p, "dq.open-opps-past-close"), ["unknown"], "a check this run made and couldn't measure is can't tell, as before");
  });
});

describe("the re-check's outputs and the pulse's counts (decision 779)", async () => {
  const { output, run, dir } = await runFromRecording({ answers: loadAnswers() });
  const pseudonyms = new Pseudonymiser(JSON.parse(readFileSync(join(dir, "pseudonym-map.json"), "utf8")) as PseudonymEntry[]);
  const all = output!.findings;
  const issueRules = [...new Set(all.filter((x) => x.status === "issue").map((x) => x.ruleId))];
  // The baseline didn't run one of the issue rules at all; a later reading of another came back under an older version.
  const [notRunThen, wentBack] = [issueRules[0]!, issueRules[1]!];
  const thenFindings = all.filter((x) => x.ruleId !== notRunThen).map((x): Finding => (x.ruleId === wentBack ? { ...x, status: "not_measurable", notMeasurableReason: "Not readable then.", ruleVersion: x.ruleVersion + 1 } : x));
  const first: ReportInput = { run, calibration: loadCalibration(), findings: thenFindings, metrics: output!.metrics, manualSteps: output!.manualSteps, scores: scoreDomains(run.modules, thenFindings) };
  const second: ReportInput = { ...first, run: { ...run, id: `${run.id}-recheck`, baselineRunId: run.id }, findings: all, scores: scoreDomains(run.modules, all), baseline: first };
  const report = buildReport(second);
  const p = report.progress!;

  test("the first appearance is checked for the first time in the comparison, the deck and the raw notes, never new", () => {
    assert.deepEqual(where(p, notRunThen), ["newlyMeasured"]);
    const title = all.find((x) => x.ruleId === notRunThen)!.title;
    const page = progressPage(renderContext(pseudonyms, "GBP", false), report);
    assert.match(page, /We checked \d+ more for the first time, so they aren&#39;t counted as new\./);
    assert.ok(!/New since then/.test(page) || !page.split("New since then")[1]!.split("</ul>")[0]!.includes(title), "not listed as new");
    const raw = report.outputs.raw_notes.versions.map((v) => v.body).join("\n");
    assert.match(raw, new RegExp(`Checked for the first time \\(\\d+\\), so not counted as new:\\n(?:.*\\n)*?${title.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`));
  });

  test("the went-back reading is open, never new", () => {
    assert.deepEqual(where(p, wentBack), ["changedBack"]);
  });

  test("the pulse's counts add up: open now is still open, new, checked for the first time, changed and stricter", () => {
    const pulse = buildPulseResult(report);
    const c = pulse.counts;
    assert.equal(c.firstChecked, 1);
    assert.equal(c.changed, 1);
    assert.equal(c.stricter, undefined, "only when there are any");
    assert.equal(c.openNow, c.stillOpen! + c.new! + (c.firstChecked ?? 0) + (c.changed ?? 0) + (c.stricter ?? 0));
    assert.equal(pulse.newIssues.length, Math.min(c.new!, 5));
  });

  test("a pulse with nothing apart has the counts it had: no new keys", () => {
    const same = buildReport({ ...second, baseline: { ...first, findings: all, scores: second.scores } });
    const c = buildPulseResult(same).counts;
    assert.deepEqual(Object.keys(c), ["openNow", "fixed", "stillOpen", "new", "cantTell"]);
    assert.equal(c.openNow, c.stillOpen! + c.new!);
  });
});
