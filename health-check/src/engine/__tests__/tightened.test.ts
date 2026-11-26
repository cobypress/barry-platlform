import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { cpSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { ALL_MODULES, LATEST_RULE_VERSIONS, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { foundByStricter, passedUnder, TIGHTENED, tightenedFrom, tightenedOn, tightenedProblems, type Tightening } from "../loosened.ts";
import { compareWithBaseline, progressFigures, stricterHeading, stricterNote, type Progress, type ProgressSide } from "../../progress/compare.ts";
import { buildDigest } from "../../digest/build.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { buildReport, type BuiltReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { progressPage } from "../../render/progress.ts";
import { renderContext } from "../../render/html.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { servedPulseTitle } from "../../pulse/served.ts";
import { runHealthCheck } from "../run.ts";
import { orgWordsOf, withOrgWords } from "../org-words.ts";
import { metricMarks } from "../automatic-words.ts";
import { ruleWhere } from "../../../scripts/rule-where.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { withDeclared } from "../../testing/tightened.ts";
import type { Finding } from "../../schemas/index.ts";

// Rule versions that tightened a rule (DECISIONS.md 754), the mirror of 732. The one
// rule tightened, cur.stuck-in-stage 3 (777), runs only behind HC_STUCK_EXPECTED, so
// every test here that needs one declares its own for the test alone (withDeclared),
// never in src/engine/loosened.ts; 777's own stories are in stuck-expected.test.ts.

// With the versions behind a switch (DECISIONS.md 777): cur.stuck-in-stage 3 runs only with HC_STUCK_EXPECTED on.
const versions = LATEST_RULE_VERSIONS;
/** Raised by the synthetic org at version 2 on a check, a pulse and an automatic check: 110 of 500 open leads, 22%. */
const RULE = "dq.stale-leads";
/** Version 2 raised the line, say, from 30% to 20%: a share under 30% passed under version 1. */
const judged = (version = 2, from = [1]): Tightening => ({ version, on: "2026-11-25", from, passedBefore: (facts) => (facts.rate ?? 100) < 30 });
const unjudged = (version = 2, from = [1]): Tightening => ({ version, on: "2026-11-25", from });
const tightened = (...t: Tightening[]) => ({ tightened: { [RULE]: t } });
const CATEGORIES = ["fixed", "remaining", "added", "unknown", "newlyMeasured", "ruleChanged", "stricter"] as const;
const where = (p: Progress, ruleId = RULE): string[] => CATEGORIES.filter((k) => (p[k] ?? []).some((c) => c.ruleId === ruleId));
const NOTE = /stricter/i;

/**
 * The run in `recorded` as a re-check of a copy of itself 30 days earlier, where
 * `earlier` changes the earlier run's findings. Returns the re-check's folder.
 */
function plant(recorded: string, earlier: (f: Finding) => Finding | null, now: (f: Finding) => Finding = (f) => f): string {
  const parent = tempDir();
  const dir = join(parent, "hc-2026-11-09-recheck");
  const before = join(parent, "hc-2026-10-09-earlier");
  cpSync(recorded, dir, { recursive: true });
  cpSync(recorded, before, { recursive: true });
  const read = (d: string, file: string) => JSON.parse(readFileSync(join(d, file), "utf8"));
  const run = read(recorded, "run.json");
  writeFileSync(join(before, "run.json"), JSON.stringify({ ...run, id: "hc-2026-10-09-earlier", asOf: "2026-10-09T12:00:00.000Z", baselineRunId: null }));
  writeFileSync(join(dir, "run.json"), JSON.stringify({ ...run, baselineRunId: "hc-2026-10-09-earlier" }));
  const findings = read(recorded, "findings.json") as Finding[];
  writeFileSync(join(before, "findings.json"), JSON.stringify(findings.map(earlier).filter((f): f is Finding => f !== null)));
  writeFileSync(join(dir, "findings.json"), JSON.stringify(findings.map(now)));
  return dir;
}
/** The rule passed under version 1 in the earlier run, its reading unchanged. */
const passedAtV1 = (f: Finding): Finding => (f.ruleId === RULE ? { ...f, status: "passed", ruleVersion: 1 } : f);
const report = (dir: string): { built: BuiltReport; loaded: ReturnType<typeof loadRun> } => {
  const loaded = loadRun(dir);
  return { built: buildReport(loaded.input), loaded };
};
const rawNotes = (r: BuiltReport): string => JSON.stringify(r.outputs.raw_notes.versions);
const deck = (r: BuiltReport, loaded: ReturnType<typeof loadRun>): string => progressPage(renderContext(loaded.pseudonyms, r.input.calibration.currency, false), r);

const consultant = (await runFromRecording({ answers: loadAnswers() })).dir;

describe("the declarations (decision 754)", () => {
  test("one is declared, cur.stuck-in-stage 3 behind HC_STUCK_EXPECTED (decision 777): rule-where.json carries only it, and with the switch off no digest finding carries passedUnder", () => {
    // The lead journey's versions are declared as changed, never as tightened (DECISIONS.md 778): the stricter note
    // is reachable only through cur.stuck-in-stage 3, behind HC_STUCK_EXPECTED, which is not for switch-on.
    assert.deepEqual(Object.keys(TIGHTENED), ["cur.stuck-in-stage"]);
    assert.deepEqual(tightenedProblems(TIGHTENED, versions), []);
    assert.deepEqual(Object.entries(ruleWhere()).filter(([, e]) => "tightenedFrom" in e || "tightenedOn" in e).map(([id]) => id), ["cur.stuck-in-stage"]);
    const { built, loaded } = report(consultant);
    const digest = buildDigest({ report: built, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: built.input.run.asOf });
    assert.ok(digest.findings.every((f) => !("passedUnder" in f)));
  });

  test("checked as loosenings are: a real rule, a version it has had, earlier versions that exist, a day", () => {
    const at = (version: number, from: number[], on = "2026-11-25") => ({ [RULE]: [{ version, from, on }] });
    assert.deepEqual(tightenedProblems(at(2, [1]), versions), []);
    assert.match(tightenedProblems(at(3, [1]), versions).join(), /version 3 isn't one the rule has had/);
    assert.match(tightenedProblems(at(2, [2]), versions).join(), /from 2, which isn't an earlier version/);
    assert.match(tightenedProblems(at(2, []), versions).join(), /tightened from nothing/);
    assert.match(tightenedProblems({ [RULE]: [{ version: 2, from: [1], on: "2026-11-25" }, { version: 2, from: [1], on: "2026-11-25" }] }, versions).join(), /a version tightened twice/);
    assert.match(tightenedProblems(at(2, [1], "25 November"), versions).join(), /isn't a day/);
    assert.match(tightenedProblems({ "no.such-rule": [{ version: 2, from: [1], on: "2026-11-25" }] }, versions).join(), /no rule has this id/);
  });

  test("published in rule-where.json as tightenedFrom and tightenedOn, apart from a loosening of the same version", async () => {
    await withDeclared({ ...tightened(judged()), loosened: { [RULE]: [{ version: 2, from: [1], on: "2026-11-24" }] } }, () => {
      assert.deepEqual(tightenedFrom(RULE), { "2": [1] });
      assert.deepEqual(tightenedOn(RULE), { "2": "2026-11-25" });
      const entry = ruleWhere()[RULE]!;
      assert.deepEqual(Object.keys(entry), ["version", "loosenedFrom", "loosenedOn", "tightenedFrom", "tightenedOn"]);
      assert.deepEqual(entry.tightenedFrom, { "2": [1] });
      assert.deepEqual(entry.loosenedOn, { "2": "2026-11-24" });
    });
    assert.equal(tightenedFrom(RULE), undefined, "taken away after");
  });
});

describe("an issue after a pass, judged on the issue's own reading (decision 754)", () => {
  const issue = (rate: number, ruleVersion = 2) => ({ ruleId: RULE, ruleVersion, status: "issue", facts: { rate } });

  test("passedUnder: the earlier versions the reading would have passed; only on a tightened rule's issue", async () => {
    assert.equal(passedUnder(issue(22), undefined), undefined, "nothing declared");
    await withDeclared(tightened(judged()), () => {
      assert.deepEqual(passedUnder(issue(22), undefined), [1]);
      assert.deepEqual(passedUnder(issue(35), undefined), [], "the old line raises it too");
      assert.equal(passedUnder({ ...issue(22), status: "passed" }, undefined), undefined, "a pass carries none");
      assert.equal(passedUnder(issue(22, 1), undefined), undefined, "an issue before the tightening");
    });
    await withDeclared(tightened(unjudged()), () => assert.deepEqual(passedUnder(issue(22), undefined), [1], "no judge: its versions stay unsettled, never shown raised"));
  });

  test("an older tightening with no judge never silences a newer one's judge (review case A)", async () => {
    // 2 tightened from 1 with no judge; 3 tightened from 2, judging a reading of 10% or more raised under 2 too.
    const a = { tightened: { [RULE]: [unjudged(2, [1]), { version: 3, on: "2026-11-25", from: [2], passedBefore: (f: Readonly<Record<string, number>>) => (f.rate ?? 100) < 10 }] } };
    await withDeclared(a, () => {
      assert.deepEqual(passedUnder(issue(35, 3), undefined), [1], "unsettled under 1, shown raised under 2");
      assert.equal(foundByStricter(RULE, 2, 3, passedUnder(issue(35, 3), undefined)), false, "a pass under 2 then an issue under 3 the judge says 2 raises too: new");
      assert.equal(foundByStricter(RULE, 1, 3, passedUnder(issue(35, 3), undefined)), true, "a pass under 1 crosses the unjudged tightening: found by a stricter check");
      assert.deepEqual(passedUnder(issue(5, 3), undefined), [1, 2], "a reading the judge passes under 2");
    });
  });

  test("a judge answers per earlier version, and the pass's own version decides (review case E)", async () => {
    // 3 tightened from 1 and 2: the reading passes under 2's line but is raised under 1's.
    const perVersion: Tightening = { version: 3, on: "2026-11-25", from: [1, 2], passedBefore: (_f, _c, earlier) => earlier === 2 };
    await withDeclared({ tightened: { [RULE]: [perVersion] } }, () => {
      assert.deepEqual(passedUnder(issue(22, 3), undefined), [2]);
      assert.equal(foundByStricter(RULE, 2, 3, [2]), true);
      assert.equal(foundByStricter(RULE, 1, 3, [2]), false, "raised under 1 as well: new");
    });
    // 2 says "passes under 1"; 3, also from 1, says "raised under 1 and 2". A tightening that shows it raised under the pass's version decides: new.
    await withDeclared({ tightened: { [RULE]: [{ version: 2, on: "2026-11-01", from: [1], passedBefore: () => true }, { version: 3, on: "2026-11-25", from: [1, 2], passedBefore: () => false }] } }, () => {
      assert.deepEqual(passedUnder(issue(35, 3), undefined), []);
      assert.equal(foundByStricter(RULE, 1, 3, []), false);
    });
    // The simple form, one answer for every earlier version, still works.
    await withDeclared({ tightened: { [RULE]: [{ ...judged(3, [1, 2]) }] } }, () => assert.deepEqual(passedUnder(issue(22, 3), undefined), [1, 2]));
  });

  test("a judge that throws fails the comparison, loudly, as a loosening's does", async () => {
    const { built } = report(consultant);
    const side = (findings: Finding[]): ProgressSide => ({ runId: "r", asOf: built.input.run.asOf, findings, scores: built.input.scores ?? [], overall: built.overall, value: built.value, limitsReadAt: null });
    const own = built.teamFindings.find((f) => f.ruleId === RULE)!;
    const others = built.teamFindings.filter((f) => f.ruleId !== RULE);
    await withDeclared({ tightened: { [RULE]: [{ version: 2, on: "2026-11-25", from: [1], passedBefore: () => { throw new Error("judge broke"); } }] } }, () => {
      assert.throws(() => compareWithBaseline(side([...others, { ...own, status: "passed", ruleVersion: 1 }]), side(built.teamFindings)), /judge broke/);
    });
  });

  test("foundByStricter: only across a tightening from the pass's version, and only where the old line would pass", async () => {
    await withDeclared(tightened(judged(2, [1])), () => {
      assert.equal(foundByStricter(RULE, 1, 2, [1]), true);
      assert.equal(foundByStricter(RULE, 1, 2, []), false, "the old line raises it too: new");
      assert.equal(foundByStricter(RULE, 2, 2, [1]), false, "same version: new");
      assert.equal(foundByStricter(RULE, 1, 2, undefined), true, "can't judge: never new we can't show");
      assert.equal(foundByStricter("dq.open-opps-no-activity", 1, 2, undefined), false, "a rule not tightened");
    });
  });

  test("tightened twice, and a run that skips a version", async () => {
    // 2 tightened from 1; 3 tightened from 1 and 2. A pass under 1 then an issue under 3 crosses both.
    const twice = { tightened: { [RULE]: [judged(2, [1]), { ...judged(3, [1, 2]), passedBefore: (f: Readonly<Record<string, number>>) => (f.rate ?? 100) < 25 }] } };
    await withDeclared(twice, () => {
      assert.equal(foundByStricter(RULE, 1, 3, [1, 2]), true, "1 to 3, skipping 2");
      assert.equal(foundByStricter(RULE, 2, 3, [1, 2]), true, "2 to 3");
      assert.equal(foundByStricter(RULE, 2, 3, [1]), false, "the reading would have been raised under 2: new");
    });
    // Only 3 tightened, and only from 2: a pass under 1 then an issue under 3 isn't across a declared tightening, so it is new.
    await withDeclared({ tightened: { [RULE]: [judged(3, [2])] } }, () => {
      assert.equal(foundByStricter(RULE, 1, 3, [2]), false);
      assert.equal(foundByStricter(RULE, 2, 3, [2]), true);
    });
  });

  test("the earlier version unknown: found by a stricter check unless every earlier version would raise it too", async () => {
    await withDeclared({ tightened: { [RULE]: [judged(3, [1, 2])] } }, () => {
      assert.equal(foundByStricter(RULE, undefined, 3, [1, 2]), true);
      assert.equal(foundByStricter(RULE, undefined, 3, [2]), true, "passed under some: can't show it is new");
      assert.equal(foundByStricter(RULE, undefined, 3, undefined), true, "no judge");
      assert.equal(foundByStricter(RULE, undefined, 3, []), false, "raised under every earlier version: new");
    });
  });
});

describe("a re-check after a tightening, the org unchanged (decision 754)", () => {
  test("open, never new: in the open count and the score, out of new, said once in the deck, listed in the raw notes", async () => {
    await withDeclared(tightened(judged()), () => {
      const { built, loaded } = report(plant(consultant, passedAtV1));
      const p = built.progress!;
      assert.deepEqual(where(p), ["stricter"]);
      assert.deepEqual(p.stricter!.map((c) => c.title), ["Leads are going unworked"]);
      assert.ok(built.teamFindings.some((f) => f.ruleId === RULE && f.status === "issue"), "still an issue in this run, so in the plan, value and score");
      const pulse = buildPulseResult(built);
      assert.equal(pulse.counts.new, p.added.length);
      assert.ok(!pulse.newIssues.some((i) => i.title === "Leads are going unworked"), "not in the email's new titles");
      assert.equal(pulse.counts.openNow, new Set(built.teamFindings.filter((f) => f.status === "issue").map((f) => f.ruleId)).size, "counted open");
      const html = deck(built, loaded);
      assert.match(html, /<p class="note">We&#39;ve made one of our checks stricter since your last health check, and it raised an issue it may not have raised before\. It&#39;s open, but we don&#39;t count it as new\. The raw notes name it\.<\/p>/);
      assert.equal(html.match(NOTE)?.length, 1, "said once");
      assert.ok(!/New since then[^<]*<\/h3><ul>(?:(?!<\/ul>).)*Leads are going unworked/.test(html), "not listed as new");
      assert.match(rawNotes(built), /Found by a stricter check \(1\), so not counted as new:\\nLeads are going unworked/);
      assert.ok(!/New since then \(\d+\):[^"]*Leads are going unworked/.test(rawNotes(built)));
      assert.ok(progressFigures(p).includes(1));
      for (const html of [executiveHtml(built, loaded.pseudonyms), fullReportHtml(built, loaded.pseudonyms)]) assert.match(html, /one of our checks stricter/);
      const digest = buildDigest({ report: built, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: built.input.run.asOf });
      assert.deepEqual(digest.findings.filter((f) => f.ruleId === RULE).map((f) => f.passedUnder), [[1]]);
      assert.ok(digest.findings.filter((f) => f.ruleId !== RULE).every((f) => !("passedUnder" in f)));
    });
  });

  test("two rules: the plural note", async () => {
    const other = "dq.open-opps-no-activity";
    await withDeclared({ tightened: { [RULE]: [judged()], [other]: [unjudged()] } }, () => {
      const { built, loaded } = report(plant(consultant, (f) => (f.ruleId === RULE || f.ruleId === other ? { ...f, status: "passed", ruleVersion: 1 } : f)));
      assert.deepEqual(built.progress!.stricter!.map((c) => c.ruleId).sort(), [other, RULE].sort());
      assert.match(deck(built, loaded), /We&#39;ve made 2 of our checks stricter since your last health check, and they raised issues they may not have raised before\. They&#39;re open, but we don&#39;t count them as new\. The raw notes name them\./);
      assert.match(rawNotes(built), /Found by a stricter check \(2\), so not counted as new:/);
    });
  });

  test("with no tightening declared, the same pair is new, as today, and nothing says stricter", () => {
    const { built, loaded } = report(plant(consultant, passedAtV1));
    assert.deepEqual(where(built.progress!), ["added"]);
    assert.equal(built.progress!.stricter, undefined, "the key is absent");
    assert.doesNotMatch(deck(built, loaded), NOTE);
    assert.doesNotMatch(rawNotes(built), NOTE);
    assert.ok(buildPulseResult(built).newIssues.some((i) => i.title === "Leads are going unworked"));
  });
});

describe("the cases that stay new, or stay as they were (decision 754)", () => {
  test("tightened, and the org changed so the old version would also raise it: new", async () => {
    await withDeclared(tightened(judged()), () => {
      const worse = (f: Finding): Finding => (f.ruleId === RULE ? { ...f, facts: { ...f.facts, rate: 35 } } : f);
      const { built } = report(plant(consultant, passedAtV1, worse));
      assert.deepEqual(where(built.progress!), ["added"]);
      assert.equal(buildPulseResult(built).counts.new, built.progress!.added.length);
    });
  });

  test("tightened then loosened: a pass after an issue reads only the loosening, an issue after a pass only the tightening", async () => {
    const both = { tightened: { [RULE]: [judged(2, [1])] }, loosened: { [RULE]: [{ version: 2, on: "2026-11-25", from: [1] }] } };
    await withDeclared(both, () => {
      assert.deepEqual(where(report(plant(consultant, passedAtV1)).built.progress!), ["stricter"]);
      // Raised under 1, passing under 2: the loosening keeps it apart, and the tightening has no say.
      const { built } = report(plant(consultant, (f) => (f.ruleId === RULE ? { ...f, ruleVersion: 1 } : f), (f) => (f.ruleId === RULE ? { ...f, status: "passed" } : f)));
      assert.deepEqual(where(built.progress!), ["ruleChanged"]);
    });
  });

  test("a rule the earlier run didn't check is not found by a stricter check: as today", async () => {
    await withDeclared(tightened(unjudged()), () => {
      const { built } = report(plant(consultant, (f) => (f.ruleId === RULE ? null : f)));
      assert.deepEqual(where(built.progress!), ["added"]);
    });
    // One of the code checker's groups newly run stays newly checked (673), tightened or not.
    const { built } = report(consultant);
    const side = (findings: Finding[]): ProgressSide => ({ runId: "r", asOf: built.input.run.asOf, findings, scores: built.input.scores ?? [], overall: built.overall, value: built.value, limitsReadAt: null });
    const lint: Finding = { ...built.teamFindings.find((f) => f.ruleId === RULE)!, id: "f-000000000001", ruleId: "tech.lint-tests", ruleVersion: 2 };
    await withDeclared({ tightened: { "tech.lint-tests": [unjudged()] } }, () => {
      assert.deepEqual(where(compareWithBaseline(side(built.teamFindings), side([...built.teamFindings, lint])), "tech.lint-tests"), ["newlyMeasured"]);
      assert.deepEqual(where(compareWithBaseline(side([...built.teamFindings, { ...lint, status: "passed", ruleVersion: 1 }]), side([...built.teamFindings, lint])), "tech.lint-tests"), ["stricter"], "but passed then: found by a stricter check");
    });
  });

  test("segments: one the old line would also raise makes the rule new; one not measured before has no pass to stand on, so new", async () => {
    const { built } = report(consultant);
    const side = (findings: Finding[]): ProgressSide => ({ runId: "r", asOf: built.input.run.asOf, findings, scores: built.input.scores ?? [], overall: built.overall, value: built.value, limitsReadAt: null });
    const own = built.teamFindings.find((f) => f.ruleId === RULE)!;
    const others = built.teamFindings.filter((f) => f.ruleId !== RULE);
    const at = (status: Finding["status"], ruleVersion: number, rate: number, id = own.id, extra: Partial<Finding> = {}): Finding => ({ ...own, id, status, ruleVersion, facts: { ...own.facts, rate }, ...extra });
    await withDeclared(tightened(judged()), () => {
      const b = compareWithBaseline(side([...others, at("passed", 1, 20), at("passed", 1, 20, "f-000000000002")]), side([...others, at("issue", 2, 22), at("issue", 2, 40, "f-000000000002")]));
      assert.deepEqual(where(b), ["added"], "review case B");
      const c = compareWithBaseline(side([...others, at("passed", 1, 20), at("not_measurable", 1, 0, "f-000000000002", { notMeasurableReason: "The leads couldn't be read." })]), side([...others, at("issue", 2, 22), at("issue", 2, 22, "f-000000000002")]));
      assert.deepEqual(where(c), ["added"], "review case C");
      const both = compareWithBaseline(side([...others, at("passed", 1, 20), at("passed", 1, 20, "f-000000000002")]), side([...others, at("issue", 2, 22), at("issue", 2, 22, "f-000000000002")]));
      assert.deepEqual(where(both), ["stricter"], "every segment passed then and passes the old line");
    });
  });

  test("an issue that was an issue before is still open, and one not measured before is new, as today", async () => {
    await withDeclared(tightened(unjudged()), () => {
      assert.deepEqual(where(report(plant(consultant, (f) => (f.ruleId === RULE ? { ...f, ruleVersion: 1 } : f))).built.progress!), ["remaining"]);
      assert.deepEqual(where(report(plant(consultant, (f) => (f.ruleId === RULE ? { ...f, status: "not_measurable", notMeasurableReason: "The leads couldn't be read.", ruleVersion: 1 } : f))).built.progress!), ["added"]);
    });
  });

  test("the earlier run's version unknown, through the comparison", async () => {
    const { built } = report(consultant);
    const side = (findings: Finding[]): ProgressSide => ({ runId: "r", asOf: built.input.run.asOf, findings, scores: built.input.scores ?? [], overall: built.overall, value: built.value, limitsReadAt: null });
    const own = built.teamFindings.find((f) => f.ruleId === RULE)!;
    const others = built.teamFindings.filter((f) => f.ruleId !== RULE);
    const unknownPass = { ...own, status: "passed", ruleVersion: undefined } as unknown as Finding;
    await withDeclared(tightened(judged()), () => {
      assert.deepEqual(where(compareWithBaseline(side([...others, unknownPass]), side(built.teamFindings))), ["stricter"]);
      assert.deepEqual(where(compareWithBaseline(side([...others, unknownPass]), side([...others, { ...own, facts: { ...own.facts, rate: 35 } }]))), ["added"], "every earlier version would raise it: new");
    });
  });
});

describe("a pulse, and an automatic customer's gated pulse (decisions 746, 754)", () => {
  test("a pulse (its subset of rules): open, never in counts.new or the new titles", async () => {
    const pulse = (await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(), connectedUser: CONNECTED_USER, log: () => {}, product: "pulse" })).dir;
    const dir = plant(pulse, passedAtV1);
    const without = buildPulseResult(report(dir).built);
    await withDeclared(tightened(judged()), () => {
      const { built } = report(dir);
      assert.equal(built.input.run.product, "pulse");
      assert.deepEqual(where(built.progress!), ["stricter"]);
      const result = buildPulseResult(built);
      assert.equal(result.counts.new, without.counts.new! - 1);
      assert.equal(result.counts.openNow, without.counts.openNow, "still open");
      assert.equal(result.score.now, without.score.now, "the score is the run's own");
      assert.ok(!result.newIssues.some((i) => i.title === "Leads are going unworked"));
    });
  });

  test("an automatic check: released with nothing unmarked or unmatched, the gated pulse titles leave it out, and the new sentences are ours", async () => {
    const calibration = await automaticSyntheticCalibration();
    const automatic = (await runFromRecording({ calibration, reviewMode: "automatic", transport: new AutomaticReplay() })).dir;
    const dir = plant(automatic, passedAtV1);
    await withDeclared(tightened(judged()), () => {
      const { built, loaded } = report(dir);
      assert.deepEqual(where(built.progress!), ["stricter"]);
      const checked = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: built.input.run.asOf });
      assert.equal(checked.held, null, "not held");
      assert.equal(checked.unmatched, 0);
      assert.equal(checked.unmarked, 0);
      assert.deepEqual(checked.digest!.findings.filter((f) => f.ruleId === RULE).map((f) => f.passedUnder), [[1]]);
      const gated = buildPulseResult(built, servedPulseTitle(checked.digest!, loaded.pseudonyms));
      assert.equal(gated.counts.new, built.progress!.added.length);
      assert.ok(!gated.newIssues.some((i) => i.title === "Leads are going unworked"));
      assert.match(fullReportHtml(built, loaded.pseudonyms), /one of our checks stricter/);
    });
  });

  test("test declarations can't overlap: a second call while one is open is refused, and the tables come back as they stood (review overlap.ts)", async () => {
    let inner: unknown;
    // The tables as they stand: cur.stuck-in-stage 3's real declaration (DECISIONS.md 777) and nothing of a test's.
    const standing = { ...TIGHTENED };
    const first = withDeclared({ tightened: { b: [] } }, async () => {
      await new Promise((r) => setTimeout(r, 5));
    });
    await assert.rejects(withDeclared({ tightened: { a: [] } }, async () => (inner = Object.keys(TIGHTENED))), /already open/);
    await first;
    assert.equal(inner, undefined, "the second never ran");
    await assert.rejects(withDeclared(tightened(judged()), () => withDeclared(tightened(judged()), () => 0)), /already open/, "nested");
    assert.deepEqual(TIGHTENED, standing);
    await withDeclared(tightened(judged()), () => assert.ok(RULE in TIGHTENED, "open again after a refusal"));
    assert.deepEqual(TIGHTENED, standing);
  });

  test("every new sentence is made by said, so an automatic run knows it as ours, with none of the org's words", () => {
    withOrgWords(() => {
      for (const text of [stricterNote(1), stricterNote(3), stricterHeading(1), stricterHeading(4)]) {
        assert.deepEqual(orgWordsOf(text), [], text);
        assert.deepEqual(metricMarks(text)?.spans, [], text);
      }
    });
  });
});
