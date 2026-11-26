import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { ANCHOR, CASE_PATH_STATUSES, seededRandom, stuckCases, caseRecords, richCases, withCases, type CaseSpec, type CaseStatus } from "../../testing/case-paths-org.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { serviceActivityExtractor, serviceCasesExtractor } from "../../extract/service.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { COUNTED_WHEN_MEASURED, INSIGHT_METRICS, INSIGHT_RULES, scoreDomains } from "../scoring.ts";
import { ALL_MODULES, SERVICE_CLOUD_MODULES } from "../index.ts";
import { casePathsModule, withCasePaths } from "../service/index.ts";
import { serviceOperationsModule } from "../service/operations.ts";
import { LEAST_LAG_DAYS, lineSaid, snapshotAt, steadyMarginOf, steadyShare, STUCK_BAR, type SnapCase } from "../service/paths.ts";
import { zBar } from "../historic-pipeline/paths/insights.ts";
import type { AnalysisOutput } from "../../engine/evaluate.ts";
import type { Finding, Metric } from "../../schemas/index.ts";

// The case status paths (DECISIONS.md 776), on constructed case sets put through
// the real analysis with the switch on, and the synthetic org with it off.

let extracts: Map<string, unknown>;
before(async () => {
  extracts = loadExtracts((await runFromRecording({ modules: ALL_MODULES })).dir);
});

const NEW_RULES = ["svc.stuck-in-status", "svc.reopened-time", "svc.closed-straight", "svc.sent-back-cases"];
const on = (specs: CaseSpec[], options: Parameters<typeof analyseExtracts>[1] & { statuses?: CaseStatus[]; zone?: string } = {}): AnalysisOutput => {
  const changed = withCases(extracts, caseRecords(specs, options.statuses));
  if (options.zone) (changed.get(orgExtractor.id) as { data: Array<{ TimeZoneSidKey: string }> }).data[0]!.TimeZoneSidKey = options.zone;
  return analyseExtracts(changed, { modules: SERVICE_CLOUD_MODULES, ...options, casePaths: true });
};
const all = (a: AnalysisOutput): Finding[] => [...a.findings, ...a.insights];
const one = (a: AnalysisOutput, ruleId: string): Finding => {
  const f = all(a).filter((x) => x.ruleId === ruleId);
  assert.equal(f.length, 1, `${ruleId}: ${f.length}`);
  return f[0]!;
};
const textOf = (f: Finding): string => f.summary ?? f.notMeasurableReason ?? "";
const metric = (a: AnalysisOutput): Metric => [...a.metrics, ...a.insightMetrics].find((m) => m.id === "service.status_paths")!;
const leading = (e: string): number => Number(/^(\d+) /.exec(e)?.[1] ?? 0);
const DAY = 86_400_000;
const NOW = Date.parse("2026-10-31T12:00:00.000Z");
const NEW = { key: "New", label: "New", closed: false };
const WORKING = { key: "Working", label: "Working", closed: false };
/** Cases for the stuck check's snapshot alone: each made `[daysBefore, stay]`, an hour in New, then Working for `stay` days (ended when that is before the run). `gap` shifts them `gap` days later. */
const withStays = (stays: Array<[number, number]>, gap: number): SnapCase[] =>
  stays.map(([before, stay], i) => {
    const made = NOW - (before - gap) * DAY;
    const end = made + 3_600_000 + stay * DAY;
    return { id: `s${i}`, madeAt: made, priority: "High", origin: "Email", visits: [{ status: NEW, start: made, end: made + 3_600_000 }, { status: WORKING, start: made + 3_600_000, end: end <= NOW ? end : null }] };
  });

/**
 * Quick cases, New to Working to Closed in two days, made 40 days and more
 * before the run, and one recent open case: with no open case at all, the
 * service module's open cases metric stops the run (a fault of the base, not of
 * this lane, said in decision 776).
 */
const quick = (n: number, from = 40): CaseSpec[] => [ANCHOR, ...quickClosed(n, from), { made: 3, steps: [["Working", 0.5]] }];
const quickClosed = (n: number, from = 40): CaseSpec[] => Array.from({ length: n }, (_, i) => ({ made: from + (i % 300), priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps: [["Working", 0.5], ["Closed", 2]] }));

/**
 * The send-back comparison's org: quick cases that never wait, cases that go
 * Working, Waiting and close, and cases sent back from Waiting to Working. `slow`
 * cases on each side close after 60 days, past the period's lag.
 */
function sentBackOrg(groupSlow: number, otherSlow: number, quickN = 250): CaseSpec[] {
  const side = (n: number, slow: number, sent: boolean, offset: number): CaseSpec[] =>
    Array.from({ length: n }, (_, i) => {
      const days = i % Math.floor(n / slow) === 0 && Math.floor(i / Math.floor(n / slow)) < slow ? 60 : 6;
      const steps: Array<[string, number]> = sent ? [["Working", 0.5], ["Waiting", 2], ["Working", 4], ["Closed", days]] : [["Working", 0.5], ["Waiting", 2], ["Closed", days]];
      return { made: 70 + ((i * 7 + offset) % 280), priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps };
    });
  return [...quick(quickN), ...side(60, groupSlow, true, 3), ...side(80, otherSlow, false, 5)];
}

describe("the switch", () => {
  test("off, the modules are the engine's own; on, only service operations is swapped, its rules and metrics after the rest", () => {
    assert.deepEqual(withCasePaths(ALL_MODULES).filter((m, i) => m !== ALL_MODULES[i]), [casePathsModule]);
    assert.deepEqual(casePathsModule.rules.map((r) => r.id), [...serviceOperationsModule.rules.map((r) => r.id), ...NEW_RULES]);
    assert.ok(INSIGHT_RULES.has("svc.sent-back-cases") && INSIGHT_METRICS.has("service.status_paths"));
    for (const id of ["svc.stuck-in-status", "svc.reopened-time", "svc.closed-straight"]) assert.ok(COUNTED_WHEN_MEASURED.has(id) && !INSIGHT_RULES.has(id), id);
  });

  test("a run without Service Cloud is the same with the switch on", () => {
    const sales = ALL_MODULES.filter((m) => m.cloud === "sales");
    assert.deepEqual(withCasePaths(sales), sales);
    assert.deepEqual(analyseExtracts(extracts, { casePaths: true }), analyseExtracts(extracts));
  });

  test("off, the synthetic org's analysis with Service Cloud has none of it, and is the analysis with no switch given", () => {
    const off = analyseExtracts(extracts, { modules: ALL_MODULES });
    assert.deepEqual(analyseExtracts(extracts, { modules: ALL_MODULES, casePaths: false }), off);
    const ids = JSON.stringify([all(off).map((f) => f.ruleId), off.metrics.map((m) => m.id), off.insightMetrics.map((m) => m.id), off.rulesRun]);
    for (const id of [...NEW_RULES, "service.status_paths"]) assert.ok(!ids.includes(id), id);
  });

  test("on, the synthetic org's other findings and metrics are those with it off", () => {
    const off = analyseExtracts(extracts, { modules: ALL_MODULES });
    const a = analyseExtracts(extracts, { modules: ALL_MODULES, casePaths: true });
    assert.deepEqual(a.findings.filter((f) => !NEW_RULES.includes(f.ruleId)), off.findings);
    assert.deepEqual(a.metrics, off.metrics);
    assert.deepEqual(a.insightMetrics.filter((m) => m.id !== "service.status_paths"), off.insightMetrics);
    assert.deepEqual(a.rulesRun, [...off.rulesRun, "svc.closed-straight", "svc.reopened-time", "svc.stuck-in-status"].sort());
  });
});

describe("who is counted", () => {
  test("every case read is counted or in a named exclusion, in a zone behind UTC, on it and ahead of it", () => {
    // A case made an hour after the year read starts (its own day only partly read), an open case made before the year,
    // and recent cases.
    const specs: CaseSpec[] = [...richCases(), { made: 365, at: "13:00", steps: [["Working", 0.5], ["Closed", 2]] }, { made: 500, steps: [] }, ...Array.from({ length: 10 }, (_, i): CaseSpec => ({ made: 2 + i, steps: [["Working", 0.2], ["Closed", 0.5]] }))];
    for (const zone of ["America/Los_Angeles", "Europe/London", "Asia/Tokyo"]) {
      const f = one(on(specs, { zone }), "svc.closed-straight");
      const cohort = f.evidence[1]!;
      const read = caseRecords(specs).cases.filter((c) => !c.IsClosed || Date.parse(c.ClosedDate!.replace("+0000", "Z")) >= Date.parse("2025-10-31T12:00:00Z")).length;
      const excluded = cohort.exclusions.reduce((t, e) => t + leading(e), 0);
      assert.equal(cohort.count! + excluded, read, `${zone}: ${cohort.exclusions.join(" | ")}`);
      assert.ok(cohort.exclusions.some((e) => /^3 cases made before 1 November 2025, the first full day of the year we read$/.test(e)), `${zone}: ${cohort.exclusions.join(" | ")}`);
    }
  });

  test("every open case is counted by the stuck check or in a named exclusion", () => {
    const specs: CaseSpec[] = [...stuckCases(), { made: 5, steps: [["Escalated", 1]] }, { made: 6, steps: [["Working", 0.5]], statusNow: "Waiting" }];
    const f = one(on(specs), "svc.stuck-in-status");
    const open = f.evidence.at(-1)!;
    assert.equal(open.count! + open.exclusions.reduce((t, e) => t + leading(e), 0), caseRecords(specs).cases.filter((c) => !c.IsClosed).length, open.exclusions.join(" | "));
    assert.ok(open.exclusions.includes("2 open cases whose status we couldn't place: not in your case status settings, a closed status, or not where their recorded changes end"), open.exclusions.join(" | "));
  });

  test("the period's last day is in it: a case made on that day is counted, the day after is too recent", () => {
    // The lag here is our minimum of 14 days, so the period ends on 17 October 2026, 14 days before the run's day.
    const base = one(on(quick(300)), "svc.closed-straight");
    const onLast = one(on([...quick(300), { made: 14, steps: [["Working", 0.2], ["Closed", 0.5]] }]), "svc.closed-straight");
    const after = one(on([...quick(300), { made: 13, steps: [["Working", 0.2], ["Closed", 0.5]] }]), "svc.closed-straight");
    assert.equal(onLast.facts!.n, base.facts!.n + 1);
    assert.equal(after.facts!.n, base.facts!.n);
  });

  test("the censoring rule: cases too recent for the lag are left out and said, never counted as cases that failed", () => {
    const base = one(on(richCases()), "svc.closed-straight");
    const recent = [...richCases(), ...Array.from({ length: 100 }, (_, i): CaseSpec => ({ made: 1 + (i % 10), steps: [["Closed", 0.1]] }))];
    const f = one(on(recent), "svc.closed-straight");
    assert.deepEqual(f.facts, base.facts);
    const tooRecent = (x: Finding): number => leading(x.evidence[1]!.exclusions.find((e) => /^\d+ cases made after .*, too recent to have had 14 days, the least time we allow for a case to close$/.test(e)) ?? "");
    assert.equal(tooRecent(f), tooRecent(base) + 100, f.evidence[1]!.exclusions.join(" | "));
  });

  test("the lag is a nearest rank of the closed cases' days, never under our minimum, and said as ours when it applies", () => {
    const m = metric(on(richCases()));
    assert.equal(m.status, "measured");
    if (m.status !== "measured") return;
    assert.equal(m.stats.lagDays, LEAST_LAG_DAYS);
    assert.ok(m.assumptions.some((a) => a.includes(`The period ends ${LEAST_LAG_DAYS} days before the run, our minimum`)), m.assumptions.join(" | "));
    // Slow closes: 25 cases at 50 days and 3 at 100 among 28 made; the lag is the 26th shortest, 100, not an interpolation.
    const slow = [ANCHOR, ...Array.from({ length: 28 }, (_, i): CaseSpec => ({ made: 120 + i, steps: [["Working", 0.5], ["Closed", i < 25 ? 50 : 100]] })), { made: 3, steps: [["Working", 0.5]] } as CaseSpec];
    const s = metric(on(slow));
    assert.equal(s.status === "measured" ? s.stats.lagDays : null, 100);
  });

  test("more than one in twenty of the period's cases still open is not measurable, and said", () => {
    const open = [...richCases(), ...Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 60 + i, steps: [["Working", 0.5]] }))];
    const f = one(on(open), "svc.closed-straight");
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /^Of the \d+ cases made between .* and .*, 30 are still open, too many to say how these cases end\.$/);
  });

  test("no Status change at all while cases closed: not measurable, with what to check, and never a fault", () => {
    const records = caseRecords(richCases().map((s) => ({ ...s, noHistory: true })));
    const a = analyseExtracts(withCases(extracts, records), { modules: SERVICE_CLOUD_MODULES, casePaths: true });
    for (const id of NEW_RULES) {
      const f = one(a, id);
      assert.equal(f.status, "not_measurable");
      assert.match(f.notMeasurableReason!, /^We couldn't follow your cases' statuses: we found no recorded change to a case's Status in the last year, though 301 cases were closed in it\. Either Salesforce isn't keeping a history of the Status field, or we can't see it\./);
    }
  });

  test("history that starts late: cases made before the day after the first change are left out, and said", () => {
    const specs = richCases().map((s) => ({ ...s, noHistory: s.made > 200 }));
    const f = one(on(specs), "svc.closed-straight");
    assert.ok(f.evidence[1]!.exclusions.some((e) => /cases made before .*, the day after the first status change we found, so their earlier statuses may not be recorded$/.test(e)), f.evidence[1]!.exclusions.join(" | "));
    assert.match(textOf(f), /^Of the \d+ cases made between (\d+) (\w+) 2026 and/);
  });

  test("an empty priority, origin or status is read as none, and no run stops on it", () => {
    const specs: CaseSpec[] = [...richCases().map((s, i) => (i % 7 === 0 ? { ...s, priority: "", origin: "" } : s)), { made: 4, steps: [], statusNow: "" }];
    const a = on(specs);
    for (const id of NEW_RULES) assert.ok(all(a).some((f) => f.ruleId === id), id);
    const f = one(a, "svc.closed-straight");
    assert.ok(f.segmentsChecked.length > 0);
  });
});

describe("the order of changes and what a change says", () => {
  // Each case moves New to Working at once, then half the cases go on to Waiting in the same second, and close at 3 days.
  const tied = (): CaseSpec[] => [ANCHOR, ...Array.from({ length: 300 }, (_, i): CaseSpec => ({ made: 40 + i, priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps: i % 2 ? [["Working", 0.5], ["Waiting", 0.5], ["Closed", 3]] : [["Working", 0.5], ["Closed", 3]] })), { made: 3, steps: [["Working", 0.5]] }];
  const history = (x: Map<string, unknown>) => (x.get(serviceActivityExtractor.id) as { statusChanges: { data: unknown[] } }).statusChanges.data;
  const analysed = (mutate: (x: Map<string, unknown>) => void) => {
    const x = withCases(extracts, caseRecords(tied()));
    mutate(x);
    const a = analyseExtracts(x, { modules: SERVICE_CLOUD_MODULES, casePaths: true });
    return JSON.stringify([all(a).filter((f) => NEW_RULES.includes(f.ruleId)).map((f) => [f.ruleId, f.status, f.summary ?? f.notMeasurableReason, f.facts]), metric(a)]);
  };

  test("changes in the same second are chained, never taken in the order the read returned them", () => {
    const m = metric(on(tied()));
    assert.ok(m.status === "measured");
    const paths = m.status === "measured" ? m.segments.filter((s) => s.dimension === "path").map((s) => s.key).sort() : [];
    assert.deepEqual(paths, ["New > Working > Closed", "New > Working > Waiting > Closed"]);
    assert.equal(analysed((x) => history(x).reverse()), analysed(() => {}));
  });

  test("the history read in any order gives the same results, to the byte", () => {
    const base = analysed(() => {});
    for (const seed of [1, 2, 3]) {
      const r = seededRandom(seed);
      assert.equal(analysed((x) => history(x).sort(() => r() - 0.5)), base, `shuffle ${seed}`);
    }
  });

  test("ties that can't be put in one order leave the case out, named", () => {
    // A to B and B to A in the same second: a cycle with no start.
    const specs: CaseSpec[] = [...quick(300), { made: 80, steps: [["Working", 1], ["New", 1], ["Closed", 3]] }];
    const f = one(on(specs), "svc.closed-straight");
    assert.ok(f.evidence[1]!.exclusions.includes("1 case with status changes in the same second that we couldn't put in one order"), f.evidence[1]!.exclusions.join(" | "));
  });

  test("a first change with no old value is the case being made: its new value is the status it started in", () => {
    const withMade = (x: Map<string, unknown>) => {
      const cases = x.get(serviceCasesExtractor.id) as { open: { data: Array<{ Id: string; CreatedDate: string }> }; closedCases: { data: Array<{ Id: string; CreatedDate: string }> } };
      for (const c of [...cases.open.data, ...cases.closedCases.data]) history(x).push({ CaseId: c.Id, OldValue: null, NewValue: "New", CreatedDate: c.CreatedDate });
    };
    const x = withCases(extracts, caseRecords(quick(300)));
    withMade(x);
    const f = all(analyseExtracts(x, { modules: SERVICE_CLOUD_MODULES, casePaths: true })).find((y) => y.ruleId === "svc.closed-straight")!;
    assert.equal(f.facts!.n, one(on(quick(300)), "svc.closed-straight").facts!.n);
  });
});

describe("open cases past their status's line, against earlier weeks (C1)", () => {
  test("a steady org: today is set beside a steady queue and its own earlier weeks, and nothing is raised", () => {
    const f = one(on(stuckCases()), "svc.stuck-in-status");
    assert.equal(f.status, "passed", textOf(f));
    assert.match(textOf(f), /^Today \d+ of \d+ open cases \([\d.]+%\) have been in their status longer than 75% of the stays that began there between 120 and 30 days earlier\. With your cases' own stay times, a steady queue would have about [\d.]+% past that point\. On the same day of the week in the \d+ earlier weeks we could compare, back to .*, the share was a median of [\d.]+%, and 90% of those weeks were at or below [\d.]+%\. Days are calendar days\. We raise this only when today's share is above [\d.]+% and at least \d+ points above the median of [\d.]+%, or at least \d+ points above the steady [\d.]+%, and it isn't\.$/);
    // The steady share, from the stays themselves, is close to what the steady org shows.
    assert.ok(Math.abs(f.facts!.steady! - f.facts!.rate!) < 5, JSON.stringify(f.facts));
  });

  test("a backlog building in the last month is raised, naming the status, its line rounded up, which comparison it exceeds, and the lines as ours", () => {
    const f = one(on(stuckCases({ backlog: true, waiting: true })), "svc.stuck-in-status");
    assert.equal(f.status, "issue", textOf(f));
    assert.match(textOf(f), / In Waiting on Customer, \d+ of \d+ open cases are past that status's line: 75% of the stays that began there between 120 and 30 days earlier ended within [\d.]+ days?\. That is more than both what was usual in earlier weeks and what a steady queue would have\. We raise this when today's share is above [\d.]+% and at least \d+ points above the median of [\d.]+%, or at least \d+ points above the steady [\d.]+%: these lines are ours\.$/);
  });

  test("a block of cases left in a status all year reads as usual against earlier weeks, and is raised against a steady queue", () => {
    const block = Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 300 + i, priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][i % 3]!, steps: [["Working", 0.04]] }));
    const f = one(on([...stuckCases(), ...block]), "svc.stuck-in-status");
    assert.equal(f.status, "issue", textOf(f));
    assert.match(textOf(f), / That is more than what a steady queue would have\./);
  });

  test("a stay not yet ended counts in the steady share at its length so far, never left out", () => {
    // Three two-day stays and one begun 63 days ago and still going: leaving that one out would say no case should be past the line.
    const snap = snapshotAt(withStays([[60, 2], [61, 2], [62, 2], [63, 200]], 0), NOW, 0, 3);
    assert.equal(snap.lines.get("Working"), 2);
    assert.ok(snap.expected.get("Working")! > 0.8, String(snap.expected.get("Working")));
  });

  test("the steady margin: 10 points, the z bar's points at today's count, or 30% of the steady share, whichever is most", () => {
    assert.equal(steadyMarginOf(10, 1000), 10);
    assert.equal(steadyMarginOf(20, 30), 22);
    assert.equal(steadyMarginOf(55, 160), 17);
  });

  test("the steady share is the length-biased share past the line: E[max(0, X - line)] / E[X]", () => {
    assert.equal(steadyShare([1, 2, 3, 6], 2), (0 + 0 + 1 + 4) / 12);
    assert.equal(steadyShare([], 1), 0);
  });

  test("the line uses only stays that began a gap before the day judged, so a recent backlog doesn't lift its own line", () => {
    // Three stays of two days begun two months ago, and three of fifty days begun in the last fortnight, inside the gap.
    const snap = snapshotAt([...withStays([[60, 2], [61, 2], [62, 2]], 0), ...withStays([[10, 50], [11, 50], [12, 50]], 0).map((x) => ({ ...x, id: `g${x.id}` }))], NOW, 0, 3);
    assert.equal(snap.lines.get("Working"), 2);
  });

  test("a stay not ended by the day judged counts as the longest: no line when too few have ended", () => {
    const snap = snapshotAt(withStays([[60, 200], [61, 200], [62, 2]], 0), NOW, 0, 3);
    assert.equal(snap.lines.has("Working"), false);
  });

  test("compared unrounded: a line of 0.54 days holds a case 0.52 days in, and the line is said rounded up, in hours under a day", () => {
    const cases = [...withStays([[60, 0.54], [61, 0.54], [62, 0.54]], 0), { id: "open", madeAt: NOW - 0.52 * DAY - 3_600_000, priority: "High", origin: "Email", visits: [{ status: NEW, start: NOW - 0.52 * DAY - 3_600_000, end: NOW - 0.52 * DAY }, { status: WORKING, start: NOW - 0.52 * DAY, end: null }] }];
    const snap = snapshotAt(cases, NOW, 0, 3);
    assert.equal(snap.lines.get("Working"), 0.54);
    assert.deepEqual(snap.targets.map((t) => t.past), [false]);
    assert.equal(lineSaid(0.54), "13 hours");
    assert.equal(lineSaid(0.03), "an hour");
    assert.equal(lineSaid(1.04), "1.1 days");
    assert.equal(lineSaid(1), "1 day");
  });

  test("an open case with no change of its own, made before the first change found, has been in its status since that change, not since it was made", () => {
    const from = NOW - 100 * DAY;
    const old = { id: "old", madeAt: NOW - 400 * DAY, priority: "High", origin: "Email", visits: [{ status: WORKING, start: NOW - 400 * DAY, end: null }] };
    const snap = snapshotAt([...withStays([[60, 2], [61, 2], [62, 2]], 0), old], NOW, from, 3);
    const t = snap.targets.find((x) => x.id === "old")!;
    assert.equal(t.since, from);
  });

  test("too few earlier weeks: the steady comparison alone, and said; too few open cases today: not measurable", () => {
    assert.match(textOf(one(on(stuckCases().filter((x) => x.made < 150)), "svc.stuck-in-status")), / No earlier week had 20 or more open cases with a line to compare with\. Days are calendar days\. We raise this only when today's share is at least \d+ points above the steady [\d.]+%, and it isn't\.$/);
    assert.match(textOf(one(on(stuckCases({ perDay: 2 })), "svc.stuck-in-status")), /^\d+ open cases? (is|are) in a status with a line today, below the calibrated minimum of 20\.$/);
  });

  test("the bar: above what 90% of the earlier weeks were at or below, and by a margin the noise probe set", () => {
    assert.deepEqual(STUCK_BAR, { quantile: 0.9, points: 10, z: 3, relative: 0.3, mode: "both" });
  });
});

describe("one of a thing, said in the singular", () => {
  test("one closed straight, one status compared, one open case today", () => {
    assert.match(textOf(one(on([...stuckCases({ perDay: 1 })]), "svc.stuck-in-status")), /^\d+ open cases? (is|are) in a status with a line today/);
    const straight = one(on([...quick(300), { made: 80, steps: [["Closed", 0.2]] }]), "svc.closed-straight");
    assert.match(textOf(straight), /, 1 \(0.3%\) was closed straight from the status they started in/);
    const compared = one(on(sentBackOrg(15, 20)), "svc.sent-back-cases");
    assert.match(textOf(compared), /^We compared 1 status that cases were sent back from with the cases that reached it without being sent back\./);
  });
});

describe("reopened cases (C2)", () => {
  test("raised when strictly more than half spent longer after their first close than before it, with medians and averages in calendar days", () => {
    const f = one(on(richCases()), "svc.reopened-time");
    assert.equal(f.status, "issue");
    assert.match(textOf(f), /^Of the 38 cases made between .* that were closed and then reopened, 38 spent longer from their first close to their final close, or to today, than they had taken to be closed the first time\. Before their first close they took a median of [\d.]+ calendar days( \(an average of [\d.]+\))?\. The 38 closed again took a median of [\d.]+ calendar days/);
  });

  test("equal time before and after a first close is not longer", () => {
    const equal: CaseSpec[] = [...quick(200), ...Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 80 + i, steps: [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 8]] }))];
    const f = one(on(equal), "svc.reopened-time");
    assert.equal(f.status, "passed", textOf(f));
    assert.equal(f.facts!.longer, 0);
  });

  test("a close date that is the first close, though a later close is recorded, makes the check say it can't tell", () => {
    const specs: CaseSpec[] = [...quick(200), ...Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 80 + i, steps: [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 10]] }))];
    const x = withCases(extracts, caseRecords(specs));
    const cases = (x.get(serviceCasesExtractor.id) as { closedCases: { data: Array<{ Id: string; CreatedDate: string; ClosedDate: string }> } }).closedCases.data;
    // Salesforce keeping the first close date on one reopened case.
    const keep = cases.find((c) => c.Id === caseRecords(specs).cases[202]!.Id)!;
    keep.ClosedDate = new Date(Date.parse(keep.CreatedDate.replace("+0000", "Z")) + 4 * DAY).toISOString().replace("Z", "+0000");
    const f = all(analyseExtracts(x, { modules: SERVICE_CLOUD_MODULES, casePaths: true })).find((y) => y.ruleId === "svc.reopened-time")!;
    assert.equal(f.status, "not_measurable");
    assert.equal(f.notMeasurableReason, "On 1 reopened case, the close date we read is its first close, though a later close is recorded, so we can't tell from the close date how long these cases took after their first close.");
  });

  test("exactly half is not most: a pass, never an issue", () => {
    const half: CaseSpec[] = [...quick(200), ...Array.from({ length: 40 }, (_, i): CaseSpec => ({ made: 80 + i, steps: i % 2 ? [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 10]] : [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 6]] }))];
    const f = one(on(half), "svc.reopened-time");
    assert.equal(f.status, "passed");
    assert.match(textOf(f), /, 20 spent longer .* which isn't more than half of them\./);
  });

  test("still open after a reopen goes each way: an answer the open cases could change is not given", () => {
    // 19 of 40 longer for certain; 2 reopened and still open could make it 21.
    const specs: CaseSpec[] = [
      ...quick(300),
      ...Array.from({ length: 19 }, (_, i): CaseSpec => ({ made: 80 + i, steps: [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 10]] })),
      ...Array.from({ length: 19 }, (_, i): CaseSpec => ({ made: 120 + i, steps: [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 6]] })),
      ...Array.from({ length: 2 }, (_, i): CaseSpec => ({ made: 40 + i, steps: [["Working", 0.5], ["Closed", 30], ["Working", 31]] })),
    ];
    const f = one(on(specs), "svc.reopened-time");
    assert.equal(f.status, "not_measurable");
    assert.match(f.notMeasurableReason!, /so far 19 of them spent longer after their first close than before it, and 2 still open could take that past half/);
  });

  test("none reopened, one reopened and too few reopened each say so in their own words", () => {
    assert.match(one(on(quick(300)), "svc.reopened-time").notMeasurableReason!, /^None of the 300 cases made between .* was closed and then reopened/);
    const oneCase = [...quick(300), { made: 90, steps: [["Working", 0.5], ["Closed", 4], ["Working", 5], ["Closed", 10]] } as CaseSpec];
    assert.match(one(on(oneCase), "svc.reopened-time").notMeasurableReason!, /^1 case made between .* was closed and then reopened, below the calibrated minimum of 20\.$/);
  });
});

describe("cases closed straight (C3)", () => {
  test("raised over the line, titled by it, the line said as ours, the highest segment named, and no cause claimed", () => {
    const specs: CaseSpec[] = [...quick(200), ...Array.from({ length: 100 }, (_, i): CaseSpec => ({ made: 50 + i, steps: [["Duplicate", 0.2]], origin: "Web" }))];
    const f = one(on(specs), "svc.closed-straight");
    assert.equal(f.status, "issue");
    assert.equal(f.title, "More than 25% of cases are closed straight from the status they started in");
    assert.match(textOf(f), /^Of the 300 cases made between .*, 100 \(33.3%\) were closed straight from the status they started in, with no other status between\. That is over our line of 25%\. Highest by (origin: Web|priority: Medium), at \d+(\.\d)?%\. Some may be duplicates, spam or cases closed by automation, and some may be questions answered at once: the statuses alone can't tell them apart\.$/);
  });

  test("raised by one segment alone: titled by that segment and its line, never \"many\"", () => {
    const specs: CaseSpec[] = [...quick(270), ...Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 50 + i, steps: [["Duplicate", 0.2]], origin: "Fax" }))];
    const f = one(on(specs), "svc.closed-straight");
    assert.equal(f.status, "issue");
    assert.equal(f.title, "More than 50% of cases with origin Fax are closed straight from the status they started in");
    assert.match(textOf(f), /The total is within our line of 25%, but by origin, Fax is at 100% \(30 of 30\), over our line of 50% for any one\./);
  });

  test("a segment is never named where every case shares it", () => {
    const specs: CaseSpec[] = [...quick(200), ...Array.from({ length: 100 }, (_, i): CaseSpec => ({ made: 50 + i, steps: [["Duplicate", 0.2]] }))].map((x) => ({ ...x, origin: "Email", priority: "High" }));
    const f = one(on(specs), "svc.closed-straight");
    assert.doesNotMatch(textOf(f), /by (origin|priority)/);
  });

  test("closed cases with no recorded status change aren't counted, and the sentence says how many", () => {
    const specs: CaseSpec[] = [...quick(300), ...Array.from({ length: 5 }, (_, i): CaseSpec => ({ made: 60 + i, start: "Closed", steps: [] }))];
    assert.match(textOf(one(on(specs), "svc.closed-straight")), / 5 cases closed with no recorded status change aren't counted\./);
  });

  test("a case not yet closed isn't counted as closed straight, and the sentence says how many", () => {
    const specs: CaseSpec[] = [...quick(300), { made: 80, steps: [] }];
    assert.match(textOf(one(on(specs), "svc.closed-straight")), / 1 case has never been closed, and isn't counted as closed straight\./);
  });
});

describe("the stored send-back insight (C4)", () => {
  test("raised only between cases that reached the same point, stored and never among the findings", () => {
    const a = on(sentBackOrg(20, 6));
    assert.ok(!a.findings.some((f) => f.ruleId === "svc.sent-back-cases"));
    const f = one(a, "svc.sent-back-cases");
    assert.equal(f.status, "issue");
    assert.equal(f.severity, "info");
    assert.match(textOf(f), /^Of the 140 cases made between .* that went from Working on to Waiting on Customer, the 60 sent back from Waiting on Customer to Working were closed within 14 days of being made 66.7% of the time, against 92.5% of the 80 that weren't\./);
    assert.match(textOf(f), /not that one is the reason for the other\.$/);
  });

  test("never against survivors: cases that never reached the later status don't make a gap", () => {
    // Same share slow on both sides; 250 quick cases that never waited are on neither side.
    const f = one(on(sentBackOrg(15, 20)), "svc.sent-back-cases");
    assert.notEqual(f.status, "issue", textOf(f));
  });

  test("the bar counts every comparison made: two send-backs tested, the bar is set for two", () => {
    const single = all(on(sentBackOrg(20, 6))).filter((f) => f.ruleId === "svc.sent-back-cases");
    assert.deepEqual(single.map((f) => [f.facts!.tested, f.facts!.bar]), [[1, zBar(1)]]);
    // A second send-back, from Waiting to New, with as many cases on each side.
    const toNew = Array.from({ length: 60 }, (_, i): CaseSpec => ({ made: 72 + ((i * 5) % 270), priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps: [["Working", 0.5], ["Waiting", 2], ["New", 3], ["Closed", i % 4 === 0 ? 60 : 6]] }));
    const two = all(on([...sentBackOrg(20, 6), ...toNew])).filter((f) => f.ruleId === "svc.sent-back-cases");
    assert.ok(two.length > 0);
    for (const f of two) assert.deepEqual([f.facts!.tested, f.facts!.bar], [2, zBar(2)], textOf(f));
    assert.ok(zBar(2) > zBar(1));
  });
});

describe("the stored metric", () => {
  test("counts and days only, the five most common paths and every other path in one row", () => {
    const m = metric(on(richCases()));
    assert.equal(m.status, "measured");
    if (m.status !== "measured") return;
    assert.equal(m.n, m.stats.closed);
    const paths = m.segments.filter((s) => s.dimension === "path");
    assert.ok(paths.length <= 6);
    assert.equal(paths.reduce((t, s) => t + s.n, 0), m.n);
    assert.equal(paths[0]!.key, "New > Working > Closed");
  });
});

describe("the org's words", () => {
  const HOSTILE: CaseStatus[] = [
    { ApiName: "New", MasterLabel: "New", IsClosed: false },
    { ApiName: "Working_9", MasterLabel: "Stage 9 of 12, 50% done", IsClosed: false },
    { ApiName: "Waiting", MasterLabel: "Closed", IsClosed: false },
    { ApiName: "Done", MasterLabel: "ask jane.doe@example.com <b>now</b>", IsClosed: true },
    { ApiName: "Dup", MasterLabel: "500000000000001AAA", IsClosed: true },
    { ApiName: "Long", MasterLabel: "Mary Smith ".repeat(27).trim(), IsClosed: false },
  ];
  const hostileSpecs = (): CaseSpec[] =>
    Array.from({ length: 300 }, (_, i): CaseSpec => {
      const p = ["High", "Medium", "Low"][i % 3]!;
      const steps: Array<[string, number]> =
        i % 5 === 0 ? [["Dup", 0.3]] : i % 4 === 1 ? [["Working_9", 0.5], ["Long", 1], ["Waiting", 2], ["Done", 3]] : i % 7 === 2 ? [["Working_9", 0.5], ["Done", 2], ["Waiting", 3], ["Done", 9]] : [["Working_9", 0.5], ["Waiting", 1], ["Done", 2]];
      return { made: 40 + i, priority: p, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps };
    }).concat(Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 1 + (i % 10), steps: [["Working_9", 0.2], ["Waiting", 0.4]] })));

  test("every status in a sentence is marked as the org's, whatever it holds", () => {
    // With a backlog waiting in the status labelled "Closed", so the stuck check names it.
    const backlog = stuckCases({ backlog: true, waiting: true }).map((x) => ({ ...x, steps: x.steps.map(([t, d]): [string, number] => [t === "Working" ? "Working_9" : t === "Closed" ? "Done" : t, d]) }));
    const a = on([...hostileSpecs(), ...backlog], { statuses: HOSTILE });
    let seen = 0;
    for (const f of all(a).filter((x) => NEW_RULES.includes(x.ruleId))) {
      for (const field of ["title", "summary", "recommendation", "notMeasurableReason"] as const) {
        const text = f[field];
        if (typeof text !== "string") continue;
        const marked = f.orgWords?.[field];
        assert.ok(marked, `${f.ruleId} ${field} carries its marks`);
        for (const s of HOSTILE.filter((h) => h.ApiName !== "New").map((h) => h.MasterLabel!)) {
          let at = text.indexOf(s);
          while (at >= 0) {
            seen++;
            assert.ok(marked.spans.some((sp) => sp.start <= at && sp.end >= at + s.length), `${f.ruleId}: "${s}" unmarked in ${text}`);
            at = text.indexOf(s, at + 1);
          }
        }
      }
    }
    // The test reads something: hostile names are in the sentences.
    assert.ok(seen > 0);
  });

  test("each hostile name, as the status the stuck check names, is in its sentence and marked whole, or left out where it would read as a person", () => {
    const NAMES = ["Stage 9 of 12, 50% done", "Closed", "<script>x</script> 100%", "ask jane.doe@example.com <b>now</b>", "500000000000001", "500000000000001AAA", "Mary Smith", "Mary Smith ".repeat(27).trim()];
    const ALWAYS_SAID = new Set(["Stage 9 of 12, 50% done", "Closed", "<script>x</script> 100%"]);
    const NEVER_SAID = new Set(["ask jane.doe@example.com <b>now</b>", "500000000000001", "500000000000001AAA"]);
    for (const name of NAMES) {
      const statuses = CASE_PATH_STATUSES.map((st) => (st.ApiName === "Waiting" ? { ...st, MasterLabel: name } : st));
      const f = one(on(stuckCases({ backlog: true, waiting: true }), { statuses }), "svc.stuck-in-status");
      const text = f.summary!;
      assert.equal(f.status, "issue", name);
      assert.doesNotMatch(text, /\[\[/, `${name}: never a token`);
      if (!text.includes(name)) {
        assert.ok(!ALWAYS_SAID.has(name), `${name} should be said: ${text}`);
        assert.doesNotMatch(text, / In .*, \d+ of \d+ open cases are past that status's line/, `${name}: the status sentence is left out`);
        continue;
      }
      assert.ok(!NEVER_SAID.has(name), `${name} must never be written: ${text}`);
      const at = text.indexOf(`In ${name}, `) + 3;
      assert.ok(at > 3, `${name}: ${text}`);
      assert.ok(f.orgWords!.summary!.spans.some((sp) => sp.start === at && sp.end === at + name.length), `${name} marked whole: ${text}`);
    }
  });

  test("a status the org labels \"Closed\" but which isn't closed is read by its flag, never by its name", () => {
    const a = on(hostileSpecs(), { statuses: HOSTILE });
    const f = one(a, "svc.closed-straight");
    assert.notEqual(f.status, "not_measurable", textOf(f));
  });
});

describe("scoring", () => {
  test("a new check not measurable never takes away a score the area has without it", () => {
    const off = analyseExtracts(extracts, { modules: ALL_MODULES }).findings;
    // Half the area's checks unmeasured: scored at exactly half. Three more unmeasured checks would take that away.
    const ops = [...new Set(off.filter((f) => f.domain === "service_operations").map((f) => f.ruleId))];
    const half = off.map((f) => (f.domain === "service_operations" && ops.indexOf(f.ruleId) < ops.length / 2 ? { ...f, status: "not_measurable" as const, notMeasurableReason: "x" } : f));
    const base = scoreDomains(["service_operations"], half);
    assert.equal(base[0]!.status, "measured");
    const added = [...half, ...["svc.stuck-in-status", "svc.reopened-time", "svc.closed-straight"].map((ruleId) => ({ ...half.find((f) => f.domain === "service_operations")!, id: `x-${ruleId}`, ruleId, status: "not_measurable" as const, notMeasurableReason: "We couldn't follow your cases' statuses." }))];
    assert.deepEqual(scoreDomains(["service_operations"], added as Finding[]), base);
  });

  test("the synthetic org's service operations score with the switch on, and the overall, are said in decision 776", () => {
    const a = analyseExtracts(extracts, { modules: ALL_MODULES, casePaths: true });
    const off = analyseExtracts(extracts, { modules: ALL_MODULES });
    const [s, o] = [scoreDomains(["service_operations"], a.findings)[0]!, scoreDomains(["service_operations"], off.findings)[0]!];
    assert.equal(s.status, "measured");
    assert.equal(o.status, "measured");
    assert.ok(s.status === "measured" && o.status === "measured" && s.score <= o.score);
    assert.equal(loadCalibration().minSample, 20);
    assert.ok(a.findings.some((f) => f.ruleId === "svc.stuck-in-status" && f.status === "issue"));
    const cases = (extracts.get(serviceCasesExtractor.id) as { open: { data: unknown[] } }).open.data.length;
    assert.ok(cases > 0);
  });
});
