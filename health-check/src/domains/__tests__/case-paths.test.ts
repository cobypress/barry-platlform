import { test, describe, before } from "node:test";
import assert from "node:assert/strict";
import { analyseExtracts, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { ANCHOR, CASE_PATH_STATUSES, caseRecords, richCases, withCases, type CaseSpec, type CaseStatus } from "../../testing/case-paths-org.ts";
import { orgExtractor } from "../../extract/configuration.ts";
import { serviceCasesExtractor } from "../../extract/service.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { COUNTED_WHEN_MEASURED, INSIGHT_METRICS, INSIGHT_RULES, scoreDomains } from "../scoring.ts";
import { ALL_MODULES, SERVICE_CLOUD_MODULES } from "../index.ts";
import { casePathsModule, withCasePaths } from "../service/index.ts";
import { serviceOperationsModule } from "../service/operations.ts";
import { LEAST_LAG_DAYS } from "../service/paths.ts";
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
    const specs: CaseSpec[] = [...richCases(), { made: 5, steps: [["Escalated", 1]] }, { made: 6, steps: [["Working", 0.5]], statusNow: "Waiting" }];
    const f = one(on(specs), "svc.stuck-in-status");
    const open = f.evidence.at(-1)!;
    assert.equal(open.count! + open.exclusions.reduce((t, e) => t + leading(e), 0), caseRecords(specs).cases.filter((c) => !c.IsClosed).length, open.exclusions.join(" | "));
    assert.ok(open.exclusions.includes("2 open cases whose status we couldn't place: not in your case status settings, a closed status, or not where their recorded changes end"), open.exclusions.join(" | "));
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

describe("open cases stuck in a status (C1)", () => {
  test("raised from the stays of the period's cases, with the stays not yet ended counted as the longest, and the share strictly over the line", () => {
    const f = one(on(richCases()), "svc.stuck-in-status");
    assert.equal(f.status, "issue");
    assert.equal(f.facts!.n, 40);
    assert.match(textOf(f), /^We set each open case beside the cases made between .*\. \d+ of 40 open cases \(\d+(\.\d)?%\) have been in their current status longer than 75% of those cases' stays in it lasted\. Days are calendar days\./);
  });

  test("a stay not yet ended counts as the longest: an open case in a status no case has left sets no line", () => {
    // Every case made in the period that reached Waiting is still there, so no line can be set for Waiting.
    // More than the minimum of stays there, none ended: counted as the longest, so no line, and the open cases in it are left out.
    const specs: CaseSpec[] = [...quick(300), ...Array.from({ length: 25 }, (_, i): CaseSpec => ({ made: 60 + i, steps: [["Working", 0.5], ["Waiting", 1]] })), { made: 3, steps: [["Working", 0.5], ["Waiting", 1]] }];
    const f = one(on(specs), "svc.stuck-in-status");
    const open = f.evidence.at(-1)!;
    assert.ok(open.exclusions.some((e) => /^26 open cases in a status too few cases made .* spent time in, or left, to set a line$/.test(e)), open.exclusions.join(" | "));
  });

  test("not measurable reads the period's minimum, and a pass says no segment is over the line", () => {
    const calm = richCases().filter((s) => s.made > 12);
    const f = one(on([...calm, { made: 2, steps: [["Working", 0.5]] }]), "svc.stuck-in-status");
    assert.equal(f.status, "passed");
    assert.match(textOf(f), /\. 0 of 1 open case \(0%\) have been in .* Days are calendar days\. No segment is above 60%\.$/);
  });
});

describe("one of a thing, said in the singular", () => {
  test("one open case past the line, one closed straight, one status compared", () => {
    const stuck = one(on([...quickClosed(300), ANCHOR, { made: 3, steps: [["Working", 0.25]], at: "00:00" }]), "svc.stuck-in-status");
    assert.match(textOf(stuck), /\. 1 of 1 open case \(100%\) has been in its current status/);
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
  test("raised over the line, with the worst segment by starting status, priority or origin, and no cause claimed", () => {
    const specs: CaseSpec[] = [...quick(200), ...Array.from({ length: 100 }, (_, i): CaseSpec => ({ made: 50 + i, steps: [["Duplicate", 0.2]], origin: "Web" }))];
    const f = one(on(specs), "svc.closed-straight");
    assert.equal(f.status, "issue");
    assert.match(textOf(f), /^Of the 300 cases made between .*, 100 \(33.3%\) were closed straight from the status they started in, with no other status between\. Worst by origin: Web, at \d+(\.\d)?%\. Some may be duplicates, spam or cases closed by automation, and some may be questions answered at once: the statuses alone can't tell them apart\.$/);
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
    const a = on(hostileSpecs(), { statuses: HOSTILE });
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

  test("each hostile name, as the status the stuck check names, is in its sentence and marked whole", () => {
    const waiting = Array.from({ length: 30 }, (_, i): CaseSpec => ({ made: 30 + i, priority: ["High", "Medium", "Low"][i % 3]!, origin: ["Email", "Web", "Phone"][Math.floor(i / 3) % 3]!, steps: [["Working", 0.25], ["Waiting", 0.5]] }));
    const NAMES = ["Stage 9 of 12, 50% done", "Closed", "ask jane.doe@example.com <b>now</b>", "500000000000001", "500000000000001AAA", "Mary Smith", "Mary Smith ".repeat(27).trim(), "<script>x</script> 100%"];
    for (const name of NAMES) {
      const statuses = CASE_PATH_STATUSES.map((st) => (st.ApiName === "Waiting" ? { ...st, MasterLabel: name } : st));
      const f = one(on([...richCases(), ...waiting], { statuses }), "svc.stuck-in-status");
      const text = f.summary!;
      if (!text.includes(name)) {
        // The privacy scrub took part of it out (an email, an id): the sentence then loses its marks, so it can never be served as ours.
        assert.match(text, /\[\[(email|id|[a-z]+)[^\]]*\]\]/, `${name}: ${text}`);
        assert.equal(f.orgWords?.summary, undefined, `${name}: a scrubbed sentence keeps no marks`);
        continue;
      }
      const at = text.indexOf(`In ${name}, `) >= 0 ? text.indexOf(`In ${name}, `) + 3 : text.indexOf(`stays in ${name} ended`) + 9;
      assert.ok(at > 8, `${name}: ${text}`);
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
