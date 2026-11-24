import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { HistoryRow, OpportunityExtract, OpportunityRow } from "../../extract/opportunities.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import type { Deal } from "../historic-pipeline/cohort.ts";
import { historicPipelineModule } from "../historic-pipeline/index.ts";
import { DAYS_GAP_MIN, DAYS_GAP_SHARE, WIN_GAP } from "../historic-pipeline/paths/insights.ts";
import { hasRework, openPathOf, OTHER_PATHS, PATH_VARIANTS, sendBacks, skipsOf, standardPathOf, TOP_VARIANTS, variantsOf, type PathDeal } from "../historic-pipeline/paths/variants.ts";

// Path variants and the insights read from them (DECISIONS.md 703), on deal
// histories built by hand, then on the synthetic org.

const { dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration: Calibration = { ...loadCalibration(), minSample: 5 };
const RULE = "pipe.path-insights";
const DAY = 86_400_000;
type Measured = Extract<Metric, { status: "measured" }>;

// ─── Hand-built deals ────────────────────────────────────────────────────────

type Outcome = "won" | "lost" | "no_decision";
/** `segment` is set as the deal's type, lead source and record type alike, so three of the dimensions split on it. */
type Spec = { path: string[]; outcome: Outcome; days: number; segment?: string };
const CLOSED: Record<Outcome, string> = { won: "Closed Won", lost: "Closed Lost", no_decision: "Closed No Decision" };
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");

/** `count` deals along `path`, `won` of them won and the rest lost, each `days` from creation to close. */
const deals = (count: number, path: string[], won: number, days: number, segment?: string): Spec[] =>
  Array.from({ length: count }, (_, i) => ({ path, outcome: i < won ? "won" : "lost", days, ...(segment ? { segment } : {}) }));

/** The recorded extracts with every opportunity and history row replaced by these deals. */
function extractsOf(specs: Spec[], history: "ok" | "none" | "unavailable" = "ok"): Map<string, unknown> {
  const changed = clone(extracts);
  const e = changed.get("opportunities") as OpportunityExtract;
  const owner = e.opportunities.status === "ok" ? e.opportunities.data[0]!.OwnerId : "005000000000000001";
  const opportunities: OpportunityRow[] = [];
  const rows: HistoryRow[] = [];
  specs.forEach((s, i) => {
    const id = `006${String(900_000 + i).padStart(15, "0")}`;
    // Created a few hours apart in April 2025, so every close falls in the window and, for the shorter deals, one quarter.
    const created = Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const closed = created + s.days * DAY;
    const closeDate = new Date(closed).toISOString().slice(0, 10);
    opportunities.push({ Id: id, OwnerId: owner, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: true, IsWon: s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
    s.path.forEach((stage, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created + k * 60_000) }));
    rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(closed) });
  });
  if (e.opportunities.status === "ok") e.opportunities = { ...e.opportunities, data: opportunities };
  if (e.history.status === "ok") {
    if (history === "ok") e.history = { ...e.history, data: rows };
    if (history === "none") e.history = { ...e.history, data: [] };
    if (history === "unavailable") e.history = { status: "unavailable", reason: "the read was refused", source: e.history.source } as OpportunityExtract["history"];
  }
  return changed;
}

function analysed(specs: Spec[], options: { history?: "ok" | "none" | "unavailable"; calibration?: Calibration } = {}): { findings: Finding[]; metric: Metric } {
  const output = analyseExtracts(extractsOf(specs, options.history), { calibration: options.calibration ?? calibration, modules: [historicPipelineModule] });
  return { findings: output.findings.filter((f) => f.ruleId === RULE), metric: output.metrics.find((m) => m.id === PATH_VARIANTS)! };
}

const issues = (findings: Finding[]) => findings.filter((f) => f.status === "issue");
/** The skip and rework insights, leaving out where deals die, which the usual path's own lost deals can raise. */
const isDeadEnd = (f: Finding): boolean => f.facts.dying !== undefined;
const comparisons = (findings: Finding[]) => issues(findings).filter((f) => !isDeadEnd(f));

const USUAL = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
const SKIPS_DISCOVERY = ["Prospecting", "Proposal", "Negotiation"];
const REWORK = ["Prospecting", "Discovery", "Prospecting", "Discovery", "Proposal", "Negotiation"];

/** A PathDeal for the pure functions: only what they read. */
const pd = (path: string[], won: boolean, days: number, i = 0): PathDeal => ({ id: `d${i}-${path.join("/")}-${won}-${days}`, deal: {} as Deal, path, won, days });

// ─── Paths ───────────────────────────────────────────────────────────────────

describe("a deal's path", () => {
  const categories = new Map(loadCalibration().stages.map((s) => [s.name, s.category]));
  const stages = { categoryOf: (s: string) => categories.get(s) ?? null };
  const deal = (names: string[]): Deal => ({ history: names.map((StageName, i) => ({ Id: `h${i}`, OpportunityId: "o", StageName, Amount: null, CloseDate: "2025-01-01", CreatedDate: `2025-01-0${i + 1}` })) }) as unknown as Deal;

  test("collapses a run of rows in the same stage, and leaves closed stages out", () => {
    assert.deepEqual(openPathOf(deal(["Prospecting", "Prospecting", "Discovery", "Discovery", "Discovery", "Proposal", "Closed Won"]), stages), ["Prospecting", "Discovery", "Proposal"]);
  });

  test("keeps a return to an earlier stage, which is rework", () => {
    const path = openPathOf(deal(["Prospecting", "Discovery", "Discovery", "Prospecting", "Discovery", "Closed Lost"]), stages);
    assert.deepEqual(path, ["Prospecting", "Discovery", "Prospecting", "Discovery"]);
    assert.ok(hasRework(path));
    // Back from Discovery to Prospecting once; going on to Discovery again is a move forward.
    assert.deepEqual(sendBacks(path), [{ from: "Discovery", to: "Prospecting" }]);
    assert.ok(!hasRework(["Prospecting", "Discovery", "Proposal"]));
    assert.deepEqual(sendBacks(["Prospecting", "Discovery", "Proposal", "Discovery", "Prospecting"]), [{ from: "Proposal", to: "Discovery" }, { from: "Discovery", to: "Prospecting" }]);
  });

  test("a skip is a stage of the usual path moved past without entering it, never one a deal closed before reaching", () => {
    assert.deepEqual(skipsOf(SKIPS_DISCOVERY, USUAL), ["Discovery"]);
    assert.deepEqual(skipsOf(["Prospecting", "Negotiation"], USUAL), ["Discovery", "Proposal"]);
    assert.deepEqual(skipsOf(["Prospecting"], USUAL), []);
    assert.deepEqual(skipsOf(["Prospecting", "Discovery"], USUAL), []);
    assert.deepEqual(skipsOf(USUAL, USUAL), []);
  });
});

describe("the usual path and the variants", () => {
  test("the usual path is the one won deals took most often, not the one most deals took", () => {
    const closed = [...Array.from({ length: 9 }, (_, i) => pd(["Prospecting"], false, 10, i)), ...Array.from({ length: 4 }, (_, i) => pd(SKIPS_DISCOVERY, true, 20, i)), ...Array.from({ length: 3 }, (_, i) => pd(USUAL, true, 30, i))];
    assert.deepEqual(standardPathOf(closed), SKIPS_DISCOVERY);
  });

  test("a tie in won deals goes to the path more closed deals took, then to the first by name", () => {
    const tied = [pd(USUAL, true, 1, 1), pd(USUAL, true, 1, 2), pd(SKIPS_DISCOVERY, true, 1, 3), pd(SKIPS_DISCOVERY, true, 1, 4), pd(SKIPS_DISCOVERY, false, 1, 5)];
    assert.deepEqual(standardPathOf(tied), SKIPS_DISCOVERY);
    const even = [pd(USUAL, true, 1, 1), pd(SKIPS_DISCOVERY, true, 1, 2)];
    // JSON of ["Prospecting","Discovery",...] sorts before ["Prospecting","Proposal",...].
    assert.deepEqual(standardPathOf(even), USUAL);
    assert.deepEqual(standardPathOf([...even].reverse()), USUAL);
    assert.equal(standardPathOf([pd(USUAL, false, 1)]), null);
  });

  test("the top variants and one row for all other paths, the deals adding up to every closed deal", () => {
    const paths = [USUAL, SKIPS_DISCOVERY, REWORK, ["Prospecting"], ["Prospecting", "Discovery"], ["Prospecting", "Discovery", "Proposal"], ["Prospecting", "Negotiation"], ["Discovery"]];
    const closed = paths.flatMap((path, k) => Array.from({ length: 10 - k }, (_, i) => pd(path, k === 0 && i < 6, 10 + i, i)));
    const v = variantsOf(closed, USUAL);
    assert.equal(v.shown.length, TOP_VARIANTS);
    assert.deepEqual(v.shown.map((x) => x.deals.length), [10, 9, 8, 7, 6]);
    assert.equal(v.other!.deals.length, 5 + 4 + 3);
    assert.equal(v.other!.paths, 3);
    assert.equal(v.shown.reduce((n, x) => n + x.deals.length, 0) + v.other!.deals.length, closed.length);
    const shares = v.shown.reduce((n, x) => n + x.share, 0) + v.other!.share;
    assert.ok(Math.abs(shares - 100) <= 0.05 * (v.shown.length + 1), `shares add to ${shares}`);
    assert.deepEqual(v.shown[1]!.skips, ["Discovery"]);
    assert.equal(v.shown[2]!.rework, true);
    assert.equal(v.shown[0]!.standard, true);
  });

  test("the usual path is always listed, as an extra row when it isn't among the most common", () => {
    const closed = [...[["A"], ["B"], ["C"], ["D"], ["E"]].flatMap((path, k) => Array.from({ length: 5 }, (_, i) => pd(path, false, 1, k * 10 + i))), pd(USUAL, true, 1, 90), pd(USUAL, true, 1, 91)];
    const v = variantsOf(closed, USUAL);
    assert.equal(v.shown.length, TOP_VARIANTS + 1);
    assert.ok(v.shown.at(-1)!.standard);
    assert.equal(v.other, null);
  });

  test("a variant's win rate is over its own deals only, and its time is the median, not the mean", () => {
    const closed = [pd(USUAL, true, 1, 1), pd(USUAL, true, 2, 2), pd(USUAL, false, 3, 3), pd(USUAL, false, 100, 4), ...Array.from({ length: 6 }, (_, i) => pd(SKIPS_DISCOVERY, false, 5, 10 + i))];
    const usual = variantsOf(closed, USUAL).shown.find((x) => x.standard)!;
    assert.equal(usual.winRate, 50);
    assert.equal(usual.medianDays, 2.5);
  });
});

// ─── Insights ────────────────────────────────────────────────────────────────

describe("the insights, raised above the bar and not below it", () => {
  // The usual path: 20 deals, 12 won (60%), 60 days each.
  const usual = deals(20, USUAL, 12, 60);

  test("deals that skip a stage and win more often and sooner are said to, with the basis", () => {
    const { findings, metric } = analysed([...usual, ...deals(10, SKIPS_DISCOVERY, 9, 30)]);
    const [f, ...rest] = comparisons(findings);
    assert.equal(rest.length, 0, rest.map((x) => x.title).join("; "));
    assert.deepEqual(f!.subjects, [{ type: "stage", label: "Discovery" }]);
    assert.equal(f!.title, "Deals that skip Discovery are won more often and close sooner than on your usual path");
    assert.match(f!.summary, /^Deals that skip Discovery are won 90% of the time, against 60% on your usual path, and close in a median of 30 days from creation, against 60 days\. Based on 10 closed deals that skipped it and 20 that took your usual path, of 30 closed deals\./);
    assert.match(f!.summary, /not that one is the reason for the other\.$/);
    assert.doesNotMatch(`${f!.title} ${f!.summary} ${f!.recommendation}`, /because|caus/i);
    assert.equal(f!.severity, "low");
    assert.equal(f!.facts.winGap, 30);
    assert.equal(metric.status, "measured");
  });

  test("the reverse: a skip that wins less often is said the other way round", () => {
    const [f] = comparisons(analysed([...usual, ...deals(10, SKIPS_DISCOVERY, 2, 61)]).findings);
    assert.equal(f!.title, "Deals that skip Discovery are won less often than on your usual path");
    assert.equal(f!.severity, "medium");
  });

  test("below the sample floor on one side nothing is raised, and the variant table still shows the figures", () => {
    const { findings, metric } = analysed([...usual, ...deals(4, SKIPS_DISCOVERY, 4, 10)]);
    assert.equal(comparisons(findings).length, 0);
    const m = metric as Measured;
    const row = (dimension: string) => m.segments.find((s) => s.dimension === dimension && s.key === SKIPS_DISCOVERY.join(" > "))!;
    assert.equal(row("path").n, 4);
    assert.equal(row("path win rate").value, 100);
    assert.equal(row("path median days").value, 10);
  });

  test("below the difference bar nothing is raised", () => {
    // 6 of 10 won is 60%, the usual path's rate, and 50 days is 10 days and 17% under its 60.
    // 9 of 10 won (90%) against 17 of 20 (85%), and 50 days is 10 days and 17% under 60. Only 4 deals are
    // lost, too few to say where deals die, so the rule passes.
    const { findings } = analysed([...deals(20, USUAL, 17, 60), ...deals(10, SKIPS_DISCOVERY, 9, 50)]);
    assert.equal(findings.length, 1);
    assert.equal(findings[0]!.status, "passed");
    assert.match(findings[0]!.summary, new RegExp(`by ${WIN_GAP} points of win rate, or by ${DAYS_GAP_SHARE}% and ${DAYS_GAP_MIN} days of its median time to close`));
    // At each bar is raised: 10 points of win rate, or 15 days (25% of 60).
    assert.equal(comparisons(analysed([...usual, ...deals(10, SKIPS_DISCOVERY, 7, 60)]).findings).length, 1);
    assert.equal(comparisons(analysed([...usual, ...deals(10, SKIPS_DISCOVERY, 6, 45)]).findings).length, 1);
    assert.equal(comparisons(analysed([...usual, ...deals(10, SKIPS_DISCOVERY, 6, 46)]).findings).length, 0);
  });

  test("a gap that turns round in most of the segments that can check it isn't raised", () => {
    // Overall the skippers win more (11 of 17, 64.7%, against 16 of 30, 53.3%), but in each segment they win
    // less: New 11 of 12 against 14 of 15, Renewal 0 of 5 against 2 of 15. Type, lead source and record type
    // split on it, six segments turned round, against three dimensions with a single segment, which are the whole.
    const mixed = [...deals(15, USUAL, 14, 60, "New"), ...deals(15, USUAL, 2, 60, "Renewal"), ...deals(12, SKIPS_DISCOVERY, 11, 60, "New"), ...deals(5, SKIPS_DISCOVERY, 0, 60, "Renewal")];
    assert.equal(comparisons(analysed(mixed).findings).length, 0);
    // With the segments agreeing (New 10 of 10 against 12 of 15, Renewal 1 of 5 against 1 of 10), a gap is raised.
    const agreeing = [...deals(15, USUAL, 12, 60, "New"), ...deals(10, USUAL, 1, 60, "Renewal"), ...deals(10, SKIPS_DISCOVERY, 10, 60, "New"), ...deals(5, SKIPS_DISCOVERY, 1, 60, "Renewal")];
    const [f] = comparisons(analysed(agreeing).findings);
    assert.match(f!.summary, /The gap points the same way in 9 of 9 segments, such as teams and record types, with enough deals on both sides\./);
  });

  test("rework: deals sent back a stage are said to take longer and be won less often", () => {
    const [f, ...rest] = comparisons(analysed([...usual, ...deals(10, REWORK, 2, 90)]).findings);
    assert.equal(rest.length, 0, rest.map((x) => x.title).join("; "));
    assert.deepEqual(f!.subjects.map((x) => x.label), ["Discovery", "Prospecting"]);
    assert.equal(f!.title, "Deals sent back from Discovery to Prospecting are won less often and take longer to close");
    assert.match(f!.summary, /^Deals sent back from Discovery to Prospecting are won 20% of the time, against 60% on your usual path, and close in a median of 90 days from creation, against 60 days\. Based on 10 closed deals that went back this way and 20 that took your usual path, of 30 closed deals\./);
    assert.equal(f!.severity, "medium");
  });

  test("rework below the sample floor isn't raised", () => {
    assert.equal(comparisons(analysed([...usual, ...deals(4, REWORK, 0, 90)]).findings).length, 0);
  });

  test("where deals die: the stage after which the largest share of lost deals were lost", () => {
    const lost = [...deals(6, ["Prospecting"], 0, 20), ...deals(3, ["Prospecting", "Discovery"], 0, 30)];
    const { findings } = analysed([...usual, ...lost]);
    const f = issues(findings).find(isDeadEnd)!;
    assert.equal(f.title, "More deals are lost after Negotiation than after any other stage");
    // 8 of the usual path's deals were lost after Negotiation, 6 after Prospecting and 3 after Discovery.
    assert.equal(f.summary, "8 of the 17 lost and no decision deals (47.1%) were last open in Negotiation, after a median of 60 days from creation, against 60 days for a won deal to close. Based on 17 of 29 closed deals.");
  });

  test("no stage holds a third of lost deals, or too few lost deals: not raised", () => {
    const spread = [...deals(5, ["Prospecting"], 0, 20), ...deals(5, ["Prospecting", "Discovery"], 0, 20), ...deals(5, ["Prospecting", "Discovery", "Proposal"], 0, 20)];
    assert.ok(!issues(analysed([...deals(20, USUAL, 17, 60), ...spread]).findings).some(isDeadEnd));
    assert.ok(!issues(analysed([...deals(20, USUAL, 16, 60)]).findings).some(isDeadEnd));
  });
});

// ─── Not measurable ──────────────────────────────────────────────────────────

describe("not measurable, with the plain reason, sorted into its kind", () => {
  const usual = deals(20, USUAL, 12, 60);
  const check = (result: { findings: Finding[]; metric: Metric }, reason: RegExp, kind: string) => {
    const [f] = result.findings;
    assert.equal(f!.status, "not_measurable");
    assert.match(f!.notMeasurableReason!, reason);
    assert.equal(notMeasurableKind(f!.notMeasurableReason!), kind);
    assert.equal(result.metric.status, "not_measurable");
    assert.equal((result.metric as Extract<Metric, { status: "not_measurable" }>).reason, f!.notMeasurableReason);
  };

  test("stage history couldn't be read", () => check(analysed(usual, { history: "unavailable" }), /^OpportunityHistory couldn't be read: /, "unreadable"));
  test("stage history isn't tracked", () => check(analysed(usual, { history: "none" }), /^Stage history isn't tracked for these deals: none of the 20 deals that closed in the window has a recorded open stage\.$/, "too_little_data"));
  test("too few closed deals", () => check(analysed(usual.slice(0, 4)), /^4 closed deals with stage history is below the calibrated minimum of 5\.$/, "too_little_data"));
  test("too few won deals", () => check(analysed(deals(20, USUAL, 4, 60)), /^4 won deals with stage history is below the calibrated minimum of 5\.$/, "too_little_data"));
  test("every deal took a different path", () => {
    const stages = ["Prospecting", "Qualified", "Discovery", "Proposal", "Negotiation", "Legal Review"];
    const unique = stages.map((s, i) => ({ path: [s], outcome: "won" as const, days: 10 })).concat(stages.slice(1).map((s) => ({ path: ["Prospecting", s], outcome: "won" as const, days: 10 })));
    check(analysed(unique), /^Each of the 11 closed deals took a different path through your stages, so no path is common enough to compare with\.$/, "too_little_data");
  });
  test("every won deal took a different path", () => {
    const stages = ["Prospecting", "Qualified", "Discovery", "Proposal", "Negotiation", "Legal Review"];
    const won = stages.map((s) => ({ path: [s, "Negotiation"], outcome: "won" as const, days: 10 }));
    check(analysed([...won, ...deals(5, ["Prospecting"], 0, 5)]), /^Each of the 6 won deals took a different path through your stages, so there's no usual path to compare with\.$/, "too_little_data");
  });
  test("a stage with no meaning in a path", () => {
    check(analysed([...usual, ...deals(5, ["Prospecting", "Mystery"], 0, 5)]), /^The calibration doesn't say what these stages mean: Mystery\.$/, "needs_input");
  });
});

// ─── Org words ───────────────────────────────────────────────────────────────

describe("stage names are marked as the org's words", () => {
  test("every mark in an insight is a stage name, and every stage name is marked", () => {
    const [f] = comparisons(analysed([...deals(20, USUAL, 12, 60), ...deals(10, SKIPS_DISCOVERY, 9, 30)]).findings);
    for (const key of ["title", "summary", "recommendation"] as const) {
      const spans = f!.orgWords?.[key]?.spans;
      assert.ok(spans, key);
      const marked = spans.map((s) => f![key].slice(s.start, s.end));
      assert.ok(marked.every((w) => USUAL.includes(w)), `${key}: ${marked.join(", ")}`);
    }
    assert.deepEqual(f!.orgWords!.summary!.spans.map((s) => f!.summary.slice(s.start, s.end)), ["Discovery"]);
  });

  test("the usual path in the metric's assumptions is marked, and the reasons are said", () => {
    const { metric, findings } = analysed([...deals(20, USUAL, 16, 60)]);
    assert.equal(metric.status, "measured");
    assert.ok(metric.assumptions.some((a) => a.endsWith("Prospecting > Discovery > Proposal > Negotiation")));
    assert.equal(findings[0]!.status, "passed");
    assert.ok(findings[0]!.orgWords?.summary, "the pass is marked");
  });
});
