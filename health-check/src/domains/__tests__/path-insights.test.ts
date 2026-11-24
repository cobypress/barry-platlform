import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { HistoryRow, OpportunityExtract, OpportunityRow } from "../../extract/opportunities.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import type { Deal } from "../historic-pipeline/cohort.ts";
import { historicPipelineModule } from "../historic-pipeline/index.ts";
import { closingOut, confidenceOf, raised, zBar, zOf, Z_FLOOR, type Comparison } from "../historic-pipeline/paths/insights.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { buildReport } from "../../narrative/sections.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
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
type Spec = { path: string[]; outcome: Outcome; days: number; segment?: string; owner?: number };
const CLOSED: Record<Outcome, string> = { won: "Closed Won", lost: "Closed Lost", no_decision: "Closed No Decision" };
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");

/** `count` deals along `path`, `won` of them won and the rest lost, each `days` from creation to close. */
const deals = (count: number, path: string[], won: number, days: number, segment?: string, wonDays = days): Spec[] =>
  Array.from({ length: count }, (_, i) => ({ path, outcome: i < won ? "won" : "lost", days: i < won ? wonDays : days, ...(segment ? { segment } : {}) }));

/** The recorded extracts with every opportunity and history row replaced by these deals. */
function extractsOf(specs: Spec[], history: "ok" | "none" | "unavailable" = "ok"): Map<string, unknown> {
  const changed = clone(extracts);
  const e = changed.get("opportunities") as OpportunityExtract;
  const owners = e.opportunities.status === "ok" ? [...new Set(e.opportunities.data.map((o) => o.OwnerId))] : ["005000000000000001"];
  const opportunities: OpportunityRow[] = [];
  const rows: HistoryRow[] = [];
  specs.forEach((s, i) => {
    const id = `006${String(900_000 + i).padStart(15, "0")}`;
    // Created a few hours apart in April 2025, so every close falls in the window and, for the shorter deals, one quarter.
    const created = Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const closed = created + s.days * DAY;
    const closeDate = new Date(closed).toISOString().slice(0, 10);
    opportunities.push({ Id: id, OwnerId: owners[(s.owner ?? 0) % owners.length]!, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: true, IsWon: s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
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
  // The documents' findings and measures hold none of them: the analysis keeps them apart (DECISIONS.md 703).
  assert.ok(!output.findings.some((f) => f.ruleId === RULE) && !output.metrics.some((m) => m.id === PATH_VARIANTS));
  return { findings: output.insights.filter((f) => f.ruleId === RULE), metric: output.insightMetrics.find((m) => m.id === PATH_VARIANTS)! };
}

const issues = (findings: Finding[]) => findings.filter((f) => f.status === "issue");
/** The skip and rework insights, leaving out where deals die, which the usual path's own lost deals can raise. */
const comparisons = issues;

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
    // Before the deal's first recorded stage is not a skip: its history may simply start late.
    assert.deepEqual(skipsOf(["Proposal", "Negotiation"], USUAL), []);
    assert.deepEqual(skipsOf(["Discovery", "Negotiation"], USUAL), ["Proposal"]);
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

describe("the insights: each side reached the same point, and raised only above every bar", () => {
  // Through Discovery to Proposal: 60 deals, 30 won (50%). The usual path, with the most won deals.
  const through = (won = 30, wonDays = 60) => deals(60, USUAL, won, 60, undefined, wonDays);

  test("deals that skip a stage and are won more often, against those that went through it, with the basis", () => {
    const { findings } = analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60)]);
    const [f, ...rest] = comparisons(findings);
    assert.equal(rest.length, 0, rest.map((x) => x.title).join("; "));
    assert.deepEqual(f!.subjects, [{ type: "stage", label: "Discovery" }]);
    assert.equal(f!.title, "Deals that skip Discovery are won more often than deals that went through it");
    assert.equal(
      f!.summary,
      "Of the 90 closed deals that reached Proposal, the 30 that skipped Discovery were won 83.3% of the time, against 50% of the 60 that went through it. We couldn't check this within teams or record types: none had enough of these deals on both sides. This shows how often they went together, not that one is the reason for the other.",
    );
    assert.doesNotMatch(`${f!.title} ${f!.summary} ${f!.recommendation}`, /because|caus|usual path/i);
    assert.doesNotMatch(f!.recommendation, /left out|leave/);
    assert.equal(f!.severity, "info");
    // No segment could check it: the lowest confidence the engine has for an issue.
    assert.equal(f!.confidence, "low");
    assert.deepEqual(f!.facts, { reached: 90, deals: 30, otherDeals: 60, winRate: 83.3, otherWinRate: 50 });
  });

  test("the reverse: a skip that is won less often", () => {
    const [f] = comparisons(analysed([...through(45), ...deals(30, SKIPS_DISCOVERY, 12, 60)]).findings);
    assert.equal(f!.title, "Deals that skip Discovery are won less often than deals that went through it");
  });

  test("days are compared on won deals only, and said only beside a win-rate insight that clears", () => {
    // Skippers won in 30 days and lost in 90; deals through Discovery won in 60 and lost in 10: across outcomes the medians would mislead.
    const [f] = comparisons(analysed([...through(30, 60).map((d) => (d.outcome === "lost" ? { ...d, days: 10 } : d)), ...deals(30, SKIPS_DISCOVERY, 25, 90, undefined, 30)]).findings);
    assert.match(f!.summary, /Won deals that skipped Discovery took a median of 30 days to win, against 60 for won deals that went through it\./);
    assert.equal(f!.facts.wonMedianDays, 30);
    // A gap of 6 days is under the 7-day floor though it is over a quarter of 16: not said.
    const [g] = comparisons(analysed([...through(30, 16), ...deals(30, SKIPS_DISCOVERY, 25, 60, undefined, 10)]).findings);
    assert.doesNotMatch(g!.summary, /took a median/);
    // Days alone never raise one: the same win rate on both sides, won deals far apart in time.
    assert.equal(comparisons(analysed([...through(30, 60), ...deals(30, SKIPS_DISCOVERY, 15, 60, undefined, 10)]).findings).length, 0);
  });

  test("below the win-rate gap, the z bar, or the won and lost each side needs, nothing is raised", () => {
    // 6.7 points: under the gap.
    assert.equal(comparisons(analysed([...through(30), ...deals(30, SKIPS_DISCOVERY, 17, 60)]).findings).length, 0);
    // 18.8 points with 5 lost on each side, but only 16 skippers: z about 1.3.
    assert.equal(comparisons(analysed([...through(30), ...deals(16, SKIPS_DISCOVERY, 11, 60)]).findings).length, 0);
    // 30 skippers, 28 won: 2 lost is under the 5 each side needs.
    assert.equal(comparisons(analysed([...through(30), ...deals(30, SKIPS_DISCOVERY, 28, 60)]).findings).length, 0);
  });

  test("the calibrated minimum holds on the other side too, not only on the group", () => {
    // At the calibrated 20: 60 skippers, 12 won, against deals through Discovery won 70% of the time. 19 of those is one under.
    const at20: Calibration = { ...calibration, minSample: 20 };
    assert.equal(comparisons(analysed([...deals(19, USUAL, 13, 60), ...deals(60, SKIPS_DISCOVERY, 12, 60)], { calibration: at20 }).findings).length, 0);
    assert.equal(comparisons(analysed([...deals(20, USUAL, 14, 60), ...deals(60, SKIPS_DISCOVERY, 12, 60)], { calibration: at20 }).findings).length, 1);
  });

  test("the z bar rises with the comparisons tried (Bonferroni), never under 2", () => {
    assert.equal(zBar(1), Z_FLOOR);
    assert.ok(zBar(10) > 2.8 && zBar(10) < 2.82, String(zBar(10)));
    assert.ok(zBar(40) > zBar(10));
    assert.ok(Math.abs(zOf({ n: 30, won: 25 }, { n: 60, won: 30 }) - 3.06) < 0.01);
  });

  test("with segments that can check it, the gap must point the same way in most; confidence follows the sides", () => {
    // New and Renewal, each in type, lead source and record type: six segments, none holding 90% of a side.
    const agreeing = [...deals(40, USUAL, 20, 60, "New"), ...deals(40, USUAL, 20, 60, "Renewal"), ...deals(25, SKIPS_DISCOVERY, 20, 60, "New"), ...deals(25, SKIPS_DISCOVERY, 20, 60, "Renewal")];
    const [f] = comparisons(analysed(agreeing).findings);
    assert.match(f!.summary, /The gap points the same way in 6 of 6 teams, record types and other segments that could check it\./);
    assert.equal(f!.confidence, "high");
    // Turned round in one of the two record types (and so in half the six segments): no majority.
    const split = [...deals(40, USUAL, 20, 60, "New"), ...deals(40, USUAL, 30, 60, "Renewal"), ...deals(25, SKIPS_DISCOVERY, 25, 60, "New"), ...deals(25, SKIPS_DISCOVERY, 15, 60, "Renewal")];
    assert.equal(comparisons(analysed(split).findings).length, 0);
    const c = (n: number): Comparison => ({ reached: "x", group: { deals: [], n, won: 1, rate: 1 }, other: { deals: [], n: 100, won: 1, rate: 1 }, gap: 20, z: 3, days: null, compared: 2, held: 2, types: { many: false, compared: 0, held: 0 } });
    const ctx = { calibration: { minSample: 20 } } as Parameters<typeof confidenceOf>[0];
    assert.equal(confidenceOf(ctx, c(39)), "medium");
    assert.equal(confidenceOf(ctx, c(40)), "high");
    assert.equal(confidenceOf(ctx, { ...c(40), compared: 0, held: 0 }), "low");
    // One segment alone checks nothing: not raised.
    assert.equal(raised({ ...c(40), compared: 1, held: 1 }, 2), false);
  });

  test("rework: of the deals that reached a stage, those sent back, against the rest", () => {
    const [f, ...rest] = comparisons(analysed([...through(45), ...deals(30, REWORK, 12, 60)]).findings);
    assert.equal(rest.length, 0, rest.map((x) => x.title).join("; "));
    assert.deepEqual(f!.subjects.map((x) => x.label), ["Discovery", "Prospecting"]);
    assert.equal(f!.title, "Deals sent back from Discovery to Prospecting are won less often than the rest that reached Discovery");
    assert.match(f!.summary, /^Of the 90 closed deals that reached Discovery, the 30 sent back to Prospecting were won 40% of the time, against 75% of the rest\./);
  });

  test("a lost deal's last move back closes it out: not rework, in neither side, and counted in the table", () => {
    const { findings, metric } = analysed([...through(30), ...deals(30, ["Prospecting", "Discovery", "Prospecting"], 0, 60)]);
    assert.equal(comparisons(findings).length, 0);
    assert.ok(metric.assumptions.includes("30 lost deals whose last move was back to an earlier stage aren't counted as sent back: that move closed them out"));
    assert.equal(closingOut(pd(["Prospecting", "Discovery", "Prospecting"], false, 1)), true);
    assert.equal(closingOut(pd(["Prospecting", "Discovery", "Prospecting"], true, 1)), false);
    assert.equal(closingOut(pd(REWORK, false, 1)), false);
  });
});

// The independent review's probes (orchestration/health-check/probes/e7-path-insights-review/review.probe.ts), at the
// real calibrated minimum of 20: each now raises nothing, or the honest insight.
describe("the review's probes", () => {
  const real: Calibration = { ...loadCalibration() };
  const many = (n: number, path: string[], won: number, days: number, wonDays = days, extra: Partial<Spec> = {}): Spec[] =>
    Array.from({ length: n }, (_, i) => ({ path, outcome: i < won ? "won" : "lost", days: i < won ? wonDays : days, ...extra }));
  const [A, B, C, D] = USUAL as [string, string, string, string];
  const none = (specs: Spec[]) => assert.deepEqual(comparisons(analysed(specs, { calibration: real }).findings).map((f) => f.title), []);

  test("1a' survivorship: of deals that reached Proposal, skippers and the rest are won equally often", () => {
    none([...many(20, [A], 0, 15), ...many(20, [A, B], 0, 25), ...many(20, [A, B, C], 0, 35), ...many(40, [A, B, C, D], 36, 45, 60), ...many(10, [A, C], 0, 35), ...many(20, [A, C, D], 18, 45, 60)]);
  });
  test("1b rework: deals sent back are won as often as the rest that reached the stage", () => {
    const RW = [A, B, A, B];
    none([...many(20, [A], 0, 15), ...many(30, [A, B], 0, 25), ...many(20, [A, B, C], 0, 35), ...many(40, [A, B, C, D], 36, 45, 60), ...many(15, RW, 0, 30), ...many(10, [...RW, C], 0, 40), ...many(20, [...RW, C, D], 18, 50, 65)]);
  });
  test("1c history starting late is no skip", () => {
    none([...many(20, [A], 0, 15), ...many(20, [A, B], 0, 25), ...many(20, [A, B, C], 0, 35), ...many(40, [A, B, C, D], 32, 45, 60), ...many(30, [C, D], 29, 50, 70)]);
  });
  test("1d a record type with no such stage isn't skipping it", () => {
    none([...many(20, [A], 0, 15, 15, { segment: "X" }), ...many(40, [A, B, C, D], 34, 45, 60, { segment: "X" }), ...many(20, [A], 0, 15, 15, { segment: "Y" }), ...many(40, [A, C, D], 22, 45, 60, { segment: "Y" })]);
  });
  test("1e-iii won deals take as long on both paths: no time difference is raised", () => {
    none([...many(20, [A], 0, 15), ...many(41, [A, B, C, D], 21, 20, 80), ...many(41, [A, C, D], 18, 20, 80)]);
  });
  test("1f where deals die is no longer said by this rule", () => {
    const { findings } = analysed([...many(40, [A], 0, 10), ...many(35, [A, B], 0, 20), ...many(35, [A, B, C], 0, 30), ...many(90, [A, B, C, D], 90, 40, 60)], { calibration: real });
    assert.ok(!findings.some((f) => /lost after/.test(f.title)));
  });
  test("1g only 4 of the 40 deals through Discovery were lost, under the 5 a side needs: nothing is raised", () => {
    none([...many(20, [A], 0, 15), ...many(40, [A, B, C, D], 36, 45, 60), ...many(40, [A, C, D], 20, 45, 60)]);
  });
});

// The review's noise probe: path and outcome independent, so any insight is a false one. Fixed seeds.
describe("noise: under independence at most 5% of orgs get an insight", () => {
  const real: Calibration = { ...loadCalibration() };
  const [A, B, C, D] = USUAL as [string, string, string, string];
  let seed = 12345;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648), seed / 2147483648);
  const logn = (median: number) => {
    const z = Math.sqrt(-2 * Math.log(rnd() || 1e-9)) * Math.cos(2 * Math.PI * rnd());
    return Math.max(1, Math.round(median * Math.exp(0.6 * z)));
  };
  const nullOrg = (n: number, variants: string[][], weights: number[], segs: boolean): Spec[] =>
    Array.from({ length: n }, () => {
      let r = rnd() * weights.reduce((a, b) => a + b, 0);
      let k = 0;
      while (r > weights[k]!) r -= weights[k++]!;
      const won = rnd() < 0.5;
      return { path: variants[k]!, outcome: won ? "won" : "lost", days: logn(60), ...(segs ? { segment: ["S1", "S2", "S3"][Math.floor(rnd() * 3)], owner: Math.floor(rnd() * 4) } : {}) } as Spec;
    });
  const SIZES = [
    ["60 deals, two paths, no segments", 60, [[A, B, C, D], [A, C, D]], [1, 1], false],
    ["60 deals, two paths, 3 segments and 4 reps", 60, [[A, B, C, D], [A, C, D]], [1, 1], true],
    ["120 deals, two paths, segments", 120, [[A, B, C, D], [A, C, D]], [1, 1], true],
    ["300 deals, five variants (two skips, two send-backs), segments", 300, [[A, B, C, D], [A, C, D], [A, B, D], [A, B, A, B, C, D], [A, B, C, B, C, D]], [3, 1, 1, 1, 1], true],
  ] as const;
  const TRIALS = 200;
  for (const [label, n, variants, weights, segs] of SIZES) {
    test(label, () => {
      seed = 12345 + n + (segs ? 1 : 0);
      let any = 0;
      for (let t = 0; t < TRIALS; t++) if (comparisons(analysed(nullOrg(n, variants as unknown as string[][], weights as unknown as number[], segs), { calibration: real }).findings).length) any++;
      console.log(`noise ${label}: ${any} of ${TRIALS} orgs`);
      assert.ok(any <= TRIALS * 0.05, `${any} of ${TRIALS}`);
    });
  }
});

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
    const [f] = comparisons(analysed([...deals(60, USUAL, 30, 60), ...deals(30, SKIPS_DISCOVERY, 25, 60)]).findings);
    for (const key of ["title", "summary", "recommendation"] as const) {
      const spans = f!.orgWords?.[key]?.spans;
      assert.ok(spans, key);
      const marked = spans.map((s) => f![key].slice(s.start, s.end));
      assert.ok(marked.every((w) => USUAL.includes(w)), `${key}: ${marked.join(", ")}`);
    }
    assert.deepEqual(f!.orgWords!.summary!.spans.map((s) => f!.summary.slice(s.start, s.end)), ["Proposal", "Discovery"]);
  });

  test("the usual path in the metric's assumptions is marked as a label, and the pass is said", () => {
    const { metric, findings } = analysed([...deals(20, USUAL, 16, 60)]);
    assert.equal(metric.status, "measured");
    assert.ok(metric.assumptions.some((a) => a.endsWith("Prospecting > Discovery > Proposal > Negotiation")));
    assert.ok(metric.assumptions.includes("The usual path is chosen by how often won deals took it, so it is a label, never a side of a comparison"));
    assert.equal(findings[0]!.status, "passed");
    assert.ok(findings[0]!.orgWords?.summary, "the pass is marked");
  });
});

// ─── The synthetic org: stored, not served ───────────────────────────────────

/** A run's digest as built from its own files, and as built with the path insights taken out: the same bytes. */
function digestsOf(runDir: string): [string, string] {
  const loaded = loadRun(runDir);
  const madeAt = "2026-11-24T09:00:00.000Z";
  const without = { ...loaded.input, findings: loaded.input.findings.filter((f) => f.ruleId !== RULE), metrics: loaded.input.metrics.filter((m) => m.id !== PATH_VARIANTS) };
  return [JSON.stringify(checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt })), JSON.stringify(checkedDigest({ input: without, pseudonyms: loaded.pseudonyms, inventory: null, madeAt }))];
}

describe("the synthetic org keeps the insights in its files and serves none of them yet (decision 703)", async () => {
  const automatic = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });

  test("the consultant-led run's files hold the rule and the table; what the rule says is the honest result", () => {
    const loaded = loadRun(dir);
    const ours = loaded.input.findings.filter((f) => f.ruleId === RULE);
    assert.equal(ours.length, 1);
    console.log(`synthetic org: ${ours.map((f) => `${f.status}: ${f.title} / ${f.summary}`).join(" | ")}`);
    assert.ok(loaded.input.metrics.some((m) => m.id === PATH_VARIANTS));
  });

  test("the digest of a consultant-led and an automatic run is byte for byte the one without them", () => {
    for (const runDir of [dir, automatic.dir]) {
      const [withThem, without] = digestsOf(runDir);
      assert.equal(withThem, without);
      assert.doesNotMatch(withThem, /pipe\.path-insights|historic\.path_variants/);
    }
  });

  test("the automatic run's checked digest is not held", () => {
    const loaded = loadRun(automatic.dir);
    const c = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-24T09:00:00.000Z" });
    assert.equal(c.held, null);
  });
});

describe("a consultant-led run's documents leave the insights out, and are what they'd be without them (decision 703)", () => {
  const loaded = loadRun(dir);
  const withThem = buildReport(loaded.input);
  const without = buildReport({ ...loaded.input, findings: loaded.input.findings.filter((f) => f.ruleId !== RULE), metrics: loaded.input.metrics.filter((m) => m.id !== PATH_VARIANTS) });

  test("the run folder holds them, the report leaves them out", () => {
    assert.ok(loaded.input.findings.some((f) => f.ruleId === RULE));
    assert.ok(loaded.input.metrics.some((m) => m.id === PATH_VARIANTS));
    assert.ok(!withThem.teamFindings.some((f) => f.ruleId === RULE) && !withThem.metrics.some((m) => m.id === PATH_VARIANTS));
  });

  test("the executive summary, the full report and the one-pager are byte for byte the same", () => {
    assert.equal(executiveHtml(withThem, loaded.pseudonyms), executiveHtml(without, loaded.pseudonyms));
    assert.equal(fullReportHtml(withThem, loaded.pseudonyms), fullReportHtml(without, loaded.pseudonyms));
    assert.equal(onePagerHtml(withThem, loaded.pseudonyms), onePagerHtml(without, loaded.pseudonyms));
  });
});
