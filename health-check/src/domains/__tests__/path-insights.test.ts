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
import { AS_OF } from "../../testing/synthetic-org.ts";
import { pathInsights } from "../historic-pipeline/paths/insights.ts";
import { readFileSync } from "node:fs";
import { join } from "node:path";
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
/**
 * `segment` is set as the deal's type, lead source and record type alike, so three of the dimensions split on it.
 * `open`: still open at the run's date, in the last stage of its path. `createdAt`: made then, not in April 2025.
 * `gapDays`: each stage of its path entered that many days after the one before, not a minute.
 */
type Spec = { path: string[]; outcome: Outcome; days: number; segment?: string; owner?: number; open?: boolean; createdAt?: number; gapDays?: number };
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
    const created = s.createdAt ?? Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const closed = created + s.days * DAY;
    const closeDate = new Date(closed).toISOString().slice(0, 10);
    const final = s.open ? s.path.at(-1)! : CLOSED[s.outcome];
    opportunities.push({ Id: id, OwnerId: owners[(s.owner ?? 0) % owners.length]!, StageName: final, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: !s.open, IsWon: !s.open && s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
    s.path.forEach((stage, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created + k * (s.gapDays ? s.gapDays * DAY : 60_000)) }));
    if (!s.open) rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(closed) });
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
    assert.equal(f!.title, "Deals that skipped Discovery were won more often than deals that went through it");
    assert.equal(
      f!.summary,
      "Of the 90 deals created between 1 April 2025 and 1 September 2026 that started before Discovery and reached Proposal, the 30 that skipped Discovery were won 83.3% of the time, against 50% of the 60 that went through it. We couldn't check this within teams, record types or other groups: these deals couldn't be split into groups big enough to compare on both sides. This shows how often they went together, not that one is the reason for the other.",
    );
    assert.doesNotMatch(`${f!.title} ${f!.summary} ${f!.recommendation}`, /because|caus|usual path/i);
    assert.doesNotMatch(f!.recommendation, /left out|leave/);
    assert.equal(f!.severity, "info");
    // No segment could check it: the lowest confidence the engine has for an issue.
    assert.equal(f!.confidence, "low");
    assert.deepEqual(f!.facts, { reached: 90, open: 0, deals: 30, otherDeals: 60, winRate: 83.3, otherWinRate: 50 });
  });

  test("the reverse: a skip that is won less often", () => {
    const [f] = comparisons(analysed([...through(45), ...deals(30, SKIPS_DISCOVERY, 12, 60)]).findings);
    assert.equal(f!.title, "Deals that skipped Discovery were won less often than deals that went through it");
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

  test("the z bar rises with the comparisons tried (Bonferroni), never under 2.15 (decision 712)", () => {
    assert.equal(zBar(1), Z_FLOOR);
    assert.equal(Z_FLOOR, 2.15);
    // One comparison, a gap of 23.3 points at z 2.11: past the old floor of 2, which raised it, and under 2.15.
    assert.equal(comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 22, 60)]).findings).length, 0);
    assert.equal(comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 23, 60)]).findings).length, 1);
    assert.ok(zBar(10) > 2.8 && zBar(10) < 2.82, String(zBar(10)));
    assert.ok(zBar(40) > zBar(10));
    assert.ok(Math.abs(zOf({ n: 30, won: 25 }, { n: 60, won: 30 }) - 3.06) < 0.01);
  });

  test("two comparisons tried raise the bar for both: a gap at z 2.21 is said alone, not beside a second comparison (decision 712)", () => {
    // 21 of 28 skippers won against 30 of 60 through Discovery: z 2.21, past one comparison's 2.15, under two's 2.25.
    const skippers = deals(28, SKIPS_DISCOVERY, 21, 60);
    assert.equal(comparisons(analysed([...through(), ...skippers]).findings).length, 1);
    // Twenty more that skipped Proposal, won half the time: a second comparison tried, with no gap of its own.
    const both = analysed([...through(), ...skippers, ...deals(20, ["Prospecting", "Discovery", "Negotiation"], 10, 60)]).findings;
    assert.equal(comparisons(both).length, 0);
    assert.equal(both[0]!.facts.tried, 2);
    assert.equal(zBar(2), 2.25);
  });

  test("the period ends where 90% of the deals had closed, not half of them (decision 712)", () => {
    // 60 deals closed 60 days after they were made, 30 after 120: the 90th percentile is 120 days, so the period ends
    // 120 days before the run (3 July 2026); at the median it would end 60 days before (1 September).
    const [f] = comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 120)]).findings);
    assert.match(f!.summary, /^Of the 90 deals created between 1 April 2025 and 3 July 2026 that started before Discovery and reached Proposal,/);
  });

  test("with segments that can check it, the gap must point the same way in most; confidence follows the sides", () => {
    // New and Renewal, each in type, lead source and record type: six segments, none holding 90% of a side.
    const agreeing = [...deals(40, USUAL, 20, 60, "New"), ...deals(40, USUAL, 20, 60, "Renewal"), ...deals(25, SKIPS_DISCOVERY, 20, 60, "New"), ...deals(25, SKIPS_DISCOVERY, 20, 60, "Renewal")];
    const [f] = comparisons(analysed(agreeing).findings);
    assert.match(f!.summary, /The gap pointed the same way in 6 of the 6 teams, record types and other groups we could check it in\./);
    assert.equal(f!.confidence, "high");
    // Turned round in one of the two record types (and so in half the six segments): no majority.
    const split = [...deals(40, USUAL, 20, 60, "New"), ...deals(40, USUAL, 30, 60, "Renewal"), ...deals(25, SKIPS_DISCOVERY, 25, 60, "New"), ...deals(25, SKIPS_DISCOVERY, 15, 60, "Renewal")];
    assert.equal(comparisons(analysed(split).findings).length, 0);
    const c = (n: number): Comparison => ({ reached: "x", group: { deals: [], n, won: 1, rate: 1 }, other: { deals: [], n: 100, won: 1, rate: 1 }, gap: 20, z: 3, from: "2024-10-31", to: "2026-01-01", open: { group: 0, other: 0 }, low: { gap: 20, z: 3, cmh: 3 }, high: { gap: 20, z: 3, cmh: 3 }, days: null, compared: 2, held: 2, types: { many: false, compared: 0, held: 0 } });
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
    assert.equal(f!.title, "Deals sent back from Discovery to Prospecting were won less often than the rest that reached Discovery");
    assert.match(f!.summary, /^Of the 90 deals created between 1 April 2025 and 1 September 2026 that reached Discovery, the 30 sent back to Prospecting were won 40% of the time, against 75% of the rest\./);
  });

  test("a last move back on closing closes the deal out, won or lost alike: not rework, in neither side, and counted in the table", () => {
    // Prospecting, Discovery 30 days later, back to Prospecting 30 days after that, and closed the same day.
    const { findings, metric } = analysed([...through(30), ...deals(30, ["Prospecting", "Discovery", "Prospecting"], 0, 60).map((d) => ({ ...d, gapDays: 30 }))]);
    assert.equal(comparisons(findings).length, 0);
    assert.ok(metric.assumptions.includes("30 deals whose last move, back to an earlier stage, came within a day of their close aren't counted as sent back: that move closed them out"));
    // Decided on timing alone (decision 712): a won deal is closed out the same way, and a move back a month before closing is rework.
    const closedAfter = (path: string[], won: boolean, days: number): PathDeal => {
      const t0 = Date.parse("2025-04-01T09:00:00Z");
      const history = path.map((StageName, k) => ({ StageName, CreatedDate: new Date(t0 + k * 30 * DAY).toISOString() }));
      return { ...pd(path, won, 1), deal: { history, closedAt: new Date(t0 + (path.length - 1) * 30 * DAY + days * DAY) } as unknown as Deal };
    };
    const back = ["Prospecting", "Discovery", "Prospecting"];
    assert.equal(closingOut(closedAfter(back, false, 0.5)), true);
    assert.equal(closingOut(closedAfter(back, true, 0.5)), true);
    assert.equal(closingOut(closedAfter(back, false, 30)), false);
    assert.equal(closingOut(closedAfter(back, true, 30)), false);
    assert.equal(closingOut(closedAfter(REWORK, false, 0)), false);
  });
});

// Decision 712: each comparison takes the deals made in its stage's period that reached the stage (reached.ts), never the
// deals that closed in the window, and counts the few still open both ways.
describe("who is compared: the deals made in the period that reached the stage, open or closed (decision 712)", () => {
  const through = (won = 30) => deals(60, USUAL, won, 60);
  const RUN = Date.parse(AS_OF);
  const open = (n: number, path: string[]): Spec[] => deals(n, path, 0, 60).map((d) => ({ ...d, open: true }));
  const pass = (findings: Finding[]) => findings.find((f) => f.status === "passed")!;

  test("a gap made only by the mix changing over time is not raised: within each band of deals made together there's none (decision 712)", () => {
    // Made from 25 June 2025: 20 skippers and 80 through Discovery, 85% won on both sides. Made from 26 September, in
    // the next 91-day band: 80 and 20, 55% won on both. Together the skippers were won 61% of the time against 79%,
    // z 2.8. Each band closes in the same week of October, so no closed quarter sets them apart either.
    const band = (at: string, days: number, skip: number, skipWon: number, thru: number, thruWon: number): Spec[] =>
      [...deals(skip, SKIPS_DISCOVERY, skipWon, days), ...deals(thru, USUAL, thruWon, days)].map((d, i) => ({ ...d, createdAt: Date.parse(at) + i * 3_600_000 }));
    const mixed = [...band("2025-06-25T09:00:00Z", 100, 20, 17, 80, 68), ...band("2025-09-26T09:00:00Z", 10, 80, 44, 20, 11)];
    assert.equal(comparisons(analysed(mixed).findings).length, 0);
    // The same outcomes all made in one band: the gap is there within it, and is said.
    assert.equal(comparisons(analysed(mixed.map((d, i) => ({ ...d, days: 10, createdAt: Date.parse("2025-09-26T09:00:00Z") + i * 3_600_000 }))).findings).length, 1);
  });

  test("a skip compares deals that started before the stage, on both sides, and says so (decision 712)", () => {
    // Ten more whose history starts at Discovery: they went through it, but didn't start before it, so they're in neither side.
    const [f] = comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60), ...deals(10, ["Discovery", "Proposal", "Negotiation"], 5, 60)]).findings);
    assert.match(f!.summary, /^Of the 90 deals created between 1 April 2025 and 1 September 2026 that started before Discovery and reached Proposal, the 30 that skipped Discovery were won 83\.3% of the time, against 50% of the 60 that went through it\./);
  });

  test("deals made after the period ends aren't compared, though they closed in the window", () => {
    // Ten more skippers, all lost, made 30 days before the run and closed 5 days before it: inside the lag.
    const late = deals(10, SKIPS_DISCOVERY, 0, 25).map((d) => ({ ...d, createdAt: RUN - 30 * DAY }));
    const [f] = comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60), ...late]).findings);
    assert.match(f!.summary, /^Of the 90 deals created between 1 April 2025 and \d+ \w+ 2026 that started before Discovery and reached Proposal, the 30 that skipped Discovery were won 83\.3% of the time/);
  });

  test("chosen by the day a deal was made, not the day it reached the stage: deals made before the window aren't compared", () => {
    // Twenty more skippers, all lost, made six weeks before the window starts and reaching Proposal 60 days later, inside it.
    const early = deals(20, SKIPS_DISCOVERY, 0, 150).map((d) => ({ ...d, createdAt: Date.parse("2024-09-15T09:00:00Z"), gapDays: 60 }));
    const [f] = comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60), ...early]).findings);
    assert.match(f!.summary, /^Of the 90 deals created between 31 October 2024 and \d+ \w+ 2026 that started before Discovery and reached Proposal, the 30 that skipped Discovery/);
  });

  // Decision 712: nothing raised is never stored as "no difference". Each state says what we could and couldn't tell.
  test("compared and nothing clears: we couldn't tell, and the smallest gap these deals could have shown", () => {
    const findings = analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 17, 60)]).findings;
    assert.equal(comparisons(findings).length, 0);
    // 47 of 90 won, 30 against 60: a standard error of 11.2 points, times the bar of 2.15, is 24.0, said as 25.
    assert.equal(
      pass(findings).summary,
      "We couldn't tell whether skipping a stage or being sent back went with winning more or less often. In the 1 comparison we could make, between deals that reached the same point, a gap in win rate would have had to be about 25 points or more to tell it from chance, and none was, so a smaller difference may be there unseen.",
    );
  });

  test("nothing to compare: no deal skipped a stage of the usual path or was sent back", () => {
    assert.equal(pass(analysed(deals(40, USUAL, 20, 60)).findings).summary, "No deal skipped a stage of the usual path or was sent back to an earlier stage, so there was nothing to compare.");
  });

  test("too few won and lost: said so, and only that", () => {
    assert.equal(pass(analysed([...through(), ...deals(8, SKIPS_DISCOVERY, 4, 60)]).findings).summary, "No skipped stage or move back had enough won and lost deals on both sides, among deals that reached the same point, to compare.");
  });

  test("deals take longer to close than the period allows: said so, with the day the period would start", () => {
    // Made in May 2024 and closed some 900 days later: 90% took 900 days, so the period would end before the window starts.
    const made = Date.parse("2024-05-01T09:00:00Z");
    const slow = [...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60)].map((d, i) => ({ ...d, days: 900, createdAt: made + i * 3_600_000 }));
    assert.equal(
      pass(analysed(slow).findings).summary,
      "We couldn't compare any skipped stage or move back. Deals that reached Proposal took so long to close that none made since 31 October 2024 has had time to, so none could be counted yet.",
    );
  });

  test("more than 5% of the period still open: not compared, and the pass says which stage and why", () => {
    const findings = analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60), ...open(6, SKIPS_DISCOVERY)]).findings;
    assert.equal(comparisons(findings).length, 0);
    assert.equal(
      pass(findings).summary,
      "We couldn't compare any skipped stage or move back. Of the deals made early enough to compare that reached Proposal, too many are still open.",
    );
  });

  test("the few still open are counted both ways, and both must clear: one open skipper is said, three hold it back", () => {
    // Closed alone, 23 of 30 skippers won against 30 of 60: z 2.42. With the open as lost, z 2.22 with one and 1.85 with three.
    const [f] = comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 23, 60), ...open(1, SKIPS_DISCOVERY)]).findings);
    assert.match(f!.summary, /^Of the 91 deals created between 1 April 2025 and 1 September 2026 that started before Discovery and reached Proposal, 1 is still open\. Of the other 90, the 30 that skipped Discovery were won 76\.7% of the time, against 50% of the 60 that went through it\./);
    assert.equal(f!.facts.open, 1);
    assert.equal(comparisons(analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 23, 60), ...open(3, SKIPS_DISCOVERY)]).findings).length, 0);
    const several = analysed([...through(), ...deals(30, SKIPS_DISCOVERY, 25, 60), ...open(2, SKIPS_DISCOVERY), ...open(2, USUAL)]).findings;
    assert.match(comparisons(several)[0]!.summary, /, 4 are still open\. Of the other 90, the 30 that skipped/);
  });

  test("a send-back's sentence says the lost deals its last move closed out are left out, every count adding up", () => {
    const closedOut = deals(4, ["Prospecting", "Discovery", "Prospecting"], 0, 60).map((d) => ({ ...d, gapDays: 30 }));
    const [f] = comparisons(analysed([...through(45), ...deals(30, REWORK, 12, 60), ...closedOut]).findings);
    assert.match(f!.summary, /^Of the 94 deals created between 1 April 2025 and 1 September 2026 that reached Discovery, 4 whose last move, back to Prospecting on closing, closed them out are left out\. Of the other 90, the 30 sent back to Prospecting were won 40% of the time, against 75% of the rest\./);
    const [g] = comparisons(analysed([...through(45), ...deals(30, REWORK, 12, 60), ...closedOut.slice(0, 1), ...open(1, USUAL)]).findings);
    assert.match(g!.summary, /^Of the 92 deals created between 1 April 2025 and 1 September 2026 that reached Discovery, 1 whose last move, back to Prospecting on closing, closed it out is left out, and 1 is still open\. Of the other 90,/);
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
  test("1a'' survivorship with enough lost deals on the usual path: against deals that reached Proposal, the skippers are no different", () => {
    // Reached Proposal through Discovery: 80, 30 won (37.5%); skippers 30, 11 won (36.7%). On the whole usual path: 40, 30 won (75%).
    none([...many(40, [A, B, C, D], 30, 45, 60), ...many(40, [A, B, C], 0, 35), ...many(30, [A, C, D], 11, 45, 60)]);
  });
  test("1c' history starting late, with lost deals among them: still no skip", () => {
    none([...many(20, [A, B, C], 0, 35), ...many(40, [A, B, C, D], 32, 45, 60), ...many(30, [C, D], 24, 50, 70)]);
  });
  test("7 a lost deal's closing-out move back isn't counted with the deals really sent back", () => {
    // Sent back and on: 30, 22 won (73%); the rest through Discovery 75%. Thirty more closed out by a move back are in neither.
    none([...many(60, [A, B, C, D], 45, 45, 60), ...many(30, [A, B, A, B, C, D], 22, 50, 65), ...many(30, [A, B, A], 0, 30, 30, { gapDays: 15 })]);
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
    assert.deepEqual(f!.orgWords!.summary!.spans.map((s) => f!.summary.slice(s.start, s.end)), ["Discovery", "Proposal", "Discovery"]);
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
// Dated orgs (decision 712; the 1,000-org grid is a probe: orchestration/health-check/probes/e11-path-insights-noise/
// builder). The e8 review's generator: deals made over four years at a rate growing or shrinking by `growth` a year,
// each walking the four stages 40 days apart with true reached-then-won shares 20/30/50/70; 30% of those that reach
// Proposal skip Discovery whatever their outcome, and close 20 days sooner whether won or lost (the last two cells tie
// time to the outcome too); each closes an exponential time after its last stage, wins and losses
// at their own speeds; open at the run's date where it hasn't closed. Path and outcome are independent, so any
// insight is a false one. The cells are the review's three worst and the probe's worst at 1,000 deals, where the
// 10-point gap is about the z bar and so adds no margin, and two where choosing by close date made a gap.
describe("noise on dated deals: arrivals growing or shrinking, wins and losses closing at their own speeds", () => {
  const real: Calibration = { ...loadCalibration() };
  const [A, B, C, D] = USUAL as [string, string, string, string];
  const RUN = Date.parse(AS_OF);
  const WINDOW_FROM = Date.parse((JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as { analysisWindow: { from: string } }).analysisWindow.from);
  const GAP = 40;
  const SPAN = 4 * 365;
  let seed = 1;
  // mulberry32: an LCG's successive draws are correlated, which can tie an outcome to its time.
  const rnd = (): number => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  type Dated = { path: string[]; won: boolean; at: number; enter: number[]; closedAt: number | null; segment: string; owner: number };
  /** How a skip changes time: every skipper's later stages `save` days sooner; only a won one's; or a won one closes in half the time. */
  /**
   * How a skip ties to time: every skipper's later stages `save` days sooner; only a won one's; a won one closing in half
   * the time; or the mix changing over time: a year before the run deals skip less and are won less (`drift`), or both
   * swing with the month (`season`). Outcome and skip stay independent at any one time.
   */
  type Kind = "both" | "won-reach" | "won-close" | "drift" | "season";
  function datedOrg(n: number, growth: number, win: number, loss: number, save: number, kind: Kind): Dated[] {
    const out: Dated[] = [];
    const r = Math.log(growth) / 365;
    const peak = Math.max(1, growth ** 4);
    for (let made = 0; out.length < n && made < n * 200; made++) {
      const at = RUN - rnd() * SPAN * DAY;
      if (rnd() * peak > Math.exp(r * ((at - RUN) / DAY + SPAN))) continue;
      const late = at > RUN - 365 * DAY;
      const swing = kind === "season" ? 0.15 * Math.sin((2 * Math.PI * new Date(at).getUTCMonth()) / 12) : 0;
      const winAt = kind === "drift" ? (late ? 0.55 : 0.85) : 0.7 + swing;
      const skipShare = kind === "drift" ? (late ? 0.1 : 0.5) : 0.3 + swing;
      const path = [A];
      let won = false;
      for (const [k, on] of [20 / 30, 30 / 50, 50 / 70, winAt].entries()) {
        if (rnd() >= on) break;
        if (k === 3) won = true;
        else path.push([B, C, D][k]!);
      }
      const skip = path.length >= 3 && rnd() < skipShare;
      const saved = kind === "won-reach" && !won ? 0 : save;
      const timed = path.map((stage, k) => ({ stage, t: at + k * GAP * DAY })).filter((x) => !skip || x.stage !== B).map((x) => (skip && x.t > at + GAP * DAY ? { ...x, t: x.t - saved * DAY } : x));
      const closedAt = timed.at(-1)!.t - Math.log(1 - rnd()) * (won ? (kind === "won-close" && skip ? win / 2 : win) : loss) * DAY;
      // One record type and one type where the mix changes over time, as the review's cells had: the record-type and
      // segment checks then can't stand in for the bands, so the cells test the bands themselves.
      const drawn = ["S1", "S2", "S3"][Math.floor(rnd() * 3)]!;
      const segment = kind === "drift" || kind === "season" ? "S1" : drawn;
      const owner = Math.floor(rnd() * 4);
      if (closedAt <= RUN) {
        if (closedAt < WINDOW_FROM && at < WINDOW_FROM) continue;
        out.push({ path: timed.map((x) => x.stage), won, at, enter: timed.map((x) => x.t), closedAt, segment, owner });
      } else {
        const reached = timed.filter((x) => x.t <= RUN);
        out.push({ path: reached.map((x) => x.stage), won: false, at, enter: reached.map((x) => x.t), closedAt: null, segment, owner });
      }
    }
    return out;
  }
  function extractsOfDated(specs: Dated[]): Map<string, unknown> {
    const changed = clone(extracts);
    const e = changed.get("opportunities") as OpportunityExtract;
    const owners = e.opportunities.status === "ok" ? [...new Set(e.opportunities.data.map((o) => o.OwnerId))] : ["005000000000000001"];
    const opportunities: OpportunityRow[] = [];
    const rows: HistoryRow[] = [];
    specs.forEach((s, i) => {
      const id = `006${String(900_000 + i).padStart(15, "0")}`;
      const closeDate = new Date(s.closedAt ?? RUN + 60 * DAY).toISOString().slice(0, 10);
      const final = s.closedAt === null ? s.path.at(-1)! : s.won ? CLOSED.won : CLOSED.lost;
      opportunities.push({ Id: id, OwnerId: owners[s.owner % owners.length]!, StageName: final, Amount: null, CloseDate: closeDate, CreatedDate: stamp(s.at), IsClosed: s.closedAt !== null, IsWon: s.closedAt !== null && s.won, LastActivityDate: null, Type: s.segment, LeadSource: s.segment, RecordType: s.segment, CurrencyIsoCode: null, LossReason: null });
      s.path.forEach((stage, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(s.enter[k]!) }));
      if (s.closedAt !== null) rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: final, Amount: null, CloseDate: closeDate, CreatedDate: stamp(s.closedAt) });
    });
    if (e.opportunities.status === "ok") e.opportunities = { ...e.opportunities, data: opportunities };
    if (e.history.status === "ok") e.history = { ...e.history, data: rows };
    return changed;
  }
  // The rule alone, so the grid runs in seconds: nothing else in the module reads what it says.
  const only = { ...historicPipelineModule, metrics: [], rules: [pathInsights] };
  // Tied to time and outcome together: only the won skippers reach Proposal sooner, by 40 days (by close date, 12.8% of
  // 1,000 orgs got a false "won less often"; by the day they reached the stage, 7.3% of 600; by the day made, 2.7%),
  // or close in half the time (by close date, 59%). The mix changing over time: before the bands (decision 712), a
  // year of fewer skips and fewer wins made a false "won more often" in 8 to 17% of orgs, and a season in 6.7%.
  // 150 orgs a cell can't tell 3% from 5% (a cell at a true 3.2% reaches 8 of 150 about one time in 9), so most cells
  // are held under 10%, and the 2,250 orgs together at most 5%; the cells that tell one way of choosing deals from
  // another run more orgs and are held at 5%.
  const CELLS: ReadonlyArray<readonly [number, number, number, number, Kind, number, number]> = [
    [2, 180, 30, 20, "both", 150, 0.1],
    [0.25, 30, 180, 20, "both", 150, 0.1],
    [3, 180, 30, 20, "both", 150, 0.1],
    [1, 180, 90, 20, "both", 150, 0.1],
    [0.5, 30, 30, 20, "both", 150, 0.1],
    [1, 30, 180, 20, "both", 150, 0.1],
    [0.25, 30, 30, 40, "won-reach", 600, 0.05],
    [0.25, 180, 90, 0, "won-close", 150, 0.1],
    [3, 90, 90, 0, "drift", 300, 0.05],
    [0.25, 90, 90, 0, "season", 300, 0.05],
  ];
  const how: Record<Kind, (save: number) => string> = {
    both: (save) => `skippers ${save} days sooner`,
    "won-reach": (save) => `won skippers reaching it ${save} days sooner`,
    "won-close": () => "won skippers closing in half the time",
    drift: () => "skips and wins both fewer in the last year",
    season: () => "skips and wins both swinging with the month",
  };
  let all = 0;
  let orgs = 0;
  for (const [growth, win, loss, save, kind, TRIALS, limit] of CELLS) {
    test(`growth ${growth}x a year, wins closing at ${win} days and losses at ${loss}, 1,000 deals, ${how[kind](save)}`, () => {
      seed = 12_000_000 + growth * 100_000 + win * 1_000 + loss + { both: 0, "won-reach": 7, "won-close": 11, drift: 13, season: 17 }[kind];
      let any = 0;
      const ways: Record<string, number> = {};
      for (let t = 0; t < TRIALS; t++) {
        const raised = issues(analyseExtracts(extractsOfDated(datedOrg(1000, growth, win, loss, save, kind)), { calibration: real, modules: [only] }).insights);
        if (raised.length) any++;
        for (const f of raised) ways[/more often/.test(f.title) ? "more" : "less"] = (ways[/more often/.test(f.title) ? "more" : "less"] ?? 0) + 1;
      }
      all += any;
      orgs += TRIALS;
      console.log(`dated noise growth=${growth} win=${win} loss=${loss} ${kind}: ${any} of ${TRIALS} orgs ${JSON.stringify(ways)}`);
      assert.ok(limit === 0.1 ? any < TRIALS * limit : any <= TRIALS * limit, `${any} of ${TRIALS}`);
    });
  }
  test("every cell together: at most 5% of orgs", () => {
    console.log(`dated noise, every cell: ${all} of ${orgs} orgs`);
    assert.ok(all <= orgs * 0.05, `${all} of ${orgs}`);
  });
});

function digestsOf(runDir: string): [string, string] {
  const loaded = loadRun(runDir);
  const madeAt = "2026-11-24T09:00:00.000Z";
  const without = { ...loaded.input, findings: loaded.input.findings.filter((f) => f.ruleId !== RULE), metrics: loaded.input.metrics.filter((m) => m.id !== PATH_VARIANTS) };
  // Apart from the digest's own paths block (DECISIONS.md 758), which serves the raised insights alone.
  const { digest, pathsWithheld: _w, ...counts } = checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt });
  const { paths: _p, ...rest } = digest ?? {};
  return [JSON.stringify({ digest: digest ? rest : null, ...counts }), JSON.stringify(checkedDigest({ input: without, pseudonyms: loaded.pseudonyms, inventory: null, madeAt }))];
}

describe("the synthetic org keeps the insights in its files and serves none of them outside the digest's paths block (decisions 703, 758)", async () => {
  const automatic = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });

  test("the consultant-led run's files hold the rule and the table; what the rule says is the honest result", () => {
    const loaded = loadRun(dir);
    const ours = loaded.input.findings.filter((f) => f.ruleId === RULE);
    assert.equal(ours.length, 1);
    console.log(`synthetic org: ${ours.map((f) => `${f.status}: ${f.title} / ${f.summary}`).join(" | ")}`);
    assert.ok(loaded.input.metrics.some((m) => m.id === PATH_VARIANTS));
  });

  test("the digest of a consultant-led and an automatic run, apart from its paths block, is byte for byte the one without them", () => {
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
