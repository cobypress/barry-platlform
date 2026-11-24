import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { HistoryRow, OpportunityExtract, OpportunityRow, StageRow } from "../../extract/opportunities.ts";
import { ENGINE_STAGES_SOQL, PROBABILITY_FIELD, probabilityRefusal, STAGES_SOQL, stagesExtractor } from "../../extract/opportunities.ts";
import type { Sourced } from "../../extract/types.ts";
import { notMeasurableKind } from "../../engine/not-measurable.ts";
import { runHealthCheck } from "../../engine/run.ts";
import type { Calibration, Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { historicPipelineModule } from "../historic-pipeline/index.ts";
import { PROBABILITY_METRIC, PROBABILITY_RULE, reached } from "../historic-pipeline/probability/index.ts";
import { outside, wilson } from "../historic-pipeline/probability/wilson.ts";
import { checkedDigest } from "../../digest/checked.ts";
import { buildReport } from "../../narrative/sections.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { executiveHtml } from "../../render/executive.ts";
import { fullReportHtml } from "../../render/full-report.ts";
import { onePagerHtml } from "../../render/one-pager.ts";
import { loadRun } from "../../render/render-run.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { authorise } from "../../salesforce/allowlist.ts";
import { orgStages } from "../../connect/org-stages.ts";
import { readCheckpoint, writeCheckpoint } from "../../engine/checkpoint.ts";
import { renderRawNotesDocx } from "../../render/raw-notes-docx.ts";
import { strFromU8, unzipSync } from "fflate";
import { dirname, join } from "node:path";
import { readFileSync, writeFileSync } from "node:fs";

// Each open stage's probability in Salesforce against how often the closed
// deals that reached it were won (DECISIONS.md 706), on deal histories built
// by hand, then on the synthetic org.

const { dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration: Calibration = { ...loadCalibration(), minSample: 5 };
const real: Calibration = { ...loadCalibration() };
const DAY = 86_400_000;
const RUN = Date.parse(AS_OF);
const WINDOW_FROM = Date.parse((JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as { analysisWindow: { from: string } }).analysisWindow.from);
type Measured = Extract<Metric, { status: "measured" }>;

// ─── Hand-built deals ────────────────────────────────────────────────────────

type Outcome = "won" | "lost" | "no_decision";
/**
 * `segment` is set as the deal's type, lead source and record type alike. `late`: made that many days before its
 * history starts. Dated deals give `at` (made), `enter` (each stage of the path entered) and `closedAt`, null while
 * the deal is still open at the run's date; without them a deal is made in April 2025 and closes 40 days later.
 */
type Spec = { path: string[]; outcome: Outcome; segment?: string; late?: number; owner?: number; at?: number; enter?: number[]; closedAt?: number | null };
const CLOSED: Record<Outcome, string> = { won: "Closed Won", lost: "Closed Lost", no_decision: "Closed No Decision" };
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");

/** `count` deals along `path`, `won` of them won and the rest lost. */
const deals = (count: number, path: string[], won: number, extra: Partial<Spec> = {}): Spec[] =>
  Array.from({ length: count }, (_, i) => ({ path, outcome: i < won ? "won" : "lost", ...extra }));

const USUAL = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
const [A, B, C, D] = USUAL as [string, string, string, string];
/** The synthetic calibration's open stages, set as the tests need. */
const PROBABILITIES: Record<string, number> = { Prospecting: 20, Qualified: 25, Discovery: 30, Proposal: 50, Negotiation: 70, "Legal Review": 90 };

type Stages = "ok" | "no probabilities" | "unavailable";

/** The recorded extracts with every opportunity and history row replaced by these deals, and the stages' probabilities set. */
function extractsOf(specs: Spec[], options: { history?: "ok" | "none" | "unavailable"; stages?: Stages; probabilities?: Record<string, number | null> } = {}): Map<string, unknown> {
  const changed = clone(extracts);
  const e = changed.get("opportunities") as OpportunityExtract;
  const owners = e.opportunities.status === "ok" ? [...new Set(e.opportunities.data.map((o) => o.OwnerId))] : ["005000000000000001"];
  const opportunities: OpportunityRow[] = [];
  const rows: HistoryRow[] = [];
  specs.forEach((s, i) => {
    const id = `006${String(900_000 + i).padStart(15, "0")}`;
    const start = s.at ?? Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const created = start - (s.late ?? 0) * DAY;
    const enter = s.enter ?? s.path.map((_, k) => start + k * 60_000);
    const closed = s.closedAt === undefined ? start + 40 * DAY : s.closedAt;
    const closeDate = new Date(closed ?? RUN + 60 * DAY).toISOString().slice(0, 10);
    const stage = closed === null ? s.path.at(-1)! : CLOSED[s.outcome];
    opportunities.push({ Id: id, OwnerId: owners[(s.owner ?? 0) % owners.length]!, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: closed !== null, IsWon: closed !== null && s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
    s.path.forEach((name, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: name, Amount: null, CloseDate: closeDate, CreatedDate: stamp(enter[k]!) }));
    if (closed !== null) rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(closed) });
  });
  if (e.opportunities.status === "ok") e.opportunities = { ...e.opportunities, data: opportunities };
  if (e.history.status === "ok") {
    const history = options.history ?? "ok";
    if (history === "ok") e.history = { ...e.history, data: rows };
    if (history === "none") e.history = { ...e.history, data: [] };
    if (history === "unavailable") e.history = { status: "unavailable", reason: "the read was refused", source: e.history.source } as OpportunityExtract["history"];
  }
  const stages = changed.get("opportunity-stages") as Sourced<StageRow[]>;
  assert.equal(stages.status, "ok");
  if (stages.status === "ok") {
    const set = { ...PROBABILITIES, ...options.probabilities };
    const read = options.stages ?? "ok";
    const data = stages.data.map((r) => ({ ...r, DefaultProbability: read === "ok" && r.ApiName in set ? set[r.ApiName]! : read === "ok" ? r.DefaultProbability : null }));
    changed.set(
      "opportunity-stages",
      read === "unavailable" ? { status: "unavailable", reason: "INSUFFICIENT_ACCESS: no", source: stages.source } : { ...stages, data, ...(read === "no probabilities" ? { missingFields: [PROBABILITY_FIELD] } : {}) },
    );
  }
  return changed;
}

function analysed(specs: Spec[], options: Parameters<typeof extractsOf>[1] & { calibration?: Calibration } = {}): { findings: Finding[]; metric: Metric } {
  const output = analyseExtracts(extractsOf(specs, options), { calibration: options.calibration ?? calibration, modules: [historicPipelineModule] });
  // Kept apart from what the documents read, as the path insights are (DECISIONS.md 703, 706).
  assert.ok(!output.findings.some((f) => f.ruleId === PROBABILITY_RULE) && !output.metrics.some((m) => m.id === PROBABILITY_METRIC));
  return { findings: output.insights.filter((f) => f.ruleId === PROBABILITY_RULE), metric: output.insightMetrics.find((m) => m.id === PROBABILITY_METRIC)! };
}

const issues = (findings: Finding[]) => findings.filter((f) => f.status === "issue");
const segmentOf = (m: Metric, dimension: string, key: string) => (m as Measured).segments.find((s) => s.dimension === dimension && s.key === key);
const reachedOf = (m: Metric, stage: string) => segmentOf(m, "reached, then won", stage)!.n;

// ─── The interval ────────────────────────────────────────────────────────────

describe("the Wilson interval", () => {
  test("matches the textbook values, stays inside 0 to 1, and is null with no deals", () => {
    const i = wilson(5, 10, 1.96)!;
    assert.ok(Math.abs(i.low - 0.2366) < 1e-4 && Math.abs(i.high - 0.7634) < 1e-4, JSON.stringify(i));
    const none = wilson(0, 10, 1.96)!;
    assert.equal(none.low, 0);
    assert.ok(Math.abs(none.high - 0.2775) < 1e-4, JSON.stringify(none));
    const all = wilson(10, 10, 1.96)!;
    assert.equal(all.high, 1);
    assert.ok(Math.abs(all.low - 0.7225) < 1e-4);
    assert.equal(wilson(0, 0, 1.96), null);
    assert.equal(wilson(11, 10, 1.96), null);
  });

  test("narrows with more deals and widens with a higher bar", () => {
    const [small, big] = [wilson(30, 100, 2)!, wilson(300, 1000, 2)!];
    assert.ok(big.high - big.low < small.high - small.low);
    const [loose, strict] = [wilson(30, 100, 2)!, wilson(30, 100, 2.8)!];
    assert.ok(strict.low < loose.low && strict.high > loose.high);
    assert.ok(outside(0.5, small) && !outside(0.3, small));
  });
});

// ─── Reached ─────────────────────────────────────────────────────────────────

describe("a stage is reached when the deal's own history shows it there", () => {
  test("a deal whose history starts at Proposal didn't reach Prospecting or Discovery; one that skipped Discovery didn't reach it", () => {
    const { metric } = analysed([...deals(10, USUAL, 5), ...deals(6, [C, D], 3), ...deals(7, [A, C, D], 4)]);
    assert.equal(metric.status, "measured");
    assert.equal(reachedOf(metric, A), 17);
    assert.equal(reachedOf(metric, B), 10);
    assert.equal(reachedOf(metric, C), 23);
    assert.equal(reachedOf(metric, D), 23);
    assert.equal(segmentOf(metric, "reached, then won", A)!.value, 9);
    assert.equal(reached({ path: [C, D] }, A), false);
  });

  test("closed deals are counted by where they went, never by the stage they are in now", () => {
    // Every deal here is closed: none is in an open stage today, and each still reached Negotiation.
    const { metric } = analysed(deals(30, USUAL, 21));
    assert.equal(reachedOf(metric, D), 30);
  });

  test("deals created before the stage history starts are left out, and the metric says how many", () => {
    // The first 12 deals were made 30 days before their history starts, and so before the org's does.
    const { metric } = analysed([...deals(12, USUAL, 0, { late: 30 }), ...deals(20, USUAL, 10)]);
    assert.equal(metric.status, "measured");
    assert.equal((metric as Measured).n, 20);
    assert.equal((metric as Measured).stats.beforeHistory, 12);
    assert.ok(metric.assumptions.some((a) => a.startsWith("12 deals created before your stage history starts are left out")));
  });

  test("no decision counts as not won", () => {
    const { metric } = analysed([...deals(10, USUAL, 5), ...Array.from({ length: 10 }, () => ({ path: USUAL, outcome: "no_decision" as const }))]);
    assert.equal(segmentOf(metric, "won share", D)!.value, 25);
  });
});

// ─── When a stage is named ───────────────────────────────────────────────────

describe("a stage is named only past every bar", () => {
  test("named: the sentence first, the org's figures, a direction and no cause", () => {
    // Negotiation set to 90%: 66 of 100 that reached it were won.
    const { findings } = analysed(deals(100, USUAL, 66), { calibration: real, probabilities: { Prospecting: 66, Discovery: 66, Proposal: 66, Negotiation: 90 } });
    const [f, ...rest] = issues(findings);
    assert.equal(rest.length, 0);
    assert.equal(f!.title, "The probability on Negotiation in Salesforce is higher than how often its deals are won");
    assert.match(f!.summary, /^Negotiation is set to 90% in Salesforce\. Of the 100 deals that reached it between [A-Z][a-z]+ \d{4} and [A-Z][a-z]+ \d{4}, 66% were won, so it is set higher than what happens\./);
    assert.match(f!.summary, /it doesn't say why they differ\.$/);
    assert.equal(f!.severity, "info");
    assert.deepEqual(f!.subjects.map((s) => s.label), [D]);
    // No number to set it to, no money, no forecast error.
    assert.doesNotMatch(`${f!.summary} ${f!.recommendation}`, /should be|set it to|change it to|£|\$|€|forecast is (?:off|wrong)/i);
  });

  test("one finding per run, naming every stage past the bars, each in its own direction", () => {
    const { findings } = analysed(deals(100, USUAL, 50), { calibration: real, probabilities: { Prospecting: 20, Discovery: 50, Proposal: 50, Negotiation: 80 } });
    assert.equal(findings.length, 1);
    const [f] = issues(findings);
    assert.equal(f!.title, "The probabilities on 2 stages in Salesforce are higher or lower than how often their deals are won");
    assert.match(f!.summary, /Prospecting is set to 20% .* so it is set lower than what happens\. (?:[^.]*\. )?Negotiation is set to 80% .* so it is set higher than what happens\./);
  });

  test("too few deals, won or lost: not named, however big the gap", () => {
    const set = { Prospecting: 99, Discovery: 99, Proposal: 99, Negotiation: 99 };
    assert.equal(issues(analysed(deals(19, USUAL, 9), { calibration: real, probabilities: set }).findings).length, 0);
    // 40 deals, 4 lost: under the 5 lost a stage needs.
    assert.equal(issues(analysed(deals(40, USUAL, 36), { calibration: real, probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: 50 } }).findings).length, 0);
    // 40 deals, 4 won.
    assert.equal(issues(analysed(deals(40, USUAL, 4), { calibration: real, probabilities: set }).findings).length, 0);
  });

  test("a gap of 10 points or more, but inside the interval: not named", () => {
    // 13 of 20 won (65%) against 50%: a 15-point gap that 20 deals can't tell from chance.
    const { findings, metric } = analysed(deals(20, USUAL, 13), { calibration: real, probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: 50 } });
    assert.equal(issues(findings).length, 0);
    assert.ok(segmentOf(metric, "won share interval low", D)!.value! < 50);
  });

  test("outside the interval, but under 10 points: not named", () => {
    const { findings } = analysed(deals(2000, USUAL, 1100), { calibration: real, probabilities: { Prospecting: 55, Discovery: 55, Proposal: 55, Negotiation: 49 } });
    assert.equal(issues(findings).length, 0);
  });

  test("with more than one record type, the gap must hold by 10 points inside one with enough deals", () => {
    const probabilities = { Prospecting: 90, Discovery: 90, Proposal: 90, Negotiation: 90 };
    // X: 40 deals at 82%, 8 points under. Y: 15 deals, under the minimum, at 20%. Overall 65%, 25 under, in no record type.
    const blended = [...deals(40, USUAL, 33, { segment: "X" }), ...deals(15, USUAL, 3, { segment: "Y" })];
    assert.equal(issues(analysed(blended, { calibration: real, probabilities }).findings).length, 0);
    // Y given enough deals: the gap holds inside it.
    const held = [...deals(40, USUAL, 33, { segment: "X" }), ...deals(30, USUAL, 6, { segment: "Y" })];
    assert.equal(issues(analysed(held, { calibration: real, probabilities }).findings).length, 1);
  });

  test("with no segment to check it, named at the lowest confidence and saying so", () => {
    const { findings } = analysed(deals(100, USUAL, 50), { calibration: real, probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: 80 } });
    const [f] = issues(findings);
    assert.equal(f!.confidence, "low");
    assert.match(f!.summary, /We couldn't check this within teams, reps or other groups: none had enough of these deals\./);
  });

  test("a stage with no probability set is listed and never named", () => {
    const { findings, metric } = analysed(deals(100, USUAL, 50), { calibration: real, probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: null } });
    assert.equal(issues(findings).length, 0);
    assert.equal(segmentOf(metric, "probability in Salesforce", D)!.value, null);
  });

  test("the pass says what was tested", () => {
    const { findings } = analysed(deals(100, USUAL, 50), { calibration: real, probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: 50 } });
    assert.equal(findings[0]!.status, "passed");
    assert.match(findings[0]!.summary!, /beyond what chance could make of the 4 stages we could test\.$/);
  });
});

// ─── Who is counted: the deals that reached a stage in its period ───────────

describe("each stage counts the deals that reached it in its period, open or closed (fix round 1)", () => {
  const set = { Prospecting: 66, Discovery: 66, Proposal: 66, Negotiation: 90 };
  /** `n` deals along the usual path, made `daysAgo` before the run, still open. */
  const openDeals = (n: number, at: number): Spec[] => Array.from({ length: n }, (_, i) => ({ path: USUAL, outcome: "lost", at: at + i * 60_000, enter: USUAL.map((_, k) => at + i * 60_000 + k * 1000), closedAt: null }));

  test("deals that reached it within the lag aren't counted yet, so the recent open ones don't hold it back", () => {
    // 100 reached Negotiation in April 2025 and closed 40 days later; 20 more reached it 10 days before the run.
    const { findings, metric } = analysed([...deals(100, USUAL, 66), ...openDeals(20, RUN - 10 * DAY)], { calibration: real, probabilities: set });
    const stats = (metric as Measured).stats;
    assert.equal(stats.stage4LagDays, 40);
    assert.equal(stats.stage4Cohort, 100);
    assert.equal(stats.stage4Open, 0);
    assert.ok(metric.assumptions.includes("Negotiation: 90% of the closed deals that reached it had closed within 40 days, so its period ends on 2026-09-21"), metric.assumptions.join(" | "));
    assert.equal(issues(findings).length, 1);
  });

  test("more than 5% of the period's deals still open: not named, and the metric says why", () => {
    // 10 of 110 that reached it in April 2025 are still open: 9%.
    const { findings, metric } = analysed([...deals(100, USUAL, 66), ...openDeals(10, Date.parse("2025-04-20T09:00:00Z"))], { calibration: real, probabilities: set });
    assert.equal(issues(findings).length, 0);
    assert.equal(segmentOf(metric, "still open", D)!.value, 10);
    assert.equal(segmentOf(metric, "won share", D)!.notMeasurableReason, "Too many of the deals that reached Negotiation in this period are still open to say how often they are won");
  });

  test("5% or fewer still open: named, saying how many are open and over what the share is", () => {
    const { findings } = analysed([...deals(100, USUAL, 66), ...openDeals(5, Date.parse("2025-04-20T09:00:00Z"))], { calibration: real, probabilities: set });
    const [f] = issues(findings);
    assert.match(f!.summary, /^Negotiation is set to 90% in Salesforce\. Of the 105 deals that reached it between [A-Z][a-z]+ \d{4} and September 2026, 5 are still open and 66% of the other 100 were won, so it is set higher than what happens\./);
  });

  test("chosen by when they reached it, not when they closed: deals that closed in the window after reaching it before don't count", () => {
    // 60 reached Negotiation before the window and closed in it, all won: by close date they'd lift the share to 79%.
    const early = WINDOW_FROM - 30 * DAY;
    const before = Array.from({ length: 60 }, (_, i): Spec => ({ path: USUAL, outcome: "won", at: early + i * 60_000, enter: USUAL.map((_, k) => early + i * 60_000 + k * 1000), closedAt: WINDOW_FROM + 10 * DAY }));
    const { metric } = analysed([...deals(100, USUAL, 66), ...before], { calibration: real, probabilities: set });
    assert.equal((metric as Measured).stats.stage4Reached, 100);
    assert.equal(segmentOf(metric, "won share", D)!.value, 66);
  });
});

describe("the ways the deals are split (rule 4)", () => {
  test("each way said once by name, record type left to its own check", () => {
    const { findings } = analysed([...deals(50, USUAL, 33, { segment: "X" }), ...deals(50, USUAL, 33, { segment: "Y" })], { calibration: real, probabilities: { Prospecting: 66, Discovery: 66, Proposal: 66, Negotiation: 90 } });
    const [f] = issues(findings);
    assert.match(f!.summary, / It is on the same side of the setting in each of the 2 ways we could split these deals \(by lead source and type\)\. /);
    assert.doesNotMatch(f!.summary, /record type/);
  });

  test("a segment holding 90% of the deals or more checks nothing", () => {
    // Lead source and type: A holds 95 of 100, so only B's 5 are left to compare, and one segment is no way to split them.
    const { findings } = analysed([...deals(95, USUAL, 63, { segment: "A" }), ...deals(5, USUAL, 3, { segment: "B" })], { probabilities: { Prospecting: 66, Discovery: 66, Proposal: 66, Negotiation: 90 } });
    const [f] = issues(findings);
    assert.match(f!.summary, /We couldn't check this within teams, reps or other groups: none had enough of these deals\./);
    assert.equal(f!.confidence, "low");
  });
});

// ─── Not measurable ──────────────────────────────────────────────────────────

describe("not measurable, with the plain reason, sorted into its kind", () => {
  const usual = deals(20, USUAL, 12);
  const check = (result: { findings: Finding[]; metric: Metric }, reason: RegExp, kind: string) => {
    const [f] = result.findings;
    assert.equal(f!.status, "not_measurable");
    assert.match(f!.notMeasurableReason!, reason);
    assert.equal(notMeasurableKind(f!.notMeasurableReason!), kind);
    assert.equal(result.metric.status, "not_measurable");
    assert.equal((result.metric as Extract<Metric, { status: "not_measurable" }>).reason, f!.notMeasurableReason);
  };

  test("the probabilities weren't read", () => check(analysed(usual, { stages: "no probabilities" }), /^Salesforce didn't give us the probability set on each stage: it answered that the field isn't there for the connected user, so there's nothing to hold how often deals are won against\.$/, "unreadable"));
  test("the stage settings weren't read", () => check(analysed(usual, { stages: "unavailable" }), /^Salesforce's stage settings couldn't be read, so the probability set on each stage isn't known: /, "unreadable"));
  test("stage history couldn't be read", () => check(analysed(usual, { history: "unavailable" }), /^OpportunityHistory couldn't be read: /, "unreadable"));
  test("stage history isn't tracked", () => check(analysed(usual, { history: "none" }), /^Stage history isn't tracked for these deals: none of the 20 deals that closed in the window has a recorded open stage\.$/, "too_little_data"));
  test("too few closed deals", () => check(analysed(usual.slice(0, 4)), /^4 closed deals with stage history is below the calibrated minimum of 5\.$/, "too_little_data"));
  test("too few closed deals once those made before the history are left out", () => check(analysed([...deals(10, USUAL, 5, { late: 30 }), ...deals(4, USUAL, 2)]), /^4 closed deals created since your stage history starts is below the calibrated minimum of 5\.$/, "too_little_data"));
  test("a stage with no meaning", () => check(analysed([...usual, ...deals(5, [A, "Mystery"], 0)]), /^The calibration doesn't say what these stages mean: Mystery\.$/, "needs_input"));
  test("no stage reached has a probability", () => {
    const none = { Prospecting: null, Discovery: null, Proposal: null, Negotiation: null };
    check(analysed(usual, { probabilities: none }), /^None of the open stages your closed deals reached has a probability set in Salesforce\.$/, "unreadable");
  });
});

// ─── An automatic run's assumptions ──────────────────────────────────────────

describe("on an automatic run the pipeline's own assumptions attach, as on every pipeline figure", () => {
  test("blended record types and a stage read from Salesforce's flags are said on the metric and the finding", () => {
    // Negotiation left out of the calibration, so its meaning comes from Salesforce's flags (598); two record types (692).
    const automatic: Calibration = { ...real, source: "org_settings", stages: real.stages.filter((s) => s.name !== D) };
    const specs = [...deals(60, USUAL, 30, { segment: "X" }), ...deals(60, USUAL, 30, { segment: "Y" })];
    const output = analyseExtracts(extractsOf(specs, { probabilities: { Prospecting: 50, Discovery: 50, Proposal: 50, Negotiation: 80 } }), { calibration: automatic, modules: [historicPipelineModule] });
    const metric = output.insightMetrics.find((m) => m.id === PROBABILITY_METRIC)!;
    const finding = output.insights.find((f) => f.ruleId === PROBABILITY_RULE)!;
    const winRate = output.metrics.find((m) => m.id === "historic.win_rate")!;
    const notes = winRate.assumptions.filter((a) => /record types|Salesforce's own setting/.test(a));
    assert.equal(notes.length, 2, winRate.assumptions.join(" | "));
    for (const note of notes) assert.ok(metric.assumptions.includes(note), note);
    assert.equal(finding.status, "issue");
    assert.deepEqual(finding.subjects.map((s) => s.label), [D]);
    for (const note of notes) assert.ok(JSON.stringify(finding).includes(note), note);
    assert.notEqual(finding.confidence, "high");
  });
});

// ─── Org words ───────────────────────────────────────────────────────────────

describe("stage names are marked as the org's words", () => {
  test("every mark is a stage name, and every stage name is marked", () => {
    const { findings } = analysed(deals(100, USUAL, 50), { calibration: real, probabilities: { Prospecting: 20, Discovery: 50, Proposal: 50, Negotiation: 80 } });
    const [f] = issues(findings);
    for (const key of ["title", "summary"] as const) {
      const spans = f!.orgWords?.[key]?.spans;
      assert.ok(spans, key);
      assert.ok(spans.map((s) => f![key].slice(s.start, s.end)).every((w) => USUAL.includes(w)), key);
    }
    assert.deepEqual(f!.orgWords!.summary!.spans.map((s) => f!.summary.slice(s.start, s.end)), [A, D]);
    // The title of two stages names none; the recommendation names none.
    assert.doesNotMatch(f!.title, /Prospecting|Negotiation/);
  });
});

// ─── The read ────────────────────────────────────────────────────────────────

describe("the stage read (decision 706)", () => {
  test("the engine's read passes the real allowlist; the connect job's read is unchanged", () => {
    for (const soql of [ENGINE_STAGES_SOQL, STAGES_SOQL]) assert.equal(authorise({ method: "GET", path: `/services/data/v66.0/query?q=${encodeURIComponent(soql)}` }), "rest.query");
    assert.equal(STAGES_SOQL, "SELECT ApiName, MasterLabel, IsActive, IsClosed, IsWon, SortOrder FROM OpportunityStage");
    assert.match(readFileSync(new URL("../../connect/org-stages.ts", import.meta.url), "utf8"), /client\.query<unknown>\(STAGES_SOQL,/);
    // What the connect job keeps of a stage: no probability.
    assert.ok(!("defaultProbability" in orgStages([{ ApiName: "A", MasterLabel: "A", IsActive: true, IsClosed: false, IsWon: false, SortOrder: 1, DefaultProbability: 10 }])[0]!));
  });

  const simulatedRun = async (failQuery?: (soql: string) => { errorCode: string; message: string } | null) => {
    const transport = new Simulator({ failQuery });
    const { dir: runDir, run } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: [historicPipelineModule], outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER });
    const read = <T>(file: string) => JSON.parse(readFileSync(join(runDir, file), "utf8")) as T;
    return { dir: runDir, run, transport, findings: read<Finding[]>("findings.json"), metrics: read<Metric[]>("metrics.json") };
  };
  const stageQueries = (t: Simulator): string[] => t.requests.map((r) => new URL(r.url).searchParams.get("q") ?? "").filter((q) => /FROM OpportunityStage/.test(q));
  const refused = (code: string, message = "No such column 'DefaultProbability' on entity 'OpportunityStage'.") => (soql: string) => (/DefaultProbability/.test(soql) ? { errorCode: code, message } : null);
  // Every other finding and metric, with the stage read's evidence naming the query, which on a refusal is honestly the plain one.
  const strip = <T extends { ruleId?: string; id?: string }>(rows: T[]) =>
    JSON.parse(JSON.stringify(rows.filter((x) => x.ruleId !== PROBABILITY_RULE && x.id !== PROBABILITY_METRIC)).replaceAll(ENGINE_STAGES_SOQL, STAGES_SOQL)) as T[];

  test("refused, the stages are read again without it; only this comparison goes without, and nothing else in the run moves", async () => {
    const [normal, without] = [await simulatedRun(), await simulatedRun(refused("INVALID_FIELD"))];
    const stages = readCheckpoint<Sourced<StageRow[]>>(join(without.dir, "checkpoints", "opportunity-stages.json"));
    assert.equal(stages.status, "ok");
    assert.ok(stages.status === "ok" && stages.missingFields?.includes(PROBABILITY_FIELD) && stages.data.every((r) => r.DefaultProbability === null));
    assert.equal(stages.status === "ok" && stages.source.kind === "soql" && stages.source.query, STAGES_SOQL);
    const ours = without.findings.find((f) => f.ruleId === PROBABILITY_RULE)!;
    assert.equal(ours.status, "not_measurable");
    assert.match(ours.notMeasurableReason!, /^Salesforce didn't give us the probability/);
    assert.equal(normal.findings.find((f) => f.ruleId === PROBABILITY_RULE)!.status, "issue");
    assert.ok(JSON.stringify(normal.findings).includes(ENGINE_STAGES_SOQL));
    assert.deepEqual(strip(without.findings), strip(normal.findings));
    assert.deepEqual(strip(without.metrics), strip(normal.metrics));
  });

  test("refused for access, the same, and the reason says which", async () => {
    const { findings, transport } = await simulatedRun(refused("INSUFFICIENT_ACCESS", "insufficient access rights on object id"));
    assert.equal(findings.find((f) => f.ruleId === PROBABILITY_RULE)!.notMeasurableReason, "Salesforce refused the connected user the probability set on each stage, so there's nothing to hold how often deals are won against.");
    assert.deepEqual(stageQueries(transport), [ENGINE_STAGES_SOQL, STAGES_SOQL]);
  });

  test("a refusal's class is read from the reason as readableReason gives it; anything else is no refusal of the field", () => {
    assert.equal(probabilityRefusal("OpportunityStage.DefaultProbability isn't in this org."), "invalid_field");
    assert.equal(probabilityRefusal("INVALID_FIELD: DefaultProbability"), "invalid_field");
    assert.equal(probabilityRefusal("INSUFFICIENT_ACCESS: insufficient access rights on object id"), "no_access");
    assert.equal(probabilityRefusal("INSUFFICIENT_ACCESS_OR_READONLY: no"), "no_access");
    for (const reason of ["QUERY_TIMEOUT: Your query request was running for too long.", "UNKNOWN_EXCEPTION: An unexpected error occurred.", "OpportunityStage.IsWon isn't in this org.", "Opportunity.DefaultProbability isn't in this org."]) assert.equal(probabilityRefusal(reason), null, reason);
  });

  test("any other failure is the stage read's own, as before: no read without the field, the stages unread", async () => {
    for (const code of ["QUERY_TIMEOUT", "UNKNOWN_EXCEPTION"]) {
      const { dir: runDir, findings, transport } = await simulatedRun((soql) => (/FROM OpportunityStage/.test(soql) ? { errorCode: code, message: "Try again later." } : null));
      assert.deepEqual(stageQueries(transport), [ENGINE_STAGES_SOQL], code);
      const stages = readCheckpoint<Sourced<StageRow[]>>(join(runDir, "checkpoints", "opportunity-stages.json"));
      assert.equal(stages.status, "unavailable", code);
      assert.match(findings.find((f) => f.ruleId === PROBABILITY_RULE)!.notMeasurableReason!, new RegExp(`^Salesforce's stage settings couldn't be read, so the probability set on each stage isn't known: ${code}`));
    }
    // Refused both ways: the stages are unread, as before 706.
    const both = await simulatedRun((soql) => (/FROM OpportunityStage/.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "no" } : null));
    const stages = readCheckpoint<Sourced<StageRow[]>>(join(both.dir, "checkpoints", "opportunity-stages.json"));
    assert.equal(stages.status, "unavailable");
    assert.equal(stages.source.kind === "soql" && stages.source.query, STAGES_SOQL);
  });

  test("a run in flight with a version 1 checkpoint of the stages reads them again on resume, with the probabilities", async () => {
    const first = await simulatedRun();
    const path = join(first.dir, "run.json");
    const state = JSON.parse(readFileSync(path, "utf8")) as { status: string; steps: Array<{ id: string; version: number }> };
    state.status = "stopped_budget";
    state.steps.find((s) => s.id === stagesExtractor.id)!.version = 1;
    writeFileSync(path, JSON.stringify(state));
    // The checkpoint as version 1 wrote it: no probabilities.
    const checkpoint = join(first.dir, "checkpoints", "opportunity-stages.json");
    const old = readCheckpoint<Sourced<StageRow[]>>(checkpoint);
    assert.equal(old.status, "ok");
    if (old.status === "ok") writeCheckpoint(checkpoint, { ...old, data: old.data.map(({ DefaultProbability: _p, ...r }) => r), source: { ...old.source, query: STAGES_SOQL } });
    const transport = new Simulator();
    const resumed = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: [historicPipelineModule], outDir: dirname(first.dir), resume: first.run.id, now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER });
    assert.deepEqual(stageQueries(transport), [ENGINE_STAGES_SOQL]);
    assert.equal(resumed.run.steps.find((s) => s.id === stagesExtractor.id)!.version, 2);
    const again = readCheckpoint<Sourced<StageRow[]>>(checkpoint);
    assert.ok(again.status === "ok" && again.data.some((r) => typeof r.DefaultProbability === "number"));
  });
});

// ─── Noise ───────────────────────────────────────────────────────────────────

// Every deal starts at Prospecting and moves on, or is lost, so that a deal that reached each stage is won exactly as
// often as the stage's probability says. Any stage named is a false one. Fixed seeds.
describe("noise: when the probabilities are true, at most 5% of orgs get the finding", () => {
  const TRUE = [20, 30, 50, 70];
  let seed = 54321;
  const rnd = () => ((seed = (seed * 1103515245 + 12345) % 2147483648), seed / 2147483648);
  const trueOrg = (n: number, segs: boolean): Spec[] =>
    Array.from({ length: n }, () => {
      const path = [A];
      let won = false;
      for (let k = 0; ; k++) {
        const on = k + 1 < TRUE.length ? TRUE[k]! / TRUE[k + 1]! : TRUE[k]! / 100;
        if (rnd() >= on) break;
        if (k + 1 === TRUE.length) {
          won = true;
          break;
        }
        path.push(USUAL[k + 1]!);
      }
      return { path, outcome: won ? "won" : "lost", ...(segs ? { segment: ["S1", "S2", "S3"][Math.floor(rnd() * 3)], owner: Math.floor(rnd() * 4) } : {}) } as Spec;
    });
  const probabilities = { Prospecting: 20, Discovery: 30, Proposal: 50, Negotiation: 70 };
  const SIZES = [
    ["60 deals, no segments", 60, false],
    ["120 deals, 3 segments and 4 reps", 120, true],
    ["300 deals, segments", 300, true],
    ["600 deals, no segments", 600, false],
  ] as const;
  const TRIALS = 200;
  for (const [label, n, segs] of SIZES) {
    test(label, () => {
      seed = 54321 + n + (segs ? 1 : 0);
      let any = 0;
      for (let t = 0; t < TRIALS; t++) if (issues(analysed(trueOrg(n, segs), { calibration: real, probabilities }).findings).length) any++;
      console.log(`noise ${label}: ${any} of ${TRIALS} orgs`);
      assert.ok(any <= TRIALS * 0.05, `${any} of ${TRIALS}`);
    });
  }

  test("the generator is true to the probabilities", () => {
    seed = 1;
    const many = trueOrg(20_000, false);
    USUAL.forEach((stage, k) => {
      const there = many.filter((s) => s.path.includes(stage));
      const share = (100 * there.filter((s) => s.outcome === "won").length) / there.length;
      assert.ok(Math.abs(share - TRUE[k]!) < 2, `${stage}: ${share}`);
    });
  });
});

// The reviewer's dated generator: deals made over four years at a rate growing (or shrinking) by `growth` a year,
// each walking the stages 20 days apart with true reached-then-won chances, then closing after an exponential time,
// wins slower than losses. Open at the run's date where they haven't closed. Sampled by close date, a shrinking org
// got the finding in a third of orgs; chosen by when they reached each stage, none may get it more than 5% of the time.
describe("noise on dated deals: arrivals growing or shrinking, wins slower than losses, at most 5% of orgs in every cell", () => {
  const TRUE = [20, 30, 50, 70];
  const probabilities = { Prospecting: 20, Discovery: 30, Proposal: 50, Negotiation: 70 };
  let seed = 1;
  // mulberry32: an LCG's successive draws are correlated, which can tie an outcome to its time.
  const rnd = (): number => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const walk = (): { path: string[]; won: boolean } => {
    const path = [A];
    for (let k = 0; ; k++) {
      const on = k + 1 < TRUE.length ? TRUE[k]! / TRUE[k + 1]! : TRUE[k]! / 100;
      if (rnd() >= on) return { path, won: false };
      if (k + 1 === TRUE.length) return { path, won: true };
      path.push(USUAL[k + 1]!);
    }
  };
  /** `n` deals the opportunities read would hold: open, or made or closed in the window. */
  function datedOrg(n: number, growth: number, win: number, loss = 30): Spec[] {
    const span = 4 * 365;
    const r = Math.log(growth) / 365;
    const out: Spec[] = [];
    for (let made = 0; out.length < n && made < n * 50; made++) {
      const u = rnd();
      const t = r === 0 ? u * span : Math.log(1 + u * (Math.exp(r * span) - 1)) / r;
      const at = RUN - (span - t) * DAY;
      const w = walk();
      const enter = w.path.map((_, k) => at + k * 20 * DAY);
      const closedAt = enter.at(-1)! + -Math.log(1 - rnd()) * (w.won ? win : loss) * DAY;
      const segment = ["S1", "S2", "S3"][Math.floor(rnd() * 3)]!;
      const owner = Math.floor(rnd() * 4);
      if (closedAt <= RUN) {
        if (closedAt < WINDOW_FROM && at < WINDOW_FROM) continue;
        out.push({ path: w.path, outcome: w.won ? "won" : "lost", at, enter, closedAt, segment, owner });
      } else {
        const k = enter.filter((e) => e <= RUN).length;
        out.push({ path: w.path.slice(0, k), outcome: "lost", at, enter: enter.slice(0, k), closedAt: null, segment, owner });
      }
    }
    return out;
  }
  const TRIALS = 100;
  for (const growth of [1, 1.5, 2, 0.5]) {
    for (const win of [90, 120, 180]) {
      for (const n of [300, 1000]) {
        test(`growth ${growth}x a year, wins closing at ${win} days, ${n} deals`, () => {
          seed = 7_000_000 + growth * 100_000 + win * 100 + n;
          let any = 0;
          for (let t = 0; t < TRIALS; t++) if (issues(analysed(datedOrg(n, growth, win), { calibration: real, probabilities }).findings).length) any++;
          console.log(`dated noise growth=${growth} win=${win} n=${n}: ${any} of ${TRIALS} orgs`);
          assert.ok(any <= TRIALS * 0.05, `${any} of ${TRIALS}`);
        });
      }
    }
  }
});

// ─── The synthetic org: stored, not served ───────────────────────────────────

const withoutOurs = <T extends { findings: Finding[]; metrics: Metric[] }>(input: T): T => ({ ...input, findings: input.findings.filter((f) => f.ruleId !== PROBABILITY_RULE), metrics: input.metrics.filter((m) => m.id !== PROBABILITY_METRIC) });
const MADE_AT = "2026-11-24T09:00:00.000Z";

describe("the synthetic org keeps the comparison in its files and serves none of it yet (decision 706)", async () => {
  const automatic = await runFromRecording({ calibration: await automaticSyntheticCalibration(), reviewMode: "automatic", transport: new AutomaticReplay() });

  test("the trap is caught, and only the trap: Negotiation, set to 90%", () => {
    const loaded = loadRun(dir);
    const ours = loaded.input.findings.filter((f) => f.ruleId === PROBABILITY_RULE);
    assert.equal(ours.length, 1);
    console.log(`synthetic org: ${ours.map((f) => `${f.status}: ${f.title} / ${f.summary}`).join(" | ")}`);
    assert.equal(ours[0]!.status, "issue");
    assert.equal(
      ours[0]!.summary,
      "Negotiation is set to 90% in Salesforce. Of the 248 deals that reached it between October 2024 and September 2026, 11 are still open and 68.8% of the other 237 were won, so it is set higher than what happens. It is on the same side of the setting in each of the 4 ways we could split these deals (by team, rep, lead source and type). This compares each setting with what happened to the deals that reached the stage; it doesn't say why they differ.",
    );
    assert.deepEqual(ours[0]!.subjects.map((s) => s.label), ["Negotiation"]);
    const metric = loaded.input.metrics.find((m) => m.id === PROBABILITY_METRIC)! as Measured;
    assert.equal(metric.status, "measured");
    assert.equal(metric.value, 1);
  });

  test("the digest of a consultant-led, an automatic and a pulse run is byte for byte the one without it", () => {
    for (const runDir of [dir, automatic.dir]) {
      const loaded = loadRun(runDir);
      for (const input of [loaded.input, { ...loaded.input, run: { ...loaded.input.run, product: "pulse" as const } }]) {
        const [withIt, without] = [input, withoutOurs(input)].map((i) => JSON.stringify(checkedDigest({ input: i, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT })));
        assert.equal(withIt, without);
        assert.doesNotMatch(withIt!, /probability-vs-reality|stage_probability_vs_won/);
        assert.equal(JSON.stringify(buildPulseResult(buildReport(input))), JSON.stringify(buildPulseResult(buildReport(withoutOurs(input)))));
      }
    }
  });

  test("the raw notes document differs only in the stage read's evidence lines, which name the read as it ran", async () => {
    const paragraphs = async (input: ReturnType<typeof loadRun>["input"], pseudonyms: ReturnType<typeof loadRun>["pseudonyms"]): Promise<string[]> => {
      const xml = strFromU8(unzipSync(new Uint8Array(await renderRawNotesDocx(buildReport(input), pseudonyms)))["word/document.xml"]!);
      return [...xml.matchAll(/<w:p[ >].*?<\/w:p>/gs)].map((m) => [...m[0].matchAll(/<w:t(?: [^>]*)?>([^<]*)<\/w:t>/g)].map((t) => t[1]).join(""));
    };
    for (const [runDir, lines] of [[dir, 4], [automatic.dir, 3]] as const) {
      const loaded = loadRun(runDir);
      // As the run's files would have been before 706: our rows out, and the stage read as it was.
      const before = JSON.parse(JSON.stringify(withoutOurs(loaded.input)).replaceAll(ENGINE_STAGES_SOQL, STAGES_SOQL)) as typeof loaded.input;
      const [now, then] = [await paragraphs(loaded.input, loaded.pseudonyms), await paragraphs(before, loaded.pseudonyms)];
      assert.equal(now.length, then.length);
      const moved = now.flatMap((line, i) => (line === then[i] ? [] : [[then[i], line]]));
      assert.equal(moved.length, lines, runDir);
      for (const [was, is] of moved) assert.deepEqual([was, is], [`Query: ${STAGES_SOQL}`, `Query: ${ENGINE_STAGES_SOQL}`]);
    }
  });

  test("the automatic run's checked digest is not held", () => {
    const loaded = loadRun(automatic.dir);
    assert.equal(checkedDigest({ input: loaded.input, pseudonyms: loaded.pseudonyms, inventory: null, madeAt: MADE_AT }).held, null);
  });

  test("a consultant-led and an automatic run's documents leave it out, and are byte for byte what they'd be without it", () => {
    for (const runDir of [dir, automatic.dir]) {
      const loaded = loadRun(runDir);
      const [withIt, without] = [buildReport(loaded.input), buildReport(withoutOurs(loaded.input))];
      assert.ok(loaded.input.findings.some((f) => f.ruleId === PROBABILITY_RULE) && loaded.input.metrics.some((m) => m.id === PROBABILITY_METRIC));
      assert.ok(!withIt.teamFindings.some((f) => f.ruleId === PROBABILITY_RULE) && !withIt.metrics.some((m) => m.id === PROBABILITY_METRIC));
      assert.equal(executiveHtml(withIt, loaded.pseudonyms), executiveHtml(without, loaded.pseudonyms));
      assert.equal(fullReportHtml(withIt, loaded.pseudonyms), fullReportHtml(without, loaded.pseudonyms));
      assert.equal(onePagerHtml(withIt, loaded.pseudonyms), onePagerHtml(without, loaded.pseudonyms));
    }
  });
});
