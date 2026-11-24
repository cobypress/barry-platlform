import { describe, test } from "node:test";
import assert from "node:assert/strict";
import type { HistoryRow, OpportunityExtract, OpportunityRow, StageRow } from "../../extract/opportunities.ts";
import { ENGINE_STAGES_SOQL, PROBABILITY_FIELD, STAGES_SOQL } from "../../extract/opportunities.ts";
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
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { join } from "node:path";
import { readFileSync } from "node:fs";

// Each open stage's probability in Salesforce against how often the closed
// deals that reached it were won (DECISIONS.md 706), on deal histories built
// by hand, then on the synthetic org.

const { dir } = await runFromRecording({ answers: loadAnswers() });
const extracts = loadExtracts(dir);
const calibration: Calibration = { ...loadCalibration(), minSample: 5 };
const real: Calibration = { ...loadCalibration() };
const DAY = 86_400_000;
type Measured = Extract<Metric, { status: "measured" }>;

// ─── Hand-built deals ────────────────────────────────────────────────────────

type Outcome = "won" | "lost" | "no_decision";
/** `segment` is set as the deal's type, lead source and record type alike. `late`: made that many days before its history starts. */
type Spec = { path: string[]; outcome: Outcome; segment?: string; late?: number; owner?: number };
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
    const start = Date.parse("2025-04-01T09:00:00Z") + i * 3_600_000;
    const created = start - (s.late ?? 0) * DAY;
    const closed = start + 40 * DAY;
    const closeDate = new Date(closed).toISOString().slice(0, 10);
    opportunities.push({ Id: id, OwnerId: owners[(s.owner ?? 0) % owners.length]!, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(created), IsClosed: true, IsWon: s.outcome === "won", LastActivityDate: null, Type: s.segment ?? null, LeadSource: s.segment ?? null, RecordType: s.segment ?? null, CurrencyIsoCode: null, LossReason: null });
    s.path.forEach((stage, k) => rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: stage, Amount: null, CloseDate: closeDate, CreatedDate: stamp(start + k * 60_000) }));
    rows.push({ Id: `008${String(rows.length + 1).padStart(15, "0")}`, OpportunityId: id, StageName: CLOSED[s.outcome], Amount: null, CloseDate: closeDate, CreatedDate: stamp(closed) });
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
    assert.equal(reached({ id: "x", deal: {} as never, path: [C, D], won: true, days: 1 }, A), false);
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
    assert.ok(metric.assumptions.some((a) => a.startsWith("12 closed deals created before your stage history starts are left out")));
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
    assert.match(f!.summary, /^Negotiation is set to 90% in Salesforce\. Of the 100 closed deals that reached it, 66% were won, so it is set higher than what happens\./);
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
    assert.match(f!.summary, /Prospecting is set to 20% .* so it is set lower than what happens\. Negotiation is set to 80% .* so it is set higher than what happens\./);
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
    assert.match(f!.summary, /We couldn't check this within teams or record types/);
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

  test("the probabilities weren't read", () => check(analysed(usual, { stages: "no probabilities" }), /^Salesforce didn't give us the probability set on each stage, so there's nothing to hold how often deals are won against\.$/, "unreadable"));
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
    const { dir: runDir } = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: [historicPipelineModule], outDir: tempDir(), now: new Date(AS_OF), transport, connectedUser: CONNECTED_USER });
    const read = <T>(file: string) => JSON.parse(readFileSync(join(runDir, file), "utf8")) as T;
    return { dir: runDir, findings: read<Finding[]>("findings.json"), metrics: read<Metric[]>("metrics.json") };
  };
  const refused = (code: string) => (soql: string) => (/DefaultProbability/.test(soql) ? { errorCode: code, message: "No such column 'DefaultProbability' on entity 'OpportunityStage'." } : null);
  // Every other finding and metric, with the stage read's evidence naming the query, which on a refusal is honestly the plain one.
  const strip = <T extends { ruleId?: string; id?: string }>(rows: T[]) =>
    JSON.parse(JSON.stringify(rows.filter((x) => x.ruleId !== PROBABILITY_RULE && x.id !== PROBABILITY_METRIC)).replaceAll(ENGINE_STAGES_SOQL, STAGES_SOQL)) as T[];

  test("refused, the stages are read again without it; only this comparison goes without, and nothing else in the run moves", async () => {
    const [normal, without] = [await simulatedRun(), await simulatedRun(refused("INVALID_FIELD"))];
    const stages = readCheckpoint<Sourced<StageRow[]>>(join(without.dir, "checkpoints", "opportunity-stages.json"));
    assert.equal(stages.status, "ok");
    assert.ok(stages.status === "ok" && stages.missingFields?.includes(PROBABILITY_FIELD) && stages.data.every((r) => r.DefaultProbability === null));
    assert.equal(stages.status === "ok" && stages.source.query, STAGES_SOQL);
    const ours = without.findings.find((f) => f.ruleId === PROBABILITY_RULE)!;
    assert.equal(ours.status, "not_measurable");
    assert.match(ours.notMeasurableReason!, /^Salesforce didn't give us the probability/);
    assert.equal(normal.findings.find((f) => f.ruleId === PROBABILITY_RULE)!.status, "issue");
    assert.ok(JSON.stringify(normal.findings).includes(ENGINE_STAGES_SOQL));
    assert.deepEqual(strip(without.findings), strip(normal.findings));
    assert.deepEqual(strip(without.metrics), strip(normal.metrics));
  });

  test("any refusal is read again without it; refused both ways, the stages are unread as before", async () => {
    const { findings } = await simulatedRun(refused("INSUFFICIENT_ACCESS"));
    assert.match(findings.find((f) => f.ruleId === PROBABILITY_RULE)!.notMeasurableReason!, /^Salesforce didn't give us the probability/);
    const both = await simulatedRun((soql) => (/FROM OpportunityStage/.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "no" } : null));
    const stages = readCheckpoint<Sourced<StageRow[]>>(join(both.dir, "checkpoints", "opportunity-stages.json"));
    assert.equal(stages.status, "unavailable");
    assert.equal(stages.source.query, STAGES_SOQL);
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
    assert.match(ours[0]!.summary, /^Negotiation is set to 90% in Salesforce\. Of the 238 closed deals that reached it, 68\.9% were won, so it is set higher than what happens\./);
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
