import { before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { readCheckpoint, writeCheckpoint } from "../../engine/checkpoint.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords, type SimulatorOptions } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import { asAtTheRun, OpportunityRow, type HistoryRow, type OpportunityExtract, type StageRow } from "../opportunities.ts";
import type { Sourced } from "../types.ts";

// The opportunities read stops at the run's own date (DECISIONS.md 465). A deal
// made since isn't read, and one changed since is wound back from its history,
// so a run read late, or resumed, counts the deals every pipeline figure counts
// as they stood at the run's date. The synthetic org holds nothing after its
// own date, so each test here builds the same org as it might be a few days on.

const T = new Date(org.AS_OF);
const HOUR = 3_600_000;
const stamp = (ms: number): string => new Date(ms).toISOString().replace("Z", "+0000");
const OPEN = ["Prospecting", "Discovery", "Proposal", "Negotiation"];
const byTime = (a: HistoryRow, b: HistoryRow): number => (`${a.CreatedDate}|${a.Id}` < `${b.CreatedDate}|${b.Id}` ? -1 : 1);
const after = (h: { CreatedDate: string }): boolean => Date.parse(h.CreatedDate) > T.getTime();

type Planted = { closedSince: string; closedNoStep: string; movedOn: string; reworked: string; noRowByTheEnd: string; made: [string, string] };

/**
 * The synthetic org a few days after its date: two deals open at it and won
 * since, one with a next step then and one without, one moved on a stage, a won deal whose amount and close date were
 * corrected, two deals made since (one with a next step, one without) and,
 * unless left out, a migrated deal with no history that was changed since.
 * Each deal changed is one whose last history row matches it, as Salesforce's
 * would.
 */
function laterOrg(options: { unwindable?: boolean } = {}): { records: OrgRecords; planted: Planted; was: Record<string, org.SyntheticOpportunity> } {
  const pipeline = clone(org.pipeline);
  const rowsOf = (id: string) => pipeline.history.filter((h) => h.OpportunityId === id).sort(byTime);
  const settled = (o: org.SyntheticOpportunity): boolean => {
    const last = rowsOf(o.Id).at(-1);
    return !!last && last.StageName === o.StageName && last.Amount === o.Amount && last.CloseDate === o.CloseDate;
  };
  const base = new Set(org.basePipeline.opportunities.map((o) => o.Id));
  const find = (match: (o: org.SyntheticOpportunity) => boolean) => pipeline.opportunities.find((o) => base.has(o.Id) && match(o))!;
  const closedSince = find((o) => !o.IsClosed && o.NextStep !== null && settled(o));
  const closedNoStep = find((o) => !o.IsClosed && o.NextStep === null && settled(o));
  const movedOn = find((o) => o !== closedSince && !o.IsClosed && o.NextStep !== null && OPEN.indexOf(o.StageName) >= 0 && OPEN.indexOf(o.StageName) < OPEN.length - 1 && settled(o));
  const reworked = find((o) => o.IsWon && o.CloseDate >= "2026-01-01" && settled(o));
  const noRowByTheEnd = find((o) => o.IsClosed && rowsOf(o.Id).length === 0);
  const was = clone({ [closedSince.Id]: closedSince, [closedNoStep.Id]: closedNoStep, [movedOn.Id]: movedOn, [reworked.Id]: reworked, [noRowByTheEnd.Id]: noRowByTheEnd });

  let n = 0;
  const row = (o: org.SyntheticOpportunity, at: number): org.SyntheticHistory => ({ Id: org.sfId("008", 900_001 + n++), OpportunityId: o.Id, StageName: o.StageName, Amount: o.Amount, CloseDate: o.CloseDate, CreatedDate: stamp(at) });
  for (const [k, won] of [closedSince, closedNoStep].entries()) {
    Object.assign(won, { StageName: "Closed Won", IsClosed: true, IsWon: true, CloseDate: stamp(T.getTime() + (24 + k) * HOUR).slice(0, 10) });
    pipeline.history.push(row(won, T.getTime() + (24 + k) * HOUR));
  }
  Object.assign(movedOn, { StageName: OPEN[OPEN.indexOf(movedOn.StageName) + 1]! });
  pipeline.history.push(row(movedOn, T.getTime() + 5 * HOUR));
  Object.assign(reworked, { Amount: reworked.Amount! * 2, CloseDate: "2026-11-02" });
  pipeline.history.push(row(reworked, T.getTime() + 6 * HOUR));
  if (options.unwindable !== false) {
    Object.assign(noRowByTheEnd, { Amount: noRowByTheEnd.Amount! + 2_000 });
    pipeline.history.push(row(noRowByTheEnd, T.getTime() + 4 * HOUR));
  }
  const made = [1, 2].map((k): org.SyntheticOpportunity => ({ ...clone(movedOn), Id: org.sfId("006", 900_000 + k), StageName: "Prospecting", CreatedDate: stamp(T.getTime() + (1 + k) * HOUR), NextStep: k === 1 ? "Call the buyer on Friday" : null }));
  for (const o of made) {
    pipeline.opportunities.push(o);
    pipeline.history.push(row(o, Date.parse(o.CreatedDate)));
  }
  pipeline.history.sort((a, b) => a.Id.localeCompare(b.Id));
  const planted = { closedSince: closedSince.Id, closedNoStep: closedNoStep.Id, movedOn: movedOn.Id, reworked: reworked.Id, noRowByTheEnd: noRowByTheEnd.Id, made: [made[0]!.Id, made[1]!.Id] as [string, string] };
  return { records: { ...SYNTHETIC_RECORDS, pipeline }, planted, was };
}

const run = (simulator: SimulatorOptions, overrides: Partial<RunOptions> = {}) =>
  runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: tempDir(), now: T, transport: new Simulator(simulator), log: () => {}, ...overrides });

const checkpoint = <T>(dir: string, id: string): T => readCheckpoint(join(dir, "checkpoints", `${id}.json`)) as T;
const ok = <T>(s: Sourced<T>): T => (s.status === "ok" ? s.data : assert.fail(`unavailable: ${s.reason}`));

describe("the opportunities read on an org changed since the run's date (DECISIONS.md 465)", () => {
  const later = laterOrg();
  const { planted, was } = later;
  let atT: { dir: string; findings: Finding[]; metrics: Metric[] };
  let late: OpportunityExtract;
  let lateSteps: string[];

  before(async () => {
    const straight = await run({});
    atT = { dir: straight.dir, findings: straight.output!.findings, metrics: straight.output!.metrics };
    const read = await run({ records: later.records });
    late = checkpoint<OpportunityExtract>(read.dir, "opportunities");
    lateSteps = ok(checkpoint<Sourced<string[]>>(read.dir, "next-steps"));
  });

  const deal = (id: string) => ok(late.opportunities).find((o) => o.Id === id);

  test("the org holds each change after the run's date, so the test isn't empty", () => {
    const rows = later.records.pipeline.history;
    for (const id of [planted.closedSince, planted.closedNoStep, planted.movedOn, planted.reworked, planted.noRowByTheEnd]) {
      assert.ok(rows.some((h) => h.OpportunityId === id && after(h)), `${id} changed since`);
    }
    for (const id of [planted.closedSince, planted.closedNoStep, planted.movedOn, planted.reworked]) assert.ok(rows.some((h) => h.OpportunityId === id && !after(h)), `${id} has history by the run's date`);
    assert.ok(!rows.some((h) => h.OpportunityId === planted.noRowByTheEnd && !after(h)));
    for (const id of planted.made) assert.ok(after(later.records.pipeline.opportunities.find((o) => o.Id === id)!));
  });

  test("a deal made since isn't read, nor its history, nor its next step", () => {
    for (const id of planted.made) {
      assert.equal(deal(id), undefined);
      assert.ok(!ok(late.history).some((h) => h.OpportunityId === id));
      assert.ok(!lateSteps.includes(id));
    }
    assert.ok(later.records.pipeline.opportunities.find((o) => o.Id === planted.made[0])!.NextStep, "the one with a next step would be in the next-steps read without the end");
  });

  test("a deal open at the run's date and won since reads as open, in its stage then", () => {
    for (const id of [planted.closedSince, planted.closedNoStep]) {
      const then = was[id]!;
      assert.deepEqual(
        { StageName: deal(id)?.StageName, IsClosed: deal(id)?.IsClosed, IsWon: deal(id)?.IsWon, Amount: deal(id)?.Amount, CloseDate: deal(id)?.CloseDate },
        { StageName: then.StageName, IsClosed: false, IsWon: false, Amount: then.Amount, CloseDate: then.CloseDate },
      );
      assert.ok(OPEN.includes(then.StageName));
    }
  });

  test("and is named as one the next-steps read can't have listed, with or without a next step then, as nothing else is", () => {
    assert.ok(was[planted.closedSince]!.NextStep && !was[planted.closedNoStep]!.NextStep);
    assert.deepEqual([...late.closedSince!].sort(), [planted.closedSince, planted.closedNoStep].sort());
    // Open now and as read, so the read lists it as it stands.
    assert.ok(!late.closedSince!.includes(planted.movedOn));
    // Closed at the run's date as well as now, so no figure tests its next step.
    assert.ok(!late.closedSince!.includes(planted.reworked));
    assert.ok(!lateSteps.includes(planted.closedSince), "the read lists deals open now");
  });

  test("a deal moved on a stage since reads in its stage then", () => {
    assert.equal(deal(planted.movedOn)?.StageName, was[planted.movedOn]!.StageName);
    assert.notEqual(later.records.pipeline.opportunities.find((o) => o.Id === planted.movedOn)!.StageName, was[planted.movedOn]!.StageName);
  });

  test("a won deal whose amount and close date changed since has them as they were", () => {
    const then = was[planted.reworked]!;
    const now = later.records.pipeline.opportunities.find((o) => o.Id === planted.reworked)!;
    assert.ok(now.Amount !== then.Amount && now.CloseDate !== then.CloseDate);
    assert.deepEqual(
      { Amount: deal(planted.reworked)?.Amount, CloseDate: deal(planted.reworked)?.CloseDate, StageName: deal(planted.reworked)?.StageName, IsClosed: deal(planted.reworked)?.IsClosed, IsWon: deal(planted.reworked)?.IsWon },
      { Amount: then.Amount, CloseDate: then.CloseDate, StageName: "Closed Won", IsClosed: true, IsWon: true },
    );
  });

  test("a deal changed since with no history row by the run's date is as read, with all its rows, and counted", () => {
    const now = later.records.pipeline.opportunities.find((o) => o.Id === planted.noRowByTheEnd)!;
    assert.equal(deal(planted.noRowByTheEnd)?.Amount, now.Amount);
    assert.notEqual(now.Amount, was[planted.noRowByTheEnd]!.Amount);
    assert.deepEqual(
      ok(late.history).filter((h) => h.OpportunityId === planted.noRowByTheEnd),
      later.records.pipeline.history.filter((h) => h.OpportunityId === planted.noRowByTheEnd),
    );
    assert.deepEqual(late.sinceTheRun, { woundBack: 4, asRead: 1, madeSince: 0 });
  });

  test("no history row kept is after the run's date, but those of the deal that couldn't be wound back", () => {
    assert.deepEqual(
      ok(late.history).filter(after).map((h) => h.OpportunityId),
      [planted.noRowByTheEnd],
    );
  });

  test("every other deal and history row is as it was read at the run's date", () => {
    const straight = checkpoint<OpportunityExtract>(atT.dir, "opportunities");
    const others = (rows: OpportunityRow[]) => rows.filter((o) => o.Id !== planted.noRowByTheEnd);
    assert.deepEqual(others(ok(late.opportunities)), others(ok(straight.opportunities)));
    const kept = (rows: HistoryRow[]) => rows.filter((h) => !after(h));
    assert.deepEqual(kept(ok(late.history)), kept(ok(straight.history)));
    assert.deepEqual(late.names, straight.names);
  });
});

// The one stated exception is missing next steps: a deal open at the run's
// date and closed since has a next step then that can't be read, so the rule
// leaves it out and says so, where at the run's date it was tested.
const NEXT_STEPS = "cur.missing-next-steps";
const finding = (findings: Finding[], ruleId: string) => findings.filter((f) => f.ruleId === ruleId);
const metric = (metrics: Metric[], id: string) => metrics.filter((m) => m.id === id);
const listed = (dir: string, ruleId: string): string[] =>
  (JSON.parse(readFileSync(join(dir, "action-lists.json"), "utf8")) as { lists: Array<{ ruleId: string; rows: Array<{ id: string }> }> }).lists.filter((l) => l.ruleId === ruleId).flatMap((l) => l.rows.map((r) => r.id));

describe("read late, every figure is what it was at the run's date, but missing next steps, which says what it leaves out (DECISIONS.md 465)", () => {
  // Without the deal that can't be wound back: it's as read, by design.
  const later = laterOrg({ unwindable: false });
  let atT: { dir: string; findings: Finding[]; metrics: Metric[] };
  let late: { dir: string; findings: Finding[]; metrics: Metric[] };

  before(async () => {
    const straight = await run({});
    atT = { dir: straight.dir, ...straight.output! };
    const read = await run({ records: later.records });
    late = { dir: read.dir, ...read.output! };
  });

  test("days in stage and stuck deals, a win rate and won value", () => {
    for (const id of ["cur.stuck-in-stage", "cur.stale-deals", "cur.close-date-pushes"]) {
      assert.ok(finding(atT.findings, id).length > 0, id);
      assert.deepEqual(finding(late.findings, id), finding(atT.findings, id), id);
    }
    for (const id of ["current.median_days_in_stage", "current.open_deals", "current.open_pipeline", "historic.win_rate", "historic.win_rate_by_value", "historic.quarterly_won_value", "historic.won_value_by_customer"]) {
      assert.ok(metric(atT.metrics, id).length > 0, id);
      assert.deepEqual(metric(late.metrics, id), metric(atT.metrics, id), id);
    }
  });

  test("the stated exception: missing next steps leaves out the deals closed since, as their next step then can't be read, and says so", () => {
    const [then, now] = [finding(atT.findings, NEXT_STEPS), finding(late.findings, NEXT_STEPS)].map((f) => (assert.equal(f.length, 1), f[0]!));
    const { closedSince, closedNoStep } = later.planted;
    // At the run's date: the deal with a next step was tested and not flagged, the one without was flagged.
    assert.ok(!listed(atT.dir, NEXT_STEPS).includes(closedSince) && listed(atT.dir, NEXT_STEPS).includes(closedNoStep));
    // Read late, neither is flagged or in the action list: the one with a next step isn't wrongly said to have none.
    assert.ok(!listed(late.dir, NEXT_STEPS).includes(closedSince) && !listed(late.dir, NEXT_STEPS).includes(closedNoStep));
    assert.deepEqual(listed(late.dir, NEXT_STEPS), listed(atT.dir, NEXT_STEPS).filter((id) => id !== closedNoStep));
    // Both are out of the deals tested, and one was a hit.
    assert.deepEqual({ n: now.facts.n, hits: now.facts.hits }, { n: then.facts.n! - 2, hits: then.facts.hits! - 1 });
    assert.equal(now.evidence[0]!.count, then.evidence[0]!.count! - 2);
    assert.deepEqual(now.evidence[0]!.exclusions, [...then.evidence[0]!.exclusions, "2 deals closed since this run's date, whose next step then can't be read"]);
    assert.ok(now.summary.includes(` ${now.facts.hits} of ${now.facts.n} `) && now.summary.includes("2 deals open at this run's date and closed since are left out, as we can't read the next step they had then."), now.summary);
    assert.ok(!then.summary.includes("closed since"));
    assert.deepEqual([now.status, now.title, now.recommendation, now.severity], [then.status, then.title, then.recommendation, then.severity]);
  });

  test("and every other finding and metric on this org is what it was", () => {
    const others = (findings: Finding[]) => findings.filter((f) => f.ruleId !== NEXT_STEPS);
    assert.deepEqual(others(late.findings), others(atT.findings));
    assert.equal(late.findings.length, atT.findings.length);
    assert.deepEqual(late.metrics, atT.metrics);
  });

  test("they would move, read late, without the wind-back or the end: the planted changes reach these figures", () => {
    // The same run's extracts, with the deals and history as Salesforce holds them now.
    const extracts = loadExtracts(late.dir);
    const measured = analyseExtracts(extracts, { modules: ALL_MODULES, answers: loadAnswers() });
    const now = clone(extracts);
    const step = now.get("opportunities") as OpportunityExtract;
    const current = new Map(later.records.pipeline.opportunities.map((o) => [o.Id, o]));
    const deals = ok(step.opportunities);
    for (const o of deals) Object.assign(o, (({ StageName, Amount, CloseDate, IsClosed, IsWon }) => ({ StageName, Amount, CloseDate, IsClosed, IsWon }))(current.get(o.Id)!));
    const read = new Set(deals.map((o) => o.Id));
    ok(step.history).push(...later.records.pipeline.history.filter((h) => read.has(h.OpportunityId) && after(h)));
    const notWoundBack = analyseExtracts(now, { modules: ALL_MODULES, answers: loadAnswers() });
    for (const id of ["cur.stuck-in-stage", "cur.stale-deals"]) assert.notDeepEqual(finding(notWoundBack.findings, id), finding(measured.findings, id), id);
    for (const id of ["current.median_days_in_stage", "historic.won_value_by_customer", "historic.quarterly_won_value"]) assert.notDeepEqual(metric(notWoundBack.metrics, id), metric(measured.metrics, id), id);

    const unended = clone(extracts);
    const made = later.records.pipeline.opportunities.filter(after);
    ok((unended.get("opportunities") as OpportunityExtract).opportunities).push(...made.map((o) => ({ ...ok(step.opportunities).find((d) => d.Id === later.planted.movedOn)!, Id: o.Id, StageName: o.StageName, CreatedDate: o.CreatedDate })));
    const notEnded = analyseExtracts(unended, { modules: ALL_MODULES, answers: loadAnswers() });
    assert.notDeepEqual(finding(notEnded.findings, "cur.missing-next-steps"), finding(measured.findings, "cur.missing-next-steps"));
  });
});

describe("a run stopped part-way through the deals and resumed after the org changed", () => {
  const simulatorRun = (records: OrgRecords, outDir: string, used: number, overrides: Partial<RunOptions> = {}, simulator: SimulatorOptions = {}) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: T, transport: new Simulator({ records, used, ...simulator }), pageSize: 50, log: () => {}, ...overrides });

  /** A run stopped a few pages into the deals, as run.test.ts stops one, with the ids on the pages it kept. */
  async function stoppedInTheDeals(): Promise<{ out: string; id: string; partial: string; kept: string[] }> {
    const probe = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: probe, pageSize: 50 });
    const first = probe.requests.findIndex((r) => decodeURIComponent(r.url).includes("FROM Opportunity WHERE (CreatedDate") && r.url.includes("query"));
    const billable = probe.requests.slice(0, first + 3).filter((r) => !r.url.endsWith("/limits") && !r.url.endsWith("/services/data/")).length;
    const out = tempDir();
    const stopped = await simulatorRun(SYNTHETIC_RECORDS, out, 50_000 - billable);
    assert.equal(stopped.run.status, "stopped_budget");
    const partial = join(stopped.dir, "checkpoints", "opportunities.partial.jsonl");
    assert.ok(existsSync(partial), "pages so far were saved");
    const kept = readFileSync(partial, "utf8").trim().split("\n").map((line) => (JSON.parse(line) as { Id: string }).Id);
    return { out, id: stopped.run.id, partial, kept };
  }

  /** The step and findings an uninterrupted read at the run's date gives, with the same API use as a resumed session, which the limits read reports. */
  async function uninterrupted(): Promise<{ dir: string; step: OpportunityExtract; findings: Finding[] }> {
    const straight = await simulatorRun(SYNTHETIC_RECORDS, tempDir(), 1000, { answers: loadAnswers() });
    return { dir: straight.dir, step: checkpoint<OpportunityExtract>(straight.dir, "opportunities"), findings: straight.output!.findings };
  }

  /** Every finding and metric is the uninterrupted run's, but missing next steps, the stated exception, which leaves out the two deals closed since. */
  function sameFigures(resumed: { dir: string; output: { findings: Finding[] } | null }, straight: { dir: string; findings: Finding[] }, closedSince: string[]): void {
    const others = (findings: Finding[]) => findings.filter((f) => f.ruleId !== NEXT_STEPS);
    assert.deepEqual(others(resumed.output!.findings), others(straight.findings));
    assert.equal(readFileSync(join(resumed.dir, "metrics.json"), "utf8"), readFileSync(join(straight.dir, "metrics.json"), "utf8"));
    const [now, then] = [finding(resumed.output!.findings, NEXT_STEPS)[0]!, finding(straight.findings, NEXT_STEPS)[0]!];
    assert.deepEqual([now.facts.n, now.evidence[0]!.count], [then.facts.n! - 2, then.evidence[0]!.count! - 2]);
    assert.deepEqual(listed(resumed.dir, NEXT_STEPS), listed(straight.dir, NEXT_STEPS).filter((id) => !closedSince.includes(id)));
  }

  test("keeps the pages it read and reads the rest to the run's date, so the step is what an uninterrupted read gives", async () => {
    // Without the deal that can't be wound back: it's as read, by design.
    const later = laterOrg({ unwindable: false });
    const { out, id, kept } = await stoppedInTheDeals();
    // One changed deal was read before the stop, at the run's date; three after it, late.
    assert.ok(kept.includes(later.planted.reworked) && ![later.planted.closedSince, later.planted.closedNoStep, later.planted.movedOn].some((d) => kept.includes(d)), kept.join(", "));

    const resumed = await simulatorRun(later.records, out, 1000, { resume: id, answers: loadAnswers() });
    assert.equal(resumed.run.status, "in_review");
    const straight = await uninterrupted();
    const step = checkpoint<OpportunityExtract>(resumed.dir, "opportunities");
    assert.deepEqual(step.opportunities, straight.step.opportunities);
    assert.deepEqual(step.history, straight.step.history);
    assert.deepEqual(step.names, straight.step.names);
    assert.deepEqual(step.sinceTheRun, { woundBack: 4, asRead: 0, madeSince: 0 });
    assert.deepEqual([...step.closedSince!].sort(), [later.planted.closedSince, later.planted.closedNoStep].sort());
    sameFigures(resumed, straight, [later.planted.closedSince, later.planted.closedNoStep]);
  });

  /**
   * The later org with one more deal made since, whose id sorts first, and a
   * run stopped in the deals whose kept pages hold it, as a read without the
   * end kept them before this change was deployed.
   */
  async function keptMadeSince() {
    const later = laterOrg({ unwindable: false });
    const made = { ...clone(later.records.pipeline.opportunities.find((o) => o.Id === later.planted.made[0])!), Id: org.sfId("006", 0) };
    const madeRow: org.SyntheticHistory = { Id: org.sfId("008", 900_100), OpportunityId: made.Id, StageName: made.StageName, Amount: made.Amount, CloseDate: made.CloseDate, CreatedDate: made.CreatedDate };
    assert.ok(after(made) && after(madeRow) && !SYNTHETIC_RECORDS.pipeline.opportunities.some((o) => o.Id === made.Id));
    const records: OrgRecords = { ...later.records, pipeline: { opportunities: [made, ...later.records.pipeline.opportunities], history: [...later.records.pipeline.history, madeRow].sort((a, b) => a.Id.localeCompare(b.Id)) } };

    const stopped = await stoppedInTheDeals();
    // The deal made since first, as Salesforce returns a row.
    const lines = readFileSync(stopped.partial, "utf8").trim().split("\n");
    const page = { ...(JSON.parse(lines[0]!) as Record<string, unknown>), Id: made.Id, Name: "A deal made since", OwnerId: made.OwnerId, StageName: made.StageName, Amount: made.Amount, CloseDate: made.CloseDate, CreatedDate: made.CreatedDate, IsClosed: false, IsWon: false };
    writeFileSync(stopped.partial, `${[JSON.stringify(page), ...lines].join("\n")}\n`);
    assert.ok(stopped.kept[0]! > made.Id);
    return { later, made, records, ...stopped };
  }

  test("leaves out a deal made since that a page kept from a read without the end holds, with its history and name, and counts it", async () => {
    const { later, made, records, out, id } = await keptMadeSince();
    const resumed = await simulatorRun(records, out, 1000, { resume: id, answers: loadAnswers() });
    assert.equal(resumed.run.status, "in_review");
    const step = checkpoint<OpportunityExtract>(resumed.dir, "opportunities");
    assert.ok(!ok(step.opportunities).some((o) => o.Id === made.Id));
    assert.ok(!ok(step.history).some((h) => h.OpportunityId === made.Id));
    assert.ok(step.names && !(made.Id in step.names));
    assert.deepEqual(step.sinceTheRun, { woundBack: 4, asRead: 0, madeSince: 1 });
    const straight = await uninterrupted();
    assert.deepEqual(step.opportunities, straight.step.opportunities);
    assert.deepEqual(step.history, straight.step.history);
    assert.deepEqual(step.names, straight.step.names);
    sameFigures(resumed, straight, [later.planted.closedSince, later.planted.closedNoStep]);
  });

  test("leaves it out where the history read then fails, though nothing can be wound back", async () => {
    const { made, records, out, id } = await keptMadeSince();
    const failQuery = (soql: string) => (/FROM OpportunityHistory/.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "Not today" } : null);
    await simulatorRun(records, out, 1000, { resume: id, answers: loadAnswers() }, { failQuery });
    const step = checkpoint<OpportunityExtract>(join(out, id), "opportunities");
    assert.equal(step.history.status, "unavailable");
    assert.ok(!ok(step.opportunities).some((o) => o.Id === made.Id));
    assert.ok(step.names && !(made.Id in step.names));
    assert.deepEqual(step.sinceTheRun, { madeSince: 1 });
  });
});

describe("a run in flight with a deals checkpoint from version 4", () => {
  test("reads the step again, so the deals are as they stood at the run's date, not as the earlier read found them late", async () => {
    const later = laterOrg({ unwindable: false });
    const first = await run({});
    const fresh = await run({ records: later.records });
    // As a run in flight before this change left it: the deals done at version
    // 4, read late without the end or the wind-back, so as Salesforce holds them
    // now, and the next steps still to read.
    const path = join(first.dir, "run.json");
    const state = JSON.parse(readFileSync(path, "utf8")) as { status: string; steps: Array<{ id: string; status: string; version: number }> };
    state.status = "stopped_budget";
    for (const step of state.steps) {
      if (step.id === "opportunities") step.version = 4;
      if (step.id === "next-steps") step.status = "pending";
    }
    writeFileSync(path, JSON.stringify(state));
    rmSync(join(first.dir, "checkpoints", "next-steps.json"));
    const atT = checkpoint<OpportunityExtract>(first.dir, "opportunities");
    const asNow = later.records.pipeline.opportunities.map((o) =>
      OpportunityRow.parse({ Id: o.Id, AccountId: o.AccountId, OwnerId: o.OwnerId, StageName: o.StageName, Amount: o.Amount, CloseDate: o.CloseDate, CreatedDate: o.CreatedDate, IsClosed: o.IsClosed, IsWon: o.IsWon, LastActivityDate: o.LastActivityDate, Type: o.Type, LeadSource: o.LeadSource, RecordType: o.RecordType.DeveloperName, CurrencyIsoCode: null, LossReason: o.Loss_Reason__c }),
    );
    const { sinceTheRun: _s, closedSince: _c, ...shape } = atT;
    writeCheckpoint(join(first.dir, "checkpoints", "opportunities.json"), {
      ...shape,
      opportunities: { ...atT.opportunities, data: asNow },
      history: { ...atT.history, data: later.records.pipeline.history },
      names: { ...atT.names, ...Object.fromEntries(later.planted.made.map((id) => [id, "A deal made since"])) },
    });

    const sim = new Simulator({ records: later.records });
    const resumed = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: dirname(first.dir), now: new Date(), transport: sim, resume: first.run.id, log: () => {} });
    assert.ok(sim.requests.some((r) => decodeURIComponent(r.url).includes("FROM Opportunity WHERE (CreatedDate")), "the deals were read again");
    assert.equal(resumed.run.steps.find((s) => s.id === "opportunities")?.version, 5);

    const step = checkpoint<OpportunityExtract>(first.dir, "opportunities");
    const lateRead = checkpoint<OpportunityExtract>(fresh.dir, "opportunities");
    assert.deepEqual(step, lateRead);
    for (const id of later.planted.made) assert.ok(!ok(step.opportunities).some((o) => o.Id === id) && !(id in step.names!));
    assert.equal(ok(step.opportunities).find((o) => o.Id === later.planted.closedSince)?.IsClosed, false);
    assert.deepEqual(resumed.output!.findings, fresh.output!.findings);
    assert.deepEqual(resumed.output!.metrics, fresh.output!.metrics);
  });
});

// Small, hand-made deals for the wind-back itself.
const STAGES: Sourced<StageRow[]> = { status: "ok", data: org.STAGES, source: { kind: "soql", api: "rest", query: "SELECT ApiName FROM OpportunityStage" }, truncated: false };
const dealRow = (Id: string, StageName: string, Amount: number | null, CloseDate: string, IsClosed: boolean, IsWon: boolean): OpportunityRow => ({
  Id, OwnerId: org.userId(1), StageName, Amount, CloseDate, CreatedDate: stamp(T.getTime() - 90 * 24 * HOUR), IsClosed, IsWon, LastActivityDate: null, Type: null, LeadSource: null, RecordType: null, CurrencyIsoCode: null, LossReason: null, AccountId: null,
});
const historyRow = (n: number, OpportunityId: string, StageName: string, Amount: number | null, CloseDate: string, at: number): HistoryRow => ({ Id: org.sfId("008", n), OpportunityId, StageName, Amount, CloseDate, CreatedDate: stamp(at) });
const oneDeal = {
  opportunities: { status: "ok", data: [dealRow(org.sfId("006", 1), "Proposal", 1000, "2026-12-01", false, false)], source: STAGES.source, truncated: false } as Sourced<OpportunityRow[]>,
  history: { status: "ok", data: [historyRow(1, org.sfId("006", 1), "Proposal", 1000, "2026-12-01", T.getTime() - HOUR)], source: STAGES.source, truncated: false } as Sourced<HistoryRow[]>,
};

describe("with nothing after the run's date", () => {
  test("the step reads every deal and history row exactly as the org holds them, as it did before the end", async () => {
    const { dir } = await run({});
    const step = checkpoint<OpportunityExtract>(dir, "opportunities");
    const byId = <R extends { Id: string }>(rows: R[]) => [...rows].sort((a, b) => a.Id.localeCompare(b.Id));
    const expected = byId(org.pipeline.opportunities).map((o) =>
      OpportunityRow.parse({ Id: o.Id, AccountId: o.AccountId, OwnerId: o.OwnerId, StageName: o.StageName, Amount: o.Amount, CloseDate: o.CloseDate, CreatedDate: o.CreatedDate, IsClosed: o.IsClosed, IsWon: o.IsWon, LastActivityDate: o.LastActivityDate, Type: o.Type, LeadSource: o.LeadSource, RecordType: o.RecordType.DeveloperName, CurrencyIsoCode: null, LossReason: o.Loss_Reason__c }),
    );
    assert.deepEqual(ok(step.opportunities), expected);
    assert.deepEqual(ok(step.history), byId(org.pipeline.history));
    assert.deepEqual(step.sinceTheRun, { woundBack: 0, asRead: 0, madeSince: 0 });
    assert.deepEqual(step.closedSince, []);
    // And as a run from the recording reads them, whose answers were recorded before the end and haven't moved.
    const recorded = checkpoint<OpportunityExtract>((await runFromRecording({ modules: ALL_MODULES })).dir, "opportunities");
    assert.deepEqual(recorded.opportunities.status === "ok" && recorded.opportunities.data, ok(step.opportunities));
    assert.deepEqual(recorded.history.status === "ok" && recorded.history.data, ok(step.history));
  });

  test("the wind-back hands back the very rows it was given", () => {
    const opportunities = ok(oneDeal.opportunities);
    const history = ok(oneDeal.history);
    const names = { [opportunities[0]!.Id]: "A deal" };
    const out = asAtTheRun(opportunities, history, STAGES, T, { names });
    assert.equal(out.opportunities, opportunities);
    assert.equal(out.history, history);
    assert.equal(out.names, names);
    assert.deepEqual([out.woundBack, out.asRead, out.madeSince, out.closedSince], [0, 0, 0, []]);
  });
});

describe("a deal made since on a page kept from a read without the end", () => {
  const kept = dealRow(org.sfId("006", 1), "Proposal", 1000, "2026-12-01", false, false);
  const made = { ...dealRow(org.sfId("006", 2), "Prospecting", 500, "2026-12-20", false, false), CreatedDate: stamp(T.getTime() + 2 * HOUR) };
  const names = { [kept.Id]: "A deal", [made.Id]: "A deal made since" };
  const keptRow = historyRow(1, kept.Id, "Proposal", 1000, "2026-12-01", T.getTime() - HOUR);
  const madeRow = historyRow(2, made.Id, "Prospecting", 500, "2026-12-20", T.getTime() + 2 * HOUR);

  test("is left out, with its history and its name, and counted, where nothing else changed since", () => {
    const out = asAtTheRun([kept, made], [keptRow, madeRow], STAGES, T, { names });
    assert.deepEqual(out.opportunities, [kept]);
    assert.deepEqual(out.history, [keptRow]);
    assert.deepEqual(out.names, { [kept.Id]: "A deal" });
    assert.deepEqual([out.woundBack, out.asRead, out.madeSince], [0, 0, 1]);
  });

  test("is left out where a deal beside it is wound back", () => {
    const moved = dealRow(org.sfId("006", 3), "Negotiation", 1000, "2026-12-01", false, false);
    const rows = [keptRow, madeRow, historyRow(3, moved.Id, "Proposal", 1000, "2026-12-01", T.getTime() - HOUR), historyRow(4, moved.Id, "Negotiation", 1000, "2026-12-01", T.getTime() + HOUR)];
    const out = asAtTheRun([kept, made, moved], rows, STAGES, T, { names });
    assert.deepEqual(out.opportunities.map((o) => [o.Id, o.StageName]), [[kept.Id, "Proposal"], [moved.Id, "Proposal"]]);
    assert.deepEqual(out.history.map((h) => h.Id), [keptRow.Id, rows[2]!.Id]);
    assert.deepEqual(out.names, { [kept.Id]: "A deal" });
    assert.deepEqual([out.woundBack, out.asRead, out.madeSince], [1, 0, 1]);
  });

  test("is left out where there's no history to read, as when the history read failed", () => {
    const out = asAtTheRun([kept, made], [], STAGES, T, { names });
    assert.deepEqual(out.opportunities, [kept]);
    assert.deepEqual(out.names, { [kept.Id]: "A deal" });
    assert.equal(out.madeSince, 1);
  });

  test("is made by the run's start rounded up to its second, as the query ends", () => {
    const at = new Date(T.getTime() + 400);
    const inTheSecond = { ...made, CreatedDate: new Date(T.getTime() + 900).toISOString().replace("Z", "+0000") };
    assert.equal(asAtTheRun([kept, inTheSecond], [], STAGES, at).madeSince, 0);
    const nextSecond = { ...made, CreatedDate: new Date(T.getTime() + 1001).toISOString().replace("Z", "+0000") };
    assert.equal(asAtTheRun([kept, nextSecond], [], STAGES, at).madeSince, 1);
  });

  test("but one whose CreatedDate doesn't parse is kept as read", () => {
    const unparsed = { ...made, CreatedDate: "not a date" };
    const out = asAtTheRun([kept, unparsed], [], STAGES, T, { names });
    assert.deepEqual(out.opportunities, [kept, unparsed]);
    assert.deepEqual(out.names, names);
    assert.equal(out.madeSince, 0);
  });
});

describe("winding a deal back", () => {
  const id = org.sfId("006", 7);
  const now = dealRow(id, "Closed Won", 3000, "2026-11-05", true, true);
  const rows = [
    historyRow(1, id, "Discovery", 1000, "2026-12-01", T.getTime() - 48 * HOUR),
    historyRow(2, id, "Proposal", 2000, "2026-12-15", T.getTime() - HOUR),
    historyRow(3, id, "Closed Won", 3000, "2026-11-05", T.getTime() + HOUR),
  ];

  test("takes the stage, amount and close date of the last row by the run's date, and IsClosed and IsWon from that stage", () => {
    const out = asAtTheRun([now], rows, STAGES, T);
    assert.deepEqual(out.opportunities, [{ ...now, StageName: "Proposal", Amount: 2000, CloseDate: "2026-12-15", IsClosed: false, IsWon: false }]);
    assert.deepEqual(out.history, rows.slice(0, 2));
    assert.deepEqual([out.woundBack, out.asRead], [1, 0]);
  });

  test("of two rows written in the same second, takes the later id, as the pipeline orders them", () => {
    const tied = [rows[0]!, { ...rows[1]!, Id: org.sfId("008", 5), CreatedDate: rows[0]!.CreatedDate }, rows[2]!];
    assert.equal(asAtTheRun([now], tied, STAGES, T).opportunities[0]!.StageName, "Proposal");
    const reversed = [rows[0]!, { ...rows[1]!, Id: org.sfId("008", 0), CreatedDate: rows[0]!.CreatedDate }, rows[2]!];
    assert.equal(asAtTheRun([now], reversed, STAGES, T).opportunities[0]!.StageName, "Discovery");
  });

  test("ends at the run's start rounded up to its second, as the query does", () => {
    const at = new Date(T.getTime() + 400);
    const inTheSecond = [rows[0]!, { ...rows[2]!, CreatedDate: new Date(T.getTime() + 900).toISOString().replace("Z", "+0000") }];
    assert.equal(asAtTheRun([now], inTheSecond, STAGES, at).woundBack, 0, "a row in the run's own second is by its end");
    const nextSecond = [rows[0]!, { ...rows[2]!, CreatedDate: new Date(T.getTime() + 1001).toISOString().replace("Z", "+0000") }];
    assert.equal(asAtTheRun([now], nextSecond, STAGES, at).opportunities[0]!.StageName, "Discovery");
  });

  test("leaves a deal as read, with all its rows, when the stages couldn't be read", () => {
    const unread: Sourced<StageRow[]> = { status: "unavailable", reason: "Not today", source: STAGES.source };
    const out = asAtTheRun([now], rows, unread, T);
    assert.deepEqual(out.opportunities, [now]);
    assert.deepEqual(out.history, rows);
    assert.deepEqual([out.woundBack, out.asRead], [0, 1]);
  });

  test("leaves a deal as read, with all its rows, when its stage then isn't in the stages read", () => {
    const renamed = rows.map((h, i) => (i === 1 ? { ...h, StageName: "Shortlisted" } : h));
    const out = asAtTheRun([now], renamed, STAGES, T);
    assert.deepEqual(out.opportunities, [now]);
    assert.deepEqual(out.history, renamed);
    assert.deepEqual([out.woundBack, out.asRead], [0, 1]);
  });

  test("names a deal wound back to open and closed as read, which the next-steps read can't list, and only that", () => {
    const open = new Set(["Prospecting", "Discovery", "Proposal", "Negotiation"]);
    assert.deepEqual(asAtTheRun([now], rows, STAGES, T, { openStages: open }).closedSince, [id]);
    // Closed as read, but in a stage the calibration calls open, so the read lists it as it stands.
    const calibratedOpen = new Set([...open, "Closed Won"]);
    assert.deepEqual(asAtTheRun([now], rows, STAGES, T, { openStages: calibratedOpen }).closedSince, []);
    // Open as read: moved on a stage since.
    const movedOn = { ...now, StageName: "Negotiation", IsClosed: false, IsWon: false };
    assert.deepEqual(asAtTheRun([movedOn], rows.map((h, i) => (i === 2 ? { ...h, StageName: "Negotiation" } : h)), STAGES, T, { openStages: open }).closedSince, []);
    // Closed at the run's date too: no figure tests its next step.
    const closedThen = rows.map((h, i) => (i === 1 ? { ...h, StageName: "Closed Lost" } : h));
    assert.deepEqual(asAtTheRun([now], closedThen, STAGES, T, { openStages: open }).closedSince, []);
    // Not wound back: kept as read, closed, so no figure tests it.
    const unread: Sourced<StageRow[]> = { status: "unavailable", reason: "Not today", source: STAGES.source };
    assert.deepEqual(asAtTheRun([now], rows, unread, T, { openStages: open }).closedSince, []);
  });

  test("leaves a deal as read when it has no row by the run's date, and a deal next to it is still wound back", () => {
    const other = org.sfId("006", 8);
    const out = asAtTheRun([now, dealRow(other, "Closed Lost", 500, "2026-11-04", true, false)], [...rows, historyRow(9, other, "Closed Lost", 500, "2026-11-04", T.getTime() + 2 * HOUR)], STAGES, T);
    assert.deepEqual(out.opportunities.map((o) => [o.StageName, o.IsClosed]), [["Proposal", false], ["Closed Lost", true]]);
    assert.deepEqual(out.history.map((h) => h.Id), [rows[0]!.Id, rows[1]!.Id, org.sfId("008", 9)]);
    assert.deepEqual([out.woundBack, out.asRead], [1, 1]);
  });
});

test("where the history can't be read, every deal is as read and only the deals made since are counted", async () => {
  const later = laterOrg();
  const { dir } = await run({ records: later.records, failQuery: (soql) => (/FROM OpportunityHistory/.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "Not today" } : null) });
  const step = checkpoint<OpportunityExtract>(dir, "opportunities");
  assert.equal(step.history.status, "unavailable");
  assert.equal(ok(step.opportunities).find((o) => o.Id === later.planted.closedSince)?.StageName, "Closed Won");
  // Nothing is known of what changed since, so neither count is given. The read's end left out the deals made since.
  assert.deepEqual(step.sinceTheRun, { madeSince: 0 });
  assert.equal(step.closedSince, undefined);
});
