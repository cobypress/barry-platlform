import { before, describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { ALL_MODULES, SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { readCheckpoint, writeCheckpoint } from "../../engine/checkpoint.ts";
import { runHealthCheck, type RunOptions } from "../../engine/run.ts";
import type { Finding, Metric } from "../../schemas/index.ts";
import { analyseExtracts, clone, loadExtracts, runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, SYNTHETIC_RECORDS, type OrgRecords, type SimulatorOptions } from "../../testing/simulator.ts";
import * as org from "../../testing/synthetic-org.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { FORECAST_MAX_ROWS, type ForecastData } from "../forecast.ts";
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

type Planted = { wonSince: string; wonNoStep: string; movedOn: string; reworked: string; noRowByTheEnd: string; made: [string, string] };

/**
 * The synthetic org a few days after its date: two deals open at it and won
 * since, one in Commit with a next step then and one with neither, one moved on
 * a stage, a won deal whose amount and close date were corrected, two deals
 * made since (one with a next step, one without) and, unless left out, a
 * migrated deal with no history that was changed since. Each deal changed is
 * one whose last history row matches it, as Salesforce's would, and the
 * simulator's forecast facts follow the deals won since.
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
  const committed = new Set(org.FORECAST_FACTS.filter((f) => f.ForecastCategoryName === "Commit").map((f) => f.OpportunityId));
  const wonSince = find((o) => committed.has(o.Id) && !o.IsClosed && o.NextStep !== null && settled(o));
  const wonNoStep = find((o) => !o.IsClosed && o.NextStep === null && settled(o));
  const movedOn = find((o) => o !== wonSince && !o.IsClosed && o.NextStep !== null && OPEN.indexOf(o.StageName) >= 0 && OPEN.indexOf(o.StageName) < OPEN.length - 1 && settled(o));
  const reworked = find((o) => o.IsWon && o.CloseDate >= "2026-01-01" && settled(o));
  const noRowByTheEnd = find((o) => o.IsClosed && rowsOf(o.Id).length === 0);
  const was = clone({ [wonSince.Id]: wonSince, [wonNoStep.Id]: wonNoStep, [movedOn.Id]: movedOn, [reworked.Id]: reworked, [noRowByTheEnd.Id]: noRowByTheEnd });

  let n = 0;
  const row = (o: org.SyntheticOpportunity, at: number): org.SyntheticHistory => ({ Id: org.sfId("008", 900_001 + n++), OpportunityId: o.Id, StageName: o.StageName, Amount: o.Amount, CloseDate: o.CloseDate, CreatedDate: stamp(at) });
  for (const [k, won] of [wonSince, wonNoStep].entries()) {
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
  const planted = { wonSince: wonSince.Id, wonNoStep: wonNoStep.Id, movedOn: movedOn.Id, reworked: reworked.Id, noRowByTheEnd: noRowByTheEnd.Id, made: [made[0]!.Id, made[1]!.Id] as [string, string] };
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
  let lateClosed: string[];

  before(async () => {
    const straight = await run({});
    atT = { dir: straight.dir, findings: straight.output!.findings, metrics: straight.output!.metrics };
    const read = await run({ records: later.records });
    late = checkpoint<OpportunityExtract>(read.dir, "opportunities");
    lateSteps = ok(checkpoint<Sourced<string[]>>(read.dir, "next-steps"));
    lateClosed = ok(checkpoint<ForecastData>(read.dir, "forecast").closedSinceTheRun);
  });

  const deal = (id: string) => ok(late.opportunities).find((o) => o.Id === id);

  test("the org holds each change after the run's date, so the test isn't empty", () => {
    const rows = later.records.pipeline.history;
    for (const id of [planted.wonSince, planted.wonNoStep, planted.movedOn, planted.reworked, planted.noRowByTheEnd]) {
      assert.ok(rows.some((h) => h.OpportunityId === id && after(h)), `${id} changed since`);
    }
    for (const id of [planted.wonSince, planted.wonNoStep, planted.movedOn, planted.reworked]) assert.ok(rows.some((h) => h.OpportunityId === id && !after(h)), `${id} has history by the run's date`);
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
    for (const id of [planted.wonSince, planted.wonNoStep]) {
      const then = was[id]!;
      assert.deepEqual(
        { StageName: deal(id)?.StageName, IsClosed: deal(id)?.IsClosed, IsWon: deal(id)?.IsWon, Amount: deal(id)?.Amount, CloseDate: deal(id)?.CloseDate },
        { StageName: then.StageName, IsClosed: false, IsWon: false, Amount: then.Amount, CloseDate: then.CloseDate },
      );
      assert.ok(OPEN.includes(then.StageName));
    }
  });

  test("and the next-steps read lists it where it has a next step, as it changed since the run's start, and not where it has none (DECISIONS.md 466)", () => {
    assert.ok(was[planted.wonSince]!.NextStep && !was[planted.wonNoStep]!.NextStep);
    assert.ok(lateSteps.includes(planted.wonSince), "closed now, and listed");
    assert.ok(!lateSteps.includes(planted.wonNoStep));
    // Open now, so listed as it was before.
    assert.ok(lateSteps.includes(planted.movedOn));
  });

  test("and the forecast step lists it among the deals closed now and changed since, where it's the only kind the deals read holds as open", () => {
    // Every deal closed now and changed since is listed: the won deal reworked
    // since and the one that can't be wound back too, which were closed at the
    // run's date, so the deals read holds them as closed and no figure leaves them out.
    assert.deepEqual([...lateClosed].sort(), [planted.wonSince, planted.wonNoStep, planted.reworked, planted.noRowByTheEnd].sort());
    assert.deepEqual(lateClosed.filter((id) => deal(id)?.IsClosed === false).sort(), [planted.wonSince, planted.wonNoStep].sort());
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

const NEXT_STEPS = "cur.missing-next-steps";
const COMMIT_SHARES = ["fct.commit-at-risk", "fct.early-stage-commits"];
const AGAINST_QUOTA = "forecast.commit_against_quota";
const QUOTA_HOLDERS = new Set(org.QUOTAS.filter((q) => q.ForecastingTypeId === org.sfId("0Db", 1)).map((q) => q.QuotaOwnerId));
const finding = (findings: Finding[], ruleId: string) => findings.filter((f) => f.ruleId === ruleId);
const metric = (metrics: Metric[], id: string) => metrics.filter((m) => m.id === id);
const listed = (dir: string, ruleId: string): string[] =>
  (JSON.parse(readFileSync(join(dir, "action-lists.json"), "utf8")) as { lists: Array<{ ruleId: string; rows: Array<{ id: string }> }> }).lists.filter((l) => l.ruleId === ruleId).flatMap((l) => l.rows.map((r) => r.id));
const factsOf = (dir: string): Map<string, string | null> => new Map(ok(checkpoint<ForecastData>(dir, "forecast").facts).map((f) => [f.OpportunityId, f.ForecastCategoryName]));

// The words a customer reads, word for word (DECISIONS.md 466).
const LEFT_OUT: Record<number, { exclusion: string; sentence: string; reason: string }> = {
  1: {
    exclusion: "1 deal closed since this run's date, whose forecast category then can't be read",
    sentence: "One deal open at this run's date and closed since is left out, as we can't read the forecast category it had then.",
    reason: "One deal open at this run's date has closed since, so its forecast category then can't be read.",
  },
  2: {
    exclusion: "2 deals closed since this run's date, whose forecast category then can't be read",
    sentence: "2 deals open at this run's date and closed since are left out, as we can't read the forecast category they had then.",
    reason: "2 deals open at this run's date have closed since, so their forecast category then can't be read.",
  },
};

/**
 * Each Commit share at the run's date with these deals taken out of Commit,
 * worked out from that run's own extracts by the rules as they were before the
 * list, so apart from it.
 */
function withoutThem(dir: string, ids: string[], modules = ALL_MODULES, calibration = loadCalibration()): Finding[] {
  const extracts = loadExtracts(dir);
  for (const f of ok((extracts.get("forecast") as ForecastData).facts)) if (ids.includes(f.OpportunityId)) f.ForecastCategoryName = "Pipeline";
  return analyseExtracts(extracts, { modules, answers: loadAnswers(), calibration }).findings;
}

/**
 * The Commit figures read late, with these deals, open at the run's date, closed
 * since: each share is the run's date's without them, and says so word for
 * word; the figure against quota has no value where a quota holder owns one.
 */
function leavesOut(late: { findings: Finding[]; metrics: Metric[] }, atT: { dir: string; findings: Finding[]; metrics: Metric[] }, ids: string[], owned: number, modules = ALL_MODULES, calibration = loadCalibration()): void {
  const words = LEFT_OUT[ids.length]!;
  const minus = withoutThem(atT.dir, ids, modules, calibration);
  for (const id of COMMIT_SHARES) {
    const [then, now, without] = [atT.findings, late.findings, minus].map((f) => (assert.equal(finding(f, id).length, 1, id), finding(f, id)[0]!));
    assert.deepEqual({ n: now.facts.n, hits: now.facts.hits, count: now.evidence[0]!.count }, { n: without.facts.n, hits: without.facts.hits, count: without.evidence[0]!.count }, id);
    assert.deepEqual(now.evidence[0]!.exclusions, [...then.evidence[0]!.exclusions, words.exclusion], id);
    assert.ok(now.summary.includes(` ${words.sentence}`), now.summary);
    assert.equal(now.summary.replace(` ${words.sentence}`, ""), without.summary, id);
    assert.deepEqual([now.status, now.title], [without.status, without.title], id);
    assert.ok(!then.summary.includes("closed since"));
  }
  const [then, now] = [metric(atT.metrics, AGAINST_QUOTA)[0]!, metric(late.metrics, AGAINST_QUOTA)[0]!];
  assert.equal(then.status, "measured");
  if (owned === 0) return assert.deepEqual(now, then);
  assert.equal(now.status, "not_measurable");
  assert.equal(now.reason, LEFT_OUT[owned]!.reason);
  assert.equal(now.evidence.length, 1);
  const evidence = now.evidence[0]!;
  assert.match(evidence.kind === "soql" ? evidence.query : "", /^SELECT Id FROM Opportunity WHERE IsClosed = true AND LastModifiedDate >= \S+ AND CreatedDate <= \S+$/);
  assert.equal(now.evidence[0]!.count, owned);
}

describe("read late, every figure is what it was at the run's date, but the Commit figures, which say what they leave out (DECISIONS.md 465 and 466)", () => {
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

  test("missing next steps is what it was: a deal won since with a next step then isn't counted as empty, and one with none then still is", () => {
    const [then, now] = [finding(atT.findings, NEXT_STEPS), finding(late.findings, NEXT_STEPS)].map((f) => (assert.equal(f.length, 1), f[0]!));
    const { wonSince, wonNoStep } = later.planted;
    assert.deepEqual(now, then);
    assert.deepEqual(listed(late.dir, NEXT_STEPS), listed(atT.dir, NEXT_STEPS));
    // Tested, and not flagged; open and empty at the run's date, so flagged, read late too.
    assert.ok(!listed(late.dir, NEXT_STEPS).includes(wonSince) && listed(late.dir, NEXT_STEPS).includes(wonNoStep));
    assert.ok(!now.summary.includes("closed since"));
  });

  test("the org's forecast facts follow it: the deal in Commit at the run's date and won since is Closed now, so without the list it would fall out of Commit", () => {
    const { wonSince, wonNoStep } = later.planted;
    assert.deepEqual([factsOf(atT.dir).get(wonSince), factsOf(late.dir).get(wonSince)], ["Commit", "Closed"]);
    assert.deepEqual([wonSince, wonNoStep].map((id) => QUOTA_HOLDERS.has(later.records.pipeline.opportunities.find((o) => o.Id === id)!.OwnerId)), [true, true]);
  });

  test("and every other finding and metric on this org is what it was, the Commit figures leaving out the two deals won since and saying so", () => {
    const others = (findings: Finding[]) => findings.filter((f) => !COMMIT_SHARES.includes(f.ruleId));
    assert.deepEqual(others(late.findings), others(atT.findings));
    assert.equal(late.findings.length, atT.findings.length);
    const otherMetrics = (metrics: Metric[]) => metrics.filter((m) => m.id !== AGAINST_QUOTA);
    assert.deepEqual(otherMetrics(late.metrics), otherMetrics(atT.metrics));
    assert.equal(late.metrics.length, atT.metrics.length);
    // Both owned by quota holders.
    leavesOut(late, atT, [later.planted.wonSince, later.planted.wonNoStep], 2);
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

    // Without the deals closed since, the facts as they stand would drop the one in Commit from the figure against quota.
    const unlisted = clone(extracts);
    (unlisted.get("forecast") as ForecastData).closedSinceTheRun = { ...(unlisted.get("forecast") as ForecastData).closedSinceTheRun, data: [] } as ForecastData["closedSinceTheRun"];
    const asTheyStand = analyseExtracts(unlisted, { modules: ALL_MODULES, answers: loadAnswers() });
    const [then, standing] = [metric(atT.metrics, AGAINST_QUOTA)[0]!, metric(asTheyStand.metrics, AGAINST_QUOTA)[0]!];
    assert.ok(then.status === "measured" && standing.status === "measured" && standing.value! < then.value!, `${then.status === "measured" && then.value} then, ${standing.status === "measured" && standing.value} as they stand`);
  });
});

describe("a run stopped part-way through the deals and resumed after the org changed", () => {
  const simulatorRun = (records: OrgRecords, outDir: string, used: number, overrides: Partial<RunOptions> = {}, simulator: SimulatorOptions = {}) =>
    runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: T, transport: new Simulator({ records, used, ...simulator }), pageSize: 50, log: () => {}, ...overrides });

  /** A run on the org at its date stopped by the budget just before the request `at` picks out of an uninterrupted run's, as run.test.ts stops one. */
  async function stoppedAt(at: (requests: Simulator["requests"]) => number): Promise<{ out: string; id: string; dir: string }> {
    const probe = new Simulator();
    await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: T, transport: probe, pageSize: 50 });
    const billable = probe.requests.slice(0, at(probe.requests)).filter((r) => !r.url.endsWith("/limits") && !r.url.endsWith("/services/data/")).length;
    const out = tempDir();
    const stopped = await simulatorRun(SYNTHETIC_RECORDS, out, 50_000 - billable);
    assert.equal(stopped.run.status, "stopped_budget");
    return { out, id: stopped.run.id, dir: stopped.dir };
  }
  const query = (pattern: RegExp) => (requests: Simulator["requests"]) => requests.findIndex((r) => r.url.includes("query") && pattern.test(decodeURIComponent(r.url)));

  /** A run stopped a few pages into the deals, with the ids on the pages it kept. */
  async function stoppedInTheDeals(): Promise<{ out: string; id: string; partial: string; kept: string[] }> {
    const stopped = await stoppedAt((requests) => query(/FROM Opportunity WHERE \(CreatedDate/)(requests) + 3);
    const partial = join(stopped.dir, "checkpoints", "opportunities.partial.jsonl");
    assert.ok(existsSync(partial), "pages so far were saved");
    const kept = readFileSync(partial, "utf8").trim().split("\n").map((line) => (JSON.parse(line) as { Id: string }).Id);
    return { out: stopped.out, id: stopped.id, partial, kept };
  }

  /** The step and figures an uninterrupted read at the run's date gives, with the same API use as a resumed session, which the limits read reports. */
  async function uninterrupted(): Promise<{ dir: string; step: OpportunityExtract; findings: Finding[]; metrics: Metric[] }> {
    const straight = await simulatorRun(SYNTHETIC_RECORDS, tempDir(), 1000, { answers: loadAnswers() });
    return { dir: straight.dir, step: checkpoint<OpportunityExtract>(straight.dir, "opportunities"), ...straight.output! };
  }

  /**
   * Every finding and metric is the uninterrupted run's, missing next steps and
   * its action list too, but the Commit figures, read late, which leave out the
   * two deals won since and say so (DECISIONS.md 466).
   */
  function sameFigures(resumed: { dir: string; output: { findings: Finding[]; metrics: Metric[] } | null }, straight: { dir: string; findings: Finding[]; metrics: Metric[] }, wonSince: string[]): void {
    const others = (findings: Finding[]) => findings.filter((f) => !COMMIT_SHARES.includes(f.ruleId));
    assert.deepEqual(others(resumed.output!.findings), others(straight.findings));
    const otherMetrics = (metrics: Metric[]) => metrics.filter((m) => m.id !== AGAINST_QUOTA);
    assert.deepEqual(otherMetrics(resumed.output!.metrics), otherMetrics(straight.metrics));
    assert.deepEqual(finding(resumed.output!.findings, NEXT_STEPS), finding(straight.findings, NEXT_STEPS));
    assert.deepEqual(listed(resumed.dir, NEXT_STEPS), listed(straight.dir, NEXT_STEPS));
    leavesOut(resumed.output!, straight, wonSince, 2, SALES_CLOUD_MODULES);
  }

  test("keeps the pages it read and reads the rest to the run's date, so the step is what an uninterrupted read gives", async () => {
    // Without the deal that can't be wound back: it's as read, by design.
    const later = laterOrg({ unwindable: false });
    const { out, id, kept } = await stoppedInTheDeals();
    // One changed deal was read before the stop, at the run's date; three after it, late.
    assert.ok(kept.includes(later.planted.reworked) && ![later.planted.wonSince, later.planted.wonNoStep, later.planted.movedOn].some((d) => kept.includes(d)), kept.join(", "));

    const resumed = await simulatorRun(later.records, out, 1000, { resume: id, answers: loadAnswers() });
    assert.equal(resumed.run.status, "in_review");
    const straight = await uninterrupted();
    const step = checkpoint<OpportunityExtract>(resumed.dir, "opportunities");
    assert.deepEqual(step.opportunities, straight.step.opportunities);
    assert.deepEqual(step.history, straight.step.history);
    assert.deepEqual(step.names, straight.step.names);
    assert.deepEqual(step.sinceTheRun, { woundBack: 4, asRead: 0, madeSince: 0 });
    sameFigures(resumed, straight, [later.planted.wonSince, later.planted.wonNoStep]);
  });

  // Next steps are read after the deals, and a run can stop between them and
  // resume a day later. A deal open at the run's date and closed in between was
  // flagged as having no next step though it had one (DECISIONS.md 466).
  describe("missing next steps on a run stopped after the deals and resumed after a deal open at the run's date was won (DECISIONS.md 466)", () => {
    test("stopped after the deals step and before the next steps: the same hits, deals and sentence as at the run's date", async () => {
      const later = laterOrg({ unwindable: false });
      const { wonSince, wonNoStep } = later.planted;
      const stopped = await stoppedAt(query(/NextStep != null/));
      const state = JSON.parse(readFileSync(join(stopped.dir, "run.json"), "utf8")) as { steps: Array<{ id: string; status: string }> };
      assert.deepEqual(["opportunities", "next-steps"].map((id) => state.steps.find((s) => s.id === id)?.status), ["done", "pending"]);
      // The deals were read at the run's date, with both deals open.
      const deals = ok(checkpoint<OpportunityExtract>(stopped.dir, "opportunities").opportunities);
      assert.deepEqual([wonSince, wonNoStep].map((id) => deals.find((o) => o.Id === id)?.IsClosed), [false, false]);

      const resumed = await simulatorRun(later.records, stopped.out, 1000, { resume: stopped.id, answers: loadAnswers() });
      assert.equal(resumed.run.status, "in_review");
      const straight = await uninterrupted();
      const [now, then] = [finding(resumed.output!.findings, NEXT_STEPS)[0]!, finding(straight.findings, NEXT_STEPS)[0]!];
      assert.deepEqual({ hits: now.facts.hits, n: now.facts.n, summary: now.summary }, { hits: then.facts.hits, n: then.facts.n, summary: then.summary });
      // Read after it was won, and listed, as it changed since the run's start.
      assert.ok(ok(checkpoint<Sourced<string[]>>(resumed.dir, "next-steps")).includes(wonSince));
      // The planted case for the side the decision admits: open and empty at the run's date, so still a hit.
      assert.ok(listed(resumed.dir, NEXT_STEPS).includes(wonNoStep) && !listed(resumed.dir, NEXT_STEPS).includes(wonSince));
      assert.deepEqual(now, then);
      assert.deepEqual(listed(resumed.dir, NEXT_STEPS), listed(straight.dir, NEXT_STEPS));
      // The API limits were read before the stop, at half the day's allowance, so the findings on them differ from the straight run's; the Commit figures read late leave the two out.
      leavesOut(resumed.output!, straight, [wonSince, wonNoStep], 2, SALES_CLOUD_MODULES);
    });

    test("stopped between the deals read and the history read, inside the one step: the same", async () => {
      const later = laterOrg({ unwindable: false });
      const { wonSince, wonNoStep } = later.planted;
      const stopped = await stoppedAt(query(/FROM OpportunityHistory/));
      // Every deal page was kept, read at the run's date, and no history.
      const checkpoints = join(stopped.dir, "checkpoints");
      const kept = readFileSync(join(checkpoints, "opportunities.partial.jsonl"), "utf8").trim().split("\n").map((line) => JSON.parse(line) as { Id: string; IsClosed: boolean });
      assert.equal(kept.length, org.pipeline.opportunities.length);
      assert.deepEqual([wonSince, wonNoStep].map((id) => kept.find((o) => o.Id === id)?.IsClosed), [false, false]);
      assert.ok(!existsSync(join(checkpoints, "opportunity-history.partial.jsonl")));

      const resumed = await simulatorRun(later.records, stopped.out, 1000, { resume: stopped.id, answers: loadAnswers() });
      assert.equal(resumed.run.status, "in_review");
      // Its history, read late, shows the win, so it's wound back, though it was open as read.
      assert.deepEqual(checkpoint<OpportunityExtract>(resumed.dir, "opportunities").sinceTheRun, { woundBack: 4, asRead: 0, madeSince: 0 });
      const straight = await uninterrupted();
      const [now, then] = [finding(resumed.output!.findings, NEXT_STEPS)[0]!, finding(straight.findings, NEXT_STEPS)[0]!];
      assert.deepEqual({ hits: now.facts.hits, n: now.facts.n, summary: now.summary }, { hits: then.facts.hits, n: then.facts.n, summary: then.summary });
      assert.ok(listed(resumed.dir, NEXT_STEPS).includes(wonNoStep) && !listed(resumed.dir, NEXT_STEPS).includes(wonSince));
      sameFigures(resumed, straight, [wonSince, wonNoStep]);
    });
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
    sameFigures(resumed, straight, [later.planted.wonSince, later.planted.wonNoStep]);
  });

  // The list of deals closed since is set against facts read afresh each time
  // the forecast step runs, so it's read whole each time too. Resumed from pages
  // kept before a stop, it missed a deal with a lower id that closed after the
  // stop, and the figure against quota was measured and low (DECISIONS.md 467).
  describe("the forecast step stopped after the facts and before the list, and resumed after a deal in Commit was won (DECISIONS.md 467)", () => {
    const OLD_PAGES = "closed-since-the-run.partial.jsonl";
    const LIST = /IsClosed = true AND LastModifiedDate >= /;

    /** A quota holder's deal in Commit won since, and a deal closed at the run's date, with a later id, edited since in a field no history row tracks. */
    function wonAndEdited() {
      const later = oneWonSince((o) => QUOTA_HOLDERS.has(o.OwnerId) && (o.Amount ?? 0) > 0);
      const edited = later.records.pipeline.opportunities.find((o) => o.IsClosed && o.Id > later.id && !after(o))!;
      assert.ok(SYNTHETIC_RECORDS.pipeline.opportunities.find((o) => o.Id === edited.Id)!.IsClosed, "closed at the run's date");
      return { ...later, edited: edited.Id, editedAt: { [edited.Id]: stamp(T.getTime() + 2 * HOUR) } };
    }

    async function stoppedBeforeTheList(): Promise<{ out: string; id: string; dir: string }> {
      const stopped = await stoppedAt((requests) => {
        const list = query(LIST)(requests);
        assert.ok(list > 0 && query(/FROM ForecastingFact/)(requests) < list, "the facts are read before the list");
        return list;
      });
      const state = JSON.parse(readFileSync(join(stopped.dir, "run.json"), "utf8")) as { steps: Array<{ id: string; status: string }> };
      assert.notEqual(state.steps.find((s) => s.id === "forecast")?.status, "done");
      // Nothing of the step is kept to resume from: no checkpoint, and no pages of the list.
      const checkpoints = join(stopped.dir, "checkpoints");
      assert.ok(!existsSync(join(checkpoints, "forecast.json")));
      assert.deepEqual(readdirSync(checkpoints).filter((f) => f.endsWith(".partial.jsonl")), []);
      return stopped;
    }

    /**
     * Resumed at the run's date on the later org: both deals are on the list,
     * and every figure is the uninterrupted run's but the Commit figures, which
     * leave the one won since out, one deal fewer in each share's Commit.
     */
    async function resumedAsAtTheRun(stopped: { out: string; id: string }, later: ReturnType<typeof wonAndEdited>): Promise<void> {
      const resumed = await simulatorRun(later.records, stopped.out, 1000, { resume: stopped.id, answers: loadAnswers() }, { editedAt: later.editedAt });
      assert.equal(resumed.run.status, "in_review");
      assert.deepEqual([...ok(checkpoint<ForecastData>(resumed.dir, "forecast").closedSinceTheRun)].sort(), [later.id, later.edited].sort());
      const straight = await uninterrupted();
      const quota = metric(resumed.output!.metrics, AGAINST_QUOTA)[0]!;
      assert.deepEqual([quota.status, quota.status === "not_measurable" && quota.reason], ["not_measurable", LEFT_OUT[1]!.reason]);
      for (const id of COMMIT_SHARES) {
        const [now, then] = [finding(resumed.output!.findings, id)[0]!, finding(straight.findings, id)[0]!];
        assert.ok(now.summary.includes(LEFT_OUT[1]!.sentence), id);
        assert.equal(then.facts.n! - now.facts.n!, 1, id);
      }
      // The API limits were read before the stop, at half the day's allowance, so only the Commit figures are set against the straight run's.
      leavesOut(resumed.output!, straight, [later.id], 1, SALES_CLOUD_MODULES);
    }

    test("reads the facts and the list again, so the deal won since is on the list and the figure against quota has none", async () => {
      const later = wonAndEdited();
      const stopped = await stoppedBeforeTheList();
      await resumedAsAtTheRun(stopped, later);
    });

    test("doesn't use pages kept under the list's old key, and clears them", async () => {
      const later = wonAndEdited();
      const stopped = await stoppedBeforeTheList();
      // As a stop on the keyset read left them: a page holding the deal edited
      // since, whose id is after the one won, so the old read went on from there.
      const old = join(stopped.dir, "checkpoints", OLD_PAGES);
      writeFileSync(old, `${JSON.stringify({ Id: later.edited })}\n`);
      await resumedAsAtTheRun(stopped, later);
      assert.ok(!existsSync(old));
    });
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
    const atTheRun = { findings: first.output!.findings, metrics: first.output!.metrics };
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
    const { sinceTheRun: _s, ...shape } = atT;
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
    assert.equal(ok(step.opportunities).find((o) => o.Id === later.planted.wonSince)?.IsClosed, false);
    // Every figure is the late read's, but the Commit figures: the forecast step
    // was read at the run's date, before anything closed, and is kept, so they
    // are the run's date's. The late read leaves out the deals won since.
    const others = (findings: Finding[]) => findings.filter((f) => !COMMIT_SHARES.includes(f.ruleId));
    const otherMetrics = (metrics: Metric[]) => metrics.filter((m) => m.id !== AGAINST_QUOTA);
    assert.deepEqual(others(resumed.output!.findings), others(fresh.output!.findings));
    assert.deepEqual(otherMetrics(resumed.output!.metrics), otherMetrics(fresh.output!.metrics));
    for (const id of COMMIT_SHARES) assert.deepEqual(finding(resumed.output!.findings, id), finding(atTheRun.findings, id), id);
    assert.deepEqual(metric(resumed.output!.metrics, AGAINST_QUOTA), metric(atTheRun.metrics, AGAINST_QUOTA));
    assert.equal(resumed.run.steps.find((s) => s.id === "forecast")?.version, 4);
  });
});

// The forecast facts are as they stand, so a deal in Commit at the run's date
// and won since has the category Closed when they're read late. The deals read
// winds it back to open, so it would fall out of won and Commit both
// (DECISIONS.md 466). Each org here has that one change and nothing else.
const CLOSED_SINCE = /^SELECT Id FROM Opportunity WHERE IsClosed = true AND LastModifiedDate >= /;

/** The synthetic org a day after its date, with one deal in Commit at it, whose last history row matches it, won since (or moved since into another closed stage). */
function oneWonSince(match: (o: org.SyntheticOpportunity) => boolean, stage = "Closed Won"): { records: OrgRecords; id: string; was: org.SyntheticOpportunity } {
  const pipeline = clone(org.pipeline);
  const committed = new Set(org.FORECAST_FACTS.filter((f) => f.ForecastCategoryName === "Commit").map((f) => f.OpportunityId));
  const last = (id: string) => pipeline.history.filter((h) => h.OpportunityId === id).sort(byTime).at(-1);
  const deal = pipeline.opportunities.find((o) => {
    const row = last(o.Id);
    return committed.has(o.Id) && !o.IsClosed && !!row && row.StageName === o.StageName && row.Amount === o.Amount && row.CloseDate === o.CloseDate && match(o);
  })!;
  const was = clone(deal);
  const at = T.getTime() + 24 * HOUR;
  Object.assign(deal, { StageName: stage, IsClosed: true, IsWon: stage === "Closed Won", CloseDate: stamp(at).slice(0, 10) });
  pipeline.history.push({ Id: org.sfId("008", 900_201), OpportunityId: deal.Id, StageName: deal.StageName, Amount: deal.Amount, CloseDate: deal.CloseDate, CreatedDate: stamp(at) });
  pipeline.history.sort((a, b) => a.Id.localeCompare(b.Id));
  return { records: { ...SYNTHETIC_RECORDS, pipeline }, id: deal.Id, was };
}

describe("the Commit figures read late, after a deal in Commit at the run's date was won (DECISIONS.md 466)", () => {
  let atT: { dir: string; findings: Finding[]; metrics: Metric[] };
  before(async () => {
    const straight = await run({});
    atT = { dir: straight.dir, ...straight.output! };
  });

  /** Every finding and metric is the run's date's, but the Commit figures. */
  function othersAsAtT(late: { findings: Finding[]; metrics: Metric[] }): void {
    const others = (findings: Finding[]) => findings.filter((f) => !COMMIT_SHARES.includes(f.ruleId));
    assert.deepEqual(others(late.findings), others(atT.findings));
    assert.deepEqual(late.metrics.filter((m) => m.id !== AGAINST_QUOTA), atT.metrics.filter((m) => m.id !== AGAINST_QUOTA));
    assert.equal(late.findings.length, atT.findings.length);
  }

  test("owned by a quota holder: the figure against quota has none, and both shares leave it out, word for word", async () => {
    // An early-stage deal with a next step, so the early-stage share loses a hit and missing next steps would lose a deal.
    const later = oneWonSince((o) => QUOTA_HOLDERS.has(o.OwnerId) && o.NextStep !== null && o.StageName === "Prospecting");
    const read = await run({ records: later.records });
    const late = { dir: read.dir, ...read.output! };
    // The org is truthful: its facts have the deal Closed now, and the step lists it as closed since.
    assert.deepEqual([factsOf(atT.dir).get(later.id), factsOf(late.dir).get(later.id)], ["Commit", "Closed"]);
    assert.deepEqual(ok(checkpoint<ForecastData>(late.dir, "forecast").closedSinceTheRun), [later.id]);

    const quota = metric(late.metrics, AGAINST_QUOTA)[0]!;
    assert.equal(quota.status, "not_measurable");
    assert.equal(quota.reason, "One deal open at this run's date has closed since, so its forecast category then can't be read.");
    leavesOut(late, atT, [later.id], 1);
    // The run's date's minus that deal, said directly: one deal fewer in Commit, and one hit fewer where it was early.
    for (const id of COMMIT_SHARES) assert.equal(finding(late.findings, id)[0]!.facts.n, finding(atT.findings, id)[0]!.facts.n! - 1, id);
    assert.equal(finding(late.findings, "fct.early-stage-commits")[0]!.facts.hits, finding(atT.findings, "fct.early-stage-commits")[0]!.facts.hits! - 1);
    othersAsAtT(late);
  });

  test("owned by someone with no quota: the figure against quota is the run's date's, and the shares still leave it out", async () => {
    const later = oneWonSince((o) => !QUOTA_HOLDERS.has(o.OwnerId));
    const read = await run({ records: later.records });
    const late = { dir: read.dir, ...read.output! };
    assert.deepEqual([factsOf(atT.dir).get(later.id), factsOf(late.dir).get(later.id)], ["Commit", "Closed"]);
    assert.deepEqual(metric(late.metrics, AGAINST_QUOTA), metric(atT.metrics, AGAINST_QUOTA));
    leavesOut(late, atT, [later.id], 0);
    othersAsAtT(late);
  });

  test("where the deals closed since can't be read, the three figures are not measurable with Salesforce's reason, and nothing else moves", async () => {
    const read = await run({ failQuery: (soql) => (CLOSED_SINCE.test(soql) ? { errorCode: "INSUFFICIENT_ACCESS", message: "Not today" } : null) });
    const list = checkpoint<ForecastData>(read.dir, "forecast").closedSinceTheRun;
    assert.ok(list.status === "unavailable" && list.reason.includes("Not today"));
    const reason = `Deals closed since this run's date couldn't be read: ${list.status === "unavailable" ? list.reason : ""}`;
    const query = (e: { kind: string; query?: string }) => (e.kind === "soql" ? e.query : undefined);
    for (const id of COMMIT_SHARES) {
      const f = finding(read.output!.findings, id)[0]!;
      assert.deepEqual([f.status, f.summary], ["not_measurable", reason], id);
      assert.ok(f.evidence.some((e) => CLOSED_SINCE.test(query(e) ?? "") && e.count === null), id);
    }
    const quota = metric(read.output!.metrics, AGAINST_QUOTA)[0]!;
    assert.deepEqual([quota.status, quota.status === "not_measurable" && quota.reason], ["not_measurable", reason]);
    assert.ok(CLOSED_SINCE.test(query(quota.evidence[0]!) ?? ""));
    othersAsAtT(read.output!);
  });

  test("where the list stops at its row limit, the three figures are not measurable and say so, and nothing else moves (DECISIONS.md 467)", async () => {
    // Salesforce's answer to the list cut at the forecast's row limit with more
    // to come: as many deals as the limit, beyond the org's own.
    const sim = new Simulator();
    const transport: Transport = {
      async send(request) {
        const res = await sim.send(request);
        if (!CLOSED_SINCE.test(new URL(request.url).searchParams.get("q") ?? "")) return res;
        const records = Array.from({ length: FORECAST_MAX_ROWS }, (_, k) => ({ Id: org.sfId("006", 5_000_000 + k) }));
        return { ...res, text: JSON.stringify({ totalSize: FORECAST_MAX_ROWS + 1, done: false, records, nextRecordsUrl: `/services/data/v${org.API_VERSION}/query/01g000000000001-${FORECAST_MAX_ROWS}` }) };
      },
    };
    const read = await run({}, { transport });
    const list = checkpoint<ForecastData>(read.dir, "forecast").closedSinceTheRun;
    assert.ok(list.status === "ok" && list.truncated && list.data.length === FORECAST_MAX_ROWS);
    // Read once, and not past the limit.
    assert.equal(sim.requests.filter((r) => CLOSED_SINCE.test(new URL(r.url).searchParams.get("q") ?? "")).length, 1);
    assert.ok(!sim.requests.some((r) => r.url.includes(`-${FORECAST_MAX_ROWS}`)));
    const reason = "The read of deals closed since this run's date stopped at its row limit after 100,000 rows, so the rest weren't read.";
    const query = (e: { kind: string; query?: string }) => (e.kind === "soql" ? e.query : undefined);
    for (const id of COMMIT_SHARES) {
      const f = finding(read.output!.findings, id)[0]!;
      assert.deepEqual([f.status, f.summary], ["not_measurable", reason], id);
      assert.ok(f.evidence.some((e) => CLOSED_SINCE.test(query(e) ?? "") && e.count === FORECAST_MAX_ROWS), id);
    }
    const quota = metric(read.output!.metrics, AGAINST_QUOTA)[0]!;
    assert.deepEqual([quota.status, quota.status === "not_measurable" && quota.reason], ["not_measurable", reason]);
    assert.ok(CLOSED_SINCE.test(query(quota.evidence[0]!) ?? "") && quota.evidence[0]!.count === FORECAST_MAX_ROWS);
    othersAsAtT(read.output!);
  });

  test("a run in flight with next steps done at version 1 and the forecast at version 3, both read late, reads both again at the run's date", async () => {
    const later = oneWonSince((o) => QUOTA_HOLDERS.has(o.OwnerId) && o.NextStep !== null && o.StageName === "Prospecting");
    const first = await run({});
    const fresh = await run({ records: later.records });
    // As a run in flight before this change left it, resumed after the deal was
    // won: each step read late, the old way. Next steps lists the deals open
    // then, so not the deal; the facts have it Closed, and there's no list.
    const path = join(first.dir, "run.json");
    const state = JSON.parse(readFileSync(path, "utf8")) as { status: string; steps: Array<{ id: string; status: string; version: number }> };
    state.status = "stopped_budget";
    for (const step of state.steps) {
      if (step.id === "next-steps") step.version = 1;
      if (step.id === "forecast") step.version = 3;
    }
    writeFileSync(path, JSON.stringify(state));
    const v1 = later.records.pipeline.opportunities.filter((o) => !o.IsClosed && o.NextStep !== null && !after(o)).map((o) => o.Id).sort();
    assert.ok(!v1.includes(later.id));
    const steps = checkpoint<Sourced<string[]>>(first.dir, "next-steps");
    writeCheckpoint(join(first.dir, "checkpoints", "next-steps.json"), { ...steps, data: v1, source: { kind: "soql", api: "rest", query: "SELECT Id FROM Opportunity WHERE (IsClosed = false) AND NextStep != null" } });
    const { closedSinceTheRun: _c, ...v3 } = checkpoint<ForecastData>(first.dir, "forecast");
    writeCheckpoint(join(first.dir, "checkpoints", "forecast.json"), { ...v3, facts: { ...v3.facts, data: ok(v3.facts).map((f) => (f.OpportunityId === later.id ? { ...f, ForecastCategoryName: "Closed" } : f)) } });

    const sim = new Simulator({ records: later.records });
    const resumed = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: dirname(first.dir), now: new Date(), transport: sim, resume: first.run.id, log: () => {} });
    const queries = sim.requests.map((r) => new URL(r.url).searchParams.get("q") ?? "");
    assert.ok(queries.some((q) => /NextStep != null/.test(q)) && queries.some((q) => CLOSED_SINCE.test(q)), "both were read again");
    assert.ok(queries.some((q) => /FROM ForecastingFact/.test(q)));
    // Every other step was reused as it was.
    assert.ok(!queries.some((q) => /FROM Opportunity WHERE \(CreatedDate|FROM OpportunityHistory/.test(q)), "the deals were kept");
    assert.deepEqual(["next-steps", "forecast"].map((id) => resumed.run.steps.find((s) => s.id === id)?.version), [2, 4]);
    assert.ok(ok(checkpoint<Sourced<string[]>>(first.dir, "next-steps")).includes(later.id));

    // Missing next steps is the run's date's; the Commit figures leave the deal out, as the late run's do.
    assert.deepEqual(finding(resumed.output!.findings, NEXT_STEPS), finding(fresh.output!.findings, NEXT_STEPS));
    const atTheRun = { dir: first.dir, findings: first.output!.findings, metrics: first.output!.metrics };
    assert.deepEqual(finding(resumed.output!.findings, NEXT_STEPS), finding(atTheRun.findings, NEXT_STEPS));
    const quota = metric(resumed.output!.metrics, AGAINST_QUOTA)[0]!;
    assert.equal(quota.status === "not_measurable" && quota.reason, "One deal open at this run's date has closed since, so its forecast category then can't be read.");
    for (const id of COMMIT_SHARES) assert.deepEqual(finding(resumed.output!.findings, id), finding(fresh.output!.findings, id), id);
    assert.deepEqual(metric(resumed.output!.metrics, AGAINST_QUOTA), metric(fresh.output!.metrics, AGAINST_QUOTA));
  });

  test("next steps read part-way at version 1 and stopped aren't resumed from, and those pages are cleared", async () => {
    const later = oneWonSince((o) => QUOTA_HOLDERS.has(o.OwnerId) && o.NextStep !== null && o.StageName === "Prospecting");
    const first = await run({});
    const path = join(first.dir, "run.json");
    const state = JSON.parse(readFileSync(path, "utf8")) as { status: string; steps: Array<{ id: string; status: string; version: number }> };
    state.status = "stopped_budget";
    for (const step of state.steps) if (step.id === "next-steps") Object.assign(step, { status: "pending", version: 1 });
    writeFileSync(path, JSON.stringify(state));
    rmSync(join(first.dir, "checkpoints", "next-steps.json"));
    // A version 1 read's pages, made after the deal was won, so without it.
    const v1 = later.records.pipeline.opportunities.filter((o) => !o.IsClosed && o.NextStep !== null && !after(o)).map((o) => o.Id).sort();
    const old = join(first.dir, "checkpoints", "next-steps.partial.jsonl");
    writeFileSync(old, `${v1.map((Id) => JSON.stringify({ Id })).join("\n")}\n`);

    const resumed = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), answers: loadAnswers(), modules: ALL_MODULES, outDir: dirname(first.dir), now: new Date(), transport: new Simulator({ records: later.records }), resume: first.run.id, log: () => {} });
    assert.ok(ok(checkpoint<Sourced<string[]>>(first.dir, "next-steps")).includes(later.id));
    assert.deepEqual(finding(resumed.output!.findings, NEXT_STEPS), finding(first.output!.findings, NEXT_STEPS));
    assert.ok(!existsSync(old));
  });
});

// The list goes by Salesforce's flag and the deals' open by the calibration's
// stages, and the two can disagree (pipe.calibration-contradicts-org). Here the
// calibration calls Closed No Decision open, which Salesforce flags closed.
describe("the deals closed since, where the calibration calls open a stage Salesforce flags closed (DECISIONS.md 467)", () => {
  const STAGE = "Closed No Decision";
  const calibration = loadCalibration();
  calibration.stages = calibration.stages.map((s) => (s.name === STAGE ? { ...s, category: "open" as const, order: 7 } : s));
  let atT: { dir: string; findings: Finding[]; metrics: Metric[] };
  before(async () => {
    const straight = await run({}, { calibration });
    atT = { dir: straight.dir, ...straight.output! };
  });

  test("the org and calibration disagree, and the figure against quota is measured at the run's date, so the tests aren't empty", () => {
    assert.ok(org.STAGES.find((s) => s.ApiName === STAGE)!.IsClosed);
    assert.ok(finding(atT.findings, "pipe.calibration-contradicts-org")[0]!.summary.includes(`${STAGE} is calibrated as open, but Salesforce flags it as closed`));
    assert.equal(metric(atT.metrics, AGAINST_QUOTA)[0]!.status, "measured");
  });

  test("a quota holder's deal in it at the run's date, edited since with no history row, is on the list and moves no figure", async () => {
    const deal = org.pipeline.opportunities.find((o) => o.StageName === STAGE && QUOTA_HOLDERS.has(o.OwnerId) && !after(o))!;
    assert.ok(!org.pipeline.history.some((h) => h.OpportunityId === deal.Id && after(h)));
    const read = await run({ editedAt: { [deal.Id]: stamp(T.getTime() + 2 * HOUR) } }, { calibration });
    // On the list, and closed by Salesforce's flag at the run's date as the deals read holds it, in a stage the calibration calls open.
    assert.deepEqual(ok(checkpoint<ForecastData>(read.dir, "forecast").closedSinceTheRun), [deal.Id]);
    assert.equal(ok(checkpoint<OpportunityExtract>(read.dir, "opportunities").opportunities).find((o) => o.Id === deal.Id)?.IsClosed, true);
    for (const id of COMMIT_SHARES) {
      assert.deepEqual(finding(read.output!.findings, id), finding(atT.findings, id), id);
      assert.ok(!finding(read.output!.findings, id)[0]!.summary.includes("closed since"), id);
    }
    assert.deepEqual(metric(read.output!.metrics, AGAINST_QUOTA), metric(atT.metrics, AGAINST_QUOTA));
    assert.deepEqual(read.output!.findings, atT.findings);
    assert.deepEqual(read.output!.metrics, atT.metrics);
  });

  test("a quota holder's deal in Commit, open by both at the run's date and moved into it since, is closed since: the figure against quota has none and the shares leave it out", async () => {
    const later = oneWonSince((o) => QUOTA_HOLDERS.has(o.OwnerId) && (o.Amount ?? 0) > 0, STAGE);
    assert.ok(calibration.stages.some((s) => s.name === later.was.StageName && s.category === "open") && !later.was.IsClosed);
    const read = await run({ records: later.records }, { calibration });
    const late = { dir: read.dir, ...read.output! };
    assert.deepEqual(ok(checkpoint<ForecastData>(read.dir, "forecast").closedSinceTheRun), [later.id]);
    // Wound back to its stage then, open by Salesforce's flag.
    assert.equal(ok(checkpoint<OpportunityExtract>(read.dir, "opportunities").opportunities).find((o) => o.Id === later.id)?.IsClosed, false);
    leavesOut(late, atT, [later.id], 1, ALL_MODULES, calibration);
    for (const id of COMMIT_SHARES) assert.equal(finding(atT.findings, id)[0]!.facts.n! - finding(late.findings, id)[0]!.facts.n!, 1, id);
    const others = (findings: Finding[]) => findings.filter((f) => !COMMIT_SHARES.includes(f.ruleId));
    assert.deepEqual(others(late.findings), others(atT.findings));
    assert.deepEqual(late.metrics.filter((m) => m.id !== AGAINST_QUOTA), atT.metrics.filter((m) => m.id !== AGAINST_QUOTA));
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
    // And nothing has closed since, so the forecast step's list is empty and no Commit figure leaves a deal out.
    assert.deepEqual(checkpoint<ForecastData>(dir, "forecast").closedSinceTheRun.status === "ok" && ok(checkpoint<ForecastData>(dir, "forecast").closedSinceTheRun), []);
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
    assert.deepEqual([out.woundBack, out.asRead, out.madeSince], [0, 0, 0]);
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

  test("is left out with every history row of its own, even one dated by the run's date", () => {
    // A deal loaded with its created date set (Salesforce's audit fields) can
    // carry history rows dated before it, which no later-row test would catch.
    const early = { ...madeRow, CreatedDate: stamp(T.getTime() - HOUR) };
    const out = asAtTheRun([kept, made], [keptRow, early], STAGES, T, { names });
    assert.deepEqual(out.history, [keptRow]);
    assert.equal(out.madeSince, 1);
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
  assert.equal(ok(step.opportunities).find((o) => o.Id === later.planted.wonSince)?.StageName, "Closed Won");
  // Nothing is known of what changed since, so neither count is given. The read's end left out the deals made since.
  assert.deepEqual(step.sinceTheRun, { madeSince: 0 });
});
