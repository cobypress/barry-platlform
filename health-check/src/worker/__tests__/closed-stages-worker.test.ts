import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { ORG_SETTINGS_CALIBRATOR } from "../../connect/auto-calibration.ts";
import type { OrgStage } from "../../connect/org-stages.ts";
import { machineSet } from "../../connect/stage-outcomes.ts";
import { Calibration } from "../../schemas/index.ts";
import { AutomaticReplay, automaticSyntheticCalibration, stagesOf } from "../../testing/automatic.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { ANSWER_CALIBRATION_STAGES_SQL, CUSTOMER_STAGE_ANSWER_SQL, PostgresWorkerStore, SAVED_ORG_STAGES_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { setStagesBeforeRun, stageNamesStartLine } from "../stage-answer.ts";
import type { Run } from "../../schemas/index.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// A saved automatic calibration's closed stages set again when its run starts
// afresh (DECISIONS.md 756): the customer's newest answer and, with
// HC_STAGE_NAMES on, the name rule, through one guarded write of the stages
// only, never on a person's calibration, and reading no Salesforce.

const ORG15 = ORG_ID.slice(0, 15);
const ON = { on: true, orgs: new Set([ORG15]) };
const ALL = { "Closed Lost": "lost", "Closed Lost - No Budget": "lost", "Closed No Decision": "no_decision" };
const AT = "2026-10-31T09:00:00.000Z";
const first = await runFromRecording();
const STAGES: OrgStage[] = stagesOf(first.dir);
const closedOf = (c: Calibration | null | undefined) => (c?.stages ?? []).filter(machineSet);

type Start = { calibration?: Calibration; answer?: unknown; stageNames?: boolean; reviewMode?: Run["reviewMode"]; saved?: OrgStage[] | null; prepare?: (store: MemoryWorkerStore) => void };

async function setUp(s: Start = {}) {
  const transport = new AutomaticReplay();
  const deps = baseDeps({ dataTransport: transport, automatic: ON, stageNames: s.stageNames === true });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: s.reviewMode ?? "automatic" }), calibration: structuredClone(s.calibration ?? (await automaticSyntheticCalibration())), intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  if (s.saved !== null) await deps.store.saveOrgStages(RUN_ID, s.saved ?? STAGES, AT);
  if (s.answer !== undefined) deps.store.customerStageAnswers.set(ORG15, s.answer);
  s.prepare?.(deps.store);
  return { deps, transport, handle: createJobHandler(deps) };
}
const stageWrites = (store: MemoryWorkerStore) => store.writes.filter((w) => w.method === "answerCalibrationStages").length;
const salesforceCalls = (t: AutomaticReplay) => t.requests.length;

describe("at a fresh start of an already calibrated automatic run", () => {
  test("the customer's answer is saved into the calibration's stages, marked as theirs, nothing else changes, and the run is released", async () => {
    const before = await automaticSyntheticCalibration();
    const { deps, handle } = await setUp({ answer: JSON.stringify(ALL) });
    const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.equal(outcome.outcome, "done", outcome.detail);
    const after = deps.store.runs.get(RUN_ID)!.calibration!;
    assert.deepEqual(closedOf(after), [
      { name: "Closed Lost", category: "lost", from: "customer" },
      { name: "Closed No Decision", category: "no_decision", from: "customer" },
      { name: "Closed Lost - No Budget", category: "lost", from: "customer" },
    ]);
    assert.deepEqual({ ...after, stages: after.stages.filter((s) => !machineSet(s)) }, before, "nothing else in it changed");
    assert.equal(stageWrites(deps.store), 1);
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released", "released by code: nothing unmarked, unmatched or held");
    const digest = deps.store.digests.get(RUN_ID)!.digest;
    assert.ok(!JSON.stringify(digest).includes("can't tell lost from no decision"));
    assert.ok(deps.lines.some((l) => l === `Automatic run ${RUN_ID}'s closed stages were set again before it ran: 3 from the customer's answer, 0 from their names; nothing else in its calibration changed.`));
    for (const line of deps.lines) for (const s of ["Closed Lost", "Closed No Decision"]) assert.ok(!line.includes(s), line);
  });

  test("with the name rule on: read from their names, marked so, and the run released with the assumption said", async () => {
    const { deps, handle } = await setUp({ stageNames: true });
    const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.deepEqual(closedOf(deps.store.runs.get(RUN_ID)!.calibration).map((s) => s.from), ["name", "name", "name"]);
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    assert.ok(JSON.stringify(deps.store.digests.get(RUN_ID)!.digest).includes("We've assumed what some stages mean from their names, and you can tell us if any is wrong"));
  });

  test("no answer and the switch off: nothing is read beyond the answer, nothing written, and the run's calibration is exactly as saved", async () => {
    const before = await automaticSyntheticCalibration();
    let stagesRead = 0;
    const { deps, handle } = await setUp({
      prepare: (store) => {
        const read = store.savedOrgStages.bind(store);
        store.savedOrgStages = async (id) => (stagesRead++, read(id));
      },
    });
    await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, before);
    assert.equal(stageWrites(deps.store), 0);
    assert.equal(stagesRead, 0);
  });

  test("no Salesforce call: setting the stages sends nothing to the org", async () => {
    const { deps, transport } = await setUp({ answer: ALL, stageNames: true });
    const input = (await deps.store.loadRunJob(RUN_ID))!;
    await setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} });
    assert.equal(salesforceCalls(transport), 0);
  });

  test("a person's calibration, a consultant-led run, another org's calibration: never touched", async () => {
    const base = await automaticSyntheticCalibration();
    const cases: Array<[string, Start]> = [
      ["a person's (confirmed)", { calibration: Calibration.parse({ ...base, source: "confirmed", confirmedBy: "consultant@example.com" }) }],
      ["org_settings but confirmed by a person", { calibration: Calibration.parse({ ...base, confirmedBy: "consultant@example.com" }) }],
      ["a consultant-led run", { reviewMode: "consultant" }],
      ["another org's calibration", { calibration: Calibration.parse({ ...base, orgId: "00D000000000009AAA" }) }],
    ];
    for (const [why, s] of cases) {
      const { deps } = await setUp({ ...s, answer: ALL, stageNames: true });
      const input = (await deps.store.loadRunJob(RUN_ID))!;
      const out = await setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} });
      assert.deepEqual(out, input.calibration, why);
      assert.equal(stageWrites(deps.store), 0, why);
      assert.deepEqual(closedOf(deps.store.runs.get(RUN_ID)!.calibration), [], why);
    }
  });

  test("a person who saves meanwhile wins: the guarded write matches nothing and the run goes on with the person's", async () => {
    const { deps } = await setUp({ answer: ALL });
    const person = Calibration.parse({ ...(await automaticSyntheticCalibration()), source: "confirmed", confirmedBy: "consultant@example.com" });
    deps.store.beforeStageWrite = () => void (deps.store.runs.get(RUN_ID)!.calibration = structuredClone(person));
    const input = (await deps.store.loadRunJob(RUN_ID))!;
    const out = await setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} });
    assert.deepEqual(out, person);
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, person);
    assert.equal(stageWrites(deps.store), 0);
  });

  test("a later answer replaces an earlier one, and replaces what names read", async () => {
    const { deps } = await setUp({ answer: { "Closed Lost": "no_decision" }, stageNames: true });
    const run = async () => setStagesBeforeRun({ runId: RUN_ID, input: (await deps.store.loadRunJob(RUN_ID))!, deps, log: () => {} });
    await run();
    assert.deepEqual(closedOf(deps.store.runs.get(RUN_ID)!.calibration).map((s) => [s.name, s.category, s.from]), [
      ["Closed Lost", "no_decision", "customer"],
      ["Closed No Decision", "no_decision", "name"],
      ["Closed Lost - No Budget", "lost", "name"],
    ]);
    deps.store.customerStageAnswers.set(ORG15, { "Closed Lost": "lost", "Closed No Decision": "lost" });
    await run();
    assert.deepEqual(closedOf(deps.store.runs.get(RUN_ID)!.calibration).map((s) => [s.name, s.category, s.from]), [
      ["Closed Lost", "lost", "customer"],
      ["Closed No Decision", "lost", "customer"],
      ["Closed Lost - No Budget", "lost", "name"],
    ]);
    assert.equal(stageWrites(deps.store), 2);
  });

  test("two deliveries at once: one writes, the other goes on with the row as saved, and both run with the same calibration", async () => {
    const { deps } = await setUp({ answer: ALL });
    const input = (await deps.store.loadRunJob(RUN_ID))!;
    const [a, b] = await Promise.all([setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} }), setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} })]);
    assert.deepEqual(a, b);
    assert.deepEqual(a, deps.store.runs.get(RUN_ID)!.calibration);
    assert.equal(stageWrites(deps.store), 1);
  });

  test("a write that throws: the run goes on with the row as stored; unreadable then, a delivery that isn't the last stops before the run", async () => {
    const { deps } = await setUp({ answer: ALL });
    deps.store.failStageWrite = true;
    const input = (await deps.store.loadRunJob(RUN_ID))!;
    assert.deepEqual(await setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} }), input.calibration);
    deps.store.loadRunJob = async () => null;
    await assert.rejects(setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {} }), /queue delivers it again/);
    assert.deepEqual(await setStagesBeforeRun({ runId: RUN_ID, input, deps, log: () => {}, final: true }), input.calibration);
  });

  test("an answer that can't be read, or no saved stages: the calibration as it was, never a held or failed run", async () => {
    for (const s of [{ answer: "not json" }, { answer: ALL, saved: null }, { answer: ALL, prepare: (store: MemoryWorkerStore) => void (store.failStageAnswer = true) }] as Start[]) {
      const { deps, handle } = await setUp(s);
      const outcome = await handle({ kind: "run", runId: RUN_ID }, { final: false });
      assert.equal(outcome.outcome, "done", outcome.detail);
      assert.deepEqual(closedOf(deps.store.runs.get(RUN_ID)!.calibration), []);
      assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    }
  });

  test("the switch off and no saved stages: readings from names are still taken out, and nothing is added", async () => {
    const { deps } = await setUp({ stageNames: true });
    await setStagesBeforeRun({ runId: RUN_ID, input: (await deps.store.loadRunJob(RUN_ID))!, deps, log: () => {} });
    assert.equal(closedOf(deps.store.runs.get(RUN_ID)!.calibration).length, 3);
    deps.stageNames = false;
    deps.store.orgStages.delete(RUN_ID);
    deps.store.customerStageAnswers.set(ORG15, ALL);
    await setStagesBeforeRun({ runId: RUN_ID, input: (await deps.store.loadRunJob(RUN_ID))!, deps, log: () => {} });
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await automaticSyntheticCalibration());
  });

  test("the switch turned off after names were read: they are dropped at the next fresh start, back to undecided", async () => {
    const { deps } = await setUp({ stageNames: true });
    await setStagesBeforeRun({ runId: RUN_ID, input: (await deps.store.loadRunJob(RUN_ID))!, deps, log: () => {} });
    assert.equal(closedOf(deps.store.runs.get(RUN_ID)!.calibration).length, 3);
    deps.stageNames = false;
    await setStagesBeforeRun({ runId: RUN_ID, input: (await deps.store.loadRunJob(RUN_ID))!, deps, log: () => {} });
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, await automaticSyntheticCalibration());
  });
});

describe("the stores and the switch", () => {
  class Pool implements SqlPool {
    readonly seen: Array<{ text: string; values: unknown[] }> = [];
    readonly answer: (text: string) => SqlResult;
    constructor(answer: (text: string) => SqlResult) {
      this.answer = answer;
    }
    async query(text: string, values: unknown[] = []): Promise<SqlResult> {
      this.seen.push({ text, values });
      return this.answer(text);
    }
    async connect(): Promise<SqlClient & { release(): void }> {
      return { query: (t, v) => this.query(t, v), release: () => {} };
    }
  }

  test("Postgres: the answer read is one read-only statement for the active customer's newest closed_stages answer; no table is none", async () => {
    assert.match(CUSTOMER_STAGE_ANSWER_SQL, /^SELECT /);
    assert.doesNotMatch(CUSTOMER_STAGE_ANSWER_SQL, /\b(INSERT|UPDATE|DELETE)\b/);
    assert.match(CUSTOMER_STAGE_ANSWER_SQL, /c\."orgId15" = \$1 AND c\."status" = 'active' AND a\."question" = 'closed_stages' ORDER BY a\."answeredAt" DESC/);
    const pool = new Pool(() => ({ rows: [{ value: '{"A":"lost"}' }], rowCount: 1 }));
    assert.equal(await new PostgresWorkerStore(pool).customerStageAnswer(ORG_ID), '{"A":"lost"}');
    assert.deepEqual(pool.seen[0]!.values, [ORG15]);
    assert.equal(await new PostgresWorkerStore(new Pool(() => { throw Object.assign(new Error("x"), { code: "42P01" }); })).customerStageAnswer(ORG_ID), null);
    await assert.rejects(new PostgresWorkerStore(new Pool(() => { throw Object.assign(new Error("Closed Lost planted"), { code: "57014" }); })).customerStageAnswer(ORG_ID), (e: Error) => /SQLSTATE 57014/.test(e.message) && !e.message.includes("planted"));
  });

  test("Postgres: the saved stages read back in their shape, anything else is none", async () => {
    assert.equal(SAVED_ORG_STAGES_SQL, `SELECT "stages" FROM "HealthCheckOrgStages" WHERE "runId" = $1`);
    assert.deepEqual(await new PostgresWorkerStore(new Pool(() => ({ rows: [{ stages: STAGES }], rowCount: 1 }))).savedOrgStages(RUN_ID), STAGES);
    for (const stages of [[{ apiName: 1 }], "nope", null]) assert.equal(await new PostgresWorkerStore(new Pool(() => ({ rows: [{ stages }], rowCount: 1 }))).savedOrgStages(RUN_ID), null);
    assert.equal(await new PostgresWorkerStore(new Pool(() => ({ rows: [], rowCount: 0 }))).savedOrgStages(RUN_ID), null);
  });

  test("Postgres: the write is one guarded UPDATE of the stages only, on the org's settings' row with exactly the stages read", async () => {
    assert.match(ANSWER_CALIBRATION_STAGES_SQL, /^UPDATE "HealthCheckCalibration" SET "stages" = \$2::jsonb WHERE "runId" = \$1 AND "source" = 'org_settings' AND "confirmedBy" = \$3 AND "stages"::jsonb = \$4::jsonb$/);
    const was = (await automaticSyntheticCalibration()).stages;
    const now = [...was, { name: "Closed Lost", category: "lost" as const, from: "customer" as const }];
    const pool = new Pool(() => ({ rows: [], rowCount: 1 }));
    assert.equal(await new PostgresWorkerStore(pool).answerCalibrationStages(RUN_ID, was, now), "updated");
    assert.deepEqual(pool.seen[0]!.values, [RUN_ID, JSON.stringify(now), ORG_SETTINGS_CALIBRATOR, JSON.stringify(was)]);
    assert.equal(await new PostgresWorkerStore(new Pool(() => ({ rows: [], rowCount: 0 }))).answerCalibrationStages(RUN_ID, was, now), "unchanged");
  });

  test("Postgres: no answers table is said once, naming the stage answer, apart from the currency's line", async () => {
    const lines: string[] = [];
    const store = new PostgresWorkerStore(new Pool(() => { throw Object.assign(new Error("x"), { code: "42P01" }); }), { log: (l) => lines.push(l) });
    await store.customerStageAnswer(ORG_ID);
    await store.customerStageAnswer(ORG_ID);
    await store.customerCurrency(ORG_ID);
    assert.equal(lines.length, 2);
    assert.match(lines[0]!, /no customer's answer about its closed stages is read/);
    assert.match(lines[1]!, /no customer's currency is read/);
  });

  test("the switch is said at start, on or off", () => {
    assert.match(stageNamesStartLine(true), /^Closed stages read by name are on/);
    assert.match(stageNamesStartLine(false), /^Closed stages read by name are off: HC_STAGE_NAMES isn't on/);
  });
});
