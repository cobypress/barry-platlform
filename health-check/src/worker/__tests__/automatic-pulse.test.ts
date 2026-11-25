import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { Calibration, type Run } from "../../schemas/index.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { loadCalibration } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { ORG_ID } from "../../testing/synthetic-org.ts";
import { ORG_SETTINGS_ON_CONSULTANT_RUN } from "../run-job.ts";
import { PostgresWorkerStore, RUN_LINEAGE_SQL, type SqlPool } from "../postgres-store.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, runInput, syntheticConnection } from "./helpers.ts";

// An automatic customer's monthly pulse (DECISIONS.md 746): the console makes it
// as every pulse, with no review mode (a pulse is never automatic in its
// records) and a copy of its baseline's calibration, read from the org's
// settings. It runs as a consultant-led customer's pulse does. A run a person
// reviews still never starts from a calibration no person confirmed.

const ORG15 = ORG_ID.slice(0, 15);
const ON = { on: true, orgs: new Set([ORG15]) };
const BASELINE = "hc-2026-09-30-00000001";
const SECOND = "hc-2026-10-15-00000003";
const PULSE = "hc-2026-10-31-00000002";
const last = { final: true };

/** A customer's first check, released, and a pulse against it (and, `chain`, against a pulse before it). */
async function customer(o: { mode: "automatic" | "consultant"; calibration: Calibration; pulseCalibration?: Calibration; chain?: boolean; baselineOrg?: string; automatic?: { on: boolean; orgs: Set<string> } }) {
  const deps = baseDeps({ dataTransport: new Simulator(), automatic: o.automatic ?? ON });
  await deps.tokens.save(syntheticConnection());
  const handle = createJobHandler(deps);
  deps.store.addRun(runInput({ run: consoleRun({ id: BASELINE, reviewMode: o.mode, ...(o.baselineOrg ? { orgId: o.baselineOrg } : {}) }), calibration: o.calibration, ...(o.mode === "automatic" ? { intake: null, answers: [] } : {}) }));
  if (!o.baselineOrg) await handle({ kind: "run", runId: BASELINE }, last);
  let baseline = BASELINE;
  if (o.chain) {
    deps.dataTransport = new Simulator();
    deps.store.addRun(runInput({ run: consoleRun({ id: SECOND, product: "pulse", baselineRunId: BASELINE }), calibration: o.pulseCalibration ?? o.calibration }));
    const second = await handle({ kind: "run", runId: SECOND }, last);
    assert.equal(second.outcome, "done", second.detail);
    baseline = SECOND;
  }
  // As makePulse makes it: product pulse, no review mode, the baseline's calibration copied.
  deps.dataTransport = new Simulator();
  deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: baseline }), calibration: o.pulseCalibration ?? o.calibration }));
  const outcome = await handle({ kind: "run", runId: PULSE }, last);
  return { deps, outcome, stored: deps.store.runs.get(PULSE)! };
}

const automaticOne = async () => automaticSyntheticCalibration();
const personOne = () => loadCalibration();

describe("an automatic customer's monthly pulse (746)", () => {
  test("runs end to end and is saved as a consultant-led customer's pulse is: its result, its digest, its status", async () => {
    const auto = await customer({ mode: "automatic", calibration: await automaticOne() });
    const consultant = await customer({ mode: "consultant", calibration: personOne() });
    assert.equal(auto.deps.store.runs.get(BASELINE)!.run.status, "released", "the automatic check was released by code");
    assert.equal(auto.outcome.outcome, "done", auto.outcome.detail);
    assert.equal(consultant.outcome.outcome, "done", consultant.outcome.detail);
    assert.equal(auto.stored.run.status, consultant.stored.run.status);
    assert.ok(auto.deps.store.pulseResults.get(PULSE), "the pulse's result");
    assert.equal(auto.deps.store.pulseResults.get(PULSE)!.result.baselineRunId, BASELINE);
    const digest = auto.deps.store.digests.get(PULSE)?.digest;
    assert.ok(digest, "the pulse's digest");
    assert.equal(digest!.reviewed, false);
    assert.equal(digest!.product, "pulse");
    // Never released or held by code: a pulse is never an automatic run's release.
    assert.ok(!auto.deps.store.writes.some((w) => w.id === PULSE && /release|hold/i.test(w.method)));
    assert.ok(!auto.deps.lines.some((l) => l.includes(ORG_SETTINGS_ON_CONSULTANT_RUN)));
  });

  test("a pulse whose baseline is a pulse of an automatic check runs too", async () => {
    const r = await customer({ mode: "automatic", calibration: await automaticOne(), chain: true });
    assert.equal(r.outcome.outcome, "done", r.outcome.detail);
  });

  test("still refused: a pulse whose baseline is consultant-led with a calibration no person confirmed", async () => {
    const r = await customer({ mode: "consultant", calibration: personOne(), pulseCalibration: await automaticOne() });
    assert.equal(r.outcome.outcome, "failed");
    assert.equal(r.outcome.detail, ORG_SETTINGS_ON_CONSULTANT_RUN);
  });

  test("still refused: a pulse whose automatic baseline is another org's, or isn't there", async () => {
    const other = await customer({ mode: "automatic", calibration: await automaticOne(), baselineOrg: "00D000000000099AAA" });
    assert.equal(other.outcome.detail, ORG_SETTINGS_ON_CONSULTANT_RUN);
  });

  test("an automatic customer's pulse reads only where the switch lets its check read", async () => {
    const deps = baseDeps({ dataTransport: new Simulator(), automatic: { on: false, orgs: new Set() } });
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput({ run: consoleRun({ id: BASELINE, reviewMode: "automatic", status: "released" }), calibration: await automaticOne(), intake: null, answers: [] }));
    deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: BASELINE }), calibration: await automaticOne() }));
    const out = await createJobHandler(deps)({ kind: "run", runId: PULSE }, last);
    assert.equal(out.outcome, "failed");
    assert.notEqual(out.detail, ORG_SETTINGS_ON_CONSULTANT_RUN);
  });

  test("a store that can't say: tried again while the queue will, then the last delivery fails, heard, with nothing read", async () => {
    const sim = new Simulator();
    const deps = baseDeps({ dataTransport: sim, automatic: ON });
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput({ run: consoleRun({ id: BASELINE, reviewMode: "automatic", status: "released" }), calibration: await automaticOne(), intake: null, answers: [] }));
    deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: BASELINE }), calibration: await automaticOne() }));
    deps.store.failLineage = true;
    await assert.rejects(createJobHandler(deps)({ kind: "run", runId: PULSE }, { final: false }));
    const out = await createJobHandler(deps)({ kind: "run", runId: PULSE }, last);
    // Whether the customer already sees it can't be told: the last delivery fails, so its consultant hears (746), and nothing is read.
    assert.equal(out.outcome, "failed");
    assert.match(out.detail, /baselines or state couldn't be read, so it wasn't run/);
    assert.equal(deps.store.runs.get(PULSE)!.run.status, "failed");
    assert.equal(sim.requests.length, 0);
  });
});

describe("a run a person reviews, as before (692)", () => {
  test("a consultant-led check with a calibration read from the org's settings is still refused, with nothing read", async () => {
    const sim = new Simulator();
    const deps = baseDeps({ dataTransport: sim, automatic: ON });
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "consultant" } as Partial<Run>), calibration: await automaticOne() }));
    const out = await createJobHandler(deps)({ kind: "run", runId: consoleRun().id }, last);
    assert.equal(out.outcome, "failed");
    assert.equal(out.detail, ORG_SETTINGS_ON_CONSULTANT_RUN);
    assert.equal(sim.requests.length, 0);
  });
});

describe("the Postgres store's side of it", () => {
  test("one read-only statement; the mode through the row as JSON, anything but automatic read as consultant-led", async () => {
    const sent: Array<{ text: string; values: unknown[] }> = [];
    let rows: Array<Record<string, unknown>> = [{ orgId: ORG_ID, product: "barometer", baselineRunId: null, reviewMode: "automatic" }];
    const pool = { query: async (text: string, values: unknown[] = []) => (sent.push({ text, values }), { rows, rowCount: rows.length }) } as unknown as SqlPool;
    const store = new PostgresWorkerStore(pool);
    assert.deepEqual(await store.runLineage(BASELINE), { orgId: ORG_ID, product: "barometer", reviewMode: "automatic", baselineRunId: null });
    assert.deepEqual(sent[0], { text: RUN_LINEAGE_SQL, values: [BASELINE] });
    assert.match(RUN_LINEAGE_SQL, /^SELECT .* FROM "HealthCheckRun" WHERE "id" = \$1$/);
    rows = [{ orgId: ORG_ID, product: "pulse", baselineRunId: BASELINE, reviewMode: null }];
    assert.deepEqual(await store.runLineage(PULSE), { orgId: ORG_ID, product: "pulse", reviewMode: "consultant", baselineRunId: BASELINE });
    rows = [];
    assert.equal(await store.runLineage(PULSE), null);
  });
});
