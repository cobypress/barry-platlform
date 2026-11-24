import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createJobHandler } from "../worker.ts";
import { AGAIN_FILE, BUDGET_STOPPED, BUDGET_STOPPED_STEP } from "../run-job.ts";
import { HOLD_REASONS } from "../release.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { AS_OF, ORG_ID } from "../../testing/synthetic-org.ts";
import { Metric } from "../../schemas/index.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";

// An automatic run whose lost reads look like Salesforce being down is held
// `reads_lost` for a person, with its results saved and checked as any held
// run's, every time it happens (DECISIONS.md 715). Each test drives the real
// worker handler over the synthetic org, with the console's side played by
// hand as its person store does it (Run again clears every open flag, the
// hold, and sets the run back to extracting).

const ON = { on: true, orgs: new Set([ORG_ID.slice(0, 15)]) };
const SERVER_FAULT: TransportResponse = {
  status: 500,
  header: () => null,
  text: `<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>soapenv:Server</faultcode><faultstring>An unexpected error occurred. ErrorId: 1</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`,
};
const isSoap = (r: TransportRequest): boolean => Boolean(r.headers?.SOAPAction);

/** A module whose one rule says, in words put together at run time, that it measured nothing: a digest text no marks can vouch for. */
const UNMARKED: DomainModule = {
  id: "marks_probe",
  cloud: "sales",
  label: "Marks probe",
  extractors: [],
  rules: [{ id: "cfg.marks-probe", version: 1, domain: "configuration", title: "A probe rule", evaluate: (ctx) => ({ outcome: "pass", summary: `${["Quokka", "Wombat"].join(" ")} looked fine.`, evidence: [ctx.ev({ kind: "metadata", path: "flows/*.flow" })], segmentExemptReason: "Probe." }) }],
  metrics: [],
};

/** A module whose one metric's id the digest can't hold, so no digest can be made. */
const NO_DIGEST: DomainModule = {
  id: "digest_probe",
  cloud: "sales",
  label: "Digest probe",
  extractors: [],
  rules: [],
  metrics: [
    () => [
      Metric.parse({
        status: "measured", id: "probe metric with spaces", domain: "configuration", label: "A probe count", definition: "A count of nothing.", unit: "count",
        evidence: [{ kind: "engine", path: "probe", count: null, dateRange: null, exclusions: [], capturedAt: AS_OF }],
        exclusions: [], assumptions: [], value: 0, n: 0, dateRange: null, segments: [], segmentExemptReason: "One figure.", series: [], stats: {},
      }),
    ],
  ],
};

type Options = { mode?: "automatic" | "consultant"; probe?: DomainModule };

async function world(options: Options = {}) {
  const mode = options.mode ?? "automatic";
  const state = { down: true };
  const inner: Transport = mode === "automatic" ? new AutomaticReplay() : new ReplayTransport(loadRecording());
  const dataTransport: Transport = { send: async (r) => (state.down && isSoap(r) ? SERVER_FAULT : inner.send(r)) };
  // The clock stands still: the replay's queries are recorded at the synthetic org's own time.
  const clock = Date.parse(AS_OF);
  const deps = baseDeps({ dataTransport, now: () => new Date(clock), ...(mode === "automatic" ? { automatic: ON } : {}), ...(options.probe ? { modules: [...ALL_MODULES, options.probe] } : {}) });
  await deps.tokens.save(syntheticConnection());
  const run = consoleRun(mode === "automatic" ? { reviewMode: "automatic" } : {});
  const withProbe = options.probe ? { ...run, modules: [...run.modules, options.probe.id] } : run;
  if (mode === "automatic") deps.store.addRun(runInput({ run: withProbe, calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
  else deps.store.addRun(runInput({ run: withProbe }));
  const handle = createJobHandler(deps);
  const stored = () => deps.store.runs.get(RUN_ID)!;
  const flag = (kind: string) => deps.store.flags.get(`${RUN_ID}|${kind}`);
  const open = () => [...deps.store.flags.values()].filter((f) => f.runId === RUN_ID && f.clearedAt === null).map((f) => f.kind).sort();
  return {
    deps,
    state,
    stored,
    flag,
    open,
    /** The last delivery of the run's own job, or of a "Run again" pressed at `again`. */
    deliver: (again?: number) => handle({ kind: "run", runId: RUN_ID }, { final: true, ...(again ? { jobId: `run-${RUN_ID}-again-${again}` } : {}) }),
    /** The console's Run again (its runHeldRunAgain): every open flag cleared, the hold gone, back to extracting. */
    runAgain: () => {
      const at = new Date(clock).toISOString();
      for (const kind of open()) deps.store.postAndClearFlag(RUN_ID, kind, "staff@example.com", at);
      deps.store.held.delete(RUN_ID);
      stored().run.status = "extracting";
      return clock;
    },
  };
}

describe("lost reads that look like an outage hold an automatic run (DECISIONS.md 715)", () => {
  test("the Metadata API down on the last delivery: held reads_lost, results and digest saved, not released, a flag with all four counts", async () => {
    const w = await world();
    const outcome = await w.deliver();
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is held: reads_lost` });
    assert.equal(w.deps.store.held.get(RUN_ID), "reads_lost");
    assert.notEqual(w.stored().run.status, "released");
    assert.notEqual(w.stored().run.status, "failed");
    assert.equal(w.deps.store.releases.has(RUN_ID), false);
    assert.ok(w.stored().output, "its results are saved");
    const digest = w.deps.store.getRunDigest(RUN_ID);
    assert.ok(digest, "its digest is saved");
    const f = w.flag("reads_lost")!;
    assert.deepEqual(Object.keys(f.counts).sort(), ["lost", "notMeasurable", "unmarked", "unmatched"]);
    assert.ok(f.counts.lost! > 0 && f.counts.notMeasurable! > 0, JSON.stringify(f.counts));
    assert.deepEqual([f.counts.unmatched, f.counts.unmarked], [0, 0]);
    assert.ok(Date.parse(f.raisedAt) >= Date.parse(digest.madeAt), `${f.raisedAt} before ${digest.madeAt}`);
    assert.ok(!w.open().includes("run_failed"));
    // Nothing is written that a Run again would read as a once-only mark.
    assert.ok(!w.stored().run.steps.some((s) => s.id === "worker" || s.id === "reads-lost"));
  });

  test("lost reads and a digest that isn't safe: held digest_held, the reads_lost flag open beside it with the digest's counts", async () => {
    const w = await world({ probe: UNMARKED });
    const outcome = await w.deliver();
    assert.match(outcome.detail, /is held: digest_held$/, outcome.detail);
    assert.equal(w.deps.store.held.get(RUN_ID), "digest_held");
    assert.ok(w.open().includes("digest_held") && w.open().includes("reads_lost"), w.open().join());
    const held = w.flag("digest_held")!.counts;
    const lost = w.flag("reads_lost")!.counts;
    assert.ok(held.unmarked! > 0);
    assert.deepEqual([lost.unmatched, lost.unmarked], [held.unmatched, held.unmarked]);
    assert.equal(w.deps.store.releases.has(RUN_ID), false);
  });

  test("lost reads and no digest at all: no_digest is the hold, and the reads_lost flag carries no digest counts", async () => {
    const w = await world({ probe: NO_DIGEST });
    const outcome = await w.deliver();
    assert.match(outcome.detail, /is held: no_digest$/, outcome.detail);
    assert.equal(w.deps.store.getRunDigest(RUN_ID), null);
    const lost = w.flag("reads_lost")!.counts;
    assert.deepEqual(Object.keys(lost).sort(), ["lost", "notMeasurable"]);
  });

  test("Run again with Salesforce answering: released, and the cleared flags stay cleared", async () => {
    const w = await world();
    await w.deliver();
    assert.equal(w.deps.store.held.get(RUN_ID), "reads_lost");
    const pressed = w.runAgain();
    w.state.down = false;
    const outcome = await w.deliver(pressed);
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is released` });
    assert.equal(w.stored().run.status, "released");
    assert.deepEqual(w.open(), []);
    assert.ok(w.flag("reads_lost")!.clearedAt !== null);
  });

  test("Run again with the outage still on: held reads_lost again, never released, its flag opened again for this attempt", async () => {
    const w = await world();
    await w.deliver();
    const pressed = w.runAgain();
    const outcome = await w.deliver(pressed);
    assert.match(outcome.detail, /is held: reads_lost$/, outcome.detail);
    assert.equal(w.deps.store.releases.has(RUN_ID), false);
    const again = w.flag("reads_lost")!;
    // Opened again as the console's FLAG_CONFLICT does: uncleared and unposted, so it reaches a person again.
    assert.deepEqual([again.clearedAt, again.clearedBy, again.postedAt], [null, null, null]);
    assert.ok(Date.parse(again.raisedAt) >= Date.parse(w.deps.store.getRunDigest(RUN_ID)!.madeAt));
    // A third press, outage still on: held again. No count of presses releases it.
    const third = w.runAgain();
    assert.match((await w.deliver(third)).detail, /is held: reads_lost$/);
  });

  test("a run that failed with the old READS_LOST can be run again under this code: released when Salesforce answers", async () => {
    const w = await world();
    w.state.down = false;
    // As the old worker left it: failed with its sentence on the worker step, its run_failed flag, and a folder
    // whose mark has the old once-only key.
    const OLD = "Salesforce didn't answer part of what the run reads, even after several tries, so the run wasn't released with those checks missing. Run it again once Salesforce is answering. If it loses them again, the run is released with those checks marked not measured.";
    await w.deps.store.recordRunFailure(RUN_ID, OLD, AS_OF);
    const dir = join(w.deps.runsDir, RUN_ID);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, AGAIN_FILE), `${JSON.stringify({ again: "1700000000000", readsLostBefore: true })}\n`);
    const pressed = w.runAgain();
    const outcome = await w.deliver(pressed);
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is released` });
    assert.ok(!w.stored().run.steps.some((s) => s.error === OLD), "the old sentence is replaced with the run's steps");
  });

  test("the same press on old code's folder, outage on: held, never released by the old once-only mark", async () => {
    const w = await world();
    const pressed = 1764000000000;
    const dir = join(w.deps.runsDir, RUN_ID);
    mkdirSync(dir, { recursive: true });
    // A retry of a press the old worker started: its mark says readsLostBefore, which nothing reads now.
    writeFileSync(join(dir, AGAIN_FILE), `${JSON.stringify({ again: String(pressed), readsLostBefore: true })}\n`);
    const outcome = await w.deliver(pressed);
    assert.match(outcome.detail, /is held: reads_lost$/, outcome.detail);
  });

  test("a consultant-led run with the same outage goes to review as before: not held, no flag", async () => {
    const w = await world({ mode: "consultant" });
    await w.deliver();
    assert.ok(["awaiting_manual", "in_review"].includes(w.stored().run.status), w.stored().run.status);
    assert.equal(w.deps.store.held.has(RUN_ID), false);
    assert.deepEqual(w.open(), []);
  });

  test("the order of holds is written down: what the customer could read, then lost reads, then the run as a whole", () => {
    assert.deepEqual([...HOLD_REASONS], ["section_failed", "no_digest", "digest_held", "reads_lost", "mostly_unmeasured", "score_moved"]);
  });
});

describe("an automatic budget stop raises its flag (DECISIONS.md 715)", () => {
  test("stopped at a tenth of the limit: the step, and the stopped_budget flag once, not again on a delivery made again", async () => {
    const deps = baseDeps({ dataTransport: new Simulator({ dailyLimit: 1_500 }), automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration: await automaticSyntheticCalibration(), intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const handle = createJobHandler(deps);
    const first = await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.equal(first.outcome, "stopped_budget");
    const stored = deps.store.runs.get(RUN_ID)!;
    assert.equal(stored.run.steps.find((s) => s.id === BUDGET_STOPPED_STEP)?.error, BUDGET_STOPPED);
    const flag = structuredClone(deps.store.flags.get(`${RUN_ID}|stopped_budget`));
    assert.ok(flag);
    assert.deepEqual(flag.counts, {});
    const second = await handle({ kind: "run", runId: RUN_ID }, { final: false });
    assert.equal(second.outcome, "stopped_budget");
    assert.equal(deps.store.writes.filter((x) => x.method === "raiseFlag" && x.detail === "stopped_budget").length, 1);
    assert.deepEqual(deps.store.flags.get(`${RUN_ID}|stopped_budget`), flag);
  });

  test("the store raises it only on an automatic run stopped at its budget", async () => {
    const deps = baseDeps();
    deps.store.addRun(runInput());
    deps.store.runs.get(RUN_ID)!.run.status = "stopped_budget";
    await deps.store.flagBudgetStop(RUN_ID, AS_OF);
    assert.deepEqual(deps.store.flagsOf(RUN_ID), []);
  });
});
