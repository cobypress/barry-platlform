import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { canRelease, Metric, Release, ReportSection, type Calibration } from "../../schemas/index.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import type { DomainModule } from "../../domains/types.ts";
import { AS_OF, ORG_ID } from "../../testing/synthetic-org.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { CONSOLE_CHECKS, passesCheck } from "../console-checks.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import type { MemoryWorkerStore } from "../memory-store.ts";

// An automatic run end to end over the synthetic org (DECISIONS.md 693): the
// calibration the connect job builds from the org's own stages, nothing
// answered, no person anywhere. It never waits, every section is checked by
// code, the digest is written with the release, and the release names nobody.

const ORG15 = ORG_ID.slice(0, 15);
const ON = { on: true, orgs: new Set([ORG15]) };
const job = { kind: "run" as const, runId: RUN_ID };
const PREVIOUS = "hc-2026-09-30-00000001";

async function automaticJob(options: { calibration?: Calibration; previousScore?: number } = {}) {
  const calibration = options.calibration ?? (await automaticSyntheticCalibration());
  const deps = baseDeps({ dataTransport: new AutomaticReplay(), automatic: ON });
  // A consultant's release of an earlier run of the same org, whose rows a hold must leave as they are.
  if (options.previousScore !== undefined) {
    deps.store.addRun(runInput({ run: consoleRun({ id: PREVIOUS, status: "released", startedAt: "2026-09-30T09:00:00.000Z" }) }));
    Object.assign(deps.store.runs.get(PREVIOUS)!, { overallScore: options.previousScore, overallGrade: "C" });
    deps.store.releases.set(PREVIOUS, [Release.parse({ id: "rel-0001", runId: PREVIOUS, output: "executive_summary", sectionIds: ["s-0001"], releasedBy: "consultant@example.com", releasedAt: "2026-10-01T09:00:00.000Z" })]);
  }
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  const handle = createJobHandler(deps);
  const outcome = await handle(job, { final: false });
  return { deps, store: deps.store, outcome, handle };
}

const statuses = (store: MemoryWorkerStore, method: string) => store.writes.filter((w) => w.method === method).map((w) => w.detail);

describe("an automatic run, end to end, with nothing answered (DECISIONS.md 693)", async () => {
  const { store, outcome, deps } = await automaticJob();
  const stored = store.runs.get(RUN_ID)!;

  test("it is released, and never waited for a person", () => {
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is released` });
    assert.equal(stored.run.status, "released");
    assert.ok(!statuses(store, "saveRunProgress").includes("awaiting_manual"));
    assert.ok(!statuses(store, "saveRunOutput").includes("awaiting_manual"));
    // Both of the engine's manual steps were raised, and skipped: nobody was asked.
    const steps = stored.output!.manualSteps;
    assert.ok(steps.length >= 2);
    assert.ok(steps.every((s) => s.status === "skipped"));
    // And the rule that needed one says not measurable, with the automatic reason, not a person's.
    const eac = stored.output!.findings.find((f) => f.ruleId === "utl.eac-advanced-sync");
    if (eac) {
      assert.equal(eac.status, "not_measurable");
      assert.match(eac.notMeasurableReason ?? "", /on an automatic run nobody is asked for it/);
    }
    assert.equal(store.held.has(RUN_ID), false);
    assert.deepEqual(store.flagsOf(RUN_ID), []);
  });

  test("every section is checked, with no approver, and its current text is one the run holds", () => {
    const sections = stored.output!.sections;
    assert.ok(sections.length > 10);
    for (const s of sections) {
      assert.equal(s.status, "checked", s.key);
      assert.equal(s.approval, null);
      assert.ok(ReportSection.safeParse(s).success);
      assert.ok(stored.output!.versions.some((v) => v.id === s.currentVersionId && v.numberCheck.passed), s.key);
      const row = { status: s.status, approvedBy: null, approvedAt: null };
      assert.ok(passesCheck(CONSOLE_CHECKS.sectionApproved, row) && passesCheck(CONSOLE_CHECKS.sectionApproval, row));
    }
    assert.deepEqual(canRelease(sections, "automatic"), { ok: true });
  });

  test("one release row per output, automatic and naming nobody, each passing the console's own checks", () => {
    const rows = store.releases.get(RUN_ID)!;
    const outputs = [...new Set(stored.output!.sections.map((s) => s.output))].sort();
    assert.deepEqual(rows.map((r) => r.output).sort(), outputs);
    for (const r of rows) {
      assert.equal(r.kind, "automatic");
      assert.equal(r.releasedBy, null);
      assert.ok(passesCheck(CONSOLE_CHECKS.releasedBy, { kind: r.kind, releasedBy: r.releasedBy }));
      assert.ok(passesCheck(CONSOLE_CHECKS.releaseKind, { kind: r.kind }));
      assert.deepEqual(r.sectionIds.sort(), stored.output!.sections.filter((s) => s.output === r.output).map((s) => s.id).sort());
      assert.equal(r.releasedAt, AS_OF);
    }
    // The run's row as released passes the hold's check.
    assert.ok(passesCheck(CONSOLE_CHECKS.held, { heldReason: null, reviewMode: "automatic", status: "released" }));
  });

  test("the digest is written in the same save, before the release, and says no person reviewed it", () => {
    const order = store.writes.filter((w) => w.id === RUN_ID).map((w) => w.method);
    // The memory store's one save says its digest, its results and its release in that order; the Postgres store's statements are held to the same order in release.test.ts.
    assert.deepEqual(order.slice(order.lastIndexOf("saveRunProgress") + 1), ["saveRunDigest", "saveRunOutput", "releaseAutomatic"]);
    const digest = store.getRunDigest(RUN_ID)!;
    assert.equal(digest.reviewed, false);
    assert.equal(digest.runId, RUN_ID);
  });

  test("the run folder says automatic, and nothing in it names an approver or a releaser", () => {
    const run = JSON.parse(readFileSync(join(stored.runDir!, "run.json"), "utf8")) as { reviewMode: string };
    assert.equal(run.reviewMode, "automatic");
    const sections = JSON.parse(readFileSync(join(stored.runDir!, "report-sections.json"), "utf8")) as Array<{ sections: Array<{ approval: unknown }> }>;
    assert.ok(sections.flatMap((o) => o.sections).every((s) => s.approval === null));
    assert.ok(deps.lines.some((l) => l === `Automatic run ${RUN_ID} passed every check and is released.`));
  });
});

describe("a score that moved 15 points or more since the org's last released run holds the run (DECISIONS.md 693)", async () => {
  const { store, outcome, handle } = await automaticJob({ previousScore: 5 });
  const stored = store.runs.get(RUN_ID)!;

  test("held, not released, with no release row and the reason set", () => {
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is held: score_moved` });
    assert.notEqual(stored.run.status, "released");
    assert.equal(store.releases.has(RUN_ID), false);
    assert.equal(store.held.get(RUN_ID), "score_moved");
    assert.ok(passesCheck(CONSOLE_CHECKS.held, { heldReason: "score_moved", reviewMode: "automatic", status: stored.run.status }));
  });

  test("one flag, with the scores as counts, and still one after the job is delivered again", async () => {
    assert.deepEqual(store.flagsOf(RUN_ID), ["score_moved"]);
    const counts = store.flags.get(`${RUN_ID}|score_moved`)!.counts;
    assert.equal(counts.previous, 5);
    assert.ok(counts.moved! >= 15 && counts.score === stored.overallScore);
    const again = await handle(job, { final: true });
    assert.equal(again.outcome, "skipped");
    assert.deepEqual(store.flagsOf(RUN_ID), ["score_moved"]);
  });

  test("the earlier run's release is untouched", () => {
    assert.equal(store.runs.get(PREVIOUS)!.run.status, "released");
    assert.deepEqual(store.releases.get(PREVIOUS), [Release.parse({ id: "rel-0001", runId: PREVIOUS, output: "executive_summary", sectionIds: ["s-0001"], releasedBy: "consultant@example.com", releasedAt: "2026-10-01T09:00:00.000Z" })]);
    assert.equal(store.flagsOf(PREVIOUS).length, 0);
  });
});

describe("the score check compares only with a run that was released (DECISIONS.md 693)", () => {
  test("an earlier run held or in review, however far its score, is never the comparison", async () => {
    const { store } = await automaticJob({ previousScore: 5 });
    // Same org, same score, but never released: the first job's comparison is the released one; take it away.
    const second = await automaticJob({ previousScore: 5 });
    second.store.runs.get(PREVIOUS)!.run = { ...second.store.runs.get(PREVIOUS)!.run, status: "in_review" };
    assert.equal(await second.store.lastReleasedScore("another-run", ORG15, "barometer"), null);
    assert.equal(await store.lastReleasedScore("another-run", ORG15, "barometer"), 5);
    // Another org's, or another product's, is never it either.
    assert.equal(await store.lastReleasedScore("another-run", "00D000000000009", "barometer"), null);
    assert.equal(await store.lastReleasedScore("another-run", ORG15, "pulse"), null);
  });

  test("with no earlier released run, a run with any score is released", async () => {
    const { store } = await automaticJob({ previousScore: undefined });
    assert.equal(store.runs.get(RUN_ID)!.run.status, "released");
  });
});

describe("probe H: a failed automatic run is never run again or released by code (DECISIONS.md 693)", () => {
  test("a second message after the failure is skipped, nothing read, nothing released, its flag left open", async () => {
    const calibration = await automaticSyntheticCalibration();
    const transport = new AutomaticReplay();
    const deps = baseDeps({ dataTransport: transport, automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    await deps.store.recordRunFailure(RUN_ID, "The run stopped with an error: boom", AS_OF);
    const outcome = await createJobHandler(deps)(job, { final: false });
    assert.deepEqual(outcome, { outcome: "skipped", detail: `Automatic run ${RUN_ID} failed, so it waits for a person to run it again and nothing was read` });
    assert.equal(transport.requests.length, 0);
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assert.equal(deps.store.releases.has(RUN_ID), false);
    assert.deepEqual(deps.store.flagsOf(RUN_ID), ["run_failed"]);
  });

  test("a failure landing while the run job works refuses its release whole", async () => {
    const calibration = await automaticSyntheticCalibration();
    const deps = baseDeps({ dataTransport: new AutomaticReplay(), automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const save = deps.store.saveRunOutput.bind(deps.store);
    deps.store.saveRunOutput = async (runId, output) => {
      await deps.store.recordRunFailure(runId, "Lost: another delivery gave up", AS_OF);
      return save(runId, output);
    };
    const outcome = await createJobHandler(deps)(job, { final: false });
    assert.equal(outcome.outcome, "skipped");
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assert.equal(deps.store.releases.has(RUN_ID), false);
  });

  test("a consultant-led run that failed is run again by a second message, as before", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), automatic: ON });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    await deps.store.recordRunFailure(RUN_ID, "boom", AS_OF);
    assert.equal((await createJobHandler(deps)(job, { final: false })).outcome, "done");
  });
});

describe("a hold landing between the job's two readings (the reviewer's N2)", () => {
  test("is seen at the second reading: nothing is read from the org", async () => {
    const calibration = await automaticSyntheticCalibration();
    const transport = new AutomaticReplay();
    const deps = baseDeps({ dataTransport: transport, automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const load = deps.store.loadRunJob.bind(deps.store);
    let reads = 0;
    deps.store.loadRunJob = async (runId) => {
      if (++reads === 2) await deps.store.holdRun(runId, "calibration_unreadable", AS_OF);
      return load(runId);
    };
    const outcome = await createJobHandler(deps)(job, { final: true });
    assert.equal(outcome.outcome, "skipped");
    assert.equal(reads, 2);
    assert.equal(transport.requests.length, 0);
    assert.equal(deps.store.releases.has(RUN_ID), false);
  });
});

describe("probe C: a held run is never read or released by code (DECISIONS.md 693)", () => {
  test("held before its job runs: skipped, nothing read, not released, its one flag left open", async () => {
    const calibration = await automaticSyntheticCalibration();
    const transport = new AutomaticReplay();
    const deps = baseDeps({ dataTransport: transport, automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    assert.equal(await deps.store.holdRun(RUN_ID, "calibration_unreadable", AS_OF), true);
    const outcome = await createJobHandler(deps)(job, { final: true });
    assert.deepEqual(outcome, { outcome: "skipped", detail: `Run ${RUN_ID} is held (calibration_unreadable), so it waits for a person and nothing was read` });
    assert.equal(transport.requests.length, 0);
    assert.notEqual(deps.store.runs.get(RUN_ID)!.run.status, "released");
    assert.equal(deps.store.releases.has(RUN_ID), false);
    assert.equal(deps.store.held.get(RUN_ID), "calibration_unreadable");
    assert.deepEqual(deps.store.flagsOf(RUN_ID), ["calibration_unreadable"]);
  });

  test("held while its job runs: the release is refused whole, and the hold stands", async () => {
    const calibration = await automaticSyntheticCalibration();
    const deps = baseDeps({ dataTransport: new AutomaticReplay(), automatic: ON });
    deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }), calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    // A hold landing after the job's second reading, as a second connect's could.
    const save = deps.store.saveRunOutput.bind(deps.store);
    deps.store.saveRunOutput = async (runId, output) => {
      await deps.store.holdRun(runId, "calibration_unreadable", AS_OF);
      return save(runId, output);
    };
    const outcome = await createJobHandler(deps)(job, { final: false }).catch((err: Error) => ({ outcome: "thrown", detail: err.message }));
    assert.equal(outcome.outcome, "thrown");
    assert.match(outcome.detail, /couldn't be released: the console doesn't have it as an unreleased, unheld automatic run/);
    assert.equal(deps.store.releases.has(RUN_ID), false);
    assert.notEqual(deps.store.runs.get(RUN_ID)!.run.status, "released");
    assert.equal(deps.store.held.get(RUN_ID), "calibration_unreadable");
    // The next delivery finds the hold and reads nothing.
    deps.store.saveRunOutput = save;
    const again = await createJobHandler(deps)(job, { final: true });
    assert.equal(again.outcome, "skipped");
  });
});

describe("the digest stored is the checked one (the reviewer's M2)", () => {
  test("a metric text that can't be marked is withheld in the digest the store holds, not served as built", async () => {
    // A module whose one metric's label is put together at run time from words no source writes whole, and no said made.
    const words = ["Quokka", "Wombat"].join(" ");
    const probe: DomainModule = {
      id: "marks_probe",
      cloud: "sales",
      label: "Marks probe",
      extractors: [],
      rules: [],
      metrics: [
        () => [
          Metric.parse({
            status: "measured", id: "probe.unmarked", domain: "configuration", label: `${words} counted`, definition: "A count of nothing.", unit: "count",
            evidence: [{ kind: "engine", path: "probe", count: null, dateRange: null, exclusions: [], capturedAt: AS_OF }],
            exclusions: [], assumptions: [], value: 0, n: 0, dateRange: null, segments: [], segmentExemptReason: "One figure.", series: [], stats: {},
          }),
        ],
      ],
    };
    const calibration = await automaticSyntheticCalibration();
    const deps = baseDeps({ dataTransport: new AutomaticReplay(), automatic: ON, modules: [...ALL_MODULES, probe] });
    const run = consoleRun({ reviewMode: "automatic" });
    deps.store.addRun(runInput({ run: { ...run, modules: [...run.modules, "marks_probe"] }, calibration, intake: null, answers: [] }));
    await deps.tokens.save(syntheticConnection());
    const outcome = await createJobHandler(deps)(job, { final: true });
    assert.equal(outcome.outcome, "done", outcome.detail);
    const stored = deps.store.getRunDigest(RUN_ID)!;
    // Its label can't be marked, so the metric is left out whole (fix round 2): never its value beside a withheld label.
    assert.equal(stored.metrics.find((m) => m.id === "probe.unmarked"), undefined);
    assert.ok(!JSON.stringify(stored).includes(words));
  });
});

describe("a consultant-led run is as it was (DECISIONS.md 693)", async () => {
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), automatic: ON });
  deps.store.addRun(runInput());
  await deps.tokens.save(syntheticConnection());
  const outcome = await createJobHandler(deps)(job, { final: false });
  const stored = deps.store.runs.get(RUN_ID)!;

  test("it waits for its manual steps, and nothing is checked, released, held or flagged", () => {
    assert.equal(outcome.outcome, "done");
    // The fixture's answers aren't given here, so it waits, as it always has.
    assert.equal(stored.run.status, "awaiting_manual");
    assert.ok(stored.output!.manualSteps.some((s) => s.status === "pending"));
    assert.ok(stored.output!.sections.every((s) => s.status !== "checked"));
    assert.equal(deps.store.releases.has(RUN_ID), false);
    assert.equal(deps.store.held.has(RUN_ID), false);
    assert.deepEqual(deps.store.flagsOf(RUN_ID), []);
    assert.equal(deps.store.getRunDigest(RUN_ID), null);
    assert.ok(!deps.store.writes.some((w) => w.method === "releaseAutomatic" || w.method === "raiseFlag"));
  });

  test("its run folder has no file an automatic run adds", () => {
    assert.ok(existsSync(join(stored.runDir!, "run.json")));
    assert.match(readFileSync(join(stored.runDir!, "run.json"), "utf8"), /"reviewMode": "consultant"/);
  });
});
