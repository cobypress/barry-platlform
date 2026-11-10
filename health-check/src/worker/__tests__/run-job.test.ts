import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FACTS_FILE } from "../../extract/facts.ts";
import { FactsFile, factsReport } from "../../facts/summary.ts";
import type { Rejection } from "../../narrative/apply.ts";
import type { NarrativeClient, NarrativeRequest } from "../../narrative/client.ts";
import { OUTPUT_FILES, SURVEY_FILE } from "../../render/render-run.ts";
import { SYNTHETIC_COMMENT, syntheticSurvey } from "../../testing/survey.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { JobBusyError } from "../lock.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { ConsoleFactsFile, FACTS_DETAIL_MAX, FACTS_FILE_MAX_BYTES, factsOf, SECTIONS_FILE } from "../run-job.ts";
import { ASSUMPTIONS } from "../../facts/assumptions.ts";
import type { ConnectionProvider } from "../../salesforce/connection.ts";
import { Run } from "../../schemas/run.ts";
import { RunClosedError, type RunProgress } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { ACCESS, baseDeps, CONNECTION, consoleRun, FlakyTransport, INTAKE, REFRESH, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

const job = { kind: "run", runId: RUN_ID };
const once = { final: false };
const last = { final: true };

async function setup(overrides: Parameters<typeof baseDeps>[0] = {}, input = runInput()) {
  const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), ...overrides });
  deps.store.addRun(input);
  await deps.tokens.save(syntheticConnection());
  return { deps, handle: createJobHandler(deps) };
}

const stored = (deps: { store: MemoryWorkerStore }) => deps.store.runs.get(RUN_ID)!;

describe("a run job", () => {
  test("runs the engine through the stored connection and hands the console its results", async () => {
    const { deps, handle } = await setup();
    const outcome = await handle(job, once);
    assert.deepEqual(outcome, { outcome: "done", detail: `Run ${RUN_ID} is awaiting manual` });
    const run = stored(deps);
    assert.equal(run.run.status, "awaiting_manual");
    assert.ok(run.run.steps.length > 5 && run.run.steps.every((s) => s.status === "done"), "the engine's steps replace the placeholder");
    assert.notEqual(run.run.calibrationHash, "unconfirmed");
    assert.ok(run.output!.findings.length > 0 && run.output!.metrics.length > 0 && run.output!.scores.length > 0);
    assert.equal(run.output!.manualSteps.length, 2);
    assert.ok(run.output!.sections.length > 10 && run.output!.versions.length >= run.output!.sections.length);
    assert.ok(run.output!.sections.every((s) => s.runId === RUN_ID));
    assert.equal(run.runDir, join(deps.runsDir, RUN_ID));
    for (const file of ["run.json", "findings.json", "intake.json", SECTIONS_FILE]) assert.ok(existsSync(join(run.runDir!, file)), file);
  });

  test("keeps the survey it built the sections from in the run's folder, without a word of any comment", async () => {
    const { deps, handle } = await setup({}, { ...runInput(), survey: syntheticSurvey(12) });
    await handle(job, once);
    const run = stored(deps);
    const kept = readFileSync(join(run.runDir!, SURVEY_FILE), "utf8");
    assert.ok(!kept.includes(SYNTHETIC_COMMENT));
    assert.equal(JSON.parse(kept).responses.length, 12);
    const sections = run.output!.sections.filter((s) => s.key === "team_survey");
    assert.deepEqual(sections.map((s) => s.output).sort(), ["executive_summary", "full_health_check", "raw_notes"]);
    const texts = run.output!.versions.filter((v) => sections.some((s) => s.currentVersionId === v.id)).map((v) => v.body);
    assert.ok(texts.every((t) => t.length > 0 && !t.includes(SYNTHETIC_COMMENT)));
    assert.ok(!readFileSync(join(run.runDir!, SECTIONS_FILE), "utf8").includes(SYNTHETIC_COMMENT));
  });

  test("a survey that was sent and nobody answered says so in the sections it hands over", async () => {
    const { deps, handle } = await setup({}, { ...runInput(), survey: syntheticSurvey(0) });
    await handle(job, once);
    const run = stored(deps);
    assert.ok(existsSync(join(run.runDir!, SURVEY_FILE)));
    const sections = run.output!.sections.filter((s) => s.key === "team_survey");
    assert.equal(sections.length, 3);
    for (const s of sections) assert.match(run.output!.versions.find((v) => v.id === s.currentVersionId)!.body, /^Fewer than 5 people answered the team survey/);
  });

  test("runs Service Cloud's two areas when the console's modules include service", async () => {
    const base = runInput();
    const { deps, handle } = await setup({}, { ...base, run: { ...base.run, modules: [...base.run.modules, "service"] } });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "done");
    const run = stored(deps);
    assert.ok(run.output!.findings.some((f) => f.domain === "service_operations") && run.output!.findings.some((f) => f.domain === "service_setup"));
    assert.ok(run.output!.scores.some((s) => s.domain === "service_operations"));
    assert.ok(run.output!.sections.some((s) => s.key === "domain_service_operations"));
  });

  test("refuses a module it doesn't know", async () => {
    const base = runInput();
    const { deps, handle } = await setup({}, { ...base, run: { ...base.run, modules: [...base.run.modules, "marketing"] } });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "failed");
    assert.match(stored(deps).run.steps.at(-1)!.error ?? "", /doesn't know these modules: marketing/);
  });

  test("never writes the console's own fields", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const run = stored(deps).run;
    assert.equal(run.customerRef, "SYN-0001");
    assert.equal(run.createdBy, "consultant@example.com");
    assert.equal(run.benchmarkOptIn, true);
    assert.equal(run.startedAt, "2026-10-31T09:00:00.000Z");
  });

  test("reports progress as each step finishes, before the results", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const methods = deps.store.writes.map((w) => w.method);
    assert.ok(methods.filter((m) => m === "saveRunProgress").length >= 5);
    assert.equal(methods.at(-1), "saveRunOutput");
    assert.equal(deps.store.writes.find((w) => w.method === "saveRunProgress")!.detail, "extracting");
  });

  test("goes to review when the console's manual answers cover every step", async () => {
    const { deps, handle } = await setup({}, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    assert.equal(stored(deps).run.status, "in_review");
    assert.ok(stored(deps).output!.manualSteps.every((s) => s.status !== "pending"));
  });

  test("a run that goes to review hands over its drafts in review, so the consultant can approve them", async () => {
    const { deps, handle } = await setup({}, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    const statuses = new Set(stored(deps).output!.sections.map((s) => s.status));
    assert.ok(!statuses.has("draft"), [...statuses].join(", "));
    assert.ok(statuses.has("in_review"));
  });

  test("a run waiting on manual steps keeps its drafts as drafts", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    assert.ok(stored(deps).output!.sections.some((s) => s.status === "draft"));
  });

  test("puts no token in anything the console sees", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    const seen = JSON.stringify([...deps.store.runs.values(), deps.store.writes]) + deps.lines.join("\n");
    assert.ok(!seen.includes(ACCESS) && !seen.includes(REFRESH));
  });
});

describe("a set-up review run job", () => {
  test("runs with the console's scope, only the set-up areas, and no pipeline section", async () => {
    const { deps, handle } = await setup({}, runInput({ run: consoleRun({ scope: "essentials" }) }));
    await handle(job, once);
    const run = stored(deps);
    const onDisk = JSON.parse(readFileSync(join(run.runDir!, "run.json"), "utf8")) as { scope: string; modules: string[] };
    assert.equal(onDisk.scope, "essentials");
    assert.deepEqual(onDisk.modules.sort(), ["adoption", "automation", "best_practice", "configuration", "integrations", "security", "technical_health", "utilisation"]);
    assert.ok(run.output!.findings.every((f) => onDisk.modules.includes(f.domain)));
    assert.ok(!run.output!.sections.some((s) => ["pipeline", "pipeline_deep_dive", "comparisons"].includes(s.key)));
  });
});

describe("a run job delivered again", () => {
  test("in review or released, it does nothing", async () => {
    for (const status of ["in_review", "released"] as const) {
      const { deps, handle } = await setup({}, runInput({ run: consoleRun({ status }) }));
      const outcome = await handle(job, once);
      assert.equal(outcome.outcome, "skipped");
      assert.equal(deps.store.writes.length, 0);
    }
  });

  test("after it finished, it reuses every checkpoint and writes the same results", async () => {
    const transport = new ReplayTransport(loadRecording());
    const { deps, handle } = await setup({ dataTransport: transport });
    await handle(job, once);
    const first = structuredClone(stored(deps).output);
    const before = transport.requests.length;
    await handle(job, once);
    const again = transport.requests.slice(before);
    assert.ok(again.every((r) => !new URL(r.url).searchParams.has("q") && !r.url.includes("/composite/")), "no data was read a second time");
    assert.deepEqual(stored(deps).output, first);
  });

  test("after a crash part-way, it resumes without repeating finished steps", async () => {
    const recording = loadRecording();
    const { deps, handle } = await setup({ dataTransport: new FlakyTransport(new ReplayTransport(recording), 12) });
    await assert.rejects(handle(job, once), /ECONNRESET/);
    const done = stored(deps).run.steps.filter((s) => s.status === "done").map((s) => s.id);
    assert.ok(done.length > 0, "some steps finished before the crash");
    assert.equal(stored(deps).output, null);

    const second = new ReplayTransport(recording);
    const resumed = createJobHandler({ ...deps, dataTransport: second });
    assert.equal((await resumed(job, once)).outcome, "done");
    assert.ok(stored(deps).run.steps.every((s) => s.status === "done"));
    const straight = await setup();
    await straight.handle(job, once);
    assert.deepEqual(stored(deps).output!.findings, stored(straight.deps).output!.findings, "the same answer as a run that never stopped");
  });

  test("a crash on the last delivery is recorded as a failure the console can show", async () => {
    const { deps, handle } = await setup({ dataTransport: new FlakyTransport(new ReplayTransport(loadRecording()), 12) });
    const outcome = await handle(job, last);
    assert.equal(outcome.outcome, "failed");
    const run = stored(deps).run;
    assert.equal(run.status, "failed");
    assert.match(run.steps.find((s) => s.id === "worker")!.error!, /The run stopped with an error: .*ECONNRESET/);
  });

  test("two deliveries at once: the second waits for a retry", async () => {
    const { deps, handle } = await setup();
    const results = await Promise.allSettled([handle(job, once), handle(job, once)]);
    assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    const rejected = results.find((r) => r.status === "rejected") as PromiseRejectedResult;
    assert.ok(rejected.reason instanceof JobBusyError);
    assert.equal(deps.store.writes.filter((w) => w.method === "saveRunOutput").length, 1);
  });

  test("if the console moves the run on meanwhile, the results aren't written over it", async () => {
    class MovingStore extends MemoryWorkerStore {
      override async saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
        await super.saveRunProgress(runId, progress, runDir);
        this.runs.get(runId)!.run.status = "in_review";
      }
    }
    const store = new MovingStore();
    const { handle } = await setup({ store });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "skipped");
    assert.match(outcome.detail, /in review/);
    assert.equal(store.runs.get(RUN_ID)!.output, null);
    assert.ok(new RunClosedError(RUN_ID, "in_review").message.includes("leaves it alone"));
  });
});

describe("a run job that can't start", () => {
  const cases: Array<[string, Parameters<typeof runInput>[0], RegExp]> = [
    ["no calibration", { calibration: null }, /calibration hasn't been confirmed/],
    ["a CLI login", { run: consoleRun({ connectionKind: "sf-cli" }) }, /only reads through a connection/],
    ["no connection named", { connectionName: null }, /Send the customer a connect link/],
    ["a connection that doesn't exist", { connectionName: "someone-else" }, /couldn't be loaded: No stored connection/],
    ["a module the worker doesn't have", { run: consoleRun({ modules: ["telepathy"] }) }, /doesn't know these modules: telepathy/],
    ["a connection to another org", { run: consoleRun({ orgId: "00D000000000099AAA" }) }, /different org/],
  ];
  for (const [name, input, reason] of cases) {
    test(`${name}: fails with the reason, having read nothing`, async () => {
      const transport = new ReplayTransport(loadRecording());
      const { deps, handle } = await setup({ dataTransport: transport }, runInput(input));
      const outcome = await handle(job, once);
      assert.equal(outcome.outcome, "failed");
      assert.match(outcome.detail, reason);
      assert.equal(stored(deps).run.status, "failed");
      assert.match(stored(deps).run.steps.find((s) => s.id === "worker")!.error!, reason);
      assert.equal(transport.requests.length, 0);
    });
  }

  test("an unknown run is skipped, and an id that isn't a folder name is refused", async () => {
    const { handle } = await setup();
    assert.equal((await handle({ kind: "run", runId: "hc-2026-10-31-99999999" }, once)).outcome, "skipped");
    assert.equal((await handle({ kind: "run", runId: "../etc" }, once)).outcome, "failed");
  });

  test("stops at the API budget, with its place kept for the next job", async () => {
    const { deps, handle } = await setup({ dataTransport: new Simulator({ used: 60_000 }) });
    const outcome = await handle(job, once);
    assert.equal(outcome.outcome, "stopped_budget");
    assert.equal(stored(deps).run.status, "stopped_budget");
    assert.equal(stored(deps).output, null);
  });

  test("a stored connection is still checked against the run's org on every delivery", async () => {
    const { deps, handle } = await setup();
    await deps.tokens.save(syntheticConnection({ orgId: "00D000000000099AAA" }));
    assert.match((await handle(job, once)).detail, /different org/);
    assert.equal(CONNECTION, stored(deps).connectionName);
    assert.ok(!readFileSync(join(deps.tokens.dir, `${CONNECTION}.json`), "utf8").includes(ACCESS), "tokens are encrypted at rest");
  });
});

describe("progress and the finished status", () => {
  test("progress never claims the run is finished: only the results write does", async () => {
    const { deps, handle } = await setup({}, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    const progress = deps.store.writes.filter((w) => w.method === "saveRunProgress").map((w) => w.detail);
    assert.ok(!progress.includes("in_review") && !progress.includes("awaiting_manual"), String(progress));
    assert.equal(progress.at(-1), "analysing");
    assert.equal(deps.store.writes.at(-1)!.detail, "in_review");
  });
});

describe("the sandbox flag", () => {
  test("progress keeps the console's value; the results carry what the org says", async () => {
    const { deps, handle } = await setup({}, runInput({ run: consoleRun({ isSandbox: true }) }));
    let duringRun: boolean[] = [];
    const save = deps.store.saveRunProgress.bind(deps.store);
    deps.store.saveRunProgress = async (runId, progress, runDir) => {
      duringRun.push(progress.isSandbox);
      return save(runId, progress, runDir);
    };
    await handle(job, once);
    assert.ok(duringRun.length > 0 && duringRun.every(Boolean));
    assert.equal(stored(deps).run.isSandbox, false, "the synthetic org is production");
    duringRun = [];
  });
});

describe("Claude's drafts", () => {
  const rejectionsIn = (deps: { runsDir: string }): Rejection[] => JSON.parse(readFileSync(join(deps.runsDir, RUN_ID, OUTPUT_FILES.rejections), "utf8")) as Rejection[];
  const writing = (text: (prompt: string) => string): NarrativeClient & { calls: number } => {
    const client = {
      calls: 0,
      complete: async ({ prompt, model }: NarrativeRequest) => {
        client.calls++;
        return { stopReason: "end_turn", model, text: JSON.stringify({ paragraphs: [{ text: text(prompt), sources: [] }] }) };
      },
    };
    return client;
  };

  test("with a client, each briefed section gets a claude_draft version, saved with the results", async () => {
    const client = writing(() => "We looked closely at this area.");
    const { deps, handle } = await setup({ narrative: client });
    assert.equal((await handle(job, once)).outcome, "done");
    const output = stored(deps).output!;
    const drafts = output.versions.filter((v) => v.source === "claude_draft");
    assert.ok(drafts.length > 10 && drafts.length === client.calls, `${drafts.length} drafts from ${client.calls} calls`);
    for (const draft of drafts) {
      const section = output.sections.find((s) => s.id === draft.sectionId)!;
      assert.equal(section.currentVersionId, draft.id);
      assert.equal(draft.body, "We looked closely at this area.");
    }
    assert.ok(output.versions.filter((v) => v.source === "engine").length === output.sections.length, "the engine's versions stay");
    assert.deepEqual(rejectionsIn(deps), []);
    assert.ok(deps.lines.some((l) => /Claude drafted \d+ of \d+ sections/.test(l)));
    // The run folder's copy matches what the console got.
    const saved = JSON.parse(readFileSync(join(deps.runsDir, RUN_ID, SECTIONS_FILE), "utf8")) as Array<{ versions: Array<{ source: string }> }>;
    assert.equal(saved.flatMap((o) => o.versions).filter((v) => v.source === "claude_draft").length, drafts.length);
  });

  test("a run in review hands over its drafts in review, and a rejected section stays needs input", async () => {
    const client = writing((prompt) => (prompt.startsWith("Section: Verdict") ? "You could save £9,999,999." : "We looked closely at this area."));
    const { deps, handle } = await setup({ narrative: client }, runInput({ answers: loadAnswers() }));
    await handle(job, once);
    const sections = stored(deps).output!.sections;
    assert.equal(sections.find((s) => s.key === "verdict")!.status, "needs_input");
    assert.equal(sections.find((s) => s.key === "overview")!.status, "in_review");
    assert.ok(!sections.some((s) => s.status === "draft"));
    assert.deepEqual(rejectionsIn(deps).map((r) => r.sectionId.split(":").at(-1)), ["verdict"]);
  });

  test("when Claude can't be reached, the run still finishes with the engine's text", async () => {
    let calls = 0;
    const down: NarrativeClient = { complete: async () => (calls++, Promise.reject(new Error("Connection error."))) };
    const { deps, handle } = await setup({ narrative: down });
    const plain = await setup();
    assert.equal((await handle(job, once)).outcome, "done");
    await plain.handle(job, once);
    assert.equal(calls, 1, "it stops asking after the first failure");
    assert.deepEqual(stored(deps).output, stored(plain.deps).output, "the same results as a worker with no key");
    const rejections = rejectionsIn(deps);
    assert.equal(rejections.length, 1);
    assert.equal(rejections[0]!.unavailable, true);
    assert.ok(deps.lines.some((l) => /couldn't be reached.*Connection error/.test(l)));
  });

  test("if drafting breaks in a way nobody expected, the run still finishes with the engine's text", async () => {
    const broken = { complete: async () => undefined } as unknown as NarrativeClient;
    const { deps, handle } = await setup({ narrative: broken });
    const plain = await setup();
    assert.equal((await handle(job, once)).outcome, "done");
    await plain.handle(job, once);
    assert.deepEqual(stored(deps).output, stored(plain.deps).output);
    assert.match(rejectionsIn(deps)[0]!.reasons[0]!, /Drafting stopped/);
    assert.ok(deps.lines.some((l) => /Drafting stopped for/.test(l)));
  });

  test("without a client, nothing is drafted and no rejections file is written", async () => {
    const { deps, handle } = await setup();
    await handle(job, once);
    assert.ok(stored(deps).output!.versions.every((v) => v.source === "engine"));
    assert.equal(existsSync(join(deps.runsDir, RUN_ID, OUTPUT_FILES.rejections)), false);
  });
});

describe("review fixes: failures after the engine finishes", () => {
  const badIntake = () => runInput({ intake: { ...INTAKE, summary: "" } });

  test("on the last delivery, the run is marked failed with the reason, not left analysing", async () => {
    const { deps, handle } = await setup({}, badIntake());
    const outcome = await handle(job, last);
    assert.equal(outcome.outcome, "failed");
    assert.equal(stored(deps).run.status, "failed");
    assert.match(stored(deps).run.steps.find((s) => s.id === "worker")!.error!, /The run finished, but its results couldn't be saved/);
  });

  test("before the last delivery, it's thrown for a retry, which reads no data again", async () => {
    const transport = new ReplayTransport(loadRecording());
    const { deps, handle } = await setup({ dataTransport: transport }, badIntake());
    await assert.rejects(handle(job, once));
    assert.equal(stored(deps).run.status, "analysing");
    const before = transport.requests.length;
    deps.store.runs.get(RUN_ID)!.intake = INTAKE;
    assert.equal((await handle(job, once)).outcome, "done");
    assert.ok(transport.requests.slice(before).every((r) => !new URL(r.url).searchParams.has("q")));
  });

  test("a results write the store refuses for another reason is recorded on the last delivery", async () => {
    const { deps, handle } = await setup();
    deps.store.saveRunOutput = async () => Promise.reject(new Error("deadlock detected"));
    const outcome = await handle(job, last);
    assert.equal(outcome.outcome, "failed");
    assert.match(stored(deps).run.steps.find((s) => s.id === "worker")!.error!, /deadlock detected/);
  });
});

describe("a free scan's run job", () => {
  test("writes the teaser, gives the connection back, and asks Claude for nothing", async () => {
    const asked: NarrativeRequest[] = [];
    const narrative = { complete: async (request: NarrativeRequest) => (asked.push(request), Promise.reject(new Error("a scan never drafts"))) } as unknown as NarrativeClient;
    const { deps, handle } = await setup({ narrative }, runInput({ run: consoleRun({ scope: "essentials", product: "scan" }) }));
    await handle(job, last);
    const saved = deps.store.scanResults.get(RUN_ID);
    assert.ok(saved, "the teaser is saved");
    const { result } = saved;
    assert.ok(result.shown.length > 0 && result.shown.length <= 4);
    assert.equal(result.lockedIssues, stored(deps).output!.findings.filter((f) => f.status === "issue").length - result.shown.length);
    assert.ok(result.areas.length > 0 && result.areas.every((a) => ["adoption", "automation", "best_practice", "configuration", "integrations", "security", "technical_health", "utilisation"].includes(a.domain)));
    // Both set-up areas are in the scan, so their issues count among the locked ones.
    for (const domain of ["integrations", "technical_health"]) assert.ok(result.areas.some((a) => a.domain === domain && a.issues > 0), domain);
    assert.doesNotMatch(JSON.stringify(result), /\[\[[a-z]+:|@example\.com/);
    assert.equal(asked.length, 0);
    assert.equal(deps.tokens.has(CONNECTION), false, "the scan's connection is gone");
    assert.equal(existsSync(join(deps.runsDir, RUN_ID)), false, "nothing of the org stays on the worker");
  });

  test("sends the facts summary with its results, as an ordinary run does, though its folder is then deleted (DECISIONS.md 324)", async () => {
    const { deps, handle } = await setup({}, runInput({ run: consoleRun({ scope: "essentials", product: "scan" }) }));
    await handle(job, last);
    const facts = stored(deps).output!.facts;
    assert.ok(facts, "the scan's facts are sent");
    assert.ok(facts.counts.confirmed > 0);
    assert.equal(facts.counts.contradicted, facts.differed.length);
    assert.equal(existsSync(join(deps.runsDir, RUN_ID)), false);
    const { deps: ordinary, handle: run } = await setup();
    await run(job, last);
    assert.ok(stored(ordinary).output!.facts?.counts.confirmed, "an ordinary run's too");
  });

  test("an ordinary run keeps its connection and writes no teaser", async () => {
    const { deps, handle } = await setup();
    await handle(job, last);
    assert.equal(deps.store.scanResults.size, 0);
    assert.equal(deps.tokens.has(CONNECTION), true);
  });
});

describe("the facts file a run job sends (DECISIONS.md 433)", () => {
  // Made up: a fact we assume, with marked text so a log line that quoted the file would show.
  const fact = (id: string, detail = "DETAIL-MARK Scontrol isn't in the org's objects."): FactsFile["facts"][number] => ({
    id,
    question: "integrations.i",
    covers: "part",
    says: "Scontrol can be queried in any org, including one that never had s-controls.",
    outcome: "contradicted",
    detail,
    observed: { Scontrol: false },
    affects: ["tech.classic-leftovers"],
  });
  const file = (facts = [fact("object.scontrol")]): FactsFile => ({ version: 1, checkedAt: "2026-10-31T09:30:00.000Z", apiVersion: "62.0", connectionKind: "oauth", calls: 4, facts });

  const read = (content: string) => {
    const dir = mkdtempSync(join(tmpdir(), "hc-facts-"));
    try {
      writeFileSync(join(dir, FACTS_FILE), content);
      const lines: string[] = [];
      return { ...factsOf(dir, (line) => lines.push(line)), lines };
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  };

  test("is carried beside the summary when it parses, names only facts we assume and is under the cap", () => {
    const sent = file([fact("object.scontrol"), { ...fact("object.package-licence"), outcome: "confirmed" }]);
    const got = read(JSON.stringify(sent));
    assert.deepEqual(got.file, sent);
    assert.deepEqual(got.summary, factsReport(sent));
    assert.deepEqual(got.lines, []);
  });

  test("is refused when it names a fact we don't assume, and the summary is still sent", () => {
    const sent = file([fact("object.scontrol"), fact("ID-MARK.not-assumed")]);
    const got = read(JSON.stringify(sent));
    assert.equal(got.file, null);
    assert.deepEqual(got.summary, factsReport(sent));
    assert.equal(got.lines.length, 1);
    assert.match(got.lines[0]!, /names a fact we don't assume/);
    assert.doesNotMatch(got.lines[0]!, /MARK|object\.scontrol|integrations/);
  });

  test("is refused over the size cap, and the summary is still sent", () => {
    // Details are cut before the size is measured, so only many names can fill
    // the file: each fact we assume, with as many observed names as the console
    // keeps, each as long as it keeps, until one more wouldn't fit under the cap.
    const size = (f: FactsFile): number => Buffer.byteLength(JSON.stringify(f), "utf8");
    const fits = file([]);
    full: for (const a of ASSUMPTIONS) {
      const added = { ...fact(a.id, ""), observed: {} as Record<string, string> };
      fits.facts.push(added);
      if (size(fits) > FACTS_FILE_MAX_BYTES - 1) {
        fits.facts.pop();
        break;
      }
      for (let n = 0; n < 50; n++) {
        const key = `OBS-MARK.${a.id}.${n}.`.padEnd(200, "k");
        added.observed[key] = "v".repeat(100);
        if (size(fits) > FACTS_FILE_MAX_BYTES - 1) {
          delete added.observed[key];
          break full;
        }
      }
    }
    // The last of the room goes in a detail, which is short enough not to be cut.
    const last = fits.facts.at(-1)!;
    last.detail = "y".repeat(FACTS_FILE_MAX_BYTES - 1 - size(fits));
    assert.ok(last.detail.length <= FACTS_DETAIL_MAX);
    assert.equal(size(fits), FACTS_FILE_MAX_BYTES - 1);
    const carried = read(JSON.stringify(fits));
    assert.deepEqual(carried.file, fits, "just under the cap is carried");
    assert.deepEqual(carried.lines, []);

    const sent = structuredClone(fits);
    sent.facts.at(-1)!.detail += "y";
    assert.equal(size(sent), FACTS_FILE_MAX_BYTES);
    const got = read(JSON.stringify(sent));
    assert.equal(got.file, null);
    assert.deepEqual(got.summary, factsReport(sent));
    assert.equal(got.lines.length, 1);
    assert.match(got.lines[0]!, /larger than the worker sends/);
    assert.doesNotMatch(got.lines[0]!, /MARK|yyyy|object\.scontrol/);
  });

  test("sends a long detail on one line, cut inside the console's limit, and leaves facts.json on disk whole", () => {
    // Salesforce's error text, made up, with the line breaks and runs of spaces a server's reply can have.
    const long = `DETAIL-MARK INVALID_TYPE:\n  sObject type 'Scontrol' is not supported.\r\n\t${"Lorem ipsum\n\n dolor sit amet. ".repeat(300)}`.slice(0, 5000);
    assert.equal(long.length, 5000);
    const other = { ...fact("object.package-licence"), outcome: "confirmed" as const };
    const onDisk = JSON.stringify(file([fact("object.scontrol", long), other]), null, 2);
    const dir = mkdtempSync(join(tmpdir(), "hc-facts-"));
    try {
      writeFileSync(join(dir, FACTS_FILE), onDisk);
      const lines: string[] = [];
      const got = factsOf(dir, (line) => lines.push(line));
      const sent = got.file!.facts[0]!.detail;
      assert.equal(sent.length, 1000, "the console keeps 1000");
      assert.ok(sent.endsWith("…"));
      assert.doesNotMatch(sent, /\s\s|[\n\r\t]/);
      assert.ok(sent.startsWith("DETAIL-MARK INVALID_TYPE: sObject type 'Scontrol' is not supported. Lorem ipsum dolor sit amet. Lorem"));
      assert.deepEqual(got.file!.facts[1], other, "the other facts are sent as they are");
      assert.deepEqual({ ...got.file!, facts: [] }, file([]));
      assert.deepEqual(got.summary, factsReport(JSON.parse(onDisk)));
      assert.deepEqual(lines, []);
      assert.equal(readFileSync(join(dir, FACTS_FILE), "utf8"), onDisk, "facts.json on disk is unchanged");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test("never cuts a detail through the middle of a character", () => {
    // The emoji's two halves would sit either side of the cut, and half of one isn't valid in Postgres's jsonb.
    const sent = file([fact("object.scontrol", `${"a".repeat(998)}\u{1F600}${"b".repeat(10)}`)]);
    const detail = read(JSON.stringify(sent)).file!.facts[0]!.detail;
    assert.equal(detail, `${"a".repeat(998)}…`);
    assert.doesNotMatch(detail, /[\uD800-\uDFFF]/);
  });

  test("sends a detail without its control characters, which Postgres's jsonb can't always hold", () => {
    const sent = file([fact("object.scontrol", "DETAIL-MARK INVALID_TYPE:\u0000sObject\u0007type \u009B'Scontrol'\u007F\u0000")]);
    const got = read(JSON.stringify(sent));
    assert.equal(got.file!.facts[0]!.detail, "DETAIL-MARK INVALID_TYPE: sObject type 'Scontrol'");
    assert.deepEqual(got.lines, []);
  });

  test("is refused when a NUL is anywhere but a detail, and the summary is still sent", () => {
    const cases: Array<[string, FactsFile]> = [
      ["an observed name", file([{ ...fact("object.scontrol"), observed: { "Scontrol\u0000MARK": false } }])],
      ["an observed value", file([{ ...fact("object.scontrol"), observed: { Scontrol: "picklist\u0000MARK" } }])],
    ];
    for (const [what, sent] of cases) {
      const got = read(JSON.stringify(sent));
      assert.equal(got.file, null, what);
      assert.deepEqual(got.summary, factsReport(sent), what);
      assert.deepEqual(got.lines, ["The run's facts.json doesn't fit the console's shape for it, so the console gets the facts summary but not the file."], what);
    }
  });

  test("sends a detail of exactly the console's limit as it is", () => {
    const sent = file([fact("object.scontrol", `DETAIL-MARK ${"z".repeat(1000 - 12)}`)]);
    assert.equal(sent.facts[0]!.detail.length, 1000);
    assert.deepEqual(read(JSON.stringify(sent)).file, sent);
  });

  test("is refused when a value breaks one of the console's caps, and the summary is still sent", () => {
    const cases: Array<[string, FactsFile]> = [
      ["an observed value of 101 characters", file([{ ...fact("object.scontrol"), observed: { Scontrol: `OBS-MARK${"v".repeat(93)}` } }])],
      ["an affected check with a capital letter", file([{ ...fact("object.scontrol"), affects: ["tech.Classic-MARK"] }])],
      ["an empty sentence", file([{ ...fact("object.scontrol"), says: "" }])],
      ["a fact listed twice", file([fact("object.scontrol"), fact("object.scontrol")])],
      ["a connection kind that isn't a name", { ...file(), connectionKind: "OAuth MARK" }],
      ["a time that isn't one", { ...file(), checkedAt: "Tuesday MARK" }],
    ];
    assert.equal((cases[0]![1].facts[0]!.observed.Scontrol as string).length, 101);
    for (const [what, sent] of cases) {
      const got = read(JSON.stringify(sent));
      assert.equal(got.file, null, what);
      assert.deepEqual(got.summary, factsReport(sent), what);
      // One fixed line, so nothing of the file can be in it.
      assert.deepEqual(got.lines, ["The run's facts.json doesn't fit the console's shape for it, so the console gets the facts summary but not the file."], what);
    }
  });

  test("the engine's own facts fit the console's caps: every assumption and every connection kind", () => {
    // facts.json records the connection provider's kind. The compiler holds this
    // list to ConnectionProvider["kind"], so a kind added there fails typecheck here.
    const kinds = Object.keys({ "sf-cli": true, oauth: true } satisfies Record<ConnectionProvider["kind"], true>);
    assert.deepEqual([...kinds].sort(), [...Run.shape.connectionKind.options].sort(), "the run schema allows the same kinds");
    assert.ok(ASSUMPTIONS.length <= 100, "no more facts than the console keeps");
    assert.equal(new Set(ASSUMPTIONS.map((a) => a.id)).size, ASSUMPTIONS.length, "each id once");
    for (const connectionKind of kinds) {
      const sent: FactsFile = {
        ...file(),
        connectionKind,
        facts: ASSUMPTIONS.map((a) => ({ id: a.id, question: a.question, covers: a.covers, says: a.says, outcome: "confirmed", detail: "", observed: {}, affects: [...a.affects] })),
      };
      const parsed = ConsoleFactsFile.safeParse(sent);
      assert.ok(parsed.success, `${connectionKind}: ${parsed.error?.issues.map((i) => `${i.path.join(".")} ${i.message}`).join("; ")}`);
    }
    // The same caps said plainly, so a slip in the worker's copy of the console's schema can't hide one.
    const name = /^[a-z0-9][a-z0-9._-]*$/;
    for (const a of ASSUMPTIONS) {
      for (const n of [a.id, a.question, ...a.affects]) assert.ok(name.test(n) && n.length <= 100, `${a.id}: ${n}`);
      assert.ok(a.says.length >= 1 && a.says.length <= 500, a.id);
      assert.ok(a.affects.length <= 50, a.id);
    }
    for (const k of kinds) assert.ok(name.test(k) && k.length <= 40, k);
  });

  test("is refused when it doesn't parse, and there's no summary, as before", () => {
    for (const content of ["{ DETAIL-MARK not json", JSON.stringify({ ...file(), facts: [{ ...fact("object.scontrol"), outcome: "DETAIL-MARK" }] })]) {
      const got = read(content);
      assert.equal(got.file, null);
      assert.equal(got.summary, null);
      assert.equal(got.lines.length, 1);
      assert.match(got.lines[0]!, /didn't parse/);
      assert.doesNotMatch(got.lines[0]!, /MARK|object\.scontrol/);
    }
  });

  test("a run sends the facts.json its folder holds, whole", async () => {
    const { deps, handle } = await setup();
    await handle(job, last);
    const run = stored(deps);
    const onDisk = FactsFile.parse(JSON.parse(readFileSync(join(run.runDir!, FACTS_FILE), "utf8")));
    assert.ok(onDisk.facts.length > 0);
    assert.deepEqual(run.output!.factsFile, onDisk);
    assert.deepEqual(run.output!.facts, factsReport(onDisk));
  });
});
