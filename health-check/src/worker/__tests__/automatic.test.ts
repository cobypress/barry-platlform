import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { workerCommand, type WorkerModules } from "../../cli/worker.ts";
import type { BullModule } from "../bullmq.ts";
import { buildDigest } from "../../digest/build.ts";
import { buildReport, currentBody } from "../../narrative/sections.ts";
import { renderRawNotes } from "../../notes/raw-notes.ts";
import { executiveHtml } from "../../render/executive.ts";
import { approvedReport, AUTOMATIC_DOCUMENTS_NOT_BUILT, type ApprovedRun } from "../../render/render-released.ts";
import { loadRun } from "../../render/render-run.ts";
import { CHECKED_OUTSIDE_AUTOMATIC, ReportSection, Run, SectionsOfRun, type SectionVersion } from "../../schemas/index.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { AUTOMATIC_ORG_NOT_LISTED, AUTOMATIC_SWITCHED_OFF, automaticRefusal, automaticStartLine, readAutomaticOrgs } from "../automatic.ts";
import { processDocumentsJob } from "../documents-job.ts";
import { readWorkerEnv } from "../env.ts";
import { runFromRows, runSql, RUN, RUN_DOCUMENTS } from "../postgres-store.ts";
import { createJobHandler } from "../worker.ts";
import type { DocumentsJobInput } from "../types.ts";
import { baseDeps, consoleRun, INTAKE, RUN_ID, runInput, syntheticConnection, temp } from "./helpers.ts";

// An automatic run (DECISIONS.md 688) can't get past its claim on this
// worker, can't be released or made into documents, and changes nothing a
// consultant-led run reads, writes or prints.

const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });
const ORG15 = consoleRun().orgId.slice(0, 15);

describe("what's stored before decision 688 still reads", () => {
  test("the recorded run folder's run and every section parse, as consultant-led", () => {
    const run = Run.parse(JSON.parse(readFileSync(join(recorded, "run.json"), "utf8")));
    assert.equal(run.reviewMode, "consultant");
    // The folder keeps no sections; the run job's file of them is read below, from a delivery.
    const sections = Object.values(buildReport(loadRun(recorded).input).outputs).flatMap((o) => JSON.parse(JSON.stringify(o.sections)) as unknown[]);
    assert.ok(sections.length > 10);
    for (const s of sections) assert.ok(ReportSection.safeParse(s).success);
    // The fixture calibration's hash as the engine gave it before decision 688, so a run started then still resumes.
    assert.equal(run.calibrationHash, "6e51340ac87a11f9");
  });

  test("the store reads a run with no reviewMode column, or null in it, as consultant-led, and automatic as automatic", () => {
    const { reviewMode: _m, analysisWindow, steps: _s, ...rest } = consoleRun();
    const row = { ...rest, analysisFrom: analysisWindow.from, analysisTo: analysisWindow.to, connectionName: "synthetic-co" };
    assert.equal(runFromRows(row, []).reviewMode, "consultant");
    assert.equal(runFromRows({ ...row, reviewMode: null }, []).reviewMode, "consultant");
    assert.equal(runFromRows({ ...row, reviewMode: "automatic" }, []).reviewMode, "automatic");
    assert.throws(() => runFromRows({ ...row, reviewMode: "unreviewed" }, []));
    // Read through the row as JSON, so the query works before the console's migration adds the column.
    for (const columns of [RUN, RUN_DOCUMENTS]) {
      const sql = runSql(columns);
      assert.match(sql, /, to_jsonb\("HealthCheckRun"\) ->> 'reviewMode' AS "reviewMode" FROM "HealthCheckRun" WHERE "id" = \$1$/);
      assert.doesNotMatch(sql, /SELECT[^]*"reviewMode",/);
    }
  });
});

describe("the worker's switch", () => {
  const env = (extra: Record<string, string> = {}): NodeJS.ProcessEnv => ({
    REDIS_URL: "redis://localhost:6379",
    HC_WORKER_DATABASE_URL: "postgres://worker:pw@db.example.com/console",
    HC_CONNECT_KEY: randomBytes(32).toString("base64"),
    HC_TOKEN_KEY: randomBytes(32).toString("base64"),
    HC_CONNECTIONS_DIR: `${temp()}/connections`,
    HC_RUNS_DIR: `${temp()}/runs`,
    HC_SF_CLIENT_ID: "3MVG9fake.client.id",
    HC_SF_CLIENT_SECRET: "fake-client-secret",
    HC_SF_REDIRECT_URI: "https://console.example.com/connect/callback",
    ...extra,
  });

  test("on only for exactly on; the orgs are 15-character ids, and anything else stops the start", () => {
    const read = (extra: Record<string, string>) => {
      const r = readWorkerEnv(env(extra));
      return r.ok ? r.config.automatic : r.problems.join("; ");
    };
    assert.deepEqual(read({}), { on: false, orgs: new Set() });
    for (const value of ["", "ON", " on", "on ", "1", "true"]) assert.equal((read({ HC_AUTOMATIC: value }) as { on: boolean }).on, false, JSON.stringify(value));
    assert.deepEqual(read({ HC_AUTOMATIC: "on", HC_AUTOMATIC_ORGS: ` ${ORG15}, 00D000000000002 ,` }), { on: true, orgs: new Set([ORG15, "00D000000000002"]) });
    assert.match(String(read({ HC_AUTOMATIC_ORGS: `${ORG15}AAA` })), /HC_AUTOMATIC_ORGS must be 15-character org ids separated by commas: 1 entry isn't/);
    assert.deepEqual(readAutomaticOrgs("00D00000000000!,x"), { ok: false, bad: 2 });
  });

  test("decides nothing for a consultant-led run, whatever the switch", () => {
    for (const sw of [undefined, { on: false, orgs: new Set<string>() }, { on: true, orgs: new Set([ORG15]) }]) assert.equal(automaticRefusal(consoleRun(), sw), null);
  });

  test("says at start which state the worker is in, with no org id", () => {
    assert.equal(automaticStartLine({ on: false, orgs: new Set([ORG15]) }), "Automatic runs are off: HC_AUTOMATIC isn't on, so an automatic run's connect is refused and its run fails at its claim.");
    const on = automaticStartLine({ on: true, orgs: new Set([ORG15, "00D000000000002"]) });
    assert.equal(on, "Automatic runs are switched on for 2 orgs: one connects, is calibrated and queued, and its run is checked and released or held by code.");
    assert.ok(!on.includes(ORG15));
  });

  test("the worker logs that line once at start", { timeout: 10_000 }, async () => {
    class Unrecoverable extends Error {}
    class FakeWorker {
      on() {
        return this;
      }
      async close() {}
    }
    class FakeQueue {
      async add() {}
      async close() {}
    }
    class FakePool {
      async query(): Promise<never> {
        throw new Error("no database in tests");
      }
      async connect(): Promise<never> {
        throw new Error("no database in tests");
      }
      async end() {}
    }
    const bull = { Worker: FakeWorker, Queue: FakeQueue, UnrecoverableError: Unrecoverable } as unknown as BullModule;
    const modules: WorkerModules = { bull, pg: { Pool: FakePool as unknown as WorkerModules["pg"]["Pool"] } };
    const startLines = async (extra: Record<string, string>) => {
      const lines: string[] = [];
      const code = await workerCommand(env(extra), { load: async () => modules, until: Promise.resolve(), log: (l) => lines.push(l), sweep: async () => [], scanSweep: (async () => {}) as never });
      assert.equal(code, 0);
      return lines.filter((l) => l.startsWith("Automatic runs"));
    };
    assert.deepEqual(await startLines({}), ["Automatic runs are off: HC_AUTOMATIC isn't on, so an automatic run's connect is refused and its run fails at its claim."]);
    assert.deepEqual(await startLines({ HC_AUTOMATIC: "on", HC_AUTOMATIC_ORGS: ORG15 }), ["Automatic runs are switched on for 1 org: one connects, is calibrated and queued, and its run is checked and released or held by code."]);
  });
});

describe("an automatic run job", () => {
  const job = { kind: "run" as const, runId: RUN_ID };
  const cases: Array<[string, Parameters<typeof baseDeps>[0], string]> = [
    ["with the switch absent", {}, AUTOMATIC_SWITCHED_OFF],
    ["with the switch off and the org listed", { automatic: { on: false, orgs: new Set([ORG15]) } }, AUTOMATIC_SWITCHED_OFF],
    ["with the switch on and the org not listed", { automatic: { on: true, orgs: new Set(["00D000000000002"]) } }, AUTOMATIC_ORG_NOT_LISTED],
  ];
  for (const [name, overrides, reason] of cases) {
    test(`fails at its claim ${name}, with one plain reason and nothing read`, async () => {
      const transport = new ReplayTransport(loadRecording());
      const deps = baseDeps({ dataTransport: transport, ...overrides });
      deps.store.addRun(runInput({ run: consoleRun({ reviewMode: "automatic" }) }));
      await deps.tokens.save(syntheticConnection());
      const outcome = await createJobHandler(deps)(job, { final: false });
      assert.deepEqual(outcome, { outcome: "failed", detail: reason });
      const stored = deps.store.runs.get(RUN_ID)!;
      assert.equal(stored.run.status, "failed");
      assert.equal(stored.output, null);
      assert.equal(transport.requests.length, 0, "no Salesforce call");
    });
  }

  test("a consultant-led run with the switch on is untouched by it", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), automatic: { on: true, orgs: new Set() } });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    const outcome = await createJobHandler(deps)(job, { final: false });
    assert.equal(outcome.outcome, "done", outcome.detail);
    const stored = deps.store.runs.get(RUN_ID)!;
    assert.equal(stored.run.reviewMode, "consultant");
    // The sections file the job wrote, as a stored run's, parses, and fits a consultant-led run.
    const written = JSON.parse(readFileSync(join(stored.runDir!, "report-sections.json"), "utf8")) as Array<{ sections: unknown[] }>;
    const sections = written.flatMap((o) => o.sections).map((x) => ReportSection.parse(x));
    assert.ok(sections.length > 10 && SectionsOfRun.safeParse({ reviewMode: "consultant", sections }).success);
    assert.ok(stored.output!.sections.every((x) => x.status !== "checked"));
  });
});

/** Every section approved, as the console holds a consultant-led run's at release. */
function approvedRun(): ApprovedRun & { sections: ReportSection[]; versions: SectionVersion[] } {
  const report = buildReport(loadRun(recorded).input);
  const sections: ReportSection[] = [];
  const versions: SectionVersion[] = [];
  for (const built of Object.values(report.outputs)) {
    for (const s of built.sections) {
      sections.push({ ...s, status: "approved", approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } });
      versions.push(built.versions.find((v) => v.id === s.currentVersionId)!);
    }
  }
  return { intake: INTAKE, sections, versions, overrides: [] };
}

/** The recorded folder, copied, with its run.json's review mode set. */
function folderAs(reviewMode: "consultant" | "automatic"): string {
  const dir = join(temp(), "run");
  mkdirSync(dir, { recursive: true });
  for (const file of ["run.json", "calibration.json", "findings.json", "metrics.json", "manual-steps.json", "scores.json", "pseudonym-map.json", "intake.json", "action-lists.json"]) {
    try {
      writeFileSync(join(dir, file), readFileSync(join(recorded, file)));
    } catch {
      // Not every run writes every file.
    }
  }
  const run = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Record<string, unknown>;
  writeFileSync(join(dir, "run.json"), JSON.stringify({ ...run, reviewMode }));
  return dir;
}

describe("the readers", () => {
  test("the released documents refuse an automatic run, and a checked section on a consultant-led one", () => {
    assert.throws(() => approvedReport(folderAs("automatic"), approvedRun()), new RegExp(AUTOMATIC_DOCUMENTS_NOT_BUILT.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
    const approved = approvedRun();
    const checked = { ...approved, sections: approved.sections.map((s, i) => (i === 0 ? { ...s, status: "checked" as const, approval: null } : s)) };
    assert.throws(() => approvedReport(folderAs("consultant"), checked), new RegExp(CHECKED_OUTSIDE_AUTOMATIC));
    assert.doesNotThrow(() => approvedReport(folderAs("consultant"), approved));
  });

  test("a documents job for an automatic run fails with a plain refusal, as does one with a checked section on a consultant-led run", async () => {
    const make = async (input: Partial<DocumentsJobInput>) => {
      const deps = baseDeps({ drive: null, renderReleased: async () => assert.fail("nothing is printed") });
      const { sections, versions } = approvedRun();
      const full: DocumentsJobInput = { run: consoleRun({ status: "released" }), intake: INTAKE, sections, versions, overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [], ...input };
      mkdirSync(join(deps.runsDir, full.run.id), { recursive: true });
      deps.store.addDocumentsJob(full);
      return processDocumentsJob(RUN_ID, deps, { final: true });
    };
    assert.deepEqual(await make({ run: consoleRun({ status: "released", reviewMode: "automatic" }) }), { outcome: "failed", detail: AUTOMATIC_DOCUMENTS_NOT_BUILT });
    const { sections } = approvedRun();
    const checked = sections.map((s, i) => (i === 0 ? { ...s, status: "checked" as const, approval: null } : s));
    assert.deepEqual(await make({ sections: checked }), { outcome: "failed", detail: CHECKED_OUTSIDE_AUTOMATIC });
  });

  test("the digest says an automatic run wasn't reviewed, and a consultant-led check still was", () => {
    const loaded = loadRun(recorded);
    const digest = (reviewMode: "consultant" | "automatic") =>
      buildDigest({ report: buildReport({ ...loaded.input, run: { ...loaded.input.run, reviewMode } }), pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-23T09:00:00.000Z" });
    assert.equal(digest("consultant").reviewed, true);
    assert.equal(digest("automatic").reviewed, false);
  });

  test("the documents print an automatic run's approved sections as a draft and its checked ones as released (DECISIONS.md 693), and a consultant-led run's approved ones as before", () => {
    const loaded = loadRun(recorded);
    const page = (reviewMode: "consultant" | "automatic", status: "approved" | "checked") => {
      const report = buildReport({ ...loaded.input, run: { ...loaded.input.run, reviewMode } });
      const built = report.outputs.executive_summary;
      built.sections = built.sections.map((s) => ({ ...s, status, approval: status === "approved" ? { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } : null }));
      return executiveHtml(report, loaded.pseudonyms);
    };
    assert.ok(!page("consultant", "approved").includes("Draft, not approved for release"));
    assert.ok(page("automatic", "approved").includes("Draft, not approved for release"));
    // Released by code once every section is checked (693): before, nothing an automatic run held could be released.
    assert.ok(!page("automatic", "checked").includes("Draft, not approved for release"));
    assert.ok(page("consultant", "checked").includes("Draft, not approved for release"));
    assert.ok(!page("automatic", "checked").includes("consultant@example.com"));
  });

  test("a calibration read from the org's settings is printed as that, in the raw notes and the assumptions section, never as a person's confirmation", () => {
    const loaded = loadRun(recorded);
    const calibration = { ...loaded.input.calibration, source: "org_settings" as const };
    const notes = renderRawNotes({ ...loaded.input, calibration, pseudonyms: loaded.pseudonyms });
    assert.ok(notes.includes("- Calibration read from the org's own settings on"));
    assert.ok(!notes.includes("Calibration confirmed by"));
    const report = buildReport({ ...loaded.input, calibration });
    const assumptions = report.outputs.raw_notes.sections.find((s) => s.key === "assumptions")!;
    const body = currentBody(report.outputs.raw_notes, assumptions.key);
    assert.ok(body.includes("with no person's confirmation"));
    assert.ok(!body.includes("Calibration confirmed by"));
    // A confirmed one prints as before.
    assert.ok(renderRawNotes({ ...loaded.input, pseudonyms: loaded.pseudonyms }).includes(`- Calibration confirmed by ${loaded.input.calibration.confirmedBy} on`));
  });
});
