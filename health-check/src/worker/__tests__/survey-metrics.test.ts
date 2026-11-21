import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { buildDigest } from "../../digest/build.ts";
import { ALL_MODULES } from "../../domains/index.ts";
import { buildReport, surveyMetricsOf } from "../../narrative/sections.ts";
import { buildPulseResult } from "../../pulse/result.ts";
import { keepSurveyMetrics, loadRun, SURVEY_FILE } from "../../render/render-run.ts";
import type { Metric, Run } from "../../schemas/index.ts";
import { MIN_RESPONSES, SURVEY, SURVEY_METRIC_IDS } from "../../survey/summary.ts";
import { withoutComments } from "../../survey/questions.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { plantRecheck } from "../../testing/recheck.ts";
import { loadAnswers, loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { SYNTHETIC_COMMENT, syntheticSurvey } from "../../testing/survey.ts";
import { processDocumentsJob } from "../documents-job.ts";
import type { DocumentDrive, DriveItem } from "../drive.ts";
import { PostgresWorkerStore, SURVEY_METRICS_DELETE_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { processRunJob } from "../run-job.ts";
import { progressOf, type DocumentsJobInput, type RunJobInput } from "../types.ts";
import { baseDeps, consoleRun, INTAKE, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// Decision 645: a run with a team survey holds the survey's metrics among its
// stored metrics, made by the code that makes them for its report, so its
// sections cite only what it holds. They change no score or count, hold no
// figure from fewer than five answers, a later delivery replaces them, and a
// console that refuses one never costs the run its results.

const survey = (m: Metric[]) => m.filter((x) => SURVEY_METRIC_IDS.includes(x.id));
const org = (m: Metric[]) => m.filter((x) => !SURVEY_METRIC_IDS.includes(x.id));

async function deliver(input: RunJobInput, deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) })) {
  if (!deps.store.runs.has(RUN_ID)) {
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(input);
  } else deps.store.runs.get(RUN_ID)!.survey = input.survey ?? null;
  const outcome = await processRunJob(RUN_ID, deps, { final: true });
  assert.equal(outcome.outcome, "done");
  const run = deps.store.runs.get(RUN_ID)!;
  return { deps, run, stored: deps.store.storedMetrics(RUN_ID), dir: run.runDir! };
}

const none = await deliver(runInput());
const below = await deliver(runInput({ survey: syntheticSurvey(MIN_RESPONSES - 1) }));
const atMin = await deliver(runInput({ survey: syntheticSurvey(MIN_RESPONSES) }));
const twelve = await deliver(runInput({ survey: syntheticSurvey(12) }));

describe("a surveyed run's stored metrics (decision 645)", () => {
  test("hold every survey metric its sections cite, equal to the figures its documents print, and metrics.json holds the same", () => {
    for (const { run, stored, dir, deps } of [below, atMin, twelve]) {
      const ids = new Set(stored.map((m) => m.id));
      const cited = run.output!.sections.flatMap((s) => s.metricIds);
      assert.ok(cited.some((id) => SURVEY_METRIC_IDS.includes(id)), "the survey's sections cite its metrics");
      assert.deepEqual(cited.filter((id) => !ids.has(id)), [], "nothing cited that the run doesn't hold");
      assert.deepEqual(deps.lines.filter((l) => l.includes("not in the run")), []);
      // The documents are made from the folder: the report they print, and its survey view.
      const printed = buildReport(loadRun(dir).input);
      assert.deepEqual(survey(stored), printed.survey!.metrics);
      assert.deepEqual(survey(stored), survey(printed.metrics));
      assert.deepEqual(survey(JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Metric[]), survey(stored));
    }
    // The figures the pages print are these rows' own.
    const shown = buildReport(loadRun(twelve.dir).input).survey!;
    const value = (id: string) => twelve.stored.find((m) => m.id === id)!;
    const n = value(SURVEY.responses);
    assert.equal(n.status === "measured" && n.value, 12);
    assert.ok(shown.paragraph.startsWith(`${12} people in your team answered our survey.`));
    const ease = value(SURVEY.ease);
    assert.ok(ease.status === "measured" && shown.paragraph.includes(`how easy Salesforce is ${ease.value} out of 5`));
  });

  test("are the ones the digest carries, through its own metrics", () => {
    const loaded = loadRun(twelve.dir);
    const digest = buildDigest({ report: buildReport(loaded.input), pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
    assert.deepEqual(digest.metrics.map((m) => m.id).filter((id) => SURVEY_METRIC_IDS.includes(id)), survey(twelve.stored).map((m) => m.id));
  });

  test("change no score, overall score, finding, org metric or complexity, with or without a survey", () => {
    for (const other of [below, atMin, twelve]) {
      assert.deepEqual(other.run.output!.scores, none.run.output!.scores);
      assert.deepEqual([other.run.overallScore, other.run.overallGrade], [none.run.overallScore, none.run.overallGrade]);
      assert.deepEqual(other.run.output!.findings, none.run.output!.findings, "and so every count of checks measured and not");
      assert.deepEqual(other.run.output!.metrics, none.run.output!.metrics, "the org's own metrics are the run's without a survey");
      assert.deepEqual(org(other.stored), none.stored);
      assert.deepEqual(other.run.output!.complexity, none.run.output!.complexity);
    }
    assert.deepEqual(survey(none.stored), []);
    assert.equal(none.run.output!.surveyMetrics!.length, 0);
  });

  test("change no re-check's comparison or pulse's result: the same surveyed re-check with them stored and without", async () => {
    const { dir: recorded } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
    const folder = (name: string, store: boolean): string => {
      const dir = join(tempDir(), name);
      plantRecheck(recorded, dir);
      writeFileSync(join(dir, SURVEY_FILE), JSON.stringify(withoutComments(syntheticSurvey(12))));
      if (store) keepSurveyMetrics(dir, surveyMetricsOf(buildReport(loadRun(dir).input)));
      return dir;
    };
    const [without, stored] = [folder("recheck", false), folder("recheck", true)];
    assert.ok(survey(JSON.parse(readFileSync(join(stored, "metrics.json"), "utf8")) as Metric[]).length > 0);
    const [a, b] = [buildReport(loadRun(without).input), buildReport(loadRun(stored).input)];
    assert.ok(a.progress && b.progress);
    assert.deepEqual(b.progress, a.progress);
    assert.deepEqual([b.overall, b.value, b.outputs], [a.overall, a.value, a.outputs]);
    const pulse = (dir: string) => {
      const run = JSON.parse(readFileSync(join(dir, "run.json"), "utf8")) as Run;
      writeFileSync(join(dir, "run.json"), JSON.stringify({ ...run, product: "pulse" }));
      return buildPulseResult(buildReport(loadRun(dir).input));
    };
    assert.deepEqual(pulse(stored), pulse(without));
    // And the areas a re-check compares are the same as a re-check's with no survey at all.
    const none = join(tempDir(), "recheck");
    plantRecheck(recorded, none);
    assert.deepEqual(b.progress.areas, buildReport(loadRun(none).input).progress!.areas);
  });
});

describe("the survey's privacy in the stored rows (decision 645, rule 7)", () => {
  test(`under ${MIN_RESPONSES} answers, one row saying so, with no figure at all, not even the count`, () => {
    const rows = survey(below.stored);
    assert.equal(rows.length, 1);
    const [only] = rows;
    assert.equal(only!.id, SURVEY.responses);
    assert.equal(only!.status, "not_measurable");
    assert.match(only!.status === "not_measurable" ? only!.reason : "", /^Fewer than 5 people answered the team survey/);
    assert.ok(!("value" in only!) && !("n" in only!) && !("segments" in only!));
    assert.deepEqual(only!.evidence.map((e) => e.count), [null]);
    // The only digit in the row is the five of the reason.
    assert.deepEqual(JSON.stringify(only).replace(/"capturedAt":"[^"]*"/, "").match(/\d+/g), ["5"]);
  });

  test(`at ${MIN_RESPONSES} and above, figures from the answers and never a comment, a response id or when it came in`, () => {
    for (const [{ stored }, n] of [[atMin, MIN_RESPONSES], [twelve, 12]] as const) {
      const rows = survey(stored);
      assert.deepEqual(rows.map((m) => m.id), SURVEY_METRIC_IDS);
      assert.ok(rows.every((m) => m.status === "measured" && m.n === n && m.domain === "survey"));
      const text = JSON.stringify(rows);
      assert.ok(!text.includes(SYNTHETIC_COMMENT));
      assert.doesNotMatch(text, /sr-\d{10}|2026-11-0\dT09/);
      // A role is shown on its own only with five or more answers.
      for (const m of rows) for (const s of m.status === "measured" ? m.segments.filter((x) => x.dimension === "role") : []) assert.ok(s.n >= MIN_RESPONSES, `${m.id} ${s.key}`);
    }
  });
});

describe("a later write replaces the survey's metrics (decision 645)", () => {
  test("a later run job delivery with more answers replaces them, each once", async () => {
    const first = await deliver(runInput({ survey: syntheticSurvey(MIN_RESPONSES - 1) }));
    assert.equal(survey(first.stored).length, 1);
    const more = await deliver(runInput({ survey: syntheticSurvey(9) }), first.deps);
    const rows = survey(more.stored);
    assert.deepEqual(rows.map((m) => m.id), SURVEY_METRIC_IDS, "each once");
    assert.equal(rows[0]!.status === "measured" && rows[0]!.value, 9);
    assert.deepEqual(survey(JSON.parse(readFileSync(join(more.dir, "metrics.json"), "utf8")) as Metric[]), rows);
    // With none from the console, the sections are built from the survey the folder keeps (147), and the rows follow them.
    const kept = await deliver(runInput({ survey: null }), first.deps);
    assert.deepEqual(survey(kept.stored), survey(buildReport(loadRun(kept.dir).input).metrics));
    assert.deepEqual(survey(kept.stored).map((m) => m.id), SURVEY_METRIC_IDS, "each once");
  });

  test("the documents job stores the printed report's, replacing the run's, never the console's answers as they stand now, and keeps them in metrics.json", async () => {
    const { deps, dir } = await deliver(runInput({ survey: syntheticSurvey(MIN_RESPONSES - 1) }));
    const printed = survey(twelve.stored);
    const job: DocumentsJobInput = { run: consoleRun({ status: "released", customerRef: "SYN-0001" }), intake: { ...INTAKE, customerName: "Synthetic Co" }, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [], survey: syntheticSurvey(20) };
    deps.store.addDocumentsJob(job);
    deps.drive = new NoDrive();
    deps.renderReleased = async (_dir, _approved, out) => {
      mkdirSync(out, { recursive: true });
      const files = Object.fromEntries(["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"].map((k) => [k, join(out, `${k}.bin`)]));
      for (const f of Object.values(files)) writeFileSync(f, "synthetic");
      return { files: files as never, warnings: [], held: [], surveyMetrics: printed };
    };
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    assert.deepEqual(survey(deps.store.storedMetrics(RUN_ID)), printed);
    assert.deepEqual(org(deps.store.storedMetrics(RUN_ID)), none.stored);
    assert.deepEqual(survey(JSON.parse(readFileSync(join(dir, "metrics.json"), "utf8")) as Metric[]), printed);

    // Again, with the console moving the documents' row on before the save: the folder is left as it was.
    const folderBefore = readFileSync(join(dir, "metrics.json"), "utf8");
    deps.store.addDocumentsJob(job);
    deps.renderReleased = async (_dir, _approved, out) => {
      mkdirSync(out, { recursive: true });
      const files = Object.fromEntries(["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"].map((k) => [k, join(out, `${k}.bin`)]));
      for (const f of Object.values(files)) writeFileSync(f, "synthetic");
      return { files: files as never, warnings: [], held: [], surveyMetrics: survey(below.stored) };
    };
    deps.store.saveDocuments = async () => false;
    await processDocumentsJob(RUN_ID, deps, { final: true });
    assert.equal(readFileSync(join(dir, "metrics.json"), "utf8"), folderBefore);
  });
});

/** A survey whose every answer band and blocker is given by several people, so the documents print a full set. */
const shownSurvey = (n: number) => syntheticSurvey(n, (_a, i) => ({ recommend: i < 6 ? 10 : 3, blockers: i < 6 ? ["data_wrong"] : ["nothing"], comment: i < 6 ? "Synthetic." : null }));

describe("after the documents, the survey's rows and the digest describe the same answers (decisions 645 and 147)", () => {
  test("a real make stores the run job's own rows, and a later delivery with one more answer changes neither the rows nor the digest", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    const first = await deliver(runInput({ survey: shownSurvey(12) }), deps);
    const fromRunJob = survey(first.stored);
    assert.ok(fromRunJob.length === SURVEY_METRIC_IDS.length && fromRunJob.every((m) => m.status === "measured"), "every figure shown");
    // The console releases it, every section approved as drafted.
    const run = deps.store.runs.get(RUN_ID)!;
    run.run = { ...run.run, status: "released" };
    const sections = run.output!.sections.map((sec) => ({ ...sec, status: "approved" as const, approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } }));
    deps.store.addDocumentsJob({ run: consoleRun({ status: "released", customerRef: "SYN-0001" }), intake: { ...INTAKE, customerName: "Synthetic Co" }, sections, versions: run.output!.versions, overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [], survey: shownSurvey(13) });
    deps.drive = new NoDrive();
    // The real make, from the folder, as the worker runs it.
    assert.equal((await processDocumentsJob(RUN_ID, deps, { final: true })).outcome, "done");
    const afterMake = deps.store.storedMetrics(RUN_ID);
    assert.deepEqual(survey(afterMake), fromRunJob, "the printed report's are the run job's, for the same survey");
    assert.deepEqual(org(afterMake), org(first.stored), "the run's own rows untouched");
    const digest = deps.store.getRunDigest(RUN_ID)!;
    assert.ok(digest, "the make wrote a digest");
    const digestSurvey = () => digest.metrics.filter((m) => SURVEY_METRIC_IDS.includes(m.id));
    assert.deepEqual(digestSurvey().map((m) => [m.id, m.status === "measured" ? m.value : null]), fromRunJob.map((m) => [m.id, m.status === "measured" ? m.value : null]));

    // One more answer, and the console sends the run job again: the released run's save is refused, so nothing moves.
    run.survey = shownSurvey(13);
    const again = await processRunJob(RUN_ID, deps, { final: true });
    assert.deepEqual(survey(deps.store.storedMetrics(RUN_ID)), survey(afterMake), "the survey's rows still describe the documents' answers");
    assert.deepEqual(deps.store.getRunDigest(RUN_ID), digest, "and so does the digest");
    assert.deepEqual(deps.store.storedMetrics(RUN_ID), afterMake);
    assert.equal(again.outcome, "skipped");
  });
});

/** Just enough of Drive for one make. */
class NoDrive implements DocumentDrive {
  readonly rootFolderId = "fld-root";
  #n = 0;
  #item = (name: string): DriveItem => ({ id: `fil-${String(++this.#n).padStart(4, "0")}`, name, url: `https://drive.example.com/${this.#n}` });
  async folder() {
    return null;
  }
  async folders() {
    return [];
  }
  async files() {
    return [];
  }
  async createFolder(_parent: string, name: string) {
    return this.#item(name);
  }
  async putFile(_parent: string, name: string) {
    return this.#item(name);
  }
  async moveFile() {}
}

// ─── The Postgres store, with a console that refuses the survey's rows ───────

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;

class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [(text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "extracting" }], rowCount: 1 } : undefined)];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}

const PLANTED = "planted-value-0001";
const refusing = (code: string): Handler => (text, values) => {
  if (text.startsWith(`INSERT INTO "HealthCheckMetric"`) && String(values[0]).startsWith("survey.")) throw Object.assign(new Error(`${PLANTED} broke the check`), { code });
  return undefined;
};
const metricInserts = (pool: FakePool) => pool.statements.filter((s) => s.text.startsWith(`INSERT INTO "HealthCheckMetric"`)).map((s) => String(s.values[0]));

describe("a console that refuses a survey metric never costs the run its results (decision 645)", () => {
  const output = () => ({ progress: progressOf(consoleRun()), runDir: "/runs/x", findings: [], metrics: org(twelve.stored), surveyMetrics: survey(twelve.stored), scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null });

  test("the run's own metrics are written first and the survey's in a savepoint of their own", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool, { log: () => {} }).saveRunOutput(RUN_ID, output());
    const texts = pool.texts();
    assert.deepEqual(metricInserts(pool), twelve.stored.map((m) => m.id));
    const at = texts.indexOf("SAVEPOINT survey_metrics");
    assert.ok(at > 0 && texts.slice(0, at).filter((t) => t.startsWith(`INSERT INTO "HealthCheckMetric"`)).length === org(twelve.stored).length);
    assert.ok(texts.includes("RELEASE SAVEPOINT survey_metrics"));
    assert.equal(texts.at(-1), "COMMIT");
  });

  for (const code of ["23514", "42703", "22P02"]) {
    test(`a ${code} on a survey row goes back to its savepoint, says the run, the table and the code alone, and the results commit`, async () => {
      const pool = new FakePool();
      pool.handlers.unshift(refusing(code));
      const logs: string[] = [];
      await new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }).saveRunOutput(RUN_ID, output());
      const texts = pool.texts();
      assert.ok(texts.includes("ROLLBACK TO SAVEPOINT survey_metrics"));
      assert.ok(!texts.includes("ROLLBACK"));
      assert.equal(texts.at(-1), "COMMIT");
      assert.deepEqual(logs, [`Run ${RUN_ID}: the console's HealthCheckMetric table refused the team survey's metrics with SQLSTATE ${code}, so they aren't saved; the run's own are.`]);
      assert.ok(!logs.join("").includes(PLANTED));
    });
  }

  test("a lost connection still rolls the whole save back, to be tried again", async () => {
    const pool = new FakePool();
    pool.handlers.unshift(refusing("08006"));
    await assert.rejects(new PostgresWorkerStore(pool, { log: () => {} }).saveRunOutput(RUN_ID, output()));
    assert.ok(pool.texts().includes("ROLLBACK"));
    assert.ok(!pool.texts().includes("COMMIT"));
  });

  test("the documents' delete is exactly the survey's eight ids for the run, and leaves every org row in a table holding both", async () => {
    assert.equal(SURVEY_METRICS_DELETE_SQL, `DELETE FROM "HealthCheckMetric" WHERE "runId" = $1 AND "id" = ANY($2::text[])`);
    assert.deepEqual(SURVEY_METRIC_IDS.length, 8);
    assert.ok(SURVEY_METRIC_IDS.every((id) => id.startsWith("survey.")) && org(twelve.stored).every((m) => !m.id.startsWith("survey.")));
    // A table with the run's rows, org and survey, and another run's: the statements as written, run against it.
    const table: Array<{ runId: string; id: string }> = [...twelve.stored.map((m) => ({ runId: RUN_ID, id: m.id })), { runId: "hc-2026-11-01-00000002", id: SURVEY.responses }];
    const pool = new FakePool();
    pool.handlers.unshift((text, values) => {
      const del = /^DELETE FROM "HealthCheckMetric" WHERE "runId" = \$1( AND "id" = ANY\(\$2::text\[\]\))?$/.exec(text);
      if (del) {
        const before = table.length;
        for (let i = table.length - 1; i >= 0; i--) if (table[i]!.runId === values[0] && (!del[1] || (values[1] as string[]).includes(table[i]!.id))) table.splice(i, 1);
        return { rows: [], rowCount: before - table.length };
      }
      if (text.startsWith(`DELETE FROM "HealthCheckMetric"`)) throw new Error(`A statement the table doesn't know: ${text}`);
      if (text.startsWith(`INSERT INTO "HealthCheckMetric"`)) table.push({ runId: String(values[1]), id: String(values[0]) });
      return undefined;
    });
    const made = { at: "2026-11-21T09:00:00.000Z", clientName: "Synthetic Co", folderName: "Synthetic Co", driveFolderId: "fld-1", driveFolderUrl: "https://drive.example.com/1", files: [], warnings: [] };
    assert.equal(await new PostgresWorkerStore(pool, { log: () => {} }).saveDocuments(RUN_ID, made, null, survey(twelve.stored)), true);
    const ours = table.filter((r) => r.runId === RUN_ID).map((r) => r.id);
    assert.deepEqual(ours.filter((id) => !id.startsWith("survey.")), org(twelve.stored).map((m) => m.id), "every org row still there");
    assert.deepEqual(ours.filter((id) => id.startsWith("survey.")).sort(), survey(twelve.stored).map((m) => m.id).sort(), "each survey row once");
    assert.ok(table.some((r) => r.runId !== RUN_ID), "another run's rows untouched");
  });

  test("the documents' write replaces the run's survey rows after marking the row done, and a refusal costs those rows alone", async () => {
    const made = { at: "2026-11-21T09:00:00.000Z", clientName: "Synthetic Co", folderName: "Synthetic Co", driveFolderId: "fld-1", driveFolderUrl: "https://drive.example.com/1", files: [], warnings: [] };
    const pool = new FakePool();
    await new PostgresWorkerStore(pool, { log: () => {} }).saveDocuments(RUN_ID, made, null, survey(twelve.stored));
    const texts = pool.texts();
    const done = texts.findIndex((t) => t.startsWith(`UPDATE "HealthCheckDocuments"`));
    const del = pool.statements.findIndex((s) => s.text.startsWith(`DELETE FROM "HealthCheckMetric"`));
    assert.ok(done >= 0 && del > done);
    assert.deepEqual(pool.statements[del]!.values, [RUN_ID, SURVEY_METRIC_IDS], "the survey's rows only, never the run's own");
    assert.deepEqual(metricInserts(pool), SURVEY_METRIC_IDS);

    const refused = new FakePool();
    refused.handlers.unshift(refusing("23514"));
    const logs: string[] = [];
    await new PostgresWorkerStore(refused, { log: (l) => void logs.push(l) }).saveDocuments(RUN_ID, made, null, survey(twelve.stored));
    assert.ok(refused.texts().includes("ROLLBACK TO SAVEPOINT survey_metrics"));
    assert.equal(refused.texts().at(-1), "COMMIT");
    assert.equal(logs.length, 1);

    // A row the console moved on meanwhile: nothing of the survey's is touched.
    const moved = new FakePool();
    moved.handlers.unshift((text) => (text.startsWith(`UPDATE "HealthCheckDocuments"`) ? { rows: [], rowCount: 0 } : undefined));
    await new PostgresWorkerStore(moved, { log: () => {} }).saveDocuments(RUN_ID, made, null, survey(twelve.stored));
    assert.deepEqual(metricInserts(moved), []);
    assert.ok(!moved.texts().some((t) => t.includes(`"HealthCheckMetric"`)));
  });
});
