import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers, loadCalibration } from "../../testing/replay.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  CONNECT_LINK,
  FINDING,
  insertSql,
  intakeFromRow,
  METRIC,
  PostgresWorkerStore,
  RUN,
  selectSql,
  type SqlClient,
  type SqlPool,
  type SqlResult,
} from "../postgres-store.ts";
import { progressOf, RunClosedError } from "../types.ts";
import { consoleRun, INTAKE } from "./helpers.ts";

// The Postgres store has never met a database (it's marked unverified). These
// tests hold its SQL to the console's columns and its transactions to their
// shape, through a pool that records every statement.

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;

class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [];
  released = 0;
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => void this.released++ };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}

const RUN_ROW = {
  ...consoleRun(),
  analysisFrom: "2024-10-31",
  analysisTo: "2026-10-31",
  connectionName: "synthetic-co",
  analysisWindow: undefined,
  steps: undefined,
};
delete (RUN_ROW as Record<string, unknown>).analysisWindow;
delete (RUN_ROW as Record<string, unknown>).steps;

describe("the intake the worker gives the engine", () => {
  const run = consoleRun();
  const consultant = { statedProblems: "Forecast calls feel unreliable.\nDeals stall late.", customerName: null, linkedinUrl: null, industry: "retail", headcountBand: null };
  // Made up, as every fixture is: the shape the console's connect page stores.
  const answers = {
    respondentName: "Sam Admin",
    companyName: "Synthetic Co Ltd",
    industry: "technology",
    headcountBand: "51-200",
    currency: "GBP",
    fiscalYearStartMonth: 4,
    goals: "Grow new business by a fifth.",
    whyNow: "A new sales director starts in January.",
    problems: "- Pipeline reports don't match the spreadsheet.\n- Reps don't log calls.",
    targetSource: "spreadsheet",
  };

  test("without the customer's answers, is the consultant's, one problem per line", () => {
    const intake = intakeFromRow({ ...consultant, customerAnswers: null }, run);
    assert.deepEqual(intake.statedProblems.map((p) => [p.id, p.statement, p.raisedBy]), [["p1", "Forecast calls feel unreliable.", undefined], ["p2", "Deals stall late.", undefined]]);
    assert.equal(intake.summary, consultant.statedProblems);
  });

  test("adds the customer's problems after the consultant's, marked as theirs, and their goals to the summary", () => {
    const intake = intakeFromRow({ ...consultant, customerAnswers: answers }, run);
    assert.deepEqual(intake.statedProblems.map((p) => [p.id, p.statement, p.raisedBy]), [
      ["p1", "Forecast calls feel unreliable.", undefined],
      ["p2", "Deals stall late.", undefined],
      ["p3", "Pipeline reports don't match the spreadsheet.", "customer"],
      ["p4", "Reps don't log calls.", "customer"],
    ]);
    assert.match(intake.summary, /^Forecast calls feel unreliable\.\nDeals stall late\.\n\nThe customer's goals for the next 12 months:\nGrow new business by a fifth\.\n\nWhy now: A new sales director starts in January\.$/);
  });

  test("the customer's company details fill only what the consultant left blank", () => {
    const intake = intakeFromRow({ ...consultant, customerAnswers: answers }, run);
    assert.equal(intake.customerName, "Synthetic Co Ltd");
    assert.equal(intake.headcountBand, "51-200");
    assert.equal(intake.industry, "retail", "the consultant's industry stands");
  });

  test("answers it can't read are left out, rather than stopping the run", () => {
    const intake = intakeFromRow({ ...consultant, customerAnswers: { goals: "" } }, run);
    assert.equal(intake.statedProblems.length, 2);
    assert.equal(intake.customerName, undefined);
  });

  test("what Salesforce costs them, and the consents, come from the consultant's columns first, then the customer's answers", () => {
    const intake = intakeFromRow({ ...consultant, customerAnswers: { ...answers, salesforceSpendAnnual: 90000, hourlyCost: 40 }, salesforceSpendAnnual: 120000, hourlyCost: null, namedRepConsent: true, caseStudyConsent: false }, run);
    assert.equal(intake.salesforceSpendAnnual, 120000);
    assert.equal(intake.hourlyCost, 40);
    assert.equal(intake.namedRepConsent, true);
    assert.equal(intake.caseStudyConsent, false);
    const blank = intakeFromRow({ ...consultant, customerAnswers: null }, run);
    assert.equal(blank.salesforceSpendAnnual, undefined);
    assert.equal(blank.namedRepConsent, undefined);
  });

  test("when the customer never replied, the answers confirmed from the call fill in, each read on its own", () => {
    const callAnswers = {
      answers: { goals: "Halve the time reps spend on admin.", problems: "Nobody trusts the forecast.", headcountBand: "51-200", industry: "not an industry", hourlyCost: 35 },
      lines: {},
      confirmed: {},
    };
    const intake = intakeFromRow({ ...consultant, industry: null, customerAnswers: null, callAnswers }, run);
    assert.match(intake.summary, /The customer's goals for the next 12 months:\nHalve the time reps spend on admin\./);
    assert.deepEqual(intake.statedProblems.at(-1), { id: "p3", statement: "Nobody trusts the forecast.", raisedBy: "customer", findingIds: [] });
    assert.equal(intake.headcountBand, "51-200");
    assert.equal(intake.industry, undefined, "an answer that doesn't read is left out, and the rest still count");
    assert.equal(intake.hourlyCost, 35);
  });

  test("once the customer has replied, the call's answers aren't read again, because the console folded them in", () => {
    const callAnswers = { answers: { problems: "Said on the call." }, lines: {}, confirmed: {} };
    const intake = intakeFromRow({ ...consultant, customerAnswers: answers, callAnswers }, run);
    assert.ok(!intake.statedProblems.some((p) => p.statement === "Said on the call."));
  });
});

describe("SQL building", () => {
  test("reads timestamps as ISO UTC text and dates as text, whatever the process's time zone", () => {
    const sql = selectSql("HealthCheckRun", RUN, `"id" = $1`);
    assert.match(sql, /to_char\("asOf", 'YYYY-MM-DD"T"HH24:MI:SS\.MS"Z"'\) AS "asOf"/);
    assert.match(sql, /"analysisFrom"::text AS "analysisFrom"/);
  });

  test("casts each value to its column, and sends JSON arrays as JSON, not Postgres arrays", () => {
    const { text, values } = insertSql("HealthCheckMetric", METRIC, { id: "m1", runId: "r", evidence: [{ a: 1 }], exclusions: ["x"], dateRange: null });
    assert.match(text, /\$8::jsonb/);
    assert.match(text, /\$9::text\[\]/);
    assert.equal(values[7], JSON.stringify([{ a: 1 }]));
    assert.deepEqual(values[8], ["x"]);
    assert.equal(values[12], null);
  });

  test("refuses a field the table doesn't have, so a schema change can't drop data quietly", () => {
    assert.throws(() => insertSql("HealthCheckFinding", FINDING, { id: "f", surprise: 1 }), /no column surprise/);
  });
});

describe("PostgresWorkerStore", () => {
  test("loads a run job: the run, its steps in order, the engine's intake, calibration and answers", async () => {
    const pool = new FakePool();
    const calibration = loadCalibration();
    const answer = loadAnswers()[0]!;
    pool.handlers.push((text) => {
      if (text.includes(`FROM "HealthCheckRun" WHERE`)) return { rows: [RUN_ROW], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckRunStep"`)) return { rows: [{ runId: "x", id: "extract", version: 1, status: "pending", calls: 0, finishedAt: null, error: null }], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckIntake"`)) return { rows: [{ statedProblems: "- Forecasts are unreliable.\n\n2) Nobody trusts the pipeline", customerName: null, linkedinUrl: null, industry: null, headcountBand: "11-50" }], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckCalibration"`)) return { rows: [{ orgId: null, salesCycleAssumptionDays: null, fiscalYearStartMonth: null, ...calibration }], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckManualAnswer"`)) return { rows: [{ method: null, pastedText: null, screenshotRef: null, prefill: null, skipReason: null, ...answer }], rowCount: 1 };
      return undefined;
    });
    const job = (await new PostgresWorkerStore(pool).loadRunJob("hc-2026-10-31-00000001"))!;
    assert.equal(job.run.id, consoleRun().id);
    assert.deepEqual(job.run.analysisWindow, { from: "2024-10-31", to: "2026-10-31" });
    assert.equal(job.connectionName, "synthetic-co");
    assert.deepEqual(job.intake!.statedProblems.map((p) => p.statement), ["Forecasts are unreliable.", "Nobody trusts the pipeline"]);
    assert.equal(job.intake!.headcountBand, "11-50");
    assert.equal(job.intake!.capturedBy, "consultant@example.com");
    assert.deepEqual(job.calibration, calibration);
    assert.equal(job.answers[0]!.stepKey, answer.stepKey);
    assert.match(pool.texts().find((t) => t.includes("HealthCheckRunStep"))!, /ORDER BY "seq"/);
  });

  test("reads the run's scope, and a run with none is complete", async () => {
    const scopeOf = async (scope: string | null): Promise<string> => {
      const pool = new FakePool();
      pool.handlers.push((text) => (text.includes(`FROM "HealthCheckRun" WHERE`) ? { rows: [{ ...RUN_ROW, scope }], rowCount: 1 } : undefined));
      return (await new PostgresWorkerStore(pool).loadRunJob(RUN_ROW.id))!.run.scope;
    };
    assert.equal(await scopeOf("essentials"), "essentials");
    assert.equal(await scopeOf("complete"), "complete");
    assert.equal(await scopeOf(null), "complete");
    await assert.rejects(scopeOf("everything"));
    assert.match(selectSql("HealthCheckRun", RUN, `"id" = $1`), /"scope"/);
  });

  test("an unknown run is null", async () => {
    assert.equal(await new PostgresWorkerStore(new FakePool()).loadRunJob("nope"), null);
  });

  test("writes a whole engine run in one transaction, replacing what the run held", async () => {
    const { run, dir, output } = await runFromRecording({ runId: "hc-2026-10-31-00000001" });
    writeFileSync(join(dir, "intake.json"), JSON.stringify(INTAKE));
    const { input } = loadRun(dir);
    const outputs = Object.values(buildReport(input).outputs);
    const pool = new FakePool();
    pool.handlers.push((text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "extracting" }], rowCount: 1 } : undefined));
    await new PostgresWorkerStore(pool).saveRunOutput(run.id, {
      progress: progressOf(run),
      runDir: dir,
      findings: output!.findings,
      metrics: output!.metrics,
      scores: input.scores ?? [],
      manualSteps: output!.manualSteps,
      sections: outputs.flatMap((o) => o.sections),
      versions: outputs.flatMap((o) => o.versions),
    });
    const texts = pool.texts();
    assert.equal(texts[0], "BEGIN");
    assert.equal(texts.at(-1), "COMMIT");
    assert.equal(pool.released, 1);
    assert.ok(texts.indexOf(`DELETE FROM "HealthCheckFinding" WHERE "runId" = $1`) < texts.findIndex((t) => t.startsWith(`INSERT INTO "HealthCheckFinding"`)));
    const inserts = (table: string) => texts.filter((t) => t.startsWith(`INSERT INTO "${table}"`)).length;
    assert.equal(inserts("HealthCheckFinding"), output!.findings.length);
    assert.equal(inserts("HealthCheckSectionVersion"), outputs.flatMap((o) => o.versions).length);
    assert.equal(inserts("HealthCheckRunStep"), run.steps.length);
    const update = pool.statements.find((s) => s.text.startsWith(`UPDATE "HealthCheckRun"`))!;
    assert.match(update.text, /"runDir" = \$12/);
    assert.equal(update.values[11], dir);
    assert.doesNotMatch(update.text, /customerRef|createdBy|benchmarkOptIn/, "the console's fields are left alone");
  });

  test("refuses, and rolls back, once the console has the run in review", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "in_review" }], rowCount: 1 } : undefined));
    const store = new PostgresWorkerStore(pool);
    await assert.rejects(store.recordRunFailure("r1", "boom", "2026-11-03T00:00:00.000Z"), RunClosedError);
    assert.deepEqual(pool.texts().slice(-1), ["ROLLBACK"]);
    assert.ok(!pool.texts().some((t) => t.startsWith("UPDATE")));
    assert.equal(pool.released, 1);
  });

  test("records a failure on a 'worker' step, replacing any earlier one", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "extracting" }], rowCount: 1 } : undefined));
    await new PostgresWorkerStore(pool).recordRunFailure("r1", "No calibration", "2026-11-03T00:00:00.000Z");
    const insert = pool.statements.find((s) => s.text.startsWith(`INSERT INTO "HealthCheckRunStep"`))!;
    assert.deepEqual(insert.values, ["r1", "worker", 1, "failed", 0, "2026-11-03T00:00:00.000Z", "No calibration"]);
    assert.ok(pool.texts().includes(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1 AND "id" = 'worker'`));
  });

  test("loads a link with its state and sealed secrets", async () => {
    const pool = new FakePool();
    const link = {
      id: "00000000-0000-4000-8000-000000000001",
      tokenHash: "A".repeat(43),
      customerRef: "SYN-0001",
      connectionName: "acme",
      loginUrl: "https://login.salesforce.com",
      createdBy: "consultant@example.com",
      createdAt: "2026-11-03T20:00:00.000Z",
      expiresAt: "2026-11-06T20:00:00.000Z",
      usedAt: null,
      revokedAt: null,
    };
    pool.handlers.push((text) => (text.startsWith(`SELECT "orgId" FROM "HealthCheckRun"`) ? { rows: [{ orgId: "awaiting-connect" }], rowCount: 1 } : undefined));
    pool.handlers.push((text) => (text.includes(`FROM "HealthCheckConnectLink"`) ? { rows: [{ ...link, state: "pending", sealedPending: "v1.a.b.c", sealedCode: "v1.d.e.f" }], rowCount: 1 } : undefined));
    const job = await new PostgresWorkerStore(pool).loadConnectJob(link.id);
    assert.deepEqual(job, { link, state: "pending", runOrgId: "awaiting-connect", sealedPending: "v1.a.b.c", sealedCode: "v1.d.e.f" });
    assert.deepEqual(Object.keys(CONNECT_LINK).slice(0, 10), Object.keys(link));
  });

  test("settles only a pending link, and clears its sealed secrets", async () => {
    const pool = new FakePool();
    const store = new PostgresWorkerStore(pool);
    await store.markLinkUsed("l1", { at: "2026-11-03T20:00:00.000Z", orgId: "00D000000000001AAA", userId: "005000000000001AAA", note: null });
    // In the same transaction, a run still awaiting its org takes the connected one, and no other run changes.
    const run = pool.statements.find((s) => s.text.startsWith(`UPDATE "HealthCheckRun"`))!;
    assert.match(run.text, /WHERE "id" = \(SELECT "runId" FROM "HealthCheckConnectLink" WHERE "id" = \$1\) AND "orgId" = \$3$/);
    assert.deepEqual(run.values, ["l1", "00D000000000001AAA", "awaiting-connect"]);
    assert.deepEqual(pool.texts().filter((t) => t === "BEGIN" || t === "COMMIT"), ["BEGIN", "COMMIT"]);
    await store.refuseLink("l2", "The user can write", "2026-11-03T20:00:00.000Z");
    for (const s of pool.statements.filter((s) => s.text.startsWith(`UPDATE "HealthCheckConnectLink"`))) {
      assert.match(s.text, /WHERE "id" = \$1 AND "state" = 'pending'$/);
      assert.match(s.text, /"sealedPending" = NULL, "sealedCode" = NULL/);
    }
    pool.handlers.push(() => ({ rows: [], rowCount: 0 }));
    await assert.rejects(store.markLinkUsed("l3", { at: "2026-11-03T20:00:00.000Z", orgId: "00D000000000001AAA", userId: "005000000000001AAA", note: null }), /wasn't pending/);
    await assert.rejects(store.refuseLink("l3", "x", "2026-11-03T20:00:00.000Z"), /wasn't pending/);
  });
});

describe("PostgresWorkerStore, for documents", () => {
  test("loads a released run's approved sections, their current text, overrides and the folder it had", async () => {
    const pool = new FakePool();
    pool.handlers.push((text) => {
      if (text.includes(`FROM "HealthCheckRun" WHERE`)) return { rows: [{ ...RUN_ROW, status: "released", preparedBy: "Synthetic Consultant" }], rowCount: 1 };
      if (text.includes(`FROM "HealthCheckRunStep"`)) return { rows: [], rowCount: 0 };
      if (text.includes(`FROM "HealthCheckIntake"`)) return { rows: [{ statedProblems: "Forecasts are unreliable.", customerName: "Synthetic Co", linkedinUrl: null, industry: null, headcountBand: null, customerAnswers: null }], rowCount: 1 };
      if (text.startsWith("SELECT") && text.includes(`FROM "HealthCheckSection" WHERE "runId"`) && !text.includes("HealthCheckSectionVersion")) {
        return { rows: [{ id: "s1", runId: RUN_ROW.id, output: "executive_summary", key: "verdict", title: "Verdict", order: 0, status: "approved", currentVersionId: "v2", findingIds: [], metricIds: [], manualStepIds: [], approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" }], rowCount: 1 };
      }
      if (text.includes(`FROM "HealthCheckSectionVersion"`)) {
        return { rows: [{ id: "v2", sectionId: "s1", version: 2, body: "Reworded.", source: "consultant_edit", changedBy: "consultant@example.com", changedAt: "2026-11-04T17:00:00.000Z", changeSummary: "Shorter", numberCheck: { passed: true, checkedAt: "2026-11-04T17:00:00.000Z", unmatched: [] } }], rowCount: 1 };
      }
      if (text.includes(`FROM "HealthCheckNumberOverride"`)) {
        return { rows: [{ id: "o1", runId: RUN_ROW.id, targetType: "finding", targetId: "f1", targetField: "n", previousValue: 76, newValue: 70, reason: "Six are test deals", overriddenBy: "consultant@example.com", overriddenAt: "2026-11-04T16:00:00.000Z" }], rowCount: 1 };
      }
      if (text.includes(`FROM "HealthCheckDocuments"`)) return { rows: [{ runId: RUN_ROW.id, status: "queued", driveFolderId: "folder-0000000001", files: [] }], rowCount: 1 };
      return undefined;
    });
    const job = (await new PostgresWorkerStore(pool).loadDocumentsJob(RUN_ROW.id))!;
    assert.equal(job.run.status, "released");
    assert.equal(job.intake!.customerName, "Synthetic Co");
    assert.deepEqual(job.sections[0]!.approval, { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" });
    assert.equal(job.versions[0]!.body, "Reworded.");
    assert.deepEqual(job.overrides[0]!.target, { type: "finding", id: "f1", field: "n" });
    assert.equal(job.state, "queued");
    assert.equal(job.driveFolderId, "folder-0000000001");
    assert.equal(job.preparedBy, "Synthetic Consultant");
    assert.match(pool.texts().find((t) => t.includes("HealthCheckSectionVersion"))!, /"currentVersionId"/);
  });

  test("reads who prepared the report for the cover, and only the documents job does", async () => {
    const preparedBy = async (value: unknown) => {
      const pool = new FakePool();
      pool.handlers.push((text) => (text.includes(`FROM "HealthCheckRun" WHERE`) ? { rows: [{ ...RUN_ROW, status: "released", preparedBy: value }], rowCount: 1 } : { rows: [], rowCount: 0 }));
      const job = (await new PostgresWorkerStore(pool).loadDocumentsJob(RUN_ROW.id))!;
      assert.match(pool.texts()[0]!, /"preparedBy"/);
      return job.preparedBy;
    };
    assert.equal(await preparedBy(null), null);
    assert.equal(await preparedBy("  "), null);
    assert.equal(await preparedBy(" Synthetic Consultant "), "Synthetic Consultant");
    // The run job reads the run without it, so a run never waits on the cover's column.
    assert.doesNotMatch(selectSql("HealthCheckRun", RUN, `"id" = $1`), /preparedBy/);
  });

  test("moves only a requested row to making, and writes results only to a row it's making", async () => {
    const pool = new FakePool();
    const store = new PostgresWorkerStore(pool);
    pool.handlers.push(() => ({ rows: [], rowCount: 0 }));
    assert.equal(await store.startDocuments(RUN_ROW.id, "2026-11-04T19:00:00.000Z"), false);
    await store.saveDocuments(RUN_ROW.id, { at: "2026-11-04T19:01:00.000Z", clientName: "Synthetic Co", folderName: "Health check 1", driveFolderId: "f", driveFolderUrl: "https://drive.google.com/f", files: [], warnings: [] });
    await store.failDocuments(RUN_ROW.id, "x", "2026-11-04T19:01:00.000Z");
    const [start, save, fail] = pool.texts();
    assert.match(start!, /"status" IN \('queued', 'making'\)/);
    assert.match(save!, /AND "status" = 'making'/);
    assert.match(fail!, /AND "status" IN \('queued', 'making'\)/);
  });
});
