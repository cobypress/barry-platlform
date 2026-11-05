import type { ScanResult } from "../scan/result.ts";
import type { PulseResult } from "../pulse/result.ts";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import { Intake } from "../narrative/intake.ts";
import { SURVEY_VERSION, type Survey } from "../survey/questions.ts";
import { BenchmarkRow, HeadcountBand, Industry } from "../schemas/benchmark.ts";
import { Calibration, ConnectLink, ManualAnswer, NumberOverride, ReportSection, Run, SectionVersion, type Finding, type ManualStep, type Metric, type Score } from "../schemas/index.ts";
import {
  LinkState,
  ORG_AWAITING_CONNECT,
  RunClosedError,
  type ConnectJobInput,
  type DocumentFile,
  type DocumentsJobInput,
  type DocumentsMade,
  type DocumentsState,
  type LinkConnected,
  type RunJobInput,
  type RunOutputRecord,
  type RunProgress,
  type PulseRun,
  type ReleasedRun,
  type WorkerStore,
} from "./types.ts";

// UNVERIFIED: written against the console's migrations (blackcloud-next
// migrations/20261103_health_check*.sql) and the columns this lane asked lane
// C2 for (docs/overnight/night-2/worker.md), but never run against a database.
// Run the contract tests against a Neon branch before trusting it.
//
// It speaks plain SQL through the smallest interface `pg` satisfies, so the
// engine needs no Postgres library to build or test it. Timestamps are
// TIMESTAMP(3) without a zone, holding UTC as Prisma writes them: they go in as
// ISO strings (Postgres ignores the Z for that type) and come out through
// to_char, so the Node process's own time zone never touches them.

export type SqlResult = { rows: Array<Record<string, unknown>>; rowCount: number | null };
export interface SqlClient {
  query(text: string, values?: unknown[]): Promise<SqlResult>;
}
export interface SqlPool extends SqlClient {
  connect(): Promise<SqlClient & { release(): void }>;
}

type Kind = "text" | "num" | "bool" | "json" | "ts" | "date" | "array";
type Columns = Record<string, Kind>;

const CAST: Record<Kind, string> = { text: "", num: "", bool: "", json: "::jsonb", ts: "::timestamp(3)", date: "::date", array: "::text[]" };

const q = (name: string): string => `"${name}"`;

/** A column as text the schemas take: ISO UTC for a timestamp, YYYY-MM-DD for a date. */
function readColumn(name: string, kind: Kind): string {
  if (kind === "ts") return `to_char(${q(name)}, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS ${q(name)}`;
  if (kind === "date") return `${q(name)}::text AS ${q(name)}`;
  return q(name);
}

export function selectSql(table: string, columns: Columns, where: string, orderBy = ""): string {
  return `SELECT ${Object.entries(columns).map(([n, k]) => readColumn(n, k)).join(", ")} FROM ${q(table)} WHERE ${where}${orderBy ? ` ORDER BY ${orderBy}` : ""}`;
}

/** One row, one statement, so BIGSERIAL "seq" follows write order. */
export function insertSql(table: string, columns: Columns, row: Record<string, unknown>): { text: string; values: unknown[] } {
  const names = Object.keys(columns);
  for (const name of Object.keys(row)) if (!(name in columns)) throw new Error(`${table} has no column ${name}`);
  const values = names.map((n) => {
    const v = row[n];
    if (v === undefined || v === null) return null;
    // Arrays held as JSONB would otherwise become Postgres arrays.
    return columns[n] === "json" ? JSON.stringify(v) : v;
  });
  return { text: `INSERT INTO ${q(table)} (${names.map(q).join(", ")}) VALUES (${names.map((n, i) => `$${i + 1}${CAST[columns[n]!]}`).join(", ")})`, values };
}

// ─── The console's tables, as far as the worker reads or writes them ─────────

export const RUN: Columns = {
  id: "text", product: "text", customerRef: "text", orgId: "text", isSandbox: "bool", connectionKind: "text", status: "text", modules: "array",
  ruleSetVersion: "text", calibrationHash: "text", baselineRunId: "text", asOf: "ts", analysisFrom: "date", analysisTo: "date", apiVersion: "text",
  budget: "json", benchmarkOptIn: "bool", createdBy: "text", startedAt: "ts", finishedAt: "ts",
  // New, from C2: see the log.
  connectionName: "text",
  // "complete" or "essentials" (DECISIONS 94). Null is a run from before scopes, which was complete.
  scope: "text",
};
/**
 * The run as the documents job reads it: the cover's name too. blackcloud-next
 * migrations/20261106_health_check_prepared_by.sql. Kept out of RUN so a run
 * job never reads it, and never needs the column.
 */
export const RUN_DOCUMENTS: Columns = { ...RUN, preparedBy: "text" };
export const RUN_STEP: Columns = { runId: "text", id: "text", version: "num", status: "text", calls: "num", finishedAt: "ts", error: "text" };
export const INTAKE: Columns = {
  statedProblems: "text", customerName: "text", linkedinUrl: "text", industry: "text", headcountBand: "text", customerAnswers: "json",
  salesforceSpendAnnual: "num", hourlyCost: "num", namedRepConsent: "bool", caseStudyConsent: "bool", callAnswers: "json",
};
// The console's survey answers (contract with the console: DECISIONS.md 147).
export const SURVEY_RESPONSE: Columns = { id: "text", submittedAt: "ts", answers: "json" };
// A "version" column the console may add. Read through the row as JSON, so the
// query works with the column and without it: missing reads as null (DECISIONS.md 150).
export const SURVEY_RESPONSE_SQL = selectSql("HealthCheckSurveyResponse", SURVEY_RESPONSE, `"runId" = $1`, `"submittedAt", "id"`).replace(
  ` FROM "HealthCheckSurveyResponse"`,
  `, to_jsonb("HealthCheckSurveyResponse") ->> 'version' AS "version" FROM "HealthCheckSurveyResponse"`,
);
// The console's row saying the survey was sent (DECISIONS.md 149).
export const SURVEY_SENT: Columns = { runId: "text", openedAt: "ts", closedAt: "ts" };

/** The version an answer was given to: the row's, or 1 when it has none. One that isn't a whole number is unknown, so it's dropped. */
function versionOf(value: unknown): number {
  if (value === null || value === undefined || value === "") return 1;
  const n = Number(value);
  return Number.isInteger(n) ? n : 0;
}

/**
 * A run's survey, its answers in the order they came in. Null when it wasn't
 * sent, so nothing about it is shown. Sent with no answers is still a survey:
 * the documents say fewer than five answered. Answers mean it was sent, row or not.
 */
export function surveyFromRows(rows: Array<Record<string, unknown>>, sent = false): Survey | null {
  if (rows.length === 0 && !sent) return null;
  return { version: SURVEY_VERSION, responses: rows.map((r) => ({ id: String(r.id), submittedAt: String(r.submittedAt), version: versionOf(r.version), answers: r.answers })) };
}

export const CALIBRATION: Columns = {
  version: "num", orgId: "text", confirmedBy: "text", confirmedAt: "ts", stages: "json", scope: "json", users: "json", fields: "json", targets: "json",
  salesCycleAssumptionDays: "num", fiscalYearStartMonth: "num", analysisWindowMonths: "num", minSample: "num", dormantDays: "num", currency: "text",
  licenceCosts: "json", effortMinutes: "json",
};
// Service Cloud's limits, in a "service" JSONB column the console may add. Read
// through the row as JSON, as the survey version is, so the query works with
// the column and without it: missing reads as null, and the defaults apply (DECISIONS.md 165).
export const CALIBRATION_SQL = selectSql("HealthCheckCalibration", CALIBRATION, `"runId" = $1`).replace(
  ` FROM "HealthCheckCalibration"`,
  `, to_jsonb("HealthCheckCalibration") -> 'service' AS "service" FROM "HealthCheckCalibration"`,
);
export const MANUAL_ANSWER: Columns = {
  stepKey: "text", outcome: "text", method: "text", values: "json", pastedText: "text", screenshotRef: "text", prefill: "json", skipReason: "text",
  answeredBy: "text", answeredAt: "ts",
};
export const FINDING: Columns = {
  id: "text", runId: "text", ruleId: "text", ruleVersion: "num", domain: "text", status: "text", title: "text", summary: "text", recommendation: "text",
  recommendationType: "text", severity: "text", confidence: "text", evidence: "json", value: "json", effort: "json", owner: "text", dependencies: "array",
  guards: "json", segmentsChecked: "json", segmentExemptReason: "text", subjects: "json", facts: "json", notMeasurableReason: "text", manualStepIds: "array",
};
export const METRIC: Columns = {
  id: "text", runId: "text", status: "text", domain: "text", label: "text", definition: "text", unit: "text", evidence: "json", exclusions: "array",
  assumptions: "array", value: "num", n: "num", dateRange: "json", segments: "json", segmentExemptReason: "text", series: "json", stats: "json", reason: "text",
};
export const SCORE: Columns = { runId: "text", domain: "text", status: "text", score: "num", ruleSetVersion: "text", weights: "json", findingIds: "array", reason: "text" };
export const MANUAL_STEP: Columns = {
  id: "text", runId: "text", key: "text", domain: "text", raisedBy: "text", ruleIds: "array", title: "text", whyItMatters: "text", whereToGo: "text",
  whatToCapture: "text", fields: "json", answerMethods: "array", status: "text", ifSkipped: "text",
};
export const SECTION: Columns = {
  id: "text", runId: "text", output: "text", key: "text", title: "text", order: "num", status: "text", currentVersionId: "text", findingIds: "array",
  metricIds: "array", manualStepIds: "array", approvedBy: "text", approvedAt: "ts",
};
export const VERSION: Columns = {
  id: "text", sectionId: "text", version: "num", body: "text", source: "text", changedBy: "text", changedAt: "ts", changeSummary: "text", numberCheck: "json",
};
export const OVERRIDE: Columns = {
  id: "text", runId: "text", targetType: "text", targetId: "text", targetField: "text", previousValue: "num", newValue: "num", reason: "text", overriddenBy: "text", overriddenAt: "ts",
};
/** blackcloud-next migrations/20261105_health_check_documents.sql. */
export const DOCUMENTS: Columns = {
  runId: "text", status: "text", requestedBy: "text", requestedAt: "ts", startedAt: "ts", finishedAt: "ts", error: "text", warnings: "json", clientName: "text",
  folderName: "text", driveFolderId: "text", driveFolderUrl: "text", files: "json",
};
export const BENCHMARK_ROW: Columns = {
  id: "text", metricKey: "text", ruleSetVersion: "text", value: "num", unit: "text", industry: "text", headcountBand: "text", userBand: "text", dealSizeBand: "text", period: "text",
};
/** New, from C2: see the log. The first ten columns are ConnectLink in src/schemas/connect.ts. */
export const CONNECT_LINK: Columns = {
  id: "text", tokenHash: "text", customerRef: "text", connectionName: "text", loginUrl: "text", createdBy: "text", createdAt: "ts", expiresAt: "ts",
  usedAt: "ts", revokedAt: "ts", state: "text", sealedPending: "text", sealedCode: "text",
};

// ─── Rows to records ──────────────────────────────────────────────────────────

const withoutNulls = (o: Record<string, unknown>): Record<string, unknown> => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== null));

export function runFromRows(row: Record<string, unknown>, steps: Array<Record<string, unknown>>): Run {
  const { analysisFrom, analysisTo, connectionName: _name, preparedBy: _by, scope, ...rest } = row;
  return Run.parse({ ...rest, ...(scope === null || scope === undefined ? {} : { scope }), analysisWindow: { from: analysisFrom, to: analysisTo }, steps });
}

/**
 * The parts of what the customer's admin answered on the connect page that the
 * engine's intake uses (blackcloud-next lib/health-check/customer-answers.ts
 * has the whole form). The rest is for the consultant.
 */
const CustomerAnswersForIntake = z.object({
  companyName: z.string().min(1),
  industry: Industry,
  headcountBand: HeadcountBand,
  goals: z.string().min(1),
  whyNow: z.string().min(1).optional(),
  problems: z.string().min(1),
});

/**
 * Answers the consultant confirmed from the discovery call, each taken on its
 * own: one that doesn't read is left out rather than losing the rest.
 */
const CallAnswersForIntake = z.object({
  answers: z.object({
    companyName: z.string().min(1).optional().catch(undefined),
    industry: Industry.optional().catch(undefined),
    headcountBand: HeadcountBand.optional().catch(undefined),
    goals: z.string().min(1).optional().catch(undefined),
    whyNow: z.string().min(1).optional().catch(undefined),
    problems: z.string().min(1).optional().catch(undefined),
    salesforceSpendAnnual: z.number().positive().optional().catch(undefined),
    hourlyCost: z.number().positive().optional().catch(undefined),
  }),
});

const positive = (value: unknown): number | null => (typeof value === "number" && value > 0 ? value : null);

const statements = (text: string) => text.split(/\r?\n/).map((l) => l.replace(/^\s*(?:[-*•]|\d+[.)])\s+/, "").trim()).filter(Boolean);

/**
 * The console's free-text problems as the engine's intake: one stated problem
 * per line, as the console's toEngineIntake does. The customer's own problems
 * follow, marked as raised by the customer, and their goals join the summary.
 * Their company details fill only what the consultant left blank.
 */
export function intakeFromRow(row: Record<string, unknown>, run: Run): Intake {
  const text = String(row.statedProblems ?? "").trim();
  const customer = CustomerAnswersForIntake.safeParse(row.customerAnswers ?? undefined);
  const answers = customer.success ? customer.data : null;
  // When the customer replied, the console has already folded the call's answers into theirs. The call only counts when they didn't.
  const call = answers ? null : CallAnswersForIntake.safeParse(row.callAnswers ?? undefined);
  const fromCall = call?.success ? call.data.answers : {};
  const raw = (row.customerAnswers ?? {}) as Record<string, unknown>;
  const extra = withoutNulls({
    customerName: row.customerName ?? answers?.companyName ?? fromCall.companyName ?? null,
    linkedinUrl: row.linkedinUrl,
    industry: row.industry ?? answers?.industry ?? fromCall.industry ?? null,
    headcountBand: row.headcountBand ?? answers?.headcountBand ?? fromCall.headcountBand ?? null,
    salesforceSpendAnnual: positive(row.salesforceSpendAnnual) ?? positive(raw.salesforceSpendAnnual) ?? fromCall.salesforceSpendAnnual ?? null,
    hourlyCost: positive(row.hourlyCost) ?? positive(raw.hourlyCost) ?? fromCall.hourlyCost ?? null,
    namedRepConsent: typeof row.namedRepConsent === "boolean" ? row.namedRepConsent : null,
    caseStudyConsent: typeof row.caseStudyConsent === "boolean" ? row.caseStudyConsent : null,
  });
  const ours = statements(text);
  const theirProblems = answers?.problems ?? fromCall.problems;
  const theirs = theirProblems ? statements(theirProblems) : [];
  const goalsText = answers?.goals ?? fromCall.goals;
  const whyNow = answers ? answers.whyNow : fromCall.whyNow;
  const goals = goalsText ? [`The customer's goals for the next 12 months:\n${goalsText.trim()}`, ...(whyNow ? [`Why now: ${whyNow.trim()}`] : [])] : [];
  return Intake.parse({
    ...extra,
    summary: [text, ...goals].filter(Boolean).join("\n\n"),
    statedProblems: [
      ...ours.map((statement, i) => ({ id: `p${i + 1}`, statement, findingIds: [] })),
      ...theirs.map((statement, i) => ({ id: `p${ours.length + i + 1}`, statement, raisedBy: "customer", findingIds: [] })),
    ],
    capturedBy: run.createdBy,
    capturedAt: run.startedAt,
  });
}

export function calibrationFromRow(row: Record<string, unknown>): Calibration {
  return Calibration.parse(withoutNulls(row));
}

export function answerFromRow(row: Record<string, unknown>): ManualAnswer {
  return ManualAnswer.parse(withoutNulls(row));
}

export function sectionFromRow(row: Record<string, unknown>): ReportSection {
  const { approvedBy, approvedAt, ...rest } = row;
  return ReportSection.parse({ ...rest, approval: approvedBy && approvedAt ? { approvedBy, approvedAt } : null });
}

export function versionFromRow(row: Record<string, unknown>): SectionVersion {
  return SectionVersion.parse(row);
}

export function overrideFromRow(row: Record<string, unknown>): NumberOverride {
  const { targetType, targetId, targetField, ...rest } = row;
  return NumberOverride.parse({ ...rest, target: { type: targetType, id: targetId, field: targetField } });
}

export function linkFromRow(row: Record<string, unknown>, runOrgId: string): ConnectJobInput {
  const { state, sealedPending, sealedCode, ...link } = row;
  return { link: ConnectLink.parse(link), state: LinkState.parse(state), runOrgId, sealedPending: (sealedPending as string | null) ?? null, sealedCode: (sealedCode as string | null) ?? null };
}

// ─── Records to rows ──────────────────────────────────────────────────────────

export function stepRows(runId: string, progress: RunProgress): Array<Record<string, unknown>> {
  return progress.steps.map((s) => ({ runId, ...s }));
}

export function findingRow(runId: string, f: Finding): Record<string, unknown> {
  const { id, ruleId, ruleVersion, domain, status, title, summary, recommendation, recommendationType, severity, confidence, evidence, value, effort, owner, dependencies, guards, segmentsChecked, segmentExemptReason, subjects, facts, notMeasurableReason, manualStepIds } = f;
  return { id, runId, ruleId, ruleVersion, domain, status, title, summary, recommendation, recommendationType, severity, confidence, evidence, value, effort, owner, dependencies, guards, segmentsChecked, segmentExemptReason, subjects, facts, notMeasurableReason, manualStepIds };
}

export function metricRow(runId: string, m: Metric): Record<string, unknown> {
  const base = { id: m.id, runId, status: m.status, domain: m.domain, label: m.label, definition: m.definition, unit: m.unit, evidence: m.evidence, exclusions: m.exclusions, assumptions: m.assumptions };
  if (m.status === "measured") return { ...base, value: m.value, n: m.n, dateRange: m.dateRange, segments: m.segments, segmentExemptReason: m.segmentExemptReason, series: m.series, stats: m.stats, reason: null };
  return { ...base, value: null, n: null, dateRange: null, segments: null, segmentExemptReason: null, series: null, stats: null, reason: m.reason };
}

export function scoreRow(runId: string, s: Score): Record<string, unknown> {
  if (s.status === "measured") return { runId, domain: s.domain, status: s.status, score: s.score, ruleSetVersion: s.ruleSetVersion, weights: s.weights, findingIds: s.findingIds, reason: null };
  return { runId, domain: s.domain, status: s.status, score: null, ruleSetVersion: s.ruleSetVersion, weights: null, findingIds: [], reason: s.reason };
}

export function manualStepRow(runId: string, s: ManualStep): Record<string, unknown> {
  return { ...s, runId };
}

export function sectionRow(s: ReportSection): Record<string, unknown> {
  const { approval, ...rest } = s;
  return { ...rest, approvedBy: approval?.approvedBy ?? null, approvedAt: approval?.approvedAt ?? null };
}

export function versionRow(v: SectionVersion): Record<string, unknown> {
  return { ...v };
}

// ─── The store ────────────────────────────────────────────────────────────────

const CLOSED = new Set(["in_review", "released"]);
const one = (r: SqlResult): Record<string, unknown> | undefined => r.rows[0];

export class PostgresWorkerStore implements WorkerStore {
  readonly #pool: SqlPool;

  constructor(pool: SqlPool) {
    this.#pool = pool;
  }

  async #transaction<T>(work: (client: SqlClient) => Promise<T>): Promise<T> {
    const client = await this.#pool.connect();
    try {
      await client.query("BEGIN");
      const result = await work(client);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  /**
   * A query on one of the console's survey tables. A database without the table
   * yet (Postgres 42P01) has no rows, so a worker deployed before that migration
   * still runs every check rather than failing them.
   */
  async #surveyQuery(text: string, runId: string): Promise<SqlResult> {
    try {
      return await this.#pool.query(text, [runId]);
    } catch (err) {
      if ((err as { code?: string }).code === "42P01") return { rows: [], rowCount: 0 };
      throw err;
    }
  }

  /** The run's survey: sent or not, and its answers. Without either table, it wasn't sent. */
  async #survey(runId: string): Promise<Survey | null> {
    const [sent, answers] = await Promise.all([this.#surveyQuery(selectSql("HealthCheckSurvey", SURVEY_SENT, `"runId" = $1`), runId), this.#surveyQuery(SURVEY_RESPONSE_SQL, runId)]);
    return surveyFromRows(answers.rows, sent.rows.length > 0);
  }

  async loadRunJob(runId: string): Promise<RunJobInput | null> {
    const row = one(await this.#pool.query(selectSql("HealthCheckRun", RUN, `"id" = $1`), [runId]));
    if (!row) return null;
    const [steps, intake, calibration, answers, survey] = await Promise.all([
      this.#pool.query(selectSql("HealthCheckRunStep", RUN_STEP, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckIntake", INTAKE, `"runId" = $1`), [runId]),
      this.#pool.query(CALIBRATION_SQL, [runId]),
      this.#pool.query(selectSql("HealthCheckManualAnswer", MANUAL_ANSWER, `"runId" = $1`, `"answeredAt", "stepKey"`), [runId]),
      this.#survey(runId),
    ]);
    const run = runFromRows(row, steps.rows.map(({ runId: _r, ...s }) => s));
    return {
      run,
      intake: one(intake) ? intakeFromRow(one(intake)!, run) : null,
      calibration: one(calibration) ? calibrationFromRow(one(calibration)!) : null,
      connectionName: (row.connectionName as string | null) ?? null,
      answers: answers.rows.map(answerFromRow),
      survey,
    };
  }

  /** Locks the run's row for the transaction, and refuses one the console has moved on. */
  async #openRun(client: SqlClient, runId: string): Promise<void> {
    const row = one(await client.query(`SELECT "status" FROM "HealthCheckRun" WHERE "id" = $1 FOR UPDATE`, [runId]));
    if (!row) throw new Error(`No run ${runId}`);
    if (CLOSED.has(String(row.status))) throw new RunClosedError(runId, String(row.status));
  }

  async #writeProgress(client: SqlClient, runId: string, p: RunProgress, runDir: string | null): Promise<void> {
    await client.query(
      `UPDATE "HealthCheckRun" SET "status" = $2, "ruleSetVersion" = $3, "calibrationHash" = $4, "asOf" = $5::timestamp(3), "analysisFrom" = $6::date, "analysisTo" = $7::date, "apiVersion" = $8, "budget" = $9::jsonb, "isSandbox" = $10, "finishedAt" = $11::timestamp(3)${runDir === null ? "" : `, "runDir" = $12`} WHERE "id" = $1`,
      [runId, p.status, p.ruleSetVersion, p.calibrationHash, p.asOf, p.analysisWindow.from, p.analysisWindow.to, p.apiVersion, JSON.stringify(p.budget), p.isSandbox, p.finishedAt, ...(runDir === null ? [] : [runDir])],
    );
    await client.query(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1`, [runId]);
    for (const step of stepRows(runId, p)) await this.#insert(client, "HealthCheckRunStep", RUN_STEP, step);
  }

  async #insert(client: SqlClient, table: string, columns: Columns, row: Record<string, unknown>): Promise<void> {
    const { text, values } = insertSql(table, columns, row);
    await client.query(text, values);
  }

  saveRunProgress(runId: string, progress: RunProgress, runDir: string): Promise<void> {
    return this.#transaction(async (client) => {
      await this.#openRun(client, runId);
      await this.#writeProgress(client, runId, progress, runDir);
    });
  }

  saveRunOutput(runId: string, output: RunOutputRecord): Promise<void> {
    return this.#transaction(async (client) => {
      await this.#openRun(client, runId);
      await this.#writeProgress(client, runId, output.progress, output.runDir);
      // Replaced whole, so a second delivery leaves the same rows. Deleting a
      // section takes its versions and proposals with it (ON DELETE CASCADE).
      for (const table of ["HealthCheckFinding", "HealthCheckMetric", "HealthCheckScore", "HealthCheckManualStep", "HealthCheckSection"]) {
        await client.query(`DELETE FROM ${q(table)} WHERE "runId" = $1`, [runId]);
      }
      for (const f of output.findings) await this.#insert(client, "HealthCheckFinding", FINDING, findingRow(runId, f));
      for (const m of output.metrics) await this.#insert(client, "HealthCheckMetric", METRIC, metricRow(runId, m));
      for (const s of output.scores) await this.#insert(client, "HealthCheckScore", SCORE, scoreRow(runId, s));
      for (const s of output.manualSteps) await this.#insert(client, "HealthCheckManualStep", MANUAL_STEP, manualStepRow(runId, s));
      for (const s of output.sections) await this.#insert(client, "HealthCheckSection", SECTION, sectionRow(s));
      for (const v of output.versions) await this.#insert(client, "HealthCheckSectionVersion", VERSION, versionRow(v));
    });
  }

  recordRunFailure(runId: string, reason: string, at: string): Promise<void> {
    return this.#transaction(async (client) => {
      await this.#openRun(client, runId);
      await client.query(`UPDATE "HealthCheckRun" SET "status" = 'failed' WHERE "id" = $1`, [runId]);
      await client.query(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1 AND "id" = 'worker'`, [runId]);
      await this.#insert(client, "HealthCheckRunStep", RUN_STEP, { runId, id: "worker", version: 1, status: "failed", calls: 0, finishedAt: at, error: reason });
    });
  }

  async loadConnectJob(linkId: string): Promise<ConnectJobInput | null> {
    const row = one(await this.#pool.query(selectSql("HealthCheckConnectLink", CONNECT_LINK, `"id" = $1`), [linkId]));
    if (!row) return null;
    const run = one(await this.#pool.query(`SELECT "orgId", "id", "product" FROM "HealthCheckRun" WHERE "id" = (SELECT "runId" FROM "HealthCheckConnectLink" WHERE "id" = $1)`, [linkId]));
    if (typeof run?.orgId !== "string") return null;
    const job = linkFromRow(row, run.orgId);
    return run.product === "scan" && typeof run.id === "string" ? { ...job, scanRunId: run.id } : job;
  }

  markLinkUsed(linkId: string, c: LinkConnected): Promise<void> {
    return this.#transaction(async (client) => {
      const result = await client.query(
        `UPDATE "HealthCheckConnectLink" SET "state" = 'used', "usedAt" = $2::timestamp(3), "connectedOrgId" = $3, "connectedUserId" = $4, "checkNote" = $5, "sealedPending" = NULL, "sealedCode" = NULL WHERE "id" = $1 AND "state" = 'pending'`,
        [linkId, c.at, c.orgId, c.userId, c.note],
      );
      if (result.rowCount !== 1) throw new Error(`Link ${linkId} wasn't pending, so it wasn't marked used`);
      // A run that didn't know its org yet takes the one the customer connected.
      await client.query(
        `UPDATE "HealthCheckRun" SET "orgId" = $2 WHERE "id" = (SELECT "runId" FROM "HealthCheckConnectLink" WHERE "id" = $1) AND "orgId" = $3`,
        [linkId, c.orgId, ORG_AWAITING_CONNECT],
      );
    });
  }

  async refuseLink(linkId: string, reason: string, at: string): Promise<void> {
    const result = await this.#pool.query(
      `UPDATE "HealthCheckConnectLink" SET "state" = 'refused', "refusedReason" = $2, "refusedAt" = $3::timestamp(3), "sealedPending" = NULL, "sealedCode" = NULL WHERE "id" = $1 AND "state" = 'pending'`,
      [linkId, reason, at],
    );
    if (result.rowCount !== 1) throw new Error(`Link ${linkId} wasn't pending, so it wasn't refused`);
  }

  async loadDocumentsJob(runId: string): Promise<DocumentsJobInput | null> {
    const row = one(await this.#pool.query(selectSql("HealthCheckRun", RUN_DOCUMENTS, `"id" = $1`), [runId]));
    if (!row) return null;
    const [steps, intake, sections, versions, overrides, documents, survey] = await Promise.all([
      this.#pool.query(selectSql("HealthCheckRunStep", RUN_STEP, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckIntake", INTAKE, `"runId" = $1`), [runId]),
      this.#pool.query(selectSql("HealthCheckSection", SECTION, `"runId" = $1`, `"output", "order"`), [runId]),
      this.#pool.query(selectSql("HealthCheckSectionVersion", VERSION, `"id" IN (SELECT "currentVersionId" FROM "HealthCheckSection" WHERE "runId" = $1)`), [runId]),
      this.#pool.query(selectSql("HealthCheckNumberOverride", OVERRIDE, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckDocuments", DOCUMENTS, `"runId" = $1`), [runId]),
      this.#survey(runId),
    ]);
    const run = runFromRows(row, steps.rows.map(({ runId: _r, ...s }) => s));
    const doc = one(documents);
    return {
      run,
      intake: one(intake) ? intakeFromRow(one(intake)!, run) : null,
      survey,
      sections: sections.rows.map(sectionFromRow),
      versions: versions.rows.map(versionFromRow),
      overrides: overrides.rows.map(overrideFromRow),
      // The console trims it, but a blank one mustn't print "by , Black Cloud".
      preparedBy: (row.preparedBy as string | null | undefined)?.trim() || null,
      state: (doc?.status as DocumentsState | undefined) ?? null,
      driveFolderId: (doc?.driveFolderId as string | null | undefined) ?? null,
      files: (doc?.files as DocumentFile[] | null | undefined) ?? [],
    };
  }

  async startDocuments(runId: string, at: string): Promise<boolean> {
    const result = await this.#pool.query(
      `UPDATE "HealthCheckDocuments" SET "status" = 'making', "startedAt" = $2::timestamp(3), "error" = NULL WHERE "runId" = $1 AND "status" IN ('queued', 'making')`,
      [runId, at],
    );
    return result.rowCount === 1;
  }

  async saveDocuments(runId: string, made: DocumentsMade): Promise<void> {
    await this.#pool.query(
      `UPDATE "HealthCheckDocuments" SET "status" = 'done', "finishedAt" = $2::timestamp(3), "error" = NULL, "warnings" = $3::jsonb, "clientName" = $4, "folderName" = $5, "driveFolderId" = $6, "driveFolderUrl" = $7, "files" = $8::jsonb WHERE "runId" = $1 AND "status" = 'making'`,
      [runId, made.at, JSON.stringify(made.warnings), made.clientName, made.folderName, made.driveFolderId, made.driveFolderUrl, JSON.stringify(made.files)],
    );
  }

  addBenchmarkRows(rows: BenchmarkRow[]): Promise<void> {
    return this.#transaction(async (client) => {
      // The id is a fresh random one: Prisma's cuid default lives in the client, not the database.
      for (const row of rows) await this.#insert(client, "HealthCheckBenchmarkRow", BENCHMARK_ROW, { id: randomUUID(), ...BenchmarkRow.parse(row) });
    });
  }

  async listReleasedRuns(): Promise<ReleasedRun[]> {
    // The last output's release date, so retention never starts early. A
    // released run with no release record has a null date, and is left alone.
    const { rows } = await this.#pool.query(
      `SELECT r."id" AS "runId", to_char(MAX(rel."releasedAt"), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "releasedAt" FROM "HealthCheckRun" r LEFT JOIN "HealthCheckRelease" rel ON rel."runId" = r."id" WHERE r."status" = 'released' GROUP BY r."id" ORDER BY r."id"`,
    );
    return rows.map((row: Record<string, unknown>) => ({ runId: String(row.runId), releasedAt: typeof row.releasedAt === "string" ? row.releasedAt : null }));
  }

  async recordRunQueued(runId: string, at: string): Promise<void> {
    await this.#pool.query(`UPDATE "HealthCheckRun" SET "queuedAt" = $2::timestamp(3), "queueError" = NULL WHERE "id" = $1`, [runId, at]);
  }

  async saveScanResult(runId: string, result: ScanResult, madeAt: string): Promise<void> {
    await this.#pool.query(
      `INSERT INTO "HealthCheckScanResult" ("runId", "madeAt", "result") VALUES ($1, $2::timestamp(3), $3::jsonb) ON CONFLICT ("runId") DO UPDATE SET "madeAt" = EXCLUDED."madeAt", "result" = EXCLUDED."result"`,
      [runId, madeAt, JSON.stringify(result)],
    );
  }

  async savePulseResult(runId: string, result: PulseResult, madeAt: string): Promise<void> {
    // blackcloud-next's HealthCheckPulseResult ("runId" PK, "madeAt", "result" JSONB), as its scan result is.
    await this.#pool.query(
      `INSERT INTO "HealthCheckPulseResult" ("runId", "madeAt", "result") VALUES ($1, $2::timestamp(3), $3::jsonb) ON CONFLICT ("runId") DO UPDATE SET "madeAt" = EXCLUDED."madeAt", "result" = EXCLUDED."result"`,
      [runId, madeAt, JSON.stringify(result)],
    );
  }

  async listPulseRuns(): Promise<PulseRun[]> {
    const { rows } = await this.#pool.query(`SELECT "id" AS "runId", to_char("finishedAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "finishedAt" FROM "HealthCheckRun" WHERE "product" = 'pulse' ORDER BY "id"`);
    return rows.map((row: Record<string, unknown>) => ({ runId: String(row.runId), finishedAt: typeof row.finishedAt === "string" ? row.finishedAt : null }));
  }

  async saveMicrosite(runId: string, sealedPages: string, madeAt: string, expiresAt: string): Promise<void> {
    await this.#pool.query(
      `INSERT INTO "HealthCheckMicrosite" ("id", "runId", "version", "sealedPages", "madeAt", "expiresAt") VALUES ($1, $2, 1, $3, $4::timestamp(3), $5::timestamp(3))
       ON CONFLICT ("runId") DO UPDATE SET "version" = "HealthCheckMicrosite"."version" + 1, "sealedPages" = EXCLUDED."sealedPages", "madeAt" = EXCLUDED."madeAt", "expiresAt" = EXCLUDED."expiresAt"`,
      [randomUUID(), runId, sealedPages, madeAt, expiresAt],
    );
  }

  async loadBenchmarkRows(): Promise<BenchmarkRow[]> {
    const columns = Object.keys(BENCHMARK_ROW).filter((c) => c !== "id");
    const { rows } = await this.#pool.query(`SELECT ${columns.map((c) => `"${c}"`).join(", ")} FROM "HealthCheckBenchmarkRow"`);
    // numeric comes back from pg as a string.
    return rows.map((row: Record<string, unknown>) => BenchmarkRow.parse({ ...row, value: Number(row.value) }));
  }

  async failDocuments(runId: string, reason: string, at: string): Promise<void> {
    await this.#pool.query(`UPDATE "HealthCheckDocuments" SET "status" = 'failed', "finishedAt" = $2::timestamp(3), "error" = $3 WHERE "runId" = $1 AND "status" IN ('queued', 'making')`, [runId, at, reason]);
  }
}

/** Only for the column check in the tests. */
export const ALL_TABLES = { RUN, RUN_STEP, INTAKE, CALIBRATION, MANUAL_ANSWER, FINDING, METRIC, SCORE, MANUAL_STEP, SECTION, VERSION, CONNECT_LINK, OVERRIDE, DOCUMENTS, BENCHMARK_ROW } as const;
