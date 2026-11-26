import { isMyDomainHost } from "../connect/instance-host.ts";
import type { StepTiming } from "../engine/run.ts";
import { StoreError, type OrgStage } from "../connect/org-stages.ts";
import type { Inventory } from "../schemas/inventory.ts";
import type { Complexity } from "../schemas/complexity.ts";
import type { ScanResult } from "../scan/result.ts";
import type { PulseResult } from "../pulse/result.ts";
import { DIGEST_KEYS, type RunDigest } from "../schemas/digest.ts";
import { isCurrencyCode, type CalibratedStage } from "../schemas/calibration.ts";
import { ORG_SETTINGS_CALIBRATOR } from "../connect/auto-calibration.ts";
import { unstorableAt } from "./inventory.ts";
import { randomUUID } from "node:crypto";
import { z } from "zod";
import { Intake } from "../narrative/intake.ts";
import { SURVEY_VERSION, type Survey } from "../survey/questions.ts";
import { SURVEY_METRIC_IDS } from "../survey/summary.ts";
import { Interview } from "../interviews/questions.ts";
import { BenchmarkRow, HeadcountBand, Industry } from "../schemas/benchmark.ts";
import { storedFromRow, waitingRow, type ConsoleSummariesRow, type StoredSummaries } from "../handbook/summary-row.ts";
import { WorkerAtRun } from "./at-run.ts";
import { PULSE_HOLD_REASONS, readLosses, SHOWABLE_PULSE_HOLDS, type PulseHoldReason, type RunLosses } from "./release.ts";
import { Calibration, ConnectLink, ManualAnswer, NumberOverride, ReportSection, Run, SectionVersion, type Finding, type ManualStep, type Metric, type Score } from "../schemas/index.ts";
import {
  DISCONNECT_STEP,
  LinkState,
  ORG_AWAITING_CONNECT,
  RunClosedError,
  SCAN_REVOKE_NOT_CONFIRMED,
  type ConnectJobInput,
  type DocumentFile,
  type DocumentsJobInput,
  type DocumentsJobReads,
  type DocumentsMade,
  type DocumentsState,
  type DriveShare,
  type ShareJobInput,
  type LinkConnected,
  type RunJobInput,
  type RunOutputRecord,
  type RunProgress,
  type PulseRun,
  type ReleasedRun,
  type AutomaticRunState,
  type WorkerStore,
  type CurrencyWas,
  type RunLineage,
  type PulseState,
  type PortalDocumentRow,
  type RunDelivery,
  DOCUMENTS_HELD_FLAG,
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
/** The console's migration that adds the run's review mode (DECISIONS.md 688). */
export const REVIEW_MODE_MIGRATION = "20261211_health_check_automatic.sql";
/**
 * A run read with its "reviewMode", which the console's migration
 * REVIEW_MODE_MIGRATION adds. Read through the row as JSON, as the survey's
 * version is, so the query works before the column exists: missing or null
 * reads as consultant-led (DECISIONS.md 688).
 */
export const runSql = (columns: Columns): string =>
  selectSql("HealthCheckRun", columns, `"id" = $1`).replace(` FROM "HealthCheckRun"`, `, to_jsonb("HealthCheckRun") ->> 'reviewMode' AS "reviewMode" FROM "HealthCheckRun"`);
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

// The console's stakeholder interviews (DECISIONS.md 180). "answers" and
// "version" are read through the row as JSON, as the survey's version is, so
// the query works before and after the console adds either column. The
// interviewer and who recorded it are never read: the engine has no use for them.
export const INTERVIEW: Columns = { id: "text", role: "text", heldOn: "date", notes: "text" };
export const INTERVIEW_SQL = selectSql("HealthCheckInterview", INTERVIEW, `"runId" = $1`, `"heldOn", "id"`).replace(
  ` FROM "HealthCheckInterview"`,
  `, to_jsonb("HealthCheckInterview") -> 'answers' AS "answers", to_jsonb("HealthCheckInterview") ->> 'version' AS "version" FROM "HealthCheckInterview"`,
);

/**
 * A run's interviews, in the order they were held. One that can't be read (a
 * role the engine doesn't know, a date it can't parse) is left out rather than
 * stopping the run; its answers are read question by question later.
 */
export function interviewsFromRows(rows: Array<Record<string, unknown>>): Interview[] | null {
  const read = rows.flatMap((r) => {
    const version = r.version === null || r.version === undefined || r.version === "" ? undefined : Number(r.version);
    const parsed = Interview.safeParse({ id: r.id, role: r.role, heldOn: r.heldOn, notes: r.notes ?? "", answers: r.answers ?? {}, ...(version === undefined || !Number.isInteger(version) || version < 1 ? {} : { version }) });
    return parsed.success ? [parsed.data] : [];
  });
  return read.length ? read : null;
}

// The console's record of who the run's Drive folder is shared with (DECISIONS.md 252):
// one row a run, folder and address, written only by the worker.
export const DRIVE_SHARE: Columns = {
  runId: "text", email: "text", folderId: "text", permissionId: "text", state: "text", sharedAt: "ts", removedAt: "ts", removedWhy: "text", error: "text", updatedAt: "ts",
};

/** A share row, or null when it can't be read as one. */
export function shareFromRow(row: Record<string, unknown>): DriveShare | null {
  const state = row.state;
  if (state !== "shared" && state !== "removed" && state !== "failed") return null;
  const why = row.removedWhy;
  return {
    runId: String(row.runId),
    email: String(row.email),
    folderId: String(row.folderId),
    permissionId: (row.permissionId as string | null) ?? null,
    state,
    sharedAt: (row.sharedAt as string | null) ?? null,
    removedAt: (row.removedAt as string | null) ?? null,
    removedWhy: why === "reader_removed" || why === "folder_changed" || why === "retention" ? why : null,
    error: (row.error as string | null) ?? null,
    updatedAt: String(row.updatedAt),
  };
}

/** The microsite's readers, from its JSON list of { email, addedBy, addedAt }. Anything that isn't an address is left out. */
export function readersFromRow(row: Record<string, unknown> | undefined): string[] {
  const list = Array.isArray(row?.readers) ? (row!.readers as unknown[]) : [];
  return [...new Set(list.flatMap((r) => (r && typeof r === "object" && typeof (r as { email?: unknown }).email === "string" ? [(r as { email: string }).email.trim().toLowerCase()] : [])).filter((e) => e.includes("@")))];
}

export const CALIBRATION: Columns = {
  version: "num", orgId: "text", confirmedBy: "text", confirmedAt: "ts", stages: "json", scope: "json", users: "json", fields: "json", targets: "json",
  salesCycleAssumptionDays: "num", fiscalYearStartMonth: "num", analysisWindowMonths: "num", minSample: "num", dormantDays: "num", currency: "text",
  licenceCosts: "json", effortMinutes: "json",
};
// Service Cloud's limits, in a "service" JSONB column the console may add. Read
// through the row as JSON, as the survey version is, so the query works with
// the column and without it: missing reads as null, and the defaults apply (DECISIONS.md 165).
// Its "source" likewise: missing or null is a calibration a person confirmed (DECISIONS.md 688).
export const CALIBRATION_SQL = selectSql("HealthCheckCalibration", CALIBRATION, `"runId" = $1`).replace(
  ` FROM "HealthCheckCalibration"`,
  `, to_jsonb("HealthCheckCalibration") ->> 'source' AS "source", to_jsonb("HealthCheckCalibration") -> 'service' AS "service" FROM "HealthCheckCalibration"`,
);
/** An automatic run's calibration as the worker writes it (DECISIONS.md 692): every column it reads, and its source. */
export const CALIBRATION_WRITE: Columns = { runId: "text", ...CALIBRATION, source: "text" };
export function calibrationRow(runId: string, c: Calibration): Record<string, unknown> {
  // What an automatic run knows of its currency, and the locale's suggestion (DECISIONS.md 700), ride in the "fields" JSON column, so no migration: the console has no column of its own for it.
  const withCurrency = c.currencyFrom ? { ...c.fields, currencyFrom: c.currencyFrom, ...(c.currencySuggested ? { currencySuggested: c.currencySuggested } : {}) } : c.fields;
  // The model's "unsure" answers (DECISIONS.md 766) the same way: hashes only, and only when there are any.
  const fields = c.modelUnsure?.length ? { ...withCurrency, modelUnsure: c.modelUnsure } : withCurrency;
  return Object.fromEntries(Object.keys(CALIBRATION_WRITE).map((k) => [k, k === "runId" ? runId : k === "fields" ? fields : (c as Record<string, unknown>)[k]]));
}
/**
 * A saved automatic calibration's currency named from the org's settings (DECISIONS.md 745): the code and its
 * standing only, every other column and every other key of "fields" left as they are, and only while the row
 * is still the one the run read: the org's settings' with no person's confirmation, the same currency, and the
 * same standing, so two deliveries can't both write and a person's save is never written over.
 */
export const NAME_CALIBRATION_CURRENCY_SQL =
  `UPDATE "HealthCheckCalibration" SET "currency" = $2, "fields" = ("fields" - 'currencySuggested') || jsonb_build_object('currencyFrom', 'connect_api') ` +
  `WHERE "runId" = $1 AND "source" = 'org_settings' AND "confirmedBy" = $3 AND "currency" = $4 AND jsonb_typeof("fields") = 'object' AND "fields" ->> 'currencyFrom' = $5`;
/** A held pulse's would-be result, kept for a person's Show beside the run's losses in "budget", which no portal view reads (DECISIONS.md 746). */
export const KEEP_PULSE_SQL = `UPDATE "HealthCheckRun" SET "budget" = jsonb_set("budget"::jsonb, '{heldPulse}', $2::jsonb, true) WHERE "id" = $1`;
/** Where a pulse stands: its result, its kept result, its open flags. */
export const PULSE_STATE_SQL =
  `SELECT EXISTS (SELECT 1 FROM "HealthCheckPulseResult" p WHERE p."runId" = r."id") AS "hasResult", (r."budget"::jsonb -> 'heldPulse') IS NOT NULL AS "kept", ` +
  `COALESCE((SELECT array_agg(f."kind" ORDER BY f."kind") FROM "HealthCheckRunFlag" f WHERE f."runId" = r."id" AND f."clearedAt" IS NULL), '{}'::text[]) AS "openFlags" FROM "HealthCheckRun" r WHERE r."id" = $1`;
/** A run's stored overall score, through the row as JSON so a console without the column reads none. */
export const RUN_OVERALL_SCORE_SQL = `SELECT to_jsonb("HealthCheckRun") ->> 'overallScore' AS "score" FROM "HealthCheckRun" WHERE "id" = $1`;
/** A person's Show of a held pulse (746), in order, in one transaction: the run locked, its facts, the result, the kept copy gone, the flags cleared, the audit row. */
export const SHOW_PULSE_LOCK_SQL = `SELECT "id" FROM "HealthCheckRun" WHERE "id" = $1 FOR UPDATE`;
export const SHOW_PULSE_FACTS_SQL =
  `SELECT r."product", r."budget"::jsonb -> 'heldPulse' AS "kept", EXISTS (SELECT 1 FROM "HealthCheckPulseResult" p WHERE p."runId" = r."id") AS "hasResult", ` +
  `(SELECT to_jsonb(c) ->> 'source' FROM "HealthCheckCalibration" c WHERE c."runId" = r."id") AS "source", ` +
  `(SELECT d."madeAt" FROM "HealthCheckRunDigest" d WHERE d."runId" = r."id") AS "digestMadeAt", ` +
  `COALESCE((SELECT json_agg(json_build_object('kind', f."kind", 'counts', f."counts") ORDER BY f."kind") FROM "HealthCheckRunFlag" f WHERE f."runId" = r."id" AND f."clearedAt" IS NULL), '[]'::json) AS "openFlags" FROM "HealthCheckRun" r WHERE r."id" = $1`;
/** No ON CONFLICT: a result already there rolls the Show back. */
export const SHOW_PULSE_RESULT_SQL = `INSERT INTO "HealthCheckPulseResult" ("runId", "madeAt", "result") VALUES ($1, $2::timestamp(3), $3::jsonb)`;
export const SHOW_PULSE_UNKEEP_SQL = `UPDATE "HealthCheckRun" SET "budget" = "budget"::jsonb - 'heldPulse' WHERE "id" = $1`;
/** As the console's CLEAR_FLAGS_SQL and AUDIT_SQL (person-postgres.ts), word for word. */
export const SHOW_PULSE_CLEAR_FLAGS_SQL = `UPDATE "HealthCheckRunFlag" SET "clearedBy" = $2, "clearedAt" = $3::timestamp(3) WHERE "runId" = $1 AND "kind" = ANY($4::text[]) AND "clearedAt" IS NULL RETURNING "kind"`;
export const SHOW_PULSE_AUDIT_SQL = `INSERT INTO "audit_log" ("source", "action", "status", "correlation_id", "payload", "created_at") VALUES ($1, $2, 'ok', $3, $4::jsonb, $5::timestamp(3))`;
/** A pulse's result, replaced on a second delivery. */
export const PULSE_RESULT_UPSERT_SQL = `INSERT INTO "HealthCheckPulseResult" ("runId", "madeAt", "result") VALUES ($1, $2::timestamp(3), $3::jsonb) ON CONFLICT ("runId") DO UPDATE SET "madeAt" = EXCLUDED."madeAt", "result" = EXCLUDED."result"`;
/**
 * Who cleared a flag when code, not a person, settled it (746): an automatic customer's pulse released after an
 * earlier hold. Not an address, so never read as a person (the console's person actions take theirs from a verified
 * token), and in the console's own style for its code's acts ("Barometer Ongoing").
 */
export const CODE_CLEARED_BY = "Barometer worker";
/** A released pulse's open hold flags, cleared by code at the release (the console's CLEAR_FLAGS_SQL, its kinds the pulse's holds). */
export const RELEASED_PULSE_CLEAR_FLAGS_SQL = `UPDATE "HealthCheckRunFlag" SET "clearedBy" = $2, "clearedAt" = $3::timestamp(3) WHERE "runId" = $1 AND "kind" = ANY($4::text[]) AND "clearedAt" IS NULL RETURNING "kind"`;
/** The console's audit source for a person's actions on an automatic run (its person-store.ts AUDIT_SOURCE). */
export const PERSON_AUDIT_SOURCE = "health-check";

/** Why a held pulse can't be shown, or null when it can (746). One rule for both stores. */
export function pulseShowRefusal(f: { product: string; source: string | null; kept: boolean; hasResult: boolean; openFlags: readonly string[] } | null): string | null {
  if (!f) return "There's no such run.";
  if (f.product !== "pulse" || f.source !== "org_settings") return "Only an automatic customer's monthly scan is shown this way.";
  if (f.hasResult) return "This monthly scan is already shown.";
  if (f.openFlags.length === 0) return "This monthly scan isn't held.";
  if (!f.openFlags.every((k) => (SHOWABLE_PULSE_HOLDS as readonly string[]).includes(k))) return "This monthly scan's details didn't pass our checks, so it can't be shown: run it again.";
  // Kept once, and dropped by a later save of the run's progress: the content passed, but there's nothing to publish.
  if (!f.kept) return "Nothing was kept to show for this monthly scan: run it again.";
  return null;
}

/** A held pulse's would-be result as kept (746): with the time the held attempt made it, which the console orders pulses by. */
export type KeptPulse = { madeAt: string; result: PulseResult };
/** A kept result read back, or null when it isn't one we wrote whole. */
export function keptPulseOf(value: unknown): KeptPulse | null {
  const v = typeof value === "string" ? (() => { try { return JSON.parse(value) as unknown; } catch { return null; } })() : value;
  if (!v || typeof v !== "object") return null;
  const { madeAt, result } = v as { madeAt?: unknown; result?: unknown };
  return typeof madeAt === "string" && !Number.isNaN(Date.parse(madeAt)) && result && typeof result === "object" ? { madeAt, result: result as PulseResult } : null;
}

/** The order the console's Show names a hold by: the pulse's holds in PULSE_HOLD_REASONS' order. */
export const firstHold = (kinds: readonly string[]): string => [...kinds].sort((a, b) => PULSE_HOLD_REASONS.indexOf(a as PulseHoldReason) - PULSE_HOLD_REASONS.indexOf(b as PulseHoldReason))[0] ?? "";

/** Counts only, as the console's auditFlags writes them. */
const numbersOnlyOf = (counts: unknown): Record<string, number> =>
  counts && typeof counts === "object" && !Array.isArray(counts) ? Object.fromEntries(Object.entries(counts as Record<string, unknown>).filter((e): e is [string, number] => typeof e[1] === "number" && Number.isFinite(e[1]))) : {};

/** A held pulse's Show as "audit_log" keeps it: the console's PersonAudit shape, key for key (its person-store.ts). */
export function pulseShowAudit(runId: string, by: string, at: string, flags: ReadonlyArray<{ kind: string; counts: unknown }>, digestMadeAt: string | null) {
  return { action: "show_to_customer" as const, runId, by, at, state: firstHold(flags.map((f) => f.kind)), flags: flags.map((f) => ({ kind: f.kind, counts: numbersOnlyOf(f.counts) })), digestMadeAt };
}

/**
 * Whether a held pulse's output may still write its hold (746): never over a result already there. One got there
 * while this delivery ran, released by another delivery or shown by a person (both lock the run row, as this save
 * does, so they come strictly before or after it), and it stands: no flags reopened, nothing kept, nothing deleted.
 */
export const PULSE_HAS_RESULT_SQL = `SELECT EXISTS (SELECT 1 FROM "HealthCheckPulseResult" p WHERE p."runId" = $1) AS "shown"`;
/** An automatic run's hold (DECISIONS.md 693), read through the row as JSON like its mode. Asked only for an automatic run, so a consultant-led run's reads are unchanged. */
export const HELD_REASON_SQL = `SELECT to_jsonb("HealthCheckRun") ->> 'heldReason' AS "heldReason" FROM "HealthCheckRun" WHERE "id" = $1`;
/** A run's org, product, mode and baseline, for a pulse's baseline chain (DECISIONS.md 746); the mode through the row as JSON, as everywhere. */
export const RUN_LINEAGE_SQL = `SELECT "orgId", "product", "baselineRunId", to_jsonb("HealthCheckRun") ->> 'reviewMode' AS "reviewMode" FROM "HealthCheckRun" WHERE "id" = $1`;
/** A full check's review mode at connect, through the row as JSON (DECISIONS.md 688, 692). */
export const REVIEW_MODE_SQL = `SELECT to_jsonb("HealthCheckRun") ->> 'reviewMode' AS "reviewMode" FROM "HealthCheckRun" WHERE "id" = $1`;
/**
 * How every flag is raised (DECISIONS.md 693, 698): an open flag is left as it
 * is, so the first raise wins and a redelivery adds nothing; a flag a person
 * cleared is opened again, with the new time and counts and unposted, so a
 * run held or failed again for the same reason after "Run again" reaches a
 * person again. Who cleared it is the console's to have recorded elsewhere
 * first; both clear columns go null together, as its cleared check asks.
 */
export const FLAG_CONFLICT =
  `ON CONFLICT ("runId", "kind") DO UPDATE SET "raisedAt" = EXCLUDED."raisedAt", "counts" = EXCLUDED."counts", "postedAt" = NULL, "clearedBy" = NULL, "clearedAt" = NULL ` +
  `WHERE "HealthCheckRunFlag"."clearedAt" IS NOT NULL`;
/** The console's check also wants the run unreleased; held twice keeps the first reason. */
/**
 * The hold at connect and its flag in one statement (DECISIONS.md 692, 693):
 * the flag is raised only where this statement set the hold, so a redelivery
 * that finds the run held already raises nothing more, and a hold is never
 * recorded without its flag.
 */
export const HOLD_RUN_SQL =
  `WITH held AS (UPDATE "HealthCheckRun" SET "heldReason" = $2 WHERE "id" = $1 AND "reviewMode" = 'automatic' AND "heldReason" IS NULL AND "status" <> 'released' RETURNING "id"), ` +
  `flag AS (INSERT INTO "HealthCheckRunFlag" ("runId", "kind", "raisedAt", "counts") SELECT "id", $2, $3::timestamp(3), $4::jsonb FROM held ${FLAG_CONFLICT}) ` +
  `SELECT count(*)::int AS "held" FROM held`;
/** A flag's counts as written: whole, non-negative numbers under word-like names, and nothing else (DECISIONS.md 694, 700). */
export const countsOnly = (counts: Record<string, number>): Record<string, number> =>
  Object.fromEntries(Object.entries(counts).filter(([k, v]) => /^[a-z][A-Za-z]{0,39}$/.test(k) && Number.isSafeInteger(v) && v >= 0));
/** A flag for a person (migration 20261211): counts only, raised once while open (FLAG_CONFLICT). */
export const RAISE_FLAG_SQL = `INSERT INTO "HealthCheckRunFlag" ("runId", "kind", "raisedAt", "counts") VALUES ($1, $2, $3::timestamp(3), $4::jsonb) ${FLAG_CONFLICT}`;
/** A failed run's flag, only for an automatic run: read through the row as JSON, so a console without the column raises none. */
export const FAILURE_FLAG_SQL =
  `INSERT INTO "HealthCheckRunFlag" ("runId", "kind", "raisedAt", "counts") SELECT "id", 'run_failed', $2::timestamp(3), '{}'::jsonb FROM "HealthCheckRun" ` +
  `WHERE "id" = $1 AND to_jsonb("HealthCheckRun") ->> 'reviewMode' = 'automatic' ${FLAG_CONFLICT}`;
/** An automatic run stopped at its budget raises its flag (DECISIONS.md 715): only while the console has it automatic and stopped there. */
export const BUDGET_FLAG_SQL =
  `INSERT INTO "HealthCheckRunFlag" ("runId", "kind", "raisedAt", "counts") SELECT "id", 'stopped_budget', $2::timestamp(3), '{}'::jsonb FROM "HealthCheckRun" ` +
  `WHERE "id" = $1 AND "reviewMode" = 'automatic' AND "status" = 'stopped_budget' ${FLAG_CONFLICT}`;
/** An automatic run's hold with its results (DECISIONS.md 693). One row or the save rolls back. */
export const HOLD_OUTPUT_SQL = `UPDATE "HealthCheckRun" SET "heldReason" = $2 WHERE "id" = $1 AND "reviewMode" = 'automatic' AND "heldReason" IS NULL AND "status" <> 'released'`;
/** One release row by code: kind automatic, naming nobody (migration 20261211's check). */
export const RELEASE_ROW_SQL = `INSERT INTO "HealthCheckRelease" ("id", "runId", "output", "sectionIds", "kind", "releasedBy", "releasedAt") VALUES ($1, $2, $3, $4::text[], 'automatic', NULL, $5::timestamp(3))`;
/**
 * Last, once the digest and the release rows are in: the customer may see it.
 * Never a held run, and never touching the hold: only a person clears one
 * (DECISIONS.md 693). One row or the save rolls back.
 */
export const RELEASE_RUN_SQL = `UPDATE "HealthCheckRun" SET "status" = 'released' WHERE "id" = $1 AND "reviewMode" = 'automatic' AND "heldReason" IS NULL AND "status" NOT IN ('released', 'failed')`;
/**
 * One of the portal's documents (DECISIONS.md 698, migration 20261212),
 * replacing what an earlier make left for that run and output, and written
 * only while the run is a released, unheld automatic run: a run a person took
 * back meanwhile gets no row, and the make rolls back.
 */
export const PORTAL_DOCUMENT_SQL =
  `INSERT INTO "HealthCheckPortalDocument" ("runId", "output", "sealed", "madeAt", "expiresAt", "heldForUs") ` +
  `SELECT r."id", $2, $3::bytea, $4::timestamp(3), $5::timestamp(3), $6::boolean FROM "HealthCheckRun" r ` +
  `WHERE r."id" = $1 AND r."status" = 'released' AND r."reviewMode" = 'automatic' AND r."heldReason" IS NULL ` +
  `ON CONFLICT ("runId", "output") DO UPDATE SET "sealed" = EXCLUDED."sealed", "madeAt" = EXCLUDED."madeAt", "expiresAt" = EXCLUDED."expiresAt", "heldForUs" = EXCLUDED."heldForUs"`;
/** The clean-up (DECISIONS.md 698): times are written in UTC, so compared with UTC, as the console's view does. */
export const PORTAL_SWEEP_LOCK_TIMEOUT_SQL = `SET LOCAL lock_timeout = '2s'`;
export const PORTAL_SWEEP_STATEMENT_TIMEOUT_SQL = `SET LOCAL statement_timeout = '5s'`;
export const PORTAL_EXPIRED_SQL = `DELETE FROM "HealthCheckPortalDocument" WHERE "expiresAt" < (now() AT TIME ZONE 'UTC')`;
/** The org's last released run of the product, by its latest release row, other than this one (DECISIONS.md 693). */
export const LAST_RELEASED_SCORE_SQL =
  `SELECT r."overallScore" AS "score" FROM "HealthCheckRun" r JOIN "HealthCheckRelease" l ON l."runId" = r."id" ` +
  `WHERE r."id" <> $1 AND left(r."orgId", 15) = $2 AND r."product" = $3 AND r."status" = 'released' AND r."overallScore" IS NOT NULL ` +
  `GROUP BY r."id", r."overallScore" ORDER BY max(l."releasedAt") DESC LIMIT 1`;
/**
 * The org's previous automatic Barometer run, any status, and what it lost (DECISIONS.md 723): the newest started
 * before this run, never this run itself. By the org's customer (its unique "orgId15" index, then
 * "HealthCheckCustomerRun_customerId_idx" and the run's primary key), as the console's driver attaches every
 * automatic run to its org's customer; "orgId" has no index. A run not attached is not seen, so it is never compared.
 * One read-only statement.
 */
export const PREVIOUS_RUN_LOSSES_SQL =
  `SELECT r."id", r."budget" -> 'readsLost' AS "losses" FROM "HealthCheckCustomer" c ` +
  `JOIN "HealthCheckCustomerRun" cr ON cr."customerId" = c."id" JOIN "HealthCheckRun" r ON r."id" = cr."runId" ` +
  `WHERE c."orgId15" = $2 AND r."id" <> $1 AND left(r."orgId", 15) = $2 AND r."product" = 'barometer' AND r."reviewMode" = 'automatic' ` +
  `AND r."startedAt" < (SELECT t."startedAt" FROM "HealthCheckRun" t WHERE t."id" = $1) ORDER BY r."startedAt" DESC, r."id" DESC LIMIT 1`;
/**
 * What an automatic Barometer run lost, beside its budget in the run's own row (DECISIONS.md 723): the flag's counts
 * are numbers only and a flag is raised only on a hold, while this must be known for every run, a run that lost
 * nothing too. Written after the progress, which rewrites the budget whole, so any later progress save takes it away.
 */
export const RUN_LOSSES_SQL = `UPDATE "HealthCheckRun" SET "budget" = jsonb_set("budget", '{readsLost}', $2::jsonb, true) WHERE "id" = $1`;
/**
 * A run recorded failed. Its stored losses go with it (DECISIONS.md 723): a Run again that fails before its first
 * progress save would otherwise leave an earlier attempt's losses for the org's next run to compare with.
 */
export const RUN_FAILED_SQL = `UPDATE "HealthCheckRun" SET "status" = 'failed', "budget" = "budget" - 'readsLost' WHERE "id" = $1`;
export const AUTOMATIC_STATE_SQL =
  `SELECT r."status", r."queuedAt" IS NOT NULL AS "queued", to_jsonb(r) ->> 'heldReason' AS "heldReason", ` +
  `EXISTS (SELECT 1 FROM "HealthCheckCalibration" c WHERE c."runId" = r."id") AS "calibrated" FROM "HealthCheckRun" r WHERE r."id" = $1`;
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
/**
 * blackcloud-next migrations/20261122_health_check_handbook_summaries.sql
 * (DECISIONS.md 494). The worker writes every column of a waiting row; the
 * documents job reads only the status, items and counts (495).
 */
export const HANDBOOK_SUMMARIES: Columns = { runId: "text", status: "text", model: "text", madeAt: "ts", items: "json", leftOut: "json", decidedBy: "text", decidedAt: "ts" };
// Only what the handbook prints from: not the model or the time, nor who decided it or when.
const HANDBOOK_SUMMARIES_READ: Columns = { runId: "text", status: "text", items: "json", leftOut: "json" };
export const HANDBOOK_SUMMARIES_MIGRATION = "20261122_health_check_handbook_summaries.sql";
/** The console's migration that adds the run digest (DECISIONS.md 632). */
export const RUN_DIGEST_MIGRATION = "20261201_health_check_run_digest.sql";
/** One statement, so a second delivery or a remake replaces the row rather than failing on its key. */
export const RUN_DIGEST_DELETE_SQL = `DELETE FROM "HealthCheckRunDigest" WHERE "runId" = $1`;
export const RUN_DIGEST_SQL = `INSERT INTO "HealthCheckRunDigest" ("runId", "madeAt", "version", "digest") VALUES ($1, $2::timestamp(3), $3, $4::jsonb) ON CONFLICT ("runId") DO UPDATE SET "madeAt" = EXCLUDED."madeAt", "version" = EXCLUDED."version", "digest" = EXCLUDED."digest"`;
/** The run's team survey metric rows, by id, before the documents write the printed report's (DECISIONS.md 645). */
export const SURVEY_METRICS_DELETE_SQL = `DELETE FROM "HealthCheckMetric" WHERE "runId" = $1 AND "id" = ANY($2::text[])`;
/** The console's migration that adds the run's overall score and grade and the link's host (DECISIONS.md 626). */
export const RUN_SCORE_AND_HOST_MIGRATION = "20261128_health_check_run_score_and_host.sql";
/** The console's migration that makes HealthCheckRunDelivery (DECISIONS.md 722). */
export const RUN_DELIVERY_MIGRATION = "20261218_health_check_run_delivery.sql";
/** The console's migration adding HealthCheckRun's "worker" column (DECISIONS.md 774). */
export const RUN_WORKER_MIGRATION = "20261221_health_check_run_worker.sql";
/**
 * One statement of its own, outside every transaction, naming only the new
 * column: a console without it (42703) refuses this alone, and nothing else
 * the run writes ever names it (DECISIONS.md 774).
 */
export const RUN_WORKER_SQL = `UPDATE "HealthCheckRun" SET "worker" = $2::jsonb WHERE "id" = $1`;
/** The most steps a delivery's row keeps: well inside the migration's 32 KB CHECK (about 95 bytes a step). */
export const MAX_DELIVERY_STEPS = 100;
/** The row that stands for the steps a capped delivery left out: their time and calls added up, and how many. */
export const OTHER_STEPS = "other-steps";

/**
 * At most MAX_DELIVERY_STEPS: the slowest kept in their order, and the rest
 * added up into one OTHER_STEPS row with their count, so the delivery's time
 * and calls stay whole and nothing is dropped unsaid.
 */
export function capSteps(steps: readonly StepTiming[]): Array<StepTiming | (StepTiming & { count: number })> {
  if (steps.length <= MAX_DELIVERY_STEPS) return [...steps];
  const keep = new Set([...steps.keys()].sort((a, b) => steps[b]!.ms - steps[a]!.ms || a - b).slice(0, MAX_DELIVERY_STEPS - 1));
  const rest = steps.filter((_, i) => !keep.has(i));
  return [
    ...steps.filter((_, i) => keep.has(i)),
    { id: OTHER_STEPS, ms: rest.reduce((n, s) => n + s.ms, 0), calls: rest.reduce((n, s) => n + s.calls, 0), outcome: "done", count: rest.length },
  ];
}

/** One delivery's row. The run's own FOREIGN KEY refuses a run deleted meanwhile (23503), which is no loss. */
export const RUN_DELIVERY_SQL = `INSERT INTO "HealthCheckRunDelivery" ("runId", "startedAt", "endedAt", "outcome", "final", "again", "steps") VALUES ($1, $2::timestamp(3), $3::timestamp(3), $4, $5, $6, $7::jsonb)`;
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
  const { analysisFrom, analysisTo, connectionName: _name, preparedBy: _by, scope, reviewMode, ...rest } = row;
  return Run.parse({
    ...rest,
    ...(scope === null || scope === undefined ? {} : { scope }),
    // No column yet, or null: consultant-led, as every run before decision 688 was. Anything else must be a mode the engine knows.
    ...(reviewMode === null || reviewMode === undefined ? {} : { reviewMode }),
    analysisWindow: { from: analysisFrom, to: analysisTo },
    steps,
  });
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
  const stored = withoutNulls(row) as Record<string, unknown>;
  // The model's "unsure" answers ride in "fields" (DECISIONS.md 766): lifted out only on a calibration read from the org's settings, and only as a list of hashes.
  const unsure = (stored.fields as Record<string, unknown> | undefined)?.modelUnsure;
  const read = stored.source === "org_settings" && Array.isArray(unsure) && unsure.length > 0 && unsure.length <= 2_000 && unsure.every((k) => typeof k === "string" && /^[0-9a-f]{16}$/.test(k)) ? { ...stored, modelUnsure: unsure } : stored;
  // Lifted out of "fields" (calibrationRow), and only on a calibration read from the org's settings: a person's never carries one.
  const fields = read.fields as Record<string, unknown> | undefined;
  const from = fields?.currencyFrom;
  const suggested = fields?.currencySuggested;
  // A currency the customer named (KAN-41), or the org's settings (740): its code is the column's own, and nothing is suggested beside it.
  if (read.source === "org_settings" && (from === "customer" || from === "connect_api")) return Calibration.parse({ ...read, currencyFrom: from });
  if (read.source === "org_settings" && (from === "org" || from === "unread")) return Calibration.parse({ ...read, currencyFrom: from, ...(typeof suggested === "string" && /^[A-Z]{3}$/.test(suggested) ? { currencySuggested: suggested } : {}) });
  return Calibration.parse(read);
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
/** A delivery's claim on its run (DECISIONS.md 538), word for word as the console's side of it is written against. */
const CLAIM_RUN_SQL = `UPDATE "HealthCheckRun" SET "status" = CASE WHEN "status" = 'awaiting_manual' THEN 'analysing' ELSE "status" END WHERE "id" = $1 AND "status" NOT IN ('in_review', 'released') RETURNING "status"`;
/**
 * DECISIONS.md 610: the latest moment another free scan was made for the org
 * (its orgId, set at connect or by the console) or connected to it (a link's
 * connectedOrgId), after $3. Org ids compare on 15 characters, so either form matches.
 */
/**
 * The currency an org's customer named in the portal (KAN-41): the newest
 * answer of the active customer whose org this is. The console keeps every
 * answer (who and when), so the newest is the one that stands.
 */
export const CUSTOMER_CURRENCY_SQL =
  `SELECT a."value" FROM "HealthCheckCustomerAnswer" a JOIN "HealthCheckCustomer" c ON c."id" = a."customerId" ` +
  `WHERE c."orgId15" = $1 AND c."status" = 'active' AND a."question" = 'currency' ORDER BY a."answeredAt" DESC, a."id" DESC LIMIT 1`;

/** The customer's newest answer to which closed stages are lost and which no decision (DECISIONS.md 756): as the currency's, another question. */
export const CUSTOMER_STAGE_ANSWER_SQL =
  `SELECT a."value" FROM "HealthCheckCustomerAnswer" a JOIN "HealthCheckCustomer" c ON c."id" = a."customerId" ` +
  `WHERE c."orgId15" = $1 AND c."status" = 'active' AND a."question" = 'closed_stages' ORDER BY a."answeredAt" DESC, a."id" DESC LIMIT 1`;
/** The org's stages as the worker last saved them for this run (DECISIONS.md 611, 756). */
export const SAVED_ORG_STAGES_SQL = `SELECT "stages" FROM "HealthCheckOrgStages" WHERE "runId" = $1`;
/**
 * The customer's answer put into a saved automatic calibration (DECISIONS.md 756): the stages only, and only while
 * the row is still the one the run read: the org's settings' with no person's confirmation and exactly those stages,
 * so two deliveries can't both write and a person's save is never written over.
 */
/**
 * The same guarded write with the model's "unsure" answers beside the stages (DECISIONS.md 766), in "fields", every
 * other key of it left as it is. Used only on a run whose model route is on, so every other write is the statement above.
 */
export const ANSWER_CALIBRATION_STAGES_UNSURE_SQL =
  `UPDATE "HealthCheckCalibration" SET "stages" = $2::jsonb, "fields" = ((CASE WHEN jsonb_typeof("fields") = 'object' THEN "fields" ELSE '{}'::jsonb END) - 'modelUnsure') || $5::jsonb ` +
  `WHERE "runId" = $1 AND "source" = 'org_settings' AND "confirmedBy" = $3 AND "stages"::jsonb = $4::jsonb`;
export const ANSWER_CALIBRATION_STAGES_SQL =
  `UPDATE "HealthCheckCalibration" SET "stages" = $2::jsonb ` +
  `WHERE "runId" = $1 AND "source" = 'org_settings' AND "confirmedBy" = $3 AND "stages"::jsonb = $4::jsonb`;
/** One stage as saveOrgStages writes it. */
const safeJson = (text: string): unknown => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};
const SavedOrgStage = z.object({ apiName: z.string(), label: z.string(), isActive: z.boolean(), isClosed: z.boolean(), isWon: z.boolean(), sortOrder: z.number().nullable() });

export const LAST_SCAN_SQL = `SELECT to_char(MAX("at"), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "at" FROM (` +
  `SELECT r."startedAt" AS "at" FROM "HealthCheckRun" r WHERE r."product" = 'scan' AND r."id" <> $2 AND LEFT(r."orgId", 15) = $1 AND r."startedAt" > $3::timestamp(3)` +
  ` UNION ALL ` +
  `SELECT l."usedAt" AS "at" FROM "HealthCheckConnectLink" l JOIN "HealthCheckRun" r ON r."id" = l."runId" WHERE r."product" = 'scan' AND r."id" <> $2 AND LEFT(l."connectedOrgId", 15) = $1 AND l."usedAt" > $3::timestamp(3)` +
  `) AS "scans"`;
const one = (r: SqlResult): Record<string, unknown> | undefined => r.rows[0];
/** An error's SQLSTATE, and nothing else of it: five characters, or a word saying there was none. */
const sqlState = (code: unknown): string => (typeof code === "string" && /^[0-9A-Z]{5}$/.test(code) ? code : "with no code");

export class PostgresWorkerStore implements WorkerStore {
  readonly #pool: SqlPool;
  readonly #log: (line: string) => void;
  /** Said once: a console without the summaries' table says so on every save and every make otherwise. */
  #saidNoSummariesTable = false;
  #saidNoOrgStagesTable = false;
  #saidNoAnswersTable = false;
  #saidUnknownAnswer = false;
  #saidNoStageAnswersTable = false;

  constructor(pool: SqlPool, options: { log?: (line: string) => void } = {}) {
    this.#pool = pool;
    this.#log = options.log ?? (() => {});
  }

  #noSummariesTable(): void {
    if (this.#saidNoSummariesTable) return;
    this.#saidNoSummariesTable = true;
    this.#log(`The console's database has no HealthCheckHandbookSummaries table yet, so Claude's summaries are neither saved nor printed. Apply the console's migration ${HANDBOOK_SUMMARIES_MIGRATION}.`);
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
   * A query on one of the console's survey or interview tables. A database
   * without the table yet (Postgres 42P01) has no rows, so a worker deployed
   * before that migration still runs every check rather than failing them.
   */
  async #surveyQuery(text: string, runId: string): Promise<SqlResult> {
    try {
      return await this.#pool.query(text, [runId]);
    } catch (err) {
      if ((err as { code?: string }).code === "42P01") return { rows: [], rowCount: 0 };
      throw err;
    }
  }

  /** The run's interviews. Without the table, there are none (DECISIONS.md 180). */
  async #interviews(runId: string): Promise<Interview[] | null> {
    return interviewsFromRows((await this.#surveyQuery(INTERVIEW_SQL, runId)).rows);
  }

  async loadShareJob(runId: string): Promise<ShareJobInput | null> {
    const run = one(await this.#pool.query(`SELECT "status" FROM "HealthCheckRun" WHERE "id" = $1`, [runId]));
    if (!run) return null;
    const [documents, access, release] = await Promise.all([
      this.#pool.query(`SELECT "driveFolderId" FROM "HealthCheckDocuments" WHERE "runId" = $1`, [runId]),
      this.#surveyQuery(`SELECT "readers" FROM "HealthCheckMicrositeAccess" WHERE "runId" = $1`, runId),
      // The last release, as the retention sweep reads it.
      this.#pool.query(`SELECT to_char(MAX("releasedAt"), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "releasedAt" FROM "HealthCheckRelease" WHERE "runId" = $1`, [runId]),
    ]);
    const releasedAt = one(release)?.releasedAt;
    // The share table is the console's newest: without it, nothing can be recorded, so nothing is shared.
    let shares: SqlResult;
    try {
      shares = await this.#pool.query(selectSql("HealthCheckDriveShare", DRIVE_SHARE, `"runId" = $1`, `"email", "folderId"`), [runId]);
    } catch (err) {
      if ((err as { code?: string }).code !== "42P01") throw err;
      return { runId, released: run.status === "released", releasedAt: null, folderId: null, readers: [], shares: [], recordable: false };
    }
    return {
      runId,
      released: run.status === "released",
      releasedAt: typeof releasedAt === "string" ? releasedAt : null,
      folderId: (one(documents)?.driveFolderId as string | null | undefined) ?? null,
      readers: readersFromRow(one(access)),
      shares: shares.rows.flatMap((r) => shareFromRow(r) ?? []),
      recordable: true,
    };
  }

  async saveShare(share: DriveShare): Promise<void> {
    const { text, values } = insertSql("HealthCheckDriveShare", DRIVE_SHARE, share);
    const update = Object.keys(DRIVE_SHARE).filter((c) => !["runId", "folderId", "email"].includes(c)).map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ");
    await this.#pool.query(`${text} ON CONFLICT ("runId", "folderId", "email") DO UPDATE SET ${update}`, values);
  }

  /** The run's survey: sent or not, and its answers. Without either table, it wasn't sent. */
  async #survey(runId: string): Promise<Survey | null> {
    const [sent, answers] = await Promise.all([this.#surveyQuery(selectSql("HealthCheckSurvey", SURVEY_SENT, `"runId" = $1`), runId), this.#surveyQuery(SURVEY_RESPONSE_SQL, runId)]);
    return surveyFromRows(answers.rows, sent.rows.length > 0);
  }

  async loadRunJob(runId: string): Promise<RunJobInput | null> {
    const row = one(await this.#pool.query(runSql(RUN), [runId]));
    if (!row) return null;
    const [steps, intake, calibration, answers, survey, interviews] = await Promise.all([
      this.#pool.query(selectSql("HealthCheckRunStep", RUN_STEP, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckIntake", INTAKE, `"runId" = $1`), [runId]),
      this.#pool.query(CALIBRATION_SQL, [runId]),
      this.#pool.query(selectSql("HealthCheckManualAnswer", MANUAL_ANSWER, `"runId" = $1`, `"answeredAt", "stepKey"`), [runId]),
      this.#survey(runId),
      this.#interviews(runId),
    ]);
    const run = runFromRows(row, steps.rows.map(({ runId: _r, ...s }) => s));
    const held = run.reviewMode === "automatic" ? one(await this.#pool.query(HELD_REASON_SQL, [runId]))?.heldReason : undefined;
    return {
      run,
      ...(run.reviewMode === "automatic" ? { heldReason: typeof held === "string" ? held : null } : {}),
      intake: one(intake) ? intakeFromRow(one(intake)!, run) : null,
      calibration: one(calibration) ? calibrationFromRow(one(calibration)!) : null,
      connectionName: (row.connectionName as string | null) ?? null,
      answers: answers.rows.map(answerFromRow),
      survey,
      interviews,
    };
  }

  /**
   * One statement, not a read and a write: the console's move to review isn't in
   * the worker's transaction, and a row lock would be gone before the folder is
   * written anyway. Only awaiting manual steps changes, the one status the
   * console moves to review from; a fresh run keeps its status until its first
   * progress write, so its progress reads as it did (DECISIONS.md 538).
   */
  async claimRun(runId: string): Promise<void> {
    const claimed = await this.#pool.query(CLAIM_RUN_SQL, [runId]);
    if (claimed.rows.length > 0) return;
    // No row: closed, or gone. Read afterwards only to say which.
    const row = one(await this.#pool.query(`SELECT "status" FROM "HealthCheckRun" WHERE "id" = $1`, [runId]));
    if (!row) throw new Error(`No run ${runId}`);
    throw new RunClosedError(runId, String(row.status));
  }

  /** Locks the run's row for the transaction, and refuses one the console has moved on. */
  async #openRun(client: SqlClient, runId: string, refuseFailed = false): Promise<void> {
    const row = one(await client.query(`SELECT "status" FROM "HealthCheckRun" WHERE "id" = $1 FOR UPDATE`, [runId]));
    if (!row) throw new Error(`No run ${runId}`);
    if (CLOSED.has(String(row.status))) throw new RunClosedError(runId, String(row.status));
    // An automatic run's results never land on a run that failed meanwhile: only a person runs it again (DECISIONS.md 693).
    if (refuseFailed && String(row.status) === "failed") throw new RunClosedError(runId, "failed");
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
    // Held to the console's schema before any SQL: a row it can't read would hold the run's release.
    const summaries = output.handbookSummaries ? waitingRow(runId, output.handbookSummaries) : null;
    if (summaries && !summaries.ok) this.#log(`Claude's summaries for run ${runId} don't fit the console's limits (${summaries.issues} problems), so they aren't saved with its results.`);
    if (summaries?.ok && summaries.dropped) this.#log(`${summaries.dropped} of Claude's summaries for run ${runId} hold something that can't be stored or printed as it stands, so they're counted as not written.`);
    return this.#transaction(async (client) => {
      await this.#openRun(client, runId, output.automatic !== undefined);
      await this.#writeProgress(client, runId, output.progress, output.runDir);
      if (output.readsLost) await client.query(RUN_LOSSES_SQL, [runId, JSON.stringify(output.readsLost)]);
      // Replaced whole, so a second delivery leaves the same rows. Deleting a
      // section takes its versions and proposals with it (ON DELETE CASCADE).
      for (const table of ["HealthCheckFinding", "HealthCheckMetric", "HealthCheckScore", "HealthCheckManualStep", "HealthCheckSection"]) {
        await client.query(`DELETE FROM ${q(table)} WHERE "runId" = $1`, [runId]);
      }
      for (const f of output.findings) await this.#insert(client, "HealthCheckFinding", FINDING, findingRow(runId, f));
      for (const m of output.metrics) await this.#insert(client, "HealthCheckMetric", METRIC, metricRow(runId, m));
      // The rows were all deleted above, so these need no delete of their own.
      if (output.surveyMetrics?.length) await this.#writeSurveyMetrics(client, runId, output.surveyMetrics, false);
      for (const s of output.scores) await this.#insert(client, "HealthCheckScore", SCORE, scoreRow(runId, s));
      for (const s of output.manualSteps) await this.#insert(client, "HealthCheckManualStep", MANUAL_STEP, manualStepRow(runId, s));
      for (const s of output.sections) await this.#insert(client, "HealthCheckSection", SECTION, sectionRow(s));
      for (const v of output.versions) await this.#insert(client, "HealthCheckSectionVersion", VERSION, versionRow(v));
      await this.#writeFacts(client, runId, output.facts, output.factsFile);
      if (summaries?.ok && summaries.row) await this.#writeSummaries(client, summaries.row);
      if (output.inventory) await this.#writeInventory(client, runId, output.inventory);
      if (output.complexity) await this.#writeComplexity(client, runId, output.complexity);
      // A release writes its digest itself, with nothing to fall back to (DECISIONS.md 693).
      if (output.digest !== undefined && output.automatic?.decision !== "release") await this.#writeDigest(client, runId, output.digest);
      // Last, and each alone, so a console without the columns saves as before (DECISIONS.md 626).
      if (output.overall !== undefined) {
        const said = { subject: `Run ${runId}`, table: "HealthCheckRun", kept: "its results are" };
        await this.#newerColumn(client, "run_overall_score", { ...said, column: "overallScore", lost: "the run's overall score" }, `UPDATE "HealthCheckRun" SET "overallScore" = $2 WHERE "id" = $1`, [runId, output.overall?.score ?? null]);
        await this.#newerColumn(client, "run_overall_grade", { ...said, column: "overallGrade", lost: "the run's grade" }, `UPDATE "HealthCheckRun" SET "overallGrade" = $2 WHERE "id" = $1`, [runId, output.overall?.grade ?? null]);
      }
      // Last of all, inside the same transaction: nothing is visible to the customer until all of it commits.
      if (output.automatic) await this.#writeAutomatic(client, runId, output);
      // An automatic customer's pulse held for a person (746): no result, so the portal never lists it, and its flags.
      if (output.pulseHold) {
        const h = output.pulseHold;
        if (one(await client.query(PULSE_HAS_RESULT_SQL, [runId]))?.shown === true) {
          this.#log(`Pulse ${runId} was shown or released while this delivery ran, so its hold here was left unwritten and the result stands.`);
        } else {
          // A showable hold's would-be result, for a person's Show; after the progress above, which writes "budget" whole.
          if (h.kept) await client.query(KEEP_PULSE_SQL, [runId, JSON.stringify(h.kept)]);
          for (const f of [{ kind: h.reason, counts: h.counts }, ...h.also]) await client.query(RAISE_FLAG_SQL, [runId, f.kind, h.at, JSON.stringify(f.counts)]);
        }
      }
    });
  }

  /**
   * An automatic run's release or hold (DECISIONS.md 693), with no savepoint:
   * any refusal rolls the whole save back, so a run is never released with
   * part of what the customer reads, nor held without its flag. Released: the
   * digest first, then a release row per output naming nobody, then the status.
   * Held: the reason, then its flag and each other failed check's (DECISIONS.md 715), each raised once.
   */
  async #writeAutomatic(client: SqlClient, runId: string, output: RunOutputRecord): Promise<void> {
    const a = output.automatic!;
    if (a.decision === "hold") {
      const held = await client.query(HOLD_OUTPUT_SQL, [runId, a.reason]);
      // No row: the run is released, not automatic, or already held, and the first hold stands. The save rolls back, and the next delivery finds the hold and skips.
      if (held.rowCount !== 1) throw new Error(`Run ${runId} couldn't be held: the console doesn't have it as an unreleased, unheld automatic run`);
      for (const f of [{ kind: a.reason, counts: a.counts }, ...(a.also ?? [])]) await client.query(RAISE_FLAG_SQL, [runId, f.kind, a.at, JSON.stringify(f.counts)]);
      return;
    }
    const digest = output.digest;
    if (!digest) throw new Error(`Run ${runId} has no digest, so it isn't released`);
    const bad = unstorableAt(digest, [], DIGEST_KEYS);
    if (bad) throw new Error(`Run ${runId}'s digest holds ${bad.what} at ${bad.at}, which the console's database can't store, so it isn't released`);
    await client.query(RUN_DIGEST_SQL, [runId, digest.madeAt, digest.version, JSON.stringify(digest)]);
    for (const r of a.releases) {
      if (r.kind !== "automatic" || r.releasedBy !== null) throw new Error(`Run ${runId}'s release rows must be automatic and name nobody`);
      await client.query(RELEASE_ROW_SQL, [r.id, runId, r.output, r.sectionIds, r.releasedAt]);
    }
    const released = await client.query(RELEASE_RUN_SQL, [runId]);
    if (released.rowCount !== 1) throw new Error(`Run ${runId} couldn't be released: the console doesn't have it as an unreleased, unheld automatic run`);
  }

  async lastReleasedScore(runId: string, orgId15: string, product: string): Promise<number | null> {
    const row = one(await this.#pool.query(LAST_RELEASED_SCORE_SQL, [runId, orgId15, product]));
    const score = row?.score === undefined || row.score === null ? null : Number(row.score);
    return score !== null && Number.isFinite(score) ? score : null;
  }

  async previousRunLosses(runId: string, orgId15: string): Promise<{ runId: string; losses: RunLosses | null } | null> {
    const row = one(await this.#pool.query(PREVIOUS_RUN_LOSSES_SQL, [runId, orgId15]));
    if (!row) return null;
    return { runId: String(row.id), losses: readLosses(row.losses) };
  }

  async pulseState(runId: string): Promise<PulseState | null> {
    const row = one(await this.#pool.query(PULSE_STATE_SQL, [runId]));
    if (!row) return null;
    return { hasResult: row.hasResult === true, kept: row.kept === true, openFlags: Array.isArray(row.openFlags) ? row.openFlags.map(String) : [] };
  }

  async runOverallScore(runId: string): Promise<number | null> {
    const value = one(await this.#pool.query(RUN_OVERALL_SCORE_SQL, [runId]))?.score;
    const n = typeof value === "string" ? Number(value) : typeof value === "number" ? value : NaN;
    return Number.isFinite(n) ? n : null;
  }

  async showHeldPulse(runId: string, by: string, at: string): Promise<{ ok: true; flagsCleared: string[] } | { ok: false; why: string }> {
    return this.#transaction(async (client) => {
      if ((await client.query(SHOW_PULSE_LOCK_SQL, [runId])).rows.length === 0) return { ok: false as const, why: "There's no such run." };
      const row = one(await client.query(SHOW_PULSE_FACTS_SQL, [runId]));
      const flagsOf = (v: unknown): Array<{ kind: string; counts: unknown }> => {
        const list = typeof v === "string" ? (JSON.parse(v) as unknown) : v;
        return Array.isArray(list) ? list.filter((f): f is { kind: string; counts: unknown } => !!f && typeof (f as { kind?: unknown }).kind === "string") : [];
      };
      const flags = row ? flagsOf(row.openFlags) : [];
      const kept = row ? keptPulseOf(row.kept) : null;
      const facts = row ? { product: String(row.product), source: typeof row.source === "string" ? row.source : null, kept: kept !== null, hasResult: row.hasResult === true, openFlags: flags.map((f) => f.kind) } : null;
      const why = pulseShowRefusal(facts);
      if (why) return { ok: false as const, why };
      // The held attempt's own time, so a month shown late never reads as the latest.
      await client.query(SHOW_PULSE_RESULT_SQL, [runId, kept!.madeAt, JSON.stringify(kept!.result)]);
      await client.query(SHOW_PULSE_UNKEEP_SQL, [runId]);
      const cleared = (await client.query(SHOW_PULSE_CLEAR_FLAGS_SQL, [runId, by, at, facts!.openFlags])).rows.map((r: Record<string, unknown>) => String(r.kind)).sort();
      const digestMadeAt = row!.digestMadeAt instanceof Date ? row!.digestMadeAt.toISOString() : typeof row!.digestMadeAt === "string" ? new Date(`${row!.digestMadeAt}${/Z|[+-]\d\d:?\d\d$/.test(row!.digestMadeAt) ? "" : "Z"}`).toISOString() : null;
      await client.query(SHOW_PULSE_AUDIT_SQL, [PERSON_AUDIT_SOURCE, "show_to_customer", runId, JSON.stringify(pulseShowAudit(runId, by, at, flags, digestMadeAt)), at]);
      return { ok: true as const, flagsCleared: cleared };
    });
  }

  async runLineage(runId: string): Promise<RunLineage | null> {
    const row = one(await this.#pool.query(RUN_LINEAGE_SQL, [runId]));
    if (!row || typeof row.orgId !== "string" || typeof row.product !== "string") return null;
    return { orgId: row.orgId, product: row.product, reviewMode: row.reviewMode === "automatic" ? "automatic" : "consultant", baselineRunId: typeof row.baselineRunId === "string" ? row.baselineRunId : null };
  }

  async automaticRunState(runId: string): Promise<AutomaticRunState | null> {
    const row = one(await this.#pool.query(AUTOMATIC_STATE_SQL, [runId]));
    if (!row) return null;
    return { calibrated: row.calibrated === true, queued: row.queued === true, held: typeof row.heldReason === "string", status: String(row.status) };
  }

  /**
   * One of the console's newer columns (DECISIONS.md 626), written by a
   * statement of its own inside a savepoint of its own, after every write the
   * save made before it, none of which names it. A database without the column
   * (class 42, 42703 among them) or whose check refuses the value (class 23)
   * goes back to the savepoint and says so by the id, the table, the column and
   * the SQLSTATE alone, never a value, and the rest commits as before. Anything
   * else is thrown, so the whole save rolls back as it does today (615).
   */
  async #newerColumn(client: SqlClient, savepoint: string, said: { subject: string; table: string; column: string; lost: string; kept: string }, text: string, values: unknown[]): Promise<void> {
    await client.query(`SAVEPOINT ${savepoint}`);
    try {
      await client.query(text, values);
    } catch (err) {
      const code = this.#sideTableFault(err);
      await client.query(`ROLLBACK TO SAVEPOINT ${savepoint}`);
      this.#log(`${said.subject}: the console's ${said.table} table refused its "${said.column}" column with SQLSTATE ${code}, so ${said.lost} isn't saved; ${said.kept}. Check the table against the console's ${RUN_SCORE_AND_HOST_MIGRATION} migration.`);
      return;
    }
    await client.query(`RELEASE SAVEPOINT ${savepoint}`);
  }

  /**
   * Claude's summaries, waiting for a person (DECISIONS.md 494), in one
   * statement so nothing can decide the row between a check and the write: a
   * new row, or a waiting row's text and counts replaced, its strikes with
   * them. A row a person has decided is left as it is. Nothing is deleted:
   * a delivery with no summaries leaves an earlier waiting row alone. The
   * summaries never cost a run its results: any error on the statement goes
   * back to the savepoint and the rest of the run saves. A console without
   * the table (42P01) is said once; any other error each time, by its
   * SQLSTATE alone, since its message can quote a value. If going back to the
   * savepoint fails too, the connection is gone and the whole save with it.
   */
  async #writeSummaries(client: SqlClient, row: ConsoleSummariesRow): Promise<void> {
    const { text, values } = insertSql("HealthCheckHandbookSummaries", HANDBOOK_SUMMARIES, row);
    await client.query("SAVEPOINT handbook_summaries");
    try {
      await client.query(
        `${text} ON CONFLICT ("runId") DO UPDATE SET "model" = EXCLUDED."model", "madeAt" = EXCLUDED."madeAt", "items" = EXCLUDED."items", "leftOut" = EXCLUDED."leftOut" WHERE "HealthCheckHandbookSummaries"."status" = 'waiting'`,
        values,
      );
    } catch (err) {
      await client.query("ROLLBACK TO SAVEPOINT handbook_summaries");
      const code = (err as { code?: unknown }).code;
      if (code === "42P01") this.#noSummariesTable();
      else this.#log(`Claude's summaries for run ${row.runId} couldn't be saved (Postgres error ${sqlState(code)}), so they aren't saved with its results.`);
      return;
    }
    await client.query("RELEASE SAVEPOINT handbook_summaries");
  }

  /** The run's summaries row for the handbook (DECISIONS.md 495), never who decided it. Without the table, or a row that reads, none. */
  async #summaries(runId: string): Promise<StoredSummaries | null> {
    let result: SqlResult;
    try {
      result = await this.#pool.query(selectSql("HealthCheckHandbookSummaries", HANDBOOK_SUMMARIES_READ, `"runId" = $1`), [runId]);
    } catch (err) {
      if ((err as { code?: string }).code !== "42P01") throw err;
      this.#noSummariesTable();
      return null;
    }
    const row = one(result);
    if (!row) return null;
    const read = storedFromRow(row);
    if (read.ok) return read.stored;
    this.#log(`Run ${runId}'s row of Claude's summaries doesn't read as the console's (${read.issues} problems), so the org handbook prints none.`);
    return null;
  }

  /**
   * The facts summary and the whole file, replaced whole (blackcloud-next
   * migrations/20261120_health_check_run_facts.sql and
   * 20261121_health_check_run_facts_file.sql). The table is the console's
   * newest and the file column newer still: without the table (42P01) the
   * run's results still save, and without the column (42703) the summary does
   * too, so the statements run inside a savepoint the transaction can go back to.
   */
  async #writeFacts(client: SqlClient, runId: string, facts: RunOutputRecord["facts"], file: RunOutputRecord["factsFile"]): Promise<void> {
    const write = async (withFile: boolean): Promise<void> => {
      await client.query(`DELETE FROM "HealthCheckRunFacts" WHERE "runId" = $1`, [runId]);
      if (!facts) return;
      if (withFile) await client.query(`INSERT INTO "HealthCheckRunFacts" ("runId", "checkedAt", "facts", "file") VALUES ($1, $2::timestamp(3), $3::jsonb, $4::jsonb)`, [runId, facts.checkedAt, JSON.stringify(facts), file ? JSON.stringify(file) : null]);
      else await client.query(`INSERT INTO "HealthCheckRunFacts" ("runId", "checkedAt", "facts") VALUES ($1, $2::timestamp(3), $3::jsonb)`, [runId, facts.checkedAt, JSON.stringify(facts)]);
    };
    await client.query("SAVEPOINT run_facts");
    try {
      await write(true);
    } catch (err) {
      const code = this.#sideTableFault(err);
      await client.query("ROLLBACK TO SAVEPOINT run_facts");
      if (code !== "42703") return this.#sideTableSkipped(runId, "HealthCheckRunFacts", code, "facts");
      // A console without the file column: the summary as before.
      try {
        await write(false);
      } catch (again) {
        const retried = this.#sideTableFault(again);
        await client.query("ROLLBACK TO SAVEPOINT run_facts");
        return this.#sideTableSkipped(runId, "HealthCheckRunFacts", retried, "facts");
      }
    }
    await client.query("RELEASE SAVEPOINT run_facts");
  }

  /**
   * The org's inventory, replaced whole (DECISIONS.md 605), as the facts are:
   * DELETE then INSERT inside a savepoint, so a console whose table is missing
   * or doesn't match (615) still gets the run's results, and says so once a
   * process. Any other error rolls the whole save back, as the facts' does.
   */
  async #writeInventory(client: SqlClient, runId: string, inventory: Inventory): Promise<void> {
    await client.query("SAVEPOINT run_inventory");
    try {
      await client.query(`DELETE FROM "HealthCheckInventory" WHERE "runId" = $1`, [runId]);
      await client.query(`INSERT INTO "HealthCheckInventory" ("runId", "madeAt", "inventory") VALUES ($1, $2::timestamp(3), $3::jsonb)`, [runId, inventory.madeAt, JSON.stringify(inventory)]);
    } catch (err) {
      const code = this.#sideTableFault(err);
      await client.query("ROLLBACK TO SAVEPOINT run_inventory");
      return this.#sideTableSkipped(runId, "HealthCheckInventory", code, "inventory");
    }
    await client.query("RELEASE SAVEPOINT run_inventory");
  }

  /**
   * The org's complexity score (DECISIONS.md 614), replaced whole as the
   * inventory is: a console whose table is missing or doesn't match (615)
   * still gets the run's results, and says so once a process. Any other error
   * rolls the save back.
   */
  async #writeComplexity(client: SqlClient, runId: string, complexity: Complexity): Promise<void> {
    await client.query("SAVEPOINT run_complexity");
    try {
      await client.query(`DELETE FROM "HealthCheckComplexity" WHERE "runId" = $1`, [runId]);
      await client.query(`INSERT INTO "HealthCheckComplexity" ("runId", "madeAt", "complexity") VALUES ($1, $2::timestamp(3), $3::jsonb)`, [runId, complexity.madeAt, JSON.stringify(complexity)]);
    } catch (err) {
      const code = this.#sideTableFault(err);
      await client.query("ROLLBACK TO SAVEPOINT run_complexity");
      return this.#sideTableSkipped(runId, "HealthCheckComplexity", code, "complexity");
    }
    await client.query("RELEASE SAVEPOINT run_complexity");
  }

  /**
   * The run's digest for the customer's connector (DECISIONS.md 632), as the
   * side tables are written (615, 618): its own savepoint, so a console
   * without the table or column (class 42), one that refuses the row (class
   * 23) or a value its jsonb refuses (class 22) costs the digest only, said for
   * the run by its id, the table and the SQLSTATE; anything else rolls the
   * whole save back to be tried again. A string jsonb would refuse (a NUL, half
   * of a UTF-16 pair) is caught before the statement, as the inventory's is,
   * and said by its path alone. With no digest to write, or one that wasn't
   * written, any earlier row is deleted the same way, so the connector never
   * serves figures the run's results or documents no longer hold.
   */
  async #writeDigest(client: SqlClient, runId: string, digest: RunDigest | null): Promise<void> {
    if (digest) {
      const bad = unstorableAt(digest, [], DIGEST_KEYS);
      if (bad) this.#log(`Run ${runId}: its digest holds ${bad.what} at ${bad.at}, which the console's database can't store, so the digest isn't saved; its results are.`);
      else {
        const code = await this.#digestStatement(client, runId, RUN_DIGEST_SQL, [runId, digest.madeAt, digest.version, JSON.stringify(digest)]);
        // Written, or no table to hold an earlier one.
        if (code === null || code === "42P01") return;
      }
    }
    await this.#digestStatement(client, runId, RUN_DIGEST_DELETE_SQL, [runId]);
  }

  /** One digest statement in its own savepoint: null when it ran, else the SQLSTATE it was skipped for. */
  async #digestStatement(client: SqlClient, runId: string, text: string, values: unknown[]): Promise<string | null> {
    await client.query("SAVEPOINT run_digest");
    try {
      await client.query(text, values);
    } catch (err) {
      const code = (err as { code?: unknown } | null)?.code;
      if (typeof code !== "string" || !/^(42|23|22)[0-9A-Z]{3}$/.test(code)) throw err;
      await client.query("ROLLBACK TO SAVEPOINT run_digest");
      this.#sideTableSkipped(runId, "HealthCheckRunDigest", code, "digest");
      return code;
    }
    await client.query("RELEASE SAVEPOINT run_digest");
    return null;
  }

  /**
   * The team survey's metrics (DECISIONS.md 645), in a savepoint of their own
   * after the org's: a console whose metric table refuses one (class 23), or
   * doesn't match (42), or a value it can't hold (22), costs the survey's rows
   * alone, said by the run's id, the table and the SQLSTATE, and the run's
   * results or its documents' row commit. Anything else rolls the save back.
   * `replace` first deletes the run's survey rows, for a write that comes after
   * the run's own (the documents'), so the later survey's figures stand.
   */
  async #writeSurveyMetrics(client: SqlClient, runId: string, metrics: Metric[], replace: boolean): Promise<void> {
    await client.query("SAVEPOINT survey_metrics");
    try {
      if (replace) await client.query(SURVEY_METRICS_DELETE_SQL, [runId, SURVEY_METRIC_IDS]);
      for (const m of metrics) await this.#insert(client, "HealthCheckMetric", METRIC, metricRow(runId, m));
    } catch (err) {
      const code = (err as { code?: unknown } | null)?.code;
      if (typeof code !== "string" || !/^(42|23|22)[0-9A-Z]{3}$/.test(code)) throw err;
      await client.query("ROLLBACK TO SAVEPOINT survey_metrics");
      this.#log(`Run ${runId}: the console's HealthCheckMetric table refused the team survey's metrics with SQLSTATE ${code}, so they aren't saved; the run's own are.`);
      return;
    }
    await client.query("RELEASE SAVEPOINT survey_metrics");
  }

  /**
   * A side table's error that means the console's table doesn't match what we
   * write (class 42) or refuses the row (class 23), as its SQLSTATE; anything
   * else, a lost connection or a deadlock among them, is thrown again so the
   * whole save rolls back and the delivery is tried again (DECISIONS.md 615).
   */
  #sideTableFault(err: unknown): string {
    const code = (err as { code?: unknown } | null)?.code;
    if (typeof code === "string" && /^(42|23)[0-9A-Z]{3}$/.test(code)) return code;
    throw err;
  }

  /**
   * Said for every run it happens to, with the run's id, the table and the
   * SQLSTATE alone: the message can quote a value. Once a process (615) hid
   * every refusal after the first, so a run's missing row couldn't be traced
   * (DECISIONS.md 618).
   */
  #sideTableSkipped(runId: string, table: string, code: string, migration: string): void {
    this.#log(
      code === "42P01"
        ? `Run ${runId}: the console's database has no ${table} table yet, so the run's ${migration} isn't saved. Apply the console's ${migration} migration.`
        : `Run ${runId}: the console's ${table} table refused the run's row with SQLSTATE ${code}, so the run's ${migration} isn't saved; its results are. Check the table against the console's ${migration} migration.`,
    );
  }

  recordRunFailure(runId: string, reason: string, at: string): Promise<void> {
    return this.#transaction(async (client) => {
      await this.#openRun(client, runId);
      await client.query(RUN_FAILED_SQL, [runId]);
      await client.query(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1 AND "id" = 'worker'`, [runId]);
      await this.#insert(client, "HealthCheckRunStep", RUN_STEP, { runId, id: "worker", version: 1, status: "failed", calls: 0, finishedAt: at, error: reason });
      // An automatic run that fails is a person's to look at (DECISIONS.md 693), in its own savepoint:
      // a console without the flag table (class 42) still records the failure, and says so.
      await client.query("SAVEPOINT run_failed_flag");
      try {
        await client.query(FAILURE_FLAG_SQL, [runId, at]);
        await client.query("RELEASE SAVEPOINT run_failed_flag");
      } catch (err) {
        const code = (err as { code?: unknown } | null)?.code;
        if (typeof code !== "string" || !/^42[0-9A-Z]{3}$/.test(code)) throw err;
        await client.query("ROLLBACK TO SAVEPOINT run_failed_flag");
        this.#log(`Run ${runId}: the console's database has no HealthCheckRunFlag table it can write (SQLSTATE ${code}), so the failure raises no flag. Apply the console's ${REVIEW_MODE_MIGRATION} migration.`);
      }
    });
  }

  async flagBudgetStop(runId: string, at: string): Promise<boolean> {
    return (await this.#pool.query(BUDGET_FLAG_SQL, [runId, at])).rowCount === 1;
  }

  /** The SQLSTATEs already said, once each per worker process (DECISIONS.md 722). */
  #saidDelivery = new Set<string>();

  /**
   * DECISIONS.md 722. A courtesy: no table yet (42P01), no rights on it
   * (42501) or a row its CHECKs refuse (23514) costs only the run times and is
   * said once per worker process, never per delivery; a run gone (23503)
   * costs nothing. The steps are capped first (capSteps), so a run with very
   * many steps is cut honestly rather than refused.
   */
  async recordDelivery(d: RunDelivery): Promise<boolean> {
    try {
      await this.#pool.query(RUN_DELIVERY_SQL, [d.runId, d.startedAt, d.endedAt, d.outcome, d.final, d.again, JSON.stringify(capSteps(d.steps))]);
      return true;
    } catch (err) {
      const code = (err as { code?: unknown } | null)?.code;
      if (code === "23503") return false;
      if (code !== "42P01" && code !== "42501" && code !== "23514") throw err;
      if (!this.#saidDelivery.has(code)) {
        this.#saidDelivery.add(code);
        this.#log(
          code === "42P01"
            ? `The console's database has no HealthCheckRunDelivery table yet, so run times aren't kept. Apply the console's ${RUN_DELIVERY_MIGRATION} migration.`
            : `The console's HealthCheckRunDelivery table refused a delivery's row with SQLSTATE ${code}, so run times aren't kept. Check the table and its rights against the console's ${RUN_DELIVERY_MIGRATION} migration.`,
        );
      }
      return false;
    }
  }

  #saidWorker = new Set<string>();

  /**
   * DECISIONS.md 774. A courtesy, as the delivery's row: the row is checked
   * against the schema the console reads before it's sent, and any error is
   * said by its SQLSTATE alone, once per worker process for a missing column
   * (42703), and the run goes on. Never throws.
   */
  async recordRunWorker(runId: string, worker: WorkerAtRun): Promise<boolean> {
    try {
      const row = WorkerAtRun.parse(worker);
      const result = await this.#pool.query(RUN_WORKER_SQL, [runId, JSON.stringify(row)]);
      return result.rowCount === 1;
    } catch (err) {
      const code = sqlState((err as { code?: unknown } | null)?.code);
      if (!this.#saidWorker.has(code)) {
        this.#saidWorker.add(code);
        this.#log(
          code === "42703"
            ? `The console's HealthCheckRun table has no "worker" column yet, so runs don't record this worker's switches and build. Apply the console's ${RUN_WORKER_MIGRATION} migration.`
            : `A run's record of this worker's switches and build couldn't be saved (SQLSTATE ${code}), so runs may not show them; each run goes on as before.`,
        );
      }
      return false;
    }
  }

  /** DECISIONS.md 652: a step row, so no schema change. Not #openRun: a scan lets go after its results, in review or not. */
  noteScanRevoke(runId: string, confirmed: boolean, at: string): Promise<void> {
    return this.#transaction(async (client) => {
      await client.query(`DELETE FROM "HealthCheckRunStep" WHERE "runId" = $1 AND "id" = $2`, [runId, DISCONNECT_STEP]);
      if (!confirmed) await this.#insert(client, "HealthCheckRunStep", RUN_STEP, { runId, id: DISCONNECT_STEP, version: 1, status: "failed", calls: 0, finishedAt: at, error: SCAN_REVOKE_NOT_CONFIRMED });
    });
  }

  /** DECISIONS.md 652: the link's run, only when its product reads as scan. As the cap's join (610). */
  async scanRunOfLink(linkId: string): Promise<string | null> {
    const row = one(await this.#pool.query(`SELECT r."id", r."product" FROM "HealthCheckConnectLink" l JOIN "HealthCheckRun" r ON r."id" = l."runId" WHERE l."id" = $1`, [linkId]));
    return row?.product === "scan" && typeof row.id === "string" ? row.id : null;
  }

  async runState(runId: string): Promise<{ product: string; status: string } | null> {
    const row = one(await this.#pool.query(`SELECT "product", "status" FROM "HealthCheckRun" WHERE "id" = $1`, [runId]));
    return row && typeof row.product === "string" && typeof row.status === "string" ? { product: row.product, status: row.status } : null;
  }

  async connectionProducts(names: string[]): Promise<Map<string, Array<{ runId: string; product: string }>>> {
    const found = new Map<string, Array<{ runId: string; product: string }>>();
    if (names.length === 0) return found;
    const { rows } = await this.#pool.query(`SELECT "connectionName", "id", "product" FROM "HealthCheckRun" WHERE "connectionName" = ANY($1::text[])`, [names]);
    for (const row of rows) {
      if (typeof row.connectionName !== "string" || typeof row.id !== "string" || typeof row.product !== "string") continue;
      found.set(row.connectionName, [...(found.get(row.connectionName) ?? []), { runId: row.id, product: row.product }]);
    }
    return found;
  }

  async loadConnectJob(linkId: string): Promise<ConnectJobInput | null> {
    const row = one(await this.#pool.query(selectSql("HealthCheckConnectLink", CONNECT_LINK, `"id" = $1`), [linkId]));
    if (!row) return null;
    const run = one(await this.#pool.query(`SELECT "orgId", "id", "product" FROM "HealthCheckRun" WHERE "id" = (SELECT "runId" FROM "HealthCheckConnectLink" WHERE "id" = $1)`, [linkId]));
    if (typeof run?.orgId !== "string") return null;
    const linked = linkFromRow(row, run.orgId);
    const job = typeof run.id === "string" ? { ...linked, runId: run.id } : linked;
    if (run.product === "scan" && typeof run.id === "string") return { ...job, scanRunId: run.id };
    // A full check's review mode, read through the row as JSON so the query works
    // before the console's migration (DECISIONS.md 688, 692). Asked only for a
    // full check, so a scan's connect sends exactly what it did before.
    if (run.product !== "barometer" || typeof run.id !== "string") return job;
    const mode = one(await this.#pool.query(REVIEW_MODE_SQL, [run.id]));
    return mode?.reviewMode === "automatic" ? { ...job, automaticRunId: run.id } : job;
  }

  /**
   * One statement (DECISIONS.md 692): the calibration with its source, only
   * where the run has none, so a second connect never replaces it. A console
   * without the "source" column (42703) gets nothing: the row would read as a
   * person's confirmation. Any other error is thrown by its SQLSTATE alone.
   */
  async saveCalibration(runId: string, calibration: Calibration): Promise<"saved" | "exists" | "no_source_column"> {
    const { text, values } = insertSql("HealthCheckCalibration", CALIBRATION_WRITE, calibrationRow(runId, calibration));
    try {
      const result = await this.#pool.query(`${text} ON CONFLICT ("runId") DO NOTHING`, values);
      return result.rowCount === 1 ? "saved" : "exists";
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (code === "42703") return "no_source_column";
      throw new StoreError(`the console's database refused the calibration (SQLSTATE ${sqlState(code)})`);
    }
  }

  /** DECISIONS.md 745: one guarded statement; a code off the list is never sent. */
  async nameCalibrationCurrency(runId: string, was: CurrencyWas, code: string): Promise<"updated" | "unchanged"> {
    if (!isCurrencyCode(code)) throw new StoreError("the currency to save isn't a code on the engine's list");
    try {
      const result = await this.#pool.query(NAME_CALIBRATION_CURRENCY_SQL, [runId, code, ORG_SETTINGS_CALIBRATOR, was.currency, was.currencyFrom]);
      return result.rowCount === 1 ? "updated" : "unchanged";
    } catch (err) {
      throw new StoreError(`the console's database refused the calibration's currency (SQLSTATE ${sqlState((err as { code?: unknown }).code)})`);
    }
  }

  /**
   * The currency the org's customer named (KAN-41), or null. A console
   * without the answers table (42P01) has none, said once a process; any
   * other error is thrown by its SQLSTATE alone.
   */
  async customerCurrency(orgId15: string): Promise<string | null> {
    try {
      const value = one(await this.#pool.query(CUSTOMER_CURRENCY_SQL, [orgId15.slice(0, 15)]))?.value;
      if (value === undefined || value === null) return null;
      if (isCurrencyCode(value)) return value;
      // A stored code off the engine's list (a console on a newer list): ignored, never the run's currency, and said once.
      if (!this.#saidUnknownAnswer) {
        this.#saidUnknownAnswer = true;
        this.#log("A customer's stored currency isn't on the engine's list of currencies, so it was ignored and the amounts are shown with none. Check the list in src/schemas/calibration.ts against the console's copy.");
      }
      return null;
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (code !== "42P01") throw new StoreError(`the console's database refused the customer's answer (SQLSTATE ${sqlState(code)})`);
      if (!this.#saidNoAnswersTable) {
        this.#saidNoAnswersTable = true;
        this.#log("The console's database has no HealthCheckCustomerAnswer table yet, so no customer's currency is read. Apply the console's 20261219_health_check_portal_answers.sql migration.");
      }
      return null;
    }
  }

  /** DECISIONS.md 756: the stored value as it is, for the engine to read and check. No table yet is none, said once. */
  async customerStageAnswer(orgId15: string): Promise<unknown> {
    try {
      return one(await this.#pool.query(CUSTOMER_STAGE_ANSWER_SQL, [orgId15.slice(0, 15)]))?.value ?? null;
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (code !== "42P01") throw new StoreError(`the console's database refused the customer's answer about its closed stages (SQLSTATE ${sqlState(code)})`);
      if (!this.#saidNoStageAnswersTable) {
        this.#saidNoStageAnswersTable = true;
        this.#log("The console's database has no HealthCheckCustomerAnswer table yet, so no customer's answer about its closed stages is read. Apply the console's 20261219_health_check_portal_answers.sql migration.");
      }
      return null;
    }
  }

  /** DECISIONS.md 756: a row in a shape we don't know, or no table, is none. */
  async savedOrgStages(runId: string): Promise<OrgStage[] | null> {
    let value: unknown;
    try {
      value = one(await this.#pool.query(SAVED_ORG_STAGES_SQL, [runId]))?.stages;
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (code === "42P01") return null;
      throw new StoreError(`the console's database refused the org's saved stages (SQLSTATE ${sqlState(code)})`);
    }
    const parsed = z.array(SavedOrgStage).safeParse(typeof value === "string" ? safeJson(value) : value);
    return parsed.success ? parsed.data : null;
  }

  /** DECISIONS.md 756: one guarded statement. */
  async answerCalibrationStages(runId: string, was: CalibratedStage[], stages: CalibratedStage[], modelUnsure?: string[]): Promise<"updated" | "unchanged"> {
    try {
      const result =
        modelUnsure === undefined
          ? await this.#pool.query(ANSWER_CALIBRATION_STAGES_SQL, [runId, JSON.stringify(stages), ORG_SETTINGS_CALIBRATOR, JSON.stringify(was)])
          : await this.#pool.query(ANSWER_CALIBRATION_STAGES_UNSURE_SQL, [runId, JSON.stringify(stages), ORG_SETTINGS_CALIBRATOR, JSON.stringify(was), JSON.stringify(modelUnsure.length ? { modelUnsure } : {})]);
      return result.rowCount === 1 ? "updated" : "unchanged";
    } catch (err) {
      throw new StoreError(`the console's database refused the calibration's stages (SQLSTATE ${sqlState((err as { code?: unknown }).code)})`);
    }
  }

  /** DECISIONS.md 692: one statement, only an automatic run's and only where none is set. A console without the columns, or refusing the word, is said by its SQLSTATE alone. */
  async holdRun(runId: string, reason: "calibration_unreadable", at: string, counts: Record<string, number> = {}): Promise<boolean> {
    try {
      // Numbers only, as every flag's counts (DECISIONS.md 700: how many stages were read, and how many are won).
      const result = await this.#pool.query(HOLD_RUN_SQL, [runId, reason, at, JSON.stringify(countsOnly(counts))]);
      return Number(one(result)?.held) === 1;
    } catch (err) {
      this.#log(`Run ${runId}: the console's HealthCheckRun or HealthCheckRunFlag table refused the hold with SQLSTATE ${sqlState((err as { code?: unknown }).code)}, so the hold isn't recorded. Check the table against the console's ${REVIEW_MODE_MIGRATION} migration.`);
      return false;
    }
  }

  /** DECISIONS.md 610: the columns that exist, so no migration. Both halves leave out the scan asking. */
  async lastScanOfOrg(orgId15: string, exceptRunId: string, since: string): Promise<string | null> {
    const row = one(await this.#pool.query(LAST_SCAN_SQL, [orgId15, exceptRunId, since]));
    return typeof row?.at === "string" ? row.at : null;
  }

  /**
   * The org's stages, replaced whole (DECISIONS.md 611): DELETE then INSERT
   * in a transaction of their own, outside every other write. A console
   * without the table (42P01) says so once a process; any other error is
   * thrown by its SQLSTATE alone, because a driver's message can quote a value.
   */
  async saveOrgStages(runId: string, stages: OrgStage[], readAt: string): Promise<boolean> {
    try {
      await this.#transaction(async (client) => {
        await client.query(`DELETE FROM "HealthCheckOrgStages" WHERE "runId" = $1`, [runId]);
        await client.query(`INSERT INTO "HealthCheckOrgStages" ("runId", "readAt", "stages") VALUES ($1, $2::timestamp(3), $3::jsonb)`, [runId, readAt, JSON.stringify(stages)]);
      });
      return true;
    } catch (err) {
      const code = (err as { code?: unknown }).code;
      if (code !== "42P01") throw new StoreError(`the console's database refused it (SQLSTATE ${sqlState(code)})`);
      if (!this.#saidNoOrgStagesTable) {
        this.#saidNoOrgStagesTable = true;
        this.#log("The console's database has no HealthCheckOrgStages table yet, so the org's stages aren't saved for the calibration. Apply the console's org stages migration.");
      }
      return false;
    }
  }

  markLinkUsed(linkId: string, c: LinkConnected, instanceHost?: string | null): Promise<void> {
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
      // The org's My Domain, last and alone (DECISIONS.md 626): only one the console's check takes, and never logged.
      if (isMyDomainHost(instanceHost)) {
        const said = { subject: `Link ${linkId}`, table: "HealthCheckConnectLink", column: "instanceHost", lost: "the org's address", kept: "the link is marked used" };
        await this.#newerColumn(client, "link_instance_host", said, `UPDATE "HealthCheckConnectLink" SET "instanceHost" = $2 WHERE "id" = $1`, [linkId, instanceHost]);
      }
    });
  }

  async refuseLink(linkId: string, reason: string, at: string): Promise<void> {
    const result = await this.#pool.query(
      `UPDATE "HealthCheckConnectLink" SET "state" = 'refused', "refusedReason" = $2, "refusedAt" = $3::timestamp(3), "sealedPending" = NULL, "sealedCode" = NULL WHERE "id" = $1 AND "state" = 'pending'`,
      [linkId, reason, at],
    );
    if (result.rowCount !== 1) throw new Error(`Link ${linkId} wasn't pending, so it wasn't refused`);
  }

  async loadDocumentsJob(runId: string, reads: DocumentsJobReads = {}): Promise<DocumentsJobInput | null> {
    const row = one(await this.#pool.query(runSql(RUN_DOCUMENTS), [runId]));
    if (!row) return null;
    const [steps, intake, sections, versions, overrides, documents, survey, summaries] = await Promise.all([
      this.#pool.query(selectSql("HealthCheckRunStep", RUN_STEP, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckIntake", INTAKE, `"runId" = $1`), [runId]),
      this.#pool.query(selectSql("HealthCheckSection", SECTION, `"runId" = $1`, `"output", "order"`), [runId]),
      this.#pool.query(selectSql("HealthCheckSectionVersion", VERSION, `"id" IN (SELECT "currentVersionId" FROM "HealthCheckSection" WHERE "runId" = $1)`), [runId]),
      this.#pool.query(selectSql("HealthCheckNumberOverride", OVERRIDE, `"runId" = $1`, `"seq"`), [runId]),
      this.#pool.query(selectSql("HealthCheckDocuments", DOCUMENTS, `"runId" = $1`), [runId]),
      this.#survey(runId),
      // With the switch off, no statement names the table.
      reads.handbookSummaries === true ? this.#summaries(runId) : Promise.resolve(null),
    ]);
    const run = runFromRows(row, steps.rows.map(({ runId: _r, ...s }) => s));
    const doc = one(documents);
    // An automatic run's hold, read as the run job reads it; a consultant-led run's reads are unchanged (DECISIONS.md 698).
    const held = run.reviewMode === "automatic" ? one(await this.#pool.query(HELD_REASON_SQL, [runId]))?.heldReason : undefined;
    return {
      run,
      ...(run.reviewMode === "automatic" ? { heldReason: typeof held === "string" ? held : null } : {}),
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
      handbookSummaries: summaries,
    };
  }

  async startDocuments(runId: string, at: string): Promise<boolean> {
    const result = await this.#pool.query(
      `UPDATE "HealthCheckDocuments" SET "status" = 'making', "startedAt" = $2::timestamp(3), "error" = NULL WHERE "runId" = $1 AND "status" IN ('queued', 'making')`,
      [runId, at],
    );
    return result.rowCount === 1;
  }

  async saveDocuments(runId: string, made: DocumentsMade, digest?: RunDigest | null, surveyMetrics?: Metric[]): Promise<boolean> {
    const text = `UPDATE "HealthCheckDocuments" SET "status" = 'done', "finishedAt" = $2::timestamp(3), "error" = NULL, "warnings" = $3::jsonb, "clientName" = $4, "folderName" = $5, "driveFolderId" = $6, "driveFolderUrl" = $7, "files" = $8::jsonb WHERE "runId" = $1 AND "status" = 'making'`;
    const values = [runId, made.at, JSON.stringify(made.warnings), made.clientName, made.folderName, made.driveFolderId, made.driveFolderUrl, JSON.stringify(made.files)];
    if (digest === undefined && surveyMetrics === undefined) return (await this.#pool.query(text, values)).rowCount === 1;
    // The documents' row, the digest and the survey's metrics commit together,
    // so a lost connection leaves the make to be tried again with all of them.
    // Only a make that marked the row done writes them, or deletes the digest:
    // the console may have moved the row on meanwhile.
    let marked = false;
    await this.#transaction(async (client) => {
      marked = (await client.query(text, values)).rowCount === 1;
      if (!marked) return;
      if (digest !== undefined) await this.#writeDigest(client, runId, digest);
      if (surveyMetrics !== undefined) await this.#writeSurveyMetrics(client, runId, surveyMetrics, true);
    });
    return marked;
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

  async savePulseResult(runId: string, result: PulseResult, madeAt: string, options: { gated?: boolean } = {}): Promise<void> {
    if (!options.gated) {
      // blackcloud-next's HealthCheckPulseResult ("runId" PK, "madeAt", "result" JSONB), as its scan result is.
      await this.#pool.query(PULSE_RESULT_UPSERT_SQL, [runId, madeAt, JSON.stringify(result)]);
      return;
    }
    // An automatic customer's pulse released by code (746): the run row locked first, so this and a person's Show come one after the other.
    await this.#transaction(async (client) => {
      await client.query(SHOW_PULSE_LOCK_SQL, [runId]);
      await client.query(PULSE_RESULT_UPSERT_SQL, [runId, madeAt, JSON.stringify(result)]);
      await client.query(RELEASED_PULSE_CLEAR_FLAGS_SQL, [runId, CODE_CLEARED_BY, madeAt, [...PULSE_HOLD_REASONS]]);
      await client.query(SHOW_PULSE_UNKEEP_SQL, [runId]);
    });
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

  async savePortalDocuments(runId: string, rows: PortalDocumentRow[], heldCounts: Record<string, number> | null): Promise<void> {
    await this.#transaction(async (client) => {
      for (const row of rows) {
        const result = await client.query(PORTAL_DOCUMENT_SQL, [runId, row.output, row.sealed, row.madeAt, row.expiresAt, row.heldForUs]);
        if (result.rowCount !== 1) throw new Error(`Run ${runId} isn't a released, unheld automatic run, so its portal documents weren't saved`);
      }
      if (heldCounts) await client.query(RAISE_FLAG_SQL, [runId, DOCUMENTS_HELD_FLAG, rows[0]?.madeAt ?? new Date(0).toISOString(), JSON.stringify(heldCounts)]);
    });
  }

  async deleteExpiredPortalDocuments(_now: string): Promise<number | null> {
    try {
      // Short limits of its own, so a locked table never holds up the documents job it runs before (DECISIONS.md 698).
      return await this.#transaction(async (client) => {
        await client.query(PORTAL_SWEEP_LOCK_TIMEOUT_SQL);
        await client.query(PORTAL_SWEEP_STATEMENT_TIMEOUT_SQL);
        return (await client.query(PORTAL_EXPIRED_SQL)).rowCount ?? 0;
      });
    } catch (err) {
      if ((err as { code?: string }).code === "42P01") return null;
      throw err;
    }
  }

  async failDocuments(runId: string, reason: string, at: string): Promise<void> {
    await this.#pool.query(`UPDATE "HealthCheckDocuments" SET "status" = 'failed', "finishedAt" = $2::timestamp(3), "error" = $3 WHERE "runId" = $1 AND "status" IN ('queued', 'making')`, [runId, at, reason]);
  }
}

/** Only for the column check in the tests. */
export const ALL_TABLES = { RUN, RUN_STEP, INTAKE, CALIBRATION, MANUAL_ANSWER, FINDING, METRIC, SCORE, MANUAL_STEP, SECTION, VERSION, CONNECT_LINK, OVERRIDE, DOCUMENTS, BENCHMARK_ROW } as const;
