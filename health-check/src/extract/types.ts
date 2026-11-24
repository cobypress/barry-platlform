import { AsyncLocalStorage } from "node:async_hooks";
import type { Calibration, RefusedRead } from "../schemas/index.ts";
import { refusalSentence } from "../salesforce/refusal-words.ts";
import { classifyError, isUnavailable, type QueryApi, type QueryOptions, type SalesforceClient, type SalesforceError } from "../salesforce/client.ts";
import type { ApexSourceStore } from "./apex-source.ts";
import type { CodeLintTools } from "./code-lint.ts";

export type SourceInfo =
  | { kind: "soql"; api: QueryApi; query: string }
  | { kind: "rest"; path: string }
  | { kind: "metadata"; path: string };

// A read either worked or says why not. "We couldn't read it" and "there's
// nothing there" must never look alike, because the first means not measurable.
// `missingFields` names optional fields the org doesn't have, which were left
// out of the query. Rules that need one are not measurable, and name it.
export type Sourced<T> =
  | { status: "ok"; data: T; source: SourceInfo; truncated: boolean; missingFields?: string[] }
  | { status: "unavailable"; reason: string; source: SourceInfo };

/** Page-level checkpoint, so a big extract interrupted part-way doesn't start again. */
export interface PartialStore {
  read<R>(key: string): R[];
  append<R>(key: string, rows: R[]): void;
  clear(key: string): void;
}

export type ExtractContext = {
  client: SalesforceClient;
  calibration: Calibration;
  now: Date;
  windowStart: Date;
  partial: PartialStore;
  pageSize: number;
  /** The output of an extractor that ran earlier in this run. */
  get<T>(extractor: Extractor<T>): T;
  /**
   * Where the Apex step keeps the redacted code of what Claude may summarise
   * (DECISIONS.md 491). Only the worker gives one, with its switch on: an
   * extractor never reads the environment, and the command line never keeps code.
   */
  apexSource?: ApexSourceStore;
  /**
   * PMD where the worker found it, for the code-lint step (DECISIONS.md 670).
   * Only the worker gives it, on a health check.
   */
  codeLint?: CodeLintTools;
  /**
   * Who connected, where the caller knows (DECISIONS.md 498): the id kept with
   * an OAuth token, or the username of an `sf` login. Given afresh on every
   * start and resume, and never written into the run folder.
   */
  connectedUser?: ConnectedUser;
};

export type ConnectedUser = { id?: string; username?: string };

// One extractor is one step: resumable alone, and one BullMQ job in Milestone 5.
// Bump `version` when its output changes, so a resumed run doesn't reuse old data.
export type Extractor<T = unknown> = {
  id: string;
  version: number;
  /** PartialStore keys to clear once this extractor's output is saved. */
  partialKeys?: string[];
  /** Reads CRM records, even as counts, so a set-up review never runs it. */
  records?: true;
  run(ctx: ExtractContext): Promise<T>;
  /**
   * What every figure and check that reads this step's output must say about
   * it, such as rows left out (DECISIONS.md 702). Nothing when there's nothing to say.
   */
  notes?(output: T): string[];
};

export function unavailable<T>(source: SourceInfo, err: unknown): Sourced<T> {
  if (isUnavailable(err)) return { status: "unavailable", reason: refusalReason(err, readName(source)), source };
  throw err;
}

// Where a step's refused reads are noted while it runs (DECISIONS.md 702):
// the run sets it around each step, and keeps what's noted in run.json and
// its log. Nothing a customer reads holds a code.
const refusals = new AsyncLocalStorage<(read: Omit<RefusedRead, "step">) => void>();

/** Runs a step, passing each read Salesforce refused with a code we didn't take before 702 to `note`. */
export function noteRefusals<T>(note: (read: Omit<RefusedRead, "step">) => void, run: () => Promise<T>): Promise<T> {
  return refusals.run(note, run);
}

/** What a read was of, named for a customer: the object a query reads, or what the call was for. */
export function readName(source: SourceInfo): string {
  if (source.kind === "soql") {
    // Subqueries first, so the outer FROM is the one named.
    let query = source.query;
    for (let before = ""; before !== query; ) [before, query] = [query, query.replace(/\([^()]*\)/g, "")];
    const object = /\bFROM\s+([A-Za-z0-9_]+)/i.exec(query)?.[1];
    return object ?? "This information";
  }
  if (source.kind === "metadata") return source.path.startsWith("Tooling API") ? "The same set-up through the Tooling API" : "The org's set-up from the Metadata API";
  if (/recordCount/.test(source.path)) return "The org's record counts";
  return "This information";
}

/**
 * Why a read Salesforce refused costs its checks (DECISIONS.md 702): our own
 * sentence, naming what couldn't be read and, where the code says, why, for
 * every code alike. Salesforce's code and status are noted for the run's log
 * and run.json. Its message isn't kept anywhere: it can quote a record's id, a
 * value or a name, and none of that leaves the step's own read.
 */
export function refusalReason(err: SalesforceError, given: string): string {
  // Where we only know the kind of read, the object Salesforce named is better.
  const named = /sObject type '([A-Za-z][A-Za-z0-9_]*)' is not supported/.exec(err.message)?.[1];
  const what = named && !/^[A-Za-z][A-Za-z0-9_]*$/.test(given) ? named : given;
  // For a missing field, its API name alone, so we can see which without Salesforce's message.
  const field = err.code === "INVALID_FIELD" ? /No such column '([A-Za-z][A-Za-z0-9_]{0,79})'/.exec(err.message)?.[1] : undefined;
  refusals.getStore()?.({ read: what, code: err.code.replace(/[^A-Za-z0-9_]/g, "").slice(0, 60) || "UNKNOWN", status: err.status, ...(field ? { field } : {}) });
  const kind = classifyError(err);
  return refusalSentence(what, kind === "not_available" || kind === "no_permission" ? kind : "refused", err.code);
}

/**
 * A whole composite batch Salesforce refused (DECISIONS.md 702): the reason
 * every query in it takes, or a throw for what stops the run (a lost session,
 * the daily limit, an outage, our own fault).
 */
export function batchRefused(err: unknown, soqls: string[]): string[] {
  if (!isUnavailable(err)) throw err;
  return soqls.map((query) => refusalReason(err, readName({ kind: "soql", api: "rest", query })));
}

/**
 * Salesforce's query errors quote the query with a caret under the problem, and
 * reasons end up in report text. The query is already on the evidence, so a
 * reason keeps only what went wrong.
 */
export function readableReason(message: string): string {
  const type = /sObject type '([^']+)' is not supported/.exec(message);
  if (type) return `${type[1]} isn't available in this org, or to the connected user.`;
  const column = /No such column '([^']+)' on (?:entity|sobject of type) '?([A-Za-z0-9_]+)'?/.exec(message);
  if (column) return `${column[2]}.${column[1]} isn't in this org.`;
  if (!/\nERROR at Row/.test(message)) return message;
  const last = message.trim().split("\n").at(-1)!.trim();
  const code = /^[A-Z_]+(?=:)/.exec(message)?.[0];
  return code && !last.startsWith(code) ? `${code}: ${last}` : last;
}

export async function readSoql<R>(ctx: ExtractContext, soql: string, options: QueryOptions & { missingFields?: string[] } = {}): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: options.api ?? "rest", query: soql };
  try {
    const result = await ctx.client.query<R>(soql, options);
    const missing = options.missingFields?.length ? { missingFields: options.missingFields } : {};
    return { status: "ok", data: result.records, source, truncated: result.truncated, ...missing };
  } catch (err) {
    return unavailable(source, err);
  }
}

export function soqlDateTime(date: Date): string {
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

/**
 * SOQL's date-times have no milliseconds, so the end of a read is the run's
 * instant rounded up to its second. Rounded down, it would cut a record made
 * earlier in that second, if Salesforce keeps the milliseconds.
 */
export const upToItsSecond = (date: Date): Date => new Date(Math.ceil(date.getTime() / 1000) * 1000);

/**
 * A window on when something happened, from `from` to the run's own date, on
 * one field (DECISIONS.md 463 and 480). Without the end, a step read late, or
 * again on a resumed run, would take in records made after the run's date.
 */
export function soqlWindow(field: string, from: Date, now: Date): string {
  return `${field} >= ${soqlDateTime(from)} AND ${soqlUpTo(field, now)}`;
}

/**
 * The end alone, for a read bounded at its start by something else: open cases,
 * or an email or article link read through its case (DECISIONS.md 481).
 */
export function soqlUpTo(field: string, now: Date): string {
  return `${field} <= ${soqlDateTime(upToItsSecond(now))}`;
}

/** A SOQL string literal, quotes and all: a backslash or quote in the value can't end it early. */
export function soqlString(value: string): string {
  return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

export function soqlDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

export function daysBefore(date: Date, days: number): Date {
  return new Date(date.getTime() - days * 86_400_000);
}

export function monthsBefore(date: Date, months: number): Date {
  const out = new Date(date);
  out.setUTCMonth(out.getUTCMonth() - months);
  return out;
}
