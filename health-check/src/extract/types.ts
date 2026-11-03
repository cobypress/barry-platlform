import type { Calibration } from "../schemas/index.ts";
import { isUnavailable, type QueryApi, type QueryOptions, type SalesforceClient } from "../salesforce/client.ts";

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
};

// One extractor is one step: resumable alone, and one BullMQ job in Milestone 5.
// Bump `version` when its output changes, so a resumed run doesn't reuse old data.
export type Extractor<T = unknown> = {
  id: string;
  version: number;
  /** PartialStore keys to clear once this extractor's output is saved. */
  partialKeys?: string[];
  run(ctx: ExtractContext): Promise<T>;
};

export function unavailable<T>(source: SourceInfo, err: unknown): Sourced<T> {
  if (isUnavailable(err)) return { status: "unavailable", reason: readableReason(err.message), source };
  throw err;
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
