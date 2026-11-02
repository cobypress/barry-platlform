import type { Calibration } from "../schemas/index.ts";
import { isUnavailable, type QueryApi, type QueryOptions, type SalesforceClient } from "../salesforce/client.ts";

export type SourceInfo =
  | { kind: "soql"; api: QueryApi; query: string }
  | { kind: "rest"; path: string }
  | { kind: "metadata"; path: string };

// A read either worked or says why not. "We couldn't read it" and "there's
// nothing there" must never look alike, because the first means not measurable.
export type Sourced<T> =
  | { status: "ok"; data: T; source: SourceInfo; truncated: boolean }
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
  if (isUnavailable(err)) return { status: "unavailable", reason: err.message, source };
  throw err;
}

export async function readSoql<R>(ctx: ExtractContext, soql: string, options: QueryOptions = {}): Promise<Sourced<R[]>> {
  const source: SourceInfo = { kind: "soql", api: options.api ?? "rest", query: soql };
  try {
    const result = await ctx.client.query<R>(soql, options);
    return { status: "ok", data: result.records, source, truncated: result.truncated };
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
