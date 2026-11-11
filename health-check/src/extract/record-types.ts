import { SF_DEFAULTS } from "../config.ts";
import { SalesforceError } from "../salesforce/client.ts";
import { monthsBefore, readableReason, readSoql, soqlUpTo, soqlWindow, type Extractor, type Sourced } from "./types.ts";

// Record-type use (DECISIONS.md 497). The step reads its own list of record
// types, because the routes the engine already has disagree on what a type is
// called and none keeps the id the counts are grouped by. All three reads end
// at the run's date and run together every time: a list read at one moment set
// against counts read at another would call a type made since unused, or count
// records made since. So there are no partial keys, on purpose.

export type RecordTypeRow = { Id: string; DeveloperName: string; Name: string; SobjectType: string; IsActive: boolean; NamespacePrefix: string | null; CreatedDate: string };
/** One group of an object's records. A null type is a record with none. */
export type RecordTypeCount = { RecordTypeId: string | null; c: number };

export type RecordTypeUse = {
  types: Sourced<RecordTypeRow[]>;
  /** Where "the last 12 months" starts: the run's date less 12 months. */
  recentFrom: string;
  /** Objects whose two counts both came back whole: every record made by the run's date, and those made in the 12 months to it. */
  counted: Record<string, { all: RecordTypeCount[]; recent: RecordTypeCount[] }>;
  /** Objects whose count Salesforce refused, with its reason. */
  refused: Array<{ object: string; reason: string }>;
  /** Objects whose count came back cut short. Aggregates can't page, so a part is no proof. */
  truncated: string[];
  /** Objects with an active type of their own beyond the ones we count. */
  notRead: string[];
  /** The two counts' queries, with <object> for each object. */
  queries: { all: string; recent: string };
};

/** How many objects are counted, those with the most active types first. Two counts each, so two composite calls. */
export const RECORD_TYPE_OBJECTS = 25;
export const RECENT_MONTHS = 12;

const LIST = (end: string): string => `SELECT Id, DeveloperName, Name, SobjectType, IsActive, NamespacePrefix, CreatedDate FROM RecordType WHERE ${end}`;
const COUNT = (object: string, where: string): string => `SELECT RecordTypeId, COUNT(Id) c FROM ${object} WHERE ${where} GROUP BY RecordTypeId`;

export const isPackaged = (type: Pick<RecordTypeRow, "NamespacePrefix">): boolean => Boolean(type.NamespacePrefix);

export const recordTypeUseExtractor: Extractor<RecordTypeUse> = {
  id: "record-type-use",
  version: 1,
  // Counts records on every object it reads, so a set-up review never runs it.
  records: true,
  async run(ctx) {
    const end = soqlUpTo("CreatedDate", ctx.now);
    const from = monthsBefore(ctx.now, RECENT_MONTHS);
    const recent = soqlWindow("CreatedDate", from, ctx.now);
    const out: RecordTypeUse = {
      types: await readSoql<RecordTypeRow>(ctx, LIST(end)),
      recentFrom: from.toISOString(),
      counted: {},
      refused: [],
      truncated: [],
      notRead: [],
      queries: { all: COUNT("<object>", end), recent: COUNT("<object>", recent) },
    };
    if (out.types.status !== "ok") return out;

    const active = new Map<string, number>();
    for (const type of out.types.data) if (type.IsActive && !isPackaged(type)) active.set(type.SobjectType, (active.get(type.SobjectType) ?? 0) + 1);
    const ranked = [...active].sort(([a, n], [b, m]) => m - n || a.localeCompare(b)).map(([object]) => object);
    const objects = ranked.slice(0, RECORD_TYPE_OBJECTS);
    out.notRead = ranked.slice(RECORD_TYPE_OBJECTS);

    // Each object's two counts side by side, so 25 objects are two calls.
    const queries = objects.flatMap((object) => [COUNT(object, end), COUNT(object, recent)]);
    const results: Array<{ ok: true; records: RecordTypeCount[]; truncated: boolean } | { ok: false; reason: string }> = [];
    for (let i = 0; i < queries.length; i += SF_DEFAULTS.compositeBatchSize) {
      const chunk = queries.slice(i, i + SF_DEFAULTS.compositeBatchSize);
      try {
        for (const result of await ctx.client.queryBatch<RecordTypeCount>(chunk)) {
          results.push(result.ok ? { ok: true, records: result.result.records, truncated: result.result.truncated } : { ok: false, reason: readableReason(result.error.message) });
        }
      } catch (err) {
        // A whole call refused leaves its objects not judged, as one refused
        // count does. A budget stop isn't Salesforce's answer: it goes up, and
        // the step is read again from its start on resume.
        if (!(err instanceof SalesforceError)) throw err;
        results.push(...chunk.map(() => ({ ok: false as const, reason: readableReason(err.message) })));
      }
    }
    objects.forEach((object, i) => {
      const missing = { ok: false as const, reason: "No answer came back for this count." };
      const [all, made] = [results[2 * i] ?? missing, results[2 * i + 1] ?? missing];
      if (!all.ok) out.refused.push({ object, reason: all.reason });
      else if (!made.ok) out.refused.push({ object, reason: made.reason });
      else if (all.truncated || made.truncated) out.truncated.push(object);
      else out.counted[object] = { all: all.records, recent: made.records };
    });
    return out;
  },
};
