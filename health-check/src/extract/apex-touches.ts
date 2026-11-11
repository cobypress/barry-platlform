import { closing, codeOnly, DATABASE_DML, DATABASE_QUERY_METHOD, DML, withoutInlineQueries } from "./apex-scan.ts";

// What an Apex class or trigger works on, read from its text while the body is
// in hand (DECISIONS.md 456 and 457). Comments go and every string is blanked
// first, so nothing from either is read, and only identifiers are kept. Like the
// scan beside it, this errs towards missing a case rather than inventing one.
// The object a change is made to isn't read: `update records;` names a
// variable, and text alone can't say what the variable holds.

export const CHANGES = ["insert", "update", "upsert", "delete", "undelete", "merge", "convertLead"] as const;
export type ApexChange = (typeof CHANGES)[number];

/** The most objects kept from one body's queries. `queriesTotal` has the full count. */
export const MAX_QUERIED_OBJECTS = 50;

export type ApexTouches = {
  /** Objects after FROM in its inline queries, sorted, each once whatever its case. */
  queries: string[];
  queriesTotal: number;
  /** The kinds of change it makes, as statements or Database methods, in the order above. */
  changes: ApexChange[];
  /** It builds a query at run time, so the objects that query reads aren't known. */
  dynamicQueries: boolean;
  /** It searches, inline or at run time. A search isn't a query, and the objects it reads aren't kept. */
  searches: boolean;
};

// The scan's Database query methods, and a search built at run time, which the
// scan's loop count leaves out: a search counts against a different limit.
const RUN_TIME_QUERY = new RegExp(String.raw`\b(?:${DATABASE_QUERY_METHOD}|Search\s*\.\s*query)\s*\(`, "gi");
const SEARCH = /\[\s*FIND\b|\bSearch\s*\.\s*(?:query|find)\s*\(/i;

const FROM = /\bFROM\s+([A-Za-z]\w*)/iy;
const NESTED = /\(\s*SELECT\b/iy;

/**
 * The objects one query reads, from `start` (inside its bracket) to `end`: its
 * own FROM, the first outside any parenthesis, then any query nested after it,
 * in a WHERE. One nested before it is a child relationship in the SELECT list,
 * which names a relationship, not an object.
 */
function queried(code: string, start: number, end: number, into: string[]): void {
  let depth = 0;
  let from = -1;
  for (let i = start; i < end && from === -1; i++) {
    if (code[i] === "(") depth++;
    else if (code[i] === ")") depth--;
    else if (depth === 0 && (code[i] === "F" || code[i] === "f")) {
      FROM.lastIndex = i;
      const m = FROM.exec(code);
      if (m && i + m[0].length <= end) {
        into.push(m[1]!);
        from = i + m[0].length;
      }
    }
  }
  if (from === -1) return;
  for (let i = from; i < end; i++) {
    if (code[i] !== "(") continue;
    NESTED.lastIndex = i;
    if (!NESTED.test(code)) continue;
    const close = closing(code, i, "()");
    queried(code, i + 1, Math.min(close - 1, end), into);
    i = close - 1;
  }
}

const byLowerCase = (a: string, b: string): number => (a.toLowerCase() < b.toLowerCase() ? -1 : a.toLowerCase() > b.toLowerCase() ? 1 : 0);

/**
 * Which reading of the Apex this is (DECISIONS.md 484), written into the apex
 * step's output when it reads, and into the org's handbook.json as
 * `readBy.apex`. A re-check names which details of a class or trigger differ
 * only when both files hold the same number, so a change in how we read never
 * reads as a change in the org. Raise it with any change that moves what the
 * handbook keeps for a class or trigger nobody touched: what `readTouches`
 * finds, the scan's `testClass` (apex-scan.ts), or what the apex step keeps
 * of each body (automation.ts). Which events a trigger's Usage fields give is
 * worked out when the file is built, under its own number (build.ts, 485).
 * `source-pin.test.ts` holds it to a hash of the text of those files, and
 * `pin.test.ts` to a hash of the synthetic org's.
 */
export const APEX_READING = 1;

/** Null when the body is hidden or missing: "couldn't be read", never "touches nothing". */
export function readTouches(body: string | undefined): ApexTouches | null {
  if (!body || body === "(hidden)") return null;
  const code = codeOnly(body);

  const named: string[] = [];
  for (const m of code.matchAll(/\[\s*SELECT\b/gi)) queried(code, m.index + 1, closing(code, m.index, "[]") - 1, named);
  // Each object once, under the first spelling the code uses.
  const seen = new Map<string, string>();
  for (const name of named) if (!seen.has(name.toLowerCase())) seen.set(name.toLowerCase(), name);
  const objects = [...seen.values()].sort(byLowerCase);

  // Nothing inside an inline query's or search's brackets is a statement.
  const statements = withoutInlineQueries(code);
  const kinds = new Set<string>();
  for (const pattern of [DML, DATABASE_DML]) for (const m of statements.matchAll(pattern)) kinds.add(m[1]!.toLowerCase());

  return {
    queries: objects.slice(0, MAX_QUERIED_OBJECTS),
    queriesTotal: objects.length,
    changes: CHANGES.filter((c) => kinds.has(c.toLowerCase())),
    // getQueryLocator given an inline query reads what that query says, which is read above.
    dynamicQueries: [...code.matchAll(RUN_TIME_QUERY)].some((m) => !/^\s*\[/.test(code.slice(m.index + m[0].length))),
    searches: SEARCH.test(code),
  };
}
