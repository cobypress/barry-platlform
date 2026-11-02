// SOQL is read-only as a language, with a few clauses that aren't:
//   FOR VIEW / FOR REFERENCE update LastViewedDate and LastReferencedDate.
//   FOR UPDATE locks records.
//   UPDATE TRACKING / UPDATE VIEWSTAT write Knowledge article statistics.
// A health check must leave no trace, so all of them are refused.

export class UnsafeSoqlError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsafeSoqlError";
  }
}

// A field Salesforce won't filter on. These fail at run time with a 400, which
// would cost an API call and look like missing data, so they're stopped here.
// Fetch the rows and filter in TypeScript instead.
export class UnfilterableFieldError extends Error {
  constructor(object: string, field: string) {
    super(`${object}.${field} can't be filtered in SOQL: fetch the rows and filter in code`);
    this.name = "UnfilterableFieldError";
  }
}

const UNFILTERABLE: Record<string, readonly string[]> = {
  setupaudittrail: ["Section", "Display", "Action"],
  loginhistory: ["Status", "Application"],
};

function stripStringLiterals(soql: string): string {
  return soql.replace(/'(?:[^'\\]|\\.)*'/g, "''");
}

function filterClauses(soql: string): string[] {
  const clauses: string[] = [];
  const re = /\b(WHERE|HAVING)\b([\s\S]*?)(?=\b(?:GROUP\s+BY|ORDER\s+BY|LIMIT|OFFSET|HAVING|WITH)\b|$)/gi;
  for (const match of soql.matchAll(re)) clauses.push(match[2]!);
  return clauses;
}

/** Throws unless `soql` is a single SELECT with no side effects and no known-bad filter. */
export function assertSafeSoql(soql: string): void {
  if (typeof soql !== "string" || soql.trim() === "") throw new UnsafeSoqlError("Empty query");
  const bare = stripStringLiterals(soql);
  if (!/^\s*SELECT\s/i.test(bare)) throw new UnsafeSoqlError("Only SELECT queries are allowed");
  if (bare.includes(";")) throw new UnsafeSoqlError("Multiple statements are not allowed");
  // A lookbehind, not \b: "LIMIT 1FOR VIEW" has no word boundary before FOR.
  const sideEffect = bare.match(/(?<![A-Za-z_])FOR\s+(VIEW|REFERENCE|UPDATE)\b|(?<![A-Za-z_])UPDATE\s+(TRACKING|VIEWSTAT)\b/i);
  if (sideEffect) throw new UnsafeSoqlError(`"${sideEffect[0]}" changes data in the org`);

  const object = bare.match(/\bFROM\s+([A-Za-z0-9_]+)/i)?.[1]?.toLowerCase();
  const blocked = object ? UNFILTERABLE[object] : undefined;
  if (!blocked) return;
  for (const clause of filterClauses(bare)) {
    for (const field of blocked) {
      if (new RegExp(`(^|[^.\\w])${field}\\b`, "i").test(clause)) throw new UnfilterableFieldError(object!, field);
    }
  }
}
