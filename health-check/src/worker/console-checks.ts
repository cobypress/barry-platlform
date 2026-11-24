// The console's CHECK constraints on the rows an automatic run's release or
// hold writes, copied word for word from its migrations (DECISIONS.md 693), and
// a small reader that applies one to a row. The memory store holds every row it
// writes to these, so a test that passes there passes the database's own text,
// never a paraphrase of it; a test compares these strings with the migration
// files wherever the console's checkout is beside this one.

export type ConsoleCheck = { migration: string; name: string; sql: string };

export const CONSOLE_CHECKS = {
  runStatus: { migration: "20261103_health_check.sql", name: "HealthCheckRun_check1", sql: `"status" IN ('extracting','stopped_budget','analysing','awaiting_manual','in_review','released','failed')` },
  sectionApproved: { migration: "20261103_health_check.sql", name: "HealthCheckSection_check1", sql: `("status" = 'approved') = ("approvedBy" IS NOT NULL AND "approvedAt" IS NOT NULL)` },
  sectionApproval: { migration: "20261103_health_check.sql", name: "HealthCheckSection_check2", sql: `"status" = 'approved' OR ("approvedBy" IS NULL AND "approvedAt" IS NULL)` },
  reviewMode: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRun_reviewMode_check", sql: `"reviewMode" IS NULL OR "reviewMode" = 'automatic'` },
  // Redefined by 20261215, which added section_failed: the latest definition is the one that holds.
  heldReason: { migration: "20261217_health_check_hold_reads_lost.sql", name: "HealthCheckRun_heldReason_check", sql: `"heldReason" IS NULL OR (char_length("heldReason") <= 40 AND "heldReason" IN ('calibration_unreadable', 'no_digest', 'digest_held', 'score_moved', 'mostly_unmeasured', 'section_failed', 'reads_lost'))` },
  held: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRun_held_check", sql: `"heldReason" IS NULL OR ("reviewMode" IS NOT NULL AND "reviewMode" = 'automatic' AND "status" <> 'released')` },
  releaseKind: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRelease_kind_check", sql: `"kind" IN ('person', 'automatic')` },
  releasedBy: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRelease_releasedBy_check", sql: `("kind" = 'person') = ("releasedBy" IS NOT NULL)` },
  flagKind: { migration: "20261217_health_check_hold_reads_lost.sql", name: "HealthCheckRunFlag_kind_check", sql: `"kind" IN ('calibration_unreadable', 'no_digest', 'digest_held', 'score_moved', 'mostly_unmeasured', 'section_failed', 'run_failed', 'documents_held', 'reads_lost', 'stopped_budget')` },
  flagCounts: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRunFlag_counts_check", sql: `jsonb_typeof("counts") = 'object' AND octet_length("counts"::text) <= 1000` },
  flagCleared: { migration: "20261211_health_check_automatic.sql", name: "HealthCheckRunFlag_cleared_check", sql: `("clearedBy" IS NULL) = ("clearedAt" IS NULL)` },
} as const satisfies Record<string, ConsoleCheck>;

/**
 * The check as a JavaScript expression over `r`. Only the forms these checks
 * use are read; anything else is refused, so a changed constraint fails loudly
 * rather than passing for one it isn't. Stricter than SQL in one way only: a
 * comparison with a null is false here, where a CHECK lets unknown through,
 * and every check above spells its nulls out, so the two agree on them.
 */
function compile(sql: string): (r: Record<string, unknown>) => boolean {
  const js = sql
    .replace(/jsonb_typeof\("(\w+)"\)/g, "(r.$1 !== null && typeof r.$1 === 'object' && !Array.isArray(r.$1) ? 'object' : 'other')")
    .replace(/octet_length\("(\w+)"::text\)/g, "Buffer.byteLength(JSON.stringify(r.$1), 'utf8')")
    .replace(/char_length\("(\w+)"\)/g, "[...r.$1].length")
    .replace(/"(\w+)" IS NOT NULL/g, "(r.$1 != null)")
    .replace(/"(\w+)" IS NULL/g, "(r.$1 == null)")
    .replace(/"(\w+)" IN \(([^)]*)\)/g, "[$2].includes(r.$1)")
    .replace(/"(\w+)" <> /g, "r.$1 !== ")
    .replace(/"(\w+)" = /g, "r.$1 === ")
    .replace(/ = /g, " === ")
    .replace(/ AND /g, " && ")
    .replace(/ OR /g, " || ");
  if (/"|\bIS\b|\bIN\b|::/.test(js)) throw new Error(`A console check this reader can't read: ${sql}`);
  return new Function("r", `return Boolean(${js});`) as (r: Record<string, unknown>) => boolean;
}

const compiled = new Map<string, (r: Record<string, unknown>) => boolean>();

/** Whether the row passes the check. Columns the row doesn't hold read as null. */
export function passesCheck(check: ConsoleCheck, row: Record<string, unknown>): boolean {
  let fn = compiled.get(check.sql);
  if (!fn) compiled.set(check.sql, (fn = compile(check.sql)));
  return fn(new Proxy(row, { get: (t, k) => (typeof k === "string" && k in t ? t[k] : null) }));
}

/** Throws as the database would, naming the constraint, when the row fails any of the checks. */
export function assertChecks(row: Record<string, unknown>, ...checks: ConsoleCheck[]): void {
  for (const check of checks) {
    if (!passesCheck(check, row)) throw Object.assign(new Error(`new row violates check constraint "${check.name}"`), { code: "23514" });
  }
}
