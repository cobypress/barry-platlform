import { test } from "node:test";
import assert from "node:assert/strict";
import { ApiBudget } from "../../salesforce/budget.ts";
import { SalesforceClient } from "../../salesforce/client.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, SETUP_AUDIT } from "../../testing/synthetic-org.ts";
import { SETUP_AUDIT_DAYS, setupAuditExtractor, type SetupAuditSummary } from "../configuration.ts";
import { daysBefore, soqlDateTime, type ExtractContext, type Sourced } from "../types.ts";

// The Setup Audit Trail read has two ends: the 180 days Salesforce keeps, and
// the run's own date (DECISIONS.md 463), so a run that reads it again later
// doesn't take in changes made since.

const row = (at: number, Display: string) => ({ ...SETUP_AUDIT[0]!, Display, DelegateUser: null, CreatedDate: new Date(at).toISOString().replace("Z", "+0000") });

async function read(now: Date, rows: Array<Record<string, unknown>> = SETUP_AUDIT): Promise<{ soql: string; out: Sourced<SetupAuditSummary> }> {
  const sim = new Simulator({ setupAudit: rows });
  const client = await SalesforceClient.connect(syntheticProvider, { budget: new ApiBudget({ stopAtShare: 0.5 }), transport: sim, sleep: async () => {} });
  const out = await setupAuditExtractor.run({ client, now } as unknown as ExtractContext);
  const soql = sim.requests.map((r) => new URL(r.url).searchParams.get("q") ?? "").find((q) => q.includes("FROM SetupAuditTrail"))!;
  return { soql, out };
}

test("the query reads from 180 days before the run to the run itself, and filters on nothing but CreatedDate", async () => {
  const now = new Date(AS_OF);
  const { soql, out } = await read(now);
  const where = soql.slice(soql.indexOf(" WHERE "), soql.indexOf(" ORDER BY "));
  assert.equal(where, ` WHERE CreatedDate >= ${soqlDateTime(daysBefore(now, SETUP_AUDIT_DAYS))} AND CreatedDate <= ${soqlDateTime(now)}`);
  // Salesforce refuses a filter on these.
  assert.doesNotMatch(where, /\b(Section|Display|Action)\b/);
  assert.ok(out.status === "ok");
  assert.equal(out.data.rows, SETUP_AUDIT.length);
});

test("a run that starts part way through a second reads to the end of it, and nothing after", async () => {
  const start = new Date(AS_OF).getTime();
  const rows = [row(start + 100, "Before the run, in its second"), row(start + 1_500, "After the run"), row(start - 60_000, "A minute before")];
  const { soql, out } = await read(new Date(start + 250), rows);
  // SOQL's date-times have no milliseconds: the end is rounded up, never down.
  assert.match(soql, new RegExp(`AND CreatedDate <= ${soqlDateTime(new Date(start + 1_000))} `));
  assert.ok(out.status === "ok");
  assert.deepEqual(out.data.changes.map((c) => c.Display), ["Before the run, in its second", "A minute before"]);
});
