import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { createJobHandler } from "../worker.ts";
import { lossesOf, readLosses, sameAsLast, type RunLosses } from "../release.ts";
import { PREVIOUS_RUN_LOSSES_SQL, PostgresWorkerStore, RUN_FAILED_SQL, RUN_LOSSES_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { progressOf, type RunOutputRecord } from "../types.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";
import { AutomaticReplay, automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { AS_OF, ORG_ID } from "../../testing/synthetic-org.ts";
import { MAX_REFUSED } from "../../engine/run.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";

// When an automatic run is held reads_lost, the flag says whether the org's
// previous run lost reads in exactly the same steps (DECISIONS.md 723), so a
// person can tell "the org, again" from an outage. It only informs: nothing is
// released on it. Each run below goes through the real worker handler over the
// synthetic org, on its last delivery, with Salesforce failing chosen reads.

const ON = { on: true, orgs: new Set([ORG_ID.slice(0, 15)]) };
const ORG15 = ORG_ID.slice(0, 15);
const PREV = "hc-2026-10-01-00000001";
const LATER = "hc-2026-11-30-00000001";
const STARTED = { [PREV]: "2026-10-01T09:00:00.000Z", [RUN_ID]: "2026-10-31T09:00:00.000Z", [LATER]: "2026-11-30T09:00:00.000Z" } as Record<string, string>;

const soql = (r: TransportRequest): string => {
  try {
    return new URL(r.url).searchParams.get("q") ?? "";
  } catch {
    return "";
  }
};
const expired: TransportResponse = { status: 400, header: () => null, text: JSON.stringify([{ errorCode: "INVALID_QUERY_LOCATOR", message: "SF WORDING" }]) };
const SERVER_FAULT: TransportResponse = {
  status: 500,
  header: () => null,
  text: `<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>soapenv:Server</faultcode><faultstring>An unexpected error occurred. ErrorId: 1</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`,
};

/** What Salesforce fails on a delivery: an expired cursor on each named object's query, and a server fault on every Metadata API call. */
type Faults = { objects: string[]; soap?: boolean };
/** Lost in two steps, activity and forecast-quotas: what a big org that times out in the same two places every month looks like. */
const TWO_STEPS: Faults = { objects: ["Task", "ForecastingQuota"] };
/** Two other steps: logins and login-activity, with activity. */
const OTHER_STEPS: Faults = { objects: ["Task", "LoginHistory"] };
const NONE: Faults = { objects: [] };

async function world() {
  const faults: { now: Faults } = { now: NONE };
  const inner = new AutomaticReplay();
  const dataTransport: Transport = {
    send: async (r) => {
      if (faults.now.soap && r.headers?.SOAPAction) return SERVER_FAULT;
      const q = soql(r);
      return faults.now.objects.some((o) => q.includes(`FROM ${o} `)) ? expired : inner.send(r);
    },
  };
  const clock = Date.parse(AS_OF);
  const deps = baseDeps({ dataTransport, now: () => new Date(clock), automatic: ON });
  await deps.tokens.save(syntheticConnection());
  const calibration = await automaticSyntheticCalibration();
  const handle = createJobHandler(deps);
  const add = (id: string, mode: "automatic" | "consultant" = "automatic") => {
    const run = consoleRun({ id, startedAt: STARTED[id]!, ...(mode === "automatic" ? { reviewMode: "automatic" } : {}) });
    deps.store.addRun(mode === "automatic" ? runInput({ run, calibration, intake: null, answers: [] }) : runInput({ run }));
  };
  return {
    deps,
    add,
    stored: (id: string) => deps.store.runs.get(id)!,
    counts: (id: string) => deps.store.flags.get(`${id}|reads_lost`)?.counts,
    /** The run's last delivery with these faults, or a person's Run again of it pressed at `again`. */
    deliver: (id: string, f: Faults, again?: number) => {
      faults.now = f;
      return handle({ kind: "run", runId: id }, { final: true, ...(again ? { jobId: `run-${id}-again-${again}` } : {}) });
    },
    /** The console's Run again: open flags cleared, the hold gone, back to extracting. */
    runAgain: (id: string) => {
      for (const f of deps.store.flags.values()) if (f.runId === id && f.clearedAt === null) deps.store.postAndClearFlag(id, f.kind, "staff@example.com", AS_OF);
      deps.store.held.delete(id);
      deps.store.runs.get(id)!.run.status = "extracting";
      return clock;
    },
  };
}

describe("a reads_lost hold says whether the org's previous run lost the same reads (DECISIONS.md 723)", () => {
  test("the same two steps on the org's previous run and this one: sameAsLast 1, and still held", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    assert.match((await w.deliver(PREV, TWO_STEPS)).detail, /is held: reads_lost$/);
    assert.deepEqual(w.stored(PREV).readsLost, { steps: ["activity", "forecast-quotas"], server: false });
    // The org's first run has nothing to compare with.
    assert.equal(Object.hasOwn(w.counts(PREV)!, "sameAsLast"), false);
    const outcome = await w.deliver(RUN_ID, TWO_STEPS);
    assert.match(outcome.detail, /is held: reads_lost$/, outcome.detail);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 1);
    assert.equal(w.deps.store.releases.has(RUN_ID), false);
    assert.ok(w.deps.lines.some((l) => l.includes(`previous run of the org is ${PREV}: it lost reads in the same steps`)), w.deps.lines.join("\n"));
  });

  test("different steps on the previous run: sameAsLast 0", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    await w.deliver(PREV, OTHER_STEPS);
    assert.deepEqual(w.stored(PREV).readsLost, { steps: ["activity", "login-activity", "logins"], server: false });
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 0);
  });

  test("the previous run lost nothing and was released: sameAsLast 0", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    assert.match((await w.deliver(PREV, NONE)).detail, /is released$/);
    assert.deepEqual(w.stored(PREV).readsLost, { steps: [], server: false });
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 0);
  });

  test("the previous run lost one step and was released: its losses are stored, and differ: 0", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    assert.match((await w.deliver(PREV, { objects: ["Task"] })).detail, /is released$/);
    assert.deepEqual(w.stored(PREV).readsLost, { steps: ["activity"], server: false });
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 0);
  });

  test("no previous run, and a run of the org started later doesn't count: absent", async () => {
    const w = await world();
    w.add(RUN_ID);
    w.add(LATER);
    await w.deliver(LATER, TWO_STEPS);
    await w.deliver(RUN_ID, TWO_STEPS);
    const counts = w.counts(RUN_ID)!;
    assert.ok(counts.lost! > 0);
    assert.equal(Object.hasOwn(counts, "sameAsLast"), false, JSON.stringify(counts));
  });

  test("the previous run's losses aren't known (stored before this change, or never decided): absent", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    await w.deliver(PREV, TWO_STEPS);
    // As a row an older worker wrote: the same losses happened, but nothing in the budget says so.
    delete w.stored(PREV).readsLost;
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(Object.hasOwn(w.counts(RUN_ID)!, "sameAsLast"), false, JSON.stringify(w.counts(RUN_ID)));
    assert.ok(w.deps.lines.some((l) => l.includes("whether it lost the same reads isn't known")));
  });

  test("a previous run that never got as far as its results (added, never delivered): absent", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(Object.hasOwn(w.counts(RUN_ID)!, "sameAsLast"), false);
  });

  test("a server fault on the same steps in both runs: absent, as Salesforce may be why; on different steps, 0", async () => {
    const withServer: Faults = { ...TWO_STEPS, soap: true };
    const same = await world();
    same.add(PREV);
    same.add(RUN_ID);
    await same.deliver(PREV, withServer);
    assert.equal((same.stored(PREV).readsLost as RunLosses).server, true);
    await same.deliver(RUN_ID, withServer);
    assert.equal(Object.hasOwn(same.counts(RUN_ID)!, "sameAsLast"), false, JSON.stringify(same.counts(RUN_ID)));

    const differ = await world();
    differ.add(PREV);
    differ.add(RUN_ID);
    await differ.deliver(PREV, TWO_STEPS);
    await differ.deliver(RUN_ID, withServer);
    assert.equal(differ.counts(RUN_ID)!.sameAsLast, 0);
  });

  test("a consultant-led previous run is never the previous run, even holding the same losses: absent", async () => {
    const w = await world();
    w.add(PREV, "consultant");
    w.add(RUN_ID);
    await w.deliver(PREV, TWO_STEPS);
    // A consultant-led run stores none; were it there, it still isn't compared.
    assert.equal(w.stored(PREV).readsLost, undefined);
    w.stored(PREV).readsLost = { steps: ["activity", "forecast-quotas"], server: false };
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(Object.hasOwn(w.counts(RUN_ID)!, "sameAsLast"), false);
  });

  test("Run again compares with the org's previous run, never this run's earlier attempt", async () => {
    const w = await world();
    w.add(PREV);
    w.add(RUN_ID);
    await w.deliver(PREV, NONE);
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 0);
    assert.deepEqual(w.stored(RUN_ID).readsLost, { steps: ["activity", "forecast-quotas"], server: false });
    // The same losses again on a Run again: still 0, as the org's previous run lost nothing.
    const pressed = w.runAgain(RUN_ID);
    assert.match((await w.deliver(RUN_ID, TWO_STEPS, pressed)).detail, /is held: reads_lost$/);
    assert.equal(w.counts(RUN_ID)!.sameAsLast, 0);
  });

  test("Run again of the org's only run: absent every time", async () => {
    const w = await world();
    w.add(RUN_ID);
    await w.deliver(RUN_ID, TWO_STEPS);
    const pressed = w.runAgain(RUN_ID);
    await w.deliver(RUN_ID, TWO_STEPS, pressed);
    assert.equal(Object.hasOwn(w.counts(RUN_ID)!, "sameAsLast"), false);
  });

  test("a Run again that fails before its first progress save leaves no losses: the org's next run doesn't compare with that attempt", async () => {
    const w = await world();
    w.add(RUN_ID);
    w.add(LATER);
    await w.deliver(RUN_ID, TWO_STEPS);
    assert.deepEqual(w.stored(RUN_ID).readsLost, { steps: ["activity", "forecast-quotas"], server: false });
    const pressed = w.runAgain(RUN_ID);
    // The connection can't be loaded on the Run again's last delivery: failed before the engine saved anything.
    const load = w.deps.tokens.load.bind(w.deps.tokens);
    w.deps.tokens.load = async () => {
      throw new Error("token store down");
    };
    await w.deliver(RUN_ID, TWO_STEPS, pressed);
    w.deps.tokens.load = load;
    assert.equal(w.stored(RUN_ID).run.status, "failed");
    assert.equal(w.stored(RUN_ID).readsLost, undefined);
    await w.deliver(LATER, TWO_STEPS);
    assert.equal(Object.hasOwn(w.counts(LATER)!, "sameAsLast"), false, JSON.stringify(w.counts(LATER)));
  });

  test("a progress save takes the stored losses away, as the Postgres store's budget rewrite does", async () => {
    const w = await world();
    w.add(RUN_ID);
    w.stored(RUN_ID).readsLost = { steps: ["activity"], server: false };
    await w.deps.store.saveRunProgress(RUN_ID, progressOf(w.stored(RUN_ID).run), "/tmp/x");
    assert.equal(w.stored(RUN_ID).readsLost, undefined);
  });

  test("the previous run can't be read: the hold goes on without it, and says so in the log", async () => {
    const w = await world();
    w.add(RUN_ID);
    w.deps.store.previousRunLosses = async () => {
      throw new Error("db down");
    };
    assert.match((await w.deliver(RUN_ID, TWO_STEPS)).detail, /is held: reads_lost$/);
    assert.equal(Object.hasOwn(w.counts(RUN_ID)!, "sameAsLast"), false);
    assert.ok(w.deps.lines.some((l) => l.includes("previous run of the org couldn't be read")));
  });
});

describe("sameAsLast and the stored losses, rule by rule", () => {
  const a: RunLosses = { steps: ["activity", "forecast-quotas"], server: false };
  test("the truth table", () => {
    assert.equal(sameAsLast(a, { ...a }), 1);
    assert.equal(sameAsLast(a, { steps: ["activity"], server: false }), 0);
    assert.equal(sameAsLast(a, { steps: [], server: false }), 0);
    assert.equal(sameAsLast(a, null), undefined);
    assert.equal(sameAsLast(a, undefined), undefined);
    assert.equal(sameAsLast(null, a), undefined);
    assert.equal(sameAsLast(a, { ...a, server: true }), undefined);
    assert.equal(sameAsLast({ ...a, server: true }, a), undefined);
    assert.equal(sameAsLast({ ...a, server: true }, { steps: ["metadata"], server: true }), 0);
  });

  test("lossesOf: sorted, each step once, the server codes; not known at run.json's cap", () => {
    const lost = (step: string, code: string) => ({ step, code, lost: true as const });
    assert.deepEqual(lossesOf([lost("b", "INVALID_QUERY_LOCATOR"), lost("a", "RETRIEVE_TIMEOUT"), lost("b", "INVALID_QUERY_LOCATOR"), { step: "c", code: "INVALID_TYPE" }], MAX_REFUSED), { steps: ["a", "b"], server: false });
    assert.deepEqual(lossesOf([lost("metadata", "Server")], MAX_REFUSED), { steps: ["metadata"], server: true });
    assert.deepEqual(lossesOf([lost("metadata", "BAD_SOAP_RESPONSE")], MAX_REFUSED), { steps: ["metadata"], server: true });
    assert.deepEqual(lossesOf(undefined, MAX_REFUSED), { steps: [], server: false });
    assert.equal(lossesOf(Array.from({ length: MAX_REFUSED }, (_, i) => ({ step: `s${i}`, code: "INVALID_TYPE" })), MAX_REFUSED), null);
  });

  test("readLosses takes only what the worker writes", () => {
    assert.deepEqual(readLosses({ steps: ["b", "a"], server: false }), { steps: ["a", "b"], server: false });
    assert.deepEqual(readLosses('{"steps":[],"server":true}'), { steps: [], server: true });
    for (const bad of [null, undefined, "", "not json", [], {}, { steps: [] }, { steps: "a", server: false }, { steps: [1], server: false }, { steps: ["has space"], server: false }, { steps: [], server: 1 }]) {
      assert.equal(readLosses(bad), null, JSON.stringify(bad));
    }
  });
});

class FakePool implements SqlPool {
  /** `client`: sent on the transaction's own connection, not the pool. */
  readonly statements: Array<{ text: string; values: unknown[]; client: boolean }> = [];
  readonly answer: (text: string) => SqlResult | undefined;
  constructor(answer: (text: string) => SqlResult | undefined = () => undefined) {
    this.answer = answer;
  }
  async query(text: string, values: unknown[] = [], client = false): Promise<SqlResult> {
    this.statements.push({ text, values, client });
    if (text.includes("FOR UPDATE")) return { rows: [{ status: "analysing" }], rowCount: 1 };
    return this.answer(text) ?? { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v, true), release: () => {} };
  }
}

describe("the Postgres store's side (DECISIONS.md 723)", () => {
  test("the previous run is one read-only statement through the org's customer, never this run, by start time", () => {
    const sql = PREVIOUS_RUN_LOSSES_SQL;
    assert.match(sql, /^SELECT /);
    assert.doesNotMatch(sql, /;|\b(INSERT|UPDATE|DELETE|FOR UPDATE)\b/);
    assert.match(sql, /FROM "HealthCheckCustomer" c JOIN "HealthCheckCustomerRun" cr ON cr\."customerId" = c\."id" JOIN "HealthCheckRun" r ON r\."id" = cr\."runId"/);
    assert.match(sql, /WHERE c\."orgId15" = \$2 AND r\."id" <> \$1 AND left\(r\."orgId", 15\) = \$2 AND r\."product" = 'barometer' AND r\."reviewMode" = 'automatic'/);
    assert.match(sql, /r\."startedAt" < \(SELECT t\."startedAt" FROM "HealthCheckRun" t WHERE t\."id" = \$1\) ORDER BY r\."startedAt" DESC, r\."id" DESC LIMIT 1$/);
  });

  // Wherever the console's checkout is beside this one; otherwise there is nothing to compare with.
  const roots = [join(process.cwd(), "..", "..", "hc-console-merge", "migrations"), join(process.cwd(), "..", "..", "worktrees", "hc-console-merge", "migrations")];
  const root = roots.find((r) => existsSync(r));
  test("each step of that read has an index in the console's migrations", { skip: root ? false : "the console's migrations aren't beside this checkout" }, () => {
    const all = readdirSync(root!).filter((f) => f.endsWith(".sql")).map((f) => readFileSync(join(root!, f), "utf8")).join("\n");
    assert.match(all, /CREATE UNIQUE INDEX IF NOT EXISTS "HealthCheckCustomer_orgId15_key" ON "HealthCheckCustomer"\("orgId15"\)/);
    assert.match(all, /CREATE INDEX IF NOT EXISTS "HealthCheckCustomerRun_customerId_idx" ON "HealthCheckCustomerRun"\("customerId"\)/);
    assert.match(all, /"HealthCheckRun_pkey" PRIMARY KEY \("id"\)|CREATE TABLE IF NOT EXISTS "HealthCheckRun" \([^;]*PRIMARY KEY/);
  });

  test("previousRunLosses reads the row strictly: unknown losses are null, no row is null", async () => {
    const rows: Record<string, unknown>[][] = [[{ id: PREV, losses: { steps: ["b", "a"], server: false } }], [{ id: PREV, losses: null }], []];
    const pool = new FakePool((t) => (t === PREVIOUS_RUN_LOSSES_SQL ? { rows: rows.shift()!, rowCount: 1 } : undefined));
    const store = new PostgresWorkerStore(pool);
    assert.deepEqual(await store.previousRunLosses(RUN_ID, ORG15), { runId: PREV, losses: { steps: ["a", "b"], server: false } });
    assert.deepEqual(await store.previousRunLosses(RUN_ID, ORG15), { runId: PREV, losses: null });
    assert.equal(await store.previousRunLosses(RUN_ID, ORG15), null);
    assert.deepEqual(pool.statements.at(-1)!.values, [RUN_ID, ORG15]);
  });

  test("the losses are written into the budget right after the progress that rewrites it, in the save's one transaction; none, nothing", async () => {
    const run = consoleRun();
    const record = (readsLost?: RunLosses): RunOutputRecord => ({
      progress: progressOf(run), runDir: "/tmp/x", findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null, ...(readsLost ? { readsLost } : {}),
    });
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, record({ steps: ["activity"], server: false }));
    const texts = pool.statements.map((s) => s.text);
    const at = texts.indexOf(RUN_LOSSES_SQL);
    const progress = texts.findIndex((t) => /^UPDATE "HealthCheckRun" SET "status"/.test(t));
    const steps = texts.findLastIndex((t) => t.startsWith('INSERT INTO "HealthCheckRunStep"'));
    assert.ok(texts[0] === "BEGIN" && progress > 0 && steps > progress && at === steps + 1 && texts.at(-1) === "COMMIT", texts.join("\n"));
    assert.deepEqual(pool.statements[at]!.values, [RUN_ID, '{"steps":["activity"],"server":false}']);
    // On the transaction's connection, between its BEGIN and COMMIT, never on the pool.
    assert.ok(pool.statements.slice(0, at + 1).every((x) => x.client) && pool.statements[at]!.client, JSON.stringify(pool.statements.map((x) => [x.text.slice(0, 30), x.client])));
    // A failure record takes the losses away with the status, in the same statement.
    const failed = new FakePool();
    await new PostgresWorkerStore(failed).recordRunFailure(RUN_ID, "broken", AS_OF);
    assert.ok(failed.statements.some((x) => x.text === RUN_FAILED_SQL && x.client));
    assert.match(RUN_FAILED_SQL, /SET "status" = 'failed', "budget" = "budget" - 'readsLost' WHERE "id" = \$1$/);
    const none = new FakePool();
    await new PostgresWorkerStore(none).saveRunOutput(RUN_ID, record());
    assert.ok(!none.statements.some((s) => s.text === RUN_LOSSES_SQL));
  });
});
