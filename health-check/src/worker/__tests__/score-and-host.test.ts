import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { createConnectLink } from "../../connect/link.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { PostgresWorkerStore, RUN_SCORE_AND_HOST_MIGRATION, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { progressOf, runOverall, type LinkConnected, type RunOutputRecord } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// The run's overall score and grade, and the org's My Domain host on a used
// connect link, for the customer portal (DECISIONS.md 626). Each is the
// console's newer column, which a database may not have yet: it is written by
// a statement of its own in a savepoint of its own, so a run's results and a
// link's state save exactly as before without it.

type Handler = (text: string, values: unknown[]) => SqlResult | undefined;

class FakePool implements SqlPool {
  readonly statements: Array<{ text: string; values: unknown[] }> = [];
  readonly handlers: Handler[] = [(text) => (text.includes("FOR UPDATE") ? { rows: [{ status: "extracting" }], rowCount: 1 } : undefined)];
  async query(text: string, values: unknown[] = []): Promise<SqlResult> {
    this.statements.push({ text, values });
    for (const h of this.handlers) {
      const r = h(text, values);
      if (r) return r;
    }
    return { rows: [], rowCount: 1 };
  }
  async connect(): Promise<SqlClient & { release(): void }> {
    return { query: (t, v) => this.query(t, v), release: () => {} };
  }
  texts(): string[] {
    return this.statements.map((s) => s.text);
  }
}

/** A value the error's message quotes, which must never reach the log. */
const PLANTED = "planted-value-0001";
const failingOn = (column: string, code: string): Handler => (text) => {
  if (text.includes(`"${column}" = $2`)) throw Object.assign(new Error(`${PLANTED} broke on ${column}`), { code });
  return undefined;
};

/** The statements and values a save sent, without any savepoint named in `savepoints` or the statements inside it. */
function withoutSavepoints(pool: FakePool, savepoints: string[]): Array<{ text: string; values: unknown[] }> {
  const out: Array<{ text: string; values: unknown[] }> = [];
  let inside: string | null = null;
  for (const s of pool.statements) {
    const opened = savepoints.find((p) => s.text === `SAVEPOINT ${p}`);
    if (opened) inside = opened;
    else if (inside && (s.text === `RELEASE SAVEPOINT ${inside}` || s.text === `ROLLBACK TO SAVEPOINT ${inside}`)) inside = null;
    else if (!inside) out.push(s);
  }
  return out;
}

const NEWER = /"overallScore"|"overallGrade"|"instanceHost"/;
const RUN_SAVEPOINTS = ["run_overall_score", "run_overall_grade"];
const OUTPUT: Omit<RunOutputRecord, "progress" | "runDir"> = { findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null };
const OVERALL = { score: 73, grade: "B" } as const;

async function save(pool: FakePool, overall: RunOutputRecord["overall"] | "left out", logs: string[] = []): Promise<void> {
  const run = consoleRun();
  const output: RunOutputRecord = { progress: progressOf(run), runDir: "/runs/x", ...OUTPUT, ...(overall === "left out" ? {} : { overall }) };
  await new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }).saveRunOutput(run.id, output);
}

describe("the run's overall score and grade in the Postgres store (decision 626)", () => {
  test("are written after every other write, each by its own statement in its own savepoint, and no earlier statement names them", async () => {
    const pool = new FakePool();
    await save(pool, OVERALL);
    const texts = pool.texts();
    const score = texts.indexOf(`UPDATE "HealthCheckRun" SET "overallScore" = $2 WHERE "id" = $1`);
    const grade = texts.indexOf(`UPDATE "HealthCheckRun" SET "overallGrade" = $2 WHERE "id" = $1`);
    assert.deepEqual(texts.slice(score - 1, score + 2), ["SAVEPOINT run_overall_score", texts[score], "RELEASE SAVEPOINT run_overall_score"]);
    assert.deepEqual(texts.slice(grade - 1, grade + 2), ["SAVEPOINT run_overall_grade", texts[grade], "RELEASE SAVEPOINT run_overall_grade"]);
    assert.deepEqual(pool.statements[score]!.values, [RUN_ID, 73]);
    assert.deepEqual(pool.statements[grade]!.values, [RUN_ID, "B"]);
    assert.equal(texts.at(-1), "COMMIT");
    assert.equal(grade, texts.length - 3, "the last write");
    assert.ok(texts.slice(0, score - 1).every((t) => !NEWER.test(t)), "no statement from before names a new column");
  });

  test("no overall score writes null to both, so an earlier delivery's is cleared", async () => {
    const pool = new FakePool();
    await save(pool, null);
    const newer = pool.statements.filter((s) => NEWER.test(s.text));
    assert.deepEqual(newer.map((s) => s.values), [[RUN_ID, null], [RUN_ID, null]]);
  });

  test("left out, nothing is written to either column", async () => {
    const pool = new FakePool();
    await save(pool, "left out");
    assert.ok(pool.texts().every((t) => !NEWER.test(t) && !t.includes("run_overall")));
  });

  for (const code of ["42703", "42P01", "42804"]) {
    test(`on a database without the columns (${code}), the run's results save byte for byte as before, and each column is said once by the run, table, column and code`, async () => {
      const before = new FakePool();
      await save(before, "left out");
      const pool = new FakePool();
      pool.handlers.unshift(failingOn("overallScore", code), failingOn("overallGrade", code));
      const logs: string[] = [];
      await save(pool, OVERALL, logs);
      assert.deepEqual(withoutSavepoints(pool, RUN_SAVEPOINTS), before.statements);
      assert.equal(pool.texts().at(-1), "COMMIT");
      assert.ok(!pool.texts().includes("ROLLBACK"));
      assert.deepEqual(pool.texts().filter((t) => t.startsWith("ROLLBACK TO SAVEPOINT")), ["ROLLBACK TO SAVEPOINT run_overall_score", "ROLLBACK TO SAVEPOINT run_overall_grade"]);
      assert.deepEqual(logs, [
        `Run ${RUN_ID}: the console's HealthCheckRun table refused its "overallScore" column with SQLSTATE ${code}, so the run's overall score isn't saved; its results are. Check the table against the console's ${RUN_SCORE_AND_HOST_MIGRATION} migration.`,
        `Run ${RUN_ID}: the console's HealthCheckRun table refused its "overallGrade" column with SQLSTATE ${code}, so the run's grade isn't saved; its results are. Check the table against the console's ${RUN_SCORE_AND_HOST_MIGRATION} migration.`,
      ]);
      assert.ok(logs.every((l) => !l.includes(PLANTED) && !l.includes("73")));
    });
  }

  test("a check constraint refusing the score (23514) costs the score only: the grade is still written and the results commit", async () => {
    const pool = new FakePool();
    pool.handlers.unshift(failingOn("overallScore", "23514"));
    const logs: string[] = [];
    await save(pool, OVERALL, logs);
    assert.ok(pool.texts().includes("ROLLBACK TO SAVEPOINT run_overall_score"));
    assert.ok(pool.texts().includes("RELEASE SAVEPOINT run_overall_grade"));
    assert.equal(pool.texts().at(-1), "COMMIT");
    assert.equal(logs.length, 1);
    assert.match(logs[0]!, /"overallScore" column with SQLSTATE 23514,/);
  });

  for (const code of ["08006", "40001", "40P01", "57014"]) {
    test(`a ${code} on either column still rolls the whole save back, as today`, async () => {
      for (const column of ["overallScore", "overallGrade"]) {
        const pool = new FakePool();
        pool.handlers.unshift(failingOn(column, code));
        await assert.rejects(save(pool, OVERALL), new RegExp(PLANTED));
        assert.equal(pool.texts().at(-1), "ROLLBACK");
        assert.ok(!pool.texts().includes("COMMIT"));
        assert.ok(!pool.texts().some((t) => t.startsWith("ROLLBACK TO SAVEPOINT run_overall")));
      }
    });
  }

  test("an error with no SQLSTATE still rolls the whole save back", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => {
      if (text.includes(`"overallScore" = $2`)) throw new Error("socket hang up");
      return undefined;
    });
    await assert.rejects(save(pool, OVERALL), /socket hang up/);
    assert.equal(pool.texts().at(-1), "ROLLBACK");
  });
});

const LINK = "lnk-00000001";
const CONNECTED: LinkConnected = { at: "2026-10-31T09:30:00.000Z", orgId: "00D000000000001AAA", userId: "005000000000001AAA", note: null };
const HOST = "acme.my.salesforce.com";
const LINK_SAVEPOINT = "link_instance_host";

async function markUsed(pool: FakePool, host?: string | null, logs: string[] = []): Promise<void> {
  await new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }).markLinkUsed(LINK, CONNECTED, host);
}

describe("the org's host when a link is used, in the Postgres store (decision 626)", () => {
  test("a used link records the bare My Domain host, by its own statement in its own savepoint, after the link and run are written", async () => {
    const pool = new FakePool();
    await markUsed(pool, HOST);
    const texts = pool.texts();
    const at = texts.indexOf(`UPDATE "HealthCheckConnectLink" SET "instanceHost" = $2 WHERE "id" = $1`);
    assert.deepEqual(texts.slice(at - 1), ["SAVEPOINT link_instance_host", texts[at], "RELEASE SAVEPOINT link_instance_host", "COMMIT"]);
    assert.deepEqual(pool.statements[at]!.values, [LINK, HOST]);
    assert.ok(texts.slice(0, at - 1).every((t) => !NEWER.test(t)), "the link's own UPDATE is as it was");
  });

  for (const host of [undefined, null, "login.salesforce.com", "na1.salesforce.com", "evil.example", "https://acme.my.salesforce.com", "ACME.my.salesforce.com"]) {
    test(`a host that isn't a bare My Domain (${String(host)}) is not written, and the link is marked used as before`, async () => {
      const before = new FakePool();
      await markUsed(before);
      const pool = new FakePool();
      await markUsed(pool, host);
      assert.deepEqual(pool.statements, before.statements);
    });
  }

  for (const code of ["42703", "42P01", "23514"]) {
    test(`on a database without the column, or whose check refuses it (${code}), the link is marked used byte for byte as before, and the host is never logged`, async () => {
      const before = new FakePool();
      await markUsed(before);
      const pool = new FakePool();
      pool.handlers.unshift(failingOn("instanceHost", code));
      const logs: string[] = [];
      await markUsed(pool, HOST, logs);
      assert.deepEqual(withoutSavepoints(pool, [LINK_SAVEPOINT]), before.statements);
      assert.equal(pool.texts().at(-1), "COMMIT");
      assert.ok(pool.texts().includes("ROLLBACK TO SAVEPOINT link_instance_host"));
      assert.deepEqual(logs, [`Link ${LINK}: the console's HealthCheckConnectLink table refused its "instanceHost" column with SQLSTATE ${code}, so the org's address isn't saved; the link is marked used. Check the table against the console's ${RUN_SCORE_AND_HOST_MIGRATION} migration.`]);
      assert.ok(logs.every((l) => !l.includes("acme") && !l.includes(PLANTED)));
    });
  }

  test("a lost connection on the host still rolls the transaction back, as any lost connection does today", async () => {
    const pool = new FakePool();
    pool.handlers.unshift(failingOn("instanceHost", "08006"));
    await assert.rejects(markUsed(pool, HOST), new RegExp(PLANTED));
    assert.equal(pool.texts().at(-1), "ROLLBACK");
  });
});

describe("the memory store keeps the same fields (decision 626)", () => {
  const run = consoleRun();
  const output = (overall?: RunOutputRecord["overall"]): RunOutputRecord => ({ progress: progressOf(run), runDir: "/runs/x", ...OUTPUT, ...(overall === undefined ? {} : { overall }) });
  const made = createConnectLink({ baseUrl: "https://console.example.com", customerRef: "SYN-0001", connectionName: "synthetic-co", createdBy: "consultant@example.com" }).link;
  const link = () => ({ ...made, id: LINK });

  test("reads back the score, grade and host it was given, and null where there's no score", async () => {
    const store = new MemoryWorkerStore();
    store.addRun(runInput({ run }));
    await store.saveRunOutput(run.id, output(OVERALL));
    assert.deepEqual([store.runs.get(run.id)!.overallScore, store.runs.get(run.id)!.overallGrade], [73, "B"]);
    assert.equal("overall" in store.runs.get(run.id)!.output!, false, "a column, not part of the output");
    await store.saveRunOutput(run.id, output(null));
    assert.deepEqual([store.runs.get(run.id)!.overallScore, store.runs.get(run.id)!.overallGrade], [null, null]);
    store.addLink(link(), "pending");
    await store.markLinkUsed(LINK, CONNECTED, HOST);
    assert.equal(store.links.get(LINK)!.instanceHost, HOST);
  });

  test("as a database without the columns, it holds exactly what a store given none of them holds", async () => {
    const plain = new MemoryWorkerStore();
    const without = new MemoryWorkerStore({ withoutNewerColumns: true });
    for (const store of [plain, without]) {
      store.addRun(runInput({ run }));
      store.addLink(link(), "pending");
    }
    await plain.saveRunOutput(run.id, output());
    await plain.markLinkUsed(LINK, CONNECTED);
    await without.saveRunOutput(run.id, output(OVERALL));
    await without.markLinkUsed(LINK, CONNECTED, HOST);
    assert.equal(JSON.stringify([...without.runs], null, 1), JSON.stringify([...plain.runs], null, 1));
    assert.equal(JSON.stringify([...without.links]), JSON.stringify([...plain.links]));
    assert.equal(JSON.stringify(without.writes), JSON.stringify(plain.writes));
  });

  test("a host that isn't a bare My Domain is never kept", async () => {
    const store = new MemoryWorkerStore();
    store.addLink(link(), "pending");
    await store.markLinkUsed(LINK, CONNECTED, "na1.salesforce.com");
    assert.equal("instanceHost" in store.links.get(LINK)!, false);
  });
});

describe("what a run hands over (decision 626)", () => {
  test("no overall score, or one the console's checks would refuse, is null", () => {
    assert.equal(runOverall({ status: "not_measurable", reason: "Too few areas." }), null);
    const measured = { status: "measured" as const, uncapped: 73, capped: false, areas: [], word: "Good" };
    assert.deepEqual(runOverall({ ...measured, score: 73, grade: "B" }), { score: 73, grade: "B" });
    for (const [score, grade] of [[101, "A"], [-1, "E"], [72.5, "B"], [Number.NaN, "B"], [73, "F"]] as const) assert.equal(runOverall({ ...measured, score, grade }), null, `${score} ${grade}`);
  });

  test("the synthetic org's run row carries the same score and grade as the report", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    deps.store.addRun(runInput());
    await deps.tokens.save(syntheticConnection());
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, { final: true })).outcome, "done");
    const report = buildReport(loadRun(join(deps.runsDir, RUN_ID)).input);
    assert.equal(report.overall.status, "measured", "the synthetic org has an overall score");
    if (report.overall.status !== "measured") return;
    const stored = deps.store.runs.get(RUN_ID)!;
    assert.deepEqual([stored.overallScore, stored.overallGrade], [report.overall.score, report.overall.grade]);
  });

  test("a pulse writes its score, the same as its result's score now", async () => {
    const PULSE = "hc-2026-10-31-00000002";
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: "hc-2026-08-31-00000009" }) }));
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: PULSE }, { final: true })).outcome, "done");
    const stored = deps.store.runs.get(PULSE)!;
    const now = deps.store.pulseResults.get(PULSE)!.result.score.now;
    assert.equal(typeof stored.overallScore, "number");
    assert.equal(stored.overallScore, now);
    assert.match(String(stored.overallGrade), /^[A-E]$/);
  });
});
