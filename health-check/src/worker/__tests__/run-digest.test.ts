import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { join } from "node:path";
import { buildDigest } from "../../digest/build.ts";
import { buildReport } from "../../narrative/sections.ts";
import { loadRun } from "../../render/render-run.ts";
import type { NumberOverride, ReportSection, SectionVersion } from "../../schemas/index.ts";
import type { RunDigest } from "../../schemas/digest.ts";
import { SIZED_RULE } from "../../testing/recheck.ts";
import { loadRecording, ReplayTransport } from "../../testing/replay.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { processDocumentsJob } from "../documents-job.ts";
import type { DocumentDrive, DriveItem } from "../drive.ts";
import { PostgresWorkerStore, RUN_DIGEST_DELETE_SQL, RUN_DIGEST_SQL, type SqlClient, type SqlPool, type SqlResult } from "../postgres-store.ts";
import { digestOf } from "../run-job.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import type { ApprovedRun, DocumentKey } from "../../render/render-released.ts";
import { mkdirSync, writeFileSync } from "node:fs";
import { progressOf, type DocumentsMade, type RunOutputRecord } from "../types.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, INTAKE, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// Where the worker writes the run digest (DECISIONS.md 632): a pulse's with
// its results, a check's when its documents are made, each in a savepoint of
// its own as the side tables are (615, 618), so a console without the table
// never costs a run its results or its documents.

const { dir: recorded } = await runFromRecording({ answers: loadAnswers() });
const loaded = loadRun(recorded);
const DIGEST: RunDigest = buildDigest({ report: buildReport(loaded.input), pseudonyms: loaded.pseudonyms, inventory: null, madeAt: "2026-11-21T09:00:00.000Z" });
const last = { final: true };

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
const failing = (code: string): Handler => (text) => {
  if (text.startsWith(`INSERT INTO "HealthCheckRunDigest"`)) throw Object.assign(new Error(`${PLANTED} broke`), { code });
  return undefined;
};

const OUTPUT: Omit<RunOutputRecord, "progress" | "runDir"> = { findings: [], metrics: [], scores: [], manualSteps: [], sections: [], versions: [], facts: null, factsFile: null, digest: DIGEST };
const save = (store: PostgresWorkerStore) => store.saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT });
const MADE: DocumentsMade = { at: "2026-11-21T09:00:00.000Z", clientName: "Synthetic Co", folderName: "Health check 1", driveFolderId: "folder-0001", driveFolderUrl: "https://drive.example.com/folder-0001", files: [], warnings: [] };

describe("the digest's savepoint with a pulse's results (DECISIONS.md 632)", () => {
  test("the row is one statement, keyed on the run: the run id, when it was made, its version and the digest", async () => {
    const pool = new FakePool();
    await save(new PostgresWorkerStore(pool));
    const insert = pool.statements.find((s) => s.text === RUN_DIGEST_SQL)!;
    assert.deepEqual(insert.values.slice(0, 3), [RUN_ID, DIGEST.madeAt, 1]);
    assert.deepEqual(JSON.parse(insert.values[3] as string), DIGEST);
    const texts = pool.texts();
    assert.ok(texts.indexOf("SAVEPOINT run_digest") < texts.indexOf(RUN_DIGEST_SQL) && texts.indexOf(RUN_DIGEST_SQL) < texts.indexOf("RELEASE SAVEPOINT run_digest"));
    assert.equal(texts.at(-1), "COMMIT");
  });

  for (const code of ["42P01", "42703", "23502", "23503"]) {
    test(`a ${code} costs the digest only: back to its savepoint, the results commit, and each save says the run, the table and the code`, async () => {
      const pool = new FakePool();
      pool.handlers.unshift(failing(code));
      const logs: string[] = [];
      const store = new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) });
      await save(store);
      await save(store);
      const texts = pool.texts();
      assert.equal(texts.filter((t) => t === "COMMIT").length, 2);
      assert.ok(!texts.includes("ROLLBACK"));
      assert.equal(texts.filter((t) => t === "ROLLBACK TO SAVEPOINT run_digest").length, 2);
      assert.ok(texts.some((t) => t.startsWith(`INSERT INTO "HealthCheckFinding"`) || t.startsWith(`DELETE FROM "HealthCheckFinding"`)), "the results were written");
      const said = logs.filter((l) => l.includes("HealthCheckRunDigest"));
      assert.equal(said.length, 2);
      // A missing table says so, and asks for the migration; any other refusal gives its SQLSTATE.
      for (const line of said) assert.ok(line.startsWith(`Run ${RUN_ID}: `) && (code === "42P01" ? line.includes("no HealthCheckRunDigest table") : line.includes(`SQLSTATE ${code}`)), line);
      assert.ok(logs.every((l) => !l.includes(PLANTED)));
    });
  }

  for (const code of ["08006", "40001", "40P01"]) {
    test(`a ${code} (connection or serialisation) rolls the whole save back, to be tried again`, async () => {
      const pool = new FakePool();
      pool.handlers.unshift(failing(code));
      await assert.rejects(save(new PostgresWorkerStore(pool)), new RegExp(PLANTED));
      assert.equal(pool.texts().at(-1), "ROLLBACK");
      assert.ok(!pool.texts().includes("COMMIT"));
      assert.ok(!pool.texts().includes("ROLLBACK TO SAVEPOINT run_digest"));
    });
  }

  test("a save with no digest never names the table", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT, digest: undefined });
    assert.ok(pool.texts().every((t) => !t.includes("HealthCheckRunDigest")));
  });
});

describe("the digest's savepoint with a check's documents (DECISIONS.md 632)", () => {
  const isDocuments = (t: string) => t.startsWith(`UPDATE "HealthCheckDocuments" SET "status" = 'done'`);

  test("without a digest, the documents' row is one statement as before", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveDocuments(RUN_ID, MADE);
    assert.equal(pool.texts().length, 1);
    assert.ok(isDocuments(pool.texts()[0]!));
  });

  test("with one, both commit together, the digest in its own savepoint", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveDocuments(RUN_ID, MADE, DIGEST);
    assert.deepEqual(pool.texts().map((t) => (isDocuments(t) ? "documents" : t === RUN_DIGEST_SQL ? "digest" : t)), ["BEGIN", "documents", "SAVEPOINT run_digest", "digest", "RELEASE SAVEPOINT run_digest", "COMMIT"]);
  });

  test("a console without the table (42P01) or refusing the row (23502) keeps the documents done, and says so", async () => {
    for (const code of ["42P01", "23502"]) {
      const pool = new FakePool();
      pool.handlers.unshift(failing(code));
      const logs: string[] = [];
      await new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }).saveDocuments(RUN_ID, MADE, DIGEST);
      assert.equal(pool.texts().at(-1), "COMMIT");
      assert.ok(pool.texts().includes("ROLLBACK TO SAVEPOINT run_digest"));
      assert.ok(pool.texts().some(isDocuments));
      assert.equal(logs.filter((l) => l.startsWith(`Run ${RUN_ID}: `) && l.includes("HealthCheckRunDigest")).length, 1);
    }
  });

  test("a lost connection (08006) rolls the documents' row back with it, so the make is tried again", async () => {
    const pool = new FakePool();
    pool.handlers.unshift(failing("08006"));
    await assert.rejects(new PostgresWorkerStore(pool).saveDocuments(RUN_ID, MADE, DIGEST), new RegExp(PLANTED));
    assert.equal(pool.texts().at(-1), "ROLLBACK");
  });

  test("a row the console has moved on writes no digest", async () => {
    const pool = new FakePool();
    pool.handlers.unshift((text) => (isDocuments(text) ? { rows: [], rowCount: 0 } : undefined));
    await new PostgresWorkerStore(pool).saveDocuments(RUN_ID, MADE, DIGEST);
    assert.ok(!pool.texts().includes(RUN_DIGEST_SQL));
    assert.equal(pool.texts().at(-1), "COMMIT");
  });
});

/** Drive as plain maps, for a make that needs somewhere to put the files. */
class StubDrive implements DocumentDrive {
  readonly rootFolderId = "root-folder-0001";
  readonly items = new Map<string, DriveItem & { parent: string; folder: boolean }>();
  #next = 1;
  #add(parent: string, name: string, folder: boolean): DriveItem {
    const id = `drive-item-${String(this.#next++).padStart(4, "0")}`;
    const item = { id, name, url: `https://drive.example.com/${id}`, parent, folder, createdAt: "2026-11-04T18:00:00.000Z" };
    this.items.set(id, item);
    return item;
  }
  async folder(id: string) {
    const f = this.items.get(id);
    return f?.folder ? f : null;
  }
  async folders(parentId: string) {
    return [...this.items.values()].filter((i) => i.parent === parentId && i.folder);
  }
  async files(parentId: string) {
    return [...this.items.values()].filter((i) => i.parent === parentId && !i.folder);
  }
  async createFolder(parentId: string, name: string) {
    return this.#add(parentId, name, true);
  }
  async putFile(parentId: string, name: string, _mime: string, _body: Buffer, replaceId?: string) {
    return replaceId ? this.items.get(replaceId)! : this.#add(parentId, name, false);
  }
  async moveFile(id: string, _from: string, to: string) {
    this.items.get(id)!.parent = to;
  }
}

describe("where the worker writes the digest (DECISIONS.md 632)", () => {
  test("a pulse writes one with its results, from the report its result is made from", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    await deps.tokens.save(syntheticConnection());
    const PULSE = "hc-2026-10-31-00000002";
    deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: "hc-2026-08-31-00000009" }) }));
    const outcome = await createJobHandler(deps)({ kind: "run", runId: PULSE }, last);
    assert.equal(outcome.outcome, "done", outcome.detail);
    const digest = deps.store.getRunDigest(PULSE)!;
    assert.ok(digest, "the digest");
    assert.equal(digest.runId, PULSE);
    assert.equal(digest.product, "pulse");
    assert.equal(digest.reviewed, false);
    // The pulse's own result counts the same money.
    assert.equal(digest.value.total.expected, deps.store.pulseResults.get(PULSE)!.result.worth.now);
    const order = deps.store.writes.map((w) => `${w.method}:${w.id}`);
    assert.ok(order.indexOf(`saveRunDigest:${PULSE}`) < order.indexOf(`saveRunOutput:${PULSE}`));
  });

  test("a pulse saved to a console without the table keeps its results and its result row", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()) });
    await deps.tokens.save(syntheticConnection());
    deps.store.digestTable = false;
    const PULSE = "hc-2026-10-31-00000003";
    deps.store.addRun(runInput({ run: consoleRun({ id: PULSE, product: "pulse", baselineRunId: "hc-2026-08-31-00000009" }) }));
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: PULSE }, last)).outcome, "done");
    assert.equal(deps.store.getRunDigest(PULSE), null);
    assert.ok(deps.store.runs.get(PULSE)!.output!.findings.length > 0);
    assert.ok(deps.store.pulseResults.get(PULSE));
  });

  test("a check writes none until its documents are made, then one from the approved report, corrections carried", async () => {
    const deps = baseDeps({ dataTransport: new ReplayTransport(loadRecording()), drive: new StubDrive() });
    await deps.tokens.save(syntheticConnection());
    deps.store.addRun(runInput());
    assert.equal((await createJobHandler(deps)({ kind: "run", runId: RUN_ID }, last)).outcome, "done");
    assert.equal(deps.store.getRunDigest(RUN_ID), null, "a run with no documents yet has no digest");
    assert.ok(!deps.store.writes.some((w) => w.method === "saveRunDigest"));

    // Released, with a consultant's correction to one sized issue.
    const dir = join(deps.runsDir, RUN_ID);
    const built = buildReport(loadRun(dir).input);
    const sized = built.teamFindings.find((f) => f.ruleId === SIZED_RULE && f.status === "issue" && f.value.kind !== "not_sized")!;
    const asRead = sized.value.kind === "not_sized" ? 0 : sized.value.amount;
    const override: NumberOverride = { id: "o-1", runId: RUN_ID, target: { type: "finding", id: sized.id, field: "value.amount" }, previousValue: asRead, newValue: asRead + 100_000, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-11-04T12:00:00.000Z" };
    const sections: ReportSection[] = [];
    const versions: SectionVersion[] = [];
    for (const out of Object.values(buildReport({ ...loadRun(dir).input, overrides: [override] }).outputs)) {
      for (const s of out.sections) {
        sections.push({ ...s, status: "approved", approval: { approvedBy: "consultant@example.com", approvedAt: "2026-11-04T18:00:00.000Z" } });
        versions.push(out.versions.find((v) => v.id === s.currentVersionId)!);
      }
    }
    deps.store.addDocumentsJob({ run: consoleRun({ status: "released" }), intake: { ...INTAKE, customerName: "Synthetic Co" }, sections, versions, overrides: [override], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
    const made = await processDocumentsJob(RUN_ID, deps, last);
    assert.equal(made.outcome, "done", made.detail);
    const digest = deps.store.getRunDigest(RUN_ID)!;
    assert.ok(digest, "the digest, once the documents are made");
    assert.equal(digest.reviewed, true);
    const f = digest.findings.find((x) => x.id === sized.id)!;
    assert.equal(f.value.kind !== "not_sized" && f.value.amount, asRead + 100_000);
    const order = deps.store.writes.map((w) => w.method);
    assert.equal(order.indexOf("saveRunDigest"), order.indexOf("saveDocuments") + 1);
  });

  test("a digest that can't be made costs nothing, and the log says so by the error's class alone", () => {
    const lines: string[] = [];
    const made = digestOf(RUN_ID, () => {
      throw new TypeError(`${PLANTED} in the text`);
    }, (l) => void lines.push(l));
    assert.equal(made, null);
    assert.deepEqual(lines, [`Run ${RUN_ID}'s digest couldn't be made (TypeError), so the connector has none for it.`]);
  });
});

/** The digest with one string Postgres's jsonb refuses. */
function unstorable(kind: "nul" | "surrogate"): RunDigest {
  const d = structuredClone(DIGEST);
  const bad = kind === "nul" ? "A summary with a \u0000 in it" : "A label with half a pair \uD83D in it";
  if (kind === "nul") d.findings[0]!.summary = bad;
  else d.metrics[0]!.label = bad;
  return d;
}

describe("a digest the console's jsonb would refuse (DECISIONS.md 632, fix round)", () => {
  for (const kind of ["nul", "surrogate"] as const) {
    test(`a ${kind === "nul" ? "NUL" : "lone surrogate"} is caught before the statement: no INSERT, any earlier row deleted, the results commit, and the log names the run and the path, never the text`, async () => {
      const pool = new FakePool();
      const logs: string[] = [];
      await new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }).saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT, digest: unstorable(kind) });
      const texts = pool.texts();
      assert.ok(!texts.includes(RUN_DIGEST_SQL));
      assert.ok(texts.includes(RUN_DIGEST_DELETE_SQL));
      assert.equal(texts.at(-1), "COMMIT");
      const at = kind === "nul" ? "findings.0.summary" : "metrics.0.label";
      assert.deepEqual(logs.filter((l) => l.includes("digest")), [`Run ${RUN_ID}: its digest holds ${kind === "nul" ? "a NUL character" : "a broken character (half of a UTF-16 pair)"} at ${at}, which the console's database can't store, so the digest isn't saved; its results are.`]);
    });
  }

  for (const code of ["22P05", "22021"]) {
    test(`a ${code} from the jsonb inside the savepoint is skipped and said, and the results commit`, async () => {
      const pool = new FakePool();
      pool.handlers.unshift(failing(code));
      const logs: string[] = [];
      await save(new PostgresWorkerStore(pool, { log: (l) => void logs.push(l) }));
      assert.equal(pool.texts().at(-1), "COMMIT");
      assert.ok(pool.texts().includes("ROLLBACK TO SAVEPOINT run_digest"));
      assert.equal(logs.filter((l) => l.startsWith(`Run ${RUN_ID}: `) && l.includes(`SQLSTATE ${code}`)).length, 1);
      assert.ok(logs.every((l) => !l.includes(PLANTED)));
    });
  }

  test("the memory store keeps the same rule: the refused digest isn't kept, an earlier one goes, and the results are saved", async () => {
    const store = new MemoryWorkerStore();
    store.addRun(runInput());
    await store.saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT });
    assert.ok(store.getRunDigest(RUN_ID));
    await store.saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT, digest: unstorable("nul") });
    assert.equal(store.getRunDigest(RUN_ID), null);
    assert.ok(store.runs.get(RUN_ID)!.output);
  });
});

describe("a stale digest is never served (DECISIONS.md 632, fix round)", () => {
  test("a make with no digest deletes the run's row with the documents' row, in its own savepoint", async () => {
    const pool = new FakePool();
    await new PostgresWorkerStore(pool).saveDocuments(RUN_ID, MADE, null);
    assert.deepEqual(pool.texts().map((t) => (t.startsWith(`UPDATE "HealthCheckDocuments"`) ? "documents" : t === RUN_DIGEST_DELETE_SQL ? "delete" : t)), ["BEGIN", "documents", "SAVEPOINT run_digest", "delete", "RELEASE SAVEPOINT run_digest", "COMMIT"]);
  });

  test("a delete a console without the table refuses costs nothing; a lost connection rolls the make back", async () => {
    const missing = new FakePool();
    missing.handlers.unshift((text) => {
      if (text === RUN_DIGEST_DELETE_SQL) throw Object.assign(new Error(PLANTED), { code: "42P01" });
      return undefined;
    });
    await new PostgresWorkerStore(missing).saveDocuments(RUN_ID, MADE, null);
    assert.equal(missing.texts().at(-1), "COMMIT");
    const lost = new FakePool();
    lost.handlers.unshift((text) => {
      if (text === RUN_DIGEST_DELETE_SQL) throw Object.assign(new Error(PLANTED), { code: "08006" });
      return undefined;
    });
    await assert.rejects(new PostgresWorkerStore(lost).saveDocuments(RUN_ID, MADE, null), new RegExp(PLANTED));
    assert.equal(lost.texts().at(-1), "ROLLBACK");
  });

  test("make, correct, remake with the digest's build failing: the earlier digest is gone, not served beside the corrected documents", async () => {
    let build: "ok" | "fails" = "ok";
    const render = async (_dir: string, _approved: ApprovedRun, out: string) => {
      mkdirSync(out, { recursive: true });
      const files = {} as Record<DocumentKey, string>;
      for (const output of ["raw_notes", "executive_summary", "full_health_check", "ceo_one_pager", "action_lists", "tickets", "readout_kit"] as DocumentKey[]) {
        files[output] = join(out, `${output}.bin`);
        writeFileSync(files[output], `synthetic ${output}`);
      }
      return { files, warnings: [], held: [], digest: build === "ok" ? { ok: true as const, digest: DIGEST } : { ok: false as const, error: "TypeError" } };
    };
    const deps = baseDeps({ drive: new StubDrive(), renderReleased: render });
    mkdirSync(join(deps.runsDir, RUN_ID), { recursive: true });
    writeFileSync(join(deps.runsDir, RUN_ID, "run.json"), "{}");
    deps.store.addDocumentsJob({ run: consoleRun({ status: "released" }), intake: { ...INTAKE, customerName: "Synthetic Co" }, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] });
    assert.equal((await processDocumentsJob(RUN_ID, deps, last)).outcome, "done");
    assert.ok(deps.store.getRunDigest(RUN_ID), "the first make's digest");
    // A consultant corrects a figure and asks for the documents again; this time the digest can't be built.
    const doc = deps.store.documents.get(RUN_ID)!;
    doc.input.state = "queued";
    doc.input.overrides = [{ id: "o-1", runId: RUN_ID, target: { type: "finding", id: DIGEST.findings[0]!.id, field: "value.amount" }, previousValue: 1, newValue: 2, reason: "Checked with the customer.", overriddenBy: "Synthetic Consultant", overriddenAt: "2026-11-05T12:00:00.000Z" }];
    build = "fails";
    assert.equal((await processDocumentsJob(RUN_ID, deps, last)).outcome, "done");
    assert.equal(deps.store.getRunDigest(RUN_ID), null);
    assert.ok(deps.lines.includes(`Run ${RUN_ID}'s digest couldn't be made (TypeError), so the connector has none for it.`));
    assert.ok(deps.store.writes.some((w) => w.method === "deleteRunDigest"));
  });

  test("a pulse delivered again whose digest can't be made takes the earlier one away", async () => {
    const store = new MemoryWorkerStore();
    store.addRun(runInput());
    await store.saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT });
    await store.saveRunOutput(RUN_ID, { progress: progressOf(consoleRun()), runDir: "/runs/x", ...OUTPUT, digest: null });
    assert.equal(store.getRunDigest(RUN_ID), null);
  });
});
