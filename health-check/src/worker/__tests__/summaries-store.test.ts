import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { MODELS } from "../../config.ts";
import { MemoryWorkerStore } from "../memory-store.ts";
import { progressOf, RunClosedError, type DocumentsJobInput, type RunOutputRecord } from "../types.ts";
import { consoleRun, INTAKE, RUN_ID, runInput } from "./helpers.ts";

// The memory store keeps the Postgres store's rules for Claude's summaries
// (DECISIONS.md 494), so the worker's tests see what the console's table would
// hold: a new row waits; a waiting row is replaced, its strikes with it; a row
// a person decided is never touched; a closed run is refused before anything.

const FIRST: NonNullable<RunOutputRecord["handbookSummaries"]> = {
  model: MODELS.handbookSummaries,
  madeAt: "2026-11-03T10:00:00.000Z",
  items: [
    { kind: "class", name: "LeadRouter", summary: "Sends each new lead to the right queue." },
    { kind: "class", name: "FinanceSync", summary: "Copies each invoice to the finance system." },
  ],
  leftOut: { tests: 2, tooLarge: 0, overLimit: 0, unreadable: 0, refused: 0, notWritten: 1 },
};
const SECOND: typeof FIRST = { ...FIRST, madeAt: "2026-11-04T10:00:00.000Z", items: [{ kind: "trigger", name: "AccountTrigger", summary: "Hands each saved account to its handler." }], leftOut: { ...FIRST.leftOut, notWritten: 2 } };

function store(): MemoryWorkerStore {
  const s = new MemoryWorkerStore();
  s.addRun(runInput());
  return s;
}

const output = (handbookSummaries?: typeof FIRST): RunOutputRecord => ({
  progress: progressOf(consoleRun({ status: "in_review" })),
  runDir: "/runs/x",
  findings: [],
  metrics: [],
  scores: [],
  manualSteps: [],
  sections: [],
  versions: [],
  facts: null,
  factsFile: null,
  ...(handbookSummaries ? { handbookSummaries } : {}),
});

/** The run's next delivery: the console has the run back from analysing, as a save made before review would find it. */
function reopen(s: MemoryWorkerStore): void {
  s.runs.get(RUN_ID)!.run.status = "analysing";
}

describe("the memory store's summaries row", () => {
  test("none yet: a waiting row, nobody and no time decided, nothing struck", async () => {
    const s = store();
    await s.saveRunOutput(RUN_ID, output(FIRST));
    assert.deepEqual(s.summaries.get(RUN_ID), {
      runId: RUN_ID,
      status: "waiting",
      model: FIRST.model,
      madeAt: FIRST.madeAt,
      items: FIRST.items.map((i) => ({ ...i, struck: false })),
      leftOut: FIRST.leftOut,
      decidedBy: null,
      decidedAt: null,
    });
  });

  test("a waiting row with one struck is replaced by the new items, none struck", async () => {
    const s = store();
    await s.saveRunOutput(RUN_ID, output(FIRST));
    s.strikeSummary(RUN_ID, 1);
    assert.equal(s.summaries.get(RUN_ID)!.items[1]!.struck, true);
    reopen(s);
    await s.saveRunOutput(RUN_ID, output(SECOND));
    const row = s.summaries.get(RUN_ID)!;
    assert.equal(row.status, "waiting");
    assert.deepEqual(row.items, [{ ...SECOND.items[0]!, struck: false }]);
    assert.equal(row.madeAt, SECOND.madeAt);
    assert.deepEqual(row.leftOut, SECOND.leftOut);
  });

  test("an approved row, and a left-out one, are untouched byte for byte", async () => {
    for (const status of ["approved", "left_out"] as const) {
      const s = store();
      await s.saveRunOutput(RUN_ID, output(FIRST));
      s.strikeSummary(RUN_ID, 0);
      s.decideSummaries(RUN_ID, status);
      const before = JSON.stringify(s.summaries.get(RUN_ID));
      reopen(s);
      await s.saveRunOutput(RUN_ID, output(SECOND));
      assert.equal(JSON.stringify(s.summaries.get(RUN_ID)), before, status);
    }
  });

  test("a second delivery with no summaries leaves a waiting row as it was", async () => {
    const s = store();
    await s.saveRunOutput(RUN_ID, output(FIRST));
    s.strikeSummary(RUN_ID, 0);
    const before = JSON.stringify(s.summaries.get(RUN_ID));
    reopen(s);
    await s.saveRunOutput(RUN_ID, output());
    assert.equal(JSON.stringify(s.summaries.get(RUN_ID)), before);
  });

  test("a closed run is refused before anything is written", async () => {
    for (const status of ["in_review", "released"] as const) {
      const s = store();
      s.runs.get(RUN_ID)!.run.status = status;
      await assert.rejects(s.saveRunOutput(RUN_ID, output(FIRST)), RunClosedError);
      assert.equal(s.summaries.size, 0, status);
    }
  });

  test("summaries the console couldn't read write no row, and the run is saved", async () => {
    const s = store();
    await s.saveRunOutput(RUN_ID, output({ ...FIRST, model: "m".repeat(101) }));
    assert.equal(s.summaries.size, 0);
    assert.equal(s.writes.at(-1)!.method, "saveRunOutput");
    assert.equal(s.runs.get(RUN_ID)!.run.status, "in_review");
  });

  test("the documents job reads the row only when asked, and never who decided it", async () => {
    const s = store();
    await s.saveRunOutput(RUN_ID, output(FIRST));
    s.decideSummaries(RUN_ID, "approved", "approver.zz@example.com", "2026-11-05T11:11:00.000Z");
    const input: DocumentsJobInput = { run: consoleRun({ status: "released" }), intake: INTAKE, sections: [], versions: [], overrides: [], preparedBy: null, state: "queued", driveFolderId: null, files: [] };
    s.addDocumentsJob(input);
    assert.equal((await s.loadDocumentsJob(RUN_ID))!.handbookSummaries, null);
    assert.equal((await s.loadDocumentsJob(RUN_ID, { handbookSummaries: false }))!.handbookSummaries, null);
    const read = (await s.loadDocumentsJob(RUN_ID, { handbookSummaries: true }))!;
    assert.deepEqual(read.handbookSummaries, { status: "approved", items: FIRST.items.map((i) => ({ ...i, struck: false })), leftOut: FIRST.leftOut });
    assert.ok(!JSON.stringify(read).includes("approver.zz") && !JSON.stringify(read).includes("11:11:00"));
  });
});

describe("a summary holding a character printing or the database would change or refuse", () => {
  const CHARACTERS: Array<[string, string]> = [["U+0000", "\u0000"], ["U+000B", "\u000B"], ["U+0001", "\u0001"], ["U+FFFF", "￿"], ["a lone high surrogate", "\uD800"], ["a lone low surrogate", "\uDC00"]];

  test("is dropped at save with notWritten one higher, and the other items written", async () => {
    for (const [what, c] of CHARACTERS) {
      const s = store();
      await s.saveRunOutput(RUN_ID, output({ ...FIRST, items: [{ ...FIRST.items[0]!, summary: `Sends each lead${c} on.` }, FIRST.items[1]!] }));
      const row = s.summaries.get(RUN_ID)!;
      assert.deepEqual(row.items, [{ ...FIRST.items[1]!, struck: false }], what);
      assert.equal(row.leftOut.notWritten, FIRST.leftOut.notWritten + 1, what);
      // Alone, it leaves no row at all.
      const alone = store();
      await alone.saveRunOutput(RUN_ID, output({ ...FIRST, items: [{ ...FIRST.items[0]!, name: `Lead${c}Router` }] }));
      assert.equal(alone.summaries.size, 0, what);
    }
  });
});
