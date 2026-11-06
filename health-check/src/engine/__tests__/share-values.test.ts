import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { join } from "node:path";
import { readCheckpoint } from "../checkpoint.ts";
import { shareRepeatedValues } from "../share-values.ts";
import { runFromRecording } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { ALL_MODULES } from "../../domains/index.ts";

// Repeated values held once (DECISIONS.md 346): nothing that reads them can tell.

test("every extract of the synthetic org's run reads the same after its repeated values are shared", async () => {
  const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
  const folder = join(dir, "checkpoints");
  for (const file of readdirSync(folder).filter((f) => f.endsWith(".json"))) {
    const original = readCheckpoint(join(folder, file));
    assert.deepEqual(shareRepeatedValues(readCheckpoint(join(folder, file))), original, file);
  }
});

test("dates, nulls, numbers and nested rows are left as they are", () => {
  const when = new Date(Date.UTC(2026, 0, 1));
  const value = { data: [{ Id: "006000000000001AAA", OwnerId: "005000000000001AAA", n: 3, x: null, when, RecordType: { DeveloperName: "Standard" } }, { Id: "006000000000002AAA", OwnerId: "005000000000001AAA", n: 4, x: null, when, RecordType: { DeveloperName: "Standard" } }] };
  const copy = structuredClone(value);
  assert.deepEqual(shareRepeatedValues(value), copy);
  assert.ok(value.data[0]!.when instanceof Date);
});
