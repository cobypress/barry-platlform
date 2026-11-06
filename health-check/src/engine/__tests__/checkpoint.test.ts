import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileLines, readCheckpoint, SPILL_AT, writeCheckpoint } from "../checkpoint.ts";
import { runFromRecording, tempDir } from "../../testing/harness.ts";
import { loadAnswers } from "../../testing/replay.ts";
import { ALL_MODULES } from "../../domains/index.ts";

// Checkpoints written and read in pieces (DECISIONS.md 345). What comes back
// must be exactly what JSON.stringify and JSON.parse would have given.

const same = (value: unknown): unknown => JSON.parse(JSON.stringify(value));
const roundTrip = (value: unknown): { back: unknown; text: string } => {
  const path = join(tempDir(), "checkpoint.json");
  writeCheckpoint(path, value);
  return { back: readCheckpoint(path), text: readFileSync(path, "utf8") };
};

describe("a checkpoint", () => {
  test("comes back as JSON would bring it, awkward values and all, and the file is still JSON", () => {
    const tricky = Array.from({ length: SPILL_AT + 5 }, (_, i) => ({
      Id: `00T${String(i).padStart(12, "0")}AAA`,
      text: ["ends with [", "] starts so", "line\nbreak", "é and 日本", "", "\u0000spill"][i % 6],
      missing: undefined,
      when: new Date(Date.UTC(2026, 0, 1 + (i % 28))),
      nested: [[1, 2], { a: [] }],
      nothing: null,
    }));
    const value = { status: "ok", data: tricky, small: [1, undefined, 3], source: { query: "SELECT Id FROM Task" }, truncated: false, inner: { more: tricky.slice(0, 3), big: [...tricky] } };
    const { back, text } = roundTrip(value);
    assert.deepEqual(back, same(value));
    assert.deepEqual(JSON.parse(text), same(value), "a whole-file JSON.parse reads it too");
    assert.ok(text.split("\n").length > 2 * SPILL_AT, "long arrays an item to a line");
    for (const v of [null, 3, "x", [], {}, Array.from({ length: SPILL_AT }, (_, i) => i)]) assert.deepEqual(roundTrip(v).back, same(v));
  });

  test("every checkpoint of the synthetic org's run comes back as it went in", async () => {
    const { dir } = await runFromRecording({ answers: loadAnswers(), modules: ALL_MODULES });
    const folder = join(dir, "checkpoints");
    for (const file of readdirSync(folder).filter((f) => f.endsWith(".json"))) {
      const value = readCheckpoint(join(folder, file));
      assert.deepEqual(roundTrip(value).back, value, file);
    }
  });

  test("one from before, with two-space indents, or on one line, is still read", () => {
    const value = { status: "ok", data: [{ Id: "a", list: [1, 2] }], source: { query: "x" } };
    for (const text of [JSON.stringify(value, null, 2), JSON.stringify(value)]) {
      const path = join(tempDir(), "old.json");
      writeFileSync(path, `${text}\n`);
      assert.deepEqual(readCheckpoint(path), value);
    }
  });

  test("lines are read whole across the reader's chunks, with no character split", () => {
    const path = join(tempDir(), "lines.jsonl");
    const lines = Array.from({ length: 300_000 }, (_, i) => JSON.stringify({ n: i, s: "é日".repeat(i % 7) }));
    writeFileSync(path, `${lines.join("\n")}\n`);
    let i = 0;
    for (const line of fileLines(path)) if (line) assert.equal(line, lines[i++]);
    assert.equal(i, lines.length);
  });
});
