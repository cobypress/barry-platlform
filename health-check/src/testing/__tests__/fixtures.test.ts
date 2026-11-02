import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { recordFixtures } from "../record-fixtures.ts";
import { FIXTURE_DIR, loadRecording } from "../replay.ts";
import { fileURLToPath } from "node:url";
import { unzipSync, strFromU8 } from "fflate";

// Rule 8. Positive rules on purpose: a list of customer names to ban would
// itself put customer names in the repo.

const SYNTHETIC_FILE = fileURLToPath(new URL("../synthetic-org.ts", import.meta.url));
const texts = (): Array<[string, string]> => [
  ...readdirSync(FIXTURE_DIR).map((f): [string, string] => [f, readFileSync(join(FIXTURE_DIR, f), "utf8")]),
  ["synthetic-org.ts", readFileSync(SYNTHETIC_FILE, "utf8")],
];

const EMAIL = /[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+/g;
const SF_ID = /\b(?:00D|005|001|003|006|008|00Q)[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?\b/g;
const SYNTHETIC_ID = /^[0-9A-Za-z]{3}0{6}\d{6}(AAA)?$/;

describe("fixtures are synthetic", () => {
  test("every email address uses a reserved domain", () => {
    for (const [name, text] of texts()) {
      for (const email of text.match(EMAIL) ?? []) assert.match(email, /@example\.(com|org)$/, `${name}: ${email}`);
    }
  });

  test("every Salesforce id is zero-padded, so can't be a real record", () => {
    for (const [name, text] of texts()) {
      for (const id of text.match(SF_ID) ?? []) assert.match(id, SYNTHETIC_ID, `${name}: ${id}`);
    }
  });

  test("metadata inside the recorded zip follows the same rules", () => {
    const zipFile = loadRecording().exchanges.map((e) => e.text.match(/<zipFile>([^<]+)<\/zipFile>/)?.[1]).find(Boolean)!;
    const files = unzipSync(Buffer.from(zipFile, "base64"));
    assert.ok(Object.keys(files).length > 5);
    for (const bytes of Object.values(files)) {
      const text = strFromU8(bytes);
      for (const email of text.match(EMAIL) ?? []) assert.match(email, /@example\.(com|org)$/);
      for (const id of text.match(SF_ID) ?? []) assert.match(id, SYNTHETIC_ID);
    }
  });

  test("recordings hold no credentials", () => {
    const text = JSON.stringify(loadRecording());
    assert.doesNotMatch(text, /synthetic-token-not-real|Bearer |sessionId/);
    assert.doesNotMatch(text, /https:\/\/[a-z0-9-]+\.(my\.)?salesforce\.com/);
  });
});

describe("the recording", () => {
  test("matches what the code asks for today", async () => {
    const fresh = await recordFixtures();
    assert.deepEqual(fresh.exchanges.map((e) => e.key), loadRecording().exchanges.map((e) => e.key), "run npm run fixtures");
  });
});
