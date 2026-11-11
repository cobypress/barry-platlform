import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { APEX_READING } from "../../extract/apex-touches.ts";
import { BUILD_READING } from "../build.ts";
import { METADATA_READING } from "../detail.ts";

// Each of the three numbers in handbook.json's readBy held beside a hash of
// the text of the code that shapes what the handbook keeps under it
// (DECISIONS.md 485). The output pin (pin.test.ts) knows only what the
// synthetic org holds, so a parser fix for something it doesn't hold passes
// there: 83d4045 and b914ad8 each did. This fails on any edit to these files,
// noisy on purpose, as the fingerprint is, and asks the one question that
// matters: could a handbook now hold something different for a component
// nobody touched?

const root = fileURLToPath(new URL("../../../", import.meta.url));
const text = (path: string): string => readFileSync(join(root, path), "utf8");

/** From the line holding `from` to the end of the first block closed at `close` after `within`. Fails rather than hash nothing. */
function region(path: string, from: string, within: string, close: string): string {
  const whole = text(path);
  const start = whole.indexOf(from);
  assert.ok(start >= 0 && whole.indexOf(from, start + 1) < 0, `${path}: "${from}" is there once`);
  const inner = whole.indexOf(within, start);
  assert.ok(inner >= 0, `${path}: "${within}" follows it`);
  const end = whole.indexOf(close, inner);
  assert.ok(end >= 0, `${path}: the block after "${within}" closes`);
  return whole.slice(whole.lastIndexOf("\n", start) + 1, end + close.length);
}

/** The caps in schema.ts: each a one-line constant, so a line is the whole of one. */
function caps(names: RegExp): string {
  const lines = text("src/handbook/schema.ts")
    .split("\n")
    .filter((line) => names.test(line));
  assert.ok(lines.length > 0 && lines.every((line) => line.trimEnd().endsWith(";")), "schema.ts: each cap is on one line");
  return lines.join("\n");
}

/** An installed dependency's version, as the lock file pins it. */
function locked(name: string): string {
  const lock = JSON.parse(text("package-lock.json")) as { packages: Record<string, { version?: string }> };
  const version = lock.packages[`node_modules/${name}`]?.version;
  assert.ok(version, `${name} is in the lock file`);
  return `${name}@${version}`;
}

type Source = { what: string; why: string; text: () => string };

const whole = (path: string, why: string): Source => ({ what: path, why, text: () => text(path) });

/** What shapes what the handbook keeps, by the number that vouches for it. */
const SOURCES: Record<"metadata" | "apex" | "build", Source[]> = {
  metadata: [
    whole("src/handbook/detail.ts", "works out each flow's and rule's detail, on both routes"),
    whole("src/extract/metadata-parse.ts", "the retrieve's parser and its decoding, and the flow's object"),
    whole("src/extract/metadata-tooling.ts", "what Tooling hands over, redacted as it's read and kept part way"),
    whole("src/privacy/secrets.ts", "the redaction of every piece of free text"),
    { what: "src/handbook/schema.ts: the caps", why: "detail.ts cuts steps, fields, conditions, paths and text at them when the step reads", text: () => caps(/^export const MAX_(STEPS|FIELDS|CONDITIONS|PATHS|TEXT) =/) },
    {
      what: "src/extract/automation.ts: the metadata step",
      why: "which flows and objects the retrieve asks for, and which route reads them",
      text: () => region("src/extract/automation.ts", "// Salesforce's wildcard for CustomObject", "export const metadataExtractor", "\n};\n"),
    },
    {
      what: "src/salesforce/client.ts: retrieveMetadata",
      why: "unzips the retrieve and decodes each file to the text the parser reads",
      text: () => region("src/salesforce/client.ts", "  async retrieveMetadata(", "  async retrieveMetadata(", "\n  }\n"),
    },
    { what: "package-lock.json: fast-xml-parser and fflate", why: "the XML parser and the unzip under them", text: () => `${locked("fast-xml-parser")}\n${locked("fflate")}` },
  ],
  apex: [
    whole("src/extract/apex-touches.ts", "what each body touches"),
    whole("src/extract/apex-scan.ts", "the comments and strings blanked first, the statements, and whether it's a test"),
    {
      what: "src/extract/automation.ts: the apex step",
      why: "what the step keeps of each body as it reads it: the scan, whether it's a test and what it touches",
      text: () => region("src/extract/automation.ts", "// The body isn't kept", "export const apexExtractor", "\n};\n"),
    },
  ],
  build: [
    whole("src/handbook/build.ts", "builds the file from the steps' output: a trigger's events, the sorting, the caps and cuts, each last change"),
    { what: "src/handbook/schema.ts: the caps and the events", why: "the Apex lists are cut at them, and a trigger's events ordered by them, when the file is built", text: () => caps(/^export const (MAX_\w+|TRIGGER_EVENTS) =/) },
    whole("src/domains/object-names.ts", "names a trigger's object from its id when the file is built"),
  ],
};

const PINS = {
  metadata: { number: 1, hash: "0838b8a50807bd43", constant: "METADATA_READING in src/handbook/detail.ts", reading: METADATA_READING, kind: "a flow or validation rule" },
  apex: { number: 1, hash: "8052dd2011545d9b", constant: "APEX_READING in src/extract/apex-touches.ts", reading: APEX_READING, kind: "an Apex class or trigger" },
  build: { number: 1, hash: "e5ea57d0c75a7a52", constant: "BUILD_READING in src/handbook/build.ts", reading: BUILD_READING, kind: "a flow, validation rule, Apex class or trigger" },
};

const hashOf = (sources: Source[]): string =>
  createHash("sha256")
    .update(JSON.stringify(sources.map((s) => [s.what, s.text()])))
    .digest("hex")
    .slice(0, 16);

for (const key of ["metadata", "apex", "build"] as const) {
  test(`readBy.${key}'s number is pinned beside the text of what shapes it`, () => {
    const pin = PINS[key];
    assert.equal(pin.reading, pin.number, `${pin.constant} moved: update this pin's number and hash`);
    const hash = hashOf(SOURCES[key]);
    const files = SOURCES[key].map((s) => `  ${s.what}: ${s.why}`).join("\n");
    assert.equal(
      hash,
      pin.hash,
      `The code that shapes what handbook.json keeps under readBy.${key} changed (hash ${hash}):\n${files}\nCould a handbook now hold something different for ${pin.kind} nobody touched? If so, raise ${pin.constant} and update this pin's number and hash. If not, update the hash alone.`,
    );
  });
}
