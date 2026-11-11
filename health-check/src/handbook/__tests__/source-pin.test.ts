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
// nobody touched? A fourth entry, with no number, holds the code that gives
// each component its last-changed date (decision 486).

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
      why: "what the step keeps of each body as it reads it: the scan, whether it's a test and what it touches (and each one's last-changed date, which no number vouches for: see the dates below)",
      text: () => region("src/extract/automation.ts", "// The body isn't kept", "export const apexExtractor", "\n};\n"),
    },
  ],
  build: [
    whole("src/handbook/build.ts", "builds the file from the steps' output: a trigger's events, the sorting, the caps and cuts, each last change (which date goes to which component no number vouches for: see the dates below)"),
    { what: "src/handbook/schema.ts: the caps and the events", why: "the Apex lists are cut at them, and a trigger's events ordered by them, when the file is built", text: () => caps(/^export const (MAX_\w+|TRIGGER_EVENTS) =/) },
    whole("src/domains/object-names.ts", "names a trigger's object from its id when the file is built"),
    {
      // The custom objects step has no number of its own, so its names are
      // held under the build's, which names a trigger's object from them
      // (decision 486). A run read before a change here and built after it
      // would carry the old names under the new number, since a step is reused
      // while its version is unchanged (src/engine/run.ts). So an edit that
      // changes a custom object's name raises the step's version as well as
      // BUILD_READING, and a run in flight reads the step again.
      what: "src/extract/configuration.ts: the custom objects step",
      why: "makes each custom object's name, from which the build names a trigger's object. If the edit changes a custom object's name, raise customObjectsExtractor's version in src/extract/configuration.ts as well as BUILD_READING, so a run in flight reads the step again and can't build old names under the new number (DECISIONS.md 486)",
      text: () => region("src/extract/configuration.ts", "export type CustomObjectRow =", "export const customObjectsExtractor", "\n};\n"),
    },
    {
      what: "src/extract/metadata-parse.ts: unreadReason",
      why: "says, when the file is built, whether the validation rules were read whole",
      text: () => region("src/extract/metadata-parse.ts", "export type UnreadKind =", "export function unreadReason(", "\n}\n"),
    },
  ],
};

const PINS = {
  metadata: { number: 1, hash: "0838b8a50807bd43", constant: "METADATA_READING in src/handbook/detail.ts", reading: METADATA_READING, kind: "a flow or validation rule" },
  apex: { number: 1, hash: "142d6e56dea8ff84", constant: "APEX_READING in src/extract/apex-touches.ts", reading: APEX_READING, kind: "an Apex class or trigger" },
  build: { number: 1, hash: "71bfb343fab8b85c", constant: "BUILD_READING in src/handbook/build.ts", reading: BUILD_READING, kind: "a flow, validation rule, Apex class or trigger" },
};

const hashOf = (sources: Source[]): string =>
  createHash("sha256")
    .update(JSON.stringify(sources.map((s) => [s.what, s.text()])))
    .digest("hex")
    .slice(0, 16);

// Which last-changed date each component is given (decision 486). Dates are
// compared against every earlier handbook whatever its numbers (decision 484),
// so no number can vouch for them, and this entry has none: an edit here asks
// for a decision, not a raise. The build's matching of a date to its component
// is in build.ts and the apex step's reading of each class's and trigger's date
// in its region of automation.ts, hashed above under readBy.build and
// readBy.apex, so neither is hashed again here.
const DATES = {
  hash: "825a48bf67ed71fe",
  sources: [whole("src/extract/component-changes.ts", "which version's date each flow is given, and the name and object each flow's and rule's date is matched by")],
};

test("which last-changed date each component is given is pinned, with no number", () => {
  const hash = hashOf(DATES.sources);
  const files = DATES.sources.map((s) => `  ${s.what}: ${s.why}`).join("\n");
  assert.equal(
    hash,
    DATES.hash,
    `The code that gives each flow and validation rule its last-changed date changed (hash ${hash}):\n${files}\nLast-changed dates are compared against every earlier handbook whatever its numbers, so no number vouches for them: a change to which date a component nobody touched is given would be printed as a change in every re-check. Stop and decide before changing this hash (DECISIONS.md 486). The code in src/handbook/build.ts that matches a date to its component, and the apex step's reading of each class's and trigger's date in src/extract/automation.ts, are hashed under readBy.build and readBy.apex, not here again; the same holds for them.`,
  );
});

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
