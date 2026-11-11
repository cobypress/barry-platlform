import { test, before } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { APEX_READING } from "../../extract/apex-touches.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator, type SimulatorOptions } from "../../testing/simulator.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { METADATA_READING } from "../detail.ts";
import { readHandbook } from "../read.ts";
import type { Handbook } from "../schema.ts";

// Each reading's number held to what it reads from the synthetic org
// (DECISIONS.md 484). A re-check compares a kind's details only when both
// files hold the same number, so a change to how we read that leaves the number
// alone would show every component it moves as changed in the org. Only what
// the reading works out is hashed: not when each was last changed, not readBy,
// not the recent changes, so nothing else moves a pin. The second of two
// checks: source-pin.test.ts holds each number to the text of its code
// (decision 485), which catches what the synthetic org doesn't hold.

const PINS = {
  metadata: { reading: 1, hash: "78228ad58aa193a2" },
  apex: { reading: 1, hash: "70dc66b571d6a373" },
};

const fix = (half: keyof typeof PINS, route: string, hash: string): string =>
  `The ${half} reading of the synthetic org moved on the ${route} route (hash ${hash}). If how we read changed, raise ${half === "metadata" ? "METADATA_READING in src/handbook/detail.ts" : "APEX_READING in src/extract/apex-touches.ts, or BUILD_READING in src/handbook/build.ts if only a trigger's events or object moved,"} and update this pin's number and hash; if only the synthetic org changed, update the hash alone.`;

const hashOf = (value: unknown): string => createHash("sha256").update(JSON.stringify(value)).digest("hex").slice(0, 16);

/** Each flow's and rule's detail, without its last change. */
function metadataDetail(h: Handbook): unknown {
  if (h.flows.status !== "ok" || h.validationRules.status !== "ok") return assert.fail("the synthetic org's flows and rules were read");
  const undated = <T extends { lastChanged: unknown }>({ lastChanged: _, ...rest }: T) => rest;
  return { flows: h.flows.items.map(undated), validationRules: h.validationRules.items.map(undated) };
}

/**
 * What the code tells of each class and trigger: whether it's a test, what it
 * touches, and a trigger's events and the object it's on, which the build
 * names from the custom objects step (decision 486).
 */
function apexDetail(h: Handbook): unknown {
  if (h.apexClasses.status !== "ok" || h.apexTriggers.status !== "ok") return assert.fail("the synthetic org's Apex was read");
  return {
    classes: h.apexClasses.items.map((c) => [c.name, c.isTest, c.touches]),
    triggers: h.apexTriggers.items.map((t) => [t.name, t.isTest, t.touches, t.events, t.object]),
  };
}

const ROUTES: Array<[string, SimulatorOptions]> = [
  ["retrieve", {}],
  ["Tooling", { readOnly: true }],
];
const handbooks = new Map<string, Handbook>();
before(async () => {
  for (const [route, options] of ROUTES) {
    const result = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator(options), log: () => {} });
    const read = readHandbook(result.dir);
    handbooks.set(route, read.ok ? read.handbook : assert.fail(read.why));
  }
});

test("the flows' and rules' reading is pinned to its number, on both routes", () => {
  assert.equal(METADATA_READING, PINS.metadata.reading, "METADATA_READING moved: update this pin's number, and its hash if the reading moved it");
  for (const [route] of ROUTES) {
    const hash = hashOf(metadataDetail(handbooks.get(route)!));
    assert.equal(hash, PINS.metadata.hash, fix("metadata", route, hash));
  }
});

test("the Apex reading is pinned to its number, on both routes", () => {
  assert.equal(APEX_READING, PINS.apex.reading, "APEX_READING moved: update this pin's number, and its hash if the reading moved it");
  for (const [route] of ROUTES) {
    const hash = hashOf(apexDetail(handbooks.get(route)!));
    assert.equal(hash, PINS.apex.hash, fix("apex", route, hash));
  }
});
