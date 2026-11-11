import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { runHealthCheck } from "../../engine/run.ts";
import { readCheckpoint } from "../../engine/checkpoint.ts";
import { APEX_READING } from "../../extract/apex-touches.ts";
import type { ApexData } from "../../extract/automation.ts";
import type { MetadataBundle } from "../../extract/metadata-parse.ts";
import type { Sourced } from "../../extract/types.ts";
import { loadPseudonymMap } from "../../render/render-run.ts";
import { docxText } from "../../testing/docx-text.ts";
import { EARLIER_AS_OF } from "../../testing/earlier-handbook.ts";
import { tempDir } from "../../testing/harness.ts";
import { loadCalibration, syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import type { Transport } from "../../salesforce/transport.ts";
import { AS_OF } from "../../testing/synthetic-org.ts";
import { BUILD_READING, HANDBOOK_FILE } from "../build.ts";
import { compareHandbooks, type KindComparison } from "../compare.ts";
import { METADATA_READING } from "../detail.ts";
import { renderHandbookDocx, SINCE_TITLE } from "../docx.ts";
import { readHandbook } from "../read.ts";
import { Handbook, HANDBOOK_VERSION } from "../schema.ts";
import { withoutReadings } from "./older-files.ts";

// Which reading made a handbook (DECISIONS.md 484) is stamped on each step's
// output when the step reads, never when the file is built: a run in flight
// when this deploys resumes with checkpoints from before, reads nothing again,
// and its handbook says null for the half it reused, so a re-check set against
// that half compares only what was added or removed and when each last changed.

type Steps = { steps: Array<{ id: string; version: number; status: string; calls: number }> };
const read = <T>(dir: string, file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const run = (options: { outDir?: string; resume?: string } = {}) => {
  const transport = new Simulator();
  const done = runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: options.outDir ?? tempDir(), now: new Date(AS_OF), transport, log: () => {}, resume: options.resume });
  return done.then((result) => ({ ...result, transport }));
};

const handbookIn = (dir: string): Handbook => {
  const h = readHandbook(dir);
  return h.ok ? h.handbook : assert.fail(h.why);
};

/** How often the transport was asked to read what each half's step reads. */
function reads(sim: Simulator): { metadata: number; apex: number } {
  const asked = sim.requests.map((r) => `${decodeURIComponent(r.url)}\n${r.body ?? ""}`);
  return {
    // The retrieve, or Tooling's flows one at a time: the metadata step's own reads on either route.
    metadata: asked.filter((a) => a.includes("<met:retrieve>") || /FROM FlowDefinition/.test(a)).length,
    apex: asked.filter((a) => /FROM Apex(Class|Trigger) WHERE/.test(a)).length,
  };
}

/** A finished run with one step's checkpoint as an engine before decision 484 wrote it: no stamp, the step's version as it is. */
async function fromBefore(strip: "metadata" | "apex") {
  const first = await run();
  // The count below finds each step's reads, so none on the resume means none.
  assert.ok(reads(first.transport).metadata > 0 && reads(first.transport).apex > 0);
  const checkpoints = join(first.dir, "checkpoints");
  if (strip === "metadata") {
    const bundle = readCheckpoint<Sourced<MetadataBundle>>(join(checkpoints, "metadata.json"));
    assert.ok(bundle.status === "ok" && bundle.data.handbook?.handbookReading === METADATA_READING, "the step stamped its own output");
    delete bundle.data.handbook!.handbookReading;
    writeFileSync(join(checkpoints, "metadata.json"), JSON.stringify(bundle));
  } else {
    const apex = readCheckpoint<ApexData>(join(checkpoints, "apex.json"));
    assert.equal(apex.handbookReading, APEX_READING, "the step stamped its own output");
    delete apex.handbookReading;
    writeFileSync(join(checkpoints, "apex.json"), JSON.stringify(apex));
  }
  return first;
}

const compared = (c: KindComparison) => (c.status === "compared" ? c : assert.fail(`not compared: ${c.unreadIn}`));

/** The whole of the run's handbook set against an earlier one, as the customer reads it. */
async function documentText(earlier: Handbook, dir: string): Promise<string> {
  return docxText(await renderHandbookDocx(handbookIn(dir), { clientName: "Synthetic Co", runDate: AS_OF, pseudonyms: loadPseudonymMap(dir), earlier: { handbook: earlier, asOf: EARLIER_AS_OF } }));
}

/** One kind's words in the part. */
function kindIn(text: string, title: string): string {
  const part = text.slice(text.indexOf(`\n## ${SINCE_TITLE}\n`), text.indexOf("\n## Recent changes\n"));
  const start = part.indexOf(`\n### ${title}\n`);
  assert.ok(start >= 0, `the part has ${title}`);
  const rest = part.slice(start + `\n### ${title}\n`.length);
  return rest.slice(0, rest.indexOf("\n### ") < 0 ? undefined : rest.indexOf("\n### ")).trim();
}

const kindText = async (earlier: Handbook, dir: string, title: string): Promise<string> => kindIn(await documentText(earlier, dir), title);

/**
 * The sentence for a kind one health check holds no number for (decision
 * 486), the same whichever case left it empty, and true of each: a file from
 * before the numbers were kept, a reused step read before then, and a Tooling
 * read stopped under one number and resumed under another.
 */
const UNKNOWN = (which: "your earlier health check" | "this health check", many: string, one: string): string =>
  `We can't tell which version of our reader read your ${many} in ${which}, and two versions can read the same ${one} differently, so we've compared only which were added or removed and when each was last changed.`;

/** The heart of what the part said before decision 486, false of a read resumed under another number. */
const OLD_WORDS = /we began to record/;

const UNRECORDED_NOW = (many: string, one: string): RegExp => new RegExp(`\\n\\n${UNKNOWN("this health check", many, one).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`);

describe("a run resumed with a step's checkpoint from before the readings were kept", () => {
  for (const half of ["metadata", "apex"] as const) {
    test(`the ${half} step's: not read again, and its half of readBy is null while the other's is the number`, async () => {
      const earlier = handbookIn((await run()).dir);
      const first = await fromBefore(half);
      const calls = read<Steps>(first.dir, "run.json").steps.map((s) => [s.id, s.version, s.calls]);
      const resumed = await run({ outDir: join(first.dir, ".."), resume: first.run.id });
      assert.ok(resumed.output, "the run finished");
      // Nothing read again: no step's reads, its version and its calls as they were.
      assert.deepEqual(reads(resumed.transport), { metadata: 0, apex: 0 });
      assert.deepEqual(read<Steps>(first.dir, "run.json").steps.map((s) => [s.id, s.version, s.calls]), calls);

      const file = read<{ version: number; readBy: unknown }>(first.dir, HANDBOOK_FILE);
      assert.equal(file.version, HANDBOOK_VERSION);
      assert.equal(HANDBOOK_VERSION, 6);
      assert.deepEqual(file.readBy, half === "metadata" ? { metadata: null, apex: APEX_READING, build: BUILD_READING } : { metadata: METADATA_READING, apex: null, build: BUILD_READING });

      // Set against a run read afresh, the reused half is compared by its dates alone, and says it's this run's.
      const c = compareHandbooks(earlier, handbookIn(first.dir));
      const readings = Object.values(c).map((k) => compared(k).reading);
      assert.deepEqual(readings, half === "metadata" ? ["unknownNow", "unknownNow", null, null] : [null, null, "unknownNow", "unknownNow"]);
      if (half === "metadata") assert.match(await kindText(earlier, first.dir, "Flows"), UNRECORDED_NOW("flows", "flow"));
      else assert.match(await kindText(earlier, first.dir, "Apex classes"), UNRECORDED_NOW("Apex classes", "Apex class"));
    });
  }

  test("the apex step's from before, with the metadata read afresh: the number for the half read now, null for the half reused", async () => {
    const first = await fromBefore("apex");
    const run1 = read<Steps>(first.dir, "run.json");
    // Stopped before the metadata step finished, as a run at its budget is.
    run1.steps.find((s) => s.id === "metadata")!.status = "pending";
    writeFileSync(join(first.dir, "run.json"), JSON.stringify(run1));
    rmSync(join(first.dir, "checkpoints", "metadata.json"));
    const resumed = await run({ outDir: join(first.dir, ".."), resume: first.run.id });
    assert.ok(resumed.output, "the run finished");
    const asked = reads(resumed.transport);
    assert.ok(asked.metadata > 0, "the metadata step read");
    assert.equal(asked.apex, 0, "the Apex wasn't read again");
    assert.deepEqual(read<{ readBy: unknown }>(first.dir, HANDBOOK_FILE).readBy, { metadata: METADATA_READING, apex: null, build: BUILD_READING });
  });
});

describe("a Tooling read stopped part way and resumed (decision 485)", () => {
  // The items kept part way through were redacted by the code that read them,
  // so a read resumed from them stamps the number only where the kept file says
  // they were read under it.
  const READING_KEPT = "metadata-tooling-reading.partial.jsonl";
  const ITEMS_KEPT = "metadata-tooling.partial.jsonl";
  /** Tooling's reads of one flow's definition each. */
  const flowReads = (sim: Simulator): number => sim.requests.filter((r) => /FROM Flow WHERE Id/.test(decodeURIComponent(r.url))).length;

  async function stoppedPartWay(): Promise<{ outDir: string; runId: string; checkpoints: string }> {
    const simulator = new Simulator({ readOnly: true });
    // The flows and rules are read and kept; the custom fields after them never come.
    const dropping: Transport = { send: (request) => (decodeURIComponent(request.url).includes("FROM FieldDefinition") ? Promise.reject(new Error("socket hang up")) : simulator.send(request)) };
    const outDir = tempDir();
    await assert.rejects(runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport: dropping, log: () => {} }), /socket hang up/);
    const [runId] = readdirSync(outDir);
    const checkpoints = join(outDir, runId!, "checkpoints");
    assert.ok(flowReads(simulator) > 0 && readFileSync(join(checkpoints, ITEMS_KEPT), "utf8").includes('"key":"Flow/'), "flows were read and kept part way");
    // The number alone, nothing read from the org.
    assert.equal(readFileSync(join(checkpoints, READING_KEPT), "utf8"), `${JSON.stringify({ handbookReading: METADATA_READING })}\n`);
    return { outDir, runId: runId!, checkpoints };
  }

  const cases: Array<[string, (checkpoints: string) => void, number | null]> = [
    ["a kept file that records no number, as one an engine before this kept", (c) => rmSync(join(c, READING_KEPT)), null],
    ["a kept file that records another number", (c) => writeFileSync(join(c, READING_KEPT), `${JSON.stringify({ handbookReading: METADATA_READING + 1 })}\n`), null],
    ["a kept file that records this number", () => {}, METADATA_READING],
  ];
  for (const [what, edit, expected] of cases) {
    test(`${what}: the flows' and rules' reading is ${expected === null ? "null" : "the number"}`, async () => {
      const { outDir, runId, checkpoints } = await stoppedPartWay();
      edit(checkpoints);
      const transport = new Simulator({ readOnly: true });
      const resumed = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir, now: new Date(AS_OF), transport, log: () => {}, resume: runId });
      assert.ok(resumed.output, "the run finished");
      // Resumed from the kept items: the step ran again, and read no flow again.
      assert.ok(reads(transport).metadata > 0, "the metadata step ran");
      assert.equal(flowReads(transport), 0, "no flow read again");
      assert.deepEqual(read<{ readBy: unknown }>(resumed.dir, HANDBOOK_FILE).readBy, { metadata: expected, apex: APEX_READING, build: BUILD_READING });
      // Both kept files are cleared once the step's output is saved.
      assert.ok(!existsSync(join(checkpoints, READING_KEPT)) && !existsSync(join(checkpoints, ITEMS_KEPT)));
      if (expected !== null) return;

      // Set against a run read afresh by the same route, the part says we
      // can't tell which version read this one, which is true whether a number
      // was never kept or another was (decision 486).
      const fresh = await runHealthCheck({ provider: syntheticProvider, calibration: loadCalibration(), modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: new Simulator({ readOnly: true }), log: () => {} });
      const text = await documentText(handbookIn(fresh.dir), resumed.dir);
      assert.equal(kindIn(text, "Flows"), `No flows were added, removed or changed.\n\n${UNKNOWN("this health check", "flows", "flow")}`);
      assert.equal(kindIn(text, "Validation rules"), `No validation rules were added, removed or changed.\n\n${UNKNOWN("this health check", "validation rules", "validation rule")}`);
      assert.doesNotMatch(text, OLD_WORDS);
    });
  }
});

describe("an earlier handbook from before the numbers were kept (decision 486)", () => {
  test("a version 5 baseline: the same sentence, of the earlier health check", async () => {
    const now = await run();
    const dir = join(tempDir(), "earlier");
    mkdirSync(dir);
    const file = read<Record<string, unknown>>(now.dir, HANDBOOK_FILE);
    writeFileSync(join(dir, HANDBOOK_FILE), JSON.stringify({ ...withoutReadings(file), version: 5 }));
    const earlier = handbookIn(dir);
    assert.deepEqual(earlier.readBy, { metadata: null, apex: null, build: null });
    const text = await documentText(earlier, now.dir);
    assert.equal(kindIn(text, "Flows"), `No flows were added, removed or changed.\n\n${UNKNOWN("your earlier health check", "flows", "flow")}`);
    assert.equal(kindIn(text, "Validation rules"), `No validation rules were added, removed or changed.\n\n${UNKNOWN("your earlier health check", "validation rules", "validation rule")}`);
    assert.doesNotMatch(text, OLD_WORDS);
  });
});
