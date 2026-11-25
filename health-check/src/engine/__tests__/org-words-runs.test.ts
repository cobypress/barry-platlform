import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { checkedDigest } from "../../digest/checked.ts";
import { SALES_CLOUD_MODULES } from "../../domains/index.ts";
import { loadRun } from "../../render/render-run.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { automaticSyntheticCalibration } from "../../testing/automatic.ts";
import { tempDir } from "../../testing/harness.ts";
import { syntheticProvider } from "../../testing/replay.ts";
import { Simulator } from "../../testing/simulator.ts";
import { AS_OF, CONNECTED_USER } from "../../testing/synthetic-org.ts";
import { runHealthCheck } from "../run.ts";

// One customer's run never reaches another's in the same worker (DECISIONS.md 743).
// The store of org-worded sentences was the process's: an org with a stage named
// "Modify All Data" left the next org's privileged-permission finding, which says
// those words as ours, unknown, and its run held digest_held. Each check now has a
// store of its own. Two orgs, analysed one after the other in both orders and side
// by side, as the worker's concurrency runs them, give each exactly what it gives alone.

/** The synthetic org with one stage renamed to words a check of ours writes. */
class Renamed implements Transport {
  readonly #inner = new Simulator();
  async send(request: TransportRequest): Promise<TransportResponse> {
    const r = await this.#inner.send(request);
    return { status: r.status, header: (name) => r.header(name), text: r.text.replaceAll('"Closed Lost"', '"Modify All Data"') };
  }
}

const calibration = await automaticSyntheticCalibration();
type Org = "renamed" | "plain";
async function check(org: Org) {
  const run = await runHealthCheck({ provider: syntheticProvider, calibration, modules: SALES_CLOUD_MODULES, outDir: tempDir(), now: new Date(AS_OF), transport: org === "renamed" ? new Renamed() : new Simulator(), connectedUser: CONNECTED_USER, log: () => {}, reviewMode: "automatic" });
  const { input, pseudonyms } = loadRun(run.dir);
  const c = checkedDigest({ input, pseudonyms, inventory: null, madeAt: "2026-11-25T09:00:00.000Z" });
  // What the run's marks and its check come to, without its id or folder.
  return JSON.stringify({
    findings: input.findings.map((f) => [f.ruleId, f.status, f.title, f.summary, f.notMeasurableReason, f.orgWords ?? null]),
    metrics: input.metrics.map((m) => [m.id, m.status, m.orgWords ?? null]),
    check: { held: c.held, unmarked: c.unmarked, unmatched: c.unmatched, masks: c.masks },
  });
}

describe("two orgs' checks in one worker process (DECISIONS.md 743)", async () => {
  // Each first alone, in a process where neither has run: the plain org first, then the renamed one, each before the other could reach it.
  const plainAlone = await check("plain");
  const renamedAlone = await check("renamed");

  test("the renamed org does reach the words: its stage is said as the org's", () => {
    assert.ok(renamedAlone.includes("Modify All Data"));
    assert.notEqual(renamedAlone, plainAlone);
  });

  test("the plain org alone is released", () => {
    assert.deepEqual(JSON.parse(plainAlone).check, { held: null, unmarked: 0, unmatched: 0, masks: 0 });
  });

  test("renamed, then plain: the plain org's run is as alone, released", async () => {
    await check("renamed");
    assert.equal(await check("plain"), plainAlone);
  });

  test("plain, then renamed: the renamed org's run is as alone", async () => {
    await check("plain");
    assert.equal(await check("renamed"), renamedAlone);
  });

  test("side by side, as the worker's concurrency runs them: each as alone", async () => {
    const [a, b, c, d] = await Promise.all([check("renamed"), check("plain"), check("plain"), check("renamed")]);
    assert.deepEqual([a, b, c, d], [renamedAlone, plainAlone, plainAlone, renamedAlone]);
  });
});
