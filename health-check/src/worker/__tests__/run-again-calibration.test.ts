import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { CALIBRATION_UNREADABLE } from "../../connect/auto-calibration.ts";
import { ORG_CURRENCY_SOQL, ORG_LOCALE_SOQL } from "../../connect/org-currency.ts";
import { STAGES_SOQL } from "../../extract/opportunities.ts";
import type { Transport, TransportRequest, TransportResponse } from "../../salesforce/transport.ts";
import { AutomaticReplay } from "../../testing/automatic.ts";
import { ORG_ID, STAGES } from "../../testing/synthetic-org.ts";
import { createJobHandler } from "../worker.ts";
import { baseDeps, consoleRun, RUN_ID, runInput, syntheticConnection } from "./helpers.ts";

// Runs held before decision 700 for a currency a single-currency org can't
// give: held calibration_unreadable with no calibration, though their stages
// hold a won stage. The console's "Run again" (run-<runId>-again-<ms>, 698) on
// such a run reads the org again through its stored connection, calibrates it
// and starts from a clean folder; still none, it is held again with the true
// cause in its flag's counts (DECISIONS.md 700).

const ORG15 = ORG_ID.slice(0, 15);
const AGAIN = { final: false, jobId: `run-${RUN_ID}-again-1764000000000` };

/** The synthetic org as recorded, with the two Organization reads a single-currency org answers, and its stages as told. */
class SingleCurrencyOrg implements Transport {
  readonly inner = new AutomaticReplay();
  readonly asked: string[] = [];
  stages: "recorded" | "refused" | "none won" = "recorded";
  locale = "en_GB";
  /** Set: once the locale is read, every request fails as an outage would, until it is cleared. */
  outageAfterLocale = false;
  #down = false;
  async send(request: TransportRequest): Promise<TransportResponse> {
    const q = new URL(request.url).searchParams.get("q") ?? "";
    if (this.#down && this.outageAfterLocale) return { status: 503, header: () => null, text: "unavailable" };
    if (q === ORG_LOCALE_SOQL && this.outageAfterLocale) this.#down = true;
    if (q) this.asked.push(q);
    const fail = (errorCode: string, message: string): TransportResponse => ({ status: 400, header: () => null, text: JSON.stringify([{ errorCode, message }]) });
    if (q === ORG_CURRENCY_SOQL) return fail("INVALID_FIELD", "No such column 'DefaultCurrencyIsoCode' on entity 'Organization'.");
    if (q === ORG_LOCALE_SOQL) return { status: 200, header: () => null, text: JSON.stringify({ records: [{ attributes: { type: "Organization" }, DefaultLocaleSidKey: this.locale }], totalSize: 1, done: true }) };
    if (q === STAGES_SOQL && this.stages === "refused") return fail("INSUFFICIENT_ACCESS", "no");
    const response = await this.inner.send(request);
    if (q !== STAGES_SOQL || this.stages !== "none won") return response;
    const page = JSON.parse(response.text) as { records: Array<Record<string, unknown>> };
    return { ...response, text: JSON.stringify({ ...page, records: page.records.map((r) => ({ ...r, IsWon: false })) }) };
  }
}

/** An automatic run held at connect before 700, with no calibration, as the console leaves it after "Run again": extracting, its hold and flag cleared. */
async function pressed(options: { automatic?: boolean; switchOn?: boolean } = {}) {
  const org = new SingleCurrencyOrg();
  const deps = baseDeps({ dataTransport: org, automatic: { on: options.switchOn ?? true, orgs: new Set([ORG15]) } });
  deps.store.addRun(runInput({ run: consoleRun({ reviewMode: options.automatic === false ? "consultant" : "automatic" }), calibration: null, intake: null, answers: [] }));
  await deps.tokens.save(syntheticConnection());
  return { org, deps, handle: createJobHandler(deps) };
}

const orgReads = (org: SingleCurrencyOrg) => org.asked.filter((q) => q === ORG_CURRENCY_SOQL || q === ORG_LOCALE_SOQL).length;

describe("Run again of an automatic run with no calibration (DECISIONS.md 700)", () => {
  test("reads the stages and currency again, saves the calibration, and runs from a clean folder to its release", async () => {
    const { org, deps, handle } = await pressed();
    const dir = join(deps.runsDir, RUN_ID);
    mkdirSync(dir, { recursive: true });
    writeFileSync(join(dir, "left-from-before.txt"), "x");
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(outcome.outcome, "done", outcome.detail);
    const c = deps.store.runs.get(RUN_ID)!.calibration!;
    assert.equal(c.source, "org_settings");
    assert.equal(c.currency, "GBP");
    assert.equal(c.currencyFrom, "locale");
    assert.ok(c.stages.some((s) => s.category === "won"));
    assert.ok(!existsSync(join(dir, "left-from-before.txt")), "started from a clean folder");
    assert.equal(orgReads(org), 2);
    assert.equal(deps.store.held.size, 0);
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    assert.ok(deps.lines.includes(`Automatic run ${RUN_ID} had no calibration, so its org was read again and one was built and saved before it was run again.`));
  });

  test("redelivered after it finished, the same press reads nothing more and saves no second calibration", async () => {
    const { org, deps, handle } = await pressed();
    await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    const saved = structuredClone(deps.store.runs.get(RUN_ID)!.calibration);
    const reads = org.asked.length;
    const again = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(again.outcome, "skipped");
    assert.equal(org.asked.length, reads);
    assert.deepEqual(deps.store.runs.get(RUN_ID)!.calibration, saved);
    assert.equal(deps.store.writes.filter((w) => w.method === "saveCalibration").length, 1);
  });

  test("a delivery stopped by an outage after saving the calibration: the retry finds it and goes on, reading the currency no more", async () => {
    const { org, deps, handle } = await pressed();
    org.outageAfterLocale = true;
    await assert.rejects(handle({ kind: "run", runId: RUN_ID }, AGAIN));
    assert.ok(deps.store.runs.get(RUN_ID)!.calibration, "saved before it stopped");
    org.outageAfterLocale = false;
    const reads = orgReads(org);
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(outcome.outcome, "done", outcome.detail);
    assert.equal(orgReads(org), reads);
  });

  test("an unmapped locale still calibrates, with no currency, and runs: never held for it", async () => {
    const { org, deps, handle } = await pressed();
    org.locale = "en";
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.notEqual(outcome.outcome, "failed", outcome.detail);
    assert.equal(deps.store.runs.get(RUN_ID)!.calibration!.currencyFrom, "none");
    assert.ok(!deps.store.flagsOf(RUN_ID).includes(CALIBRATION_UNREADABLE));
  });

  test("still no calibration: held again with its true cause in the flag's counts, and the folder left as it was", async () => {
    for (const [stages, counts, cause] of [["refused", { stages: 0, won: 0 }, "no stages could be read"], ["none won", { stages: STAGES.length, won: 0 }, "no stage is closed and won"]] as const) {
      const { org, deps, handle } = await pressed();
      org.stages = stages;
      const dir = join(deps.runsDir, RUN_ID);
      mkdirSync(dir, { recursive: true });
      writeFileSync(join(dir, "left-from-before.txt"), "x");
      const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
      assert.equal(outcome.outcome, "skipped", stages);
      assert.match(outcome.detail, new RegExp(`^Run ${RUN_ID}: Held: ${CALIBRATION_UNREADABLE}\\. `));
      assert.equal(deps.store.held.get(RUN_ID), CALIBRATION_UNREADABLE);
      const flag = [...deps.store.flags.values()].find((f) => f.runId === RUN_ID)!;
      assert.equal(flag.kind, CALIBRATION_UNREADABLE);
      assert.deepEqual(flag.counts, counts, stages);
      assert.ok(deps.lines.some((l) => l.includes(`(${cause};`)), stages);
      assert.equal(deps.store.runs.get(RUN_ID)!.calibration, null);
      assert.ok(existsSync(join(dir, "left-from-before.txt")), "nothing cleared");
      // Redelivered: held, so nothing more is read or raised.
      const reads = org.asked.length;
      await handle({ kind: "run", runId: RUN_ID }, AGAIN);
      assert.equal(org.asked.length, reads);
      assert.equal(deps.store.writes.filter((w) => w.method === "raiseFlag").length, 1);
    }
  });

  test("only a person's press: the first job, a consultant-led run, or the switch off reads nothing and refuses as before", async () => {
    for (const [name, setUp, attempt] of [
      ["the fixed id", {}, { final: false, jobId: `run-${RUN_ID}` }],
      ["a consultant-led run", { automatic: false }, AGAIN],
      ["the switch off", { switchOn: false }, AGAIN],
    ] as const) {
      const { org, deps, handle } = await pressed(setUp);
      const outcome = await handle({ kind: "run", runId: RUN_ID }, attempt);
      assert.equal(orgReads(org), 0, name);
      assert.equal(deps.store.runs.get(RUN_ID)!.calibration, null, name);
      assert.notEqual(outcome.outcome, "done", name);
    }
  });
});
