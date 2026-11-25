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
  /** The org's settings through the Connect API (DECISIONS.md 740): unset, the replay has no answer for them, so they can't say. */
  settings: unknown = undefined;
  async send(request: TransportRequest): Promise<TransportResponse> {
    if (this.settings !== undefined && new URL(request.url).pathname.endsWith("/connect/organization")) return { status: 200, header: () => null, text: JSON.stringify(this.settings) };
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
    assert.equal(c.currency, "XXX");
    assert.equal(c.currencyFrom, "org");
    assert.equal(c.currencySuggested, "GBP");
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

  test("an unmapped locale still calibrates, its one currency unnamed and nothing suggested, and runs: never held for it", async () => {
    const { org, deps, handle } = await pressed();
    org.locale = "en";
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.notEqual(outcome.outcome, "failed", outcome.detail);
    assert.equal(deps.store.runs.get(RUN_ID)!.calibration!.currencyFrom, "org");
    assert.equal(deps.store.runs.get(RUN_ID)!.calibration!.currencySuggested, undefined);
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

  test("a connection to another org reads nothing and saves nothing", async () => {
    const { org, deps, handle } = await pressed();
    await deps.tokens.save(syntheticConnection({ orgId: "00D000000000099AAA" }));
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(outcome.outcome, "failed");
    assert.match(outcome.detail, /is for a different org from this run, so nothing was read/);
    assert.equal(org.asked.length, 0);
    assert.equal(deps.store.runs.get(RUN_ID)!.calibration, null);
    assert.ok(!deps.store.writes.some((w) => w.method === "saveCalibration" || w.method === "saveOrgStages"));
  });

  test("fix round 1: a hold that can't be recorded fails the job, never leaving the run extracting with no hold and no calibration", async () => {
    const { org, deps, handle } = await pressed();
    org.stages = "refused";
    deps.store.heldReasonColumn = false;
    // Not the last delivery: back to the queue, to try again.
    await assert.rejects(handle({ kind: "run", runId: RUN_ID }, AGAIN), /its hold couldn't be recorded/);
    // The last: the failure is recorded, with its flag.
    const outcome = await handle({ kind: "run", runId: RUN_ID }, { ...AGAIN, final: true });
    assert.equal(outcome.outcome, "failed");
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "failed");
    assert.ok(deps.store.flagsOf(RUN_ID).includes("run_failed"));
  });
});

describe("Run again with a currency the customer named in the portal (KAN-41)", () => {
  test("the rebuilt calibration names it, and the released run's digest names it with no note", async () => {
    const { deps, handle } = await pressed();
    deps.store.customerCurrencies.set(ORG15, "EUR");
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(outcome.outcome, "done", outcome.detail);
    const c = deps.store.runs.get(RUN_ID)!.calibration!;
    assert.equal(c.currency, "EUR");
    assert.equal(c.currencyFrom, "customer");
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    const digest = deps.store.digests.get(RUN_ID)!.digest;
    assert.equal(digest.value.currency, "EUR");
    assert.equal(digest.value.currencyNote, undefined);
    assert.ok(digest.findings.every((f) => f.amountsUnnamed === undefined));
  });

  test("an answer changed later: the newest stands at the next calibration", async () => {
    const { deps, handle } = await pressed();
    deps.store.customerCurrencies.set(ORG15, "EUR");
    deps.store.customerCurrencies.set(ORG15, "CHF");
    await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(deps.store.runs.get(RUN_ID)!.calibration!.currency, "CHF");
  });
});

describe("Run again with a currency the org's settings name (740)", () => {
  test("the rebuilt calibration names it as the settings', over the customer's answer, and the released run's digest names it with no note", async () => {
    const { org, deps, handle } = await pressed();
    org.settings = { features: { defaultCurrencyIsoCode: "GBP", multiCurrency: false } };
    deps.store.customerCurrencies.set(ORG15, "EUR");
    const outcome = await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    assert.equal(outcome.outcome, "done", outcome.detail);
    const c = deps.store.runs.get(RUN_ID)!.calibration!;
    assert.deepEqual([c.currency, c.currencyFrom, c.currencySuggested], ["GBP", "connect_api", undefined]);
    assert.ok(!org.asked.includes(ORG_LOCALE_SOQL), "the locale isn't read once the settings name the currency");
    assert.ok(deps.lines.some((l) => /customer named EUR in the portal, but the org's settings name GBP/.test(l)));
    assert.equal(deps.store.runs.get(RUN_ID)!.run.status, "released");
    const digest = deps.store.digests.get(RUN_ID)!.digest;
    assert.equal(digest.value.currency, "GBP");
    assert.equal(digest.value.currencyNote, undefined);
    assert.equal(digest.value.currencySuggested, undefined);
    assert.ok(digest.findings.every((f) => f.amountsUnnamed === undefined));
  });

  test("settings that can't say: the run is rebuilt exactly as before 740, one currency unnamed, the locale's suggestion kept apart", async () => {
    const { org, deps, handle } = await pressed();
    org.settings = { defaultCurrencyIsoCode: "GBP" };
    await handle({ kind: "run", runId: RUN_ID }, AGAIN);
    const c = deps.store.runs.get(RUN_ID)!.calibration!;
    assert.deepEqual([c.currency, c.currencyFrom, c.currencySuggested], ["XXX", "org", "GBP"]);
    assert.ok(org.asked.includes(ORG_LOCALE_SOQL));
    assert.ok(deps.store.digests.get(RUN_ID)!.digest.value.currencyNote);
  });
});
